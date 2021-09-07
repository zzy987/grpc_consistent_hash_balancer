package balancer

import (
	"context"
	"fmt"
	"grpc_consistent_hash_balancer/util"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var (
	SubConnNotFoundError  = fmt.Errorf("SubConn not found")
	ResetSubConnFailError = fmt.Errorf("reset SubConn fail")
)

func init() {
	balancer.Register(newConsistentHashBalancerBuilder())
}

// newConsistentHashBalancerBuilder returns a consistentHashBalancerBuilder.
func newConsistentHashBalancerBuilder() balancer.Builder {
	return &consistentHashBalancerBuilder{}
}

// consistentHashBalancerBuilder is a empty struct with functions Build and Name, implemented from balancer.Builder
type consistentHashBalancerBuilder struct{}

// Build creates a consistentHashBalancer, and starts its scManager.
func (builder *consistentHashBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &consistentHashBalancer{
		cc:             cc,
		addrInfos:      make(map[string]resolver.Address),
		subConns:       make(map[string]balancer.SubConn),
		scInfos:        sync.Map{},
		pickResultChan: make(chan PickResult),
		pickResults:    util.NewQueue(),
		scCounts:       make(map[balancer.SubConn]*int32),
	}
	go b.scManager()
	return b
}

// Name returns the name of the consistentHashBalancer registering in grpc.
func (builder *consistentHashBalancerBuilder) Name() string {
	return Policy
}

// subConnInfo records the state and addr corresponding to the SubConn.
type subConnInfo struct {
	state connectivity.State
	addr  string
}

// consistentHashBalancer is modified from baseBalancer, you can refer to https://github.com/grpc/grpc-go/blob/master/balancer/base/balancer.go
type consistentHashBalancer struct {
	// cc points to the balancer.ClientConn who creates the consistentHashBalancer.
	cc balancer.ClientConn

	// state indicates the state of the whole ClientConn from the perspective of the balancer.
	state connectivity.State

	// addrInfos maps the address string to resolver.Address.
	addrInfos map[string]resolver.Address
	// subConns maps the address string to balancer.SubConn.
	subConns map[string]balancer.SubConn
	// scInfos maps the balancer.SubConn to subConnInfo.
	scInfos sync.Map

	// picker is a balancer.Picker created by the balancer but used by the ClientConn.
	picker balancer.Picker

	// resolverErr is the last error reported by the resolver; cleared on successful resolution.
	resolverErr error
	// connErr is the last connection error; cleared upon leaving TransientFailure
	connErr error

	// pickResultChan is the channel for the picker to report PickResult to the balancer.
	pickResultChan chan PickResult
	// pickResults is the Queue storing PickResult with a undone context, updating asynchronously.
	pickResults *util.Queue
	// scCounts records the amount of PickResults with the SubConn in pickResults.
	scCounts map[balancer.SubConn]*int32
	// scCountsLock is the lock for the scCounts map.
	scCountsLock sync.Mutex
}

// UpdateClientConnState is implemented from balancer.Balancer, modified from the baseBalancer,
// ClientConn will call it after Builder builds the balancer to pass the necessary data.
func (b *consistentHashBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	b.resolverErr = nil
	addrsSet := make(map[string]struct{})
	for _, a := range s.ResolverState.Addresses {
		addr := a.Addr
		b.addrInfos[addr] = a
		addrsSet[addr] = struct{}{}
		if sc, ok := b.subConns[addr]; !ok {
			newSC, err := b.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
			if err != nil {
				log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
				continue
			}
			b.subConns[addr] = newSC
			b.scInfos.Store(newSC, &subConnInfo{
				state: connectivity.Idle,
				addr:  addr,
			})
			// newSC.Connect()

			// newSC.Connect() -> acBalancerWrapper.Connect() -> addrConn.connect() -> addrConn.updateConnectivityState()
			// -> ClientConn.handleSubConnStateChange() -> ccBalancerWrapper.handleSubConnStateChange()
			// -> ccBalancerWrapper.updateCh.Put() -> ccBalancerWrapper.watcher() -> balancer.UpdateSubConnState(),
			// and UpdateSubConnState calls ClientConn.UpdateState() at last.
			// And when a connection is ready, it will enter balancer.UpdateSubConnState(), too,
			// and the picker will be generated at the first time in the design of the baseBalancer.

			// PickerWrapper will block if the picker is nil, or the picker.Pick returns a bad SubConn,
			// and it will be unblocked when ClientConn.UpdateState() is called.
			// Because we do not connect here, grpc will not enter UpdateSubConn or other functions that will call ClientConn.UpdateState() with a picker.
			// and the programme will be blocked by the pickerWrapper permanently.
			// We should use another way to achieve ClientConn.UpdateState() with a picker before the first pick,
			// for example, I call ClientConn.UpdateState() directly.
		} else {
			b.cc.UpdateAddresses(sc, []resolver.Address{a})
		}
	}
	for a, sc := range b.subConns {
		if _, ok := addrsSet[a]; !ok {
			b.cc.RemoveSubConn(sc)
			delete(b.subConns, a)
		}
	}
	if len(s.ResolverState.Addresses) == 0 {
		b.ResolverError(fmt.Errorf("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// As we want to do the connection management ourselves, we don't set up connection here.
	// Connection has not been ready yet. The next two lines is a trick aims to make grpc continue executing.
	// if the picker is nil in ClientConn, it will block at next pick in pickerWrapper,
	// So I have to generate the picker first.
	b.regeneratePicker()
	// I call ClientConn.UpdateState() directly.
	b.cc.UpdateState(balancer.State{ConnectivityState: connectivity.Ready, Picker: b.picker})

	return nil
}

// ResolverError is implemented from balancer.Balancer, copied from baseBalancer.
func (b *consistentHashBalancer) ResolverError(err error) {
	b.resolverErr = err

	b.regeneratePicker()
	if b.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	b.cc.UpdateState(balancer.State{
		ConnectivityState: b.state,
		Picker:            b.picker,
	})
}

// regeneratePicker generates a new picker to replace the old one with new data, and update the state of the balancer.
func (b *consistentHashBalancer) regeneratePicker() {
	availableSCs := make(map[string]balancer.SubConn)
	for addr, sc := range b.subConns {
		// The next line may not be safe, but we have to use subConns without check,
		// for we have not set up connections with any SubConn, all of them are Idle.
		if st, ok := b.scInfos.Load(sc); ok && st.(*subConnInfo).state != connectivity.Shutdown && st.(*subConnInfo).state != connectivity.TransientFailure {
			availableSCs[addr] = sc
		}
	}
	if len(availableSCs) == 0 {
		b.state = connectivity.TransientFailure
		b.picker = base.NewErrPicker(b.mergeErrors())
	} else {
		b.state = connectivity.Ready
		b.picker = NewConsistentHashPickerWithReportChan(availableSCs, b.pickResultChan)
	}
}

// mergeErrors is copied from baseBalancer.
func (b *consistentHashBalancer) mergeErrors() error {
	if b.connErr == nil {
		return fmt.Errorf("last resolver error: %v", b.resolverErr)
	}
	if b.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", b.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", b.connErr, b.resolverErr)
}

// UpdateSubConnState is implemented by balancer.Balancer, modified from baseBalancer
func (b *consistentHashBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	v, ok := b.scInfos.Load(sc)
	if !ok {
		return
	}
	info := v.(*subConnInfo)
	oldS := info.state
	log.Printf("state of one subConn changed from %s to %s\n", oldS.String(), s.String())
	if oldS == connectivity.TransientFailure && s == connectivity.Connecting {
		return
	}
	info.state = s
	switch s {
	case connectivity.Idle:
		// I do not know when it will come here.
		// sc.Connect()
	case connectivity.Shutdown:
		b.scInfos.Delete(sc)
	case connectivity.TransientFailure:
		b.connErr = state.ConnectionError
	}

	if (s == connectivity.TransientFailure || s == connectivity.Shutdown) != (oldS == connectivity.TransientFailure || oldS == connectivity.Shutdown) ||
		b.state == connectivity.TransientFailure {
		b.regeneratePicker()
	}

	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
}

// Close is implemented by balancer.Balancer, copied from baseBalancer.
func (b *consistentHashBalancer) Close() {}

// resetSubConn will replace a SubConn with a new idle SubConn.
func (b *consistentHashBalancer) resetSubConn(sc balancer.SubConn) error {
	addr, err := b.getSubConnAddr(sc)
	if err != nil {
		return err
	}
	log.Printf("Reset connect with addr %s", addr)
	err = b.resetSubConnWithAddr(addr)
	return err
}

// getSubConnAddr returns the address string of a SubConn.
func (b *consistentHashBalancer) getSubConnAddr(sc balancer.SubConn) (string, error) {
	v, ok := b.scInfos.Load(sc)
	if !ok {
		return "", SubConnNotFoundError
	}
	return v.(*subConnInfo).addr, nil
}

// resetSubConnWithAddr creates a new idle SubConn for the address string, and remove the old one.
func (b *consistentHashBalancer) resetSubConnWithAddr(addr string) error {
	sc, ok := b.subConns[addr]
	if !ok {
		return SubConnNotFoundError
	}
	b.scInfos.Delete(sc)
	b.cc.RemoveSubConn(sc)
	newSC, err := b.cc.NewSubConn([]resolver.Address{b.addrInfos[addr]}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
	if err != nil {
		log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
		return ResetSubConnFailError
	}
	b.subConns[addr] = newSC
	b.scInfos.Store(newSC, &subConnInfo{
		state: connectivity.Idle,
		addr:  addr,
	})
	b.regeneratePicker()
	b.cc.UpdateState(balancer.State{ConnectivityState: b.state, Picker: b.picker})
	return nil
}

// scManager launches two goroutines to receive PickResult and query whether the context of the stored PickResult is done.
func (b *consistentHashBalancer) scManager() {
	// The first goroutine listens to the pickResultChan, put pickResults into a queue.
	// Because the second go routine will reset a SubConn if there is no pickResult with the SubConn in the queue,
	// and we want to hold a SubConn for a while to reuse it, I use a "shadow context" with timeout to achieve both.
	go func() {
		for {
			pr := <-b.pickResultChan
			b.pickResults.EnQueue(pr)
			// Use a shadow context to ensure one of the necessary conditions of calling resetSubConn is that at least a defined time duration has passed since each previous request.
			// This trick can reduce a goroutine traversing the entire map and compare time.Now() and the recorded expired time.
			shadowCtx, _ := context.WithTimeout(context.Background(), connectionLifetime)
			b.pickResults.EnQueue(PickResult{Ctx: shadowCtx, SC: pr.SC})
			b.scCountsLock.Lock()
			cnt, ok := b.scCounts[pr.SC]
			if !ok {
				cnt = new(int32)
				*cnt = 0
				b.scCounts[pr.SC] = cnt
			}
			// I want to use sync.map to replace the map scCounts, so I use atomic. But I find it (the replacement) is not easy...
			atomic.AddInt32(cnt, 2)
			b.scCountsLock.Unlock()
		}
	}()

	// The second goroutine checks the pickResults in the queue. if the context of a pickResult is done, it will drop the pickResults.
	// It will reset a SubConn when there is no pickResults with the SubConn after dropped one.
	// Oh no, TODO what if the picker picks the SubConn is resetting?
	go func() {
		for {
			v, ok := b.pickResults.DeQueue()
			if !ok {
				time.Sleep(connectionLifetime)
				continue
			}
			pr := v.(PickResult)
			select {
			case <-pr.Ctx.Done():
				b.scCountsLock.Lock()
				cnt, ok := b.scCounts[pr.SC]
				if !ok {
					b.scCountsLock.Unlock()
					break
				}
				atomic.AddInt32(cnt, -1)
				if *cnt == 0 {
					delete(b.scCounts, pr.SC)
					b.resetSubConn(pr.SC)
				}
				b.scCountsLock.Unlock()
			default:
				b.pickResults.EnQueue(pr)
			}
		}
	}()
}
