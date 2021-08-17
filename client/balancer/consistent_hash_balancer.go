package balancer

import "C"
import (
	"context"
	"fmt"
	"grpc_consistent_hash_balancer/util"
	"log"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

var (
	SubConnNotFoundError  = fmt.Errorf("SubConn not found")
	ResetSubConnFailError = fmt.Errorf("reset SubConn fail")
)

// RegisterConsistentHashBalancerBuilder register a consistentHashBalancerBuilder to balancer package in grpc.
func RegisterConsistentHashBalancerBuilder() {
	balancer.Register(newConsistentHashBalancerBuilder())
}

// newConsistentHashBalancerBuilder returns a consistentHashBalancerBuilder.
func newConsistentHashBalancerBuilder() balancer.Builder {
	return &consistentHashBalancerBuilder{}
}

// consistentHashBalancerBuilder is a empty struct with functions Build and Name, implemented from balancer.Builder
type consistentHashBalancerBuilder struct{}

// Build creates a consistentHashBalancer, and starts its scManager.
func (c *consistentHashBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &consistentHashBalancer{
		cc:             cc,
		addrInfos:      make(map[string]resolver.Address),
		subConns:       make(map[string]balancer.SubConn),
		scInfos:        sync.Map{},
		csEvltr:        &balancer.ConnectivityStateEvaluator{},
		pickResultChan: make(chan PickResult),
		pickResults:    util.NewQueue(),
		scCounts:       make(map[balancer.SubConn]*int32),
	}
	go b.scManager()
	return b
}

// Name returns the name of the consistentHashBalancer registering in grpc.
func (c *consistentHashBalancerBuilder) Name() string {
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
	//ccCheater sync.Once

	// csEvltr appears in baseBalancer, I don't know much about it.
	csEvltr *balancer.ConnectivityStateEvaluator
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
func (c *consistentHashBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	c.resolverErr = nil
	addrsSet := make(map[string]struct{})
	for _, a := range s.ResolverState.Addresses {
		addr := a.Addr
		c.addrInfos[addr] = a
		addrsSet[addr] = struct{}{}
		if sc, ok := c.subConns[addr]; !ok {
			newSC, err := c.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
			if err != nil {
				log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
				continue
			}
			c.subConns[addr] = newSC
			c.scInfos.Store(newSC, &subConnInfo{
				state: connectivity.Idle,
				addr:  addr,
			})
			//newSC.Connect()

			// The next three lines in comment is a way to cheat grpc.
			//c.ccCheater.Do(func() {
			//	newSC.Connect()
			//})
		} else {
			c.cc.UpdateAddresses(sc, []resolver.Address{a})
		}
	}
	for a, sc := range c.subConns {
		if _, ok := addrsSet[a]; !ok {
			c.cc.RemoveSubConn(sc)
			delete(c.subConns, a)
		}
	}
	if len(s.ResolverState.Addresses) == 0 {
		c.ResolverError(fmt.Errorf("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// As we want to do the connection management ourselves, we don't set up connection here.
	// Connection has not been ready yet. The next two lines is a trick aims to make grpc continue executing.
	c.regeneratePicker()
	c.cc.UpdateState(balancer.State{ConnectivityState: connectivity.Ready, Picker: c.picker})

	return nil
}

// ResolverError is implemented from balancer.Balancer, copied from baseBalancer.
func (c *consistentHashBalancer) ResolverError(err error) {
	c.resolverErr = err
	if len(c.subConns) == 0 {
		c.state = connectivity.TransientFailure
	}

	if c.state != connectivity.TransientFailure {
		// The picker will not change since the balancer does not currently
		// report an error.
		return
	}
	c.regeneratePicker()
	c.cc.UpdateState(balancer.State{
		ConnectivityState: c.state,
		Picker:            c.picker,
	})
}

// regeneratePicker generates a new picker to replace the old one with new data.
func (c *consistentHashBalancer) regeneratePicker() {
	if c.state == connectivity.TransientFailure {
		c.picker = base.NewErrPicker(c.mergeErrors())
		return
	}
	readySCs := make(map[string]balancer.SubConn)
	for addr, sc := range c.subConns {
		// The next line may not be safe, but we have to use subConns without check, for we didn't set up connections in function UpdateClientConnState.
		if st, ok := c.scInfos.Load(sc); ok && st.(*subConnInfo).state != connectivity.Shutdown {
			readySCs[addr] = sc
		}
	}
	if c.picker == nil {
		c.picker = NewConsistentHashPickerWithReportChan(readySCs, c.pickResultChan)
	} else if cp, ok := c.picker.(*consistentHashPicker); ok {
		cp.Refresh(c.subConns)
	} else {
		c.picker = NewConsistentHashPickerWithReportChan(readySCs, c.pickResultChan)
	}

}

// mergeErrors is copied from baseBalancer.
func (c *consistentHashBalancer) mergeErrors() error {
	if c.connErr == nil {
		return fmt.Errorf("last resolver error: %v", c.resolverErr)
	}
	if c.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", c.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", c.connErr, c.resolverErr)
}

// UpdateSubConnState is implemented by balancer.Balancer, modified from baseBalancer
func (c *consistentHashBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	// make sure the delete in reset will execute before the query here, to directly return from the function if the SubConn state changes because of reset.
	v, ok := c.scInfos.Load(sc)
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
		sc.Connect()
	case connectivity.Shutdown:
		c.scInfos.Delete(sc)
	case connectivity.TransientFailure:
		c.connErr = state.ConnectionError
	}

	c.state = c.csEvltr.RecordTransition(oldS, s)

	if (s == connectivity.Ready) != (oldS == connectivity.Ready) ||
		c.state == connectivity.TransientFailure {
		c.regeneratePicker()
	}

	c.cc.UpdateState(balancer.State{ConnectivityState: c.state, Picker: c.picker})
}

// Close is implemented by balancer.Balancer, copied from baseBalancer.
func (c *consistentHashBalancer) Close() {}

// resetSubConn will replace a SubConn with a new idle SubConn.
func (c *consistentHashBalancer) resetSubConn(sc balancer.SubConn) error {
	addr, err := c.getSubConnAddr(sc)
	if err != nil {
		return err
	}
	log.Printf("Reset connect with addr %s", addr)
	err = c.resetSubConnWithAddr(addr)
	return err
}

// getSubConnAddr returns the address string of a SubConn.
func (c *consistentHashBalancer) getSubConnAddr(sc balancer.SubConn) (string, error) {
	v, ok := c.scInfos.Load(sc)
	if !ok {
		return "", SubConnNotFoundError
	}
	return v.(*subConnInfo).addr, nil
}

// resetSubConnWithAddr creates a new idle SubConn for the address string, and remove the old one.
func (c *consistentHashBalancer) resetSubConnWithAddr(addr string) error {
	sc, ok := c.subConns[addr]
	if !ok {
		return SubConnNotFoundError
	}
	c.scInfos.Delete(sc)
	c.cc.RemoveSubConn(sc)
	newSC, err := c.cc.NewSubConn([]resolver.Address{c.addrInfos[addr]}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
	if err != nil {
		log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
		return ResetSubConnFailError
	}
	c.subConns[addr] = newSC
	c.scInfos.Store(newSC, &subConnInfo{
		state: connectivity.Idle,
		addr:  addr,
	})
	if cp, ok := c.picker.(*consistentHashPicker); ok {
		cp.ResetAddrSubConn(addr, newSC)
		c.cc.UpdateState(balancer.State{ConnectivityState: c.state, Picker: c.picker})
	} else {
		c.regeneratePicker()
		c.cc.UpdateState(balancer.State{ConnectivityState: c.state, Picker: c.picker})
	}
	return nil
}

// scManager launches two goroutines to receive PickResult and query whether the context of the stored PickResult is done.
func (c *consistentHashBalancer) scManager() {
	go func() {
		for {
			pr := <-c.pickResultChan
			c.pickResults.EnQueue(pr)
			// use a shadow context to ensure one of the necessary conditions of calling resetSubConn is that at least a defined time duration has passed since each previous request.
			shadowCtx, _ := context.WithTimeout(context.Background(), connectionLifetime)
			c.pickResults.EnQueue(PickResult{Ctx: shadowCtx, SC: pr.SC})
			c.scCountsLock.Lock()
			cnt, ok := c.scCounts[pr.SC]
			if !ok {
				cnt = new(int32)
				*cnt = 0
				c.scCounts[pr.SC] = cnt
			}
			atomic.AddInt32(cnt, 2)
			c.scCountsLock.Unlock()
		}
	}()

	go func() {
		for {
			v, ok := c.pickResults.DeQueue()
			if !ok {
				continue
			}
			pr := v.(PickResult)
			select {
			case <-pr.Ctx.Done():
				c.scCountsLock.Lock()
				cnt, ok := c.scCounts[pr.SC]
				if !ok {
					c.scCountsLock.Unlock()
					break
				}
				atomic.AddInt32(cnt, -1)
				if *cnt == 0 {
					delete(c.scCounts, pr.SC)
					c.resetSubConn(pr.SC)
				}
				c.scCountsLock.Unlock()
			default:
				c.pickResults.EnQueue(pr)
			}
		}
	}()
}
