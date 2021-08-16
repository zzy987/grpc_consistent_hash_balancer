package balancer

import "C"
import (
	"fmt"
	"grpc_consistent_hash_balancer/util"
	"log"
	"sync"
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

func RegisterConsistentHashBalancerBuilder() {
	balancer.Register(newConsistentHashBalancerBuilder())
}

func newConsistentHashBalancerBuilder() balancer.Builder {
	return &consistentHashBalancerBuilder{}
}

type consistentHashBalancerBuilder struct{}

func (c *consistentHashBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	b := &consistentHashBalancer{
		cc:             cc,
		addrInfos:      make(map[string]resolver.Address),
		subConns:       make(map[string]balancer.SubConn),
		scInfos:        make(map[balancer.SubConn]*subConnInfo),
		csEvltr:        &balancer.ConnectivityStateEvaluator{},
		pickResultChan: make(chan PickResult),
		pickResults:    util.NewQueue(),
		scRecords:      make(map[balancer.SubConn]*scRecord),
	}
	go b.scManager()
	return b
}

func (c *consistentHashBalancerBuilder) Name() string {
	return Policy
}

type subConnInfo struct {
	state connectivity.State
	addr  string
}

type scRecord struct {
	latestAccess    time.Time
	connectionCount int
}

// consistentHashBalancer is modified from baseBalancer, you can refer to https://github.com/grpc/grpc-go/blob/master/balancer/base/balancer.go
type consistentHashBalancer struct {
	cc balancer.ClientConn
	//ccCheater sync.Once

	csEvltr *balancer.ConnectivityStateEvaluator
	state   connectivity.State

	addrInfos   map[string]resolver.Address
	subConns    map[string]balancer.SubConn
	scInfos     map[balancer.SubConn]*subConnInfo
	scInfosLock sync.RWMutex

	picker balancer.Picker

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	connErr     error // the last connection error; cleared upon leaving TransientFailure

	pickResultChan chan PickResult
	pickResults    *util.Queue
	scRecords      map[balancer.SubConn]*scRecord
	scRecordsLock  sync.Mutex
}

func (c *consistentHashBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	c.resolverErr = nil
	addrsSet := make(map[string]struct{})
	c.scInfosLock.Lock()
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
			c.scInfos[newSC] = &subConnInfo{
				state: connectivity.Idle,
				addr:  addr,
			}
			//newSC.Connect()

			// The next three lines is a way to cheat grpc.
			//c.ccCheater.Do(func() {
			//	newSC.Connect()
			//})
		} else {
			c.cc.UpdateAddresses(sc, []resolver.Address{a})
		}
	}
	c.scInfosLock.Unlock()
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

func (c *consistentHashBalancer) regeneratePicker() {
	if c.state == connectivity.TransientFailure {
		c.picker = base.NewErrPicker(c.mergeErrors())
		return
	}
	readySCs := make(map[string]balancer.SubConn)
	c.scInfosLock.RLock()
	for addr, sc := range c.subConns {
		// The next line may not be safe, but we have to use subConns without check, for we didn't set up connections in function UpdateClientConnState.
		if st, ok := c.scInfos[sc]; ok && st.state != connectivity.Shutdown {
			readySCs[addr] = sc
		}
	}
	c.scInfosLock.RUnlock()
	c.picker = NewConsistentHashPickerWithReportChan(readySCs, c.pickResultChan)
}

func (c *consistentHashBalancer) mergeErrors() error {
	if c.connErr == nil {
		return fmt.Errorf("last resolver error: %v", c.resolverErr)
	}
	if c.resolverErr == nil {
		return fmt.Errorf("last connection error: %v", c.connErr)
	}
	return fmt.Errorf("last connection error: %v; last resolver error: %v", c.connErr, c.resolverErr)
}

func (c *consistentHashBalancer) UpdateSubConnState(sc balancer.SubConn, state balancer.SubConnState) {
	s := state.ConnectivityState
	// Actually the scInfosLock is used to make sure the delete in reset will execute before the query here, to directly return from the function if the SubConn state changes because of reset.
	c.scInfosLock.RLock()
	oldInfo, ok := c.scInfos[sc]
	c.scInfosLock.RUnlock()
	if !ok {
		return
	}
	oldS := oldInfo.state
	log.Printf("state of one subConn changed from %s to %s\n", oldS.String(), s.String())
	if oldS == connectivity.TransientFailure && s == connectivity.Connecting {
		return
	}
	c.scInfosLock.Lock()
	c.scInfos[sc].state = s
	switch s {
	case connectivity.Idle:
		sc.Connect()
	case connectivity.Shutdown:
		delete(c.scInfos, sc)
	case connectivity.TransientFailure:
		c.connErr = state.ConnectionError
	}
	c.scInfosLock.Unlock()

	c.state = c.csEvltr.RecordTransition(oldS, s)

	if (s == connectivity.Ready) != (oldS == connectivity.Ready) ||
		c.state == connectivity.TransientFailure {
		c.regeneratePicker()
	}

	c.cc.UpdateState(balancer.State{ConnectivityState: c.state, Picker: c.picker})
}

func (c *consistentHashBalancer) Close() {}

func (c *consistentHashBalancer) resetSubConn(sc balancer.SubConn) error {
	addr, err := c.getSubConnAddr(sc)
	if err != nil {
		return err
	}
	log.Printf("Reset connect with addr %s", addr)
	err = c.resetSubConnWithAddr(addr)
	return err
}

func (c *consistentHashBalancer) getSubConnAddr(sc balancer.SubConn) (string, error) {
	c.scInfosLock.RLock()
	scInfo, ok := c.scInfos[sc]
	c.scInfosLock.RUnlock()
	if !ok {
		return "", SubConnNotFoundError
	}
	return scInfo.addr, nil
}

func (c *consistentHashBalancer) resetSubConnWithAddr(addr string) error {
	sc, ok := c.subConns[addr]
	if !ok {
		return SubConnNotFoundError
	}
	c.scInfosLock.Lock()
	delete(c.scInfos, sc)
	c.cc.RemoveSubConn(sc)
	newSC, err := c.cc.NewSubConn([]resolver.Address{c.addrInfos[addr]}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
	if err != nil {
		log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
		return ResetSubConnFailError
	}
	c.subConns[addr] = newSC
	c.scInfos[newSC] = &subConnInfo{
		state: connectivity.Idle,
		addr:  addr,
	}
	c.scInfosLock.Unlock()
	c.regeneratePicker()
	c.cc.UpdateState(balancer.State{ConnectivityState: c.state, Picker: c.picker})
	return nil
}

func (c *consistentHashBalancer) scManager() {
	go func() {
		for {
			pr := <-c.pickResultChan
			c.pickResults.EnQueue(pr)
			c.scRecordsLock.Lock()
			sr, ok := c.scRecords[pr.SC]
			if !ok {
				c.scRecords[pr.SC] = &scRecord{connectionCount: 0, latestAccess: time.Now()}
				sr = c.scRecords[pr.SC]
			}
			sr.connectionCount++
			sr.latestAccess = time.Now()
			c.scRecordsLock.Unlock()
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
				c.scRecordsLock.Lock()
				sr, ok := c.scRecords[pr.SC]
				if !ok {
					// never come here
					c.scRecordsLock.Unlock()
					break
				}
				sr.connectionCount--
				if sr.connectionCount == 0 {
					delete(c.scRecords, pr.SC)
					c.resetSubConn(pr.SC)
				}
				c.scRecordsLock.Unlock()
			default:
				c.pickResults.EnQueue(pr)
			}
		}
	}()

	//go func() {
	//	for {
	//		for sc, sr := range c.scRecords {
	//			if sr.connectionCount == 0 {
	//				c.resetSubConn(sc)
	//				c.scRecordsLock.Lock()
	//				delete(c.scRecords, sc)
	//				c.scRecordsLock.Unlock()
	//			}
	//		}
	//	}
	//}()
}
