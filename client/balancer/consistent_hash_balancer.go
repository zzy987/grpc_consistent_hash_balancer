package balancer

import (
	"fmt"
	"log"

	"google.golang.org/grpc/attributes"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/resolver"
)

func RegisterConsistentHashBalancerBuilder() {
	balancer.Register(newConsistentHashBalancerBuilder())
}

func newConsistentHashBalancerBuilder() balancer.Builder {
	return &consistentHashBalancerBuilder{}
}

type consistentHashBalancerBuilder struct{}

func (c *consistentHashBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	return &consistentHashBalancer{
		cc:       cc,
		subConns: make(map[resolver.Address]subConnInfo),
		scStates: make(map[balancer.SubConn]connectivity.State),
		csEvltr:  &balancer.ConnectivityStateEvaluator{},
	}
}

func (c *consistentHashBalancerBuilder) Name() string {
	return Policy
}

type subConnInfo struct {
	subConn balancer.SubConn
	attrs   *attributes.Attributes
}

// consistentHashBalancer is modified from baseBalancer, you can refer to https://github.com/grpc/grpc-go/blob/master/balancer/base/balancer.go
type consistentHashBalancer struct {
	cc balancer.ClientConn
	//ccCheater sync.Once

	csEvltr *balancer.ConnectivityStateEvaluator
	state   connectivity.State

	subConns map[resolver.Address]subConnInfo
	scStates map[balancer.SubConn]connectivity.State
	picker   balancer.Picker

	resolverErr error // the last error reported by the resolver; cleared on successful resolution
	connErr     error // the last connection error; cleared upon leaving TransientFailure
}

func (c *consistentHashBalancer) UpdateClientConnState(s balancer.ClientConnState) error {
	c.resolverErr = nil
	addrsSet := make(map[resolver.Address]struct{})
	for _, a := range s.ResolverState.Addresses {
		aNoAttrs := a
		aNoAttrs.Attributes = nil
		addrsSet[aNoAttrs] = struct{}{}
		if scInfo, ok := c.subConns[aNoAttrs]; !ok {
			sc, err := c.cc.NewSubConn([]resolver.Address{a}, balancer.NewSubConnOptions{HealthCheckEnabled: false})
			if err != nil {
				log.Printf("Consistent Hash Balancer: failed to create new SubConn: %v", err)
				continue
			}
			c.subConns[aNoAttrs] = subConnInfo{subConn: sc, attrs: a.Attributes}
			c.scStates[sc] = connectivity.Idle
			//sc.Connect()

			// The next three lines is a way to cheat grpc. See line 99-100 for more details.
			//c.ccCheater.Do(func() {
			//	sc.Connect()
			//})
		} else {
			scInfo.attrs = a.Attributes
			c.subConns[aNoAttrs] = scInfo
			c.cc.UpdateAddresses(scInfo.subConn, []resolver.Address{a})
		}
	}
	for a, scInfo := range c.subConns {
		if _, ok := addrsSet[a]; !ok {
			c.cc.RemoveSubConn(scInfo.subConn)
			delete(c.subConns, a)
		}
	}
	if len(s.ResolverState.Addresses) == 0 {
		c.ResolverError(fmt.Errorf("produced zero addresses"))
		return balancer.ErrBadResolverState
	}

	// As we want to do the connection management ourselves, we don't set up connection here in the loop.
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
	for addr, scInfo := range c.subConns {
		//if st, ok := c.scStates[scInfo.subConn]; ok && st == connectivity.Ready {
		// The next line may not be safe, but we have to use subConns without check, for we didn't set up connections in function UpdateClientConnState.
		if _, ok := c.scStates[scInfo.subConn]; ok {
			readySCs[addr.Addr] = scInfo.subConn
		}
	}
	c.picker = newConsistentHashPicker(readySCs)
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
	oldS, ok := c.scStates[sc]
	if !ok {
		return
	}
	log.Printf("state of one subConn changed from %s to %s\n", oldS.String(), s.String())
	if oldS == connectivity.TransientFailure && s == connectivity.Connecting {
		return
	}
	c.scStates[sc] = s
	switch s {
	case connectivity.Idle:
		sc.Connect()
	case connectivity.Shutdown:
		delete(c.scStates, sc)
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

func (c *consistentHashBalancer) Close() {}
