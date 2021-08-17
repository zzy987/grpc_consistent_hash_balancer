package main

import (
	"log"

	"google.golang.org/grpc/resolver"
)

// Following is an example name resolver implementation. Read the name
// resolution example to learn more about it.

var (
	Resolver *exampleResolver
)

func init() {
	resolver.Register(&exampleResolverBuilder{})
}

type exampleResolverBuilder struct{}

func (*exampleResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	Resolver = &exampleResolver{
		target: target,
		cc:     cc,
		// addrsUpdateChan can be blocked
		addrsUpdateChan: make(chan []string, 1),
	}
	Resolver.addrsUpdateChan <- addrs
	go Resolver.start()
	return Resolver, nil
}
func (*exampleResolverBuilder) Scheme() string { return scheme }

type exampleResolver struct {
	target resolver.Target
	cc     resolver.ClientConn
	//addrsStore      map[string][]string
	addrsUpdateChan chan []string
}

func (r *exampleResolver) start() {
	for addrStrs := range r.addrsUpdateChan {
		log.Printf("resolver built with addresses %v\n", addrStrs)
		addresses := make([]resolver.Address, len(addrStrs))
		for i, s := range addrStrs {
			addresses[i] = resolver.Address{Addr: s}
		}
		r.cc.UpdateState(resolver.State{Addresses: addresses})
	}
}

func (*exampleResolver) ResolveNow(o resolver.ResolveNowOptions) {}

func (r *exampleResolver) Close() {
	close(r.addrsUpdateChan)
}

func (r *exampleResolver) UpdateAddress(addrs []string) {
	r.addrsUpdateChan <- addrs
}
