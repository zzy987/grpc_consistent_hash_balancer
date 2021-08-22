package main

import (
	"log"

	"google.golang.org/grpc/resolver"
)

// Following is an example name resolver implementation. Read the name
// resolution example to learn more about it.

var (
	// Resolver exposes the resolver we give to grpc, for updating address conveniently.
	Resolver *exampleResolver
	addrs = []string{"localhost:50000", "localhost:50001", "localhost:50002"}
	//addrs = []string{"localhost:50000"}
)

func init() {
	// exampleResolverBuilder will register to grpc when init, grpc will build the resolver when needed.
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

// Scheme returns the scheme of the resolver. if a grpc dial want to hit the resolver, it should use a uri like scheme://service.
func (*exampleResolverBuilder) Scheme() string { return scheme }

type exampleResolver struct {
	target resolver.Target
	cc     resolver.ClientConn
	// addrsUpdateChan is create for dynamically update the resolver when use. There are other ways.
	addrsUpdateChan chan []string
}

// start is called in Build, for making some further initialization and listening to the updates.
// It is the cc.UpdateState() that make ClientConn know which addresses contained by the scheme. Dial() does not get any data from resolver directly.
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

// ResolveNow is a mystery. I don't know what it means.
func (*exampleResolver) ResolveNow(o resolver.ResolveNowOptions) {}

// Close things need to close.
func (r *exampleResolver) Close() {
	close(r.addrsUpdateChan)
}

// UpdateAddress can update address of the resolver.
// I make the resolver a package level variable so we can use Resolver.UpdateAddress to update conveniently.
// You can use other ways to update. Remember to call ClientConn.UpdateState after updating.
func (r *exampleResolver) UpdateAddress(addrs []string) {
	r.addrsUpdateChan <- addrs
}
