package balancer

import (
	"log"

	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"
)

type consistentHashPicker struct {
	subConns map[string]balancer.SubConn
	hashRing *hashring.HashRing
}

func newConsistentHashPicker(subConns map[string]balancer.SubConn) *consistentHashPicker {
	addrs := make([]string, 0)
	for addr := range subConns {
		addrs = append(addrs, addr)
	}
	log.Printf("consistent hash picker built with addresses %v\n", addrs)
	return &consistentHashPicker{
		subConns: subConns,
		hashRing: hashring.New(addrs),
	}
}

func (p *consistentHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var ret balancer.PickResult
	if key, ok := info.Ctx.Value(Key).(string); ok {
		if targetAddr, ok := p.hashRing.GetNode(key); ok {
			ret.SubConn = p.subConns[targetAddr]
		}
	}
	//ret.SubConn = p.subConns["localhost:50000"]
	return ret, nil
}
