package balancer

import (
	"context"
	"log"
	"sync"

	"github.com/serialx/hashring"
	"google.golang.org/grpc/balancer"
)

// TODO add lock for maps or use sync.map
type consistentHashPicker struct {
	subConns    map[string]balancer.SubConn // address string -> balancer.SubConn
	hashRing    *hashring.HashRing
	pickHistory sync.Map // task_id -> target_address
	needReport  bool
	reportChan  chan<- PickResult
}

type PickResult struct {
	Ctx context.Context
	SC  balancer.SubConn
}

func NewConsistentHashPicker(subConns map[string]balancer.SubConn) *consistentHashPicker {
	addrs := make([]string, 0)
	for addr := range subConns {
		addrs = append(addrs, addr)
	}
	log.Printf("consistent hash picker built with addresses %v\n", addrs)
	return &consistentHashPicker{
		subConns:    subConns,
		hashRing:    hashring.New(addrs),
		pickHistory: sync.Map{},
		needReport:  false,
	}
}

func NewConsistentHashPickerWithReportChan(subConns map[string]balancer.SubConn, reportChan chan<- PickResult) *consistentHashPicker {
	addrs := make([]string, 0)
	for addr := range subConns {
		addrs = append(addrs, addr)
	}
	log.Printf("consistent hash picker built with addresses %v\n", addrs)
	return &consistentHashPicker{
		subConns:    subConns,
		hashRing:    hashring.New(addrs),
		pickHistory: sync.Map{},
		needReport:  true,
		reportChan:  reportChan,
	}
}

func (p *consistentHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var ret balancer.PickResult
	key, ok := info.Ctx.Value(Key).(string)
	if !ok {
		key = info.FullMethodName
	}
	log.Printf("pick for %s\n", key)
	if historyAddr, ok := p.pickHistory.Load(key); ok {
		ret.SubConn = p.subConns[historyAddr.(string)]
		if p.needReport {
			p.reportChan <- PickResult{Ctx: info.Ctx, SC: ret.SubConn}
		}
	} else if targetAddr, ok := p.hashRing.GetNode(key); ok {
		ret.SubConn = p.subConns[targetAddr]
		p.pickHistory.Store(key, targetAddr)
		if p.needReport {
			p.reportChan <- PickResult{Ctx: info.Ctx, SC: ret.SubConn}
		}
	}
	//ret.SubConn = p.subConns["localhost:50000"]
	if ret.SubConn == nil {
		return ret, balancer.ErrNoSubConnAvailable
	}
	return ret, nil
}

func (p *consistentHashPicker) ResetAddrSubConn(addr string, sc balancer.SubConn) {
	p.subConns[addr] = sc
}

// Refresh recreate the picker with the old pickHistory.
func (p *consistentHashPicker) Refresh(subConns map[string]balancer.SubConn) {
	addrs := make([]string, 0)
	for addr := range subConns {
		addrs = append(addrs, addr)
	}
	log.Printf("consistent hash picker built with addresses %v\n", addrs)
	p.subConns = subConns
	p.hashRing = hashring.New(addrs)
}
