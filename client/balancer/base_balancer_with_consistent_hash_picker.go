package balancer

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

func RegisterBalancerWithConsistentHashPickerBuilder() {
	balancer.Register(newBalancerWithConsistentHashPickerBuilder())
}

func newBalancerWithConsistentHashPickerBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Policy,
		&consistentHashPickerBuilder{},
		base.Config{HealthCheck: true})
}

type consistentHashPickerBuilder struct{}

func (b *consistentHashPickerBuilder) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	grpclog.Infof("consistentHashPicker: newPicker called with buildInfo: %v", buildInfo)
	if len(buildInfo.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	subConns := make(map[string]balancer.SubConn)
	for sc, conInfo := range buildInfo.ReadySCs {
		subConns[conInfo.Address.Addr] = sc
	}

	return newConsistentHashPicker(subConns)
}
