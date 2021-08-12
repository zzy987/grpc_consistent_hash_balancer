package main

import (
	"context"
	"fmt"
	"grpc_capsulation/client/balancer"
	"grpc_capsulation/rpc"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
)

const (
	scheme      = "demo"
	serviceName = "grpc.demo.consistent_hash_balancer"
)

var (
	grpcServicePolicy = fmt.Sprintf(`{
		"loadBalancingPolicy": "%s",
		"methodConfig": [{
		  "name": [{"service": "grpc.demo.Echo"}],
		  "waitForReady": true,
		  "retryPolicy": {
			  "MaxAttempts": 4,
			  "InitialBackoff": ".01s",
			  "MaxBackoff": ".01s",
			  "BackoffMultiplier": 1.0,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }
		}]}`, balancer.Policy)
	addrs = []string{"localhost:50000", "localhost:50001", "localhost:50002"}
)

func unaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		log.Println("--- interceptor get error ---")
		err = invoker(ctx, method, req, reply, cc, opts...)
	}
	return err
}

func callUnaryEcho(c rpc.EchoClient, message string) {
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), balancer.Key, fmt.Sprintf("%d", rand.Int63())), time.Minute)
	defer cancel()
	r, err := c.UnaryEcho(ctx, &rpc.EchoRequest{Message: message})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println(r.Message)
}

func makeRPCs(cc *grpc.ClientConn, n int) {
	hwc := rpc.NewEchoClient(cc)
	for i := 0; i < n; i++ {
		callUnaryEcho(hwc, "this is examples/load_balancing")
	}
}

func main() {
	rand.Seed(time.Now().Unix())
	balancer.RegisterConsistentHashBalancerBuilder()
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", scheme, serviceName),
		grpc.WithDefaultServiceConfig(grpcServicePolicy),
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(unaryInterceptor),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	makeRPCs(conn, 10)
}
