package main

import (
	"context"
	"fmt"
	"grpc_consistent_hash_balancer/client/balancer"
	"grpc_consistent_hash_balancer/rpc"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
)

const (
	scheme                    = "demo"
	serviceName               = "grpc.demo.consistent_hash_balancer"
	maxAttempts               = 4
	durationBetweenTwoRpcCall = time.Second * 2
)

var (
	grpcServicePolicy = fmt.Sprintf(`{
		"loadBalancingPolicy": "%s"
	}`, balancer.Policy)
)

func unaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var err error
	for i := maxAttempts; i > 0; i-- {
		err = invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			break
		}
	}
	return err
}

func callUnaryEcho(c rpc.EchoClient, message string) {
	key := fmt.Sprintf("%d", rand.Int63())
	log.Printf("call for key %s\n", key)
	// As the parameters of the pick function are specified by grpc, I have to pass the task_id by context.
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), balancer.Key, key), time.Minute)
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
		time.Sleep(durationBetweenTwoRpcCall)
	}
}

func main() {
	rand.Seed(time.Now().Unix())
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

	makeRPCs(conn, 100)
}
