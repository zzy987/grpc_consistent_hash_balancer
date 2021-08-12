package main

import (
	"context"
	"fmt"
	"grpc_capsulation/rpc"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc"
)

var addrs = []string{":50000", ":50001", ":50002"}

type ecServer struct {
	rpc.UnimplementedEchoServer
	addr       string
	reqCounter int32
	reqModulo  int32
	mu         sync.Mutex
}

func (s *ecServer) UnaryEcho(ctx context.Context, req *rpc.EchoRequest) (*rpc.EchoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqCounter++
	if (s.reqModulo > 0) && (s.reqCounter%s.reqModulo == 0) {
		log.Printf("%s request succeeded count: %d \n", s.addr, s.reqCounter)
		return &rpc.EchoResponse{Message: fmt.Sprintf("%s (from %s)", req.Message, s.addr)}, nil
	}
	log.Printf("%s request failed count: %d \n", s.addr, s.reqCounter)
	return nil, status.Errorf(codes.Unavailable, "maybeFailRequest: failing it")
}

func startServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	rpc.RegisterEchoServer(s, &ecServer{addr: addr, reqCounter: 0, reqModulo: 4})
	log.Printf("serving on %s\n", addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			startServer(addr)
		}(addr)
	}
	wg.Wait()
}
