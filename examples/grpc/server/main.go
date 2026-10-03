//go:build linux || darwin || windows

// Command server is a gRPC greeter.
//
//	go run ./examples/grpc/server
package main

import (
	"context"
	"flag"
	"fmt"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

type server struct{}

func (server) SayHello(ctx context.Context, in *greeter.HelloRequest) (*greeter.HelloReply, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return &greeter.HelloReply{Message: "hello, " + in.Name}, nil
}

func (server) SayHellos(in *greeter.HelloRequest, stream grpc.ServerStreamingServer[greeter.HelloReply]) error {
	for i := 1; i <= 3; i++ {
		if err := stream.Send(&greeter.HelloReply{Message: fmt.Sprintf("hello #%d, %s", i, in.Name)}); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "listen address")
	flag.Parse()

	// Every call runs on the engine's handler pool; grpc.TaskPool(pool)
	// would run them on a pool of the program's own instead.
	s := grpc.NewServer()
	greeter.RegisterGreeterServer(s, server{})

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, s)
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("gRPC server listening on %s", *addr))
}
