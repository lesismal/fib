//go:build linux || darwin || windows

// Command client calls the gRPC greeter: a unary call, a call the server
// refuses, and a server-streaming call.
//
//	go run ./examples/grpc/client
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/grpc/greeter"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/status"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "server address")
	flag.Parse()

	engine, stop := example.ClientEngine()
	defer stop()
	conn, err := grpc.NewClient(*addr, grpc.WithEngine(engine))
	if err != nil {
		example.Fatal(err)
	}
	// The connection closes before its engine does.
	defer conn.Close()
	client := greeter.NewGreeterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := client.SayHello(ctx, &greeter.HelloRequest{Name: "fib"})
	if err != nil {
		example.Fatal(err)
	}
	fmt.Println(reply.Message)

	_, err = client.SayHello(ctx, &greeter.HelloRequest{})
	fmt.Printf("refused: %s: %s\n", status.Code(err), status.Convert(err).Message())

	stream, err := client.SayHellos(ctx, &greeter.HelloRequest{Name: "stream"})
	if err != nil {
		example.Fatal(err)
	}
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			example.Fatal(err)
		}
		fmt.Println(reply.Message)
	}
}
