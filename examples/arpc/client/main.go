//go:build linux || darwin || windows

// Command client calls the arpc server: a call, an asynchronous call, a call
// of a typed service, and a stream.
//
//	go run ./examples/arpc/client
package main

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
)

type SumReq struct{ A, B int }
type SumRsp struct{ Sum int }

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "server address")
	count := flag.Int("n", 3, "calls to make")
	flag.Parse()

	engine, stop := example.ClientEngine()
	defer stop()
	client, err := arpc.Dial(engine, "tcp", *addr, 3*time.Second, nil)
	if err != nil {
		example.Fatal(err)
	}
	// The client stops before its engine closes.
	defer client.Stop()

	for i := 1; i <= *count; i++ {
		var echo string
		if err := client.Call("/echo", example.Message(i), &echo, 5*time.Second); err != nil {
			example.Fatal(err)
		}
		fmt.Printf("echo: %s\n", echo)
	}

	done := make(chan struct{})
	err = client.CallAsync("/echo", "async", func(ctx *arpc.Context, err error) {
		defer close(done)
		var echo string
		if err == nil {
			err = ctx.Bind(&echo)
		}
		fmt.Printf("async echo: %q, %v\n", echo, err)
	}, 5*time.Second)
	if err != nil {
		example.Fatal(err)
	}
	<-done

	var sum SumRsp
	if err := client.Call("Math.Sum", &SumReq{A: 2, B: 3}, &sum, 5*time.Second); err != nil {
		example.Fatal(err)
	}
	fmt.Printf("Math.Sum(2, 3) = %d\n", sum.Sum)

	stream := client.NewStream("/upper")
	for _, word := range []string{"fib", "arpc"} {
		if err := stream.Send(word); err != nil {
			example.Fatal(err)
		}
	}
	stream.CloseSend()
	for {
		var word string
		if err := stream.Recv(&word); err != nil {
			if err != io.EOF {
				example.Fatal(err)
			}
			break
		}
		fmt.Printf("stream: %s\n", word)
	}
}
