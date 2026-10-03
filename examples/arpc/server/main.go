//go:build linux || darwin || windows

// Command server is an arpc server: an echo method, a typed service, and a
// stream that answers each message in upper case.
//
//	go run ./examples/arpc/server
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/examples/example"
)

type SumReq struct{ A, B int }
type SumRsp struct{ Sum int }

// Math is registered as "Math.Sum".
type Math struct{}

func (Math) Sum(ctx context.Context, req *SumReq, rsp *SumRsp) { rsp.Sum = req.A + req.B }

func main() {
	addr := flag.String("addr", "127.0.0.1:8888", "listen address")
	flag.Parse()

	server := arpc.NewServer()
	// Handlers run on the engine's handler pool unless told otherwise; an
	// echo is quick enough to answer where the connection is read.
	server.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var s string
		if err := ctx.Bind(&s); err != nil {
			ctx.Error(err)
			return
		}
		ctx.Write(s)
	}, false)
	if err := server.Handler.Register("Math", Math{}); err != nil {
		example.Fatal(err)
	}
	server.Handler.HandleStream("/upper", func(s *arpc.Stream) {
		defer s.CloseSend()
		for {
			var msg string
			if err := s.Recv(&msg); err != nil {
				if err != io.EOF {
					fmt.Println("stream:", err)
				}
				return
			}
			if err := s.Send(strings.ToUpper(msg)); err != nil {
				return
			}
		}
	})
	server.Handler.HandleConnected(func(c *arpc.Client) {
		fmt.Printf("connected: %s\n", c.Conn().RemoteAddr())
	})

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, server)
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("arpc server listening on %s", *addr))
}
