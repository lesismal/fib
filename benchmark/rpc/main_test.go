//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/benchmark/rpc/internal/example"
	"github.com/lesismal/fib/grpc"
)

// startServer runs the server role with args until the test ends.
func startServer(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, append([]string{"-role", "server", "-addr", "127.0.0.1:0"}, args...), &out, func(addr string) { addrs <- addr })
	}()
	addr := <-addrs
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
		if !strings.Contains(out.String(), "listening "+addr) {
			t.Errorf("output %q", out.String())
		}
	})
	return addr
}

var result = regexp.MustCompile(`^calls=(\d+) fails=(\d+) rate=\d+/s\n$`)

// client runs the client role, briefly, against addr and returns its calls
// and failures.
func client(t *testing.T, addr string, args ...string) (calls, fails string) {
	t.Helper()
	var out bytes.Buffer
	args = append([]string{"-role", "client", "-addr", addr, "-conns", "2", "-callers", "2", "-dur", "50ms", "-size", "8"}, args...)
	if err := run(context.Background(), args, &out, nil); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	m := result.FindStringSubmatch(out.String())
	if m == nil {
		t.Fatalf("output %q", out.String())
	}
	return m[1], m[2]
}

// Both protocols' servers answer both protocols' clients, with and without
// coalesced writes.
func TestServerAndClient(t *testing.T) {
	for _, proto := range []string{"arpc", "grpc"} {
		t.Run(proto, func(t *testing.T) {
			addr := startServer(t, "-proto", proto, "-coalesce=false")
			for _, coalesce := range []string{"", "true", "false"} {
				if calls, fails := client(t, addr, "-proto", proto, "-client-coalesce", coalesce); calls == "0" || fails != "0" {
					t.Fatalf("-client-coalesce=%q: calls=%s fails=%s", coalesce, calls, fails)
				}
			}
		})
	}
}

// Calls a server does not answer are counted as failures, and not as calls:
// the rate is of the calls that succeeded.
func TestClientCountsFailures(t *testing.T) {
	serve := func(h fib.Handler) string {
		engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:0", Handler: h})
		if err != nil {
			t.Fatal(err)
		}
		stop := example.Start(engine)
		t.Cleanup(func() { _ = stop() })
		return example.Addr(engine)
	}
	// An arpc server with no /echo, and a gRPC server with no greeter.
	if calls, fails := client(t, serve(arpc.NewServer()), "-proto", "arpc"); calls != "0" || fails == "0" {
		t.Errorf("arpc: calls=%s fails=%s", calls, fails)
	}
	if calls, fails := client(t, serve(grpc.NewServer()), "-proto", "grpc"); calls != "0" || fails == "0" {
		t.Errorf("grpc: calls=%s fails=%s", calls, fails)
	}
}

// The arpc server answers a request whose payload it cannot bind, one that
// comes flagged as an error, with an error.
func TestArpcServerAnswersAnUnboundRequestWithAnError(t *testing.T) {
	addr := startServer(t, "-proto", "arpc")
	engine, stop := example.ClientEngine()
	t.Cleanup(stop)
	handler := arpc.NewHandler()
	// The request was pushed rather than called, so its response finds no
	// call waiting for it.
	answers := make(chan bool, 1)
	handler.HandleSessionMiss(func(_ *arpc.Client, msg *arpc.Message) { answers <- msg.IsError() })
	c, err := arpc.Dial(engine, "tcp", addr, 5*time.Second, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	msg := c.NewMessage(arpc.CmdRequest, "/echo", "x")
	msg.SetError(true)
	if err := c.PushMsg(msg, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case isError := <-answers:
		if !isError {
			t.Fatal("the answer is not an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no answer")
	}
}

// The greeter's streaming method is there only to satisfy the interface.
func TestSayHellosDoesNothing(t *testing.T) {
	if err := (gsrv{}).SayHellos(nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRunErrors(t *testing.T) {
	ctx := context.Background()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := l.Addr().String()
	_ = l.Close()
	for _, args := range [][]string{
		{"-nope"},
		{"-role", "server", "-addr", "not an address"},
		{"-role", "client", "-client-coalesce", "maybe"},
		{"-role", "client", "-proto", "arpc", "-addr", refused, "-conns", "1"},
		{"-role", "client", "-proto", "grpc", "-addr", "", "-conns", "1"},
	} {
		if err := run(ctx, args, io.Discard, nil); err == nil {
			t.Errorf("%q: no error", args)
		}
	}
}

func TestMainReportsAnError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"rpc", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}
