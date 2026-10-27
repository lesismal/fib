//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// ShutdownBinding as Close runs, once Run has returned: ShutdownBinding's
// work waits on the stopped poller's queue, which Close drains as it
// releases the poller. With no loop of its own the engine's list of
// listeners is then changed on ShutdownBinding's goroutine, while Close's
// release goes on with it; release must neither read it unguarded nor close
// a listener ShutdownBinding has taken to close itself.
func TestShutdownBindingDuringClose(t *testing.T) {
	config := DefaultConfig()
	config.Name = "shutdown-binding-close"
	config.IOPollers, config.IOPollerCount = true, 1
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	i, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	addr := bindingAddrs(t, server, i, 1)[0]
	server.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	shutDone := make(chan error, 1)
	go func() { shutDone <- server.ShutdownBinding(context.Background(), i) }()
	// Close once ShutdownBinding's work is on the poller's queue.
	poller := server.pollers[0]
	deadline := time.Now().Add(5 * time.Second)
	for {
		poller.commandMu.Lock()
		queued := poller.commands != nil && len(poller.commands.items) > 0
		poller.commandMu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ShutdownBinding queued nothing on the poller")
		}
		time.Sleep(100 * time.Microsecond)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-shutDone; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("ShutdownBinding = %v", err)
	}
	l, err := net.Listen("tcp", addr.String())
	if err != nil {
		t.Fatalf("Close left %s bound: %v", addr, err)
	}
	l.Close()
}
