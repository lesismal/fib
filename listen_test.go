//go:build linux || darwin

package fib

import (
	"net"
	"testing"
	"time"
)

// TestStopListeningRefusesConnections checks that the engine stops taking
// connections before Close closes the ones it has, as Close counts on: a
// client reconnecting as soon as its connection closes must be refused
// rather than let into the backlog of a server that is going away. Darwin
// does not stop a listening socket that is shut down, so it needs the
// descriptor swapped out from under the listener instead.
func TestStopListeningRefusesConnections(t *testing.T) {
	for _, pollers := range []int{0, 2} {
		config := DefaultConfig()
		config.Addr = "127.0.0.1:0"
		config.IOPollers = pollers > 0
		config.IOPollerCount = pollers
		engine, err := Bind(config, HandlerFuncs{})
		if err != nil {
			t.Fatal(err)
		}
		addr, err := engine.LocalAddr()
		if err != nil {
			t.Fatal(err)
		}
		if c, err := net.DialTimeout("tcp", addr.String(), time.Second); err != nil {
			t.Fatalf("pollers %d: before: %v", pollers, err)
		} else {
			_ = c.Close()
		}
		engine.stopListening()
		for _, p := range engine.pollers {
			p.stopListening()
		}
		if c, err := net.DialTimeout("tcp", addr.String(), time.Second); err == nil {
			_ = c.Close()
			t.Errorf("pollers %d: a connection was taken after the engine stopped listening", pollers)
		}
		if err := engine.Close(); err != nil {
			t.Errorf("pollers %d: Close: %v", pollers, err)
		}
	}
}
