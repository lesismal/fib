package dht

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

func startEngine(t *testing.T) *fib.Engine {
	t.Helper()
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	e, err := fib.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	t.Cleanup(func() {
		e.Stop()
		<-done
		_ = e.Close()
	})
	return e
}

func listenAddr(n *Node) netip.AddrPort {
	return unmap(n.LocalAddr().(*net.UDPAddr).AddrPort())
}

// Nodes on fib UDP bindings find each other and each other's peers.
func TestListen(t *testing.T) {
	e := startEngine(t)
	var nodes []*Node
	for range 6 {
		n, err := Listen(e, "127.0.0.1:0", Config{Routers: []string{}, QueryTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		nodes = append(nodes, n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, n := range nodes[1:] {
		n.AddNodes(NodeInfo{Addr: listenAddr(nodes[0])})
		if err := n.Bootstrap(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if id, err := nodes[2].Ping(ctx, listenAddr(nodes[3])); err != nil || id != nodes[3].ID() {
		t.Fatalf("ping: %v %v", id, err)
	}
	ih := metainfo.InfoHash(RandomID())
	if n, err := nodes[1].Announce(ctx, ih, 0, true, nil); err != nil || n == 0 {
		t.Fatalf("announce: %d %v", n, err)
	}
	var got []netip.AddrPort
	if err := nodes[5].GetPeers(ctx, ih, func(p []netip.AddrPort) { got = append(got, p...) }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != listenAddr(nodes[1]) {
		t.Fatalf("peers %v, want %v", got, listenAddr(nodes[1]))
	}
}
