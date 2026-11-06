//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package turnserver

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/turn"
)

// TestRelayPortsNotShared checks a relayed port of the range is never one
// that another relay, or another socket that sets SO_REUSEPORT as the
// engine's do, holds: the kernel would let the two share it and split
// their datagrams. A port is the range's again once its relay closes.
func TestRelayPortsNotShared(t *testing.T) {
	s, _, e := startServer(t, fib.DefaultConfig(), Config{})
	first, err := s.bindRelay(e)
	if err != nil {
		t.Fatal(err)
	}
	port := first.Addr().Port()
	s.cfg.PortMin, s.cfg.PortMax = port, port
	if r, err := s.bindRelay(e); !errors.Is(err, turn.ErrNoRelay) {
		t.Fatalf("a second relay on the first one's port: %v, %v", r, err)
	}
	// A port reserved by a relay still binding is not claimed twice, even
	// while no socket holds it yet.
	free, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fp := uint16(free.LocalAddr().(*net.UDPAddr).Port)
	free.Close()
	faddr := net.JoinHostPort(loopback.String(), strconv.Itoa(int(fp)))
	if err := s.claimPort(fp, faddr); err != nil {
		t.Fatal(err)
	}
	if err := s.claimPort(fp, faddr); !errors.Is(err, errPortHeld) {
		t.Fatalf("claiming a reserved port again: %v", err)
	}
	s.releasePort(fp)
	if err := s.claimPort(fp, faddr); err != nil {
		t.Fatalf("claiming a released port: %v", err)
	}
	s.releasePort(fp)
	first.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := s.bindRelay(e)
		if err == nil {
			if r.Addr().Port() != port {
				t.Fatalf("bound %v, want port %d", r.Addr(), port)
			}
			r.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the port of a closed relay is not freed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A port another engine holds, with fib's default SO_REUSEPORT.
	other, err := fib.New(fib.DefaultConfig(), fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: &fib.HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	addrs, err := other.BindingAddrs(0)
	if err != nil || len(addrs) == 0 {
		t.Fatal(addrs, err)
	}
	held := uint16(addrs[0].(*net.UDPAddr).Port)
	s.cfg.PortMin, s.cfg.PortMax = held, held
	if r, err := s.bindRelay(e); !errors.Is(err, turn.ErrNoRelay) {
		t.Fatalf("a relay on a port another engine holds: %v, %v", r, err)
	}
}

// TestRelayPortsConcurrent checks relays bound at once over a range get
// ports of their own, and those past the range's size are refused.
func TestRelayPortsConcurrent(t *testing.T) {
	s, _, e := startServer(t, fib.DefaultConfig(), Config{})
	// A range of free ports: n held and given back, so likely still free.
	const n = 4
	var ports []uint16
	for len(ports) < n {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := uint16(pc.LocalAddr().(*net.UDPAddr).Port)
		pc.Close()
		if p < 65535-n {
			ports = []uint16{p}
			for k := uint16(1); k < n; k++ {
				ports = append(ports, p+k)
			}
		}
	}
	s.cfg.PortMin, s.cfg.PortMax = ports[0], ports[n-1]
	var (
		mu    sync.Mutex
		bound = map[uint16]int{}
		wg    sync.WaitGroup
	)
	for range 2 * n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.bindRelay(e)
			if err != nil {
				return
			}
			t.Cleanup(r.Close)
			mu.Lock()
			bound[r.Addr().Port()]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	for p, c := range bound {
		if c != 1 {
			t.Errorf("port %d bound by %d relays", p, c)
		}
	}
	if len(bound) > n {
		t.Errorf("%d relays on a range of %d ports", len(bound), n)
	}
}
