//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package socks

import (
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// refuseLoopback is an AllowResolved that refuses loopback addresses and
// records what it was asked.
type refuseLoopback struct {
	mu    sync.Mutex
	asked []netip.AddrPort
}

func (r *refuseLoopback) allow(_ *Request, addr netip.AddrPort) error {
	r.mu.Lock()
	r.asked = append(r.asked, addr)
	r.mu.Unlock()
	if addr.Addr().IsLoopback() {
		return errors.New("loopback")
	}
	return nil
}

func (r *refuseLoopback) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.asked)
}

// Allow sees a name, which it cannot tell resolves to loopback;
// AllowResolved sees the address, and a CONNECT to a name, an IP or the
// unspecified address that reaches loopback is refused.
func TestServerAllowResolvedRefuses(t *testing.T) {
	var allowed []string
	var mu sync.Mutex
	r := &refuseLoopback{}
	proxyAddr, _ := proxy(t, Config{
		Allow: func(req *Request) error {
			mu.Lock()
			allowed = append(allowed, req.Addr)
			mu.Unlock()
			return nil
		},
		AllowResolved: r.allow,
	})
	_, port, _ := net.SplitHostPort(echo(t))
	for _, addr := range []string{"localhost:" + port, "127.0.0.1:" + port, "0.0.0.0:" + port} {
		conn := dialRaw(t, proxyAddr)
		connect5(t, conn, 1, addr)
		if reply := readN(t, conn, 2); reply[1] != ReplyNotAllowed {
			t.Fatalf("%s: reply %x", addr, reply)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, asked := range r.asked {
		if !asked.Addr().IsLoopback() || asked.Port() == 0 {
			t.Fatalf("AllowResolved asked about %v", asked)
		}
	}
	if len(r.asked) < 3 {
		t.Fatalf("AllowResolved asked %d times", len(r.asked))
	}
}

// What AllowResolved grants is what is dialed, and a *ReplyError it returns
// is the reply.
func TestServerAllowResolvedGrants(t *testing.T) {
	var mu sync.Mutex
	var asked []netip.AddrPort
	proxyAddr, _ := proxy(t, Config{AllowResolved: func(req *Request, addr netip.AddrPort) error {
		mu.Lock()
		asked = append(asked, addr)
		mu.Unlock()
		if addr.Port() == 1 {
			return &ReplyError{Version: req.Version, Code: ReplyHostUnreachable}
		}
		return nil
	}})
	target := echo(t)
	_, port, _ := net.SplitHostPort(target)
	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 1, "localhost:"+port)
	if reply := readN(t, conn, 10); reply[1] != 0 {
		t.Fatalf("reply %x", reply)
	}
	write(t, conn, []byte("hi"))
	expect(t, conn, []byte("hi"))
	mu.Lock()
	got := asked
	mu.Unlock()
	if len(got) != 1 || got[0].String() != target {
		t.Fatalf("AllowResolved asked about %v, the echo is %s", got, target)
	}

	conn = dialRaw(t, proxyAddr)
	connect5(t, conn, 1, "127.0.0.1:1")
	if reply := readN(t, conn, 2); reply[1] != ReplyHostUnreachable {
		t.Fatalf("reply %x", reply)
	}
}

// A Dial of the caller's own resolves names itself, and AllowResolved is
// not asked.
func TestServerAllowResolvedWithOwnDial(t *testing.T) {
	r := &refuseLoopback{}
	proxyAddr, _ := proxy(t, Config{Dial: DialDirect, AllowResolved: r.allow})
	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 1, echo(t))
	if reply := readN(t, conn, 10); reply[1] != 0 {
		t.Fatalf("reply %x", reply)
	}
	if n := r.count(); n != 0 {
		t.Fatalf("AllowResolved asked %d times with Dial set", n)
	}
}

// A UDP association's destination AllowResolved refuses gets none of its
// datagrams, and is not resolved again for each.
func TestServerUDPAllowResolved(t *testing.T) {
	var asked atomic.Int32
	proxyAddr, _ := proxyWith(t, func(*fib.Config) {}, Config{AllowResolved: func(_ *Request, addr netip.AddrPort) error {
		if addr.Port() == 9 {
			asked.Add(1)
			return errors.New("refused")
		}
		return nil
	}})
	target := udpEcho(t)
	u := associate(t, proxyAddr, true)
	for _, dest := range []string{"127.0.0.1:9", "localhost:9"} {
		before := asked.Load()
		for range 3 {
			u.send(dest, "lost")
		}
		if got := u.until(target, "through"); got != "echo:through" {
			t.Fatalf("got %q", got)
		}
		// A name is resolved off the peer's round, and may have an IPv4
		// and an IPv6 address.
		time.Sleep(50 * time.Millisecond)
		if n := asked.Load() - before; n == 0 || n > 2 {
			t.Fatalf("%s: AllowResolved asked %d times for 3 datagrams", dest, n)
		}
	}
}
