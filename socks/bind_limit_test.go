//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package socks

import (
	"net"
	"testing"
	"time"
)

func TestNewServerMaxBinds(t *testing.T) {
	if s := NewServer(Config{}); s.config.MaxBinds != DefaultMaxBinds {
		t.Fatalf("MaxBinds %d", s.config.MaxBinds)
	}
	s := NewServer(Config{MaxBinds: -1})
	for range DefaultMaxBinds + 1 {
		if !s.startBind() {
			t.Fatal("a negative MaxBinds bounded BINDs")
		}
	}
}

// No more than MaxBinds BINDs wait for their peers at once, and one that
// ends makes room for another.
func TestServerMaxBinds(t *testing.T) {
	proxyAddr, server := proxy(t, Config{MaxBinds: 2})
	bind := func() (net.Conn, byte) {
		conn := dialRaw(t, proxyAddr)
		connect5(t, conn, 2, "10.9.9.9:0")
		reply := readN(t, conn, 2)
		readN(t, conn, 8)
		return conn, reply[1]
	}
	first, code := bind()
	if code != ReplySucceeded {
		t.Fatalf("first BIND reply %d", code)
	}
	if _, code := bind(); code != ReplySucceeded {
		t.Fatalf("second BIND reply %d", code)
	}
	if _, code := bind(); code != ReplyGeneralFailure {
		t.Fatalf("BIND beyond MaxBinds reply %d", code)
	}
	_ = first.Close()
	deadline := time.Now().Add(testTimeout)
	for {
		server.mu.Lock()
		binds := server.binds
		server.mu.Unlock()
		if binds < 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the BIND of a client that went still counts")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, code := bind(); code != ReplySucceeded {
		t.Fatalf("BIND after one ended reply %d", code)
	}
}

// A BIND that names its peer by a name accepts only the addresses it
// resolves to: one that resolves to none accepts nobody, where it used to
// accept anyone.
func TestServerBindByName(t *testing.T) {
	// A resolver with no network may time out rather than fail; either ends
	// the BIND within BindTimeout as unreachable.
	proxyAddr, _ := proxy(t, Config{BindTimeout: 3 * time.Second})

	conn := dialRaw(t, proxyAddr)
	connect5(t, conn, 2, "localhost:0")
	reply := readN(t, conn, 10)
	if reply[1] != ReplySucceeded {
		t.Fatalf("reply %x", reply)
	}
	port := int(reply[8])<<8 | int(reply[9])
	peer := dialRaw(t, net.JoinHostPort("127.0.0.1", itoa(port)))
	if reply := readN(t, conn, 10); reply[1] != ReplySucceeded {
		t.Fatalf("second reply %x for a peer of localhost", reply)
	}
	write(t, peer, []byte("hi"))
	expect(t, conn, []byte("hi"))

	conn = dialRaw(t, proxyAddr)
	connect5(t, conn, 2, "no-such-host.invalid:0")
	reply = readN(t, conn, 10)
	if reply[1] != ReplySucceeded {
		t.Fatalf("reply %x", reply)
	}
	port = int(reply[8])<<8 | int(reply[9])
	if stranger, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", itoa(port)), testTimeout); err == nil {
		defer stranger.Close()
	}
	if reply := readN(t, conn, 2); reply[1] != ReplyHostUnreachable {
		t.Fatalf("second reply %x for a name that does not resolve", reply)
	}
}
