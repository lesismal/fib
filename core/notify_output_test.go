//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/outputwait"
)

// notifyServer runs an engine whose connections are handed out as they open,
// and returns its address.
func notifyServer(t *testing.T, config Config) (*Engine, string, <-chan *Connection) {
	t.Helper()
	opened := make(chan *Connection, 4)
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
		Open: func(c *Connection) { opened <- c },
	}})
	if err != nil {
		t.Fatal(err)
	}
	var addr net.Addr
	if config.Network == "udp" {
		addr, err = server.LocalUDPAddr()
	} else {
		addr, err = server.LocalAddr()
	}
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return server, addr.String(), opened
}

// backlog dials server, takes the connection it opens, and queues size bytes
// on it that the peer, which does not read yet, leaves in the socket.
func backlog(t *testing.T, addr string, opened <-chan *Connection, size int) (*Connection, net.Conn) {
	t.Helper()
	peer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	var c *Connection
	select {
	case c = <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("no connection opened")
	}
	for sent := 0; sent < size; sent += 1 << 20 {
		if err := c.SendOwned(make([]byte, 1<<20)); err != nil {
			t.Fatal(err)
		}
	}
	return c, peer
}

func waitNotice(t *testing.T, notice <-chan error) error {
	t.Helper()
	select {
	case err := <-notice:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("the notice never came")
		return nil
	}
}

// The callback comes once the peer has read the backlog down to half the
// limit, and not before; under the limit nothing is armed.
func TestNotifyOutputWhenDrained(t *testing.T) {
	_, addr, opened := notifyServer(t, DefaultConfig())
	c, peer := backlog(t, addr, opened, 16<<20)
	const limit = 1 << 20
	notice := make(chan error, 1)
	if !c.NotifyOutput(limit, func(err error) { notice <- err }) {
		t.Fatalf("nothing armed with %d bytes queued", outputwait.Pending(c))
	}
	select {
	case err := <-notice:
		t.Fatalf("notified (%v) before the peer read anything", err)
	case <-time.After(100 * time.Millisecond):
	}
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	if err := waitNotice(t, notice); err != nil {
		t.Fatal(err)
	}
	if pending := outputwait.Pending(c); pending > limit/2 {
		t.Fatalf("notified with %d bytes queued", pending)
	}
	deadline := time.Now().Add(10 * time.Second)
	for outputwait.Pending(c) > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if outputwait.Notify(c, limit, func(error) { t.Error("notified with nothing queued") }) {
		t.Fatal("armed with nothing queued")
	}
}

// A connection that closes, an engine that stops, and one already closing all
// call the callback with EPIPE rather than leave it waiting.
func TestNotifyOutputWhenClosed(t *testing.T) {
	t.Run("close", func(t *testing.T) {
		_, addr, opened := notifyServer(t, DefaultConfig())
		c, _ := backlog(t, addr, opened, 16<<20)
		notice := make(chan error, 1)
		if !outputwait.Notify(c, 1<<20, func(err error) { notice <- err }) {
			t.Fatal("nothing armed")
		}
		_ = c.Close()
		if err := waitNotice(t, notice); !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("notice %v, want EPIPE", err)
		}
		// Closing: it is armed and called at once.
		if !outputwait.Notify(c, 1<<20, func(err error) { notice <- err }) {
			t.Fatal("nothing armed on a closed connection")
		}
		if err := waitNotice(t, notice); !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("notice %v, want EPIPE", err)
		}
	})
	t.Run("stop", func(t *testing.T) {
		server, addr, opened := notifyServer(t, DefaultConfig())
		c, _ := backlog(t, addr, opened, 16<<20)
		notice := make(chan error, 1)
		if !outputwait.Notify(c, 1<<20, func(err error) { notice <- err }) {
			t.Fatal("nothing armed")
		}
		server.Stop()
		if err := waitNotice(t, notice); !errors.Is(err, syscall.EPIPE) {
			t.Fatalf("notice %v, want EPIPE", err)
		}
	})
}

// A notice and a writer waiting in Wait share the connection's tracking: the
// writer leaving does not drop the notice, which still comes.
func TestNotifyOutputBesideWait(t *testing.T) {
	_, addr, opened := notifyServer(t, DefaultConfig())
	c, peer := backlog(t, addr, opened, 16<<20)
	waited := make(chan error, 1)
	go func() { waited <- outputwait.Wait(c, 8<<20) }()
	time.Sleep(50 * time.Millisecond)
	notice := make(chan error, 1)
	if !outputwait.Notify(c, 1<<20, func(err error) { notice <- err }) {
		t.Fatal("nothing armed")
	}
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	if err := waitNotice(t, notice); err != nil {
		t.Fatal(err)
	}
}

// A UDP connection has no queue to wait for.
func TestNotifyOutputUDP(t *testing.T) {
	config := DefaultConfig()
	config.Network = "udp"
	_, addr, opened := notifyServer(t, config)
	peer, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-opened:
		if outputwait.Notify(c, 0, func(error) { t.Error("notified on UDP") }) {
			t.Fatal("armed on UDP")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no connection opened")
	}
}
