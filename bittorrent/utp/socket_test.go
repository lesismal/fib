package utp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
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

// echo serves s, echoing every connection until its peer's FIN.
func echo(s net.Listener) {
	for {
		c, err := s.Accept()
		if err != nil {
			return
		}
		go func() {
			_, _ = io.Copy(c, c)
			_ = c.(*Conn).CloseWrite()
			_, _ = c.Read(make([]byte, 1))
			_ = c.Close()
		}()
	}
}

func TestSocketEcho(t *testing.T) {
	e := startEngine(t)
	srv, err := Listen(e, "udp4", "127.0.0.1:0", Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	go echo(srv)

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Dial(e, srv.Addr().String(), Config{})
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			data := randBytes(uint64(60+i), 2<<20)
			go func() {
				_, _ = c.Write(data)
				_ = c.CloseWrite()
			}()
			got, err := io.ReadAll(c)
			if err != nil {
				t.Error(err)
				return
			}
			if sha256.Sum256(got) != sha256.Sum256(data) {
				t.Errorf("echo of %d bytes differs (%d back)", len(data), len(got))
			}
		}()
	}
	wg.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for srv.Mux().Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := srv.Mux().Len(); n > 0 {
		t.Fatalf("%d connections left on the server", n)
	}
}

// TestSocketShared dials both ways between two Sockets, each the one port
// of its side, and sends other datagrams through the same ports.
func TestSocketShared(t *testing.T) {
	e := startEngine(t)
	a, err := Listen(e, "udp4", "127.0.0.1:0", Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Listen(e, "udp4", "127.0.0.1:0", Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	other := make(chan string, 1)
	b.OnOther(func(from netip.AddrPort, d []byte) { other <- string(d) })
	go echo(a)
	go echo(b)

	if err := a.WriteTo([]byte("d1:y1:qe"), b.Addr().(*net.UDPAddr).AddrPort()); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-other:
		if s != "d1:y1:qe" {
			t.Fatalf("other datagram %q", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the non-uTP datagram did not arrive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, pair := range [][2]*Socket{{a, b}, {b, a}} {
		c, err := pair[0].DialContext(ctx, pair[1].Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		if c.LocalAddr().String() != pair[0].Addr().String() {
			t.Fatalf("dialed from %v, not the socket's port %v", c.LocalAddr(), pair[0].Addr())
		}
		msg := randBytes(70, 300<<10)
		go func() {
			_, _ = c.Write(msg)
			_ = c.CloseWrite()
		}()
		got, err := io.ReadAll(c)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, msg) {
			t.Fatal("echo differs")
		}
		_ = c.Close()
	}
}

func TestSocketCloseResets(t *testing.T) {
	e := startEngine(t)
	srv, err := Listen(e, "udp4", "127.0.0.1:0", Config{})
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := srv.AcceptUTP()
		if err == nil {
			accepted <- c
		}
	}()
	c, err := Dial(e, srv.Addr().String(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	<-accepted
	_ = srv.Close()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != ErrReset {
		t.Fatalf("read after the server closed: %v", err)
	}
}
