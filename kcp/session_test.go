package kcp

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
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

var fast3 = Config{NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true}

func echoServer(t *testing.T, e *fib.Engine, cfg ServerConfig) *Server {
	t.Helper()
	srv, err := Listen(e, "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	if cfg.OnSession == nil {
		go func() {
			for {
				c, err := srv.Accept()
				if err != nil {
					return
				}
				go func() {
					defer c.Close()
					_, _ = io.Copy(c, c)
				}()
			}
		}()
	}
	return srv
}

func TestEchoStream(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"plain", Config{}},
		{"fast3", fast3},
		{"stream", Config{StreamMode: true, NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2}},
		{"aes-fec", func() Config {
			c := fast3
			c.Crypt, _ = NewCrypt("aes", bytes.Repeat([]byte{3}, 32))
			c.DataShards, c.ParityShards = 10, 3
			return c
		}()},
		{"gcm-fec", func() Config {
			c := fast3
			c.Crypt, _ = NewCrypt("aes-128-gcm", bytes.Repeat([]byte{3}, 16))
			c.DataShards, c.ParityShards = 4, 2
			return c
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := startEngine(t)
			srv := echoServer(t, e, ServerConfig{Config: tc.cfg})
			c, err := Dial(e, srv.Addr().String(), tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			data := make([]byte, 4<<20)
			for i := range data {
				data[i] = byte(i*7 + i>>9)
			}
			errc := make(chan error, 1)
			go func() {
				for b := data; len(b) > 0; {
					n := min(len(b), 50000)
					if _, err := c.Write(b[:n]); err != nil {
						errc <- err
						return
					}
					b = b[n:]
				}
				errc <- nil
			}()
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
			got := make([]byte, len(data))
			if _, err := io.ReadFull(c, got); err != nil {
				t.Fatal(err)
			}
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatal("echo differs")
			}
			if len(srv.Sessions()) != 1 {
				t.Fatalf("%d sessions", len(srv.Sessions()))
			}
			t.Logf("rtt %v, client %+v", c.RTT(), c.Stats())
		})
	}
}

func TestMessages(t *testing.T) {
	e := startEngine(t)
	cfg := fast3
	cfg.OnMessage = func(s *Session, msg []byte) {
		// Echo each message whole, from the worker.
		if err := s.Send(msg); err != nil {
			t.Error(err)
		}
	}
	srv := echoServer(t, e, ServerConfig{Config: cfg})
	var mu sync.Mutex
	var got [][]byte
	done := make(chan struct{})
	const n = 300
	ccfg := fast3
	ccfg.OnMessage = func(s *Session, msg []byte) {
		mu.Lock()
		got = append(got, append([]byte(nil), msg...))
		if len(got) == n {
			close(done)
		}
		mu.Unlock()
	}
	c, err := Dial(e, srv.Addr().String(), ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var sent [][]byte
	for i := range n {
		m := bytes.Repeat([]byte{byte(i)}, 1+(i*997)%(20*c.MSS()))
		sent = append(sent, m)
		for c.WaitSend() > 256 {
			time.Sleep(time.Millisecond)
		}
		if err := c.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		mu.Lock()
		t.Fatalf("%d of %d messages", len(got), n)
	}
	for i := range sent {
		if !bytes.Equal(got[i], sent[i]) {
			t.Fatalf("message %d: %d bytes, want %d", i, len(got[i]), len(sent[i]))
		}
	}
}

func TestIdleTimeoutAndDeadline(t *testing.T) {
	e := startEngine(t)
	closed := make(chan error, 1)
	scfg := ServerConfig{Config: Config{IdleTimeout: 200 * time.Millisecond, OnClose: func(s *Session, err error) { closed <- err }}}
	srv := echoServer(t, e, scfg)
	c, err := Dial(e, srv.Addr().String(), Config{IdleTimeout: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != ErrIdle {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not closed when idle")
	}
	if len(srv.Sessions()) != 0 {
		t.Fatal("idle session kept")
	}
	_ = c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := c.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read past deadline: %v", err)
	}
	// Data on the closed conversation does not start a session.
	if _, err := c.Write([]byte("late")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(srv.Sessions()) != 0 {
		t.Fatal("a conversation's later segment started a session")
	}
}

func TestReplacedConversation(t *testing.T) {
	e := startEngine(t)
	var mu sync.Mutex
	var reasons []error
	srv := echoServer(t, e, ServerConfig{Config: Config{OnClose: func(s *Session, err error) {
		mu.Lock()
		reasons = append(reasons, err)
		mu.Unlock()
	}}})
	// Two conversations from one address: a client restarting on its port.
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	raddr := srv.Addr().(*net.UDPAddr)
	for conv := uint32(1); conv <= 2; conv++ {
		k := New(conv, func(p []byte) { _, _ = pc.WriteToUDP(p, raddr) })
		_ = k.Send([]byte("hi"))
		k.Flush(0)
		buf := make([]byte, 1500)
		_ = pc.SetReadDeadline(time.Now().Add(5 * time.Second))
		for k.PeekSize() < 0 {
			n, _, err := pc.ReadFromUDP(buf)
			if err != nil {
				t.Fatal(err)
			}
			_ = k.Input(buf[:n], 1)
		}
		ss := srv.Sessions()
		if len(ss) != 1 || ss[0].Conv() != conv {
			t.Fatalf("conv %d: sessions %v", conv, ss)
		}
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != ErrReplaced {
		t.Fatalf("closes %v", reasons)
	}
}

func TestServerClose(t *testing.T) {
	e := startEngine(t)
	srv := echoServer(t, e, ServerConfig{MaxSessions: 1})
	c1, err := Dial(e, srv.Addr().String(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := Dial(e, srv.Addr().String(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_, _ = c1.Write([]byte("a"))
	buf := make([]byte, 1)
	_ = c1.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := c1.Read(buf); err != nil {
		t.Fatal(err)
	}
	_, _ = c2.Write([]byte("b"))
	_ = c2.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, err := c2.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("second session past MaxSessions: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("accept after close: %v", err)
	}
	if len(srv.Sessions()) != 0 {
		t.Fatal("sessions left")
	}
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	if _, err := c1.Read(buf); err != io.EOF {
		t.Fatalf("read after close: %v", err)
	}
}

func TestDeadLinkCloses(t *testing.T) {
	e := startEngine(t)
	// Nobody listens there.
	pc, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	addr := pc.LocalAddr().String()
	defer pc.Close()
	closed := make(chan error, 1)
	c, err := Dial(e, addr, Config{NoDelay: 1, Interval: 10 * time.Millisecond, DeadLink: 4, OnClose: func(s *Session, err error) { closed <- err }})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write([]byte("anyone?"))
	select {
	case err := <-closed:
		if err != ErrDeadLink {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("never declared dead")
	}
	if _, err := c.Write([]byte("x")); err != ErrDeadLink {
		t.Fatalf("write on a dead link: %v", err)
	}
}
