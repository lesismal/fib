//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// closeWatcher reports the error each connection closed with.
type closeWatcher struct {
	HandlerFuncs
	closed chan error
}

func newCloseWatcher(data func(*Connection, []byte)) *closeWatcher {
	w := &closeWatcher{closed: make(chan error, 8)}
	w.HandlerFuncs = HandlerFuncs{
		Data:  data,
		Close: func(_ *Connection, err error) { w.closed <- err },
	}
	return w
}

func (w *closeWatcher) await(t *testing.T, within time.Duration) error {
	t.Helper()
	select {
	case err := <-w.closed:
		return err
	case <-time.After(within):
		t.Fatal("the connection did not close")
		return nil
	}
}

func (w *closeWatcher) quiet(t *testing.T, for_ time.Duration) {
	t.Helper()
	select {
	case err := <-w.closed:
		t.Fatalf("the connection closed with %v", err)
	case <-time.After(for_):
	}
}

func TestReadDeadlineClosesTheConnection(t *testing.T) {
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v, want os.ErrDeadlineExceeded", err)
	}
	// The peer sees the close too.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("the peer's connection stayed open")
	}
}

// TestReadDeadlineMovedBeforeItFires checks the generation guard: a deadline
// pushed back must not be closed by the timer it outlived.
func TestReadDeadlineMovedBeforeItFires(t *testing.T) {
	var reads atomic.Int64
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		reads.Add(1)
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Keep pushing the deadline back for well past its length.
	for i := 0; i < 10; i++ {
		if _, err = conn.Write([]byte("tick")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(80 * time.Millisecond)
	}
	select {
	case err := <-watcher.closed:
		t.Fatalf("a deadline that kept moving closed the connection with %v", err)
	default:
	}
	if reads.Load() == 0 {
		t.Fatal("nothing was delivered")
	}
	// Stop moving it and it fires.
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestZeroDeadlineRemovesIt(t *testing.T) {
	watcher := newCloseWatcher(func(c *Connection, data []byte) {
		if string(data) == "arm" {
			_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
			return
		}
		_ = c.SetReadDeadline(time.Time{})
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("arm")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err = conn.Write([]byte("off")); err != nil {
		t.Fatal(err)
	}
	watcher.quiet(t, 500*time.Millisecond)
}

func TestWriteDeadlineIgnoresADrainedConnection(t *testing.T) {
	watcher := newCloseWatcher(func(c *Connection, data []byte) {
		_ = c.SetWriteDeadline(time.Now().Add(150 * time.Millisecond))
		_ = c.Send(data)
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("echo")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.ReadFull(conn, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	// The reply went out, so the write deadline has nothing to be about.
	watcher.quiet(t, 500*time.Millisecond)
}

func TestWriteDeadlineClosesAStalledConnection(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 16<<20)
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		_ = c.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
		_ = c.Send(payload)
	})
	config := DefaultConfig()
	// Keep the backlog off the watermarks, so the connection is closed by its
	// deadline rather than paused by its own backpressure.
	config.WriteBufferHighWatermark = 64 << 20
	config.MaxPendingBytes = 128 << 20
	_, addr := startEchoServer(t, config, watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		// Pin this side's receive buffer before anything is sent. Left alone,
		// a receive window that auto-tunes as far as the payload — which is
		// what Windows does — would take the whole reply into the kernel even
		// though nothing here ever reads it, and then the write deadline would
		// have nothing to be about.
		if err = tcp.SetReadBuffer(16 << 10); err != nil {
			t.Fatal(err)
		}
	}
	// Never read, so the reply cannot drain.
	if _, err = conn.Write([]byte("go")); err != nil {
		t.Fatal(err)
	}
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestReadTakesBytesOffTheSocket(t *testing.T) {
	got := make(chan string, 1)
	handler := HandlerFuncs{Data: func(c *Connection, data []byte) {
		// The read loop delivered the first byte; take the rest by hand.
		rest := make([]byte, 64)
		deadline := time.Now().Add(2 * time.Second)
		read := append([]byte(nil), data...)
		for len(read) < 5 && time.Now().Before(deadline) {
			n, err := c.Read(rest)
			if n > 0 {
				read = append(read, rest[:n]...)
				continue
			}
			if !errors.Is(err, ErrWouldBlock) {
				t.Errorf("Read error = %v", err)
				break
			}
			time.Sleep(time.Millisecond)
		}
		select {
		case got <- string(read):
		default:
		}
	}}
	_, addr := startEchoServer(t, DefaultConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, b := range []string{"h", "e", "l", "l", "o"} {
		if _, err = io.WriteString(conn, b); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case s := <-got:
		if s != "hello" {
			t.Fatalf("read %q, want %q", s, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never assembled the message")
	}
}

func TestReadAndWriteReportWhyTheConnectionClosed(t *testing.T) {
	reported := make(chan error, 1)
	watcher := newCloseWatcher(func(c *Connection, _ []byte) {
		_ = c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		go func() {
			time.Sleep(300 * time.Millisecond)
			_, err := c.Read(make([]byte, 1))
			reported <- err
		}()
	})
	_, addr := startEchoServer(t, DefaultConfig(), watcher)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := watcher.await(t, 5*time.Second); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("OnClose error = %v", err)
	}
	select {
	case err := <-reported:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Read after the deadline closed it = %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read never reported")
	}
}

func TestConnectionAddresses(t *testing.T) {
	addrs := make(chan [2]string, 1)
	handler := HandlerFuncs{Data: func(c *Connection, _ []byte) {
		local, remote := c.LocalAddr(), c.RemoteAddr()
		if local == nil || remote == nil {
			t.Errorf("LocalAddr=%v RemoteAddr=%v", local, remote)
			return
		}
		select {
		case addrs <- [2]string{local.String(), remote.String()}:
		default:
		}
	}}
	_, addr := startEchoServer(t, DefaultConfig(), handler)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-addrs:
		if got[0] != addr {
			t.Fatalf("LocalAddr = %q, want the listener's %q", got[0], addr)
		}
		if got[1] != conn.LocalAddr().String() {
			t.Fatalf("RemoteAddr = %q, want the client's %q", got[1], conn.LocalAddr())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}
}
