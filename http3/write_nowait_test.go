//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"
	"unsafe"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// noWaitPump writes payload with WriteNoWait, a piece at a time, resuming
// from OnWritable whenever it is told it is above the limit, as a proxy
// passing a body on does, and reports how it ended on done.
type noWaitPump struct {
	c       *fibhttp.Context
	payload []byte
	sent    int
	above   int
	slowest time.Duration
	done    chan error
}

func (p *noWaitPump) run(err error) {
	for err == nil && p.sent < len(p.payload) {
		piece := p.payload[p.sent:min(p.sent+16<<10, len(p.payload))]
		start := time.Now()
		var above bool
		above, err = p.c.WriteNoWait(piece)
		p.slowest = max(p.slowest, time.Since(start))
		if err != nil {
			break
		}
		p.sent += len(piece)
		if above {
			p.above++
			if p.c.OnWritable(p.run) {
				return
			}
		}
	}
	p.c.Release()
	p.done <- err
}

// TestWriteNoWait checks that over HTTP/3 a handler writing with WriteNoWait
// never waits, is told when it is above the limit, and gets the whole body
// through by resuming from OnWritable.
func TestWriteNoWait(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<18) // 4MB
	pumps := make(chan *noWaitPump, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		c.Retain()
		p := &noWaitPump{c: c, payload: payload, done: make(chan error, 1)}
		pumps <- p
		p.run(nil)
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, "GET", base+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(payload) {
		t.Fatalf("client read %d bytes, want %d", len(got), len(payload))
	}
	p := <-pumps
	if err := <-p.done; err != nil {
		t.Fatalf("the pump ended with %v", err)
	}
	if p.above == 0 {
		t.Error("WriteNoWait never reported above the limit")
	}
	if p.slowest > time.Second {
		t.Errorf("a WriteNoWait took %v", p.slowest)
	}
}

// TestOnWritableWriteTimeout checks that a response whose client stops
// taking it, written with WriteNoWait, hears of WriteTimeout through
// OnWritable's function.
func TestOnWritableWriteTimeout(t *testing.T) {
	pumps := make(chan *noWaitPump, 1)
	base := startServer(t, Config{WriteTimeout: 300 * time.Millisecond}, func(c *fibhttp.Context) {
		c.Retain()
		p := &noWaitPump{c: c, payload: make([]byte, 4<<20), done: make(chan error, 1)}
		pumps <- p
		p.run(nil)
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: 16 << 10})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	s.HoldCredit()
	head, _ := h3Request{path: "/stall"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	p := <-pumps
	select {
	case err := <-p.done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the pump ended with %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pump never heard of WriteTimeout")
	}
	if p.above == 0 {
		t.Error("WriteNoWait never reported above the limit")
	}
}

// TestOnWritableHoldsTheStream checks that the stream of a response whose
// OnWritable function is still to run is not recycled meanwhile: the
// function runs after the stream has closed and its Context has ended, when
// nothing else holds the stream, and it read a stream that another request
// may have taken.
func TestOnWritableHoldsTheStream(t *testing.T) {
	streams := make(chan *requestStream, 1)
	gate, refs := make(chan struct{}), make(chan int32, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		off := unsafe.Offsetof(requestStream{}.block) + unsafe.Offsetof(fibhttp.StreamRequest{}.Request)
		rs := (*requestStream)(unsafe.Add(unsafe.Pointer(c.Request), -int(off)))
		if rs.req != c.Request {
			t.Error("the request's stream is not where the test looks for it")
			return
		}
		piece := make([]byte, 16<<10)
		for {
			above, err := c.WriteNoWait(piece)
			if err != nil {
				t.Error(err)
				return
			}
			if above {
				break
			}
		}
		if !c.OnWritable(func(error) {
			<-gate
			refs <- rs.refs.Load()
		}) {
			t.Error("OnWritable reported room")
			return
		}
		streams <- rs
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: 16 << 10})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	s.HoldCredit()
	head, _ := h3Request{path: "/stall"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	rs := <-streams
	s.StopSending(uint64(ErrCodeRequestCancelled))
	// The connection lets go of the stream once it is reset, and the Context
	// once the handler has returned: then only the function may hold it.
	sc := rs.sc
	deadline := time.Now().Add(5 * time.Second)
	for {
		sc.mu.Lock()
		open := len(sc.streams)
		sc.mu.Unlock()
		if open == 0 && rs.refs.Load() <= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(gate)
	if n := <-refs; n != 1 {
		t.Fatalf("the stream had %d references while OnWritable's function ran, want its own", n)
	}
}
