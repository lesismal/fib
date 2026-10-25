package quic

import (
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The benchmarks here run over the in-memory path the tests use, so that
// what they measure is the transport: loss recovery, congestion control,
// framing and packet protection. They leave out the syscalls a UDP socket
// adds, which the HTTP/3 benchmarks in package http3 include.

// benchHandler is the client's handler in a benchmark: it forwards to the
// pair's own testHandler, so that the handshake and the close are still
// seen, but the payload of a stream, which may be megabytes, is handed to
// onData rather than kept.
type benchHandler struct {
	*testHandler
	onData func(s *Stream, data []byte, fin bool)
}

func (h *benchHandler) OnStreamData(s *Stream, data []byte, fin bool) {
	if h.onData != nil {
		h.onData(s, data, fin)
	}
}

// newBenchPair connects a client and a server over a pipe, with client and
// server as what their handlers do with a stream's data. Neither keeps a
// stream's payload or records its end, which a benchmark that moves
// megabytes and opens thousands of streams would otherwise pile up: the
// test handler's channels are for tests that read them.
func newBenchPair(b *testing.B, loss float64, configure func(client, server *Config),
	client, server func(s *Stream, data []byte, fin bool)) *testPair {
	b.Helper()
	ch, sh := &benchHandler{onData: client}, &benchHandler{onData: server}
	return newTestPairOpts(b, loss, configure, pairOpts{
		wrapClient: func(th *testHandler) Handler { ch.testHandler = th; return ch },
		wrapServer: func(th *testHandler) Handler { sh.testHandler = th; return sh },
	})
}

// benchFins hands a benchmark goroutine the fin of the stream it is waiting
// for: it registers the stream before writing to it, and the handler only
// closes what is there. A handler that sent on a channel instead would hold
// the connection's calls, and the benchmark's cleanup of it, for as long as
// nobody received, which a benchmark that failed cannot.
type benchFins struct {
	mu      sync.Mutex
	waiters map[uint64]chan struct{}
}

func newBenchFins() *benchFins {
	return &benchFins{waiters: make(map[uint64]chan struct{})}
}

func (f *benchFins) await(id uint64) chan struct{} {
	ch := make(chan struct{})
	f.mu.Lock()
	f.waiters[id] = ch
	f.mu.Unlock()
	return ch
}

func (f *benchFins) onData(s *Stream, data []byte, fin bool) {
	if !fin {
		return
	}
	f.mu.Lock()
	ch := f.waiters[s.ID()]
	delete(f.waiters, s.ID())
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// benchWait waits for a fin, failing the benchmark rather than hanging if it
// never comes.
func benchWait(b *testing.B, ch chan struct{}, what string) {
	b.Helper()
	select {
	case <-ch:
	case <-time.After(time.Minute):
		b.Fatalf("%s timed out", what)
	}
}

// benchEcho answers each request with the request itself: a benchmark's
// completion is then the fin of its own stream, whatever else is in flight.
func benchEcho() func(s *Stream, data []byte, fin bool) {
	reqs := make(map[uint64][]byte)
	return func(s *Stream, data []byte, fin bool) {
		reqs[s.ID()] = append(reqs[s.ID()], data...)
		if !fin {
			return
		}
		body := reqs[s.ID()]
		delete(reqs, s.ID())
		_ = s.Write(body, true)
	}
}

// benchReplies matches the fin of a stream to the goroutine waiting for it.
// The client's handler runs one call at a time, so replies and partials need
// no lock; only the waiters are shared with the benchmark's goroutines.
type benchReplies struct {
	mu      sync.Mutex
	waiters map[string]chan struct{}
	partial map[uint64][]byte
}

func newBenchReplies() *benchReplies {
	return &benchReplies{waiters: make(map[string]chan struct{}), partial: make(map[uint64][]byte)}
}

func (r *benchReplies) onData(s *Stream, data []byte, fin bool) {
	r.partial[s.ID()] = append(r.partial[s.ID()], data...)
	if !fin {
		return
	}
	id := string(r.partial[s.ID()])
	delete(r.partial, s.ID())
	r.mu.Lock()
	ch := r.waiters[id]
	delete(r.waiters, id)
	r.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (r *benchReplies) wait(id string) chan struct{} {
	ch := make(chan struct{})
	r.mu.Lock()
	r.waiters[id] = ch
	r.mu.Unlock()
	return ch
}

// request is what the request benchmarks send: eight bytes naming the
// request, which the echo gives back.
func request(id uint64) []byte {
	return fmt.Appendf(nil, "%08d", id)
}

// BenchmarkQUICTransfer measures a bulk transfer over one connection: the
// client asks for a payload, and the server sends it on the stream. The
// server is not told how large it is, so the transfer is a round of stream
// frames, acknowledgements and congestion window growth, as a download is.
func BenchmarkQUICTransfer(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20, 4 << 20} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			payload := make([]byte, size)
			_, _ = rand.Read(payload)
			fins := newBenchFins()
			p := newBenchPair(b, 0, nil, fins.onData, func(s *Stream, data []byte, fin bool) {
				if fin {
					_ = s.Write(payload, true)
				}
			})
			req := []byte("x")
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := p.client.OpenStream()
				if err != nil {
					b.Fatal(err)
				}
				ch := fins.await(s.ID())
				if err := s.Write(req, true); err != nil {
					b.Fatal(err)
				}
				benchWait(b, ch, "the transfer")
			}
		})
	}
}

// BenchmarkQUICRequests measures a small request and its response over one
// connection, one at a time: what a request costs in an HTTP/3 connection
// once the stream is opened and closed, and what it allocates.
func BenchmarkQUICRequests(b *testing.B) {
	fins := newBenchFins()
	p := newBenchPair(b, 0, nil, fins.onData, benchEcho())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			b.Fatal(err)
		}
		ch := fins.await(s.ID())
		if err := s.Write(request(uint64(i)), true); err != nil {
			b.Fatal(err)
		}
		benchWait(b, ch, "the request")
	}
}

// BenchmarkQUICConcurrentRequests measures requests that are in flight at
// once on one connection, from as many goroutines as there are processors:
// streams open and close while others wait for their answers.
func BenchmarkQUICConcurrentRequests(b *testing.B) {
	replies := newBenchReplies()
	p := newBenchPair(b, 0, nil, replies.onData, benchEcho())
	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := request(seq.Add(1))
			ch := replies.wait(string(id))
			s, err := p.client.OpenStream()
			if err != nil {
				b.Error(err)
				return
			}
			if err := s.Write(id, true); err != nil {
				b.Error(err)
				return
			}
			select {
			case <-ch:
			case <-time.After(time.Minute):
				b.Error("the request timed out")
				return
			}
		}
	})
}

// BenchmarkQUICLossyTransfer measures the same bulk transfer on a path that
// loses datagrams, where loss recovery, the congestion controller's response
// and pacing decide the rate.
func BenchmarkQUICLossyTransfer(b *testing.B) {
	const size = 256 << 10
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	for _, loss := range []float64{0.01, 0.05} {
		b.Run(fmt.Sprintf("%.0f%%", loss*100), func(b *testing.B) {
			fins := newBenchFins()
			p := newBenchPair(b, loss, nil, fins.onData, func(s *Stream, data []byte, fin bool) {
				if fin {
					_ = s.Write(payload, true)
				}
			})
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s, err := p.client.OpenStream()
				if err != nil {
					b.Fatal(err)
				}
				ch := fins.await(s.ID())
				if err := s.Write([]byte("x"), true); err != nil {
					b.Fatal(err)
				}
				benchWait(b, ch, "the transfer")
			}
		})
	}
}

// BenchmarkQUICStreamOpen measures opening and finishing a stream with no
// data on it: the bookkeeping a request adds to the connection, without the
// round trip.
func BenchmarkQUICStreamOpen(b *testing.B) {
	fins := newBenchFins()
	p := newBenchPair(b, 0, nil, fins.onData, func(s *Stream, data []byte, fin bool) {
		if fin {
			_ = s.Write(nil, true)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			b.Fatal(err)
		}
		ch := fins.await(s.ID())
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		benchWait(b, ch, "the stream")
	}
}
