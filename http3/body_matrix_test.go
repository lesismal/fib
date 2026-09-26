//go:build linux || darwin || windows

package http3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	stdhttp "net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/internal/qpack"
	"github.com/lesismal/fib/http3/internal/quic"
	"github.com/lesismal/fib/internal/tlstest"
)

// The HTTP/3 body matrix is the HTTP/1 and HTTP/2 one of package http run
// over QUIC, which is always over TLS: every body framing through every way a
// handler takes it, the request stream written whole or in pieces with pauses
// between them, two requests on the connection, and a server that reads
// bodies whole, one that streams them all, and one that streams those past a
// threshold.

type h3Config struct {
	name      string
	stream    bool
	threshold int64
}

var h3Configs = []h3Config{
	{"buffered", false, 0},
	{"stream", true, 0},
	{"stream-64k", true, 64 << 10},
}

func (m h3Config) config() Config {
	return Config{StreamRequestBody: m.stream, StreamRequestBodyThreshold: m.threshold}
}

// streams reports whether the server streams req's body: whether it streams
// bodies and this one is past the threshold. For a body without a
// content-length that is settled by that alone too, since such a body
// streams once more than the threshold of it has arrived, and the whole of it
// is more.
func (m h3Config) streams(req h3Request) bool {
	return m.stream && req.framing != h3None && int64(len(req.body)) > m.threshold
}

type h3Framing string

const (
	h3None    h3Framing = "none"
	h3Length  h3Framing = "length"
	h3Unsized h3Framing = "unsized"
	h3Trailer h3Framing = "trailer"
	h3Expect  h3Framing = "expect"
)

var h3Framings = []h3Framing{h3None, h3Length, h3Unsized, h3Trailer, h3Expect}

var h3Modes = []string{"read", "onbody", "complete", "async"}

type h3Report struct {
	Len        int
	Sum        string
	Trailer    string
	Complete   bool
	Streamed   bool
	WouldBlock bool
	Err        string
}

var h3Violations struct {
	sync.Mutex
	list []string
}

func h3Violation(format string, args ...any) {
	h3Violations.Lock()
	h3Violations.list = append(h3Violations.list, fmt.Sprintf(format, args...))
	h3Violations.Unlock()
}

func checkH3Violations(t *testing.T) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	h3Violations.Lock()
	defer h3Violations.Unlock()
	for _, v := range h3Violations.list {
		t.Error(v)
	}
	h3Violations.list = nil
}

// h3Serve is one request being served by h3MatrixHandler, which takes its
// body the ways package http's matrix does and answers with what it got.
type h3Serve struct {
	c        *fibhttp.Context
	r        *stdhttp.Request
	report   h3Report
	digest   hash.Hash
	returned atomic.Bool
	ended    bool
}

func h3MatrixHandler(c *fibhttp.Context, r *stdhttp.Request) {
	s := &h3Serve{c: c, r: r, digest: sha256.New()}
	s.report.Complete, s.report.Streamed = c.BodyComplete(), c.RequestBody() != nil
	defer s.returned.Store(true)
	switch strings.TrimPrefix(r.URL.Path, "/") {
	case "read":
		buf := make([]byte, 16<<10)
		for {
			n, err := r.Body.Read(buf)
			s.add(buf[:n])
			switch {
			case err == nil:
				continue
			case errors.Is(err, io.EOF):
				s.answer(nil)
			case errors.Is(err, fibhttp.ErrWouldBlock):
				s.report.WouldBlock = true
				s.onBody(true)
			default:
				s.answer(err)
			}
			return
		}
	case "onbody":
		s.onBody(true)
	case "complete":
		if !s.report.Complete {
			s.onBody(true)
			return
		}
		body, err := io.ReadAll(r.Body)
		s.add(body)
		s.answer(err)
	case "async":
		c.Retain()
		go func() {
			time.Sleep(5 * time.Millisecond)
			s.onBody(false)
			c.Release()
		}()
	default:
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("alive"))
	}
}

func (s *h3Serve) add(data []byte) {
	s.digest.Write(data)
	s.report.Len += len(data)
}

func (s *h3Serve) onBody(inHandler bool) {
	returned := make(chan struct{})
	s.c.OnBody(func(data []byte, fin bool, err error) {
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			h3Violation("%s: a body callback ran inside OnBody", s.r.URL.Path)
		}
		if inHandler && !s.returned.Load() {
			h3Violation("%s: a body callback ran before the handler returned", s.r.URL.Path)
		}
		if s.ended {
			h3Violation("%s: a body callback after the last one", s.r.URL.Path)
			return
		}
		s.add(data)
		if fin || err != nil {
			s.ended = true
			s.answer(err)
		}
	})
	close(returned)
}

func (s *h3Serve) answer(err error) {
	s.report.Sum = hex.EncodeToString(s.digest.Sum(nil))
	if s.r.Trailer != nil {
		s.report.Trailer = s.r.Trailer.Get("X-Sum")
	}
	if err != nil {
		s.report.Err = err.Error()
	}
	body, _ := json.Marshal(s.report)
	_ = s.c.Respond(stdhttp.StatusOK, "application/json", body)
}

// h3Wire is a raw QUIC connection that writes request streams by hand, so
// that a request can be cut into pieces anywhere, and gathers what comes
// back on each.
type h3Wire struct {
	t         *testing.T
	mu        sync.Mutex
	qc        *quic.Conn
	handshake chan struct{}
	streams   map[uint64]*h3WireStream
}

type h3WireStream struct {
	parser  frameParser
	status  int
	interim []int
	body    []byte
	done    bool
	reset   bool
	err     error
	changed chan struct{}
}

func (w *h3Wire) OnOpen(*fib.Connection)                 {}
func (w *h3Wire) OnPriorityData(*fib.Connection, []byte) {}
func (w *h3Wire) OnData(_ *fib.Connection, data []byte) {
	w.mu.Lock()
	qc := w.qc
	w.mu.Unlock()
	if qc != nil {
		qc.HandleDatagram(data)
	}
}
func (w *h3Wire) OnClose(*fib.Connection, error) {}

type h3WireHandler struct{ w *h3Wire }

func (h h3WireHandler) OnHandshake(*quic.Conn) { close(h.w.handshake) }

func (h h3WireHandler) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	if !s.Bidirectional() {
		return
	}
	st := h.w.stream(s.ID())
	h.w.mu.Lock()
	defer h.w.mu.Unlock()
	if st.err == nil {
		st.err = st.parser.feed(data, func(chunk []byte) error {
			st.body = append(st.body, chunk...)
			return nil
		}, func(typ uint64, payload []byte) error {
			if typ != frameHeaders {
				return nil
			}
			return qpack.Decode(payload, 1<<20, func(f qpack.HeaderField) error {
				if f.Name != ":status" {
					return nil
				}
				status, err := strconv.Atoi(f.Value)
				switch {
				case err != nil:
					return err
				case status < 200:
					st.interim = append(st.interim, status)
				default:
					st.status = status
				}
				return nil
			})
		})
	}
	st.done = st.done || fin
	st.notifyLocked()
}

func (h h3WireHandler) OnStreamReset(s *quic.Stream, _ uint64) {
	st := h.w.stream(s.ID())
	h.w.mu.Lock()
	st.reset, st.done = true, true
	st.notifyLocked()
	h.w.mu.Unlock()
}
func (h h3WireHandler) OnStopSending(*quic.Stream, uint64) {}
func (h h3WireHandler) OnStreamsAvailable(*quic.Conn)      {}
func (h h3WireHandler) OnClose(*quic.Conn, error)          {}

func (st *h3WireStream) notifyLocked() {
	close(st.changed)
	st.changed = make(chan struct{})
}

func (w *h3Wire) stream(id uint64) *h3WireStream {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.streams[id]
	if st == nil {
		st = &h3WireStream{parser: frameParser{maxFrame: 1 << 20}, changed: make(chan struct{})}
		w.streams[id] = st
	}
	return st
}

// await waits until cond holds of the stream.
func (w *h3Wire) await(id uint64, what string, cond func(*h3WireStream) bool) *h3WireStream {
	w.t.Helper()
	st := w.stream(id)
	deadline := time.After(20 * time.Second)
	for {
		w.mu.Lock()
		ok, changed := cond(st), st.changed
		w.mu.Unlock()
		if ok {
			return st
		}
		select {
		case <-changed:
		case <-deadline:
			w.t.Fatalf("stream %d: no %s", id, what)
		}
	}
}

func (w *h3Wire) response(id uint64) (int, []byte) {
	w.t.Helper()
	st := w.await(id, "response", func(st *h3WireStream) bool { return st.done || st.err != nil })
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case st.err != nil:
		w.t.Fatalf("stream %d: %v", id, st.err)
	case st.reset:
		w.t.Fatalf("stream %d was reset", id)
	}
	return st.status, st.body
}

func (w *h3Wire) awaitContinue(id uint64) {
	w.t.Helper()
	st := w.await(id, "100 Continue", func(st *h3WireStream) bool { return len(st.interim) > 0 || st.done })
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(st.interim) == 0 || st.interim[0] != stdhttp.StatusContinue {
		w.t.Fatalf("stream %d: interim responses %v", id, st.interim)
	}
}

// dialH3Wire connects to base from engine, or from an engine of its own when
// engine is nil.
func dialH3Wire(t *testing.T, engine *fib.Engine, base string) *h3Wire {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	if engine == nil {
		engine = newH3ClientEngine(t)
	}
	w := &h3Wire{t: t, handshake: make(chan struct{}), streams: map[uint64]*h3WireStream{}}
	dialed := make(chan error, 1)
	err = engine.DialWithHandler("udp", "127.0.0.1:"+u.Port(), time.Second, w, func(fc *fib.Connection, err error) {
		if err != nil {
			dialed <- err
			return
		}
		w.mu.Lock()
		w.qc, err = quic.Dial(fc, fc.RemoteAddr(), quic.Config{TLSConfig: clientTLS}, h3WireHandler{w})
		w.mu.Unlock()
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.handshake:
	case <-time.After(15 * time.Second):
		// Generous, since a handshake packet lost among the matrix's
		// other connections is resent only after a backoff.
		t.Fatal("no handshake")
	}
	return w
}

// newH3ClientEngine starts an engine for raw QUIC clients to dial from.
func newH3ClientEngine(t *testing.T) *fib.Engine {
	t.Helper()
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	return engine
}

// h3MatrixSlots bounds how many matrix cases run at once, each with a QUIC
// connection of its own: past a few, their datagrams outrun the sockets'
// buffers on a small machine and the cases time out on loss recovery rather
// than on anything the server does.
var h3MatrixSlots = make(chan struct{}, 4)

// h3Request is one request of a case, encoded as the frames of its stream:
// HEADERS, then DATA, then a trailing HEADERS for a trailer.
type h3Request struct {
	path    string
	framing h3Framing
	body    []byte
	trailer string
}

func newH3Request(mode string, framing h3Framing, size, n int) h3Request {
	req := h3Request{path: "/" + mode, framing: framing}
	if framing != h3None {
		req.body = make([]byte, size)
		for i := range req.body {
			req.body[i] = byte('a' + (i*7+n)%26)
		}
	}
	if framing == h3Trailer {
		req.trailer = fmt.Sprintf("sum-%d", n)
	}
	return req
}

func (r h3Request) frames() (head, body []byte) {
	method := stdhttp.MethodPost
	if r.framing == h3None {
		method = stdhttp.MethodGet
	}
	fields := []qpack.HeaderField{{Name: ":method", Value: method}, {Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "localhost"}, {Name: ":path", Value: r.path}}
	switch r.framing {
	case h3Length:
		fields = append(fields, qpack.HeaderField{Name: "content-length", Value: strconv.Itoa(len(r.body))})
	case h3Expect:
		fields = append(fields, qpack.HeaderField{Name: "content-length", Value: strconv.Itoa(len(r.body))},
			qpack.HeaderField{Name: "expect", Value: "100-continue"})
	}
	head = headersFrame(fields...)
	// The body in DATA frames of uneven sizes.
	rest := r.body
	for i := 0; len(rest) > 0; i++ {
		n := min(len(rest), 9001+i*257)
		body = appendFrameHeader(body, frameData, n)
		body = append(body, rest[:n]...)
		rest = rest[n:]
	}
	if r.trailer != "" {
		body = append(body, headersFrame(qpack.HeaderField{Name: "x-sum", Value: r.trailer})...)
	}
	return head, body
}

func h3Pieces(b []byte, n int) [][]byte {
	var pieces [][]byte
	for i := n; i > 1 && len(b) > 0; i-- {
		cut := min(max(1, len(b)/i+len(b)%7), len(b))
		pieces = append(pieces, b[:cut])
		b = b[cut:]
	}
	if len(b) > 0 {
		pieces = append(pieces, b)
	}
	return pieces
}

// send writes a request on a stream of its own, and ends the stream.
func (w *h3Wire) send(t *testing.T, r h3Request, gaps bool) uint64 {
	t.Helper()
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head, body := r.frames()
	expect := r.framing == h3Expect
	write := func(p []byte, fin bool) {
		if err := s.Write(p, fin); err != nil {
			t.Fatal(err)
		}
	}
	switch {
	case !gaps && !expect:
		write(append(head, body...), true)
	case !gaps:
		write(head, false)
		w.awaitContinue(s.ID())
		write(body, true)
	default:
		for _, piece := range h3Pieces(head, 2) {
			write(piece, false)
			time.Sleep(10 * time.Millisecond)
		}
		if expect {
			w.awaitContinue(s.ID())
		}
		for _, piece := range h3Pieces(body, 3) {
			write(piece, false)
			time.Sleep(10 * time.Millisecond)
		}
		write(nil, true)
	}
	return s.ID()
}

func TestBodyMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("the body matrix runs every combination")
	}
	bases := map[string]string{}
	for _, m := range h3Configs {
		bases[m.name] = startServer(t, m.config(), h3MatrixHandler)
	}
	// One engine for every case's client, rather than one each.
	clients := newH3ClientEngine(t)
	t.Run("cases", func(t *testing.T) {
		for _, m := range h3Configs {
			for _, framing := range h3Framings {
				for _, size := range []struct {
					name string
					size int
				}{{"small", 100}, {"large", 768 << 10}} {
					if framing == h3None && size.size != 100 {
						continue
					}
					for _, gaps := range []bool{false, true} {
						for _, mode := range h3Modes {
							send := "once"
							if gaps {
								send = "gaps"
							}
							t.Run(fmt.Sprintf("h3/%s/%s/%s/%s/%s", m.name, framing, size.name, send, mode), func(t *testing.T) {
								t.Parallel()
								h3MatrixSlots <- struct{}{}
								defer func() { <-h3MatrixSlots }()
								w := dialH3Wire(t, clients, bases[m.name])
								// Two requests on the connection, each on a
								// stream of its own, the second opened once the
								// first has been written.
								reqs := []h3Request{newH3Request(mode, framing, size.size, 0), newH3Request(mode, framing, size.size, 1)}
								ids := make([]uint64, len(reqs))
								for i, req := range reqs {
									ids[i] = w.send(t, req, gaps)
								}
								for i, req := range reqs {
									status, body := w.response(ids[i])
									checkH3Response(t, m, req, gaps, status, body)
								}
							})
						}
					}
				}
			}
		}
	})
	checkH3Violations(t)
}

func checkH3Response(t *testing.T, m h3Config, req h3Request, gaps bool, status int, body []byte) {
	t.Helper()
	if status != stdhttp.StatusOK {
		t.Fatalf("%s: status %d: %s", req.path, status, body)
	}
	var rep h3Report
	if err := json.Unmarshal(body, &rep); err != nil {
		t.Fatalf("%s: response %q: %v", req.path, body, err)
	}
	sum := sha256.Sum256(req.body)
	switch {
	case rep.Err != "":
		t.Fatalf("%s: the body ended with %s", req.path, rep.Err)
	case rep.Len != len(req.body):
		t.Fatalf("%s: the handler got %d bytes of %d", req.path, rep.Len, len(req.body))
	case rep.Sum != hex.EncodeToString(sum[:]):
		t.Fatalf("%s: the handler got the body's bytes wrong", req.path)
	case rep.Trailer != req.trailer:
		t.Fatalf("%s: trailer %q, want %q", req.path, rep.Trailer, req.trailer)
	}
	streams := m.streams(req)
	if rep.Streamed != streams {
		t.Fatalf("%s: streamed %v, want %v", req.path, rep.Streamed, streams)
	}
	if !streams && (!rep.Complete || rep.WouldBlock) {
		t.Fatalf("%s: a body read whole was not complete (complete %v, would block %v)", req.path, rep.Complete, rep.WouldBlock)
	}
	if streams && gaps && rep.Complete {
		// Normally the handler runs before a body sent in pieces is all
		// here; a loaded machine may deliver it at once, or, for a small body
		// without a content-length, its one DATA frame only arrives whole with
		// its last piece. Neither is wrong, only less telling.
		t.Logf("%s: a streamed body sent in pieces was already complete when the handler ran", req.path)
	}
}

// TestBodyMatrixAbort resets a request stream in the middle of its body. A
// body that streams tells its handler so, exactly once, through OnBody's
// error; a body read whole never reaches its handler at all. Either way the
// connection goes on serving other streams.
func TestBodyMatrixAbort(t *testing.T) {
	for _, m := range h3Configs {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			var (
				mu      sync.Mutex
				ran     bool
				endings []error
			)
			base := startServer(t, m.config(), func(c *fibhttp.Context, r *stdhttp.Request) {
				if r.URL.Path != "/cut" {
					h3MatrixHandler(c, r)
					return
				}
				mu.Lock()
				ran = true
				mu.Unlock()
				c.OnBody(func(data []byte, fin bool, err error) {
					if fin || err != nil {
						mu.Lock()
						endings = append(endings, err)
						mu.Unlock()
					}
				})
			})
			w := dialH3Wire(t, nil, base)
			s, err := w.qc.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			req := h3Request{path: "/cut", framing: h3Length, body: bytes.Repeat([]byte("c"), 256<<10)}
			head, body := req.frames()
			_ = s.Write(head, false)
			_ = s.Write(body[:len(body)/2], false)
			time.Sleep(50 * time.Millisecond)
			s.Reset(uint64(ErrCodeRequestCancelled))

			streams := m.streams(req)
			deadline := time.Now().Add(5 * time.Second)
			for streams && time.Now().Before(deadline) {
				mu.Lock()
				n := len(endings)
				mu.Unlock()
				if n > 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			gotRan, gotEndings := ran, endings
			mu.Unlock()
			if streams {
				if len(gotEndings) != 1 || gotEndings[0] == nil {
					t.Fatalf("the callback ended %v, want once with an error", gotEndings)
				}
			} else if gotRan {
				t.Fatal("the handler ran for a body that never arrived whole")
			}
			id := w.send(t, h3Request{path: "/alive", framing: h3None}, false)
			if status, got := w.response(id); status != stdhttp.StatusOK || string(got) != "alive" {
				t.Fatalf("after the reset: %d %q", status, got)
			}
		})
	}
}

// TestBodyMatrixLimits checks where each configuration draws the line on a
// body's size: one read whole past MaxBodyBytes is refused with 413 on its
// header, before any of it is sent, and one that streams is bounded by
// MaxStreamedBodyBytes instead, so the same body is served when it streams.
func TestBodyMatrixLimits(t *testing.T) {
	for _, m := range h3Configs[:2] {
		t.Run(m.name, func(t *testing.T) {
			config := m.config()
			config.MaxBodyBytes = 64 << 10
			w := dialH3Wire(t, nil, startServer(t, config, h3MatrixHandler))
			req := newH3Request("onbody", h3Length, 256<<10, 0)
			if m.streams(req) {
				id := w.send(t, req, false)
				status, body := w.response(id)
				checkH3Response(t, m, req, false, status, body)
				return
			}
			s, err := w.qc.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			head, _ := req.frames()
			_ = s.Write(head, false)
			st := w.await(s.ID(), "status", func(st *h3WireStream) bool { return st.status != 0 || st.done })
			w.mu.Lock()
			status := st.status
			w.mu.Unlock()
			if status != stdhttp.StatusRequestEntityTooLarge {
				t.Fatalf("status %d, want 413", status)
			}
		})
	}
	checkH3Violations(t)
}

// TestBodyMatrixFlowControl checks what paces a client whose streamed body
// the handler is slow to take: QUIC's stream window, given back only as the
// handler consumes the body. Until the handler takes any of it no more than a
// window's worth arrives, and once it does the rest follows.
func TestBodyMatrixFlowControl(t *testing.T) {
	type outcome struct {
		before int64
		err    error
	}
	done := make(chan outcome, 1)
	base := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context, r *stdhttp.Request) {
		c.Retain()
		go func() {
			defer c.Release()
			time.Sleep(300 * time.Millisecond)
			o := outcome{before: c.RequestBody().Consumed()}
			total := 0
			c.OnBody(func(data []byte, fin bool, err error) {
				total += len(data)
				if fin || err != nil {
					o.err = err
					done <- o
					_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(total)))
				}
			})
		}()
	})
	w := dialH3Wire(t, nil, base)
	const size = 6 << 20
	req := h3Request{path: "/slow", framing: h3Length, body: make([]byte, size)}
	id := w.send(t, req, false)
	status, body := w.response(id)
	if status != stdhttp.StatusOK || string(body) != strconv.Itoa(size) {
		t.Fatalf("answered %d %q, want %d", status, body, size)
	}
	// QUIC's default stream window, plus the first delivery, which is given
	// back before the request can hold the credit.
	const window = 1<<20 + 64<<10
	switch o := <-done; {
	case o.err != nil:
		t.Fatalf("the body ended with %v", o.err)
	case o.before > window:
		t.Fatalf("%d bytes arrived before the handler took any, past the stream window", o.before)
	case o.before == 0:
		t.Fatal("nothing arrived before the handler took the body")
	default:
		t.Logf("%d bytes arrived before the handler took any", o.before)
	}
}

// TestOnBodyWaitsForTheHandler is package http's test of the same name for
// HTTP/3: the body goes on arriving while the handler runs on another
// goroutine, and a handler that called OnBody with nothing arrived yet still
// hears nothing until it has returned.
func TestOnBodyWaitsForTheHandler(t *testing.T) {
	registered := make(chan struct{})
	early := make(chan bool, 1)
	base := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context, r *stdhttp.Request) {
		var returned atomic.Bool
		var got []byte
		c.OnBody(func(data []byte, fin bool, err error) {
			got = append(got, data...)
			if fin || err != nil {
				early <- !returned.Load()
				_ = c.Respond(stdhttp.StatusOK, "text/plain", got)
			}
		})
		close(registered)
		time.Sleep(200 * time.Millisecond)
		returned.Store(true)
	})
	w := dialH3Wire(t, nil, base)
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head, body := h3Request{path: "/", framing: h3Length, body: []byte("hello")}.frames()
	_ = s.Write(head, false)
	<-registered
	_ = s.Write(body, true)
	if status, got := w.response(s.ID()); status != stdhttp.StatusOK || string(got) != "hello" {
		t.Fatalf("answered %d %q", status, got)
	}
	if <-early {
		t.Fatal("the body reached its callback before the handler returned")
	}
}
