//go:build linux || darwin || windows

package http

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	stdtls "crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/tlstest"
	fibtls "github.com/lesismal/fib/tls"
)

// The body matrix drives every way a request body can reach a handler through
// every way a handler can take it, and checks the same things of each: the
// handler is given every byte and the trailer, a body the server buffered is
// complete when the handler runs, OnBody never calls back before it returns
// nor before the handler has, each body ends in exactly one last call, and
// the response written from that call goes out — with a second request
// pipelined behind the first, so that the connection is seen to carry on.
//
// The dimensions are:
//
//   - protocol: HTTP/1.1 and HTTP/2, each in cleartext and over TLS (HTTP/3,
//     which only runs over QUIC's TLS, has its own matrix in package http3);
//   - framing: no body, Content-Length, chunked (for HTTP/2, DATA frames with
//     no content-length), with a trailer, and with Expect: 100-continue;
//   - size: a body that arrives in one read, and one that takes many;
//   - sending: the whole exchange in one write, or in pieces with a pause
//     between them — the header split in two, and the body in three pieces
//     cut at arbitrary points, mid chunk line and mid frame included;
//   - the handler: reading Request.Body and taking what would block through
//     OnBody, OnBody alone, BodyComplete deciding between the two, and OnBody
//     called from another goroutine after the handler has returned;
//   - the server: buffering bodies whole, streaming every body, and
//     streaming only those past a threshold.

type matrixProto struct {
	name   string
	secure bool
	h2     bool
}

var matrixProtos = []matrixProto{
	{"h1", false, false},
	{"h1-tls", true, false},
	{"h2c", false, true},
	{"h2-tls", true, true},
}

type matrixFraming string

const (
	mframeNone    matrixFraming = "none"
	mframeLength  matrixFraming = "length"
	mframeChunked matrixFraming = "chunked"
	mframeTrailer matrixFraming = "trailer"
	mframeExpect  matrixFraming = "expect"
)

var matrixFramings = []matrixFraming{mframeNone, mframeLength, mframeChunked, mframeTrailer, mframeExpect}

type matrixConfig struct {
	name      string
	stream    bool
	threshold int64
}

var matrixConfigs = []matrixConfig{
	{"buffered", false, 0},
	{"stream", true, 0},
	{"stream-64k", true, 64 << 10},
}

func (m matrixConfig) config() Config {
	config := DefaultConfig()
	config.StreamRequestBody = m.stream
	config.StreamRequestBodyThreshold = m.threshold
	return config
}

var matrixModes = []string{"read", "onbody", "complete", "async"}

var matrixSizes = []struct {
	name string
	size int
}{{"small", 100}, {"large", 768 << 10}}

// matrixReport is what the handler answers with.
type matrixReport struct {
	Len        int
	Sum        string
	Trailer    string
	Complete   bool
	Streamed   bool
	WouldBlock bool
	Err        string
}

// matrixViolations collects what a body callback saw go wrong, which it
// cannot report through the response it may already have written.
var matrixViolations struct {
	sync.Mutex
	list []string
}

func matrixViolation(format string, args ...any) {
	matrixViolations.Lock()
	matrixViolations.list = append(matrixViolations.list, fmt.Sprintf(format, args...))
	matrixViolations.Unlock()
}

func checkMatrixViolations(t *testing.T) {
	t.Helper()
	// A callback after the last would come at once, if at all.
	time.Sleep(50 * time.Millisecond)
	matrixViolations.Lock()
	defer matrixViolations.Unlock()
	for _, v := range matrixViolations.list {
		t.Error(v)
	}
	matrixViolations.list = nil
}

// matrixServe is one request being served by matrixHandler.
type matrixServe struct {
	c        *Context
	r        *stdhttp.Request
	report   matrixReport
	digest   hash.Hash
	returned atomic.Bool
	ended    bool
}

func matrixHandler(c *Context, r *stdhttp.Request) {
	s := &matrixServe{c: c, r: r, digest: sha256.New()}
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
			case errors.Is(err, ErrWouldBlock):
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
		_ = c.Respond(stdhttp.StatusNotFound, "text/plain", nil)
	}
}

func (s *matrixServe) add(data []byte) {
	s.digest.Write(data)
	s.report.Len += len(data)
}

// onBody takes the rest of the body through OnBody, and checks that OnBody
// returned before any call, which a callback run inside it would wait on
// forever, and, when it was called by the handler, that no call came before
// the handler returned.
func (s *matrixServe) onBody(inHandler bool) {
	returned := make(chan struct{})
	s.c.OnBody(func(data []byte, fin bool, err error) {
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			matrixViolation("%s: a body callback ran inside OnBody", s.r.URL.Path)
		}
		if inHandler && !s.returned.Load() {
			matrixViolation("%s: a body callback ran before the handler returned", s.r.URL.Path)
		}
		if s.ended {
			matrixViolation("%s: a body callback after the last one", s.r.URL.Path)
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

func (s *matrixServe) answer(err error) {
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

// matrixServers is a server for each configuration, in cleartext and over
// TLS.
type matrixServers map[string][2]string

func startMatrixServers(t *testing.T, handler HandlerFunc, configs ...matrixConfig) matrixServers {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	servers := matrixServers{}
	for _, m := range configs {
		plain := serve(t, NewHandlerWithConfig(m.config(), handler))
		secure := serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandlerWithConfig(m.config(), handler)))
		servers[m.name] = [2]string{plain, secure}
	}
	return servers
}

func (s matrixServers) addr(config string, secure bool) string {
	if secure {
		return s[config][1]
	}
	return s[config][0]
}

// matrixRequest is one request of a case.
type matrixRequest struct {
	path    string
	framing matrixFraming
	body    []byte
	trailer string
}

func newMatrixRequest(mode string, framing matrixFraming, size, n int) matrixRequest {
	req := matrixRequest{path: "/" + mode, framing: framing}
	if framing != mframeNone {
		req.body = make([]byte, size)
		for i := range req.body {
			req.body[i] = byte('a' + (i*7+n)%26)
		}
	}
	if framing == mframeTrailer {
		req.trailer = fmt.Sprintf("sum-%d", n)
	}
	return req
}

// h1 encodes the request as HTTP/1.1: its header, and its body as framed.
func (m matrixRequest) h1() (head, body []byte) {
	var b strings.Builder
	method := stdhttp.MethodPost
	if m.framing == mframeNone {
		method = stdhttp.MethodGet
	}
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: test\r\n", method, m.path)
	switch m.framing {
	case mframeLength:
		fmt.Fprintf(&b, "Content-Length: %d\r\n", len(m.body))
	case mframeExpect:
		fmt.Fprintf(&b, "Content-Length: %d\r\nExpect: 100-continue\r\n", len(m.body))
	case mframeChunked:
		b.WriteString("Transfer-Encoding: chunked\r\n")
	case mframeTrailer:
		b.WriteString("Transfer-Encoding: chunked\r\nTrailer: X-Sum\r\n")
	}
	b.WriteString("\r\n")
	head = []byte(b.String())
	switch m.framing {
	case mframeLength, mframeExpect:
		body = m.body
	case mframeChunked, mframeTrailer:
		// Chunks of uneven sizes, the first with an extension.
		rest := m.body
		for i := 0; len(rest) > 0; i++ {
			n := min(len(rest), 7001+i*113)
			ext := ""
			if i == 0 {
				ext = ";ext=1"
			}
			body = fmt.Appendf(body, "%x%s\r\n", n, ext)
			body = append(body, rest[:n]...)
			body = append(body, "\r\n"...)
			rest = rest[n:]
		}
		body = append(body, "0\r\n"...)
		if m.trailer != "" {
			body = fmt.Appendf(body, "X-Sum: %s\r\n", m.trailer)
		}
		body = append(body, "\r\n"...)
	}
	return head, body
}

// h2 encodes the request as HTTP/2 frames on stream id: its HEADERS, and its
// body as DATA frames, followed by a trailing HEADERS for a trailer.
func (m matrixRequest) h2(tc *h2TestConn, id uint32, secure bool) (head, body []byte) {
	method := stdhttp.MethodPost
	if m.framing == mframeNone {
		method = stdhttp.MethodGet
	}
	fields := []string{":method", method, ":scheme", strings.TrimSuffix(scheme(secure), "://"),
		":authority", "test", ":path", m.path}
	switch m.framing {
	case mframeLength:
		fields = append(fields, "content-length", strconv.Itoa(len(m.body)))
	case mframeExpect:
		fields = append(fields, "content-length", strconv.Itoa(len(m.body)), "expect", "100-continue")
	}
	block := tc.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		block = tc.enc.AppendField(block, fields[i], fields[i+1], false)
	}
	head = h2AppendHeaderBlock(nil, id, block, m.framing == mframeNone, h2DefaultMaxFrameSize)
	if m.framing == mframeNone {
		return head, nil
	}
	rest := m.body
	for len(rest) > 0 {
		n := min(len(rest), h2DefaultMaxFrameSize)
		var flags uint8
		if n == len(rest) && m.trailer == "" {
			flags = h2FlagEndStream
		}
		body = h2AppendFrameHeader(body, h2FrameData, flags, id, n)
		body = append(body, rest[:n]...)
		rest = rest[n:]
	}
	if m.trailer != "" {
		trailer := tc.enc.AppendField(tc.enc.Begin(nil), "x-sum", m.trailer, false)
		body = h2AppendHeaderBlock(body, id, trailer, true, h2DefaultMaxFrameSize)
	}
	return head, body
}

// matrixWire is a client connection the matrix drives.
type matrixWire interface {
	write(b []byte)
	// awaitContinue waits for the 100 Continue of the n-th request.
	awaitContinue(n int)
	// response reads the response to the n-th request.
	response(n int) (status int, body []byte)
	close()
}

type h1Wire struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func (w *h1Wire) write(b []byte) {
	w.t.Helper()
	if _, err := w.c.Write(b); err != nil {
		w.t.Fatal(err)
	}
}

func (w *h1Wire) awaitContinue(int) {
	w.t.Helper()
	line, err := w.r.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "HTTP/1.1 100 ") {
		w.t.Fatalf("interim response %q, %v", line, err)
	}
	if blank, err := w.r.ReadString('\n'); err != nil || blank != "\r\n" {
		w.t.Fatalf("interim response ended with %q, %v", blank, err)
	}
}

func (w *h1Wire) response(int) (int, []byte) {
	w.t.Helper()
	resp, err := stdhttp.ReadResponse(w.r, nil)
	if err != nil {
		w.t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		w.t.Fatal(err)
	}
	return resp.StatusCode, body
}

func (w *h1Wire) close() { w.c.Close() }

type h2Wire struct {
	tc *h2TestConn
}

func (w *h2Wire) write(b []byte) { w.tc.write(b) }

func (w *h2Wire) awaitContinue(n int) {
	st := w.tc.stream(matrixStreamID(n))
	for len(st.interim) == 0 && !st.done {
		w.tc.read()
	}
	if len(st.interim) == 0 || st.interim[0] != "100" {
		w.tc.t.Fatalf("interim responses %v", st.interim)
	}
}

func (w *h2Wire) response(n int) (int, []byte) {
	header, body := w.tc.response(matrixStreamID(n))
	status, _ := strconv.Atoi(header[":status"])
	return status, body
}

func (w *h2Wire) close() { w.tc.c.Close() }

func matrixStreamID(n int) uint32 { return uint32(2*n + 1) }

// dialMatrix connects to addr as proto speaks.
func dialMatrix(t *testing.T, addr string, proto matrixProto) matrixWire {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if proto.secure {
		_, clientTLS, err := tlstest.Configs()
		if err != nil {
			t.Fatal(err)
		}
		clientTLS.NextProtos = []string{"http/1.1"}
		if proto.h2 {
			clientTLS.NextProtos = []string{"h2"}
		}
		tc := stdtls.Client(c, clientTLS)
		_ = tc.SetDeadline(time.Now().Add(5 * time.Second))
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		if want := clientTLS.NextProtos[0]; tc.ConnectionState().NegotiatedProtocol != want {
			t.Fatalf("negotiated %q, want %q", tc.ConnectionState().NegotiatedProtocol, want)
		}
		c = tc
	}
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	if !proto.h2 {
		return &h1Wire{t: t, c: c, r: bufio.NewReader(c)}
	}
	tc := newH2TestConn(t, c)
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	tc.write(append([]byte(h2Preface), h2AppendSettings(nil)...))
	return &h2Wire{tc: tc}
}

// matrixPieces cuts b into n pieces at uneven points.
func matrixPieces(b []byte, n int) [][]byte {
	var pieces [][]byte
	for i := n; i > 1 && len(b) > 0; i-- {
		cut := max(1, len(b)/i+len(b)%7)
		cut = min(cut, len(b))
		pieces = append(pieces, b[:cut])
		b = b[cut:]
	}
	if len(b) > 0 {
		pieces = append(pieces, b)
	}
	return pieces
}

const matrixGap = 10 * time.Millisecond

// sendMatrix sends the requests, whole in one write or in pieces with pauses
// between them. A request that expects 100 Continue has its body sent only
// once the server has asked for it.
func sendMatrix(w matrixWire, heads, bodies [][]byte, gaps bool, expect bool) {
	if !gaps {
		if !expect {
			var all []byte
			for i := range heads {
				all = append(append(all, heads[i]...), bodies[i]...)
			}
			w.write(all)
			return
		}
		for i := range heads {
			w.write(heads[i])
			w.awaitContinue(i)
			w.write(bodies[i])
		}
		return
	}
	for i := range heads {
		for _, piece := range matrixPieces(heads[i], 2) {
			w.write(piece)
			time.Sleep(matrixGap)
		}
		if expect {
			w.awaitContinue(i)
		}
		for _, piece := range matrixPieces(bodies[i], 3) {
			w.write(piece)
			time.Sleep(matrixGap)
		}
	}
}

func TestBodyMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("the body matrix runs every combination")
	}
	servers := startMatrixServers(t, matrixHandler, matrixConfigs...)
	t.Run("cases", func(t *testing.T) {
		for _, proto := range matrixProtos {
			for _, m := range matrixConfigs {
				for _, framing := range matrixFramings {
					for _, size := range matrixSizes {
						if framing == mframeNone && size.size != matrixSizes[0].size {
							continue
						}
						for _, gaps := range []bool{false, true} {
							for _, mode := range matrixModes {
								send := "once"
								if gaps {
									send = "gaps"
								}
								name := fmt.Sprintf("%s/%s/%s/%s/%s/%s", proto.name, m.name, framing, size.name, send, mode)
								t.Run(name, func(t *testing.T) {
									t.Parallel()
									runMatrixCase(t, servers.addr(m.name, proto.secure), proto, m, framing, size.size, gaps, mode)
								})
							}
						}
					}
				}
			}
		}
	})
	checkMatrixViolations(t)
}

func runMatrixCase(t *testing.T, addr string, proto matrixProto, m matrixConfig, framing matrixFraming, size int, gaps bool, mode string) {
	w := dialMatrix(t, addr, proto)
	defer w.close()
	// Two requests on the connection, the second pipelined behind the first
	// on HTTP/1 and on a stream of its own on HTTP/2, except where the first
	// waits for 100 Continue: the second goes once the first is answered.
	count := 2
	if framing == mframeExpect {
		count = 1
	}
	reqs := make([]matrixRequest, count)
	heads, bodies := make([][]byte, count), make([][]byte, count)
	for i := range reqs {
		reqs[i] = newMatrixRequest(mode, framing, size, i)
		if proto.h2 {
			heads[i], bodies[i] = reqs[i].h2(w.(*h2Wire).tc, matrixStreamID(i), proto.secure)
		} else {
			heads[i], bodies[i] = reqs[i].h1()
		}
	}
	sendMatrix(w, heads, bodies, gaps, framing == mframeExpect)
	for i, req := range reqs {
		status, body := w.response(i)
		checkMatrixResponse(t, proto, m, req, gaps, status, body)
	}
	if framing == mframeExpect {
		// The connection carries on past a request that waited.
		next := newMatrixRequest(mode, mframeLength, size, 1)
		var head, body []byte
		if proto.h2 {
			head, body = next.h2(w.(*h2Wire).tc, matrixStreamID(1), proto.secure)
		} else {
			head, body = next.h1()
		}
		sendMatrix(w, [][]byte{head}, [][]byte{body}, false, false)
		status, got := w.response(1)
		checkMatrixResponse(t, proto, m, next, false, status, got)
	}
}

func checkMatrixResponse(t *testing.T, proto matrixProto, m matrixConfig, req matrixRequest, gaps bool, status int, body []byte) {
	t.Helper()
	if status != stdhttp.StatusOK {
		t.Fatalf("%s: status %d: %s", req.path, status, body)
	}
	var rep matrixReport
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
	// A body streams when the server streams bodies and it is past the
	// threshold. Whether one of no declared length does — chunked on
	// HTTP/1, DATA without content-length on HTTP/2 — is settled by that
	// alone, as much as for one with a length, since such a body is handed
	// over once more than the threshold of it has arrived, and the whole of
	// it is more.
	streams := m.stream && req.framing != mframeNone && int64(len(req.body)) > m.threshold
	if rep.Streamed != streams {
		t.Fatalf("%s: streamed %v, want %v", req.path, rep.Streamed, streams)
	}
	if !streams && (!rep.Complete || rep.WouldBlock) {
		t.Fatalf("%s: a buffered body was not complete (complete %v, would block %v)", req.path, rep.Complete, rep.WouldBlock)
	}
	if streams && gaps && rep.Complete && req.framing != mframeNone {
		// The body is sent in pieces with pauses between them, so the
		// handler normally runs before it is all here. A loaded machine may
		// deliver it all at once, and on HTTP/2 a small body without a
		// content-length travels in one DATA frame, which is handed over only
		// once it has arrived whole, with its last piece. Neither is wrong,
		// only less telling.
		t.Logf("%s: a streamed body sent in pieces was already complete when the handler ran", req.path)
	}
}

// TestBodyMatrixAbort cuts a request off in the middle of its body, on every
// protocol and configuration: a handler given the body as it streams hears of
// it exactly once, through OnBody's error, and one whose server buffers the
// body never runs at all. Either way the server goes on serving.
func TestBodyMatrixAbort(t *testing.T) {
	var (
		mu      sync.Mutex
		ran     = map[string]bool{}
		endings = map[string][]error{}
	)
	handler := func(c *Context, r *stdhttp.Request) {
		key := r.URL.Query().Get("key")
		mu.Lock()
		ran[key] = true
		mu.Unlock()
		if key == "" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("alive"))
			return
		}
		c.OnBody(func(data []byte, fin bool, err error) {
			if fin || err != nil {
				mu.Lock()
				endings[key] = append(endings[key], err)
				mu.Unlock()
			}
		})
	}
	servers := startMatrixServers(t, handler, matrixConfigs...)
	for _, proto := range matrixProtos {
		for _, m := range matrixConfigs {
			t.Run(proto.name+"/"+m.name, func(t *testing.T) {
				t.Parallel()
				addr := servers.addr(m.name, proto.secure)
				key := proto.name + "-" + m.name
				req := matrixRequest{path: "/cut?key=" + key, framing: mframeLength, body: bytes.Repeat([]byte("c"), 768<<10)}
				w := dialMatrix(t, addr, proto)
				var head, body []byte
				if proto.h2 {
					head, body = req.h2(w.(*h2Wire).tc, 1, proto.secure)
				} else {
					head, body = req.h1()
				}
				w.write(head)
				w.write(body[:len(body)/2])
				time.Sleep(50 * time.Millisecond)
				w.close()

				streams := m.stream
				deadline := time.Now().Add(5 * time.Second)
				for streams && time.Now().Before(deadline) {
					mu.Lock()
					n := len(endings[key])
					mu.Unlock()
					if n > 0 {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				time.Sleep(100 * time.Millisecond)
				mu.Lock()
				gotRan, gotEndings := ran[key], endings[key]
				mu.Unlock()
				if streams {
					if len(gotEndings) != 1 || gotEndings[0] == nil {
						t.Fatalf("the callback ended %v, want once with an error", gotEndings)
					}
				} else if gotRan {
					t.Fatal("the handler ran for a buffered body that never arrived whole")
				}

				alive := dialMatrix(t, addr, proto)
				get := matrixRequest{path: "/alive", framing: mframeNone}
				if proto.h2 {
					head, _ = get.h2(alive.(*h2Wire).tc, 1, proto.secure)
				} else {
					head, _ = get.h1()
				}
				alive.write(head)
				if status, got := alive.response(0); status != stdhttp.StatusOK || string(got) != "alive" {
					t.Fatalf("after the cut: %d %q", status, got)
				}
			})
		}
	}
}

// TestBodyMatrixLimits checks where each configuration draws the line on a
// body's size: a buffered body past MaxBodyBytes is refused with 413 on every
// protocol before any of it is read, and a streamed one is bounded by
// MaxStreamedBodyBytes instead, so the same body is served when it streams.
func TestBodyMatrixLimits(t *testing.T) {
	configs := []matrixConfig{{"buffered", false, 0}, {"stream", true, 0}}
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	servers := matrixServers{}
	for _, m := range configs {
		config := m.config()
		config.MaxBodyBytes = 64 << 10
		plain := serve(t, NewHandlerWithConfig(config, HandlerFunc(matrixHandler)))
		secure := serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandlerWithConfig(config, HandlerFunc(matrixHandler))))
		servers[m.name] = [2]string{plain, secure}
	}
	for _, proto := range matrixProtos {
		for _, m := range configs {
			t.Run(proto.name+"/"+m.name, func(t *testing.T) {
				t.Parallel()
				w := dialMatrix(t, servers.addr(m.name, proto.secure), proto)
				req := newMatrixRequest("onbody", mframeLength, 256<<10, 0)
				var head, body []byte
				if proto.h2 {
					head, body = req.h2(w.(*h2Wire).tc, 1, proto.secure)
				} else {
					head, body = req.h1()
				}
				streams := m.stream
				w.write(head)
				if !streams {
					// Refused on its header; the body is never asked for.
					if status, _ := w.response(0); status != stdhttp.StatusRequestEntityTooLarge {
						t.Fatalf("status %d, want 413", status)
					}
					return
				}
				w.write(body)
				status, got := w.response(0)
				checkMatrixResponse(t, proto, m, req, false, status, got)
			})
		}
	}
	checkMatrixViolations(t)
}

// TestBodyMatrixFlowControl checks what paces a client whose streamed body
// the handler is slow to take on HTTP/2, where the connection cannot stop
// reading for one stream: the stream's window, given back only as the
// handler consumes the body. Until the handler takes any of it no more than a
// window's worth arrives, and once it does the rest follows. net/http's
// client is the peer, since it keeps to the windows it is given.
func TestBodyMatrixFlowControl(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	type outcome struct {
		before int64
		total  int
		err    error
	}
	done := make(chan outcome, 2)
	handler := HandlerFunc(func(c *Context, r *stdhttp.Request) {
		c.Retain()
		go func() {
			defer c.Release()
			// Long enough for a client that ignored the window to send
			// everything.
			time.Sleep(300 * time.Millisecond)
			o := outcome{before: c.RequestBody().Consumed()}
			c.OnBody(func(data []byte, fin bool, err error) {
				o.total += len(data)
				if fin || err != nil {
					o.err = err
					done <- o
					_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(o.total)))
				}
			})
		}()
	})
	for _, secure := range []bool{false, true} {
		var addr string
		if secure {
			serverTLS, _, err := tlstest.Configs()
			if err != nil {
				t.Fatal(err)
			}
			addr = serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandlerWithConfig(config, handler)))
		} else {
			addr = serve(t, NewHandlerWithConfig(config, handler))
		}
		client := netHTTPClient(t, secure)
		const size = 6 << 20
		resp, err := client.Post(scheme(secure)+addr+"/slow", "application/octet-stream", bytes.NewReader(make([]byte, size)))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.ProtoMajor != 2 || string(body) != strconv.Itoa(size) {
			t.Fatalf("secure=%v: %s answered %q, want %d", secure, resp.Proto, body, size)
		}
		o := <-done
		switch {
		case o.err != nil:
			t.Fatalf("secure=%v: the body ended with %v", secure, o.err)
		case o.before > h2StreamWindow:
			t.Fatalf("secure=%v: %d bytes arrived before the handler took any, past the %d byte window", secure, o.before, h2StreamWindow)
		case o.before == 0:
			t.Fatalf("secure=%v: nothing arrived before the handler took the body", secure)
		}
	}
}

// TestOnBodyWaitsForTheHandlerOnHTTP2 covers what HTTP/1 cannot show: on a
// multiplexed connection the body goes on arriving, on the goroutine reading
// the connection, while the handler runs on another. A handler that calls
// OnBody with nothing arrived yet and then carries on still hears nothing
// until it has returned.
func TestOnBodyWaitsForTheHandlerOnHTTP2(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	registered := make(chan struct{})
	early := make(chan bool, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
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
		// The body arrives while the handler is still here.
		time.Sleep(200 * time.Millisecond)
		returned.Store(true)
	})))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "test", ":path", "/", "content-length", "5")
	<-registered
	tc.data(1, true, []byte("hello"))
	if _, body := tc.response(1); string(body) != "hello" {
		t.Fatalf("body %q", body)
	}
	if <-early {
		t.Fatal("the body reached its callback before the handler returned")
	}
}
