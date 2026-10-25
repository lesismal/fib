//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// expectMode is what expectServer does once a request's HEADERS arrive.
type expectMode int

const (
	// expectContinue answers 100 after a pause, then echoes the body.
	expectContinue expectMode = iota
	// expectSilent never answers the expectation, and echoes the body.
	expectSilent
	// expectRefuse answers 417 at once, without the body.
	expectRefuse
)

// expectOutcome is what expectServer saw of one request.
type expectOutcome struct {
	// early says DATA arrived before the 100 was sent, bodyAfter how long
	// after the HEADERS the body ended, body what it was, and reset that
	// the client reset its sending side.
	early     bool
	bodyAfter time.Duration
	body      string
	reset     bool
}

// expectServer answers requests as their HEADERS arrive, rather than once
// the whole request has, which is what a request that expects 100-continue
// needs of a server.
type expectServer struct {
	mode     expectMode
	pause    time.Duration
	outcomes chan expectOutcome
}

type expectStream struct {
	mu        sync.Mutex
	parser    frameParser
	headersAt time.Time
	body      []byte
	answered  atomic.Bool
	early     atomic.Bool
	reported  bool
}

func (sv *expectServer) OnHandshake(qc *quic.Conn) {
	if control, err := qc.OpenUniStream(); err == nil {
		_ = control.Write(appendSettings([]byte{streamControl}), false)
	}
}

func (sv *expectServer) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	if !s.Bidirectional() {
		return
	}
	es, _ := s.Context.(*expectStream)
	if es == nil {
		es = &expectStream{parser: frameParser{maxFrame: 1 << 20}}
		s.Context = es
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	_ = es.parser.feed(data, func(chunk []byte) error {
		if !es.answered.Load() {
			es.early.Store(true)
		}
		es.body = append(es.body, chunk...)
		return nil
	}, func(typ uint64, _ []byte) error {
		if typ != frameHeaders || !es.headersAt.IsZero() {
			return nil
		}
		es.headersAt = time.Now()
		switch sv.mode {
		case expectContinue:
			go func() {
				time.Sleep(sv.pause)
				es.answered.Store(true)
				_ = s.Write(responseFrames("100"), false)
			}()
		case expectSilent:
			es.answered.Store(true)
		case expectRefuse:
			es.answered.Store(true)
			_ = s.Write(append(responseFrames("417", "content-length", "2"), dataFrame([]byte("no"))...), true)
		}
		return nil
	})
	if !fin || es.reported {
		return
	}
	es.reported = true
	sv.outcomes <- expectOutcome{early: es.early.Load(), bodyAfter: time.Since(es.headersAt), body: string(es.body)}
	if sv.mode != expectRefuse {
		_ = s.Write(append(responseFrames("200", "content-length", strconv.Itoa(len(es.body))), dataFrame(es.body)...), true)
	}
}

func (sv *expectServer) OnStreamReset(s *quic.Stream, _ uint64) {
	es, _ := s.Context.(*expectStream)
	if es == nil {
		return
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	if !es.reported {
		es.reported = true
		sv.outcomes <- expectOutcome{early: es.early.Load(), body: string(es.body), reset: true}
	}
}

func (sv *expectServer) OnStopSending(*quic.Stream, uint64) {}
func (sv *expectServer) OnStreamsAvailable(*quic.Conn)      {}
func (sv *expectServer) OnClose(*quic.Conn, error)          {}

type expectServerHandler struct {
	server *expectServer
	config quic.Config
}

func (h *expectServerHandler) OnOpen(*fib.Connection)                 {}
func (h *expectServerHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *expectServerHandler) OnData(c *fib.Connection, data []byte) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.HandleDatagram(data)
		return
	}
	if !quic.IsInitial(data) {
		return
	}
	if qc := quic.Accept(c, c.RemoteAddr(), h.config, h.server, data); qc != nil {
		c.SetAttachment(qc)
	}
}

func (h *expectServerHandler) OnClose(c *fib.Connection, err error) {
	if qc, ok := c.Attachment().(*quic.Conn); ok {
		qc.Abort(err)
	}
	c.SetAttachment(nil)
}

func startExpectServer(t *testing.T, mode expectMode, pause time.Duration) (*expectServer, string) {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	server := &expectServer{mode: mode, pause: pause, outcomes: make(chan expectOutcome, 4)}
	handler := &expectServerHandler{server: server, config: quic.Config{TLSConfig: ConfigureTLS(serverTLS)}}
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	return server, fmt.Sprintf("https://localhost:%d", addr.Port)
}

func (sv *expectServer) outcome(t *testing.T) expectOutcome {
	t.Helper()
	select {
	case o := <-sv.outcomes:
		return o
	case <-time.After(10 * time.Second):
		t.Fatal("the server saw no end of the request")
	}
	return expectOutcome{}
}

func expectRequest(t *testing.T, url, body string, streams bool) *stdhttp.Request {
	t.Helper()
	var reader io.Reader = strings.NewReader(body)
	if streams {
		pr, pw := io.Pipe()
		go func() {
			_, _ = io.WriteString(pw, body)
			_ = pw.Close()
		}()
		reader = pr
	}
	req := mustRequest(t, "POST", url, reader)
	req.Header.Set("Expect", "100-continue")
	return req
}

// The HEADERS of a request that expects 100-continue go alone, and its body
// once the server answers 100, whether it was read whole or streams.
func TestClientExpectContinue(t *testing.T) {
	for _, streams := range []bool{false, true} {
		t.Run("streams="+strconv.FormatBool(streams), func(t *testing.T) {
			server, url := startExpectServer(t, expectContinue, 300*time.Millisecond)
			client := newClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = time.Minute })
			resp, err := client.Go(expectRequest(t, url, "h3 body", streams)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); got != "h3 body" {
				t.Fatalf("got %q", got)
			}
			if o := server.outcome(t); o.early || o.body != "h3 body" {
				t.Fatalf("server saw %+v: the body came before the 100", o)
			}
		})
	}
}

// A server that never answers the expectation gets the body once
// ExpectContinueTimeout has run out.
func TestClientExpectContinueTimeout(t *testing.T) {
	const wait = 300 * time.Millisecond
	server, url := startExpectServer(t, expectSilent, 0)
	client := newClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = wait })
	resp, err := client.Go(expectRequest(t, url, "late", false)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "late" {
		t.Fatalf("got %q", got)
	}
	if o := server.outcome(t); o.bodyAfter < wait*2/3 {
		t.Fatalf("the body ended %v after the HEADERS, before the %v wait", o.bodyAfter, wait)
	}
}

// A final response before the 100 is the answer: the body is never sent, and
// the request stream's sending side is reset.
func TestClientExpectContinueRefused(t *testing.T) {
	for _, streams := range []bool{false, true} {
		t.Run("streams="+strconv.FormatBool(streams), func(t *testing.T) {
			server, url := startExpectServer(t, expectRefuse, 0)
			client := newClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = 300 * time.Millisecond })
			resp, err := client.Go(expectRequest(t, url, "unwanted", streams)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != stdhttp.StatusExpectationFailed || readBody(t, resp) != "no" {
				t.Fatalf("got %d", resp.StatusCode)
			}
			if o := server.outcome(t); !o.reset || o.body != "" {
				t.Fatalf("server saw %+v, want a reset and no body", o)
			}
			// Past the wait, nothing more is sent on the stream.
			time.Sleep(500 * time.Millisecond)
		})
	}
}

// fib's HTTP/3 server answers the expectation, so the body goes without the
// wait.
func TestClientExpectContinueToFibServer(t *testing.T) {
	url := startServer(t, Config{}, echo)
	client := newClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = time.Minute })
	start := time.Now()
	resp, err := client.Go(expectRequest(t, url, "to fib", false)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); !strings.Contains(got, "to fib") {
		t.Fatalf("got %q", got)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the body waited for ExpectContinueTimeout")
	}
}
