//go:build linux || darwin || windows

// Command client drives the gateway example over every protocol it speaks.
//
//	go run ./examples/gateway/upstream
//	go run ./examples/gateway/server
//	go run ./examples/gateway/client
//
// It reaches the gateway as HTTP/1.1, HTTP/2 and HTTP/3, and for each asks for
// a small echo, a large download that is taken piece by piece with OnBody, and
// a slow stream that ends with a trailer, from each of the gateway's three
// upstreams (/h1, /h2 and /h3, which the gateway reaches over HTTP/1.1,
// HTTP/2 and HTTP/3). Then it opens WebSocket connections through the gateway,
// in the clear and over TLS, and has the upstream echo them.
//
// Everything is asynchronous: a request is sent with Do and answered in a
// callback, a WebSocket is dialed with a callback, and main only waits on
// channels for the results to print them.
package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/websocket"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8446", "gateway TLS address")
	plainAddr := flag.String("plain-addr", "127.0.0.1:8081", "gateway cleartext address; empty to skip the ws:// check")
	size := flag.Int64("size", 8<<20, "bytes to download")
	certFlags := certs.ClientFlagsWithCA(flag.CommandLine, certs.GatewayCertFile)
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	engine, stop := example.ClientEngine()
	defer stop()

	clients := []struct {
		name string
		doer doer
	}{
		{"HTTP/1.1", newHTTPClient(engine, tlsConfig, true)},
		{"HTTP/2", newHTTPClient(engine, tlsConfig, false)},
		{"HTTP/3", newHTTP3Client(engine, tlsConfig)},
	}
	failed := 0
	for _, client := range clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			base := "https://" + *addr + "/" + upstream
			label := fmt.Sprintf("%-8s -> gateway -> %s", client.name, upstream)
			failed += check(label, echo(client.doer, base))
			failed += check(label, download(client.doer, base, *size))
			failed += check(label, stream(client.doer, base))
		}
	}
	dialer := func(secure bool) *websocket.Dialer {
		config := websocket.DefaultDialerConfig()
		config.HandshakeTimeout = 5 * time.Second
		config.Subprotocols = []string{"superchat"}
		config.TLSConfig = tlsConfig
		return websocket.NewDialer(engine, config)
	}
	for _, upstream := range []string{"h1", "h2", "h3"} {
		if *plainAddr != "" {
			failed += check("ws://  -> gateway -> "+upstream, relay(dialer(false), "ws://"+*plainAddr+"/"+upstream+"/ws"))
		}
		failed += check("wss:// -> gateway -> "+upstream, relay(dialer(true), "wss://"+*addr+"/"+upstream+"/ws"))
	}
	if failed > 0 {
		example.Fatal(fmt.Errorf("%d checks failed", failed))
	}
	fmt.Println("all checks passed")
}

// check prints the outcome of one exchange, and counts a failure.
func check(label string, outcome outcome) int {
	if outcome.err != nil {
		fmt.Printf("FAIL %s %s: %v\n", label, outcome.what, outcome.err)
		return 1
	}
	fmt.Printf("ok   %s %s: %s\n", label, outcome.what, outcome.detail)
	return 0
}

type outcome struct {
	what, detail string
	err          error
}

// doer is what the HTTP/1.1, HTTP/2 and HTTP/3 clients have in common.
type doer interface {
	Do(req *http.Request, callback func(*fibhttp.ClientResponse, error))
}

// newHTTPClient is the package http client, which speaks HTTP/2 to a server
// that chooses it through ALPN, or HTTP/1.1 only if asked to.
func newHTTPClient(engine *fib.Engine, tlsConfig *tls.Config, http1Only bool) doer {
	config := fibhttp.DefaultClientConfig()
	config.TLSConfig = tlsConfig
	config.DisableHTTP2 = http1Only
	// Take large bodies as they arrive rather than buffered whole.
	config.StreamResponseBody = true
	return fibhttp.NewClient(engine, config)
}

func newHTTP3Client(engine *fib.Engine, tlsConfig *tls.Config) doer {
	config := http3.DefaultClientConfig()
	config.TLSConfig = tlsConfig
	config.StreamResponseBody = true
	return http3.NewClient(engine, config)
}

// reply is what one request came to.
type reply struct {
	status  int
	proto   string
	header  http.Header
	trailer http.Header
	body    []byte
	pieces  int
	bytes   int64
	elapsed time.Duration
	err     error
}

// fetch sends req and waits for its whole response, which arrives in
// callbacks: the response itself, then each piece of its body. onPiece, if
// set, sees every piece, and keep says whether to keep the body.
func fetch(d doer, req *http.Request, keep bool, onPiece func(offset int64, piece []byte) error) reply {
	var r reply
	done := make(chan struct{})
	start := time.Now()
	d.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		if err != nil {
			r.err = err
			close(done)
			return
		}
		r.status, r.proto, r.header = resp.StatusCode, resp.Proto, resp.Header
		resp.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				r.err = err
			}
			if len(data) > 0 {
				r.pieces++
				if onPiece != nil && r.err == nil {
					r.err = onPiece(r.bytes, data)
				}
				if keep {
					r.body = append(r.body, data...)
				}
				r.bytes += int64(len(data))
			}
			if fin {
				r.trailer = resp.Trailer()
				r.elapsed = time.Since(start)
				close(done)
			}
		})
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		r.err = fmt.Errorf("timed out")
	}
	return r
}

func echo(d doer, base string) outcome {
	out := outcome{what: "echo"}
	req, _ := http.NewRequest(http.MethodPost, base+"/echo?x=1", strings.NewReader("hello gateway"))
	req.Header.Set("Content-Type", "text/plain")
	r := fetch(d, req, true, nil)
	if r.err != nil {
		out.err = r.err
		return out
	}
	text := string(r.body)
	switch {
	case r.status != 200:
		out.err = fmt.Errorf("status %d", r.status)
	case !strings.Contains(text, "body: hello gateway") || !strings.Contains(text, "X-Forwarded-For: 127.0.0.1"):
		out.err = fmt.Errorf("unexpected reply %q", text)
	default:
		out.detail = fmt.Sprintf("%s via %s, upstream saw %s", r.proto, r.header.Get("Via"), strings.SplitN(text, "\n", 2)[0])
	}
	return out
}

func download(d doer, base string, size int64) outcome {
	out := outcome{what: "download"}
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/download?size=%d", base, size), nil)
	r := fetch(d, req, false, func(offset int64, piece []byte) error {
		for i, b := range piece {
			if b != byte('a'+(offset+int64(i))%26) {
				return fmt.Errorf("byte %d is %q", offset+int64(i), b)
			}
		}
		return nil
	})
	switch {
	case r.err != nil:
		out.err = r.err
	case r.status != 200 || r.bytes != size:
		out.err = fmt.Errorf("status %d, %d of %d bytes", r.status, r.bytes, size)
	default:
		out.detail = fmt.Sprintf("%s, %d bytes in %d pieces, %v", r.proto, r.bytes, r.pieces, r.elapsed.Round(time.Millisecond))
	}
	return out
}

func stream(d doer, base string) outcome {
	out := outcome{what: "stream"}
	req, _ := http.NewRequest(http.MethodGet, base+"/stream?n=4&delay=100ms", nil)
	r := fetch(d, req, true, nil)
	switch {
	case r.err != nil:
		out.err = r.err
	case string(r.body) != "line 1 of 4\nline 2 of 4\nline 3 of 4\nline 4 of 4\n":
		out.err = fmt.Errorf("body %q", r.body)
	case r.trailer.Get("X-Lines") != "4":
		out.err = fmt.Errorf("trailer %v", r.trailer)
	case r.pieces < 2 || r.elapsed < 300*time.Millisecond:
		out.err = fmt.Errorf("%d pieces in %v: the gateway did not stream", r.pieces, r.elapsed)
	default:
		out.detail = fmt.Sprintf("%s, %d lines as they were sent over %v, trailer X-Lines=%s",
			r.proto, r.pieces, r.elapsed.Round(time.Millisecond), r.trailer.Get("X-Lines"))
	}
	return out
}

// relay opens a WebSocket through the gateway and has the upstream echo a text
// message and a large binary one, then closes it with a code of our own.
func relay(dialer *websocket.Dialer, url string) outcome {
	out := outcome{what: "websocket"}
	messages := make(chan []byte, 4)
	closed := make(chan string, 1)
	handler := websocket.HandlerFuncs{
		Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) {
			messages <- append([]byte(nil), data...)
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			closed <- fmt.Sprintf("code=%d reason=%q", code, reason)
		},
	}
	type dialed struct {
		conn *websocket.Connection
		err  error
	}
	result := make(chan dialed, 1)
	dialer.Dial(url, nil, handler, func(conn *websocket.Connection, _ *http.Response, err error) {
		result <- dialed{conn, err}
	})
	var conn *websocket.Connection
	select {
	case d := <-result:
		if d.err != nil {
			out.err = d.err
			return out
		}
		conn = d.conn
	case <-time.After(10 * time.Second):
		out.err = fmt.Errorf("dial timed out")
		return out
	}
	expect := func(want []byte) error {
		select {
		case got := <-messages:
			if !bytes.Equal(got, want) {
				return fmt.Errorf("echoed %d bytes, want %d", len(got), len(want))
			}
			return nil
		case <-time.After(10 * time.Second):
			return fmt.Errorf("no echo")
		}
	}
	text := []byte("hello through the gateway")
	big := bytes.Repeat([]byte("0123456789abcdef"), 64<<10) // 1 MiB
	if out.err = conn.WriteText(string(text)); out.err == nil {
		out.err = expect(text)
	}
	if out.err == nil {
		if out.err = conn.WriteBinary(big); out.err == nil {
			out.err = expect(big)
		}
	}
	if out.err != nil {
		return out
	}
	_ = conn.Close(4001, "bye")
	select {
	case how := <-closed:
		out.detail = fmt.Sprintf("subprotocol %q, text and %d KiB binary echoed, closed %s", conn.Subprotocol(), len(big)>>10, how)
	case <-time.After(5 * time.Second):
		out.err = fmt.Errorf("never closed")
	}
	return out
}
