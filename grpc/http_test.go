//go:build linux || darwin || windows

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/grpc/status"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// startHTTP serves the test service through package http, beside a plain
// route on the same port, the way a program puts gRPC and HTTP on one port.
func startHTTP(t *testing.T, impl *echoImpl, config fibhttp.Config, opts ...ServerOption) string {
	t.Helper()
	return startHandler(t, fibhttp.NewHandlerWithConfig(config, httpRouter(impl, opts...)))
}

func httpRouter(impl *echoImpl, opts ...ServerOption) *fibhttp.Router {
	s := NewServer(opts...)
	s.RegisterService(&echoDesc, impl)
	r := fibhttp.NewRouter()
	r.Handle("/test.Echo/*", s)
	r.Get("/", func(c *fibhttp.Context, _ *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("http"))
	})
	return r
}

// streamingConfig runs each handler as its request starts arriving, which a
// call whose client streams needs.
func streamingConfig() fibhttp.Config {
	config := fibhttp.DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0
	return config
}

func TestServeHTTPUnary(t *testing.T) {
	addr := startHTTP(t, &echoImpl{}, fibhttp.DefaultConfig())
	_, c := dial(t, addr)
	ctx := testCtx(t)
	r, err := c.Unary(ctx, &Req{Msg: "hello", N: 3})
	if err != nil || r.Msg != "hello" || r.N != 3 {
		t.Fatalf("Unary = %+v, %v", r, err)
	}
	_, err = c.Unary(ctx, &Req{Msg: "fail"})
	if s := status.Convert(err); s.Code() != codes.NotFound || s.Message() != "no such thing" {
		t.Fatalf("fail = %v", err)
	}
	_, err = c.Unary(ctx, &Req{Msg: "plain"})
	if s := status.Convert(err); s.Code() != codes.Unknown || s.Message() != "plain error" {
		t.Fatalf("plain error = %v", err)
	}
	if _, err = c.Unary(ctx, &Req{Msg: "panic"}); status.Code(err) != codes.Internal {
		t.Fatalf("panic = %v", err)
	}
	big := strings.Repeat("b", 3<<20)
	if r, err := c.Unary(ctx, &Req{Msg: big}); err != nil || r.Msg != big {
		t.Fatalf("a message over the window: %d bytes, %v", len(r.GetMsg()), err)
	}
	if r, err := c.Unary(ctx, &Req{Msg: "zipped " + strings.Repeat("z", 1000)}, UseCompressor("gzip")); err != nil || !strings.HasPrefix(r.Msg, "zipped") {
		t.Fatalf("gzip = %+v, %v", r, err)
	}
	cc, _ := dial(t, addr)
	if err := cc.Invoke(ctx, "/test.Echo/Nope", &Req{}, &Rsp{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown method = %v", err)
	}
}

func TestServeHTTPStreams(t *testing.T) {
	// A server stream needs nothing more than the request whole.
	_, c := dial(t, startHTTP(t, &echoImpl{}, fibhttp.DefaultConfig()))
	ctx := testCtx(t)
	count, err := c.Count(ctx, &Req{Msg: "c", N: 500})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		m, err := count.Recv()
		if err == io.EOF {
			if i != 500 {
				t.Fatalf("Count ended after %d", i)
			}
			break
		}
		if err != nil || m.N != i {
			t.Fatalf("Count %d = %+v, %v", i, m, err)
		}
	}

	// A client stream and a bidirectional one, with the request streamed, and
	// past what the pipe it crosses holds.
	_, c = dial(t, startHTTP(t, &echoImpl{}, streamingConfig()))
	sum, err := c.Sum(ctx)
	if err != nil {
		t.Fatal(err)
	}
	chunk := strings.Repeat("x", 30<<10)
	for i := 1; i <= 100; i++ {
		if err := sum.Send(&Req{Msg: chunk, N: i}); err != nil {
			t.Fatal(err)
		}
	}
	r, err := sum.CloseAndRecv()
	if err != nil || r.N != 5050 || len(r.Msg) != 100*len(chunk) {
		t.Fatalf("Sum = %+v, %v", r, err)
	}
	// A bidirectional call is answered when it ends, so its client sends
	// everything before it reads.
	chat, err := c.Chat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	words := []string{"a", "b", "c"}
	for i, w := range words {
		if err := chat.Send(&Req{Msg: w, N: i}); err != nil {
			t.Fatal(err)
		}
	}
	chat.CloseSend()
	for i, w := range words {
		m, err := chat.Recv()
		if err != nil || m.Msg != strings.ToUpper(w) || m.N != i {
			t.Fatalf("Chat %s = %+v, %v", w, m, err)
		}
	}
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatalf("Chat end = %v", err)
	}
}

func TestServeHTTPMetadata(t *testing.T) {
	_, c := dial(t, startHTTP(t, &echoImpl{}, fibhttp.DefaultConfig()))
	ctx := metadata.AppendToOutgoingContext(testCtx(t), "k", "v1", "K", "v2", "k-bin", "\x01\x02")
	var header, trailer metadata.MD
	r, err := c.Unary(ctx, &Req{Msg: "meta"}, Header(&header), Trailer(&trailer))
	if err != nil {
		t.Fatal(err)
	}
	if r.Msg != "v1,v2|[0102]|/test.Echo/Unary|true" {
		t.Fatalf("the server saw %q", r.Msg)
	}
	if fmt.Sprint(header.Get("h")) != "[header]" || fmt.Sprint(trailer.Get("t")) != "[trailer]" || fmt.Sprint(trailer.Get("t-bin")) != "[\x00\xff]" {
		t.Fatalf("header %v trailer %v", header, trailer)
	}
}

func TestServeHTTPDeadlineAndCancel(t *testing.T) {
	impl := &echoImpl{ctxDone: make(chan error, 1)}
	_, c := dial(t, startHTTP(t, impl, fibhttp.DefaultConfig()))
	ctx, cancel := context.WithTimeout(testCtx(t), 100*time.Millisecond)
	defer cancel()
	if _, err := c.Unary(ctx, &Req{Msg: "sleep"}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline = %v", err)
	}
	ctx, cancel = context.WithCancel(testCtx(t))
	done := make(chan error, 1)
	go func() {
		_, err := c.Unary(ctx, &Req{Msg: "watch"})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("canceled call = %v", err)
	}
	select {
	case err := <-impl.ctxDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the handler's context ended with %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the handler's context outlived the call")
	}
}

// The port answers HTTP as well, and turns away what is not a gRPC call as
// grpc-go does.
func TestServeHTTPBesideHTTP(t *testing.T) {
	addr := startHTTP(t, &echoImpl{}, fibhttp.DefaultConfig())
	var h1, h2 stdhttp.Protocols
	h1.SetHTTP1(true)
	h2.SetUnencryptedHTTP2(true)
	for _, tc := range []struct {
		protocols *stdhttp.Protocols
		method    string
		ctype     string
		status    int
	}{
		{&h1, "GET", "", 200},
		{&h2, "GET", "", 200},
		{&h1, "POST", "application/grpc", 400},
		{&h2, "GET", "application/grpc", 405},
		{&h2, "POST", "application/json", 415},
	} {
		client := &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: tc.protocols}}
		path := "/test.Echo/Unary"
		if tc.ctype == "" {
			path = "/"
		}
		req, _ := stdhttp.NewRequest(tc.method, "http://"+addr+path, nil)
		if tc.ctype != "" {
			req.Header.Set("Content-Type", tc.ctype)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		client.CloseIdleConnections()
		if resp.StatusCode != tc.status || tc.status == 200 && string(body) != "http" {
			t.Errorf("%s %s %q: %d %q, want %d", resp.Proto, tc.method, tc.ctype, resp.StatusCode, body, tc.status)
		}
	}
}

// Over TLS, one listener offering h2 serves gRPC and HTTP/2 alike.
func TestServeHTTPOverTLS(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	handler := fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(httpRouter(&echoImpl{})))
	addr := startHandler(t, fib.Handler(handler))
	_, c := dial(t, addr, WithTLSConfig(clientTLS))
	if r, err := c.Unary(testCtx(t), &Req{Msg: "secure"}); err != nil || r.Msg != "secure" {
		t.Fatalf("over TLS = %+v, %v", r, err)
	}
}
