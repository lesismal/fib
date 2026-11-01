//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestAltSvcClientPieces checks the parts of AltSvcClient a working pair of
// clients does not reach: missing clients, a Cancel that comes before there
// is anything to cancel, which requests may be repeated over TCP, and
// headers that name nothing to remember.
func TestAltSvcClientPieces(t *testing.T) {
	// No TCP client: the request goes nowhere.
	var empty AltSvcClient
	var got error
	empty.Do(altsvcRequest(t, "https://example.com/"), func(_ *fibhttp.ClientResponse, err error) { got = err })
	if got != fibhttp.ErrClientClosed {
		t.Fatalf("a client without a TCP client: %v", got)
	}
	empty.Close()
	if _, ok := empty.route(altsvcRequest(t, "https://example.com/")); ok {
		t.Fatal("a client without an h3 client routed a request to it")
	}
	empty.delivered(nil, nil)
	empty.delivered(nil, &fibhttp.ClientResponse{})

	// The zero AltSvcCall cancels nothing; a Cancel before the request has
	// a client reaches it as soon as it has one, with ErrClientClosed for nil.
	AltSvcCall{}.Cancel(nil)
	call := &altSvcCall{}
	AltSvcCall{c: call}.Cancel(nil)
	if !call.ended() {
		t.Fatal("Cancel did not end the call")
	}
	var held error
	call.hold(func(err error) { held = err })
	if held != fibhttp.ErrClientClosed {
		t.Fatalf("a late hold heard %v", held)
	}

	for _, tc := range []struct {
		name string
		req  func() *stdhttp.Request
		want bool
	}{
		{"nil", func() *stdhttp.Request { return nil }, false},
		{"no URL", func() *stdhttp.Request { return &stdhttp.Request{} }, false},
		{"GET", func() *stdhttp.Request { return altsvcRequest(t, "https://example.com/") }, true},
		{"POST", func() *stdhttp.Request {
			return mustRequest(t, stdhttp.MethodPost, "https://example.com/", nil)
		}, false},
		{"GET with a body read again", func() *stdhttp.Request {
			return mustRequest(t, stdhttp.MethodGet, "https://example.com/", strings.NewReader("x"))
		}, true},
		{"GET with a body read once", func() *stdhttp.Request {
			req := mustRequest(t, stdhttp.MethodGet, "https://example.com/", nil)
			req.Body = io.NopCloser(strings.NewReader("x"))
			return req
		}, false},
		{"GET with a body that cannot be read again", func() *stdhttp.Request {
			req := mustRequest(t, stdhttp.MethodGet, "https://example.com/", strings.NewReader("x"))
			req.GetBody = func() (io.ReadCloser, error) { return nil, errors.New("gone") }
			return req
		}, false},
	} {
		if got := empty.fallback(tc.req()); got != tc.want {
			t.Errorf("fallback(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}

	if key := originKey(altsvcRequest(t, "https:///path")); key != "" {
		t.Fatalf("a URL without a host has origin %q", key)
	}
	if _, ok := (&AltSvcClient{h3: &Client{}}).route(altsvcRequest(t, "http://example.com/")); ok {
		t.Fatal("an http request was routed to HTTP/3")
	}
	for _, tc := range []struct {
		authority, want string
		ok              bool
	}{
		{"[::1", "", false},
		{"[]:443", "example.com:443", true},
		{"example.com:", "", false},
	} {
		got, ok := altSvcAddr(tc.authority, "example.com")
		if ok != tc.ok || got != tc.want {
			t.Errorf("altSvcAddr(%q) = %q, %v; want %q, %v", tc.authority, got, ok, tc.want, tc.ok)
		}
	}

	// Headers with nothing for this origin leave it as it was, and a fresh
	// advertisement keeps the backoff of a failed one.
	a := &AltSvcClient{alts: make(map[string]altSvcEntry)}
	req := altsvcRequest(t, "https://example.com/")
	a.remember(req, stdhttp.Header{})
	a.remember(req, stdhttp.Header{"Alt-Svc": {` , h2=":443", h3="example.com", h3=:1`}})
	if len(a.alts) != 0 {
		t.Fatalf("remembered %v", a.alts)
	}
	retryAt := time.Now().Add(time.Hour)
	a.alts["https://example.com:443"] = altSvcEntry{retryAt: retryAt}
	a.remember(req, stdhttp.Header{"Alt-Svc": {`h3=":8443"`}})
	if e := a.alts["https://example.com:443"]; !e.retryAt.Equal(retryAt) || e.addr != "example.com:8443" {
		t.Fatalf("re-advertised %+v", e)
	}
}

type altsvcOutcome struct {
	resp *fibhttp.ClientResponse
	err  error
}

func altsvcCallback(ch chan altsvcOutcome) func(*fibhttp.ClientResponse, error) {
	return func(resp *fibhttp.ClientResponse, err error) { ch <- altsvcOutcome{resp, err} }
}

func altsvcAwait(t *testing.T, ch chan altsvcOutcome) altsvcOutcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(csWait):
		t.Fatal("no response")
		return altsvcOutcome{}
	}
}

// altsvcAdvertise has client send the requests for origin's URL to addr over
// HTTP/3, as if a response had advertised it.
func altsvcAdvertise(t *testing.T, client *AltSvcClient, origin, addr string) {
	t.Helper()
	client.mu.Lock()
	client.alts[originKey(altsvcRequest(t, origin))] = altSvcEntry{addr: addr, serverName: "localhost",
		until: time.Now().Add(time.Hour)}
	client.mu.Unlock()
}

// TestAltSvcClientFallbackPaths sends each kind of request to an origin whose
// HTTP/3 client is closed, so that its attempt fails at once: those that are
// safe to repeat go over TCP, through Do, Start and Go alike, and the rest
// fail with the HTTP/3 client's error.
func TestAltSvcClientFallbackPaths(t *testing.T) {
	var tcpSeen atomic.Int64
	tcpURL := tcpAdvertisingServer(t, 1, &tcpSeen)
	client := newAltSvcClient(t, nil)
	client.h3.Close()

	altsvcAdvertise(t, client, tcpURL, "127.0.0.1:1")
	ch := make(chan altsvcOutcome, 1)
	client.Do(mustRequest(t, stdhttp.MethodGet, tcpURL+"/do", nil), altsvcCallback(ch))
	if o := altsvcAwait(t, ch); o.err != nil || readBody(t, o.resp.Response) != "tcp" {
		t.Fatalf("Do fell back to %v, %v", o.resp, o.err)
	}

	altsvcAdvertise(t, client, tcpURL, "127.0.0.1:1")
	ch = make(chan altsvcOutcome, 1)
	call := client.Start(mustRequest(t, stdhttp.MethodGet, tcpURL+"/start", strings.NewReader("again")), altsvcCallback(ch))
	if o := altsvcAwait(t, ch); o.err != nil || readBody(t, o.resp.Response) != "tcp" {
		t.Fatalf("Start fell back to %v, %v", o.resp, o.err)
	}
	call.Cancel(nil)

	altsvcAdvertise(t, client, tcpURL, "127.0.0.1:1")
	ch = make(chan altsvcOutcome, 1)
	client.Do(mustRequest(t, stdhttp.MethodPost, tcpURL+"/post", strings.NewReader("once")), altsvcCallback(ch))
	if o := altsvcAwait(t, ch); o.err != ErrClientClosed {
		t.Fatalf("a POST whose HTTP/3 attempt failed: %v, %v", o.resp, o.err)
	}
	if n := tcpSeen.Load(); n != 2 {
		t.Fatalf("the TCP server saw %d requests, want 2", n)
	}

	f := client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/go", nil))
	select {
	case <-f.Done():
	case <-time.After(csWait):
		t.Fatal("Go never finished")
	}
	if resp, err := f.Wait(); err != nil || readBody(t, resp) != "tcp" {
		t.Fatalf("Go: %v, %v", resp, err)
	}

	// StartUpgrade is the TCP client's, which refuses one without a handler.
	ch = make(chan altsvcOutcome, 1)
	client.StartUpgrade(mustRequest(t, stdhttp.MethodGet, tcpURL+"/upgrade", nil), nil, altsvcCallback(ch))
	if o := altsvcAwait(t, ch); o.err == nil {
		t.Fatal("an upgrade without a handler was sent")
	}
}

// TestAltSvcClientTCPOnly has a client without an HTTP/3 one send everything
// over TCP, and Go hear the TCP client's refusal once it is closed.
func TestAltSvcClientTCPOnly(t *testing.T) {
	var tcpSeen atomic.Int64
	tcpURL := tcpAdvertisingServer(t, 1, &tcpSeen)
	client := NewAltSvcClient(newAltSvcClient(t, nil).tcp, nil)
	for range 2 {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/", nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		_ = readBody(t, resp)
	}
	if n := tcpSeen.Load(); n != 2 {
		t.Fatalf("the TCP server saw %d requests, want 2", n)
	}
	client.Close()
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/", nil)).Wait(); err == nil {
		t.Fatal("a closed TCP client sent a request")
	}
}

// TestAltSvcClientCancelOverHTTP3 cancels a request whose HTTP/3 handshake
// has no answer: the callback hears the cancellation, and the request is not
// sent again over TCP.
func TestAltSvcClientCancelOverHTTP3(t *testing.T) {
	var tcpSeen atomic.Int64
	tcpURL := tcpAdvertisingServer(t, 1, &tcpSeen)
	// A UDP socket that never answers holds the handshake open.
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = silent.Close() })
	client := newAltSvcClient(t, nil)
	altsvcAdvertise(t, client, tcpURL, silent.LocalAddr().String())
	done := make(chan error, 1)
	call := client.Start(mustRequest(t, stdhttp.MethodGet, tcpURL+"/", nil), func(_ *fibhttp.ClientResponse, err error) { done <- err })
	cause := errors.New("abandoned")
	call.Cancel(cause)
	select {
	case err := <-done:
		if err != cause {
			t.Fatalf("the cancelled request heard %v", err)
		}
	case <-time.After(csWait):
		t.Fatal("the cancelled request never finished")
	}
	if n := tcpSeen.Load(); n != 0 {
		t.Fatalf("the cancelled request reached the TCP server %d times", n)
	}
}
