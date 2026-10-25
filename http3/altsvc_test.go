//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"fmt"
	stdhttp "net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func TestParseAltSvc(t *testing.T) {
	for _, tc := range []struct {
		value             string
		protocol          string
		authority, params string
		ok                bool
	}{
		{`h3=":443"`, "h3", ":443", "", true},
		{`h3="example.com:8443"; ma=3600`, "h3", "example.com:8443", "; ma=3600", true},
		{`h3-29=":443"`, "h3-29", ":443", "", true},
		{`H3=":443"`, "h3", ":443", "", true},
		{`h3=:443`, "", "", "", false},
		{`h3=":443`, "", "", "", false},
		{`=:443`, "", "", "", false},
	} {
		protocol, authority, params, ok := parseAltSvc(tc.value)
		if ok != tc.ok || protocol != tc.protocol || authority != tc.authority || params != tc.params {
			t.Errorf("parseAltSvc(%q) = %q, %q, %q, %v; want %q, %q, %q, %v",
				tc.value, protocol, authority, params, ok, tc.protocol, tc.authority, tc.params, tc.ok)
		}
	}
	for _, tc := range []struct {
		params string
		want   time.Duration
	}{
		{"", 24 * time.Hour},
		{"; ma=3600", time.Hour},
		{"; ma=0", 0},
		{"; persist=1; ma=60", time.Minute},
		{"; ma=-1", 0},
	} {
		if got := altSvcMaxAge(tc.params); got != tc.want {
			t.Errorf("altSvcMaxAge(%q) = %v, want %v", tc.params, got, tc.want)
		}
	}
	for _, tc := range []struct {
		authority, origin, want string
		ok                      bool
	}{
		{":443", "example.com", "example.com:443", true},
		{"example.com:8443", "example.com", "example.com:8443", true},
		{"other.example.com:443", "example.com", "other.example.com:443", true},
		{"example.com", "example.com", "", false},
		{":", "example.com", "", false},
	} {
		got, ok := altSvcAddr(tc.authority, tc.origin)
		if ok != tc.ok || got != tc.want {
			t.Errorf("altSvcAddr(%q, %q) = %q, %v; want %q, %v", tc.authority, tc.origin, got, ok, tc.want, tc.ok)
		}
	}
}

func altsvcRequest(t *testing.T, target string) *stdhttp.Request {
	t.Helper()
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestAltSvcRemember(t *testing.T) {
	a := &AltSvcClient{alts: make(map[string]altSvcEntry)}
	req := altsvcRequest(t, "https://example.com/path")
	header := stdhttp.Header{"Alt-Svc": {`h3=":8443"; ma=60, h3-29=":9443", h2=":443"`}}
	a.remember(req, header)
	e, ok := a.alts["https://example.com:443"]
	if !ok {
		t.Fatal("the h3 alternative was not remembered")
	}
	if e.addr != "example.com:8443" || e.serverName != "example.com" {
		t.Fatalf("remembered %+v", e)
	}
	if until := time.Until(e.until); until > time.Minute || until < 55*time.Second {
		t.Fatalf("max-age gave %v", until)
	}
	// A plain host takes the origin's own host and its port from the header.
	a.remember(req, stdhttp.Header{"Alt-Svc": {`h3="other.example.com:8443"`}})
	if e := a.alts["https://example.com:443"]; e.addr != "other.example.com:8443" || e.serverName != "example.com" {
		t.Fatalf("cross-host alternative remembered %+v", e)
	}
	// ma=0 clears it, as does clear.
	a.remember(req, stdhttp.Header{"Alt-Svc": {`h3=":8443"; ma=0`}})
	if _, ok := a.alts["https://example.com:443"]; ok {
		t.Fatal("ma=0 did not clear the alternative")
	}
	a.remember(req, stdhttp.Header{"Alt-Svc": {`h3=":8443"`}})
	a.remember(req, stdhttp.Header{"Alt-Svc": {`clear`}})
	if _, ok := a.alts["https://example.com:443"]; ok {
		t.Fatal("clear did not clear the alternative")
	}
	// An http origin has no HTTP/3 alternative.
	a.remember(altsvcRequest(t, "http://example.com/path"), stdhttp.Header{"Alt-Svc": {`h3=":8443"`}})
	if len(a.alts) != 0 {
		t.Fatalf("an insecure origin got an alternative: %v", a.alts)
	}
}

// tcpAdvertisingServer starts an HTTPS server whose responses advertise
// altPort as this origin's HTTP/3 alternative, and counts its requests.
func tcpAdvertisingServer(t *testing.T, altPort int, seen *atomic.Int64) string {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	handler := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		seen.Add(1)
		_ = c.WriteResponse(fibhttp.Response{
			StatusCode: 200,
			Header:     stdhttp.Header{"Alt-Svc": {AltSvc(altPort)}, "Content-Type": {"text/plain"}},
			Body:       []byte("tcp"),
		})
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:0",
		Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(handler))})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("https://127.0.0.1:%d", addr.Port)
}

func newAltSvcClient(t *testing.T, h3Config func(*ClientConfig)) *AltSvcClient {
	t.Helper()
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	tcpTLS := clientTLS.Clone()
	tcpTLS.ServerName = "localhost"
	tcpClient := fibhttp.NewClient(engine, fibhttp.ClientConfig{TLSConfig: tcpTLS, DisableHTTP2: true})

	h3TLS := clientTLS.Clone()
	h3TLS.ServerName = "localhost"
	config := DefaultClientConfig()
	config.TLSConfig = h3TLS
	if h3Config != nil {
		h3Config(&config)
	}
	client := NewAltSvcClient(tcpClient, NewClient(engine, config))
	t.Cleanup(client.Close)
	return client
}

// TestAltSvcClientSwitchesToHTTP3 has a response's Alt-Svc header move the
// requests after it to HTTP/3, with the origin's authority still what the
// server sees.
func TestAltSvcClientSwitchesToHTTP3(t *testing.T) {
	var tcpSeen, h3Seen atomic.Int64
	var authority atomic.Value
	h3URL := startServer(t, Config{}, func(c *fibhttp.Context) {
		h3Seen.Add(1)
		authority.Store(c.Request.Host)
		_ = c.Respond(200, "text/plain", []byte("h3"))
	})
	u, err := url.Parse(h3URL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	tcpURL := tcpAdvertisingServer(t, port, &tcpSeen)
	origin, err := url.Parse(tcpURL)
	if err != nil {
		t.Fatal(err)
	}
	client := newAltSvcClient(t, nil)

	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/first", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "tcp" {
		t.Fatalf("first response %q", body)
	}
	if tcpSeen.Load() != 1 || h3Seen.Load() != 0 {
		t.Fatalf("the first request: TCP %d, HTTP/3 %d", tcpSeen.Load(), h3Seen.Load())
	}
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/second", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "h3" {
		t.Fatalf("second response %q, want it over HTTP/3", body)
	}
	if tcpSeen.Load() != 1 || h3Seen.Load() != 1 {
		t.Fatalf("requests: TCP %d, HTTP/3 %d", tcpSeen.Load(), h3Seen.Load())
	}
	if got, _ := authority.Load().(string); got != origin.Host {
		t.Fatalf("HTTP/3 request authority %q, want the origin's %q", got, origin.Host)
	}
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/third", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	_ = readBody(t, resp)
	if tcpSeen.Load() != 1 || h3Seen.Load() != 2 {
		t.Fatalf("requests: TCP %d, HTTP/3 %d", tcpSeen.Load(), h3Seen.Load())
	}
}

// TestAltSvcClientFallsBackToTCP has a failed HTTP/3 attempt fall back to TCP
// for a request that is safe to repeat, and leave the origin on TCP rather
// than retry the handshake for each request.
func TestAltSvcClientFallsBackToTCP(t *testing.T) {
	var tcpSeen atomic.Int64
	// Port 1 has no HTTP/3 server, so the QUIC handshake cannot finish.
	tcpURL := tcpAdvertisingServer(t, 1, &tcpSeen)
	client := newAltSvcClient(t, func(config *ClientConfig) {
		config.HandshakeTimeout = 300 * time.Millisecond
		config.DialTimeout = time.Second
	})
	if resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/first", nil)).Wait(); err != nil {
		t.Fatal(err)
	} else {
		_ = readBody(t, resp)
	}
	start := time.Now()
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/second", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "tcp" {
		t.Fatalf("fallback response %q", body)
	}
	if tcpSeen.Load() != 2 {
		t.Fatalf("the TCP server saw %d requests, want 2", tcpSeen.Load())
	}
	// The next request is on TCP straight away: the origin is backed off.
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, tcpURL+"/third", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	_ = readBody(t, resp)
	if tcpSeen.Load() != 3 {
		t.Fatalf("the TCP server saw %d requests, want 3", tcpSeen.Load())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the requests after the failure took %v; the origin was not backed off", elapsed)
	}
}
