//go:build linux || darwin || windows

// Package mwtest serves a handler for the middleware's tests, and gives them
// clients that reach it over HTTP/1.1 and over HTTP/2.
package mwtest

import (
	"io"
	stdhttp "net/http"
	"strings"
	"testing"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

// Serve serves handler until the test ends, and returns its base URL.
func Serve(t testing.TB, handler fibhttp.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, fibhttp.NewHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return "http://" + addr.String()
}

// Client is a client over one protocol.
type Client struct {
	Name string
	*stdhttp.Client
}

// Clients are an HTTP/1.1 client and an HTTP/2 one, which speaks it without
// TLS from the first byte. Neither asks for compressed responses unless a
// test does, nor decompresses them.
func Clients(t testing.TB) []Client {
	var h1, h2 stdhttp.Protocols
	h1.SetHTTP1(true)
	h2.SetUnencryptedHTTP2(true)
	clients := []Client{
		{"HTTP/1.1", &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: &h1, DisableCompression: true}}},
		{"HTTP/2", &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: &h2, DisableCompression: true}}},
	}
	for _, c := range clients {
		t.Cleanup(c.CloseIdleConnections)
		c.CheckRedirect = func(*stdhttp.Request, []*stdhttp.Request) error { return stdhttp.ErrUseLastResponse }
	}
	return clients
}

// Do sends a request, and returns the response with its body read.
func (c Client) Do(t testing.TB, method, url string, header stdhttp.Header, body string) (*stdhttp.Response, string) {
	t.Helper()
	req, err := stdhttp.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for key, values := range header {
		req.Header[key] = values
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		t.Fatalf("%s %s over %s: %v", method, url, c.Name, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s over %s: reading body: %v", method, url, c.Name, err)
	}
	return resp, string(data)
}

// Run runs test once for each client.
func Run(t *testing.T, test func(t *testing.T, c Client)) {
	for _, c := range Clients(t) {
		t.Run(strings.ReplaceAll(c.Name, "/", ""), func(t *testing.T) { test(t, c) })
	}
}
