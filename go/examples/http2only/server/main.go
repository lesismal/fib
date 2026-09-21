//go:build linux || darwin || windows

// Command server is an HTTP/2-only echo server: it answers each request with
// its protocol, method, path and body, like examples/http/nontls and
// examples/http/tls, but never falls back to HTTP/1 — a connection that does
// not open with the HTTP/2 preface is ended with GOAWAY. That suits a peer
// that only ever speaks HTTP/2, including conformance tools such as h2spec,
// which expect an HTTP/2-only server to behave this way.
//
//	go run ./examples/http2only/server                  # h2c, cleartext
//	go run ./examples/http2only/server -tls -addr :8443  # h2, over TLS
//
// Without -cert and -key, -tls issues a self-signed certificate for
// localhost and writes it where the TLS example client looks for it.
package main

import (
	"flag"
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/examples/internal/certs"
	"github.com/lesismal/fib/go/examples/internal/example"
	fibhttp "github.com/lesismal/fib/go/http"
	fibtls "github.com/lesismal/fib/go/tls"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	useTLS := flag.Bool("tls", false, "serve HTTP/2 over TLS (h2) instead of cleartext (h2c)")
	certFlags := certs.RegisterServerFlags()
	flag.Parse()

	httpConfig := fibhttp.DefaultConfig()
	httpConfig.HTTP2Only = true
	handler := fibhttp.NewHandlerWithConfig(httpConfig, echo())

	config := fib.DefaultConfig()
	config.Addr = *addr

	scheme, engine, err := "http", (*fib.Engine)(nil), error(nil)
	if *useTLS {
		scheme = "https"
		tlsConfig, cfgErr := certFlags.Config()
		if cfgErr != nil {
			example.Fatal(cfgErr)
		}
		engine, err = fib.Bind(config, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), handler))
	} else {
		engine, err = fib.Bind(config, handler)
	}
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("HTTP/2-only echo server listening on %s://%s", scheme, *addr))
}

func echo() fibhttp.HandlerFunc {
	return func(c *fibhttp.Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		reply := fmt.Sprintf("%s %s %s %s", r.Proto, r.Method, r.URL.Path, body)
		if err := c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(reply)); err != nil {
			c.Conn.Close()
		}
	}
}
