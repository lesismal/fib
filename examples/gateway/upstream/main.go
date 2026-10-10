//go:build linux || darwin || windows

// Command upstream is the backend the gateway example forwards to. It speaks
// HTTP/1.1 on a cleartext port, and HTTP/2 and HTTP/3 on a TLS one, so that
// the gateway can be shown reaching an upstream over each protocol.

// 	go run ./examples/gateway/upstream

// It serves:

// 	/echo       the request's protocol, method, path, query, forwarding headers and body
// 	/download   ?size=N bytes, with a Content-Length
// 	/stream     ?n=5&delay=200ms lines, one at a time, then a trailer
// 	/upload     counts the request body
// 	/status     ?code=N answers with that status
// 	/ws         a WebSocket echo server, which closes with code 4002 when sent "close"

// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the gateway looks for it.
package main

import (
	"flag"
	"fmt"

	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/gateway/upstream/backend"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9443", "TLS listen address: TCP for HTTP/2 and HTTP/1.1, UDP for HTTP/3")
	plainAddr := flag.String("plain-addr", "127.0.0.1:9000", "cleartext HTTP/1.1 and ws:// listen address; empty to disable")
	certFlags := certs.RegisterServerFlags()
	flag.Parse()

	tlsConfig, err := certFlags.Config()
	if err != nil {
		example.Fatal(err)
	}
	engines, err := backend.NewEngines(tlsConfig, *addr, *plainAddr)
	if err != nil {
		example.Fatal(err)
	}
	for _, engine := range engines[1:] {
		go func() {
			if err := engine.Run(); err != nil {
				example.Fatal(err)
			}
		}()
		defer engine.Close()
		defer engine.Stop()
	}
	example.Serve(engines[0], fmt.Sprintf("upstream listening on https://%s (HTTP/2, HTTP/1.1; HTTP/3 on UDP) and http://%s", *addr, *plainAddr))
}
