//go:build linux || darwin || windows

// Command server takes large request bodies over HTTP/2 without holding them
// in memory: the upload example's /upload, /resume and /echo (see package
// service), served over TLS to clients that negotiate h2. A streamed HTTP/2
// body is paced by the stream's flow-control window, which the server gives
// back only as the handler consumes the body, so the sender slows down and
// memory stays flat however large the body is.
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
//	go run ./examples/http2/upload/server -dir /tmp/uploads
//	go run ./examples/http2/upload/client -mode upload -file /tmp/big.bin
//	go run ./examples/http2/upload/client -mode resume -file /tmp/big.bin -name part.bin -max-chunks 3
//	go run ./examples/http2/upload/client -mode resume -file /tmp/big.bin -name part.bin
//	go run ./examples/http2/upload/client -mode echo   -file /tmp/big.bin
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/http/upload/service"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
)

// fatal reports a startup error and exits; a test replaces it.
var fatal = example.Fatal

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, nil); err != nil {
		fatal(err)
	}
}

// run serves until ctx ends. ready, if not nil, is told the address once the
// server is listening.
func run(ctx context.Context, args []string, out io.Writer, ready func(addr string)) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(out)
	addr := flags.String("addr", "127.0.0.1:8443", "listen address")
	dir := flags.String("dir", "uploads", "directory the uploads are stored in")
	maxSize := flags.Int64("max", 64<<30, "largest body accepted, in bytes (0 for no limit)")
	certFlags := certs.ServerFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	engine, err := newEngine(*addr, *dir, *maxSize, tlsConfig)
	if err != nil {
		return err
	}
	// Bind has opened the listener, so there is an address to report.
	local, _ := engine.LocalAddr()
	running := make(chan error, 1)
	go func() { running <- engine.Run() }()
	fmt.Fprintf(out, "HTTP/2 upload server listening on https://%s, storing into %s\n", local, *dir)
	if ready != nil {
		ready(local.String())
	}
	<-ctx.Done()
	engine.Stop()
	err = <-running
	_ = engine.Close()
	return err
}

// newEngine binds the server to addr, storing into dir and refusing bodies
// larger than maxSize (zero for no limit).
func newEngine(addr, dir string, maxSize int64, tlsConfig *tls.Config) (*fib.Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	httpConfig := service.Config(maxSize)
	// Every connection is HTTP/2: one that does not open with its preface is
	// ended with GOAWAY rather than answered in HTTP/1.
	httpConfig.HTTP2Only = true
	config := fib.DefaultConfig()
	config.Addr = addr
	// fibtls decrypts in front of the HTTP handler; ConfigureTLS offers h2
	// through ALPN.
	return fib.Bind(config, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandlerWithConfig(httpConfig, service.Handler(dir))))
}
