//go:build linux || darwin || windows

// Command server takes large request bodies over HTTP/3 without holding them
// in memory: the upload example's /upload, /resume and /echo (see package
// service), served over QUIC. A streamed HTTP/3 body is paced by QUIC's flow
// control, which gives the client room for more only as the handler consumes
// what it has, so memory stays flat however large the body is.
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the client looks for it.
//
//	go run ./examples/http/upload/mkfile -size 256MiB -o /tmp/big.bin
//	go run ./examples/http3/upload/server -dir /tmp/uploads
//	go run ./examples/http3/upload/client -mode resume -file /tmp/big.bin
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
	"github.com/lesismal/fib/http3"
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
	addr := flags.String("addr", "127.0.0.1:8445", "listen address (UDP)")
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
	// Bind has opened the socket, so there is an address to report.
	local, _ := engine.LocalUDPAddr()
	running := make(chan error, 1)
	go func() { running <- engine.Run() }()
	fmt.Fprintf(out, "HTTP/3 upload server listening on https://%s (UDP), storing into %s\n", local, *dir)
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
	config := http3.DefaultConfig()
	config.TLSConfig = tlsConfig
	// A request goes to its handler once the header is in, whatever its
	// body's size, and the body is bounded by MaxStreamedBodyBytes.
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0
	config.MaxStreamedBodyBytes = maxSize
	engineConfig := fib.DefaultConfig()
	engineConfig.Network = "udp"
	engineConfig.Addr = addr
	return fib.Bind(engineConfig, http3.NewHandlerWithConfig(config, service.Handler(dir)))
}
