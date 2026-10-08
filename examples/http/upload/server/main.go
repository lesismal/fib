//go:build linux || darwin || windows

// Command server takes large HTTP/1 request bodies without holding them in
// memory. With Config.StreamRequestBody on, a handler runs as soon as the
// request header has arrived, and takes the body piece by piece through
// Context.OnBody as the connection reads it; while the handler's callback is
// busy the connection stops reading, and TCP flow control slows the sender, so
// memory stays flat however large the body is. The endpoints (/upload,
// /resume and /echo) are the shared service package's; examples/http2/upload
// and examples/http3/upload serve the same ones over HTTP/2 and HTTP/3.
//
// Try it with the client and the test file generator beside it:
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
//	go run ./examples/http/upload/server -dir /tmp/uploads
//	go run ./examples/http/upload/client -mode upload -file /tmp/big.bin
//	go run ./examples/http/upload/client -mode resume -file /tmp/big.bin -name part.bin -max-chunks 3
//	go run ./examples/http/upload/client -mode resume -file /tmp/big.bin -name part.bin
//	go run ./examples/http/upload/client -mode echo   -file /tmp/big.bin
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/http/upload/service"
	fibhttp "github.com/lesismal/fib/http"
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
	addr := flags.String("addr", "127.0.0.1:8080", "listen address")
	dir := flags.String("dir", "uploads", "directory the uploads are stored in")
	maxSize := flags.Int64("max", 64<<30, "largest body accepted, in bytes (0 for no limit)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	engine, err := newEngine(*addr, *dir, *maxSize)
	if err != nil {
		return err
	}
	// Bind has opened the listener, so there is an address to report.
	local, _ := engine.LocalAddr()
	running := make(chan error, 1)
	go func() { running <- engine.Run() }()
	fmt.Fprintf(out, "upload server listening on http://%s, storing into %s\n", local, *dir)
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
func newEngine(addr, dir string, maxSize int64) (*fib.Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	config := fib.DefaultConfig()
	config.Addr = addr
	return fib.Bind(config, fibhttp.NewHandlerWithConfig(service.Config(maxSize), service.Handler(dir)))
}
