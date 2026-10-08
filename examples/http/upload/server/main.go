//go:build linux || darwin || windows

// Command server takes large HTTP/1 request bodies without holding them in
// memory. With Config.StreamRequestBody on, a handler runs as soon as the
// request header has arrived, and takes the body piece by piece through
// Context.OnBody as the connection reads it; while the handler's callback is
// busy the connection stops reading, and TCP flow control slows the sender, so
// memory stays flat however large the body is. It serves three endpoints:
//
//	POST|PUT   /upload?name=F   Context.SaveBody: the whole file in one request
//	PUT|PATCH  /resume?name=F   Context.SaveBodyResumable: the file in chunks,
//	                            each with a Content-Range, and continued after a
//	                            dropped connection from the offset the server
//	                            reports (Content-Range: bytes */TOTAL asks)
//	POST       /echo            answers with the body it receives, as it receives it
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
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
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
	httpConfig := fibhttp.DefaultConfig()
	// Hand a request to its handler once the header is in, whatever the
	// body's size, and bound the body by MaxStreamedBodyBytes, which is for
	// bodies that stream, rather than MaxBodyBytes, which is for those held whole.
	httpConfig.StreamRequestBody = true
	httpConfig.StreamRequestBodyThreshold = 0
	httpConfig.MaxStreamedBodyBytes = maxSize
	// How much of the body may wait unread before the connection stops
	// reading its socket. Small, to show that memory stays bounded.
	httpConfig.StreamRequestBodyBuffer = 1 << 20
	httpConfig.IdleTimeout = time.Minute

	config := fib.DefaultConfig()
	config.Addr = addr
	return fib.Bind(config, fibhttp.NewHandlerWithConfig(httpConfig, handler(dir)))
}

func handler(dir string) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context, r *http.Request) {
		switch r.URL.Path {
		case "/upload":
			if allow(c, r, http.MethodPost, http.MethodPut) {
				if path, ok := target(c, dir); ok {
					c.SaveBody(path, logged(c, r))
				}
			}
		case "/resume":
			if allow(c, r, http.MethodPut, http.MethodPatch) {
				if path, ok := target(c, dir); ok {
					c.SaveBodyResumable(path, logged(c, r))
				}
			}
		case "/echo":
			if allow(c, r, http.MethodPost) {
				echo(c)
			}
		default:
			_ = c.Respond(http.StatusNotFound, "text/plain; charset=utf-8", []byte("POST /upload, PUT /resume or POST /echo\n"))
		}
	}
}

// allow answers 405 unless the request's method is one of methods.
func allow(c *fibhttp.Context, r *http.Request, methods ...string) bool {
	for _, method := range methods {
		if r.Method == method {
			return true
		}
	}
	_ = c.Respond(http.StatusMethodNotAllowed, "text/plain; charset=utf-8", []byte("method not allowed\n"))
	return false
}

// target is the file the request's name parameter stands for, inside dir. It
// answers 400 if there is no name. Base keeps a name from reaching outside dir.
func target(c *fibhttp.Context, dir string) (string, bool) {
	name := filepath.Base(c.Query("name"))
	if name == "." || name == string(filepath.Separator) {
		_ = c.Respond(http.StatusBadRequest, "text/plain; charset=utf-8", []byte("name is required\n"))
		return "", false
	}
	return filepath.Join(dir, name), true
}

// logged is the done callback of the Save functions here: it logs how the
// upload ended, and then answers it as they would by themselves.
func logged(c *fibhttp.Context, r *http.Request) func(fibhttp.Saved, error) {
	started := time.Now()
	return func(saved fibhttp.Saved, err error) {
		if err != nil {
			log.Printf("%s %s: %d of %d bytes stored: %v", r.Method, r.URL.Path, saved.Size, saved.Total, err)
		} else {
			log.Printf("%s %s: %d bytes stored in %v, complete: %v", r.Method, r.URL.Path, saved.Size,
				time.Since(started).Round(time.Millisecond), saved.Complete)
		}
		_ = c.RespondSaved(saved, err)
	}
}

// echo sends the request's body back as it arrives. Each piece OnBody hands
// over is written to the response, which streams, chunked, so that neither
// the body nor the response is ever held whole; Finish ends it with the last
// piece, and answers an empty body with an empty response. A body that fails
// midway has no one to answer: the connection is gone or about to be closed.
func echo(c *fibhttp.Context) {
	c.Header().Set("Content-Type", "application/octet-stream")
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {
			return
		}
		_, _ = c.Write(data)
		if fin {
			_ = c.Finish()
		}
	})
}
