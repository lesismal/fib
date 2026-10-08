//go:build linux || darwin || windows

// Command server receives large file uploads over HTTP/1 without holding them
// in memory. With Config.StreamRequestBody on, the handler runs as soon as the
// request header has arrived, and Context.OnBody hands it the body piece by
// piece as the connection reads it: each piece is written to a file and fed
// to a SHA-256, then dropped. What holds a fast client back is the handler
// itself — while OnBody's callback is busy the connection stops reading, and
// TCP flow control slows the sender — so memory stays flat however large the
// upload is.
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
//	go run ./examples/http/upload/server -dir /tmp/uploads
//	go run ./examples/http/upload/client -file /tmp/big.bin
//
// or with curl:
//
//	curl -T /tmp/big.bin 'http://127.0.0.1:8080/upload?name=big.bin'
package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/example"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	dir := flag.String("dir", "uploads", "directory the uploads are stored in")
	maxSize := flag.Int64("max", 64<<30, "largest upload accepted, in bytes (0 for no limit)")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		example.Fatal(err)
	}

	httpConfig := fibhttp.DefaultConfig()
	// Hand the request to the handler once its header is in, whatever the
	// body's size, and bound the body by MaxStreamedBodyBytes rather than by
	// MaxBodyBytes, which only applies to a body held whole.
	httpConfig.StreamRequestBody = true
	httpConfig.StreamRequestBodyThreshold = 0
	httpConfig.MaxStreamedBodyBytes = *maxSize
	// How much of the body may wait unread before the connection stops
	// reading its socket. Small here, to show that memory stays bounded.
	httpConfig.StreamRequestBodyBuffer = 1 << 20
	// A client that stalls mid-upload is cut off, a slow one is not.
	httpConfig.ReadTimeout = 0
	httpConfig.IdleTimeout = time.Minute

	config := fib.DefaultConfig()
	config.Addr = *addr
	engine, err := fib.Bind(config, fibhttp.NewHandlerWithConfig(httpConfig, upload(*dir)))
	if err != nil {
		example.Fatal(err)
	}
	example.Serve(engine, fmt.Sprintf("upload server listening on http://%s, storing into %s", *addr, *dir))
}

// upload answers POST and PUT on /upload?name=<file name>, storing the body
// as <dir>/<name>.
func upload(dir string) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context, r *http.Request) {
		if r.URL.Path != "/upload" {
			_ = c.Respond(http.StatusNotFound, "text/plain; charset=utf-8", []byte("POST or PUT /upload?name=<file>\n"))
			return
		}
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			_ = c.Respond(http.StatusMethodNotAllowed, "text/plain; charset=utf-8", []byte("use POST or PUT\n"))
			return
		}
		// Base keeps the name from reaching outside dir.
		name := filepath.Base(r.URL.Query().Get("name"))
		if name == "." || name == string(filepath.Separator) || name == "" {
			_ = c.Respond(http.StatusBadRequest, "text/plain; charset=utf-8", []byte("name is required\n"))
			return
		}
		final := filepath.Join(dir, name)
		partial := final + ".part"
		file, err := os.Create(partial)
		if err != nil {
			_ = c.Respond(http.StatusInternalServerError, "text/plain; charset=utf-8", []byte(err.Error()+"\n"))
			return
		}

		// The handler is called before the body has arrived: BodyComplete is
		// false for anything larger than what came with the header.
		log.Printf("upload %s: handler called, content-length=%d, body complete yet: %v",
			name, r.ContentLength, c.BodyComplete())

		var (
			sum      = sha256.New()
			received int64
			started  = time.Now()
			failed   bool
		)
		fail := func(why error) {
			failed = true
			file.Close()
			os.Remove(partial)
			log.Printf("upload %s: %v after %d bytes", name, why, received)
		}

		// OnBody registers the callback and returns; the handler is done
		// here. The calls come one at a time and in order, on the worker that
		// reads this connection, and data is only valid during each call.
		// OnBody keeps the request open until the last call returns, so the
		// response written from it needs no Retain.
		c.OnBody(func(data []byte, fin bool, err error) {
			if failed {
				return
			}
			if err != nil {
				// The connection went, the body outgrew MaxStreamedBodyBytes,
				// or its framing was broken: the last call, with no response
				// to give.
				fail(err)
				return
			}
			if len(data) > 0 {
				if _, err := file.Write(data); err != nil {
					fail(err)
					_ = c.Respond(http.StatusInternalServerError, "text/plain; charset=utf-8", []byte(err.Error()+"\n"))
					return
				}
				sum.Write(data)
				received += int64(len(data))
			}
			if !fin {
				return
			}
			if err := file.Close(); err != nil {
				fail(err)
				_ = c.Respond(http.StatusInternalServerError, "text/plain; charset=utf-8", []byte(err.Error()+"\n"))
				return
			}
			if err := os.Rename(partial, final); err != nil {
				fail(err)
				_ = c.Respond(http.StatusInternalServerError, "text/plain; charset=utf-8", []byte(err.Error()+"\n"))
				return
			}
			elapsed := time.Since(started)
			log.Printf("upload %s: %d bytes in %v (%.1f MiB/s)", name, received, elapsed.Round(time.Millisecond),
				float64(received)/(1<<20)/elapsed.Seconds())
			reply := fmt.Sprintf("stored %s bytes=%d sha256=%x\n", name, received, sum.Sum(nil))
			if err := c.Respond(http.StatusOK, "text/plain; charset=utf-8", []byte(reply)); err != nil {
				log.Printf("respond: %v", err)
			}
		})
	}
}
