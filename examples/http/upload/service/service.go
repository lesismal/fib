//go:build linux || darwin || windows

// Package service is the upload example's request handler, shared by its
// HTTP/1, HTTP/2 and HTTP/3 servers: the same handler takes large bodies over
// all three, because the body arrives through Context.OnBody whatever
// protocol carried it. It serves three endpoints:
//
//	POST|PUT   /upload?name=F   Context.SaveBody: the whole file in one request
//	PUT|PATCH  /resume?name=F   Context.SaveBodyResumable: the file in chunks,
//	                            each with a Content-Range, and continued after a
//	                            dropped connection from the offset the server
//	                            reports (Content-Range: bytes */TOTAL asks)
//	POST       /echo            answers with the body it receives, as it receives it
package service

import (
	"log"
	"net/http"
	"path/filepath"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// Config is the http.Config the servers run the handler with. A request goes
// to its handler once the header is in, whatever the body's size, and the body
// is bounded by MaxStreamedBodyBytes, which is for bodies that stream, rather
// than MaxBodyBytes, which is for those held whole. maxSize is zero for no limit.
func Config(maxSize int64) fibhttp.Config {
	config := fibhttp.DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0
	config.MaxStreamedBodyBytes = maxSize
	// How much of an HTTP/1 body may wait unread before the connection stops
	// reading its socket. Small, to show that memory stays bounded; HTTP/2
	// and HTTP/3 are paced by their flow-control windows instead.
	config.StreamRequestBodyBuffer = 1 << 20
	config.IdleTimeout = time.Minute
	return config
}

// Handler serves the endpoints, storing uploads in dir.
func Handler(dir string) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/upload":
			if allow(c, http.MethodPost, http.MethodPut) {
				if path, ok := target(c, dir); ok {
					c.SaveBody(path, logged(c))
				}
			}
		case "/resume":
			if allow(c, http.MethodPut, http.MethodPatch) {
				if path, ok := target(c, dir); ok {
					c.SaveBodyResumable(path, logged(c))
				}
			}
		case "/echo":
			if allow(c, http.MethodPost) {
				echo(c)
			}
		default:
			_ = c.Respond(http.StatusNotFound, "text/plain; charset=utf-8", []byte("POST /upload, PUT /resume or POST /echo\n"))
		}
	}
}

// allow answers 405 unless the request's method is one of methods.
func allow(c *fibhttp.Context, methods ...string) bool {
	for _, method := range methods {
		if c.Request.Method == method {
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
func logged(c *fibhttp.Context) func(fibhttp.Saved, error) {
	started := time.Now()
	return func(saved fibhttp.Saved, err error) {
		if err != nil {
			log.Printf("%s %s %s: %d of %d bytes stored: %v", c.Request.Proto, c.Request.Method, c.Request.URL.Path, saved.Size, saved.Total, err)
		} else {
			log.Printf("%s %s %s: %d bytes stored in %v, complete: %v", c.Request.Proto, c.Request.Method, c.Request.URL.Path, saved.Size,
				time.Since(started).Round(time.Millisecond), saved.Complete)
		}
		_ = c.RespondSaved(saved, err)
	}
}

// echo sends the request's body back as it arrives. Each piece OnBody hands
// over is written to the response, which streams, so that neither the body
// nor the response is ever held whole; Finish ends it with the last piece,
// and answers an empty body with an empty response. A body that fails midway
// has no one to answer: the connection is gone or about to be closed.
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
