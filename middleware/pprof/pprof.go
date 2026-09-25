//go:build linux || darwin || windows

// Package pprof serves the runtime profiles of net/http/pprof under
// /debug/pprof/, for go tool pprof and a browser to read:
//
//	go tool pprof http://localhost:8080/debug/pprof/profile?seconds=10
//
// A profile can take many seconds to gather, so each one is gathered on a
// goroutine of its own rather than on the connection's worker, and answered
// once it is done. A request whose connection goes first stops its profile.
//
// The profiles tell a lot about the process; serve them only where the
// people who can reach them may know it.
package pprof

import (
	"context"
	stdhttp "net/http"
	"net/http/pprof"
	"strings"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
)

// DefaultPrefix is where the profiles are served by default.
const DefaultPrefix = "/debug/pprof"

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Prefix is the path the profiles are served under; DefaultPrefix by
	// default.
	Prefix string
}

// New returns the middleware. Requests for paths outside Prefix go on to the
// handler.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultPrefix
	}
	prefix := "/" + strings.Trim(cfg.Prefix, "/")
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			path := r.URL.Path
			if cfg.Next != nil && cfg.Next(c, r) || !strings.HasPrefix(path, prefix) {
				next.ServeHTTP(c, r)
				return
			}
			var handler stdhttp.Handler
			switch name := path[len(prefix):]; name {
			case "":
				// The index links to the profiles relative to itself.
				target := prefix + "/"
				if r.URL.RawQuery != "" {
					target += "?" + r.URL.RawQuery
				}
				_ = c.WriteResponse(fibhttp.Response{
					StatusCode: stdhttp.StatusMovedPermanently,
					Header:     stdhttp.Header{"Location": {target}},
				})
				return
			case "/":
				handler = stdhttp.HandlerFunc(pprof.Index)
			case "/cmdline":
				handler = stdhttp.HandlerFunc(pprof.Cmdline)
			case "/profile":
				handler = stdhttp.HandlerFunc(pprof.Profile)
			case "/symbol":
				handler = stdhttp.HandlerFunc(pprof.Symbol)
			case "/trace":
				handler = stdhttp.HandlerFunc(pprof.Trace)
			default:
				if name[0] != '/' {
					// A path that only starts like the prefix, such as
					// /debug/pprofile.
					next.ServeHTTP(c, r)
					return
				}
				handler = pprof.Handler(strings.TrimPrefix(name, "/"))
			}
			serve(c, r, handler)
		})
	}
}

// serve runs handler on a goroutine of its own and answers with what it
// wrote.
func serve(c *fibhttp.Context, r *stdhttp.Request, handler stdhttp.Handler) {
	ctx, cancel := context.WithCancel(r.Context())
	c.Retain()
	c.OnCancel(func(error) { cancel() })
	go func() {
		defer c.Release()
		defer cancel()
		w := &recorder{header: make(stdhttp.Header)}
		handler.ServeHTTP(w, r.WithContext(ctx))
		if w.status == 0 {
			w.status = stdhttp.StatusOK
		}
		_ = c.WriteResponse(fibhttp.Response{StatusCode: w.status, Header: w.header, Body: w.body})
	}()
}

// recorder is the http.ResponseWriter a profile is written to, to be sent
// whole once it is done.
type recorder struct {
	header stdhttp.Header
	status int
	body   []byte
}

func (w *recorder) Header() stdhttp.Header { return w.header }

func (w *recorder) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *recorder) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = stdhttp.StatusOK
	}
	if _, ok := w.header["Content-Type"]; !ok && len(w.body) == 0 {
		w.header.Set("Content-Type", stdhttp.DetectContentType(p))
	}
	w.body = append(w.body, p...)
	return len(p), nil
}
