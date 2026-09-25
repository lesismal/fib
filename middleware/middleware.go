//go:build linux || darwin || windows

// Package middleware composes handlers of package http out of middleware:
// functions that take a handler and return one that does something around
// it. Its subpackages hold the common ones — compress, cors, csrf, etag,
// limiter, logger, pprof, recover, requestid and responsetime — each built
// by its package's New from an optional Config:
//
//	handler := middleware.Chain(app,
//		recover.New(),
//		requestid.New(),
//		logger.New(),
//		compress.New(),
//		etag.New(),
//	)
//	engine, err := fib.Bind(config, fibhttp.NewHandler(handler))
//
// A middleware that changes the response a handler writes does so through
// the hooks Context has — OnHeader, OnResponse and OnFinish — so that it
// works whichever way the handler answers: WriteResponse, Respond, or the
// http.ResponseWriter methods. Every Config has a Next function which, when
// it reports true, passes the request straight on to the handler.
package middleware

import (
	"net"
	stdhttp "net/http"
	"strings"

	fibhttp "github.com/lesismal/fib/http"
)

// Middleware wraps a handler in another that does something around it.
type Middleware func(next fibhttp.Handler) fibhttp.Handler

// Chain wraps handler in middlewares, the first of them outermost: it sees
// each request first, and the response last.
func Chain(handler fibhttp.Handler, middlewares ...Middleware) fibhttp.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] != nil {
			handler = middlewares[i](handler)
		}
	}
	return handler
}

// Skipper is the Next function of a middleware's Config: reporting true
// passes the request straight on to the handler.
type Skipper func(c *fibhttp.Context, r *stdhttp.Request) bool

// RemoteIP is the address the request came from, without its port.
func RemoteIP(r *stdhttp.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// AddVary adds name to header's Vary, unless it is there already or Vary is
// "*".
func AddVary(header stdhttp.Header, name string) {
	for _, value := range header["Vary"] {
		for _, member := range strings.Split(value, ",") {
			if member = strings.TrimSpace(member); member == "*" || strings.EqualFold(member, name) {
				return
			}
		}
	}
	header["Vary"] = append(header["Vary"], name)
}
