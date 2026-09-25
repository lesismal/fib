//go:build linux || darwin || windows

// Package recover turns a handler's panic into a 500 response, rather than
// into the loss of the connection it was serving and of every request on it.
//
// It catches what panics while the handler runs. A handler that retained its
// request and goes on answering it from a goroutine of its own recovers what
// panics there itself.
package recover

import (
	"log"
	stdhttp "net/http"
	"runtime/debug"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
)

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Handler answers a request whose handler panicked with recovered. By
	// default it answers 500 Internal Server Error, unless the response has
	// been begun already, which leaves nothing to answer with.
	Handler func(c *fibhttp.Context, r *stdhttp.Request, recovered any)
	// Log reports the panic, with the stack it was raised on unless
	// DisableStackTrace is set. By default it goes to the standard logger;
	// a function that does nothing silences it.
	Log               func(r *stdhttp.Request, recovered any, stack []byte)
	DisableStackTrace bool
}

// New returns the middleware.
//
// A panic with http.ErrAbortHandler, which net/http takes for a handler
// abandoning its response, is not logged. On HTTP/1 it ends the connection,
// so that the client does not take a response cut short for a whole one; on
// HTTP/2 and HTTP/3 it is answered as any other panic, since the connection
// there carries other requests.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Handler == nil {
		cfg.Handler = internalError
	}
	if cfg.Log == nil {
		cfg.Log = logPanic
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}
				if recovered == stdhttp.ErrAbortHandler {
					if r.ProtoMajor == 1 {
						_ = c.Conn.Close()
						return
					}
				} else {
					var stack []byte
					if !cfg.DisableStackTrace {
						stack = debug.Stack()
					}
					cfg.Log(r, recovered, stack)
				}
				cfg.Handler(c, r, recovered)
			}()
			next.ServeHTTP(c, r)
		})
	}
}

func internalError(c *fibhttp.Context, _ *stdhttp.Request, _ any) {
	status := stdhttp.StatusInternalServerError
	_ = c.Respond(status, "text/plain; charset=utf-8", []byte(stdhttp.StatusText(status)+"\n"))
}

func logPanic(r *stdhttp.Request, recovered any, stack []byte) {
	if len(stack) == 0 {
		log.Printf("http: panic serving %s %s: %v", r.Method, r.URL, recovered)
		return
	}
	log.Printf("http: panic serving %s %s: %v\n%s", r.Method, r.URL, recovered, stack)
}
