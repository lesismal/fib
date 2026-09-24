//go:build linux || darwin || windows

// Package responsetime reports how long the handler took to settle its
// response, in the response's X-Response-Time header.
package responsetime

import (
	stdhttp "net/http"
	"net/textproto"
	"strconv"
	"time"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
)

// DefaultHeader is the header the time is sent in.
const DefaultHeader = "X-Response-Time"

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Header is the header the time is sent in; DefaultHeader by default.
	Header string
	// Format writes the time; by default as milliseconds with three
	// decimals, such as "1.234ms".
	Format func(time.Duration) string
}

// New returns the middleware. The time runs from the request reaching it to
// the response's header being settled, which for a response written through
// the ResponseWriter methods is its first Write or WriteHeader.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Header == "" {
		cfg.Header = DefaultHeader
	}
	header := textproto.CanonicalMIMEHeaderKey(cfg.Header)
	if cfg.Format == nil {
		cfg.Format = Milliseconds
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			start := time.Now()
			c.OnHeader(func(_ int, h stdhttp.Header) {
				h[header] = []string{cfg.Format(time.Since(start))}
			})
			next.ServeHTTP(c, r)
		})
	}
}

// Milliseconds formats d as milliseconds with three decimals.
func Milliseconds(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 3, 64) + "ms"
}
