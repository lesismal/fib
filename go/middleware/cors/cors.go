//go:build linux || darwin || windows

// Package cors lets pages from other origins call the server, as the Fetch
// standard's Cross-Origin Resource Sharing describes: it answers preflight
// requests itself, and adds the Access-Control-* fields to the responses to
// the requests they let through.
package cors

import (
	stdhttp "net/http"
	"strconv"
	"strings"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
)

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// AllowOrigins are the origins allowed, such as "https://example.com";
	// "*" allows any, and a "*" in place of a host's first labels any of its
	// subdomains, as in "https://*.example.com". By default any is allowed,
	// unless AllowOriginsFunc is set.
	AllowOrigins []string
	// AllowOriginsFunc allows an origin AllowOrigins does not.
	AllowOriginsFunc func(origin string) bool
	// AllowMethods are the methods a preflight allows; GET, POST, HEAD, PUT,
	// DELETE and PATCH by default.
	AllowMethods []string
	// AllowHeaders are the request fields a preflight allows; by default
	// whichever the preflight asks for.
	AllowHeaders []string
	// ExposeHeaders are the response fields a page may read besides those
	// always exposed.
	ExposeHeaders []string
	// AllowCredentials lets requests carry cookies and HTTP authentication.
	// Browsers refuse it with an origin of "*", so it cannot go with an
	// AllowOrigins of "*".
	AllowCredentials bool
	// AllowPrivateNetwork answers a preflight that asks whether a public page
	// may call a server on a private network.
	AllowPrivateNetwork bool
	// MaxAge is how many seconds a browser may keep a preflight's answer; by
	// default it decides for itself. A negative one tells it to keep none.
	MaxAge int
}

var defaultMethods = []string{
	stdhttp.MethodGet, stdhttp.MethodPost, stdhttp.MethodHead,
	stdhttp.MethodPut, stdhttp.MethodDelete, stdhttp.MethodPatch,
}

// pattern is an allowed origin with a "*" for its subdomains.
type pattern struct{ prefix, suffix string }

func (p pattern) match(origin string) bool {
	if len(origin) <= len(p.prefix)+len(p.suffix) ||
		!strings.HasPrefix(origin, p.prefix) || !strings.HasSuffix(origin, p.suffix) {
		return false
	}
	sub := origin[len(p.prefix) : len(origin)-len(p.suffix)]
	return !strings.ContainsAny(sub, "/:@")
}

// New returns the middleware. It panics if AllowCredentials is set with an
// AllowOrigins of "*".
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if len(cfg.AllowOrigins) == 0 && cfg.AllowOriginsFunc == nil {
		cfg.AllowOrigins = []string{"*"}
	}
	if len(cfg.AllowMethods) == 0 {
		cfg.AllowMethods = defaultMethods
	}
	anyOrigin := false
	exact := make(map[string]bool)
	var patterns []pattern
	for _, origin := range cfg.AllowOrigins {
		origin = strings.ToLower(strings.TrimRight(strings.TrimSpace(origin), "/"))
		switch {
		case origin == "*":
			anyOrigin = true
		case strings.Contains(origin, "://*."):
			i := strings.Index(origin, "://*.")
			patterns = append(patterns, pattern{prefix: origin[:i+3], suffix: origin[i+4:]})
		case origin != "":
			exact[origin] = true
		}
	}
	if anyOrigin && cfg.AllowCredentials {
		panic("cors: AllowCredentials cannot go with an AllowOrigins of \"*\"")
	}
	allowed := func(origin string) bool {
		if anyOrigin {
			return true
		}
		lower := strings.ToLower(origin)
		if exact[lower] {
			return true
		}
		for _, p := range patterns {
			if p.match(lower) {
				return true
			}
		}
		return cfg.AllowOriginsFunc != nil && cfg.AllowOriginsFunc(origin)
	}
	// With "*" every response says the same, and the origin need not be
	// echoed; otherwise the answer depends on it, which Vary tells caches.
	allowOrigin := func(origin string) string {
		if anyOrigin {
			return "*"
		}
		return origin
	}
	methods := strings.Join(cfg.AllowMethods, ", ")
	headers := strings.Join(cfg.AllowHeaders, ", ")
	expose := strings.Join(cfg.ExposeHeaders, ", ")
	maxAge := ""
	switch {
	case cfg.MaxAge > 0:
		maxAge = strconv.Itoa(cfg.MaxAge)
	case cfg.MaxAge < 0:
		maxAge = "0"
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			origin := r.Header.Get("Origin")
			if r.Method == stdhttp.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				h := stdhttp.Header{"Vary": {"Origin, Access-Control-Request-Method, Access-Control-Request-Headers"}}
				if origin != "" && allowed(origin) {
					h["Access-Control-Allow-Origin"] = []string{allowOrigin(origin)}
					h["Access-Control-Allow-Methods"] = []string{methods}
					if headers != "" {
						h["Access-Control-Allow-Headers"] = []string{headers}
					} else if asked := r.Header.Get("Access-Control-Request-Headers"); asked != "" {
						h["Access-Control-Allow-Headers"] = []string{asked}
					}
					if cfg.AllowCredentials {
						h["Access-Control-Allow-Credentials"] = []string{"true"}
					}
					if cfg.AllowPrivateNetwork && r.Header.Get("Access-Control-Request-Private-Network") == "true" {
						h["Access-Control-Allow-Private-Network"] = []string{"true"}
					}
					if maxAge != "" {
						h["Access-Control-Max-Age"] = []string{maxAge}
					}
				}
				_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusNoContent, Header: h})
				return
			}
			if origin == "" && anyOrigin {
				next.ServeHTTP(c, r)
				return
			}
			// A response to a request from a page elsewhere differs from one
			// to a request without Origin, which caches have to be told too.
			ok := origin != "" && allowed(origin)
			c.OnHeader(func(_ int, h stdhttp.Header) {
				if !anyOrigin {
					middleware.AddVary(h, "Origin")
				}
				if !ok {
					return
				}
				h["Access-Control-Allow-Origin"] = []string{allowOrigin(origin)}
				if cfg.AllowCredentials {
					h["Access-Control-Allow-Credentials"] = []string{"true"}
				}
				if expose != "" {
					h["Access-Control-Expose-Headers"] = []string{expose}
				}
			})
			next.ServeHTTP(c, r)
		})
	}
}
