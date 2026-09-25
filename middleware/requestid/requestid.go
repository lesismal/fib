//go:build linux || darwin || windows

// Package requestid gives every request an identifier: the one the client
// sent in the request's X-Request-ID header, or else a new one. It is set on
// the request's header, where the handler and the middleware inside this one
// read it, and sent back on the response.
package requestid

import (
	"crypto/rand"
	"encoding/hex"
	stdhttp "net/http"
	"net/textproto"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
)

// DefaultHeader is the header the identifier travels in.
const DefaultHeader = "X-Request-ID"

// maxLength bounds an identifier the client sends. A longer one, or one with
// anything but printable ASCII in it, is replaced, so that it can be logged
// as it is.
const maxLength = 128

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Header is the header the identifier is read from and sent in;
	// DefaultHeader by default.
	Header string
	// Generator makes a new identifier; a random UUID by default.
	Generator func() string
}

// New returns the middleware.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Header == "" {
		cfg.Header = DefaultHeader
	}
	header := textproto.CanonicalMIMEHeaderKey(cfg.Header)
	if cfg.Generator == nil {
		cfg.Generator = UUID
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			id := r.Header.Get(header)
			if !valid(id) {
				id = cfg.Generator()
				if r.Header == nil {
					r.Header = make(stdhttp.Header)
				}
				r.Header[header] = []string{id}
			}
			c.OnHeader(func(_ int, h stdhttp.Header) {
				if _, set := h[header]; !set {
					h[header] = []string{id}
				}
			})
			next.ServeHTTP(c, r)
		})
	}
}

// FromRequest is the identifier of a request the middleware has seen, when
// it uses DefaultHeader.
func FromRequest(r *stdhttp.Request) string { return r.Header.Get(DefaultHeader) }

func valid(id string) bool {
	if id == "" || len(id) > maxLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] <= ' ' || id[i] > '~' {
			return false
		}
	}
	return true
}

// UUID returns a random (version 4) UUID.
func UUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:])
}
