//go:build linux || darwin || windows

// Package etag gives a 200 response to GET or HEAD an ETag made from its
// body, and answers a request whose If-None-Match names the ETag it would
// get with 304 Not Modified, without the body.
//
// A response whose handler set an ETag keeps it, and is answered with 304 in
// the same way. The middleware sees each response whole, so one written
// through the ResponseWriter methods is held until the handler is done; see
// Context.OnResponse.
package etag

import (
	"hash/crc32"
	stdhttp "net/http"
	"strconv"
	"strings"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
)

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Weak makes the ETags weak validators, W/"...", which say two bodies
	// are equivalent rather than the same byte for byte.
	Weak bool
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// New returns the middleware.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) ||
				r.Method != stdhttp.MethodGet && r.Method != stdhttp.MethodHead {
				next.ServeHTTP(c, r)
				return
			}
			c.OnResponse(func(response *fibhttp.Response) {
				if response.StatusCode != stdhttp.StatusOK {
					return
				}
				tag := headerValue(response.Header, "Etag")
				if tag == "" {
					if len(response.Body) == 0 {
						return
					}
					tag = Generate(response.Body, cfg.Weak)
					if response.Header == nil {
						response.Header = make(stdhttp.Header)
					}
					response.Header["Etag"] = []string{tag}
				}
				if match := r.Header["If-None-Match"]; len(match) > 0 && Matches(strings.Join(match, ","), tag) {
					NotModified(response)
				}
			})
			next.ServeHTTP(c, r)
		})
	}
}

// Generate makes an ETag for body, from its length and its CRC-32C.
func Generate(body []byte, weak bool) string {
	out := make([]byte, 0, 24)
	if weak {
		out = append(out, "W/"...)
	}
	out = append(out, '"')
	out = strconv.AppendUint(out, uint64(len(body)), 16)
	out = append(out, '-')
	out = strconv.AppendUint(out, uint64(crc32.Checksum(body, crcTable)), 16)
	return string(append(out, '"'))
}

// Matches reports whether the If-None-Match field list names tag, comparing
// them weakly, as RFC 9110 section 13.1.2 has it: W/"x" matches "x".
func Matches(list, tag string) bool {
	tag = strings.TrimPrefix(tag, "W/")
	for {
		list = strings.TrimLeft(list, " \t,")
		if list == "" {
			return false
		}
		if list[0] == '*' {
			return true
		}
		list = strings.TrimPrefix(list, "W/")
		if list == "" || list[0] != '"' {
			// Not an entity tag; skip to the next member.
			i := strings.IndexByte(list, ',')
			if i < 0 {
				return false
			}
			list = list[i:]
			continue
		}
		end := strings.IndexByte(list[1:], '"')
		if end < 0 {
			return false
		}
		if list[:end+2] == tag {
			return true
		}
		list = list[end+2:]
	}
}

// NotModified turns response into a 304 Not Modified, which keeps the
// fields RFC 9110 section 15.4.5 asks for and none of the body.
func NotModified(response *fibhttp.Response) {
	response.StatusCode = stdhttp.StatusNotModified
	response.Body = nil
	response.Trailer = nil
	for _, key := range representationFields {
		delete(response.Header, key)
	}
}

// representationFields describe a body, which a 304 does not carry.
var representationFields = [...]string{"Content-Type", "Content-Length", "Content-Encoding", "Content-Range"}

func headerValue(header stdhttp.Header, key string) string {
	if values := header[key]; len(values) > 0 {
		return values[0]
	}
	return ""
}
