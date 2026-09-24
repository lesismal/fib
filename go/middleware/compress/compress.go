//go:build linux || darwin || windows

// Package compress compresses response bodies with gzip or deflate, as the
// request's Accept-Encoding allows.
//
// A body is compressed when it is at least Config.MinLength long, its
// Content-Type is one that compresses (text, JSON, JavaScript, XML, SVG and
// the like; see Compressible), the response has no Content-Encoding of its
// own and is not a partial one, and its Cache-Control does not forbid
// transforming it. A compressed body that comes out no shorter is sent as it
// was. Every response that could have been compressed says so in its Vary
// header, so that caches keep the two apart, and a strong ETag it carries is
// made weak, since the body it goes with is no longer the same byte for byte.
//
// The middleware sees each response whole, so one written through the
// ResponseWriter methods is held until the handler is done; see
// Context.OnResponse. Put it outside etag, so that ETags are made from the
// bodies as the handler wrote them.
package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
)

// Level is how hard the compressor works.
type Level int

const (
	LevelDefault Level = iota
	LevelBestSpeed
	LevelBestCompression
)

// DefaultMinLength is the shortest body compressed by default. Below about
// a kilobyte what compression saves is less than it costs.
const DefaultMinLength = 1024

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Level is how hard the compressor works; LevelDefault by default.
	Level Level
	// MinLength is the shortest body compressed; DefaultMinLength by
	// default.
	MinLength int
	// Compressible reports whether a body with the media type given, such as
	// "text/html", is worth compressing; the package's Compressible by
	// default.
	Compressible func(mediaType string) bool
}

const (
	encodingGzip    = "gzip"
	encodingDeflate = "deflate"
)

// New returns the middleware.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.MinLength <= 0 {
		cfg.MinLength = DefaultMinLength
	}
	if cfg.Compressible == nil {
		cfg.Compressible = Compressible
	}
	level := flate.DefaultCompression
	switch cfg.Level {
	case LevelBestSpeed:
		level = flate.BestSpeed
	case LevelBestCompression:
		level = flate.BestCompression
	}
	pools := &writerPools{level: level}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			c.OnResponse(func(response *fibhttp.Response) {
				if !eligible(response, cfg) {
					return
				}
				middleware.AddVary(response.Header, "Accept-Encoding")
				if r.Method == stdhttp.MethodHead {
					return
				}
				encoding := negotiate(r.Header["Accept-Encoding"])
				if encoding == "" {
					return
				}
				body, ok := pools.compress(encoding, response.Body)
				if !ok {
					return
				}
				response.Body = body
				response.Header["Content-Encoding"] = []string{encoding}
				delete(response.Header, "Content-Length")
				if tag := headerValue(response.Header, "Etag"); tag != "" && !strings.HasPrefix(tag, "W/") {
					response.Header["Etag"] = []string{"W/" + tag}
				}
			})
			next.ServeHTTP(c, r)
		})
	}
}

// eligible reports whether response could be compressed for a client that
// accepts it. A response to HEAD counts by the length it declares.
func eligible(response *fibhttp.Response, cfg Config) bool {
	status := response.StatusCode
	if status < 200 || status == stdhttp.StatusNoContent || status == stdhttp.StatusNotModified ||
		status == stdhttp.StatusPartialContent || response.Header == nil {
		return false
	}
	h := response.Header
	if encoding := headerValue(h, "Content-Encoding"); encoding != "" && encoding != "identity" {
		return false
	}
	if _, ok := h["Content-Range"]; ok {
		return false
	}
	for _, value := range h["Cache-Control"] {
		if strings.Contains(strings.ToLower(value), "no-transform") {
			return false
		}
	}
	length := len(response.Body)
	if length == 0 {
		if n, err := strconv.Atoi(headerValue(h, "Content-Length")); err == nil {
			length = n
		}
	}
	if length < cfg.MinLength {
		return false
	}
	mediaType, _, _ := strings.Cut(headerValue(h, "Content-Type"), ";")
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	return mediaType != "" && cfg.Compressible(mediaType)
}

// Compressible reports whether a body of mediaType is worth compressing:
// text, and the structured and script types that are text underneath, but
// not images, audio, video or archives, which are compressed already.
func Compressible(mediaType string) bool {
	if strings.HasPrefix(mediaType, "text/") ||
		strings.HasSuffix(mediaType, "+json") || strings.HasSuffix(mediaType, "+xml") {
		return true
	}
	switch mediaType {
	case "application/json", "application/javascript", "application/x-javascript",
		"application/ecmascript", "application/xml", "application/wasm",
		"application/graphql-response+json", "application/x-ndjson", "application/manifest+json",
		"application/vnd.ms-fontobject", "application/x-font-ttf",
		"font/ttf", "font/otf", "image/svg+xml", "image/x-icon", "image/vnd.microsoft.icon", "image/bmp":
		return true
	}
	return false
}

// negotiate picks the encoding the Accept-Encoding fields prefer, gzip over
// deflate when they prefer both as much, or "" when they accept neither.
func negotiate(fields []string) string {
	gzipQ, deflateQ, anyQ := -1.0, -1.0, -1.0
	for _, field := range fields {
		for _, member := range strings.Split(field, ",") {
			name, params, _ := strings.Cut(member, ";")
			name = strings.ToLower(strings.TrimSpace(name))
			q := 1.0
			for _, param := range strings.Split(params, ";") {
				if key, value, ok := strings.Cut(strings.TrimSpace(param), "="); ok && strings.EqualFold(key, "q") {
					if v, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
						q = v
					}
				}
			}
			switch name {
			case "gzip", "x-gzip":
				gzipQ = q
			case "deflate":
				deflateQ = q
			case "*":
				anyQ = q
			}
		}
	}
	if gzipQ < 0 {
		gzipQ = anyQ
	}
	if deflateQ < 0 {
		deflateQ = anyQ
	}
	switch {
	case gzipQ > 0 && gzipQ >= deflateQ:
		return encodingGzip
	case deflateQ > 0:
		return encodingDeflate
	}
	return ""
}

// writerPools recycles the compressors, which cost far more to make than to
// reset.
type writerPools struct {
	level         int
	gzip, deflate sync.Pool
}

type compressor interface {
	io.WriteCloser
	Reset(io.Writer)
}

// compress returns body compressed with encoding, and whether that made it
// shorter.
func (p *writerPools) compress(encoding string, body []byte) ([]byte, bool) {
	pool := &p.gzip
	if encoding == encodingDeflate {
		pool = &p.deflate
	}
	var out bytes.Buffer
	out.Grow(len(body)/2 + 64)
	w, _ := pool.Get().(compressor)
	if w == nil {
		if encoding == encodingGzip {
			w, _ = gzip.NewWriterLevel(&out, p.level)
		} else {
			w, _ = flate.NewWriter(&out, p.level)
		}
	} else {
		w.Reset(&out)
	}
	_, err := w.Write(body)
	if err == nil {
		err = w.Close()
	}
	w.Reset(io.Discard)
	pool.Put(w)
	if err != nil || out.Len() >= len(body) {
		return nil, false
	}
	return out.Bytes(), true
}

func headerValue(header stdhttp.Header, key string) string {
	if values := header[key]; len(values) > 0 {
		return values[0]
	}
	return ""
}
