//go:build linux || darwin || windows

// Package logger writes a line for every response, once it has been handed
// to the connection.
//
// The line is laid out by Config.Format, whose ${tag}s stand for:
//
//	${time}             when the line is written, in Config.TimeFormat
//	${status}           the response's status
//	${latency}          from the request reaching the middleware to its response going out
//	${ip}               the client's address, without its port
//	${method}           the request's method
//	${path}             the request's path
//	${url}              the request's target: its path and query
//	${query}            the request's query, without the "?"
//	${host}             the request's host
//	${protocol}         the request's protocol, such as HTTP/1.1
//	${referer}          the request's Referer header
//	${ua}               the request's User-Agent header
//	${bytesReceived}    the length of the request's body, when it has one
//	${bytesSent}        the length of the response's body
//	${pid}              the process's ID
//	${reqHeader:Name}   the request's header Name
//	${respHeader:Name}  the response's header Name
//	${queryParam:name}  the request's query parameter name
//
// Anything else in Format is written as it is. Control characters in what a
// tag stands for are escaped, so that a request cannot forge a line.
//
// A request whose response is never written, such as one whose connection
// went before it was answered, has no line.
package logger

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
)

// DefaultFormat is the line written for a response by default.
const DefaultFormat = "${time} | ${status} | ${latency} | ${ip} | ${method} | ${path}\n"

// DefaultTimeFormat is how ${time} is written by default.
const DefaultTimeFormat = "15:04:05"

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Format lays out the line; DefaultFormat by default.
	Format string
	// TimeFormat is how ${time} is written, in the layout of package time;
	// DefaultTimeFormat by default.
	TimeFormat string
	// Output is where the lines go, one Write at a time; os.Stdout by
	// default.
	Output io.Writer
}

// entry is what a line is written from.
type entry struct {
	r       *stdhttp.Request
	status  int
	header  stdhttp.Header
	size    int64
	latency time.Duration
	now     time.Time
}

// part appends one piece of the line.
type part func(out []byte, e *entry) []byte

// New returns the middleware. It panics if Format has a tag it does not know.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Format == "" {
		cfg.Format = DefaultFormat
	}
	if cfg.TimeFormat == "" {
		cfg.TimeFormat = DefaultTimeFormat
	}
	if cfg.Output == nil {
		cfg.Output = os.Stdout
	}
	parts, err := parse(cfg.Format, cfg.TimeFormat)
	if err != nil {
		panic(err)
	}
	var mu sync.Mutex
	buffers := sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}
	write := func(e *entry) {
		buf := buffers.Get().(*[]byte)
		out := (*buf)[:0]
		for _, p := range parts {
			out = p(out, e)
		}
		mu.Lock()
		_, _ = cfg.Output.Write(out)
		mu.Unlock()
		*buf = out
		buffers.Put(buf)
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			start := time.Now()
			c.OnFinish(func(status int, header stdhttp.Header, size int64) {
				now := time.Now()
				write(&entry{r: r, status: status, header: header, size: size, latency: now.Sub(start), now: now})
			})
			next.ServeHTTP(c, r)
		})
	}
}

// parse turns format into the parts that write it.
func parse(format, timeFormat string) ([]part, error) {
	var parts []part
	for format != "" {
		start := strings.Index(format, "${")
		if start < 0 {
			parts = append(parts, literal(format))
			break
		}
		end := strings.IndexByte(format[start:], '}')
		if end < 0 {
			parts = append(parts, literal(format))
			break
		}
		if start > 0 {
			parts = append(parts, literal(format[:start]))
		}
		p, err := tag(format[start+2:start+end], timeFormat)
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
		format = format[start+end+1:]
	}
	return parts, nil
}

func literal(s string) part {
	return func(out []byte, _ *entry) []byte { return append(out, s...) }
}

var pid = strconv.Itoa(os.Getpid())

func tag(name, timeFormat string) (part, error) {
	if key, ok := strings.CutPrefix(name, "reqHeader:"); ok {
		key = textproto.CanonicalMIMEHeaderKey(key)
		return func(out []byte, e *entry) []byte { return appendSafe(out, headerValue(e.r.Header, key)) }, nil
	}
	if key, ok := strings.CutPrefix(name, "respHeader:"); ok {
		key = textproto.CanonicalMIMEHeaderKey(key)
		return func(out []byte, e *entry) []byte { return appendSafe(out, headerValue(e.header, key)) }, nil
	}
	if key, ok := strings.CutPrefix(name, "queryParam:"); ok {
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.URL.Query().Get(key)) }, nil
	}
	switch name {
	case "time":
		return func(out []byte, e *entry) []byte { return e.now.AppendFormat(out, timeFormat) }, nil
	case "status":
		return func(out []byte, e *entry) []byte { return strconv.AppendInt(out, int64(e.status), 10) }, nil
	case "latency":
		return func(out []byte, e *entry) []byte { return append(out, e.latency.String()...) }, nil
	case "ip":
		return func(out []byte, e *entry) []byte { return appendSafe(out, middleware.RemoteIP(e.r)) }, nil
	case "method":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.Method) }, nil
	case "path":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.URL.Path) }, nil
	case "url":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.URL.RequestURI()) }, nil
	case "query":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.URL.RawQuery) }, nil
	case "host":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.Host) }, nil
	case "protocol":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.Proto) }, nil
	case "referer":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.Header.Get("Referer")) }, nil
	case "ua":
		return func(out []byte, e *entry) []byte { return appendSafe(out, e.r.Header.Get("User-Agent")) }, nil
	case "bytesReceived":
		return func(out []byte, e *entry) []byte { return strconv.AppendInt(out, max(e.r.ContentLength, 0), 10) }, nil
	case "bytesSent":
		return func(out []byte, e *entry) []byte { return strconv.AppendInt(out, e.size, 10) }, nil
	case "pid":
		return literal(pid), nil
	}
	return nil, fmt.Errorf("logger: unknown tag ${%s}", name)
}

func headerValue(header stdhttp.Header, key string) string {
	if values := header[key]; len(values) > 0 {
		return values[0]
	}
	return ""
}

// appendSafe appends s, with its control characters escaped.
func appendSafe(out []byte, s string) []byte {
	const hex = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
		if b := s[i]; b < ' ' || b == 0x7f {
			out = append(out, '\\', 'x', hex[b>>4], hex[b&0xf])
		} else {
			out = append(out, b)
		}
	}
	return out
}
