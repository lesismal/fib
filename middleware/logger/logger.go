//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Package logger logs every response, once it has been handed to the
// connection.
//
// By default each response is a record of package fib/log, written as the
// rest of fib's logs are, in the format fib/log is set up with:
//
//	2026-10-27 10:00:00.000 INFO  http: access status=200 latency=1.2ms ip=127.0.0.1 method=GET path=/
//	{"time":"...","level":"INFO","msg":"http: access","status":200,"latency":1200000,"ip":"127.0.0.1","method":"GET","path":"/"}
//
// Config.Fields names what a record carries, from these tags:
//
//	status             the response's status
//	latency            from the request reaching the middleware to its response going out
//	ip                 the client's address, without its port
//	method             the request's method
//	path               the request's path
//	url                the request's target: its path and query
//	query              the request's query, without the "?"
//	host               the request's host
//	protocol           the request's protocol, such as HTTP/1.1
//	referer            the request's Referer header
//	ua                 the request's User-Agent header
//	bytesReceived      the length of the request's body, when it has one
//	bytesSent          the length of the response's body
//	pid                the process's ID
//	reqHeader:Name     the request's header Name
//	respHeader:Name    the response's header Name
//	queryParam:name    the request's query parameter name
//
// Config.Format lays out a line of its own instead, written straight to
// Config.Output rather than to fib/log, with each tag as ${tag} and ${time}
// for when the line is written, in Config.TimeFormat. Anything else in Format
// is written as it is. Control characters in what a tag stands for are
// escaped, there and in fib/log's formats alike, so that a request cannot
// forge a line.
//
// A request whose response is never written, such as one whose connection
// went before it was answered, has no record.
package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	stdhttp "net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/middleware"
)

// Message is the message of the records the middleware logs.
const Message = "http: access"

// DefaultFields are what a record carries by default.
var DefaultFields = []string{"status", "latency", "ip", "method", "path"}

// DefaultFormat is the line Format lays out when only Output is set.
const DefaultFormat = "${time} | ${status} | ${latency} | ${ip} | ${method} | ${path}\n"

// DefaultTimeFormat is how ${time} is written by default.
const DefaultTimeFormat = "15:04:05"

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Fields names what a record carries, from the tags of the package's
	// documentation, each as an attribute of the tag's name; DefaultFields
	// by default.
	Fields []string
	// Level is the level the records are logged at; slog.LevelInfo, its
	// zero value, by default.
	Level slog.Level
	// Logger is where the records go; by default the logger of package
	// fib/log as it is when each record is written, so that setting that
	// up later still takes.
	Logger *slog.Logger

	// Format lays out a line of its own, written to Output instead of a
	// record. Setting Format or Output selects it; Format is DefaultFormat
	// when only Output is set.
	Format string
	// TimeFormat is how ${time} is written, in the layout of package time;
	// DefaultTimeFormat by default.
	TimeFormat string
	// Output is where the lines of Format go, one Write at a time;
	// os.Stdout when only Format is set.
	Output io.Writer
}

// entry is what a record or line is written from.
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

// source gives what a tag stands for.
type source func(e *entry) slog.Value

// New returns the middleware. It panics if Fields or Format has a tag it does
// not know.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	var write func(*entry)
	if cfg.Format != "" || cfg.Output != nil {
		write = lineWriter(cfg)
	} else {
		write = recordWriter(cfg)
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			if cfg.Next != nil && cfg.Next(c) {
				next.ServeHTTP(c)
				return
			}
			start := time.Now()
			c.OnFinish(func(status int, header stdhttp.Header, size int64) {
				now := time.Now()
				write(&entry{r: c.Request, status: status, header: header, size: size, latency: now.Sub(start), now: now})
			})
			next.ServeHTTP(c)
		})
	}
}

// field is an attribute of a record: its key and where its value comes from.
type field struct {
	key    string
	source source
}

// recordWriter logs a record for each response.
func recordWriter(cfg Config) func(*entry) {
	names := cfg.Fields
	if names == nil {
		names = DefaultFields
	}
	fields := make([]field, len(names))
	for i, name := range names {
		if name == "time" {
			panic("logger: a record has its own time; leave time out of Fields")
		}
		src, err := tag(name)
		if err != nil {
			panic(err)
		}
		fields[i] = field{key: name, source: src}
	}
	level := cfg.Level
	return func(e *entry) {
		l := cfg.Logger
		if l == nil {
			l = log.Logger()
		}
		ctx := context.Background()
		if !l.Enabled(ctx, level) {
			return
		}
		r := slog.NewRecord(e.now, level, Message, 0)
		for _, f := range fields {
			r.AddAttrs(slog.Attr{Key: f.key, Value: f.source(e)})
		}
		_ = l.Handler().Handle(ctx, r)
	}
}

// lineWriter writes a line of Format for each response.
func lineWriter(cfg Config) func(*entry) {
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
	return func(e *entry) {
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
		name := format[start+2 : start+end]
		if name == "time" {
			parts = append(parts, func(out []byte, e *entry) []byte { return e.now.AppendFormat(out, timeFormat) })
		} else {
			src, err := tag(name)
			if err != nil {
				return nil, err
			}
			parts = append(parts, func(out []byte, e *entry) []byte { return appendValue(out, src(e)) })
		}
		format = format[start+end+1:]
	}
	return parts, nil
}

func literal(s string) part {
	return func(out []byte, _ *entry) []byte { return append(out, s...) }
}

var pid = os.Getpid()

func str(s string) slog.Value { return slog.StringValue(s) }

// tag returns where what the tag name stands for comes from.
func tag(name string) (source, error) {
	if key, ok := strings.CutPrefix(name, "reqHeader:"); ok {
		key = textproto.CanonicalMIMEHeaderKey(key)
		return func(e *entry) slog.Value { return str(headerValue(e.r.Header, key)) }, nil
	}
	if key, ok := strings.CutPrefix(name, "respHeader:"); ok {
		key = textproto.CanonicalMIMEHeaderKey(key)
		return func(e *entry) slog.Value { return str(headerValue(e.header, key)) }, nil
	}
	if key, ok := strings.CutPrefix(name, "queryParam:"); ok {
		return func(e *entry) slog.Value { return str(e.r.URL.Query().Get(key)) }, nil
	}
	switch name {
	case "status":
		return func(e *entry) slog.Value { return slog.IntValue(e.status) }, nil
	case "latency":
		return func(e *entry) slog.Value { return slog.DurationValue(e.latency) }, nil
	case "ip":
		return func(e *entry) slog.Value { return str(middleware.RemoteIP(e.r)) }, nil
	case "method":
		return func(e *entry) slog.Value { return str(e.r.Method) }, nil
	case "path":
		return func(e *entry) slog.Value { return str(e.r.URL.Path) }, nil
	case "url":
		return func(e *entry) slog.Value { return str(e.r.URL.RequestURI()) }, nil
	case "query":
		return func(e *entry) slog.Value { return str(e.r.URL.RawQuery) }, nil
	case "host":
		return func(e *entry) slog.Value { return str(e.r.Host) }, nil
	case "protocol":
		return func(e *entry) slog.Value { return str(e.r.Proto) }, nil
	case "referer":
		return func(e *entry) slog.Value { return str(e.r.Header.Get("Referer")) }, nil
	case "ua":
		return func(e *entry) slog.Value { return str(e.r.Header.Get("User-Agent")) }, nil
	case "bytesReceived":
		return func(e *entry) slog.Value { return slog.Int64Value(max(e.r.ContentLength, 0)) }, nil
	case "bytesSent":
		return func(e *entry) slog.Value { return slog.Int64Value(e.size) }, nil
	case "pid":
		return func(*entry) slog.Value { return slog.IntValue(pid) }, nil
	}
	return nil, fmt.Errorf("logger: unknown tag %q", name)
}

// appendValue appends v to a line of Format, a string with its control
// characters escaped.
func appendValue(out []byte, v slog.Value) []byte {
	switch v.Kind() {
	case slog.KindInt64:
		return strconv.AppendInt(out, v.Int64(), 10)
	case slog.KindDuration:
		return append(out, v.Duration().String()...)
	}
	return appendSafe(out, v.String())
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
