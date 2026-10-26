package log

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// lineHandler writes FormatLine: the time, level and message, then the
// attributes as key=value, a group's keys prefixed with its name and a dot.
// A value of Lines is written below the line, under its key, each of its
// lines indented, so that what it holds cannot pass for a line of its own;
// anything else is written on the line, quoted when it has to be, its control
// characters escaped.
type lineHandler struct {
	out        *output
	options    slog.HandlerOptions
	timeFormat string
	// prefix is the dotted names of the open groups, each with its dot.
	prefix string
	// attrs and blocks are the attributes WithAttrs added, already written:
	// those on the line, and those of Lines below it.
	attrs  []byte
	blocks []byte
}

// output is the writer the handlers made from one another share, with the
// lock that keeps their records whole.
type output struct {
	mu sync.Mutex
	w  io.Writer
}

var lineBuffers = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

func newLineHandler(w io.Writer, options *slog.HandlerOptions, timeFormat string) *lineHandler {
	if timeFormat == "" {
		timeFormat = DefaultTimeFormat
	}
	return &lineHandler{out: &output{w: w}, options: *options, timeFormat: timeFormat}
}

func (h *lineHandler) Enabled(_ context.Context, level slog.Level) bool {
	least := slog.LevelInfo
	if h.options.Level != nil {
		least = h.options.Level.Level()
	}
	return level >= least
}

func (h *lineHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	c := *h
	c.attrs = append([]byte(nil), h.attrs...)
	c.blocks = append([]byte(nil), h.blocks...)
	for _, a := range attrs {
		c.attrs, c.blocks = appendAttr(c.attrs, c.blocks, h.prefix, a)
	}
	return &c
}

func (h *lineHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.prefix = h.prefix + name + "."
	return &c
}

func (h *lineHandler) Handle(_ context.Context, r slog.Record) error {
	buf := lineBuffers.Get().(*[]byte)
	out := (*buf)[:0]
	var blocks []byte
	if !r.Time.IsZero() {
		out = r.Time.AppendFormat(out, h.timeFormat)
		out = append(out, ' ')
	}
	level := r.Level.String()
	out = append(out, level...)
	for i := len(level); i < 5; i++ {
		out = append(out, ' ')
	}
	out = append(out, ' ')
	out = appendEscaped(out, r.Message)
	if h.options.AddSource && r.PC != 0 {
		frame, _ := runtime.CallersFrames([]uintptr{r.PC}).Next()
		if frame.File != "" {
			out = append(out, " source="...)
			out = appendValue(out, frame.File+":"+strconv.Itoa(frame.Line))
		}
	}
	if len(h.attrs) > 0 {
		out = append(out, h.attrs...)
	}
	if r.NumAttrs() > 0 {
		r.Attrs(func(a slog.Attr) bool {
			out, blocks = appendAttr(out, blocks, h.prefix, a)
			return true
		})
	}
	out = append(out, '\n')
	out = append(out, h.blocks...)
	out = append(out, blocks...)
	h.out.mu.Lock()
	_, err := h.out.w.Write(out)
	h.out.mu.Unlock()
	*buf = out
	lineBuffers.Put(buf)
	return err
}

// appendAttr appends a, its key under prefix, to the line, or below it when
// its value is of Lines.
func appendAttr(line, blocks []byte, prefix string, a slog.Attr) ([]byte, []byte) {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		if len(group) == 0 {
			return line, blocks
		}
		if a.Key != "" {
			prefix += a.Key + "."
		}
		for _, g := range group {
			line, blocks = appendAttr(line, blocks, prefix, g)
		}
		return line, blocks
	}
	if a.Key == "" {
		return line, blocks
	}
	var s string
	switch a.Value.Kind() {
	case slog.KindString:
		s = a.Value.String()
	case slog.KindTime:
		line = appendKey(line, prefix, a.Key)
		return a.Value.Time().AppendFormat(line, "2006-01-02T15:04:05.000Z07:00"), blocks
	case slog.KindDuration:
		line = appendKey(line, prefix, a.Key)
		return append(line, a.Value.Duration().String()...), blocks
	case slog.KindAny:
		switch v := a.Value.Any().(type) {
		case Lines:
			return line, appendBlock(blocks, prefix, a.Key, string(v))
		case error:
			s = v.Error()
		default:
			s = a.Value.String()
		}
	default:
		line = appendKey(line, prefix, a.Key)
		return append(line, a.Value.String()...), blocks
	}
	line = appendKey(line, prefix, a.Key)
	return appendValue(line, s), blocks
}

func appendKey(out []byte, prefix, key string) []byte {
	out = append(out, ' ')
	out = appendValue(out, prefix+key)
	return append(out, '=')
}

// appendValue appends s, quoted when it is empty or has a space, a quote, an
// equals sign or anything not printable, so that the pairs can be told apart.
func appendValue(out []byte, s string) []byte {
	if needsQuote(s) {
		return strconv.AppendQuote(out, s)
	}
	return append(out, s...)
}

func needsQuote(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			if b <= ' ' || b == '"' || b == '=' || b == 0x7f {
				return true
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError || !strconv.IsPrint(r) {
			return true
		}
		i += size
	}
	return false
}

// appendBlock appends a value of several lines: its key on a line of its own,
// indented once, and each of its lines below, indented twice.
func appendBlock(out []byte, prefix, key, s string) []byte {
	out = append(out, '\t')
	out = appendEscaped(out, prefix+key)
	out = append(out, ":\n"...)
	s = strings.TrimSuffix(s, "\n")
	for {
		line, rest, more := strings.Cut(s, "\n")
		out = append(out, '\t', '\t')
		out = appendEscaped(out, line)
		out = append(out, '\n')
		if !more {
			return out
		}
		s = rest
	}
}

// appendEscaped appends s with its control characters, but for tabs,
// escaped.
func appendEscaped(out []byte, s string) []byte {
	const hex = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
		if b := s[i]; (b < ' ' && b != '\t') || b == 0x7f {
			out = append(out, '\\', 'x', hex[b>>4], hex[b&0xf])
		} else {
			out = append(out, b)
		}
	}
	return out
}
