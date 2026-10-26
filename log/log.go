// Package log is where every package of fib logs: the engine, the task
// pools, arpc, grpc and the HTTP middleware all write through the logger it
// holds, so that one call decides how all of fib's lines look.
//
// Until Setup or SetLogger is called, the logger is slog.Default, so that an
// application that sets slog's default logger gets fib's lines too. The
// environment may choose fib's own logger instead before the program
// configures anything: FIB_LOG_FORMAT picks the format (line, text or json)
// and FIB_LOG_LEVEL the least level written (debug, info, warn or error, or a
// number as slog.Level parses it).
//
// The formats are:
//
//	line  2026-10-27 10:00:00.000 INFO  fib: engine started engine=fib listen=tcp://[::]:8080
//	text  time=2026-10-27T10:00:00.000+08:00 level=INFO msg="fib: engine started" engine=fib
//	json  {"time":"2026-10-27T10:00:00.000+08:00","level":"INFO","msg":"fib: engine started","engine":"fib"}
//
// line is for people reading a terminal or a file; text and json are
// structured, one record a line, for programs collecting the logs.
package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Format is how a record is laid out.
type Format int

const (
	// FormatLine writes the time, level and message unlabelled, then the
	// attributes as key=value; a value of Lines goes on the lines below it,
	// indented.
	FormatLine Format = iota
	// FormatText writes the record as slog.TextHandler does: key=value
	// pairs, the time, level and message among them.
	FormatText
	// FormatJSON writes the record as slog.JSONHandler does: a JSON object
	// a line.
	FormatJSON
)

// String returns the name ParseFormat takes.
func (f Format) String() string {
	switch f {
	case FormatLine:
		return "line"
	case FormatText:
		return "text"
	case FormatJSON:
		return "json"
	}
	return fmt.Sprintf("Format(%d)", int(f))
}

// ParseFormat returns the format named name: line, text or json, in any case.
func ParseFormat(name string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "line":
		return FormatLine, nil
	case "text":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	}
	return 0, fmt.Errorf("log: unknown format %q: want line, text or json", name)
}

// Lines is a value of several lines, such as a stack. FormatLine writes it
// below the record's line, under its key, each of its lines indented; the
// structured formats write it as any other string. A string that is not of
// Lines is written on the line, its line breaks escaped.
type Lines string

// Table lays rows out under header as Lines, each column as wide as its
// widest cell, two spaces apart; the last column is not padded.
func Table(header []string, rows [][]string) Lines {
	widths := make([]int, len(header))
	for _, row := range append([][]string{header}, rows...) {
		for i, cell := range row[:min(len(row), len(widths))] {
			widths[i] = max(widths[i], utf8.RuneCountInString(cell))
		}
	}
	var b strings.Builder
	for _, row := range append([][]string{header}, rows...) {
		for i, cell := range row[:min(len(row), len(widths))] {
			b.WriteString(cell)
			if i < len(row)-1 && i < len(widths)-1 {
				b.WriteString(strings.Repeat(" ", widths[i]-utf8.RuneCountInString(cell)+2))
			}
		}
		b.WriteByte('\n')
	}
	return Lines(b.String())
}

// IsLine reports whether h writes FormatLine, so that what logs a list may
// write it there as one record with a Table rather than a record an item.
func IsLine(h slog.Handler) bool {
	_, ok := h.(*lineHandler)
	return ok
}

// DefaultTimeFormat is how FormatLine writes a record's time by default.
const DefaultTimeFormat = "2006-01-02 15:04:05.000"

// Config is what New builds a logger from. Its zero value writes lines at
// slog.LevelInfo and above to os.Stderr.
type Config struct {
	// Format is how a record is laid out; FormatLine by default.
	Format Format
	// Level is the least level written; slog.LevelInfo when nil. A
	// *slog.LevelVar changes it while the logger runs.
	Level slog.Leveler
	// Output is where the records go, one Write each; os.Stderr by default.
	Output io.Writer
	// TimeFormat is how FormatLine writes the time, in the layout of
	// package time; DefaultTimeFormat by default. The structured formats
	// write RFC 3339 times whatever it says.
	TimeFormat string
	// AddSource adds the file and line that logged the record.
	AddSource bool
}

// NewHandler returns the handler a logger of config writes through.
func NewHandler(config Config) slog.Handler {
	out := config.Output
	if out == nil {
		out = os.Stderr
	}
	options := &slog.HandlerOptions{Level: config.Level, AddSource: config.AddSource}
	switch config.Format {
	case FormatText:
		return slog.NewTextHandler(out, options)
	case FormatJSON:
		return slog.NewJSONHandler(out, options)
	default:
		return newLineHandler(out, options, config.TimeFormat)
	}
}

// New returns a logger of config.
func New(config Config) *slog.Logger { return slog.New(NewHandler(config)) }

// Setup has fib log as config says.
func Setup(config Config) { SetLogger(New(config)) }

var loggerValue atomic.Pointer[slog.Logger]

// SetLogger has fib log to l, whatever handler it has. Nil goes back to
// slog.Default.
func SetLogger(l *slog.Logger) { loggerValue.Store(l) }

// Logger returns the logger fib writes to.
func Logger() *slog.Logger {
	if l := loggerValue.Load(); l != nil {
		return l
	}
	return slog.Default()
}

func init() {
	if l, err := fromEnv(os.Getenv("FIB_LOG_FORMAT"), os.Getenv("FIB_LOG_LEVEL")); err != nil {
		fmt.Fprintln(os.Stderr, "fib:", err)
	} else if l != nil {
		SetLogger(l)
	}
}

// fromEnv returns the logger FIB_LOG_FORMAT and FIB_LOG_LEVEL ask for: nil
// when neither is set, and none at all when either is wrong.
func fromEnv(format, level string) (*slog.Logger, error) {
	if strings.TrimSpace(format) == "" && strings.TrimSpace(level) == "" {
		return nil, nil
	}
	var config Config
	if strings.TrimSpace(format) != "" {
		f, err := ParseFormat(format)
		if err != nil {
			return nil, fmt.Errorf("FIB_LOG_FORMAT: %w", err)
		}
		config.Format = f
	}
	if strings.TrimSpace(level) != "" {
		var l slog.Level
		if err := l.UnmarshalText([]byte(strings.TrimSpace(level))); err != nil {
			return nil, fmt.Errorf("FIB_LOG_LEVEL: %w", err)
		}
		config.Level = l
	}
	return New(config), nil
}

// Enabled reports whether the logger writes records of level.
func Enabled(level slog.Level) bool { return Logger().Enabled(context.Background(), level) }

// Debug logs msg at slog.LevelDebug, with args as slog.Logger.Log takes them.
func Debug(msg string, args ...any) { logAt(slog.LevelDebug, msg, args) }

// Info logs msg at slog.LevelInfo.
func Info(msg string, args ...any) { logAt(slog.LevelInfo, msg, args) }

// Warn logs msg at slog.LevelWarn.
func Warn(msg string, args ...any) { logAt(slog.LevelWarn, msg, args) }

// Error logs msg at slog.LevelError.
func Error(msg string, args ...any) { logAt(slog.LevelError, msg, args) }

// logAt logs a record whose source, when the handler asks for one, is the
// caller of Debug, Info, Warn or Error rather than this package.
func logAt(level slog.Level, msg string, args []any) {
	l := Logger()
	ctx := context.Background()
	if !l.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // runtime.Callers, logAt, and Debug, Info, Warn or Error
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(args...)
	_ = l.Handler().Handle(ctx, r)
}
