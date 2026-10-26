package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"testing/slogtest"
	"time"
)

// TestLineHandlerSlogtest runs slog's own conformance test on FormatLine,
// reading the lines back into maps.
func TestLineHandlerSlogtest(t *testing.T) {
	var buf bytes.Buffer
	h := NewHandler(Config{Output: &buf})
	results := func() []map[string]any {
		var ms []map[string]any
		for _, rec := range splitRecords(buf.String()) {
			m, err := parseLine(rec)
			if err != nil {
				t.Fatalf("%q: %v", rec, err)
			}
			ms = append(ms, m)
		}
		return ms
	}
	if err := slogtest.TestHandler(h, results); err != nil {
		t.Fatal(err)
	}
}

// splitRecords splits what the line handler wrote into records: a line and
// the indented lines of its values below it.
func splitRecords(s string) []string {
	var recs []string
	for _, line := range strings.SplitAfter(s, "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "\t") && len(recs) > 0 {
			recs[len(recs)-1] += line
			continue
		}
		recs = append(recs, line)
	}
	return recs
}

// parseLine reads a record of FormatLine back, its dotted keys as nested
// maps. It takes a message of one word, which is what slogtest logs.
func parseLine(rec string) (map[string]any, error) {
	m := map[string]any{}
	head, blocks, _ := strings.Cut(rec, "\n")
	if len(head) >= len(DefaultTimeFormat) {
		if ts, err := time.ParseInLocation(DefaultTimeFormat, head[:len(DefaultTimeFormat)], time.Local); err == nil {
			m[slog.TimeKey] = ts
			head = head[len(DefaultTimeFormat)+1:]
		}
	}
	level, rest, _ := strings.Cut(head, " ")
	m[slog.LevelKey] = level
	rest = strings.TrimLeft(rest, " ")
	msg, rest, _ := strings.Cut(rest, " ")
	m[slog.MessageKey] = msg
	for rest != "" {
		key, value, n, err := nextPair(rest)
		if err != nil {
			return nil, err
		}
		put(m, key, value)
		rest = strings.TrimPrefix(rest[n:], " ")
	}
	var key string
	var lines []string
	flush := func() {
		if key != "" {
			put(m, key, strings.Join(lines, "\n"))
		}
	}
	for _, line := range strings.Split(strings.TrimSuffix(blocks, "\n"), "\n") {
		switch {
		case line == "":
		case strings.HasPrefix(line, "\t\t"):
			lines = append(lines, line[2:])
		case strings.HasPrefix(line, "\t"):
			flush()
			key, lines = strings.TrimSuffix(line[1:], ":"), nil
		}
	}
	flush()
	return m, nil
}

// nextPair reads one key=value from the front of s and says how long it was.
func nextPair(s string) (key, value string, n int, err error) {
	key, n, err = nextToken(s)
	if err != nil {
		return "", "", 0, err
	}
	if n >= len(s) || s[n] != '=' {
		return "", "", 0, fmt.Errorf("no = after %q", key)
	}
	value, m, err := nextToken(s[n+1:])
	return key, value, n + 1 + m, err
}

func nextToken(s string) (string, int, error) {
	if strings.HasPrefix(s, `"`) {
		q, err := strconv.QuotedPrefix(s)
		if err != nil {
			return "", 0, err
		}
		v, err := strconv.Unquote(q)
		return v, len(q), err
	}
	n := strings.IndexAny(s, " =")
	if n < 0 {
		n = len(s)
	}
	return s[:n], n, nil
}

func put(m map[string]any, key string, value any) {
	parts := strings.Split(key, ".")
	for _, p := range parts[:len(parts)-1] {
		sub, ok := m[p].(map[string]any)
		if !ok {
			sub = map[string]any{}
			m[p] = sub
		}
		m = sub
	}
	m[parts[len(parts)-1]] = value
}

func TestLineLayout(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{Output: &buf, TimeFormat: "15:04"})
	l.Info("fib: engine started", "engine", "fib", "listen", "tcp://[::]:8080", "pollers", 4,
		"empty", "", "spaced", "a b", "latency", 1500*time.Millisecond, "error", errors.New("boom"))
	got := buf.String()
	want := ` INFO  fib: engine started engine=fib listen=tcp://[::]:8080 pollers=4 empty="" spaced="a b" latency=1.5s error=boom` + "\n"
	if len(got) < 5 || got[5:] != want {
		t.Fatalf("got %q, want the time then %q", got, want)
	}
}

// TestLineCannotBeForged pins down that what a value holds, a line break
// among it, never starts a line of its own: a string's is escaped, and the
// lines of Lines are indented.
func TestLineCannotBeForged(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{Output: &buf})
	l.Error("panicked", "path", "/a\nb", "stack", Lines("goroutine 1 [running]:\n\tmain.main()\n2026-01-01 00:00:00.000 INFO forged\n"))
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 5 {
		t.Fatalf("got %d lines, want 5:\n%s", len(lines), buf.String())
	}
	if !strings.HasSuffix(lines[0], `ERROR panicked path="/a\nb"`) {
		t.Fatalf("first line %q", lines[0])
	}
	want := []string{"\tstack:", "\t\tgoroutine 1 [running]:", "\t\t\tmain.main()", "\t\t2026-01-01 00:00:00.000 INFO forged"}
	for i, w := range want {
		if lines[i+1] != w {
			t.Fatalf("line %d is %q, want %q", i+1, lines[i+1], w)
		}
	}
}

func TestLineWithAttrsAndGroups(t *testing.T) {
	var buf bytes.Buffer
	l := New(Config{Output: &buf}).With("engine", "fib", "trace", Lines("a\nb")).WithGroup("req").With("id", 7)
	l.Info("m", "path", "/", slog.Group("peer", "ip", "1.2.3.4"))
	head, blocks, _ := strings.Cut(buf.String(), "\n")
	if !strings.HasSuffix(head, " m engine=fib req.id=7 req.path=/ req.peer.ip=1.2.3.4") {
		t.Fatalf("line %q", head)
	}
	if blocks != "\ttrace:\n\t\ta\n\t\tb\n" {
		t.Fatalf("blocks %q", blocks)
	}
}

func TestFormats(t *testing.T) {
	var buf bytes.Buffer
	New(Config{Format: FormatJSON, Output: &buf}).Info("hello", "n", 1)
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("%q: %v", buf.String(), err)
	}
	if m["msg"] != "hello" || m["n"] != float64(1) || m["level"] != "INFO" {
		t.Fatalf("got %v", m)
	}
	buf.Reset()
	New(Config{Format: FormatText, Output: &buf}).Info("hello", "n", 1)
	if !strings.Contains(buf.String(), `level=INFO msg=hello n=1`) {
		t.Fatalf("text %q", buf.String())
	}
}

func TestParseFormat(t *testing.T) {
	for _, f := range []Format{FormatLine, FormatText, FormatJSON} {
		got, err := ParseFormat(" " + strings.ToUpper(f.String()) + " ")
		if err != nil || got != f {
			t.Fatalf("ParseFormat(%q) = %v, %v", f.String(), got, err)
		}
	}
	if _, err := ParseFormat("xml"); err == nil {
		t.Fatal("xml parsed")
	}
	if s := Format(9).String(); s != "Format(9)" {
		t.Fatal(s)
	}
}

func TestLevel(t *testing.T) {
	var buf bytes.Buffer
	level := new(slog.LevelVar)
	level.Set(slog.LevelWarn)
	defer SetLogger(nil)
	Setup(Config{Output: &buf, Level: level})
	Info("dropped")
	Warn("kept")
	if Enabled(slog.LevelInfo) || !Enabled(slog.LevelWarn) {
		t.Fatal("Enabled does not follow the level")
	}
	level.Set(slog.LevelDebug)
	Debug("debug")
	Error("error")
	got := buf.String()
	if strings.Contains(got, "dropped") || !strings.Contains(got, "WARN  kept") ||
		!strings.Contains(got, "DEBUG debug") || !strings.Contains(got, "ERROR error") {
		t.Fatalf("got %q", got)
	}
}

func TestSetLoggerNilRestoresDefault(t *testing.T) {
	SetLogger(slog.New(slog.DiscardHandler))
	SetLogger(nil)
	if Logger() != slog.Default() {
		t.Fatal("SetLogger(nil) did not restore slog.Default")
	}
}

// TestSourceIsTheCaller pins down that a record logged through Info names
// the line that called Info, not this package.
func TestSourceIsTheCaller(t *testing.T) {
	for _, f := range []Format{FormatLine, FormatJSON} {
		var buf bytes.Buffer
		Setup(Config{Format: f, Output: &buf, AddSource: true})
		Info("here")
		SetLogger(nil)
		if !strings.Contains(buf.String(), "log_test.go") {
			t.Fatalf("%v: %q names no caller", f, buf.String())
		}
	}
}

func TestFromEnv(t *testing.T) {
	if l, err := fromEnv("", " "); l != nil || err != nil {
		t.Fatalf("unset: %v, %v", l, err)
	}
	if _, err := fromEnv("yaml", ""); err == nil {
		t.Fatal("bad format taken")
	}
	if _, err := fromEnv("", "loud"); err == nil {
		t.Fatal("bad level taken")
	}
	l, err := fromEnv("json", "warn")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := l.Handler().(*slog.JSONHandler); !ok {
		t.Fatalf("handler %T", l.Handler())
	}
	if l.Enabled(t.Context(), slog.LevelInfo) || !l.Enabled(t.Context(), slog.LevelWarn) {
		t.Fatal("level not taken")
	}
}

func TestTable(t *testing.T) {
	got := Table([]string{"name", "value", "about"}, [][]string{{"a", "1", "x y"}, {"long", "été", ""}})
	want := "name  value  about\na     1      x y\nlong  été    \n"
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestIsLine(t *testing.T) {
	l := New(Config{}).With("a", 1).WithGroup("g")
	if !IsLine(l.Handler()) || IsLine(New(Config{Format: FormatText}).Handler()) || IsLine(slog.Default().Handler()) {
		t.Fatal("IsLine does not tell FormatLine apart")
	}
}
