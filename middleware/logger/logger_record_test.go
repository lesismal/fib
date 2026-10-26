//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package logger_test

import (
	"encoding/json"
	"log/slog"
	stdhttp "net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/middleware/logger"
	"github.com/lesismal/fib/middleware/mwtest"
)

func okHandler(c *fibhttp.Context) { _ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok")) }

// TestLoggerRecordDefault checks that with no Format nor Output a response
// is a record of fib/log, in the format fib/log was set up with after the
// middleware was built.
func TestLoggerRecordDefault(t *testing.T) {
	handler := logger.New()(fibhttp.HandlerFunc(okHandler))
	url := mwtest.Serve(t, handler)
	out := &syncBuffer{}
	log.Setup(log.Config{Output: out})
	t.Cleanup(func() { log.SetLogger(nil) })
	mwtest.Clients(t)[0].Do(t, "GET", url+"/a%0Afake%20line", nil, "")
	// The engine may log its settings to the same logger as it starts.
	var got []string
	for deadline := time.Now().Add(5 * time.Second); len(got) == 0 && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		got = got[:0]
		for _, line := range out.lines() {
			if strings.Contains(line, logger.Message) {
				got = append(got, line)
			}
		}
	}
	re := regexp.MustCompile(`^\S+ \S+ INFO  http: access status=200 latency=\S+ ip=127\.0\.0\.1 method=GET path="/a\\nfake line"$`)
	if len(got) != 1 || !re.MatchString(got[0]) {
		t.Errorf("logged %q", got)
	}
}

// TestLoggerRecordJSON checks Fields, Level and a Logger of the middleware's
// own, writing JSON.
func TestLoggerRecordJSON(t *testing.T) {
	out := &syncBuffer{}
	handler := logger.New(logger.Config{
		Fields: []string{"status", "bytesSent", "reqHeader:X-Test", "url"},
		Level:  slog.LevelWarn,
		Logger: log.New(log.Config{Format: log.FormatJSON, Output: out}),
	})(fibhttp.HandlerFunc(okHandler))
	url := mwtest.Serve(t, handler)
	mwtest.Clients(t)[0].Do(t, "GET", url+"/p?q=1", stdhttp.Header{"X-Test": {"t"}}, "")
	got := waitLines(t, out, 1)
	if len(got) != 1 {
		t.Fatalf("logged %q", got)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(got[0]), &m); err != nil {
		t.Fatalf("%q: %v", got[0], err)
	}
	want := map[string]any{"level": "WARN", "msg": logger.Message, "status": float64(200),
		"bytesSent": float64(2), "reqHeader:X-Test": "t", "url": "/p?q=1"}
	for k, w := range want {
		if m[k] != w {
			t.Errorf("%s = %v, want %v", k, m[k], w)
		}
	}
	if _, ok := m["time"]; !ok || len(m) != len(want)+1 {
		t.Errorf("logged %v", m)
	}
}

// TestLoggerRecordBelowLevel checks that a logger that does not take the
// level writes nothing.
func TestLoggerRecordBelowLevel(t *testing.T) {
	out := &syncBuffer{}
	handler := logger.New(logger.Config{
		Level:  slog.LevelDebug,
		Logger: log.New(log.Config{Output: out}),
	})(fibhttp.HandlerFunc(okHandler))
	url := mwtest.Serve(t, handler)
	mwtest.Clients(t)[0].Do(t, "GET", url, nil, "")
	if got := strings.Join(out.lines(), ""); got != "" {
		t.Errorf("logged %q below the logger's level", got)
	}
}

func TestLoggerRecordBadFields(t *testing.T) {
	for _, fields := range [][]string{{"time"}, {"nope"}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Fields %q did not panic", fields)
				}
			}()
			logger.New(logger.Config{Fields: fields})
		}()
	}
}
