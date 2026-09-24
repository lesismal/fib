//go:build linux || darwin || windows

package logger_test

import (
	"bytes"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware/internal/mwtest"
	"github.com/lesismal/fib/go/middleware/logger"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Split(strings.TrimSuffix(b.buf.String(), "\n"), "\n")
}

func TestLogger(t *testing.T) {
	out := &syncBuffer{}
	handler := logger.New(logger.Config{
		Format: "${status} ${method} ${url} ${protocol} ${bytesSent} ${reqHeader:X-Test} ${respHeader:X-Reply} ${queryParam:q} ${ip}\n",
		Output: out,
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.WriteResponse(fibhttp.Response{
			StatusCode: stdhttp.StatusCreated,
			Header:     stdhttp.Header{"X-Reply": {"yes"}},
			Body:       []byte("hello"),
		})
	}))
	url := mwtest.Serve(t, handler)
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		c.Do(t, "GET", url+"/a%0Ab?q=1", stdhttp.Header{"X-Test": {"t"}}, "")
	})
	want := []string{
		`201 GET /a%0Ab?q=1 HTTP/1.1 5 t yes 1 127.0.0.1`,
		`201 GET /a%0Ab?q=1 HTTP/2.0 5 t yes 1 127.0.0.1`,
	}
	// A line is written once the response is handed to the connection,
	// which can be after the client has it.
	var got []string
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if got = out.lines(); len(got) == len(want) {
			break
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("logged\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestLoggerEscapes(t *testing.T) {
	out := &syncBuffer{}
	handler := logger.New(logger.Config{Format: "${path}\n", Output: out})(fibhttp.HandlerFunc(
		func(c *fibhttp.Context, _ *stdhttp.Request) { _ = c.Respond(stdhttp.StatusOK, "", nil) }))
	url := mwtest.Serve(t, handler)
	c := mwtest.Clients(t)[0]
	c.Do(t, "GET", url+"/a%0Afake%20line", nil, "")
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if lines := out.lines(); lines[0] != "" {
			break
		}
	}
	if got := out.lines(); len(got) != 1 || got[0] != `/a\x0afake line` {
		t.Errorf("logged %q", got)
	}
}

func TestLoggerUnknownTag(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("an unknown tag did not panic")
		}
	}()
	logger.New(logger.Config{Format: "${nope}"})
}
