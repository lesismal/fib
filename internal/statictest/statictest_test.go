//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package statictest

import (
	"fmt"
	stdhttp "net/http"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/lesismal/fib/middleware/mwtest"
)

// fatalTB records the failure a helper reports, and ends the goroutine it
// was called on as testing.T.Fatal does.
type fatalTB struct {
	testing.TB
	msg string
}

func (tb *fatalTB) Helper() {}

func (tb *fatalTB) Fatal(args ...any) {
	tb.msg = fmt.Sprint(args...)
	runtime.Goexit()
}

// Handler fails the test when the directory it is to serve is not there.
func TestHandlerWithoutADirectory(t *testing.T) {
	tb := &fatalTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		Handler(tb, filepath.Join(t.TempDir(), "missing"))
	}()
	<-done
	if tb.msg == "" {
		t.Fatal("Handler served a directory that is not there")
	}
}

// Setup writes every file it lists, and Handler answers a path outside its
// four routes with 404.
func TestSetupAndUnknownRoute(t *testing.T) {
	dir, files := Setup(t)
	url := mwtest.Serve(t, Handler(t, dir))
	c := mwtest.Clients(t)[0]
	if resp, _ := c.Do(t, "GET", url+"/other/small.txt", nil, ""); resp.StatusCode != stdhttp.StatusNotFound {
		t.Errorf("an unknown route: status %d, want 404", resp.StatusCode)
	}
	if resp, body := c.Do(t, "GET", url+"/copy/small.txt", nil, ""); resp.StatusCode != stdhttp.StatusOK || body != string(files["small.txt"]) {
		t.Errorf("/copy/small.txt: status %d, %q", resp.StatusCode, body)
	}
	if len(files) != 4 || len(files["big.bin"]) != BigSize || len(files["mid.bin"]) != MidSize {
		t.Errorf("Setup wrote %d files", len(files))
	}
}
