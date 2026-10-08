//go:build linux || darwin || windows

package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func writeFile(t *testing.T, n int) (path string, data []byte) {
	t.Helper()
	data = make([]byte, n)
	rng := rand.New(rand.NewPCG(9, 9))
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	path = filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

// startFib serves the upload endpoints with the fib http package the real
// server uses, into a fresh directory, and returns its URL and directory.
func startFib(t *testing.T) (base, dir string) {
	t.Helper()
	dir = t.TempDir()
	handler := fibhttp.HandlerFunc(func(c *fibhttp.Context, r *http.Request) {
		path := filepath.Join(dir, filepath.Base(c.Query("name")))
		switch r.URL.Path {
		case "/upload":
			c.SaveBody(path, nil)
		case "/resume":
			c.SaveBodyResumable(path, nil)
		case "/echo":
			c.OnBody(func(data []byte, fin bool, err error) {
				if err == nil {
					_, _ = c.Write(data)
					if fin {
						_ = c.Finish()
					}
				}
			})
		}
	})
	config := fibhttp.DefaultConfig()
	config.StreamRequestBody = true
	engineConfig := fib.DefaultConfig()
	engineConfig.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(engineConfig, fibhttp.NewHandlerWithConfig(config, handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := engine.LocalAddr()
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return "http://" + addr.String(), dir
}

func runClient(args ...string) (string, error) {
	var out bytes.Buffer
	err := run(args, &out)
	return out.String(), err
}

func TestUploadEchoAndResumeAgainstTheServer(t *testing.T) {
	base, dir := startFib(t)
	path, data := writeFile(t, 6<<20+17)
	want := fmt.Sprintf("bytes=%d sha256=%x", len(data), sha256.Sum256(data))

	out, err := runClient("-url", base, "-mode", "upload", "-file", path, "-name", "up.bin")
	if err != nil || !strings.Contains(out, want) {
		t.Fatalf("upload: %q, %v", out, err)
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "up.bin")); !bytes.Equal(stored, data) {
		t.Fatal("uploaded file differs")
	}

	out, err = runClient("-url", base, "-mode", "echo", "-file", path)
	if err != nil || !strings.Contains(out, fmt.Sprintf("%d bytes", len(data))) {
		t.Fatalf("echo: %q, %v", out, err)
	}

	// An interrupted resume, then the rest from where it stopped.
	out, err = runClient("-url", base, "-mode", "resume", "-file", path, "-name", "re.bin", "-chunk", "1000000", "-max-chunks", "2")
	if err != nil || !strings.Contains(out, "stopping early") || !strings.Contains(out, "the server has 2000000 of") {
		t.Fatalf("first resume: %q, %v", out, err)
	}
	out, err = runClient("-url", base, "-mode", "resume", "-file", path, "-name", "re.bin", "-chunk", "1000000")
	if err != nil || !strings.Contains(out, "the server has 2000000 of") || !strings.Contains(out, want) {
		t.Fatalf("second resume: %q, %v", out, err)
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "re.bin")); !bytes.Equal(stored, data) {
		t.Fatal("resumed file differs")
	}
	// Run again: the server already has all of it, and says so.
	out, err = runClient("-url", base, "-mode", "resume", "-file", path, "-name", "re.bin")
	if err != nil || !strings.Contains(out, want) {
		t.Fatalf("resume of a finished upload: %q, %v", out, err)
	}
}

// A server that has more than the client thought, or a different part of it,
// answers 409 with the offset to go on from, which the client takes.
func TestResumeFollowsTheServersOffset(t *testing.T) {
	path, data := writeFile(t, 300)
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Content-Range"))
		switch len(seen) {
		case 1: // the probe: nothing stored
			w.Header().Set("Upload-Offset", "0")
			w.WriteHeader(http.StatusAccepted)
		case 2: // the first chunk is not where the server is
			w.Header().Set("Upload-Offset", "100")
			w.WriteHeader(http.StatusConflict)
		default:
			io.Copy(io.Discard, r.Body)
			fmt.Fprintf(w, "stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data))
		}
	}))
	defer server.Close()
	out, err := runClient("-url", server.URL, "-mode", "resume", "-file", path, "-chunk", "150")
	if err != nil || !strings.Contains(out, "the server has 100 of 300") {
		t.Fatalf("%q, %v", out, err)
	}
	want := []string{"bytes */300", "bytes 0-149/300", "bytes 100-249/300"}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("requests %v, want %v", seen, want)
	}
}

func TestClientFlagAndFileErrors(t *testing.T) {
	path, _ := writeFile(t, 10)
	for _, args := range [][]string{
		{"-nope"},
		{"-chunk", "0", "-file", path},
		{"-mode", "teleport", "-file", path},
		{"-file", filepath.Join(t.TempDir(), "missing")},
		{"-file", t.TempDir()}, // a directory opens but cannot be read
	} {
		if out, err := runClient(args...); err == nil {
			t.Errorf("%v: no error, output %q", args, out)
		}
	}
}

func TestClientBadServerAddress(t *testing.T) {
	path, _ := writeFile(t, 10)
	for _, mode := range []string{"upload", "resume", "echo"} {
		if _, err := runClient("-url", "http://[::1", "-mode", mode, "-file", path); err == nil {
			t.Errorf("%s: no error for a URL that does not parse", mode)
		}
		// Nothing listens on a port that was just released.
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		url := "http://" + l.Addr().String()
		l.Close()
		if _, err := runClient("-url", url, "-mode", mode, "-file", path); err == nil {
			t.Errorf("%s: no error for a server that is not there", mode)
		}
	}
}

// truncated answers with a Content-Length it does not deliver.
func truncated(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Length", "1000")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("short"))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	hj, _ := w.(http.Hijacker)
	conn, _, _ := hj.Hijack()
	conn.Close()
}

func TestClientServerMisbehaviour(t *testing.T) {
	path, data := writeFile(t, 50)
	good := fmt.Sprintf("stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data))
	handlers := map[string]struct {
		handler http.HandlerFunc
		modes   []string
		want    string
	}{
		"bad status": {func(w http.ResponseWriter, r *http.Request) { http.Error(w, "nope", 500) },
			[]string{"upload", "resume", "echo"}, "nope"},
		"short reply": {truncated, []string{"upload", "resume"}, "unexpected EOF"},
		"wrong digest": {func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "stored bytes=50 sha256=00\n") },
			[]string{"upload", "resume"}, "does not match"},
		"no offset": {func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) },
			[]string{"resume"}, "Upload-Offset"},
		"wrong echo": {func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); fmt.Fprint(w, "other") },
			[]string{"echo"}, "differs"},
		"echo cut off": {truncated, []string{"echo"}, "unexpected EOF"},
		"good reply":   {func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, good) }, []string{"upload"}, ""},
	}
	for name, tc := range handlers {
		server := httptest.NewServer(tc.handler)
		for _, mode := range tc.modes {
			_, err := runClient("-url", server.URL, "-mode", mode, "-file", path)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("%s/%s: %v", name, mode, err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("%s/%s: error %v, want one containing %q", name, mode, err, tc.want)
			}
		}
		server.Close()
	}
}

// A response that stops partway after the status line is an error from the
// middle of a resume as well as from its probe.
func TestResumeFailsPartway(t *testing.T) {
	path, _ := writeFile(t, 300)
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Upload-Offset", "0")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		http.Error(w, "disk full", http.StatusInsufficientStorage)
	}))
	defer server.Close()
	if _, err := runClient("-url", server.URL, "-mode", "resume", "-file", path, "-chunk", "100"); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("error %v", err)
	}
}

func TestMainReportsAnError(t *testing.T) {
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	var got error
	fatal = func(err error) { got = err }
	os.Args = []string{"client", "-mode", "nonsense"}
	main()
	if got == nil || !strings.Contains(got.Error(), "nonsense") {
		t.Fatalf("fatal got %v", got)
	}
}

func TestFatalPrintsAndExits(t *testing.T) {
	if os.Getenv("CLIENT_FATAL") == "1" {
		fatal(errors.New("exit now"))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestFatalPrintsAndExits")
	cmd.Env = append(os.Environ(), "CLIENT_FATAL=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "error: exit now") {
		t.Fatalf("child: %v %q", err, out)
	}
}
