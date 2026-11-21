//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package http

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// handOverFile is an *os.File that hands its descriptor over once asked, and
// whose Close then leaves the descriptor alone.
type handOverFile struct {
	*os.File
	handed atomic.Int32
	fd     int
}

func (h *handOverFile) HandOverFD() (int, bool) {
	h.handed.Add(1)
	fd, err := syscall.Dup(h.fd)
	if err != nil {
		return 0, false
	}
	// The test's own descriptor is the os.File's; the dup is the one
	// handed over, closed by the connection.
	return fd, true
}

// TestSendFileHandsOver checks that SendFile and SendFileStall ask a
// FileHandover for its descriptor once, for the whole range or for its last
// piece, the pieces before being sent from duplicates of it, over plain
// HTTP/1, and that every range arrives whole.
func TestSendFileHandsOver(t *testing.T) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i * 13)
	}
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	handed := make(chan int32, 4)
	handler := HandlerFunc(func(c *Context) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		h := &handOverFile{File: f, fd: int(f.Fd())}
		n, _ := strconv.Atoi(c.Request.URL.Query().Get("n"))
		send := func(done func(error)) { c.SendFileStall(h, 0, int64(n), time.Second, done) }
		if c.Request.URL.Query().Get("stall") == "0" {
			send = func(done func(error)) { c.SendFile(h, 0, int64(n), done) }
		}
		send(func(err error) {
			if err != nil && !errors.Is(err, io.EOF) {
				t.Error(err)
			}
			f.Close()
			handed <- h.handed.Load()
		})
	})
	base := "http://" + serve(t, NewHandler(handler))
	client := &stdhttp.Client{Timeout: 10 * time.Second}
	for _, tc := range []struct {
		n, stall string
		want     int32
	}{
		{"100000", "", 1},   // one piece
		{"1048576", "", 1},  // four pieces of 256 KiB: the last
		{"1048576", "0", 1}, // no stall bound: one send
		{strconv.Itoa(16 << 10), "", 1},
	} {
		_, body := do(t, client, mustRequest(t, "GET", base+"/?n="+tc.n+"&stall="+tc.stall, nil))
		n, _ := strconv.Atoi(tc.n)
		if !bytes.Equal([]byte(body), data[:n]) {
			t.Fatalf("n=%s stall=%s: %d bytes, not the file's", tc.n, tc.stall, len(body))
		}
		select {
		case got := <-handed:
			if got != tc.want {
				t.Errorf("n=%s stall=%s: HandOverFD called %d times, want %d", tc.n, tc.stall, got, tc.want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("n=%s: done never ran", tc.n)
		}
	}
}
