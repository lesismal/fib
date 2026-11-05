//go:build linux

package core

import (
	"os"
	"syscall"
	"testing"
	"time"
)

// spliceRefusingFile opens a file sendfile refuses but pread reads, as Linux
// refuses its seq_file /proc files, holding at least size bytes, or skips.
func spliceRefusingFile(t *testing.T, path string, size int) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { f.Close() })
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])
	var off int64
	if _, err := syscall.Sendfile(fds[0], int(f.Fd()), &off, 1); err != syscall.EINVAL {
		t.Skipf("sendfile from %s: %v, not EINVAL", path, err)
	}
	if n, _ := syscall.Pread(int(f.Fd()), make([]byte, 1), int64(size-1)); n != 1 {
		t.Skipf("%s holds fewer than %d bytes", path, size)
	}
	return f
}

// A file sendfile cannot splice is copied to the socket a chunk at a time as
// the peer takes it, and one that turns out shorter than the range closes the
// connection, as the sendfile path does.
func TestSendFileCopiesWhatSendfileRefuses(t *testing.T) {
	const size = 100 << 10
	f := spliceRefusingFile(t, "/proc/kallsyms", size)
	c, client := acceptOne(t, "tcp", HandlerFuncs{})
	if err := c.SendFile(f, 0, size); err != nil {
		t.Fatal(err)
	}
	readAllFrom(t, client, size)

	short := spliceRefusingFile(t, "/proc/self/stat", 1)
	closed := make(chan error, 1)
	c, client = acceptOne(t, "tcp", HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	_ = c.SendFile(short, 0, 1<<20)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("a file shorter than its range left the connection open")
	}
	_ = client
}
