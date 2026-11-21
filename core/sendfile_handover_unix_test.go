//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package core

import (
	"bytes"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// handFile is a FileHandover over a raw descriptor, as a server that opens a
// file for a response holds it.
type handFile struct {
	fd     int
	handed atomic.Int32
	keep   bool
}

func (h *handFile) ReadAt(p []byte, off int64) (int, error) { return syscall.Pread(h.fd, p, off) }

func (h *handFile) SyscallConn() (syscall.RawConn, error) {
	return nil, errors.New("handFile: no RawConn")
}

func (h *handFile) HandOverFD() (int, bool) {
	h.handed.Add(1)
	return h.fd, !h.keep
}

// inodeOf is the inode fd refers to, or 0 once it refers to none.
func inodeOf(fd int) uint64 {
	var st syscall.Stat_t
	if syscall.Fstat(fd, &st) != nil {
		return 0
	}
	return uint64(st.Ino)
}

// TestSendFileHandover checks that SendFile takes the descriptor a
// FileHandover hands over, sends from it without duplicating it, and closes
// it once the file is sent; and that a FileHandover that keeps it is
// duplicated as any File is.
func TestSendFileHandover(t *testing.T) {
	const size = 2 << 20
	path, data := writeTestFile(t, size)
	for _, keep := range []bool{false, true} {
		c, peer := acceptOne(t, "tcp", HandlerFuncs{})
		fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		ino := inodeOf(fd)
		h := &handFile{fd: fd, keep: keep}
		if !keep {
			// A File that hands its descriptor over needs no RawConn.
			if _, err := h.SyscallConn(); err == nil {
				t.Fatal("handFile has a RawConn")
			}
		}
		if keep {
			// Kept, SendFile duplicates it through a RawConn, which this
			// File has none of.
			if err := c.SendFile(h, 0, size); err == nil {
				t.Fatal("a kept descriptor with no RawConn was sent")
			}
			if h.handed.Load() != 1 || inodeOf(fd) != ino {
				t.Fatalf("kept: asked %d times, descriptor still open %v", h.handed.Load(), inodeOf(fd) == ino)
			}
			syscall.Close(fd)
			continue
		}
		if err := c.SendFile(h, 0, size); err != nil {
			t.Fatal(err)
		}
		if got := readAllFrom(t, peer, size); !bytes.Equal(got, data) {
			t.Fatal("what the peer read differs from the file")
		}
		if n := h.handed.Load(); n != 1 {
			t.Fatalf("HandOverFD called %d times", n)
		}
		deadline := time.Now().Add(5 * time.Second)
		for inodeOf(fd) == ino {
			if time.Now().After(deadline) {
				t.Fatal("the connection did not close the descriptor it was handed")
			}
			time.Sleep(time.Millisecond)
		}
	}
}
