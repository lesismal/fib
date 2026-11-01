//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	stdtls "crypto/tls"
	"os"
	"syscall"
	"testing"
)

// noStatFile is a fib.File that cannot tell its size.
type noStatFile struct{ f *os.File }

func (n noStatFile) ReadAt(p []byte, off int64) (int, error) { return n.f.ReadAt(p, off) }
func (n noStatFile) SyscallConn() (syscall.RawConn, error)   { return n.f.SyscallConn() }

// A range smaller than the mark is a large file only if the file itself is,
// and a file that cannot tell its size is as large as the range.
func TestLargeFile(t *testing.T) {
	f, _ := writeFile(t, 100)
	if !largeFile(f, 10, 100) || largeFile(f, 10, 101) {
		t.Fatal("a file was not judged by its size")
	}
	if largeFile(noStatFile{f}, 10, 100) || !largeFile(noStatFile{f}, 100, 100) {
		t.Fatal("a file without Stat was not judged by its range")
	}
}

// A file sealer refuses to seal past the records set aside for it.
func TestFileSealerOverrun(t *testing.T) {
	keys, _ := recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	s := &fileSealer{keys: keys, end: 1}
	out, err := s.Seal(nil, make([]byte, maxPlaintext+1))
	if err != errFileOverrun {
		t.Fatalf("sealing two records into one: %v", err)
	}
	if want := s.SealedLen(maxPlaintext); len(out) != want {
		t.Fatalf("the first record took %d bytes, want %d", len(out), want)
	}
}

// A layer that cannot carry a file refuses one, and one whose sequence
// numbers would run out refuses it rather than wrap them, whether they run
// out on what it held or on the file's records; so does a send behind what
// it held. A failed layer gives back what it held when the cork comes off.
func TestSendFileRefusals(t *testing.T) {
	f, _ := writeFile(t, 100)
	if err := (&layer{}).SendFile(f, 0, 100); err != syscall.EPIPE {
		t.Fatalf("a layer not ready: %v", err)
	}
	keys, _ := recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	keys.seq = ^uint64(0)
	l := &layer{ready: true, tx: keys, ktls: ktlsOff}
	l.held = []byte("held")
	if err := l.SendFile(f, 0, 100); err != errSequenceExhausted {
		t.Fatalf("with something held: %v", err)
	}
	if err := l.SendFile(f, 0, 100); err != errSequenceExhausted {
		t.Fatalf("with nothing held: %v", err)
	}
	l.held = []byte("held")
	l.wmu.Lock()
	err := l.sealSendLocked(recordTypeApplicationData, []byte("x"), nil)
	l.wmu.Unlock()
	if err != errSequenceExhausted {
		t.Fatalf("a send behind something held: %v", err)
	}
	l.held, l.failed = []byte("held"), true
	l.ReleaseCorked()
	if l.held != nil {
		t.Fatal("a failed layer kept what it held")
	}
}
