//go:build linux && (amd64 || arm64)

package reuseportbpf

import (
	"bytes"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"
)

// reuseGroup opens n listening TCP sockets that share one 127.0.0.1 address
// through SO_REUSEPORT, and returns their fds and the address.
func reuseGroup(t *testing.T, n int) ([]int, *net.TCPAddr) {
	t.Helper()
	var fds []int
	var bound syscall.Sockaddr
	for i := 0; i < n; i++ {
		fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
		if err != nil {
			t.Fatalf("socket: %v", err)
		}
		_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, 0xf, 1); err != nil { // SO_REUSEPORT
			t.Fatalf("SO_REUSEPORT: %v", err)
		}
		if i == 0 {
			if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
				t.Fatalf("bind: %v", err)
			}
			bound, _ = syscall.Getsockname(fd)
		} else if err := syscall.Bind(fd, bound); err != nil {
			t.Fatalf("bind like: %v", err)
		}
		if err := syscall.Listen(fd, 128); err != nil {
			t.Fatalf("listen: %v", err)
		}
		fds = append(fds, fd)
		t.Cleanup(func() { syscall.Close(fd) })
	}
	sa := bound.(*syscall.SockaddrInet4)
	return fds, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: sa.Port}
}

// drain accepts every queued connection on fd without blocking, closes them,
// and returns how many there were.
func drain(t *testing.T, fd int) int {
	t.Helper()
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatalf("nonblock: %v", err)
	}
	n := 0
	for {
		c, _, err := syscall.Accept(fd)
		if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK {
			return n
		}
		if err != nil {
			t.Fatalf("accept: %v", err)
		}
		syscall.Close(c)
		n++
	}
}

// fdInode returns the inode fd refers to.
func fdInode(t *testing.T, fd int) uint64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		t.Fatal(err)
	}
	return st.Ino
}

// TestBalanceRoundRobin loads the eBPF selector against the real
// kernel verifier and checks that it hands connections to the group's sockets
// evenly, where the kernel's own hash need not. It skips where the process
// lacks CAP_BPF or the kernel is too old.
func TestBalanceRoundRobin(t *testing.T) {
	const sockets, conns = 4, 40
	fds, addr := reuseGroup(t, sockets)

	ProgLoadLog = make([]byte, 1<<16)
	Debug = true
	defer func() { ProgLoadLog = nil; Debug = false }()
	b, err := Balance(fds)
	if err == ErrUnsupported {
		t.Skipf("reuseport balancing unsupported here (need CAP_BPF): %v", err)
	}
	if err != nil {
		if log := ProgLoadLog[:bytes.IndexByte(ProgLoadLog, 0)+1]; len(log) > 1 {
			t.Logf("verifier log:\n%s", log)
		}
		t.Fatalf("Balance: %v", err)
	}
	defer b.Close()

	var dials []net.Conn
	for i := 0; i < conns; i++ {
		c, err := net.DialTimeout("tcp", addr.String(), time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		dials = append(dials, c)
	}
	// Let the last handshakes reach their accept queues.
	time.Sleep(100 * time.Millisecond)

	counts := make([]int, sockets)
	total := 0
	for i, fd := range fds {
		counts[i] = drain(t, fd)
		total += counts[i]
	}
	for _, c := range dials {
		c.Close()
	}
	if total != conns {
		t.Fatalf("accepted %d of %d connections: %v", total, conns, counts)
	}
	lo, hi := counts[0], counts[0]
	for _, c := range counts {
		if c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	// Round-robin over 40 connections and 4 sockets is 10 each; allow a little
	// slack for the counter's non-atomic increment under concurrent SYNs. The
	// kernel's hash, by contrast, can leave a socket empty.
	if hi-lo > 3 {
		t.Fatalf("uneven distribution %v (spread %d)", counts, hi-lo)
	}
	if lo == 0 {
		t.Fatalf("a socket got no connections: %v", counts)
	}
}

// The calls turn down what they cannot balance before they ask the kernel
// anything: a group of one socket, or nothing to register.
func TestTooSmallAGroup(t *testing.T) {
	fds, _ := reuseGroup(t, 1)
	if g, err := Balance(fds); g != nil || err != ErrUnsupported {
		t.Errorf("Balance of one socket = %v, %v", g, err)
	}
	if g, err := NewGroup(1); g != nil || err != ErrUnsupported {
		t.Errorf("NewGroup(1) = %v, %v", g, err)
	}
	if g, err := JoinGroup(-1, "k", 1, 0, fds); g != nil || err != ErrUnsupported {
		t.Errorf("JoinGroup of a group of one = %v, %v", g, err)
	}
	if g, err := JoinGroup(-1, "k", 2, 0, nil); g != nil || err != ErrUnsupported {
		t.Errorf("JoinGroup with no sockets = %v, %v", g, err)
	}
}

// The bpf(2) wrappers report the kernel's refusal of descriptors that are
// not maps, programs or sockets, rather than claiming success.
func TestBPFWrappersReportFailures(t *testing.T) {
	ProgLoadLog = make([]byte, 1<<12)
	defer func() { ProgLoadLog = nil }()
	if fd, err := loadProgram(selectProgram(-1, -1, 2)); err == nil {
		syscall.Close(fd)
		t.Error("loadProgram with no maps succeeded")
	}
	if err := registerSocket(-1, 0, 0); err == nil {
		t.Error("registerSocket into no map succeeded")
	}
	if err := attach(-1, -1); err == nil {
		t.Error("attach to no socket succeeded")
	}
}

// Only the errors that mean bpf(2) is not allowed here, and EINVAL outside
// debugging, fold into the unsupported sentinel; the rest are returned as
// they are.
func TestUnsupportedIfDenied(t *testing.T) {
	for _, err := range []error{syscall.EPERM, syscall.EACCES, syscall.ENOSYS, syscall.EINVAL} {
		if got := unsupportedIfDenied(err); got != ErrUnsupported {
			t.Errorf("unsupportedIfDenied(%v) = %v", err, got)
		}
	}
	if got := unsupportedIfDenied(syscall.EBADF); got != syscall.EBADF {
		t.Errorf("unsupportedIfDenied(EBADF) = %v", got)
	}
	Debug = true
	defer func() { Debug = false }()
	if got := unsupportedIfDenied(syscall.EINVAL); got != syscall.EINVAL {
		t.Errorf("unsupportedIfDenied(EINVAL) while debugging = %v", got)
	}
}

// pipeGroup is a group holding a pipe's descriptors and a duplicate
// in place of its maps and program.
func pipeGroup(t *testing.T) (*Group, []int) {
	t.Helper()
	var p [2]int
	if err := syscall.Pipe(p[:]); err != nil {
		t.Fatal(err)
	}
	third, err := syscall.Dup(p[0])
	if err != nil {
		t.Fatal(err)
	}
	b := &Group{Sockarray: p[0], Counter: p[1], Prog: third}
	t.Cleanup(func() { b.Close() })
	return b, []int{p[0], p[1], third}
}

// Close closes every descriptor the group holds, once, and does nothing
// on a nil group.
func TestGroupClose(t *testing.T) {
	var none *Group
	if err := none.Close(); err != nil {
		t.Fatal(err)
	}
	b, fds := pipeGroup(t)
	// A goroutine left by an earlier test may reuse a number Close freed, so
	// a descriptor counts as still open only while it is the same pipe.
	inodes := make([]uint64, len(fds))
	for i, fd := range fds {
		inodes[i] = fdInode(t, fd)
	}
	b.Close()
	for i, fd := range fds {
		var st syscall.Stat_t
		if syscall.Fstat(fd, &st) == nil && st.Ino == inodes[i] {
			t.Errorf("descriptor %d is still open", fd)
		}
	}
	if b.Sockarray != -1 || b.Counter != -1 || b.Prog != -1 {
		t.Errorf("Close left %+v", *b)
	}
	b.Close()
}

// TestSelectProgramShape checks the assembled program's size and that the two
// LD_IMM64 instructions leave their second slot zeroed, which the kernel
// requires, without needing a kernel to load it.
func TestSelectProgramShape(t *testing.T) {
	insns := selectProgram(3, 4, 7)
	if len(insns) != 25 {
		t.Fatalf("program has %d instructions, want 25", len(insns))
	}
	for _, i := range []int{3, 18} {
		if insns[i] != (bpfInsn{}) {
			t.Errorf("instruction %d (LD_IMM64 second slot) is %+v, want zero", i, insns[i])
		}
	}
	if got := fmt.Sprintf("%#x", insns[2].code); got != "0x18" {
		t.Errorf("counter map load opcode = %s, want 0x18", got)
	}
}
