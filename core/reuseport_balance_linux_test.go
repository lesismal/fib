//go:build linux && (amd64 || arm64)

package core

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
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

// TestReusePortBalancerRoundRobin loads the eBPF selector against the real
// kernel verifier and checks that it hands connections to the group's sockets
// evenly, where the kernel's own hash need not. It skips where the process
// lacks CAP_BPF or the kernel is too old.
func TestReusePortBalancerRoundRobin(t *testing.T) {
	const sockets, conns = 4, 40
	fds, addr := reuseGroup(t, sockets)

	progLoadLog = make([]byte, 1<<16)
	reusePortDebug = true
	defer func() { progLoadLog = nil; reusePortDebug = false }()
	b, err := newReusePortBalancer(fds)
	if err == errReusePortBalanceUnsupported {
		t.Skipf("reuseport balancing unsupported here (need CAP_BPF): %v", err)
	}
	if err != nil {
		if log := progLoadLog[:bytes.IndexByte(progLoadLog, 0)+1]; len(log) > 1 {
			t.Logf("verifier log:\n%s", log)
		}
		t.Fatalf("newReusePortBalancer: %v", err)
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

// TestEngineBalanceReusePort runs the whole openBinding path with
// Config.BalanceReusePort set: the engine attaches a selector to its pollers'
// shared listening group and still echoes. It skips the attach check where
// bpf(2) is refused, but the echo must work either way.
func TestEngineBalanceReusePort(t *testing.T) {
	reusePortDebug = true
	defer func() { reusePortDebug = false }()
	config := pollerConfig("balance", 4)
	config.ReusePort = true
	config.BalanceReusePort = true
	server, addr := startEchoServer(t, config, echoHandler())

	for i := 0; i < 20; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		msg := []byte("ping")
		if _, err := c.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		buf := make([]byte, len(msg))
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := c.Read(buf); err != nil || !bytes.Equal(buf, msg) {
			t.Fatalf("echo %d: %q %v", i, buf, err)
		}
		c.Close()
	}

	server.listenMu.Lock()
	n := len(server.balancers)
	server.listenMu.Unlock()
	if n == 0 {
		t.Skip("no selector attached (need CAP_BPF); echo worked without it")
	}
}

// TestPinnedReusePortBalancerRoundRobin drives the cross-process path within
// one process: each socket registers itself at its own index through the
// bpffs-pinned selector, as a prefork child would, the first call creating
// and pinning it and the rest opening it. It needs bpffs and CAP_BPF, and
// skips without them.
func TestPinnedReusePortBalancerRoundRobin(t *testing.T) {
	const sockets, conns = 4, 40
	if fi, err := os.Stat("/sys/fs/bpf"); err != nil || !fi.IsDir() {
		t.Skip("no bpffs mounted")
	}
	dir := filepath.Join("/sys/fs/bpf", fmt.Sprintf("fib-test-%d", os.Getpid()))
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Skipf("cannot make a bpffs directory (need CAP_BPF): %v", err)
	}
	defer os.RemoveAll(dir)

	fds, addr := reuseGroup(t, sockets)
	progLoadLog = make([]byte, 1<<16)
	reusePortDebug = true
	defer func() { progLoadLog = nil; reusePortDebug = false }()

	path := dir + "/" + addrKey(fds[0])
	var bals []*reusePortBalancer
	for i, fd := range fds {
		b, err := openPinnedReusePortBalancer(path, sockets, i, []int{fd})
		if err == errReusePortBalanceUnsupported {
			t.Skipf("pinned balancing unsupported here: %v (last: %v)", err, lastPinErr)
		}
		if err != nil {
			if log := progLoadLog[:bytes.IndexByte(progLoadLog, 0)+1]; len(log) > 1 {
				t.Logf("verifier log:\n%s", log)
			}
			t.Fatalf("openPinnedReusePortBalancer[%d]: %v", i, err)
		}
		bals = append(bals, b)
	}
	defer func() {
		for _, b := range bals {
			b.Close()
		}
	}()

	var dials []net.Conn
	for i := 0; i < conns; i++ {
		c, err := net.DialTimeout("tcp", addr.String(), time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		dials = append(dials, c)
	}
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
		t.Fatalf("accepted %d of %d: %v", total, conns, counts)
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
	if hi-lo > 3 || lo == 0 {
		t.Fatalf("uneven distribution across the shared selector: %v", counts)
	}
}

// TestPreforkBalanceEnv checks that the prefork child's place is read from the
// environment, and that a non-child or a lone child is not balanced.
func TestPreforkBalanceEnv(t *testing.T) {
	t.Setenv(preforkBPFFSEnv, "/sys/fs/bpf/run")
	t.Setenv(preforkChildEnv, "2")
	t.Setenv(preforkChildrenEnv, "4")
	dir, child, children, ok := preforkBalance()
	if !ok || dir != "/sys/fs/bpf/run" || child != 2 || children != 4 {
		t.Fatalf("preforkBalance() = %q %d %d %v", dir, child, children, ok)
	}
	t.Setenv(preforkChildrenEnv, "1")
	if _, _, _, ok := preforkBalance(); ok {
		t.Error("a lone child should not be balanced")
	}
	t.Setenv(preforkBPFFSEnv, "")
	t.Setenv(preforkChildrenEnv, "4")
	if _, _, _, ok := preforkBalance(); ok {
		t.Error("without a bpffs directory there is nothing to balance")
	}
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
