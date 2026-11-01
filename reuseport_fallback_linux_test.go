//go:build linux && (amd64 || arm64)

package fib

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// fdOpen reports whether fd is an open descriptor.
func fdOpen(fd int) bool {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
	return errno != syscall.EBADF
}

// The loader turns down what it cannot balance before it asks the kernel
// anything: a group of one socket, a pinned group of one, or nothing to
// register.
func TestReusePortBalancerRefusesTooSmallAGroup(t *testing.T) {
	fds, _ := reuseGroup(t, 1)
	if b, err := newReusePortBalancer(fds); b != nil || err != errReusePortBalanceUnsupported {
		t.Errorf("newReusePortBalancer of one socket = %v, %v", b, err)
	}
	path := filepath.Join(t.TempDir(), "pin")
	if b, err := openPinnedReusePortBalancer(path, 1, 0, fds); b != nil || err != errReusePortBalanceUnsupported {
		t.Errorf("openPinnedReusePortBalancer of a group of one = %v, %v", b, err)
	}
	if b, err := openPinnedReusePortBalancer(path, 2, 0, nil); b != nil || err != errReusePortBalanceUnsupported {
		t.Errorf("openPinnedReusePortBalancer with no sockets = %v, %v", b, err)
	}
}

// A pinned group whose lock file cannot be made falls back, and says why.
func TestPinnedReusePortBalancerWithoutItsLock(t *testing.T) {
	fds, _ := reuseGroup(t, 2)
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
	lastPinErr = nil
	b, err := openPinnedReusePortBalancer("/sys/fs/bpf/fib-test/x", 4, 0, fds)
	if b != nil || err != errReusePortBalanceUnsupported {
		t.Fatalf("openPinnedReusePortBalancer without its lock = %v, %v", b, err)
	}
	if lastPinErr == nil {
		t.Fatal("the fallback recorded no reason")
	}
}

// Where bpf(2) is refused, as it is without CAP_BPF, a pinned group falls
// back after taking and giving up its lock, and records the stage that
// failed. Where it is allowed, the path is outside bpffs and pinning fails
// instead; either way nothing is returned.
func TestPinnedReusePortBalancerFallsBack(t *testing.T) {
	fds, _ := reuseGroup(t, 2)
	path := filepath.Join(t.TempDir(), addrKey(fds[0]))
	lastPinErr = nil
	b, err := openPinnedReusePortBalancer(path, 4, 0, fds)
	if b != nil || err == nil {
		t.Fatalf("openPinnedReusePortBalancer outside bpffs = %v, %v", b, err)
	}
	if lastPinErr == nil {
		t.Error("the fallback recorded no reason")
	}
	// The lock was let go: taking it again does not block.
	unlock, err := lockPin(path)
	if err != nil {
		t.Fatalf("lockPin after the fallback: %v", err)
	}
	unlock()
}

// The bpf(2) wrappers report the kernel's refusal of descriptors that are
// not maps, programs or sockets, rather than claiming success.
func TestBPFWrappersReportFailures(t *testing.T) {
	progLoadLog = make([]byte, 1<<12)
	defer func() { progLoadLog = nil }()
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
	missing := filepath.Join(t.TempDir(), "missing")
	if err := objPin(-1, missing); err == nil {
		t.Error("objPin of no object succeeded")
	}
	if fd, err := objGet(missing); err == nil {
		syscall.Close(fd)
		t.Error("objGet of nothing pinned succeeded")
	}
}

// Only the errors that mean bpf(2) is not allowed here, and EINVAL outside
// debugging, fold into the unsupported sentinel; the rest are returned as
// they are, and pinFallback records the stage beside folding the same way.
func TestUnsupportedIfDenied(t *testing.T) {
	for _, err := range []error{syscall.EPERM, syscall.EACCES, syscall.ENOSYS, syscall.EINVAL} {
		if got := unsupportedIfDenied(err); got != errReusePortBalanceUnsupported {
			t.Errorf("unsupportedIfDenied(%v) = %v", err, got)
		}
	}
	if got := unsupportedIfDenied(syscall.EBADF); got != syscall.EBADF {
		t.Errorf("unsupportedIfDenied(EBADF) = %v", got)
	}
	reusePortDebug = true
	defer func() { reusePortDebug = false }()
	if got := unsupportedIfDenied(syscall.EINVAL); got != syscall.EINVAL {
		t.Errorf("unsupportedIfDenied(EINVAL) while debugging = %v", got)
	}
	lastPinErr = nil
	if got := pinFallback("stage", syscall.EBADF); got != syscall.EBADF {
		t.Errorf("pinFallback(EBADF) = %v", got)
	}
	if !errors.Is(lastPinErr, syscall.EBADF) {
		t.Errorf("lastPinErr = %v, want the stage's error", lastPinErr)
	}
}

// pipeBalancer is a balancer holding a pipe's descriptors and a duplicate
// in place of its maps and program.
func pipeBalancer(t *testing.T) (*reusePortBalancer, []int) {
	t.Helper()
	var p [2]int
	if err := syscall.Pipe(p[:]); err != nil {
		t.Fatal(err)
	}
	third, err := syscall.Dup(p[0])
	if err != nil {
		t.Fatal(err)
	}
	b := &reusePortBalancer{sockarray: p[0], counter: p[1], prog: third}
	t.Cleanup(func() { b.Close() })
	return b, []int{p[0], p[1], third}
}

// Close closes every descriptor the balancer holds, once, and does nothing
// on a nil balancer.
func TestReusePortBalancerClose(t *testing.T) {
	var none *reusePortBalancer
	if err := none.Close(); err != nil {
		t.Fatal(err)
	}
	b, fds := pipeBalancer(t)
	// Nothing else in the test opens a descriptor in between, so the
	// numbers Close freed are not yet reused.
	b.Close()
	for _, fd := range fds {
		if fdOpen(fd) {
			t.Errorf("descriptor %d is still open", fd)
		}
	}
	if b.sockarray != -1 || b.counter != -1 || b.prog != -1 {
		t.Errorf("Close left %+v", *b)
	}
	b.Close()
}

// Closing a binding closes the selectors that balance its groups, and only
// those; closing every binding closes the rest.
func TestBindingClosesItsBalancers(t *testing.T) {
	server := newOfflineServer(t)
	mine, _ := pipeBalancer(t)
	other, _ := pipeBalancer(t)
	server.listenMu.Lock()
	server.balancers = append(server.balancers, bindingBalancer{binding: 0, b: mine},
		bindingBalancer{binding: 1, b: other})
	server.listenMu.Unlock()
	server.closeBalancers(0)
	if mine.prog != -1 || other.prog == -1 {
		t.Fatalf("closing binding 0 left %+v and %+v", *mine, *other)
	}
	server.closeBalancers(-1)
	if other.prog != -1 || len(server.balancers) != 0 {
		t.Fatalf("closing every binding left %+v, %d balancers", *other, len(server.balancers))
	}
}

// An address AddBinding spreads over the pollers is balanced as one New was
// given is: where bpf(2) is refused its selector falls back to the kernel's
// hash, and the binding serves either way. In a prefork child the group is
// shared through a pinned selector, which falls back the same way.
func TestAddBindingBalancesReusePort(t *testing.T) {
	for _, prefork := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefork=%v", prefork), func(t *testing.T) {
			if prefork {
				t.Setenv(preforkBPFFSEnv, t.TempDir())
				t.Setenv(preforkChildEnv, "1")
				t.Setenv(preforkChildrenEnv, "2")
			}
			config := pollerConfig("balance-add", 2)
			config.ReusePort = true
			config.BalanceReusePort = true
			server, _ := startEchoServer(t, config, echoHandler())
			addrs := []string{"127.0.0.1:0"}
			if ln, err := net.Listen("tcp6", "[::1]:0"); err == nil {
				ln.Close()
				addrs = append(addrs, "[::1]:0")
			}
			i, err := server.AddBinding(Binding{Addrs: addrs, Handler: echoHandler()})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := server.BindingAddrs(i)
			if err != nil || len(bound) != len(addrs) {
				t.Fatalf("BindingAddrs = %v, %v", bound, err)
			}
			for _, a := range bound {
				c, err := net.DialTimeout("tcp", a.String(), 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				err = echoOnce(c, "ping")
				c.Close()
				if err != nil {
					t.Fatalf("echo from %s: %v", a, err)
				}
			}
			if err := server.RemoveBinding(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The selectors New attaches leave out a Unix listener, which the pollers
// share rather than each opening one of their own.
func TestBalanceReusePortPassesOverUnixListeners(t *testing.T) {
	config := pollerConfig("balance-unix", 2)
	config.ReusePort = true
	config.BalanceReusePort = true
	path := filepath.Join(t.TempDir(), "s")
	server, err := New(config, Binding{Network: "unix", Addr: path, Handler: echoHandler()},
		Binding{Addr: "127.0.0.1:0", Handler: echoHandler()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != 2 {
		t.Fatalf("ListenAddrs = %v, %v", addrs, err)
	}
}
