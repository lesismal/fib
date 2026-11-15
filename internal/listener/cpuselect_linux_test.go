//go:build linux && (amd64 || arm64 || arm || riscv64 || ppc64 || ppc64le || loong64)

package listener

import (
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// cpuMask is a sched_setaffinity mask of up to 1024 CPUs.
type cpuMask [16]uint64

func allowedCPUs(t *testing.T) []int {
	t.Helper()
	var mask cpuMask
	if _, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY, 0, unsafe.Sizeof(mask),
		uintptr(unsafe.Pointer(&mask))); errno != 0 {
		t.Skipf("sched_getaffinity: %v", errno)
	}
	var cpus []int
	for i := range len(mask) * 64 {
		if mask[i/64]&(1<<(i%64)) != 0 {
			cpus = append(cpus, i)
		}
	}
	return cpus
}

// connectFrom connects to 127.0.0.1:port from a thread pinned to cpu, so that
// the SYN is received on cpu, and returns the client's socket. The thread is
// left locked, so the runtime ends it rather than reuse its affinity.
func connectFrom(t *testing.T, cpu, port int) int {
	t.Helper()
	type result struct {
		fd  int
		err error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		var mask cpuMask
		mask[cpu/64] = 1 << (cpu % 64)
		if _, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, unsafe.Sizeof(mask),
			uintptr(unsafe.Pointer(&mask))); errno != 0 {
			done <- result{-1, errno}
			return
		}
		fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
		if err == nil {
			err = syscall.Connect(fd, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}})
		}
		done <- result{fd, err}
	}()
	r := <-done
	if r.err != nil {
		t.Fatalf("connect from CPU %d: %v", cpu, r.err)
	}
	return r.fd
}

// acceptAny reports which of the non-blocking listeners fds accepts the next
// connection.
func acceptAny(t *testing.T, fds []int) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for i, fd := range fds {
			nfd, _, err := syscall.Accept(fd)
			if err == nil {
				syscall.Close(nfd)
				return i
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no listener accepted the connection")
	return -1
}

// A group of n listeners with the CPU selector hands a connection whose SYN
// is received on CPU c to the listener that joined the group (c % n)-th.
func TestCPUSelectorPicksSocketByCPU(t *testing.T) {
	cpus := allowedCPUs(t)
	if len(cpus) < 2 {
		t.Skipf("needs 2 CPUs, have %d", len(cpus))
	}
	n := min(len(cpus), 4)
	var failed error
	o := Options{Network: "tcp4", Backlog: 64, ReusePort: true, CPUSelectGroup: n,
		SelectorFailed: func(err error) { failed = err }}
	first, err := Create(o, "127.0.0.1:0", true, false)
	if err != nil {
		t.Fatal(err)
	}
	fds := []int{first}
	defer func() {
		for _, fd := range fds {
			syscall.Close(fd)
		}
	}()
	for len(fds) < n {
		fd, err := CreateLike(o, first)
		if err != nil {
			t.Fatal(err)
		}
		fds = append(fds, fd)
	}
	if failed != nil {
		t.Skipf("kernel refused the reuseport selector: %v", failed)
	}
	sa, err := syscall.Getsockname(first)
	if err != nil {
		t.Fatal(err)
	}
	port := sa.(*syscall.SockaddrInet4).Port
	for _, cpu := range cpus[:min(len(cpus), 16)] {
		client := connectFrom(t, cpu, port)
		got := acceptAny(t, fds)
		syscall.Close(client)
		if want := cpu % n; got != want {
			t.Errorf("SYN on CPU %d went to listener %d, want %d", cpu, got, want)
		}
	}
}

// Without the selector the same group is left to the kernel's hash, and the
// options attach nothing to a group of one.
func TestCPUSelectorOnlyForGroups(t *testing.T) {
	called := false
	o := Options{Network: "tcp4", Backlog: 8, ReusePort: true, CPUSelectGroup: 1,
		SelectorFailed: func(error) { called = true }}
	fd, err := Create(o, "127.0.0.1:0", true, false)
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(fd)
	if called {
		t.Fatal("a group of one was given a selector")
	}
}
