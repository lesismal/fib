//go:build linux && !386

package sys

import (
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

// MemoryLimit takes a cgroup's limit where it is below the machine's memory,
// and the machine's where the cgroup sets none.
func TestMemoryLimitTakesTheCgroupLimit(t *testing.T) {
	dir := t.TempDir()
	proc, root, v1 := filepath.Join(dir, "cgroup"), filepath.Join(dir, "cgroup2"), filepath.Join(dir, "cgroup1")
	writeTestFile(t, proc, "0::/a\n")
	saved := [3]string{procSelfCgroup, cgroupRootV2, cgroupRootV1}
	procSelfCgroup, cgroupRootV2, cgroupRootV1 = proc, root, v1
	t.Cleanup(func() { procSelfCgroup, cgroupRootV2, cgroupRootV1 = saved[0], saved[1], saved[2] })

	machine := MemoryLimit()
	if machine <= 0 {
		t.Fatalf("MemoryLimit with no cgroup limit = %d", machine)
	}
	writeTestFile(t, filepath.Join(root, "a", "memory.max"), "1048576\n")
	if got := MemoryLimit(); got != 1048576 {
		t.Fatalf("MemoryLimit = %d, want the cgroup's 1048576", got)
	}
}

// SetDontFragment takes on a UDP socket, and fails on a socket that is not
// IP at all.
func TestSetDontFragment(t *testing.T) {
	fd, err := NewDatagramSocket(syscall.AF_INET)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := SetDontFragment(fd); err != nil {
		t.Errorf("SetDontFragment on UDP: %v", err)
	}
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(pair[0])
	defer syscall.Close(pair[1])
	if err := SetDontFragment(pair[0]); err == nil {
		t.Error("SetDontFragment took on a unix socket")
	}
}

// Read, Write and Writev move bytes through a socket with either set of
// calls, fall back to read and write on a descriptor that is not a socket,
// and report a failed call's errno with a count of 0.
func TestSocketIO(t *testing.T) {
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(pair[0])
	defer syscall.Close(pair[1])
	var pipe [2]int
	if err := syscall.Pipe2(pipe[:], syscall.O_NONBLOCK); err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(pipe[0])
	defer syscall.Close(pipe[1])

	buf := make([]byte, 64)
	for _, socketCalls := range []bool{false, true} {
		for name, fds := range map[string][2]int{"socket": {pair[0], pair[1]}, "pipe": {pipe[1], pipe[0]}} {
			w, r := fds[0], fds[1]
			if n, err := Write(w, []byte("ab"), socketCalls); n != 2 || err != nil {
				t.Fatalf("%s Write(%v) = %d, %v", name, socketCalls, n, err)
			}
			a, b := []byte("cd"), []byte("e")
			iov := []syscall.Iovec{{Base: unsafe.SliceData(a)}, {Base: unsafe.SliceData(b)}}
			iov[0].SetLen(len(a))
			iov[1].SetLen(len(b))
			if n, err := Writev(w, iov, socketCalls); n != 3 || err != nil {
				t.Fatalf("%s Writev(%v) = %d, %v", name, socketCalls, n, err)
			}
			if n, err := Read(r, buf, socketCalls); err != nil || string(buf[:n]) != "abcde" {
				t.Fatalf("%s Read(%v) = %q, %v", name, socketCalls, buf[:n], err)
			}
			if n, err := Read(r, buf, socketCalls); n != 0 || !IsWouldBlock(err) {
				t.Fatalf("%s Read(%v) of nothing = %d, %v", name, socketCalls, n, err)
			}
		}
		// A closed descriptor fails every call.
		if n, err := Read(-1, buf, socketCalls); n != 0 || err != syscall.EBADF {
			t.Errorf("Read(-1, %v) = %d, %v", socketCalls, n, err)
		}
		if n, err := Write(-1, buf, socketCalls); n != 0 || err != syscall.EBADF {
			t.Errorf("Write(-1, %v) = %d, %v", socketCalls, n, err)
		}
		iov := []syscall.Iovec{{Base: &buf[0]}}
		iov[0].SetLen(1)
		if n, err := Writev(-1, iov, socketCalls); n != 0 || err != syscall.EBADF {
			t.Errorf("Writev(-1, %v) = %d, %v", socketCalls, n, err)
		}
	}
}

// AcceptSocket hands back a connection and its peer's address; Recvfrom
// reads a datagram with its sender's. Both report a failed call's errno.
func TestAcceptAndRecvfrom(t *testing.T) {
	ln, err := NewSocket(syscall.AF_INET)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(ln)
	if err := syscall.Bind(ln, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(ln, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := AcceptSocket(ln); !IsWouldBlock(err) {
		t.Fatalf("AcceptSocket with nothing to accept: %v", err)
	}
	lsa, _ := syscall.Getsockname(ln)
	client, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(client)
	if err := syscall.Connect(client, lsa); err != nil {
		t.Fatal(err)
	}
	// A blocking connect to loopback returns once the connection waits in
	// the accept queue.
	csa, _ := syscall.Getsockname(client)
	fd, peer, err := AcceptSocket(ln)
	if err != nil {
		t.Fatalf("AcceptSocket: %v", err)
	}
	defer syscall.Close(fd)
	if peer != SockaddrAddrPort(csa) {
		t.Errorf("AcceptSocket peer = %v, want %v", peer, SockaddrAddrPort(csa))
	}

	udp, err := NewDatagramSocket(syscall.AF_INET)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(udp)
	if err := syscall.Bind(udp, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	usa, _ := syscall.Getsockname(udp)
	if err := syscall.Sendto(udp, []byte("dgram"), 0, usa); err != nil {
		t.Fatal(err)
	}
	var from syscall.RawSockaddrAny
	buf := make([]byte, 16)
	n, err := Recvfrom(udp, buf, &from)
	if err != nil || string(buf[:n]) != "dgram" || rawAddrPort(&from) != SockaddrAddrPort(usa) {
		t.Fatalf("Recvfrom = %q, %v from %v", buf[:n], err, rawAddrPort(&from))
	}
	if _, err := Recvfrom(udp, buf, &from); !IsWouldBlock(err) {
		t.Fatalf("Recvfrom of nothing: %v", err)
	}
}

// The small helpers: an eventfd, the errno a connect reports, the address
// in use test and the descriptor limit.
func TestHelpers(t *testing.T) {
	fd, err := EventFD()
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(fd)
	if PortableErrno(syscall.ECONNREFUSED) != syscall.ECONNREFUSED {
		t.Error("PortableErrno")
	}
	if !IsAddrInUse(syscall.EADDRINUSE) || IsAddrInUse(syscall.EINVAL) {
		t.Error("IsAddrInUse")
	}
	if FDLimit() <= 0 {
		t.Errorf("FDLimit = %d", FDLimit())
	}
}
