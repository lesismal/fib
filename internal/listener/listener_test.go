//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package listener

import (
	"net"
	"strconv"
	"syscall"
	"testing"
)

// boundPort returns the port fd is bound to.
func boundPort(t *testing.T, fd int) int {
	t.Helper()
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	return sockaddrPort(sa)
}

// A spread listener on a port another socket already holds is refused with
// EADDRINUSE by its claim, as a single listener would be, rather than joining
// the holder through SO_REUSEPORT.
func TestCreateSpreadRefusesAHeldPort(t *testing.T) {
	held, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	o := Options{Network: "tcp4", Backlog: 16}
	if fd, err := Create(o, held.Addr().String(), true, true); err == nil {
		syscall.Close(fd)
		t.Fatal("a spread listener took a port another socket holds")
	}
}

// A spread listener on IPv6 claims its address as it will bind it, dual-stack
// under "tcp" and IPv6 only under "tcp6", and its pollers' sockets bind like
// it, as do UDP sockets beside an IPv6 one.
func TestCreateSpreadIPv6(t *testing.T) {
	probe, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("no IPv6 loopback:", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()
	for _, network := range []string{"tcp", "tcp6"} {
		o := Options{Network: network, Backlog: 16}
		fd, err := Create(o, net.JoinHostPort("::1", "0"), true, true)
		if err != nil {
			t.Fatalf("%s: %v", network, err)
		}
		// An explicit port is the one that is claimed.
		fd2, err := Create(o, net.JoinHostPort("::1", strconv.Itoa(port)), true, true)
		if err != nil {
			syscall.Close(fd)
			t.Fatalf("%s, port %d: %v", network, port, err)
		}
		like, err := CreateLike(Options{Network: network, Backlog: 16, ReusePort: true}, fd2)
		if err != nil {
			t.Errorf("%s: CreateLike: %v", network, err)
		} else {
			if boundPort(t, like) != port {
				t.Errorf("%s: CreateLike bound port %d, want %d", network, boundPort(t, like), port)
			}
			syscall.Close(like)
		}
		syscall.Close(fd2)
		syscall.Close(fd)
	}
	for _, network := range []string{"udp", "udp6"} {
		o := Options{Network: network, ReusePort: true}
		fd, err := CreateUDP(o, "[::1]:0")
		if err != nil {
			t.Fatalf("%s: %v", network, err)
		}
		like, err := CreateUDPLike(o, fd)
		if err != nil {
			t.Errorf("%s: CreateUDPLike: %v", network, err)
		} else {
			if boundPort(t, like) != boundPort(t, fd) {
				t.Errorf("%s: CreateUDPLike bound port %d, want %d", network, boundPort(t, like), boundPort(t, fd))
			}
			syscall.Close(like)
		}
		syscall.Close(fd)
	}
}

// A UDP socket without ReusePort cannot bind a port another socket holds, and
// the error comes back with nothing left open.
func TestCreateUDPRefusesAHeldPort(t *testing.T) {
	held, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if fd, err := CreateUDP(Options{Network: "udp4"}, held.LocalAddr().String()); err == nil {
		syscall.Close(fd)
		t.Fatal("a UDP socket took a port another socket holds")
	}
}

// A UDP address that leaves its port to the kernel is given one no other
// socket holds, with ReusePort or without, although the engine's UDP sockets
// set SO_REUSEADDR and those with ReusePort SO_REUSEPORT too: Linux's UDP
// autobind counts a port shared through those as free, and handed two test
// processes running at once the same port, the datagrams of one's clients
// reaching the other's server. TCP listeners, spread or not, are held to the
// same, which Create's claim relies on.
func TestPortLeftToTheKernelIsUnique(t *testing.T) {
	// Enough sockets that among the ports of the ephemeral range two would
	// all but certainly meet, were the kernel to give out held ones.
	const sockets = 1000
	create := map[string]func(o Options) (int, error){
		"udp": func(o Options) (int, error) { return CreateUDP(o, "127.0.0.1:0") },
		"tcp": func(o Options) (int, error) { return Create(o, "127.0.0.1:0", true, false) },
		"tcp spread": func(o Options) (int, error) {
			return Create(o, "127.0.0.1:0", false, true)
		},
	}
	for name, open := range create {
		for _, reusePort := range []bool{false, true} {
			o := Options{Network: name[:3] + "4", Backlog: 16, ReusePort: reusePort}
			held := make(map[int]bool, sockets)
			var fds []int
			for range sockets {
				fd, err := open(o)
				if err != nil {
					t.Fatal(err)
				}
				fds = append(fds, fd)
				port := boundPort(t, fd)
				if held[port] {
					t.Errorf("%s, ReusePort %v: port %d was given out twice", name, reusePort, port)
					break
				}
				held[port] = true
			}
			for _, fd := range fds {
				syscall.Close(fd)
			}
		}
	}
}

// The ...Like constructors fail cleanly on a descriptor that is not a socket,
// and the socket constructors on a family the system does not have.
func TestListenerErrorsOnBadInput(t *testing.T) {
	o := Options{Network: "tcp", Backlog: 16}
	if _, err := CreateLike(o, -1); err == nil {
		t.Error("CreateLike(-1) succeeded")
	}
	if _, err := CreateUDPLike(o, -1); err == nil {
		t.Error("CreateUDPLike(-1) succeeded")
	}
	const noFamily = -1
	if _, err := claimAddress(o, noFamily, &syscall.SockaddrInet4{}); err == nil {
		t.Error("claimAddress on no family succeeded")
	}
	if _, err := listenSocket(o, noFamily, &syscall.SockaddrInet4{}, true, false); err == nil {
		t.Error("listenSocket on no family succeeded")
	}
	// An address no interface has is refused at the bind.
	notLocal := &syscall.SockaddrInet4{Addr: [4]byte{192, 0, 2, 1}}
	if _, err := listenSocket(o, syscall.AF_INET, notLocal, true, false); err == nil {
		t.Error("listenSocket bound an address no interface has")
	}
	if _, err := bindUDPSocket(o, noFamily, &syscall.SockaddrInet4{}); err == nil {
		t.Error("bindUDPSocket on no family succeeded")
	}
	if _, err := Create(Options{Network: "sctp"}, ":0", true, false); err == nil {
		t.Error("Create on an unknown network succeeded")
	}
	if _, err := CreateUDP(Options{Network: "sctp"}, ":0"); err == nil {
		t.Error("CreateUDP on an unknown network succeeded")
	}
	if port := sockaddrPort(&syscall.SockaddrUnix{Name: "x"}); port != 0 {
		t.Errorf("sockaddrPort(unix) = %d", port)
	}
}
