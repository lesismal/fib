//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package netaddr

import (
	"runtime"
	"syscall"
	"testing"
)

// The networks Listen and Dial take: an empty network means "tcp", a unix one
// takes a path, a udp one resolves as net.ListenPacket does, and any other is
// refused, as are addresses that do not resolve.
func TestResolveNetworks(t *testing.T) {
	if family, _, err := ResolveListen("", "127.0.0.1:0"); err != nil || family != syscall.AF_INET {
		t.Errorf("ResolveListen(\"\") = %d, %v", family, err)
	}
	if family, sa, err := ResolveListen("tcp4", "127.0.0.1:80"); err != nil || family != syscall.AF_INET || ToAddr(sa).String() != "127.0.0.1:80" {
		t.Errorf("ResolveListen(tcp4) = %d, %v, %v", family, sa, err)
	}
	if family, sa, err := ResolveListen("udp","127.0.0.1:53"); err != nil || family != syscall.AF_INET || ToAddr(sa).String() != "127.0.0.1:53" {
		t.Errorf("ResolveListen(udp) = %d, %v, %v", family, sa, err)
	}
	if family, sa, err := ResolveListen("unix", "/tmp/fib.sock"); err != nil || family != syscall.AF_UNIX || ToAddr(sa).String() != "/tmp/fib.sock" {
		t.Errorf("ResolveListen(unix) = %d, %v, %v", family, sa, err)
	}
	for _, network := range []string{"ip", "unixpacket"} {
		if _, _, err := ResolveListen(network, "127.0.0.1:0"); err == nil {
			t.Errorf("ResolveListen(%q) accepted", network)
		}
		if _, _, _, err := ResolveDial(network, "127.0.0.1:80"); err == nil {
			t.Errorf("ResolveDial(%q) accepted", network)
		}
	}
	if _, _, err := ResolveListen("tcp", "127.0.0.1:nosuchport"); err == nil {
		t.Error("ResolveListen accepted an unknown port")
	}
	if _, _, err := ResolveListen("udp", "127.0.0.1:nosuchport"); err == nil {
		t.Error("ResolveListen(udp) accepted an unknown port")
	}
	if _, _, _, err := ResolveDial("tcp", "127.0.0.1:nosuchport"); err == nil {
		t.Error("ResolveDial accepted an unknown port")
	}
	family, sa, raddr, err := ResolveDial("unix", "/tmp/fib.sock")
	if err != nil || family != syscall.AF_UNIX || raddr != nil || sa.(*syscall.SockaddrUnix).Name != "/tmp/fib.sock" {
		t.Errorf("ResolveDial(unix) = %d, %v, %v, %v", family, sa, raddr, err)
	}
	if family, _, _, err := ResolveDial("udp", "[::1]:53"); err != nil || family != syscall.AF_INET6 {
		t.Errorf("ResolveDial(udp, [::1]) = %d, %v", family, err)
	}
	if !IsDialNetwork("tcp6") || !IsDialNetwork("udp4") || !IsDialNetwork("unix") || IsDialNetwork("ip") {
		t.Error("IsDialNetwork")
	}
}

// Only a Linux path that starts with '@' is abstract, and an empty path is
// not.
func TestIsAbstractUnixPath(t *testing.T) {
	want := runtime.GOOS == "linux"
	if IsAbstractUnixPath("@fib") != want || IsAbstractUnixPath("/tmp/fib") || IsAbstractUnixPath("") {
		t.Error("IsAbstractUnixPath")
	}
}

// ToTCPAddr refuses a Unix address; an IPv6 one whose zone index names no
// interface comes back without a zone.
func TestToTCPAddr(t *testing.T) {
	if _, err := ToTCPAddr(&syscall.SockaddrUnix{Name: "/tmp/s"}); err == nil {
		t.Error("ToTCPAddr accepted a unix address")
	}
	addr, err := ToTCPAddr(&syscall.SockaddrInet6{Port: 80, Addr: [16]byte{15: 1}})
	if err != nil || addr.String() != "[::1]:80" {
		t.Errorf("ToTCPAddr(::1) = %v, %v", addr, err)
	}
	addr, err = ToTCPAddr(&syscall.SockaddrInet6{Port: 80, ZoneId: 1 << 30})
	if err != nil || addr.Zone != "" {
		t.Errorf("ToTCPAddr with an unknown zone = %v, %v", addr, err)
	}
}
