//go:build windows

package fib

import (
	"os"
	"syscall"
	"time"
	"unsafe"
)

// rawSocket is what the kernel names a socket by: a handle here.
type rawSocket = syscall.Handle

func (c *Connection) rawSocket() rawSocket { return c.socket() }

// shutdownReadLocked shuts the socket's reading side and cancels the
// zero-byte read posted on it, whose completion then arms nothing more; see
// completeRead. Callers hold c.mu.
func (c *Connection) shutdownReadLocked() error {
	h := c.socket()
	err := syscall.Shutdown(h, syscall.SHUT_RD)
	if c.readArmed {
		_ = syscall.CancelIoEx(h, &c.readOp.ov)
	}
	return err
}

// The keep-alive options Windows 10 added as socket options, in place of the
// SIO_KEEPALIVE_VALS control code, which is what older versions have.
const (
	tcpKeepIdle     = 3
	tcpKeepCount    = 16
	tcpKeepInterval = 17
)

// setKeepAliveTimes sets the idle time before the first keep-alive probe and
// the interval between probes; see Connection.SetKeepAliveConfig. A Windows
// without the socket options sets both at once through SIO_KEEPALIVE_VALS,
// where one left alone takes the net package's default instead.
func setKeepAliveTimes(s syscall.Handle, idle, interval time.Duration) error {
	if idle < 0 && interval < 0 {
		return nil
	}
	var err error
	if idle >= 0 {
		err = syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepIdle, keepAliveSeconds(idle))
	}
	if err == nil && interval >= 0 {
		err = syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepInterval, keepAliveSeconds(interval))
	}
	if err != syscall.WSAENOPROTOOPT {
		return err
	}
	if idle < 0 {
		idle = 0
	}
	if interval < 0 {
		interval = 0
	}
	vals := syscall.TCPKeepalive{
		OnOff:    1,
		Time:     uint32(keepAliveSeconds(idle)) * 1000,
		Interval: uint32(keepAliveSeconds(interval)) * 1000,
	}
	var n uint32
	return syscall.WSAIoctl(s, syscall.SIO_KEEPALIVE_VALS, (*byte)(unsafe.Pointer(&vals)),
		uint32(unsafe.Sizeof(vals)), nil, 0, &n, nil, 0)
}

// setKeepAliveCount sets how many unanswered probes end the connection. A
// Windows without the option keeps its own count.
func setKeepAliveCount(s syscall.Handle, count int) error {
	if count < 0 {
		return nil
	}
	if count == 0 {
		count = defaultKeepAliveCount
	}
	err := syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepCount, count)
	if err == syscall.WSAENOPROTOOPT {
		return nil
	}
	return err
}

// usingMultipathTCP is false: Windows has no Multipath TCP.
func usingMultipathTCP(syscall.Handle) bool { return false }

// File is not available on Windows, where a socket cannot be duplicated into
// a file; it reports syscall.EWINDOWS, as net.TCPConn.File does there.
func (c *Connection) File() (*os.File, error) {
	return nil, os.NewSyscallError("dup", syscall.EWINDOWS)
}
