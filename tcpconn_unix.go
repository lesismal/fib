//go:build linux || darwin

package fib

import (
	"net"
	"os"
	"syscall"
	"time"
)

// rawSocket is what the kernel names a socket by: a file descriptor here.
type rawSocket = int

func (c *Connection) rawSocket() rawSocket { return c.FD() }

// shutdownReadLocked shuts the socket's reading side. Callers hold c.mu.
func (c *Connection) shutdownReadLocked() error {
	return syscall.Shutdown(c.FD(), syscall.SHUT_RD)
}

// setKeepAliveTimes sets the idle time before the first keep-alive probe and
// the interval between probes; see Connection.SetKeepAliveConfig.
func setKeepAliveTimes(s int, idle, interval time.Duration) error {
	if idle >= 0 {
		if err := syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepIdle, keepAliveSeconds(idle)); err != nil {
			return err
		}
	}
	if interval >= 0 {
		return syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepInterval, keepAliveSeconds(interval))
	}
	return nil
}

// setKeepAliveCount sets how many unanswered probes end the connection.
func setKeepAliveCount(s int, count int) error {
	if count < 0 {
		return nil
	}
	if count == 0 {
		count = defaultKeepAliveCount
	}
	return syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepCount, count)
}

// File returns a duplicate of the connection's socket, as net.TCPConn.File
// does. The two share the socket, but closing the file does not close the
// connection, nor the other way round. A UDP listener's peer has no socket of
// its own and returns an error.
func (c *Connection) File() (*os.File, error) {
	if c.udp != nil && c.udp.listener != nil {
		return nil, errNoSocket
	}
	name := fileName(c.LocalAddr(), c.RemoteAddr())
	var f *os.File
	err := c.control("dup", func(s int) error {
		syscall.ForkLock.RLock()
		dup, err := syscall.Dup(s)
		if err == nil {
			syscall.CloseOnExec(dup)
		}
		syscall.ForkLock.RUnlock()
		if err != nil {
			return err
		}
		f = os.NewFile(uintptr(dup), name)
		return nil
	})
	return f, err
}

// fileName names a connection's file as the net package does.
func fileName(local, remote net.Addr) string {
	name := ""
	if local != nil {
		name = local.Network() + ":" + local.String()
	}
	if remote != nil {
		name += "->" + remote.String()
	}
	return name
}
