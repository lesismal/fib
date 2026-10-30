//go:build linux

package fib

import (
	"syscall"
	"unsafe"

	"github.com/lesismal/fib/internal/sys"
)

// kernelRecords says SendRawRecord can send here: Linux's kernel TLS takes a
// record's type in a control message beside its bytes.
const kernelRecords = true

// The kernel TLS control message that sets the type of the record a send
// makes, from linux/tls.h.
const (
	solTLS           = 282
	tlsSetRecordType = 1
)

// sysSendRecord writes data as a record of type typ, which the kernel seals.
// A write the socket takes only part of leaves the rest to be sent again,
// with the type again, as a record of its own. Callers hold c.mu.
func (c *Connection) sysSendRecord(typ uint8, data []byte) (int, error) {
	// A word-aligned buffer, as a cmsghdr needs, with room for one byte of
	// data.
	var space [4]uint64
	oob := unsafe.Slice((*byte)(unsafe.Pointer(&space[0])), syscall.CmsgSpace(1))
	h := (*syscall.Cmsghdr)(unsafe.Pointer(&oob[0]))
	h.Level, h.Type = solTLS, tlsSetRecordType
	h.SetLen(syscall.CmsgLen(1))
	oob[syscall.CmsgLen(0)] = typ
	n, err := syscall.SendmsgN(c.FD(), data, oob, nil, syscall.MSG_NOSIGNAL)
	return max(n, 0), err
}

// sysWriteMore writes buffers, which must hold some bytes, telling the kernel
// more follows them: TCP then holds a segment they leave unfilled for what
// follows rather than pushing it, and the kernel's TLS hands the records they
// fill to TCP without pushing each. Callers hold c.mu.
func (c *Connection) sysWriteMore(buffers [][]byte) (int, error) {
	var iov [maxWritevItems]syscall.Iovec
	count := 0
	for _, b := range buffers {
		if len(b) > 0 && count < len(iov) {
			iov[count].Base = &b[0]
			iov[count].SetLen(len(b))
			count++
		}
	}
	var msg syscall.Msghdr
	msg.Iov = &iov[0]
	setMsgIovlen(&msg.Iovlen, count)
	for {
		r, _, errno := syscall.Syscall(sys.SysSendmsg, uintptr(c.FD()), uintptr(unsafe.Pointer(&msg)),
			syscall.MSG_MORE|syscall.MSG_NOSIGNAL)
		if errno == syscall.EINTR {
			continue
		}
		if errno != 0 {
			return 0, errno
		}
		return int(r), nil
	}
}

// setMsgIovlen sets a Msghdr's Iovlen, which is 32 or 64 bits wide depending
// on the architecture.
func setMsgIovlen[T ~uint32 | ~uint64](field *T, n int) { *field = T(n) }

// corkKernelTLS corks the socket, on, or uncorks it, where the kernel's TLS
// seals what it sends, and reports whether it did: a sendfile corked puts
// what it sends in as few TCP sends as it fills, rather than pushing each
// record the kernel seals of it, as its last pipe's worth otherwise goes.
// Callers hold c.mu.
func (c *Connection) corkKernelTLS(on bool) bool {
	if !c.flags.has(flagKernelTLS) {
		return false
	}
	v := 0
	if on {
		v = 1
	}
	return syscall.SetsockoptInt(c.FD(), syscall.IPPROTO_TCP, syscall.TCP_CORK, v) == nil
}
