//go:build linux

package fib

import (
	"syscall"
	"unsafe"
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
