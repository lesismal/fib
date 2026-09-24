//go:build darwin

package fib

import (
	"syscall"
	"unsafe"
)

// sysRecvmsgX is recvmsg_x, macOS's batched recvmsg. It is not in the SDK's
// public headers, but the kernel has had it since macOS 10.11 and its
// counterpart sendmsg_x, and other QUIC stacks, quinn's among them, read
// with it; a kernel that refuses it has the loop read one datagram at a time.
const sysRecvmsgX = 480

// batchHeader is the kernel's struct msghdr_x: a message header that carries
// the length received into it.
type batchHeader struct {
	name       *byte
	namelen    uint32
	iov        *syscall.Iovec
	iovlen     int32
	control    *byte
	controllen uint32
	flags      int32
	datalen    uintptr
}

// recvBatch reads up to n datagrams with recvmsg_x.
func (b *udpBatch) recvBatch(fd, n int) (int, error) {
	for i := 0; i < n; i++ {
		b.hdrs[i] = batchHeader{name: (*byte)(unsafe.Pointer(&b.names[i])),
			namelen: syscall.SizeofSockaddrAny, iov: &b.iovs[i], iovlen: 1}
	}
	got, _, errno := syscall.Syscall6(sysRecvmsgX, uintptr(fd),
		uintptr(unsafe.Pointer(&b.hdrs[0])), uintptr(n), 0, 0, 0)
	if errno != 0 {
		return 0, errno
	}
	for i := 0; i < int(got); i++ {
		b.lens[i] = int(b.hdrs[i].datalen)
	}
	return int(got), nil
}

// sysSendmsgX is sendmsg_x, recvmsg_x's counterpart, which takes a
// destination for each datagram on an unconnected socket as sendmmsg does.
const sysSendmsgX = 481

// sendBatchSys sends datagrams with sendmsg_x, every one of them to to, or
// on a connected socket when to is nil.
func sendBatchSys(fd int, to syscall.Sockaddr, datagrams [][]byte) (int, error) {
	var name syscall.RawSockaddrAny
	namelen := rawSockaddr(to, &name)
	var hdrs [udpBatchSize]batchHeader
	var iovs [udpBatchSize]syscall.Iovec
	for i, d := range datagrams {
		iovs[i].Base = &d[0]
		iovs[i].SetLen(len(d))
		h := &hdrs[i]
		if namelen > 0 {
			h.name, h.namelen = (*byte)(unsafe.Pointer(&name)), namelen
		}
		h.iov, h.iovlen, h.datalen = &iovs[i], 1, uintptr(len(d))
	}
	got, _, errno := syscall.Syscall6(sysSendmsgX, uintptr(fd),
		uintptr(unsafe.Pointer(&hdrs[0])), uintptr(len(datagrams)), 0, 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(got), nil
}

// rawSockaddr writes sa in the form the kernel takes it into rsa and returns
// its length, or 0 for a nil sa.
func rawSockaddr(sa syscall.Sockaddr, rsa *syscall.RawSockaddrAny) uint32 {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		p := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		p.Len, p.Family = syscall.SizeofSockaddrInet4, syscall.AF_INET
		port := (*[2]byte)(unsafe.Pointer(&p.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		p.Addr = a.Addr
		return syscall.SizeofSockaddrInet4
	case *syscall.SockaddrInet6:
		p := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		p.Len, p.Family = syscall.SizeofSockaddrInet6, syscall.AF_INET6
		port := (*[2]byte)(unsafe.Pointer(&p.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		p.Scope_id = a.ZoneId
		p.Addr = a.Addr
		return syscall.SizeofSockaddrInet6
	}
	return 0
}
