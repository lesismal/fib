//go:build linux

package fib

import (
	"syscall"
	"unsafe"
)

// batchHeader is the kernel's struct mmsghdr: a message header and the length
// recvmmsg received into it.
type batchHeader struct {
	hdr syscall.Msghdr
	len uint32
}

// recvBatch reads up to n datagrams with recvmmsg.
func (b *udpBatch) recvBatch(fd, n int) (int, error) {
	for i := 0; i < n; i++ {
		h := &b.hdrs[i].hdr
		h.Name = (*byte)(unsafe.Pointer(&b.names[i]))
		h.Namelen = syscall.SizeofSockaddrAny
		h.Iov = &b.iovs[i]
		h.Iovlen = 1
	}
	got, _, errno := syscall.Syscall6(syscall.SYS_RECVMMSG, uintptr(fd),
		uintptr(unsafe.Pointer(&b.hdrs[0])), uintptr(n), 0, 0, 0)
	if errno != 0 {
		return 0, errno
	}
	for i := 0; i < int(got); i++ {
		b.lens[i] = int(b.hdrs[i].len)
	}
	return int(got), nil
}

// sendBatchSys sends datagrams with sendmmsg, every one of them to to, or
// on a connected socket when to is nil.
func sendBatchSys(fd int, to syscall.Sockaddr, datagrams [][]byte) (int, error) {
	s := sendScratches.Get().(*sendScratch)
	namelen := rawSockaddr(to, &s.name)
	for i, d := range datagrams {
		s.iovs[i].Base = &d[0]
		s.iovs[i].SetLen(len(d))
		h := &s.hdrs[i].hdr
		h.Name, h.Namelen = nil, 0
		if namelen > 0 {
			h.Name, h.Namelen = (*byte)(unsafe.Pointer(&s.name)), namelen
		}
		h.Iov, h.Iovlen = &s.iovs[i], 1
	}
	got, _, errno := syscall.Syscall6(sysSendmmsg, uintptr(fd),
		uintptr(unsafe.Pointer(&s.hdrs[0])), uintptr(len(datagrams)), 0, 0, 0)
	// The datagrams are the caller's again: nothing kept may point at them.
	clear(s.iovs[:len(datagrams)])
	sendScratches.Put(s)
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
		p.Family = syscall.AF_INET
		port := (*[2]byte)(unsafe.Pointer(&p.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		p.Addr = a.Addr
		return syscall.SizeofSockaddrInet4
	case *syscall.SockaddrInet6:
		p := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		p.Family = syscall.AF_INET6
		port := (*[2]byte)(unsafe.Pointer(&p.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		p.Scope_id = a.ZoneId
		p.Addr = a.Addr
		return syscall.SizeofSockaddrInet6
	}
	return 0
}
