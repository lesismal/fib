//go:build linux

package udpbatch

import (
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/lesismal/fib/internal/sys"
)

// batchHeader is the kernel's struct mmsghdr: a message header and the length
// recvmmsg received into it.
type batchHeader struct {
	hdr syscall.Msghdr
	len uint32
}

// recvBatch reads up to n datagrams with recvmmsg.
func (b *Batch) recvBatch(fd, n int) (int, error) {
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

// UDP_SEGMENT (generic segmentation offload for UDP, Linux 4.18): a message
// whose control data carries it is a run of datagrams of that size, the last
// of which may be shorter, which the kernel takes through its stack once and
// cuts up at the end. maxGSOSegments and maxGSOBytes keep a run within what
// every kernel that has it takes.
const (
	solUDP         = 17
	udpSegment     = 103
	maxGSOSegments = 64
	maxGSOBytes    = 65000
)

// setLen sets a length field of the kernel's structures, which is as wide as
// a pointer.
func setLen[T uint32 | uint64](field *T, n int) { *field = T(n) }

// gsoOff is set once the kernel has refused a segmented send, as one without
// UDP_SEGMENT, or a device that cannot checksum it, does; datagrams are then
// sent a message each.
var gsoOff atomic.Bool

// sendBatchSys sends datagrams with sendmmsg, every one of them to to, or
// on a connected socket when to is nil, and reports how many it sent. A run
// of datagrams of one size, the last of which may be shorter, goes as one
// message segmented with UDP_SEGMENT, which costs the kernel one pass
// through its UDP and IP stack rather than one a datagram: a QUIC response
// of 15KB is thirteen datagrams of 1200 bytes, and sending them a message
// each took half a static HTTP/3 server's time.
func sendBatchSys(fd int, to syscall.Sockaddr, datagrams [][]byte) (int, error) {
	s := sendScratches.Get().(*sendScratch)
	namelen := rawSockaddr(to, &s.name)
	segment := !gsoOff.Load()
	msgs := 0
	for i := 0; i < len(datagrams); msgs++ {
		size := len(datagrams[i])
		j, total := i+1, size
		for segment && j < len(datagrams) && j-i < maxGSOSegments && len(datagrams[j-1]) == size &&
			len(datagrams[j]) <= size && total+len(datagrams[j]) <= maxGSOBytes {
			total += len(datagrams[j])
			j++
		}
		for k := i; k < j; k++ {
			s.iovs[k].Base = &datagrams[k][0]
			s.iovs[k].SetLen(len(datagrams[k]))
		}
		h := &s.hdrs[msgs].hdr
		h.Name, h.Namelen = nil, 0
		if namelen > 0 {
			h.Name, h.Namelen = (*byte)(unsafe.Pointer(&s.name)), namelen
		}
		h.Iov = &s.iovs[i]
		setLen(&h.Iovlen, j-i)
		h.Control, h.Controllen = nil, 0
		if j-i > 1 {
			ctl := &s.ctl[msgs]
			cmsg := (*syscall.Cmsghdr)(unsafe.Pointer(&ctl[0]))
			cmsg.Level, cmsg.Type = solUDP, udpSegment
			cmsg.SetLen(syscall.CmsgLen(2))
			*(*uint16)(unsafe.Pointer(&ctl[syscall.CmsgLen(0)])) = uint16(size)
			h.Control = &ctl[0]
			h.SetControllen(syscall.CmsgSpace(2))
		}
		s.ends[msgs] = j
		i = j
	}
	got, _, errno := syscall.Syscall6(sys.SysSendmmsg, uintptr(fd),
		uintptr(unsafe.Pointer(&s.hdrs[0])), uintptr(msgs), 0, 0, 0)
	sent := 0
	if errno == 0 && got > 0 {
		sent = s.ends[got-1]
	}
	// The datagrams are the caller's again: nothing kept may point at them.
	clear(s.iovs[:len(datagrams)])
	sendScratches.Put(s)
	if errno != 0 {
		if segment && msgs < len(datagrams) && (errno == syscall.EIO || errno == syscall.EINVAL) {
			gsoOff.Store(true)
			return sendBatchSys(fd, to, datagrams)
		}
		return 0, errno
	}
	return sent, nil
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
