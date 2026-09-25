//go:build linux || darwin

package fib

import (
	"sync"
	"sync/atomic"
	"syscall"
)

// udpBatchSize is the most datagrams one batched receive takes.
const udpBatchSize = 32

// udpBatch is where the event loop reads a UDP socket's datagrams, as many
// as udpBatchSize with one system call: recvmmsg on Linux, recvmsg_x on
// macOS. Each datagram gets a slot of maxDatagramSize, since its size is not
// known before it is read, but the kernel writes only what a datagram fills,
// so what the batch holds of memory is about a page a slot. Event-loop
// ownership.
type udpBatch struct {
	buf   []byte
	names [udpBatchSize]syscall.RawSockaddrAny
	iovs  [udpBatchSize]syscall.Iovec
	hdrs  [udpBatchSize]batchHeader
	lens  [udpBatchSize]int
	// single is set once the kernel has refused a batched receive, after
	// which datagrams are read one at a time.
	single bool
}

func newUDPBatch() *udpBatch {
	b := &udpBatch{buf: make([]byte, udpBatchSize*maxDatagramSize)}
	for i := range b.iovs {
		b.iovs[i].Base = &b.buf[i*maxDatagramSize]
		b.iovs[i].SetLen(maxDatagramSize)
	}
	return b
}

// datagram is the i-th datagram the last receive read.
func (b *udpBatch) datagram(i int) []byte {
	return b.buf[i*maxDatagramSize : i*maxDatagramSize+b.lens[i]]
}

// recv reads up to n datagrams from fd, and reports how many it read and
// whether that emptied the socket, which a batch that came back short says.
// one reads a single datagram with an ordinary receive, which costs less
// than a batch of one does: for a socket the poller says holds no more than
// a datagram or so.
func (b *udpBatch) recv(fd, n int, one bool) (int, bool, error) {
	if !b.single && !one {
		got, err := b.recvBatch(fd, n)
		if err == nil {
			return got, got < n, nil
		}
		if !batchRefused(err) {
			return 0, false, err
		}
		b.single = true
	}
	got, err := recvfrom(fd, b.buf[:maxDatagramSize], &b.names[0])
	if err != nil {
		return 0, false, err
	}
	b.lens[0] = got
	return 1, false, nil
}

// batchRefused reports whether err is a kernel refusing the batched calls
// themselves, rather than failing one.
func batchRefused(err error) bool { return err == syscall.ENOSYS || err == syscall.EOPNOTSUPP }

// sendScratch is the memory a batched send describes its datagrams to the
// kernel in. The kernel is handed its address as a number, which escape
// analysis cannot follow, so a stack copy of it would be moved to the heap
// on every send; one from the pool is not.
type sendScratch struct {
	name syscall.RawSockaddrAny
	hdrs [udpBatchSize]batchHeader
	iovs [udpBatchSize]syscall.Iovec
}

var sendScratches = sync.Pool{New: func() any { return new(sendScratch) }}

// singleSends is set once the kernel has refused a batched send, after which
// datagrams are sent one at a time.
var singleSends atomic.Bool

// sendBatch sends as many of datagrams as one system call takes, up to
// udpBatchSize, to to, or on a connected socket when to is nil, and reports
// how many it sent.
func sendBatch(fd int, to syscall.Sockaddr, datagrams [][]byte) (int, error) {
	if !singleSends.Load() {
		n, err := sendBatchSys(fd, to, datagrams[:min(len(datagrams), udpBatchSize)])
		if err == nil || !batchRefused(err) {
			return n, err
		}
		singleSends.Store(true)
	}
	if to == nil {
		_, err := syscall.Write(fd, datagrams[0])
		return 1, err
	}
	return 1, syscall.Sendto(fd, datagrams[0], 0, to)
}
