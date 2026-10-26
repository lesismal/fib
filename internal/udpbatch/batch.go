//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package udpbatch

import (
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/lesismal/fib/internal/sys"
)

// Size is the most datagrams one batched receive takes.
const Size = 32

// Batch is where the event loop reads a UDP socket's datagrams, as many
// as Size with one system call: recvmmsg on Linux, recvmsg_x on
// macOS. Each datagram gets a slot of MaxDatagramSize, since its size is not
// known before it is read, but the kernel writes only what a datagram fills,
// so what the batch holds of memory is about a page a slot. Event-loop
// ownership.
type Batch struct {
	buf   []byte
	names [Size]syscall.RawSockaddrAny
	iovs  [Size]syscall.Iovec
	hdrs  [Size]batchHeader
	lens  [Size]int
	// ctl is where the control message of each read of the last batch lands,
	// which on Linux says what a coalesced run of datagrams was made of:
	// UDP_GRO's size for each but its last (see Segments).
	ctl [Size][gsoControlLen]byte
	// gso is the size of the datagrams the i-th read of the last batch holds,
	// when the kernel coalesced several of them into it, and zero when it
	// holds one.
	gso [Size]int
	// single is set once the kernel has refused a batched receive, after
	// which datagrams are read one at a time.
	single bool
}

func New() *Batch {
	b := &Batch{buf: make([]byte, Size*MaxDatagramSize)}
	for i := range b.iovs {
		b.iovs[i].Base = &b.buf[i*MaxDatagramSize]
		b.iovs[i].SetLen(MaxDatagramSize)
	}
	return b
}

// Addr is the address the i-th of the last receive's reads came from, as
// the kernel wrote it; the datagrams a coalesced read holds all came from
// it.
func (b *Batch) Addr(i int) *syscall.RawSockaddrAny { return &b.names[i] }

// Datagram is the whole of the i-th of the last receive's reads, which the
// kernel may have made of several datagrams (see Segments).
func (b *Batch) Datagram(i int) []byte {
	return b.buf[i*MaxDatagramSize : i*MaxDatagramSize+b.lens[i]]
}

// Segments returns how many datagrams the i-th of the last receive's reads
// holds: more than one where the kernel coalesced a run of them with GRO,
// whose size it gave in the read's control message, the last of them being
// as long as the read leaves.
func (b *Batch) Segments(i int) int {
	if g := b.gso[i]; g > 0 && b.lens[i] > g {
		return (b.lens[i] + g - 1) / g
	}
	return 1
}

// Segment is the j-th datagram of the i-th of the last receive's reads,
// where j is less than Segments(i).
func (b *Batch) Segment(i, j int) []byte {
	base := i * MaxDatagramSize
	start, end := base, base+b.lens[i]
	if g := b.gso[i]; g > 0 && b.lens[i] > g {
		start = base + j*g
		if e := start + g; e < end {
			end = e
		}
	}
	return b.buf[start:end]
}

// Recv takes up to n of fd's receives, and reports how many it took and
// whether that emptied the socket, which a batch that came back short says.
// one asks for a single receive, with an ordinary receive where that can
// carry what a batched one does, which costs less than a batch of one: for
// a socket the poller says holds no more than a datagram or so. A receive
// may hold several datagrams that the kernel coalesced; see Segments.
func (b *Batch) Recv(fd, n int, one bool) (int, bool, error) {
	if !b.single && (!one || batchOne) {
		want := n
		if one {
			want = 1
		}
		got, err := b.recvBatch(fd, want)
		if err == nil {
			return got, got < want, nil
		}
		if !batchRefused(err) {
			return 0, false, err
		}
		b.single = true
	}
	got, err := sys.Recvfrom(fd, b.buf[:MaxDatagramSize], &b.names[0])
	if err != nil {
		return 0, false, err
	}
	// An ordinary receive carries no control message, so it cannot say what
	// a run a coalescing kernel made of it; the batch reports it as one.
	b.lens[0], b.gso[0] = got, 0
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
	hdrs [Size]batchHeader
	iovs [Size]syscall.Iovec
	// ctl holds the UDP_SEGMENT control message of each message that sends
	// a run of datagrams as one, and ends where each message's datagrams
	// end; see sendBatchSys on Linux.
	ctl  [Size][gsoControlLen]byte
	ends [Size]int
}

// gsoControlLen has room for a control message carrying one number, which is
// what UDP_SEGMENT sends and UDP_GRO receives, on every platform.
const gsoControlLen = 32

var sendScratches = sync.Pool{New: func() any { return new(sendScratch) }}

// singleSends is set once the kernel has refused a batched send, after which
// datagrams are sent one at a time.
var singleSends atomic.Bool

// Send sends as many of datagrams as one system call takes, up to
// Size, to to, or on a connected socket when to is nil, and reports
// how many it sent.
func Send(fd int, to syscall.Sockaddr, datagrams [][]byte) (int, error) {
	if !singleSends.Load() {
		n, err := sendBatchSys(fd, to, datagrams[:min(len(datagrams), Size)])
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
