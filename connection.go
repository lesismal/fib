//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/sidepool"
)

// sendItem is one queued chunk. pooled says data came from the buffer pool,
// rather than being an array the caller handed over and the connection does
// not own. Returning a pooled array as soon as the item drains is what keeps a
// backpressured connection from allocating a fresh one per reply.
//
// file, when set, makes the item a stretch of a file, which SendFile queued.
// Where the file goes to the socket by sendfile, data stays nil; otherwise data
// is the chunk of it read so far, sealed where the file has a FileSealer, and
// not yet sent.
//
// record, when set, makes data one TLS record of that type for the kernel to
// seal, which SendRawRecord queued: it is written on its own, with the type
// beside it.
type sendItem struct {
	data   []byte
	offset int
	pooled bool
	record uint8
	file   *fileSegment
}

// Connection is safe to use from callback and application goroutines.
//
// A server holds one for every connection, so its fields are laid out to keep
// it small, and in the order a round touches them: the words a round reads or
// writes first, then the flags and booleans it checks, which share words
// rather than each padding one out, and last what only opening or closing
// needs. On 64-bit Linux and macOS it is 192 bytes, a size class whose objects
// start on a cache-line boundary, so a read round touches three lines of it
// rather than the four a 208-byte object always straddles. With the
// attachment kept in it rather than boxed, that cut go-websocket-benchmark's
// user time per message on 8 Ryzen 7800X3D CPUs by 5.5% pipelined (348 to
// 329ns) and 4.6% in echo.
type Connection struct {
	connPlatform
	engine *Engine
	// handler receives this connection's callbacks: the engine's handler for
	// an accepted connection, or the one DialWithHandler was given.
	handler      Handler
	mu           sync.Mutex
	sends        []sendItem
	sendHead     int
	pendingBytes atomic.Int64
	// charged is how much of pendingBytes counts against the server-wide
	// budget; see chargeLocked. Guarded by mu.
	charged int64
	// corkedBytes is output queued under a cork that pendingBytes does not
	// count yet; see queueCorkedLocked. Guarded by mu.
	corkedBytes int64
	// attachType and attachData are the two words of the attachment, an any
	// kept unboxed, so that reading it is not a trip to an object of its own.
	// SetAttachment writes them under mu between two steps of the sequence
	// in flags, which Attachment reads them between; see attachSeq.
	attachType unsafe.Pointer
	attachData unsafe.Pointer
	// layer, when set, carries every send, as TLS does to encrypt it.
	layer Layer
	// udp is set for a UDP connection, which exchanges datagrams.
	udp *udpState
	// dialing is set while an outbound connect is still in progress, and
	// cleared when it completes or fails. Event-loop ownership.
	dialing *dialRequest
	// deadlines close the connection when they pass. They are allocated when
	// the first is set, which most connections never do, and also hold the
	// other things few connections need: an unusual close reason, and the
	// writers waiting in waitOutput. Guarded by mu.
	deadlines *connectionDeadlines
	// flags holds the connFlag bits, which are read without the mutex.
	flags connFlags
	// bindingID is the index of the Binding that took the connection, plus
	// one, so that zero, a dialed connection's, names none; it never
	// changes. It sits beside flags, in what would be padding.
	bindingID int32
	// pendingEvents is the readiness, in the ev bits, that the connection's
	// next round is to act on. Guarded by mu.
	pendingEvents uint16
	scheduled     bool
	closing       bool
	closed        bool
	readPaused    bool
	budgetPaused  bool
	// readStalled records that a read round stopped with bytes possibly
	// still in the socket, because the write backlog filled up first. Those
	// bytes raise no further edge on their own, so the event loop owes the
	// connection either a paused-then-resumed read or a direct redelivery.
	readStalled bool
	// readDeferred records that a round left a read undone because output
	// was still queued, and ended. Whatever empties the queue owes the
	// connection that read.
	readDeferred bool
	// hupOwed records that the peer ended its side while a read was stalled
	// or deferred. The edge that said so has been taken, and a read that
	// empties the socket stops at a short read without reaching the end, so
	// the round that next reads is handed the end with it.
	hupOwed bool
	// roundStalled records that the current round's read stopped with
	// bytes possibly left in the socket, for a hold or for the write
	// budget. Only the round that owns the connection touches it.
	roundStalled bool
	// closePending records that the loop has closed the connection and left
	// OnClose, and the release of the descriptor, to its last round; see
	// Engine.closeConnection. Guarded by mu.
	closePending bool
	// closeDue records that the connection's OnClose is still to run: set
	// with closePending, and cleared only once OnClose has returned, where
	// closePending is cleared as the last round takes the close on. A
	// connection whose OnClose is due is not yet finished, for Shutdown and
	// ShutdownBinding. Guarded by mu.
	closeDue bool
	// hangupNoted records that a hangup has been reported while a round was
	// running, which happens once; see noteEvent. Guarded by mu.
	hangupNoted    bool
	flushing       bool
	corked         bool
	closeAfterSend bool
	// writeShut records CloseWrite, after which sends are refused, and
	// shutWritePending a CloseWrite still waiting for queued output to reach
	// the socket before it shuts the write side. Guarded by mu.
	writeShut        bool
	shutWritePending bool
	// writeLimit is what SetWriteQueueLimit chose for this connection:
	// writeLimitSet says it did, and then writeLimit is the limit, zero or
	// less meaning none, rather than Config.MaxWriteQueueBytes. Guarded by
	// mu.
	writeLimit    int64
	writeLimitSet bool
	// lingerFor is how long a CloseAfterSendLinger goes on reading once its
	// output has gone; see lingerLocked. Guarded by mu.
	lingerFor time.Duration
	// dialed marks a connection Dial opened, as against one a listener
	// accepted; it never changes.
	dialed bool
	// closeCode is why the connection closed, which Read and Write report
	// to a caller that comes back to it afterwards: the usual reasons are
	// coded here, and any other kept in deadlines; see setCloseReasonLocked.
	// Guarded by mu.
	closeCode uint8
	// peer is the address the connection was accepted from, when accepting
	// it told, or the one SetRemoteAddrPort gave; see RemoteAddrPort.
	peer packedAddrPort
}

// inBinding reports whether the binding-th binding took c, or, for a binding
// below zero, whether any did.
func (c *Connection) inBinding(binding int) bool {
	return binding < 0 || int(c.bindingID)-1 == binding
}

// The ev bits have to fit pendingEvents.
const _ = uint16(evAll)

// connFlag is a bit of a connection's flags.
type connFlag uint32

const (
	// flagRawExposed records that the socket has been handed out, through
	// File or a RawConn, and so may have been duplicated: closing a socket
	// then leaves it in the event loop's interest set, from which it has to be
	// taken out first. It is set under mu, before the socket is handed out,
	// so a close, which marks the connection closed under mu first, sees it.
	flagRawExposed connFlag = 1 << iota
	// flagReadHeld is an application-driven pause, set through HoldReads by a
	// handler that has more input buffered than it has consumed. The read
	// loop checks it between reads.
	flagReadHeld
	// flagReadShut records CloseRead: the connection reads nothing more, and
	// the end of input that shutting the read side raises does not close it.
	flagReadShut
	// flagCoalesceOn and flagCoalesceOff record what SetCoalesceWrites chose
	// for the connection; with neither, Config.CoalesceWrites decides.
	flagCoalesceOn
	flagCoalesceOff
	// flagUnix marks a Unix socket, which has none of TCP's options. It is
	// set before the connection is handed out and never changes.
	flagUnix
	// flagLingering marks a CloseAfterSendLinger whose output has gone and
	// whose write side is shut: what still arrives is read and dropped, never
	// handed to OnData, until the peer ends its side or the linger runs out.
	flagLingering
	// flagNoWriteBackpressure records SetWriteBackpressure(false): the
	// connection's own backlog does not pause its reads.
	flagNoWriteBackpressure
	// flagCloseReason marks a reason CloseWithError recorded when it asked
	// for the close, which the close reports over whatever the event loop
	// meets before carrying it out: a layer that answers a failed handshake
	// with an alert has the peer hang up in reply, often before then.
	flagCloseReason
	// flagFileQueued marks a connection with some of a file SendFile queued
	// still to send, which connectionDeadlines.fileBytes counts, so that
	// what reads the backlog without the lock knows to look there too.
	flagFileQueued
	// flagPeerSet marks a peer address SetRemoteAddrPort gave, in peer, which
	// RemoteAddr then reports too.
	flagPeerSet
)

// attachSeq is the unit of the sequence count kept in the upper half of
// connFlags, which is odd while SetAttachment is writing the attachment.
const attachSeq = 1 << 16

// connFlags are the flags of a connection that are read without its mutex,
// packed into one atomic word rather than an atomic.Bool of four bytes apiece.
type connFlags struct{ bits atomic.Uint32 }

func (f *connFlags) has(flag connFlag) bool { return f.bits.Load()&uint32(flag) != 0 }

// set sets flag to on and reports whether it was set before.
func (f *connFlags) set(flag connFlag, on bool) bool {
	if on {
		return f.bits.Or(uint32(flag))&uint32(flag) != 0
	}
	return f.bits.And(^uint32(flag))&uint32(flag) != 0
}

// replace sets flag and clears other in one step, so that no reader sees both
// or neither.
func (f *connFlags) replace(other, flag connFlag) {
	for {
		bits := f.bits.Load()
		if f.bits.CompareAndSwap(bits, bits&^uint32(other)|uint32(flag)) {
			return
		}
	}
}

// packedAddrPort is an IP address and port without a zone, which is what
// accepting a connection reports its peer as, in 19 bytes rather than the 32
// of a netip.AddrPort, whose address keeps a pointer for the zone. It is all
// bytes, so it needs no alignment and packs among the booleans.
type packedAddrPort struct {
	// addr is the address in its 16-byte form, where an IPv4 address is
	// mapped into IPv6.
	addr [16]byte
	// port is big-endian.
	port [2]byte
	// kind is 4 for an IPv4 address, 6 for an IPv6 one, and 0 for none.
	kind byte
}

// packAddrPort packs ap, which must have no zone.
func packAddrPort(ap netip.AddrPort) packedAddrPort {
	if !ap.IsValid() {
		return packedAddrPort{}
	}
	p := packedAddrPort{addr: ap.Addr().As16(), kind: 6}
	binary.BigEndian.PutUint16(p.port[:], ap.Port())
	if ap.Addr().Is4() {
		p.kind = 4
	}
	return p
}

// valid reports whether p holds an address.
func (p *packedAddrPort) valid() bool { return p.kind != 0 }

// unpack returns the AddrPort p was packed from.
func (p *packedAddrPort) unpack() netip.AddrPort {
	port := binary.BigEndian.Uint16(p.port[:])
	switch p.kind {
	case 4:
		return netip.AddrPortFrom(netip.AddrFrom4([4]byte(p.addr[12:])), port)
	case 6:
		return netip.AddrPortFrom(netip.AddrFrom16(p.addr), port)
	}
	return netip.AddrPort{}
}

// Cork holds what is sent on the connection from now on until Flush, rather
// than writing each send as it is made, as a read round does for the replies
// its OnData makes. A goroutine that answers several requests of one
// connection away from its round corks it first, so that the answers reach
// the socket in one write, and must Flush when it is done: nothing else
// writes what a Cork holds, unless a read round of the connection flushes it
// meanwhile along with its own, as one does when it ends and every 256KB it
// reads. It does nothing on a UDP
// connection.
func (c *Connection) Cork() {
	if c.udp != nil {
		return
	}
	c.mu.Lock()
	c.corked = true
	c.traceLocked("cork")
	c.mu.Unlock()
}

// Corked reports whether the connection is corked: whether what is sent now
// is queued for a Flush, or for the end of the read round, rather than
// written. A CorkHolder asks as it takes a send.
func (c *Connection) Corked() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.corked
}

// SetCoalesceWrites sets whether the read rounds of this connection hold the
// replies a handler sends from OnData and write them together when the round
// ends, whatever Config.CoalesceWrites says for the engine. Call it from
// OnOpen, so that every round sees it; one already running keeps what it began
// with.
//
// It suits a protocol that knows how it writes. One that answers on the read
// round, such as HTTP/1 or WebSocket, gains from the default; one whose
// replies are produced away from the round, by handlers on a pool, batches
// them itself, and the round has little to hold for it. It does nothing on a
// UDP connection, whose sends are datagrams.
func (c *Connection) SetCoalesceWrites(enabled bool) {
	if enabled {
		c.flags.replace(flagCoalesceOff, flagCoalesceOn)
	} else {
		c.flags.replace(flagCoalesceOn, flagCoalesceOff)
	}
}

// coalesces reports whether a read round of c corks its replies.
func (c *Connection) coalesces() bool {
	switch bits := c.flags.bits.Load(); {
	case bits&uint32(flagCoalesceOn) != 0:
		return true
	case bits&uint32(flagCoalesceOff) != 0:
		return false
	}
	return c.engine.coalesceWrites
}

// Attachment returns application state associated with the connection.
func (c *Connection) Attachment() any {
	for {
		seq := c.flags.bits.Load() &^ (attachSeq - 1)
		if seq&attachSeq == 0 {
			typ := atomic.LoadPointer(&c.attachType)
			data := atomic.LoadPointer(&c.attachData)
			if c.flags.bits.Load()&^(attachSeq-1) == seq {
				var value any
				words := (*eface)(unsafe.Pointer(&value))
				words.typ, words.data = typ, data
				return value
			}
		}
		runtime.Gosched()
	}
}

// eface is the layout of an any.
type eface struct{ typ, data unsafe.Pointer }

// SetAttachment associates application state with the connection. Passing nil
// clears it. Protocol handlers use this to avoid a global connection-state map.
func (c *Connection) SetAttachment(value any) {
	words := (*eface)(unsafe.Pointer(&value))
	c.mu.Lock()
	c.flags.bits.Add(attachSeq)
	atomic.StorePointer(&c.attachType, words.typ)
	atomic.StorePointer(&c.attachData, words.data)
	c.flags.bits.Add(attachSeq)
	c.mu.Unlock()
}

// Close ends the connection, dropping whatever has been queued for sending
// but not yet handed to the kernel; CloseAfterSend waits for that instead. The
// close itself is carried out by the event loop, so there is no error to
// report and Close always returns nil, including for a connection that is
// already closing. It satisfies net.Conn and io.Closer.
func (c *Connection) Close() error {
	c.closeWithError(nil)
	return nil
}

// CloseAfterSend closes the connection after all data already accepted by Send
// has been handed to the kernel.
func (c *Connection) CloseAfterSend() {
	if l := c.layer; l != nil {
		l.CloseAfterSend()
		return
	}
	c.closeAfterSendRaw()
}

// CloseAfterSendLinger is CloseAfterSend for a connection whose peer may
// still be sending, such as an HTTP/1 client in the middle of an upload the
// server answered without reading. Closing a socket with input unread makes
// the kernel reset the connection, and a reset that reaches the peer before
// it has read what was sent throws that away. So once everything sent has
// gone, after the layer's closing message, the connection shuts its write
// side instead, and goes on reading and dropping whatever arrives, never
// handing it to OnData, until the peer ends its own side or d has passed;
// then it closes. A d of zero or less is CloseAfterSend.
func (c *Connection) CloseAfterSendLinger(d time.Duration) {
	if d > 0 && c.udp == nil {
		c.mu.Lock()
		if !c.closeAfterSend && c.lingerFor == 0 {
			c.lingerFor = d
		}
		c.mu.Unlock()
	}
	c.CloseAfterSend()
}

// closeAfterSendRaw is CloseAfterSend below any layer.
func (c *Connection) closeAfterSendRaw() {
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return
	}
	c.closeAfterSend = true
	closeNow := c.sendHead == len(c.sends) && !c.flushing
	if closeNow && c.lingerLocked() {
		closeNow = false
	}
	c.mu.Unlock()
	if closeNow {
		c.closeWithError(nil)
	}
}

// lingerLocked carries out the lingering close of CloseAfterSendLinger once
// its output has gone: it shuts the write side, arms the read deadline that
// bounds the linger, and reports true, or reports false when the connection
// is to close at once. Callers hold c.mu.
func (c *Connection) lingerLocked() bool {
	d := c.lingerFor
	if d <= 0 || c.closing || c.closed || c.flags.has(flagReadShut) {
		return false
	}
	c.lingerFor = 0
	if !c.writeShut || c.shutWritePending {
		c.writeShut, c.shutWritePending = true, false
		if syscall.Shutdown(c.rawSocket(), syscall.SHUT_WR) != nil {
			return false
		}
	}
	c.flags.set(flagLingering, true)
	at := time.Now().Add(d)
	if dl := c.deadlinesLocked(at); dl != nil {
		c.armDeadlineLocked(&dl.read, at, false)
	}
	return true
}

// lingerOver reports whether the lingering close has been carried out, or
// another close has overtaken it, so that what is left to read is not worth
// reading.
func (c *Connection) lingerOver() bool {
	c.mu.Lock()
	over := c.closing || c.closed
	c.mu.Unlock()
	return over
}

func (c *Connection) closeWithError(err error) {
	c.mu.Lock()
	request := !c.closing && !c.closed
	c.closing = true
	if request && err != nil {
		c.setCloseReasonLocked(err)
		c.flags.set(flagCloseReason, true)
	}
	c.wakeOutputWaitLocked()
	c.mu.Unlock()
	if request {
		c.engine.request(command{kind: commandClose, connection: c, err: err})
	}
}

// canWriteDirectlyLocked reports whether send may hand data straight to the
// socket instead of queueing it. Callers hold c.mu.
func (c *Connection) canWriteDirectlyLocked() bool {
	return c.sendHead == len(c.sends) && !c.flushing && !c.corked
}

// sendQueueClasses is how many capacities of item array the send queues share
// between them: 1, 2, 4 and so on up to twice the items one writev takes. A
// queue that outgrows the largest grows as append grows it, and its array is
// left to the garbage collector once it drains.
const sendQueueClasses = 8

// sendQueues hold the item arrays of drained send queues, one pool for each
// class, by the address of the first item, which an interface holds without
// an allocation of its own.
//
// A connection gives its array back whenever its queue drains, which is at the
// end of nearly every read round, since a round queues its replies under its
// cork and writes them all when it ends. Keeping the array instead would cost
// every connection that ever replied an array for good, 48 bytes for the one
// item a round usually needs, and 96 for a pipelining peer's two, for no
// purpose once it is idle. A round takes one back and returns it on the same
// worker, which keeps the two to sync.Pool's per-CPU slot.
var sendQueues [sendQueueClasses]sync.Pool

// takeSendQueue returns an empty item array with room for 1<<class items.
func takeSendQueue(class int) []sendItem {
	if first, _ := sendQueues[class].Get().(*sendItem); first != nil {
		return unsafe.Slice(first, 1<<class)[:0]
	}
	return make([]sendItem, 0, 1<<class)
}

// giveSendQueue hands back an array whose items have all been cleared, if it
// is one of the classes the queues share.
func giveSendQueue(items []sendItem) {
	n := cap(items)
	if n == 0 || n&(n-1) != 0 || n > 1<<(sendQueueClasses-1) {
		return
	}
	sendQueues[bits.TrailingZeros(uint(n))].Put(&items[:1][0])
}

// pushLocked appends item to the send queue, taking an array for it when the
// queue has none or no room left. Callers hold c.mu.
func (c *Connection) pushLocked(item sendItem) {
	if c.sendHead == len(c.sends) {
		// Nothing is queued any more, so the item slice can start over.
		c.sends, c.sendHead = c.sends[:0], 0
	}
	if len(c.sends) == cap(c.sends) {
		c.growQueueLocked()
	}
	c.sends = append(c.sends, item)
}

// growQueueLocked moves the queue to an array twice the size, or to one item
// for a queue that has none, leaving behind the items that have already
// drained. Callers hold c.mu.
func (c *Connection) growQueueLocked() {
	old := c.sends
	var next []sendItem
	if class := bits.Len(uint(cap(old))); class < sendQueueClasses {
		next = takeSendQueue(class)
	} else {
		next = make([]sendItem, 0, 2*cap(old))
	}
	c.sends = append(next, old[c.sendHead:]...)
	c.sendHead = 0
	clear(old)
	giveSendQueue(old)
}

// resetQueueLocked gives up the array of a drained queue. Every item has
// already surrendered its buffer and been cleared as it drained. Callers hold
// c.mu.
func (c *Connection) resetQueueLocked() {
	giveSendQueue(c.sends)
	c.sends, c.sendHead = nil, 0
}

// releaseItemLocked returns a drained item's buffer to the pool. A file item
// closes its descriptor. Callers hold c.mu.
func (c *Connection) releaseItemLocked(item *sendItem) {
	if item.pooled {
		c.engine.releaseSendBuffer(item.data)
	} else if item.record != 0 {
		bufferpool.Put(item.data)
	}
	if item.file != nil {
		if left := item.file.remaining; left > 0 {
			// Dropped before it all went, as a closing connection drops it.
			c.moveFileLocked(left)
		}
		item.file.close()
	}
	*item = sendItem{}
}

// queuedLocked is all the connection still has to send: what Send queued,
// counted or under a cork, and what is left of the files SendFile queued.
// Callers hold c.mu.
func (c *Connection) queuedLocked() int64 {
	n := c.pendingBytes.Load() + c.corkedBytes
	if d := c.deadlines; d != nil {
		n += d.fileBytes
	}
	return n
}

// addFileLocked counts n bytes of a file SendFile has queued. Callers hold
// c.mu.
func (c *Connection) addFileLocked(n int64) {
	d := c.deadlines
	if d == nil {
		d = &connectionDeadlines{}
		c.deadlines = d
	}
	d.fileBytes += n
	c.flags.set(flagFileQueued, true)
}

// moveFileLocked uncounts n bytes of a queued file without anything waiting
// on the backlog being told: they have been read into memory to be sent,
// where pendingBytes counts them, or dropped with a connection that is
// closing, which tells the waiters itself. Callers hold c.mu.
func (c *Connection) moveFileLocked(n int64) {
	if d := c.deadlines; d != nil {
		d.fileBytes = max(d.fileBytes-n, 0)
		if d.fileBytes == 0 {
			c.flags.set(flagFileQueued, false)
		}
	}
}

// fileSentLocked uncounts n bytes of a queued file that sendfile has handed
// the socket, and wakes what waits for the backlog to drain, as subPending
// does for bytes from memory. Callers hold c.mu.
func (c *Connection) fileSentLocked(n int64) {
	if n <= 0 {
		return
	}
	c.moveFileLocked(n)
	c.foldCorkedLocked()
	if w := c.outputWaitLocked(); w != nil && c.queuedLocked() <= w.release {
		w.cond.Broadcast()
	}
	c.fireNoticesLocked(nil)
}

// consumeLocked takes n bytes the socket accepted off the head of the queue.
// Callers hold c.mu.
func (c *Connection) consumeLocked(n int) {
	left := int64(n)
	for c.sendHead < len(c.sends) {
		item := &c.sends[c.sendHead]
		if item.file != nil && item.data == nil {
			// Sent from the file itself, by sendfile.
			if left < item.file.remaining {
				item.file.advance(left)
				break
			}
			left -= item.file.remaining
			item.file.advance(item.file.remaining)
			c.releaseItemLocked(item)
			c.sendHead++
			continue
		}
		remaining := int64(len(item.data) - item.offset)
		if left < remaining {
			item.offset += int(left)
			break
		}
		left -= remaining
		if item.file != nil && item.file.remaining > 0 {
			// A staged chunk of a file went out; the next is read when the
			// socket can take it. Nothing queued behind the file was sent.
			item.data, item.offset = nil, 0
			break
		}
		c.releaseItemLocked(item)
		c.sendHead++
	}
}

// queueLocked copies the parts into the send queue. Consecutive chunks merge
// into the trailing item's buffer, so a round that answers several messages
// leaves a single item for the socket and allocates nothing once that buffer
// has grown. Callers hold c.mu.
func (c *Connection) queueLocked(first, second []byte) {
	if n := len(c.sends); n > 0 {
		tail := &c.sends[n-1]
		// Merging is only safe while the socket has taken nothing from the
		// item: appending may move the array, and re-pointing an item a write
		// has already consumed part of would disturb that write. It also has
		// to fit: growing past the buffer's capacity would move the round's
		// replies up a size class each time a message is added to them, so
		// each round would leave behind a chain of ever larger buffers nothing
		// that round asks for again. Starting a new item instead keeps every
		// buffer at the size a round is served from, and writev still hands
		// the whole round to the socket in one call.
		if tail.pooled && tail.offset == 0 &&
			len(tail.data)+len(first)+len(second) <= cap(tail.data) {
			tail.data = append(append(tail.data, first...), second...)
			return
		}
	}
	// A chunk larger than the pooled size grows through the pool as well, so
	// that even an outsized message comes from a class rather than from a
	// fresh allocation append would have had to make.
	data := bufferpool.Join(c.engine.acquireSendBuffer(), first, second)
	c.pushLocked(sendItem{data: data, pooled: true})
}

// queueCorkedLocked is how a corked connection takes a send, which is how a
// read round takes every reply its handler makes. Nothing is written while
// corked, so a send only has to be queued, and it nearly always merges into
// the item the round's first reply started. The rest of a send's work waits
// for whoever next needs it: the bytes are counted in corkedBytes, which
// foldCorkedLocked moves into pendingBytes before anything reads or moves the
// backlog, and whether the budget counts them, or a watermark has been
// crossed, is settled when the cork comes off, as it was before. Callers hold
// c.mu.
func (c *Connection) queueCorkedLocked(first, second []byte) {
	c.queueLocked(first, second)
	c.corkedBytes += int64(len(first) + len(second))
}

// queueCorkedSharedLocked is queueCorkedLocked for a parts send whose second
// part stays the caller's: the head is copied into the cork's buffer as any
// reply is, and the shared part is queued as the caller gave it, so that a
// corked response above the copy threshold costs its body's memory rather than
// a copy of it. Callers hold c.mu.
func (c *Connection) queueCorkedSharedLocked(first, second []byte) {
	c.queueSharedPartsLocked(first, second)
	c.corkedBytes += int64(len(first) + len(second))
}

// queueSharedPartsLocked queues what a parts send the caller shares did not
// hand the socket: first, the head of the message, is copied into the queue's
// pooled buffer, and second is queued as the caller gave it, since the
// connection does not own it and nothing here writes into it. Callers hold
// c.mu.
func (c *Connection) queueSharedPartsLocked(first, second []byte) {
	if len(first) > 0 {
		c.queueLocked(first, nil)
	}
	if len(second) > 0 {
		c.queueOwnedLocked(second)
	}
}

// foldCorkedLocked counts in pendingBytes what queueCorkedLocked queued
// without counting. Whatever reads or moves the backlog calls it first.
// Callers hold c.mu.
func (c *Connection) foldCorkedLocked() {
	if n := c.corkedBytes; n != 0 {
		c.corkedBytes = 0
		c.pendingBytes.Add(n)
	}
}

// queueOwnedLocked queues data the caller handed over. The connection does not
// own the array, so the item carries no pooled buffer and later chunks cannot
// merge into it. Callers hold c.mu.
func (c *Connection) queueOwnedLocked(data []byte) {
	c.pushLocked(sendItem{data: data})
}

// settlePauseLocked brings the pause up to date with the backlog wherever
// it runs, and hands a read the connection is owed back to the round that is
// running, and reports whether the event loop has to settle the rest. owed
// says the caller just recorded such a read.
//
// Where the backend leaves read readiness registered while reads are
// paused (see pausesInPlace), pausing and resuming is the readPaused flag
// alone, which needs no event loop: a worker that drains its backlog below
// the low watermark resumes its own reads and goes on to read in the same
// round, rather than asking the loop to re-register the socket and waiting
// for the event that re-registering raises and the task that event starts.
// The server-wide budget stays the loop's, which keeps the connections it
// holds back. Callers hold c.mu.
func (c *Connection) settlePauseLocked(owed bool) bool {
	c.foldCorkedLocked()
	pause, byBudget := c.pauseDecision(c.readPaused)
	if !pausesInPlace || byBudget || c.budgetPaused {
		return owed || pause != c.readPaused
	}
	if c.closing || c.closed {
		return false
	}
	if pause != c.readPaused {
		c.readPaused = pause
		if pause {
			c.engine.readsPausedByWatermark.Add(1)
		} else {
			c.engine.readsResumed.Add(1)
		}
	}
	if pause || !c.readStalled {
		return false
	}
	if !c.scheduled {
		// No round is running to take the read; the loop hands it over.
		return true
	}
	c.readStalled = false
	c.pendingEvents |= uint16(evIn)
	return false
}

// pauseDecision is the single definition of whether a connection's reads should
// be paused, used by the worker to decide whether a refresh is worth asking for
// and by the event loop to carry it out, so the two cannot disagree. The second
// result reports that the server-wide budget, rather than this connection's own
// backlog, is what forces the pause: such a connection may have nothing left to
// flush and so cannot re-evaluate on its own, and the event loop has to wake it
// once the budget recovers.
func (c *Connection) pauseDecision(readPaused bool) (pause, byBudget bool) {
	if c.udp != nil {
		// Datagrams are never queued for sending, so there is nothing to wait
		// for, and a flood is bounded by the receive queue instead.
		return false, false
	}
	if c.flags.has(flagReadHeld) {
		// The application asked for the socket to be left alone; only it can
		// say when reads resume.
		return true, false
	}
	e := c.engine
	if e.maxPendingBytes > 0 && e.pendingTotal.Load() >= e.maxPendingBytes {
		return true, true
	}
	if e.writeHighWatermark <= 0 || c.flags.has(flagNoWriteBackpressure) {
		return false, false
	}
	pendingBytes := c.pendingBytes.Load()
	if pendingBytes >= int64(e.writeHighWatermark) {
		return true, false
	}
	// Hysteresis: once paused, stay paused until the backlog falls well below
	// the watermark. Resuming at the watermark itself would make every reply
	// re-cross it, and each crossing pauses and resumes reads: a flag where
	// they pause in place, a trip through the event loop where they do not.
	return readPaused && pendingBytes > int64(e.writeLowWatermark), false
}

// addPending grows the connection's outbound backlog by n bytes it has just
// queued. Bytes queued outside a cork are queued because the socket would not
// take them, which is backlog, and count against the server-wide budget at
// once; bytes a corked round queues for its own replies count only if they are
// still queued once the round has flushed them; see chargeLocked. Callers hold
// c.mu.
func (c *Connection) addPending(n int64) {
	if n <= 0 {
		return
	}
	c.pendingBytes.Add(n)
	if !c.corked {
		c.chargeLocked()
	}
}

// chargeLocked counts against the server-wide budget whatever of the
// connection's backlog it does not count yet.
//
// The budget is one counter for every connection of the engine and its
// pollers, so moving it is a write every CPU contends for. A round's replies
// are queued under its cork and nearly always flushed whole when it ends, so
// they are kept out of it: counting each reply in and out again held HttpArena's
// pipelined HTTP/1 profile on 64 CPUs to 15.3M responses a second against
// 16.6M without. Only what a write leaves behind is counted, which is what the
// budget bounds anyway: output the peers are not taking. Callers hold c.mu.
func (c *Connection) chargeLocked() {
	c.foldCorkedLocked()
	if delta := c.pendingBytes.Load() - c.charged; delta > 0 {
		c.charged += delta
		c.engine.pendingTotal.Add(delta)
	}
}

// roundChargeBytes is how much output a round may hold under its cork before
// it counts against the server-wide budget anyway. A round's replies are kept
// out of the budget because they nearly always go out when it ends; one that
// queues more than this is the kind the budget is there for, and many such
// rounds at once would otherwise read on, each to its own watermark, past a
// budget that cannot see what they hold.
const roundChargeBytes = 32 << 10

// roundFlushBytes is how much a round reads, without a read coming up short,
// before it hands its corked replies to the socket and looks whether the
// connection is closing. A round otherwise ends only on a short read, which a
// peer that sends faster than the round reads may not allow for a long time,
// and until then a reply, and a CloseAfterSend waiting behind it, would wait
// with it.
const roundFlushBytes = 256 << 10

// settleLongRound is what a round that has read roundFlushBytes does before
// it reads on: it flushes the replies its cork holds, corking again for the
// ones to come, and reports stop once the connection is closing, which ends
// the round so that the close, which waits for it, can go ahead.
func (c *Connection) settleLongRound() (stop bool, err error) {
	if c.coalesces() {
		if err := c.uncork(); err != nil {
			return true, err
		}
	}
	c.mu.Lock()
	stop = c.closing || c.closed
	if !stop && c.coalesces() {
		c.corked = true
	}
	c.mu.Unlock()
	return stop, nil
}

// chargeRound brings the backlog up to date before a round decides whether to
// read on: it counts the replies its cork appended uncounted, and counts the
// round's output against the budget once it has grown past roundChargeBytes,
// so that the budget sees it first.
func (c *Connection) chargeRound() {
	c.mu.Lock()
	c.foldCorkedLocked()
	if c.pendingBytes.Load() >= roundChargeBytes {
		c.chargeLocked()
	}
	c.mu.Unlock()
}

// subPending shrinks the connection's backlog by n bytes the socket took, and
// gives back whatever of it the budget no longer has to count. Callers hold
// c.mu.
func (c *Connection) subPending(n int64) {
	if n <= 0 {
		return
	}
	c.foldCorkedLocked()
	pending := c.pendingBytes.Add(-n)
	if pending < 0 {
		c.pendingBytes.Store(0)
		pending = 0
	}
	if w := c.outputWaitLocked(); w != nil && c.queuedLocked() <= w.release {
		w.cond.Broadcast()
	}
	c.fireNoticesLocked(nil)
	if c.charged > pending {
		released := c.charged - pending
		c.charged = pending
		c.engine.releaseBudget(released)
	}
}

// Send copies data before returning. It first attempts a direct nonblocking write.
func (c *Connection) Send(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.send(data, sendCopy)
}

// SendOwned sends data without copying it. Ownership transfers to the
// connection immediately; the caller must not access data after the call.
func (c *Connection) SendOwned(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.send(data, sendOwned)
}

// SendShared sends data without copying it, like SendOwned, but promises to
// treat it as read-only: the connection never writes into data or hands it to
// a pool, and only reads it until it is written. The same array can therefore
// go to many connections at once, as a broadcast does, and what the slow ones
// cannot take yet waits in their queues as one shared array instead of a copy
// each. The caller must not change data afterwards, but may go on reading it.
// It is copied, as Send copies, only where a layer such as TLS transforms it.
func (c *Connection) SendShared(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.send(data, sendOwned)
}

// sendRaw writes bytes to the socket below any layer.
func (c *Connection) sendRaw(data []byte) error {
	return c.send(data, sendCopy)
}

// sendRawPooled is sendRaw for a buffer from package bufferpool, which the
// connection takes over; see SendRawPooled.
func (c *Connection) sendRawPooled(data []byte) error {
	return c.send(data, sendPooled)
}

// sendMode is what send does with the bytes it does not write at once:
// copies them into its queue, keeps the caller's array, or keeps the caller's
// buffer from the pool and gives it back once it is written.
type sendMode uint8

const (
	sendCopy sendMode = iota
	sendOwned
	sendPooled
)

func (c *Connection) send(data []byte, mode sendMode) error {
	if c.udp != nil {
		err := c.sendDatagram(data)
		if mode == sendPooled {
			bufferpool.Put(data)
		}
		return err
	}
	if len(data) == 0 {
		if mode == sendPooled {
			bufferpool.Put(data)
		}
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend || c.writeShut {
		c.mu.Unlock()
		if mode == sendPooled {
			bufferpool.Put(data)
		}
		return syscall.EPIPE
	}
	if mode == sendCopy && c.corked {
		c.queueCorkedLocked(data, nil)
		over := c.sendQueueOverLocked()
		c.mu.Unlock()
		if over {
			return c.closeOverSendQueue()
		}
		return nil
	}
	sent := 0
	direct := c.canWriteDirectlyLocked()
	if direct {
		for {
			n, err := c.sysWrite(data)
			if err == syscall.EINTR {
				continue
			}
			if err != nil && !sys.IsWouldBlock(err) {
				c.mu.Unlock()
				if mode == sendPooled {
					bufferpool.Put(data)
				}
				c.closeWithError(err)
				return err
			}
			if err == nil {
				sent = n
			}
			break
		}
		if sent == len(data) {
			c.mu.Unlock()
			if mode == sendPooled {
				bufferpool.Put(data)
			}
			return nil
		}
	}
	queued := data[sent:]
	switch mode {
	case sendCopy:
		c.queueLocked(queued, nil)
	case sendOwned:
		c.queueOwnedLocked(queued)
	default:
		// Kept whole, with what the socket took past as the item's offset,
		// so that it goes back to the pool in the size class it came from.
		c.pushLocked(sendItem{data: data, offset: sent, pooled: true})
	}
	c.addPending(int64(len(queued)))
	c.traceLocked("send queued %d direct=%v mode=%d", len(queued), direct, mode)
	// Write interest stays armed, so queueing alone changes nothing; only a
	// watermark crossing does. While corked the flush at the end of the read
	// round settles the pause instead.
	var armErr error
	if direct {
		armErr = c.awaitWritableLocked()
	}
	over := c.sendQueueOverLocked()
	refresh := !c.corked && c.settlePauseLocked(false)
	c.mu.Unlock()
	if over {
		return c.closeOverSendQueue()
	}
	if armErr != nil {
		c.closeWithError(armErr)
		return armErr
	}
	if refresh {
		c.engine.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// SendParts writes a two-part message without first joining the parts. If the
// socket is backpressured, only the unsent suffix is copied before returning.
func (c *Connection) SendParts(first, second []byte) error {
	return c.sendParts(first, second, false)
}

// SendPartsShared writes a two-part message like SendParts, but keeps second,
// the part the caller shares, as the caller gave it: what the socket does not
// take waits in the queue as that slice rather than as a copy of it, as
// SendShared keeps its array. The connection never writes into second and
// never hands it to a pool, and only reads it until it is sent, so the same
// array can go to many connections at once, and the caller must not change it
// until then. first, the head of the message, is copied when the socket does
// not take it, so a head built for one response may go back to its pool once
// this returns. Where a layer such as TLS transforms the parts, both are
// copied, as SendParts copies them.
func (c *Connection) SendPartsShared(first, second []byte) error {
	return c.sendParts(first, second, true)
}

// sendParts is SendParts and SendPartsShared: shared says second is kept as
// the caller gave it rather than copied.
func (c *Connection) sendParts(first, second []byte, shared bool) error {
	if l := c.layer; l != nil {
		// Layer.Send is an interface call, so whatever it is handed escapes to
		// the heap. Only second would be harmless, being the caller's buffer
		// already; first is usually a frame header on the caller's stack,
		// which a copy keeps there for every connection without a layer.
		return l.Send(append([]byte(nil), first...), second)
	}
	if c.udp != nil {
		return c.sendDatagramParts(first, second)
	}
	total := len(first) + len(second)
	if total == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend || c.writeShut {
		c.mu.Unlock()
		return syscall.EPIPE
	}
	if c.corked {
		if shared {
			c.queueCorkedSharedLocked(first, second)
		} else {
			c.queueCorkedLocked(first, second)
		}
		over := c.sendQueueOverLocked()
		c.mu.Unlock()
		if over {
			return c.closeOverSendQueue()
		}
		return nil
	}
	sent := 0
	direct := c.canWriteDirectlyLocked()
	if direct {
		for {
			n, err := c.sysWrite2(first, second)
			if err == syscall.EINTR {
				continue
			}
			if err != nil && !sys.IsWouldBlock(err) {
				c.mu.Unlock()
				c.closeWithError(err)
				return err
			}
			if err == nil {
				sent = n
			}
			break
		}
		if sent == total {
			c.mu.Unlock()
			return nil
		}
	}
	if shared {
		if sent < len(first) {
			c.queueSharedPartsLocked(first[sent:], second)
		} else {
			c.queueSharedPartsLocked(nil, second[sent-len(first):])
		}
	} else if sent < len(first) {
		c.queueLocked(first[sent:], second)
	} else {
		c.queueLocked(second[sent-len(first):], nil)
	}
	c.addPending(int64(total - sent))
	var armErr error
	if direct {
		armErr = c.awaitWritableLocked()
	}
	over := c.sendQueueOverLocked()
	refresh := !c.corked && c.settlePauseLocked(false)
	c.mu.Unlock()
	if over {
		return c.closeOverSendQueue()
	}
	if armErr != nil {
		c.closeWithError(armErr)
		return armErr
	}
	if refresh {
		c.engine.request(command{kind: commandRefresh, connection: c})
	}
	return nil
}

// RunTask implements taskpool.Task without allocating a method value for each
// readiness notification.
func (c *Connection) RunTask() {
	defer c.engine.taskWG.Done()
	c.process()
}

func (c *Connection) process() {
	defer func() {
		if recovered := recover(); recovered != nil {
			// TaskPool isolates task panics. Roll back connection ownership before
			// propagating the panic to the pool, otherwise scheduled would remain
			// true and this connection could never be submitted again.
			c.mu.Lock()
			c.scheduled = false
			closing := c.closePending
			c.closePending = false
			c.mu.Unlock()
			if closing {
				// The connection was already closed, and this round was to
				// finish the close.
				c.finishClose()
			} else {
				c.closeWithError(fmt.Errorf("handler panic: %v", recovered))
			}
			panic(recovered)
		}
	}()
	// deferred carries readiness that this round observed but did not act on
	// because output was still queued. It is folded back into pendingEvents
	// before the next round so the notification is never lost: readiness is
	// edge-triggered, so a dropped read edge would only reappear once the peer
	// sent more data, leaving readable bytes stranded on a connection that
	// looks idle.
	var deferred uint32
	for {
		c.mu.Lock()
		c.pendingEvents |= uint16(deferred)
		if c.pendingEvents == uint16(deferred) && (deferred == 0 || c.sendHead != len(c.sends)) {
			// Only the deferred readiness remains. Stop here instead of spinning,
			// and leave the read to whoever empties the queue. That need not be
			// a write edge: a Flush from another goroutine that took the cork
			// off this round's output writes it all without the socket ever
			// filling, and then no write edge comes. So the read is recorded
			// here, under the lock the queue drains under, and the drain hands
			// it back. A queue that drained since the read was deferred leaves
			// nothing to wait for, and the read runs now.
			c.readDeferred = deferred != 0
			c.hupOwed = c.hupOwed || deferred&evRdHup != 0
			c.scheduled = false
			c.traceLocked("round stop deferred=%#x", deferred)
			closing := c.closePending
			c.closePending = false
			c.mu.Unlock()
			if closing {
				c.finishClose()
			}
			return
		}
		events := uint32(c.pendingEvents)
		c.pendingEvents = 0
		c.roundStalled = false
		if c.hupOwed && events&evIn != 0 {
			events |= evRdHup
			c.hupOwed = false
		}
		c.traceLocked("round events=%#x", events)
		closed := c.closed || c.closing
		if c.flags.has(flagReadShut) {
			// CloseRead ended input: what the socket still reports, the end
			// of input the shutdown itself raises among it, is not read.
			events &^= evIn | evPri | evRdHup
		}
		// Most rounds bring input to a connection with nothing queued, which
		// leaves nothing to flush first and no output to defer the read for.
		// Such a round begins its read under this lock, rather than taking
		// it again for each of those checks and once more to begin.
		readNow := !closed && c.udp == nil && events&(evIn|evPri) == evIn && c.sendHead == len(c.sends)
		if readNow {
			c.beginReadLocked()
		}
		c.mu.Unlock()
		deferred = 0
		alive := !closed
		if c.udp != nil {
			// The loop has already read the socket; the round only hands the
			// queued datagrams to the handler.
			if alive && events&evIn != 0 {
				c.drainDatagrams()
			}
			continue
		}
		var closeErr error
		if readNow {
			closeErr = c.readRound()
			alive = closeErr == nil
			if alive {
				c.rearmRead()
			}
		} else {
			// Flush before reading, so that a round which both frees socket send
			// space and delivers new input hands the older output to the socket
			// first.
			if alive && events&evOut != 0 && c.hasFlushableOutput() {
				closeErr = c.flushOutput()
				alive = closeErr == nil
			}
			if alive && events&evPri != 0 {
				closeErr = c.drainPriorityInput()
				alive = closeErr == nil
			}
			if alive && events&evIn != 0 {
				if c.hasQueuedOutput() && (c.engine.writeHighWatermark <= 0 && !c.flags.has(flagNoWriteBackpressure) ||
					c.readShouldStop()) {
					// The output queued already fills the connection's budget, or
					// the handler holds reads, or there is no watermark and the
					// queue is all that bounds this connection's buffering. Queued
					// output means write interest is armed or a write is in
					// flight, so a later round is guaranteed. Carry the read, and
					// any half-close that arrived with it, into that round so the
					// peer's final bytes are still delivered after the flush.
					//
					// Output queued below the watermark does not stop the read: the
					// watermark is what bounds a connection's buffering. Stopping at
					// the first queued byte left the peer's requests in the socket
					// while it waited for their replies, and a pipelining client
					// kept sending into a receive queue that filled with small
					// segments until the kernel dropped them: go-websocket-benchmark's
					// pipelined echo (50k connections, 8 server CPUs, the client on
					// their SMT siblings) retransmitted 310-330k segments a second
					// and served 3.06-3.10M messages a second, and reading on to the
					// watermark halved the retransmits and served 3.96-3.99M.
					deferred = evIn | events&evRdHup
				} else {
					closeErr = c.drainInput()
					alive = closeErr == nil
					if alive {
						c.rearmRead()
					}
				}
			}
		}
		if alive && events&evErr != 0 {
			closeErr = c.socketError()
			alive = false
		} else if alive && events&(evHup|evRdHup)&^deferred != 0 {
			if c.flags.has(flagReadHeld) || c.roundStalled {
				// The handler holds reads, or the round's read stopped short
				// for a hold or the write budget, so what the peer sent
				// before it ended may still be in the socket, behind the
				// end. The read handed back later takes it and then the
				// end, which closes the connection as it would have here:
				// the end is owed with it, since its edge is spent. The
				// round's own stop counts even when the hold that caused it
				// was lifted since, on another goroutine: closing here
				// would drop what is left.
				c.mu.Lock()
				c.hupOwed = true
				c.mu.Unlock()
				c.stallRead()
			} else {
				closeErr = io.EOF
				alive = false
			}
		}
		if !alive {
			c.closeWithError(closeErr)
		}
	}
}

// finishClose ends a closed connection's last round: OnClose runs here,
// after everything the connection delivered before it, and the loop is then
// asked to release the descriptor, which it does itself once it has stopped.
func (c *Connection) finishClose() {
	e := c.engine
	defer func() {
		if !e.request(command{kind: commandDetach, connection: c}) {
			e.detach(c)
		}
	}()
	c.mu.Lock()
	err := c.closeReasonLocked()
	c.mu.Unlock()
	c.handler.OnClose(c, err)
	c.mu.Lock()
	c.closeDue = false
	c.mu.Unlock()
}

// hasQueuedOutput reports whether bytes accepted by Send are still waiting for
// the socket. Reads are deferred while it is true.
//
// It also brings the backlog up to date for readShouldStop, which the caller
// consults next without the lock.
func (c *Connection) hasQueuedOutput() bool {
	c.mu.Lock()
	c.foldCorkedLocked()
	queued := c.sendHead != len(c.sends)
	c.mu.Unlock()
	return queued
}

// hasFlushableOutput reports whether a write edge has queued output to hand
// to the socket. Output held by Cork is not: it waits for Flush, however
// writable the socket is. A round's own cork never shows here, since rounds do
// not overlap and each uncorks before it ends, so a cork this sees is the
// application's. The first round of a connection is the edge that matters:
// the socket reports writable once it is registered, and that round may reach
// a worker only after the application corked the connection and sent.
func (c *Connection) hasFlushableOutput() bool {
	c.mu.Lock()
	flushable := c.sendHead != len(c.sends) && !c.corked
	c.mu.Unlock()
	return flushable
}

// drainInput reads until the socket is empty, handing each chunk to the
// handler. With Config.CoalesceWrites, which is the default, replies produced
// along the way are corked: rather than one write syscall per message they
// accumulate in one contiguous buffer and reach the socket in a single write
// when the round ends. Without it they are written as they are sent.
func (c *Connection) drainInput() error {
	c.mu.Lock()
	c.beginReadLocked()
	c.mu.Unlock()
	return c.readRound()
}

// beginReadLocked sets a read round up: it corks the connection if its
// rounds coalesce, and clears what the last round left owed. Callers hold
// c.mu.
func (c *Connection) beginReadLocked() {
	if c.coalesces() {
		c.corked = true
	}
	c.readStalled = false
	c.readDeferred = false
	c.roundStalled = false
}

// readRound is drainInput once beginReadLocked has set the round up.
func (c *Connection) readRound() error {
	err := c.readLoop()
	if flushErr := c.uncork(); err == nil {
		err = flushErr
	}
	return err
}

// uncork flushes whatever the handler queued while the round was corked. The
// cork is dropped first so a send from another goroutine racing the flush
// writes for itself instead of waiting for a round that has already ended.
// Uncorking an already-uncorked connection does nothing, so the caller that
// ends the round does not re-flush a queue a mid-round flush already left
// behind.
func (c *Connection) uncork() error {
	c.mu.Lock()
	wasCorked := c.corked
	c.corked = false
	if wasCorked {
		if h, ok := c.layer.(CorkHolder); ok {
			// The cork comes off before the layer is asked, so that a send
			// made meanwhile is the layer's to send at once, behind what it
			// held. A cork put on meanwhile stays on, and what it holds waits
			// for its own uncork.
			c.mu.Unlock()
			h.ReleaseCorked()
			c.mu.Lock()
		}
	}
	c.foldCorkedLocked()
	c.traceLocked("uncork wasCorked=%v", wasCorked)
	if !wasCorked || c.sendHead == len(c.sends) {
		c.mu.Unlock()
		return nil
	}
	// The flush goes on under the same lock.
	return c.flushLocked()
}

// Flush hands what has been sent so far to the socket now. Sends made from
// OnData are normally held until the handler returns, so that the replies to
// one read reach the socket in a single write; a handler that streams a
// response, and wants its first part on the wire while it produces the rest,
// calls Flush. Sends made outside OnData are written at once and need no
// Flush. What the socket cannot take yet stays queued, as for any send.
func (c *Connection) Flush() error {
	if c.udp != nil {
		return nil
	}
	return c.uncork()
}

// outputWait is what the goroutines waiting in waitOutput for a connection's
// queued output to drain share. It exists while any of them waits, and is
// guarded by the connection's mu, which cond waits with. waiters counts them,
// release is the largest backlog any of them waits for, and edges counts the
// write edges the event loop has seen for the connection since the first of
// them came, which tells one that flushed whether another edge has come since.
type outputWait struct {
	cond    sync.Cond
	waiters int
	release int64
	edges   uint64
}

// outputWaitLocked returns the connection's waiting writers, or nil when
// none waits. Callers hold c.mu.
func (c *Connection) outputWaitLocked() *outputWait {
	if d := c.deadlines; d != nil {
		return d.drain
	}
	return nil
}

// wakeOutputWaitLocked wakes the writers waiting in waitOutput, if any, to
// look at the connection again. Callers hold c.mu.
func (c *Connection) wakeOutputWaitLocked() {
	if w := c.outputWaitLocked(); w != nil {
		w.cond.Broadcast()
	}
	if c.closing || c.closed || c.engine.stopping.Load() {
		c.fireNoticesLocked(syscall.EPIPE)
	}
}

// fireNoticesLocked runs the callbacks waiting in notifyOutput whose backlog
// has been reached, or every one of them with err when it is not nil. They
// run through sidepool.Post, never here, under the lock, and without
// waiting for room, which nothing here may do. Callers hold c.mu.
func (c *Connection) fireNoticesLocked(err error) {
	d := c.deadlines
	if d == nil || len(d.notices) == 0 {
		return
	}
	pending := c.queuedLocked()
	kept := d.notices[:0]
	for _, n := range d.notices {
		if err == nil && pending > n.release {
			kept = append(kept, n)
			continue
		}
		fn := n.fn
		sidepool.Post(func() { fn(err) })
	}
	clear(d.notices[len(kept):])
	d.notices = kept
	if len(kept) == 0 && d.drain == nil {
		c.engine.trackOutputWait(c, false)
	}
}

// NotifyOutput is for a writer that may not wait for this connection's
// queued output to drain, such as one that acknowledges what it has sent here
// to a peer elsewhere: when more than limit bytes of what Send and SendFile
// accepted are still queued for the socket it arranges for fn to run, on
// another goroutine, once no more than half of limit are, or with
// syscall.EPIPE once the connection is closing, and reports true; when no
// more than limit are, it reports false and fn never runs. On a UDP
// connection it always reports false.
func (c *Connection) NotifyOutput(limit int64, fn func(error)) bool {
	return c.notifyOutput(limit, fn)
}

// notifyOutput is waitOutput for a caller that may not wait: when more than
// limit bytes of what Send accepted are queued for the socket it arranges for
// fn to run, through sidepool.Post, once no more than half of limit are, or
// with syscall.EPIPE once the connection is closing, and reports true; when
// no more than limit are, it reports false and fn never runs. Unlike a
// waiter, nothing here flushes: the connection's own rounds do, at each write
// edge, since no handler of theirs is held up by the caller.
func (c *Connection) notifyOutput(limit int64, fn func(error)) bool {
	if c.udp != nil {
		return false
	}
	c.mu.Lock()
	c.foldCorkedLocked()
	closing := c.closing || c.closed || c.engine.stopping.Load()
	if !closing && c.queuedLocked() <= limit {
		c.mu.Unlock()
		return false
	}
	d := c.deadlines
	if d == nil {
		d = &connectionDeadlines{}
		c.deadlines = d
	}
	d.notices = append(d.notices, drainNotice{release: limit / 2, fn: fn})
	c.engine.trackOutputWait(c, true)
	if closing {
		c.fireNoticesLocked(syscall.EPIPE)
	}
	c.mu.Unlock()
	return true
}

// noteWritableLocked tells the writers waiting in waitOutput, if any, that the
// socket has room again, or has failed, which their next flush finds out.
// Callers hold c.mu.
func (c *Connection) noteWritableLocked() {
	if w := c.outputWaitLocked(); w != nil {
		w.edges++
		w.cond.Broadcast()
	}
}

func init() {
	outputwait.Wait = func(c any, limit int64) error { return c.(*Connection).waitOutput(limit) }
	outputwait.Pending = func(c any) int64 { return c.(*Connection).pendingBytes.Load() }
	outputwait.Unsent = func(c any) int64 { return c.(*Connection).unsent() }
	outputwait.Notify = func(c any, limit int64, fn func(error)) bool {
		return c.(*Connection).notifyOutput(limit, fn)
	}
}

// unsent is how many bytes Send accepted are still queued for the socket,
// and what is left of the files SendFile queued. A flush writes and then
// uncounts what the socket took under c.mu, so read under it the count never
// includes bytes already on their way; corkedBytes are queued too, just not
// counted yet.
func (c *Connection) unsent() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queuedLocked()
}

// waitOutput waits while more than limit bytes of what Send accepted are
// still queued for the socket, until no more than half of limit are, so that
// a writer producing faster than its peer takes holds no more than about
// limit of it in memory. It reports syscall.EPIPE, as Send does, once the
// connection is closing, and the error a write failed with.
//
// The goroutine that waits flushes the queue itself, at each write edge the
// event loop sees, or error, rather than leaving that to the connection's
// round: it
// may be the handler running in that round, as an HTTP/1 handler streaming a
// response is, and the round only flushes on a write edge once its handler
// has returned. So the event loop wakes it for every write edge, and a flush
// by anyone that leaves no more than it waits for wakes it too. Output held
// by a cork is let go, as Flush lets it go, since the wait would otherwise
// be for output nothing is going to write.
func (c *Connection) waitOutput(limit int64) error {
	if c.pendingBytes.Load() <= limit && !c.flags.has(flagFileQueued) || c.udp != nil {
		return nil
	}
	release := limit / 2
	c.mu.Lock()
	d := c.deadlines
	if d == nil {
		d = &connectionDeadlines{}
		c.deadlines = d
	}
	w := d.drain
	if w == nil {
		w = &outputWait{}
		w.cond.L = &c.mu
		d.drain = w
		c.engine.trackOutputWait(c, true)
	}
	w.waiters++
	w.release = max(w.release, release)
	var err error
	for {
		if c.closing || c.closed || c.engine.stopping.Load() {
			// A stopped engine's loop sees no more write edges; see
			// Engine.releaseOutputWaiters.
			err = syscall.EPIPE
			break
		}
		c.foldCorkedLocked()
		if c.queuedLocked() <= release {
			break
		}
		edges := w.edges
		if c.corked || c.sendHead != len(c.sends) && !c.flushing && !c.writeBusyLocked() {
			if h, ok := c.layer.(CorkHolder); ok && c.corked {
				// What the layer holds is not queued yet: it is sent
				// first, and the queue looked at again.
				c.corked = false
				c.mu.Unlock()
				h.ReleaseCorked()
				c.mu.Lock()
				continue
			}
			c.corked = false
			// flushLocked releases mu. A write edge that comes before it is
			// taken again counts in edges, which says to flush once more
			// rather than wait for an edge that has already come.
			err = c.flushLocked()
			if err != nil && err != syscall.EPIPE {
				c.closeWithError(err)
			}
			c.mu.Lock()
			if err != nil {
				break
			}
			if w.edges != edges {
				continue
			}
			if c.closing || c.closed || c.queuedLocked() <= release {
				continue
			}
		}
		// Whatever writes next, this flush at the next edge or another
		// goroutine's that drains the queue, wakes the wait.
		w.cond.Wait()
	}
	if w.waiters--; w.waiters == 0 {
		d.drain = nil
		if len(d.notices) == 0 {
			c.engine.trackOutputWait(c, false)
		}
	}
	c.mu.Unlock()
	return err
}

// trackOutputWait records that a writer has begun waiting in waitOutput on c,
// with waiting set, or that the last one waiting has stopped, so that Stop
// can let them go. Callers hold c.mu, which Stop takes only after
// outputWaitMu is released.
func (e *Engine) trackOutputWait(c *Connection, waiting bool) {
	e.outputWaitMu.Lock()
	if waiting {
		if e.outputWaiters == nil {
			e.outputWaiters = make(map[*Connection]struct{})
		}
		e.outputWaiters[c] = struct{}{}
	} else {
		delete(e.outputWaiters, c)
	}
	e.outputWaitMu.Unlock()
}

// stallRead ends a read round with bytes possibly still in the socket and asks
// the event loop to settle what happens to them: it either pauses reads, so
// that resuming them later redelivers what is left, or, when whatever stopped
// the round is already over, hands the read straight back. Without the stall
// mark those bytes would wait for an edge that only the peer sending more can
// raise.
func (c *Connection) stallRead() {
	c.mu.Lock()
	c.readStalled = true
	refresh := c.settlePauseLocked(true)
	c.mu.Unlock()
	if refresh {
		c.engine.request(command{kind: commandRefresh, connection: c})
	}
}

// HoldReads stops or resumes reading from this connection's socket. It is the
// read-side counterpart of the write watermarks: a handler that buffers input
// it has not passed on yet — a streaming HTTP request body whose handler has
// not consumed it — holds reads while its buffer is full, so that the peer is
// slowed by TCP flow control rather than by memory growing here. Releasing the
// hold redelivers whatever the socket already had, without the peer having to
// send anything more.
//
// It may be called from any goroutine, including from inside OnData, where the
// current read round stops before its next read. Holds do not nest: the last
// call wins, and a hold left set on a closed connection is harmless. It does
// nothing on a UDP connection, whose datagrams the event loop has already read.
func (c *Connection) HoldReads(hold bool) {
	if c.udp != nil || c.flags.set(flagReadHeld, hold) == hold {
		return
	}
	c.engine.request(command{kind: commandRefresh, connection: c})
}

// ReadsHeld reports whether HoldReads is currently holding reads back.
func (c *Connection) ReadsHeld() bool { return c.flags.has(flagReadHeld) }

// SetWriteBackpressure says whether the connection's own backlog paces its
// reads, which it does by default: while more than
// Config.WriteBufferHighWatermark of what it was sent waits for its socket,
// it stops reading, so that a peer that sends faster than it reads the
// replies is held back by TCP rather than by memory here.
//
// A relay, which passes what one connection reads on to another, turns it
// off on both and paces each by the other's backlog instead, with HoldReads.
// Left on, a connection whose writes to its peer back up would stop reading
// that peer's answers, and a peer that answers as it reads, waiting to send
// before it reads more, would then wait for it forever. Holds and the
// server-wide MaxPendingBytes still pause it. It may be called from any
// goroutine, and takes effect from the next read.
func (c *Connection) SetWriteBackpressure(on bool) {
	if c.flags.set(flagNoWriteBackpressure, !on) == !on {
		return
	}
	c.engine.request(command{kind: commandRefresh, connection: c})
}

// SetWriteQueueLimit caps the bytes this connection may have queued for its
// socket, as Config.MaxWriteQueueBytes does for every connection: a send that
// takes the queue past the limit closes the connection with
// ErrSendQueueFull and reports it, rather than letting a peer that never
// reads grow the queue without bound. Bytes a corked round has queued count,
// and so do the sealed bytes a layer such as TLS leaves behind; a file
// SendFile has queued but not read does not. A limit of zero or less removes
// the cap, and a negative one restores the engine's, which is
// Config.MaxWriteQueueBytes. It may be called from any goroutine, and takes
// effect from the next send.
func (c *Connection) SetWriteQueueLimit(limit int64) {
	c.mu.Lock()
	c.writeLimitSet, c.writeLimit = limit >= 0, max(limit, 0)
	c.mu.Unlock()
}

// WriteQueueLimit reports the limit SetWriteQueueLimit set, or
// Config.MaxWriteQueueBytes when none was, and zero when there is none.
func (c *Connection) WriteQueueLimit() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeQueueLimitLocked()
}

// writeQueueLimitLocked is WriteQueueLimit, for callers that hold c.mu.
func (c *Connection) writeQueueLimitLocked() int64 {
	if c.writeLimitSet {
		return c.writeLimit
	}
	if c.engine == nil {
		return 0
	}
	return c.engine.writeQueueLimit
}

// sendQueueOverLocked reports whether what is queued for the socket is past
// the connection's limit. It counts the bytes waiting from memory, whether
// queued outright or under a cork, and not a file SendFile has queued but not
// read, which the peer's reading of the socket paces anyway. Callers hold
// c.mu.
func (c *Connection) sendQueueOverLocked() bool {
	limit := c.writeQueueLimitLocked()
	if limit <= 0 {
		return false
	}
	return c.pendingBytes.Load()+c.corkedBytes > limit
}

// closeOverSendQueue closes a connection whose send queue a send just took
// past its limit, and reports the error that closes it. It is called after
// c.mu is released.
func (c *Connection) closeOverSendQueue() error {
	c.closeWithError(ErrSendQueueFull)
	return ErrSendQueueFull
}

// readShouldStop reports whether this round should stop reading: either queued
// output has reached the budget that bounds how much this connection, or the
// server as a whole, buffers in userspace, or the application is holding reads.
func (c *Connection) readShouldStop() bool {
	pause, _ := c.pauseDecision(false)
	return pause
}

func (c *Connection) readLoop() error {
	buf := bufferpool.Get(c.engine.readBufferSize)
	defer bufferpool.Put(buf)
	// read counts what the round has read since it last settled; see
	// roundFlushBytes.
	read := 0
	for {
		if c.flags.has(flagReadShut) {
			// CloseRead, perhaps from inside OnData, ended input.
			return nil
		}
		if c.flags.has(flagReadHeld) {
			// The application is holding reads until it has worked through
			// what it already has. Leave the rest in the socket, where TCP
			// flow control slows the peer down instead of this side growing:
			// releasing the hold refreshes the connection, which hands back
			// the read owed for whatever was left behind.
			c.roundStalled = true
			c.stallRead()
			return nil
		}
		n, err := c.sysRead(buf)
		c.traceStep("read n=%d err=%v", n, err)
		if n > 0 {
			if !c.flags.has(flagLingering) {
				c.handler.OnData(c, buf[:n])
			} else if c.lingerOver() {
				// The round would otherwise drop input for as long as a
				// peer that goes on sending keeps the socket full, and the
				// close waits for the round to end.
				return nil
			}
			if n < len(buf) {
				// A short read means the socket buffer is empty, so the next
				// read would only return EAGAIN. Data arriving after this
				// point raises a fresh edge, which resubmits the connection.
				//
				// The round ends here, with nothing left in the socket to stop
				// for, and its uncork settles the replies: it counts them,
				// flushes them, counts against the budget what the socket did
				// not take, and asks the loop to pause reads if that crosses a
				// watermark. A handler that holds reads asked the loop itself.
				return nil
			}
			if read += n; read >= roundFlushBytes {
				read = 0
				if stop, err := c.settleLongRound(); stop || err != nil {
					return err
				}
			}
			c.chargeRound()
			if c.readShouldStop() {
				// Either the replies queued so far already fill the write
				// budget or the handler is holding reads. Hand what is queued
				// to the socket before stopping rather than stopping outright:
				// stopping would leave readable bytes behind an edge that does
				// not fire again until the peer sends more, and it is the
				// flush, not the queue depth, that says whether the peer is
				// actually keeping up.
				if flushErr := c.uncork(); flushErr != nil {
					return flushErr
				}
				if c.readShouldStop() {
					// The peer is behind, or the handler still has more than
					// it can take. Stop reading and let the event loop settle
					// the bytes left behind.
					c.roundStalled = true
					c.stallRead()
					return nil
				}
				if c.coalesces() {
					c.mu.Lock()
					c.corked = true
					c.mu.Unlock()
				}
			}
			continue
		}
		if n == 0 && err == nil {
			return io.EOF
		}
		if err == syscall.EINTR {
			continue
		}
		if sys.IsWouldBlock(err) {
			return nil
		}
		return err
	}
}

func (c *Connection) drainPriorityInput() error {
	buf := []byte{0}
	for {
		n, err := c.sysRecvOOB(buf)
		if n > 0 {
			c.handler.OnPriorityData(c, buf[:n])
			continue
		}
		if n == 0 && err == nil {
			return io.EOF
		}
		if err == syscall.EINTR {
			continue
		}
		// EINVAL means no urgent data is waiting, and EOPNOTSUPP a socket
		// that has none at all, such as a Unix one.
		if sys.IsWouldBlock(err) || err == syscall.EINVAL || err == syscall.EOPNOTSUPP {
			return nil
		}
		return err
	}
}

func (c *Connection) flushOutput() error {
	c.mu.Lock()
	return c.flushLocked()
}

// flushLocked is flushOutput for a caller that already holds c.mu, which it
// releases. The lock is held across each write, and a write that takes the
// whole queue is settled under the same lock, so the usual flush takes it
// once.
func (c *Connection) flushLocked() error {
	if c.closing || c.closed || c.flushing || c.writeBusyLocked() {
		c.traceLocked("flush busy")
		usable := !c.closing && !c.closed
		if usable {
			// What is queued waits on another flush or a write in flight,
			// which is backlog the budget counts.
			c.chargeLocked()
		}
		c.mu.Unlock()
		if usable {
			return nil
		}
		return syscall.EPIPE
	}
	c.flushing = true
	for {
		if c.sendHead == len(c.sends) {
			c.resetQueueLocked()
			c.flushing = false
			c.finishCloseWriteLocked()
			closeAfterSend := c.closeAfterSend
			owed := c.readDeferred
			if owed {
				// A round left its read to this drain; see process. It is
				// handed back as a stalled one is.
				c.readDeferred = false
				c.readStalled = true
			}
			refresh := c.settlePauseLocked(owed)
			c.traceLocked("flush drained owed=%v", owed)
			if closeAfterSend && c.lingerLocked() {
				closeAfterSend = false
			}
			c.mu.Unlock()
			if closeAfterSend {
				c.closeWithError(nil)
				return nil
			}
			if refresh {
				c.engine.request(command{kind: commandRefresh, connection: c})
			}
			return nil
		}
		var n int
		var err error
		attempted := 0
		// fromFile marks bytes sendfile took straight from a file, which were
		// never counted as pending since they occupied no memory.
		fromFile := false
		pending := len(c.sends) - c.sendHead
		if head := &c.sends[c.sendHead]; head.file != nil {
			n, attempted, fromFile, err = c.sysSendFileLocked(head)
		} else if head.record != 0 {
			data := head.data[head.offset:]
			attempted = len(data)
			n, err = c.sysSendRecord(head.record, data)
		} else if c.engine.useWritev && pending > 1 {
			count := pending
			if count > maxWritevItems {
				count = maxWritevItems
			}
			var batch [maxWritevItems][]byte
			buffers := batch[:count]
			for i := 0; i < count; i++ {
				item := &c.sends[c.sendHead+i]
				if item.file != nil || item.record != 0 {
					// A file, or a record of the kernel's, is sent on its
					// own once everything before it has gone.
					buffers = buffers[:i]
					break
				}
				buffers[i] = item.data[item.offset:]
				attempted += len(buffers[i])
			}
			n, err = c.sysWritev(buffers)
		} else {
			item := c.sends[c.sendHead]
			attempted = len(item.data) - item.offset
			n, err = c.sysWrite(item.data[item.offset:])
		}
		if n > 0 {
			if fromFile {
				c.fileSentLocked(int64(n))
			} else {
				c.subPending(int64(n))
			}
			c.consumeLocked(n)
			if n < attempted {
				// A short write means the socket send buffer is full, so
				// retrying now would only earn an EAGAIN. Wait for the socket
				// to become writable again.
				c.flushing = false
				c.chargeLocked()
				armErr := c.awaitWritableLocked()
				refresh := c.settlePauseLocked(false)
				c.traceLocked("flush short %d/%d armErr=%v", n, attempted, armErr)
				c.mu.Unlock()
				if armErr != nil {
					c.closeWithError(armErr)
					return armErr
				}
				if refresh {
					c.engine.request(command{kind: commandRefresh, connection: c})
				}
				return nil
			}
			if c.sendHead != len(c.sends) {
				// More is queued than one write takes. Let a send, or the
				// event loop, have the lock between writes.
				c.mu.Unlock()
				c.mu.Lock()
			}
			continue
		}
		if err == syscall.EINTR {
			continue
		}
		c.flushing = false
		if sys.IsWouldBlock(err) {
			c.chargeLocked()
			armErr := c.awaitWritableLocked()
			refresh := c.settlePauseLocked(false)
			c.traceLocked("flush wouldblock armErr=%v", armErr)
			c.mu.Unlock()
			if armErr != nil {
				c.closeWithError(armErr)
				return armErr
			}
			if refresh {
				c.engine.request(command{kind: commandRefresh, connection: c})
			}
			return nil
		}
		c.traceLocked("flush ret n=%d err=%v", n, err)
		c.mu.Unlock()
		// The socket has failed, so the connection ends here, as it does
		// when a send finds it so: a caller that does not look at the error,
		// such as a Flush from another goroutine, would otherwise leave it
		// open with its output queued and nothing posted to report the
		// failure, which on Windows, with the read the round deferred to
		// this drain, is nothing at all.
		if err != nil {
			c.closeWithError(err)
		}
		return err
	}
}

// The methods below are net.TCPConn's, beyond those net.Conn already asks
// for. Each one that has no meaning for the connection's kind does nothing
// and reports no error: TCP's own options on a Unix socket or a UDP
// connection, and anything touching the socket on a UDP listener's peer,
// which shares its listener's socket with every other peer.

// SetNoDelay turns Nagle's algorithm off, when noDelay is true, or back on.
// Every TCP connection fib accepts or dials starts with it off, so that a
// small reply is not held back until the peer acknowledges the one before.
func (c *Connection) SetNoDelay(noDelay bool) error {
	return c.tcpControl(func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, boolInt(noDelay))
	})
}

// SetKeepAlive turns keep-alive probes on or off.
func (c *Connection) SetKeepAlive(keepalive bool) error {
	return c.tcpControl(func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, boolInt(keepalive))
	})
}

// SetKeepAlivePeriod sets how long the connection stays idle before the
// first keep-alive probe, rounded up to whole seconds, as
// net.TCPConn.SetKeepAlivePeriod does: zero means the net package's default
// of 15 seconds and a negative period leaves the setting alone.
func (c *Connection) SetKeepAlivePeriod(d time.Duration) error {
	return c.tcpControl(func(s rawSocket) error { return setKeepAliveTimes(s, d, -1) })
}

// SetKeepAliveConfig applies config as net.TCPConn.SetKeepAliveConfig does:
// Idle, Interval and Count take the net package's defaults when zero and are
// left alone when negative. A platform that cannot set Count leaves it alone.
func (c *Connection) SetKeepAliveConfig(config net.KeepAliveConfig) error {
	return c.tcpControl(func(s rawSocket) error {
		err := syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, boolInt(config.Enable))
		if err == nil {
			err = setKeepAliveTimes(s, config.Idle, config.Interval)
		}
		if err == nil {
			err = setKeepAliveCount(s, config.Count)
		}
		return err
	})
}

// SetLinger sets what closing does with output the kernel has yet to deliver,
// as net.TCPConn.SetLinger does: a negative sec sends it in the background,
// zero discards it and resets the connection, and a positive sec lets the
// close wait up to that many seconds for it.
func (c *Connection) SetLinger(sec int) error {
	return c.tcpControl(func(s rawSocket) error {
		l := syscall.Linger{Onoff: 1, Linger: int32(sec)}
		if sec < 0 {
			l = syscall.Linger{}
		}
		return syscall.SetsockoptLinger(s, syscall.SOL_SOCKET, syscall.SO_LINGER, &l)
	})
}

// SetReadBuffer sets the size of the socket's receive buffer. It applies to
// any socket of the connection's own, a Unix or UDP one included.
func (c *Connection) SetReadBuffer(bytes int) error {
	return c.control("setsockopt", func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bytes)
	})
}

// SetWriteBuffer sets the size of the socket's send buffer. It applies to any
// socket of the connection's own, a Unix or UDP one included.
func (c *Connection) SetWriteBuffer(bytes int) error {
	return c.control("setsockopt", func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_SNDBUF, bytes)
	})
}

// CloseRead shuts down the reading side of the connection. OnData is not
// called again, and the end of input that follows does not close the
// connection, which goes on sending until it is closed. Only a stream,
// TCP or Unix, has a side to shut; on a UDP connection it does nothing.
func (c *Connection) CloseRead() error {
	// A repeat does nothing, even once the connection has closed: shutting
	// both sides raises a hangup, which closes it.
	if c.udp != nil || c.flags.has(flagReadShut) {
		return nil
	}
	return c.control("shutdown", func(s rawSocket) error {
		if c.flags.set(flagReadShut, true) {
			return nil
		}
		return c.shutdownReadLocked()
	})
}

// CloseWrite shuts down the writing side of the connection once everything
// Send has already accepted has reached the socket, so the peer reads all of
// it and then the end of the stream. Sends are refused from the call on. It
// works below any layer, whose own closing message, such as TLS's
// close_notify, it does not send. On a UDP connection it does nothing.
func (c *Connection) CloseWrite() error {
	if c.udp != nil {
		return nil
	}
	// A repeat does nothing, as CloseRead's does.
	c.mu.Lock()
	shut := c.writeShut
	c.mu.Unlock()
	if shut {
		return nil
	}
	return c.control("shutdown", func(s rawSocket) error {
		if c.writeShut {
			return nil
		}
		c.writeShut = true
		if c.sendHead != len(c.sends) || c.flushing || c.writeBusyLocked() {
			// Whatever drains the queue shuts the side; see
			// finishCloseWriteLocked.
			c.shutWritePending = true
			return nil
		}
		return syscall.Shutdown(s, syscall.SHUT_WR)
	})
}

// finishCloseWriteLocked carries out a CloseWrite that was waiting for the
// queue, which its caller has just drained. Callers hold c.mu.
func (c *Connection) finishCloseWriteLocked() {
	if !c.shutWritePending {
		return
	}
	c.shutWritePending = false
	if !c.closing && !c.closed {
		_ = syscall.Shutdown(c.rawSocket(), syscall.SHUT_WR)
	}
}

// MultipathTCP reports whether the connection is using Multipath TCP. It is
// false for anything but TCP, and wherever the platform has no Multipath TCP.
func (c *Connection) MultipathTCP() (bool, error) {
	if !c.IsTCP() {
		return false, nil
	}
	var using bool
	err := c.control("getsockopt", func(s rawSocket) error {
		using = usingMultipathTCP(s)
		return nil
	})
	return using, err
}

// SyscallConn returns the connection's socket for raw access. Its Control
// holds the connection's lock while f runs, so that the event loop cannot
// close the socket, and hand its descriptor to another connection, from under
// f; f must not call the connection's methods, or it deadlocks. Its Read and
// Write call f once and report ErrWouldBlock when f reports that it is not
// done, since waiting for readiness is the event loop's job; a Read races the
// loop's own reads, as Connection.Read does. A UDP listener's peer has no
// socket of its own and returns an error.
func (c *Connection) SyscallConn() (syscall.RawConn, error) {
	if c.udp != nil && c.udp.listener != nil {
		return nil, errNoSocket
	}
	return rawConn{c}, nil
}

type rawConn struct{ c *Connection }

func (r rawConn) Control(f func(fd uintptr)) error {
	return r.c.control("raw-control", func(s rawSocket) error {
		r.c.flags.set(flagRawExposed, true)
		f(uintptr(s))
		return nil
	})
}

func (r rawConn) Read(f func(fd uintptr) bool) error {
	return r.c.control("raw-read", func(s rawSocket) error {
		r.c.flags.set(flagRawExposed, true)
		if !f(uintptr(s)) {
			return ErrWouldBlock
		}
		return nil
	})
}

func (r rawConn) Write(f func(fd uintptr) bool) error {
	return r.c.control("raw-write", func(s rawSocket) error {
		r.c.flags.set(flagRawExposed, true)
		if !f(uintptr(s)) {
			return ErrWouldBlock
		}
		return nil
	})
}

// control runs f on the connection's own socket. It holds c.mu while f runs:
// the event loop marks a connection closed under c.mu before it releases the
// socket, so the descriptor f is given cannot be closed, and taken by another
// connection, until f returns. A UDP listener's peer has no socket of its
// own, and f does not run for it. A syscall.Errno from f is wrapped as the
// net package wraps it, with op naming the call.
func (c *Connection) control(op string, f func(s rawSocket) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed {
		return net.ErrClosed
	}
	if c.udp != nil && c.udp.listener != nil {
		return nil
	}
	err := f(c.rawSocket())
	if errno, ok := err.(syscall.Errno); ok {
		return os.NewSyscallError(op, errno)
	}
	return err
}

// tcpControl is control for a setsockopt only TCP has.
func (c *Connection) tcpControl(f func(s rawSocket) error) error {
	if !c.IsTCP() {
		return nil
	}
	return c.control("setsockopt", f)
}

// keepAliveSeconds turns a keep-alive time into the whole seconds the kernel
// takes, rounding up, with zero standing for the net package's default.
func keepAliveSeconds(d time.Duration) int {
	if d == 0 {
		d = defaultKeepAlive
	}
	return int((d + time.Second - 1) / time.Second)
}

// defaultKeepAlive and defaultKeepAliveCount are the net package's defaults
// for a keep-alive time and probe count left at zero.
const (
	defaultKeepAlive      = 15 * time.Second
	defaultKeepAliveCount = 9
)

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// connectionDeadlines are a connection's read and write deadlines.
type connectionDeadlines struct {
	read, write deadline
	// closeReason is why the connection closed, when it is none of the
	// reasons closeKind tells apart.
	closeReason error
	// drain is what writers waiting in waitOutput share, while any waits.
	drain *outputWait
	// notices are the callbacks waiting in notifyOutput.
	notices []drainNotice
	// fileBytes is what is left to send of the files SendFile queued, which
	// pendingBytes leaves out: sendfile sends them from the file, so they
	// hold no memory, and neither the read-pausing watermark nor the
	// server-wide budget counts them. What paces a writer by the backlog,
	// waitOutput and notifyOutput, and the write deadline, count them with
	// it; see queuedLocked.
	fileBytes int64
}

// drainNotice is a callback waiting for the connection's queued output to
// drain to release; see notifyOutput.
type drainNotice struct {
	release int64
	fn      func(error)
}

// The values of closeCode: none, the end of input, a deadline, a reason kept
// in deadlines, and from closeErrno on, an errno that much above it.
const (
	closeNone uint8 = iota
	closeEOF
	closeDeadline
	closeOther
	closeErrno
)

// setCloseReasonLocked records why the connection closed. The usual reasons
// take no allocation; another goes in deadlines, which it allocates if no
// deadline did. Callers hold c.mu.
func (c *Connection) setCloseReasonLocked(err error) {
	switch err {
	case nil:
		c.closeCode = closeNone
		return
	case io.EOF:
		c.closeCode = closeEOF
		return
	case os.ErrDeadlineExceeded:
		c.closeCode = closeDeadline
		return
	}
	if errno, ok := err.(syscall.Errno); ok && errno <= syscall.Errno(255-closeErrno) {
		c.closeCode = closeErrno + uint8(errno)
		return
	}
	if c.deadlines == nil {
		c.deadlines = &connectionDeadlines{}
	}
	c.closeCode = closeOther
	c.deadlines.closeReason = err
}

// closeReasonLocked is the error setCloseReasonLocked recorded. Callers hold
// c.mu.
func (c *Connection) closeReasonLocked() error {
	switch code := c.closeCode; {
	case code == closeEOF:
		return io.EOF
	case code == closeDeadline:
		return os.ErrDeadlineExceeded
	case code == closeOther:
		return c.deadlines.closeReason
	case code >= closeErrno:
		return syscall.Errno(code - closeErrno)
	}
	return nil
}

// deadline is one of a connection's two deadlines. Its timer is made the
// first time the deadline is set and reset each time it moves after that.
// A timer that fires while the deadline is moving finds at later than now,
// or zero, so a connection given more time is not closed by the deadline it
// has outlived.
type deadline struct {
	timer *time.Timer
	at    time.Time
}

// SetDeadline sets both the read and the write deadline. A deadline here does
// not make a call fail, as it does on a blocking socket, since neither Read
// nor Write ever waits: it closes the connection when it passes, and OnClose
// is given os.ErrDeadlineExceeded. It is therefore how a server bounds a peer
// that has gone quiet or stopped reading, and a handler that wants a rolling
// timeout sets the next one each time it hears from the peer.
//
// The read deadline closes the connection when it passes, whatever the peer
// has sent meanwhile; the write deadline closes it only if output accepted by
// Send has still not reached the kernel. A zero time removes a deadline, and
// one already in the past closes the connection at once. It may be called
// from any goroutine.
func (c *Connection) SetDeadline(t time.Time) error {
	c.mu.Lock()
	closed := c.closing || c.closed
	if !closed {
		if d := c.deadlinesLocked(t); d != nil {
			c.armDeadlineLocked(&d.read, t, false)
			c.armDeadlineLocked(&d.write, t, true)
		}
	}
	c.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	return nil
}

// SetReadDeadline closes the connection at t, whether or not the peer has sent
// anything. See SetDeadline.
func (c *Connection) SetReadDeadline(t time.Time) error {
	return c.setDeadline(t, false)
}

// SetWriteDeadline closes the connection at t if output accepted by Send has
// still not reached the kernel by then; output that has all gone out leaves
// the connection alone. See SetDeadline.
func (c *Connection) SetWriteDeadline(t time.Time) error {
	return c.setDeadline(t, true)
}

func (c *Connection) setDeadline(t time.Time, write bool) error {
	c.mu.Lock()
	closed := c.closing || c.closed
	if !closed {
		if d := c.deadlinesLocked(t); d != nil {
			if write {
				c.armDeadlineLocked(&d.write, t, true)
			} else {
				c.armDeadlineLocked(&d.read, t, false)
			}
		}
	}
	c.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	return nil
}

// deadlinesLocked returns the connection's deadlines, allocating them when t
// is the first to be set, or nil when none has been and t, a zero time, has
// none to remove. Callers hold c.mu.
func (c *Connection) deadlinesLocked(t time.Time) *connectionDeadlines {
	if c.deadlines == nil && !t.IsZero() {
		c.deadlines = &connectionDeadlines{}
	}
	return c.deadlines
}

// armDeadlineLocked points d at t. A deadline set to the time it already holds
// keeps its timer as it is, so a handler that re-states the same deadline on
// every read round does not move a timer per round, and one that moves it
// resets the timer it has rather than make another.
func (c *Connection) armDeadlineLocked(d *deadline, t time.Time, write bool) {
	if traceOn.Load() {
		in := "zero"
		if !t.IsZero() {
			in = time.Until(t).Round(time.Millisecond).String()
		}
		c.traceLocked("deadline write=%v in=%s from %s", write, in, traceCallers())
	}
	if d.at.Equal(t) {
		return
	}
	d.at = t
	switch {
	case t.IsZero():
		if d.timer != nil {
			d.timer.Stop()
		}
	case d.timer == nil:
		d.timer = time.AfterFunc(max(time.Until(t), 0), func() { c.expireDeadline(d, write) })
	default:
		d.timer.Reset(max(time.Until(t), 0))
	}
}

// expireDeadline closes a connection whose deadline has passed, unless the
// deadline moved while the timer was on its way or the connection is already
// going. A deadline still ahead, which a timer reaches early when the wall
// clock steps back under a t without a monotonic reading, has its timer set
// again for the rest.
func (c *Connection) expireDeadline(d *deadline, write bool) {
	c.mu.Lock()
	stale := d.at.IsZero() || c.closing || c.closed
	if wait := time.Until(d.at); !stale && wait > 0 {
		d.timer.Reset(wait)
		stale = true
	}
	c.foldCorkedLocked()
	queued := c.queuedLocked()
	c.traceLocked("deadline expired write=%v stale=%v", write, stale)
	c.mu.Unlock()
	if stale {
		return
	}
	if write && queued == 0 {
		// Everything Send and SendFile accepted has reached the kernel, so
		// there is no write left for the deadline to be about.
		return
	}
	if !write && c.flags.has(flagLingering) {
		// A lingering close that ran out of time is the close it put off.
		c.closeWithError(nil)
		return
	}
	c.closeWithError(os.ErrDeadlineExceeded)
}

// stopDeadlinesLocked stops both timers, so a closed connection is not kept
// reachable by a deadline it will never reach.
func (c *Connection) stopDeadlinesLocked() {
	if c.deadlines == nil {
		return
	}
	for _, d := range [...]*deadline{&c.deadlines.read, &c.deadlines.write} {
		d.at = time.Time{}
		if d.timer != nil {
			d.timer.Stop()
		}
	}
}

// SendFile sends count bytes of f, starting at offset, after whatever was
// sent before it and ahead of whatever is sent after. On Linux, macOS,
// FreeBSD and DragonFly the bytes go from the file to the socket by
// sendfile(2), and on Windows by TransmitFile, without passing through user
// space; on NetBSD and OpenBSD they are read a chunk at a time as the socket
// takes them. Either way the file is read only as the peer keeps up, so a
// large file costs no more memory than a small one.
//
// What is left of the range to send counts in the connection's backlog as
// NotifyOutput, a writer waiting for output to drain and the write deadline
// see it, as bytes from Send count, so a peer that stops reading holds up
// those as it would for bytes; it takes no memory, though, so neither
// WriteBufferHighWatermark nor MaxPendingBytes, which bound memory, count it.
//
// The connection sends from its own duplicate of f's descriptor, so the
// caller may close f as soon as SendFile returns. The file must hold the
// whole range: a file that turns out shorter closes the connection, since the
// peer was promised count bytes.
//
// A connection with a layer, such as TLS, hands the file to the layer when it
// is a FileLayer that can carry it, as the tls package's is once its
// handshake is done: the file is then read, a chunk at a time, as the socket
// takes it and sealed on the way, or goes by sendfile with the kernel sealing
// it. Through any other layer the range is read and sent through the layer
// before SendFile returns.
func (c *Connection) SendFile(f File, offset, count int64) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp != nil {
		return ErrSendFileDatagram
	}
	if count == 0 {
		return nil
	}
	if l := c.layer; l != nil {
		if fl, ok := l.(FileLayer); ok && fl.CanSendFile() {
			return fl.SendFile(f, offset, count)
		}
		return sendFileCopy(c, f, offset, count, func(b []byte) error { return l.Send(b, nil) })
	}
	return c.sendFile(f, offset, count, nil)
}

// SendFileRaw is SendFile below any layer: count bytes of f from offset go to
// the socket as they are, by sendfile where the platform has it, or, with a
// sealer, a chunk at a time as the socket takes them, each read and passed
// through sealer on the way. A FileLayer hands its files on this way. What is
// left of the range counts in the backlog as SendFile's does: as file until
// a chunk is read, and then as the sealed bytes it became.
func (c *Connection) SendFileRaw(f File, offset, count int64, sealer FileSealer) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp != nil {
		return ErrSendFileDatagram
	}
	if count == 0 {
		return nil
	}
	return c.sendFile(f, offset, count, sealer)
}

// QueuesFiles reports whether SendFile queues a file, to be read as the
// socket takes it, rather than reading all of it before it returns, as it
// does through a layer that cannot carry a file. A caller that has a file of
// any size to send, and can pace the reading of it itself, asks first.
func (c *Connection) QueuesFiles() bool {
	if c.udp != nil {
		return false
	}
	l := c.layer
	if l == nil {
		return true
	}
	fl, ok := l.(FileLayer)
	return ok && fl.CanSendFile()
}

// sendFile queues count bytes of f from offset, with sealer if it has one,
// and writes what it can of them at once if nothing is ahead of them.
func (c *Connection) sendFile(f File, offset, count int64, sealer FileSealer) error {
	seg, err := newFileSegment(f, offset, count)
	if err != nil {
		return err
	}
	seg.sealer = sealer
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend || c.writeShut {
		c.mu.Unlock()
		seg.close()
		return syscall.EPIPE
	}
	direct := c.canWriteDirectlyLocked()
	c.pushLocked(sendItem{file: seg})
	c.addFileLocked(count)
	c.mu.Unlock()
	if !direct {
		// Whatever is queued ahead of the file is being flushed, or will be
		// when the round that corked the connection ends, and the file goes
		// out after it.
		return nil
	}
	if err := c.flushOutput(); err != nil {
		c.closeWithError(err)
		return err
	}
	return nil
}

// stageSealedLocked reads the next chunk of the sealed file item, unless one
// is already waiting in its data, and seals it into the segment's buffer,
// which becomes the item's data. The chunk moves from what is left of the
// file to what is in memory, counted in pendingBytes as the bytes it sealed
// into. Callers hold c.mu.
func (c *Connection) stageSealedLocked(item *sendItem) error {
	if item.data != nil {
		return nil
	}
	s := item.file
	want := int(min(s.remaining, sendFileChunk))
	plain := bufferpool.Get(want)
	defer bufferpool.Put(plain)
	read := 0
	for read < want {
		n, err := s.readAt(plain[read:want], s.offset+int64(read))
		read += n
		if n == 0 || err != nil && read < want {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	if s.sealed == nil {
		s.sealed = newSealBuffer(s.sealer.SealedLen(want))
	}
	sealed, err := s.sealer.Seal(s.sealed[:0], plain[:want])
	if err != nil {
		return err
	}
	s.sealed = sealed
	item.data, item.offset = sealed, 0
	s.advance(int64(want))
	c.moveFileLocked(int64(want))
	c.addPending(int64(len(sealed)))
	if c.sendQueueOverLocked() {
		return ErrSendQueueFull
	}
	return nil
}

// ControlIdle runs fn on the connection's socket, below any layer, if nothing
// sent is still queued or being written, and reports whether it ran it: so
// that fn changes how the socket sends, as switching it to the kernel's TLS
// does, between two writes and none in progress. Nothing is written to the
// socket until fn returns. It reports false on a connection that is closing.
func (c *Connection) ControlIdle(fn func(fd uintptr) error) (bool, error) {
	if c.udp != nil {
		return false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.flushing || c.writeBusyLocked() || c.sendHead != len(c.sends) ||
		c.queuedLocked() != 0 {
		return false, nil
	}
	return true, fn(uintptr(c.rawSocket()))
}

// SendRawRecord sends data below any layer as one TLS record of type typ
// that the kernel seals, after whatever was sent before it and ahead of
// whatever is sent after: for a layer that has handed the socket's sealing to
// the kernel's TLS, through ControlIdle, which then makes every other send a
// record of application data. It copies data, as SendRaw does. Where the
// kernel has no TLS it fails.
func (c *Connection) SendRawRecord(typ uint8, data []byte) error {
	if !kernelRecords || c.udp != nil || typ == 0 {
		return errNoKernelRecords
	}
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend || c.writeShut {
		c.mu.Unlock()
		return syscall.EPIPE
	}
	direct := c.canWriteDirectlyLocked()
	data = bufferpool.Append(nil, data)
	c.pushLocked(sendItem{data: data, record: typ})
	c.addPending(int64(len(data)))
	over := c.sendQueueOverLocked()
	c.mu.Unlock()
	if over {
		return c.closeOverSendQueue()
	}
	if !direct {
		return nil
	}
	if err := c.flushOutput(); err != nil {
		c.closeWithError(err)
		return err
	}
	return nil
}

// errNoKernelRecords is what SendRawRecord reports where it cannot send.
var errNoKernelRecords = errors.New("fib: no kernel TLS records here")

// Read takes what the socket has without waiting for the peer, and reports
// ErrWouldBlock when it has nothing. It is for a handler that wants to read
// the rest of a message itself, inside OnData, rather than wait for the next
// callback; incoming bytes otherwise reach the handler through OnData, and a
// Read racing the read loop takes bytes the loop would have delivered.
//
// It returns io.EOF once the peer has closed its side, and the reason the
// connection closed — os.ErrDeadlineExceeded for a deadline, net.ErrClosed
// otherwise — once it has. A UDP connection has no Read: its datagrams are
// read by the event loop and delivered whole to OnData.
func (c *Connection) Read(b []byte) (int, error) {
	if c.udp != nil {
		return 0, errUDPRead
	}
	if err := c.closedError(); err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	for {
		n, err := c.sysRead(b)
		switch {
		case n > 0:
			return n, nil
		case err == nil:
			return 0, io.EOF
		case err == syscall.EINTR:
			continue
		case sys.IsWouldBlock(err):
			return 0, ErrWouldBlock
		case err == syscall.EBADF:
			// The connection closed between the check above and the read.
			return 0, c.closedErrorOr(net.ErrClosed)
		}
		return 0, err
	}
}

// Write hands b to Send and reports it all written, since Send copies what it
// is given and queues whatever the socket cannot take. It therefore does not
// wait for the peer; SetWriteDeadline bounds how long the queue may stand.
func (c *Connection) Write(b []byte) (int, error) {
	if err := c.Send(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// LocalAddr returns the address this side of the connection is bound to: a
// *net.UDPAddr for a UDP connection, a *net.UnixAddr for a Unix socket and a
// *net.TCPAddr otherwise. It returns nil once the socket is gone.
func (c *Connection) LocalAddr() net.Addr {
	if c.udp != nil {
		if l := c.udp.listener; l != nil {
			sa, err := l.sockname()
			if err != nil {
				return nil
			}
			return sockaddrToUDPAddr(sa)
		}
	}
	sa, err := c.sockname()
	if err != nil {
		return nil
	}
	if c.udp != nil {
		return sockaddrToUDPAddr(sa)
	}
	return netaddr.ToAddr(sa)
}

// closedError is why the connection is no longer usable, or nil while it is.
func (c *Connection) closedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closing && !c.closed {
		return nil
	}
	if err := c.closeReasonLocked(); err != nil {
		return err
	}
	return net.ErrClosed
}

// closedErrorOr is closedError with a fallback for a connection that closed
// under a caller without having recorded why yet.
func (c *Connection) closedErrorOr(fallback error) error {
	if err := c.closedError(); err != nil {
		return err
	}
	return fallback
}
