//go:build linux || darwin || windows

package fib

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/sys"
)

// sendItem is one queued chunk. pooled says data came from the buffer pool,
// rather than being an array the caller handed over and the connection does
// not own. Returning a pooled array as soon as the item drains is what keeps a
// backpressured connection from allocating a fresh one per reply.
//
// file, when set, makes the item a stretch of a file, which SendFile queued.
// Where the file goes to the socket by sendfile, data stays nil; otherwise data
// is the chunk of it read so far and not yet sent.
type sendItem struct {
	data   []byte
	offset int
	pooled bool
	file   *fileSegment
}

type connectionAttachment struct{ value any }

// Connection is safe to use from callback and application goroutines.
type Connection struct {
	connPlatform
	engine *Engine
	// peer is the address the connection was accepted from, when accepting
	// it told; see RemoteAddrPort.
	peer netip.AddrPort
	// rawExposed records that the socket has been handed out, through File
	// or a RawConn, and so may have been duplicated: closing a socket then
	// leaves it in the event loop's interest set, from which it has to be
	// taken out first. It is set under mu, before the socket is handed out,
	// so a close, which marks the connection closed under mu first, sees it.
	rawExposed atomic.Bool
	// handler receives this connection's callbacks: the engine's handler for
	// an accepted connection, or the one DialWithHandler was given.
	handler       Handler
	mu            sync.Mutex
	pendingEvents uint32
	sends         []sendItem
	sendHead      int
	scheduled     bool
	closing       bool
	closed        bool
	readPaused    bool
	budgetPaused  bool
	// readHeld is an application-driven pause, set through HoldReads by a
	// handler that has more input buffered than it has consumed. It is read
	// without the mutex by the read loop, which checks it between reads.
	readHeld atomic.Bool
	// readStalled records that a read round stopped with bytes possibly
	// still in the socket, because the write backlog filled up first. Those
	// bytes raise no further edge on their own, so the event loop owes the
	// connection either a paused-then-resumed read or a direct redelivery.
	readStalled bool
	// readDeferred records that a round left a read undone because output
	// was still queued, and ended. Whatever empties the queue owes the
	// connection that read.
	readDeferred bool
	// closePending records that the loop has closed the connection and left
	// OnClose, and the release of the descriptor, to its last round; see
	// Engine.closeConnection. Guarded by mu.
	closePending   bool
	flushing       bool
	corked         bool
	closeAfterSend bool
	pendingBytes   atomic.Int64
	// charged is how much of pendingBytes counts against the server-wide
	// budget; see chargeLocked. Guarded by mu.
	charged    int64
	attachment atomic.Pointer[connectionAttachment]
	// dialing is set while an outbound connect is still in progress, and
	// cleared when it completes or fails. Event-loop ownership.
	dialing *dialRequest
	// dialed marks a connection Dial opened, as against one a listener
	// accepted; it never changes.
	dialed bool
	// readDeadline and writeDeadline close the connection when they pass.
	// Guarded by mu.
	readDeadline  deadline
	writeDeadline deadline
	// closeReason is why the connection closed, which Read and Write report
	// to a caller that comes back to it afterwards. Guarded by mu.
	closeReason error
	// layer, when set, carries every send, as TLS does to encrypt it.
	layer Layer
	// udp is set for a UDP connection, which exchanges datagrams.
	udp *udpState
	// unix marks a Unix socket, which has none of TCP's options.
	unix bool
	// readShut records CloseRead: the connection reads nothing more, and the
	// end of input that shutting the read side raises does not close it. It
	// is read without the mutex by the read loop, as readHeld is.
	readShut atomic.Bool
	// writeShut records CloseWrite, after which sends are refused, and
	// shutWritePending a CloseWrite still waiting for queued output to reach
	// the socket before it shuts the write side. Guarded by mu.
	writeShut        bool
	shutWritePending bool
}

// Cork holds what is sent on the connection from now on until Flush, rather
// than writing each send as it is made, as a read round does for the replies
// its OnData makes. A goroutine that answers several requests of one
// connection away from its round corks it first, so that the answers reach
// the socket in one write, and must Flush when it is done: nothing else
// writes what a Cork holds, unless a read round of the connection ends
// meanwhile and flushes it along with its own. It does nothing on a UDP
// connection.
func (c *Connection) Cork() {
	if c.udp != nil {
		return
	}
	c.mu.Lock()
	c.corked = true
	c.mu.Unlock()
}

// Attachment returns application state associated with the connection.
func (c *Connection) Attachment() any {
	if value := c.attachment.Load(); value != nil {
		return value.value
	}
	return nil
}

// SetAttachment associates application state with the connection. Passing nil
// clears it. Protocol handlers use this to avoid a global connection-state map.
func (c *Connection) SetAttachment(value any) {
	if value == nil {
		c.attachment.Store(nil)
		return
	}
	c.attachment.Store(&connectionAttachment{value: value})
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

// closeAfterSendRaw is CloseAfterSend below any layer.
func (c *Connection) closeAfterSendRaw() {
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend {
		c.mu.Unlock()
		return
	}
	c.closeAfterSend = true
	closeNow := c.sendHead == len(c.sends) && !c.flushing
	c.mu.Unlock()
	if closeNow {
		c.closeWithError(nil)
	}
}

func (c *Connection) closeWithError(err error) {
	c.mu.Lock()
	request := !c.closing && !c.closed
	c.closing = true
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

// rewindQueueLocked restarts an emptied item queue. Every item has already
// surrendered its buffer as it drained, so there is nothing left to return.
// Callers hold c.mu.
func (c *Connection) rewindQueueLocked() {
	if cap(c.sends) > maxWritevItems*2 {
		c.sends = nil
	} else {
		c.sends = c.sends[:0]
	}
	c.sendHead = 0
}

// resetQueueLocked restarts an emptied queue. Callers hold c.mu.
func (c *Connection) resetQueueLocked() {
	c.rewindQueueLocked()
}

// releaseItemLocked returns a drained item's buffer to the pool. A file item
// closes its descriptor. Callers hold c.mu.
func (c *Connection) releaseItemLocked(item *sendItem) {
	if item.pooled {
		c.engine.releaseSendBuffer(item.data)
	}
	if item.file != nil {
		item.file.close()
	}
	*item = sendItem{}
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
	if c.sendHead == len(c.sends) {
		// Nothing is queued any more, so the item slice can start over.
		c.rewindQueueLocked()
	}
	// A chunk larger than the pooled size grows through the pool as well, so
	// that even an outsized message comes from a class rather than from a
	// fresh allocation append would have had to make.
	data := bufferpool.Join(c.engine.acquireSendBuffer(), first, second)
	c.sends = append(c.sends, sendItem{data: data, pooled: true})
}

// queueOwnedLocked queues data the caller handed over. The connection does not
// own the array, so the item carries no pooled buffer and later chunks cannot
// merge into it. Callers hold c.mu.
func (c *Connection) queueOwnedLocked(data []byte) {
	if c.sendHead == len(c.sends) {
		c.rewindQueueLocked()
	}
	c.sends = append(c.sends, sendItem{data: data})
}

// pauseStateChangedLocked reports whether queued output has crossed a watermark
// so that the epoll read interest no longer matches it. It mirrors the decision
// refreshConnection makes, so that a refresh is only requested when the event
// loop actually has an epoll_ctl to perform. Callers hold c.mu.
func (c *Connection) pauseStateChangedLocked() bool {
	pause, _ := c.pauseDecision(c.readPaused)
	return pause != c.readPaused
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
	if c.readHeld.Load() {
		// The application asked for the socket to be left alone; only it can
		// say when reads resume.
		return true, false
	}
	e := c.engine
	if e.maxPendingBytes > 0 && e.pendingTotal.Load() >= e.maxPendingBytes {
		return true, true
	}
	if e.writeHighWatermark <= 0 {
		return false, false
	}
	pendingBytes := c.pendingBytes.Load()
	if pendingBytes >= int64(e.writeHighWatermark) {
		return true, false
	}
	// Hysteresis: once paused, stay paused until the backlog falls well below
	// the watermark. Resuming at the watermark itself would make every reply
	// re-cross it, and each crossing costs an eventfd write and an epoll_ctl.
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

// chargeRound counts a round's output against the budget once it has grown
// past roundChargeBytes, so that the budget sees it before the round decides
// whether to read on. A round that holds less costs one atomic load.
func (c *Connection) chargeRound() {
	if c.pendingBytes.Load() < roundChargeBytes {
		return
	}
	c.mu.Lock()
	c.chargeLocked()
	c.mu.Unlock()
}

// subPending shrinks the connection's backlog by n bytes the socket took, and
// gives back whatever of it the budget no longer has to count. Callers hold
// c.mu.
func (c *Connection) subPending(n int64) {
	if n <= 0 {
		return
	}
	pending := c.pendingBytes.Add(-n)
	if pending < 0 {
		c.pendingBytes.Store(0)
		pending = 0
	}
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

// sendClosed reports whether the connection has stopped accepting sends.
func (c *Connection) sendClosed() bool {
	c.mu.Lock()
	closed := c.closing || c.closed || c.closeAfterSend || c.writeShut
	c.mu.Unlock()
	return closed
}

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
		if c.sendHead == len(c.sends) {
			c.rewindQueueLocked()
		}
		c.sends = append(c.sends, sendItem{data: data, offset: sent, pooled: true})
	}
	c.addPending(int64(len(queued)))
	// Write interest stays armed, so queueing alone needs no registration
	// change; only a watermark crossing does. While corked the flush at the end
	// of the read round settles the read interest instead.
	var armErr error
	if direct {
		armErr = c.awaitWritableLocked()
	}
	refresh := !c.corked && c.pauseStateChangedLocked()
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

// SendParts writes a two-part message without first joining the parts. If the
// socket is backpressured, only the unsent suffix is copied before returning.
func (c *Connection) SendParts(first, second []byte) error {
	if l := c.layer; l != nil {
		return l.Send(first, second)
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
	if sent < len(first) {
		c.queueLocked(first[sent:], second)
	} else {
		c.queueLocked(second[sent-len(first):], nil)
	}
	c.addPending(int64(total - sent))
	var armErr error
	if direct {
		armErr = c.awaitWritableLocked()
	}
	refresh := !c.corked && c.pauseStateChangedLocked()
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
		c.pendingEvents |= deferred
		if c.pendingEvents == deferred && (deferred == 0 || c.sendHead != len(c.sends)) {
			// Only the deferred readiness remains. Stop here instead of spinning,
			// and leave the read to whoever empties the queue. That need not be
			// a write edge: a Flush from another goroutine that took the cork
			// off this round's output writes it all without the socket ever
			// filling, and then no write edge comes. So the read is recorded
			// here, under the lock the queue drains under, and the drain hands
			// it back. A queue that drained since the read was deferred leaves
			// nothing to wait for, and the read runs now.
			c.readDeferred = deferred != 0
			c.scheduled = false
			closing := c.closePending
			c.closePending = false
			c.mu.Unlock()
			if closing {
				c.finishClose()
			}
			return
		}
		events := c.pendingEvents
		c.pendingEvents = 0
		closed := c.closed || c.closing
		c.mu.Unlock()
		if c.readShut.Load() {
			// CloseRead ended input: what the socket still reports, the end
			// of input the shutdown itself raises among it, is not read.
			events &^= evIn | evPri | evRdHup
		}
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
			if c.hasQueuedOutput() && (c.engine.writeHighWatermark <= 0 || c.readShouldStop()) {
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
		if alive && events&evErr != 0 {
			closeErr = c.socketError()
			alive = false
		} else if alive && events&(evHup|evRdHup)&^deferred != 0 {
			closeErr = io.EOF
			alive = false
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
	err := c.closeReason
	c.mu.Unlock()
	c.handler.OnClose(c, err)
}

// hasQueuedOutput reports whether bytes accepted by Send are still waiting for
// the socket. Reads are deferred while it is true.
func (c *Connection) hasQueuedOutput() bool {
	c.mu.Lock()
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
// handler. Replies produced along the way are corked: rather than one write
// syscall per message they accumulate in one contiguous buffer and reach the
// socket in a single write when the round ends.
func (c *Connection) drainInput() error {
	c.mu.Lock()
	c.corked = true
	c.readStalled = false
	c.readDeferred = false
	c.mu.Unlock()
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
	queued := c.sendHead != len(c.sends)
	c.mu.Unlock()
	if !wasCorked || !queued {
		return nil
	}
	return c.flushOutput()
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

// stallRead ends a read round with bytes possibly still in the socket and asks
// the event loop to settle what happens to them: it either pauses reads, so
// that resuming them later redelivers what is left, or, when whatever stopped
// the round is already over, hands the read straight back. Without the stall
// mark those bytes would wait for an edge that only the peer sending more can
// raise.
func (c *Connection) stallRead() {
	c.mu.Lock()
	c.readStalled = true
	c.mu.Unlock()
	c.engine.request(command{kind: commandRefresh, connection: c})
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
	if c.udp != nil || c.readHeld.Swap(hold) == hold {
		return
	}
	c.engine.request(command{kind: commandRefresh, connection: c})
}

// ReadsHeld reports whether HoldReads is currently holding reads back.
func (c *Connection) ReadsHeld() bool { return c.readHeld.Load() }

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
	for {
		if c.readShut.Load() {
			// CloseRead, perhaps from inside OnData, ended input.
			return nil
		}
		if c.readHeld.Load() {
			// The application is holding reads until it has worked through
			// what it already has. Leave the rest in the socket, where TCP
			// flow control slows the peer down instead of this side growing:
			// releasing the hold refreshes the connection, and re-arming
			// reads redelivers whatever was left behind.
			c.stallRead()
			return nil
		}
		n, err := c.sysRead(buf)
		if n > 0 {
			c.handler.OnData(c, buf[:n])
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
					c.stallRead()
					return nil
				}
				c.mu.Lock()
				c.corked = true
				c.mu.Unlock()
			}
			if n < len(buf) {
				// A short read means the socket buffer is empty, so the next
				// read would only return EAGAIN. Data arriving after this
				// point raises a fresh edge, which resubmits the connection.
				return nil
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
	if c.closing || c.closed || c.flushing || c.writeBusyLocked() {
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
	c.mu.Unlock()
	for {
		c.mu.Lock()
		if c.sendHead == len(c.sends) {
			c.resetQueueLocked()
			c.flushing = false
			c.finishCloseWriteLocked()
			closeAfterSend := c.closeAfterSend
			refresh := c.pauseStateChangedLocked()
			if c.readDeferred {
				// A round left its read to this drain; see process. The
				// event loop hands it back as it does a stalled one.
				c.readDeferred = false
				c.readStalled = true
				refresh = true
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
		} else if c.engine.useWritev && pending > 1 {
			count := pending
			if count > maxWritevItems {
				count = maxWritevItems
			}
			var batch [maxWritevItems][]byte
			buffers := batch[:count]
			for i := 0; i < count; i++ {
				item := &c.sends[c.sendHead+i]
				if item.file != nil {
					// A file is sent on its own once everything before it
					// has gone.
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
			if !fromFile {
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
				refresh := c.pauseStateChangedLocked()
				c.mu.Unlock()
				if armErr != nil {
					return armErr
				}
				if refresh {
					c.engine.request(command{kind: commandRefresh, connection: c})
				}
				return nil
			}
			c.mu.Unlock()
			continue
		}
		if err == syscall.EINTR {
			c.mu.Unlock()
			continue
		}
		c.flushing = false
		if sys.IsWouldBlock(err) {
			c.chargeLocked()
			armErr := c.awaitWritableLocked()
			refresh := c.pauseStateChangedLocked()
			c.mu.Unlock()
			if armErr != nil {
				return armErr
			}
			if refresh {
				c.engine.request(command{kind: commandRefresh, connection: c})
			}
			return nil
		}
		c.mu.Unlock()
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
	if c.udp != nil {
		return nil
	}
	return c.control("shutdown", func(s rawSocket) error {
		if c.readShut.Swap(true) {
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
		r.c.rawExposed.Store(true)
		f(uintptr(s))
		return nil
	})
}

func (r rawConn) Read(f func(fd uintptr) bool) error {
	return r.c.control("raw-read", func(s rawSocket) error {
		r.c.rawExposed.Store(true)
		if !f(uintptr(s)) {
			return ErrWouldBlock
		}
		return nil
	})
}

func (r rawConn) Write(f func(fd uintptr) bool) error {
	return r.c.control("raw-write", func(s rawSocket) error {
		r.c.rawExposed.Store(true)
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

// deadline is one of a connection's two deadlines. Its timer is rebuilt
// whenever the deadline moves, and gen tells a timer that fires while it is
// moving from the one that is current, so a connection given more time is not
// closed by the deadline it has outlived.
type deadline struct {
	timer *time.Timer
	gen   uint64
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
		c.armDeadlineLocked(&c.readDeadline, t, false)
		c.armDeadlineLocked(&c.writeDeadline, t, true)
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
	return c.setDeadline(&c.readDeadline, t, false)
}

// SetWriteDeadline closes the connection at t if output accepted by Send has
// still not reached the kernel by then; output that has all gone out leaves
// the connection alone. See SetDeadline.
func (c *Connection) SetWriteDeadline(t time.Time) error {
	return c.setDeadline(&c.writeDeadline, t, true)
}

func (c *Connection) setDeadline(d *deadline, t time.Time, write bool) error {
	c.mu.Lock()
	closed := c.closing || c.closed
	if !closed {
		c.armDeadlineLocked(d, t, write)
	}
	c.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	return nil
}

// armDeadlineLocked points d at t. A deadline set to the time it already holds
// keeps the timer it has, so a handler that re-states the same deadline on
// every read round does not rebuild a timer per round.
func (c *Connection) armDeadlineLocked(d *deadline, t time.Time, write bool) {
	if d.timer != nil && d.at.Equal(t) || d.timer == nil && t.IsZero() {
		return
	}
	d.gen++
	d.at = t
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if t.IsZero() {
		return
	}
	gen := d.gen
	d.timer = time.AfterFunc(max(time.Until(t), 0), func() { c.expireDeadline(d, gen, write) })
}

// expireDeadline closes a connection whose deadline has passed, unless the
// deadline moved while the timer was on its way or the connection is already
// going.
func (c *Connection) expireDeadline(d *deadline, gen uint64, write bool) {
	c.mu.Lock()
	stale := d.gen != gen || c.closing || c.closed
	c.mu.Unlock()
	if stale {
		return
	}
	if write && c.pendingBytes.Load() == 0 {
		// Everything Send accepted has reached the kernel, so there is no
		// write left for the deadline to be about.
		return
	}
	c.closeWithError(os.ErrDeadlineExceeded)
}

// stopDeadlinesLocked drops both timers, so a closed connection is not kept
// reachable by a deadline it will never reach.
func (c *Connection) stopDeadlinesLocked() {
	for _, d := range [...]*deadline{&c.readDeadline, &c.writeDeadline} {
		if d.timer != nil {
			d.timer.Stop()
			d.timer = nil
		}
		d.gen++
	}
}

// SendFile sends count bytes of f, starting at offset, after whatever was
// sent before it and ahead of whatever is sent after. On Linux and macOS the
// bytes go from the file to the socket by sendfile(2), without passing
// through user space; on Windows they are read a chunk at a time as the
// socket takes them. Either way the file is read only as the peer keeps up,
// so a large file costs no more memory than a small one.
//
// The connection sends from its own duplicate of f's descriptor, so the
// caller may close f as soon as SendFile returns. The file must hold the
// whole range: a file that turns out shorter closes the connection, since the
// peer was promised count bytes.
//
// A connection with a layer, such as TLS, cannot hand the file to the kernel,
// since the layer has to transform its bytes: the range is read and sent
// through the layer before SendFile returns.
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
		return sendFileCopy(c, f, offset, count, func(b []byte) error { return l.Send(b, nil) })
	}
	seg, err := newFileSegment(f, offset, count)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend || c.writeShut {
		c.mu.Unlock()
		seg.close()
		return syscall.EPIPE
	}
	direct := c.canWriteDirectlyLocked()
	if c.sendHead == len(c.sends) {
		c.rewindQueueLocked()
	}
	c.sends = append(c.sends, sendItem{file: seg})
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
	if c.closeReason != nil {
		return c.closeReason
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
