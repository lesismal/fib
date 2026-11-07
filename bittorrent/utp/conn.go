package utp

import (
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"
)

// Protocol constants, libutp's.
const (
	// maxInflight bounds the packets in flight: the receiver keeps at
	// most 1024 ahead of the next it expects, and the last slot is the
	// FIN's.
	maxInflight = 1023
	// reorderMax is how far ahead of the next packet expected one may be
	// and still be kept.
	reorderMax = 1024
	// dupAcksBeforeResend is how many duplicate acknowledgements, or
	// packets selectively acknowledged past a missing one, make it lost.
	dupAcksBeforeResend = 3
	// maxCwndIncrease is how many bytes LEDBAT may grow the congestion
	// window by in a round trip.
	maxCwndIncrease = 3000
	// minWindow is the least congestion window, in bytes.
	minWindow = 10
	// maxWindowDecay is how often, in microseconds, a loss may halve the
	// congestion window.
	maxWindowDecay = 100 * 1000
	// initialRTO is the retransmission timeout before an RTT is measured,
	// in milliseconds: RFC 6298's 1 second (libutp's is 3).
	initialRTO = 1000
	// synRetries is how many times a SYN is sent again before Dial gives
	// up: four SYNs over 15 seconds.
	synRetries = 3
	// dataRetries is how many consecutive timeouts end a connection.
	dataRetries = 4
	// handshakeTimeout is how long an accepted connection may wait for
	// the initiator's first packet after its SYN.
	handshakeTimeout = 10 * time.Second
	// maxProbeInterval is the longest wait between zero-window probes,
	// libutp's 15 seconds.
	maxProbeInterval = 15 * 1000 * 1000
	// maxLinger is how long, in microseconds, a connection that ended
	// with both FINs stays to acknowledge the peer's FIN again.
	maxLinger = 3 * 1000 * 1000
)

type connState uint8

const (
	stateSynSent connState = iota
	stateSynRecv
	stateConnected
	stateDone
)

// outPacket is a packet sent and not yet acknowledged.
type outPacket struct {
	seq           uint16
	typ           uint8
	payload       []byte
	transmissions int
	needResend    bool // lost to a timeout, and out of the window
	queued        bool // in resendNow
	sentAt        int64
}

// Stats counts a connection's packets.
type Stats struct {
	// PacketsSent and PacketsReceived count datagrams.
	PacketsSent, PacketsReceived int
	// Retransmits counts packets sent again; FastRetransmits those sent
	// again on duplicate, selective or late acknowledgements, Probes the
	// tail loss probes, Timeouts the retransmission timeouts.
	Retransmits, FastRetransmits, Probes, Timeouts int
	// CongestionWindow is LEDBAT's window, in bytes.
	CongestionWindow int
}

// Conn is a uTP connection: a net.Conn whose Read and Write carry a
// stream of bytes, with deadlines. Its protocol state is driven by the
// datagrams its Mux takes, by its own timer and by the application's
// calls, serialized by a mutex; what each of them sends goes out in one
// batch through the Mux's send function.
//
// Write blocks while Config.SendBuffer bytes are queued, which happens
// when the peer's window or LEDBAT's congestion window holds them back.
// Bytes the reader has not taken close the window the connection
// advertises, so a slow reader slows the sender down.
type Conn struct {
	m         *Mux
	cfg       *Config
	remote    netip.AddrPort
	recvID    uint16
	sendID    uint16
	initiator bool

	mu    sync.Mutex
	state connState
	err   error // why it ended; nil for a close

	// Sending.
	seqNr             uint16 // the next to take
	out               ring[*outPacket]
	inflightPkts      int // cur_window_packets: seqNr minus the oldest unacknowledged
	curWindow         int // payload bytes in flight
	sbuf              []byte
	soff              int
	resendNow         []uint16
	finQueued         bool
	finSent           bool
	finAcked          bool
	cwnd              float64
	ssthresh          float64
	slowStart         bool
	peerWnd           int
	probe             bool // one packet may go past the peer's window
	probeInterval     int64
	zeroWndAt         int64
	tlpAt             int64 // the tail loss probe's deadline
	lastMaxedOut      int64
	lastDecay         int64
	rtt, rttVar       int // milliseconds
	rttSampled        bool
	rto               int // milliseconds
	retransmitTimeout int
	rtoAt             int64
	retransmits       int
	dupAcks           int
	lateAcks          int
	fastResendSeq     uint16
	fastTimeout       bool
	ourHist           delayHist // the delay of our packets to the peer
	theirHist         delayHist // the delay of the peer's to us
	replyMicro        uint32

	// Receiving.
	ackNr        uint16 // the last packet taken in order
	in           ring[[]byte]
	reorderCount int
	reorderBytes int
	gotFin       bool
	finReached   bool
	eofSeq       uint16
	rbuf         []byte
	roff         int
	lastRcvWin   int
	ackDue       bool
	readShutdown bool

	lastSent    int64
	lastHeard   int64
	handshakeAt int64
	closeAt     int64
	lingering   bool
	lingerAt    int64

	timer   *time.Timer
	timerAt int64

	dgrams  [][]byte
	arena   []byte
	inDirty bool
	sent    bool // a data packet went out in this output, carrying the ack

	closed    bool // Close was called
	connected chan struct{}
	die       chan struct{}
	readable  chan struct{}
	writable  chan struct{}
	after     []func()
	onGone    func()
	stats     Stats

	rdl, wdl time.Time
}

func (c *Conn) init() {
	c.cwnd = float64(c.maxPayload())
	c.ssthresh = float64(c.maxWindow())
	c.slowStart = true
	c.peerWnd = c.maxPayload()
	c.rto = initialRTO
	c.retransmitTimeout = initialRTO
	c.timer = time.AfterFunc(time.Hour, c.onTimer)
	c.timer.Stop()
}

func (c *Conn) maxPayload() int { return c.cfg.PacketSize - HeaderSize }

func (c *Conn) maxWindow() int { return (maxInflight - 1) * c.maxPayload() }

func (c *Conn) sbufLen() int { return len(c.sbuf) - c.soff }

func (c *Conn) rbufLen() int { return len(c.rbuf) - c.roff }

// rcvWindow is the window to advertise: the reader's room.
func (c *Conn) rcvWindow() int {
	if c.readShutdown {
		return c.cfg.RecvBuffer
	}
	return max(c.cfg.RecvBuffer-c.rbufLen(), 0)
}

// slack is how far past the window a peer's packets are still taken: one
// probe, and the packets of a window advertised before the reader's last
// bytes arrived.
func (c *Conn) slack() int { return 8 * c.maxPayload() }

// unlock sends what is due, rearms the timer, releases the mutex and runs
// what had to wait for that.
func (c *Conn) unlock() {
	now := micros()
	c.output(now)
	c.flushDgrams()
	c.rearm(now)
	after := c.after
	c.after = nil
	c.mu.Unlock()
	for _, fn := range after {
		fn()
	}
}

// unlockQuiet releases the mutex after a datagram, leaving what it causes
// for the Mux's Flush.
func (c *Conn) unlockQuiet() {
	if c.state != stateDone || c.lingering || len(c.dgrams) > 0 {
		c.m.markDirty(c)
	}
	after := c.after
	c.after = nil
	c.mu.Unlock()
	for _, fn := range after {
		fn()
	}
}

func (c *Conn) flushDgrams() {
	if len(c.dgrams) == 0 {
		return
	}
	c.stats.PacketsSent += len(c.dgrams)
	c.m.send(c.remote, c.dgrams)
	clear(c.dgrams)
	c.dgrams = c.dgrams[:0]
	if cap(c.arena) > 256<<10 {
		c.arena = nil
	} else {
		c.arena = c.arena[:0]
	}
}

// writePacket adds a packet to the batch.
func (c *Conn) writePacket(typ uint8, seq, ack uint16, payload []byte, now int64) {
	sack := typ == stState && c.reorderCount > 0 && !c.finReached
	size := HeaderSize + len(payload)
	h := header{
		typ:    typ,
		connID: c.sendID,
		ts:     uint32(now),
		tsDiff: c.replyMicro,
		seq:    seq,
		ack:    ack,
	}
	if typ == stSyn {
		// A SYN carries the ID its answers come back with.
		h.connID = c.recvID
	}
	if typ != stReset {
		c.lastRcvWin = c.rcvWindow()
		h.wnd = uint32(c.lastRcvWin)
	}
	if sack {
		h.ext = extSACK
		size += 2 + sackBytes
	}
	start := len(c.arena)
	c.arena = append(c.arena, make([]byte, size)...)
	b := c.arena[start : start+size : start+size]
	putHeader(b, h)
	p := b[HeaderSize:]
	if sack {
		// Bit i of the mask is packet ackNr+2+i: ackNr+1 is the one
		// missing, or there would be nothing to acknowledge selectively.
		p[0], p[1] = 0, sackBytes
		var mask uint32
		for i := range sackBytes * 8 {
			if _, ok := c.in.get(c.ackNr + 2 + uint16(i)); ok {
				mask |= 1 << i
			}
		}
		p[2], p[3], p[4], p[5] = byte(mask), byte(mask>>8), byte(mask>>16), byte(mask>>24)
		p = p[2+sackBytes:]
	}
	copy(p, payload)
	c.dgrams = append(c.dgrams, b)
	if typ != stReset {
		c.lastSent = now
	}
}

// ackField is the ack_nr of a packet of type typ. Before an accepted
// connection hears from its initiator, its data acknowledge one short of
// the SYN: libutp, while its SYN is unanswered, drops data, and data
// that acknowledged the SYN would leave it waiting for an answer it then
// never sends again. Acknowledging short of the SYN leaves the SYN
// unanswered, so libutp sends it again, and the SYN's answer goes back.
func (c *Conn) ackField(typ uint8) uint16 {
	if c.state == stateSynRecv && (typ == stData || typ == stFin) {
		return c.ackNr - 1
	}
	return c.ackNr
}

// transmit sends p, for the first time or again.
func (c *Conn) transmit(p *outPacket, now int64) {
	if p.transmissions == 0 || p.needResend {
		c.curWindow += len(p.payload)
	}
	if p.transmissions > 0 {
		c.stats.Retransmits++
	}
	p.needResend = false
	p.transmissions++
	p.sentAt = now
	c.writePacket(p.typ, p.seq, c.ackField(p.typ), p.payload, now)
	if p.typ == stData || p.typ == stFin {
		c.sent = true
	}
}

// queueResend sends p again in the next output, whatever the windows.
func (c *Conn) queueResend(p *outPacket) {
	if !p.queued {
		p.queued = true
		c.resendNow = append(c.resendNow, p.seq)
	}
}

// output sends what is due: the packets given up as lost, those the
// windows make room for, the FIN, and an acknowledgement unless a data
// packet carried it.
func (c *Conn) output(now int64) {
	c.sent = false
	if c.state == stateDone {
		if c.lingering && c.ackDue {
			c.writePacket(stState, c.seqNr, c.ackNr, nil, now)
		}
		c.ackDue = false
		return
	}
	for _, seq := range c.resendNow {
		if p, ok := c.out.get(seq); ok && p.queued {
			p.queued = false
			c.transmit(p, now)
		}
	}
	c.resendNow = c.resendNow[:0]
	if c.state == stateSynSent {
		return
	}

	peerLimited := false
	fits := func(n int) bool {
		if c.curWindow+n > c.peerWnd && !c.probe {
			peerLimited = true
			return false
		}
		// One packet may always be in flight, however small LEDBAT's
		// window has shrunk: libutp waits for a timeout to reset it.
		return c.curWindow == 0 || float64(c.curWindow+n) <= c.cwnd
	}
	sentOne := func() {
		if c.probe {
			c.probe = false
		}
	}
	blocked := false
	for i := c.inflightPkts; i > 0 && !blocked; i-- {
		p, ok := c.out.get(c.seqNr - uint16(i))
		if !ok || !p.needResend {
			continue
		}
		if !fits(len(p.payload)) {
			blocked = true
			break
		}
		c.transmit(p, now)
		sentOne()
	}
	for !blocked && c.sbufLen() > 0 && c.inflightPkts < maxInflight-1 {
		n := min(c.sbufLen(), c.maxPayload())
		if n < c.maxPayload() && !c.cfg.NoDelay && c.inflightPkts > 0 && !c.finQueued {
			break // Nagle: wait to fill the packet, or for the others' acks
		}
		if !fits(n) {
			blocked = true
			break
		}
		p := &outPacket{seq: c.seqNr, typ: stData, payload: make([]byte, n)}
		copy(p.payload, c.sbuf[c.soff:])
		c.soff += n
		if c.soff == len(c.sbuf) {
			c.sbuf, c.soff = c.sbuf[:0], 0
		}
		c.push(p, now)
		c.transmit(p, now)
		sentOne()
		notify(c.writable)
	}
	if blocked {
		c.lastMaxedOut = now
		if peerLimited && c.curWindow == 0 && c.zeroWndAt == 0 {
			if c.probeInterval == 0 {
				c.probeInterval = int64(max(c.rto, 500)) * 1000
			}
			c.zeroWndAt = now + c.probeInterval
			c.probeInterval = min(2*c.probeInterval, maxProbeInterval)
		}
	}
	if c.finQueued && !c.finSent && c.sbufLen() == 0 && c.inflightPkts < maxInflight {
		p := &outPacket{seq: c.seqNr, typ: stFin}
		c.push(p, now)
		c.finSent = true
		c.transmit(p, now)
	}
	if c.ackDue && (!c.sent || (c.reorderCount > 0 && !c.finReached)) {
		c.writePacket(stState, c.seqNr, c.ackNr, nil, now)
	}
	c.ackDue = false
}

// push takes the next sequence number for p and keeps it until acked.
func (c *Conn) push(p *outPacket, now int64) {
	if c.inflightPkts == 0 {
		c.retransmitTimeout = c.rto
		c.rtoAt = now + int64(c.rto)*1000
		c.armProbe(now)
	}
	c.out.put(p.seq, p, c.seqNr-uint16(c.inflightPkts))
	c.seqNr++
	c.inflightPkts++
}

// process takes a packet for the connection, as libutp's
// utp_process_incoming.
func (c *Conn) process(h header, sack, payload []byte, now int64) {
	c.stats.PacketsReceived++
	if c.state == stateDone {
		// A connection lingering after both FINs answers the peer's
		// FIN again, in case the answer was lost.
		if c.lingering && h.typ != stState {
			c.ackDue = true
		}
		return
	}
	// An acknowledgement of a packet not sent is from a spoofed address
	// or a confused peer. One older than the packets in flight and three
	// is from a packet the path held back a while: libutp drops such a
	// packet, data and all, where only its acknowledgement is ignored
	// here.
	if seqLess(c.seqNr-1, h.ack) {
		return
	}
	window := uint16(max(c.inflightPkts+dupAcksBeforeResend, dupAcksBeforeResend))
	staleAck := seqLess(h.ack, c.seqNr-1-window)
	if staleAck && c.state == stateSynSent {
		return
	}
	c.lastHeard = now
	switch c.state {
	case stateSynSent:
		// Any answer establishes the connection; data from an acceptor
		// that does not wait for our first packet, as fib's, answer the
		// SYN as well as a STATE would (see ackField).
		c.ackNr = h.seq - 1
		c.state = stateConnected
		if h.typ != stState && c.inflightPkts == 1 {
			c.ackPacket(c.seqNr-1, now)
			c.inflightPkts = 0
		}
		// Acknowledge the answer, which tells an acceptor that waits for
		// our first packet that we are here before we have data.
		c.ackDue = true
		close(c.connected)
	case stateSynRecv:
		c.state = stateConnected
		c.handshakeAt = 0
	}

	seqnr := h.seq - c.ackNr - 1
	if seqnr >= reorderMax {
		if seqnr >= 1<<16-reorderMax && h.typ != stState {
			c.ackDue = true // a packet again, whose ack was lost
		}
		return
	}

	if !staleAck {
		c.takeAcks(h, sack, now)
	}
	if c.finAcked && c.closed {
		c.finish(now)
		return
	}

	if h.typ == stState {
		return
	}
	if h.typ == stFin && !c.gotFin {
		c.gotFin = true
		c.eofSeq = h.seq
	}
	if seqnr == 0 {
		if !c.deliver(payload) {
			return // no room: the peer sends it again
		}
		c.ackNr++
		c.advance()
		c.ackDue = true
		return
	}
	// Ahead of the next expected: kept until the gap fills.
	if c.gotFin && seqLess(c.eofSeq, h.seq) {
		return
	}
	c.ackDue = true
	if _, ok := c.in.get(h.seq); ok {
		return
	}
	if c.reorderBytes+len(payload) > c.cfg.RecvBuffer+c.slack() {
		return
	}
	c.in.put(h.seq, append([]byte(nil), payload...), c.ackNr+1)
	c.reorderCount++
	c.reorderBytes += len(payload)
}

// takeAcks acts on the acknowledgements of a packet, as the middle of
// libutp's utp_process_incoming: the packets acknowledged, cumulatively
// and selectively, the round trip and the delays they measure, LEDBAT,
// the peer's window, and the losses they show.
func (c *Conn) takeAcks(h header, sack []byte, now int64) {
	acks := int(h.ack - (c.seqNr - 1 - uint16(c.inflightPkts)))
	if acks > c.inflightPkts {
		acks = 0
	}
	if c.inflightPkts > 0 {
		oldest := c.seqNr - uint16(c.inflightPkts)
		if h.ack == oldest-1 {
			// Only a STATE repeating the acknowledgement is a duplicate
			// acknowledgement: data repeat it whenever the peer has
			// something to say.
			if h.typ == stState {
				c.dupAcks++
				if c.dupAcks == dupAcksBeforeResend {
					c.fastResendOldest(now, false)
				}
			}
			// But any packet sent when the oldest should have arrived,
			// that still lacks it, says it was lost (RACK, RFC 8985).
			// libutp sends no STATE, and so no SACK, while it has data
			// to send, which leaves a timeout to find each loss of a
			// transfer both ways.
			if p, ok := c.out.get(oldest); ok && c.rttSampled && p.transmissions > 0 && !p.queued && now-p.sentAt > c.rackWindow() {
				c.lateAcks++
				if c.lateAcks == dupAcksBeforeResend {
					c.lateAcks = 0
					c.fastResendOldest(now, true)
				}
			}
		} else {
			c.dupAcks, c.lateAcks = 0, 0
		}
	}

	ackedBytes := 0
	minRTT := int64(1<<63 - 1)
	oldest := c.seqNr - uint16(c.inflightPkts)
	for i := range acks {
		if p, ok := c.out.get(oldest + uint16(i)); ok && p.transmissions > 0 {
			ackedBytes += len(p.payload)
			minRTT = min(minRTT, rttOf(p, now))
		}
	}
	if sack != nil {
		ackedBytes += c.sackBytes(h.ack+2, sack, &minRTT, now)
	}

	// The one-way delays, each measured against the other end's clock:
	// the peer's packets to us, which we report back, and ours to the
	// peer, which it reports.
	var theirDelay uint32
	if h.ts != 0 {
		theirDelay = uint32(now) - h.ts
	}
	c.replyMicro = theirDelay
	prevBase := c.theirHist.base
	if theirDelay != 0 {
		c.theirHist.add(theirDelay, now)
	}
	if prevBase != 0 && tsLess(c.theirHist.base, prevBase) && prevBase-c.theirHist.base <= 10000 {
		// Our clock runs slower than the peer's: what looks like our
		// delay growing is the drift.
		c.ourHist.shift(prevBase - c.theirHist.base)
	}
	actualDelay := h.tsDiff
	if actualDelay == 1<<31-1 {
		actualDelay = 0
	}
	if actualDelay != 0 {
		c.ourHist.add(actualDelay, now)
	}
	if v := int64(c.ourHist.value()); v > minRTT {
		// The queuing delay cannot exceed the round trip.
		c.ourHist.shift(uint32(v - minRTT))
	}
	if actualDelay != 0 && ackedBytes > 0 {
		c.ccontrol(ackedBytes, minRTT, now)
	}

	c.peerWnd = int(h.wnd)
	if c.peerWnd >= c.maxPayload() {
		c.zeroWndAt, c.probeInterval = 0, 0
	}
	if c.finSent && acks == c.inflightPkts && acks > 0 {
		c.finAcked = true
	}
	if seqLess(c.fastResendSeq, h.ack+1) {
		c.fastResendSeq = h.ack + 1
	}
	for range acks {
		if c.ackPacket(c.seqNr-uint16(c.inflightPkts), now) == 2 {
			break
		}
		c.inflightPkts--
	}
	// Packets selectively acknowledged before are gone already.
	for c.inflightPkts > 0 {
		if _, ok := c.out.get(c.seqNr - uint16(c.inflightPkts)); ok {
			break
		}
		c.inflightPkts--
	}
	if c.inflightPkts == 0 {
		c.rtoAt, c.tlpAt = 0, 0
	}
	if c.fastTimeout {
		// After a timeout the oldest packet went again; each ack that
		// leaves the next one oldest sends that one again too, until one
		// shows it arrived.
		oldest := c.seqNr - uint16(c.inflightPkts)
		if oldest != c.fastResendSeq {
			c.fastTimeout = false
		} else if p, ok := c.out.get(oldest); ok && p.transmissions > 0 {
			c.fastResendSeq++
			c.queueResend(p)
		}
	}
	if sack != nil {
		c.selectiveAck(h.ack+2, sack, now)
	}
	if acks > 0 {
		notify(c.writable)
		if c.inflightPkts > 0 {
			c.armProbe(now)
		}
	}
}

// advance takes the packets kept ahead that are now in order, and sees
// whether the FIN was reached.
func (c *Conn) advance() {
	for {
		if !c.finReached && c.gotFin && c.eofSeq == c.ackNr {
			c.finReached = true
			c.ackDue = true
			// Whatever was kept past the FIN is not the peer's data.
			c.in.reset()
			c.reorderCount, c.reorderBytes = 0, 0
			notify(c.readable)
		}
		if c.reorderCount == 0 {
			return
		}
		d, ok := c.in.get(c.ackNr + 1)
		if !ok || !c.deliver(d) {
			return
		}
		c.in.del(c.ackNr + 1)
		c.reorderCount--
		c.reorderBytes -= len(d)
		c.ackNr++
		c.ackDue = true
	}
}

// deliver hands payload to the reader, and reports false when there is no
// room for it.
func (c *Conn) deliver(payload []byte) bool {
	if len(payload) == 0 || c.readShutdown {
		return true
	}
	if c.rbufLen()+len(payload) > c.cfg.RecvBuffer+c.slack() {
		return false
	}
	if c.roff > 0 && c.roff >= len(c.rbuf)/2 {
		n := copy(c.rbuf, c.rbuf[c.roff:])
		c.rbuf, c.roff = c.rbuf[:n], 0
	}
	c.rbuf = append(c.rbuf, payload...)
	notify(c.readable)
	return true
}

func rttOf(p *outPacket, now int64) int64 {
	if p.sentAt < now {
		return now - p.sentAt
	}
	return 50000
}

// ackPacket removes an acknowledged packet: 0 when it was in flight, 1
// when it was gone already and 2 when it was never sent.
func (c *Conn) ackPacket(seq uint16, now int64) int {
	p, ok := c.out.get(seq)
	if !ok {
		return 1
	}
	if p.transmissions == 0 {
		return 2
	}
	c.out.del(seq)
	if p.transmissions == 1 {
		// Karn: only a packet sent once times the round trip.
		ertt := int((now - p.sentAt) / 1000)
		if !c.rttSampled {
			c.rtt, c.rttVar, c.rttSampled = ertt, ertt/2, true
		} else {
			delta := c.rtt - ertt
			if delta < 0 {
				delta = -delta
			}
			c.rttVar += (delta - c.rttVar) / 4
			c.rtt += (ertt - c.rtt) / 8
		}
		c.rto = max(c.rtt+4*c.rttVar, int(c.cfg.MinRTO/time.Millisecond))
	}
	c.retransmitTimeout = c.rto
	c.rtoAt = now + int64(c.rto)*1000
	if !p.needResend {
		c.curWindow -= len(p.payload)
	}
	c.retransmits = 0
	return 0
}

// sackBytes counts the bytes a selective acknowledgement acknowledges
// that are still in flight, as libutp's selective_ack_bytes.
func (c *Conn) sackBytes(base uint16, mask []byte, minRTT *int64, now int64) int {
	if c.inflightPkts == 0 {
		return 0
	}
	mask = mask[:min(len(mask), 128)]
	n := 0
	for bit := len(mask)*8 - 1; bit >= 0; bit-- {
		v := base + uint16(bit)
		if c.seqNr-v-1 >= uint16(c.inflightPkts-1) {
			continue // not sent, or the oldest, which a SACK never has
		}
		p, ok := c.out.get(v)
		if !ok || p.transmissions == 0 {
			continue
		}
		if mask[bit>>3]&(1<<(bit&7)) != 0 {
			n += len(p.payload)
			*minRTT = min(*minRTT, rttOf(p, now))
		}
	}
	return n
}

// selectiveAck acts on a selective acknowledgement, as libutp's
// selective_ack: the packets it has arrived, and a packet it lacks with
// three or more after it that arrived is lost, and sent again at once.
func (c *Conn) selectiveAck(base uint16, mask []byte, now int64) {
	if c.inflightPkts == 0 {
		return
	}
	mask = mask[:min(len(mask), 128)]
	count := 0
	var resends []uint16
	// A packet sent again is lost again when packets sent a while after
	// it arrived (RACK, RFC 8985): libutp sends a packet again on a SACK
	// only once, and waits for a timeout when that is lost too.
	newest := int64(0)
	reo := max(int64(c.rtt)*1000/4, 1000)
	lostAgain := func(p *outPacket) bool {
		return p.transmissions > 1 && !p.queued && p.sentAt+reo < newest
	}
	for bit := len(mask)*8 - 1; bit >= 0; bit-- {
		v := base + uint16(bit)
		if c.seqNr-v-1 >= uint16(c.inflightPkts-1) {
			continue
		}
		set := mask[bit>>3]&(1<<(bit&7)) != 0
		if set {
			count++
		}
		p, ok := c.out.get(v)
		if !ok || p.transmissions == 0 {
			continue
		}
		if set {
			newest = max(newest, p.sentAt)
			c.ackPacket(v, now)
			continue
		}
		if count >= dupAcksBeforeResend && (v-c.fastResendSeq <= reorderMax || lostAgain(p)) {
			resends = append(resends, v)
		}
	}
	if count >= dupAcksBeforeResend {
		if base-1-c.fastResendSeq <= reorderMax {
			resends = append(resends, base-1)
		} else if p, ok := c.out.get(base - 1); ok && lostAgain(p) {
			resends = append(resends, base-1)
		}
	}
	// resends runs from the newest down: send the oldest first, at most
	// four.
	backOff, sent := false, 0
	for i := len(resends) - 1; i >= 0 && sent < 4; i-- {
		v := resends[i]
		p, ok := c.out.get(v)
		if !ok {
			continue
		}
		backOff = true
		c.queueResend(p)
		c.stats.FastRetransmits++
		if seqLess(c.fastResendSeq, v+1) {
			c.fastResendSeq = v + 1
		}
		sent++
	}
	if backOff {
		c.maybeDecayWin(now)
	}
	c.dupAcks = count
}

// fastResendOldest sends the oldest packet again on three duplicate
// acknowledgements, which libutp leaves as a TODO and BEP 29 asks for.
// With late set, the packet went more than a RACK window ago, and goes
// again even if it went again before.
func (c *Conn) fastResendOldest(now int64, late bool) {
	oldest := c.seqNr - uint16(c.inflightPkts)
	p, ok := c.out.get(oldest)
	if !ok || p.transmissions == 0 || (seqLess(oldest, c.fastResendSeq) && !late) {
		return
	}
	c.queueResend(p)
	c.stats.FastRetransmits++
	if seqLess(c.fastResendSeq, oldest+1) {
		c.fastResendSeq = oldest + 1
	}
	c.maybeDecayWin(now)
}

// armProbe sets the tail loss probe (RFC 8985): with packets in flight
// and no acknowledgement for two round trips, the oldest goes again,
// which finds a loss at the end of a burst, or of a packet whose
// acknowledgements the peer sends none of, long before the
// retransmission timeout, which is at least half a second.
func (c *Conn) armProbe(now int64) {
	if c.rttSampled {
		c.tlpAt = now + max(2*int64(c.rtt)*1000, 10000)
	}
}

// rackWindow is how long, in microseconds, after a packet is sent an
// acknowledgement that still lacks it says it was lost: a round trip and
// a quarter, for the reordering and the jitter of the path.
func (c *Conn) rackWindow() int64 {
	rtt := int64(c.rtt) * 1000
	return rtt + max(rtt/4, 2000)
}

// maybeDecayWin halves the congestion window on a loss, at most once in
// maxWindowDecay.
func (c *Conn) maybeDecayWin(now int64) {
	if now-c.lastDecay < maxWindowDecay {
		return
	}
	c.cwnd = max(c.cwnd/2, minWindow)
	c.lastDecay = now
	c.slowStart = false
	c.ssthresh = c.cwnd
}

// ccontrol is LEDBAT, as libutp's apply_ccontrol: the window grows by up
// to maxCwndIncrease bytes a round trip while the queuing delay is under
// the target, in proportion to how far under, and shrinks as far over.
func (c *Conn) ccontrol(ackedBytes int, minRTT int64, now int64) {
	ourDelay := min(int64(c.ourHist.value()), minRTT)
	target := float64(c.cfg.TargetDelay / time.Microsecond)
	offTarget := target - float64(ourDelay)
	acked := float64(ackedBytes)
	windowFactor := min(acked, c.cwnd) / max(c.cwnd, acked)
	delayFactor := offTarget / target
	gain := maxCwndIncrease * windowFactor * delayFactor
	appLimited := now-c.lastMaxedOut > 1000*1000
	if gain > 0 && appLimited {
		// Not held back by the window for a second: the application
		// sends slower than the window allows, which then says nothing
		// of the path, and must not grow without bound.
		gain = 0
	}
	ledbat := max(c.cwnd+gain, minWindow)
	if c.slowStart {
		// Slow start doubles the window each round trip, as TCP's,
		// until the delay nears the target (libutp grows it by a packet
		// a round trip).
		ss := c.cwnd
		if !appLimited {
			ss += acked
		}
		switch {
		case ss > c.ssthresh:
			c.slowStart = false
		case float64(ourDelay) > target*0.9:
			c.slowStart = false
			c.ssthresh = c.cwnd
		default:
			c.cwnd = max(ss, ledbat)
		}
	} else {
		c.cwnd = ledbat
	}
	c.cwnd = min(max(c.cwnd, minWindow), float64(c.maxWindow()))
}

// onTimer runs the connection's deadlines.
func (c *Conn) onTimer() {
	c.mu.Lock()
	c.timerAt = 0
	c.checkTimeouts(micros())
	c.unlock()
}

// checkTimeouts acts on the deadlines that have passed, as libutp's
// check_timeouts.
func (c *Conn) checkTimeouts(now int64) {
	if c.state == stateDone {
		if c.lingering && now >= c.lingerAt {
			c.lingering = false
			c.after = append(c.after, func() { c.m.forget(c) })
		}
		return
	}
	if c.closeAt != 0 && now >= c.closeAt {
		c.abort(nil)
		return
	}
	if idle := c.cfg.IdleTimeout; idle > 0 && now-c.lastHeard >= int64(idle/time.Microsecond) {
		c.abort(ErrIdle)
		return
	}
	if c.state == stateSynRecv && c.handshakeAt != 0 && now >= c.handshakeAt {
		c.destroy(ErrTimeout, now)
		return
	}
	if c.tlpAt != 0 && now >= c.tlpAt {
		c.tlpAt = 0
		if p, ok := c.out.get(c.seqNr - uint16(c.inflightPkts)); ok && p.transmissions > 0 && !p.queued {
			c.queueResend(p)
			c.stats.Probes++
		}
	}
	if c.zeroWndAt != 0 && now >= c.zeroWndAt {
		c.zeroWndAt = 0
		c.probe = true
	}
	if c.rtoAt != 0 && now >= c.rtoAt && c.inflightPkts > 0 {
		if c.retransmits >= dataRetries || (c.state == stateSynSent && c.retransmits >= synRetries) {
			if c.closed {
				c.destroy(nil, now)
			} else {
				c.destroy(ErrTimeout, now)
			}
			return
		}
		c.stats.Timeouts++
		c.tlpAt = 0
		c.retransmitTimeout *= 2
		c.rtoAt = now + int64(c.retransmitTimeout)*1000
		c.dupAcks, c.lateAcks = 0, 0
		// Everything in flight is lost: the window starts over from a
		// packet, and the oldest goes again at once.
		c.cwnd = float64(c.maxPayload())
		c.slowStart = true
		for i := 1; i <= c.inflightPkts; i++ {
			if p, ok := c.out.get(c.seqNr - uint16(i)); ok && p.transmissions > 0 && !p.needResend {
				p.needResend = true
				c.curWindow -= len(p.payload)
			}
		}
		if c.state == stateSynSent && c.retransmits > 0 {
			// The SYN went twice unanswered. libutp drops a SYN whose
			// connection it has, and its accepted connection waits for
			// data forever: once its answer is lost, no SYN of the same
			// ID gets one. Reset that half-open connection and start
			// again with another ID; the first SYN again keeps the ID, for
			// a SYN lost on its way, which is the likelier.
			c.writePacket(stReset, c.seqNr-1, 0, nil, now)
			c.m.rekey(c)
		}
		c.retransmits++
		c.fastTimeout = true
		if p, ok := c.out.get(c.seqNr - uint16(c.inflightPkts)); ok {
			c.queueResend(p)
		}
	}
	if ka := c.cfg.KeepAlive; ka > 0 && c.state == stateConnected && !c.finSent &&
		now-c.lastSent >= int64(ka/time.Microsecond) {
		// libutp's keepalive: a STATE one short of the acknowledgement,
		// which keeps the NAT's mapping and changes nothing.
		c.writePacket(stState, c.seqNr, c.ackNr-1, nil, now)
	}
}

// rearm sets the timer for the next deadline. A timer already set to go
// off sooner is left alone: it finds nothing due and rearms, which costs
// less than resetting it on every acknowledgement.
func (c *Conn) rearm(now int64) {
	at := int64(0)
	consider := func(t int64) {
		if t != 0 && (at == 0 || t < at) {
			at = t
		}
	}
	if c.state == stateDone {
		if c.lingering {
			consider(c.lingerAt)
		}
	} else {
		if c.inflightPkts > 0 {
			consider(c.rtoAt)
		}
		consider(c.zeroWndAt)
		if c.inflightPkts > 0 {
			consider(c.tlpAt)
		}
		consider(c.handshakeAt)
		consider(c.closeAt)
		if ka := c.cfg.KeepAlive; ka > 0 && c.state == stateConnected && !c.finSent {
			consider(c.lastSent + int64(ka/time.Microsecond))
		}
		if idle := c.cfg.IdleTimeout; idle > 0 {
			consider(c.lastHeard + int64(idle/time.Microsecond))
		}
	}
	if at == 0 {
		if c.timerAt != 0 {
			c.timer.Stop()
			c.timerAt = 0
		}
		return
	}
	if c.timerAt != 0 && c.timerAt <= at {
		return
	}
	c.timerAt = at
	c.timer.Reset(time.Duration(max(at-now, 0)) * time.Microsecond)
}

// dupSyn answers a SYN again, whose answer was lost.
func (c *Conn) dupSyn() {
	if c.state == stateSynRecv {
		c.ackDue = true
	}
}

// reset takes the peer's RESET.
func (c *Conn) reset() {
	switch {
	case c.state == stateDone:
		if c.lingering {
			c.lingering = false
			c.after = append(c.after, func() { c.m.forget(c) })
		}
	case c.closed:
		c.destroy(nil, micros())
	case c.state == stateSynSent:
		c.destroy(ErrRefused, micros())
	default:
		c.destroy(ErrReset, micros())
	}
}

// abort resets the connection, telling the peer.
func (c *Conn) abort(err error) {
	if c.state == stateDone {
		return
	}
	now := micros()
	if c.state != stateSynSent {
		c.writePacket(stReset, c.seqNr, c.ackNr, nil, now)
	}
	c.destroy(err, now)
}

// finish ends a connection whose FIN was acknowledged after Close; it
// lingers to acknowledge the peer's FIN again when it got one.
func (c *Conn) finish(now int64) {
	if c.finReached {
		c.lingering = true
		c.lingerAt = now + min(max(3*int64(c.rto)*1000, 500*1000), maxLinger)
	}
	c.destroy(nil, now)
}

// destroy ends the connection.
func (c *Conn) destroy(err error, now int64) {
	if c.state == stateDone {
		return
	}
	c.state = stateDone
	c.err = err
	close(c.die)
	select {
	case <-c.connected:
	default:
		close(c.connected)
	}
	c.out.reset()
	c.in.reset()
	c.resendNow = c.resendNow[:0]
	c.sbuf, c.soff = nil, 0
	c.inflightPkts, c.curWindow = 0, 0
	if !c.lingering {
		c.after = append(c.after, func() { c.m.forget(c) })
	}
}

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// wait waits for ch, the connection to end, or the deadline.
func wait(ch, die chan struct{}, dl time.Time) error {
	var timeout <-chan time.Time
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return os.ErrDeadlineExceeded
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case <-ch:
	case <-die:
	case <-timeout:
		return os.ErrDeadlineExceeded
	}
	return nil
}

// endErr is what Read and Write report once the connection ended.
func (c *Conn) endErr() error {
	if c.closed || c.err == nil {
		return net.ErrClosed
	}
	return c.err
}

// Read reads the peer's bytes, as net.Conn; it reports io.EOF once the
// peer's FIN and everything before it are read.
func (c *Conn) Read(b []byte) (int, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, net.ErrClosed
		}
		if c.rbufLen() > 0 {
			n := copy(b, c.rbuf[c.roff:])
			c.roff += n
			if c.roff == len(c.rbuf) {
				c.rbuf, c.roff = c.rbuf[:0], 0
			}
			c.afterRead()
			c.unlock()
			return n, nil
		}
		if c.finReached {
			c.mu.Unlock()
			return 0, io.EOF
		}
		if c.state == stateDone {
			err := c.endErr()
			c.mu.Unlock()
			return 0, err
		}
		if len(b) == 0 {
			c.mu.Unlock()
			return 0, nil
		}
		dl := c.rdl
		c.mu.Unlock()
		if err := wait(c.readable, c.die, dl); err != nil {
			return 0, err
		}
	}
}

// afterRead takes what waited for room, and tells the peer the window
// opened when it opened from less than two packets or by half the buffer.
func (c *Conn) afterRead() {
	if c.state == stateDone {
		return
	}
	if c.reorderCount > 0 || (c.gotFin && !c.finReached) {
		c.advance()
	}
	w, mp := c.rcvWindow(), c.maxPayload()
	if (c.lastRcvWin < 2*mp && w >= 2*mp) || w-c.lastRcvWin >= c.cfg.RecvBuffer/2 {
		c.ackDue = true
	}
}

// Write sends b, as net.Conn, blocking while Config.SendBuffer bytes are
// queued.
func (c *Conn) Write(b []byte) (int, error) {
	n := 0
	for len(b) > 0 {
		c.mu.Lock()
		if c.closed || c.finQueued {
			c.mu.Unlock()
			return n, net.ErrClosed
		}
		if c.state == stateDone {
			err := c.endErr()
			c.mu.Unlock()
			return n, err
		}
		if room := c.cfg.SendBuffer - c.sbufLen(); room > 0 {
			k := min(room, len(b))
			if c.soff > 0 && c.soff >= len(c.sbuf)/2 {
				m := copy(c.sbuf, c.sbuf[c.soff:])
				c.sbuf, c.soff = c.sbuf[:m], 0
			}
			c.sbuf = append(c.sbuf, b[:k]...)
			b = b[k:]
			n += k
			c.unlock()
			continue
		}
		dl := c.wdl
		c.mu.Unlock()
		if err := wait(c.writable, c.die, dl); err != nil {
			return n, err
		}
	}
	return n, nil
}

// CloseWrite sends a FIN once what was written is sent: the peer reads
// io.EOF after it, and the connection still reads.
func (c *Conn) CloseWrite() error {
	c.mu.Lock()
	if c.closed || c.state == stateDone {
		c.mu.Unlock()
		return net.ErrClosed
	}
	c.finQueued = true
	c.unlock()
	return nil
}

// Close closes the connection. What was written is still sent, then a FIN,
// for up to Config.Linger; what the peer sends after is acknowledged and
// dropped. The connection ends when the FIN is acknowledged.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.readShutdown = true
	c.rbuf, c.roff = nil, 0
	c.in.reset()
	c.reorderCount, c.reorderBytes = 0, 0
	now := micros()
	switch {
	case c.state == stateDone:
	case c.state == stateSynSent,
		c.state == stateSynRecv && c.inflightPkts == 0 && c.sbufLen() == 0:
		c.destroy(nil, now)
	case c.finAcked:
		c.finish(now)
	default:
		c.finQueued = true
		c.closeAt = now + int64(c.cfg.Linger/time.Microsecond)
	}
	notify(c.readable)
	notify(c.writable)
	c.unlock()
	return nil
}

// Stats returns the connection's counters.
func (c *Conn) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.CongestionWindow = int(c.cwnd)
	return s
}

// RTT returns the smoothed round-trip time.
func (c *Conn) RTT() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Duration(c.rtt) * time.Millisecond
}

// LocalAddr returns the Mux's address.
func (c *Conn) LocalAddr() net.Addr { return c.m.Addr() }

// RemoteAddr returns the peer's address.
func (c *Conn) RemoteAddr() net.Addr { return net.UDPAddrFromAddrPort(c.remote) }

// RemoteAddrPort returns the peer's address.
func (c *Conn) RemoteAddrPort() netip.AddrPort { return c.remote }

// SetDeadline sets the read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.rdl, c.wdl = t, t
	c.mu.Unlock()
	notify(c.readable)
	notify(c.writable)
	return nil
}

// SetReadDeadline sets the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rdl = t
	c.mu.Unlock()
	notify(c.readable)
	return nil
}

// SetWriteDeadline sets the write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.wdl = t
	c.mu.Unlock()
	notify(c.writable)
	return nil
}

var _ net.Conn = (*Conn)(nil)
