package kcp

import (
	"encoding/binary"
	"errors"
)

// The segment header: conv(4) cmd(1) frg(1) wnd(2) ts(4) sn(4) una(4)
// len(4), little-endian.
const (
	// Overhead is the size of a segment's header.
	Overhead = 24
	snOffset = 12

	cmdPush = 81 // data
	cmdAck  = 82 // acknowledgement
	cmdWask = 83 // window probe: asks the peer for its window
	cmdWins = 84 // window size: answers a probe

	askSend = 1 // a WASK is due
	askTell = 2 // a WINS is due

	rtoNoDelay = 30    // minimum RTO in no-delay mode, ms
	rtoMin     = 100   // minimum RTO, ms
	rtoDefault = 200   // RTO before the first round trip is measured, ms
	rtoMax     = 60000 // maximum RTO, ms

	// DefaultSendWindow and DefaultRecvWindow are the windows, in
	// segments, of a KCP whose window is not set.
	DefaultSendWindow = 128
	DefaultRecvWindow = 128
	// DefaultMTU is the largest datagram a KCP outputs when its MTU is not
	// set, the KCP header included and any framing (crypt, FEC) not.
	DefaultMTU = 1400
	// DefaultInterval is the flush interval of the reference
	// implementations, which this KCP only uses in its RTO.
	DefaultInterval = 100
	// DefaultDeadLink is how many times a segment is sent before the
	// link is declared dead.
	DefaultDeadLink = 20

	threshInit = 2
	threshMin  = 2
	probeInit  = 500    // first window probe after the peer's window closes, ms
	probeLimit = 120000 // largest gap between window probes, ms

	// maxFragments is how many segments one message may take: frg is a
	// byte, counting down to 0.
	maxFragments = 256
)

// Errors of KCP's methods.
var (
	ErrConv          = errors.New("kcp: segment of another conversation")
	ErrMalformed     = errors.New("kcp: malformed segment")
	ErrMessageTooBig = errors.New("kcp: message needs more than 255 fragments")
	ErrEmpty         = errors.New("kcp: empty message")
	ErrNoMessage     = errors.New("kcp: no complete message")
	ErrShortBuffer   = errors.New("kcp: buffer shorter than the message")
)

type segment struct {
	cmd      uint8
	frg      uint8
	wnd      uint16
	ts       uint32
	sn       uint32
	una      uint32
	resendts uint32
	rto      uint32
	fastack  uint32
	xmit     uint32
	acked    bool
	data     []byte
}

type ackItem struct{ sn, ts uint32 }

func timediff(later, earlier uint32) int32 { return int32(later - earlier) }

// KCP is one conversation of the KCP protocol (github.com/skywind3000/kcp)
// without any I/O: the caller feeds it the peer's packets with Input, its
// own messages with Send, the time with every call, and calls Flush when
// NextFlush says to; it gets the packets to send through the output
// function, and the peer's messages from Recv. Its segments are those of
// the reference implementation and of github.com/xtaci/kcp-go, so it talks
// to either.
//
// Unlike the reference implementation, which flushes on a fixed interval,
// a KCP here is flushed when something is due: acknowledgements after the
// packets that call for them, new data once Send queues it or the window
// opens, a retransmission at its timeout, a window probe at its time.
// NextFlush returns no time at all for a conversation with nothing in
// flight, so an idle one costs no timer.
//
// A KCP is not safe for concurrent use. Times are milliseconds on any
// clock, wrapping at 2^32.
type KCP struct {
	conv, mtu, mss uint32
	dead           bool

	sndUna, sndNxt, rcvNxt uint32
	ssthresh               uint32
	rxRttvar, rxSrtt       int32
	rxRto, rxMinrto        uint32
	sndWnd, rcvWnd, rmtWnd uint32
	cwnd, incr             uint32
	probe                  uint32
	current                uint32
	interval               uint32
	nodelay                int
	tsProbe, probeWait     uint32
	deadLink               uint32
	fastresend             int32
	fastlimit              int32
	nocwnd, stream         bool

	sndQueue []segment
	rcvQueue []segment
	sndBuf   []segment
	rcvBuf   []segment
	acklist  []ackItem

	buffer []byte
	output func([]byte)

	// Counters.
	xmitTotal, retransTotal, fastTotal, lostTotal uint64
}

// New returns the conversation conv, which passes the packets it sends to
// output, a slice valid only for the call.
func New(conv uint32, output func(packet []byte)) *KCP {
	k := &KCP{
		conv:      conv,
		sndWnd:    DefaultSendWindow,
		rcvWnd:    DefaultRecvWindow,
		rmtWnd:    DefaultRecvWindow,
		mtu:       DefaultMTU,
		mss:       DefaultMTU - Overhead,
		rxRto:     rtoDefault,
		rxMinrto:  rtoMin,
		interval:  DefaultInterval,
		ssthresh:  threshInit,
		fastlimit: 5,
		deadLink:  DefaultDeadLink,
		cwnd:      1,
		output:    output,
		buffer:    make([]byte, 0, DefaultMTU),
	}
	k.incr = k.mss
	return k
}

// Conv returns the conversation ID.
func (k *KCP) Conv() uint32 { return k.conv }

// MSS returns the largest payload of a segment.
func (k *KCP) MSS() int { return int(k.mss) }

// SetMTU sets the largest packet the KCP outputs, its headers included;
// it must leave room for a segment header and a byte.
func (k *KCP) SetMTU(mtu int) bool {
	if mtu <= Overhead || mtu > 1<<16 {
		return false
	}
	k.mtu = uint32(mtu)
	k.mss = k.mtu - Overhead
	k.buffer = make([]byte, 0, mtu)
	if k.incr < k.mss {
		k.incr = k.mss
	}
	return true
}

// NoDelay sets the reference implementation's speed knobs: nodelay 1 or 2
// lowers the minimum RTO to 30 ms and grows a timed-out segment's RTO by
// half rather than doubling it, interval (ms, 10 to 5000; 0 leaves it) is
// the flush interval the RTO is figured with, resend > 0 retransmits a
// segment that many later segments were acknowledged past without waiting
// for its timeout, and nc turns congestion control off, leaving only the
// windows. kcptun's "fast3" is NoDelay(1, 10, 2, true).
func (k *KCP) NoDelay(nodelay, interval, resend int, nc bool) {
	if nodelay >= 0 {
		k.nodelay = nodelay
		if nodelay != 0 {
			k.rxMinrto = rtoNoDelay
		} else {
			k.rxMinrto = rtoMin
		}
	}
	if interval > 0 {
		k.interval = uint32(min(max(interval, 10), 5000))
	}
	if resend >= 0 {
		k.fastresend = int32(resend)
	}
	k.nocwnd = nc
}

// SetWindow sets the send and receive windows, in segments; a value <= 0
// leaves that window. The receive window is at least the largest message,
// 256 segments, in the reference implementation; here it may be smaller,
// and a message that needs more segments than it holds never completes.
func (k *KCP) SetWindow(send, recv int) {
	if send > 0 {
		k.sndWnd = uint32(send)
	}
	if recv > 0 {
		k.rcvWnd = uint32(recv)
	}
}

// SetStreamMode makes Send append to the last segment queued while it has
// room, rather than start a message of its own: the peer's Recv then
// returns whatever segments arrived, not the messages sent.
func (k *KCP) SetStreamMode(on bool) { k.stream = on }

// SetDeadLink sets how many times a segment is sent before Dead reports
// true.
func (k *KCP) SetDeadLink(n int) {
	if n > 0 {
		k.deadLink = uint32(n)
	}
}

// Dead reports whether a segment has been sent DeadLink times without an
// acknowledgement.
func (k *KCP) Dead() bool { return k.dead }

// WaitSend returns how many segments are queued or in flight.
func (k *KCP) WaitSend() int { return len(k.sndBuf) + len(k.sndQueue) }

// SendWindow returns the send window, in segments.
func (k *KCP) SendWindow() int { return int(k.sndWnd) }

// RTO returns the current retransmission timeout, ms.
func (k *KCP) RTO() uint32 { return k.rxRto }

// SRTT returns the smoothed round-trip time, ms.
func (k *KCP) SRTT() int32 { return k.rxSrtt }

// Stats are a KCP's counters.
type Stats struct {
	// Sent is every segment sent, Retransmitted those sent again after a
	// timeout or fast, FastRetransmitted the fast ones. LostEvents counts
	// the flushes that retransmitted after a timeout.
	Sent, Retransmitted, FastRetransmitted, LostEvents uint64
	// FECRecovered counts the packets a Session's forward error
	// correction recovered; a bare KCP leaves it zero.
	FECRecovered uint64
}

// Stats returns the counters.
func (k *KCP) Stats() Stats {
	return Stats{Sent: k.xmitTotal, Retransmitted: k.retransTotal, FastRetransmitted: k.fastTotal, LostEvents: k.lostTotal}
}

// PeekSize returns the size of the next complete message, or -1 when
// there is none.
func (k *KCP) PeekSize() int {
	if len(k.rcvQueue) == 0 {
		return -1
	}
	seg := &k.rcvQueue[0]
	if seg.frg == 0 {
		return len(seg.data)
	}
	if len(k.rcvQueue) < int(seg.frg)+1 {
		return -1
	}
	n := 0
	for i := range k.rcvQueue {
		n += len(k.rcvQueue[i].data)
		if k.rcvQueue[i].frg == 0 {
			break
		}
	}
	return n
}

// Recv copies the next complete message into b, which must hold it.
func (k *KCP) Recv(b []byte) (int, error) {
	size := k.PeekSize()
	if size < 0 {
		return 0, ErrNoMessage
	}
	if size > len(b) {
		return 0, ErrShortBuffer
	}
	reopen := uint32(len(k.rcvQueue)) >= k.rcvWnd
	n, count := 0, 0
	for i := range k.rcvQueue {
		seg := &k.rcvQueue[i]
		n += copy(b[n:], seg.data)
		count++
		frg := seg.frg
		seg.data = nil
		if frg == 0 {
			break
		}
	}
	k.rcvQueue = removeFront(k.rcvQueue, count)
	k.moveRecv()
	if reopen && uint32(len(k.rcvQueue)) < k.rcvWnd {
		// The window had closed: tell the peer it is open again.
		k.probe |= askTell
	}
	return n, nil
}

func removeFront(q []segment, n int) []segment {
	if n == 0 {
		return q
	}
	m := copy(q, q[n:])
	clear(q[m:])
	return q[:m]
}

// moveRecv moves the segments that follow on from rcvBuf to rcvQueue.
func (k *KCP) moveRecv() {
	n := 0
	for n < len(k.rcvBuf) {
		seg := &k.rcvBuf[n]
		if seg.sn != k.rcvNxt || uint32(len(k.rcvQueue)) >= k.rcvWnd {
			break
		}
		k.rcvQueue = append(k.rcvQueue, *seg)
		k.rcvNxt++
		n++
	}
	k.rcvBuf = removeFront(k.rcvBuf, n)
}

// Send queues a message, split into segments of MSS bytes: at most 255 of
// them, unless in stream mode, where any size goes. The KCP keeps b.
func (k *KCP) Send(b []byte) error {
	if len(b) == 0 {
		return ErrEmpty
	}
	mss := int(k.mss)
	if k.stream {
		if n := len(k.sndQueue); n > 0 {
			last := &k.sndQueue[n-1]
			if room := mss - len(last.data); room > 0 {
				take := min(room, len(b))
				last.data = append(last.data, b[:take]...)
				last.frg = 0
				b = b[take:]
			}
		}
		if len(b) == 0 {
			return nil
		}
	}
	count := (len(b) + mss - 1) / mss
	if !k.stream && count >= maxFragments {
		return ErrMessageTooBig
	}
	for i := 0; i < count; i++ {
		size := min(len(b), mss)
		seg := segment{data: make([]byte, size)}
		copy(seg.data, b[:size])
		if !k.stream {
			seg.frg = uint8(count - i - 1)
		}
		k.sndQueue = append(k.sndQueue, seg)
		b = b[size:]
	}
	return nil
}

func (k *KCP) updateAck(rtt int32) {
	if k.rxSrtt == 0 {
		k.rxSrtt = rtt
		k.rxRttvar = rtt / 2
	} else {
		delta := rtt - k.rxSrtt
		if delta < 0 {
			delta = -delta
		}
		k.rxRttvar = (3*k.rxRttvar + delta) / 4
		k.rxSrtt = (7*k.rxSrtt + rtt) / 8
		if k.rxSrtt < 1 {
			k.rxSrtt = 1
		}
	}
	rto := uint32(k.rxSrtt) + max(k.interval, 4*uint32(k.rxRttvar))
	k.rxRto = min(max(k.rxMinrto, rto), rtoMax)
}

// shrinkBuf drops the acknowledged segments at the front of sndBuf and
// moves sndUna past them.
func (k *KCP) shrinkBuf() {
	n := 0
	for n < len(k.sndBuf) && k.sndBuf[n].acked {
		n++
	}
	k.sndBuf = removeFront(k.sndBuf, n)
	if len(k.sndBuf) > 0 {
		k.sndUna = k.sndBuf[0].sn
	} else {
		k.sndUna = k.sndNxt
	}
}

func (k *KCP) parseAck(sn uint32) {
	if timediff(sn, k.sndUna) < 0 || timediff(sn, k.sndNxt) >= 0 || len(k.sndBuf) == 0 {
		return
	}
	// sndBuf holds sndUna.. in order, without gaps.
	if i := int(sn - k.sndBuf[0].sn); i < len(k.sndBuf) && k.sndBuf[i].sn == sn {
		seg := &k.sndBuf[i]
		seg.acked = true
		seg.data = nil
	}
}

func (k *KCP) parseUna(una uint32) {
	n := 0
	for n < len(k.sndBuf) && timediff(una, k.sndBuf[n].sn) > 0 {
		n++
	}
	k.sndBuf = removeFront(k.sndBuf, n)
}

func (k *KCP) parseFastack(sn, ts uint32) {
	if timediff(sn, k.sndUna) < 0 || timediff(sn, k.sndNxt) >= 0 {
		return
	}
	for i := range k.sndBuf {
		seg := &k.sndBuf[i]
		if timediff(sn, seg.sn) < 0 {
			break
		}
		if sn != seg.sn && !seg.acked && timediff(ts, seg.ts) >= 0 {
			seg.fastack++
		}
	}
}

// parseData puts a received segment in rcvBuf, in order, unless it is
// outside the window or there already.
func (k *KCP) parseData(seg segment) {
	sn := seg.sn
	if timediff(sn, k.rcvNxt+k.rcvWnd) >= 0 || timediff(sn, k.rcvNxt) < 0 {
		return
	}
	i := len(k.rcvBuf)
	for i > 0 {
		prev := k.rcvBuf[i-1].sn
		if prev == sn {
			return
		}
		if timediff(sn, prev) > 0 {
			break
		}
		i--
	}
	seg.data = append([]byte(nil), seg.data...)
	k.rcvBuf = append(k.rcvBuf, segment{})
	copy(k.rcvBuf[i+1:], k.rcvBuf[i:])
	k.rcvBuf[i] = seg
	k.moveRecv()
}

// Input reads a packet of the peer's, at time now: one or more segments.
// The data of the segments it keeps is copied.
func (k *KCP) Input(packet []byte, now uint32) error {
	k.current = now
	prevUna := k.sndUna
	var maxack, latestTS uint32
	ackSeen := false
	if len(packet) < Overhead {
		return ErrMalformed
	}
	for len(packet) >= Overhead {
		conv := binary.LittleEndian.Uint32(packet)
		if conv != k.conv {
			return ErrConv
		}
		cmd := packet[4]
		frg := packet[5]
		wnd := binary.LittleEndian.Uint16(packet[6:])
		ts := binary.LittleEndian.Uint32(packet[8:])
		sn := binary.LittleEndian.Uint32(packet[12:])
		una := binary.LittleEndian.Uint32(packet[16:])
		length := binary.LittleEndian.Uint32(packet[20:])
		packet = packet[Overhead:]
		if uint64(length) > uint64(len(packet)) {
			return ErrMalformed
		}
		if cmd < cmdPush || cmd > cmdWins {
			return ErrMalformed
		}
		k.rmtWnd = uint32(wnd)
		k.parseUna(una)
		k.shrinkBuf()
		switch cmd {
		case cmdAck:
			if rtt := timediff(k.current, ts); rtt >= 0 {
				k.updateAck(rtt)
			}
			k.parseAck(sn)
			k.shrinkBuf()
			if !ackSeen {
				ackSeen = true
				maxack, latestTS = sn, ts
			} else if timediff(sn, maxack) > 0 && timediff(ts, latestTS) > 0 {
				maxack, latestTS = sn, ts
			}
		case cmdPush:
			if timediff(sn, k.rcvNxt+k.rcvWnd) < 0 {
				k.acklist = append(k.acklist, ackItem{sn, ts})
				if timediff(sn, k.rcvNxt) >= 0 {
					k.parseData(segment{cmd: cmd, frg: frg, wnd: wnd, ts: ts, sn: sn, una: una, data: packet[:length]})
				}
			}
		case cmdWask:
			k.probe |= askTell
		case cmdWins:
		}
		packet = packet[length:]
	}
	if ackSeen {
		k.parseFastack(maxack, latestTS)
	}
	if timediff(k.sndUna, prevUna) > 0 && k.cwnd < k.rmtWnd {
		mss := k.mss
		if k.cwnd < k.ssthresh {
			k.cwnd++
			k.incr += mss
		} else {
			k.incr = max(k.incr, mss)
			k.incr += mss*mss/k.incr + mss/16
			if (k.cwnd+1)*mss <= k.incr {
				k.cwnd = (k.incr + mss - 1) / mss
			}
		}
		if k.cwnd > k.rmtWnd {
			k.cwnd = k.rmtWnd
			k.incr = k.rmtWnd * mss
		}
	}
	return nil
}

func (k *KCP) wndUnused() uint16 {
	if n := uint32(len(k.rcvQueue)); n < k.rcvWnd {
		return uint16(min(k.rcvWnd-n, 0xffff))
	}
	return 0
}

func appendHeader(b []byte, conv uint32, cmd, frg uint8, wnd uint16, ts, sn, una uint32, length int) []byte {
	b = binary.LittleEndian.AppendUint32(b, conv)
	b = append(b, cmd, frg)
	b = binary.LittleEndian.AppendUint16(b, wnd)
	b = binary.LittleEndian.AppendUint32(b, ts)
	b = binary.LittleEndian.AppendUint32(b, sn)
	b = binary.LittleEndian.AppendUint32(b, una)
	return binary.LittleEndian.AppendUint32(b, uint32(length))
}

// room makes room for size bytes in the packet being built, outputting it
// when full.
func (k *KCP) room(size int) {
	if len(k.buffer)+size > int(k.mtu) && len(k.buffer) > 0 {
		k.output(k.buffer)
		k.buffer = k.buffer[:0]
	}
}

// Flush sends, at time now, what is due: acknowledgements, window probes
// and answers, new segments the windows allow, and retransmissions whose
// time has come or that later acknowledgements call for.
func (k *KCP) Flush(now uint32) {
	k.current = now
	wnd := k.wndUnused()
	una := k.rcvNxt
	for _, a := range k.acklist {
		k.room(Overhead)
		k.buffer = appendHeader(k.buffer, k.conv, cmdAck, 0, wnd, a.ts, a.sn, una, 0)
	}
	k.acklist = k.acklist[:0]

	if k.rmtWnd == 0 {
		if k.probeWait == 0 {
			k.probeWait = probeInit
			k.tsProbe = now + k.probeWait
		} else if timediff(now, k.tsProbe) >= 0 {
			k.probeWait = min(max(k.probeWait, probeInit)+k.probeWait/2, probeLimit)
			k.tsProbe = now + k.probeWait
			k.probe |= askSend
		}
	} else {
		k.tsProbe, k.probeWait = 0, 0
	}
	if k.probe&askSend != 0 {
		k.room(Overhead)
		k.buffer = appendHeader(k.buffer, k.conv, cmdWask, 0, wnd, now, 0, una, 0)
	}
	if k.probe&askTell != 0 {
		k.room(Overhead)
		k.buffer = appendHeader(k.buffer, k.conv, cmdWins, 0, wnd, now, 0, una, 0)
	}
	k.probe = 0

	cwnd := k.cwndNow()
	n := 0
	for ; n < len(k.sndQueue) && timediff(k.sndNxt, k.sndUna+cwnd) < 0; n++ {
		seg := k.sndQueue[n]
		seg.cmd = cmdPush
		seg.sn = k.sndNxt
		k.sndNxt++
		k.sndBuf = append(k.sndBuf, seg)
	}
	k.sndQueue = removeFront(k.sndQueue, n)

	resent := uint32(0xffffffff)
	if k.fastresend > 0 {
		resent = uint32(k.fastresend)
	}
	rtomin := uint32(0)
	if k.nodelay == 0 {
		rtomin = k.rxRto >> 3
	}
	change, lost := false, false
	for i := range k.sndBuf {
		seg := &k.sndBuf[i]
		if seg.acked {
			continue
		}
		send := false
		switch {
		case seg.xmit == 0:
			send = true
			seg.rto = k.rxRto
			seg.resendts = now + seg.rto + rtomin
		case timediff(now, seg.resendts) >= 0:
			send = true
			if k.nodelay == 0 {
				seg.rto += max(seg.rto, k.rxRto)
			} else {
				step := seg.rto
				if k.nodelay >= 2 {
					step = k.rxRto
				}
				seg.rto += step / 2
			}
			seg.resendts = now + seg.rto
			lost = true
			k.retransTotal++
		case seg.fastack >= resent && (k.fastlimit <= 0 || seg.xmit <= uint32(k.fastlimit)):
			send = true
			seg.fastack = 0
			seg.resendts = now + seg.rto
			change = true
			k.retransTotal++
			k.fastTotal++
		}
		if !send {
			continue
		}
		seg.xmit++
		k.xmitTotal++
		seg.ts = now
		seg.wnd = wnd
		seg.una = una
		k.room(Overhead + len(seg.data))
		k.buffer = appendHeader(k.buffer, k.conv, cmdPush, seg.frg, wnd, now, seg.sn, una, len(seg.data))
		k.buffer = append(k.buffer, seg.data...)
		if seg.xmit >= k.deadLink {
			k.dead = true
		}
	}
	if len(k.buffer) > 0 {
		k.output(k.buffer)
		k.buffer = k.buffer[:0]
	}

	if change {
		inflight := k.sndNxt - k.sndUna
		k.ssthresh = max(inflight/2, threshMin)
		k.cwnd = k.ssthresh + resent
		k.incr = k.cwnd * k.mss
	}
	if lost {
		k.lostTotal++
		k.ssthresh = max(cwnd/2, threshMin)
		k.cwnd = 1
		k.incr = k.mss
	}
	if k.cwnd < 1 {
		k.cwnd = 1
		k.incr = k.mss
	}
}

func (k *KCP) cwndNow() uint32 {
	cwnd := min(k.sndWnd, k.rmtWnd)
	if !k.nocwnd {
		cwnd = min(k.cwnd, cwnd)
	}
	return cwnd
}

// NextFlush returns when Flush has something to send, which may be now or
// before, and false when nothing is due at any time: nothing queued that
// the windows let go, nothing in flight, no acknowledgement or probe to
// send.
func (k *KCP) NextFlush(now uint32) (uint32, bool) {
	if len(k.acklist) > 0 || k.probe != 0 {
		return now, true
	}
	if len(k.sndQueue) > 0 && timediff(k.sndNxt, k.sndUna+k.cwndNow()) < 0 {
		return now, true
	}
	at, ok := uint32(0), false
	earlier := func(t uint32) {
		if !ok || timediff(t, at) < 0 {
			at, ok = t, true
		}
	}
	resent := uint32(0xffffffff)
	if k.fastresend > 0 {
		resent = uint32(k.fastresend)
	}
	for i := range k.sndBuf {
		seg := &k.sndBuf[i]
		if seg.acked {
			continue
		}
		if seg.xmit == 0 || (seg.fastack >= resent && (k.fastlimit <= 0 || seg.xmit <= uint32(k.fastlimit))) {
			return now, true
		}
		earlier(seg.resendts)
	}
	if k.rmtWnd == 0 && (len(k.sndQueue) > 0 || len(k.sndBuf) > 0) {
		if k.probeWait == 0 {
			return now, true // starts the probe timer
		}
		earlier(k.tsProbe)
	}
	return at, ok
}
