package srt

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"time"
)

// Defaults of Config.
const (
	DefaultLatency           = 120 * time.Millisecond
	DefaultHandshakeTimeout  = 3 * time.Second
	DefaultPeerIdleTimeout   = 5 * time.Second
	DefaultFlowWindow        = 8192
	DefaultSendBufferPackets = 8192
	// MaxPayloadSize is the largest payload of a data packet: an MTU of
	// 1500 bytes less the IPv4, UDP and SRT headers.
	MaxPayloadSize = 1456
	// TSPayloadSize is the payload SRT carries MPEG-TS in: 7 transport
	// packets, as ffmpeg and every encoder send.
	TSPayloadSize = 1316
)

const (
	mtu               = 1500
	handshakeRetry    = 250 * time.Millisecond
	keepaliveInterval = time.Second
	ackInterval       = 10 * time.Millisecond
	lightACKPackets   = 64
	minNAKInterval    = 20 * time.Millisecond
	maxNAKRanges      = 128
	ackHistory        = 64
	// sndDropMin is the least age at which the sender gives up on a packet
	// the receiver has not acknowledged, as libsrt's
	// SRT_TLPKTDROP_MINTHRESHOLD_MS.
	sndDropMin = time.Second
	// driftWindow is how many packets the receiver watches before it lets
	// its time base follow a sender whose clock runs slow, by at most
	// driftStep.
	driftWindow = 1000
	driftStep   = time.Millisecond
)

// State is the state of a Conn.
type State int

const (
	StateHandshake State = iota
	StateConnected
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateHandshake:
		return "handshake"
	case StateConnected:
		return "connected"
	}
	return "closed"
}

var (
	// ErrClosed is what Write reports on a closed Conn.
	ErrClosed = errors.New("srt: connection closed")
	// ErrNotConnected is what Write reports before the handshake ends.
	ErrNotConnected = errors.New("srt: not connected")
	// ErrPeerClosed is what OnClose hears when the peer sends SHUTDOWN.
	ErrPeerClosed = errors.New("srt: peer shut down")
	// ErrPeerIdle is what OnClose hears when the peer sends nothing for
	// PeerIdleTimeout.
	ErrPeerIdle = errors.New("srt: peer idle timeout")
	// ErrHandshakeTimeout is what OnClose hears when a caller's handshake
	// does not finish within HandshakeTimeout.
	ErrHandshakeTimeout = errors.New("srt: handshake timeout")
	// ErrTooLarge is what Write reports for a payload over the limit.
	ErrTooLarge = errors.New("srt: payload larger than the packet size")
	// ErrBufferFull is what Write reports when the send buffer holds
	// SendBufferPackets packets: the payload is dropped.
	ErrBufferFull = errors.New("srt: send buffer full")
)

// RejectReason is why a listener refused a caller (the SRT_REJ_ codes),
// or, from RejectBadRequest on, why the application did (the SRT_REJX_
// codes of the access control guidelines, modelled on HTTP's).
type RejectReason uint32

const (
	RejectUnknown    RejectReason = 0
	RejectSystem     RejectReason = 1
	RejectPeer       RejectReason = 2
	RejectResource   RejectReason = 3
	RejectRogue      RejectReason = 4
	RejectBacklog    RejectReason = 5
	RejectIPE        RejectReason = 6
	RejectClose      RejectReason = 7
	RejectVersion    RejectReason = 8
	RejectRdvCookie  RejectReason = 9
	RejectBadSecret  RejectReason = 10
	RejectUnsecure   RejectReason = 11
	RejectMessageAPI RejectReason = 12
	RejectCongestion RejectReason = 13
	RejectFilter     RejectReason = 14
	RejectGroup      RejectReason = 15
	RejectTimeout    RejectReason = 16

	RejectBadRequest   RejectReason = 1400
	RejectUnauthorized RejectReason = 1401
	RejectOverload     RejectReason = 1402
	RejectForbidden    RejectReason = 1403
	RejectNotFound     RejectReason = 1404
	RejectBadMode      RejectReason = 1405
	RejectUnacceptable RejectReason = 1406
	RejectConflict     RejectReason = 1409
)

// Error makes a RejectReason an error, which is how OnClose reports a
// caller's rejection and how an Accept hook asks for one.
func (r RejectReason) Error() string {
	return "srt: connection rejected (" + strconv.Itoa(int(r)) + ")"
}

// Config configures a Conn. Output is required; the other callbacks may be
// nil. Every callback is called from the method that causes it, on the
// caller's goroutine, and must not call back into the Conn.
type Config struct {
	// Latency is the TSBPD latency this side asks for, both ways; the
	// larger of the two sides' wins. DefaultLatency when zero.
	Latency time.Duration
	// Passphrase, when set, encrypts the connection: 10 to 79 bytes, from
	// which PBKDF2 derives the key that wraps the stream key. PBKeyLen is
	// the stream key's length, 16, 24 or 32 bytes; 16 when zero. A
	// listener takes the caller's key length.
	Passphrase string
	PBKeyLen   int
	// StreamID is the stream ID a caller sends; a listener ignores it.
	StreamID string
	// PayloadSize bounds what Write takes; MaxPayloadSize when zero.
	PayloadSize int
	// FlowWindow is the receive buffer, in packets, which also bounds the
	// packets the peer may have in flight; DefaultFlowWindow when zero.
	FlowWindow int
	// SendBufferPackets bounds the packets sent and not yet acknowledged
	// plus those waiting for the peer's flow window;
	// DefaultSendBufferPackets when zero.
	SendBufferPackets int
	// HandshakeTimeout and PeerIdleTimeout default to
	// DefaultHandshakeTimeout and DefaultPeerIdleTimeout.
	HandshakeTimeout time.Duration
	PeerIdleTimeout  time.Duration
	// Rand is the source of socket IDs, sequence numbers and keys;
	// crypto/rand when nil.
	Rand io.Reader

	// Output sends one datagram to the peer. The slice is only valid for
	// the call.
	Output func(b []byte)
	// OnConnect hears that the handshake is done.
	OnConnect func()
	// OnData hears every payload in order, at its TSBPD time; payloads
	// that came too late were dropped. The slice is only valid for the
	// call.
	OnData func(payload []byte)
	// OnClose hears that the Conn closed: nil after Close, otherwise why.
	OnClose func(err error)
}

func (cfg *Config) setDefaults() error {
	if cfg.Latency <= 0 {
		cfg.Latency = DefaultLatency
	}
	if cfg.Passphrase != "" {
		if len(cfg.Passphrase) < 10 || len(cfg.Passphrase) > 79 {
			return errPassphrase
		}
		if cfg.PBKeyLen == 0 {
			cfg.PBKeyLen = 16
		}
		if cfg.PBKeyLen != 16 && cfg.PBKeyLen != 24 && cfg.PBKeyLen != 32 {
			return errKeyLen
		}
	}
	if len(cfg.StreamID) > maxStreamID {
		return errLongStreamID
	}
	if cfg.PayloadSize <= 0 || cfg.PayloadSize > MaxPayloadSize {
		cfg.PayloadSize = MaxPayloadSize
	}
	if cfg.FlowWindow <= 0 {
		cfg.FlowWindow = DefaultFlowWindow
	}
	if cfg.SendBufferPackets <= 0 {
		cfg.SendBufferPackets = DefaultSendBufferPackets
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if cfg.PeerIdleTimeout <= 0 {
		cfg.PeerIdleTimeout = DefaultPeerIdleTimeout
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Reader
	}
	if cfg.Output == nil {
		cfg.Output = func([]byte) {}
	}
	return nil
}

// Stats counts what a Conn has done.
type Stats struct {
	PacketsSent       uint64
	BytesSent         uint64
	PacketsRetrans    uint64
	PacketsSendDrop   uint64 // given up on by the sender, too late
	PacketsReceived   uint64
	BytesReceived     uint64
	PacketsLost       uint64 // found missing by the receiver
	PacketsRecvDrop   uint64 // skipped by the receiver, too late
	PacketsDuplicate  uint64
	PacketsUndecrypt  uint64
	NAKsSent          uint64
	NAKsReceived      uint64
	RTT               time.Duration
	SendBufferPackets int
	SendBufferBytes   int
}

type sndPacket struct {
	origin time.Time
	buf    []byte // the whole datagram
	queued bool   // in the retransmission queue
}

type rcvPacket struct {
	ts      int64 // the unwrapped timestamp, microseconds
	payload []byte
}

type lossEntry struct {
	lossRange
	lastNAK time.Time
}

type ackRecord struct {
	no   uint32
	seq  uint32
	sent time.Time
}

// Conn is one SRT connection in live mode, either side of it, without any
// I/O of its own: the caller feeds it datagrams (Handle) and time
// (HandleTimeout, at NextTimeout), and it sends through Config.Output.
// Its methods must not be called concurrently.
type Conn struct {
	cfg    Config
	state  State
	caller bool
	start  time.Time
	err    error

	socketID uint32
	peerID   uint32
	streamID string

	rcvLatency     time.Duration
	sndLatency     time.Duration
	peerFlowWindow int
	// peerAvail is the receive buffer room beyond the acknowledged
	// sequence number the peer's last full ACK reported.
	peerAvail int

	km    *keyMaterial
	kmMsg []byte
	crypt *cryptor
	kk    int

	// The caller's handshake.
	isn     uint32
	cookie  uint32
	hsStart time.Time
	hsNext  time.Time
	// hsResp is a listener's conclusion response, sent again when the
	// caller repeats its conclusion.
	hsResp []byte

	// Sending.
	snd      []*sndPacket
	sndBase  uint32 // the sequence number of snd[0]
	sndNext  uint32
	sndSent  int // snd[:sndSent] went out at least once
	sndBytes int
	msgno    uint32
	rexmit   []uint32
	lastSend time.Time

	// Receiving.
	rcv         []*rcvPacket // a ring of FlowWindow slots
	rcvHead     int          // the slot of rcvBase
	rcvBase     uint32       // the next sequence number to deliver
	rcvMax      uint32       // the next sequence number never seen
	rcvCount    int
	loss        []lossEntry
	base        time.Time // local time of the peer's timestamp 0, plus the least delay
	haveBase    bool
	tsLast      uint32
	tsFull      int64
	haveTS      bool
	driftN      int
	driftMin    time.Time
	nextDeliver time.Time
	lastRecv    time.Time
	nextNAK     time.Time
	nextACK     time.Time
	// ackAcked is the sequence number of the last ACK the peer confirmed
	// with an ACKACK: until it catches up with ackSeq, a full ACK goes out
	// every ackInterval, since an ACK may be lost.
	ackAcked    uint32
	availSent   int // the buffer room the last ACK reported
	ackNo       uint32
	acks        [ackHistory]ackRecord
	sinceACK    int
	rateCount   int
	rateBytes   int
	rateSince   time.Time
	rtt, rttVar time.Duration

	stats Stats
	buf   []byte
}

func newConn(cfg Config, now time.Time) *Conn {
	return &Conn{
		cfg:            cfg,
		start:          now,
		lastRecv:       now,
		lastSend:       now,
		rtt:            100 * time.Millisecond,
		rttVar:         50 * time.Millisecond,
		peerFlowWindow: cfg.FlowWindow,
		peerAvail:      cfg.FlowWindow,
		rcv:            make([]*rcvPacket, cfg.FlowWindow),
		msgno:          0,
	}
}

// NewCaller starts the handshake of a caller: the induction request goes
// out through Output before NewCaller returns.
func NewCaller(cfg Config, now time.Time) (*Conn, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	c := newConn(cfg, now)
	c.caller = true
	var r [8]byte
	if _, err := io.ReadFull(cfg.Rand, r[:]); err != nil {
		return nil, err
	}
	c.socketID = socketIDOf(binary.BigEndian.Uint32(r[:]))
	c.isn = binary.BigEndian.Uint32(r[4:]) & (maxSeq - 1)
	c.sndBase, c.sndNext = c.isn, c.isn
	c.streamID = cfg.StreamID
	if cfg.Passphrase != "" {
		km, err := newKeyMaterial(cfg.Rand, cfg.PBKeyLen)
		if err != nil {
			return nil, err
		}
		if c.kmMsg, err = km.marshal(cfg.Passphrase); err != nil {
			return nil, err
		}
		if c.crypt, err = newCryptor(km); err != nil {
			return nil, err
		}
		c.km, c.kk = km, kkEven
	}
	c.hsStart = now
	c.sendInduction(now)
	return c, nil
}

// socketIDOf makes a socket ID of random bits: nonzero, and below 2^30 as
// libsrt's are.
func socketIDOf(r uint32) uint32 {
	if id := r & (1<<30 - 1); id != 0 {
		return id
	}
	return 1
}

// State returns the state of c.
func (c *Conn) State() State { return c.state }

// Err returns why c closed, nil while it is open or after Close.
func (c *Conn) Err() error { return c.err }

// StreamID returns the stream ID: the caller's own, or, on a listener's
// Conn, the one the caller sent.
func (c *Conn) StreamID() string { return c.streamID }

// SocketID returns c's own SRT socket ID, and PeerSocketID the peer's.
func (c *Conn) SocketID() uint32     { return c.socketID }
func (c *Conn) PeerSocketID() uint32 { return c.peerID }

// Latency returns the TSBPD latency of what c receives, as negotiated.
func (c *Conn) Latency() time.Duration { return c.rcvLatency }

// Encrypted reports whether c encrypts its payloads.
func (c *Conn) Encrypted() bool { return c.crypt != nil }

// SendBacklog returns the bytes of payload c holds that the peer has not
// acknowledged, for a sender's slow-consumer policy.
func (c *Conn) SendBacklog() int { return c.sndBytes }

// Stats returns c's counters.
func (c *Conn) Stats() Stats {
	s := c.stats
	s.RTT = c.rtt
	s.SendBufferPackets = len(c.snd)
	s.SendBufferBytes = c.sndBytes
	return s
}

func (c *Conn) timestamp(now time.Time) uint32 {
	return uint32(now.Sub(c.start) / time.Microsecond)
}

func (c *Conn) output(b []byte, now time.Time) {
	c.lastSend = now
	c.cfg.Output(b)
}

// sendControl sends a control packet with cif as its body.
func (c *Conn) sendControl(now time.Time, typ uint16, subtype uint16, info uint32, cif []byte) {
	h := header{ctrl: true, typ: typ, subtype: subtype, info: info, ts: c.timestamp(now), dst: c.peerID}
	c.buf = append(h.append(c.buf[:0]), cif...)
	c.output(c.buf, now)
}

// sendHandshake sends hs to dst.
func (c *Conn) sendHandshake(now time.Time, dst uint32, hs *handshake) {
	h := header{ctrl: true, typ: ctrlHandshake, ts: c.timestamp(now), dst: dst}
	c.buf = hs.append(h.append(c.buf[:0]))
	c.output(c.buf, now)
}

func (c *Conn) sendInduction(now time.Time) {
	c.hsNext = now.Add(handshakeRetry)
	c.sendHandshake(now, 0, &handshake{
		version: 4, extField: 2, isn: c.isn, mtu: mtu, flowWindow: uint32(c.cfg.FlowWindow),
		typ: hsInduction, socketID: c.socketID,
	})
}

func (c *Conn) srtFlags() uint32 {
	return flagTSBPDSnd | flagTSBPDRcv | flagCrypt | flagTLPktDrop | flagPeriodicNAK | flagRexmit
}

func (c *Conn) sendConclusion(now time.Time) {
	c.hsNext = now.Add(handshakeRetry)
	lat := uint16(c.cfg.Latency / time.Millisecond)
	hs := &handshake{
		version: 5, extField: extFlagHSReq, isn: c.isn, mtu: mtu, flowWindow: uint32(c.cfg.FlowWindow),
		typ: hsConclusion, socketID: c.socketID, cookie: c.cookie,
		hsReq: &hsExt{version: srtVersion, flags: c.srtFlags(), rcvDelay: lat, sndDelay: lat},
	}
	if c.kmMsg != nil {
		hs.encryption = uint16(c.cfg.PBKeyLen / 8)
		hs.extField |= extFlagKMReq
		hs.km = c.kmMsg
	}
	if c.streamID != "" {
		hs.extField |= extFlagConfig
		hs.streamID, hs.hasSID = c.streamID, true
	}
	c.sendHandshake(now, 0, hs)
}

// Close shuts c down, telling the peer, and calls OnClose(nil). It does
// nothing on a closed Conn.
func (c *Conn) Close(now time.Time) {
	if c.state == StateConnected {
		c.sendControl(now, ctrlShutdown, 0, 0, make([]byte, 4))
	}
	c.closeWith(nil)
}

func (c *Conn) closeWith(err error) {
	if c.state == StateClosed {
		return
	}
	c.state = StateClosed
	c.err = err
	c.snd, c.rexmit, c.loss, c.sndBytes = nil, nil, nil, 0
	clear(c.rcv)
	c.rcvCount = 0
	if c.cfg.OnClose != nil {
		c.cfg.OnClose(err)
	}
}

func (c *Conn) connected(now time.Time, peerTS uint32) {
	c.state = StateConnected
	c.lastRecv = now
	c.base = now.Add(-time.Duration(peerTS) * time.Microsecond)
	c.haveBase = true
	c.tsLast, c.tsFull, c.haveTS = peerTS, int64(peerTS), true
	c.nextACK = now.Add(ackInterval)
	c.rateSince = now
	if c.cfg.OnConnect != nil {
		c.cfg.OnConnect()
	}
}

// NextTimeout returns when HandleTimeout is next due, false once c has
// closed.
func (c *Conn) NextTimeout() (time.Time, bool) {
	if c.state == StateClosed {
		return time.Time{}, false
	}
	if c.state == StateHandshake {
		return earliest(c.hsNext, c.hsStart.Add(c.cfg.HandshakeTimeout)), true
	}
	t := earliest(c.lastSend.Add(keepaliveInterval), c.lastRecv.Add(c.cfg.PeerIdleTimeout))
	if c.ackDue() {
		t = earliest(t, c.nextACK)
	}
	if len(c.loss) > 0 {
		t = earliest(t, c.nextNAK)
	}
	if !c.nextDeliver.IsZero() {
		t = earliest(t, c.nextDeliver)
	}
	if len(c.snd) > 0 {
		t = earliest(t, c.snd[0].origin.Add(c.sndDropAge()))
	}
	return t, true
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// HandleTimeout does what is due at now: retransmitting the handshake,
// acknowledging, reporting losses again, delivering, dropping what is too
// late to send, keeping the connection alive, and timing out a silent peer.
func (c *Conn) HandleTimeout(now time.Time) {
	switch c.state {
	case StateClosed:
		return
	case StateHandshake:
		if !now.Before(c.hsStart.Add(c.cfg.HandshakeTimeout)) {
			c.closeWith(ErrHandshakeTimeout)
			return
		}
		if !now.Before(c.hsNext) {
			if c.cookie == 0 {
				c.sendInduction(now)
			} else {
				c.sendConclusion(now)
			}
		}
		return
	}
	if !now.Before(c.lastRecv.Add(c.cfg.PeerIdleTimeout)) {
		c.closeWith(ErrPeerIdle)
		return
	}
	c.deliver(now)
	if c.state == StateClosed {
		return
	}
	if !now.Before(c.nextACK) {
		if c.ackDue() {
			c.sendACK(now)
		}
		c.nextACK = now.Add(ackInterval)
	}
	if len(c.loss) > 0 && !now.Before(c.nextNAK) {
		c.sendPeriodicNAK(now)
	}
	c.dropTooLate(now)
	if !now.Before(c.lastSend.Add(keepaliveInterval)) {
		c.sendControl(now, ctrlKeepalive, 0, 0, make([]byte, 4))
	}
}

// Handle reads one datagram from the peer.
func (c *Conn) Handle(b []byte, now time.Time) {
	if c.state == StateClosed {
		return
	}
	var h header
	if h.unmarshal(b) != nil {
		return
	}
	if c.state == StateHandshake {
		if h.ctrl && h.typ == ctrlHandshake {
			c.handleCallerHandshake(&h, b[headerSize:], now)
		}
		return
	}
	if h.dst != c.socketID && !(h.ctrl && h.typ == ctrlHandshake) {
		return
	}
	c.lastRecv = now
	if !h.ctrl {
		c.handleData(&h, b[headerSize:], now)
		c.deliver(now)
		return
	}
	cif := b[headerSize:]
	switch h.typ {
	case ctrlHandshake:
		var hs handshake
		if c.hsResp != nil && hs.unmarshal(cif) == nil && hs.typ == hsConclusion && hs.socketID == c.peerID {
			c.output(c.hsResp, now)
		}
	case ctrlACK:
		c.handleACK(&h, cif, now)
	case ctrlNAK:
		c.handleNAK(cif, now)
	case ctrlACKACK:
		c.handleACKACK(h.info, now)
	case ctrlShutdown:
		c.closeWith(ErrPeerClosed)
	case ctrlDropReq:
		if len(cif) >= 8 {
			c.handleDropReq(binary.BigEndian.Uint32(cif)&(maxSeq-1), binary.BigEndian.Uint32(cif[4:])&(maxSeq-1))
		}
	case ctrlUser:
		if h.subtype == extKMReq {
			c.handleKMRefresh(cif, now)
		}
	}
	c.deliver(now)
}

// handleCallerHandshake reads the listener's answers to a caller.
func (c *Conn) handleCallerHandshake(h *header, cif []byte, now time.Time) {
	var hs handshake
	if hs.unmarshal(cif) != nil {
		return
	}
	switch {
	case hs.typ >= hsRejectBase && hs.typ < hsDone:
		c.closeWith(RejectReason(hs.typ - hsRejectBase))
	case hs.typ == hsInduction && c.cookie == 0:
		if hs.version < 5 || hs.extField != srtMagic {
			c.closeWith(RejectVersion)
			return
		}
		c.cookie = hs.cookie
		if c.cookie == 0 {
			c.cookie = 1 // a listener that sends no cookie takes any
		}
		c.sendConclusion(now)
	case hs.typ == hsConclusion && c.cookie != 0 && h.dst == c.socketID:
		if hs.hsReq == nil || !hs.hsRsp {
			c.closeWith(RejectVersion)
			return
		}
		if c.kmMsg != nil {
			switch {
			case hs.km == nil:
				c.closeWith(RejectUnsecure)
				return
			case len(hs.km) < kmHeaderSize:
				c.closeWith(RejectBadSecret)
				return
			}
		}
		c.peerID = hs.socketID
		c.rcvLatency = time.Duration(hs.hsReq.sndDelay) * time.Millisecond
		c.sndLatency = time.Duration(hs.hsReq.rcvDelay) * time.Millisecond
		c.rcvLatency = max(c.rcvLatency, c.cfg.Latency)
		c.sndLatency = max(c.sndLatency, c.cfg.Latency)
		if hs.flowWindow > 0 {
			c.peerFlowWindow = int(min(hs.flowWindow, 1<<20))
			c.peerAvail = c.peerFlowWindow
		}
		c.rcvBase, c.rcvMax, c.ackAcked = hs.isn, hs.isn, hs.isn
		c.connected(now, h.ts)
	}
}

// sndDropAge is how old an unacknowledged packet gets before the sender
// gives up on it: the receiver has dropped it by then.
func (c *Conn) sndDropAge() time.Duration {
	return max(sndDropMin, c.sndLatency+c.sndLatency/4+2*ackInterval)
}

// Write sends payload as one message. It fails with ErrTooLarge over
// PayloadSize, ErrBufferFull when the send buffer is full, and
// ErrNotConnected or ErrClosed outside the connected state.
func (c *Conn) Write(payload []byte, now time.Time) error {
	switch {
	case c.state == StateClosed:
		return ErrClosed
	case c.state != StateConnected:
		return ErrNotConnected
	case len(payload) > c.cfg.PayloadSize:
		return ErrTooLarge
	}
	c.dropTooLate(now)
	if len(c.snd) >= c.cfg.SendBufferPackets {
		return ErrBufferFull
	}
	c.msgno = c.msgno%msgnoMask + 1
	h := header{seq: c.sndNext, info: ppSolo | uint32(c.kk)<<kkShift | c.msgno, ts: c.timestamp(now), dst: c.peerID}
	buf := make([]byte, 0, headerSize+len(payload))
	buf = append(h.append(buf), payload...)
	if c.crypt != nil {
		c.crypt.xor(c.kk, c.sndNext, buf[headerSize:])
	}
	c.sndNext = seqAdd(c.sndNext, 1)
	c.snd = append(c.snd, &sndPacket{origin: now, buf: buf})
	c.sndBytes += len(payload)
	c.trySend(now)
	return nil
}

// trySend sends the retransmissions due, then new packets as far as the
// peer's flow window lets.
func (c *Conn) trySend(now time.Time) {
	for len(c.rexmit) > 0 {
		seq := c.rexmit[0]
		c.rexmit = c.rexmit[1:]
		i := seqDiff(seq, c.sndBase)
		if i < 0 || i >= c.sndSent {
			continue
		}
		p := c.snd[i]
		p.queued = false
		w := binary.BigEndian.Uint32(p.buf[4:])
		binary.BigEndian.PutUint32(p.buf[4:], w|flagRexmitPkt)
		c.stats.PacketsRetrans++
		c.output(p.buf, now)
	}
	if len(c.rexmit) == 0 {
		c.rexmit = c.rexmit[:0:0]
	}
	for c.sndSent < len(c.snd) && c.sndSent < min(c.peerFlowWindow, c.peerAvail) {
		p := c.snd[c.sndSent]
		c.sndSent++
		c.stats.PacketsSent++
		c.stats.BytesSent += uint64(len(p.buf) - headerSize)
		c.output(p.buf, now)
	}
}

// popSent forgets the first n packets of the send buffer.
func (c *Conn) popSent(n int) {
	for _, p := range c.snd[:n] {
		c.sndBytes -= len(p.buf) - headerSize
	}
	clear(c.snd[:n])
	c.snd = c.snd[n:]
	c.sndSent = max(c.sndSent-n, 0)
	c.sndBase = seqAdd(c.sndBase, n)
	if len(c.snd) == 0 {
		c.snd = nil
	}
}

// dropTooLate gives up on the packets at the head of the send buffer that
// are too old for the receiver to use, and tells it with a DROPREQ.
func (c *Conn) dropTooLate(now time.Time) {
	age := c.sndDropAge()
	n := 0
	for n < len(c.snd) && !now.Before(c.snd[n].origin.Add(age)) {
		n++
	}
	if n == 0 {
		return
	}
	first, last := c.sndBase, seqAdd(c.sndBase, n-1)
	msgno := binary.BigEndian.Uint32(c.snd[0].buf[4:]) & msgnoMask
	c.stats.PacketsSendDrop += uint64(n)
	c.popSent(n)
	var cif [8]byte
	binary.BigEndian.PutUint32(cif[:], first)
	binary.BigEndian.PutUint32(cif[4:], last)
	c.sendControl(now, ctrlDropReq, 0, msgno, cif[:])
}

// handleACK reads an acknowledgement of what c sent.
func (c *Conn) handleACK(h *header, cif []byte, now time.Time) {
	if len(cif) < 4 {
		return
	}
	ack := binary.BigEndian.Uint32(cif) & (maxSeq - 1)
	if h.info != 0 && len(cif) >= 16 {
		// A full ACK: answer with ACKACK so the receiver measures RTT.
		c.sendControl(now, ctrlACKACK, 0, h.info, make([]byte, 4))
		if rtt := time.Duration(binary.BigEndian.Uint32(cif[4:])) * time.Microsecond; rtt > 0 {
			c.rtt = rtt
			c.rttVar = time.Duration(binary.BigEndian.Uint32(cif[8:])) * time.Microsecond
		}
	}
	n := seqDiff(ack, c.sndBase)
	if n < 0 || n > c.sndSent {
		return // old, or acknowledging what was never sent
	}
	c.popSent(n)
	if h.info != 0 && len(cif) >= 16 {
		c.peerAvail = int(min(binary.BigEndian.Uint32(cif[12:]), 1<<20))
	}
	c.trySend(now)
}

// handleNAK queues the packets a NAK reports lost for retransmission.
func (c *Conn) handleNAK(cif []byte, now time.Time) {
	c.stats.NAKsReceived++
	var gone *lossRange
	parseLossList(cif, func(from, to uint32) {
		if seqDiff(from, c.sndBase) < 0 {
			// Packets the sender no longer has: the receiver should
			// stop asking.
			last := to
			if seqDiff(to, c.sndBase) >= 0 {
				last = seqAdd(c.sndBase, -1)
			}
			if gone == nil {
				gone = &lossRange{from, last}
			} else {
				gone.to = last
			}
			from = c.sndBase
			if seqDiff(to, from) < 0 {
				return
			}
		}
		start := seqDiff(from, c.sndBase)
		end := min(seqDiff(to, c.sndBase), c.sndSent-1)
		for i := start; i <= end; i++ {
			if p := c.snd[i]; !p.queued {
				p.queued = true
				c.rexmit = append(c.rexmit, seqAdd(c.sndBase, i))
			}
		}
	})
	if gone != nil {
		var b [8]byte
		binary.BigEndian.PutUint32(b[:], gone.from)
		binary.BigEndian.PutUint32(b[4:], gone.to)
		c.sendControl(now, ctrlDropReq, 0, 0, b[:])
	}
	c.trySend(now)
}

// handleKMRefresh installs the keys of a KMREQ sent after the handshake,
// as a sender does when it changes keys, and answers with the KMRSP.
func (c *Conn) handleKMRefresh(cif []byte, now time.Time) {
	if c.cfg.Passphrase == "" {
		return
	}
	km, err := unmarshalKM(cif, c.cfg.Passphrase)
	if err != nil {
		c.sendControl(now, ctrlUser, extKMRsp, 0, binary.BigEndian.AppendUint32(nil, kmStateBadSecret))
		return
	}
	if c.km != nil && km.salt == c.km.salt {
		// The same salt: keep a key the refresh leaves out.
		if km.even == nil {
			km.even = c.km.even
		}
		if km.odd == nil {
			km.odd = c.km.odd
		}
	}
	crypt, err := newCryptor(km)
	if err != nil {
		return
	}
	c.km, c.crypt = km, crypt
	c.sendControl(now, ctrlUser, extKMRsp, 0, cif)
}

// handleData stores one data packet.
func (c *Conn) handleData(h *header, payload []byte, now time.Time) {
	seq := h.seq
	off := seqDiff(seq, c.rcvBase)
	if off < 0 {
		c.stats.PacketsDuplicate++ // delivered or dropped already
		return
	}
	if off >= len(c.rcv) {
		c.stats.PacketsRecvDrop++ // no room: the peer ignored the window
		return
	}
	slot := (c.rcvHead + off) % len(c.rcv)
	if c.rcv[slot] != nil {
		c.stats.PacketsDuplicate++
		return
	}
	rexmit := h.info&flagRexmitPkt != 0
	switch d := seqDiff(seq, c.rcvMax); {
	case d > 0:
		r := lossRange{c.rcvMax, seqAdd(seq, -1)}
		c.loss = append(c.loss, lossEntry{lossRange: r, lastNAK: now})
		c.stats.PacketsLost += uint64(d)
		c.sendNAK(now, []lossRange{r})
		c.rcvMax = seqAdd(seq, 1)
	case d == 0:
		c.rcvMax = seqAdd(seq, 1)
	default:
		c.removeLoss(seq)
	}
	data := append([]byte(nil), payload...)
	if kk := int(h.info&kkMask) >> kkShift; kk != kkNone {
		if c.crypt == nil || !c.crypt.xor(kk, seq, data) {
			c.stats.PacketsUndecrypt++
			data = nil // still occupies its place, so it is not asked for again
		}
	}
	ts := c.unwrapTS(h.ts)
	c.rcv[slot] = &rcvPacket{ts: ts, payload: data}
	c.rcvCount++
	c.stats.PacketsReceived++
	c.stats.BytesReceived += uint64(len(payload))
	c.rateCount++
	c.rateBytes += len(payload)
	if !rexmit {
		c.trackDrift(ts, now)
	}
	if c.sinceACK++; c.sinceACK >= lightACKPackets {
		c.sinceACK = 0
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], c.ackSeq())
		c.sendControl(now, ctrlACK, 0, 0, b[:])
	}
}

// unwrapTS extends a 32-bit microsecond timestamp, which wraps every 71
// minutes, to 64 bits, as the nearest such time to the latest seen.
func (c *Conn) unwrapTS(ts uint32) int64 {
	if !c.haveTS {
		c.tsLast, c.tsFull, c.haveTS = ts, int64(ts), true
		return c.tsFull
	}
	full := c.tsFull + int64(int32(ts-c.tsLast))
	if full > c.tsFull {
		c.tsLast, c.tsFull = ts, full
	}
	return full
}

// trackDrift keeps the time base at the least delay seen: a packet that
// arrives earlier than the base predicts moves it back at once, and the
// least of each window of packets moves it forward a little at most, which
// follows a sender clock slower than the receiver's without letting a
// queueing delay eat the latency.
func (c *Conn) trackDrift(ts int64, now time.Time) {
	b := now.Add(-time.Duration(ts) * time.Microsecond)
	if b.Before(c.base) {
		c.base = b
	}
	if c.driftN == 0 || b.Before(c.driftMin) {
		c.driftMin = b
	}
	if c.driftN++; c.driftN >= driftWindow {
		if d := c.driftMin.Sub(c.base); d > 0 {
			c.base = c.base.Add(min(d, driftStep))
		}
		c.driftN = 0
	}
}

// removeLoss takes seq out of the loss list, splitting its range.
func (c *Conn) removeLoss(seq uint32) {
	for i := range c.loss {
		e := &c.loss[i]
		if seqDiff(seq, e.from) < 0 || seqDiff(seq, e.to) > 0 {
			continue
		}
		switch {
		case e.from == e.to:
			c.loss = append(c.loss[:i], c.loss[i+1:]...)
		case seq == e.from:
			e.from = seqAdd(e.from, 1)
		case seq == e.to:
			e.to = seqAdd(e.to, -1)
		default:
			tail := lossEntry{lossRange{seqAdd(seq, 1), e.to}, e.lastNAK}
			e.to = seqAdd(seq, -1)
			c.loss = append(c.loss[:i+1], append([]lossEntry{tail}, c.loss[i+1:]...)...)
		}
		return
	}
}

// removeLossBefore takes every sequence number before seq out of the loss
// list.
func (c *Conn) removeLossBefore(seq uint32) {
	n := 0
	for n < len(c.loss) && seqDiff(c.loss[n].to, seq) < 0 {
		n++
	}
	c.loss = c.loss[n:]
	if len(c.loss) > 0 && seqDiff(c.loss[0].from, seq) < 0 {
		c.loss[0].from = seq
	}
	if len(c.loss) == 0 {
		c.loss = nil
	}
}

// handleDropReq stops asking for packets the sender gave up on.
func (c *Conn) handleDropReq(first, last uint32) {
	if seqDiff(last, first) < 0 || seqDiff(last, c.rcvMax) >= 0 {
		return
	}
	for i := len(c.loss) - 1; i >= 0; i-- {
		e := &c.loss[i]
		if seqDiff(e.to, first) < 0 || seqDiff(e.from, last) > 0 {
			continue
		}
		// Keep what lies outside [first, last].
		var keep []lossEntry
		if seqDiff(e.from, first) < 0 {
			keep = append(keep, lossEntry{lossRange{e.from, seqAdd(first, -1)}, e.lastNAK})
		}
		if seqDiff(e.to, last) > 0 {
			keep = append(keep, lossEntry{lossRange{seqAdd(last, 1), e.to}, e.lastNAK})
		}
		c.loss = append(c.loss[:i], append(keep, c.loss[i+1:]...)...)
	}
}

// ackDue reports whether the next ACK tick sends a full ACK: while the
// peer has not confirmed the latest acknowledgement, or while the last one
// told it the buffer was more than half full and delivery has made room
// since.
func (c *Conn) ackDue() bool {
	return c.ackSeq() != c.ackAcked || c.availSent < len(c.rcv)/2 && c.avail() > c.availSent
}

// avail is the receive buffer room beyond the acknowledged sequence number.
func (c *Conn) avail() int {
	return max(len(c.rcv)-seqDiff(c.ackSeq(), c.rcvBase), 0)
}

// ackSeq is the sequence number up to which everything has arrived.
func (c *Conn) ackSeq() uint32 {
	if len(c.loss) > 0 {
		return c.loss[0].from
	}
	return c.rcvMax
}

func (c *Conn) sendACK(now time.Time) {
	ack := c.ackSeq()
	c.sinceACK = 0
	if c.ackNo++; c.ackNo == 0 {
		c.ackNo = 1
	}
	c.acks[c.ackNo%ackHistory] = ackRecord{no: c.ackNo, seq: ack, sent: now}
	elapsed := now.Sub(c.rateSince)
	var pps, bps uint32
	if elapsed > 0 {
		pps = uint32(int64(c.rateCount) * int64(time.Second) / int64(elapsed))
		bps = uint32(int64(c.rateBytes) * int64(time.Second) / int64(elapsed))
	}
	c.rateCount, c.rateBytes, c.rateSince = 0, 0, now
	var cif [28]byte
	binary.BigEndian.PutUint32(cif[0:], ack)
	binary.BigEndian.PutUint32(cif[4:], uint32(c.rtt/time.Microsecond))
	binary.BigEndian.PutUint32(cif[8:], uint32(c.rttVar/time.Microsecond))
	c.availSent = c.avail()
	binary.BigEndian.PutUint32(cif[12:], uint32(c.availSent))
	binary.BigEndian.PutUint32(cif[16:], pps)
	binary.BigEndian.PutUint32(cif[20:], max(pps, 1))
	binary.BigEndian.PutUint32(cif[24:], bps)
	c.sendControl(now, ctrlACK, 0, c.ackNo, cif[:])
}

// handleACKACK measures the round-trip time of the ACK it answers.
func (c *Conn) handleACKACK(no uint32, now time.Time) {
	r := c.acks[no%ackHistory]
	if r.no != no || r.sent.IsZero() {
		return
	}
	c.acks[no%ackHistory] = ackRecord{}
	c.ackAcked = r.seq
	sample := now.Sub(r.sent)
	d := c.rtt - sample
	if d < 0 {
		d = -d
	}
	c.rttVar = (3*c.rttVar + d) / 4
	c.rtt = (7*c.rtt + sample) / 8
}

func (c *Conn) nakInterval() time.Duration {
	return max((c.rtt+4*c.rttVar)/2, minNAKInterval)
}

func (c *Conn) sendNAK(now time.Time, ranges []lossRange) {
	c.stats.NAKsSent++
	c.buf = c.buf[:0]
	h := header{ctrl: true, typ: ctrlNAK, ts: c.timestamp(now), dst: c.peerID}
	c.buf = appendLossList(h.append(c.buf), ranges)
	c.output(c.buf, now)
	if len(c.loss) > 0 && c.nextNAK.Before(now) {
		c.nextNAK = now.Add(c.nakInterval())
	}
}

// sendPeriodicNAK reports again the losses not reported for a NAK interval,
// since a NAK or the retransmission it asked for may be lost too.
func (c *Conn) sendPeriodicNAK(now time.Time) {
	iv := c.nakInterval()
	var ranges []lossRange
	for i := range c.loss {
		e := &c.loss[i]
		if now.Sub(e.lastNAK) >= iv && len(ranges) < maxNAKRanges {
			ranges = append(ranges, e.lossRange)
			e.lastNAK = now
		}
	}
	c.nextNAK = now.Add(iv)
	if len(ranges) > 0 {
		c.sendNAK(now, ranges)
	}
}

// deliver hands over the packets whose time has come, in order, skipping
// the missing ones before a packet that is due.
func (c *Conn) deliver(now time.Time) {
	c.nextDeliver = time.Time{}
	for c.rcvCount > 0 && c.state == StateConnected {
		p := c.rcv[c.rcvHead]
		skip := 0
		if p == nil {
			// The head is missing: find the first packet that is not.
			n := seqDiff(c.rcvMax, c.rcvBase)
			for skip = 1; skip < n; skip++ {
				if p = c.rcv[(c.rcvHead+skip)%len(c.rcv)]; p != nil {
					break
				}
			}
			if p == nil {
				return
			}
		}
		at := c.base.Add(time.Duration(p.ts)*time.Microsecond + c.rcvLatency)
		if now.Before(at) {
			c.nextDeliver = at
			return
		}
		if skip > 0 {
			// Too late for the missing packets: give up on them.
			c.stats.PacketsRecvDrop += uint64(skip)
			c.advance(skip)
			c.removeLossBefore(c.rcvBase)
		}
		c.rcv[c.rcvHead] = nil
		c.rcvCount--
		c.advance(1)
		if len(c.loss) > 0 {
			c.removeLossBefore(c.rcvBase)
		}
		if p.payload != nil && c.cfg.OnData != nil {
			c.cfg.OnData(p.payload)
		}
	}
}

func (c *Conn) advance(n int) {
	c.rcvHead = (c.rcvHead + n) % len(c.rcv)
	c.rcvBase = seqAdd(c.rcvBase, n)
	if seqDiff(c.rcvMax, c.rcvBase) < 0 {
		c.rcvMax = c.rcvBase
	}
}
