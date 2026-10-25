// Package quic implements the QUIC transport (RFC 9000, 9001 and 9002) that
// HTTP/3 runs on, over datagrams that something else sends and receives.
//
// A Conn does no I/O of its own: it is fed the datagrams that arrive from
// its peer and sends through a PacketConn, which in fib is a UDP
// connection of the engine, one per peer address. The path a connection
// sends on is that PacketConn and the peer's address. A server follows its
// client to a new address when the client's address changes under it, as a
// NAT rebinding does (see HandlePathDatagrams); a client never moves, and
// servers ask clients not to with disable_active_migration.
//
// The TLS 1.3 handshake is crypto/tls's QUICConn, or the stand-in for it
// that Config.NewTLS makes. A server accepts 0-RTT data when Config.EarlyData
// is set (RFC 9001 section 4.6).
package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"math/bits"
	"net"
	"sync"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/sidepool"
)

// Packet number spaces.
const (
	spaceInitial = iota
	spaceHandshake
	spaceApp
	numSpaces
)

// BatchSender is a PacketConn that sends several datagrams with one call,
// which a connection uses for a round of sending that has more than one. It
// must not keep them once it returns.
type BatchSender interface {
	SendBatch(datagrams [][]byte) error
}

// DatagramsSender is a PacketConn that sends several datagrams with one call
// and reports how many of them it sent, all of them unless it fails. A
// connection that sends through one keeps the datagrams a full socket had no
// room for, and sends them again shortly, where a send that fails otherwise
// leaves them lost. It must not keep them once it returns. fib.Connection
// is one.
type DatagramsSender interface {
	SendDatagrams(datagrams [][]byte) (int, error)
}

// PacketConn sends a connection's datagrams to its peer.
type PacketConn interface {
	// Send sends one datagram. It must not keep the datagram once it
	// returns: the connection reuses the memory for the next one.
	Send(datagram []byte) error
	// Close releases the path once the connection is over.
	Close() error
}

// Handler is told what happens on a connection. Its methods are called one
// at a time per connection, in order, with no lock held, so they may call
// back into the connection and its streams.
type Handler interface {
	// OnHandshake runs once the handshake is complete.
	OnHandshake(c *Conn)
	// OnStreamData delivers a stream's data in order, with fin set on the
	// last call. The first call for a stream the peer opened is how the
	// stream becomes known. data belongs to the handler, unless
	// Config.RecycleDatagrams is set: then it is valid only until the call
	// returns, and what the handler keeps it copies.
	OnStreamData(s *Stream, data []byte, fin bool)
	// OnStreamReset runs when the peer abandons its sending side.
	OnStreamReset(s *Stream, code uint64)
	// OnStopSending runs when the peer asks this side to stop sending; the
	// sending side has already been reset with the peer's code.
	OnStopSending(s *Stream, code uint64)
	// OnStreamsAvailable runs when the peer allows more streams.
	OnStreamsAvailable(c *Conn)
	// OnClose runs once when the connection is over, with why.
	OnClose(c *Conn, err error)
}

// EarlyDataHandler is a Handler that takes the 0-RTT data of a server
// connection as it arrives. OnEarlyData runs once, before the first call about
// data that came in 0-RTT packets, which is before OnHandshake: the handshake
// is still going on, the client is not authenticated, and the data can be
// replayed (see Stream.EarlyData). A Handler without it is told of 0-RTT
// data only after OnHandshake, when the handshake is over.
type EarlyDataHandler interface {
	OnEarlyData(c *Conn)
}

// CallsDoneHandler is a Handler that is also told when a run of its calls
// ends: the calls queued by the datagrams that arrived together, say. It is
// called like the others, one at a time and with no lock held, and before
// what the calls wrote is sent, so that what a handler put off until the end
// of the run goes with it.
type CallsDoneHandler interface {
	OnCallsDone(c *Conn)
}

// PathHandler is a Handler that is also told when the connection moves to
// another path, or back, which only a server's connections do (see
// HandlePathDatagrams). It is called like the others, one at a time and
// with no lock held; RemoteAddr is then the new address.
type PathHandler interface {
	OnPathChange(c *Conn)
}

// TLSConn is the TLS 1.3 handshake of a connection, as crypto/tls's QUICConn
// offers it (see there for what the methods do). *tls.QUICConn is one; so is
// the handshake of package fib/tls, which takes no goroutine to wait for the
// peer in.
type TLSConn interface {
	Start(ctx context.Context) error
	HandleData(level tls.QUICEncryptionLevel, data []byte) error
	NextEvent() tls.QUICEvent
	SetTransportParameters(params []byte)
	SendSessionTicket(opts tls.QUICSessionTicketOptions) error
	ConnectionState() tls.ConnectionState
	Close() error
}

// Config sets up a connection. Zero values take the defaults.
type Config struct {
	// NewTLS, if set, makes the TLS handshake of a connection, for a client
	// or a server, from the TLSConfig. It may return nil, and then
	// crypto/tls's QUICConn is used, as it is when NewTLS is not set.
	NewTLS func(client bool, config *tls.Config) TLSConn
	// TLSConfig is required. Its NextProtos are the ALPN offer, which QUIC
	// requires to be agreed.
	TLSConfig *tls.Config
	// MaxIdleTimeout closes a connection that hears nothing from its peer
	// for this long. Default 30 seconds.
	MaxIdleTimeout time.Duration
	// HandshakeTimeout bounds the handshake. Default 10 seconds.
	HandshakeTimeout time.Duration
	// KeepAlivePeriod, when set, pings a quiet connection this often so
	// that it does not idle out.
	KeepAlivePeriod time.Duration
	// MaxIncomingStreams and MaxIncomingUniStreams are how many streams of
	// each kind the peer may have open at once. Defaults 100 and 16.
	MaxIncomingStreams    uint64
	MaxIncomingUniStreams uint64
	// StreamReceiveWindow and ConnReceiveWindow are the flow control
	// windows. Defaults 1 MiB and 16 MiB.
	StreamReceiveWindow uint64
	ConnReceiveWindow   uint64
	// ResetKey, on a server, lets its peers recognize stateless resets.
	ResetKey *ResetKey
	// TokenKey, on a server, checks the token of a client Initial that
	// answers the server's Retry (see TokenKey.Retry): Accept takes a good
	// one as proof of the client's address, and answers one it made that
	// is not good with INVALID_TOKEN. Without it, a token is ignored.
	TokenKey *TokenKey
	// ExcessiveLoadCode is the application error code a connection closes
	// with once its peer has made it do more work without asking for
	// anything than it allows: PING after PING, say, or streams opened and
	// reset at once (see flood.go). HTTP/3 has H3_EXCESSIVE_LOAD for it.
	// Zero closes it with the transport error PROTOCOL_VIOLATION instead.
	ExcessiveLoadCode uint64
	// StreamResetBurst and StreamResetRate bound the streams a server's
	// peer may abandon while they are open, or have this side reset for an
	// error it made on them: a bucket of StreamResetBurst of them, refilled
	// at StreamResetRate a second, past which the connection closes with
	// ExcessiveLoadCode (see flood.go). ControlFrameBurst and
	// ControlFrameRate bound, in the same way, the frames a peer may send
	// that ask for work and carry nothing. Zero means
	// DefaultStreamResetBurst, DefaultStreamResetRate,
	// DefaultControlFrameBurst and DefaultControlFrameRate; either of a
	// pair below zero, no limit.
	StreamResetBurst  int
	StreamResetRate   int
	ControlFrameBurst int
	ControlFrameRate  int
	// MaxAckDelay is the longest this side holds back the acknowledgement
	// of a packet that asks for one, which it tells the peer in
	// max_ack_delay (RFC 9000 section 18.2) for the peer's probe timeout to
	// allow for. Shorter has the peer hear of what arrived sooner, at the
	// cost of more acknowledgements. Default 25 milliseconds; at most
	// 16383, and at least one.
	MaxAckDelay time.Duration
	// KeyUpdateInterval, when set, updates the 1-RTT keys a connection
	// sends with once they have been in use this long (RFC 9001 section
	// 6), as well as once they have protected as many packets as the AEAD
	// allows, which they are updated at regardless: a key used for less
	// protects less of what is sent should it ever leak. An update waits
	// for the peer to have acknowledged a packet of the current keys, so
	// a connection that sends nothing does not update.
	KeyUpdateInterval time.Duration
	// CongestionControl is the congestion controller: CUBIC, the default,
	// or NewReno.
	CongestionControl CongestionControl
	// MaxDatagramSize is the largest datagram sent once the handshake is
	// over, if the peer's max_udp_payload_size allows it, until path MTU
	// discovery finds the path carries more. Default 1200, the size every
	// path carries; more saves packets, and the syscalls that send them, on
	// a path known to carry them. At most 1452. A connection whose larger
	// datagrams stop getting through goes back to 1200 (see pmtud.go).
	MaxDatagramSize int
	// DisablePathMTUDiscovery turns path MTU discovery (RFC 8899) off. With
	// it on, the default, a connection whose path stops datagrams that are
	// too large rather than fragment them (see DontFragmenter) probes for
	// the largest datagram it carries once the handshake is confirmed, up to
	// MaxProbeDatagramSize and the peer's max_udp_payload_size, and sends
	// datagrams of that size.
	DisablePathMTUDiscovery bool
	// MaxProbeDatagramSize is the largest datagram path MTU discovery looks
	// for. Default and at most 1452, what an Ethernet MTU of 1500 bytes
	// leaves for UDP over IPv6; one no larger than MaxDatagramSize turns
	// discovery off.
	MaxProbeDatagramSize int
	// EarlyData lets a client send 0-RTT data on a session it resumes, if
	// its TLS keeps sessions that allow it (see CanSendEarly), and lets a
	// server accept 0-RTT data from clients resuming a session: its tickets
	// then allow it, and what a client sends in its first flight is
	// delivered without waiting for the handshake to end (see
	// EarlyDataHandler). Early data is not forward secret, and an
	// attacker can replay it: a ticket carries it once, but a server that
	// is not the one that issued the ticket cannot know. Applications must
	// serve only what is safe to repeat in it.
	EarlyData bool
	// RecycleDatagrams says that the datagrams HandleDatagram and
	// HandleDatagrams are given are buffers from package bufferpool that the
	// connection takes over, and gives back once it has done with them. What
	// it has to keep past that, such as data that arrived ahead of what comes
	// before it, it copies; and OnStreamData's data is then valid only for
	// the call.
	RecycleDatagrams bool
}

func (config Config) withDefaults() Config {
	if tc := config.TLSConfig; tc != nil && tc.MinVersion < tls.VersionTLS13 {
		// QUIC is TLS 1.3 only, whatever a config carried over from a TCP
		// server allows (RFC 9001 section 4.2).
		tc = tc.Clone()
		tc.MinVersion = tls.VersionTLS13
		config.TLSConfig = tc
	}
	if config.MaxIdleTimeout <= 0 {
		config.MaxIdleTimeout = 30 * time.Second
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = 10 * time.Second
	}
	if config.MaxIncomingStreams == 0 {
		config.MaxIncomingStreams = 100
	}
	if config.MaxIncomingUniStreams == 0 {
		config.MaxIncomingUniStreams = 16
	}
	if config.StreamReceiveWindow == 0 {
		config.StreamReceiveWindow = 1 << 20
	}
	if config.ConnReceiveWindow == 0 {
		config.ConnReceiveWindow = 16 << 20
	}
	config.MaxDatagramSize = min(max(config.MaxDatagramSize, maxDatagram), maxDatagramLimit)
	if config.MaxProbeDatagramSize <= 0 {
		config.MaxProbeDatagramSize = maxDatagramLimit
	}
	config.MaxProbeDatagramSize = min(max(config.MaxProbeDatagramSize, maxDatagram), maxDatagramLimit)
	if config.MaxAckDelay <= 0 {
		config.MaxAckDelay = DefaultMaxAckDelay
	}
	config.MaxAckDelay = min(max(config.MaxAckDelay, time.Millisecond), maxAckDelayLimit).Truncate(time.Millisecond)
	return config
}

type pnSpace struct {
	tx, rx    *keys
	discarded bool

	// Sending.
	nextPN               uint64
	largestAcked         int64
	sent                 sentQueue
	ackElicitingInFlight int
	lossTime             time.Time
	lastAckElicitingSent time.Time
	probes               int

	// Receiving: what to acknowledge, and whether and when to. ackFloor is
	// the packet number below which nothing needs acknowledging any more:
	// the peer has acknowledged an ACK frame that covered it (RFC 9000
	// section 13.2.4).
	recv             rangeSet
	ackFloor         uint64
	largestRecvTime  time.Time
	ackPending       bool
	ackNow           bool
	unackedEliciting int
	ackDeadline      time.Time

	cryptoSend sendBuffer
	cryptoRecv recvBuffer
}

type peerCID struct {
	cid, token []byte
}

// maxBufferedPackets bounds the packets kept until their keys arrive.
const maxBufferedPackets = 16

// maxCryptoBuffer bounds out-of-order handshake data.
const maxCryptoBuffer = 64 << 10

// Conn is one QUIC connection.
type Conn struct {
	mu       sync.Mutex
	pc       PacketConn
	remote   net.Addr
	handler  Handler
	config   Config
	isClient bool
	tls      TLSConn
	// tlsState is the TLS state, kept once a server has let tls go (see
	// releaseTLS).
	tlsState *tls.ConnectionState

	// Connection IDs: scid is this side's, dcid the peer's in use, and odcid
	// the one the client first sent to.
	scid, dcid, odcid []byte
	// A client's Retry state, and whether it has heard from the server.
	retrySCID    []byte
	token        []byte
	heardPeer    bool
	peerCIDs     map[uint64]peerCID
	dcidSeq      uint64
	retiredPrior uint64
	retireCIDs   []uint64
	// peerResetToken lets a client recognize a stateless reset.
	peerResetToken []byte

	spaces             [numSpaces]pnSpace
	handshakeComplete  bool
	handshakeConfirmed bool
	addressValidated   bool
	bytesRecv          uint64
	bytesSent          uint64
	// maxDatagram is the largest datagram this side sends: maxDatagram
	// until the handshake is over, then what the config and the peer allow.
	maxDatagram int
	// mtu is path MTU discovery's state (see pmtud.go).
	mtu        mtuState
	local      transportParams
	peerParams *transportParams

	// 1-RTT key updates (RFC 9001 section 6). txGen and rxGen count the
	// updates each direction has made; the Key Phase bit is the low bit of
	// the sender's. prevRx are the keys of the phase before, for packets
	// that arrive late, and phaseStart the first packet number of the
	// current receiving phase.
	txGen      uint64
	rxGen      uint64
	prevRx     *keys
	nextRx     *keys
	phaseStart uint64
	// txPhaseFirstPN is the first packet number sent in the current
	// sending phase, txPhaseAcked whether one of them has been
	// acknowledged, and txPhasePackets how many have been sent: an update
	// may only follow an acknowledgement, and has to come before the
	// AEAD's confidentiality limit.
	txPhaseFirstPN uint64
	txPhaseAcked   bool
	txPhasePackets uint64
	// txPhaseSince is when the current sending phase began, or when the
	// handshake was confirmed for the first, which Config.KeyUpdateInterval
	// counts from.
	txPhaseSince time.Time
	// decryptFailures counts packets that failed authentication, which the
	// AEAD's integrity limit bounds.
	decryptFailures uint64
	// glitches and resets are what the peer may make the connection do
	// without asking for anything (see flood.go), and ackedNew says whether
	// the packet being handled acknowledged something not acknowledged
	// before, which a PING bundled with it does not count against glitches.
	glitches, resets floodBudget
	ackedNew         bool
	// dataBlockedAt and streamsBlockedAt, for bidirectional and then
	// unidirectional streams, are the highest limits the peer has said it
	// was blocked at, which a BLOCKED frame for a new one goes above.
	dataBlockedAt    uint64
	streamsBlockedAt [2]uint64
	// dataBlocked and streamsBlocked, for bidirectional and then
	// unidirectional streams, are the BLOCKED frames this side reports the
	// peer's limits in (see blocked.go).
	dataBlocked    blockedReport
	streamsBlocked [2]blockedReport

	rtt          rttStats
	cc           congestion
	ptoCount     int
	lossDeadline time.Time

	timer *time.Timer
	// timerAt is what the timer is set for, or zero once it has fired.
	timerAt time.Time

	// Flow control: what the peer allows this side to send, and what this
	// side allows the peer.
	peerMaxData    uint64
	sentData       uint64
	recvMaxData    uint64
	recvData       uint64
	recvConsumed   uint64
	maxDataPending bool

	streams               map[uint64]*Stream
	openedBidi, openedUni uint64
	peerMaxBidi           uint64
	peerMaxUni            uint64
	// Streams the peer opens: how many, how many are finished, and the
	// limits this side has given.
	peerOpenedBidi, peerOpenedUni uint64
	peerDoneBidi, peerDoneUni     uint64
	maxBidi, maxUni               uint64
	maxStreamsBidiPending         bool
	maxStreamsUniPending          bool
	sendQueue                     []*Stream

	handshakeDonePending bool
	pingPending          bool
	pathResponses        [][8]byte

	// A server's path migration (see migrateLocked). pathValidated says the
	// path the connection sends on is known to reach the peer. While it is
	// not, prevPC and prevRemote are the last one that was, challenge is what
	// the PATH_CHALLENGE to the new one asks of it, pathDeadline when
	// validation gives up, and pathRecv and pathSent the bytes that crossed
	// the new path, which the amplification limit holds to one to three.
	pathValidated      bool
	prevPC             PacketConn
	prevRemote         net.Addr
	challenge          [8]byte
	challengePending   bool
	pathDeadline       time.Time
	pathRecv, pathSent uint64
	// rxPC and rxRemote are the path of the datagram being handled, and
	// nonProbing says whether the packet being handled carried a frame other
	// than a probing one (RFC 9000 section 9.1).
	rxPC       PacketConn
	rxRemote   net.Addr
	nonProbing bool
	// givenUp are paths the connection has left for good, closed once the
	// lock is let go.
	givenUp []PacketConn

	created     time.Time
	idleTimeout time.Duration
	// lastActivity restarts the idle timer: it is when a packet last
	// arrived, or when the first ack-eliciting packet since then left
	// (RFC 9000 section 10.1). sentSinceRecv says whether that has happened.
	lastActivity  time.Time
	sentSinceRecv bool

	closed   bool
	closeErr error
	// closeWhenDone is the close CloseWhenDone asked for, and closeDeadline,
	// once every bidirectional stream is finished, how long it still waits
	// for the peer to acknowledge what the others were written.
	closeWhenDone *ApplicationError
	closeDeadline time.Time
	// closePacket is the datagram that carried this side's CONNECTION_CLOSE,
	// kept until closingUntil, the end of the closing period (RFC 9000
	// section 10.2.1), to send again to what still arrives: the first may
	// have been lost, and the peer, which goes on probing, would otherwise
	// hold on to the connection until it idled out. closingSeen counts what
	// arrived meanwhile, the close going again at each power of two of it.
	// A connection the peer closed has closingUntil set, without a
	// closePacket, for the draining period (RFC 9000 section 10.2.2).
	closePacket  []byte
	closingUntil time.Time
	closingSeen  int

	// events are the handler calls waiting for dispatch, and spareEvents
	// the array the last batch of them was run from, kept for the next.
	events      []event
	spareEvents []event
	dispatching bool
	// sending says a goroutine is sending, which only one does at a time;
	// wantFlush that there may be something to send, which the sending one
	// sends too before it stops. flushDeadline bounds how long what was
	// written while the handler was being called may wait for its calls to
	// end (see flush).
	sending       bool
	wantFlush     bool
	flushDeadline time.Time
	// awaited counts the writes ExpectWrite said are on their way.
	awaited int
	// sendRoom is what writers waiting for their streams' unsent bytes to
	// drain wait on, and roomWaiters counts them; see Stream.WaitSendable.
	sendRoom    sync.Cond
	roomWaiters int
	// roomStreams are the streams with a callback waiting in
	// NotifySendable.
	roomStreams []*Stream
	// buffered are packets that arrived before their keys.
	buffered [][]byte

	// 0-RTT, for a server: the keys of the client's early packets, once TLS
	// has accepted its early data (they go with the first 1-RTT packet);
	// whether the packet being handled is an early one; the early data
	// calls held back until the handshake is over, for a handler that is
	// not an EarlyDataHandler; and whether OnEarlyData has been queued.
	earlyRx      *keys
	curEarly     bool
	heldEarly    []event
	holdEarly    bool
	earlyStarted bool

	// 0-RTT, for a client: the keys its early packets are sent under, until
	// the 1-RTT ones replace them; the limits the server gave on the session
	// it resumes, which the early data works to until the server's own
	// parameters arrive; and whether the server turned the early data away.
	earlyTx       *keys
	earlyOffered  bool
	remembered    *transportParams
	earlyRejected bool
	// peerParamsRaw is the server's transport parameters as it sent them,
	// which are kept with the session for the next connection.
	peerParamsRaw []byte
	// roundLens are the lengths of the datagrams a round of sending sends,
	// one after another in its buffer, which only the sending goroutine uses
	// (see flush).
	roundLens [maxRoundDatagrams]uint16
	// blocked is the buffer of a round whose datagrams the socket had no
	// room for, kept to send again: blockedN of them, from blockedOff on,
	// their lengths at the start of roundLens. Only the sending goroutine
	// uses them.
	blocked    []byte
	blockedOff int
	blockedN   int
	// Pacing (see pacer.go): the budget, in bytes, as it was at paceLast;
	// paced says the round stopped for want of it; sendAt is when a round
	// that stopped for pacing or for a full socket goes on.
	paceBudget int
	paceLast   time.Time
	paced      bool
	sendAt     time.Time

	// freeSent are sent packet records to reuse, and ackedScratch and
	// lostScratch the arrays acknowledgement and loss detection list theirs
	// in; see newSentPacket.
	freeSent     []*sentPacket
	ackedScratch []*sentPacket
	lostScratch  []*sentPacket

	// Context is for the application, which the connection never touches.
	Context any
}

func (c *Conn) now() time.Time { return time.Now() }

// updateTxKeys moves the sending side to the next key phase.
func (c *Conn) updateTxKeys(s *pnSpace) {
	s.tx = s.tx.next()
	c.txGen++
	c.txPhaseFirstPN = s.nextPN
	c.txPhaseAcked = false
	c.txPhasePackets = 0
	c.txPhaseSince = c.now()
}

// maybeUpdateKeys starts a key update once this side has sent as many
// packets as one key may protect (RFC 9001 section 6.6), or has used the key
// for Config.KeyUpdateInterval. An update needs a confirmed handshake, and
// an acknowledgement of the phase in use, so that two updates cannot
// overtake each other (RFC 9001 section 6.5).
func (c *Conn) maybeUpdateKeys(now time.Time) {
	s := &c.spaces[spaceApp]
	if !c.handshakeConfirmed || s.tx == nil {
		return
	}
	if c.txPhaseSince.IsZero() {
		c.txPhaseSince = now
	}
	if c.txGen != c.rxGen || !c.txPhaseAcked {
		return
	}
	if c.txPhasePackets >= confidentialityLimit(s.tx.suite) ||
		c.config.KeyUpdateInterval > 0 && now.Sub(c.txPhaseSince) >= c.config.KeyUpdateInterval {
		c.updateTxKeys(s)
	}
}

func randomCID() []byte {
	b := make([]byte, cidLen)
	_, _ = rand.Read(b)
	return b
}

func newConn(pc PacketConn, remote net.Addr, config Config, handler Handler, isClient bool) *Conn {
	config = config.withDefaults()
	now := time.Now()
	c := &Conn{
		pc:           pc,
		remote:       remote,
		handler:      handler,
		config:       config,
		isClient:     isClient,
		scid:         randomCID(),
		peerCIDs:     make(map[uint64]peerCID),
		rtt:          newRTTStats(),
		cc:           newCongestion(config.CongestionControl),
		streams:      make(map[uint64]*Stream),
		created:      now,
		lastActivity: now,
		idleTimeout:  config.MaxIdleTimeout,
		recvMaxData:  config.ConnReceiveWindow,
		maxBidi:      config.MaxIncomingStreams,
		maxUni:       config.MaxIncomingUniStreams,
		maxDatagram:  maxDatagram,
		glitches:     newFloodBudget(config.ControlFrameBurst, config.ControlFrameRate, DefaultControlFrameBurst, DefaultControlFrameRate),
		resets:       newFloodBudget(config.StreamResetBurst, config.StreamResetRate, DefaultStreamResetBurst, DefaultStreamResetRate),
		// The path a connection starts on is validated by its handshake,
		// with the amplification limit that addressValidated keeps.
		pathValidated: true,
	}
	_, isEarly := handler.(EarlyDataHandler)
	c.holdEarly = !isEarly
	c.sendRoom.L = &c.mu
	for i := range c.spaces {
		c.spaces[i].largestAcked = -1
	}
	c.local = defaultParams()
	c.local.maxIdleTimeout = config.MaxIdleTimeout
	c.local.initialMaxData = config.ConnReceiveWindow
	c.local.maxStreamDataBidiL = config.StreamReceiveWindow
	c.local.maxStreamDataBidiR = config.StreamReceiveWindow
	c.local.maxStreamDataUni = config.StreamReceiveWindow
	c.local.maxStreamsBidi = config.MaxIncomingStreams
	c.local.maxStreamsUni = config.MaxIncomingUniStreams
	c.local.maxAckDelay = config.MaxAckDelay
	c.local.initialSCID, c.local.hasInitialSCID = c.scid, true
	return c
}

// newTLS makes the connection's TLS handshake. One that can go on by itself,
// once a certificate callback it ran on another goroutine has returned, is
// given wakeTLS to say so.
func (c *Conn) newTLS() TLSConn {
	if c.config.NewTLS != nil {
		if t := c.config.NewTLS(c.isClient, c.config.TLSConfig); t != nil {
			if w, ok := t.(interface{ SetWake(func()) }); ok {
				w.SetWake(c.wakeTLS)
			}
			return t
		}
	}
	if c.isClient {
		return tls.QUICClient(&tls.QUICConfig{TLSConfig: c.config.TLSConfig, EnableSessionEvents: c.config.EarlyData})
	}
	return tls.QUICServer(&tls.QUICConfig{TLSConfig: c.config.TLSConfig, EnableSessionEvents: c.config.EarlyData})
}

// Dial starts the client side of a connection, sending its first packets
// through pc. Datagrams from the server go to HandleDatagram.
func Dial(pc PacketConn, remote net.Addr, config Config, handler Handler) (*Conn, error) {
	if config.TLSConfig == nil {
		return nil, errors.New("quic: Config.TLSConfig is required")
	}
	c := newConn(pc, remote, config, handler, true)
	c.odcid = randomCID()
	c.dcid = c.odcid
	c.spaces[spaceInitial].tx, c.spaces[spaceInitial].rx = initialKeys(c.odcid, true)
	c.tls = c.newTLS()
	c.tls.SetTransportParameters(c.local.encode())
	c.mu.Lock()
	err := c.tls.Start(context.Background())
	if err == nil {
		err = c.processTLSEvents()
	}
	if err != nil {
		c.mu.Unlock()
		c.tls.Close()
		return nil, err
	}
	c.wantFlush = true
	c.mu.Unlock()
	c.dispatch()
	return c, nil
}

// Accept starts the server side of a connection from the client's first
// datagram, which IsInitial has approved. It returns nil if the datagram
// does not start a connection after all.
//
// A datagram whose token Config.TokenKey made for the client, in a Retry
// that it answers, starts a connection whose client address is known to be
// its own; one whose token the key made but is not good is answered with
// INVALID_TOKEN, and starts none.
func Accept(pc PacketConn, remote net.Addr, config Config, handler Handler, datagram []byte) *Conn {
	h, err := parseHeader(datagram, cidLen)
	if err != nil || h.typ != packetInitial || len(h.dcid) < 8 || config.TLSConfig == nil {
		return nil
	}
	odcid := h.dcid
	retried := false
	if k := config.TokenKey; k != nil && len(h.token) > 0 {
		status, first := k.check(h.token, remote, h.dcid)
		switch status {
		case TokenInvalid:
			sendInvalidToken(pc, h)
			return nil
		case TokenValid:
			odcid, retried = first, true
		}
	}
	c := newConn(pc, remote, config, handler, false)
	// The client's Initial and 0-RTT packets go to h.dcid, under keys of
	// it, which after a Retry is the connection ID the Retry named.
	c.odcid = bytes.Clone(h.dcid)
	c.dcid = bytes.Clone(h.scid)
	c.peerCIDs[0] = peerCID{cid: c.dcid}
	c.local.originalDCID, c.local.hasOriginalDCID = bytes.Clone(odcid), true
	if retried {
		// The client's address is its own, so the amplification limit no
		// longer applies (RFC 9000 section 8.1.2), and it checks that the
		// Retry it had came from this server.
		c.addressValidated = true
		c.local.retrySCID, c.local.hasRetrySCID = c.odcid, true
	}
	c.local.disableMigration = true
	if config.ResetKey != nil {
		c.local.statelessResetToken = config.ResetKey.token(c.scid)
	}
	c.spaces[spaceInitial].tx, c.spaces[spaceInitial].rx = initialKeys(c.odcid, false)
	c.tls = c.newTLS()
	// A server could wait to be asked, but its parameters are known already.
	c.tls.SetTransportParameters(c.local.encode())
	c.mu.Lock()
	if err := c.tls.Start(context.Background()); err != nil {
		c.mu.Unlock()
		c.tls.Close()
		return nil
	}
	c.mu.Unlock()
	c.HandleDatagram(datagram)
	return c
}

// RemoteAddr is the peer's address: that of the path the connection sends
// on.
func (c *Conn) RemoteAddr() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.remote
}

// LocalCID is the connection ID this side chose, which the peer's packets
// carry: a server that serves several connections finds a connection's
// packets by it when they arrive from an address it does not know.
func (c *Conn) LocalCID() []byte { return c.scid }

// Handler is the handler the connection was accepted or dialed with.
func (c *Conn) Handler() Handler { return c.handler }

// Owns reports whether packets sent to the connection ID cid are this
// connection's: cid is its own, or, on a server, the one the client chose
// for its first packets, which its Initial and 0-RTT packets go to until it
// has the server's. A server whose clients run several connections from one
// address tells their packets apart by it.
func (c *Conn) Owns(cid []byte) bool {
	return bytes.Equal(cid, c.scid) || !c.isClient && bytes.Equal(cid, c.odcid)
}

// SendsOn reports whether pc is the path the connection sends on.
func (c *Conn) SendsOn(pc PacketConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return pc == c.pc
}

// ConnectionState is the TLS state of the connection.
func (c *Conn) ConnectionState() tls.ConnectionState {
	c.mu.Lock()
	t, state := c.tls, c.tlsState
	c.mu.Unlock()
	if state != nil {
		return *state
	}
	return t.ConnectionState()
}

// HandleDatagram processes a datagram from the peer, on the path the
// connection sends on.
//
// What it has to send in reply - acknowledgements, and whatever the handler
// writes while it is told about the datagram - goes out once the handler
// calls are done, in as few packets as it fits in.
func (c *Conn) HandleDatagram(datagram []byte) { c.HandlePathDatagram(nil, nil, datagram) }

// HandleDatagrams processes datagrams that arrived together on the path the
// connection sends on, and answers them together: one acknowledgement for
// all of them, and what the handler writes for them packed into the same
// packets.
func (c *Conn) HandleDatagrams(datagrams [][]byte) { c.HandlePathDatagrams(nil, nil, datagrams) }

// HandlePathDatagram is HandleDatagram for a datagram that arrived through
// pc from remote, which need not be the path the connection sends on. A
// nil pc is that path.
//
// A server whose client's address changed, as a NAT rebinding changes it,
// finds the client's packets arriving from an address it has no connection
// for, and hands them over with the path they came on. The connection moves
// there once one of them proves to be the client's latest (RFC 9000 section
// 9.3): an authentic 1-RTT packet, numbered above every one before it, with
// a frame in it other than a probing one. It then sends there, at most three
// times what arrived there, until the client has answered a PATH_CHALLENGE
// on it, and goes back to the path it left if the client does not answer in
// time. Once the new path is validated, the old one is closed.
func (c *Conn) HandlePathDatagram(pc PacketConn, remote net.Addr, datagram []byte) {
	c.mu.Lock()
	if !c.closed {
		now := c.now()
		c.setRxPathLocked(pc, remote)
		c.handleDatagramLocked(datagram, now)
		c.armAckLocked(now)
		c.wantFlush = true
	} else {
		c.answerClosingLocked(1)
	}
	c.recycleLocked(datagram)
	givenUp := c.takeGivenUpLocked()
	c.mu.Unlock()
	closePaths(givenUp)
	c.dispatch()
}

// HandlePathDatagrams is HandleDatagrams for datagrams that arrived through
// pc from remote; see HandlePathDatagram.
func (c *Conn) HandlePathDatagrams(pc PacketConn, remote net.Addr, datagrams [][]byte) {
	c.mu.Lock()
	if !c.closed {
		now := c.now()
		c.setRxPathLocked(pc, remote)
		for _, d := range datagrams {
			if c.closed {
				break
			}
			c.handleDatagramLocked(d, now)
		}
		c.armAckLocked(now)
		c.wantFlush = true
	} else {
		c.answerClosingLocked(len(datagrams))
	}
	for _, d := range datagrams {
		c.recycleLocked(d)
	}
	givenUp := c.takeGivenUpLocked()
	c.mu.Unlock()
	closePaths(givenUp)
	c.dispatch()
}

// setRxPathLocked records the path the datagrams being handled arrived on.
// Callers hold c.mu.
func (c *Conn) setRxPathLocked(pc PacketConn, remote net.Addr) {
	if pc == nil {
		pc, remote = c.pc, c.remote
	}
	c.rxPC, c.rxRemote = pc, remote
}

// armAckLocked makes sure that the timer fires by the time an
// acknowledgement the datagrams just processed call for is due. It is
// usually sent before then, with what the handler writes in reply, but the
// handler may not write, or may take longer than the peer allows the
// acknowledgement to wait. Callers hold c.mu.
func (c *Conn) armAckLocked(now time.Time) {
	s := &c.spaces[spaceApp]
	if s.ackPending && !s.ackNow && !s.ackDeadline.IsZero() && !c.closed &&
		(c.timerAt.IsZero() || s.ackDeadline.Before(c.timerAt)) {
		c.armTimerLocked(now)
	}
}

// recycleLocked gives a datagram back to the pool, with
// Config.RecycleDatagrams, once the handler calls it led to have run: they
// are queued ahead of it, and may be dispatched by another goroutine after
// this one has returned. Callers hold c.mu.
func (c *Conn) recycleLocked(datagram []byte) {
	if c.config.RecycleDatagrams {
		c.events = append(c.events, event{kind: evRecycle, data: datagram})
	}
}

// Abort ends the connection without telling the peer, as when the path
// under it is gone.
func (c *Conn) Abort(err error) {
	c.mu.Lock()
	c.terminateLocked(err)
	c.mu.Unlock()
	c.dispatch()
}

// Close ends the connection with an application error code, telling the
// peer.
func (c *Conn) Close(code uint64, reason string) {
	c.mu.Lock()
	c.closeLocked(&ApplicationError{Code: code, Reason: reason})
	c.mu.Unlock()
	c.dispatch()
}

// CloseWhenDone closes the connection with an application error code once
// every bidirectional stream is finished, its data delivered: a graceful
// end, for after the application protocol has told the peer to open no
// more. What was written to unidirectional streams, which may never end, is
// delivered too, the message that told the peer so among it: the close waits
// for the peer to acknowledge it, three probe timeouts at most, rather than
// overtake it.
func (c *Conn) CloseWhenDone(code uint64, reason string) {
	c.mu.Lock()
	if !c.closed && c.closeWhenDone == nil {
		c.closeWhenDone = &ApplicationError{Code: code, Reason: reason}
		c.closeIfDoneLocked()
	}
	c.mu.Unlock()
	c.dispatch()
}

func (c *Conn) closeIfDoneLocked() {
	if c.closeWhenDone == nil || c.closed {
		return
	}
	unacked := false
	for id, s := range c.streams {
		if isBidi(id) {
			// One the peer opened since, to be refused, starts the wait
			// again once it is finished.
			c.closeDeadline = time.Time{}
			return
		}
		unacked = unacked || s.unacked()
	}
	if unacked {
		// What is written is sent after the handler's calls, or by the
		// goroutine sending already, and a close now would go before it.
		now := c.now()
		if c.closeDeadline.IsZero() {
			c.closeDeadline = now.Add(3 * c.rtt.pto())
			c.armTimerLocked(now)
		}
		if now.Before(c.closeDeadline) {
			return
		}
	}
	c.closeLocked(c.closeWhenDone)
}

// OpenStream opens a bidirectional stream, or fails with ErrStreamLimit
// while the peer allows no more; OnStreamsAvailable says when to try again.
func (c *Conn) OpenStream() (*Stream, error) { return c.openStream(true) }

// OpenUniStream opens a unidirectional stream.
func (c *Conn) OpenUniStream() (*Stream, error) { return c.openStream(false) }

func (c *Conn) openStream(bidi bool) (*Stream, error) {
	c.mu.Lock()
	s, err := c.openStreamLocked(bidi)
	if err == ErrStreamLimit && c.blockedPending() {
		// The peer hears that this side wants more.
		c.wantFlush = true
		c.mu.Unlock()
		c.dispatch()
		return nil, err
	}
	c.mu.Unlock()
	return s, err
}

func (c *Conn) openStreamLocked(bidi bool) (*Stream, error) {
	if c.closed {
		return nil, ErrClosed
	}
	if !c.handshakeComplete && c.earlyRx == nil && c.earlyTx == nil {
		// Early data is sent, and answered, before the handshake ends, on
		// streams opened then too.
		return nil, errors.New("quic: handshake not complete")
	}
	var id uint64
	if bidi {
		if c.openedBidi >= c.peerMaxBidi {
			c.streamsBlocked[0].block(c.peerMaxBidi)
			return nil, ErrStreamLimit
		}
		id = c.openedBidi << 2
		c.openedBidi++
	} else {
		if c.openedUni >= c.peerMaxUni {
			c.streamsBlocked[1].block(c.peerMaxUni)
			return nil, ErrStreamLimit
		}
		id = c.openedUni<<2 | 2
		c.openedUni++
	}
	if !c.isClient {
		id |= 1
	}
	return c.newStream(id), nil
}

// newStream sets up a stream's state from the limits of both sides.
func (c *Conn) newStream(id uint64) *Stream {
	s := &Stream{conn: c, id: id, recvFin: -1}
	local := isClientStream(id) == c.isClient
	s.hasSend = isBidi(id) || local
	s.hasRecv = isBidi(id) || !local
	s.sendMax = c.streamSendMax(id)
	if s.hasRecv {
		s.recvMax = c.config.StreamReceiveWindow
	}
	c.streams[id] = s
	return s
}

// streamSendMax is how much the peer lets this side send on a stream, to
// begin with.
func (c *Conn) streamSendMax(id uint64) uint64 {
	p := c.peerParams
	local := isClientStream(id) == c.isClient
	switch {
	case p == nil:
		return 0
	case !isBidi(id):
		return p.maxStreamDataUni
	case local:
		return p.maxStreamDataBidiR
	}
	return p.maxStreamDataBidiL
}

// peerStream finds the stream a frame refers to, opening the peer's streams
// up to it. recvSide says whether the frame is about the stream's receiving
// side. A nil stream with no error is one already finished.
func (c *Conn) peerStream(id uint64, recvSide bool) (*Stream, error) {
	local := isClientStream(id) == c.isClient
	if !isBidi(id) && local == recvSide {
		return nil, transportErr(errStreamState, "frame for the wrong direction of a unidirectional stream")
	}
	if s := c.streams[id]; s != nil {
		return s, nil
	}
	seq := id >> 2
	if local {
		opened := c.openedUni
		if isBidi(id) {
			opened = c.openedBidi
		}
		if seq >= opened {
			return nil, transportErr(errStreamState, "frame for a stream not yet opened")
		}
		return nil, nil
	}
	opened, limit := &c.peerOpenedUni, c.maxUni
	if isBidi(id) {
		opened, limit = &c.peerOpenedBidi, c.maxBidi
	}
	if seq < *opened {
		return nil, nil
	}
	if seq >= limit {
		return nil, transportErr(errStreamLimit, "too many streams")
	}
	for ; *opened <= seq; *opened++ {
		c.newStream(*opened<<2 | id&3)
	}
	return c.streams[id], nil
}

// maybeFinish forgets a stream once both of its sides are done, and gives
// the peer another stream in place of one of its own.
func (c *Conn) maybeFinish(s *Stream) {
	if s.done || !s.sendDone() || !s.recvFinished() {
		return
	}
	s.done = true
	delete(c.streams, s.id)
	c.closeIfDoneLocked()
	if s.local() {
		return
	}
	if isBidi(s.id) {
		c.peerDoneBidi++
		if c.maxBidi-c.peerDoneBidi < c.config.MaxIncomingStreams/2+1 {
			c.maxBidi = c.peerDoneBidi + c.config.MaxIncomingStreams
			c.maxStreamsBidiPending = true
		}
	} else {
		c.peerDoneUni++
		if c.maxUni-c.peerDoneUni < c.config.MaxIncomingUniStreams/2+1 {
			c.maxUni = c.peerDoneUni + c.config.MaxIncomingUniStreams
			c.maxStreamsUniPending = true
		}
	}
}

func (c *Conn) queueStream(s *Stream) {
	if !s.queued && !s.done {
		s.queued = true
		c.sendQueue = append(c.sendQueue, s)
	}
}

// dispatch runs queued handler calls, one goroutine at a time, so that the
// handler sees a connection's events in order and never concurrently; then
// it sends what there is to send.
func (c *Conn) dispatch() {
	c.mu.Lock()
	if c.dispatching {
		c.mu.Unlock()
		c.flush()
		return
	}
	c.dispatching = true
	for len(c.events) > 0 {
		// What is queued while the batch runs goes on the spare array, so
		// the batch is this goroutine's alone.
		events := c.events
		c.events, c.spareEvents = c.spareEvents[:0], nil
		c.mu.Unlock()
		for i := range events {
			c.run(&events[i])
		}
		if h, ok := c.handler.(CallsDoneHandler); ok {
			h.OnCallsDone(c)
		}
		clear(events)
		c.mu.Lock()
		if c.spareEvents == nil {
			c.spareEvents = events[:0]
		}
	}
	c.dispatching = false
	c.mu.Unlock()
	c.flush()
}

// ExpectWrite tells the connection that a write is on its way from another
// goroutine: the response to a request its handler has handed on, say.
// Until as many WriteDone, what there is to send waits for the writes still
// expected, flushHold at most, as what is written while the handler is being
// called waits for the calls to end (see flush): a burst of requests
// answered on other goroutines is then answered in one burst, the answers
// packed into as few packets as they fit in and the acknowledgement of the
// requests among them, rather than a packet an answer and one more for the
// acknowledgement. A connection with one request at a time sends its answer
// as soon as it is written. Every ExpectWrite needs a WriteDone.
func (c *Conn) ExpectWrite() {
	c.mu.Lock()
	c.awaited++
	c.mu.Unlock()
}

// WriteDone ends an ExpectWrite, once its write has been made or will not be.
// A write expected for longer than flushHold has already stopped holding
// anything back.
func (c *Conn) WriteDone() {
	c.mu.Lock()
	if c.awaited > 0 {
		c.awaited--
	}
	last := c.awaited == 0 && c.wantFlush
	c.mu.Unlock()
	if last {
		c.dispatch()
	}
}

// flushHold bounds how long a write made while the handler is being called
// waits for the calls to end before it is sent anyway.
const flushHold = time.Millisecond

// flush sends what there is to send, on one goroutine at a time, so that
// packets leave in the order they are numbered. A goroutine that finds
// another one sending leaves its data to it, which sends it too before it
// stops: that is what packs the writes of goroutines that answer requests
// concurrently into full packets. Packets are built with the lock held but
// sent without it, so that nobody waits on the syscalls.
//
// While the handler is being called, or writes ExpectWrite announced are
// still to come, what is written waits for them, so that the answers to a
// burst of requests leave together with the acknowledgement of the burst; a
// handler that takes longer than flushHold has it sent without waiting for
// it.
func (c *Conn) flush() {
	c.mu.Lock()
	held := c.heldLocked()
	if held && c.wantFlush && !c.closed && c.flushDeadline.IsZero() {
		// Nothing to hold back but an acknowledgement that is not due is
		// nothing to hold: the acknowledgement has a deadline of its own,
		// which the timer keeps (see armAckLocked).
		if c.sendableLocked() {
			now := c.now()
			c.flushDeadline = now.Add(flushHold)
			c.armTimerLocked(now)
		}
	}
	if held || c.sending {
		c.mu.Unlock()
		return
	}
	c.sendLocked()
	c.mu.Unlock()
}

// heldLocked reports whether what there is to send waits for the handler
// calls, or the writes expected, to end (see flush). Callers hold c.mu.
func (c *Conn) heldLocked() bool { return c.dispatching || c.awaited > 0 }

// sendableLocked reports whether a round of sending would find something to
// send now, other than an acknowledgement that is not yet due, which the
// timer sends once it is. Callers hold c.mu.
func (c *Conn) sendableLocked() bool {
	for space := range c.spaces {
		s := &c.spaces[space]
		if s.tx == nil || s.discarded {
			continue
		}
		if s.probes > 0 || s.ackPending && s.ackNow || c.framesWaiting(space) {
			return true
		}
	}
	return false
}

// sendLocked sends until there is nothing left to send, unless another
// goroutine is sending already. Callers hold c.mu.
func (c *Conn) sendLocked() {
	if c.sending {
		return
	}
	c.sending = true
	for c.wantFlush && !c.closed {
		c.wantFlush = false
		c.sendRoundLocked()
	}
	c.sending = false
	c.wakeWritersLocked()
}

// wakeWritersLocked lets the writers waiting in WaitSendable look again at
// what their streams have left to send. Callers hold c.mu.
func (c *Conn) wakeWritersLocked() {
	if c.roomWaiters > 0 {
		c.sendRoom.Broadcast()
	}
	if len(c.roomStreams) == 0 {
		return
	}
	kept := c.roomStreams[:0]
	for _, s := range c.roomStreams {
		var err error
		switch {
		case c.closed, !s.hasSend || s.reset:
			err = ErrClosed
		case s.send.end()-s.send.next > uint64(s.roomLimit):
			kept = append(kept, s)
			continue
		}
		fn := s.roomFn
		s.roomFn = nil
		// The callback writes to the stream, which takes the lock held
		// here, so it runs elsewhere, without waiting for room.
		sidepool.Post(func() { fn(err) })
	}
	clear(c.roomStreams[len(kept):])
	c.roomStreams = kept
}

func (c *Conn) handleDatagramLocked(d []byte, now time.Time) {
	c.bytesRecv += uint64(len(d))
	whole := d
	processed := false
	for len(d) > 0 && !c.closed {
		h, err := parseHeader(d, len(c.scid))
		if err == errUnknownVersion {
			if c.isClient && h.version == 0 {
				c.handleVersionNegotiation(d)
			}
			return
		}
		if err != nil {
			break
		}
		pkt := d[:h.end]
		d = d[h.end:]
		if h.typ == packetRetry {
			c.handleRetry(pkt, h)
			return
		}
		if h.typ != packet1RTT && !bytes.Equal(h.dcid, c.scid) && (c.isClient || !bytes.Equal(h.dcid, c.odcid)) {
			continue
		}
		if c.handlePacket(pkt, h, now) {
			processed = true
		}
	}
	if !processed && c.isClient && c.isStatelessReset(whole) {
		c.terminateLocked(ErrStatelessReset)
		return
	}
	if !c.pathValidated && c.rxPC == c.pc {
		c.pathRecv += uint64(len(whole))
	}
	c.retryBuffered(now)
}

// retryBuffered processes packets that arrived before their keys, now that
// there may be keys for them.
func (c *Conn) retryBuffered(now time.Time) {
	for len(c.buffered) > 0 && !c.closed {
		pending := c.buffered
		c.buffered = nil
		progress := false
		for _, pkt := range pending {
			h, err := parseHeader(pkt, len(c.scid))
			if err != nil {
				continue
			}
			if c.keysFor(h.typ) == nil {
				if h.typ == packet0RTT && (c.peerParams != nil || c.isClient) {
					// TLS has answered the ClientHello without accepting
					// early data, so there will be no keys for these.
					continue
				}
				c.buffered = append(c.buffered, pkt)
				continue
			}
			c.handlePacket(pkt, h, now)
			progress = true
		}
		if !progress {
			return
		}
	}
}

func packetSpace(typ int) int {
	switch typ {
	case packetInitial:
		return spaceInitial
	case packetHandshake:
		return spaceHandshake
	}
	return spaceApp
}

func (c *Conn) keysFor(typ int) *keys {
	if typ == packet0RTT {
		return c.earlyRx
	}
	s := &c.spaces[packetSpace(typ)]
	if s.discarded {
		return nil
	}
	return s.rx
}

func (c *Conn) isStatelessReset(d []byte) bool {
	if len(d) < 21 || d[0]&0x80 != 0 || c.peerResetToken == nil {
		return false
	}
	return bytes.Equal(d[len(d)-statelessResetTokenLen:], c.peerResetToken)
}

// handlePacket decrypts and processes one packet, reporting whether it was
// authentic.
func (c *Conn) handlePacket(pkt []byte, h header, now time.Time) bool {
	space := packetSpace(h.typ)
	s := &c.spaces[space]
	if s.discarded {
		return false
	}
	rx := s.rx
	early := h.typ == packet0RTT
	if early {
		if c.isClient {
			return false
		}
		rx = c.earlyRx
	}
	if rx == nil {
		if early && c.peerParams != nil {
			// The ClientHello is answered, without early keys: TLS declined
			// the early data, or its keys are gone.
			return false
		}
		if space != spaceInitial && len(c.buffered) < maxBufferedPackets {
			// A copy, since it outlives the datagram it arrived in.
			c.buffered = append(c.buffered, bytes.Clone(pkt))
		}
		return false
	}
	largest := int64(-1)
	if len(s.recv) > 0 {
		largest = int64(s.recv.largest())
	}
	pn, n, ok := unprotect(pkt, h.pnOffset, rx.hp, largest)
	if !ok {
		return false
	}
	hdrEnd := h.pnOffset + n
	k := rx
	updating := false
	var phase bool
	if h.typ == packet1RTT {
		phase = pkt[0]&0x04 != 0
		if phase != (c.rxGen&1 == 1) {
			if c.prevRx != nil && pn < c.phaseStart {
				k = c.prevRx
			} else {
				if c.nextRx == nil {
					c.nextRx = s.rx.next()
				}
				k, updating = c.nextRx, true
			}
		}
	}
	var payload []byte
	var err error
	if updating {
		// A failed trial must not spoil the packet for anything else.
		trial := bytes.Clone(pkt)
		payload, err = k.open(trial, hdrEnd, pn)
	} else {
		payload, err = k.open(pkt, hdrEnd, pn)
	}
	if err != nil {
		if space == spaceApp {
			// Too many forgeries against one key would weaken it, so the
			// connection ends well before the AEAD's integrity limit
			// (RFC 9001 section 6.6).
			if c.decryptFailures++; c.decryptFailures >= integrityLimit(rx.suite) {
				c.closeLocked(transportErr(errAEADLimitReached, "too many packets failed authentication"))
				return true
			}
		}
		return false
	}
	if updating {
		if !c.handshakeConfirmed {
			c.closeLocked(transportErr(errKeyUpdate, "key update before the handshake was confirmed"))
			return true
		}
		c.prevRx = s.rx
		s.rx = c.nextRx
		c.nextRx = nil
		c.rxGen++
		c.phaseStart = pn
		if c.txGen < c.rxGen {
			// The peer started this update, so this side follows it.
			c.updateTxKeys(s)
		}
	}
	reserved := byte(0x18)
	if pkt[0]&0x80 != 0 {
		reserved = 0x0c
	}
	if pkt[0]&reserved != 0 {
		c.closeLocked(transportErr(errProtocolViolation, "reserved header bits set"))
		return true
	}
	if pn < s.ackFloor || !s.recv.add(pn) {
		// A packet already seen, or too old to tell, which may be one.
		return true
	}
	if c.isClient && !c.heardPeer && h.typ != packet1RTT {
		// The server picks its own connection ID, which the client uses from
		// its first response on (RFC 9000 section 7.2).
		c.heardPeer = true
		c.dcid = bytes.Clone(h.scid)
		c.peerCIDs[0] = peerCID{cid: c.dcid}
	}
	if !c.isClient && space == spaceHandshake && !c.spaces[spaceInitial].discarded {
		// Only the client could have produced a Handshake packet, so its
		// address is its own (RFC 9000 section 8.1), if a Retry has not
		// shown it already.
		c.addressValidated = true
		c.discardSpace(spaceInitial)
	}
	if h.typ == packet1RTT && c.earlyRx != nil {
		// A 1-RTT packet is the end of the client's early data
		// (RFC 9001 section 4.9.3).
		c.earlyRx = nil
	}
	c.lastActivity = now
	c.sentSinceRecv = false
	// The calls the packet's frames queue start here, which is where a
	// move to the path it came on goes (see migrateLocked).
	firstEvent := len(c.events)
	c.curEarly = early
	ackEliciting, err := c.handleFrames(space, payload, now)
	c.curEarly = false
	if err != nil {
		c.closeLocked(err)
		return true
	}
	if early {
		c.queueEarly(firstEvent)
	}
	if c.closed || s.discarded {
		return true
	}
	if c.rxPC != c.pc && !c.isClient && c.handshakeConfirmed && h.typ == packet1RTT &&
		c.nonProbing && int64(pn) > largest {
		c.migrateLocked(now, firstEvent)
	}
	if int64(pn) >= largest {
		s.largestRecvTime = now
	}
	if ackEliciting {
		s.ackPending = true
		s.unackedEliciting++
		switch {
		case space != spaceApp || s.unackedEliciting >= 2 || int64(pn) != largest+1:
			s.ackNow = true
		case s.ackDeadline.IsZero():
			s.ackDeadline = now.Add(c.local.maxAckDelay)
		}
	}
	return true
}

// queueEarly sees to the calls an early packet queued, from firstEvent on:
// they are held until the handshake is over if the handler is not one for
// early data, and otherwise preceded, the first time, by OnEarlyData.
func (c *Conn) queueEarly(firstEvent int) {
	if len(c.events) == firstEvent {
		return
	}
	if c.holdEarly {
		for _, e := range c.events[firstEvent:] {
			// The datagram may be given back before these run.
			e.data = bytes.Clone(e.data)
			c.heldEarly = append(c.heldEarly, e)
		}
		clear(c.events[firstEvent:])
		c.events = c.events[:firstEvent]
		return
	}
	if !c.earlyStarted {
		c.earlyStarted = true
		c.events = append(c.events, event{})
		copy(c.events[firstEvent+1:], c.events[firstEvent:])
		c.events[firstEvent] = event{kind: evEarlyData}
	}
}

// handleCrypto takes in handshake data and hands what is in order to TLS.
func (c *Conn) handleCrypto(space int, off uint64, data []byte) error {
	err := c.feedCrypto(space, off, data)
	if err == nil && !c.isClient && c.handshakeComplete && c.tls != nil {
		c.releaseTLS()
	}
	return err
}

// releaseTLS lets a server's TLS connection go once the handshake is over,
// its session ticket written: a client sends nothing more for TLS to read,
// and what TLS keeps of the handshake would otherwise live, and weigh, as
// long as the connection does. The state ConnectionState reports is kept.
// Callers hold c.mu.
func (c *Conn) releaseTLS() {
	state := c.tls.ConnectionState()
	c.tlsState = &state
	c.tls.Close()
	c.tls = nil
}

func (c *Conn) feedCrypto(space int, off uint64, data []byte) error {
	s := &c.spaces[space]
	if off+uint64(len(data)) > s.cryptoRecv.offset+maxCryptoBuffer {
		return transportErr(errCryptoBufferExceeds, "too much out-of-order handshake data")
	}
	level := [...]tls.QUICEncryptionLevel{
		tls.QUICEncryptionLevelInitial,
		tls.QUICEncryptionLevelHandshake,
		tls.QUICEncryptionLevelApplication,
	}[space]
	for chunk := s.cryptoRecv.take(off, data); ; chunk = nil {
		if chunk == nil {
			chunk = s.cryptoRecv.pop()
		}
		if chunk == nil {
			return nil
		}
		if c.tls == nil {
			// The handshake is over, and a client has no more to say to a
			// server's TLS (RFC 9001 section 4.1.3).
			return &TransportError{Code: errCryptoBase + alertUnexpectedMessage, Reason: "handshake data after the handshake"}
		}
		if err := c.tls.HandleData(level, chunk); err != nil {
			return tlsError(err)
		}
		if err := c.processTLSEvents(); err != nil {
			return err
		}
		if s.discarded {
			return nil
		}
	}
}

// wakeTLS takes the events of a handshake that has gone on by itself, and sends
// what they have to send. It is called from the goroutine that ran the
// certificate callback the handshake waited for.
func (c *Conn) wakeTLS() {
	c.mu.Lock()
	if !c.closed && c.tls != nil {
		if err := c.processTLSEvents(); err != nil {
			c.closeLocked(err)
		}
		c.wantFlush = true
	}
	givenUp := c.takeGivenUpLocked()
	c.mu.Unlock()
	closePaths(givenUp)
	c.dispatch()
}

// tlsError turns a TLS failure into the transport error that carries its
// alert (RFC 9001 section 4.8).
func tlsError(err error) error {
	var alert tls.AlertError
	if errors.As(err, &alert) {
		return &TransportError{Code: errCryptoBase + uint64(alert), Reason: err.Error()}
	}
	return &TransportError{Code: errInternal, Reason: err.Error()}
}

func (c *Conn) processTLSEvents() error {
	for {
		e := c.tls.NextEvent()
		space := spaceApp
		switch e.Level {
		case tls.QUICEncryptionLevelInitial:
			space = spaceInitial
		case tls.QUICEncryptionLevelHandshake:
			space = spaceHandshake
		case tls.QUICEncryptionLevelEarly:
			space = -1
		}
		switch e.Kind {
		case tls.QUICNoEvent:
			return nil
		case tls.QUICSetReadSecret, tls.QUICSetWriteSecret:
			k, err := newKeys(e.Suite, bytes.Clone(e.Data))
			if err != nil {
				return transportErr(errInternal, err.Error())
			}
			switch {
			case space < 0 && e.Kind == tls.QUICSetReadSecret && !c.isClient:
				c.earlyRx = k
			case space < 0 && e.Kind == tls.QUICSetWriteSecret && c.isClient && c.remembered != nil:
				c.earlyTx = k
				c.earlyOffered = true
			case space < 0:
			case e.Kind == tls.QUICSetReadSecret:
				c.spaces[space].rx = k
			default:
				c.spaces[space].tx = k
				if space == spaceApp {
					c.earlyTx = nil
				}
			}
		case tls.QUICResumeSession:
			if e.SessionState == nil || !e.SessionState.EarlyData {
				break
			}
			if c.isClient {
				// A client offers early data on a session whose limits it
				// has, and works to them until the server's arrive.
				p, ok := parseRemembered(e.SessionState.Extra)
				if !ok || !c.config.EarlyData {
					e.SessionState.EarlyData = false
					break
				}
				c.remembered = &p
				c.peerParams = &p
				c.peerMaxData, c.peerMaxBidi, c.peerMaxUni = p.initialMaxData, p.maxStreamsBidi, p.maxStreamsUni
			} else if !c.earlyAllowed(e.SessionState.Extra) {
				// A server takes early data only against the limits it gave
				// before.
				e.SessionState.EarlyData = false
			}
		case tls.QUICStoreSession:
			// The session a client can resume is kept with the server's
			// limits, which its early data will work to.
			if e.SessionState != nil && c.peerParamsRaw != nil {
				e.SessionState.Extra = append(e.SessionState.Extra, append([]byte{1}, c.peerParamsRaw...))
			}
			if st, ok := c.tls.(interface {
				StoreSession(*tls.SessionState) error
			}); ok && e.SessionState != nil {
				if err := st.StoreSession(e.SessionState); err != nil {
					return tlsError(err)
				}
			}
		case tls.QUICRejectedEarlyData:
			c.rejectEarlyLocked()
		case tls.QUICErrorEvent:
			return tlsError(e.Err)
		case tls.QUICWriteData:
			if space >= 0 && !c.spaces[space].discarded {
				c.spaces[space].cryptoSend.write(e.Data)
			}
		case tls.QUICTransportParameters:
			if err := c.handlePeerParams(e.Data); err != nil {
				return err
			}
		case tls.QUICTransportParametersRequired:
			c.tls.SetTransportParameters(c.local.encode())
		case tls.QUICHandshakeDone:
			if err := c.onHandshakeComplete(); err != nil {
				return err
			}
		}
	}
}

func (c *Conn) handlePeerParams(data []byte) error {
	p, err := parseParams(data, c.isClient)
	if err != nil {
		return err
	}
	if !p.hasInitialSCID || !bytes.Equal(p.initialSCID, c.dcid) {
		return transportErr(errTransportParameter, "initial_source_connection_id does not match")
	}
	if c.isClient {
		if !p.hasOriginalDCID || !bytes.Equal(p.originalDCID, c.odcid) {
			return transportErr(errTransportParameter, "original_destination_connection_id does not match")
		}
		if p.hasRetrySCID != (c.retrySCID != nil) || c.retrySCID != nil && !bytes.Equal(p.retrySCID, c.retrySCID) {
			return transportErr(errTransportParameter, "retry_source_connection_id does not match")
		}
		c.peerResetToken = p.statelessResetToken
		c.peerCIDs[0] = peerCID{cid: c.dcid, token: p.statelessResetToken}
	}
	c.peerParams = &p
	c.peerParamsRaw = bytes.Clone(data)
	c.peerMaxData = p.initialMaxData
	c.peerMaxBidi = p.maxStreamsBidi
	c.peerMaxUni = p.maxStreamsUni
	if c.remembered != nil {
		// Streams opened under the limits remembered take the real ones.
		for id, s := range c.streams {
			if s.hasSend {
				s.sendMax = c.streamSendMax(id)
			}
		}
	}
	if p.maxIdleTimeout > 0 && p.maxIdleTimeout < c.idleTimeout {
		c.idleTimeout = p.maxIdleTimeout
	}
	return nil
}

func (c *Conn) onHandshakeComplete() error {
	if c.peerParams == nil {
		return transportErr(errTransportParameter, "no transport parameters")
	}
	if c.remembered != nil && !c.earlyRejected {
		// The server took early data, sent under the limits it had given, and
		// must not have given less now (RFC 9000 section 7.4.1).
		if !limitsNotBelow(c.peerParams, c.remembered) {
			return transportErr(errProtocolViolation, "transport parameters below those remembered for 0-RTT")
		}
	}
	c.handshakeComplete = true
	c.maxDatagram = int(min(uint64(c.config.MaxDatagramSize), c.peerParams.maxUDPPayloadSize))
	if !c.isClient {
		c.handshakeDonePending = true
		c.confirmHandshake()
		if tc := c.config.TLSConfig; !tc.SessionTicketsDisabled {
			opts := tls.QUICSessionTicketOptions{}
			if c.config.EarlyData {
				opts.EarlyData = true
				opts.Extra = [][]byte{c.rememberedLimits()}
			}
			if err := c.tls.SendSessionTicket(opts); err != nil {
				return tlsError(err)
			}
		}
	}
	c.events = append(c.events, event{kind: evHandshake})
	if len(c.heldEarly) > 0 {
		// The early data that waited for the handshake comes after it.
		c.events = append(c.events, c.heldEarly...)
		clear(c.heldEarly)
		c.heldEarly = nil
	}
	return nil
}

// rejectEarlyLocked is the server turning a client's 0-RTT data away: it goes
// again under the 1-RTT keys.
func (c *Conn) rejectEarlyLocked() {
	if !c.isClient || c.earlyRejected {
		return
	}
	c.earlyRejected = true
	c.earlyTx = nil
	c.resendEarlyLocked()
}

// CanSendEarly reports whether a client's connection sends what it writes as
// 0-RTT data, before the handshake is over: its TLS resumed a session that
// allows it, and the server has not turned it away. What is written before
// the handshake is over is sent again if the server does turn it away, so
// that it arrives either way, but a server that accepts it may be sent it
// twice by an attacker (see Config.EarlyData).
func (c *Conn) CanSendEarly() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.earlyTx != nil
}

// EarlyDataAccepted reports whether a client's server took its 0-RTT data:
// it was offered, and not turned away. It is settled once the handshake is
// over.
func (c *Conn) EarlyDataAccepted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isClient && c.earlyOffered && !c.earlyRejected
}

// confirmHandshake is when the handshake is known to be over on both sides
// and its keys can go (RFC 9001 section 4.1.2).
func (c *Conn) confirmHandshake() {
	if c.handshakeConfirmed || !c.handshakeComplete {
		return
	}
	c.handshakeConfirmed = true
	c.discardSpace(spaceHandshake)
	c.startMTUDiscoveryLocked(c.now())
}

func (c *Conn) handleVersionNegotiation(d []byte) {
	if c.heardPeer {
		return
	}
	h, _ := parseHeader(d, 0)
	if !bytes.Equal(h.dcid, c.scid) || !bytes.Equal(h.scid, c.odcid) {
		return
	}
	p := 7 + len(h.dcid) + len(h.scid)
	for ; p+4 <= len(d); p += 4 {
		if d[p] == 0 && d[p+1] == 0 && d[p+2] == 0 && d[p+3] == 1 {
			// A version negotiation packet listing the version in use is
			// not genuine (RFC 9000 section 6.2).
			return
		}
	}
	c.terminateLocked(ErrVersionNegotiation)
}

// handleRetry restarts a client's handshake towards the connection ID a
// Retry packet names, with the token it carries (RFC 9000 section 17.2.5).
func (c *Conn) handleRetry(pkt []byte, h header) {
	if !c.isClient || c.heardPeer || c.retrySCID != nil || len(h.token) <= 16 {
		return
	}
	tagAt := len(pkt) - 16
	if !bytes.Equal(retryTag(c.odcid, pkt[:tagAt]), pkt[tagAt:]) || bytes.Equal(h.scid, c.dcid) {
		return
	}
	c.retrySCID = bytes.Clone(h.scid)
	c.dcid = c.retrySCID
	c.token = bytes.Clone(h.token[:len(h.token)-16])
	s := &c.spaces[spaceInitial]
	s.tx, s.rx = initialKeys(c.dcid, true)
	// Everything sent so far is sent again under the new keys.
	s.sent.each(s.sent.end(), func(p *sentPacket) bool {
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
		}
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
		return true
	})
	s.sent.reset()
	s.cryptoSend.onLost(0, s.cryptoSend.next)
	c.ptoCount = 0
	if c.earlyTx != nil {
		// The early data goes again, to the connection ID the Retry names.
		c.resendEarlyLocked()
	}
}

// closeLocked ends the connection with err, telling the peer.
func (c *Conn) closeLocked(err error) {
	if c.closed {
		return
	}
	c.sendClose(err)
	c.terminateLocked(err)
}

// sendClose sends CONNECTION_CLOSE in every space the peer may be able to
// read, since this side cannot tell which it has keys for.
func (c *Conn) sendClose(err error) {
	var plans []packetPlan
	size := 0
	padInitial := false
	for space := range c.spaces {
		s := &c.spaces[space]
		if s.tx == nil || s.discarded {
			continue
		}
		if space == spaceApp && !c.handshakeComplete {
			continue
		}
		payload := appendConnectionClose(nil, err, space == spaceApp)
		n := pnLen(s.nextPN, s.largestAcked)
		plan := packetPlan{space: space, pn: s.nextPN, pnLen: n, payload: payload, sp: &sentPacket{pn: s.nextPN}}
		s.nextPN++
		if space == spaceInitial && c.isClient {
			padInitial = true
		}
		plans = append(plans, plan)
		size += c.packetSize(&plan)
	}
	if len(plans) == 0 {
		return
	}
	if padInitial && size < minInitialDatagram {
		last := &plans[len(plans)-1]
		last.payload = append(last.payload, make([]byte, minInitialDatagram-size)...)
	}
	out := make([]byte, 0, max(size, minInitialDatagram))
	for i := range plans {
		out = c.sealPlan(out, &plans[i])
	}
	_ = c.pc.Send(out)
	c.closePacket = out
	c.closingUntil = c.now().Add(c.closingPeriod())
}

// minClosingPeriod is the shortest the closing period lasts. Three probe
// timeouts of this side's are what RFC 9000 asks for at least, but the peer
// probes by its own clock, which backs off and, on a loaded machine, can run
// far behind this side's estimate: a server whose probe came 314ms after its
// client closed, with the client's probe timeout at 26ms, was told of the
// close by nothing but its idle timeout.
const minClosingPeriod = time.Second

// closingPeriod is how long a connection that closed answers what arrives
// with its CONNECTION_CLOSE: three of its probe timeouts as they have backed
// off, at least minClosingPeriod, and no longer than its idle timeout, past
// which the peer has given up on the connection anyway.
func (c *Conn) closingPeriod() time.Duration {
	d := max(3*(c.rtt.pto()<<c.ptoCount), minClosingPeriod)
	if c.idleTimeout > 0 {
		d = min(d, c.idleTimeout)
	}
	return d
}

// answerClosingLocked answers n datagrams that arrived for a connection in
// its closing period with the CONNECTION_CLOSE it sent, again: the 1st, 2nd,
// 4th, 8th and so on, so that a peer that goes on sending gets fewer answers
// the more it sends (RFC 9000 section 10.2.1). Callers hold c.mu.
func (c *Conn) answerClosingLocked(n int) {
	if c.closePacket == nil || n == 0 || !c.now().Before(c.closingUntil) {
		return
	}
	before := c.closingSeen
	c.closingSeen += n
	// A power of two reached among those that just arrived.
	if bits.Len(uint(before)) != bits.Len(uint(c.closingSeen)) {
		_ = c.pc.Send(c.closePacket)
	}
}

// terminateLocked ends the connection at once, without telling the peer.
func (c *Conn) terminateLocked(err error) {
	if c.closed {
		return
	}
	c.closed = true
	c.closeErr = err
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.blocked != nil && !c.sending {
		bufferpool.Put(c.blocked)
		c.blocked = nil
	}
	c.wakeWritersLocked()
	for _, s := range c.streams {
		s.done = true
	}
	c.streams = nil
	c.sendQueue = nil
	c.buffered = nil
	c.events = append(c.events, event{kind: evClose})
}

// armTimerLocked sets the connection's one timer for the earliest thing
// that has to happen, unless it is set for sooner already.
func (c *Conn) armTimerLocked(now time.Time) {
	if c.closed {
		return
	}
	deadline := c.idleDeadline()
	earliest := func(t time.Time) {
		if !t.IsZero() && t.Before(deadline) {
			deadline = t
		}
	}
	if !c.handshakeComplete {
		earliest(c.created.Add(c.config.HandshakeTimeout))
	}
	earliest(c.lossDeadline)
	earliest(c.flushDeadline)
	earliest(c.sendAt)
	earliest(c.pathDeadline)
	earliest(c.closeDeadline)
	earliest(c.mtuProbeTimer())
	if s := &c.spaces[spaceApp]; s.ackPending && !s.ackNow {
		earliest(s.ackDeadline)
	}
	if c.config.KeepAlivePeriod > 0 && c.handshakeComplete {
		earliest(c.keepAliveAt())
	}
	if c.timer != nil && !c.timerAt.IsZero() && !deadline.Before(c.timerAt) {
		// The timer fires at or before the deadline as it is: one that
		// fires early finds nothing due and is set again then (see
		// onTimer), which costs less than setting it again for every round
		// of sending, each of which moves the probe timeout on.
		return
	}
	c.timerAt = deadline
	d := max(deadline.Sub(now), 0)
	if c.timer == nil {
		c.timer = time.AfterFunc(d, c.onTimer)
	} else {
		c.timer.Reset(d)
	}
}

// idleDeadline is when the connection idles out: never sooner than three
// probe timeouts, so that loss recovery gets its chance (RFC 9000 section
// 10.1).
func (c *Conn) idleDeadline() time.Time {
	return c.lastActivity.Add(max(c.idleTimeout, 3*c.rtt.pto()))
}

// keepAliveAt is when a quiet connection next needs a PING.
func (c *Conn) keepAliveAt() time.Time {
	last := c.lastActivity
	if t := c.spaces[spaceApp].lastAckElicitingSent; t.After(last) {
		last = t
	}
	return last.Add(c.config.KeepAlivePeriod)
}

func (c *Conn) onTimer() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.timerAt = time.Time{}
	now := c.now()
	switch {
	case !c.handshakeComplete && !now.Before(c.created.Add(c.config.HandshakeTimeout)):
		c.terminateLocked(ErrHandshakeTimeout)
	case !now.Before(c.idleDeadline()):
		c.terminateLocked(ErrIdleTimeout)
	case !c.closeDeadline.IsZero() && !now.Before(c.closeDeadline):
		c.closeIfDoneLocked()
		c.armTimerLocked(now)
	default:
		due := false
		if !c.pathDeadline.IsZero() && !now.Before(c.pathDeadline) {
			c.pathFailedLocked()
			due = true
		}
		if !c.lossDeadline.IsZero() && !now.Before(c.lossDeadline) {
			c.onLossDetectionTimeout(now)
			due = true
		}
		if c.config.KeepAlivePeriod > 0 && c.handshakeComplete && !now.Before(c.keepAliveAt()) {
			c.pingPending = true
			due = true
		}
		if !c.flushDeadline.IsZero() && !now.Before(c.flushDeadline) {
			// Writes expected that have held the rest back for flushHold are
			// not coming soon, a handler that keeps its request, say: they no
			// longer hold anything back.
			c.awaited = 0
			due = true
		}
		if s := &c.spaces[spaceApp]; s.ackPending && !s.ackDeadline.IsZero() && !now.Before(s.ackDeadline) {
			due = true
		}
		if t := c.mtuProbeTimer(); !t.IsZero() && !now.Before(t) {
			// Path MTU discovery searches again.
			due = true
		}
		if !c.sendAt.IsZero() && !now.Before(c.sendAt) {
			// Pacing has a datagram's worth again, or the socket that was
			// full may have room.
			c.sendAt = time.Time{}
			due = true
		}
		if due {
			// The timer sends whether or not the handler is being called:
			// loss recovery does not wait for a handler that takes its time.
			c.wantFlush = true
			c.sendLocked()
		} else {
			// The timer was set for something that has since been done or
			// moved, a probe timeout the acknowledgement just handled made
			// moot, say: what there is to send keeps waiting for the
			// handler, or it would leave the acknowledgement of a request
			// without the answer being written to it.
			c.armTimerLocked(now)
		}
	}
	givenUp := c.takeGivenUpLocked()
	c.mu.Unlock()
	closePaths(givenUp)
	c.dispatch()
}

// endClosing ends the closing period: the CONNECTION_CLOSE is no longer sent
// again, and the paths it went on close.
func (c *Conn) endClosing(paths []PacketConn) {
	c.mu.Lock()
	c.closePacket = nil
	c.mu.Unlock()
	closePaths(paths)
}
