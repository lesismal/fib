package sctp

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

var (
	ErrClosed          = errors.New("sctp: association closed")
	ErrMalformed       = errors.New("sctp: malformed packet")
	ErrChecksum        = errors.New("sctp: bad checksum")
	ErrVerificationTag = errors.New("sctp: wrong verification tag")
	ErrBadCookie       = errors.New("sctp: invalid state cookie")
	ErrMessageTooLarge = errors.New("sctp: message too large")
	ErrEmptyMessage    = errors.New("sctp: empty message")
	ErrInvalidStream   = errors.New("sctp: invalid stream")
	ErrStreamClosing   = errors.New("sctp: stream is being reset")
	ErrAborted         = errors.New("sctp: association aborted by peer")
	ErrTimeout         = errors.New("sctp: peer stopped responding")
	ErrProtocol        = errors.New("sctp: protocol violation")
)

// State is where an association is in RFC 9260's state diagram (section 4).
type State uint8

const (
	StateClosed State = iota
	StateCookieWait
	StateCookieEchoed
	StateEstablished
	StateShutdownPending
	StateShutdownSent
	StateShutdownReceived
	StateShutdownAckSent
)

var stateNames = [...]string{"closed", "cookie-wait", "cookie-echoed", "established",
	"shutdown-pending", "shutdown-sent", "shutdown-received", "shutdown-ack-sent"}

func (s State) String() string {
	if int(s) < len(stateNames) {
		return stateNames[s]
	}
	return fmt.Sprintf("State(%d)", uint8(s))
}

// Defaults for Config's zero fields.
const (
	DefaultPort              = 5000
	DefaultMTU               = 1200
	DefaultMaxMessageSize    = 256 << 10
	DefaultRemoteMaxMessage  = 64 << 10
	DefaultReceiveWindow     = 1 << 20
	DefaultStreams           = 65535
	DefaultRTOInitial        = time.Second
	DefaultRTOMin            = 200 * time.Millisecond
	DefaultRTOMax            = 60 * time.Second
	DefaultMaxRetransmits    = 10
	DefaultMaxInitRetransmit = 8
	DefaultHeartbeatInterval = 30 * time.Second
	DefaultSACKDelay         = 200 * time.Millisecond
	cookieLifetime           = 60 * time.Second
)

// Config sets up an Association. Only Output is required.
type Config struct {
	// Output sends one SCTP packet, as DTLS application data. The buffer is
	// only valid during the call. Output must not call back into the
	// Association.
	Output func(packet []byte)

	// Client is whether this side is the DTLS client, which picks even
	// stream ids for the data channels it opens, the server odd ones
	// (RFC 8832 section 6).
	Client bool

	// LocalPort and RemotePort are the SCTP ports from SDP's sctp-port,
	// 5000 when zero.
	LocalPort, RemotePort uint16

	// MTU bounds the packets the association builds: DTLS records over
	// UDP of 1200 bytes when zero.
	MTU int

	// MaxMessageSize is the largest message this side reassembles; a peer
	// that sends a larger one is aborted. 256 KiB when zero. It is what
	// SDP's max-message-size should advertise.
	MaxMessageSize int

	// RemoteMaxMessageSize is the peer's max-message-size from SDP: Send
	// refuses larger messages. Zero means 64 KiB, the default RFC 8841
	// gives when SDP leaves it out; a negative value means no limit (SDP's
	// max-message-size:0).
	RemoteMaxMessageSize int

	// ReceiveWindow is the advertised receiver window, 1 MiB when zero.
	ReceiveWindow int

	// Streams is the number of streams offered each way, 65535 when zero.
	Streams uint16

	// RTOInitial, RTOMin and RTOMax bound the retransmission timeout
	// (RFC 9260 section 6.3). The minimum defaults below RFC 9260's 1s,
	// as browsers' stacks do, since DTLS over a WebRTC path is
	// interactive.
	RTOInitial, RTOMin, RTOMax time.Duration

	// MaxRetransmits is how many timeouts in a row the association
	// survives before it closes with ErrTimeout (Association.Max.Retrans),
	// and MaxInitRetransmits the same for INIT and COOKIE ECHO.
	MaxRetransmits, MaxInitRetransmits int

	// HeartbeatInterval is how often an idle association probes its peer;
	// negative turns heartbeats off.
	HeartbeatInterval time.Duration

	// SACKDelay is how long a SACK waits for a second packet to acknowledge
	// with it (RFC 9260 section 6.2), 200ms when zero.
	SACKDelay time.Duration

	// CookieSecret keys the HMAC of the state cookie. A random one is made
	// when nil.
	CookieSecret []byte

	// Now is the clock for calls that take no time of their own (Send and
	// the data channel methods). time.Now when nil.
	Now func() time.Time

	// OnEstablished runs when the association comes up.
	OnEstablished func()
	// OnClose runs once when the association ends: nil after a graceful
	// shutdown or a local Abort, otherwise why.
	OnClose func(err error)
	// OnMessage receives messages on streams that carry no data channel.
	// data is only valid during the call.
	OnMessage func(stream uint16, ppid uint32, data []byte)
	// OnDataChannel runs for each data channel the peer opens. The handler
	// should set the channel's callbacks before it returns.
	OnDataChannel func(ch *DataChannel)
}

// Association is one SCTP association over a DTLS connection. It is
// sans-IO: the caller feeds it packets with HandlePacket and the passage of
// time with HandleTimeout, at NextTimeout, and it sends through
// Config.Output. Calls must not overlap; callbacks run inside them and may
// call Send and the data channel methods, whose output is then sent as the
// outer call returns.
type Association struct {
	cfg        Config
	mtu        int
	maxPayload int

	state  State
	closed bool
	now    time.Time
	depth  int

	localTag, peerTag uint32
	localTSN          uint32
	outStreams        uint16
	inStreams         uint16
	peerForwardTSN    bool
	peerReconfig      bool

	// Handshake.
	cookieEcho []byte
	t1         time.Time
	t1RTO      time.Duration
	initTries  int

	// Retransmission timing (RFC 9260 section 6.3).
	rto, srtt, rttvar time.Duration
	rttValid          bool
	errorCount        int

	sender
	receiver
	reconfig

	streams []*stream
	chans   []*DataChannel

	// Heartbeats.
	hbAt      time.Time
	hbPending bool
	hbNonce   uint64

	// Shutdown.
	t2 time.Time

	out    []byte
	pool   bufPool
	secret []byte
}

// stream is one stream id's state both ways, with the data channel on it.
type stream struct {
	// Inbound: the next ordered SSN to deliver and chunks waiting for
	// their message to complete or for their turn.
	nextSSN   uint16
	ordered   []rchunk
	unordered []rchunk
	// headN and headBytes remember how much of the head of ordered is
	// known to be the contiguous start of the next message, so a large
	// message arriving in order is checked once, not once per fragment.
	headN, headBytes int
	// asm is a message being reassembled in order, while asmActive.
	asm       rchunk
	asmActive bool

	// Outbound.
	ssn       uint16
	buffered  int
	queued    int
	resetting bool

	ch *DataChannel
}

// NewAssociation makes an association in the closed state. Call Connect to
// send INIT, or wait for the peer's; WebRTC endpoints usually both connect,
// which RFC 9260's handling of colliding INITs resolves.
func NewAssociation(cfg Config) *Association {
	if cfg.LocalPort == 0 {
		cfg.LocalPort = DefaultPort
	}
	if cfg.RemotePort == 0 {
		cfg.RemotePort = DefaultPort
	}
	if cfg.MTU <= 0 {
		cfg.MTU = DefaultMTU
	}
	if cfg.MTU < 256 {
		cfg.MTU = 256
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = DefaultMaxMessageSize
	}
	if cfg.RemoteMaxMessageSize == 0 {
		cfg.RemoteMaxMessageSize = DefaultRemoteMaxMessage
	}
	if cfg.ReceiveWindow <= 0 {
		cfg.ReceiveWindow = DefaultReceiveWindow
	}
	if cfg.ReceiveWindow < cfg.MTU*4 {
		cfg.ReceiveWindow = cfg.MTU * 4
	}
	if cfg.Streams == 0 {
		cfg.Streams = DefaultStreams
	}
	if cfg.RTOInitial <= 0 {
		cfg.RTOInitial = DefaultRTOInitial
	}
	if cfg.RTOMin <= 0 {
		cfg.RTOMin = DefaultRTOMin
	}
	if cfg.RTOMax <= 0 {
		cfg.RTOMax = DefaultRTOMax
	}
	if cfg.MaxRetransmits <= 0 {
		cfg.MaxRetransmits = DefaultMaxRetransmits
	}
	if cfg.MaxInitRetransmits <= 0 {
		cfg.MaxInitRetransmits = DefaultMaxInitRetransmit
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.SACKDelay <= 0 {
		cfg.SACKDelay = DefaultSACKDelay
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Output == nil {
		cfg.Output = func([]byte) {}
	}
	a := &Association{
		cfg:        cfg,
		mtu:        cfg.MTU,
		maxPayload: cfg.MTU - headerSize - dataHeaderSize,
		rto:        cfg.RTOInitial,
		out:        make([]byte, headerSize, cfg.MTU),
		secret:     cfg.CookieSecret,
	}
	var r [16]byte
	_, _ = rand.Read(r[:])
	a.localTag = binary.BigEndian.Uint32(r[0:])
	if a.localTag == 0 {
		a.localTag = 1
	}
	a.localTSN = binary.BigEndian.Uint32(r[4:])
	a.hbNonce = binary.BigEndian.Uint64(r[8:])
	if a.secret == nil {
		a.secret = make([]byte, 32)
		_, _ = rand.Read(a.secret)
	}
	a.sender.init(a.localTSN, a.mtu)
	a.reqSeq = a.localTSN
	return a
}

// State reports where the association is.
func (a *Association) State() State { return a.state }

// SetRemoteMaxMessageSize sets what Config.RemoteMaxMessageSize sets, for a
// peer whose SDP arrives after the association is made.
func (a *Association) SetRemoteMaxMessageSize(n int) {
	if n == 0 {
		n = -1
	}
	a.cfg.RemoteMaxMessageSize = n
}

// enter and exit bracket every call from outside: output that callbacks
// cause while one runs is gathered and sent as the outermost returns.
func (a *Association) enter(now time.Time) {
	a.now = now
	a.depth++
}

func (a *Association) exit() {
	a.depth--
	if a.depth > 0 {
		return
	}
	if a.closed {
		a.sendPacket()
		return
	}
	a.flush()
}

// Connect starts the association from this side by sending INIT.
func (a *Association) Connect(now time.Time) {
	if a.closed || a.state != StateClosed {
		return
	}
	a.enter(now)
	defer a.exit()
	a.state = StateCookieWait
	a.t1RTO = a.cfg.RTOInitial
	a.t1 = now.Add(a.t1RTO)
	a.sendInit()
}

// HandlePacket processes one SCTP packet the peer sent. A packet that fails
// its checksum or verification tag is dropped and the error says why; the
// association carries on.
func (a *Association) HandlePacket(b []byte, now time.Time) error {
	if a.closed {
		return ErrClosed
	}
	vtag, chunks, err := parsePacket(b)
	if err != nil {
		return err
	}
	a.enter(now)
	defer a.exit()
	return a.handleChunks(vtag, chunks)
}

func (a *Association) handleChunks(vtag uint32, chunks []byte) error {
	gapsBefore := len(a.gaps) > 0
	gotData := false
	defer func() {
		if gotData && !a.closed {
			a.afterData(gapsBefore)
		}
	}()
	for first := true; len(chunks) > 0 && !a.closed; first = false {
		typ, flags, v, rest, err := nextChunk(chunks)
		if err != nil {
			return err
		}
		chunks = rest
		if typ == chunkInit {
			// RFC 9260 section 6.10: INIT is alone in its packet, with a
			// zero tag.
			if !first || len(rest) != 0 || vtag != 0 {
				return ErrMalformed
			}
			return a.handleInit(v)
		}
		want := a.localTag
		if (typ == chunkAbort || typ == chunkShutdownComplete) && flags&flagTagReflected != 0 {
			want = a.peerTag
		}
		if vtag != want || want == 0 {
			return ErrVerificationTag
		}
		switch typ {
		case chunkData:
			gotData = true
			err = a.handleData(flags, v)
		case chunkSack:
			err = a.handleSack(v)
		case chunkInitAck:
			err = a.handleInitAck(v)
		case chunkCookieEcho:
			err = a.handleCookieEcho(v)
		case chunkCookieAck:
			if a.state == StateCookieEchoed {
				a.establish()
			}
		case chunkHeartbeat:
			a.handleHeartbeat(v)
		case chunkHeartbeatAck:
			a.handleHeartbeatAck(v)
		case chunkAbort:
			a.handleAbort(v)
		case chunkShutdown:
			err = a.handleShutdown(v)
		case chunkShutdownAck:
			a.handleShutdownAck()
		case chunkShutdownComplete:
			if a.state == StateShutdownAckSent {
				a.terminate(nil)
			}
		case chunkError:
			// Errors the peer reports change nothing here: the only one
			// that matters to a WebRTC association, a stale cookie, cannot
			// happen to an INIT this side retransmits with a fresh one.
		case chunkForwardTSN:
			gotData = true
			err = a.handleForwardTSN(v)
		case chunkReconfig:
			err = a.handleReconfig(v)
		default:
			// RFC 9260 section 3.2: the top two bits of an unknown type say
			// whether to go on and whether to report it.
			if typ&0x40 != 0 {
				a.reportChunk(typ, flags, v)
			}
			if typ&0x80 == 0 {
				return nil
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// reportChunk reports an unrecognized chunk in an ERROR chunk, quoting
// the start of it.
func (a *Association) reportChunk(typ, flags uint8, v []byte) {
	if a.peerTag == 0 {
		return
	}
	if len(v) > 64 {
		v = v[:64]
	}
	n := 4 + 4 + len(v)
	b := a.reserve(chunkError, 0, n)
	binary.BigEndian.PutUint16(b, causeUnrecognizedChunk)
	binary.BigEndian.PutUint16(b[2:], uint16(n))
	b[4], b[5] = typ, flags
	binary.BigEndian.PutUint16(b[6:], uint16(4+len(v)))
	copy(b[8:], v)
}

// NextTimeout reports when HandleTimeout next has work to do.
func (a *Association) NextTimeout() (time.Time, bool) {
	if a.closed {
		return time.Time{}, false
	}
	var t time.Time
	for _, d := range [...]time.Time{a.t1, a.t2, a.t3, a.sackAt, a.hbAt, a.reconfAt} {
		if !d.IsZero() && (t.IsZero() || d.Before(t)) {
			t = d
		}
	}
	return t, !t.IsZero()
}

// HandleTimeout runs the timers that are due at now.
func (a *Association) HandleTimeout(now time.Time) {
	if a.closed {
		return
	}
	a.enter(now)
	defer a.exit()
	due := func(t time.Time) bool { return !t.IsZero() && !now.Before(t) }
	if due(a.t1) {
		a.onT1()
	}
	if due(a.t3) && !a.closed {
		a.onT3()
	}
	if due(a.sackAt) {
		a.sackAt = time.Time{}
		a.sackNow = true
	}
	if due(a.hbAt) && !a.closed {
		a.onHeartbeatTimer()
	}
	if due(a.t2) && !a.closed {
		a.onT2()
	}
	if due(a.reconfAt) && !a.closed {
		a.reconfAt = time.Time{}
		a.reconfResend = true
	}
}

func (a *Association) onT1() {
	a.initTries++
	if a.initTries > a.cfg.MaxInitRetransmits {
		a.terminate(fmt.Errorf("%w: during the handshake", ErrTimeout))
		return
	}
	a.t1RTO = min(2*a.t1RTO, a.cfg.RTOMax)
	a.t1 = a.now.Add(a.t1RTO)
	switch a.state {
	case StateCookieWait:
		a.sendInit()
	case StateCookieEchoed:
		a.sendCookieEcho()
	default:
		a.t1 = time.Time{}
	}
}

// sendInit sends INIT alone, with a zero tag.
func (a *Association) sendInit() {
	a.sendPacket()
	a.appendInit(chunkInit, nil)
	a.sendPacketTag(0)
}

// appendInit writes INIT or INIT ACK; the latter carries a state cookie.
// The tag and initial TSN are this association's, whichever side starts,
// which is what lets colliding INITs (RFC 9260 section 5.2.1) meet.
func (a *Association) appendInit(typ uint8, cookie []byte) {
	n := 16 + 8 + 4
	if cookie != nil {
		n += 4 + len(cookie)
	}
	b := a.reserve(typ, 0, n)
	binary.BigEndian.PutUint32(b, a.localTag)
	binary.BigEndian.PutUint32(b[4:], uint32(a.cfg.ReceiveWindow))
	binary.BigEndian.PutUint16(b[8:], a.cfg.Streams)
	binary.BigEndian.PutUint16(b[10:], a.cfg.Streams)
	binary.BigEndian.PutUint32(b[12:], a.localTSN)
	p := b[16:]
	binary.BigEndian.PutUint16(p, paramSupportedExtensions)
	binary.BigEndian.PutUint16(p[2:], 6)
	p[4], p[5], p[6], p[7] = chunkReconfig, chunkForwardTSN, 0, 0
	binary.BigEndian.PutUint16(p[8:], paramForwardTSNSupported)
	binary.BigEndian.PutUint16(p[10:], 4)
	if cookie != nil {
		binary.BigEndian.PutUint16(p[12:], paramStateCookie)
		binary.BigEndian.PutUint16(p[14:], uint16(4+len(cookie)))
		copy(p[16:], cookie)
	}
}

const cookieSize = 64

func (a *Association) handleInit(v []byte) error {
	var c initChunk
	if err := parseInit(v, &c); err != nil {
		return err
	}
	switch a.state {
	case StateShutdownAckSent, StateShutdownSent, StateShutdownReceived, StateShutdownPending:
		return nil
	}
	// The cookie holds what the peer's INIT said, so that this side keeps
	// nothing until the peer echoes it, and the MAC proves it ours.
	var ck [cookieSize]byte
	binary.BigEndian.PutUint32(ck[0:], a.localTag)
	binary.BigEndian.PutUint32(ck[4:], c.tag)
	binary.BigEndian.PutUint32(ck[8:], c.tsn)
	binary.BigEndian.PutUint32(ck[12:], c.rwnd)
	binary.BigEndian.PutUint16(ck[16:], c.outStreams)
	binary.BigEndian.PutUint16(ck[18:], c.inStreams)
	if c.forwardTSN {
		ck[20] |= 1
	}
	if c.reconfig {
		ck[20] |= 2
	}
	binary.BigEndian.PutUint64(ck[24:], uint64(a.now.UnixNano()))
	m := hmac.New(sha256.New, a.secret)
	m.Write(ck[:32])
	m.Sum(ck[:32])
	a.sendPacket()
	a.appendInit(chunkInitAck, ck[:])
	a.sendPacketTag(c.tag)
	return nil
}

func (a *Association) handleInitAck(v []byte) error {
	if a.state != StateCookieWait {
		return nil
	}
	var c initChunk
	if err := parseInit(v, &c); err != nil {
		return err
	}
	if c.cookie == nil {
		return fmt.Errorf("%w: INIT ACK without a state cookie", ErrMalformed)
	}
	a.setPeer(c.tag, c.tsn, c.rwnd, c.outStreams, c.inStreams, c.forwardTSN, c.reconfig)
	a.cookieEcho = append(a.cookieEcho[:0], c.cookie...)
	a.state = StateCookieEchoed
	a.initTries = 0
	a.t1RTO = a.cfg.RTOInitial
	a.t1 = a.now.Add(a.t1RTO)
	a.sendCookieEcho()
	return nil
}

func (a *Association) sendCookieEcho() {
	a.sendPacket()
	b := a.reserve(chunkCookieEcho, 0, len(a.cookieEcho))
	copy(b, a.cookieEcho)
	a.sendPacket()
}

func (a *Association) handleCookieEcho(v []byte) error {
	if len(v) != cookieSize {
		return ErrBadCookie
	}
	m := hmac.New(sha256.New, a.secret)
	m.Write(v[:32])
	var sum [32]byte
	if !hmac.Equal(m.Sum(sum[:0]), v[32:]) || binary.BigEndian.Uint32(v) != a.localTag {
		return ErrBadCookie
	}
	age := a.now.Sub(time.Unix(0, int64(binary.BigEndian.Uint64(v[24:]))))
	if age > cookieLifetime || age < -cookieLifetime {
		return fmt.Errorf("%w: stale", ErrBadCookie)
	}
	peerTag := binary.BigEndian.Uint32(v[4:])
	switch a.state {
	case StateClosed, StateCookieWait, StateCookieEchoed:
		a.setPeer(peerTag, binary.BigEndian.Uint32(v[8:]), binary.BigEndian.Uint32(v[12:]),
			binary.BigEndian.Uint16(v[16:]), binary.BigEndian.Uint16(v[18:]), v[20]&1 != 0, v[20]&2 != 0)
		a.reserve(chunkCookieAck, 0, 0)
		a.establish()
	default:
		// RFC 9260 section 5.2.4 case D: our COOKIE ACK was lost. A peer
		// that restarted with a new tag is not supported and is ignored.
		if peerTag == a.peerTag {
			a.reserve(chunkCookieAck, 0, 0)
		}
	}
	return nil
}

// setPeer takes on what the peer's INIT or INIT ACK said, before the
// association is up.
func (a *Association) setPeer(tag, tsn, rwnd uint32, os, mis uint16, fwd, reconf bool) {
	a.peerTag = tag
	a.peerCum = tsn - 1
	a.peerReqSeq = tsn
	a.lastResult = reconfigSuccessNothing
	a.outStreams = min(a.cfg.Streams, mis)
	a.inStreams = min(a.cfg.Streams, os)
	a.peerForwardTSN = fwd
	a.peerReconfig = reconf
	a.rwnd = int(rwnd)
	a.ssthresh = int(rwnd)
}

func (a *Association) establish() {
	a.state = StateEstablished
	a.t1 = time.Time{}
	a.cookieEcho = nil
	a.errorCount = 0
	a.scheduleHeartbeat()
	if a.cfg.OnEstablished != nil {
		a.cfg.OnEstablished()
	}
	a.openChannels()
}

func (a *Association) scheduleHeartbeat() {
	if a.cfg.HeartbeatInterval > 0 {
		a.hbAt = a.now.Add(a.cfg.HeartbeatInterval + a.rto)
	}
}

// onHeartbeatTimer probes an idle peer; while data is outstanding T3-rtx
// watches the path instead.
func (a *Association) onHeartbeatTimer() {
	a.scheduleHeartbeat()
	if a.state != StateEstablished || a.outstanding() {
		return
	}
	if a.hbPending {
		a.errorCount++
		if a.errorCount > a.cfg.MaxRetransmits {
			a.terminate(ErrTimeout)
			return
		}
		a.rto = min(2*a.rto, a.cfg.RTOMax)
	}
	a.hbPending = true
	a.hbNonce++
	b := a.reserve(chunkHeartbeat, 0, 20)
	binary.BigEndian.PutUint16(b, paramHeartbeatInfo)
	binary.BigEndian.PutUint16(b[2:], 20)
	binary.BigEndian.PutUint64(b[4:], uint64(a.now.UnixNano()))
	binary.BigEndian.PutUint64(b[12:], a.hbNonce)
}

func (a *Association) handleHeartbeat(v []byte) {
	if a.peerTag == 0 {
		return
	}
	b := a.reserve(chunkHeartbeatAck, 0, len(v))
	copy(b, v)
}

func (a *Association) handleHeartbeatAck(v []byte) {
	if len(v) != 20 || binary.BigEndian.Uint16(v) != paramHeartbeatInfo ||
		binary.BigEndian.Uint64(v[12:]) != a.hbNonce || !a.hbPending {
		return
	}
	a.hbPending = false
	a.errorCount = 0
	sent := time.Unix(0, int64(binary.BigEndian.Uint64(v[4:])))
	if r := a.now.Sub(sent); r >= 0 {
		a.rttSample(r)
	}
}

// rttSample updates SRTT, RTTVAR and RTO by RFC 9260 section 6.3.1.
func (a *Association) rttSample(r time.Duration) {
	if !a.rttValid {
		a.srtt = r
		a.rttvar = r / 2
		a.rttValid = true
	} else {
		d := a.srtt - r
		if d < 0 {
			d = -d
		}
		a.rttvar = (3*a.rttvar + d) / 4
		a.srtt = (7*a.srtt + r) / 8
	}
	a.rto = min(max(a.srtt+4*a.rttvar, a.cfg.RTOMin), a.cfg.RTOMax)
}

func (a *Association) handleAbort(v []byte) {
	err := ErrAborted
	if len(v) >= 4 {
		err = fmt.Errorf("%w (cause %d)", ErrAborted, binary.BigEndian.Uint16(v))
	}
	a.out = a.out[:headerSize]
	a.terminate(err)
}

// Abort ends the association at once, telling the peer with ABORT.
func (a *Association) Abort() {
	if a.closed {
		return
	}
	a.enter(a.cfg.Now())
	defer a.exit()
	a.abort(nil, causeUserInitiatedAbort)
}

// abort sends ABORT with cause and ends the association with err.
func (a *Association) abort(err error, cause uint16) {
	if a.peerTag != 0 {
		a.out = a.out[:headerSize]
		b := a.reserve(chunkAbort, 0, 4)
		binary.BigEndian.PutUint16(b, cause)
		binary.BigEndian.PutUint16(b[2:], 4)
		a.sendPacket()
	}
	a.terminate(err)
}

// Shutdown closes the association gracefully: data already queued is
// delivered first (RFC 9260 section 9.2).
func (a *Association) Shutdown() {
	if a.closed {
		return
	}
	a.enter(a.cfg.Now())
	defer a.exit()
	switch a.state {
	case StateEstablished:
		a.state = StateShutdownPending
	case StateClosed, StateCookieWait, StateCookieEchoed:
		a.abort(nil, causeUserInitiatedAbort)
	}
}

func (a *Association) handleShutdown(v []byte) error {
	if len(v) < 4 {
		return ErrMalformed
	}
	switch a.state {
	case StateEstablished, StateShutdownPending, StateShutdownReceived:
		a.state = StateShutdownReceived
		a.ackUpTo(binary.BigEndian.Uint32(v))
	case StateShutdownSent:
		a.ackUpTo(binary.BigEndian.Uint32(v))
		a.state = StateShutdownAckSent
		a.reserve(chunkShutdownAck, 0, 0)
		a.t2 = a.now.Add(a.rto)
	}
	return nil
}

func (a *Association) handleShutdownAck() {
	switch a.state {
	case StateShutdownSent, StateShutdownAckSent:
		a.reserve(chunkShutdownComplete, 0, 0)
		a.sendPacket()
		a.terminate(nil)
	}
}

// checkShutdown moves a shutdown on once nothing is left to send.
func (a *Association) checkShutdown() {
	if a.hasPending() || a.outstanding() {
		return
	}
	switch a.state {
	case StateShutdownPending:
		a.state = StateShutdownSent
		a.appendShutdown()
		a.t2 = a.now.Add(a.rto)
	case StateShutdownReceived:
		a.state = StateShutdownAckSent
		a.reserve(chunkShutdownAck, 0, 0)
		a.t2 = a.now.Add(a.rto)
	}
}

func (a *Association) appendShutdown() {
	b := a.reserve(chunkShutdown, 0, 4)
	binary.BigEndian.PutUint32(b, a.peerCum)
}

func (a *Association) onT2() {
	a.errorCount++
	if a.errorCount > a.cfg.MaxRetransmits {
		a.terminate(ErrTimeout)
		return
	}
	a.rto = min(2*a.rto, a.cfg.RTOMax)
	a.t2 = a.now.Add(a.rto)
	switch a.state {
	case StateShutdownSent:
		a.appendShutdown()
	case StateShutdownAckSent:
		a.reserve(chunkShutdownAck, 0, 0)
	default:
		a.t2 = time.Time{}
	}
}

// terminate ends the association: its channels close, then OnClose runs.
func (a *Association) terminate(err error) {
	if a.closed {
		return
	}
	a.sendPacket()
	a.closed = true
	a.state = StateClosed
	a.t1, a.t2, a.t3, a.sackAt, a.hbAt, a.reconfAt = time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Time{}, time.Time{}
	chans := a.chans
	a.chans = nil
	for _, ch := range chans {
		ch.closed()
	}
	if a.cfg.OnClose != nil {
		a.cfg.OnClose(err)
	}
}

// stream returns id's stream, made on first use.
func (a *Association) stream(id uint16) *stream {
	if int(id) < len(a.streams) {
		if s := a.streams[id]; s != nil {
			return s
		}
	} else {
		n := max(int(id)+1, 2*len(a.streams))
		n = min(n, 65536)
		s := make([]*stream, n)
		copy(s, a.streams)
		a.streams = s
	}
	s := &stream{}
	a.streams[id] = s
	return s
}

// reserve appends a chunk of type typ with an n-byte value to the packet
// being built, sending that packet first if the chunk would not fit, and
// returns the value for the caller to fill.
func (a *Association) reserve(typ, flags uint8, n int) []byte {
	l := chunkHeaderSize + n
	p := (l + 3) &^ 3
	if len(a.out)+p > a.mtu && len(a.out) > headerSize {
		a.sendPacket()
	}
	off := len(a.out)
	if off+p > cap(a.out) {
		b := make([]byte, off, off+p)
		copy(b, a.out)
		a.out = b
	}
	a.out = a.out[:off+p]
	c := a.out[off:]
	c[0], c[1] = typ, flags
	binary.BigEndian.PutUint16(c[2:], uint16(l))
	clear(c[l:p])
	return c[chunkHeaderSize:l]
}

// room is how many bytes the packet being built has left.
func (a *Association) room() int { return a.mtu - len(a.out) }

func (a *Association) sendPacket() { a.sendPacketTag(a.peerTag) }

func (a *Association) sendPacketTag(tag uint32) {
	b := a.out
	if len(b) <= headerSize {
		return
	}
	binary.BigEndian.PutUint16(b, a.cfg.LocalPort)
	binary.BigEndian.PutUint16(b[2:], a.cfg.RemotePort)
	binary.BigEndian.PutUint32(b[4:], tag)
	binary.LittleEndian.PutUint32(b[8:], checksum(b))
	a.cfg.Output(b)
	a.out = b[:headerSize]
}

// flush sends what the association has to send: acknowledgements and
// other control chunks first, then retransmissions and new data as the
// congestion and receiver windows allow, bundled up to the MTU.
func (a *Association) flush() {
	switch a.state {
	case StateEstablished, StateShutdownPending, StateShutdownReceived, StateShutdownSent, StateShutdownAckSent:
	default:
		a.sendPacket()
		return
	}
	if a.sackNow || (!a.sackAt.IsZero() && a.wantsToSend()) {
		a.appendSack()
	}
	a.flushReconfig()
	a.flushData()
	if a.closed {
		a.sendPacket()
		return
	}
	if a.state != StateEstablished {
		a.checkShutdown()
	}
	a.sendPacket()
}
