package webrtc

import (
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc/ice"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sctp"
	"github.com/lesismal/fib/webrtc/sdp"
	"github.com/lesismal/fib/webrtc/srtp"
	"github.com/lesismal/fib/webrtc/turn"
)

var (
	// ErrClosed is what a closed PeerConnection's methods return.
	ErrClosed = errors.New("webrtc: peer connection closed")
	// ErrSignalingState is what a description or candidate out of turn
	// returns: an answer with no offer, a second offer before the answer.
	ErrSignalingState = errors.New("webrtc: description not allowed in this signaling state")
	// ErrDescription is what a session description the PeerConnection
	// cannot use returns.
	ErrDescription = errors.New("webrtc: invalid session description")
	// ErrFingerprint is what a DTLS handshake fails with when the peer's
	// certificate matches no fingerprint of its description.
	ErrFingerprint = errors.New("webrtc: peer certificate does not match its fingerprint")
	// ErrConfiguration is what NewPeerConnection returns for a
	// Configuration it cannot use.
	ErrConfiguration = errors.New("webrtc: invalid configuration")
	// ErrTooManyCandidates is what AddICECandidate returns once
	// Configuration.MaxRemoteCandidates are waiting for a remote
	// description.
	ErrTooManyCandidates = errors.New("webrtc: too many pending remote candidates")
)

// ICETransportPolicy is RTCIceTransportPolicy: which local candidates ICE
// uses.
type ICETransportPolicy uint8

const (
	ICETransportPolicyAll ICETransportPolicy = iota
	ICETransportPolicyRelay
)

// ICEServer is a STUN or TURN server, as RTCIceServer names one.
type ICEServer struct {
	// URLs are "stun:host[:port]" or "turn:..." URLs.
	URLs []string
	// Username and Credential are a TURN server's long-term credentials.
	Username, Credential string
}

// Configuration configures a PeerConnection. Its zero value, with an
// Engine or a Mux, is a full ICE agent with host candidates, a fresh
// certificate, the default codecs and header extensions, and data
// channels of 256 KiB messages.
type Configuration struct {
	// Mux is the shared UDP port the PeerConnection is reached at. When it
	// is nil the PeerConnection opens a Mux of its own on Engine, at
	// ListenAddr, and closes it with itself: one port per peer, as a
	// browser or a client has.
	Mux *Mux
	// Engine is the fib engine of a PeerConnection without a Mux.
	Engine *fib.Engine
	// ListenAddr is where a PeerConnection without a Mux listens: ":0"
	// when empty, every interface on a port the kernel picks.
	ListenAddr string

	// Certificates holds the DTLS certificate, the first one; one is made
	// with tls.GenerateWebRTCCertificate when it is empty. A server may
	// share one among its PeerConnections.
	Certificates []stdtls.Certificate

	// ICEServers are the STUN servers asked for server-reflexive
	// candidates and the TURN servers ("turn:host[:port]", over UDP) an
	// allocation is made on for a relay candidate, with the server's
	// Username and Credential. TURN needs a PeerConnection with a port of
	// its own, without a Mux: a TURN server tells its clients apart by
	// their address, which the PeerConnections of one Mux share. A lite
	// PeerConnection asks no STUN server, and a TURN URL ("turn:" or
	// "turns:") makes NewPeerConnection return ErrConfiguration.
	ICEServers []ICEServer
	// ICETransportPolicy ICETransportPolicyRelay gathers relay candidates
	// only, so that media goes through a TURN server and the peer never
	// learns this side's addresses. It needs a TURN server and a full ICE
	// agent: NewPeerConnection returns ErrConfiguration otherwise.
	ICETransportPolicy ICETransportPolicy
	// ICELite makes the PeerConnection a lite ICE agent (RFC 8445 section
	// 2.5), as a server with a public address is: it only answers the
	// peer's checks, is always controlled, and gathers host candidates
	// only. Its peer must be a full agent, as every browser is. STUN
	// servers are not asked; TURN servers and ICETransportPolicyRelay are
	// refused, with or without a Mux.
	ICELite bool
	// BundlePolicy must be BundlePolicyMaxBundle, its zero value.
	BundlePolicy BundlePolicy

	// Codecs are the payload formats offered and accepted;
	// DefaultCodecs() when nil.
	Codecs []Codec
	// HeaderExtensions are the RTP header extensions offered and
	// accepted; DefaultHeaderExtensions() when nil.
	HeaderExtensions []HeaderExtension

	// MaxMessageSize is the largest data channel message received, which
	// SDP's max-message-size advertises; 256 KiB when zero.
	MaxMessageSize int
	// SCTPReceiveWindow is the SCTP receiver window; 1 MiB when zero.
	SCTPReceiveWindow int
	// SCTPPoolBytes bounds the spare message buffers of the SCTP
	// association, per size class: see sctp.Config.PoolBytes. Zero means
	// 64 KiB, a fraction of what an association keeps on its own, which
	// suits a server with many peers; a negative value keeps none.
	SCTPPoolBytes int

	// ICEDisconnectedTimeout, ICEFailedTimeout and ICEKeepaliveInterval
	// tune the ICE agent: how long the selected pair may go unanswered
	// before it is disconnected (8s), how long checks or consent may fail
	// before ICE fails (30s), and how often consent is checked (5s). A
	// lite PeerConnection, which sends no checks, applies the first two
	// to how long its peer has been silent.
	ICEDisconnectedTimeout, ICEFailedTimeout, ICEKeepaliveInterval time.Duration

	// RTCPReportInterval is how often sender and receiver reports are
	// sent while media flows; 1s when zero, never when negative.
	RTCPReportInterval time.Duration
	// MaxRemoteSSRCs bounds the media streams received, each of which
	// costs SRTP state and a TrackRemote; 64 when zero. Packets of further
	// SSRCs are dropped before they are decrypted.
	MaxRemoteSSRCs int
	// MaxRemoteCandidates bounds the trickled candidates held until the
	// remote description arrives; 64 when zero.
	MaxRemoteCandidates int
}

const (
	defaultMaxRemoteSSRCs      = 64
	defaultMaxRemoteCandidates = 64
	defaultSCTPPoolBytes       = 64 << 10
	defaultReportInterval      = time.Second
	// maxBoundAddrs bounds the remote addresses one PeerConnection routes
	// to itself on its Mux: those it checks and those that check it.
	maxBoundAddrs = 256
	// maxEarlyDTLS bounds the DTLS datagrams kept while the remote
	// description that says who the DTLS client is has not arrived.
	maxEarlyDTLS = 16
	// localID is the LocalID of the Mux socket in the ICE agent; a TURN
	// allocation would take another.
	localID ice.LocalID = 1
)

// PeerConnection is a WebRTC peer, as RTCPeerConnection is: one ICE agent,
// one DTLS connection, SRTP for media and an SCTP association for data
// channels, all bundled on one transport, negotiated by offer and answer.
//
// Packets arrive from fib's workers, possibly several at once for one
// PeerConnection, and its timers fire on their own goroutine; a mutex
// orders them with the application's calls. Callbacks run outside that
// mutex, one at a time and in order, on whichever goroutine finds them
// queued: they may call back into the PeerConnection, but a callback that
// blocks delays every later one, and the packets of the worker it runs on.
type PeerConnection struct {
	cfg         Configuration
	mux         *Mux
	ownMux      bool
	cert        stdtls.Certificate
	fingerprint string
	cname       string
	codecs      []Codec
	exts        []HeaderExtension
	stunServers []netip.AddrPort
	turnServers []turnServer

	mu          sync.Mutex
	events      []event
	spare       []event
	dispatching bool
	timer       *time.Timer
	timerAt     time.Time

	closed      bool
	signaling   SignalingState
	connState   PeerConnectionState
	iceState    ICEConnectionState
	gathering   ICEGatheringState
	gatherDone  chan struct{}
	gatherFired bool

	// ICE.
	ufrag, pwd     string
	agent          *ice.Agent
	controlling    bool
	remoteUfrag    string
	remotePwd      string
	remoteLite     bool
	pendingCands   []ice.Candidate
	localCands     []ice.Candidate
	localSet       bool
	bound          map[netip.AddrPort]struct{}
	remoteAddr     netip.AddrPort
	lastRecv       time.Time
	pendingCreds   [2]string
	pendingRestart bool
	oldUfrag       string
	remoteLocal    ice.LocalID
	relays         []*relay
	agentGathered  bool
	selections     int

	// DTLS and SRTP.
	dtls          *tls.DTLSConn
	dtlsRole      sdp.Setup // this side's: active is the DTLS client
	dtlsStarted   bool
	dtlsDone      bool
	dtlsFailed    bool
	remoteFprints []sdp.Fingerprint
	earlyDTLS     [][]byte
	srtpOut       *srtp.Context
	srtpIn        *srtp.Context

	// SCTP and data channels.
	sctp         *sctp.Association
	dataMid      string
	dataAccepted bool
	sctpPort     int
	remoteMaxMs  int
	hasRemoteMs  bool
	channels     []*DataChannel

	// Media.
	transceivers []*Transceiver
	nextMid      int
	midOrder     []string
	extIDs       map[string]uint8
	firSeq       uint8
	extMid       uint8
	extRid       uint8
	remoteSSRCs  map[uint32]string
	tracks       map[uint32]*TrackRemote
	senders      map[uint32]*RTPSender
	rtcpSSRC     uint32
	nextReport   time.Time
	rhdr         rtp.Header
	whdr         rtp.Header
	wbuf, sbuf   []byte
	rtcpBuf      []byte

	// Descriptions.
	sessionID      uint64
	sessionVersion uint64
	localDesc      *sdp.SessionDescription
	localType      SDPType
	remoteDesc     *sdp.SessionDescription
	remoteType     SDPType
	lastOffer      string
	lastAnswer     string

	// Callbacks.
	onICECandidate      func(*ICECandidateInit)
	onConnState         func(PeerConnectionState)
	onICEState          func(ICEConnectionState)
	onGatheringState    func(ICEGatheringState)
	onSignalingState    func(SignalingState)
	onDataChannel       func(*DataChannel)
	onTrack             func(*TrackRemote)
	onRTCP              func([]byte)
	onNegotiationNeeded func()
}

// NewPeerConnection makes a PeerConnection. It needs a Mux to share, or
// an Engine to open one of its own on.
func NewPeerConnection(config Configuration) (*PeerConnection, error) {
	if config.BundlePolicy != BundlePolicyMaxBundle {
		return nil, fmt.Errorf("%w: only max-bundle is supported", ErrConfiguration)
	}
	pc := &PeerConnection{
		cfg:         config,
		gatherDone:  make(chan struct{}),
		bound:       make(map[netip.AddrPort]struct{}),
		remoteSSRCs: make(map[uint32]string),
		tracks:      make(map[uint32]*TrackRemote),
		senders:     make(map[uint32]*RTPSender),
		extIDs:      make(map[string]uint8),
		sessionID:   sdp.NewSessionID(),
		cname:       randomString(16),
		rtcpSSRC:    randomUint32(),
		codecs:      config.Codecs,
		exts:        config.HeaderExtensions,
		wbuf:        make([]byte, 0, 1500),
		sbuf:        make([]byte, 0, 1600),
	}
	if pc.codecs == nil {
		pc.codecs = DefaultCodecs()
	}
	if pc.exts == nil {
		pc.exts = DefaultHeaderExtensions()
	}
	if pc.cfg.MaxRemoteSSRCs <= 0 {
		pc.cfg.MaxRemoteSSRCs = defaultMaxRemoteSSRCs
	}
	if pc.cfg.MaxRemoteCandidates <= 0 {
		pc.cfg.MaxRemoteCandidates = defaultMaxRemoteCandidates
	}
	if pc.cfg.SCTPPoolBytes == 0 {
		pc.cfg.SCTPPoolBytes = defaultSCTPPoolBytes
	}
	if pc.cfg.MaxMessageSize <= 0 {
		pc.cfg.MaxMessageSize = sctp.DefaultMaxMessageSize
	}
	if pc.cfg.RTCPReportInterval == 0 {
		pc.cfg.RTCPReportInterval = defaultReportInterval
	}
	if len(config.Certificates) > 0 {
		pc.cert = config.Certificates[0]
	} else {
		cert, err := tls.GenerateWebRTCCertificate()
		if err != nil {
			return nil, err
		}
		pc.cert = cert
	}
	if len(pc.cert.Certificate) == 0 {
		return nil, fmt.Errorf("%w: certificate has no DER", ErrConfiguration)
	}
	pc.fingerprint = tls.CertificateFingerprint(pc.cert.Certificate[0])
	for _, s := range config.ICEServers {
		for _, u := range s.URLs {
			addr, isTURN, err := iceServer(u)
			if err != nil {
				return nil, err
			}
			switch {
			case isTURN && config.ICELite:
				return nil, fmt.Errorf("%w: a lite PeerConnection gathers no relay candidate: %q", ErrConfiguration, u)
			case !addr.IsValid():
			case isTURN:
				pc.turnServers = append(pc.turnServers, turnServer{addr, s.Username, s.Credential})
			default:
				pc.stunServers = append(pc.stunServers, addr)
			}
		}
	}
	if config.ICELite && config.ICETransportPolicy == ICETransportPolicyRelay {
		return nil, fmt.Errorf("%w: a lite PeerConnection gathers no relay candidate", ErrConfiguration)
	}
	if len(pc.turnServers) > 0 && config.Mux != nil {
		return nil, fmt.Errorf("%w: TURN needs a PeerConnection without a shared Mux", ErrConfiguration)
	}
	if config.ICETransportPolicy == ICETransportPolicyRelay && len(pc.turnServers) == 0 {
		return nil, fmt.Errorf("%w: the relay policy needs a TURN server", ErrConfiguration)
	}
	pc.ufrag, pc.pwd = randomString(8), randomString(24)
	pc.mux = config.Mux
	if pc.mux == nil {
		if config.Engine == nil {
			return nil, fmt.Errorf("%w: neither Mux nor Engine", ErrConfiguration)
		}
		m, err := NewMux(config.Engine, MuxConfig{Addr: config.ListenAddr})
		if err != nil {
			return nil, err
		}
		pc.mux, pc.ownMux = m, true
	}
	if err := pc.mux.register(pc.ufrag, pc); err != nil {
		return nil, err
	}
	pc.timer = time.AfterFunc(time.Hour, pc.onTimer)
	pc.timer.Stop()
	return pc, nil
}

type turnServer struct {
	addr           netip.AddrPort
	user, password string
}

// iceServer parses a "stun:" or "turn:" URL and resolves its host. One
// this side cannot use, TLS or TCP, reports an invalid address; isTURN
// still tells a TURN server ("turn:" or "turns:") apart.
func iceServer(u string) (addr netip.AddrPort, isTURN bool, err error) {
	scheme, rest, ok := strings.Cut(u, ":")
	if !ok {
		return addr, false, fmt.Errorf("%w: ICE server URL %q", ErrConfiguration, u)
	}
	rest, query, _ := strings.Cut(rest, "?")
	switch strings.ToLower(scheme) {
	case "turn":
		isTURN = true
		if strings.Contains(strings.ToLower(query), "transport=tcp") {
			return addr, true, nil
		}
	case "stun":
	case "turns":
		return addr, true, nil
	case "stuns":
		return addr, false, nil
	default:
		return addr, false, fmt.Errorf("%w: ICE server URL %q", ErrConfiguration, u)
	}
	if _, _, err := net.SplitHostPort(rest); err != nil {
		rest = net.JoinHostPort(strings.Trim(rest, "[]"), "3478")
	}
	ua, err := net.ResolveUDPAddr("udp", rest)
	if err != nil {
		return addr, isTURN, fmt.Errorf("%w: ICE server %q: %w", ErrConfiguration, u, err)
	}
	ap := ua.AddrPort()
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), isTURN, nil
}

// relay is a TURN allocation the ICE agent uses as a transport of its own.
type relay struct {
	r    *turn.ICERelay
	id   ice.LocalID
	done bool
}

func randomString(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	s := base64.RawURLEncoding.EncodeToString(b)
	// ICE characters are letters, digits, '+' and '/' (RFC 8445 section
	// 5.3); the URL alphabet's '-' and '_' are not among them.
	s = strings.NewReplacer("-", "+", "_", "/").Replace(s)
	return s[:n]
}

func randomUint32() uint32 {
	var b [4]byte
	for {
		_, _ = rand.Read(b[:])
		if v := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]); v != 0 {
			return v
		}
	}
}

// event is a callback queued to run once the mutex is released: either a
// function, or an RTP packet for a track, which a buffer from rtpPool
// holds.
type event struct {
	fn    func()
	track *TrackRemote
	buf   *[]byte
}

// queue runs fn after the mutex is released, after the callbacks queued
// before it.
func (pc *PeerConnection) queue(fn func()) {
	pc.events = append(pc.events, event{fn: fn})
}

// unlock rearms the timer, releases the mutex, and runs the queued
// callbacks unless another goroutine is running them already, in which
// case that one runs these too.
func (pc *PeerConnection) unlock() {
	pc.rearm()
	for {
		if pc.dispatching || len(pc.events) == 0 {
			pc.mu.Unlock()
			return
		}
		pc.dispatching = true
		evs := pc.events
		pc.events = pc.spare[:0]
		pc.mu.Unlock()
		for i := range evs {
			e := &evs[i]
			switch {
			case e.track != nil:
				e.track.deliver(*e.buf)
				rtpPool.Put(e.buf)
			case e.fn != nil:
				e.fn()
			}
			*e = event{}
		}
		pc.mu.Lock()
		pc.spare = evs[:0]
		pc.dispatching = false
	}
}

// rearm sets the timer to the earliest deadline of ICE, DTLS, SCTP and the
// RTCP reports.
func (pc *PeerConnection) rearm() {
	if pc.closed {
		pc.timer.Stop()
		return
	}
	var at time.Time
	take := func(t time.Time, ok bool) {
		if ok && (at.IsZero() || t.Before(at)) {
			at = t
		}
	}
	for _, r := range pc.relays {
		take(r.r.NextTimeout())
	}
	if pc.agent != nil {
		take(pc.agent.NextTimeout())
		if pc.cfg.ICELite && !pc.lastRecv.IsZero() && pc.iceState != ICEConnectionStateFailed {
			take(pc.liteDeadline(), true)
		}
	}
	if pc.dtls != nil {
		take(pc.dtls.NextTimeout())
	}
	if pc.sctp != nil {
		take(pc.sctp.NextTimeout())
	}
	if !pc.nextReport.IsZero() {
		take(pc.nextReport, true)
	}
	if at.Equal(pc.timerAt) {
		return
	}
	pc.timerAt = at
	if at.IsZero() {
		pc.timer.Stop()
		return
	}
	pc.timer.Reset(max(time.Until(at), 0))
}

func (pc *PeerConnection) onTimer() {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return
	}
	now := time.Now()
	pc.timerAt = time.Time{}
	for _, r := range pc.relays {
		if t, ok := r.r.NextTimeout(); ok && !t.After(now) {
			r.r.HandleTimeout(now)
			pc.relayProgress(r)
		}
	}
	if pc.agent != nil {
		if t, ok := pc.agent.NextTimeout(); ok && !t.After(now) {
			pc.agent.HandleTimeout(now)
		}
		if pc.cfg.ICELite {
			pc.checkLiteTimeout(now)
		}
	}
	if pc.dtls != nil {
		if t, ok := pc.dtls.NextTimeout(); ok && !t.After(now) {
			pc.dtls.HandleTimeout(now)
		}
	}
	if pc.sctp != nil {
		if t, ok := pc.sctp.NextTimeout(); ok && !t.After(now) {
			pc.sctp.HandleTimeout(now)
		}
	}
	if !pc.nextReport.IsZero() && !pc.nextReport.After(now) {
		pc.sendReports(now)
	}
	pc.unlock()
}

// liteDeadline is when a lite agent's silent peer is next due a change of
// state.
func (pc *PeerConnection) liteDeadline() time.Time {
	d := pc.cfg.ICEDisconnectedTimeout
	if d <= 0 {
		d = 8 * time.Second
	}
	if pc.iceState == ICEConnectionStateDisconnected {
		d = pc.cfg.ICEFailedTimeout
		if d <= 0 {
			d = 30 * time.Second
		}
	}
	return pc.lastRecv.Add(d)
}

// checkLiteTimeout does what a lite agent does not: notices that the peer
// has stopped checking, which a full peer does every few seconds.
func (pc *PeerConnection) checkLiteTimeout(now time.Time) {
	if pc.lastRecv.IsZero() || pc.iceState != ICEConnectionStateConnected && pc.iceState != ICEConnectionStateDisconnected {
		return
	}
	if now.Before(pc.liteDeadline()) {
		return
	}
	if pc.iceState == ICEConnectionStateConnected {
		pc.setICEState(ICEConnectionStateDisconnected)
	} else {
		pc.setICEState(ICEConnectionStateFailed)
	}
}

// handleDatagram takes a datagram the Mux routed here: STUN, DTLS, SRTP
// or SRTCP by its first byte (RFC 7983).
func (pc *PeerConnection) handleDatagram(from netip.AddrPort, b []byte) {
	pc.mu.Lock()
	if pc.closed {
		pc.mu.Unlock()
		return
	}
	now := time.Now()
	for _, r := range pc.relays {
		if r.r.Client.Server() != from {
			continue
		}
		peer, payload, handled := r.r.HandlePacket(from, b, now)
		if handled {
			pc.lastRecv = now
			pc.relayProgress(r)
			if len(payload) > 0 {
				pc.demux(r.id, peer, payload, now)
			}
			pc.unlock()
			return
		}
	}
	pc.heard(from, now)
	pc.demux(localID, from, b, now)
	pc.unlock()
}

// demux sorts a datagram from the peer by its first byte: STUN, DTLS, or
// SRTP and SRTCP. local is the transport it came by, the Mux's socket or a
// TURN relay.
func (pc *PeerConnection) demux(local ice.LocalID, from netip.AddrPort, b []byte, now time.Time) {
	switch c := b[0]; {
	case c < 4:
		if pc.agent != nil {
			_ = pc.agent.HandlePacket(local, from, b, now)
		}
	case c >= 20 && c <= 63:
		if !pc.remoteAddr.IsValid() {
			pc.remoteAddr, pc.remoteLocal = from, local
		}
		pc.handleDTLS(b, now)
	case c >= 128 && c <= 191:
		if pc.srtpIn != nil {
			if rtp.IsRTCP(b) {
				pc.handleRTCP(b, now)
			} else {
				pc.handleRTP(b, now)
			}
		}
	}
}

// relayProgress notes an allocation made or failed, which ends its part of
// gathering.
func (pc *PeerConnection) relayProgress(r *relay) {
	if r.done {
		return
	}
	if _, _, ok := r.r.Client.Relayed(); ok || r.r.Client.Closed() {
		r.done = true
		pc.checkGathering()
	}
}

// heard notes a datagram that came to the Mux from addr: its address is
// routed here, and a lite agent counts it as a sign of life.
func (pc *PeerConnection) heard(from netip.AddrPort, now time.Time) {
	pc.lastRecv = now
	pc.bind(from)
	if pc.cfg.ICELite && pc.iceState == ICEConnectionStateDisconnected {
		pc.setICEState(ICEConnectionStateConnected)
	}
}

func (pc *PeerConnection) bind(addr netip.AddrPort) {
	if _, ok := pc.bound[addr]; ok || len(pc.bound) >= maxBoundAddrs {
		return
	}
	pc.bound[addr] = struct{}{}
	pc.mux.bind(addr, pc)
}

// sendTo sends a datagram to addr through the Mux.
func (pc *PeerConnection) sendTo(addr netip.AddrPort, b []byte) {
	pc.bind(addr)
	pc.mux.send(addr, b)
}

// sendPeer sends a datagram on the selected pair.
func (pc *PeerConnection) sendPeer(b []byte) {
	if pc.remoteAddr.IsValid() {
		pc.sendOn(pc.remoteLocal, pc.remoteAddr, b)
	}
}

// sendOn sends a datagram to addr by the transport local.
func (pc *PeerConnection) sendOn(local ice.LocalID, addr netip.AddrPort, b []byte) {
	if local == localID || local == 0 {
		pc.sendTo(addr, b)
		return
	}
	for _, r := range pc.relays {
		if r.id == local {
			r.r.Send(addr, b)
			return
		}
	}
}

// ensureAgent makes the ICE agent once the role is known: controlling for
// the offerer, unless either side is lite. Host candidates are gathered at
// once; they are reported once the local description is set.
func (pc *PeerConnection) ensureAgent(controlling bool, now time.Time) error {
	if pc.agent != nil {
		return nil
	}
	cfg := ice.Config{
		Controlling:         controlling && !pc.cfg.ICELite,
		Lite:                pc.cfg.ICELite,
		Ufrag:               pc.ufrag,
		Pwd:                 pc.pwd,
		DisconnectedTimeout: pc.cfg.ICEDisconnectedTimeout,
		FailedTimeout:       pc.cfg.ICEFailedTimeout,
		ConsentTimeout:      pc.cfg.ICEFailedTimeout,
		ConsentInterval:     pc.cfg.ICEKeepaliveInterval,
		Send:                pc.sendOn,
		OnCandidate:         pc.onLocalCandidate,
		OnGatheringDone:     pc.onGatheringDone,
		OnStateChange:       pc.onAgentState,
		OnSelectedPair:      pc.onSelectedPair,
	}
	if !pc.cfg.ICELite {
		cfg.STUNServers = pc.stunServers
	}
	a, err := ice.NewAgent(cfg)
	if err != nil {
		return err
	}
	pc.agent = a
	pc.controlling = cfg.Controlling
	pc.setGathering(ICEGatheringStateGathering)
	if !pc.cfg.ICELite {
		for i, ts := range pc.turnServers {
			r := &relay{id: localID + 1 + ice.LocalID(i)}
			client, err := turn.NewClient(turn.ClientConfig{
				Server:       ts.addr,
				Username:     ts.user,
				Password:     ts.password,
				BindChannels: true,
				Send:         pc.sendTo,
				OnFailed:     func(error) { r.done = true; pc.checkGathering() },
			})
			if err != nil {
				return err
			}
			r.r = turn.NewICERelay(a, r.id, client)
			pc.relays = append(pc.relays, r)
			if err := client.Allocate(now); err != nil {
				r.done = true
			}
		}
	}
	var sockets []ice.Socket
	if pc.cfg.ICETransportPolicy != ICETransportPolicyRelay {
		sockets = []ice.Socket{{ID: localID, Addrs: pc.mux.Addrs()}}
	}
	return a.Gather(sockets, now)
}

// checkGathering completes gathering once the agent has gathered its
// candidates and every TURN allocation is made or has failed.
func (pc *PeerConnection) checkGathering() {
	if !pc.agentGathered {
		return
	}
	for _, r := range pc.relays {
		if !r.done {
			return
		}
	}
	pc.setGathering(ICEGatheringStateComplete)
	if pc.localSet {
		pc.emitCandidate(nil)
	}
}

func (pc *PeerConnection) onLocalCandidate(c ice.Candidate) {
	pc.localCands = append(pc.localCands, c)
	if pc.localSet {
		pc.emitCandidate(&c)
	}
}

func (pc *PeerConnection) emitCandidate(c *ice.Candidate) {
	fn := pc.onICECandidate
	if fn == nil {
		return
	}
	var init *ICECandidateInit
	if c != nil {
		init = &ICECandidateInit{Candidate: c.String(), SDPMid: pc.firstMid(), UsernameFragment: pc.ufrag}
	}
	pc.queue(func() { fn(init) })
}

func (pc *PeerConnection) onGatheringDone() {
	pc.agentGathered = true
	pc.checkGathering()
}

func (pc *PeerConnection) setGathering(s ICEGatheringState) {
	if pc.gathering == s {
		return
	}
	pc.gathering = s
	if s == ICEGatheringStateComplete && !pc.gatherFired {
		pc.gatherFired = true
		close(pc.gatherDone)
	}
	if fn := pc.onGatheringState; fn != nil {
		pc.queue(func() { fn(s) })
	}
}

func (pc *PeerConnection) onAgentState(s ice.State) {
	var st ICEConnectionState
	switch s {
	case ice.StateNew:
		st = ICEConnectionStateNew
	case ice.StateChecking:
		st = ICEConnectionStateChecking
	case ice.StateConnected:
		st = ICEConnectionStateConnected
	case ice.StateDisconnected:
		st = ICEConnectionStateDisconnected
	case ice.StateFailed:
		st = ICEConnectionStateFailed
	case ice.StateClosed:
		st = ICEConnectionStateClosed
	}
	pc.setICEState(st)
}

func (pc *PeerConnection) setICEState(s ICEConnectionState) {
	if pc.iceState == s {
		return
	}
	pc.iceState = s
	if fn := pc.onICEState; fn != nil {
		pc.queue(func() { fn(s) })
	}
	pc.updateConnState()
}

// updateConnState derives the PeerConnectionState from ICE and DTLS.
func (pc *PeerConnection) updateConnState() {
	var s PeerConnectionState
	switch {
	case pc.closed:
		s = PeerConnectionStateClosed
	case pc.iceState == ICEConnectionStateFailed || pc.dtlsFailed:
		s = PeerConnectionStateFailed
	case pc.iceState == ICEConnectionStateDisconnected:
		s = PeerConnectionStateDisconnected
	case pc.iceState == ICEConnectionStateConnected && pc.dtlsDone:
		s = PeerConnectionStateConnected
	case pc.iceState == ICEConnectionStateNew && pc.dtls == nil:
		s = PeerConnectionStateNew
	default:
		s = PeerConnectionStateConnecting
	}
	if pc.iceState == ICEConnectionStateConnected {
		pc.startDTLS(time.Now())
	}
	if s == pc.connState {
		return
	}
	pc.connState = s
	if fn := pc.onConnState; fn != nil {
		pc.queue(func() { fn(s) })
	}
}

func (pc *PeerConnection) onSelectedPair(p ice.Pair) {
	pc.remoteAddr, pc.remoteLocal = p.Remote.AddrPort(), p.Local.LocalID
	pc.selections++
	pc.startDTLS(time.Now())
}

// startDTLS starts the client's handshake once ICE has a pair to send on.
func (pc *PeerConnection) startDTLS(now time.Time) {
	if pc.dtls == nil || pc.dtlsStarted || pc.dtlsRole != sdp.SetupActive || !pc.remoteAddr.IsValid() {
		return
	}
	if pc.iceState != ICEConnectionStateConnected && !pc.cfg.ICELite {
		return
	}
	pc.dtlsStarted = true
	if err := pc.dtls.Start(now); err != nil {
		pc.failDTLS()
	}
}

// startTransports makes the DTLS connection and the SCTP association once
// both descriptions are known, and starts what can be started.
func (pc *PeerConnection) startTransports(now time.Time) error {
	if pc.dtls == nil {
		if err := pc.newDTLS(); err != nil {
			return err
		}
		pc.updateConnState()
		for _, b := range pc.earlyDTLS {
			pc.handleDTLS(b, now)
		}
		pc.earlyDTLS = nil
	}
	if pc.dataMid != "" && pc.sctp == nil {
		pc.newSCTP()
		if pc.dtlsDone {
			pc.sctp.Connect(now)
		}
	}
	if pc.sctp != nil && pc.hasRemoteMs {
		pc.sctp.SetRemoteMaxMessageSize(pc.remoteMaxMs)
	}
	pc.startDTLS(now)
	return nil
}

func (pc *PeerConnection) newDTLS() error {
	conf := &stdtls.Config{
		Certificates:          []stdtls.Certificate{pc.cert},
		InsecureSkipVerify:    true,
		ClientAuth:            stdtls.RequireAnyClientCert,
		VerifyPeerCertificate: pc.verifyPeer,
	}
	opts := tls.DTLSOptions{
		Output:                  pc.sendPeer,
		OnData:                  pc.onDTLSData,
		OnHandshakeComplete:     pc.onDTLSHandshake,
		SRTPProfiles:            []uint16{tls.SRTP_AEAD_AES_128_GCM, tls.SRTP_AES128_CM_HMAC_SHA1_80},
		InsecureSkipHelloVerify: true,
	}
	var err error
	if pc.dtlsRole == sdp.SetupActive {
		pc.dtls, err = tls.NewDTLSClient(conf, opts)
	} else {
		pc.dtls, err = tls.NewDTLSServer(conf, opts)
	}
	return err
}

// verifyPeer checks the peer's certificate against the fingerprints of its
// description. It runs inside HandleDatagram, under the mutex.
func (pc *PeerConnection) verifyPeer(raw [][]byte, _ [][]*x509.Certificate) error {
	if len(raw) == 0 {
		return ErrFingerprint
	}
	for _, f := range pc.remoteFprints {
		if v, ok := fingerprintOf(f.Algorithm, raw[0]); ok && strings.EqualFold(v, f.Value) {
			return nil
		}
	}
	return ErrFingerprint
}

func (pc *PeerConnection) handleDTLS(b []byte, now time.Time) {
	if pc.dtls == nil {
		if len(pc.earlyDTLS) < maxEarlyDTLS {
			pc.earlyDTLS = append(pc.earlyDTLS, append([]byte(nil), b...))
		}
		return
	}
	if pc.dtlsFailed {
		return
	}
	err := pc.dtls.HandleDatagram(b, now)
	switch {
	case err == nil:
	case errors.Is(err, io.EOF):
		// The peer sent close_notify: it has closed its PeerConnection.
		pc.closeLocked(false)
	default:
		pc.failDTLS()
	}
}

func (pc *PeerConnection) failDTLS() {
	if !pc.dtlsFailed {
		pc.dtlsFailed = true
		pc.updateConnState()
	}
}

func (pc *PeerConnection) onDTLSHandshake() {
	pc.dtlsDone = true
	if p, ok := pc.dtls.SelectedSRTPProfile(); ok {
		profile := srtp.Profile(p)
		km, err := pc.dtls.ExportKeyingMaterial(srtp.ExporterLabel, nil, profile.KeyingMaterialLen())
		if err == nil {
			pc.srtpOut, pc.srtpIn, err = srtp.NewContexts(km, profile, pc.dtlsRole == sdp.SetupActive, nil)
		}
		if err != nil {
			pc.failDTLS()
			return
		}
		pc.scheduleReports(time.Now())
	}
	if pc.sctp != nil {
		pc.sctp.Connect(time.Now())
	}
	pc.updateConnState()
}

func (pc *PeerConnection) onDTLSData(b []byte) {
	if pc.sctp != nil {
		_ = pc.sctp.HandlePacket(b, time.Now())
	}
}

func (pc *PeerConnection) newSCTP() {
	port := pc.sctpPort
	if port == 0 {
		port = sdp.DefaultSCTPPort
	}
	pc.sctp = sctp.NewAssociation(sctp.Config{
		Output: func(p []byte) {
			if pc.dtlsDone {
				_ = pc.dtls.Write(p)
			}
		},
		Client:               pc.dtlsRole == sdp.SetupActive,
		RemotePort:           uint16(port),
		MaxMessageSize:       pc.cfg.MaxMessageSize,
		RemoteMaxMessageSize: 0,
		ReceiveWindow:        pc.cfg.SCTPReceiveWindow,
		PoolBytes:            pc.cfg.SCTPPoolBytes,
		OnDataChannel:        pc.onRemoteChannel,
		OnClose:              pc.onSCTPClose,
	})
	for _, d := range pc.channels {
		if d.ch == nil && !d.closed {
			d.open()
		}
	}
}

func (pc *PeerConnection) onSCTPClose(error) {
	for _, d := range pc.channels {
		d.closedLocked()
	}
}

// ConnectionState returns the PeerConnectionState.
func (pc *PeerConnection) ConnectionState() PeerConnectionState {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.connState
}

// ICEConnectionState returns the ICEConnectionState.
func (pc *PeerConnection) ICEConnectionState() ICEConnectionState {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.iceState
}

// ICEGatheringState returns the ICEGatheringState.
func (pc *PeerConnection) ICEGatheringState() ICEGatheringState {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.gathering
}

// SignalingState returns the SignalingState.
func (pc *PeerConnection) SignalingState() SignalingState {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.signaling
}

// GatheringComplete returns a channel closed once every local candidate
// has been gathered, after which LocalDescription holds them all: what a
// peer that does not trickle candidates, or an HTTP exchange such as WHIP,
// waits for before sending the description.
func (pc *PeerConnection) GatheringComplete() <-chan struct{} { return pc.gatherDone }

// OnICECandidate sets the callback that hands each local candidate to the
// application to signal, once the local description is set, and nil when
// gathering is complete.
func (pc *PeerConnection) OnICECandidate(fn func(*ICECandidateInit)) {
	pc.mu.Lock()
	pc.onICECandidate = fn
	pc.mu.Unlock()
}

// OnConnectionStateChange sets the callback for PeerConnectionState.
func (pc *PeerConnection) OnConnectionStateChange(fn func(PeerConnectionState)) {
	pc.mu.Lock()
	pc.onConnState = fn
	pc.mu.Unlock()
}

// OnICEConnectionStateChange sets the callback for ICEConnectionState.
func (pc *PeerConnection) OnICEConnectionStateChange(fn func(ICEConnectionState)) {
	pc.mu.Lock()
	pc.onICEState = fn
	pc.mu.Unlock()
}

// OnICEGatheringStateChange sets the callback for ICEGatheringState.
func (pc *PeerConnection) OnICEGatheringStateChange(fn func(ICEGatheringState)) {
	pc.mu.Lock()
	pc.onGatheringState = fn
	pc.mu.Unlock()
}

// OnSignalingStateChange sets the callback for SignalingState.
func (pc *PeerConnection) OnSignalingStateChange(fn func(SignalingState)) {
	pc.mu.Lock()
	pc.onSignalingState = fn
	pc.mu.Unlock()
}

// OnNegotiationNeeded sets the callback that says a change, a track added
// or removed or the first data channel created, needs an offer to take
// effect.
func (pc *PeerConnection) OnNegotiationNeeded(fn func()) {
	pc.mu.Lock()
	pc.onNegotiationNeeded = fn
	pc.mu.Unlock()
}

func (pc *PeerConnection) negotiationNeeded() {
	if fn := pc.onNegotiationNeeded; fn != nil && !pc.closed {
		pc.queue(fn)
	}
}

func (pc *PeerConnection) setSignaling(s SignalingState) {
	if pc.signaling == s {
		return
	}
	pc.signaling = s
	if fn := pc.onSignalingState; fn != nil {
		pc.queue(func() { fn(s) })
	}
}

// SelectedCandidatePair returns the local and remote candidates ICE sends
// on, once it has selected them.
func (pc *PeerConnection) SelectedCandidatePair() (local, remote string, ok bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.agent == nil {
		return "", "", false
	}
	p, ok := pc.agent.SelectedPair()
	if !ok {
		return "", "", false
	}
	return p.Local.String(), p.Remote.String(), true
}

// Close closes the PeerConnection: data channels are aborted, the peer is
// told with a DTLS close_notify, its addresses and ufrags leave the Mux,
// and a Mux of its own is closed.
func (pc *PeerConnection) Close() error {
	pc.mu.Lock()
	pc.closeLocked(true)
	pc.unlock()
	return nil
}

func (pc *PeerConnection) closeLocked(notify bool) {
	if pc.closed {
		return
	}
	for _, r := range pc.relays {
		r.r.Client.Close(time.Now())
	}
	if notify {
		if pc.sctp != nil {
			pc.sctp.Abort()
		}
		if pc.dtls != nil {
			_ = pc.dtls.Close()
		}
	}
	pc.closed = true
	for _, d := range pc.channels {
		d.closedLocked()
	}
	if pc.agent != nil {
		pc.agent.Close()
	}
	pc.timer.Stop()
	pc.setSignaling(SignalingStateClosed)
	if pc.iceState != ICEConnectionStateClosed {
		pc.iceState = ICEConnectionStateClosed
		if fn := pc.onICEState; fn != nil {
			pc.queue(func() { fn(ICEConnectionStateClosed) })
		}
	}
	pc.updateConnState()
	for _, t := range pc.transceivers {
		t.sender.detach()
	}
	addrs := make([]netip.AddrPort, 0, len(pc.bound))
	for a := range pc.bound {
		addrs = append(addrs, a)
	}
	mux, own := pc.mux, pc.ownMux
	// The Mux is released outside the mutex: closing a connection, or the
	// binding of a Mux of its own, may wait for fib.
	pc.queue(func() {
		mux.release(pc, addrs)
		if own {
			go mux.Close()
		}
	})
}

// firstMid is the mid of the first media section, which trickled
// candidates name.
func (pc *PeerConnection) firstMid() string {
	if len(pc.midOrder) > 0 {
		return pc.midOrder[0]
	}
	return "0"
}

func itoa(i int) string { return strconv.Itoa(i) }
