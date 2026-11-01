package rtc

import (
	"crypto/rand"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/rtpcodec"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/sdp"
)

// Defaults for the Config fields left at zero.
const (
	DefaultWHIPPrefix     = "/whip/"
	DefaultWHEPPrefix     = "/whep/"
	DefaultResourcePrefix = "/rtc/session/"
	DefaultGatherTimeout  = 5 * time.Second
	DefaultConnectTimeout = 15 * time.Second
	DefaultRepublishWait  = 10 * time.Second
	DefaultMaxHeldBytes   = 8 << 20
	DefaultMaxLoss        = 0.25
	DefaultMTU            = 1200
	DefaultPLIInterval    = 500 * time.Millisecond
	DefaultMaxOfferBytes  = 64 << 10
	DefaultMaxPlayWaiters = 1024
	// DefaultRetransmitPackets is the history of each track a WHEP viewer
	// is sent: about 1.2 s of a 2 Mbit/s video in 1200-byte packets.
	DefaultRetransmitPackets = 256
)

const (
	contentTypeSDP          = "application/sdp"
	contentTypeTrickleICE   = "application/trickle-ice-sdpfrag"
	allowedResourceMethods  = "PATCH, DELETE, OPTIONS"
	allowedEndpointMethods  = "POST, OPTIONS"
	corsAllowedHeaders      = "Authorization, Content-Type, If-Match"
	corsExposedHeaders      = "Location, Link, ETag, Accept-Patch, Accept-Post"
	corsPreflightMaxAgeSecs = "86400"
)

// Kind tells the two sides of the server apart.
type Kind uint8

const (
	// KindWHIP is an ingest: a WHIP client publishes a stream into the hub.
	KindWHIP Kind = iota + 1
	// KindWHEP is an egress: a WHEP client plays a hub stream.
	KindWHEP
)

func (k Kind) String() string {
	switch k {
	case KindWHIP:
		return "whip"
	case KindWHEP:
		return "whep"
	}
	return "unknown"
}

// Errors a Server answers with, which Authorize may return too.
var (
	// ErrUnauthorized refuses a request: 401 Unauthorized without a token,
	// 403 Forbidden with one.
	ErrUnauthorized = errors.New("rtc: unauthorized")
	// ErrNoMedia is what an offer with nothing both sides can send fails
	// with: 406 Not Acceptable.
	ErrNoMedia = errors.New("rtc: no acceptable media")
)

// Config is how a Server ingests and serves its streams.
type Config struct {
	// Hub is where WHIP publishes and WHEP plays.
	Hub *hub.Hub
	// WebRTC is the configuration every PeerConnection is made from. A
	// server shares one UDP port among them: set Mux, a webrtc.Mux on the
	// fib engine (webrtc.NewMux), and ICELite for a server with a
	// public address. Without a Mux each PeerConnection opens a port of
	// its own on Engine. One certificate is made for all of them when
	// Certificates is empty. Codecs is ignored: each session is
	// configured with the codecs of its stream or of the offer.
	WebRTC webrtc.Configuration
	// WHIPPrefix and WHEPPrefix are the URL paths the endpoints are
	// under: POST WHIPPrefix+"live/cam" publishes the hub path "live/cam",
	// POST WHEPPrefix+"live/cam" plays it. ResourcePrefix is where the
	// sessions they make live, for PATCH and DELETE. Empty means
	// DefaultWHIPPrefix ("/whip/"), DefaultWHEPPrefix ("/whep/") and
	// DefaultResourcePrefix ("/rtc/session/").
	WHIPPrefix, WHEPPrefix, ResourcePrefix string
	// MaxSessions bounds the WHIP and WHEP sessions together; 0 is
	// unlimited. A request past it is answered 503.
	MaxSessions int
	// Authorize, when set, decides every POST before anything is made for
	// it, with the bearer token of its Authorization header ("" without
	// one): an error refuses it, 401 without a token and 403 with one.
	// The hub's OnPublish and OnSubscribe hooks are asked after it, with
	// the request's query.
	Authorize func(r *http.Request, kind Kind, path, token string) error
	// AllowOrigin, when not empty, is sent as Access-Control-Allow-Origin
	// and CORS preflights are answered, for browser clients on another
	// origin.
	AllowOrigin string
	// LinkICEServers are sent with the answer as Link headers (RFC 9725
	// section 4.6), the STUN and TURN servers clients are to use: those a
	// server behind a Mux has no use for itself, as a TURN relay needs a
	// port of its own.
	LinkICEServers []webrtc.ICEServer
	// GatherTimeout bounds how long an answer waits for its candidates,
	// and ConnectTimeout how long a session may take to connect after it,
	// before it is closed. Zero or less means DefaultGatherTimeout (5s)
	// and DefaultConnectTimeout (15s).
	GatherTimeout, ConnectTimeout time.Duration
	// RepublishWait is how long a WHEP viewer waits for the stream's
	// next publisher when its publisher leaves; zero means
	// DefaultRepublishWait (10s), and a negative value ends it at once.
	RepublishWait time.Duration
	// PlayWait lets a WHEP offer wait this long for a stream that has no
	// publisher yet (hub.SubscribeOptions.StartWait): it is answered once
	// one arrives, or 404 when none does in time. Zero answers such an
	// offer 404 at once.
	PlayWait time.Duration
	// MaxPlayWaiters bounds the offers waiting for a publisher at once;
	// one past it is answered 503. Zero or less means
	// DefaultMaxPlayWaiters (1024).
	MaxPlayWaiters int
	// NoGOPCache starts viewers at the next keyframe rather than at the
	// hub's cached GOP.
	NoGOPCache bool
	// MaxHeldBytes bounds what a viewer holds while it connects: the
	// latest GOP, which it starts with. Zero or less means
	// DefaultMaxHeldBytes (8 MiB).
	MaxHeldBytes int
	// MaxLoss is the fraction of packets a viewer may report lost before
	// its packets are dropped, video until the next keyframe, as
	// hub.NewDropper does; zero means DefaultMaxLoss (0.25), and a
	// negative value never drops.
	MaxLoss float64
	// MTU bounds the RTP packets sent, header included; zero or less
	// means DefaultMTU (1200).
	MTU int
	// ReorderSize and ReorderDelay size the reorder buffer of each track a
	// WHIP publisher sends; zero means rtpcodec's defaults.
	ReorderSize  int
	ReorderDelay time.Duration
	// PLIInterval is the least time between two keyframe requests to a
	// publisher, zero or less meaning DefaultPLIInterval (500ms), and
	// KeyFrameInterval, when positive, how often one is sent whether
	// needed or not.
	PLIInterval, KeyFrameInterval time.Duration
	// OnKeyFrameRequest hears a viewer asking for a keyframe of a stream
	// no WHIP session of this server publishes, for an application that
	// can pass it on to its publisher. It must not block.
	OnKeyFrameRequest func(path string)
	// MaxOfferBytes bounds an offer or a PATCH body; zero or less means
	// DefaultMaxOfferBytes (64 KiB).
	MaxOfferBytes int
	// RetransmitPackets is how many of the latest RTP packets of each
	// track a WHEP viewer is sent are kept to answer its NACKs, as RTX
	// when its offer has RTX (webrtc.RetransmitConfig, whose MaxAge and
	// MaxBytesPerSecond WebRTC.Retransmit gives); zero means
	// DefaultRetransmitPackets (256), about 1.5 KiB each once sent, and
	// a negative value keeps none. Only the tracks the viewer asks NACKs
	// for, as browsers do for video, keep any. WHIP sessions keep none.
	RetransmitPackets int
}

func (c *Config) defaults() {
	if c.WHIPPrefix == "" {
		c.WHIPPrefix = DefaultWHIPPrefix
	}
	if c.WHEPPrefix == "" {
		c.WHEPPrefix = DefaultWHEPPrefix
	}
	if c.ResourcePrefix == "" {
		c.ResourcePrefix = DefaultResourcePrefix
	}
	if c.GatherTimeout <= 0 {
		c.GatherTimeout = DefaultGatherTimeout
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.RepublishWait == 0 {
		c.RepublishWait = DefaultRepublishWait
	}
	if c.MaxHeldBytes <= 0 {
		c.MaxHeldBytes = DefaultMaxHeldBytes
	}
	if c.MaxLoss == 0 {
		c.MaxLoss = DefaultMaxLoss
	}
	if c.MTU <= 0 {
		c.MTU = DefaultMTU
	}
	if c.PLIInterval <= 0 {
		c.PLIInterval = DefaultPLIInterval
	}
	if c.MaxOfferBytes <= 0 {
		c.MaxOfferBytes = DefaultMaxOfferBytes
	}
	if c.MaxPlayWaiters <= 0 {
		c.MaxPlayWaiters = DefaultMaxPlayWaiters
	}
	if c.RetransmitPackets == 0 {
		c.RetransmitPackets = DefaultRetransmitPackets
	}
}

// Server is a WHIP endpoint and a WHEP endpoint on a hub. It is a fib/http
// Handler, for a fib/http server or router, with the endpoints and the
// sessions they make under the prefixes of its Config:
//
//	mux, _ := webrtc.NewMux(engine, webrtc.MuxConfig{Addr: ":8189"})
//	srv, _ := rtc.NewServer(rtc.Config{Hub: h, WebRTC: webrtc.Configuration{Mux: mux, ICELite: true}})
//	router.Handle("/*", srv)
type Server struct {
	config Config

	mu         sync.Mutex
	sessions   map[string]*session
	publishers map[string]*session
	closed     bool
	logged     [256]atomic.Bool
	// playWaiters counts the WHEP offers waiting for a publisher.
	playWaiters atomic.Int64
}

var _ fibhttp.Handler = (*Server)(nil)

// NewServer returns a Server, with the defaults in place of config's
// zeros. It fails without a Hub, or without a Mux or an Engine for the
// PeerConnections.
func NewServer(config Config) (*Server, error) {
	if config.Hub == nil {
		return nil, errors.New("rtc: Config.Hub is nil")
	}
	if config.WebRTC.Mux == nil && config.WebRTC.Engine == nil {
		return nil, errors.New("rtc: Config.WebRTC needs a Mux or an Engine")
	}
	config.defaults()
	if len(config.WebRTC.Certificates) == 0 {
		cert, err := fibtls.GenerateWebRTCCertificate()
		if err != nil {
			return nil, err
		}
		config.WebRTC.Certificates = []stdtls.Certificate{cert}
	}
	return &Server{config: config, sessions: make(map[string]*session), publishers: make(map[string]*session)}, nil
}

// Len reports how many sessions the server has.
func (s *Server) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

// Close closes every session and refuses new ones.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	all := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		all = append(all, sess)
	}
	s.mu.Unlock()
	for _, sess := range all {
		sess.close()
	}
}

// ServeHTTP serves the endpoints and the sessions: POST an offer to an
// endpoint, PATCH trickled candidates to a session, DELETE it to end it,
// and OPTIONS for either.
func (s *Server) ServeHTTP(c *fibhttp.Context) {
	r := c.Request
	s.cors(c)
	p := r.URL.Path
	switch {
	case strings.HasPrefix(p, s.config.ResourcePrefix):
		s.serveResource(c, p[len(s.config.ResourcePrefix):])
	case strings.HasPrefix(p, s.config.WHIPPrefix):
		s.serveEndpoint(c, KindWHIP, p[len(s.config.WHIPPrefix):])
	case strings.HasPrefix(p, s.config.WHEPPrefix):
		s.serveEndpoint(c, KindWHEP, p[len(s.config.WHEPPrefix):])
	default:
		replyText(c, http.StatusNotFound, "not found")
	}
}

func (s *Server) cors(c *fibhttp.Context) {
	if s.config.AllowOrigin == "" {
		return
	}
	h := c.Header()
	h.Set("Access-Control-Allow-Origin", s.config.AllowOrigin)
	h.Set("Access-Control-Expose-Headers", corsExposedHeaders)
	if s.config.AllowOrigin != "*" {
		h.Add("Vary", "Origin")
	}
}

// options answers OPTIONS: a CORS preflight, or a client asking what the
// URL takes.
func (s *Server) options(c *fibhttp.Context, methods string) {
	h := c.Header()
	h.Set("Allow", methods)
	if methods == allowedEndpointMethods {
		h.Set("Accept-Post", contentTypeSDP)
	} else {
		h.Set("Accept-Patch", contentTypeTrickleICE)
	}
	if s.config.AllowOrigin != "" && c.Request.Header.Get("Access-Control-Request-Method") != "" {
		h.Set("Access-Control-Allow-Methods", methods)
		h.Set("Access-Control-Allow-Headers", corsAllowedHeaders)
		h.Set("Access-Control-Max-Age", corsPreflightMaxAgeSecs)
	}
	reply(c, http.StatusNoContent, "", nil)
}

func (s *Server) serveEndpoint(c *fibhttp.Context, kind Kind, path string) {
	r := c.Request
	path = strings.Trim(path, "/")
	switch {
	case path == "":
		replyText(c, http.StatusNotFound, "no stream path")
		return
	case r.Method == http.MethodOptions:
		s.options(c, allowedEndpointMethods)
		return
	case r.Method != http.MethodPost:
		c.Header().Set("Allow", allowedEndpointMethods)
		replyText(c, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != contentTypeSDP {
		replyText(c, http.StatusUnsupportedMediaType, "offer must be %s", contentTypeSDP)
		return
	}
	body := c.Body()
	if len(body) == 0 || len(body) > s.config.MaxOfferBytes {
		replyText(c, http.StatusBadRequest, "missing or oversized offer")
		return
	}
	token := bearer(r.Header.Get("Authorization"))
	if f := s.config.Authorize; f != nil {
		if err := f(r, kind, path, token); err != nil {
			s.refuse(c, token, err)
			return
		}
	}
	offer, err := sdp.ParseString(string(body))
	if err != nil {
		replyText(c, http.StatusBadRequest, "malformed offer")
		return
	}
	sess := &session{s: s, kind: kind, path: path, id: newID(), done: make(chan struct{})}
	sess.etag = `"` + sess.id + `"`
	if err := s.add(sess); err != nil {
		replyText(c, http.StatusServiceUnavailable, "%s", err.Error())
		return
	}
	desc := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(body)}
	wait := false
	sess.setup.Lock()
	if kind == KindWHIP {
		err = sess.startIngest(c, offer, desc)
	} else if wait, err = sess.subscribe(c); err == nil && !wait {
		err = sess.startEgress(offer, desc)
	}
	sess.setup.Unlock()
	if err != nil {
		sess.close()
		s.fail(c, err)
		return
	}
	if wait {
		s.awaitPublisher(c, sess, offer, desc)
		return
	}
	s.answer(c, sess)
}

// awaitPublisher answers a WHEP offer once its stream has a publisher,
// from a goroutine of its own, with the request retained. The
// subscription's StartWait bounds the wait: it running out, the client
// hanging up or the server closing ends the session, and the offer is
// answered as it failed.
func (s *Server) awaitPublisher(c *fibhttp.Context, sess *session, offer *sdp.SessionDescription, desc webrtc.SessionDescription) {
	var answered atomic.Bool
	c.Retain()
	c.OnCancel(func(error) {
		if !answered.Load() {
			go sess.close()
		}
	})
	go func() {
		defer c.Release()
		select {
		case <-sess.snd.tracked:
		case <-sess.done:
		}
		s.playWaiters.Add(-1)
		var err error
		sess.setup.Lock()
		select {
		case <-sess.done:
			// The wait ran out (hub.ErrNotFound), the hub closed, or
			// the server closed or the client went, which no answer
			// reaches.
			if err = sess.snd.err(); err == nil {
				err = errServerClosed
			}
		default:
			err = sess.startEgress(offer, desc)
		}
		sess.setup.Unlock()
		if err != nil {
			sess.close()
			s.fail(c, err)
			return
		}
		answered.Store(true)
		s.gather(c, sess)
	}()
}

// fail answers a POST that could not be served.
func (s *Server) fail(c *fibhttp.Context, err error) {
	status := http.StatusForbidden
	switch {
	case errors.Is(err, ErrNoMedia):
		status = http.StatusNotAcceptable
	case errors.Is(err, hub.ErrStreamBusy):
		status = http.StatusConflict
	case errors.Is(err, hub.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, hub.ErrLimit), errors.Is(err, hub.ErrClosed), errors.Is(err, errServerClosed):
		status = http.StatusServiceUnavailable
	case errors.Is(err, webrtc.ErrDescription), errors.Is(err, webrtc.ErrSignalingState):
		status = http.StatusBadRequest
	case errors.Is(err, errInternal):
		status = http.StatusInternalServerError
	}
	replyText(c, status, "%s", err.Error())
}

// refuse answers a request Authorize refused.
func (s *Server) refuse(c *fibhttp.Context, token string, err error) {
	if token == "" {
		c.Header().Set("WWW-Authenticate", `Bearer realm="fib"`)
		replyText(c, http.StatusUnauthorized, "%s", err.Error())
		return
	}
	replyText(c, http.StatusForbidden, "%s", err.Error())
}

// answer sends the 201 with the answer once its candidates are gathered,
// from a goroutine of its own, so that the handler never waits.
func (s *Server) answer(c *fibhttp.Context, sess *session) {
	c.Retain()
	go func() {
		defer c.Release()
		s.gather(c, sess)
	}()
}

// gather waits for the answer's candidates, up to GatherTimeout, and sends
// it.
func (s *Server) gather(c *fibhttp.Context, sess *session) {
	t := time.NewTimer(s.config.GatherTimeout)
	defer t.Stop()
	select {
	case <-sess.pc.GatheringComplete():
	case <-t.C:
	}
	s.respondAnswer(c, sess)
}

func (s *Server) respondAnswer(c *fibhttp.Context, sess *session) {
	desc := sess.pc.LocalDescription()
	h := c.Header()
	h.Set("Location", s.config.ResourcePrefix+sess.id)
	h.Set("ETag", sess.etag)
	h.Set("Accept-Patch", contentTypeTrickleICE)
	h.Set("Cache-Control", "no-store")
	for _, l := range linkHeaders(s.config.LinkICEServers) {
		h.Add("Link", l)
	}
	reply(c, http.StatusCreated, contentTypeSDP, []byte(desc.SDP))
}

func (s *Server) serveResource(c *fibhttp.Context, id string) {
	r := c.Request
	s.mu.Lock()
	sess := s.sessions[id]
	s.mu.Unlock()
	if sess == nil {
		replyText(c, http.StatusNotFound, "no such session")
		return
	}
	switch r.Method {
	case http.MethodOptions:
		s.options(c, allowedResourceMethods)
	case http.MethodDelete:
		sess.close()
		reply(c, http.StatusOK, "", nil)
	case http.MethodPatch:
		s.patch(c, sess)
	default:
		c.Header().Set("Allow", allowedResourceMethods)
		replyText(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// patch takes trickled candidates (RFC 9725 section 4.3.1). An ICE
// restart is not supported, and is answered 422 as section 4.3.2 asks of
// a server that trickles but does not restart.
func (s *Server) patch(c *fibhttp.Context, sess *session) {
	r := c.Request
	if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != contentTypeTrickleICE {
		replyText(c, http.StatusUnsupportedMediaType, "body must be %s", contentTypeTrickleICE)
		return
	}
	match := r.Header.Get("If-Match")
	if match == "*" {
		replyText(c, http.StatusUnprocessableEntity, "ICE restart is not supported")
		return
	}
	if match != "" && match != sess.etag {
		replyText(c, http.StatusPreconditionFailed, "session changed")
		return
	}
	body := c.Body()
	if len(body) > s.config.MaxOfferBytes {
		replyText(c, http.StatusBadRequest, "oversized fragment")
		return
	}
	frag, err := parseFragment(string(body))
	if err != nil {
		replyText(c, http.StatusBadRequest, "%s", err.Error())
		return
	}
	if frag.ufrag != "" && frag.ufrag != sess.remoteUfrag {
		replyText(c, http.StatusUnprocessableEntity, "ICE restart is not supported")
		return
	}
	for _, cand := range frag.candidates {
		if err := sess.pc.AddICECandidate(cand); err != nil {
			replyText(c, http.StatusBadRequest, "%s", err.Error())
			return
		}
	}
	reply(c, http.StatusNoContent, "", nil)
}

// add registers sess, within MaxSessions.
func (s *Server) add(sess *session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errServerClosed
	}
	if s.config.MaxSessions > 0 && len(s.sessions) >= s.config.MaxSessions {
		return errors.New("rtc: too many sessions")
	}
	s.sessions[sess.id] = sess
	return nil
}

func (s *Server) remove(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sess.id)
	if s.publishers[sess.path] == sess {
		delete(s.publishers, sess.path)
	}
}

// keyFrame passes a viewer's keyframe request for path on to its
// publisher: a WHIP session of this server, or OnKeyFrameRequest.
func (s *Server) keyFrame(path string) {
	s.mu.Lock()
	pub := s.publishers[path]
	s.mu.Unlock()
	if pub != nil {
		pub.rcv.Load().requestKeyFrame()
		return
	}
	if f := s.config.OnKeyFrameRequest; f != nil {
		f(path)
	}
}

// dropped logs, once per codec, a track WebRTC does not carry.
func (s *Server) dropped(codec av.CodecID) {
	if !s.logged[codec].Swap(true) {
		log.Info("rtc: track not sent over WebRTC, which cannot carry it without transcoding", "codec", codec.String())
	}
}

// errInternal marks a failure of the server's own making: 500.
var errInternal = errors.New("rtc: internal error")

// errServerClosed is a request the closing of the server ended: 503.
var errServerClosed = errors.New("rtc: server closed")

// session is one WHIP or WHEP session: a PeerConnection and what it
// publishes into or plays from the hub.
type session struct {
	s           *Server
	kind        Kind
	path, id    string
	etag        string
	remoteUfrag string
	pc          *webrtc.PeerConnection

	// WHEP: the sender, fed by sub.
	snd *sender
	sub *hub.Subscription
	// WHIP: the receiver, publishing with pub.
	rcv atomic.Pointer[receiver]
	pub *hub.Publisher

	// setup is held while the session is being set up, which whatever
	// ends or starts it from another goroutine waits for. done is closed
	// when the session is.
	setup     sync.Mutex
	done      chan struct{}
	connected atomic.Bool
	timer     *time.Timer
	closeOnce sync.Once
}

// newPeerConnection makes the session's PeerConnection with codecs, which
// keeps the latest retransmit packets of each track it sends, and has its
// state close the session when it fails.
func (sess *session) newPeerConnection(codecs []webrtc.Codec, retransmit int) error {
	cfg := sess.s.config.WebRTC
	cfg.Codecs = codecs
	cfg.Retransmit.Packets = max(retransmit, 0)
	pc, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		return fmt.Errorf("%w: %v", errInternal, err)
	}
	sess.pc = pc
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		switch st {
		case webrtc.PeerConnectionStateConnected:
			if !sess.connected.Swap(true) {
				go sess.connectedNow()
			}
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			// The callback may run inside a write to the PeerConnection,
			// under a hub stream's lock: closing goes elsewhere.
			go sess.close()
		}
	})
	return nil
}

// negotiate answers offer, keeping the remote ICE ufrag for PATCH.
func (sess *session) negotiate(offer *sdp.SessionDescription, desc webrtc.SessionDescription) error {
	if err := sess.pc.SetRemoteDescription(desc); err != nil {
		return err
	}
	answer, err := sess.pc.CreateAnswer()
	if err != nil {
		return err
	}
	if err := sess.pc.SetLocalDescription(answer); err != nil {
		return err
	}
	sess.remoteUfrag, _ = offer.ICEUfrag()
	if sess.remoteUfrag == "" {
		for _, md := range offer.Media {
			if u, ok := md.ICEUfrag(); ok {
				sess.remoteUfrag = u
				break
			}
		}
	}
	sess.timer = time.AfterFunc(sess.s.config.ConnectTimeout, func() {
		if !sess.connected.Load() {
			sess.close()
		}
	})
	return nil
}

func (sess *session) connectedNow() {
	sess.setup.Lock()
	defer sess.setup.Unlock()
	if sess.timer != nil {
		sess.timer.Stop()
	}
	if sess.snd != nil {
		sess.snd.start()
	}
	if r := sess.rcv.Load(); r != nil {
		r.startPeriodic()
	}
}

// startIngest sets up a WHIP session: a PeerConnection that receives what
// the offer sends, and a publisher of it.
func (sess *session) startIngest(c *fibhttp.Context, offer *sdp.SessionDescription, desc webrtc.SessionDescription) error {
	s := sess.s
	codecs := receiveCodecs(offer)
	if len(codecs) == 0 {
		return ErrNoMedia
	}
	if err := sess.newPeerConnection(codecs, 0); err != nil {
		return err
	}
	sess.pc.OnTrack(func(tr *webrtc.TrackRemote) {
		if r := sess.rcv.Load(); r != nil {
			r.onTrack(tr)
		}
	})
	if err := sess.negotiate(offer, desc); err != nil {
		return err
	}
	local, err := sdp.ParseString(sess.pc.LocalDescription().SDP)
	if err != nil {
		return fmt.Errorf("%w: %v", errInternal, err)
	}
	r := newReceiver(recvConfig{
		reorderSize: s.config.ReorderSize, reorderDelay: s.config.ReorderDelay,
		keyFrameInterval: s.config.KeyFrameInterval, pliInterval: s.config.PLIInterval,
	}, local, false)
	tracks := r.Tracks()
	if len(tracks) == 0 {
		return ErrNoMedia
	}
	pub, err := s.config.Hub.Publish(sess.path, tracks, hub.PublishInfo{
		Protocol: "whip", Remote: c.Conn.RemoteAddrPort(), Query: c.Request.URL.RawQuery})
	if err != nil {
		return err
	}
	sess.pub = pub
	r.onTracks = pub.SetTracks
	r.onPacket = pub.WritePacket
	sess.rcv.Store(r)
	s.mu.Lock()
	s.publishers[sess.path] = sess
	s.mu.Unlock()
	return nil
}

// subscribe starts a WHEP session with a subscription to the stream,
// reporting whether the session is to wait for its publisher.
func (sess *session) subscribe(c *fibhttp.Context) (bool, error) {
	s := sess.s
	snd := newSender(s.config.MaxHeldBytes)
	snd.onClose = func(error) { go sess.close() }
	sess.snd = snd
	var sink hub.Sink = snd
	if s.config.MaxLoss >= 0 {
		sink = hub.NewDropper(snd, snd.backlog, int64(s.config.MaxLoss*256))
	}
	wait := s.config.RepublishWait
	if wait < 0 {
		wait = 0
	}
	sub, err := s.config.Hub.Subscribe(sess.path, sink, hub.SubscribeOptions{
		Info:       hub.SubscribeInfo{Protocol: "whep", Remote: c.Conn.RemoteAddrPort(), Query: c.Request.URL.RawQuery},
		Wait:       wait,
		StartWait:  s.config.PlayWait,
		NoGOPCache: s.config.NoGOPCache,
	})
	if err != nil {
		return false, err
	}
	sess.sub = sub
	if snd.Streams() != nil {
		return false, nil
	}
	if s.config.PlayWait <= 0 {
		// Waiting is for a republish, not for a stream that is not there.
		return false, hub.ErrNotFound
	}
	if s.playWaiters.Add(1) > int64(s.config.MaxPlayWaiters) {
		s.playWaiters.Add(-1)
		return false, hub.ErrLimit
	}
	return true, nil
}

// startEgress goes on with a WHEP session whose stream has tracks: a
// PeerConnection that sends those of them WebRTC can carry.
func (sess *session) startEgress(offer *sdp.SessionDescription, desc webrtc.SessionDescription) error {
	s := sess.s
	snd := sess.snd
	streams := snd.Streams()
	outs, codecs := planEgress(streams, offer, s.config.MTU, s.dropped)
	if len(outs) == 0 {
		return ErrNoMedia
	}
	if err := sess.newPeerConnection(codecs, s.config.RetransmitPackets); err != nil {
		return err
	}
	for _, o := range outs {
		rs, err := sess.pc.AddTrack(o.local)
		if err != nil {
			return fmt.Errorf("%w: %v", errInternal, err)
		}
		o.rtp = rs
		rs.OnKeyFrameRequest(func() { s.keyFrame(sess.path) })
	}
	sess.pc.OnRTCP(func(pkt []byte) {
		snd.onRTCP(func(ssrc uint32) bool {
			for _, o := range outs {
				if o.rtp.SSRC() == ssrc {
					return true
				}
			}
			return false
		}, pkt)
	})
	if err := sess.negotiate(offer, desc); err != nil {
		return err
	}
	var sending []*outTrack
	for _, o := range outs {
		if o.rtp.Transceiver().CurrentDirection() == webrtc.DirectionSendOnly ||
			o.rtp.Transceiver().CurrentDirection() == webrtc.DirectionSendRecv {
			sending = append(sending, o)
		}
	}
	if len(sending) == 0 {
		return ErrNoMedia
	}
	snd.setTracks(sending)
	return nil
}

// planEgress returns the tracks a viewer is sent, each in the codec the
// offer has for it, and those codecs; a stream track WebRTC cannot carry,
// or the offer has no codec for, is left out and reported to dropped.
func planEgress(streams []*av.Track, offer *sdp.SessionDescription, mtu int, dropped func(av.CodecID)) ([]*outTrack, []webrtc.Codec) {
	var outs []*outTrack
	var codecs []webrtc.Codec
	for i, t := range streams {
		if t == nil {
			continue
		}
		var offered []webrtc.Codec
		if offer != nil {
			offered = offeredCodecs(offer, kindOf(t.Codec))
		}
		codec, ok := sendCodec(t, offered)
		if !ok {
			if dropped != nil {
				dropped(t.Codec)
			}
			continue
		}
		f, err := rtpcodec.NewFormat(t, codec.PayloadType)
		if err != nil {
			continue
		}
		pk, err := rtpcodec.NewPacketizer(f, rtpcodec.PacketizerConfig{MTU: mtu})
		if err != nil {
			continue
		}
		local := webrtc.NewTrackLocal(codec, "fib-"+kindName(t.Codec)+"-"+strconv.Itoa(i), "fib")
		outs = append(outs, &outTrack{codec: t.Codec, local: local, pk: pk, hub: i})
		if !containsCodec(codecs, codec) {
			codecs = append(codecs, codec)
		}
	}
	return outs, codecs
}

func kindName(c av.CodecID) string { return string(kindOf(c)) }

// close ends the session, once: what it plays or publishes, and its
// PeerConnection.
func (sess *session) close() {
	sess.closeOnce.Do(func() {
		sess.setup.Lock()
		defer sess.setup.Unlock()
		if sess.done != nil {
			close(sess.done)
		}
		sess.s.remove(sess)
		if sess.timer != nil {
			sess.timer.Stop()
		}
		if sess.sub != nil {
			sess.sub.Close()
		}
		if sess.snd != nil {
			sess.snd.close()
		}
		if r := sess.rcv.Load(); r != nil {
			r.close()
		}
		if sess.pub != nil {
			sess.pub.Close()
		}
		if sess.pc != nil {
			_ = sess.pc.Close()
		}
	})
}

// bearer returns the token of an Authorization header of the Bearer
// scheme (RFC 6750), "" for any other.
func bearer(v string) string {
	scheme, token, ok := strings.Cut(strings.TrimSpace(v), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

// linkHeaders returns the Link header values that advertise ICE servers
// (RFC 9725 section 4.6).
func linkHeaders(servers []webrtc.ICEServer) []string {
	var out []string
	for _, srv := range servers {
		for _, u := range srv.URLs {
			v := "<" + u + `>; rel="ice-server"`
			if srv.Username != "" || srv.Credential != "" {
				v += "; username=" + strconv.Quote(srv.Username) + "; credential=" + strconv.Quote(srv.Credential) +
					`; credential-type="password"`
			}
			out = append(out, v)
		}
	}
	return out
}

// newID returns a random session id.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 32)
	for i, v := range b {
		out[2*i], out[2*i+1] = hexdigits[v>>4], hexdigits[v&15]
	}
	return string(out)
}

// reply writes a whole response with the header set on c so far, which
// Context.Respond would leave out.
func reply(c *fibhttp.Context, status int, contentType string, body []byte) {
	h := c.Header().Clone()
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	_ = c.WriteResponse(fibhttp.Response{StatusCode: status, Header: h, Body: body})
}

// replyText replies with a plain text body.
func replyText(c *fibhttp.Context, status int, format string, args ...any) {
	reply(c, status, "text/plain; charset=utf-8", fmt.Appendf(nil, format, args...))
}
