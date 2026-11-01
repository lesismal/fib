package rtsp

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc/sdp"
)

// Defaults for the ServerConfig fields left at zero.
const (
	DefaultSessionTimeout       = 60 * time.Second
	DefaultIdleTimeout          = 60 * time.Second
	DefaultMaxQueuedBytes       = 4 << 20
	DefaultSenderReportInterval = 5 * time.Second
	DefaultUDPRTPAddr           = ":8000"
	DefaultUDPRTCPAddr          = ":8001"
)

// errNoHub is what a server without a hub answers PLAY and RECORD with.
var errNoHub = errors.New("rtsp: server has no hub")

// ServerConfig is how a Server serves its clients.
type ServerConfig struct {
	// Hub is where streams are published (ANNOUNCE, RECORD) and played
	// from (DESCRIBE, PLAY), by the URL's path without the slashes around
	// it: rtsp://host/live/cam is "live/cam". The hub's OnPublish and
	// OnSubscribe hooks see "rtsp" and the client's address and query, and
	// refuse with 403; OnSubscribe is asked at DESCRIBE, for the tracks,
	// and again at PLAY. Without a hub only OPTIONS and what OnRequest
	// answers are served.
	Hub *hub.Hub

	// UDPRTPAddr and UDPRTCPAddr are the UDP addresses ListenUDP binds for
	// RTP/AVP over UDP: the server's RTP and RTCP ports, shared by every
	// session. Empty means DefaultUDPRTPAddr and DefaultUDPRTCPAddr. Until
	// ListenUDP has bound them, SETUP takes only RTP/AVP/TCP.
	UDPRTPAddr  string
	UDPRTCPAddr string

	// Password, when set, has every request but OPTIONS authenticate: it
	// returns the password of user for the stream at path, or false to
	// refuse the user. Realm names the protection space (default "fib"),
	// and AuthMethods the schemes offered, AuthDigest when zero; AuthBasic
	// sends passwords in the clear. It runs on the connection's worker and
	// should not block for long.
	Password    func(path, user string) (string, bool)
	Realm       string
	AuthMethods AuthMethod

	// OnRequest, when set, sees every request, once authenticated, before
	// the server handles it, and answers it instead by returning a
	// response; nil lets the server go on. It runs under the connection's
	// lock and must not block.
	OnRequest func(c *ServerConn, req *Request) *Response

	// SessionTimeout ends a session, and its connection, after this long
	// without a request, RTCP or, while recording, RTP from the client;
	// the Session header tells the client so it sends keep-alives. Zero
	// means DefaultSessionTimeout. IdleTimeout closes a connection without
	// a session after this long without a request. Zero means
	// DefaultIdleTimeout.
	SessionTimeout time.Duration
	IdleTimeout    time.Duration

	// MaxHeaderBytes and MaxBodyBytes bound a request; see Parser.
	MaxHeaderBytes int
	MaxBodyBytes   int

	// PlayWait lets a player outlive the publisher of its stream by this
	// long, waiting for the next one (hub.SubscribeOptions.Wait): when it
	// comes with the codecs the player set up, the player carries on, its
	// RTP timestamps going on from where they were; with others it ends.
	// DESCRIBE still needs a publisher, for the tracks. Zero ends a player
	// with its publisher.
	PlayWait time.Duration

	// MaxQueuedBytes is how much a player over TCP may have queued and not
	// yet sent before its packets are dropped, video until the next
	// keyframe (see hub.NewDropper); twice as much closes it. Players over
	// UDP send RTP as it comes and are never dropped or closed for falling
	// behind. Zero means DefaultMaxQueuedBytes.
	MaxQueuedBytes int64

	// MTU bounds the RTP packets sent, header included; zero means
	// rtpcodec.DefaultMTU.
	MTU int
	// SenderReportInterval is how often RTCP sender reports go to players.
	// Zero means DefaultSenderReportInterval.
	SenderReportInterval time.Duration
	// ReorderSize and ReorderDelay size the reorder buffer of each stream
	// recorded over UDP; see rtpcodec.NewReorder.
	ReorderSize  int
	ReorderDelay time.Duration
}

// Server is a fib.Handler that serves RTSP 1.0 (RFC 2326) clients, playing
// and recording through the hub; see the package documentation. Bind it to
// a TCP address of an engine, and call ListenUDP to serve RTP over UDP too.
type Server struct {
	config ServerConfig
	auth   *authServer

	mu sync.Mutex
	// udp is the server's RTP and RTCP bindings once ListenUDP bound them.
	udp *serverUDP
	// routes find the session of a datagram by the client's address.
	routes     map[netip.AddrPort]*udpRoute
	ssrcRoutes map[uint32]*udpRoute
}

var (
	_ fib.Handler         = (*Server)(nil)
	_ fib.ShutdownHandler = (*Server)(nil)
)

// NewServer returns a Server, with the defaults in place of config's zeros.
func NewServer(config ServerConfig) *Server {
	if config.UDPRTPAddr == "" {
		config.UDPRTPAddr = DefaultUDPRTPAddr
	}
	if config.UDPRTCPAddr == "" {
		config.UDPRTCPAddr = DefaultUDPRTCPAddr
	}
	if config.Realm == "" {
		config.Realm = "fib"
	}
	if config.SessionTimeout <= 0 {
		config.SessionTimeout = DefaultSessionTimeout
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.MaxQueuedBytes <= 0 {
		config.MaxQueuedBytes = DefaultMaxQueuedBytes
	}
	if config.MTU <= 0 {
		config.MTU = rtpcodec.DefaultMTU
	}
	if config.SenderReportInterval <= 0 {
		config.SenderReportInterval = DefaultSenderReportInterval
	}
	s := &Server{config: config, routes: make(map[netip.AddrPort]*udpRoute)}
	if config.Password != nil {
		s.auth = newAuthServer(config.Realm, config.AuthMethods, 0)
	}
	return s
}

// serverUDP is the server's pair of UDP bindings.
type serverUDP struct {
	engine            *fib.Engine
	rtp, rtcp         int
	rtpPort, rtcpPort int
}

// ListenUDP binds the server's RTP and RTCP UDP addresses on engine, the
// engine its TCP binding is on, with Engine.AddBinding, so that SETUP takes
// RTP/AVP over UDP: the server sends a player's RTP from its RTP port and
// takes a recorder's there, and its RTCP likewise, telling each session
// apart by the client's address. It may be called before Run or while the
// engine runs, once.
func (s *Server) ListenUDP(engine *fib.Engine) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.udp != nil {
		return errors.New("rtsp: ListenUDP called twice")
	}
	u := &serverUDP{engine: engine}
	var err error
	if u.rtp, err = engine.AddBinding(fib.Binding{Network: "udp", Addr: s.config.UDPRTPAddr, Handler: &udpHandler{s: s}}); err != nil {
		return err
	}
	if u.rtcp, err = engine.AddBinding(fib.Binding{Network: "udp", Addr: s.config.UDPRTCPAddr, Handler: &udpHandler{s: s, rtcp: true}}); err != nil {
		_ = engine.RemoveBinding(u.rtp)
		return err
	}
	u.rtpPort, u.rtcpPort = bindingPort(engine, u.rtp), bindingPort(engine, u.rtcp)
	s.udp = u
	return nil
}

// UDPPorts returns the server's RTP and RTCP ports, 0 before ListenUDP.
func (s *Server) UDPPorts() (rtp, rtcp int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.udp == nil {
		return 0, 0
	}
	return s.udp.rtpPort, s.udp.rtcpPort
}

func bindingPort(e *fib.Engine, i int) int {
	addrs, err := e.BindingAddrs(i)
	if err != nil || len(addrs) == 0 {
		return 0
	}
	if a, ok := addrs[0].(*net.UDPAddr); ok {
		return a.Port
	}
	return 0
}

// ServerConn is one client connection of a Server, which OnRequest sees.
type ServerConn struct {
	s *Server
	c *fib.Connection

	mu     sync.Mutex
	parser Parser
	// user is who the client authenticated as: atomic, so that OnRequest,
	// which runs under mu, may ask.
	user atomic.Pointer[string]
	sess *session
	// described is the stream a DESCRIBE described, for the SETUPs after
	// it.
	described *description
	timer     *time.Timer
	due       time.Time
	active    time.Time
	closed    bool
}

// Conn returns the client's connection.
func (sc *ServerConn) Conn() *fib.Connection { return sc.c }

// User returns the user the client authenticated as, "" before it has.
func (sc *ServerConn) User() string {
	if u := sc.user.Load(); u != nil {
		return *u
	}
	return ""
}

// description is a stream's tracks as a DESCRIBE gave them, with the
// format of each, nil for those RTP cannot carry.
type description struct {
	path, query string
	base        string
	tracks      []*av.Track
	formats     []*rtpcodec.Format
}

// OnOpen starts a client's connection and the timer that closes it when
// idle. A UDP peer of the server's bindings needs nothing.
func (s *Server) OnOpen(c *fib.Connection) {
	if c.IsUDP() {
		return
	}
	sc := &ServerConn{s: s, c: c, active: time.Now()}
	sc.parser.MaxHeaderBytes, sc.parser.MaxBodyBytes = s.config.MaxHeaderBytes, s.config.MaxBodyBytes
	c.SetAttachment(sc)
	sc.mu.Lock()
	sc.timer = time.AfterFunc(s.config.IdleTimeout, sc.onTimer)
	sc.due = sc.active.Add(s.config.IdleTimeout)
	sc.mu.Unlock()
}

// OnData parses what the client sent and answers it. A malformed or
// oversized message closes the connection.
func (s *Server) OnData(c *fib.Connection, data []byte) {
	sc, ok := c.Attachment().(*ServerConn)
	if !ok {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed {
		return
	}
	if err := sc.parser.Feed(data, sc.handle); err != nil {
		if err == ErrMalformed || err == ErrTooLarge {
			code := StatusBadRequest
			if err == ErrTooLarge {
				code = StatusRequestEntityTooLarge
			}
			_ = c.Send((&Response{StatusCode: code}).Append(nil))
		}
		sc.closeLocked()
		c.CloseAfterSend()
	}
}

// OnPriorityData is not used by RTSP.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose ends the client's session.
func (s *Server) OnClose(c *fib.Connection, _ error) {
	if sc, ok := c.Attachment().(*ServerConn); ok {
		sc.mu.Lock()
		sc.closeLocked()
		sc.mu.Unlock()
	}
}

// OnShutdown closes the connection: RTSP has no way to ask a client to go
// elsewhere, and a player reconnects.
func (s *Server) OnShutdown(c *fib.Connection) {
	if !c.IsUDP() {
		_ = c.Close()
	}
}

// closeLocked ends the session and stops the timer.
func (sc *ServerConn) closeLocked() {
	if sc.closed {
		return
	}
	sc.closed = true
	if sc.timer != nil {
		sc.timer.Stop()
	}
	sc.endSessionLocked()
}

func (sc *ServerConn) endSessionLocked() {
	if sess := sc.sess; sess != nil {
		sc.sess = nil
		sess.close()
	}
}

// touch notes activity from the client.
func (sc *ServerConn) touch(now time.Time) { sc.active = now }

// timeout returns how long the client may be silent.
func (sc *ServerConn) timeout() time.Duration {
	if sc.sess != nil {
		return sc.s.config.SessionTimeout
	}
	return sc.s.config.IdleTimeout
}

// schedule has the timer fire by at.
func (sc *ServerConn) schedule(at time.Time) {
	if sc.closed || sc.timer == nil {
		return
	}
	if sc.due.IsZero() || at.Before(sc.due) {
		sc.due = at
		sc.timer.Reset(max(time.Until(at), 0))
	}
}

// onTimer closes a silent client, sends sender reports and gives up the
// gaps a reorder buffer has held too long.
func (sc *ServerConn) onTimer() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed {
		return
	}
	now := time.Now()
	sc.due = time.Time{}
	if now.Sub(sc.active) >= sc.timeout() {
		sc.closeLocked()
		_ = sc.c.Close()
		return
	}
	next := sc.active.Add(sc.timeout())
	if sess := sc.sess; sess != nil {
		if at, ok := sess.onTimer(now); ok && at.Before(next) {
			next = at
		}
	}
	sc.schedule(next)
}

// send sends a response.
func (sc *ServerConn) send(resp *Response) {
	_ = sc.c.Send(resp.Append(nil))
}

// handle takes one message of the client.
func (sc *ServerConn) handle(msg any) error {
	now := time.Now()
	switch m := msg.(type) {
	case *Interleaved:
		if sc.sess != nil {
			sc.touch(now)
			sc.sess.onInterleaved(now, m)
		}
	case *Request:
		sc.touch(now)
		resp := sc.serve(m, now)
		if cs, ok := m.Header.Lookup("CSeq"); ok {
			resp.Header = append(Header{{"CSeq", cs}}, resp.Header...)
		}
		resp.Header.Add("Server", "fib")
		sc.send(resp)
		if sc.sess != nil {
			sc.sess.afterResponse(m.Method)
		}
		sc.schedule(sc.active.Add(sc.timeout()))
	}
	// Responses to requests the server never sends are ignored.
	return nil
}

// serve answers a request.
func (sc *ServerConn) serve(req *Request, now time.Time) *Response {
	path, query, ok := streamPath(req.URL)
	if !ok && req.URL != "*" {
		return &Response{StatusCode: StatusBadRequest}
	}
	if sc.s.auth != nil && req.Method != MethodOptions {
		lookupPath := path
		if p, n := splitTrackID(path); n >= 0 {
			lookupPath = p
		}
		user, ok, stale := sc.s.auth.verify(req.Method, req.Header.Get("Authorization"), now,
			func(user string) (string, bool) { return sc.s.config.Password(lookupPath, user) })
		if !ok {
			resp := &Response{StatusCode: StatusUnauthorized}
			for _, v := range sc.s.auth.challenges(now, stale) {
				resp.Header.Add("WWW-Authenticate", v)
			}
			return resp
		}
		sc.user.Store(&user)
	}
	if hook := sc.s.config.OnRequest; hook != nil {
		if resp := hook(sc, req); resp != nil {
			return resp
		}
	}
	if sc.sess != nil && req.Method != MethodOptions {
		if v, ok := req.Header.Lookup("Session"); ok && ParseSession(v).ID != sc.sess.id {
			return &Response{StatusCode: StatusSessionNotFound}
		}
	}
	switch req.Method {
	case MethodOptions:
		return &Response{StatusCode: StatusOK, Header: Header{{"Public",
			"OPTIONS, DESCRIBE, ANNOUNCE, SETUP, PLAY, PAUSE, RECORD, TEARDOWN, GET_PARAMETER, SET_PARAMETER"}}}
	case MethodDescribe:
		return sc.describe(req, path, query)
	case MethodAnnounce:
		return sc.announce(req, path, query)
	case MethodSetup:
		return sc.setup(req, path, query)
	case MethodPlay:
		return sc.withSession(func(s *session) *Response { return s.play(req) })
	case MethodRecord:
		return sc.withSession(func(s *session) *Response { return s.record(req, now) })
	case MethodPause:
		return sc.withSession(func(s *session) *Response { return s.pause() })
	case MethodTeardown:
		sc.endSessionLocked()
		return &Response{StatusCode: StatusOK}
	case MethodGetParameter, MethodSetParameter:
		resp := &Response{StatusCode: StatusOK}
		if sc.sess != nil {
			resp.Header.Add("Session", sc.sess.header())
		}
		return resp
	}
	return &Response{StatusCode: StatusNotImplemented}
}

func (sc *ServerConn) withSession(fn func(*session) *Response) *Response {
	if sc.sess == nil {
		return &Response{StatusCode: StatusSessionNotFound}
	}
	resp := fn(sc.sess)
	if sc.sess != nil {
		resp.Header.Add("Session", sc.sess.header())
	}
	return resp
}

// hubError maps a hub error to a status.
func hubError(err error) *Response {
	switch {
	case errors.Is(err, hub.ErrNotFound):
		return &Response{StatusCode: StatusNotFound}
	case errors.Is(err, hub.ErrLimit):
		return &Response{StatusCode: StatusNotEnoughBandwidth}
	case errors.Is(err, hub.ErrClosed), errors.Is(err, errNoHub):
		return &Response{StatusCode: StatusServiceUnavailable}
	}
	return &Response{StatusCode: StatusForbidden}
}

// probe is a hub.Sink that only takes a stream's tracks.
type probe struct{ tracks []*av.Track }

func (p *probe) OnTracks(tracks []*av.Track) { p.tracks = tracks }
func (p *probe) OnPacket(*av.Packet)         {}
func (p *probe) OnClose(error)               {}

// lookup finds the tracks of a stream, and its formats.
func (sc *ServerConn) lookup(path, query, base string) (*description, error) {
	h := sc.s.config.Hub
	if h == nil {
		return nil, errNoHub
	}
	p := &probe{}
	sub, err := h.Subscribe(path, p, hub.SubscribeOptions{NoGOPCache: true, Info: sc.subscribeInfo(query)})
	if err != nil {
		return nil, err
	}
	sub.Close()
	d := &description{path: path, query: query, base: base, tracks: p.tracks, formats: make([]*rtpcodec.Format, len(p.tracks))}
	for i, t := range p.tracks {
		if t == nil {
			continue
		}
		if f, err := rtpcodec.NewFormat(t, uint8(96+i)); err == nil {
			f.Control = "trackID=" + strconv.Itoa(i)
			d.formats[i] = f
		}
	}
	return d, nil
}

func (sc *ServerConn) subscribeInfo(query string) hub.SubscribeInfo {
	return hub.SubscribeInfo{Protocol: "rtsp", Remote: sc.c.RemoteAddrPort(), Query: query}
}

// describe answers DESCRIBE with the SDP of the stream's tracks RTP can
// carry.
func (sc *ServerConn) describe(req *Request, path, query string) *Response {
	base := req.URL
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	d, err := sc.lookup(path, query, base)
	if err != nil {
		return hubError(err)
	}
	sd := sdp.New(sdp.NewSessionID(), 1)
	sd.SetLine('s', "fib")
	sd.SetConnection(sdp.Connection{NetType: "IN", AddrType: "IP4", Address: "0.0.0.0"})
	sd.AddAttribute("control", "*")
	n := 0
	for _, f := range d.formats {
		if f == nil {
			continue
		}
		md, err := f.Media()
		if err != nil {
			continue
		}
		sd.AddMedia(md)
		n++
	}
	if n == 0 {
		return &Response{StatusCode: StatusUnsupportedMediaType}
	}
	sc.described = d
	return &Response{StatusCode: StatusOK, Header: Header{
		{"Content-Type", "application/sdp"},
		{"Content-Base", base},
	}, Body: sd.Marshal()}
}

// announce takes the SDP of a stream the client is to record.
func (sc *ServerConn) announce(req *Request, path, query string) *Response {
	if sc.s.config.Hub == nil {
		return hubError(errNoHub)
	}
	if ct := req.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/sdp") {
		return &Response{StatusCode: StatusUnsupportedMediaType}
	}
	sd, err := sdp.Parse(req.Body)
	if err != nil {
		return &Response{StatusCode: StatusBadRequest}
	}
	formats := rtpcodec.ParseSession(sd)
	if len(formats) == 0 {
		return &Response{StatusCode: StatusUnsupportedMediaType}
	}
	if sc.sess != nil && sc.sess.mode != modeNone {
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	base := req.URL
	if v := req.Header.Get("Content-Base"); v != "" {
		base = v
	}
	sc.endSessionLocked()
	sc.sess = newSession(sc, path, query)
	sc.sess.mode = modeAnnounced
	sc.sess.announced = formats
	sc.sess.base = base
	return &Response{StatusCode: StatusOK}
}

// setup sets a stream of the session up, making the session on the first.
func (sc *ServerConn) setup(req *Request, path, query string) *Response {
	transports := ParseTransports(req.Header.Get("Transport"))
	if sess := sc.sess; sess != nil && (sess.mode == modeAnnounced || sess.mode == modeRecording) {
		return sess.setupRecord(req, transports)
	}
	if sc.s.config.Hub == nil {
		return hubError(errNoHub)
	}
	streamPath, track := splitTrackID(path)
	var d *description
	switch {
	case sc.sess != nil:
		if sc.sess.path != streamPath {
			return &Response{StatusCode: StatusAggregateNotAllowed}
		}
		d = sc.sess.desc
	case sc.described != nil && sc.described.path == streamPath:
		d = sc.described
	default:
		base := req.URL
		if track >= 0 {
			base = req.URL[:strings.LastIndex(req.URL, "trackID=")]
		} else if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		var err error
		if d, err = sc.lookup(streamPath, query, base); err != nil {
			return hubError(err)
		}
		sc.described = d
	}
	if track < 0 {
		// A stream with a single track may be set up by its own URL.
		if len(d.tracks) != 1 {
			return &Response{StatusCode: StatusAggregateNotAllowed}
		}
		track = 0
	}
	if track >= len(d.formats) || d.formats[track] == nil {
		return &Response{StatusCode: StatusNotFound}
	}
	if sc.sess == nil {
		sc.sess = newSession(sc, streamPath, d.query)
		sc.sess.mode = modePlayReady
		sc.sess.desc = d
	}
	return sc.sess.setupPlay(req, transports, track)
}

// newSessionID returns a random session ID.
func newSessionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}
