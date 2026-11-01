package rtsp

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/internal/lipsync"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// Defaults for the ClientConfig fields left at zero.
const (
	DefaultDialTimeout    = 10 * time.Second
	DefaultRequestTimeout = 10 * time.Second
	DefaultReadTimeout    = 10 * time.Second
)

// Errors of a Client.
var (
	// ErrClientClosed is what OnClose hears after Close.
	ErrClientClosed = errors.New("rtsp: client closed")
	// ErrNoTracks is what a Client reports for a description it can play
	// nothing of.
	ErrNoTracks = errors.New("rtsp: no track the client can carry")
	// ErrTimeout is what a Client reports for a request without a response,
	// or a stream without packets, for too long.
	ErrTimeout = errors.New("rtsp: timeout")
	// ErrNotReady is what WritePacket reports before RECORD has been
	// accepted.
	ErrNotReady = errors.New("rtsp: not recording yet")
	// ErrUnexpected is what a Client reports for a response it cannot use,
	// such as a SETUP without a Transport it asked for.
	ErrUnexpected = errors.New("rtsp: unexpected response")
)

// StatusError is a response with an error status to a Client's request.
type StatusError struct {
	Method string
	Code   int
	Reason string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("rtsp: %s: %d %s", e.Method, e.Code, e.Reason)
}

// ClientTransport is how a Client carries RTP.
type ClientTransport uint8

const (
	// TransportTCP interleaves RTP on the RTSP connection.
	TransportTCP ClientTransport = iota
	// TransportUDP sends RTP over UDP, from a pair of UDP bindings the
	// client adds to its engine for each stream.
	TransportUDP
)

// ClientConfig configures a Client.
type ClientConfig struct {
	// Engine dials the connection, and binds the UDP ports of
	// TransportUDP. It must be running.
	Engine    *fib.Engine
	Transport ClientTransport
	// UDPHost is the local host of the UDP bindings; empty means all
	// addresses.
	UDPHost string

	// Username and Password authenticate, Digest or Basic as the server
	// asks; empty takes them from the URL's user info.
	Username string
	Password string

	// DialTimeout bounds the connect, RequestTimeout each request, and
	// ReadTimeout how long a played stream may send nothing. Zero means
	// their defaults.
	DialTimeout    time.Duration
	RequestTimeout time.Duration
	ReadTimeout    time.Duration
	// KeepAlive is how often a keep-alive request (GET_PARAMETER, or
	// OPTIONS when the server does not list it) goes out; zero means half
	// the session timeout the server gives, 30 s at most.
	KeepAlive time.Duration

	// MTU bounds the RTP packets a publishing client sends; zero means
	// rtpcodec.DefaultMTU. MaxQueuedBytes bounds what it may have queued
	// for the connection, past which WritePacket drops (as PublishFromHub
	// does, video until the next keyframe); zero means DefaultMaxQueuedBytes.
	MTU            int
	MaxQueuedBytes int64
	// ReorderSize and ReorderDelay size the reorder buffer of each stream
	// played over UDP.
	ReorderSize  int
	ReorderDelay time.Duration
	UserAgent    string

	// OnTracks gives the tracks played, once PLAY is accepted and again
	// when parameter sets in band change one. OnPacket gives each packet;
	// its Track indexes the tracks. OnReady says a publishing client may
	// write. OnClose ends the client, with ErrClientClosed after Close.
	// They are called one at a time, and may call the client's methods.
	OnTracks func(tracks []*av.Track)
	OnPacket func(pkt *av.Packet)
	OnReady  func()
	OnClose  func(err error)
}

// Client is an RTSP client that plays a stream from a server or publishes
// one to it, asynchronously, on a fib engine; see Play and Publish.
type Client struct {
	config   ClientConfig
	url      string
	user     string
	password string
	publish  bool

	// cbMu serializes the callbacks; it is taken before mu, which guards
	// the rest.
	cbMu    sync.Mutex
	mu      sync.Mutex
	conn    *fib.Connection
	parser  Parser
	cseq    int
	pending *pendingRequest
	auth    *clientAuth
	public  string
	session string
	timeout time.Duration
	base    string
	streams []*clientStream
	tracks  []*av.Track
	state   clientState
	timer   *time.Timer
	nextKA  time.Time
	lastRx  time.Time
	events  []func()
	// sync lines the played streams up by the server's sender reports
	// when RTP-Info did not.
	sync lipsync.Session
	// A publishing client's packets share one media clock from its first
	// packet, whose DTS is originAt and went at wall0.
	origin   bool
	originAt time.Duration
	wall0    time.Time
	closing  atomic.Bool
	done     bool
	buf      []byte
}

type clientState uint8

const (
	stateConnecting clientState = iota
	stateSetup
	stateStreaming
)

type pendingRequest struct {
	req      *Request
	cb       func(*Response)
	deadline time.Time
	retries  int
}

// clientStream is one stream of a client's session.
type clientStream struct {
	index  int
	format *rtpcodec.Format
	tcp    bool
	ch     uint8

	rtpBind, rtcpBind     int
	rtpPort, rtcpPort     int
	serverRTP, serverRTCP netip.AddrPort
	rtpOut, rtcpOut       *udpSender

	dp        *rtpcodec.Depacketizer
	reorder   *rtpcodec.Reorder
	originSet bool
	sr        lipsync.Clock
	pkt       rtp.Packet
	pk        *rtpcodec.Packetizer
	ssrc      uint32
	nextSR    time.Time
}

// Play plays the stream at rawURL (rtsp://[user:pass@]host[:port]/path):
// it dials the server, DESCRIBEs the stream, SETs UP every track it can
// carry and PLAYs, then gives the tracks and packets to OnTracks and
// OnPacket until the stream or the client ends, which OnClose hears. It
// returns once the dial has started.
func Play(config ClientConfig, rawURL string) (*Client, error) {
	return start(config, rawURL, false, nil, nil)
}

// Publish publishes tracks to the stream at rawURL: it ANNOUNCEs them,
// SETs UP each for recording and RECORDs, then OnReady says WritePacket
// may send. Tracks RTP cannot carry are left out: their packets are
// dropped.
func Publish(config ClientConfig, rawURL string, tracks []*av.Track) (*Client, error) {
	return start(config, rawURL, true, tracks, nil)
}

// start makes the client and dials; created, when set, is given the client
// before the dial, so it has it before any callback runs.
func start(config ClientConfig, rawURL string, publish bool, tracks []*av.Track, created func(*Client)) (*Client, error) {
	if config.Engine == nil {
		return nil, errors.New("rtsp: ClientConfig.Engine is nil")
	}
	clean, user, password, err := stripUserinfo(rawURL)
	if err != nil {
		return nil, err
	}
	u, _ := url.Parse(clean)
	if !strings.EqualFold(u.Scheme, "rtsp") || u.Host == "" {
		return nil, errors.New("rtsp: not an rtsp:// URL: " + clean)
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "554")
	}
	if config.Username != "" {
		user, password = config.Username, config.Password
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = DefaultDialTimeout
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = DefaultRequestTimeout
	}
	if config.ReadTimeout <= 0 {
		config.ReadTimeout = DefaultReadTimeout
	}
	if config.MTU <= 0 {
		config.MTU = rtpcodec.DefaultMTU
	}
	if config.MaxQueuedBytes <= 0 {
		config.MaxQueuedBytes = DefaultMaxQueuedBytes
	}
	if config.UserAgent == "" {
		config.UserAgent = "fib"
	}
	c := &Client{config: config, url: clean, user: user, password: password, publish: publish}
	if publish {
		for i, t := range tracks {
			if t == nil {
				continue
			}
			f, err := rtpcodec.NewFormat(t, uint8(96+i))
			if err != nil {
				continue
			}
			f.Control = "trackID=" + strconv.Itoa(i)
			c.streams = append(c.streams, &clientStream{index: i, format: f})
		}
		if len(c.streams) == 0 {
			return nil, ErrNoTracks
		}
		c.tracks = tracks
	}
	c.timer = time.AfterFunc(time.Hour, c.onTimer)
	if created != nil {
		created(c)
	}
	err = config.Engine.DialWithHandler("tcp", host, config.DialTimeout, (*clientHandler)(c), c.dialed)
	if err != nil {
		c.timer.Stop()
		return nil, err
	}
	return c, nil
}

// Close ends the client: OnClose hears ErrClientClosed. It may be called
// from anywhere, the callbacks included.
func (c *Client) Close() {
	if c.closing.Swap(true) {
		return
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	} else {
		// The dial is still going: its end closes the client.
		c.timer.Reset(0)
	}
}

// Tracks returns the tracks played or published.
func (c *Client) Tracks() []*av.Track {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.tracks)
}

// Backlog returns how many bytes a publishing client has queued for its
// connection and not yet sent.
func (c *Client) Backlog() int64 {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil || outputwait.Queued == nil {
		return 0
	}
	return outputwait.Queued(conn)
}

// WritePacket sends a packet of a published track. It reports ErrNotReady
// before OnReady, and drops the packet, reporting nil, when the backlog is
// over MaxQueuedBytes.
func (c *Client) WritePacket(pkt *av.Packet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return ErrClientClosed
	}
	if !c.publish || c.state != stateStreaming {
		return ErrNotReady
	}
	var st *clientStream
	for _, s := range c.streams {
		if s.index == pkt.Track {
			st = s
		}
	}
	if st == nil {
		return nil
	}
	now := time.Now()
	if !c.origin {
		c.origin, c.originAt, c.wall0 = true, pkt.DTS, now
		for _, s := range c.streams {
			s.pk.SetOrigin(pkt.DTS)
		}
	}
	// A sender report with the first packet and every few seconds: a
	// receiver needs one to line the streams up, and ffmpeg's leaves
	// packets without timestamps until it has one.
	if !now.Before(st.nextSR) {
		st.nextSR = now.Add(DefaultSenderReportInterval)
		defer func() {
			b := senderReport(st.pk, now, c.originAt+now.Sub(c.wall0))
			if st.tcp {
				_ = c.conn.Send(AppendInterleaved(nil, st.ch+1, b))
			} else {
				st.rtcpOut.send(b)
			}
		}()
	}
	if st.tcp {
		if outputwait.Queued != nil && outputwait.Queued(c.conn) > c.config.MaxQueuedBytes {
			return nil
		}
		buf := c.buf[:0]
		err := st.pk.Packetize(pkt, func(rp *rtp.Packet) error {
			at := len(buf)
			buf = append(buf, '$', st.ch, 0, 0)
			var err error
			buf, err = rp.Append(buf)
			n := len(buf) - at - 4
			buf[at+2], buf[at+3] = byte(n>>8), byte(n)
			return err
		})
		c.buf = buf
		if err != nil {
			return err
		}
		return c.conn.Send(buf)
	}
	return st.pk.Packetize(pkt, func(rp *rtp.Packet) error {
		b, err := rp.Append(make([]byte, 0, rp.MarshalSize()))
		if err == nil {
			st.rtpOut.send(b)
		}
		return err
	})
}

// lock takes the callback lock and then the state lock; unlock releases
// the state lock and then runs the callbacks queued meanwhile, in order.
func (c *Client) lock() {
	c.cbMu.Lock()
	c.mu.Lock()
}

func (c *Client) unlock() {
	events := c.events
	c.events = nil
	c.mu.Unlock()
	for _, e := range events {
		e()
	}
	c.cbMu.Unlock()
}

func (c *Client) emit(e func()) { c.events = append(c.events, e) }

// dialed starts the exchange once the connection is up. It runs on the
// event loop, which must not wait on the client's locks or callbacks.
func (c *Client) dialed(conn *fib.Connection, err error) {
	go c.onDialed(conn, err)
}

func (c *Client) onDialed(conn *fib.Connection, err error) {
	c.lock()
	defer c.unlock()
	if err != nil {
		c.failLocked(err)
		return
	}
	if c.done {
		// The connection closed before this ran.
		_ = conn.Close()
		return
	}
	c.conn = conn
	if c.closing.Load() {
		c.failLocked(ErrClientClosed)
		_ = conn.Close()
		return
	}
	c.request(MethodOptions, c.url, nil, nil, c.onOptions)
}

// request sends a request and has cb take its response.
func (c *Client) request(method, u string, h Header, body []byte, cb func(*Response)) {
	req := &Request{Method: method, URL: u, Header: h, Body: body}
	c.pending = &pendingRequest{req: req, cb: cb}
	c.sendPending()
}

func (c *Client) sendPending() {
	p := c.pending
	req := p.req
	c.cseq++
	req.Header.Set("CSeq", strconv.Itoa(c.cseq))
	req.Header.Set("User-Agent", c.config.UserAgent)
	if c.session != "" && req.Method != MethodOptions {
		req.Header.Set("Session", c.session)
	}
	if c.auth != nil {
		req.Header.Set("Authorization", c.auth.authorization(req.Method, req.URL))
	}
	p.deadline = time.Now().Add(c.config.RequestTimeout)
	c.armTimer(p.deadline)
	_ = c.conn.Send(req.Append(nil))
}

func (c *Client) armTimer(at time.Time) {
	c.timer.Reset(max(time.Until(at), 0))
}

// onResponse takes the response to the pending request, answering a 401
// once with credentials.
func (c *Client) onResponse(resp *Response) {
	p := c.pending
	if p == nil || resp.CSeq() != c.cseq {
		return
	}
	if resp.StatusCode == StatusUnauthorized && c.user != "" {
		stale := false
		for _, v := range resp.Header.Values("WWW-Authenticate") {
			stale = stale || strings.Contains(strings.ToLower(v), "stale=true")
		}
		if p.retries == 0 || (stale && p.retries < 3) {
			if a, ok := newClientAuth(c.user, c.password, resp.Header.Values("WWW-Authenticate")); ok {
				p.retries++
				c.auth = a
				c.sendPending()
				return
			}
		}
	}
	c.pending = nil
	c.scheduleNext()
	if resp.StatusCode >= 300 {
		c.failLocked(&StatusError{Method: p.req.Method, Code: resp.StatusCode, Reason: resp.Reason})
		return
	}
	p.cb(resp)
}

func (c *Client) onOptions(resp *Response) {
	c.public = resp.Header.Get("Public")
	if c.publish {
		c.announce()
		return
	}
	c.request(MethodDescribe, c.url, Header{{"Accept", "application/sdp"}}, nil, c.onDescribe)
}

func (c *Client) onDescribe(resp *Response) {
	sd, err := sdp.Parse(resp.Body)
	if err != nil {
		c.failLocked(err)
		return
	}
	c.base = c.url
	for _, h := range []string{"Content-Base", "Content-Location"} {
		if v := resp.Header.Get(h); v != "" {
			c.base = v
			break
		}
	}
	if v, ok := sd.Attribute("control"); ok && strings.Contains(v, "://") {
		c.base = v
	}
	for _, f := range rtpcodec.ParseSession(sd) {
		dp, err := rtpcodec.NewDepacketizer(f, rtpcodec.DepacketizerConfig{})
		if err != nil {
			continue
		}
		c.streams = append(c.streams, &clientStream{index: len(c.streams), format: f, dp: dp})
	}
	if len(c.streams) == 0 {
		c.failLocked(ErrNoTracks)
		return
	}
	c.tracks = nil
	for _, st := range c.streams {
		c.tracks = append(c.tracks, st.format.Track)
	}
	c.state = stateSetup
	c.setupNext(0)
}

func (c *Client) announce() {
	sd := sdp.New(sdp.NewSessionID(), 1)
	sd.SetLine('s', "fib")
	sd.SetConnection(sdp.Connection{NetType: "IN", AddrType: "IP4", Address: "0.0.0.0"})
	for _, st := range c.streams {
		md, err := st.format.Media()
		if err != nil {
			c.failLocked(err)
			return
		}
		sd.AddMedia(md)
	}
	c.base = c.url
	c.state = stateSetup
	c.request(MethodAnnounce, c.url, Header{{"Content-Type", "application/sdp"}}, sd.Marshal(), func(*Response) { c.setupNext(0) })
}

// setupNext sets up stream i, or plays or records once all are.
func (c *Client) setupNext(i int) {
	if i == len(c.streams) {
		if c.publish {
			c.request(MethodRecord, c.url, Header{{"Range", "npt=0.000-"}}, nil, c.onRecord)
		} else {
			c.request(MethodPlay, c.url, Header{{"Range", "npt=0.000-"}}, nil, c.onPlay)
		}
		return
	}
	st := c.streams[i]
	if c.config.Transport == TransportUDP {
		if st.rtpPort == 0 {
			// Binding waits for the engine's loops: off the worker.
			go c.bindUDP(i)
			return
		}
	}
	t := Transport{Interleaved: [2]int{-1, -1}, ClientPort: [2]int{-1, -1}, ServerPort: [2]int{-1, -1}}
	if c.config.Transport == TransportUDP {
		t.ClientPort = [2]int{st.rtpPort, st.rtcpPort}
	} else {
		t.TCP, t.Interleaved = true, [2]int{2 * i, 2*i + 1}
	}
	if c.publish {
		t.Mode = "record"
	}
	c.request(MethodSetup, resolveControl(c.base, st.format.Control), Header{{"Transport", t.String()}}, nil,
		func(resp *Response) { c.onSetup(i, resp) })
}

func (c *Client) onSetup(i int, resp *Response) {
	st := c.streams[i]
	if v := resp.Header.Get("Session"); v != "" {
		s := ParseSession(v)
		c.session = s.ID
		if s.Timeout > 0 {
			c.timeout = time.Duration(s.Timeout) * time.Second
		}
	}
	ts := ParseTransports(resp.Header.Get("Transport"))
	if len(ts) == 0 {
		c.failLocked(ErrUnexpected)
		return
	}
	t := ts[0]
	if c.config.Transport == TransportUDP {
		if t.TCP || t.ServerPort[0] < 0 {
			c.failLocked(ErrUnexpected)
			return
		}
		ip := c.conn.RemoteAddrPort().Addr().Unmap()
		rtcp := t.ServerPort[1]
		if rtcp < 0 {
			rtcp = t.ServerPort[0] + 1
		}
		st.serverRTP = netip.AddrPortFrom(ip, uint16(t.ServerPort[0]))
		st.serverRTCP = netip.AddrPortFrom(ip, uint16(rtcp))
		st.rtpOut = newUDPSender(c.config.Engine, st.rtpBind, st.serverRTP, int(c.config.MaxQueuedBytes))
		st.rtcpOut = newUDPSender(c.config.Engine, st.rtcpBind, st.serverRTCP, 64<<10)
		if !c.publish {
			st.reorder = rtpcodec.NewReorder(c.config.ReorderSize, c.config.ReorderDelay)
		}
	} else {
		if !t.TCP {
			c.failLocked(ErrUnexpected)
			return
		}
		st.tcp = true
		st.ch = uint8(2 * i)
		if t.Interleaved[0] >= 0 {
			st.ch = uint8(t.Interleaved[0])
		}
	}
	if c.publish {
		pk, err := rtpcodec.NewPacketizer(st.format, rtpcodec.PacketizerConfig{MTU: c.config.MTU})
		if err != nil {
			c.failLocked(err)
			return
		}
		st.pk = pk
	}
	c.setupNext(i + 1)
}

// bindUDP adds the RTP and RTCP bindings of stream i, an even port and the
// one after it where it can, and goes on with its SETUP.
func (c *Client) bindUDP(i int) {
	e := c.config.Engine
	c.mu.Lock()
	st := c.streams[i]
	c.mu.Unlock()
	rtpH := &clientUDPHandler{c: c, st: st}
	rtcpH := &clientUDPHandler{c: c, st: st, rtcp: true}
	host := c.config.UDPHost
	rtpBind, rtcpBind, rtpPort, rtcpPort := -1, -1, 0, 0
	var err error
	for try := 0; try < 8 && rtcpBind < 0; try++ {
		if rtpBind, err = e.AddBinding(fib.Binding{Network: "udp", Addr: net.JoinHostPort(host, "0"), Handler: rtpH}); err != nil {
			break
		}
		rtpPort = bindingPort(e, rtpBind)
		if rtpPort%2 == 0 {
			if b, err := e.AddBinding(fib.Binding{Network: "udp", Addr: net.JoinHostPort(host, strconv.Itoa(rtpPort+1)), Handler: rtcpH}); err == nil {
				rtcpBind, rtcpPort = b, rtpPort+1
				break
			}
		}
		_ = e.RemoveBinding(rtpBind)
		rtpBind = -1
	}
	if err == nil && rtcpBind < 0 {
		// No pair: any two ports.
		if rtpBind, err = e.AddBinding(fib.Binding{Network: "udp", Addr: net.JoinHostPort(host, "0"), Handler: rtpH}); err == nil {
			rtpPort = bindingPort(e, rtpBind)
			if rtcpBind, err = e.AddBinding(fib.Binding{Network: "udp", Addr: net.JoinHostPort(host, "0"), Handler: rtcpH}); err == nil {
				rtcpPort = bindingPort(e, rtcpBind)
			} else {
				_ = e.RemoveBinding(rtpBind)
			}
		}
	}
	c.lock()
	defer c.unlock()
	if err != nil || c.done {
		if err == nil {
			_ = e.RemoveBinding(rtpBind)
			_ = e.RemoveBinding(rtcpBind)
		} else {
			c.failLocked(err)
		}
		return
	}
	st.rtpBind, st.rtcpBind, st.rtpPort, st.rtcpPort = rtpBind, rtcpBind, rtpPort, rtcpPort
	c.setupNext(i)
}

func (c *Client) onPlay(resp *Response) {
	now := time.Now()
	for _, info := range ParseRTPInfo(resp.Header.Get("RTP-Info")) {
		if !info.HasTime {
			continue
		}
		for _, st := range c.streams {
			if matchControl(c.base, st.format.Control, info.URL) && !st.originSet {
				st.originSet = true
				st.dp.SetOrigin(info.RTPTime, recordEpoch)
			}
		}
	}
	c.startStreaming(now)
	tracks := slices.Clone(c.tracks)
	if f := c.config.OnTracks; f != nil {
		c.emit(func() { f(tracks) })
	}
}

func (c *Client) onRecord(*Response) {
	c.startStreaming(time.Now())
	if f := c.config.OnReady; f != nil {
		c.emit(f)
	}
}

func (c *Client) startStreaming(now time.Time) {
	c.state = stateStreaming
	c.lastRx = now
	c.scheduleNext()
}

// keepAlive returns the keep-alive interval.
func (c *Client) keepAlive() time.Duration {
	if c.config.KeepAlive > 0 {
		return c.config.KeepAlive
	}
	d := DefaultSessionTimeout / 2
	if c.timeout > 0 {
		d = c.timeout / 2
	}
	return min(d, 30*time.Second)
}

// scheduleNext arms the timer for what is due next: the pending request's
// deadline, the next keep-alive, or the read timeout.
func (c *Client) scheduleNext() {
	if c.pending != nil {
		c.armTimer(c.pending.deadline)
		return
	}
	if c.state != stateStreaming {
		return
	}
	if c.nextKA.IsZero() {
		c.nextKA = time.Now().Add(c.keepAlive())
	}
	next := c.nextKA
	if !c.publish {
		next = earliest(next, c.lastRx.Add(c.config.ReadTimeout))
	}
	for _, st := range c.streams {
		if st.reorder != nil {
			if at, ok := st.reorder.NextTimeout(); ok {
				next = earliest(next, at)
			}
		}
	}
	c.armTimer(next)
}

func (c *Client) onTimer() {
	c.lock()
	defer c.unlock()
	if c.done {
		return
	}
	now := time.Now()
	if c.closing.Load() {
		if c.conn == nil {
			// The dial is still on: its done hears the close.
			return
		}
		c.failLocked(ErrClientClosed)
		return
	}
	if c.pending != nil {
		if !now.Before(c.pending.deadline) {
			c.failLocked(ErrTimeout)
			return
		}
		c.armTimer(c.pending.deadline)
		return
	}
	if c.state != stateStreaming {
		return
	}
	if !c.publish && now.Sub(c.lastRx) >= c.config.ReadTimeout {
		c.failLocked(ErrTimeout)
		return
	}
	for _, st := range c.streams {
		if st.reorder != nil {
			st.reorder.Expire(now, func(p *rtp.Packet) { c.depacketize(st, now, p) })
		}
	}
	if !now.Before(c.nextKA) {
		c.nextKA = now.Add(c.keepAlive())
		method := MethodOptions
		if strings.Contains(c.public, MethodGetParameter) {
			method = MethodGetParameter
		}
		c.request(method, c.url, nil, nil, func(*Response) {})
		c.sendReceiverReports()
		return
	}
	c.scheduleNext()
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// sendReceiverReports sends an empty receiver report on each UDP stream
// played, which keeps NAT bindings toward the server open.
func (c *Client) sendReceiverReports() {
	for _, st := range c.streams {
		if st.rtcpOut == nil || c.publish {
			continue
		}
		if st.ssrc == 0 {
			st.ssrc = randUint32()
		}
		b, err := rtp.AppendRTCP(nil, &rtp.ReceiverReport{SSRC: st.ssrc})
		if err == nil {
			st.rtcpOut.send(b)
		}
	}
}

// failLocked ends the client once.
func (c *Client) failLocked(err error) {
	if c.done {
		return
	}
	c.done = true
	c.timer.Stop()
	if c.closing.Load() {
		err = ErrClientClosed
	}
	for _, st := range c.streams {
		if st.dp != nil && c.state == stateStreaming {
			st.dp.Flush(func(pkt *av.Packet) { c.deliver(st, pkt) })
		}
		if st.rtpOut != nil {
			st.rtpOut.close()
			st.rtcpOut.close()
		}
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	var binds []int
	for _, st := range c.streams {
		if st.rtpPort != 0 {
			binds = append(binds, st.rtpBind, st.rtcpBind)
		}
	}
	if len(binds) > 0 {
		e := c.config.Engine
		go func() {
			for _, b := range binds {
				_ = e.RemoveBinding(b)
			}
		}()
	}
	if f := c.config.OnClose; f != nil {
		c.emit(func() { f(err) })
	}
}

// deliver hands a depacketized packet to OnPacket, after OnTracks when its
// track changed.
func (c *Client) deliver(st *clientStream, pkt *av.Packet) {
	if t := st.dp.Track(); t != c.tracks[st.index] {
		c.tracks[st.index] = t
		if f := c.config.OnTracks; f != nil {
			tracks := slices.Clone(c.tracks)
			c.emit(func() { f(tracks) })
		}
	}
	pkt.Track = st.index
	if f := c.config.OnPacket; f != nil {
		c.emit(func() { f(pkt) })
	}
}

func (c *Client) depacketize(st *clientStream, now time.Time, p *rtp.Packet) {
	if !st.originSet {
		st.originSet = true
		st.dp.SetOrigin(p.Timestamp, c.sync.Origin(st.sr, p.Timestamp, st.format.ClockRate, recordEpoch))
	}
	_ = st.dp.Depacketize(p, func(pkt *av.Packet) { c.deliver(st, pkt) })
}

// receiveRTCP takes an RTCP packet of a played stream, whose sender
// reports line the streams up.
func (c *Client) receiveRTCP(st *clientStream, b []byte) {
	if !c.publish && st.dp != nil {
		st.sr.Update(b)
	}
}

// receiveRTP takes an RTP packet of a played stream.
func (c *Client) receiveRTP(st *clientStream, now time.Time, b []byte) {
	if c.publish || c.state != stateStreaming || st.dp == nil {
		return
	}
	c.lastRx = now
	if st.pkt.Unmarshal(b) != nil {
		return
	}
	if st.reorder == nil {
		c.depacketize(st, now, &st.pkt)
		return
	}
	st.reorder.Push(now, &st.pkt, func(p *rtp.Packet) { c.depacketize(st, now, p) })
	if at, ok := st.reorder.NextTimeout(); ok {
		c.armTimer(at)
	}
}

func (c *Client) handle(msg any) error {
	switch m := msg.(type) {
	case *Response:
		c.onResponse(m)
	case *Interleaved:
		for _, st := range c.streams {
			if st.tcp && st.ch == m.Channel {
				c.receiveRTP(st, time.Now(), m.Data)
				break
			}
			if st.tcp && st.ch+1 == m.Channel {
				c.receiveRTCP(st, m.Data)
				break
			}
		}
	case *Request:
		// Requests of the server (ANNOUNCE, SET_PARAMETER) are answered
		// but not acted on.
		resp := &Response{StatusCode: StatusNotImplemented}
		if cs, ok := m.Header.Lookup("CSeq"); ok {
			resp.Header.Add("CSeq", cs)
		}
		_ = c.conn.Send(resp.Append(nil))
	}
	return nil
}

// clientHandler is the fib.Handler of a client's connection.
type clientHandler Client

func (h *clientHandler) OnOpen(*fib.Connection)                 {}
func (h *clientHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *clientHandler) OnData(_ *fib.Connection, data []byte) {
	c := (*Client)(h)
	c.lock()
	defer c.unlock()
	if c.done {
		return
	}
	if err := c.parser.Feed(data, c.handle); err != nil {
		c.failLocked(err)
	}
}

func (h *clientHandler) OnClose(_ *fib.Connection, err error) {
	c := (*Client)(h)
	c.lock()
	defer c.unlock()
	if err == nil {
		err = net.ErrClosed
	}
	c.failLocked(err)
}

// clientUDPHandler takes the datagrams of a stream's UDP binding.
type clientUDPHandler struct {
	c    *Client
	st   *clientStream
	rtcp bool
}

func (h *clientUDPHandler) OnOpen(*fib.Connection)                 {}
func (h *clientUDPHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h *clientUDPHandler) OnClose(*fib.Connection, error)         {}

// OnData takes a datagram from the server's address; others are dropped.
func (h *clientUDPHandler) OnData(conn *fib.Connection, data []byte) {
	c := h.c
	c.lock()
	defer c.unlock()
	if c.done || conn.RemoteAddrPort().Addr().Unmap() != h.st.serverRTP.Addr() {
		return
	}
	now := time.Now()
	if h.rtcp {
		c.lastRx = now
		c.receiveRTCP(h.st, data)
		return
	}
	c.receiveRTP(h.st, now, data)
}
