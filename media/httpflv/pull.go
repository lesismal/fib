package httpflv

import (
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/websocket"
)

// Defaults for the PullConfig fields left at zero. The idle timeout is
// DefaultIdleTimeout, as a player's.
const (
	DefaultDialTimeout   = 10 * time.Second
	DefaultHeaderTimeout = 10 * time.Second
	DefaultMaxRedirects  = 5
	DefaultPullBuffer    = fibhttp.DefaultStreamResponseBodyBuffer
)

// Errors a Puller ends with.
var (
	// ErrClosed is what OnClose hears after Close.
	ErrClosed = errors.New("httpflv: puller closed")
	// ErrIdleTimeout ends a pull whose origin sent nothing for
	// PullConfig.IdleTimeout.
	ErrIdleTimeout = errors.New("httpflv: origin sent nothing for the idle timeout")
	// ErrTooManyRedirects ends a pull redirected more than
	// PullConfig.MaxRedirects times.
	ErrTooManyRedirects = errors.New("httpflv: too many redirects")
)

// StatusError is the response of an origin that did not answer the pull
// with 200 OK: its status and the start of its body.
type StatusError struct {
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("httpflv: origin answered %d: %s", e.StatusCode, e.Body)
}

// PullConfig configures Pull.
type PullConfig struct {
	// Engine dials the origin and reads what it sends. It must be running.
	Engine *fib.Engine
	// TLSConfig is used for https:// and wss:// URLs; nil means a default
	// config for the URL's host. An https:// pull speaks HTTP/2 to an
	// origin that chooses it, unless TLSConfig.NextProtos says otherwise.
	TLSConfig *stdtls.Config
	// Header is sent with the request, as a Referer or a token a CDN asks
	// for.
	Header http.Header
	// Dial opens the connection in place of the engine's DialWithHandler,
	// as ClientConfig.Dial of fib/http does: a socks.Dialer's Dial reaches
	// the origin through a SOCKS proxy.
	Dial func(engine *fib.Engine, network, addr string, timeout time.Duration,
		handler fib.Handler, done func(*fib.Connection, error)) error
	// DialTimeout bounds the connect, and HeaderTimeout the wait for the
	// response's header once the request is sent (for ws:// and wss://,
	// the two together bound the handshake). Zero means
	// DefaultDialTimeout and DefaultHeaderTimeout.
	DialTimeout   time.Duration
	HeaderTimeout time.Duration
	// IdleTimeout ends a pull whose origin sends nothing for this long,
	// from the response's header on. Zero means DefaultIdleTimeout, and a
	// negative value no timeout.
	IdleTimeout time.Duration
	// MaxRedirects is how many 301, 302, 303, 307 and 308 answers the pull
	// follows, as CDNs send to an edge. Zero means DefaultMaxRedirects,
	// and a negative value none.
	MaxRedirects int
	// BufferSize is how much of the body may wait to be demuxed before the
	// connection stops being read. Zero means DefaultPullBuffer.
	BufferSize int
	// MaxTagSize bounds one FLV tag, as DemuxerConfig.MaxTagSize does.
	MaxTagSize int
	// OnTracks gives the stream's tracks, once they are known and again
	// when they change; OnPacket each packet, its Track indexing them.
	// They are called one at a time, after the hub has them, on the
	// worker reading the origin, and must not block. OnClose hears the end
	// of the pull, once: nil when the origin ended the stream, ErrClosed
	// after Close, or what stopped it, such as a StatusError or the hub
	// refusing the path.
	OnTracks func(tracks []*av.Track)
	OnPacket func(pkt *av.Packet)
	OnClose  func(err error)
}

// Puller is an HTTP-FLV or WebSocket-FLV stream being pulled from an
// origin; see Pull.
type Puller struct {
	config PullConfig
	url    *url.URL
	h      *hub.Hub
	path   string
	query  string
	client *fibhttp.Client
	ws     *websocket.Dialer
	dmx    *flv.Demuxer
	dec    *flv.Decoder
	// pub is touched only while delivering, or once the pull has ended.
	pub *hub.Publisher
	// stop says the pull is ending: the delivery under way stops at its
	// next tag.
	stop atomic.Bool
	// last is when the origin last sent anything, in Unix nanoseconds.
	last atomic.Int64

	mu        sync.Mutex
	remote    netip.AddrPort
	conn      *fib.Connection
	call      fibhttp.Call
	resp      *fibhttp.ClientResponse
	timer     *time.Timer
	redirects int
	// delivering says a callback of the client is passing what the origin
	// sent on: an end that comes meanwhile waits for it (err holds the
	// reason), so that the publisher and the user's callbacks are never
	// called after OnClose.
	delivering bool
	err        error
	ended      bool
}

// Pull plays the HTTP-FLV stream at rawURL, an http:// or https:// URL,
// or the WebSocket-FLV stream at a ws:// or wss:// one, and publishes it
// into h at path, as a relay or a pull proxy does. It returns at once.
//
// The path is reserved (hub.Hub.Reserve) once the origin answers 200 OK,
// with PublishInfo protocol "http-flv-pull" ("ws-flv-pull" for
// WebSocket-FLV), the origin's address and rawURL's query for the hub's
// OnPublish hook; a refusal, hub.ErrStreamBusy for a path that has a
// publisher, ends the pull. The stream goes live once the tracks are
// known, a change of them goes to the publisher's SetTracks, and the end
// of the pull unpublishes it. Its timestamps are the origin's, unwrapped
// from FLV's 32 bits; an origin that keeps a player's timestamps
// increasing across a republish, as Handler does, keeps the pulled
// stream's increasing too. Pulling again after an end republishes the
// path, which players that wait for a publisher (Config.Wait) follow.
//
// With a nil h nothing is published, and the tracks and packets go to
// config's callbacks alone.
func Pull(config PullConfig, rawURL string, h *hub.Hub, path string) (*Puller, error) {
	if config.Engine == nil {
		return nil, errors.New("httpflv: PullConfig needs an Engine")
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = DefaultDialTimeout
	}
	if config.HeaderTimeout <= 0 {
		config.HeaderTimeout = DefaultHeaderTimeout
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.MaxRedirects == 0 {
		config.MaxRedirects = DefaultMaxRedirects
	}
	if config.BufferSize <= 0 {
		config.BufferSize = DefaultPullBuffer
	}
	p := &Puller{config: config, url: u, h: h, path: path, query: u.RawQuery,
		dmx: flv.NewDemuxer(flv.DemuxerConfig{MaxTagSize: config.MaxTagSize})}
	p.dec = flv.NewDecoder(flv.DecoderConfig{OnTracks: p.onTracks, OnPacket: p.onPacket})
	dial := config.Dial
	if dial == nil {
		dial = (*fib.Engine).DialWithHandler
	}
	// The dial is watched for the origin's address, which the hub's
	// OnPublish hook is given, and for the connection of WebSocket-FLV,
	// which Close closes.
	watch := func(e *fib.Engine, network, addr string, timeout time.Duration, handler fib.Handler, done func(*fib.Connection, error)) error {
		return dial(e, network, addr, timeout, handler, func(c *fib.Connection, err error) {
			if err == nil {
				p.mu.Lock()
				p.remote, p.conn = c.RemoteAddrPort(), c
				p.mu.Unlock()
			}
			done(c, err)
		})
	}
	switch u.Scheme {
	case "http", "https":
		p.client = fibhttp.NewClient(config.Engine, fibhttp.ClientConfig{
			ResponseHeaderTimeout:       config.HeaderTimeout,
			DialTimeout:                 config.DialTimeout,
			Dial:                        watch,
			StreamResponseBody:          true,
			StreamResponseBodyThreshold: 4 << 10,
			StreamUnknownLengthAtHeader: true,
			StreamResponseBodyBuffer:    config.BufferSize,
			TLSConfig:                   config.TLSConfig,
		})
		p.request(u)
	case "ws", "wss":
		p.ws = websocket.NewDialer(config.Engine, websocket.DialerConfig{
			HandshakeTimeout: config.DialTimeout + config.HeaderTimeout,
			MaxMessageBytes:  int64(max(config.MaxTagSize, flv.DefaultMaxTagSize)) + 64<<10,
			TLSConfig:        config.TLSConfig,
			Dial:             watch,
		})
		p.ws.Dial(rawURL, config.Header, wsPull{p}, p.onWebSocket)
	default:
		return nil, fmt.Errorf("httpflv: unsupported URL scheme %q", u.Scheme)
	}
	return p, nil
}

// Close ends the pull, which unpublishes the stream. OnClose hears
// ErrClosed. It never blocks, and may be called from the callbacks.
func (p *Puller) Close() { p.finish(ErrClosed) }

func (p *Puller) protocol() string {
	if p.ws != nil {
		return "ws-flv-pull"
	}
	return "http-flv-pull"
}

// request sends the GET of u.
func (p *Puller) request(u *url.URL) {
	req, _ := http.NewRequest(http.MethodGet, u.String(), nil)
	for k, v := range p.config.Header {
		req.Header[k] = v
	}
	p.url = u
	call := p.client.Start(req, p.onResponse)
	p.mu.Lock()
	p.call = call
	ended := p.ended
	p.mu.Unlock()
	if ended {
		call.Cancel(ErrClosed)
	}
}

// begin marks a delivery under way, and reports false once the pull is
// ending, when there is nothing to deliver.
func (p *Puller) begin() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ended || p.err != nil {
		return false
	}
	p.delivering = true
	return true
}

// done ends a delivery, and the pull with err, or with what came while it
// went on.
func (p *Puller) done(err error) {
	p.mu.Lock()
	p.delivering = false
	if p.err != nil {
		err = p.err
	}
	p.mu.Unlock()
	if err != nil {
		p.finish(err)
	}
}

func (p *Puller) onResponse(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		p.finish(err)
		return
	}
	if !p.begin() {
		resp.Close()
		return
	}
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		next, err := p.url.Parse(resp.Header.Get("Location"))
		resp.Close()
		if err != nil {
			p.done(err)
			return
		}
		p.redirects++
		if p.redirects > p.config.MaxRedirects {
			p.done(ErrTooManyRedirects)
			return
		}
		p.done(nil)
		p.request(next)
		return
	default:
		body, _ := resp.BodyBytes()
		resp.Close()
		p.done(&StatusError{StatusCode: resp.StatusCode, Body: string(body[:min(len(body), 256)])})
		return
	}
	if err := p.reserve(); err != nil {
		resp.Close()
		p.done(err)
		return
	}
	p.mu.Lock()
	p.resp = resp
	p.mu.Unlock()
	resp.OnBody(p.onBody)
	p.done(nil)
}

func (p *Puller) onWebSocket(_ *websocket.Connection, resp *http.Response, err error) {
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			err = &StatusError{StatusCode: resp.StatusCode}
		}
		p.finish(err)
		return
	}
	if !p.begin() {
		return
	}
	p.done(p.reserve())
}

// reserve claims the hub path, and starts the idle timer: the origin has
// answered.
func (p *Puller) reserve() error {
	p.mu.Lock()
	remote := p.remote
	p.mu.Unlock()
	if p.h != nil {
		pub, err := p.h.Reserve(p.path, hub.PublishInfo{Protocol: p.protocol(), Remote: remote, Query: p.query})
		if err != nil {
			return err
		}
		p.pub = pub
	}
	p.last.Store(time.Now().UnixNano())
	if idle := p.config.IdleTimeout; idle > 0 {
		p.mu.Lock()
		p.timer = time.AfterFunc(idle, p.checkIdle)
		p.mu.Unlock()
	}
	return nil
}

// checkIdle ends a pull the origin has sent nothing for since the idle
// timeout, and otherwise waits until it could have.
func (p *Puller) checkIdle() {
	idle := p.config.IdleTimeout
	left := time.Duration(p.last.Load() + int64(idle) - time.Now().UnixNano())
	if left <= 0 {
		p.finish(ErrIdleTimeout)
		return
	}
	p.mu.Lock()
	if !p.ended {
		p.timer.Reset(left)
	}
	p.mu.Unlock()
}

// errStop stops a Feed once the pull is ending.
var errStop = errors.New("httpflv: pull stopped")

// feed demuxes and decodes what the origin sent, and reports the error
// that ends the pull, if any.
func (p *Puller) feed(data []byte) error {
	p.last.Store(time.Now().UnixNano())
	err := p.dmx.Feed(data, func(tag flv.Tag) error {
		if p.stop.Load() {
			return errStop
		}
		// A malformed tag is dropped, as one bad tag should not end a
		// stream.
		_ = p.dec.WriteTag(tag.Type, tag.Timestamp, tag.Data)
		return nil
	})
	if err == errStop {
		return nil
	}
	return err
}

func (p *Puller) onBody(data []byte, fin bool, err error) {
	if !p.begin() {
		return
	}
	if err == nil {
		err = p.feed(data)
	}
	if err == nil && fin {
		// The origin ended the stream: OnClose hears nil, unless Close
		// came first.
		p.dec.Flush()
		p.end()
		return
	}
	p.done(err)
}

// end ends a delivery and the pull, as an origin that ended the stream
// does.
func (p *Puller) end() {
	p.mu.Lock()
	p.delivering = false
	p.mu.Unlock()
	p.finish(nil)
}

func (p *Puller) onTracks(tracks []*av.Track) {
	if p.pub != nil {
		p.pub.SetTracks(slices.Clone(tracks))
	}
	if f := p.config.OnTracks; f != nil {
		f(tracks)
	}
}

func (p *Puller) onPacket(pkt *av.Packet) {
	if p.pub != nil {
		p.pub.WritePacket(pkt)
	}
	if f := p.config.OnPacket; f != nil {
		f(pkt)
	}
}

// finish ends the pull with err, once: the request or the connection, the
// idle timer, and the hub stream, then OnClose. A delivery under way ends
// it instead, when it is done.
func (p *Puller) finish(err error) {
	p.stop.Store(true)
	p.mu.Lock()
	if p.ended {
		p.mu.Unlock()
		return
	}
	if p.err == nil {
		p.err = err
	}
	if p.delivering {
		p.mu.Unlock()
		return
	}
	p.ended = true
	err = p.err
	call, resp, conn, timer := p.call, p.resp, p.conn, p.timer
	p.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	call.Cancel(ErrClosed)
	if resp != nil {
		resp.Close()
	}
	if p.client != nil {
		p.client.Close()
	} else if conn != nil {
		_ = conn.Close()
	}
	if p.pub != nil {
		p.pub.Close()
	}
	if f := p.config.OnClose; f != nil {
		f(err)
	}
}

// wsPull takes the messages of a WebSocket-FLV pull.
type wsPull struct{ p *Puller }

func (w wsPull) OnOpen(*websocket.Connection, *http.Request) {}

func (w wsPull) OnMessage(_ *websocket.Connection, op websocket.Opcode, data []byte) {
	if op != websocket.Binary || !w.p.begin() {
		return
	}
	w.p.done(w.p.feed(data))
}

func (w wsPull) OnClose(_ *websocket.Connection, code uint16, reason string, err error) {
	if !w.p.begin() {
		return
	}
	// The origin ended the stream: cleanly with a normal close, whatever
	// became of the connection after it.
	switch {
	case code == websocket.CloseNormal:
		err = nil
	case err == nil:
		err = fmt.Errorf("httpflv: origin closed the WebSocket: %d %s", code, reason)
	}
	w.p.dec.Flush()
	if err != nil {
		w.p.done(err)
		return
	}
	w.p.end()
}
