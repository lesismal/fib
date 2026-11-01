package httpflv

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/websocket"
)

// Defaults for the Config fields left at zero.
const (
	DefaultWriteQueueLimit = 2 << 20
	DefaultMaxWriteQueue   = 16 << 20
	DefaultIdleTimeout     = 30 * time.Second
)

// closePolicyViolation is the WebSocket close code (RFC 6455 section
// 7.4.1) a player gets when its stream cannot be played.
const closePolicyViolation = 1008

// Config is how a Handler serves its players.
type Config struct {
	// Hub is where the streams come from.
	Hub *hub.Hub
	// Path maps a request to the hub path it plays and the query that goes
	// with it, or reports false for a request that names no stream, which
	// is answered 404. Nil means DefaultPath: /app/stream.flv plays
	// "app/stream".
	Path func(r *http.Request) (path, query string, ok bool)
	// OnPlay decides a play: an error refuses it with 403 Forbidden, after
	// the hub's own OnSubscribe hook has had its say. It runs on the
	// request's handler goroutine.
	OnPlay func(c *fibhttp.Context, path, query string) error
	// CORS, when not empty, is sent as Access-Control-Allow-Origin, which
	// a web player on another origin (flv.js, mpegts.js) needs.
	CORS string
	// Wait lets a player wait this long for a stream that has no publisher
	// yet, or whose publisher leaves; zero answers such a request 404.
	Wait time.Duration
	// WriteQueueLimit is the backlog past which a player's packets are
	// dropped, video until the next keyframe, and MaxWriteQueue the one
	// past which an HTTP/1 player is disconnected. Zero means
	// DefaultWriteQueueLimit and DefaultMaxWriteQueue.
	WriteQueueLimit int64
	MaxWriteQueue   int64
	// IdleTimeout ends a player whose output has not moved for this long.
	// Zero means DefaultIdleTimeout, and a negative value no timeout.
	IdleTimeout time.Duration
	// Encoder configures how streams are written, such as H.265 with the
	// legacy codec id flv.js forks expect.
	Encoder flv.EncoderConfig
	// WebSocket configures the WebSocket-FLV upgrade. The zero value
	// accepts every origin; set CheckOrigin to restrict it.
	WebSocket websocket.Config
}

// DefaultPath maps /app/stream.flv, with any number of segments before
// the stream, to the hub path "app/stream" and the request's query.
func DefaultPath(r *http.Request) (string, string, bool) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	path, ok := strings.CutSuffix(p, ".flv")
	if !ok || path == "" || strings.HasSuffix(path, "/") {
		return "", "", false
	}
	return path, r.URL.RawQuery, true
}

// Handler serves hub streams as HTTP-FLV and WebSocket-FLV. It is a
// fib/http Handler, for a fib/http server or router:
//
//	h := httpflv.NewHandler(httpflv.Config{Hub: hub})
//	router.Get("/*", h.ServeHTTP)
type Handler struct {
	config Config
	ws     *websocket.ServerHandler
	// sockets maps each WebSocket player to its sink, for the WebSocket
	// handler's OnClose.
	mu      sync.Mutex
	sockets map[*websocket.Connection]*sink
}

var _ fibhttp.Handler = (*Handler)(nil)

// NewHandler returns a Handler, with the defaults in place of config's
// zeros.
func NewHandler(config Config) *Handler {
	if config.Path == nil {
		config.Path = DefaultPath
	}
	if config.WriteQueueLimit <= 0 {
		config.WriteQueueLimit = DefaultWriteQueueLimit
	}
	if config.MaxWriteQueue <= 0 {
		config.MaxWriteQueue = DefaultMaxWriteQueue
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.WebSocket.MaxMessageBytes == 0 {
		config.WebSocket.MaxMessageBytes = 4096
	}
	if config.WebSocket.CheckOrigin == nil {
		config.WebSocket.CheckOrigin = func(*http.Request) bool { return true }
	}
	h := &Handler{config: config, sockets: make(map[*websocket.Connection]*sink)}
	h.ws = websocket.NewHandlerWithConfig(config.WebSocket, websocketHandler{h})
	return h
}

func isUpgrade(r *http.Request) bool {
	if r.ProtoMajor >= 2 {
		return r.Method == http.MethodConnect
	}
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// ServeHTTP plays the stream the request names: a GET gets it as the body
// of a response that never ends, a WebSocket upgrade as one binary message
// per FLV tag.
func (h *Handler) ServeHTTP(c *fibhttp.Context) {
	r := c.Request
	path, query, ok := h.config.Path(r)
	if !ok {
		_ = c.String(http.StatusNotFound, "not found")
		return
	}
	upgrade := isUpgrade(r)
	if !upgrade && r.Method != http.MethodGet && r.Method != http.MethodHead {
		c.Header().Set("Allow", "GET, HEAD")
		_ = c.String(http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.config.OnPlay != nil {
		if err := h.config.OnPlay(c, path, query); err != nil {
			_ = c.String(http.StatusForbidden, "%s", err.Error())
			return
		}
	}
	protocol := "http-flv"
	if upgrade {
		protocol = "ws-flv"
	}
	opts := hub.SubscribeOptions{
		Info: hub.SubscribeInfo{Protocol: protocol, Remote: c.Conn.RemoteAddrPort(), Query: query},
		Wait: h.config.Wait,
	}
	s := &sink{h: h, conn: c.Conn, mux: flv.NewMuxer(h.config.Encoder), http1: r.ProtoMajor == 1}
	s.dropper = hub.NewDropper(s, s.backlog, h.config.WriteQueueLimit)
	if upgrade {
		h.serveWebSocket(c, s, path, opts)
		return
	}
	if r.Method == http.MethodHead {
		h.header(c)
		c.WriteHeader(http.StatusOK)
		return
	}
	h.serveHTTP(c, s, path, opts)
}

func (h *Handler) header(c *fibhttp.Context) {
	header := c.Header()
	header.Set("Content-Type", "video/x-flv")
	header.Set("Cache-Control", "no-cache, no-store")
	if h.config.CORS != "" {
		header.Set("Access-Control-Allow-Origin", h.config.CORS)
	}
}

func (h *Handler) serveHTTP(c *fibhttp.Context, s *sink, path string, opts hub.SubscribeOptions) {
	s.c = c
	// The response begins only once the subscription has: a stream that is
	// not there is answered 404, and the header goes with the first tags.
	c.Retain()
	sub, err := h.config.Hub.Subscribe(path, s.dropper, opts)
	if err != nil {
		c.Release()
		status := http.StatusForbidden
		if errors.Is(err, hub.ErrNotFound) {
			status = http.StatusNotFound
		}
		_ = c.String(status, "%s", err.Error())
		return
	}
	if !s.attach(sub) {
		return
	}
	if !s.began() {
		// Waiting for a publisher: the header goes now, so that the
		// client knows the server is there.
		s.mu.Lock()
		s.beginLocked()
		s.mu.Unlock()
	}
	if s.http1 {
		c.Conn.SetWriteQueueLimit(h.config.MaxWriteQueue)
	}
	c.OnCancel(func(error) { s.end() })
}

func (h *Handler) serveWebSocket(c *fibhttp.Context, s *sink, path string, opts hub.SubscribeOptions) {
	ws, err := h.ws.Upgrade(c, nil)
	if err != nil {
		return
	}
	s.ws = ws
	h.mu.Lock()
	h.sockets[ws] = s
	h.mu.Unlock()
	sub, err := h.config.Hub.Subscribe(path, s.dropper, opts)
	if err != nil {
		_ = ws.Close(closePolicyViolation, "stream not found")
		return
	}
	s.attach(sub)
}

// websocketHandler ends a WebSocket player's subscription when it closes.
type websocketHandler struct{ h *Handler }

func (w websocketHandler) OnOpen(*websocket.Connection, *http.Request)               {}
func (w websocketHandler) OnMessage(*websocket.Connection, websocket.Opcode, []byte) {}

func (w websocketHandler) OnClose(ws *websocket.Connection, _ uint16, _ string, _ error) {
	w.h.mu.Lock()
	s := w.h.sockets[ws]
	delete(w.h.sockets, ws)
	w.h.mu.Unlock()
	if s != nil {
		s.end()
	}
}

// sink writes a hub stream to one player as FLV.
type sink struct {
	h       *Handler
	conn    *fib.Connection
	http1   bool
	dropper hub.Sink
	// above says the last HTTP/2 or HTTP/3 write reported the stream's
	// flow control holding more than its limit, until it falls back.
	above atomic.Bool

	mu      sync.Mutex
	c       *fibhttp.Context
	ws      *websocket.Connection
	sub     *hub.Subscription
	mux     *flv.Muxer
	buf     []byte
	header  bool
	started bool
	// closed stops writes; ended says end has run, which it does once.
	closed  bool
	ended   bool
	writeAt time.Time
}

// backlog is what the player has queued: the connection's queue on
// HTTP/1 and WebSocket, and past the limit whenever HTTP/2's or HTTP/3's
// flow control holds the stream back.
func (s *sink) backlog() int64 {
	if s.above.Load() {
		return s.h.config.WriteQueueLimit + 1
	}
	if outputwait.Queued == nil {
		return 0
	}
	return outputwait.Queued(s.conn)
}

// attach keeps the subscription for end, and reports whether the play goes
// on: one that ended, or failed to write, while the subscription was being
// made is ended now.
func (s *sink) attach(sub *hub.Subscription) bool {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		sub.Close()
		return false
	}
	s.sub = sub
	closed := s.closed
	s.mu.Unlock()
	if closed {
		s.end()
	}
	return !closed
}

func (s *sink) began() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.header
}

// beginLocked sends the response header of an HTTP player.
func (s *sink) beginLocked() {
	if s.header || s.c == nil {
		return
	}
	s.header = true
	s.h.header(s.c)
	s.c.WriteHeader(http.StatusOK)
	s.c.Flush()
}

func (s *sink) OnTracks(tracks []*av.Track) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if !s.started {
		probe := flv.NewEncoder(s.h.config.Encoder)
		probe.SetTracks(tracks)
		if video, audio := probe.Tracks(); video == nil && audio == nil {
			// Nothing FLV carries yet: a publisher that has not sent its
			// sequence headers. The tracks come again.
			return
		}
		s.started = true
		s.buf, _ = s.mux.AppendHeader(s.buf[:0], tracks)
	} else {
		s.buf, _ = s.mux.AppendTracks(s.buf[:0], tracks)
	}
	s.writeLocked(s.buf)
}

func (s *sink) OnPacket(pkt *av.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.started {
		return
	}
	b, err := s.mux.AppendPacket(s.buf[:0], pkt)
	s.buf = b
	if err != nil || len(b) == 0 {
		return
	}
	s.writeLocked(b)
}

// OnClose ends the response when the stream ends.
func (s *sink) OnClose(error) { s.end() }

func (s *sink) writeLocked(b []byte) {
	if s.ws != nil {
		if s.ws.WriteBinary(b) != nil {
			s.closed = true
		}
	} else {
		s.beginLocked()
		above, err := s.c.WriteNoWait(b)
		if err != nil {
			s.closed = true
			_ = s.conn.Close()
			return
		}
		if above && !s.http1 && !s.above.Swap(true) {
			if !s.c.OnWritable(func(error) { s.above.Store(false) }) {
				s.above.Store(false)
			}
		}
	}
	if idle := s.h.config.IdleTimeout; idle > 0 {
		if now := time.Now(); now.Sub(s.writeAt) >= time.Second {
			s.writeAt = now
			if s.ws != nil {
				_ = s.conn.SetWriteDeadline(now.Add(idle))
			} else {
				_ = s.c.SetWriteDeadline(now.Add(idle))
			}
		}
	}
}

// end ends the play, once: the subscription, and the response or the
// WebSocket.
func (s *sink) end() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.closed, s.ended = true, true
	sub := s.sub
	s.sub = nil
	c, ws := s.c, s.ws
	s.mu.Unlock()
	if sub != nil {
		sub.Close()
	}
	if ws != nil {
		_ = ws.Close(websocket.CloseNormal, "stream ended")
	} else if c != nil {
		c.Release()
	}
}
