//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package hls

import (
	"errors"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/hub"
)

// Defaults of HandlerConfig's zero fields.
const (
	DefaultIdleTimeout   = 30 * time.Second
	DefaultWaitTimeout   = 10 * time.Second
	DefaultRepublishWait = 10 * time.Second
)

// Media types of the playlists (RFC 8216 section 4).
const PlaylistContentType = "application/vnd.apple.mpegurl"

// HandlerConfig configures a Handler.
type HandlerConfig struct {
	// Hub is where the streams come from.
	Hub *hub.Hub
	// Muxer configures the Muxer of every stream.
	Muxer Config
	// Prefix is the part of the URL path before the stream's path: with
	// "/hls/", /hls/live/s/index.m3u8 is the playlist of stream "live/s".
	// Default "/".
	Prefix string
	// IdleTimeout is how long a stream's Muxer lives without a request
	// before it is stopped and its subscription closed. Default 30 s.
	IdleTimeout time.Duration
	// WaitTimeout bounds how long a request waits: for a stream's first
	// segment, for a blocking playlist reload, for a part a preload hint
	// named. Default 10 s.
	WaitTimeout time.Duration
	// RepublishWait is how long a Muxer outlives its publisher waiting for
	// another (hub.SubscribeOptions.Wait), which continues the playlist
	// after a discontinuity rather than ending it. Default 10 s; negative
	// ends the playlist when the publisher leaves.
	RepublishWait time.Duration
	// AllowOrigin, when set, is the Access-Control-Allow-Origin of every
	// response, as "*", so that players on other origins may fetch.
	AllowOrigin string
	// MaxStreams bounds the Muxers running at once; 0 is unlimited.
	MaxStreams int
}

// Handler serves the streams of a hub over HLS, under fib/http: a request
// for a stream's playlist starts a Muxer for it, subscribed to the hub, and
// requests for its playlists, segments and parts are answered from that
// Muxer until no request has come for IdleTimeout. For a stream at path p:
//
//	<Prefix>p/index.m3u8  the multivariant playlist when the stream has
//	                      audio and video, else the media playlist
//	<Prefix>p/media.m3u8  the media playlist
//	<Prefix>p/init-*, seg-*, part-*   what the playlists name
//
// A stream the hub does not have is 404. A playlist request waits, without
// holding a worker (http.Context.Retain), for the stream's first segment,
// and with Low-Latency HLS for the segment or part _HLS_msn and _HLS_part
// name; a request for the part a preload hint named waits for it too.
// Playlists are sent with Cache-Control: no-cache, everything else with a
// max-age of an hour, since the Muxer's file names are never reused.
type Handler struct {
	config  HandlerConfig
	mu      sync.Mutex
	streams map[string]*entry
	closed  bool
}

type entry struct {
	path  string
	m     *Muxer
	sub   *hub.Subscription
	last  atomic.Int64
	timer *time.Timer
}

// NewHandler returns a Handler.
func NewHandler(config HandlerConfig) *Handler {
	if config.Prefix == "" {
		config.Prefix = "/"
	}
	if !strings.HasSuffix(config.Prefix, "/") {
		config.Prefix += "/"
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.WaitTimeout <= 0 {
		config.WaitTimeout = DefaultWaitTimeout
	}
	if config.RepublishWait == 0 {
		config.RepublishWait = DefaultRepublishWait
	} else if config.RepublishWait < 0 {
		config.RepublishWait = 0
	}
	return &Handler{config: config, streams: map[string]*entry{}}
}

// Close stops every Muxer and closes its subscription.
func (h *Handler) Close() {
	h.mu.Lock()
	streams := h.streams
	h.streams, h.closed = map[string]*entry{}, true
	h.mu.Unlock()
	for _, e := range streams {
		e.stop()
	}
}

func (e *entry) stop() {
	e.timer.Stop()
	e.sub.Close()
	e.m.Close()
}

// Muxer returns the Muxer serving the stream at path, or nil.
func (h *Handler) Muxer(path string) *Muxer {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.streams[path]; e != nil {
		return e.m
	}
	return nil
}

// errNoStream is the hub not having the stream.
var errNoStream = errors.New("hls: no such stream")

// muxer returns the entry of path, starting one when start is set.
func (h *Handler) muxer(path string, start bool, c *http.Context) (*entry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.streams[path]
	if e != nil && e.m.Ended() && start && h.publishing(path) {
		// A playlist that ended while its publisher came back: start over.
		delete(h.streams, path)
		e.stop()
		e = nil
	}
	if e != nil || !start {
		if e == nil {
			return nil, errNoStream
		}
		e.last.Store(time.Now().UnixNano())
		return e, nil
	}
	if h.closed || !h.publishing(path) {
		return nil, errNoStream
	}
	if h.config.MaxStreams > 0 && len(h.streams) >= h.config.MaxStreams {
		return nil, hub.ErrLimit
	}
	m := NewMuxer(h.config.Muxer)
	info := hub.SubscribeInfo{Protocol: "hls"}
	if c.Conn != nil {
		info.Remote = c.Conn.RemoteAddrPort()
	}
	if c.Request.URL != nil {
		info.Query = c.Request.URL.RawQuery
	}
	sub, err := h.config.Hub.Subscribe(path, m, hub.SubscribeOptions{Info: info, Wait: h.config.RepublishWait})
	if err != nil {
		return nil, err
	}
	e = &entry{path: path, m: m, sub: sub}
	e.last.Store(time.Now().UnixNano())
	e.timer = time.AfterFunc(h.config.IdleTimeout, func() { h.idle(e) })
	h.streams[path] = e
	return e, nil
}

// publishing reports whether the hub has a publisher on path.
func (h *Handler) publishing(path string) bool {
	for _, s := range h.config.Hub.Streams() {
		if s.Path == path {
			return s.Publishing
		}
	}
	return false
}

// idle stops e when no request has come for IdleTimeout, and checks again
// later otherwise.
func (h *Handler) idle(e *entry) {
	left := time.Duration(e.last.Load()+int64(h.config.IdleTimeout)) - time.Duration(time.Now().UnixNano())
	if e.m.waiting() {
		// A request waiting on it is a request.
		left = h.config.IdleTimeout
	}
	if left > 0 {
		e.timer.Reset(left)
		return
	}
	h.mu.Lock()
	if h.streams[e.path] == e {
		delete(h.streams, e.path)
	}
	h.mu.Unlock()
	e.stop()
}

// ServeHTTP serves a request for a playlist or a file of a stream.
func (h *Handler) ServeHTTP(c *http.Context) {
	switch c.Request.Method {
	case stdhttp.MethodGet, stdhttp.MethodHead:
	case stdhttp.MethodOptions:
		h.respond(c, stdhttp.StatusNoContent, "", "", nil)
		return
	default:
		h.respond(c, stdhttp.StatusMethodNotAllowed, "", "", nil)
		return
	}
	path, ok := strings.CutPrefix(c.Request.URL.Path, h.config.Prefix)
	i := strings.LastIndexByte(path, '/')
	if !ok || i <= 0 {
		h.notFound(c)
		return
	}
	stream, name := path[:i], path[i+1:]
	playlist := name == "index.m3u8" || name == "media.m3u8"
	e, err := h.muxer(stream, playlist, c)
	switch {
	case err == hub.ErrLimit:
		h.respond(c, stdhttp.StatusServiceUnavailable, "", "", nil)
		return
	case err != nil:
		// Not found, or refused by the hub's OnSubscribe.
		h.notFound(c)
		return
	}
	if playlist {
		h.servePlaylist(c, e.m, name == "index.m3u8")
		return
	}
	h.serveFile(c, e.m, name)
}

func (h *Handler) notFound(c *http.Context) {
	h.respond(c, stdhttp.StatusNotFound, "", "", nil)
}

func (h *Handler) badRequest(c *http.Context) {
	h.respond(c, stdhttp.StatusBadRequest, "", "", nil)
}

// respond answers with status and body, with the content type and
// Cache-Control given and the CORS header configured.
func (h *Handler) respond(c *http.Context, status int, contentType, cache string, body []byte) {
	header := stdhttp.Header{}
	if contentType != "" {
		header["Content-Type"] = []string{contentType}
	}
	if cache != "" {
		header["Cache-Control"] = []string{cache}
	}
	if h.config.AllowOrigin != "" {
		header["Access-Control-Allow-Origin"] = []string{h.config.AllowOrigin}
		if c.Request.Method == stdhttp.MethodOptions {
			header["Access-Control-Allow-Methods"] = []string{"GET, HEAD, OPTIONS"}
			header["Access-Control-Allow-Headers"] = []string{"*"}
		}
	}
	_ = c.WriteResponse(http.Response{StatusCode: status, Header: header, Body: body})
}

const (
	cachePlaylist = "no-cache"
	cacheMedia    = "max-age=3600"
)

// servePlaylist answers a playlist request, once what it waits for is
// there.
func (h *Handler) servePlaylist(c *http.Context, m *Muxer, index bool) {
	st := m.status()
	msn, part := int64(-1), int64(-1)
	if !index || !st.hasAV {
		var err error
		if msn, part, err = blockingParams(c); err != nil {
			h.badRequest(c)
			return
		}
		if msn >= 0 && m.config.PartDuration <= 0 {
			// Without Low-Latency HLS the playlist does not offer
			// blocking reloads; the parameters are ignored.
			msn, part = -1, -1
		}
		// A request more than two segments ahead is refused, as the
		// specification has it.
		if msn > int64(st.next)+1 && !st.ended {
			h.badRequest(c)
			return
		}
	}
	answer := func(timedOut bool) {
		var body []byte
		var ok bool
		if index && m.status().hasAV {
			body, ok = m.MultivariantPlaylist(nil, "media.m3u8")
		} else {
			body, ok = m.MediaPlaylist(nil)
		}
		if !ok {
			if timedOut {
				h.respond(c, stdhttp.StatusServiceUnavailable, "", cachePlaylist, nil)
			} else {
				h.notFound(c)
			}
			return
		}
		h.respond(c, stdhttp.StatusOK, PlaylistContentType, cachePlaylist, body)
	}
	h.block(c, m, msn, part, answer)
}

// blockingParams reads _HLS_msn and _HLS_part, -1 when absent.
func blockingParams(c *http.Context) (msn, part int64, err error) {
	msn, part = -1, -1
	if s := c.Query("_HLS_msn"); s != "" {
		if msn, err = strconv.ParseInt(s, 10, 64); err != nil || msn < 0 {
			return 0, 0, errors.New("hls: bad _HLS_msn")
		}
	}
	if s := c.Query("_HLS_part"); s != "" {
		if part, err = strconv.ParseInt(s, 10, 64); err != nil || part < 0 || msn < 0 {
			return 0, 0, errors.New("hls: bad _HLS_part")
		}
	}
	return msn, part, nil
}

// serveFile answers a request for an initialization section, segment or
// part, waiting for the next part or segment when that is what it names.
func (h *Handler) serveFile(c *http.Context, m *Muxer, name string) {
	answer := func(bool) {
		data, ok := m.File(name)
		if !ok {
			h.notFound(c)
			return
		}
		h.respond(c, stdhttp.StatusOK, m.config.Format.ContentType, cacheMedia, data)
	}
	if _, ok := m.File(name); ok {
		answer(false)
		return
	}
	msn, part, ok := m.fileWait(name)
	if !ok {
		h.notFound(c)
		return
	}
	h.block(c, m, msn, part, answer)
}

// hold is a request waiting on a Muxer.
type hold struct {
	mu    sync.Mutex
	done  bool
	timer *time.Timer
	w     *waiter
}

// block answers c once msn and part are ready on m (msn < 0: once a segment
// is), or WaitTimeout has passed, without holding the goroutine it runs on:
// the request is retained and answered from the goroutine that wakes it. A
// request whose connection goes first is released unanswered.
func (h *Handler) block(c *http.Context, m *Muxer, msn, part int64, answer func(timedOut bool)) {
	st := &hold{}
	finish := func(answering, timedOut bool) {
		st.mu.Lock()
		if st.done {
			st.mu.Unlock()
			return
		}
		st.done = true
		timer, w := st.timer, st.w
		st.mu.Unlock()
		timer.Stop()
		m.cancel(w)
		if answering {
			answer(timedOut)
		}
		c.Release()
	}
	c.Retain()
	st.mu.Lock()
	w, err := m.wait(msn, part, func() { finish(true, false) })
	if err != nil || w == nil {
		st.done = true
		st.mu.Unlock()
		if err != nil {
			h.respond(c, stdhttp.StatusServiceUnavailable, "", "", nil)
		} else {
			answer(false)
		}
		c.Release()
		return
	}
	st.w = w
	st.timer = time.AfterFunc(h.config.WaitTimeout, func() { finish(true, true) })
	st.mu.Unlock()
	c.OnCancel(func(error) { finish(false, false) })
}
