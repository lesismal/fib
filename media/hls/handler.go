//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package hls

import (
	"errors"
	stdhttp "net/http"
	"net/url"
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
	DefaultIdleTimeout    = 30 * time.Second
	DefaultWaitTimeout    = 10 * time.Second
	DefaultRepublishWait  = 10 * time.Second
	DefaultMaxPlayWaiters = 1024
)

// Media types of the playlists (RFC 8216 section 4).
const PlaylistContentType = "application/vnd.apple.mpegurl"

// HandlerConfig configures a Handler.
type HandlerConfig struct {
	// Hub is where the streams come from.
	Hub *hub.Hub
	// Muxer configures the Muxer of every stream. Its Format (FMP4 when
	// zero, or MPEGTS) is the segment format of requests that do not ask
	// for another.
	Muxer Config
	// Formats are further segment formats a player may ask for with the
	// format query parameter of a playlist URL naming one (Format.Name):
	// with Formats: []Format{MPEGTS}, index.m3u8?format=ts serves MPEG-TS
	// segments while index.m3u8 serves Muxer.Format's. Each format a
	// stream is played in runs a Muxer of its own, which counts against
	// MaxStreams.
	Formats []Format
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
	// PlayWait lets a playlist request wait this long for a stream that
	// has no publisher yet (hub.SubscribeOptions.StartWait): its Muxer
	// starts at once and the request is answered with the first segment,
	// up to PlayWait plus WaitTimeout, or 404 when no publisher came in
	// time. Zero answers such a request 404 at once.
	PlayWait time.Duration
	// MaxPlayWaiters bounds the requests waiting for a publisher at once,
	// over every stream; one past it is answered 503. Zero means
	// DefaultMaxPlayWaiters.
	MaxPlayWaiters int
	// AllowOrigin, when set, is the Access-Control-Allow-Origin of every
	// response, as "*", so that players on other origins may fetch.
	AllowOrigin string
	// MaxStreams bounds the Muxers running at once; 0 is unlimited.
	MaxStreams int
	// Encrypt, when set, returns the Encryption of the stream at path, nil
	// for none, in place of Muxer.Encryption: asked as a Muxer of it starts,
	// in each format, with the Handler's lock held (it must not call the
	// Handler). An error starts no Muxer and answers the playlist request
	// 503, so that a stream meant to be encrypted is never served clear.
	// The Handler serves no key: the URIs Encryption.Key names are the
	// caller's to answer, before the Handler, as with SessionMuxer.
	Encrypt func(path string) (*Encryption, error)
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
// A stream the hub has no publisher for is 404, unless PlayWait lets the
// playlist request wait for one (MaxPlayWaiters of them at once). A
// playlist request waits, without
// holding a worker (http.Context.Retain), for the stream's first segment,
// and with Low-Latency HLS for the segment or part _HLS_msn and _HLS_part
// name; a request for the part a preload hint named waits for it too.
// Playlists are sent with Cache-Control: no-cache, everything else with a
// max-age of an hour, since the Muxer's file names are never reused.
type Handler struct {
	config  HandlerConfig
	formats []Format // Muxer.Format, then Formats
	mu      sync.Mutex
	streams map[streamKey]*entry
	closed  bool
	// playWaiters counts the requests waiting for a publisher.
	playWaiters atomic.Int64
}

// streamKey names the Muxer of a stream in one format, an index into
// Handler.formats.
type streamKey struct {
	path   string
	format int
}

type entry struct {
	key   streamKey
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
	if config.MaxPlayWaiters <= 0 {
		config.MaxPlayWaiters = DefaultMaxPlayWaiters
	}
	h := &Handler{config: config, streams: map[streamKey]*entry{}}
	first := config.Muxer
	first.defaults()
	h.formats = append([]Format{first.Format}, config.Formats...)
	return h
}

// Close stops every Muxer and closes its subscription.
func (h *Handler) Close() {
	h.mu.Lock()
	streams := h.streams
	h.streams, h.closed = map[streamKey]*entry{}, true
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

// Muxer returns the Muxer serving the stream at path in
// HandlerConfig.Muxer's format, or nil; FormatMuxer that of another
// format.
func (h *Handler) Muxer(path string) *Muxer {
	return h.FormatMuxer(path, "")
}

// FormatMuxer returns the Muxer serving the stream at path in the format
// named format (Format.Name, "" for HandlerConfig.Muxer's), or nil.
func (h *Handler) FormatMuxer(path, format string) *Muxer {
	i, ok := h.format(format)
	if !ok {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.streams[streamKey{path, i}]; e != nil {
		return e.m
	}
	return nil
}

// format returns the index of the format named name, 0 for "".
func (h *Handler) format(name string) (int, bool) {
	if name == "" {
		return 0, true
	}
	for i, f := range h.formats {
		if f.Name == name {
			return i, true
		}
	}
	return 0, false
}

// errNoStream is the hub not having the stream.
var errNoStream = errors.New("hls: no such stream")

// errEncrypt is HandlerConfig.Encrypt failing.
var errEncrypt = errors.New("hls: no encryption for the stream")

// SessionMuxer returns the Muxer serving the stream at path, in whichever
// format, whose session (Muxer.Session) is session, or nil: the Muxer whose
// playlist named a key URI holding it, for the key server to check the key
// period with KeyEpoch.
func (h *Handler) SessionMuxer(path, session string) *Muxer {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.formats {
		if e := h.streams[streamKey{path, i}]; e != nil && e.m.session == session {
			return e.m
		}
	}
	return nil
}

// muxer returns the entry of path in format, starting one.
func (h *Handler) muxer(path string, format int, c *http.Context) (*entry, error) {
	key := streamKey{path, format}
	h.mu.Lock()
	defer h.mu.Unlock()
	e := h.streams[key]
	if e != nil && e.m.Ended() && (h.publishing(path) || h.config.PlayWait > 0 && e.m.unstarted()) {
		// A playlist that ended while its publisher came back, or a wait
		// for a publisher that ran out: start over.
		delete(h.streams, key)
		e.stop()
		e = nil
	}
	if e != nil {
		e.last.Store(time.Now().UnixNano())
		return e, nil
	}
	if h.closed || h.config.PlayWait <= 0 && !h.publishing(path) {
		return nil, errNoStream
	}
	if h.config.MaxStreams > 0 && len(h.streams) >= h.config.MaxStreams {
		return nil, hub.ErrLimit
	}
	if h.playWaiters.Load() >= int64(h.config.MaxPlayWaiters) && !h.publishing(path) {
		// No Muxer for a request that could not wait for its publisher.
		return nil, hub.ErrLimit
	}
	mc := h.config.Muxer
	mc.Format = h.formats[format]
	if h.config.Encrypt != nil {
		enc, err := h.config.Encrypt(path)
		if err != nil {
			return nil, errEncrypt
		}
		mc.Encryption = enc
	}
	m := NewMuxer(mc)
	info := hub.SubscribeInfo{Protocol: "hls"}
	if c.Conn != nil {
		info.Remote = c.Conn.RemoteAddrPort()
	}
	if c.Request.URL != nil {
		info.Query = c.Request.URL.RawQuery
	}
	sub, err := h.config.Hub.Subscribe(path, m, hub.SubscribeOptions{Info: info, Wait: h.config.RepublishWait, StartWait: h.config.PlayWait})
	if err != nil {
		return nil, err
	}
	e = &entry{key: key, m: m, sub: sub}
	e.last.Store(time.Now().UnixNano())
	e.timer = time.AfterFunc(h.config.IdleTimeout, func() { h.idle(e) })
	h.streams[key] = e
	return e, nil
}

// file returns the entry of path whose Muxer named the file name, in
// whichever format: file names are the Muxer's own.
func (h *Handler) file(path, name string) (*entry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.formats {
		if e := h.streams[streamKey{path, i}]; e != nil {
			if kind, _, _ := e.m.parseName(name); kind != fileNone {
				e.last.Store(time.Now().UnixNano())
				return e, nil
			}
		}
	}
	return nil, errNoStream
}

// publishing reports whether the hub has a publisher on path.
func (h *Handler) publishing(path string) bool {
	s, ok := h.config.Hub.Stream(path)
	return ok && s.Publishing
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
	if h.streams[e.key] == e {
		delete(h.streams, e.key)
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
	var e *entry
	var err error
	format := c.Query("format")
	if playlist {
		fi, ok := h.format(format)
		if !ok {
			h.notFound(c)
			return
		}
		e, err = h.muxer(stream, fi, c)
	} else {
		e, err = h.file(stream, name)
	}
	switch {
	case err == hub.ErrLimit, err == errEncrypt:
		h.respond(c, stdhttp.StatusServiceUnavailable, "", "", nil)
		return
	case err != nil:
		// Not found, or refused by the hub's OnSubscribe.
		h.notFound(c)
		return
	}
	if playlist {
		media := "media.m3u8"
		if format != "" {
			// The format the player asked for goes on to the media
			// playlist; segments and parts are found by name.
			media += "?format=" + url.QueryEscape(format)
		}
		h.servePlaylist(c, e.m, name == "index.m3u8", media)
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
func (h *Handler) servePlaylist(c *http.Context, m *Muxer, index bool, media string) {
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
			body, ok = m.MultivariantPlaylist(nil, media)
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
	timeout, done := h.config.WaitTimeout, func() {}
	if st.unstarted {
		// The stream has no publisher yet: the request waits for one too,
		// within MaxPlayWaiters.
		if h.playWaiters.Add(1) > int64(h.config.MaxPlayWaiters) {
			h.playWaiters.Add(-1)
			h.respond(c, stdhttp.StatusServiceUnavailable, "", "", nil)
			return
		}
		timeout += h.config.PlayWait
		done = func() { h.playWaiters.Add(-1) }
	}
	h.block(c, m, msn, part, timeout, done, answer)
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
	h.block(c, m, msn, part, h.config.WaitTimeout, func() {}, answer)
}

// hold is a request waiting on a Muxer.
type hold struct {
	mu    sync.Mutex
	done  bool
	timer *time.Timer
	w     *waiter
}

// block answers c once msn and part are ready on m (msn < 0: once a segment
// is), or timeout has passed, without holding the goroutine it runs on:
// the request is retained and answered from the goroutine that wakes it. A
// request whose connection goes first is released unanswered. done runs
// once the request is let go, however.
func (h *Handler) block(c *http.Context, m *Muxer, msn, part int64, timeout time.Duration, done func(), answer func(timedOut bool)) {
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
		done()
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
		done()
		return
	}
	st.w = w
	st.timer = time.AfterFunc(timeout, func() { finish(true, true) })
	st.mu.Unlock()
	c.OnCancel(func(error) { finish(false, false) })
}
