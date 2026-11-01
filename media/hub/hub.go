package hub

import (
	"errors"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/media/av"
)

var (
	ErrStreamBusy = errors.New("hub: stream already has a publisher")
	ErrNotFound   = errors.New("hub: stream not found")
	ErrClosed     = errors.New("hub: closed")
	ErrLimit      = errors.New("hub: limit reached")
	// ErrUnpublished is what a subscription's OnClose hears when the
	// publisher leaves and the subscription does not wait for another.
	ErrUnpublished = errors.New("hub: stream unpublished")
)

// DefaultGOPCacheBytes is the GOP cache bound of a Config that leaves
// GOPCacheBytes 0.
const DefaultGOPCacheBytes = 16 << 20

// audioWindow is how much of an audio-only stream the GOP cache keeps.
const audioWindow = time.Second

// packetOverhead is what the GOP cache counts for a packet on top of its
// data, so that a stream of tiny packets is bounded too.
const packetOverhead = 64

// Config configures a Hub.
type Config struct {
	// GOPCacheBytes bounds the GOP cache of each stream: 0 means
	// DefaultGOPCacheBytes, a negative value no cache.
	GOPCacheBytes int
	// MaxStreams bounds the number of paths with a publisher or waiting
	// subscribers; 0 is unlimited.
	MaxStreams int
	// MaxSubscribersPerStream bounds the subscriptions of a path; 0 is
	// unlimited.
	MaxSubscribersPerStream int
	// OnPublish and OnUnpublish observe the registry (either may be nil);
	// OnPublish returning an error refuses the publish, with that error:
	// the authentication hook of every protocol. They are called without
	// any of the hub's locks held.
	OnPublish   func(path string, info PublishInfo) error
	OnUnpublish func(path string)
	// OnSubscribe may refuse a play by returning an error.
	OnSubscribe func(path string, info SubscribeInfo) error
}

// PublishInfo describes a publisher, for hooks and statistics.
type PublishInfo struct {
	// Protocol names the ingest, as "rtmp" or "srt".
	Protocol string
	Remote   netip.AddrPort
	// Query is the query string of the publish URL, where tokens travel.
	Query string
}

// SubscribeInfo describes a subscriber, for hooks.
type SubscribeInfo struct {
	Protocol string
	Remote   netip.AddrPort
	Query    string
}

// Sink receives a stream. Calls are serialized per subscription. They come
// from the goroutine that publishes (a fib worker, through
// Publisher.WritePacket), subscribes or closes, under the stream's mutex,
// and must not block: a sink queues onto its connection (Send) and drops
// when the connection's write queue is over a limit, which NewDropper
// does. A sink must not call into the hub from these calls, with one
// exception: Subscription.Close, which marks the subscription closed and
// leaves unlinking it to whoever holds the stream.
type Sink interface {
	// OnTracks gives the stream's tracks: first when the subscription
	// starts, again whenever the publisher changes them (SetTracks) or a
	// new publisher arrives. Packets after it index these tracks.
	OnTracks(tracks []*av.Track)
	// OnPacket gives one packet; the packet and its data are shared by
	// every subscriber and must not be modified.
	OnPacket(pkt *av.Packet)
	// OnClose ends the subscription: ErrUnpublished when the publisher
	// left, ErrNotFound when no publisher came within Wait, ErrClosed when
	// the hub closed. It is not called for Subscription.Close.
	OnClose(err error)
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	Info SubscribeInfo
	// Wait, when > 0, lets the subscription exist before the publisher
	// arrives and survive a republish: after the publisher leaves, the
	// subscription waits up to Wait for the next one. Otherwise Subscribe
	// fails with ErrNotFound when the stream has no publisher, and the
	// subscription ends when the publisher leaves.
	Wait time.Duration
	// NoGOPCache starts the subscription from the next keyframe instead
	// of the cached GOP, trading start-up delay for latency.
	NoGOPCache bool
}

// StreamInfo is a snapshot of one stream.
type StreamInfo struct {
	Path       string
	Tracks     []*av.Track
	Publishing bool
	// Reserved says the path is claimed by a publisher whose tracks are
	// not known yet (Hub.Reserve); Publishing is false meanwhile.
	Reserved  bool
	Publisher PublishInfo
	// Since is when the current publisher started.
	Since       time.Time
	Subscribers int
	// BytesIn and PacketsIn count the current publisher's packets.
	BytesIn   uint64
	PacketsIn uint64
}

// Hub is a registry of streams by path, each with at most one publisher
// and any number of subscribers.
type Hub struct {
	cfg      Config
	gopLimit int

	mu      sync.Mutex
	streams map[string]*stream
	closed  bool
}

// New returns a Hub.
func New(config Config) *Hub {
	h := &Hub{cfg: config, gopLimit: config.GOPCacheBytes, streams: make(map[string]*stream)}
	if h.gopLimit == 0 {
		h.gopLimit = DefaultGOPCacheBytes
	}
	return h
}

// stream is one path. Its mutex serializes the publisher's packets, the
// fan-out to every sink and the changes to its subscriptions. The hub's
// mutex is taken before a stream's, never after but by TryLock.
type stream struct {
	hub  *Hub
	path string

	mu sync.Mutex
	// sweep asks whoever holds mu to unlink closed subscriptions; it is
	// set without the mutex by Subscription.Close.
	sweep atomic.Bool
	// removed is set once the stream is out of the registry; reaping
	// while a reap is pending.
	removed, reaping bool

	pub      *Publisher
	pubInfo  PublishInfo
	since    time.Time
	tracks   []*av.Track
	video    []bool
	hasVideo bool
	subs     []*Subscription

	// gop is the cache from gop[gopHead] on; gopValid says it starts at a
	// keyframe, for a stream with video.
	gop      []*av.Packet
	gopHead  int
	gopBytes int
	gopValid bool

	bytesIn, packetsIn uint64
}

// unlock unlinks closed subscriptions, releases the mutex and, when the
// stream has neither publisher nor subscriber left, takes it out of the
// registry. A Subscription.Close that ran while the mutex was held, and
// could not take it, is picked up here.
func (st *stream) unlock() {
	for {
		if st.sweep.Swap(false) {
			st.compact()
		}
		empty := st.pub == nil && len(st.subs) == 0 && !st.removed && !st.reaping
		if empty {
			st.reaping = true
		}
		st.mu.Unlock()
		if empty {
			st.hub.reap(st)
		}
		// A Close between the Swap above and Unlock left its sweep to
		// the next holder: be it, unless someone else holds the mutex.
		if !st.sweep.Load() || !st.mu.TryLock() {
			return
		}
	}
}

func (st *stream) compact() {
	live := st.subs[:0]
	for _, s := range st.subs {
		if !s.closed.Load() {
			live = append(live, s)
		}
	}
	clear(st.subs[len(live):])
	st.subs = live
}

// reap removes an empty stream from the registry. It may be called with
// other streams' mutexes held (a sink relaying into another path), so it
// takes the hub's mutex only if it is free and leaves the work to a
// goroutine otherwise.
func (h *Hub) reap(st *stream) {
	if h.mu.TryLock() {
		h.reapLocked(st)
		h.mu.Unlock()
		return
	}
	go func() {
		h.mu.Lock()
		h.reapLocked(st)
		h.mu.Unlock()
	}()
}

func (h *Hub) reapLocked(st *stream) {
	st.mu.Lock()
	st.reaping = false
	if st.sweep.Swap(false) {
		st.compact()
	}
	if !st.removed && st.pub == nil && len(st.subs) == 0 {
		if h.streams[st.path] == st {
			delete(h.streams, st.path)
		}
		st.removed = true
	}
	st.mu.Unlock()
}

// lockStream returns the stream of path with its mutex held, creating it
// when create is set, and releases the hub's mutex, which the caller
// holds.
func (h *Hub) lockStream(path string, create bool) (*stream, error) {
	if h.closed {
		h.mu.Unlock()
		return nil, ErrClosed
	}
	st := h.streams[path]
	if st == nil {
		if !create {
			h.mu.Unlock()
			return nil, ErrNotFound
		}
		if h.cfg.MaxStreams > 0 && len(h.streams) >= h.cfg.MaxStreams {
			h.mu.Unlock()
			return nil, ErrLimit
		}
		st = &stream{hub: h, path: path}
		h.streams[path] = st
	}
	st.mu.Lock()
	h.mu.Unlock()
	return st, nil
}

// setTracks installs a new set of tracks and drops the cache, whose
// packets belong to the old ones.
func (st *stream) setTracks(tracks []*av.Track) {
	st.tracks = tracks
	st.video = st.video[:0]
	st.hasVideo = false
	for _, t := range tracks {
		v := t != nil && t.Codec.IsVideo()
		st.video = append(st.video, v)
		st.hasVideo = st.hasVideo || v
	}
	st.clearGOP()
}

func (st *stream) clearGOP() {
	clear(st.gop)
	st.gop = st.gop[:0]
	st.gopHead = 0
	st.gopBytes = 0
	st.gopValid = false
}

// cache adds pkt to the GOP cache: a stream with video keeps everything
// from the last video keyframe on, an audio-only stream the last second,
// and a cache over its bound is dropped (video) until the next keyframe,
// or trimmed (audio).
func (st *stream) cache(pkt *av.Packet, video bool) {
	limit := st.hub.gopLimit
	if limit < 0 {
		return
	}
	if st.hasVideo {
		if video && pkt.Keyframe {
			st.clearGOP()
			st.gopValid = true
		}
		if !st.gopValid {
			return
		}
	}
	st.gop = append(st.gop, pkt)
	st.gopBytes += len(pkt.Data) + packetOverhead
	if st.hasVideo {
		if st.gopBytes > limit {
			st.clearGOP()
		}
		return
	}
	for st.gopHead < len(st.gop)-1 &&
		(pkt.DTS-st.gop[st.gopHead].DTS > audioWindow || st.gopBytes > limit) {
		st.gopBytes -= len(st.gop[st.gopHead].Data) + packetOverhead
		st.gop[st.gopHead] = nil
		st.gopHead++
	}
	if st.gopHead > 64 && st.gopHead*2 > len(st.gop) {
		n := copy(st.gop, st.gop[st.gopHead:])
		clear(st.gop[n:])
		st.gop = st.gop[:n]
		st.gopHead = 0
	}
}

// Publish makes the caller the publisher of path with the given tracks.
// It fails with ErrStreamBusy when the path has a publisher, reserved or
// not, with the error OnPublish returns, or with ErrLimit or ErrClosed.
// Subscriptions waiting on the path start.
func (h *Hub) Publish(path string, tracks []*av.Track, info PublishInfo) (*Publisher, error) {
	p, st, err := h.claim(path, info)
	if err != nil {
		return nil, err
	}
	st.goLive(tracks)
	st.unlock()
	return p, nil
}

// Reserve claims path for a publisher whose tracks are not known yet: an
// ingest that learns them only by demuxing what it receives (SRT, an RTMP
// publisher before its metadata) reserves the path while it can still
// refuse the peer, so a second publisher is turned away in the handshake
// rather than accepted and then shut down. It fails as Publish does, and
// runs OnPublish now, not when the stream goes live.
//
// The returned Publisher's first SetTracks makes the stream live, as
// Publish would: until then the stream is not Publishing (StreamInfo
// says Reserved), WritePacket drops what it is given, Subscribe fails
// with ErrNotFound unless SubscribeOptions.Wait lets the subscription wait
// (and Wait's timer keeps running meanwhile), and Publish of the same path
// fails with ErrStreamBusy. Close releases the reservation; OnUnpublish is
// called for it as for any publisher, since OnPublish was.
func (h *Hub) Reserve(path string, info PublishInfo) (*Publisher, error) {
	p, st, err := h.claim(path, info)
	if err != nil {
		return nil, err
	}
	p.reserved = true
	st.unlock()
	return p, nil
}

// claim runs OnPublish and makes a new Publisher the one of path, with
// the stream's mutex held on success.
func (h *Hub) claim(path string, info PublishInfo) (*Publisher, *stream, error) {
	if h.cfg.OnPublish != nil {
		if err := h.cfg.OnPublish(path, info); err != nil {
			return nil, nil, err
		}
	}
	h.mu.Lock()
	st, err := h.lockStream(path, true)
	if err != nil {
		return nil, nil, err
	}
	if st.pub != nil {
		st.unlock()
		return nil, nil, ErrStreamBusy
	}
	p := &Publisher{st: st}
	st.pub = p
	st.pubInfo = info
	st.since = time.Now()
	st.bytesIn, st.packetsIn = 0, 0
	return p, st, nil
}

// live reports whether the stream has a publisher with tracks, under mu.
func (st *stream) live() bool {
	return st.pub != nil && !st.pub.reserved
}

// goLive installs the publisher's first tracks and starts the
// subscriptions waiting for it, under mu.
func (st *stream) goLive(tracks []*av.Track) {
	st.pub.reserved = false
	st.setTracks(tracks)
	for _, s := range st.subs {
		if s.closed.Load() {
			continue
		}
		s.stopTimer()
		s.started = true
		s.waitKey = st.hasVideo
		s.sink.OnTracks(tracks)
	}
}

// Subscribe adds sink to the subscribers of path. When the stream has a
// publisher, sink hears OnTracks and the cached GOP before Subscribe
// returns, under the stream's mutex, so nothing the publisher writes
// meanwhile comes out of order. Without a publisher, Subscribe fails with
// ErrNotFound unless opts.Wait lets the subscription wait for one.
func (h *Hub) Subscribe(path string, sink Sink, opts SubscribeOptions) (*Subscription, error) {
	if h.cfg.OnSubscribe != nil {
		if err := h.cfg.OnSubscribe(path, opts.Info); err != nil {
			return nil, err
		}
	}
	h.mu.Lock()
	st, err := h.lockStream(path, opts.Wait > 0)
	if err != nil {
		return nil, err
	}
	st.compact()
	if !st.live() && opts.Wait <= 0 {
		st.unlock()
		return nil, ErrNotFound
	}
	if limit := h.cfg.MaxSubscribersPerStream; limit > 0 && len(st.subs) >= limit {
		st.unlock()
		return nil, ErrLimit
	}
	s := &Subscription{st: st, sink: sink, wait: opts.Wait}
	st.subs = append(st.subs, s)
	if !st.live() {
		s.armTimer()
		st.unlock()
		return s, nil
	}
	s.started = true
	sink.OnTracks(st.tracks)
	s.waitKey = st.hasVideo
	if !opts.NoGOPCache && (st.gopValid || !st.hasVideo) {
		for _, pkt := range st.gop[st.gopHead:] {
			sink.OnPacket(pkt)
		}
		s.waitKey = false
	}
	st.unlock()
	return s, nil
}

// Streams returns a snapshot of every stream, sorted by path.
func (h *Hub) Streams() []StreamInfo {
	h.mu.Lock()
	list := make([]*stream, 0, len(h.streams))
	for _, st := range h.streams {
		list = append(list, st)
	}
	h.mu.Unlock()
	infos := make([]StreamInfo, 0, len(list))
	for _, st := range list {
		st.mu.Lock()
		info := StreamInfo{
			Path:       st.path,
			Tracks:     st.tracks,
			Publishing: st.live(),
			Reserved:   st.pub != nil && st.pub.reserved,
			BytesIn:    st.bytesIn,
			PacketsIn:  st.packetsIn,
		}
		if st.pub != nil {
			info.Publisher = st.pubInfo
			info.Since = st.since
		}
		for _, s := range st.subs {
			if !s.closed.Load() {
				info.Subscribers++
			}
		}
		removed := st.removed
		st.unlock()
		if !removed {
			infos = append(infos, info)
		}
	}
	slices.SortFunc(infos, func(a, b StreamInfo) int { return strings.Compare(a.Path, b.Path) })
	return infos
}

// Close closes the hub: every subscription hears OnClose(ErrClosed),
// every publisher's packets are dropped from then on and OnUnpublish is
// called for its path, and Publish and Subscribe fail with ErrClosed.
func (h *Hub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	streams := h.streams
	h.streams = make(map[string]*stream)
	h.mu.Unlock()
	var unpublished []string
	for _, st := range streams {
		st.mu.Lock()
		st.removed = true
		if st.pub != nil {
			st.pub.closed = true
			st.pub = nil
			unpublished = append(unpublished, st.path)
		}
		for _, s := range st.subs {
			if s.closed.CompareAndSwap(false, true) {
				s.stopTimer()
				s.sink.OnClose(ErrClosed)
			}
		}
		clear(st.subs)
		st.subs = st.subs[:0]
		st.clearGOP()
		st.mu.Unlock()
	}
	if h.cfg.OnUnpublish != nil {
		slices.Sort(unpublished)
		for _, path := range unpublished {
			h.cfg.OnUnpublish(path)
		}
	}
}

// Publisher feeds one stream. Its methods are serialized by the caller,
// usually a protocol session's own mutex.
type Publisher struct {
	st *stream
	// Under st.mu: closed once unpublished; reserved until the first
	// SetTracks of a Publisher from Reserve.
	closed, reserved bool
}

// WritePacket hands pkt to the stream: it goes into the GOP cache and to
// every subscriber before WritePacket returns. The hub owns pkt and its
// data afterwards, which neither the caller nor any subscriber may modify.
// Packets of a closed publisher, or of a track index the stream does not
// have, are dropped.
func (p *Publisher) WritePacket(pkt *av.Packet) {
	st := p.st
	st.mu.Lock()
	if st.pub != p || pkt == nil || pkt.Track < 0 || pkt.Track >= len(st.tracks) {
		st.unlock()
		return
	}
	st.bytesIn += uint64(len(pkt.Data))
	st.packetsIn++
	video := st.video[pkt.Track]
	st.cache(pkt, video)
	for _, s := range st.subs {
		if s.closed.Load() {
			continue
		}
		if video && s.waitKey {
			if !pkt.Keyframe {
				continue
			}
			s.waitKey = false
		}
		s.sink.OnPacket(pkt)
	}
	st.unlock()
}

// SetTracks replaces the stream's tracks, as when the codec configuration
// changes mid-stream: the GOP cache is dropped and every subscriber hears
// OnTracks again, then video from the next keyframe. The first SetTracks
// of a reserved Publisher makes the stream live, as Publish does.
func (p *Publisher) SetTracks(tracks []*av.Track) {
	st := p.st
	st.mu.Lock()
	if st.pub != p {
		st.unlock()
		return
	}
	if p.reserved {
		st.goLive(tracks)
		st.unlock()
		return
	}
	st.setTracks(tracks)
	for _, s := range st.subs {
		if s.closed.Load() {
			continue
		}
		s.waitKey = st.hasVideo
		s.sink.OnTracks(tracks)
	}
	st.unlock()
}

// Close unpublishes the stream. Subscriptions that wait (SubscribeOptions
// Wait) stay for the next publisher, up to their Wait; the others hear
// OnClose(ErrUnpublished). OnUnpublish is called once Close has done so.
func (p *Publisher) Close() {
	st := p.st
	st.mu.Lock()
	if p.closed {
		st.unlock()
		return
	}
	p.closed = true
	wasReserved := p.reserved
	p.reserved = false
	st.pub = nil
	st.pubInfo = PublishInfo{}
	st.tracks = nil
	st.video = st.video[:0]
	st.hasVideo = false
	st.clearGOP()
	for _, s := range st.subs {
		if s.closed.Load() {
			continue
		}
		if s.wait > 0 {
			if !wasReserved {
				// A subscription waiting on a reservation keeps
				// the timer it has.
				s.armTimer()
			}
			continue
		}
		s.closed.Store(true)
		st.sweep.Store(true)
		s.sink.OnClose(ErrUnpublished)
	}
	st.unlock()
	if cb := st.hub.cfg.OnUnpublish; cb != nil {
		cb(st.path)
	}
}

// Subscription is one sink's subscription to a stream.
type Subscription struct {
	st     *stream
	sink   Sink
	wait   time.Duration
	closed atomic.Bool

	// Under st.mu: waitKey holds video back until the next keyframe;
	// started says a publisher has been seen; timer and timerGen expire
	// a wait for a publisher.
	waitKey  bool
	started  bool
	timer    *time.Timer
	timerGen uint64
}

func (s *Subscription) armTimer() {
	s.stopTimer()
	s.timerGen++
	gen := s.timerGen
	s.timer = time.AfterFunc(s.wait, func() { s.expire(gen) })
}

func (s *Subscription) stopTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

// expire ends a subscription whose wait for a publisher ran out.
func (s *Subscription) expire(gen uint64) {
	st := s.st
	st.mu.Lock()
	if gen != s.timerGen || st.live() || !s.closed.CompareAndSwap(false, true) {
		st.unlock()
		return
	}
	s.timer = nil
	st.sweep.Store(true)
	if s.started {
		s.sink.OnClose(ErrUnpublished)
	} else {
		s.sink.OnClose(ErrNotFound)
	}
	st.unlock()
}

// Close ends the subscription; its sink hears no OnClose. Close never
// blocks, so it may be called from anywhere, including the sink's own
// OnTracks or OnPacket: no call to the sink starts after Close returns,
// though one running on another goroutine at that moment may still be
// finishing. When the stream's mutex is busy, Close only marks the
// subscription, and whoever holds the mutex unlinks it before releasing
// it.
func (s *Subscription) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	st := s.st
	st.sweep.Store(true)
	if st.mu.TryLock() {
		s.stopTimer()
		st.unlock()
	}
}
