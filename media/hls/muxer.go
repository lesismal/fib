package hls

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/media/av"
)

// Defaults of Config's zero fields.
const (
	DefaultTargetDuration = 2 * time.Second
	DefaultPlaylistSize   = 6
	DefaultKeepSegments   = 3
	DefaultMaxBytes       = 64 << 20
	DefaultMaxWaiters     = 4096
)

// maxJump is the largest step in a track's DTS taken as the stream going
// on; a larger one, or one back by more than a second, starts a new
// timeline after a discontinuity.
const (
	maxJump = 10 * time.Second
	maxBack = time.Second
)

var (
	// ErrTooManyWaiters is the error for a blocking request beyond
	// Config.MaxWaiters.
	ErrTooManyWaiters = errors.New("hls: too many requests waiting")
)

// Config configures a Muxer.
type Config struct {
	// Format is the segment format; the zero Format is FMP4.
	Format Format
	// TargetDuration is the duration segments are cut at: a stream with
	// video at the first keyframe at least this far from the segment's
	// start, once the other tracks have reached it, an audio-only stream
	// at the first packet. Default 2 s.
	TargetDuration time.Duration
	// PlaylistSize is how many segments the media playlist lists.
	// Default 6.
	PlaylistSize int
	// KeepSegments is how many segments stay available after leaving the
	// playlist, for clients that fetched it just before. Default 3.
	KeepSegments int
	// MaxBytes bounds the media held, segments and the one being written.
	// Old segments go, even out of the playlist, to keep under it, and a
	// segment that grows past a quarter of it is cut without waiting for a
	// keyframe. Default 64 MiB.
	MaxBytes int64
	// PartDuration, when > 0, turns on Low-Latency HLS: segments are
	// written in parts of at most this long (PART-TARGET), listed with
	// EXT-X-PART and announced with EXT-X-PRELOAD-HINT, and playlist
	// requests may block (_HLS_msn, _HLS_part). 200 ms to 1 s is usual.
	PartDuration time.Duration
	// MaxWaiters bounds the requests blocked on one Muxer. Default 4096.
	MaxWaiters int
	// Encryption, when not nil, encrypts every segment and part with
	// AES-128 as it is cut, under the keys it supplies, and has the media
	// playlist name them (EXT-X-KEY). The Muxer copies it.
	Encryption *Encryption
}

func (c *Config) defaults() {
	if c.Format.New == nil {
		c.Format = FMP4
	}
	if c.TargetDuration <= 0 {
		c.TargetDuration = DefaultTargetDuration
	}
	if c.PlaylistSize <= 0 {
		c.PlaylistSize = DefaultPlaylistSize
	}
	if c.KeepSegments < 0 {
		c.KeepSegments = 0
	} else if c.KeepSegments == 0 {
		c.KeepSegments = DefaultKeepSegments
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = DefaultMaxBytes
	}
	if c.MaxWaiters <= 0 {
		c.MaxWaiters = DefaultMaxWaiters
	}
}

// segment is a media segment: whole once complete, else the one being
// written. Its data only grows, so a slice of it taken under the lock stays
// valid for as long as a response needs it. An encrypted segment is served
// once it is encrypted, in place, after it is complete and before it is
// listed; its parts are encrypted apart, into penc, which only grows too,
// and served as they are (pubParts).
type segment struct {
	msn      uint64
	dur      time.Duration
	data     []byte
	parts    []part
	pubParts int // the parts listed and served: all but those being encrypted
	init     int // the initialization section's id, 0 for none
	disc     bool
	complete bool      // cut: whole, though maybe not encrypted yet
	key      *epochKey // the key encrypting it once it is, nil for none
	penc     []byte    // the parts' ciphertext, nil once they are no longer served
}

// part is a partial segment: a range of its segment's data, and of its
// ciphertext in penc when encrypted.
type part struct {
	off, n      int
	eoff, en    int
	dur         time.Duration
	independent bool
}

// waiter is a request blocked until msn (and part) are available.
type waiter struct {
	msn, part int64
	fn        func()
}

// Muxer turns a stream into HLS: it is a hub.Sink that cuts the stream into
// segments (and parts, for Low-Latency HLS) held in memory, and renders the
// playlists that list them. A Muxer serves one stream for as long as its
// subscription lasts; OnTracks after the first (a republish or a codec
// change) starts a new timeline after a discontinuity, with a new
// initialization section when the tracks changed. The new timeline carries
// on where the last ended or, when the new tracks' first packet carries on
// the decode times of its kind (video or audio), as a publisher changing
// its tracks on the same clock sends, with the times as they ran.
//
// OnTracks, OnPacket and OnClose come from the hub, the reading methods
// from any goroutine: a mutex guards the Muxer, held only to copy slice
// headers or render a playlist, so the publisher is never held up for
// longer. Requests blocked on a segment or part to come are woken on a
// goroutine of their own, never the publisher's.
type Muxer struct {
	config  Config
	session string
	enc     *Encryption // a copy of config.Encryption

	mu       sync.Mutex
	tracks   []*av.Track
	writer   SegmentWriter
	carried  []bool
	video    int // the stream index of the video track segments are cut at, -1 for none
	codecs   string
	width    int
	height   int
	waitKey  bool
	discNext bool
	inits    map[int][]byte
	initID   int
	last     []time.Duration // per track, the DTS of its last packet
	step     []time.Duration // per track, from its packet before the last to the last
	seen     []bool

	// Where a new writer's timeline starts, which its first packet
	// decides (place): at placeEnd, the end of the writers before, or,
	// for new tracks (follow) whose first packet carries on the DTS of the
	// last packet written of its kind (kindLast, video at 1, which
	// kindSeen says there was), with the offset off of the writer before,
	// as the publisher's clock ran on.
	place    bool
	follow   bool
	placeEnd time.Duration
	off      time.Duration
	offKnown bool
	kindLast [2]time.Duration
	kindSeen [2]bool

	segs        []*segment // complete, oldest first
	open        *segment
	next        uint64 // the msn of the open segment, or of the next one
	segStart    time.Duration
	partStart   time.Duration
	prevDTS     time.Duration
	gap         time.Duration
	bytes       int64
	discRemoved int
	target      int // EXT-X-TARGETDURATION, which only grows
	peak        int64
	short       bool // peak is of a short segment, to be replaced
	ended       bool
	waiters     []*waiter

	// A cut at a keyframe waiting for the other tracks to reach it
	// (hold): the keyframe, the duration of the segment it ends, the
	// packets that came after it, and the tracks among them.
	cutKey  *av.Packet
	cutDur  time.Duration
	held    []*av.Packet
	reached []bool

	// The encryption (Encryption): the segments cut and not yet encrypted,
	// oldest first, between segs and open; what is left to encrypt, in
	// order; whether it is running, and is to be started on unlock; the
	// error that ended it. key and pencSize are the running encryption's.
	cut       []*segment
	jobs      []encJob
	running   bool
	kick      bool
	encErr    error
	key       *epochKey // the key of the last key period
	pencSize  int       // the bytes of the last encrypted segment's parts
	encryptFn func()    // m.encrypt, made once
}

var sessions atomic.Uint64

// NewMuxer returns a Muxer.
func NewMuxer(config Config) *Muxer {
	config.defaults()
	m := &Muxer{config: config, video: -1, inits: map[int][]byte{}}
	if e := config.Encryption; e != nil {
		c := *e
		m.enc = &c
		m.config.Encryption = m.enc
		m.encryptFn = m.encrypt
	}
	// Every Muxer names its files apart, so that a cache holding a segment
	// of a stream served before does not answer for this one's.
	m.session = strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 36) + strconv.FormatUint(sessions.Add(1), 36)
	m.target = int(math.Ceil(config.TargetDuration.Seconds()))
	return m
}

// OnTracks starts the stream's configuration: a new SegmentWriter, and a
// discontinuity when segments came before.
func (m *Muxer) OnTracks(tracks []*av.Track) {
	m.mu.Lock()
	defer m.unlock()
	if m.ended {
		return
	}
	m.unhold()
	m.endSegment(nil, 0)
	m.tracks = tracks
	m.start(true)
	m.wake()
}

// follower is a continuer that can also carry on another's timeline on
// the same clock, as fMP4's and MPEG-TS's can: after a discontinuity for
// new tracks whose times run on, each track's go on from its own last,
// though B-frame video's first decode time is behind the last audio's, so
// that a channel's items, or a codec change, do not jump.
type follower interface {
	continuer
	// Offset is what the writer adds to a packet's DTS, once it knows.
	Offset() (time.Duration, bool)
	// Follow makes it add off, as StartAt makes it start at a time.
	Follow(off time.Duration)
}

// continuer is a SegmentWriter whose timeline can carry on another's, as
// fMP4's and MPEG-TS's do: after a discontinuity the next writer starts
// where the last one ended, so that decode times never go back. Players
// need not keep them going (RFC 8216 section 4.3.2.3), but ffmpeg keeps
// its MP4 demuxer across a discontinuity with the same init section and
// discards samples whose decode times went back: a republish lost its
// first segment; and its correction of a transport stream's jump back
// repeated a decode time.
type continuer interface {
	End() time.Duration
	StartAt(time.Duration)
}

// start makes a writer for m.tracks, after a discontinuity when anything
// was written before. follow says the tracks are new, and the writer may
// carry on the last one's times as they ran (place).
func (m *Muxer) start(follow bool) {
	var end time.Duration
	if c, ok := m.writer.(continuer); ok {
		end = c.End()
	}
	if f, ok := m.writer.(follower); ok {
		if off, ok := f.Offset(); ok {
			m.off, m.offKnown = off, true
		}
	}
	if m.place {
		// The last writer placed nothing: the one before it ended the
		// timeline.
		end = max(end, m.placeEnd)
	}
	m.place, m.follow, m.placeEnd = true, follow && m.offKnown, end
	m.writer, m.carried, m.video, m.codecs = nil, nil, -1, ""
	m.discNext = m.next > 0
	m.last, m.step, m.seen = make([]time.Duration, len(m.tracks)), make([]time.Duration, len(m.tracks)), make([]bool, len(m.tracks))
	w, err := m.config.Format.New(m.tracks)
	if err != nil {
		return
	}
	m.writer = w
	m.carried = make([]bool, len(m.tracks))
	var codecs []string
	m.width, m.height = 0, 0
	for _, i := range w.Tracks() {
		if i < 0 || i >= len(m.tracks) {
			continue
		}
		m.carried[i] = true
		t := m.tracks[i]
		if t.Codec.IsVideo() && m.video < 0 {
			m.video = i
			m.width, m.height = videoSize(t)
		}
		if s := CodecString(t); s != "" {
			codecs = append(codecs, s)
		}
	}
	m.codecs = strings.Join(codecs, ",")
	m.waitKey = m.video >= 0
	if init := w.Init(); init != nil {
		if m.initID == 0 || string(m.inits[m.initID]) != string(init) {
			m.initID++
			m.inits[m.initID] = init
		}
	} else {
		m.initID = 0
	}
}

// OnPacket adds a packet to the segment being written, ending it, or a
// part, first when the packet is where one ends.
func (m *Muxer) OnPacket(pkt *av.Packet) {
	m.mu.Lock()
	defer m.unlock()
	if m.ended || m.writer == nil || pkt.Track < 0 || pkt.Track >= len(m.carried) || !m.carried[pkt.Track] {
		return
	}
	if m.cutKey != nil {
		m.hold(pkt)
		return
	}
	m.packet(pkt)
}

// packet adds pkt, of a track carried, to the segment being written.
func (m *Muxer) packet(pkt *av.Packet) {
	isVideo := pkt.Track == m.video
	if m.open != nil && m.seen[pkt.Track] {
		if d := pkt.DTS - m.last[pkt.Track]; d > maxJump || d < -maxBack {
			// The publisher's clock jumped: what was written ends, and a
			// new timeline begins after a discontinuity.
			m.endSegment(nil, 0)
			m.start(false)
		}
	}
	if m.waitKey {
		if !isVideo || !pkt.Keyframe {
			return
		}
		m.waitKey = false
	}
	if m.place {
		kind := 0
		if m.tracks[pkt.Track].Codec.IsVideo() {
			kind = 1
		}
		m.placeWriter(pkt, kind)
	}
	if m.open == nil {
		m.beginSegment(pkt)
	} else {
		d := pkt.DTS - m.segStart
		cut := d >= m.config.TargetDuration
		if m.video >= 0 {
			cut = cut && isVideo && pkt.Keyframe
			if cut && m.behind(pkt.DTS) {
				// The other tracks' packets decoding before the keyframe
				// are still to come: the cut waits for them (hold).
				m.cutKey, m.cutDur = pkt, d
				m.reached = slices.Grow(m.reached[:0], len(m.tracks))[:len(m.tracks)]
				clear(m.reached)
				return
			}
		}
		if cut || int64(len(m.open.data)+m.writer.Buffered()) > m.config.MaxBytes/4 {
			m.endSegment(pkt, d)
			if m.ended {
				// The encryption fell behind.
				return
			}
			m.beginSegment(pkt)
		} else if p := m.config.PartDuration; p > 0 {
			// A part ends where the next packet would take it past
			// PART-TARGET, at the cadence the packets have come at.
			if pd := pkt.DTS - m.partStart; pd > 0 && (pd >= p || pd+m.gap > p) {
				m.endPart(pkt)
			}
		}
	}
	m.write(pkt)
}

// write writes pkt to the segment being written.
func (m *Muxer) write(pkt *av.Packet) {
	if g := pkt.DTS - m.prevDTS; g > 0 && g < m.config.TargetDuration {
		m.gap = g
	}
	m.prevDTS = pkt.DTS
	if g := pkt.DTS - m.last[pkt.Track]; m.seen[pkt.Track] && g > 0 && g < m.config.TargetDuration {
		m.step[pkt.Track] = g
	}
	m.last[pkt.Track], m.seen[pkt.Track] = pkt.DTS, true
	kind := 0
	if m.tracks[pkt.Track].Codec.IsVideo() {
		kind = 1
	}
	if !m.kindSeen[kind] || pkt.DTS > m.kindLast[kind] {
		m.kindLast[kind], m.kindSeen[kind] = pkt.DTS, true
	}
	m.writer.WritePacket(pkt)
	m.account()
	m.wake()
}

// The bounds of a cut waiting for the other tracks (hold): it waits no
// longer than for packets cutWait past the keyframe, nor for more than
// maxHeld of them, nor for a track whose last packet was further back.
const (
	cutWait = 250 * time.Millisecond
	maxHeld = 256
)

// behind reports whether a track other than the video segments are cut at
// has its last packet before dts, by no more than cutWait: a packet of it
// decoding before dts, or the one after its last, which tells the last's
// duration (holder), is about to come.
func (m *Muxer) behind(dts time.Duration) bool {
	for i, seen := range m.seen {
		if seen && i != m.video && m.last[i] < dts && dts-m.last[i] <= cutWait {
			return true
		}
	}
	return false
}

// hold takes pkt while a segment's cut at the keyframe cutKey waits for
// the other tracks to reach it: a segment's tracks begin together at its
// keyframe, none of them earlier. hls.js, reading a whole fMP4 segment,
// takes it to start at its earliest decode time and to last as long as its
// video: a segment whose audio began before its keyframe ended that much
// early for it, and the first segment after a discontinuity, placed there,
// had its audio decode before the last audio before it. Chrome then starts
// its tracks over at a keyframe, and dropped the video up to the next one:
// the new item's video froze for a second at every change of tracks.
//
// A packet of another track decoding before the keyframe goes in the
// segment the keyframe ends; the others wait, in order, until every track
// near the keyframe has one, or the bounds of cutWait and maxHeld are
// reached, and then the segment is cut (release).
func (m *Muxer) hold(pkt *av.Packet) {
	k := m.cutKey.DTS
	if pkt.Track != m.video && pkt.DTS < k && m.seen[pkt.Track] {
		if d := pkt.DTS - m.last[pkt.Track]; d >= 0 && d <= maxJump {
			m.write(pkt)
			return
		}
	}
	m.held = append(m.held, pkt)
	m.reached[pkt.Track] = true
	if d := pkt.DTS - k; d >= 0 && d < cutWait && len(m.held) < maxHeld {
		for i, seen := range m.seen {
			if seen && i != m.video && !m.reached[i] && m.last[i] < k && k-m.last[i] <= cutWait {
				return
			}
		}
	}
	m.release()
}

// release cuts the segment at the keyframe held, and adds the packets held
// after it as they came.
func (m *Muxer) release() {
	key, held := m.cutKey, m.held
	m.cutKey, m.held = nil, nil
	if _, ok := m.writer.(holder); ok {
		// The first packet of each other track after the keyframe tells
		// the duration of the one before it, which ends the segment.
		done := m.reached
		clear(done)
		for i, p := range held {
			if p.Track == m.video || done[p.Track] || !m.seen[p.Track] {
				continue
			}
			done[p.Track] = true
			if d := p.DTS - m.last[p.Track]; d > 0 && d <= maxJump && m.last[p.Track] < key.DTS {
				m.write(p)
				held[i] = nil
			}
		}
	}
	m.endSegment(key, m.cutDur)
	if m.ended {
		// The encryption fell behind.
		return
	}
	m.beginSegment(key)
	m.write(key)
	for _, p := range held {
		switch {
		case p == nil:
		case m.ended || m.writer == nil:
			return
		case m.cutKey != nil:
			m.hold(p)
		default:
			m.packet(p)
		}
	}
	if m.held == nil {
		// No cut waits again: the next one holds in these.
		clear(held)
		m.held = held[:0]
	}
}

// unhold adds the keyframe a cut waits at, and the packets held after it,
// to the segment being written, uncut: the stream ends, or new tracks
// begin, before the cut.
func (m *Muxer) unhold() {
	if m.cutKey == nil {
		return
	}
	m.write(m.cutKey)
	for _, p := range m.held {
		m.write(p)
	}
	clear(m.held)
	m.cutKey, m.held = nil, m.held[:0]
}

// placeWriter starts the timeline of a new writer, whose first packet is
// pkt, of kind (video at 1): where the last one's ended, or with the
// times as they ran when the tracks are new and pkt is past the last
// packet of its kind, by up to maxJump.
func (m *Muxer) placeWriter(pkt *av.Packet, kind int) {
	m.place = false
	if f, ok := m.writer.(follower); ok && m.follow && m.kindSeen[kind] {
		if d := pkt.DTS - m.kindLast[kind]; d > 0 && d <= maxJump {
			f.Follow(m.off)
			return
		}
	}
	if c, ok := m.writer.(continuer); ok {
		c.StartAt(m.placeEnd)
	}
}

// OnClose ends the stream: the segment being written is finished and the
// playlist ends (EXT-X-ENDLIST).
func (m *Muxer) OnClose(error) { m.Close() }

// Close ends the stream as OnClose does, and wakes every blocked request.
func (m *Muxer) Close() {
	m.mu.Lock()
	defer m.unlock()
	if m.ended {
		return
	}
	m.unhold()
	m.endSegment(nil, 0)
	m.ended = true
	m.wake()
}

// unstarted reports whether the Muxer has had no stream: no OnTracks yet.
func (m *Muxer) unstarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tracks == nil
}

// Ended reports whether the stream has ended.
func (m *Muxer) Ended() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ended
}

func (m *Muxer) beginSegment(pkt *av.Packet) {
	m.open = &segment{msn: m.next, init: m.initID, disc: m.discNext}
	m.discNext = false
	m.segStart, m.partStart = pkt.DTS, pkt.DTS
	m.writer.BeginSegment()
}

// endPart flushes the writer into a part of the open segment, which next
// follows.
func (m *Muxer) endPart(next *av.Packet) {
	s := m.open
	before := len(s.data)
	var independent bool
	s.data, independent = m.writer.Flush(s.data, next)
	if n := len(s.data) - before; n > 0 && m.config.PartDuration > 0 {
		var dur time.Duration
		if next != nil {
			dur = next.DTS - m.partStart
		} else {
			dur = m.end() - m.partStart
		}
		s.parts = append(s.parts, part{off: before, n: n, dur: dur, independent: independent})
		if m.enc != nil {
			// Served once encrypted; its bytes are never written again,
			// until the segment's own encryption, queued after it.
			m.enqueue(encJob{s: s, i: len(s.parts) - 1, data: s.data[before:len(s.data):len(s.data)]})
		} else {
			s.pubParts = len(s.parts)
		}
	}
	if next != nil {
		m.partStart = next.DTS
	}
}

// end returns where the media written ends when no packet comes next to
// tell (the stream's end, new tracks, a jump in the clock): where the
// earliest of the tracks in the part being written ends, each at its last
// packet and the step before it, so that every track covers the segment.
// A player of muxed audio and video has buffered that far and no further,
// and one placing the segment after a discontinuity by the duration of the
// one before (hls.js, after reading it by its parts) found a hole after it
// where the audio ran on past the last video frame: playback stalled. A
// track that ended before the part began has stopped, and does not count;
// with none, it is the last packet and the stream's last step.
func (m *Muxer) end() time.Duration {
	var end time.Duration
	found := false
	for i, seen := range m.seen {
		if !seen {
			continue
		}
		step := m.step[i]
		if step <= 0 {
			step = m.gap
		}
		if e := m.last[i] + step; e > m.partStart && (!found || e < end) {
			end, found = e, true
		}
	}
	if !found {
		return m.prevDTS + m.gap
	}
	return end
}

// endSegment completes the open segment, d long, before next, or at the end
// of the stream when next is nil.
func (m *Muxer) endSegment(next *av.Packet, d time.Duration) {
	s := m.open
	if s == nil {
		return
	}
	m.endPart(next)
	m.open = nil
	if next == nil {
		d = m.end() - m.segStart
	}
	if len(s.data) == 0 {
		return
	}
	if d <= 0 {
		d = max(m.gap, time.Millisecond)
	}
	s.dur, s.complete = d, true
	m.next++
	if r := int(math.Round(d.Seconds())); r > m.target {
		m.target = r
	}
	// The peak bit rate is of segments of some length: a segment cut
	// short by a republish says little about the stream's rate.
	if d >= m.config.TargetDuration/2 || m.peak == 0 {
		if bw := int64(float64(len(s.data)*8) / d.Seconds()); bw > m.peak || m.short {
			m.peak, m.short = bw, d < m.config.TargetDuration/2
		}
	}
	if m.enc == nil {
		if cap(s.data)-len(s.data) > len(s.data)/8 {
			s.data = append([]byte(nil), s.data...)
		}
		m.publish(s)
		return
	}
	// Listed once encrypted, without the lock.
	m.cut = append(m.cut, s)
	m.enqueue(encJob{s: s, i: -1, data: s.data})
	// Whatever the scheduler does, the media waiting stays bounded: one
	// segment always may, being at most a quarter of MaxBytes.
	waiting := int64(0)
	for _, c := range m.cut {
		waiting += int64(len(c.data) + len(c.penc))
	}
	if len(m.cut) > 1 && waiting > m.config.MaxBytes/2 {
		m.fail(errBacklog)
	}
}

// publish lists s, complete (and encrypted), in the playlist. m.mu is held.
func (m *Muxer) publish(s *segment) {
	m.segs = append(m.segs, s)
	if m.enc != nil {
		m.pencSize = len(s.penc)
		if m.config.PartDuration > 0 {
			// The parts' ciphertext doubles what a segment takes: it goes
			// once the playlist no longer lists them, a segment later for a
			// player that read it just before.
			for _, old := range m.segs[:max(0, m.partsFrom(m.first())-1)] {
				old.penc = nil
			}
		}
	}
	m.account()
}

// account trims the segments to the window, KeepSegments and MaxBytes, and
// drops the initialization sections no segment needs any more.
func (m *Muxer) account() {
	keep := m.config.PlaylistSize + m.config.KeepSegments
	for len(m.segs) > keep || len(m.segs) > 1 && m.size() > m.config.MaxBytes {
		if m.segs[0].disc {
			m.discRemoved++
		}
		m.segs[0] = nil
		m.segs = m.segs[1:]
	}
	if len(m.inits) > 1 {
		oldest := m.initID
		switch {
		case len(m.segs) > 0:
			oldest = m.segs[0].init
		case len(m.cut) > 0:
			oldest = m.cut[0].init
		case m.open != nil:
			oldest = m.open.init
		}
		for id := range m.inits {
			if id != m.initID && id < oldest {
				delete(m.inits, id)
			}
		}
	}
}

// size is the bytes held.
func (m *Muxer) size() int64 {
	n := int64(0)
	for _, s := range m.segs {
		n += int64(cap(s.data) + cap(s.penc))
	}
	for _, s := range m.cut {
		n += int64(cap(s.data) + cap(s.penc))
	}
	if m.open != nil {
		n += int64(cap(m.open.data) + cap(m.open.penc))
	}
	if m.writer != nil {
		n += int64(m.writer.Buffered())
	}
	return n
}

// Bytes returns the bytes of media the Muxer holds.
func (m *Muxer) Bytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size()
}

// first is the index in segs of the first segment the playlist lists.
func (m *Muxer) first() int {
	return max(0, len(m.segs)-m.config.PlaylistSize)
}

// frontier returns the first segment not listed whole: the oldest of those
// cut and being encrypted, else the open one, or nil. Its pubParts are
// listed; nothing after it is.
func (m *Muxer) frontier() *segment {
	if len(m.cut) > 0 {
		return m.cut[0]
	}
	return m.open
}

// listedNext returns the msn after the last segment listed whole.
func (m *Muxer) listedNext() uint64 {
	if f := m.frontier(); f != nil {
		return f.msn
	}
	return m.next
}

// hinted returns the part to be listed next, which EXT-X-PRELOAD-HINT
// names.
func (m *Muxer) hinted() (msn uint64, part int) {
	f := m.frontier()
	switch {
	case f == nil:
		return m.next, 0
	case f.complete && f.pubParts == len(f.parts):
		// Every part listed, the segment itself being encrypted.
		return f.msn + 1, 0
	}
	return f.msn, f.pubParts
}

// settled reports whether everything cut is listed: nothing is being
// encrypted.
func (m *Muxer) settled() bool { return len(m.cut) == 0 }

// ready reports whether a request for msn and part can be answered: the
// segment is listed, or that part of it is; msn < 0 waits for the first
// segment. An ended stream answers everything, once its last segment is
// listed.
func (m *Muxer) ready(msn, part int64) bool {
	switch {
	case m.ended && m.settled():
		return true
	case msn < 0:
		return len(m.segs) > 0
	case uint64(msn) < m.listedNext():
		return true
	}
	f := m.frontier()
	if part < 0 || f == nil || uint64(msn) != f.msn {
		return false
	}
	return int64(f.pubParts) > part
}

// wait registers fn to run, on a goroutine of its own, once msn and part
// are ready, and returns the waiter to cancel; it returns nil without
// registering when they are ready now, and ErrTooManyWaiters beyond
// MaxWaiters.
func (m *Muxer) wait(msn, part int64, fn func()) (*waiter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ready(msn, part) {
		return nil, nil
	}
	if len(m.waiters) >= m.config.MaxWaiters {
		return nil, ErrTooManyWaiters
	}
	w := &waiter{msn: msn, part: part, fn: fn}
	m.waiters = append(m.waiters, w)
	return w, nil
}

// waiting reports whether requests are waiting.
func (m *Muxer) waiting() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.waiters) > 0
}

// cancel forgets w, reporting whether it was still waiting.
func (m *Muxer) cancel(w *waiter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, x := range m.waiters {
		if x == w {
			last := len(m.waiters) - 1
			m.waiters[i] = m.waiters[last]
			m.waiters[last] = nil
			m.waiters = m.waiters[:last]
			return true
		}
	}
	return false
}

// wake runs the waiters now ready, on a goroutine of their own: they write
// responses, which must not happen under the hub's lock or this one.
func (m *Muxer) wake() {
	var ready []func()
	for i := 0; i < len(m.waiters); {
		w := m.waiters[i]
		if !m.ready(w.msn, w.part) {
			i++
			continue
		}
		ready = append(ready, w.fn)
		last := len(m.waiters) - 1
		m.waiters[i] = m.waiters[last]
		m.waiters[last] = nil
		m.waiters = m.waiters[:last]
	}
	if len(ready) > 0 {
		go func() {
			for _, fn := range ready {
				fn()
			}
		}()
	}
}

// status is what a request needs to know about the stream to be answered.
type status struct {
	next      uint64 // the msn after the last complete segment
	parts     int64  // the parts of the open segment
	any       bool   // a segment is complete
	ended     bool
	unstarted bool // no stream yet: waiting for a publisher
	hasAV     bool
	codecs    string
}

func (m *Muxer) status() status {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := status{next: m.listedNext(), any: len(m.segs) > 0, ended: m.ended, unstarted: m.tracks == nil && !m.ended, codecs: m.codecs}
	if f := m.frontier(); f != nil {
		st.parts = int64(f.pubParts)
	}
	audio := false
	for i, ok := range m.carried {
		if ok && m.tracks[i].Codec.IsAudio() {
			audio = true
		}
	}
	st.hasAV = audio && m.video >= 0
	return st
}

// The file names of a Muxer: init-<session>-<id><InitExtension>,
// seg-<session>-<msn><Extension>, part-<session>-<msn>-<part><Extension>.
func (m *Muxer) initName(b []byte, id int) []byte {
	b = append(b, "init-"...)
	b = append(b, m.session...)
	b = append(b, '-')
	b = strconv.AppendInt(b, int64(id), 10)
	return append(b, m.config.Format.InitExtension...)
}

func (m *Muxer) segName(b []byte, msn uint64) []byte {
	b = append(b, "seg-"...)
	b = append(b, m.session...)
	b = append(b, '-')
	b = strconv.AppendUint(b, msn, 10)
	return append(b, m.config.Format.Extension...)
}

func (m *Muxer) partName(b []byte, msn uint64, i int) []byte {
	b = append(b, "part-"...)
	b = append(b, m.session...)
	b = append(b, '-')
	b = strconv.AppendUint(b, msn, 10)
	b = append(b, '-')
	b = strconv.AppendInt(b, int64(i), 10)
	return append(b, m.config.Format.Extension...)
}

// fileKind is what a file name names.
type fileKind int

const (
	fileNone fileKind = iota
	fileInit
	fileSegment
	filePart
)

// parseName parses one of the Muxer's file names.
func (m *Muxer) parseName(name string) (kind fileKind, a uint64, b int64) {
	var rest string
	var ok bool
	switch {
	case strings.HasPrefix(name, "init-"):
		kind, rest = fileInit, name[5:]
		rest, ok = strings.CutSuffix(rest, m.config.Format.InitExtension)
	case strings.HasPrefix(name, "seg-"):
		kind, rest = fileSegment, name[4:]
		rest, ok = strings.CutSuffix(rest, m.config.Format.Extension)
	case strings.HasPrefix(name, "part-"):
		kind, rest = filePart, name[5:]
		rest, ok = strings.CutSuffix(rest, m.config.Format.Extension)
	}
	if !ok {
		return fileNone, 0, 0
	}
	rest, ok = strings.CutPrefix(rest, m.session+"-")
	if !ok {
		return fileNone, 0, 0
	}
	num, sub, hasSub := strings.Cut(rest, "-")
	if hasSub != (kind == filePart) {
		return fileNone, 0, 0
	}
	n, err := strconv.ParseUint(num, 10, 63)
	if err != nil {
		return fileNone, 0, 0
	}
	if kind == filePart {
		p, err := strconv.ParseUint(sub, 10, 31)
		if err != nil {
			return fileNone, 0, 0
		}
		return kind, n, int64(p)
	}
	return kind, n, 0
}

// File returns the content of the initialization section, segment or part
// called name, from a playlist this Muxer rendered, and false when it is
// not, or no longer, available. The content must not be modified.
func (m *Muxer) File(name string) ([]byte, bool) {
	kind, a, b := m.parseName(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	switch kind {
	case fileInit:
		data, ok := m.inits[int(a)]
		return data, ok
	case fileSegment:
		if s := m.segment(a); s != nil && a < m.listedNext() {
			return s.data, true
		}
	case filePart:
		if s := m.segment(a); s != nil && b < int64(s.pubParts) {
			p := s.parts[b]
			if s.key != nil {
				if s.penc == nil {
					return nil, false
				}
				return s.penc[p.eoff : p.eoff+p.en : p.eoff+p.en], true
			}
			return s.data[p.off : p.off+p.n : p.off+p.n], true
		}
	}
	return nil, false
}

// segment returns the segment msn, listed, being encrypted or open, or nil.
func (m *Muxer) segment(msn uint64) *segment {
	if m.open != nil && m.open.msn == msn {
		return m.open
	}
	if len(m.cut) > 0 && msn >= m.cut[0].msn {
		if i := msn - m.cut[0].msn; i < uint64(len(m.cut)) {
			return m.cut[i]
		}
		return nil
	}
	if len(m.segs) == 0 || msn < m.segs[0].msn {
		return nil
	}
	if i := msn - m.segs[0].msn; i < uint64(len(m.segs)) {
		return m.segs[i]
	}
	return nil
}

// fileWait returns the msn and part to wait for before File(name) can
// answer: the part a preload hint named, or the segment it belongs to.
// ok is false for a name that is not worth waiting for.
func (m *Muxer) fileWait(name string) (msn, part int64, ok bool) {
	kind, a, b := m.parseName(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ended && m.settled() || (kind != filePart && kind != fileSegment) {
		return 0, 0, false
	}
	// Only the next part or segment is worth waiting for; anything
	// further, or gone, is not.
	switch kind {
	case filePart:
		if msn, part := m.hinted(); a == msn && b == int64(part) {
			return int64(a), b, true
		}
	case fileSegment:
		if a == m.listedNext() {
			return int64(a), -1, true
		}
	}
	return 0, 0, false
}
