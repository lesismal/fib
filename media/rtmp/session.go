package rtmp

import (
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
)

// Defaults shared by ServerConfig and ClientConfig.
const (
	DefaultHandshakeTimeout = 10 * time.Second
	DefaultIdleTimeout      = 30 * time.Second
	// DefaultWriteQueueLimit is the backlog past which a player, or a
	// client publishing, drops packets (video until the next keyframe).
	DefaultWriteQueueLimit = 2 << 20
	// DefaultMaxWriteQueue is the backlog past which the connection is
	// closed instead: a consumer that far behind is not catching up.
	DefaultMaxWriteQueue = 16 << 20
)

// deadlineEvery is how often a rolling deadline is pushed forward: per
// second, not per packet.
const deadlineEvery = time.Second

// session is what the server's and the client's connections share: the
// Conn behind its mutex, the messages a read round collects from it, and
// the rolling deadlines.
//
// The mutex guards only the Conn and what writes through it. A read round
// collects the messages under it and acts on them after letting it go,
// because acting may call into the hub, whose stream mutex calls sinks that
// take this mutex: a player's sink is called with its publisher's lock
// held, so holding a lock while calling into the hub could deadlock two
// connections that publish to each other.
type session struct {
	c    *fib.Connection
	idle time.Duration

	mu   sync.Mutex
	conn *Conn
	// writeAt is when the write deadline was last pushed forward.
	writeAt time.Time

	// The rest belongs to the connection's read rounds.
	inbox  []Message
	arena  []byte
	readAt time.Time
}

func (s *session) init(c *fib.Connection, config ConnConfig, idle time.Duration) {
	s.c, s.idle = c, idle
	config.Output = c.Send
	config.OnMessage = s.collect
	s.conn = NewConn(config)
}

// collect keeps a message for the read round to act on once the mutex is
// released. Payloads are copied into one arena per round.
func (s *session) collect(m *Message) error {
	start := len(s.arena)
	s.arena = append(s.arena, m.Payload...)
	msg := *m
	msg.Payload = s.arena[start:len(s.arena):len(s.arena)]
	s.inbox = append(s.inbox, msg)
	return nil
}

// feed hands what arrived to the Conn and returns the messages it made,
// valid until the next feed.
func (s *session) feed(data []byte) ([]Message, error) {
	if cap(s.arena) > 4<<20 {
		s.arena = nil
	}
	s.inbox, s.arena = s.inbox[:0], s.arena[:0]
	s.mu.Lock()
	err := s.conn.Feed(data)
	s.mu.Unlock()
	return s.inbox, err
}

// write runs fn on the Conn under the mutex, closing the connection when it
// fails.
func (s *session) write(fn func(c *Conn) error) error {
	s.mu.Lock()
	err := fn(s.conn)
	s.mu.Unlock()
	if err != nil {
		_ = s.c.Close()
	}
	return err
}

// touchRead pushes the read deadline forward, for a peer that is expected
// to keep sending: a publisher, or what a client plays.
func (s *session) touchRead() {
	if s.idle <= 0 {
		return
	}
	if now := time.Now(); now.Sub(s.readAt) >= deadlineEvery {
		s.readAt = now
		_ = s.c.SetReadDeadline(now.Add(s.idle))
	}
}

// touchWriteLocked pushes the write deadline forward, which closes a
// connection whose output stops moving for the idle timeout: a player that
// stopped reading.
func (s *session) touchWriteLocked() {
	if s.idle <= 0 {
		return
	}
	if now := time.Now(); now.Sub(s.writeAt) >= deadlineEvery {
		s.writeAt = now
		_ = s.c.SetWriteDeadline(now.Add(s.idle))
	}
}

// backlog reports what the connection has queued and not yet sent.
func (s *session) backlog() int64 {
	if outputwait.Queued == nil {
		return 0
	}
	return outputwait.Queued(s.c)
}

// egress sends a stream out over a session as RTMP media messages, as a
// hub.Sink: a server sending to a player, or a client publishing. Its
// fields are guarded by the session's mutex.
type egress struct {
	s        *session
	streamID uint32
	enc      *flv.Encoder
	// setDataFrame wraps onMetaData in @setDataFrame, as a publisher sends
	// it so that the server keeps it for its players.
	setDataFrame bool
	// live says the stream may be sent: the play's or the publish's status
	// has gone. Tracks and packets that come before, as the GOP cache a
	// subscription starts with, are kept for then, up to maxPending.
	live    bool
	stopped bool
	tracks  []*av.Track
	pending []*av.Packet
	started bool
	lastTS  uint32
	buf     []byte
	onClose func(err error)
}

// maxPending bounds the packets an egress keeps before it is live. They
// are shared with the hub, so they cost a pointer each.
const maxPending = 4096

// start makes the egress live on streamID and sends what it has kept.
func (e *egress) start(streamID uint32) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.live, e.streamID = true, streamID
	pending := e.pending
	e.pending = nil
	if e.tracks == nil || e.stopped {
		return
	}
	e.sendTracksLocked()
	for _, pkt := range pending {
		e.sendPacketLocked(pkt)
	}
}

// stop makes the egress send nothing more.
func (e *egress) stop() {
	e.s.mu.Lock()
	e.stopped = true
	e.s.mu.Unlock()
}

func (e *egress) OnTracks(tracks []*av.Track) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	e.tracks = tracks
	if e.live && !e.stopped {
		e.sendTracksLocked()
	} else {
		// Kept packets index the tracks that came before.
		e.pending = nil
	}
}

func (e *egress) sendTracksLocked() {
	e.enc.SetTracks(e.tracks)
	if video, audio := e.enc.Tracks(); video == nil && audio == nil {
		// A stream published before its tracks are known: they come in a
		// second OnTracks.
		return
	}
	var err error
	e.enc.Headers(func(typ uint8, body []byte) {
		if err != nil {
			return
		}
		mt := uint8(TypeDataAMF0)
		switch typ {
		case flv.TagAudio:
			mt = TypeAudio
		case flv.TagVideo:
			mt = TypeVideo
		default:
			if e.setDataFrame {
				body = append(amf.AppendString(nil, "@setDataFrame"), body...)
			}
		}
		err = e.s.conn.WriteMessage(&Message{Type: mt, StreamID: e.streamID, Timestamp: e.lastTS, Payload: body})
	})
	if err != nil {
		_ = e.s.c.Close()
		return
	}
	e.started = true
}

func (e *egress) OnPacket(pkt *av.Packet) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	if e.stopped {
		return
	}
	if !e.live {
		if len(e.pending) < maxPending {
			e.pending = append(e.pending, pkt)
		}
		return
	}
	e.sendPacketLocked(pkt)
}

func (e *egress) sendPacketLocked(pkt *av.Packet) {
	if !e.started {
		return
	}
	b, typ, err := e.enc.AppendPacket(e.buf[:0], pkt)
	if typ == 0 || err != nil {
		return
	}
	e.buf = b
	ts := flv.Millis(pkt.DTS)
	if err := e.s.conn.WriteMessage(&Message{Type: typ, StreamID: e.streamID, Timestamp: ts, Payload: b}); err != nil {
		_ = e.s.c.Close()
		return
	}
	e.lastTS = ts
	e.s.touchWriteLocked()
}

func (e *egress) OnClose(err error) {
	if e.onClose != nil {
		e.onClose(err)
	}
}

// publisherSink feeds what an ingest decodes into a hub stream it
// publishes, and to a sink of the user's alongside.
type publisherSink struct {
	pub  *hub.Publisher
	also hub.Sink
}

func (p *publisherSink) OnTracks(tracks []*av.Track) {
	if p.pub != nil {
		p.pub.SetTracks(tracks)
	}
	if p.also != nil {
		p.also.OnTracks(tracks)
	}
}

func (p *publisherSink) OnPacket(pkt *av.Packet) {
	if p.pub != nil {
		p.pub.WritePacket(pkt)
	}
	if p.also != nil {
		p.also.OnPacket(pkt)
	}
}

func (p *publisherSink) OnClose(err error) {
	if p.pub != nil {
		p.pub.Close()
	}
	if p.also != nil {
		p.also.OnClose(err)
	}
}

// newIngest returns a Decoder that delivers to sink.
func newIngest(sink hub.Sink) *flv.Decoder {
	return flv.NewDecoder(flv.DecoderConfig{OnTracks: sink.OnTracks, OnPacket: sink.OnPacket})
}

// ingestMessage hands a media or data message to dec; a malformed one is
// dropped, as one bad tag should not end a stream.
func ingestMessage(dec *flv.Decoder, m *Message) {
	switch m.Type {
	case TypeAudio:
		_ = dec.WriteTag(flv.TagAudio, m.Timestamp, m.Payload)
	case TypeVideo:
		_ = dec.WriteTag(flv.TagVideo, m.Timestamp, m.Payload)
	case TypeDataAMF0, TypeDataAMF3:
		_ = dec.WriteTag(flv.TagScript, m.Timestamp, DataPayload(m))
	}
}

// status builds the info object of an onStatus or _result.
func status(level, code, description string) amf.Object {
	return amf.Object{{Key: "level", Value: level}, {Key: "code", Value: code}, {Key: "description", Value: description}}
}

// statusCode reads an info object's level and code.
func statusCode(v any) (level, code, description string) {
	var obj amf.Object
	switch o := v.(type) {
	case amf.Object:
		obj = o
	case amf.ECMAArray:
		obj = amf.Object(o)
	}
	level, _ = obj.String("level")
	code, _ = obj.String("code")
	description, _ = obj.String("description")
	return level, code, description
}
