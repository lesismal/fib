package rtmp

import (
	"encoding/binary"
	"errors"

	"github.com/lesismal/fib/media/amf"
)

// Message types (RTMP specification sections 5.4 and 7.1).
const (
	TypeSetChunkSize     = 1
	TypeAbort            = 2
	TypeAck              = 3
	TypeUserControl      = 4
	TypeWindowAckSize    = 5
	TypeSetPeerBandwidth = 6
	TypeAudio            = 8
	TypeVideo            = 9
	TypeDataAMF3         = 15
	TypeSharedObjectAMF3 = 16
	TypeCommandAMF3      = 17
	TypeDataAMF0         = 18
	TypeSharedObjectAMF0 = 19
	TypeCommandAMF0      = 20
	TypeAggregate        = 22
)

// User control events (section 7.1.7).
const (
	EventStreamBegin      = 0
	EventStreamEOF        = 1
	EventStreamDry        = 2
	EventSetBufferLength  = 3
	EventStreamIsRecorded = 4
	EventPingRequest      = 6
	EventPingResponse     = 7
)

// Chunk stream ids the package writes on: protocol control messages must
// use 2; the others are a convention ffmpeg and SRS share.
const (
	csidControl = 2
	csidCommand = 3
	csidAudio   = 4
	csidData    = 5
	csidVideo   = 6
)

// Defaults for the ConnConfig fields left at zero.
const (
	DefaultChunkSize      = 4096
	DefaultMaxMessageSize = 8 << 20
	DefaultWindowAckSize  = 2500000
	// maxChunkSize bounds what a peer may set, as the 31 bits the
	// specification allows.
	maxChunkSize = 0x7fffffff
	extended     = 0xffffff
)

// Errors of the connection core.
var (
	ErrMessageTooLarge = errors.New("rtmp: message larger than the limit")
	ErrChunk           = errors.New("rtmp: malformed chunk")
	ErrClosed          = errors.New("rtmp: connection closed")
)

// Message is one RTMP message.
type Message struct {
	Type      uint8
	StreamID  uint32
	Timestamp uint32
	// ChunkStreamID is the chunk stream the message came on, or goes on;
	// 0 lets WriteMessage pick one by type.
	ChunkStreamID uint32
	// Payload is the message body. A received one is valid only during
	// the call that delivers it.
	Payload []byte
}

// ConnConfig configures a Conn.
type ConnConfig struct {
	// Client makes the Conn the handshake's client: it sends C0 and C1 on
	// Start. A server's Conn waits for them.
	Client bool
	// SimpleHandshake has a client send the simple handshake rather than
	// the Flash digest one. Servers answer whichever the client sends.
	SimpleHandshake bool
	// MaxMessageSize bounds a message the peer sends, and the bytes of
	// partial messages the Conn holds for it at once. Zero means
	// DefaultMaxMessageSize.
	MaxMessageSize int
	// Output sends bytes to the peer. The Conn reuses the buffer once
	// Output returns, so it must copy what it keeps, as fib's Send does.
	Output func(b []byte) error
	// OnMessage hears every message other than the protocol control ones
	// (set chunk size, abort, acknowledgement, window acknowledgement size,
	// set peer bandwidth), which the Conn handles itself, and other than
	// ping requests, which it answers. An aggregate message is split into
	// the messages it holds. An error stops Feed.
	OnMessage func(m *Message) error
}

// Conn is the sans-IO core of an RTMP connection: the handshake, the chunk
// stream in both directions, and the protocol control messages. It owns no
// socket: Feed gives it what arrived, and it sends through
// ConnConfig.Output. Its methods must be serialized by the caller.
type Conn struct {
	config ConnConfig

	// handshake: 0 waiting for C0+C1 (server) or S0+S1+S2 (client), 1
	// waiting for C2 (server), 2 done.
	state int
	in    []byte

	inChunkSize  int
	inStreams    map[uint32]*inStream
	partialBytes int
	bytesIn      uint64
	ackedIn      uint64
	peerWindow   uint32

	outChunkSize int
	outStreams   map[uint32]*outStream
	ownWindow    uint32
	out          []byte
	closed       bool
}

// inStream is the state of a chunk stream the peer writes on: the header
// fields a chunk may leave out, and the message being assembled.
type inStream struct {
	timestamp, delta, length uint32
	typ                      uint8
	streamID                 uint32
	// ext is the extended timestamp of the last header, when it had one.
	ext      bool
	extValue uint32
	buf      []byte
}

// outStream is the state of a chunk stream the Conn writes on.
type outStream struct {
	timestamp, delta, length uint32
	typ                      uint8
	streamID                 uint32
	ext                      bool
	started                  bool
}

// NewConn returns a Conn.
func NewConn(config ConnConfig) *Conn {
	if config.MaxMessageSize <= 0 {
		config.MaxMessageSize = DefaultMaxMessageSize
	}
	if config.OnMessage == nil {
		config.OnMessage = func(*Message) error { return nil }
	}
	return &Conn{
		config:       config,
		inChunkSize:  128,
		outChunkSize: 128,
		inStreams:    make(map[uint32]*inStream),
		outStreams:   make(map[uint32]*outStream),
	}
}

// Start sends a client's C0 and C1; a server's Start does nothing.
func (c *Conn) Start() error {
	if !c.config.Client {
		return nil
	}
	return c.output(clientHello(c.config.SimpleHandshake))
}

// HandshakeDone reports whether the handshake is over.
func (c *Conn) HandshakeDone() bool { return c.state == 2 }

// BytesIn returns how many bytes the peer has sent since the handshake.
func (c *Conn) BytesIn() uint64 { return c.bytesIn }

// Close makes the Conn refuse to send or read any more.
func (c *Conn) Close() { c.closed = true }

func (c *Conn) output(b []byte) error {
	if c.closed {
		return ErrClosed
	}
	return c.config.Output(b)
}

// Feed reads what arrived from the peer: the rest of the handshake, then
// chunks, delivering each complete message to OnMessage. What does not make
// a whole chunk yet waits for the next Feed.
func (c *Conn) Feed(b []byte) error {
	if c.closed {
		return ErrClosed
	}
	if len(c.in) > 0 {
		c.in = append(c.in, b...)
		b = c.in
	}
	n, err := c.read(b)
	rest := b[n:]
	if len(c.in) > 0 || len(rest) > 0 {
		// Keep what is left at the start of c.in, which may be where it
		// already is.
		c.in = append(c.in[:0], rest...)
		if cap(c.in) > 1<<16 && len(c.in) < 1<<12 {
			// A large message is done with: let its buffer go.
			c.in = append([]byte(nil), c.in...)
		}
	}
	return err
}

// read consumes what it can of b, and returns how much.
func (c *Conn) read(b []byte) (int, error) {
	off := 0
	for c.state < 2 {
		switch {
		case !c.config.Client && c.state == 0:
			if len(b)-off < 1+handshakeSize {
				return off, nil
			}
			reply, err := serverHandshake(b[off : off+1+handshakeSize])
			if err != nil {
				return off, err
			}
			off += 1 + handshakeSize
			if err := c.output(reply); err != nil {
				return off, err
			}
			c.state = 1
		case !c.config.Client:
			if len(b)-off < handshakeSize {
				return off, nil
			}
			off += handshakeSize
			c.state = 2
		default:
			if len(b)-off < 1+2*handshakeSize {
				return off, nil
			}
			c2, err := clientAnswer(b[off : off+1+handshakeSize])
			if err != nil {
				return off, err
			}
			off += 1 + 2*handshakeSize
			if err := c.output(c2); err != nil {
				return off, err
			}
			c.state = 2
		}
	}
	for !c.closed {
		n, err := c.chunk(b[off:])
		if err != nil || n == 0 {
			return off, err
		}
		off += n
		c.bytesIn += uint64(n)
		if err := c.maybeAck(); err != nil {
			return off, err
		}
	}
	return off, nil
}

// maybeAck acknowledges what the peer sent once its window is full.
func (c *Conn) maybeAck() error {
	if c.peerWindow == 0 || c.bytesIn-c.ackedIn < uint64(c.peerWindow) {
		return nil
	}
	c.ackedIn = c.bytesIn
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], uint32(c.bytesIn))
	return c.WriteMessage(&Message{Type: TypeAck, Payload: p[:]})
}

// chunk reads one chunk from b, returning 0 when b does not hold all of
// it.
func (c *Conn) chunk(b []byte) (int, error) {
	if len(b) < 1 {
		return 0, nil
	}
	format := b[0] >> 6
	csid := uint32(b[0] & 0x3f)
	off := 1
	switch csid {
	case 0:
		if len(b) < 2 {
			return 0, nil
		}
		csid, off = 64+uint32(b[1]), 2
	case 1:
		if len(b) < 3 {
			return 0, nil
		}
		csid, off = 64+uint32(b[1])+uint32(b[2])<<8, 3
	}
	headerSize := [4]int{11, 7, 3, 0}[format]
	if len(b) < off+headerSize {
		return 0, nil
	}
	h := b[off : off+headerSize]
	off += headerSize
	s := c.inStreams[csid]
	if s == nil {
		if format != 0 {
			// Only a full header can start a chunk stream.
			return 0, ErrChunk
		}
		if len(c.inStreams) >= 64 {
			return 0, ErrChunk
		}
		s = &inStream{}
		c.inStreams[csid] = s
	}
	continuing := len(s.buf) > 0 && format == 3
	var tsField uint32
	length, typ, streamID := s.length, s.typ, s.streamID
	if format <= 2 {
		tsField = uint32(h[0])<<16 | uint32(h[1])<<8 | uint32(h[2])
	}
	if format <= 1 {
		length = uint32(h[3])<<16 | uint32(h[4])<<8 | uint32(h[5])
		typ = h[6]
	}
	if format == 0 {
		streamID = binary.LittleEndian.Uint32(h[7:])
	}
	ext := format <= 2 && tsField == extended || format == 3 && s.ext
	var extValue uint32
	if ext {
		if len(b) < off+4 {
			return 0, nil
		}
		extValue = binary.BigEndian.Uint32(b[off:])
		if format == 3 && continuing && extValue != s.extValue {
			// Some encoders leave the extended timestamp out of the
			// chunks that continue a message, as the specification
			// once read: these four bytes are payload then.
			ext = false
		} else {
			off += 4
		}
	}
	have := 0
	if continuing {
		have = len(s.buf)
	}
	if int(length) > c.config.MaxMessageSize {
		return 0, ErrMessageTooLarge
	}
	n := min(int(length)-have, c.inChunkSize)
	if len(b) < off+n {
		// Nothing changes until the whole chunk is there.
		return 0, nil
	}
	if !continuing {
		if len(s.buf) > 0 {
			// A new header in the middle of a message drops the rest of
			// it, as an abort would.
			c.partialBytes -= len(s.buf)
			s.buf = s.buf[:0]
		}
		if format <= 2 {
			if ext {
				tsField = extValue
			}
			s.ext, s.extValue = ext, extValue
		}
		switch format {
		case 0:
			s.timestamp, s.delta = tsField, tsField
		case 1, 2:
			s.timestamp += tsField
			s.delta = tsField
		default:
			s.timestamp += s.delta
		}
		s.length, s.typ, s.streamID = length, typ, streamID
		if n == int(length) {
			// A message in one chunk is delivered from b itself.
			off += n
			return off, c.message(s, csid, b[off-n:off])
		}
	}
	if c.partialBytes+n > c.config.MaxMessageSize {
		return 0, ErrMessageTooLarge
	}
	s.buf = append(s.buf, b[off:off+n]...)
	c.partialBytes += n
	off += n
	if len(s.buf) < int(s.length) {
		return off, nil
	}
	payload := s.buf
	c.partialBytes -= len(payload)
	err := c.message(s, csid, payload)
	s.buf = s.buf[:0]
	if cap(s.buf) > 1<<20 {
		s.buf = nil
	}
	return off, err
}

// message handles a complete message.
func (c *Conn) message(s *inStream, csid uint32, payload []byte) error {
	m := Message{Type: s.typ, StreamID: s.streamID, Timestamp: s.timestamp, ChunkStreamID: csid, Payload: payload}
	switch m.Type {
	case TypeSetChunkSize:
		if len(payload) < 4 {
			return ErrChunk
		}
		size := binary.BigEndian.Uint32(payload) & maxChunkSize
		if size == 0 {
			return ErrChunk
		}
		c.inChunkSize = int(size)
		return nil
	case TypeAbort:
		if len(payload) >= 4 {
			if a := c.inStreams[binary.BigEndian.Uint32(payload)]; a != nil && a != s {
				c.partialBytes -= len(a.buf)
				a.buf = a.buf[:0]
			}
		}
		return nil
	case TypeAck:
		return nil
	case TypeWindowAckSize:
		if len(payload) >= 4 {
			c.peerWindow = binary.BigEndian.Uint32(payload)
		}
		return nil
	case TypeSetPeerBandwidth:
		// The peer limits what this side sends; a sender answers with its
		// window acknowledgement size when it changes.
		if len(payload) >= 4 {
			if size := binary.BigEndian.Uint32(payload); size != c.ownWindow {
				return c.WriteWindowAckSize(size)
			}
		}
		return nil
	case TypeUserControl:
		if len(payload) >= 6 && binary.BigEndian.Uint16(payload) == EventPingRequest {
			return c.WriteUserControl(EventPingResponse, binary.BigEndian.Uint32(payload[2:]))
		}
	case TypeAggregate:
		return c.aggregate(&m)
	}
	return c.config.OnMessage(&m)
}

// aggregate delivers the messages an aggregate message holds: FLV tags,
// each followed by its back pointer, whose timestamps are offset so that
// the first has the aggregate's.
func (c *Conn) aggregate(m *Message) error {
	b := m.Payload
	var base uint32
	for first := true; len(b) >= 11; first = false {
		size := int(b[1])<<16 | int(b[2])<<8 | int(b[3])
		ts := uint32(b[7])<<24 | uint32(b[4])<<16 | uint32(b[5])<<8 | uint32(b[6])
		if len(b) < 11+size {
			return ErrChunk
		}
		if first {
			base = ts
		}
		sub := Message{Type: b[0], StreamID: m.StreamID, Timestamp: m.Timestamp + ts - base,
			ChunkStreamID: m.ChunkStreamID, Payload: b[11 : 11+size]}
		if err := c.config.OnMessage(&sub); err != nil {
			return err
		}
		b = b[min(len(b), 11+size+4):]
	}
	return nil
}

// WriteMessage sends m, cut into chunks of the size this side announced.
func (c *Conn) WriteMessage(m *Message) error {
	if c.closed {
		return ErrClosed
	}
	csid := m.ChunkStreamID
	if csid == 0 {
		csid = defaultChunkStream(m.Type)
	}
	s := c.outStreams[csid]
	if s == nil {
		s = &outStream{}
		c.outStreams[csid] = s
	}
	length := uint32(len(m.Payload))
	format := uint8(0)
	tsField := m.Timestamp
	if s.started && s.streamID == m.StreamID && m.Timestamp >= s.timestamp {
		delta := m.Timestamp - s.timestamp
		switch {
		case length != s.length || m.Type != s.typ:
			format = 1
		case delta != s.delta:
			format = 2
		default:
			format = 3
		}
		tsField = delta
	}
	if format != 3 {
		s.ext = tsField >= extended
	}
	s.started, s.timestamp, s.delta, s.length, s.typ, s.streamID = true, m.Timestamp, tsField, length, m.Type, m.StreamID
	out := c.out[:0]
	out = appendBasicHeader(out, format, csid)
	field := min(tsField, extended)
	switch format {
	case 0:
		out = append(out, byte(field>>16), byte(field>>8), byte(field),
			byte(length>>16), byte(length>>8), byte(length), m.Type)
		out = binary.LittleEndian.AppendUint32(out, m.StreamID)
	case 1:
		out = append(out, byte(field>>16), byte(field>>8), byte(field),
			byte(length>>16), byte(length>>8), byte(length), m.Type)
	case 2:
		out = append(out, byte(field>>16), byte(field>>8), byte(field))
	}
	if s.ext {
		out = binary.BigEndian.AppendUint32(out, tsField)
	}
	p := m.Payload
	for {
		n := min(len(p), c.outChunkSize)
		out = append(out, p[:n]...)
		p = p[n:]
		if len(p) == 0 {
			break
		}
		// Continuation chunks repeat the extended timestamp, as ffmpeg,
		// librtmp and Flash expect.
		out = appendBasicHeader(out, 3, csid)
		if s.ext {
			out = binary.BigEndian.AppendUint32(out, tsField)
		}
	}
	c.out = out
	if cap(c.out) > 1<<20 {
		c.out = nil
	}
	return c.output(out)
}

func appendBasicHeader(b []byte, format uint8, csid uint32) []byte {
	switch {
	case csid < 64:
		return append(b, format<<6|byte(csid))
	case csid < 64+256:
		return append(b, format<<6, byte(csid-64))
	}
	return append(b, format<<6|1, byte(csid-64), byte((csid-64)>>8))
}

func defaultChunkStream(typ uint8) uint32 {
	switch typ {
	case TypeSetChunkSize, TypeAbort, TypeAck, TypeUserControl, TypeWindowAckSize, TypeSetPeerBandwidth:
		return csidControl
	case TypeAudio:
		return csidAudio
	case TypeVideo:
		return csidVideo
	case TypeDataAMF0, TypeDataAMF3:
		return csidData
	}
	return csidCommand
}

// SetChunkSize announces the size of the chunks this side writes from now
// on, and switches to it.
func (c *Conn) SetChunkSize(size int) error {
	size = max(1, min(size, maxChunkSize))
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], uint32(size))
	if err := c.WriteMessage(&Message{Type: TypeSetChunkSize, Payload: p[:]}); err != nil {
		return err
	}
	c.outChunkSize = size
	return nil
}

// WriteWindowAckSize tells the peer how many bytes it may send before it
// hears an acknowledgement.
func (c *Conn) WriteWindowAckSize(size uint32) error {
	c.ownWindow = size
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], size)
	return c.WriteMessage(&Message{Type: TypeWindowAckSize, Payload: p[:]})
}

// WritePeerBandwidth limits the peer's output; limitType is 0 (hard), 1
// (soft) or 2 (dynamic).
func (c *Conn) WritePeerBandwidth(size uint32, limitType uint8) error {
	var p [5]byte
	binary.BigEndian.PutUint32(p[:], size)
	p[4] = limitType
	return c.WriteMessage(&Message{Type: TypeSetPeerBandwidth, Payload: p[:]})
}

// WriteUserControl sends a user control event with its 32-bit arguments.
func (c *Conn) WriteUserControl(event uint16, args ...uint32) error {
	p := binary.BigEndian.AppendUint16(make([]byte, 0, 2+4*len(args)), event)
	for _, a := range args {
		p = binary.BigEndian.AppendUint32(p, a)
	}
	return c.WriteMessage(&Message{Type: TypeUserControl, Payload: p})
}

// WriteCommand sends an AMF0 command message on streamID: the command's
// name, transaction id, and the values that follow.
func (c *Conn) WriteCommand(streamID uint32, name string, transactionID float64, values ...any) error {
	p := amf.AppendString(nil, name)
	p = amf.AppendNumber(p, transactionID)
	p, err := amf.Append(p, values...)
	if err != nil {
		return err
	}
	return c.WriteMessage(&Message{Type: TypeCommandAMF0, StreamID: streamID, Payload: p})
}

// Command is a command message decoded.
type Command struct {
	Name          string
	TransactionID float64
	// Object is the command object, often null.
	Object any
	// Args are the values after the command object.
	Args     []any
	StreamID uint32
}

// ErrCommand is a command message that does not start with its name and
// transaction id.
var ErrCommand = errors.New("rtmp: malformed command")

// ParseCommand decodes a command message, AMF0 (type 20) or AMF3 (type
// 17, whose body is AMF0 behind a format byte, switching values to AMF3
// with the AVM+ marker).
func ParseCommand(m *Message) (*Command, error) {
	p := m.Payload
	if m.Type == TypeCommandAMF3 && len(p) > 0 && p[0] == 0 {
		p = p[1:]
	}
	values, err := amf.Decode(p)
	if err != nil {
		return nil, err
	}
	if len(values) < 2 {
		return nil, ErrCommand
	}
	cmd := &Command{StreamID: m.StreamID}
	var ok bool
	if cmd.Name, ok = values[0].(string); !ok {
		return nil, ErrCommand
	}
	if cmd.TransactionID, ok = values[1].(float64); !ok {
		return nil, ErrCommand
	}
	if len(values) > 2 {
		cmd.Object = values[2]
		cmd.Args = values[3:]
	}
	return cmd, nil
}

// DataPayload returns a data message's body as AMF0 script data, which an
// AMF3 data message (type 15) carries behind a format byte.
func DataPayload(m *Message) []byte {
	if m.Type == TypeDataAMF3 && len(m.Payload) > 0 && m.Payload[0] == 0 {
		return m.Payload[1:]
	}
	return m.Payload
}
