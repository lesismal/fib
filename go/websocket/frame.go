// Package websocket implements RFC 6455 handshakes, incremental frame parsing,
// message reassembly, and response framing.
package websocket

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/lesismal/fib/go/bufferpool"
)

type Opcode byte

const (
	Continuation Opcode = 0x0
	Text         Opcode = 0x1
	Binary       Opcode = 0x2
	Close        Opcode = 0x8
	Ping         Opcode = 0x9
	Pong         Opcode = 0xa
)

const (
	CloseNormal          = 1000
	CloseGoingAway       = 1001
	CloseProtocolError   = 1002
	CloseUnsupportedData = 1003
	CloseInvalidPayload  = 1007
	ClosePolicyViolation = 1008
	CloseMessageTooBig   = 1009
	CloseInternalError   = 1011
)

var (
	ErrProtocol       = errors.New("websocket: protocol error")
	ErrMessageTooBig  = errors.New("websocket: message too large")
	ErrInvalidPayload = errors.New("websocket: invalid payload")
)

const maxRetainedFrameBuffer = 64 << 10

type Event struct {
	Opcode  Opcode
	Payload []byte
	// Fin reports that this is the last frame of its message. It is true for
	// every event but those of a parser that delivers frames as they arrive,
	// which SetPerFrame turns on, where a message may take several.
	Fin bool
}

type fragmentedMessage struct {
	opcode     Opcode
	compressed bool
	data       []byte
}

type Parser struct {
	maxMessageBytes int64
	// fromServer makes the parser read the frames a server sends, which are
	// unmasked, instead of the masked frames a client sends. Either side must
	// reject a frame masked the other way (RFC 6455 section 5.1).
	fromServer   bool
	buffer       []byte
	owned        []byte
	borrowedTail []byte
	fragment     *fragmentedMessage
	// perFrame emits every frame of a data message as it completes instead of
	// reassembling the message, and frameMessage is the state of the message
	// being received, which then holds no payload of its own.
	perFrame     bool
	frameMessage fragmentedMessage
	// text validates the UTF-8 of the text message being received, and
	// textChecked counts the payload bytes of the frame at the head of buffer
	// that it has already seen while that frame was incomplete.
	text           utf8Validator
	textChecked    int
	pendingConsume int
	borrowedBuffer bool
	// deflate says permessage-deflate was negotiated, so a data message may
	// arrive compressed. contextTakeover says the peer compresses each message
	// with the context of the ones before it, whose tail window then keeps.
	deflate         bool
	contextTakeover bool
	window          []byte
	// inflated holds the last message FeedOneBorrowed decompressed.
	inflated []byte
}

func NewParser(maxMessageBytes int64) *Parser {
	if maxMessageBytes <= 0 {
		maxMessageBytes = 16 << 20
	}
	return &Parser{maxMessageBytes: maxMessageBytes}
}

// NewServerFrameParser returns a parser for the unmasked frames a server sends,
// which is what a client reads.
func NewServerFrameParser(maxMessageBytes int64) *Parser {
	p := NewParser(maxMessageBytes)
	p.fromServer = true
	return p
}

// SetPerFrame chooses whether a fragmented data message is emitted frame by
// frame, each event carrying Fin, or held until it can be emitted whole. Per
// frame nothing accumulates between frames, so a peer may stream a message far
// larger than maxMessageBytes, which then bounds one frame rather than the
// message. A compressed message is the exception: its frames cannot be
// inflated one by one, so it is reassembled either way and emitted whole with
// Fin set.
func (p *Parser) SetPerFrame(on bool) { p.perFrame = on }

func (p *Parser) Reset() {
	if p.borrowedBuffer {
		p.buffer = nil
	} else {
		p.buffer = p.buffer[:0]
	}
	p.fragment = nil
	p.text.reset()
	p.textChecked = 0
	p.window = nil
	p.releaseInflated()
	p.pendingConsume = 0
	p.borrowedBuffer = false
	p.borrowedTail = nil
	if len(p.buffer) == 0 {
		p.buffer = nil
		p.releaseOwned()
	}
}

// Feed parses frames and returns complete messages and control frames. A
// parser from NewParser reads a client's masked frames; see
// NewServerFrameParser for the other direction. Fragmented data messages are
// reassembled before being returned, unless SetPerFrame says otherwise.
func (p *Parser) Feed(data []byte) ([]Event, error) {
	var events []Event
	for {
		event, complete, err := p.FeedOne(data)
		data = nil
		if err != nil || !complete {
			return events, err
		}
		events = append(events, event)
	}
}

// FeedOne returns at most one complete event without allocating an event
// slice. Call it again with nil to drain additional frames already buffered.
func (p *Parser) FeedOne(data []byte) (Event, bool, error) {
	return p.feedOne(data, false)
}

// FeedOneBorrowed is the allocation-free variant used by ServerHandler for
// unfragmented frames. Event.Payload remains valid only until the next parser
// call and must not be retained by the callback.
func (p *Parser) FeedOneBorrowed(data []byte) (Event, bool, error) {
	return p.feedOne(data, true)
}

// ReleaseBorrowed detaches any input buffer retained by FeedOneBorrowed. It is
// cheap to call after each network-data callback.
func (p *Parser) ReleaseBorrowed() {
	if p.pendingConsume != 0 {
		p.consume(p.pendingConsume)
		p.pendingConsume = 0
	}
	p.borrowedTail = nil
	p.releaseInflated()
	if p.borrowedBuffer {
		p.buffer = nil
		p.borrowedBuffer = false
	}
	if len(p.buffer) == 0 {
		// Nothing is half-parsed, so the adopted array can go back for another
		// connection to use until this one needs one again.
		p.buffer = nil
		p.releaseOwned()
	}
}

// releaseOwned returns the adopted array to the pool. The caller must have
// established that no partial frame still lives in it. An array one large
// message grew goes back as well: it returns to the class its capacity
// belongs to, so it can never be handed to a connection that asked for an
// ordinary one.
func (p *Parser) releaseOwned() {
	bufferpool.Put(p.owned)
	p.owned = nil
}

func (p *Parser) feedOne(data []byte, borrowPayload bool) (Event, bool, error) {
	if p.pendingConsume != 0 {
		p.consume(p.pendingConsume)
		p.pendingConsume = 0
	}
	if len(p.buffer) == 0 && len(p.borrowedTail) != 0 {
		p.buffer = p.borrowedTail
		p.borrowedTail = nil
		p.borrowedBuffer = true
	}
	if borrowPayload && len(p.buffer) == 0 && len(data) != 0 {
		p.buffer = data
		p.borrowedBuffer = true
	} else if borrowPayload && !p.borrowedBuffer && len(p.buffer) != 0 && len(data) != 0 {
		p.buffer, p.borrowedTail = appendCurrentFrame(p.buffer, data)
		p.keepOwned()
	} else {
		if p.borrowedBuffer && len(data) != 0 {
			// The partial frame still lives in the read buffer, which is not
			// this parser's to append to: the bytes past it belong to whoever
			// lent it. Copy it out first.
			p.adopt()
		}
		p.buffer = bufferpool.Append(p.buffer, data)
		p.keepOwned()
	}
	for {
		event, emit, complete, err := p.next(borrowPayload)
		if err != nil {
			p.buffer = nil
			p.releaseOwned()
			p.borrowedBuffer = false
			p.borrowedTail = nil
			p.fragment = nil
			p.text.reset()
			p.textChecked = 0
			p.window = nil
			p.releaseInflated()
			return Event{}, false, err
		}
		if !complete {
			// A fragment emits nothing, so the bytes that followed it in the
			// same read must be picked up here rather than on the next call.
			if len(p.buffer) == 0 && len(p.borrowedTail) != 0 {
				p.buffer = p.borrowedTail
				p.borrowedTail = nil
				p.borrowedBuffer = true
				continue
			}
			if p.borrowedBuffer && len(p.buffer) != 0 {
				p.adopt()
			}
			return Event{}, false, nil
		}
		if emit {
			return event, true, nil
		}
	}
}

// keepOwned remembers the parser-owned array behind buffer so a later adopt can
// reuse it. An emptied or borrowed buffer must not displace a larger array that
// is still worth keeping, so the retained one only ever grows.
func (p *Parser) keepOwned() {
	if p.borrowedBuffer || cap(p.buffer) < cap(p.owned) {
		return
	}
	p.owned = p.buffer
}

// adopt copies the borrowed tail into the parser's own array so the read
// buffer can go back to its pool. At high message rates almost every read ends
// mid-frame, so the array is retained and reused across reads rather than
// allocated each time.
func (p *Parser) adopt() {
	capacity := len(p.buffer)
	// Sizing to the whole frame avoids regrowing while the rest of it arrives,
	// but only up to the retention limit: a peer that announces a huge frame
	// must not make every connection reserve it from the first bytes onward.
	if frameEnd, known := frameSize(p.buffer); known && frameEnd > capacity &&
		uint64(frameEnd) <= uint64(p.maxMessageBytes)+14 {
		if frameEnd > maxRetainedFrameBuffer {
			frameEnd = maxRetainedFrameBuffer
		}
		if frameEnd > capacity {
			capacity = frameEnd
		}
	}
	if cap(p.owned) < capacity {
		bufferpool.Put(p.owned)
		p.owned = bufferpool.Get(capacity)
	}
	p.owned = append(p.owned[:0], p.buffer...)
	p.buffer = p.owned
	p.borrowedBuffer = false
}

// appendCurrentFrame grows buffer, which the parser owns, through the pool
// with as much of data as the frame being received still needs, and returns
// what is left of data for the frame after it.
func appendCurrentFrame(buffer, data []byte) ([]byte, []byte) {
	for {
		frameEnd, known := frameSize(buffer)
		if known {
			need := frameEnd - len(buffer)
			if need <= 0 || need >= len(data) {
				return bufferpool.Append(buffer, data), nil
			}
			return bufferpool.Append(buffer, data[:need]), data[need:]
		}
		need := frameHeaderSize(buffer) - len(buffer)
		if need <= 0 || need >= len(data) {
			return bufferpool.Append(buffer, data), nil
		}
		buffer = bufferpool.Append(buffer, data[:need])
		data = data[need:]
	}
}

func frameHeaderSize(data []byte) int {
	if len(data) < 2 {
		return 2
	}
	size := 2
	if data[1]&0x80 != 0 {
		size += 4
	}
	switch data[1] & 0x7f {
	case 126:
		return size + 2
	case 127:
		return size + 8
	default:
		return size
	}
}

func frameSize(data []byte) (int, bool) {
	headerSize := frameHeaderSize(data)
	if len(data) < headerSize {
		return 0, false
	}
	payloadLen := uint64(data[1] & 0x7f)
	if payloadLen == 126 {
		payloadLen = uint64(binary.BigEndian.Uint16(data[2:4]))
	} else if payloadLen == 127 {
		payloadLen = binary.BigEndian.Uint64(data[2:10])
	}
	if payloadLen > uint64(^uint(0)>>1)-uint64(headerSize) {
		return 0, false
	}
	return headerSize + int(payloadLen), true
}

func (p *Parser) next(borrowPayload bool) (Event, bool, bool, error) {
	if len(p.buffer) < 2 {
		return Event{}, false, false, nil
	}
	first, second := p.buffer[0], p.buffer[1]
	fin := first&0x80 != 0
	opcode := Opcode(first & 0x0f)
	masked := second&0x80 != 0
	// RSV1 marks the first frame of a compressed message (RFC 7692 section
	// 6), and only once permessage-deflate is in use.
	compressed := first&0x40 != 0
	if first&0x30 != 0 || masked == p.fromServer ||
		(compressed && (!p.deflate || (opcode != Text && opcode != Binary))) {
		return Event{}, false, false, ErrProtocol
	}
	control := opcode >= 0x8
	if control && (!fin || second&0x7f > 125) {
		return Event{}, false, false, ErrProtocol
	}
	switch opcode {
	case Continuation:
		if p.fragment == nil {
			return Event{}, false, false, ErrProtocol
		}
	case Text, Binary:
		if p.fragment != nil {
			return Event{}, false, false, ErrProtocol
		}
	case Close, Ping, Pong:
	default:
		return Event{}, false, false, ErrProtocol
	}

	offset := 2
	payloadLen := uint64(second & 0x7f)
	if payloadLen == 126 {
		if len(p.buffer) < offset+2 {
			return Event{}, false, false, nil
		}
		payloadLen = uint64(binary.BigEndian.Uint16(p.buffer[offset : offset+2]))
		offset += 2
		if payloadLen < 126 {
			return Event{}, false, false, ErrProtocol
		}
	} else if payloadLen == 127 {
		if len(p.buffer) < offset+8 {
			return Event{}, false, false, nil
		}
		payloadLen = binary.BigEndian.Uint64(p.buffer[offset : offset+8])
		offset += 8
		if payloadLen < 65536 || payloadLen>>63 != 0 {
			return Event{}, false, false, ErrProtocol
		}
	}
	if control && payloadLen > 125 {
		return Event{}, false, false, ErrProtocol
	}
	current := int64(0)
	if p.fragment != nil {
		current = int64(len(p.fragment.data))
	}
	if !control && (payloadLen > uint64(p.maxMessageBytes) || current > p.maxMessageBytes-int64(payloadLen)) {
		return Event{}, false, false, ErrMessageTooBig
	}
	var mask []byte
	if masked {
		if len(p.buffer) < offset+4 {
			return Event{}, false, false, nil
		}
		mask = p.buffer[offset : offset+4]
		offset += 4
	}
	// A compressed message's text is checked once it is decompressed.
	text := (opcode == Text && !compressed) ||
		(opcode == Continuation && p.fragment.opcode == Text && !p.fragment.compressed)
	if payloadLen > uint64(len(p.buffer)-offset) {
		if text && !p.checkPartialText(p.buffer[offset:], mask) {
			return Event{}, false, false, ErrInvalidPayload
		}
		return Event{}, false, false, nil
	}
	frameEnd := offset + int(payloadLen)
	checked := p.textChecked
	p.textChecked = 0
	var payload []byte
	if borrowPayload {
		payload = p.buffer[offset:frameEnd]
		if masked {
			applyMask(payload, payload, mask)
		}
	} else {
		payload = make([]byte, int(payloadLen))
		if masked {
			applyMask(payload, p.buffer[offset:frameEnd], mask)
		} else {
			copy(payload, p.buffer[offset:frameEnd])
		}
	}

	if control {
		if opcode == Close {
			if len(payload) == 1 || (len(payload) >= 2 && !validCloseCode(binary.BigEndian.Uint16(payload[:2]))) {
				return Event{}, false, false, ErrProtocol
			}
			if len(payload) > 2 && !utf8.Valid(payload[2:]) {
				return Event{}, false, false, ErrInvalidPayload
			}
		}
		p.finishFrame(frameEnd, borrowPayload)
		return Event{Opcode: opcode, Payload: payload, Fin: true}, true, true, nil
	}
	if opcode == Text || opcode == Binary {
		if fin && compressed {
			payload, err := p.inflate(payload, borrowPayload)
			if err != nil {
				return Event{}, false, false, err
			}
			if opcode == Text && !utf8.Valid(payload) {
				return Event{}, false, false, ErrInvalidPayload
			}
			p.finishFrame(frameEnd, borrowPayload)
			return Event{Opcode: opcode, Payload: payload, Fin: true}, true, true, nil
		}
		if fin {
			if text {
				if checked == 0 {
					if !utf8.Valid(payload) {
						return Event{}, false, false, ErrInvalidPayload
					}
				} else if !p.text.write(payload[checked:]) || !p.text.complete() {
					return Event{}, false, false, ErrInvalidPayload
				}
				p.text.reset()
			}
			p.finishFrame(frameEnd, borrowPayload)
			return Event{Opcode: opcode, Payload: payload, Fin: true}, true, true, nil
		}
		if text && !p.text.write(payload[checked:]) {
			return Event{}, false, false, ErrInvalidPayload
		}
		if p.perFrame && !compressed {
			// The frame goes out as it is and nothing is kept but the opcode
			// the continuations belong to.
			p.frameMessage = fragmentedMessage{opcode: opcode}
			p.fragment = &p.frameMessage
			p.finishFrame(frameEnd, borrowPayload)
			return Event{Opcode: opcode, Payload: payload}, true, true, nil
		}
		p.fragment = &fragmentedMessage{opcode: opcode, compressed: compressed, data: append([]byte(nil), payload...)}
		p.consume(frameEnd)
		return Event{}, false, true, nil
	}
	if text && (!p.text.write(payload[checked:]) || (fin && !p.text.complete())) {
		return Event{}, false, false, ErrInvalidPayload
	}
	if p.perFrame && !p.fragment.compressed {
		event := Event{Opcode: p.fragment.opcode, Payload: payload, Fin: fin}
		if fin {
			p.text.reset()
			p.fragment = nil
		}
		p.finishFrame(frameEnd, borrowPayload)
		return event, true, true, nil
	}
	p.fragment.data = append(p.fragment.data, payload...)
	p.consume(frameEnd)
	if !fin {
		return Event{}, false, true, nil
	}
	event := Event{Opcode: p.fragment.opcode, Payload: p.fragment.data, Fin: true}
	if p.fragment.compressed {
		var err error
		if event.Payload, err = p.inflate(event.Payload, borrowPayload); err != nil {
			return Event{}, false, false, err
		}
		if event.Opcode == Text && !utf8.Valid(event.Payload) {
			return Event{}, false, false, ErrInvalidPayload
		}
	}
	p.text.reset()
	p.fragment = nil
	return event, true, true, nil
}

// inflate decompresses a message. For FeedOneBorrowed it decompresses into a
// pooled array the parser holds until ReleaseBorrowed, since the payload need
// only last until the next parser call; Feed hands each message out, so each
// gets an array of its own.
func (p *Parser) inflate(compressed []byte, borrowed bool) ([]byte, error) {
	var dst []byte
	if borrowed {
		dst = p.inflated[:0]
	}
	var window []byte
	if p.contextTakeover {
		window = p.window
	}
	message, err := decompressMessage(dst, compressed, window, p.maxMessageBytes)
	if err != nil {
		return nil, err
	}
	if borrowed {
		p.inflated = message
	}
	if p.contextTakeover {
		p.window = keepWindow(p.window, message)
	}
	return message, nil
}

func (p *Parser) releaseInflated() {
	bufferpool.Put(p.inflated)
	p.inflated = nil
}

// checkPartialText validates the payload bytes of an incomplete text frame
// that arrived since the last call, so invalid UTF-8 fails the connection
// without waiting for the rest of the frame. available is the payload received
// so far, still masked when mask is set.
func (p *Parser) checkPartialText(available, mask []byte) bool {
	if len(available) <= p.textChecked {
		return true
	}
	if mask == nil {
		if !p.text.write(available[p.textChecked:]) {
			return false
		}
	} else {
		var chunk [512]byte
		for i := p.textChecked; i < len(available); {
			n := copy(chunk[:], available[i:])
			for j := 0; j < n; j++ {
				chunk[j] ^= mask[(i+j)&3]
			}
			if !p.text.write(chunk[:n]) {
				return false
			}
			i += n
		}
	}
	p.textChecked = len(available)
	return true
}

func applyMask(dst, src, mask []byte) {
	mask32 := binary.LittleEndian.Uint32(mask)
	mask64 := uint64(mask32) | uint64(mask32)<<32
	i := 0
	for ; i+8 <= len(src); i += 8 {
		binary.LittleEndian.PutUint64(dst[i:i+8], binary.LittleEndian.Uint64(src[i:i+8])^mask64)
	}
	for ; i < len(src); i++ {
		dst[i] = src[i] ^ mask[i&3]
	}
}

func (p *Parser) finishFrame(frameEnd int, borrowed bool) {
	if borrowed {
		p.pendingConsume = frameEnd
	} else {
		p.consume(frameEnd)
	}
}

func (p *Parser) consume(n int) {
	if n == len(p.buffer) {
		switch {
		case p.borrowedBuffer:
			// The retained array is kept: the next partial frame reuses it.
			p.buffer = nil
		case cap(p.buffer) > maxRetainedFrameBuffer:
			// One message grew this parser's array past what is worth keeping
			// attached to a connection. The pool keeps it in the class it
			// belongs to instead.
			p.releaseOwned()
			p.buffer = nil
		default:
			p.buffer = p.buffer[:0]
		}
		p.borrowedBuffer = false
		return
	}
	copy(p.buffer, p.buffer[n:])
	p.buffer = p.buffer[:len(p.buffer)-n]
}

func validCloseCode(code uint16) bool {
	if code >= 1000 && code <= 1014 {
		return code != 1004 && code != 1005 && code != 1006
	}
	return code >= 3000 && code <= 4999
}

// MarshalFrame builds an unmasked server-to-client frame.
func MarshalFrame(opcode Opcode, payload []byte) ([]byte, error) {
	if opcode != Text && opcode != Binary && opcode != Close && opcode != Ping && opcode != Pong {
		return nil, ErrProtocol
	}
	if opcode >= 0x8 && len(payload) > 125 {
		return nil, ErrProtocol
	}
	headerLen := 2
	if len(payload) >= 126 && len(payload) <= 65535 {
		headerLen += 2
	} else if len(payload) > 65535 {
		headerLen += 8
	}
	frame := make([]byte, headerLen+len(payload))
	frame[0] = 0x80 | byte(opcode)
	offset := 2
	switch {
	case len(payload) < 126:
		frame[1] = byte(len(payload))
	case len(payload) <= 65535:
		frame[1] = 126
		binary.BigEndian.PutUint16(frame[2:4], uint16(len(payload)))
		offset = 4
	default:
		frame[1] = 127
		binary.BigEndian.PutUint64(frame[2:10], uint64(len(payload)))
		offset = 10
	}
	copy(frame[offset:], payload)
	return frame, nil
}

func frameHeader(opcode Opcode, payloadLen int) ([10]byte, int, error) {
	var header [10]byte
	if opcode != Text && opcode != Binary && opcode != Close && opcode != Ping && opcode != Pong {
		return header, 0, ErrProtocol
	}
	if opcode >= 0x8 && payloadLen > 125 {
		return header, 0, ErrProtocol
	}
	header[0] = 0x80 | byte(opcode)
	switch {
	case payloadLen < 126:
		header[1] = byte(payloadLen)
		return header, 2, nil
	case payloadLen <= 65535:
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(payloadLen))
		return header, 4, nil
	default:
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:10], uint64(payloadLen))
		return header, 10, nil
	}
}
