package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
)

// MessageType is the ID byte of a message.
type MessageType uint8

// Message types: BEP 3 and BEP 5's port, BEP 6's Fast extension, BEP 10's
// extended message and BEP 52's hash messages.
const (
	MsgChoke         MessageType = 0
	MsgUnchoke       MessageType = 1
	MsgInterested    MessageType = 2
	MsgNotInterested MessageType = 3
	MsgHave          MessageType = 4
	MsgBitfield      MessageType = 5
	MsgRequest       MessageType = 6
	MsgPiece         MessageType = 7
	MsgCancel        MessageType = 8
	MsgPort          MessageType = 9
	MsgSuggest       MessageType = 13
	MsgHaveAll       MessageType = 14
	MsgHaveNone      MessageType = 15
	MsgRejectRequest MessageType = 16
	MsgAllowedFast   MessageType = 17
	MsgExtended      MessageType = 20
	MsgHashRequest   MessageType = 21
	MsgHashes        MessageType = 22
	MsgHashReject    MessageType = 23
)

var messageNames = map[MessageType]string{
	MsgChoke: "choke", MsgUnchoke: "unchoke", MsgInterested: "interested",
	MsgNotInterested: "not interested", MsgHave: "have", MsgBitfield: "bitfield",
	MsgRequest: "request", MsgPiece: "piece", MsgCancel: "cancel", MsgPort: "port",
	MsgSuggest: "suggest", MsgHaveAll: "have all", MsgHaveNone: "have none",
	MsgRejectRequest: "reject request", MsgAllowedFast: "allowed fast",
	MsgExtended: "extended", MsgHashRequest: "hash request", MsgHashes: "hashes",
	MsgHashReject: "hash reject",
}

func (t MessageType) String() string {
	if s, ok := messageNames[t]; ok {
		return s
	}
	return "message " + strconv.Itoa(int(t))
}

// Fast reports whether t is one of BEP 6's messages, which only a peer
// that set the Fast bit may send.
func (t MessageType) Fast() bool { return t >= MsgSuggest && t <= MsgAllowedFast }

// Sizes of blocks.
const (
	// BlockSize is the size of a block nearly every client requests, and
	// the most many serve.
	BlockSize = 16 << 10
	// MaxBlockSize is the largest request a handler need serve: BEP 3
	// lets a peer close the connection on a larger one.
	MaxBlockSize = 128 << 10
)

// DefaultMaxMessageLength bounds a message when Parser.MaxLength is zero:
// a bitfield of eight million pieces, or a block of a megabyte, and
// whatever extension messages fit.
const DefaultMaxMessageLength = 1<<20 + 64

// Request is a block: a piece, the offset in it and the length.
type Request struct {
	Index, Begin, Length uint32
}

// Message is a message of the peer wire protocol. Which fields mean
// something depends on Type:
//
//   - have, suggest, allowed fast: Index;
//   - request, cancel, reject request: Index, Begin, Length;
//   - piece: Index, Begin and the block in Data;
//   - bitfield: Data;
//   - port: Port;
//   - extended: ExtendedID and the payload in Data;
//   - hash request, hash reject: PiecesRoot, BaseLayer, Index, Length and
//     ProofLayers; hashes: those and the hashes in Data;
//   - another type, unknown here: Data is its payload.
//
// Keepalive is the message of length zero, which has no type.
type Message struct {
	Keepalive   bool
	Type        MessageType
	Index       uint32
	Begin       uint32
	Length      uint32
	Port        uint16
	ExtendedID  uint8
	PiecesRoot  [32]byte
	BaseLayer   uint32
	ProofLayers uint32
	Data        []byte
}

// Request is the block of a request, cancel, reject request or piece
// message; a piece's length is its data's.
func (m *Message) Request() Request {
	if m.Type == MsgPiece {
		return Request{m.Index, m.Begin, uint32(len(m.Data))}
	}
	return Request{m.Index, m.Begin, m.Length}
}

func (m *Message) String() string {
	if m.Keepalive {
		return "keepalive"
	}
	switch m.Type {
	case MsgHave, MsgSuggest, MsgAllowedFast:
		return fmt.Sprintf("%v %d", m.Type, m.Index)
	case MsgRequest, MsgCancel, MsgRejectRequest:
		return fmt.Sprintf("%v %d+%d:%d", m.Type, m.Index, m.Begin, m.Length)
	case MsgPiece:
		return fmt.Sprintf("piece %d+%d:%d", m.Index, m.Begin, len(m.Data))
	case MsgPort:
		return fmt.Sprintf("port %d", m.Port)
	case MsgExtended:
		return fmt.Sprintf("extended %d (%d bytes)", m.ExtendedID, len(m.Data))
	case MsgBitfield:
		return fmt.Sprintf("bitfield (%d bytes)", len(m.Data))
	}
	return m.Type.String()
}

// MessageError is a message whose length does not fit its type, or that
// is longer than allowed.
type MessageError struct {
	Type   MessageType
	Length int
	Reason string
}

func (e *MessageError) Error() string {
	return fmt.Sprintf("peer: %v message of length %d: %s", e.Type, e.Length, e.Reason)
}

// hashHeaderLen is the fixed part of BEP 52's hash messages: pieces root,
// base layer, index, length and proof layers.
const hashHeaderLen = 32 + 4*4

// decode parses body, a message's type byte and payload, into m. Data
// aliases body.
func decode(body []byte, m *Message) error {
	*m = Message{Type: MessageType(body[0])}
	p := body[1:]
	bad := func(reason string) error {
		return &MessageError{Type: m.Type, Length: len(body), Reason: reason}
	}
	exact := func(n int) error {
		if len(p) != n {
			return bad("want length " + strconv.Itoa(n+1))
		}
		return nil
	}
	switch m.Type {
	case MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested, MsgHaveAll, MsgHaveNone:
		return exact(0)
	case MsgHave, MsgSuggest, MsgAllowedFast:
		if err := exact(4); err != nil {
			return err
		}
		m.Index = binary.BigEndian.Uint32(p)
	case MsgRequest, MsgCancel, MsgRejectRequest:
		if err := exact(12); err != nil {
			return err
		}
		m.Index = binary.BigEndian.Uint32(p)
		m.Begin = binary.BigEndian.Uint32(p[4:])
		m.Length = binary.BigEndian.Uint32(p[8:])
	case MsgPiece:
		if len(p) < 8 {
			return bad("too short")
		}
		m.Index = binary.BigEndian.Uint32(p)
		m.Begin = binary.BigEndian.Uint32(p[4:])
		m.Data = p[8:]
	case MsgPort:
		if err := exact(2); err != nil {
			return err
		}
		m.Port = binary.BigEndian.Uint16(p)
	case MsgExtended:
		if len(p) < 1 {
			return bad("too short")
		}
		m.ExtendedID = p[0]
		m.Data = p[1:]
	case MsgHashRequest, MsgHashReject, MsgHashes:
		if m.Type == MsgHashes {
			if len(p) < hashHeaderLen || (len(p)-hashHeaderLen)%32 != 0 {
				return bad("hashes not a multiple of 32 bytes")
			}
		} else if err := exact(hashHeaderLen); err != nil {
			return err
		}
		copy(m.PiecesRoot[:], p)
		m.BaseLayer = binary.BigEndian.Uint32(p[32:])
		m.Index = binary.BigEndian.Uint32(p[36:])
		m.Length = binary.BigEndian.Uint32(p[40:])
		m.ProofLayers = binary.BigEndian.Uint32(p[44:])
		if m.Type == MsgHashes {
			m.Data = p[hashHeaderLen:]
		}
	default:
		// The bitfield and messages of other extensions: the payload as
		// it is.
		m.Data = p
	}
	return nil
}

// payloadLen is the length of m's payload after the type byte.
func payloadLen(m *Message) int {
	switch m.Type {
	case MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested, MsgHaveAll, MsgHaveNone:
		return 0
	case MsgHave, MsgSuggest, MsgAllowedFast:
		return 4
	case MsgRequest, MsgCancel, MsgRejectRequest:
		return 12
	case MsgPiece:
		return 8 + len(m.Data)
	case MsgPort:
		return 2
	case MsgExtended:
		return 1 + len(m.Data)
	case MsgHashRequest, MsgHashReject:
		return hashHeaderLen
	case MsgHashes:
		return hashHeaderLen + len(m.Data)
	}
	return len(m.Data)
}

// EncodedLen is the length AppendMessage appends for m.
func EncodedLen(m *Message) int {
	if m.Keepalive {
		return 4
	}
	return 5 + payloadLen(m)
}

// AppendMessage appends m, with its length prefix, to dst.
func AppendMessage(dst []byte, m *Message) []byte {
	if m.Keepalive {
		return append(dst, 0, 0, 0, 0)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(1+payloadLen(m)))
	dst = append(dst, byte(m.Type))
	switch m.Type {
	case MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested, MsgHaveAll, MsgHaveNone:
	case MsgHave, MsgSuggest, MsgAllowedFast:
		dst = binary.BigEndian.AppendUint32(dst, m.Index)
	case MsgRequest, MsgCancel, MsgRejectRequest:
		dst = binary.BigEndian.AppendUint32(dst, m.Index)
		dst = binary.BigEndian.AppendUint32(dst, m.Begin)
		dst = binary.BigEndian.AppendUint32(dst, m.Length)
	case MsgPiece:
		dst = binary.BigEndian.AppendUint32(dst, m.Index)
		dst = binary.BigEndian.AppendUint32(dst, m.Begin)
		dst = append(dst, m.Data...)
	case MsgPort:
		dst = binary.BigEndian.AppendUint16(dst, m.Port)
	case MsgExtended:
		dst = append(dst, m.ExtendedID)
		dst = append(dst, m.Data...)
	case MsgHashRequest, MsgHashReject, MsgHashes:
		dst = append(dst, m.PiecesRoot[:]...)
		dst = binary.BigEndian.AppendUint32(dst, m.BaseLayer)
		dst = binary.BigEndian.AppendUint32(dst, m.Index)
		dst = binary.BigEndian.AppendUint32(dst, m.Length)
		dst = binary.BigEndian.AppendUint32(dst, m.ProofLayers)
		if m.Type == MsgHashes {
			dst = append(dst, m.Data...)
		}
	default:
		dst = append(dst, m.Data...)
	}
	return dst
}

// AppendPieceHeader appends the header of a piece message for a block of
// n bytes at begin in piece index: the block's bytes are to follow, so
// that a sender with the block elsewhere need not copy it next to the
// header first.
func AppendPieceHeader(dst []byte, index, begin uint32, n int) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(9+n))
	dst = append(dst, byte(MsgPiece))
	dst = binary.BigEndian.AppendUint32(dst, index)
	return binary.BigEndian.AppendUint32(dst, begin)
}

// ErrParserFailed is what a Parser returns once it has failed: the stream
// is out of step and cannot be parsed further.
var ErrParserFailed = errors.New("peer: parser failed earlier")

// Parser splits a stream into messages, without I/O: Feed takes bytes as
// they arrive, any number at a time, and yields each complete message.
// The zero Parser is ready.
type Parser struct {
	// MaxLength bounds a message's length, its type byte and payload; a
	// longer one fails the stream. Zero means DefaultMaxMessageLength.
	MaxLength int

	buf    []byte
	need   int
	failed bool
}

// keepBuf is the capacity a Parser keeps between messages: enough for a
// piece message of a common block, but not a large bitfield's.
const keepBuf = 64 << 10

// Feed parses data, calling fn with each message it completes. The
// message, and the Data in it, are valid only during the call: Data
// aliases data when the message lay whole in it, and the Parser's buffer
// when it came in parts. An error from fn stops the parse and is returned;
// what remained of data is dropped, and the Parser fails from then on, as
// it does on a malformed message.
func (p *Parser) Feed(data []byte, fn func(*Message) error) error {
	if p.failed {
		return ErrParserFailed
	}
	err := p.feed(data, fn)
	if err != nil {
		p.failed = true
		p.buf = nil
	}
	return err
}

func (p *Parser) feed(data []byte, fn func(*Message) error) error {
	var m Message
	max := p.MaxLength
	if max <= 0 {
		max = DefaultMaxMessageLength
	}
	for len(data) > 0 {
		if len(p.buf) > 0 {
			if len(p.buf) < 4 {
				n := min(4-len(p.buf), len(data))
				p.buf = append(p.buf, data[:n]...)
				data = data[n:]
				if len(p.buf) < 4 {
					return nil
				}
				l := binary.BigEndian.Uint32(p.buf)
				if l > uint32(max) {
					return &MessageError{Length: int(l), Reason: "longer than " + strconv.Itoa(max)}
				}
				p.need = 4 + int(l)
				if cap(p.buf) < p.need && p.need <= keepBuf {
					p.buf = append(make([]byte, 0, p.need), p.buf...)
				}
			}
			n := min(p.need-len(p.buf), len(data))
			p.buf = append(p.buf, data[:n]...)
			data = data[n:]
			if len(p.buf) < p.need {
				return nil
			}
			if err := emit(p.buf[4:p.need], &m, fn); err != nil {
				return err
			}
			if cap(p.buf) > keepBuf {
				p.buf = nil
			} else {
				p.buf = p.buf[:0]
			}
			continue
		}
		if len(data) < 4 {
			p.buf = append(p.buf, data...)
			return nil
		}
		l := binary.BigEndian.Uint32(data)
		if l > uint32(max) {
			return &MessageError{Length: int(l), Reason: "longer than " + strconv.Itoa(max)}
		}
		if len(data) < 4+int(l) {
			p.need = 4 + int(l)
			if cap(p.buf) < min(p.need, keepBuf) {
				p.buf = make([]byte, 0, min(p.need, keepBuf))
			}
			p.buf = append(p.buf, data...)
			return nil
		}
		if err := emit(data[4:4+l], &m, fn); err != nil {
			return err
		}
		data = data[4+l:]
	}
	return nil
}

func emit(body []byte, m *Message, fn func(*Message) error) error {
	if len(body) == 0 {
		*m = Message{Keepalive: true}
	} else if err := decode(body, m); err != nil {
		return err
	}
	return fn(m)
}

// Buffered is how many bytes of an incomplete message the Parser holds.
func (p *Parser) Buffered() int { return len(p.buf) }
