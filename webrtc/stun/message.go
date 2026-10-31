package stun

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

// MagicCookie is the fixed value in bytes 4 to 8 of every STUN message, which
// tells STUN apart from the other protocols sharing a port with it and keys
// the XOR of XOR-MAPPED-ADDRESS.
const MagicCookie = 0x2112A442

const (
	// HeaderSize is the size of the fixed header every message starts with.
	HeaderSize = 20
	// TransactionIDSize is the size of the transaction ID.
	TransactionIDSize = 12
	// attrHeaderSize is the size of an attribute's type and length.
	attrHeaderSize = 4
)

var (
	ErrNotSTUN          = errors.New("stun: not a STUN message")
	ErrTruncated        = errors.New("stun: truncated message")
	ErrBadLength        = errors.New("stun: message length is not a multiple of 4")
	ErrAttrNotFound     = errors.New("stun: attribute not found")
	ErrAttrSize         = errors.New("stun: attribute has the wrong size")
	ErrBadFamily        = errors.New("stun: unknown address family")
	ErrIntegrityMissing = errors.New("stun: no MESSAGE-INTEGRITY")
	ErrIntegrity        = errors.New("stun: MESSAGE-INTEGRITY does not match")
	ErrFingerprint      = errors.New("stun: FINGERPRINT does not match")
	ErrFingerprintLast  = errors.New("stun: FINGERPRINT is not the last attribute")
)

// Method is a STUN method, the 12-bit operation a message is part of.
type Method uint16

// The methods of RFC 8489 (Binding) and of TURN, RFC 8656 and RFC 6062.
const (
	MethodBinding           Method = 0x001
	MethodAllocate          Method = 0x003
	MethodRefresh           Method = 0x004
	MethodSend              Method = 0x006
	MethodData              Method = 0x007
	MethodCreatePermission  Method = 0x008
	MethodChannelBind       Method = 0x009
	MethodConnect           Method = 0x00A
	MethodConnectionBind    Method = 0x00B
	MethodConnectionAttempt Method = 0x00C
)

func (m Method) String() string {
	switch m {
	case MethodBinding:
		return "Binding"
	case MethodAllocate:
		return "Allocate"
	case MethodRefresh:
		return "Refresh"
	case MethodSend:
		return "Send"
	case MethodData:
		return "Data"
	case MethodCreatePermission:
		return "CreatePermission"
	case MethodChannelBind:
		return "ChannelBind"
	case MethodConnect:
		return "Connect"
	case MethodConnectionBind:
		return "ConnectionBind"
	case MethodConnectionAttempt:
		return "ConnectionAttempt"
	}
	return fmt.Sprintf("Method(0x%03x)", uint16(m))
}

// Class says whether a message is a request, an indication, or one of the
// two kinds of response.
type Class uint8

const (
	ClassRequest         Class = 0
	ClassIndication      Class = 1
	ClassSuccessResponse Class = 2
	ClassErrorResponse   Class = 3
)

func (c Class) String() string {
	switch c {
	case ClassRequest:
		return "request"
	case ClassIndication:
		return "indication"
	case ClassSuccessResponse:
		return "success response"
	case ClassErrorResponse:
		return "error response"
	}
	return fmt.Sprintf("Class(%d)", uint8(c))
}

// MessageType is a message's method and class, which the header interleaves
// into one 14-bit field.
type MessageType struct {
	Method Method
	Class  Class
}

// The message types ICE uses.
var (
	BindingRequest    = MessageType{MethodBinding, ClassRequest}
	BindingSuccess    = MessageType{MethodBinding, ClassSuccessResponse}
	BindingError      = MessageType{MethodBinding, ClassErrorResponse}
	BindingIndication = MessageType{MethodBinding, ClassIndication}
)

// Value is the type as the header carries it: the class bits C1 and C0 sit
// at bits 8 and 4, between the bits of the method (RFC 8489 section 5).
func (t MessageType) Value() uint16 {
	m := uint16(t.Method)
	c := uint16(t.Class)
	return m&0x000F | (m&0x0070)<<1 | (m&0x0F80)<<2 | (c&1)<<4 | (c&2)<<7
}

// ParseMessageType is the inverse of Value.
func ParseMessageType(v uint16) MessageType {
	m := v&0x000F | (v&0x00E0)>>1 | (v&0x3E00)>>2
	c := (v>>4)&1 | (v>>7)&2
	return MessageType{Method(m), Class(c)}
}

func (t MessageType) String() string { return t.Method.String() + " " + t.Class.String() }

// IsMessage reports whether b looks like a STUN message, by the test RFC 7983
// demultiplexes with: a first byte of 0 to 3, which is what the two zero bits
// that start every STUN message leave, and the magic cookie. It is what tells
// STUN from DTLS, RTP and TURN channel data arriving on one ICE socket.
func IsMessage(b []byte) bool {
	return len(b) >= HeaderSize && b[0] < 4 && binary.BigEndian.Uint32(b[4:8]) == MagicCookie
}

// NewTransactionID returns a random transaction ID, as RFC 8489 requires of
// every new transaction: one an attacker cannot guess lets a client tell a
// forged response from its own.
func NewTransactionID() (id [TransactionIDSize]byte) {
	if _, err := rand.Read(id[:]); err != nil {
		panic("stun: crypto/rand failed: " + err.Error())
	}
	return id
}

// attr is where one attribute's value lies in Raw.
type attr struct {
	typ AttrType
	off int
	n   int
}

// Message is one STUN message, decoded from or encoded into Raw.
//
// After Decode, Raw is the buffer passed to it, cut to the message, and the
// []byte values the getters return alias it: they are valid until that
// buffer is reused. To build a message, call Build and then the Add methods,
// which append to Raw as they go; Raw is then the message to send.
type Message struct {
	Type          MessageType
	TransactionID [TransactionIDSize]byte
	// Raw is the encoded message.
	Raw []byte

	attrs []attr
}

// Reset empties m while keeping the memory it holds for the next message.
func (m *Message) Reset() {
	m.Type = MessageType{}
	m.TransactionID = [TransactionIDSize]byte{}
	m.Raw = m.Raw[:0]
	m.attrs = m.attrs[:0]
}

// Build starts a new message of type t in m with no attributes, writing its
// header into Raw.
func (m *Message) Build(t MessageType, id [TransactionIDSize]byte) {
	m.Reset()
	m.Type = t
	m.TransactionID = id
	m.Raw = binary.BigEndian.AppendUint16(m.Raw, t.Value())
	m.Raw = append(m.Raw, 0, 0)
	m.Raw = binary.BigEndian.AppendUint32(m.Raw, MagicCookie)
	m.Raw = append(m.Raw, id[:]...)
}

// Append appends the encoded message to dst.
func (m *Message) Append(dst []byte) []byte { return append(dst, m.Raw...) }

// Decode parses the message at the start of b into m. b may run past the
// message, as a stream does; the message's own length says where it ends,
// and Raw is cut there.
//
// Attributes that follow MESSAGE-INTEGRITY are dropped, except for
// MESSAGE-INTEGRITY-SHA256 and FINGERPRINT, since RFC 8489 section 14.5 has
// a receiver ignore them: they are not covered by the integrity check, so an
// attacker could have added them. Nothing after FINGERPRINT is kept either.
func (m *Message) Decode(b []byte) error {
	m.Reset()
	if len(b) < HeaderSize {
		return ErrTruncated
	}
	if b[0]&0xC0 != 0 || binary.BigEndian.Uint32(b[4:8]) != MagicCookie {
		return ErrNotSTUN
	}
	n := int(binary.BigEndian.Uint16(b[2:4]))
	if n%4 != 0 {
		return ErrBadLength
	}
	if len(b) < HeaderSize+n {
		return ErrTruncated
	}
	// The capacity is cut too, so that an Add on a decoded message copies
	// rather than writing over whatever follows the message in b.
	m.Raw = b[: HeaderSize+n : HeaderSize+n]
	m.Type = ParseMessageType(binary.BigEndian.Uint16(b[0:2]))
	copy(m.TransactionID[:], b[8:HeaderSize])
	integrity := false
	for off := HeaderSize; off < len(m.Raw); {
		if len(m.Raw)-off < attrHeaderSize {
			return ErrTruncated
		}
		t := AttrType(binary.BigEndian.Uint16(m.Raw[off:]))
		l := int(binary.BigEndian.Uint16(m.Raw[off+2:]))
		v := off + attrHeaderSize
		if len(m.Raw)-v < l {
			return ErrTruncated
		}
		off = v + pad4(l)
		if off > len(m.Raw) {
			return ErrTruncated
		}
		switch {
		case t == AttrFingerprint:
			m.attrs = append(m.attrs, attr{t, v, l})
			return nil
		case t == AttrMessageIntegrity || t == AttrMessageIntegritySHA256:
			integrity = true
		case integrity:
			continue
		}
		m.attrs = append(m.attrs, attr{t, v, l})
	}
	return nil
}

func pad4(n int) int { return (n + 3) &^ 3 }

// Len is the number of attributes m holds.
func (m *Message) Len() int { return len(m.attrs) }

// Attr returns the type and value of the i-th attribute, in the order the
// message carries them.
func (m *Message) Attr(i int) (AttrType, []byte) {
	a := m.attrs[i]
	return a.typ, m.Raw[a.off : a.off+a.n]
}

// Get returns the value of the first attribute of type t.
func (m *Message) Get(t AttrType) ([]byte, bool) {
	for _, a := range m.attrs {
		if a.typ == t {
			return m.Raw[a.off : a.off+a.n], true
		}
	}
	return nil, false
}

// Contains reports whether m has an attribute of type t.
func (m *Message) Contains(t AttrType) bool {
	_, ok := m.Get(t)
	return ok
}

// index is the position in m.attrs of the first attribute of type t, or -1.
func (m *Message) index(t AttrType) int {
	for i, a := range m.attrs {
		if a.typ == t {
			return i
		}
	}
	return -1
}

// Add appends an attribute of type t with value v, padded to a multiple of
// four bytes with zeros, and updates the length in the header.
func (m *Message) Add(t AttrType, v []byte) {
	m.Raw = m.grow(t, len(v))
	copy(m.Raw[len(m.Raw)-pad4(len(v)):], v)
}

// grow appends the header of an attribute of type t with an n-byte value and
// room for the value and its padding, zeroed, records it, and fixes the
// message length. The value is written into the end of the returned slice.
func (m *Message) grow(t AttrType, n int) []byte {
	if len(m.Raw) < HeaderSize {
		m.Build(m.Type, m.TransactionID)
	}
	raw := binary.BigEndian.AppendUint16(m.Raw, uint16(t))
	raw = binary.BigEndian.AppendUint16(raw, uint16(n))
	off := len(raw)
	for range pad4(n) {
		raw = append(raw, 0)
	}
	m.attrs = append(m.attrs, attr{t, off, n})
	binary.BigEndian.PutUint16(raw[2:4], uint16(len(raw)-HeaderSize))
	return raw
}
