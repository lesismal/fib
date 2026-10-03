//go:build linux || darwin || windows

package arpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Message commands, in the low six bits of the header's cmd byte.
const (
	// CmdNone is invalid.
	CmdNone byte = 0
	// CmdRequest is a request that expects a response.
	CmdRequest byte = 1
	// CmdResponse is the response to a request.
	CmdResponse byte = 2
	// CmdNotify is a one-way message that expects no response.
	CmdNotify byte = 3
	// CmdPing is a heartbeat, which the receiver answers with CmdPong.
	CmdPing byte = 4
	// CmdPong answers CmdPing.
	CmdPong byte = 5
	// CmdStream is a message of a Stream.
	CmdStream byte = 6
)

// The header, HeadLen bytes in little endian, is followed by the method name
// and then the payload:
//
//	[0, 4)  body length: len(method) + len(payload)
//	[4]     reserved for the user's flag bits, see Message.SetFlagBit
//	[5]     cmd in the low six bits, a stream's EOF (bit 6) and local (bit 7) bits
//	[6]     flags: error, async
//	[7]     method length
//	[8, 16) sequence number
const (
	HeaderIndexBodyLenBegin = 0
	HeaderIndexBodyLenEnd   = 4
	HeaderIndexReserved     = 4
	HeaderIndexCmd          = 5
	HeaderIndexFlag         = 6
	HeaderIndexMethodLen    = 7
	HeaderIndexSeqBegin     = 8
	HeaderIndexSeqEnd       = 16

	// HeaderFlagMaskError marks an error response.
	HeaderFlagMaskError byte = 0x01
	// HeaderFlagMaskAsync marks the messages of an asynchronous call.
	HeaderFlagMaskAsync byte = 0x02

	// HeaderStreamLocalBit, in the cmd byte, marks a stream message sent by
	// the side that opened the Stream.
	HeaderStreamLocalBit byte = 0x80
	// HeaderStreamEOFBit, in the cmd byte, marks the last message a side of
	// a Stream sends.
	HeaderStreamEOFBit byte = 0x40
	// HeaderCmdBitMask is the cmd's bits of the cmd byte.
	HeaderCmdBitMask = ^(HeaderStreamLocalBit | HeaderStreamEOFBit)
)

const (
	// HeadLen is the length of a Message header.
	HeadLen int = 16
	// MaxMethodLen is the longest method name.
	MaxMethodLen int = 127
	// DefaultMaxBodyLen is the longest body, method and payload, a received
	// Message may have unless Handler.SetMaxBodyLen says otherwise.
	DefaultMaxBodyLen int = 1024*1024*64 - 16
)

// pingFrame and pongFrame are the whole of a ping and of a pong.
var (
	pingFrame = [HeadLen]byte{HeaderIndexCmd: CmdPing}
	pongFrame = [HeadLen]byte{HeaderIndexCmd: CmdPong}
)

var messagePool = sync.Pool{New: func() any { return &Message{} }}

// Message is one arpc message: the header, the method name and the payload,
// one after another in Buffer.
//
// A received Message belongs to arpc once it has been handled, and goes back
// through Handler.OnMessageDone; Handler.EnablePool makes that release its
// buffer for the next one. Retain keeps it past that, and each Retain needs a
// Release of its own: the count starts at 0 and the Message is freed when it
// drops below.
type Message struct {
	ref     atomic.Int32
	Buffer  []byte
	handler *Handler
	values  map[any]any
}

// Retain adds a reference and returns the count.
func (m *Message) Retain() int32 { return m.ref.Add(1) }

// Release drops a reference and returns the count. The one that takes it to
// -1 frees the buffer with Handler.Free and pools the Message.
func (m *Message) Release() int32 {
	n := m.ref.Add(-1)
	if n == -1 {
		if m.handler != nil {
			m.handler.Free(m.Buffer)
		}
		m.Buffer, m.handler, m.values = nil, nil, nil
		m.ref.Store(0)
		messagePool.Put(m)
	}
	return n
}

// Len returns the length of Buffer.
func (m *Message) Len() int { return len(m.Buffer) }

// Cmd returns the command, without a stream's bits.
func (m *Message) Cmd() byte { return m.Buffer[HeaderIndexCmd] & HeaderCmdBitMask }

// SetCmd sets the command, keeping a stream's bits.
func (m *Message) SetCmd(cmd byte) {
	m.Buffer[HeaderIndexCmd] = m.Buffer[HeaderIndexCmd]&^HeaderCmdBitMask | cmd&HeaderCmdBitMask
}

// IsStreamLocal reports whether the side that opened the Stream sent the
// message.
func (m *Message) IsStreamLocal() bool { return m.Buffer[HeaderIndexCmd]&HeaderStreamLocalBit != 0 }

// SetStreamLocal sets or clears the stream local bit.
func (m *Message) SetStreamLocal(local bool) { m.setBit(HeaderIndexCmd, HeaderStreamLocalBit, local) }

// IsStreamEOF reports whether this is the last message its sender sends on the
// Stream.
func (m *Message) IsStreamEOF() bool { return m.Buffer[HeaderIndexCmd]&HeaderStreamEOFBit != 0 }

// SetStreamEOF sets or clears the stream EOF bit.
func (m *Message) SetStreamEOF(eof bool) { m.setBit(HeaderIndexCmd, HeaderStreamEOFBit, eof) }

// IsError reports whether the message is an error response.
func (m *Message) IsError() bool { return m.Buffer[HeaderIndexFlag]&HeaderFlagMaskError != 0 }

// SetError sets or clears the error flag.
func (m *Message) SetError(isError bool) { m.setBit(HeaderIndexFlag, HeaderFlagMaskError, isError) }

// Error returns the payload as an error if the message is an error response,
// and nil otherwise.
func (m *Message) Error() error {
	if !m.IsError() {
		return nil
	}
	return errors.New(string(m.Data()))
}

// IsAsync reports whether the message belongs to an asynchronous call.
func (m *Message) IsAsync() bool { return m.Buffer[HeaderIndexFlag]&HeaderFlagMaskAsync != 0 }

// SetAsync sets or clears the async flag.
func (m *Message) SetAsync(isAsync bool) { m.setBit(HeaderIndexFlag, HeaderFlagMaskAsync, isAsync) }

func (m *Message) setBit(index int, mask byte, on bool) {
	if on {
		m.Buffer[index] |= mask
	} else {
		m.Buffer[index] &^= mask
	}
}

// SetFlagBit sets or clears bit index, 0 to 7, of the reserved byte, which is
// the user's. Any other index gets ErrInvalidFlagBitIndex.
func (m *Message) SetFlagBit(index int, value bool) error {
	if index < 0 || index > 7 {
		return ErrInvalidFlagBitIndex
	}
	m.setBit(HeaderIndexReserved, 1<<index, value)
	return nil
}

// IsFlagBitSet reports whether bit index of the reserved byte is set, and
// false for an index outside 0 to 7.
func (m *Message) IsFlagBitSet(index int) bool {
	if index < 0 || index > 7 {
		return false
	}
	return m.Buffer[HeaderIndexReserved]&(1<<index) != 0
}

// MethodLen returns the length of the method name.
func (m *Message) MethodLen() int { return int(m.Buffer[HeaderIndexMethodLen]) }

// SetMethodLen sets the length of the method name.
func (m *Message) SetMethodLen(l int) { m.Buffer[HeaderIndexMethodLen] = byte(l) }

// Method returns the method name.
func (m *Message) Method() string { return string(m.Buffer[HeadLen : HeadLen+m.MethodLen()]) }

// method is Method without a copy, valid until the Message is released.
func (m *Message) method() string {
	b := m.Buffer[HeadLen : HeadLen+m.MethodLen()]
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// BodyLen returns the body length the header holds.
func (m *Message) BodyLen() int {
	return int(binary.LittleEndian.Uint32(m.Buffer[HeaderIndexBodyLenBegin:HeaderIndexBodyLenEnd]))
}

// SetBodyLen sets the body length in the header.
func (m *Message) SetBodyLen(l int) {
	binary.LittleEndian.PutUint32(m.Buffer[HeaderIndexBodyLenBegin:HeaderIndexBodyLenEnd], uint32(l))
}

// Seq returns the sequence number.
func (m *Message) Seq() uint64 {
	return binary.LittleEndian.Uint64(m.Buffer[HeaderIndexSeqBegin:HeaderIndexSeqEnd])
}

// SetSeq sets the sequence number.
func (m *Message) SetSeq(seq uint64) {
	binary.LittleEndian.PutUint64(m.Buffer[HeaderIndexSeqBegin:HeaderIndexSeqEnd], seq)
}

// Data returns the payload, which is part of Buffer.
func (m *Message) Data() []byte { return m.Buffer[HeadLen+m.MethodLen():] }

// Values returns the values attached to the Message. They stay on this side
// and are never sent.
func (m *Message) Values() map[any]any { return m.values }

// Get returns the value stored for key.
func (m *Message) Get(key any) (any, bool) {
	value, ok := m.values[key]
	return value, ok
}

// Set stores value for key. It does nothing if either is nil.
func (m *Message) Set(key, value any) {
	if key == nil || value == nil {
		return
	}
	if m.values == nil {
		m.values = map[any]any{}
	}
	m.values[key] = value
}

// header is the parameters of the header of a message being built.
type header struct {
	cmd    byte
	flags  byte
	method string
	seq    uint64
}

// put writes the header and method of a message whose payload is dataLen
// bytes to b, which has HeadLen+len(h.method) bytes.
func (h *header) put(b []byte, dataLen int) {
	binary.LittleEndian.PutUint32(b[HeaderIndexBodyLenBegin:], uint32(len(h.method)+dataLen))
	b[HeaderIndexReserved] = 0
	b[HeaderIndexCmd] = h.cmd
	b[HeaderIndexFlag] = h.flags
	b[HeaderIndexMethodLen] = byte(len(h.method))
	binary.LittleEndian.PutUint64(b[HeaderIndexSeqBegin:], h.seq)
	copy(b[HeadLen:], h.method)
}

// newMessage builds a whole Message, its buffer from handler's Malloc.
func newMessage(handler *Handler, h header, data []byte, values map[any]any) *Message {
	msg := messagePool.Get().(*Message)
	msg.handler = handler
	msg.values = values
	msg.Buffer = handler.Malloc(HeadLen + len(h.method) + len(data))
	h.put(msg.Buffer, len(data))
	copy(msg.Buffer[HeadLen+len(h.method):], data)
	return msg
}

// NewMessage builds a Message with v as its payload, encoded as Client.Call
// encodes a request, from the buffers of handler, or of DefaultHandler if it
// is nil. A nil codec means DefaultCodec.
func NewMessage(cmd byte, method string, v any, seq uint64, handler *Handler, codec Codec, values map[any]any) *Message {
	if handler == nil {
		handler = DefaultHandler
	}
	data, err := valueToBytes(codec, v)
	if err != nil {
		logger().Error("arpc: encoding a message", "method", method, "error", err)
	}
	return newMessage(handler, header{cmd: cmd, method: method, seq: seq}, data, values)
}

// checkMethod reports a method name that is empty or too long.
func checkMethod(method string) error {
	if len(method) == 0 || len(method) > MaxMethodLen {
		return fmt.Errorf("arpc: invalid method length %d, should be 1 to %d", len(method), MaxMethodLen)
	}
	return nil
}

// MessageCoder transforms messages on the wire, to compress or encrypt them,
// say. Encode runs on what is sent, in the order the coders were added, and
// Decode on what is received, in the opposite order. A coder may change the
// Message it is given or return another.
type MessageCoder interface {
	Encode(*Client, *Message) *Message
	Decode(*Client, *Message) *Message
}
