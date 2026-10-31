package turn

import (
	"encoding/binary"

	"github.com/lesismal/fib/webrtc/stun"
)

// MaxFrameSize is the largest message FrameLen frames: a STUN message with
// the most attributes its 16-bit length allows.
const MaxFrameSize = stun.HeaderSize + 0xFFFF

// FrameLen returns the length of the message at the head of b, read from a
// stream, TCP or TLS, which has no datagrams to delimit them: a STUN
// message, whose header gives its length, or a ChannelData message, padded
// on a stream to a multiple of four bytes (RFC 8656 section 12.5). It
// returns 0 while b holds too little to tell, and ErrNotTURN for bytes that
// begin neither, after which the stream cannot be framed again.
func FrameLen(b []byte) (int, error) {
	if len(b) < ChannelDataHeaderSize {
		return 0, nil
	}
	n := int(binary.BigEndian.Uint16(b[2:4]))
	switch {
	case b[0] < 4:
		if n%4 != 0 {
			return 0, ErrNotTURN
		}
		if len(b) < 8 {
			return 0, nil
		}
		if binary.BigEndian.Uint32(b[4:8]) != stun.MagicCookie {
			return 0, ErrNotTURN
		}
		return stun.HeaderSize + n, nil
	case IsChannelData(b):
		return ChannelDataHeaderSize + (n+3)&^3, nil
	}
	return 0, ErrNotTURN
}

// padChannelData pads the ChannelData message b to a multiple of four
// bytes, as a stream needs.
func padChannelData(b []byte) []byte {
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	return b
}
