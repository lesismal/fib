package turn

import (
	"encoding/binary"
	"errors"
)

// Channel numbers a client may bind, RFC 8656 section 12: 0x4000 through
// 0x4FFF. The first byte of a ChannelData message is thus 0x40 to 0x4F,
// which is how RFC 7983 tells it from STUN, DTLS and RTP on one socket.
const (
	MinChannel = 0x4000
	MaxChannel = 0x4FFF
)

// ChannelDataHeaderSize is the size of a ChannelData message's header: the
// channel number and the length of the data.
const ChannelDataHeaderSize = 4

var (
	ErrNotChannelData = errors.New("turn: not a ChannelData message")
	ErrTruncated      = errors.New("turn: truncated ChannelData message")
)

// IsChannelData reports whether b looks like a ChannelData message by its
// first byte, the test RFC 7983 demultiplexes with, and is long enough to
// hold the header.
func IsChannelData(b []byte) bool {
	return len(b) >= ChannelDataHeaderSize && b[0] >= 0x40 && b[0] <= 0x4F
}

// AppendChannelData appends a ChannelData message carrying data on channel
// to dst, unpadded, as it goes over UDP; over TCP or TLS it is padded to a
// multiple of four bytes (RFC 8656 section 12.5), which the client and
// server do themselves when their transport is a stream.
func AppendChannelData(dst []byte, channel uint16, data []byte) []byte {
	dst = binary.BigEndian.AppendUint16(dst, channel)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(data)))
	return append(dst, data...)
}

// ParseChannelData returns the channel number and data of the ChannelData
// message b; data aliases b. Bytes past the length the header gives, the
// padding a sender over TCP adds, are ignored. A data length over 65535 cannot
// be expressed, so a payload that large never parses.
func ParseChannelData(b []byte) (channel uint16, data []byte, err error) {
	if len(b) < ChannelDataHeaderSize {
		return 0, nil, ErrTruncated
	}
	channel = binary.BigEndian.Uint16(b)
	if channel < MinChannel || channel > MaxChannel {
		return 0, nil, ErrNotChannelData
	}
	n := int(binary.BigEndian.Uint16(b[2:]))
	if len(b)-ChannelDataHeaderSize < n {
		return 0, nil, ErrTruncated
	}
	return channel, b[ChannelDataHeaderSize : ChannelDataHeaderSize+n], nil
}
