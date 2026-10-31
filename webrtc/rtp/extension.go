package rtp

import (
	"encoding/binary"
	"time"
)

// The URIs SDP's a=extmap names the header extensions WebRTC uses by.
const (
	ExtensionURIMid              = "urn:ietf:params:rtp-hdrext:sdes:mid"
	ExtensionURIRid              = "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id"
	ExtensionURIRepairedRid      = "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id"
	ExtensionURIAbsSendTime      = "http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time"
	ExtensionURITransportCC      = "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
	ExtensionURIAudioLevel       = "urn:ietf:params:rtp-hdrext:ssrc-audio-level"
	ExtensionURITransmissionTime = "urn:ietf:params:rtp-hdrext:toffset"
)

// The MID and RID extensions carry their identifiers as bare text, so
// GetExtension's payload is the value and SetExtension takes it as it is.

// AbsSendTime returns t as the abs-send-time extension carries it: the
// NTP time in seconds, as a 6.18 fixed point number, modulo 64 seconds.
func AbsSendTime(t time.Time) uint32 {
	return uint32(NTPTime(t)>>14) & 0xffffff
}

// AppendAbsSendTime appends the 3-byte abs-send-time payload of v, the low
// 24 bits of which count.
func AppendAbsSendTime(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>16), byte(v>>8), byte(v))
}

// ParseAbsSendTime reads an abs-send-time payload.
func ParseAbsSendTime(b []byte) (uint32, error) {
	if len(b) < 3 {
		return 0, ErrShortPacket
	}
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]), nil
}

// AppendTransportCCSeq appends the 2-byte payload of the transport-wide
// sequence number extension.
func AppendTransportCCSeq(dst []byte, seq uint16) []byte {
	return binary.BigEndian.AppendUint16(dst, seq)
}

// ParseTransportCCSeq reads a transport-wide sequence number payload.
func ParseTransportCCSeq(b []byte) (uint16, error) {
	if len(b) < 2 {
		return 0, ErrShortPacket
	}
	return binary.BigEndian.Uint16(b), nil
}

// AppendAudioLevel appends the 1-byte payload of RFC 6464's audio level:
// whether the packet holds voice, and the level in -dBov, 0 to 127.
func AppendAudioLevel(dst []byte, voice bool, level uint8) []byte {
	b := level & 0x7f
	if voice {
		b |= 0x80
	}
	return append(dst, b)
}

// ParseAudioLevel reads an RFC 6464 audio level payload.
func ParseAudioLevel(b []byte) (voice bool, level uint8, err error) {
	if len(b) < 1 {
		return false, 0, ErrShortPacket
	}
	return b[0]&0x80 != 0, b[0] & 0x7f, nil
}

// SeqUnwrapper extends 16-bit RTP sequence numbers to 64 bits, taking each
// to be the one nearest the last it saw. Numbers before the first may come
// out negative.
type SeqUnwrapper struct {
	last    int64
	started bool
}

// Unwrap returns seq extended to 64 bits.
func (u *SeqUnwrapper) Unwrap(seq uint16) int64 {
	if !u.started {
		u.started = true
		u.last = int64(seq)
		return u.last
	}
	u.last += int64(int16(seq - uint16(u.last)))
	return u.last
}

// Reset forgets the numbers seen, so the next is taken as it is.
func (u *SeqUnwrapper) Reset() { *u = SeqUnwrapper{} }
