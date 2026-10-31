package rtp

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrRTCPLength  = errors.New("rtp: RTCP length does not match the packet")
	ErrRTCPType    = errors.New("rtp: unexpected RTCP packet type")
	ErrRTCPTooMany = errors.New("rtp: too many items for one RTCP packet")
	ErrRTCPItem    = errors.New("rtp: RTCP item too long")
	ErrRTCPFormat  = errors.New("rtp: malformed RTCP packet")
)

// The RTCP packet types of RFC 3550 and RFC 4585.
const (
	TypeSR    uint8 = 200
	TypeRR    uint8 = 201
	TypeSDES  uint8 = 202
	TypeBYE   uint8 = 203
	TypeAPP   uint8 = 204
	TypeRTPFB uint8 = 205
	TypePSFB  uint8 = 206
)

// The feedback message types RTPFB and PSFB packets carry in their count
// field.
const (
	FormatNACK uint8 = 1  // RTPFB, RFC 4585
	FormatTCC  uint8 = 15 // RTPFB, transport-wide congestion control
	FormatPLI  uint8 = 1  // PSFB, RFC 4585
	FormatFIR  uint8 = 4  // PSFB, RFC 5104
	FormatAFB  uint8 = 15 // PSFB, application layer feedback such as REMB
)

// RTCPHeaderSize is the size of the header every RTCP packet starts with.
const RTCPHeaderSize = 4

// RTCPHeader is the 4-byte header of an RTCP packet.
type RTCPHeader struct {
	Padding bool
	// Count is the 5-bit field after the P bit: the report or source count,
	// or the feedback message type.
	Count uint8
	Type  uint8
	// Length is the packet's length in 32-bit words, minus one.
	Length uint16
}

// Unmarshal parses the RTCP header at the start of b.
func (h *RTCPHeader) Unmarshal(b []byte) error {
	if len(b) < RTCPHeaderSize {
		return ErrShortPacket
	}
	if b[0]>>6 != 2 {
		return ErrVersion
	}
	h.Padding = b[0]&0x20 != 0
	h.Count = b[0] & 0x1f
	h.Type = b[1]
	h.Length = binary.BigEndian.Uint16(b[2:])
	return nil
}

// Append appends the header to dst.
func (h RTCPHeader) Append(dst []byte) []byte {
	b0 := byte(2<<6) | h.Count&0x1f
	if h.Padding {
		b0 |= 0x20
	}
	dst = append(dst, b0, h.Type)
	return binary.BigEndian.AppendUint16(dst, h.Length)
}

// IsRTCP tells whether a packet multiplexed with RTP on one port is RTCP,
// by the rule of RFC 5761 section 4: its second byte, read as RTP's marker
// and payload type, falls in 192 to 223.
func IsRTCP(b []byte) bool {
	return len(b) >= 2 && b[1] >= 192 && b[1] <= 223
}

// NextRTCP splits the first packet off the compound RTCP packet b: its
// header, the packet itself and the packets after it. It does not
// allocate; the caller decodes pkt by its type into a value it reuses.
func NextRTCP(b []byte) (h RTCPHeader, pkt, rest []byte, err error) {
	if err = h.Unmarshal(b); err != nil {
		return h, nil, nil, err
	}
	n := 4 * (int(h.Length) + 1)
	if len(b) < n {
		return h, nil, nil, ErrRTCPLength
	}
	return h, b[:n:n], b[n:], nil
}

// rtcpBody checks that b is exactly one RTCP packet of the type and
// returns its header and its body, the bytes after the header with any
// padding taken off.
func rtcpBody(b []byte, typ uint8) (RTCPHeader, []byte, error) {
	var h RTCPHeader
	if err := h.Unmarshal(b); err != nil {
		return h, nil, err
	}
	if h.Type != typ {
		return h, nil, ErrRTCPType
	}
	if 4*(int(h.Length)+1) != len(b) {
		return h, nil, ErrRTCPLength
	}
	body := b[RTCPHeaderSize:]
	if h.Padding {
		if len(body) == 0 {
			return h, nil, ErrPadding
		}
		pad := int(body[len(body)-1])
		if pad == 0 || pad > len(body) {
			return h, nil, ErrPadding
		}
		body = body[:len(body)-pad]
	}
	return h, body, nil
}

// appendRTCPHeader appends the header of a packet whose body, padded to a
// multiple of 4 bytes, is size bytes long.
func appendRTCPHeader(dst []byte, count, typ uint8, size int) []byte {
	return RTCPHeader{Count: count, Type: typ, Length: uint16(size / 4)}.Append(dst)
}

// RTCP is a packet a compound RTCP packet holds. Each Unmarshal takes
// exactly one packet, as NextRTCP splits it off, and refers to it rather
// than copying from it.
type RTCP interface {
	Unmarshal(b []byte) error
	MarshalSize() int
	Append(dst []byte) ([]byte, error)
}

// UnmarshalRTCP parses every packet of a compound RTCP packet into a new
// value of its type: one of this package's types, or RawRTCP for a type it
// does not know. The values refer to b.
func UnmarshalRTCP(b []byte) ([]RTCP, error) {
	var pkts []RTCP
	for len(b) > 0 {
		h, pkt, rest, err := NextRTCP(b)
		if err != nil {
			return pkts, err
		}
		p := newRTCP(h, pkt)
		if err := p.Unmarshal(pkt); err != nil {
			return pkts, err
		}
		pkts = append(pkts, p)
		b = rest
	}
	return pkts, nil
}

func newRTCP(h RTCPHeader, pkt []byte) RTCP {
	switch h.Type {
	case TypeSR:
		return new(SenderReport)
	case TypeRR:
		return new(ReceiverReport)
	case TypeSDES:
		return new(SourceDescription)
	case TypeBYE:
		return new(Goodbye)
	case TypeAPP:
		return new(ApplicationDefined)
	case TypeRTPFB:
		switch h.Count {
		case FormatNACK:
			return new(NACK)
		case FormatTCC:
			return new(TransportCC)
		}
	case TypePSFB:
		switch h.Count {
		case FormatPLI:
			return new(PLI)
		case FormatFIR:
			return new(FIR)
		case FormatAFB:
			if len(pkt) >= 16 && string(pkt[12:16]) == "REMB" {
				return new(REMB)
			}
		}
	}
	return new(RawRTCP)
}

// AppendRTCP appends the packets to dst as one compound packet.
func AppendRTCP(dst []byte, pkts ...RTCP) ([]byte, error) {
	for _, p := range pkts {
		var err error
		if dst, err = p.Append(dst); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// RawRTCP is an RTCP packet kept as it is, for types the package does not
// decode.
type RawRTCP struct {
	Header RTCPHeader
	// Body is the packet after its header, padding included.
	Body []byte
}

func (p *RawRTCP) Unmarshal(b []byte) error {
	if err := p.Header.Unmarshal(b); err != nil {
		return err
	}
	if 4*(int(p.Header.Length)+1) != len(b) {
		return ErrRTCPLength
	}
	p.Body = b[RTCPHeaderSize:]
	return nil
}

func (p *RawRTCP) MarshalSize() int { return RTCPHeaderSize + len(p.Body) }

// Append appends the packet, with its length taken from Body.
func (p *RawRTCP) Append(dst []byte) ([]byte, error) {
	if len(p.Body)%4 != 0 || len(p.Body)/4 > 0xffff {
		return dst, ErrRTCPLength
	}
	h := p.Header
	h.Length = uint16(len(p.Body) / 4)
	dst = h.Append(dst)
	return append(dst, p.Body...), nil
}

// ntpEpochOffset is the number of seconds from the NTP epoch, 1900, to the
// Unix epoch.
const ntpEpochOffset = 2208988800

// NTPTime returns t as a 64-bit NTP timestamp, seconds since 1900 in the
// high 32 bits and their fraction in the low.
func NTPTime(t time.Time) uint64 {
	sec := uint64(t.Unix() + ntpEpochOffset)
	frac := uint64(t.Nanosecond()) << 32 / 1e9
	return sec<<32 | frac
}

// TimeFromNTP returns the time of a 64-bit NTP timestamp.
func TimeFromNTP(ntp uint64) time.Time {
	sec := int64(ntp>>32) - ntpEpochOffset
	nsec := (ntp & 0xffffffff) * 1e9 >> 32
	return time.Unix(sec, int64(nsec))
}
