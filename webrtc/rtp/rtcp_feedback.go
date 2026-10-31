package rtp

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// feedbackBody checks that b is one feedback packet of the type and format
// and returns its sender and media SSRCs and its feedback control
// information.
func feedbackBody(b []byte, typ, format uint8) (sender, media uint32, fci []byte, err error) {
	h, body, err := rtcpBody(b, typ)
	if err != nil {
		return 0, 0, nil, err
	}
	if h.Count != format {
		return 0, 0, nil, ErrRTCPType
	}
	if len(body) < 8 {
		return 0, 0, nil, ErrRTCPFormat
	}
	return binary.BigEndian.Uint32(body), binary.BigEndian.Uint32(body[4:]), body[8:], nil
}

func appendFeedbackHeader(dst []byte, typ, format uint8, sender, media uint32, fciSize int) []byte {
	dst = appendRTCPHeader(dst, format, typ, 8+fciSize)
	dst = binary.BigEndian.AppendUint32(dst, sender)
	return binary.BigEndian.AppendUint32(dst, media)
}

// NACKPair is one FCI entry of a generic NACK: a lost packet and a bitmask
// of which of the 16 after it were lost too.
type NACKPair struct {
	PacketID    uint16
	LostPackets uint16
}

// All yields the sequence number of every packet the pair reports lost.
func (p NACKPair) All(yield func(uint16) bool) {
	if !yield(p.PacketID) {
		return
	}
	for m := p.LostPackets; m != 0; m &= m - 1 {
		if !yield(p.PacketID + 1 + uint16(bits.TrailingZeros16(m))) {
			return
		}
	}
}

// AppendNACKPairs appends to dst the pairs that report the sequence
// numbers lost, which must be in ascending order, modulo 2^16.
func AppendNACKPairs(dst []NACKPair, lost []uint16) []NACKPair {
	for i := 0; i < len(lost); {
		p := NACKPair{PacketID: lost[i]}
		i++
		for i < len(lost) {
			d := lost[i] - p.PacketID
			if d == 0 {
				i++
				continue
			}
			if d > 16 {
				break
			}
			p.LostPackets |= 1 << (d - 1)
			i++
		}
		dst = append(dst, p)
	}
	return dst
}

// NACK is a generic NACK (RFC 4585 section 6.2.1), the RTPFB a receiver
// asks for packets again with.
type NACK struct {
	SenderSSRC uint32
	MediaSSRC  uint32
	// Pairs is reused from one Unmarshal to the next.
	Pairs []NACKPair
}

func (p *NACK) Unmarshal(b []byte) error {
	sender, media, fci, err := feedbackBody(b, TypeRTPFB, FormatNACK)
	if err != nil {
		return err
	}
	if len(fci)%4 != 0 {
		return ErrRTCPFormat
	}
	p.SenderSSRC, p.MediaSSRC = sender, media
	p.Pairs = p.Pairs[:0]
	for i := 0; i < len(fci); i += 4 {
		p.Pairs = append(p.Pairs, NACKPair{
			PacketID:    binary.BigEndian.Uint16(fci[i:]),
			LostPackets: binary.BigEndian.Uint16(fci[i+2:]),
		})
	}
	return nil
}

func (p *NACK) MarshalSize() int { return RTCPHeaderSize + 8 + 4*len(p.Pairs) }

func (p *NACK) Append(dst []byte) ([]byte, error) {
	if (8+4*len(p.Pairs))/4 > 0xffff {
		return dst, ErrRTCPTooMany
	}
	dst = appendFeedbackHeader(dst, TypeRTPFB, FormatNACK, p.SenderSSRC, p.MediaSSRC, 4*len(p.Pairs))
	for _, pair := range p.Pairs {
		dst = binary.BigEndian.AppendUint16(dst, pair.PacketID)
		dst = binary.BigEndian.AppendUint16(dst, pair.LostPackets)
	}
	return dst, nil
}

// PLI is a picture loss indication (RFC 4585 section 6.3.1), the PSFB a
// receiver asks for a key frame with.
type PLI struct {
	SenderSSRC uint32
	MediaSSRC  uint32
}

func (p *PLI) Unmarshal(b []byte) error {
	sender, media, _, err := feedbackBody(b, TypePSFB, FormatPLI)
	if err != nil {
		return err
	}
	p.SenderSSRC, p.MediaSSRC = sender, media
	return nil
}

func (p *PLI) MarshalSize() int { return RTCPHeaderSize + 8 }

func (p *PLI) Append(dst []byte) ([]byte, error) {
	return appendFeedbackHeader(dst, TypePSFB, FormatPLI, p.SenderSSRC, p.MediaSSRC, 0), nil
}

// FIREntry is one FCI entry of a FIR: the source asked for a key frame and
// the command's sequence number.
type FIREntry struct {
	SSRC           uint32
	SequenceNumber uint8
}

// FIR is a full intra request (RFC 5104 section 4.3.1). Its media SSRC is
// zero; the entries name the sources.
type FIR struct {
	SenderSSRC uint32
	MediaSSRC  uint32
	// Entries is reused from one Unmarshal to the next.
	Entries []FIREntry
}

func (p *FIR) Unmarshal(b []byte) error {
	sender, media, fci, err := feedbackBody(b, TypePSFB, FormatFIR)
	if err != nil {
		return err
	}
	if len(fci)%8 != 0 {
		return ErrRTCPFormat
	}
	p.SenderSSRC, p.MediaSSRC = sender, media
	p.Entries = p.Entries[:0]
	for i := 0; i < len(fci); i += 8 {
		p.Entries = append(p.Entries, FIREntry{SSRC: binary.BigEndian.Uint32(fci[i:]), SequenceNumber: fci[i+4]})
	}
	return nil
}

func (p *FIR) MarshalSize() int { return RTCPHeaderSize + 8 + 8*len(p.Entries) }

func (p *FIR) Append(dst []byte) ([]byte, error) {
	if (8+8*len(p.Entries))/4 > 0xffff {
		return dst, ErrRTCPTooMany
	}
	dst = appendFeedbackHeader(dst, TypePSFB, FormatFIR, p.SenderSSRC, p.MediaSSRC, 8*len(p.Entries))
	for _, e := range p.Entries {
		dst = binary.BigEndian.AppendUint32(dst, e.SSRC)
		dst = append(dst, e.SequenceNumber, 0, 0, 0)
	}
	return dst, nil
}

// REMB is a receiver estimated maximum bitrate message
// (draft-alvestrand-rmcat-remb-03), an application layer PSFB.
type REMB struct {
	SenderSSRC uint32
	MediaSSRC  uint32
	// Bitrate is in bits per second. The message carries it as an 18-bit
	// mantissa and a 6-bit exponent, so Append rounds it down to 18
	// significant bits.
	Bitrate uint64
	// SSRCs is reused from one Unmarshal to the next.
	SSRCs []uint32
}

func (p *REMB) Unmarshal(b []byte) error {
	sender, media, fci, err := feedbackBody(b, TypePSFB, FormatAFB)
	if err != nil {
		return err
	}
	if len(fci) < 8 || string(fci[:4]) != "REMB" {
		return ErrRTCPFormat
	}
	n := int(fci[4])
	if len(fci) < 8+4*n {
		return ErrRTCPFormat
	}
	p.SenderSSRC, p.MediaSSRC = sender, media
	exp := uint(fci[5] >> 2)
	mantissa := uint64(fci[5]&3)<<16 | uint64(fci[6])<<8 | uint64(fci[7])
	if mantissa != 0 && exp > uint(bits.LeadingZeros64(mantissa)) {
		p.Bitrate = math.MaxUint64
	} else {
		p.Bitrate = mantissa << exp
	}
	p.SSRCs = p.SSRCs[:0]
	for i := range n {
		p.SSRCs = append(p.SSRCs, binary.BigEndian.Uint32(fci[8+4*i:]))
	}
	return nil
}

func (p *REMB) MarshalSize() int { return RTCPHeaderSize + 16 + 4*len(p.SSRCs) }

func (p *REMB) Append(dst []byte) ([]byte, error) {
	if len(p.SSRCs) > 255 {
		return dst, ErrRTCPTooMany
	}
	mantissa, exp := p.Bitrate, 0
	if n := bits.Len64(mantissa); n > 18 {
		exp = n - 18
		mantissa >>= exp
	}
	dst = appendFeedbackHeader(dst, TypePSFB, FormatAFB, p.SenderSSRC, p.MediaSSRC, 8+4*len(p.SSRCs))
	dst = append(dst, 'R', 'E', 'M', 'B', byte(len(p.SSRCs)),
		byte(exp<<2)|byte(mantissa>>16), byte(mantissa>>8), byte(mantissa))
	for _, s := range p.SSRCs {
		dst = binary.BigEndian.AppendUint32(dst, s)
	}
	return dst, nil
}
