package rtp

import (
	"encoding/binary"
)

// ReceptionReport is one report block of a sender or receiver report (RFC
// 3550 section 6.4.1).
type ReceptionReport struct {
	SSRC         uint32
	FractionLost uint8
	// TotalLost is the cumulative number of packets lost, a signed 24-bit
	// number that duplicates can make negative.
	TotalLost int32
	// LastSequence is the extended highest sequence number received.
	LastSequence     uint32
	Jitter           uint32
	LastSenderReport uint32
	// Delay is the delay since the last sender report, in units of 1/65536
	// seconds.
	Delay uint32
}

const receptionReportSize = 24

func (r *ReceptionReport) unmarshal(b []byte) {
	r.SSRC = binary.BigEndian.Uint32(b)
	r.FractionLost = b[4]
	lost := int32(b[5])<<16 | int32(b[6])<<8 | int32(b[7])
	r.TotalLost = lost << 8 >> 8
	r.LastSequence = binary.BigEndian.Uint32(b[8:])
	r.Jitter = binary.BigEndian.Uint32(b[12:])
	r.LastSenderReport = binary.BigEndian.Uint32(b[16:])
	r.Delay = binary.BigEndian.Uint32(b[20:])
}

func (r *ReceptionReport) append(dst []byte) []byte {
	lost := min(max(r.TotalLost, -1<<23), 1<<23-1)
	dst = binary.BigEndian.AppendUint32(dst, r.SSRC)
	dst = append(dst, r.FractionLost, byte(lost>>16), byte(lost>>8), byte(lost))
	dst = binary.BigEndian.AppendUint32(dst, r.LastSequence)
	dst = binary.BigEndian.AppendUint32(dst, r.Jitter)
	dst = binary.BigEndian.AppendUint32(dst, r.LastSenderReport)
	return binary.BigEndian.AppendUint32(dst, r.Delay)
}

func unmarshalReports(dst []ReceptionReport, b []byte, n int) ([]ReceptionReport, []byte, error) {
	if len(b) < n*receptionReportSize {
		return dst, nil, ErrRTCPFormat
	}
	dst = dst[:0]
	for i := range n {
		dst = append(dst, ReceptionReport{})
		dst[i].unmarshal(b[i*receptionReportSize:])
	}
	return dst, b[n*receptionReportSize:], nil
}

// SenderReport is an RTCP SR packet (RFC 3550 section 6.4.1).
type SenderReport struct {
	SSRC        uint32
	NTPTime     uint64
	RTPTime     uint32
	PacketCount uint32
	OctetCount  uint32
	// Reports is reused from one Unmarshal to the next.
	Reports []ReceptionReport
	// ProfileExtensions is what follows the report blocks, a multiple of 4
	// bytes.
	ProfileExtensions []byte
}

func (p *SenderReport) Unmarshal(b []byte) error {
	h, body, err := rtcpBody(b, TypeSR)
	if err != nil {
		return err
	}
	if len(body) < 24 {
		return ErrRTCPFormat
	}
	p.SSRC = binary.BigEndian.Uint32(body)
	p.NTPTime = binary.BigEndian.Uint64(body[4:])
	p.RTPTime = binary.BigEndian.Uint32(body[12:])
	p.PacketCount = binary.BigEndian.Uint32(body[16:])
	p.OctetCount = binary.BigEndian.Uint32(body[20:])
	p.Reports, p.ProfileExtensions, err = unmarshalReports(p.Reports, body[24:], int(h.Count))
	return err
}

func (p *SenderReport) MarshalSize() int {
	return RTCPHeaderSize + 24 + receptionReportSize*len(p.Reports) + len(p.ProfileExtensions)
}

func (p *SenderReport) Append(dst []byte) ([]byte, error) {
	if len(p.Reports) > 31 {
		return dst, ErrRTCPTooMany
	}
	size := p.MarshalSize() - RTCPHeaderSize
	if len(p.ProfileExtensions)%4 != 0 || size/4 > 0xffff {
		return dst, ErrRTCPLength
	}
	dst = appendRTCPHeader(dst, uint8(len(p.Reports)), TypeSR, size)
	dst = binary.BigEndian.AppendUint32(dst, p.SSRC)
	dst = binary.BigEndian.AppendUint64(dst, p.NTPTime)
	dst = binary.BigEndian.AppendUint32(dst, p.RTPTime)
	dst = binary.BigEndian.AppendUint32(dst, p.PacketCount)
	dst = binary.BigEndian.AppendUint32(dst, p.OctetCount)
	for i := range p.Reports {
		dst = p.Reports[i].append(dst)
	}
	return append(dst, p.ProfileExtensions...), nil
}

// ReceiverReport is an RTCP RR packet (RFC 3550 section 6.4.2).
type ReceiverReport struct {
	SSRC uint32
	// Reports is reused from one Unmarshal to the next.
	Reports           []ReceptionReport
	ProfileExtensions []byte
}

func (p *ReceiverReport) Unmarshal(b []byte) error {
	h, body, err := rtcpBody(b, TypeRR)
	if err != nil {
		return err
	}
	if len(body) < 4 {
		return ErrRTCPFormat
	}
	p.SSRC = binary.BigEndian.Uint32(body)
	p.Reports, p.ProfileExtensions, err = unmarshalReports(p.Reports, body[4:], int(h.Count))
	return err
}

func (p *ReceiverReport) MarshalSize() int {
	return RTCPHeaderSize + 4 + receptionReportSize*len(p.Reports) + len(p.ProfileExtensions)
}

func (p *ReceiverReport) Append(dst []byte) ([]byte, error) {
	if len(p.Reports) > 31 {
		return dst, ErrRTCPTooMany
	}
	size := p.MarshalSize() - RTCPHeaderSize
	if len(p.ProfileExtensions)%4 != 0 || size/4 > 0xffff {
		return dst, ErrRTCPLength
	}
	dst = appendRTCPHeader(dst, uint8(len(p.Reports)), TypeRR, size)
	dst = binary.BigEndian.AppendUint32(dst, p.SSRC)
	for i := range p.Reports {
		dst = p.Reports[i].append(dst)
	}
	return append(dst, p.ProfileExtensions...), nil
}

// The SDES item types of RFC 3550 section 6.5.
const (
	SDESEnd   uint8 = 0
	SDESCNAME uint8 = 1
	SDESName  uint8 = 2
	SDESEmail uint8 = 3
	SDESPhone uint8 = 4
	SDESLoc   uint8 = 5
	SDESTool  uint8 = 6
	SDESNote  uint8 = 7
	SDESPriv  uint8 = 8
)

// SDESItem is one item of an SDES chunk.
type SDESItem struct {
	Type uint8
	Text []byte
}

// SDESChunk is the items an SDES packet gives for one source.
type SDESChunk struct {
	Source uint32
	Items  []SDESItem
}

func (c *SDESChunk) size() int {
	n := 4
	for _, it := range c.Items {
		n += 2 + len(it.Text)
	}
	// The END item, then padding to a 4-byte boundary.
	return n + 4 - n%4
}

// SourceDescription is an RTCP SDES packet (RFC 3550 section 6.5).
type SourceDescription struct {
	// Chunks, and each chunk's Items, are reused from one Unmarshal to
	// the next; item texts refer to the packet.
	Chunks []SDESChunk
}

func (p *SourceDescription) Unmarshal(b []byte) error {
	h, body, err := rtcpBody(b, TypeSDES)
	if err != nil {
		return err
	}
	p.Chunks = p.Chunks[:0]
	off := 0
	for range h.Count {
		if len(body)-off < 4 {
			return ErrRTCPFormat
		}
		if len(p.Chunks) < cap(p.Chunks) {
			p.Chunks = p.Chunks[:len(p.Chunks)+1]
		} else {
			p.Chunks = append(p.Chunks, SDESChunk{})
		}
		c := &p.Chunks[len(p.Chunks)-1]
		c.Source = binary.BigEndian.Uint32(body[off:])
		c.Items = c.Items[:0]
		start := off
		off += 4
		for {
			if off >= len(body) {
				return ErrRTCPFormat
			}
			typ := body[off]
			if typ == SDESEnd {
				off++
				// Null bytes pad the chunk to a 4-byte boundary.
				off += (4 - (off-start)%4) % 4
				if off > len(body) {
					return ErrRTCPFormat
				}
				break
			}
			if off+2 > len(body) || off+2+int(body[off+1]) > len(body) {
				return ErrRTCPFormat
			}
			n := int(body[off+1])
			c.Items = append(c.Items, SDESItem{Type: typ, Text: body[off+2 : off+2+n : off+2+n]})
			off += 2 + n
		}
	}
	return nil
}

func (p *SourceDescription) MarshalSize() int {
	n := RTCPHeaderSize
	for i := range p.Chunks {
		n += p.Chunks[i].size()
	}
	return n
}

func (p *SourceDescription) Append(dst []byte) ([]byte, error) {
	if len(p.Chunks) > 31 {
		return dst, ErrRTCPTooMany
	}
	size := p.MarshalSize() - RTCPHeaderSize
	if size/4 > 0xffff {
		return dst, ErrRTCPLength
	}
	for _, c := range p.Chunks {
		for _, it := range c.Items {
			if len(it.Text) > 255 {
				return dst, ErrRTCPItem
			}
			if it.Type == SDESEnd {
				return dst, ErrRTCPFormat
			}
		}
	}
	dst = appendRTCPHeader(dst, uint8(len(p.Chunks)), TypeSDES, size)
	for _, c := range p.Chunks {
		start := len(dst)
		dst = binary.BigEndian.AppendUint32(dst, c.Source)
		for _, it := range c.Items {
			dst = append(dst, it.Type, byte(len(it.Text)))
			dst = append(dst, it.Text...)
		}
		dst = append(dst, SDESEnd)
		for (len(dst)-start)%4 != 0 {
			dst = append(dst, 0)
		}
	}
	return dst, nil
}

// Goodbye is an RTCP BYE packet (RFC 3550 section 6.6).
type Goodbye struct {
	// Sources is reused from one Unmarshal to the next.
	Sources []uint32
	Reason  []byte
}

func (p *Goodbye) Unmarshal(b []byte) error {
	h, body, err := rtcpBody(b, TypeBYE)
	if err != nil {
		return err
	}
	n := int(h.Count)
	if len(body) < 4*n {
		return ErrRTCPFormat
	}
	p.Sources = p.Sources[:0]
	for i := range n {
		p.Sources = append(p.Sources, binary.BigEndian.Uint32(body[4*i:]))
	}
	p.Reason = nil
	if rest := body[4*n:]; len(rest) > 0 {
		r := int(rest[0])
		if 1+r > len(rest) {
			return ErrRTCPFormat
		}
		p.Reason = rest[1 : 1+r : 1+r]
	}
	return nil
}

func (p *Goodbye) MarshalSize() int {
	n := RTCPHeaderSize + 4*len(p.Sources)
	if len(p.Reason) > 0 {
		n += (1 + len(p.Reason) + 3) &^ 3
	}
	return n
}

func (p *Goodbye) Append(dst []byte) ([]byte, error) {
	if len(p.Sources) > 31 {
		return dst, ErrRTCPTooMany
	}
	if len(p.Reason) > 255 {
		return dst, ErrRTCPItem
	}
	dst = appendRTCPHeader(dst, uint8(len(p.Sources)), TypeBYE, p.MarshalSize()-RTCPHeaderSize)
	for _, s := range p.Sources {
		dst = binary.BigEndian.AppendUint32(dst, s)
	}
	if len(p.Reason) > 0 {
		start := len(dst)
		dst = append(dst, byte(len(p.Reason)))
		dst = append(dst, p.Reason...)
		for (len(dst)-start)%4 != 0 {
			dst = append(dst, 0)
		}
	}
	return dst, nil
}

// ApplicationDefined is an RTCP APP packet (RFC 3550 section 6.7).
type ApplicationDefined struct {
	SubType uint8
	SSRC    uint32
	Name    [4]byte
	// Data is a multiple of 4 bytes.
	Data []byte
}

func (p *ApplicationDefined) Unmarshal(b []byte) error {
	h, body, err := rtcpBody(b, TypeAPP)
	if err != nil {
		return err
	}
	if len(body) < 8 {
		return ErrRTCPFormat
	}
	p.SubType = h.Count
	p.SSRC = binary.BigEndian.Uint32(body)
	copy(p.Name[:], body[4:8])
	p.Data = body[8:]
	return nil
}

func (p *ApplicationDefined) MarshalSize() int { return RTCPHeaderSize + 8 + len(p.Data) }

func (p *ApplicationDefined) Append(dst []byte) ([]byte, error) {
	if p.SubType > 31 {
		return dst, ErrRTCPFormat
	}
	size := p.MarshalSize() - RTCPHeaderSize
	if len(p.Data)%4 != 0 || size/4 > 0xffff {
		return dst, ErrRTCPLength
	}
	dst = appendRTCPHeader(dst, p.SubType, TypeAPP, size)
	dst = binary.BigEndian.AppendUint32(dst, p.SSRC)
	dst = append(dst, p.Name[:]...)
	return append(dst, p.Data...), nil
}
