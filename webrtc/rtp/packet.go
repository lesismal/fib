package rtp

import (
	"encoding/binary"
	"errors"
	"iter"
)

var (
	ErrShortPacket      = errors.New("rtp: packet too short")
	ErrVersion          = errors.New("rtp: version is not 2")
	ErrPadding          = errors.New("rtp: invalid padding")
	ErrTooManyCSRC      = errors.New("rtp: more than 15 CSRCs")
	ErrPayloadType      = errors.New("rtp: payload type above 127")
	ErrExtensionLength  = errors.New("rtp: header extension length is not a multiple of 4")
	ErrExtensionProfile = errors.New("rtp: header extension profile is not RFC 8285's")
	ErrExtensionID      = errors.New("rtp: invalid header extension id")
	ErrExtensionSize    = errors.New("rtp: header extension element too large")
	ErrExtension        = errors.New("rtp: malformed header extension")
)

// HeaderSize is the size of an RTP header with no CSRC and no extension.
const HeaderSize = 12

// The header extension profiles of RFC 8285: one-byte elements, or two-byte
// elements whose profile's low 4 bits are left to the application.
const (
	ExtensionProfileOneByte uint16 = 0xbede
	ExtensionProfileTwoByte uint16 = 0x1000
)

// Header is an RTP fixed header (RFC 3550 section 5.1) with its CSRC list
// and header extension.
type Header struct {
	Padding        bool
	Marker         bool
	PayloadType    uint8
	SequenceNumber uint16
	Timestamp      uint32
	SSRC           uint32
	// CSRC is reused from one Unmarshal to the next, so a Header that has
	// held as many CSRCs before parses with no allocation.
	CSRC []uint32
	// Extension is the X bit: the header carries an extension block of
	// ExtensionProfile whose body, after its 4-byte header, is
	// ExtensionData, a multiple of 4 bytes long. Unmarshal points
	// ExtensionData into the packet.
	Extension        bool
	ExtensionProfile uint16
	ExtensionData    []byte

	// ext is the buffer SetExtension and DelExtension build ExtensionData
	// in, kept so a Header that sets an extension on every packet it sends
	// does not allocate for it.
	ext []byte
}

// Unmarshal parses the header at the start of b and returns its size. The
// header refers to b, which must not change while the header is in use.
func (h *Header) Unmarshal(b []byte) (int, error) {
	if len(b) < HeaderSize {
		return 0, ErrShortPacket
	}
	if b[0]>>6 != 2 {
		return 0, ErrVersion
	}
	h.Padding = b[0]&0x20 != 0
	h.Extension = b[0]&0x10 != 0
	cc := int(b[0] & 0x0f)
	h.Marker = b[1]&0x80 != 0
	h.PayloadType = b[1] & 0x7f
	h.SequenceNumber = binary.BigEndian.Uint16(b[2:])
	h.Timestamp = binary.BigEndian.Uint32(b[4:])
	h.SSRC = binary.BigEndian.Uint32(b[8:])
	n := HeaderSize + 4*cc
	if len(b) < n {
		return 0, ErrShortPacket
	}
	h.CSRC = h.CSRC[:0]
	for i := HeaderSize; i < n; i += 4 {
		h.CSRC = append(h.CSRC, binary.BigEndian.Uint32(b[i:]))
	}
	h.ExtensionProfile = 0
	h.ExtensionData = nil
	if h.Extension {
		if len(b) < n+4 {
			return 0, ErrShortPacket
		}
		h.ExtensionProfile = binary.BigEndian.Uint16(b[n:])
		size := 4 * int(binary.BigEndian.Uint16(b[n+2:]))
		n += 4
		if len(b) < n+size {
			return 0, ErrShortPacket
		}
		h.ExtensionData = b[n : n+size : n+size]
		n += size
	}
	return n, nil
}

// MarshalSize returns the number of bytes Append adds.
func (h *Header) MarshalSize() int {
	n := HeaderSize + 4*len(h.CSRC)
	if h.Extension {
		n += 4 + len(h.ExtensionData)
	}
	return n
}

// Append appends the header to dst.
func (h *Header) Append(dst []byte) ([]byte, error) {
	if len(h.CSRC) > 15 {
		return dst, ErrTooManyCSRC
	}
	if h.PayloadType > 127 {
		return dst, ErrPayloadType
	}
	if h.Extension && (len(h.ExtensionData)%4 != 0 || len(h.ExtensionData) > 0xffff*4) {
		return dst, ErrExtensionLength
	}
	b0 := byte(2<<6) | byte(len(h.CSRC))
	if h.Padding {
		b0 |= 0x20
	}
	if h.Extension {
		b0 |= 0x10
	}
	b1 := h.PayloadType
	if h.Marker {
		b1 |= 0x80
	}
	dst = append(dst, b0, b1)
	dst = binary.BigEndian.AppendUint16(dst, h.SequenceNumber)
	dst = binary.BigEndian.AppendUint32(dst, h.Timestamp)
	dst = binary.BigEndian.AppendUint32(dst, h.SSRC)
	for _, c := range h.CSRC {
		dst = binary.BigEndian.AppendUint32(dst, c)
	}
	if h.Extension {
		dst = binary.BigEndian.AppendUint16(dst, h.ExtensionProfile)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(h.ExtensionData)/4))
		dst = append(dst, h.ExtensionData...)
	}
	return dst, nil
}

// Packet is an RTP packet: a header, its payload and the padding after it.
type Packet struct {
	Header
	Payload []byte
	// PaddingSize counts the padding after the payload, its final count
	// byte included. Append writes it, and sets the P bit, when it is not
	// zero, whatever Header.Padding says.
	PaddingSize uint8
}

// Unmarshal parses an RTP packet in place: the payload and the header's
// fields refer to b.
func (p *Packet) Unmarshal(b []byte) error {
	n, err := p.Header.Unmarshal(b)
	if err != nil {
		return err
	}
	end := len(b)
	p.PaddingSize = 0
	if p.Padding {
		if end == n {
			return ErrPadding
		}
		pad := int(b[end-1])
		if pad == 0 || pad > end-n {
			return ErrPadding
		}
		p.PaddingSize = uint8(pad)
		end -= pad
	}
	p.Payload = b[n:end:end]
	return nil
}

// MarshalSize returns the number of bytes Append adds.
func (p *Packet) MarshalSize() int {
	return p.Header.MarshalSize() + len(p.Payload) + int(p.PaddingSize)
}

// Append appends the packet to dst.
func (p *Packet) Append(dst []byte) ([]byte, error) {
	padding := p.Header.Padding
	p.Header.Padding = p.PaddingSize > 0
	dst, err := p.Header.Append(dst)
	p.Header.Padding = padding
	if err != nil {
		return dst, err
	}
	dst = append(dst, p.Payload...)
	if p.PaddingSize > 0 {
		for range p.PaddingSize - 1 {
			dst = append(dst, 0)
		}
		dst = append(dst, p.PaddingSize)
	}
	return dst, nil
}

// HeaderLen returns the size of the RTP header at the start of b, CSRCs
// and extension included, without parsing anything else; SRTP uses it to
// find where the payload starts.
func HeaderLen(b []byte) (int, error) {
	if len(b) < HeaderSize {
		return 0, ErrShortPacket
	}
	if b[0]>>6 != 2 {
		return 0, ErrVersion
	}
	n := HeaderSize + 4*int(b[0]&0x0f)
	if b[0]&0x10 != 0 {
		if len(b) < n+4 {
			return 0, ErrShortPacket
		}
		n += 4 + 4*int(binary.BigEndian.Uint16(b[n+2:]))
	}
	if len(b) < n {
		return 0, ErrShortPacket
	}
	return n, nil
}

// extensionForm tells which RFC 8285 form a profile is, 1 or 2 bytes of
// element header, or 0 for a profile it does not define.
func extensionForm(profile uint16) int {
	switch {
	case profile == ExtensionProfileOneByte:
		return 1
	case profile&0xfff0 == ExtensionProfileTwoByte:
		return 2
	}
	return 0
}

// nextExtension reads the element at the start of data in the given form,
// skipping padding, and returns its id, its payload and what follows. It
// returns id 0 at the end of the block.
func nextExtension(data []byte, form int) (id uint8, payload, rest []byte, err error) {
	for len(data) > 0 {
		if data[0] == 0 {
			data = data[1:]
			continue
		}
		if form == 1 {
			id = data[0] >> 4
			if id == 15 {
				return 0, nil, nil, nil
			}
			size := int(data[0]&0x0f) + 1
			if len(data) < 1+size {
				return 0, nil, nil, ErrExtension
			}
			return id, data[1 : 1+size : 1+size], data[1+size:], nil
		}
		if len(data) < 2 {
			return 0, nil, nil, ErrExtension
		}
		id = data[0]
		size := int(data[1])
		if len(data) < 2+size {
			return 0, nil, nil, ErrExtension
		}
		return id, data[2 : 2+size : 2+size], data[2+size:], nil
	}
	return 0, nil, nil, nil
}

// GetExtension returns the payload of the header extension element with
// the id, or nil when there is none, the extension is not RFC 8285's or the
// block is malformed. It does not allocate.
func (h *Header) GetExtension(id uint8) []byte {
	if !h.Extension || id == 0 {
		return nil
	}
	form := extensionForm(h.ExtensionProfile)
	if form == 0 {
		return nil
	}
	data := h.ExtensionData
	for {
		got, payload, rest, err := nextExtension(data, form)
		if err != nil || got == 0 {
			return nil
		}
		if got == id {
			return payload
		}
		data = rest
	}
}

// Extensions yields each RFC 8285 header extension element in turn, its id
// and its payload, stopping at the end of the block or at a malformed
// element. It yields nothing for another profile.
func (h *Header) Extensions() iter.Seq2[uint8, []byte] {
	return func(yield func(uint8, []byte) bool) {
		if !h.Extension {
			return
		}
		form := extensionForm(h.ExtensionProfile)
		if form == 0 {
			return
		}
		data := h.ExtensionData
		for {
			id, payload, rest, err := nextExtension(data, form)
			if err != nil || id == 0 || !yield(id, payload) {
				return
			}
			data = rest
		}
	}
}

// SetExtension sets the payload of the header extension element with the
// id, replacing the one there or adding it. The block keeps the one-byte
// form while every element fits it (ids 1 to 14, payloads of 1 to 16
// bytes) and turns to the two-byte form when one does not. ExtensionData
// then refers to a buffer the Header owns and reuses on the next call, so
// the payload may come from the header's own block, and setting an
// extension on every packet a Header sends does not allocate.
func (h *Header) SetExtension(id uint8, payload []byte) error {
	if id == 0 {
		return ErrExtensionID
	}
	if len(payload) > 255 {
		return ErrExtensionSize
	}
	return h.rewriteExtensions(id, payload, true)
}

// DelExtension removes the header extension element with the id, and the
// extension block when it was the last.
func (h *Header) DelExtension(id uint8) error {
	if id == 0 || !h.Extension {
		return nil
	}
	return h.rewriteExtensions(id, nil, false)
}

func (h *Header) rewriteExtensions(id uint8, payload []byte, set bool) error {
	form := 1
	if h.Extension {
		form = extensionForm(h.ExtensionProfile)
		if form == 0 {
			return ErrExtensionProfile
		}
	}
	// The new block is built in a scratch buffer first, since both the old
	// block and the payload may live in h.ext.
	var stack [256]byte
	scratch := stack[:0]
	wantForm := form
	if set && (id > 14 || len(payload) == 0 || len(payload) > 16) {
		wantForm = 2
	}
	if h.Extension {
		data := h.ExtensionData
		for {
			got, p, rest, err := nextExtension(data, form)
			if err != nil {
				return err
			}
			if got == 0 {
				break
			}
			if got != id && (got > 14 || len(p) == 0 || len(p) > 16) {
				wantForm = 2
			}
			data = rest
		}
	}
	appendElem := func(dst []byte, id uint8, p []byte) []byte {
		if wantForm == 1 {
			dst = append(dst, id<<4|byte(len(p)-1))
		} else {
			dst = append(dst, id, byte(len(p)))
		}
		return append(dst, p...)
	}
	if h.Extension {
		data := h.ExtensionData
		for {
			got, p, rest, _ := nextExtension(data, form)
			if got == 0 {
				break
			}
			if got != id {
				scratch = appendElem(scratch, got, p)
			}
			data = rest
		}
	}
	if set {
		scratch = appendElem(scratch, id, payload)
	}
	for len(scratch)%4 != 0 {
		scratch = append(scratch, 0)
	}
	if len(scratch) == 0 {
		h.Extension = false
		h.ExtensionProfile = 0
		h.ExtensionData = nil
		return nil
	}
	if len(scratch)/4 > 0xffff {
		return ErrExtensionLength
	}
	h.ext = append(h.ext[:0], scratch...)
	h.Extension = true
	if wantForm == 1 {
		h.ExtensionProfile = ExtensionProfileOneByte
	} else if form == 1 {
		h.ExtensionProfile = ExtensionProfileTwoByte
	}
	h.ExtensionData = h.ext
	return nil
}
