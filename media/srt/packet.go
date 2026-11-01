package srt

import (
	"encoding/binary"
	"errors"
)

// headerSize is the size of every SRT packet's header (section 3).
const headerSize = 16

// Control packet types (section 3.2).
const (
	ctrlHandshake  = 0x0000
	ctrlKeepalive  = 0x0001
	ctrlACK        = 0x0002
	ctrlNAK        = 0x0003
	ctrlCongestion = 0x0004
	ctrlShutdown   = 0x0005
	ctrlACKACK     = 0x0006
	ctrlDropReq    = 0x0007
	ctrlPeerError  = 0x0008
	ctrlUser       = 0x7fff
)

// Handshake types (section 3.2.1).
const (
	hsDone       = 0xfffffffd
	hsAgreement  = 0xfffffffe
	hsConclusion = 0xffffffff
	hsWaveahand  = 0x00000000
	hsInduction  = 0x00000001
	// hsRejectBase plus a RejectReason is a rejection.
	hsRejectBase = 1000
)

// Handshake extension types (section 3.2.1.1 to 3.2.1.3).
const (
	extHSReq      = 1
	extHSRsp      = 2
	extKMReq      = 3
	extKMRsp      = 4
	extSID        = 5
	extCongestion = 6
)

// Extension field flags of a conclusion handshake.
const (
	extFlagHSReq  = 0x0001
	extFlagKMReq  = 0x0002
	extFlagConfig = 0x0004
)

// srtMagic is the extension field of a version 5 induction response.
const srtMagic = 0x4a17

// SRT flags of HSREQ and HSRSP (section 3.2.1.1).
const (
	flagTSBPDSnd    = 0x01
	flagTSBPDRcv    = 0x02
	flagCrypt       = 0x04
	flagTLPktDrop   = 0x08
	flagPeriodicNAK = 0x10
	flagRexmit      = 0x20
	flagStream      = 0x40
	flagFilter      = 0x80
)

// srtVersion is the SRT version this package says it speaks: 1.5.0.
const srtVersion = 0x010500

// maxSeq is the largest sequence number plus one: they are 31 bits.
const maxSeq = 1 << 31

// seqAdd returns s advanced by n, wrapping at 31 bits.
func seqAdd(s uint32, n int) uint32 { return uint32(int64(s)+int64(n)) & (maxSeq - 1) }

// seqDiff returns a-b, taking the shorter way round the 31-bit circle.
func seqDiff(a, b uint32) int {
	return int(int32((a-b)<<1) >> 1)
}

// Data packet message flags of the second header word: the packet position
// (PP), the in-order flag (O), the encryption key (KK) and the
// retransmitted flag (R), above a 26-bit message number.
const (
	ppSolo        = 3 << 30
	flagOrder     = 1 << 29
	kkShift       = 27
	kkMask        = 3 << kkShift
	flagRexmitPkt = 1 << 26
	msgnoMask     = 1<<26 - 1
)

// The KK values: which of the two keys encrypts a packet.
const (
	kkNone = 0
	kkEven = 1
	kkOdd  = 2
)

var errShortPacket = errors.New("srt: packet shorter than its header")

// header is the 16-byte header of a packet: a data packet when ctrl is
// false, with seq, and info the message word; a control packet otherwise,
// with typ, subtype and info the type-specific word.
type header struct {
	ctrl    bool
	seq     uint32
	typ     uint16
	subtype uint16
	info    uint32
	ts      uint32
	dst     uint32
}

func (h *header) unmarshal(b []byte) error {
	if len(b) < headerSize {
		return errShortPacket
	}
	w0 := binary.BigEndian.Uint32(b)
	h.ctrl = w0&0x80000000 != 0
	if h.ctrl {
		h.typ = uint16(w0>>16) & 0x7fff
		h.subtype = uint16(w0)
		h.seq = 0
	} else {
		h.seq = w0
		h.typ, h.subtype = 0, 0
	}
	h.info = binary.BigEndian.Uint32(b[4:])
	h.ts = binary.BigEndian.Uint32(b[8:])
	h.dst = binary.BigEndian.Uint32(b[12:])
	return nil
}

func (h *header) append(dst []byte) []byte {
	var w0 uint32
	if h.ctrl {
		w0 = 0x80000000 | uint32(h.typ&0x7fff)<<16 | uint32(h.subtype)
	} else {
		w0 = h.seq & (maxSeq - 1)
	}
	dst = binary.BigEndian.AppendUint32(dst, w0)
	dst = binary.BigEndian.AppendUint32(dst, h.info)
	dst = binary.BigEndian.AppendUint32(dst, h.ts)
	return binary.BigEndian.AppendUint32(dst, h.dst)
}

// handshake is the handshake control information field (section 3.2.1),
// with the extensions of a version 5 conclusion.
type handshake struct {
	version    uint32
	encryption uint16
	extField   uint16
	isn        uint32
	mtu        uint32
	flowWindow uint32
	typ        uint32
	socketID   uint32
	cookie     uint32
	peerIP     [16]byte

	// Extensions.
	hsReq    *hsExt // HSREQ or HSRSP
	hsRsp    bool
	km       []byte // KMREQ or KMRSP content
	kmRsp    bool
	streamID string
	hasSID   bool
}

// hsExt is the content of HSREQ and HSRSP.
type hsExt struct {
	version  uint32
	flags    uint32
	rcvDelay uint16 // ms
	sndDelay uint16 // ms
}

const handshakeSize = 48

var (
	errShortHandshake = errors.New("srt: handshake shorter than 48 bytes")
	errBadExtension   = errors.New("srt: malformed handshake extension")
	errLongStreamID   = errors.New("srt: stream ID longer than 512 bytes")
)

// maxStreamID is the longest stream ID the SID extension may carry.
const maxStreamID = 512

func (hs *handshake) unmarshal(b []byte) error {
	if len(b) < handshakeSize {
		return errShortHandshake
	}
	hs.version = binary.BigEndian.Uint32(b)
	hs.encryption = binary.BigEndian.Uint16(b[4:])
	hs.extField = binary.BigEndian.Uint16(b[6:])
	hs.isn = binary.BigEndian.Uint32(b[8:]) & (maxSeq - 1)
	hs.mtu = binary.BigEndian.Uint32(b[12:])
	hs.flowWindow = binary.BigEndian.Uint32(b[16:])
	hs.typ = binary.BigEndian.Uint32(b[20:])
	hs.socketID = binary.BigEndian.Uint32(b[24:])
	hs.cookie = binary.BigEndian.Uint32(b[28:])
	copy(hs.peerIP[:], b[32:48])
	hs.hsReq, hs.km, hs.streamID, hs.hasSID = nil, nil, "", false
	if hs.version < 5 || hs.typ != hsConclusion {
		return nil
	}
	b = b[handshakeSize:]
	for len(b) >= 4 {
		typ := binary.BigEndian.Uint16(b)
		n := int(binary.BigEndian.Uint16(b[2:])) * 4
		if 4+n > len(b) {
			return errBadExtension
		}
		body := b[4 : 4+n]
		b = b[4+n:]
		switch typ {
		case extHSReq, extHSRsp:
			if n < 12 {
				return errBadExtension
			}
			hs.hsReq = &hsExt{
				version:  binary.BigEndian.Uint32(body),
				flags:    binary.BigEndian.Uint32(body[4:]),
				rcvDelay: binary.BigEndian.Uint16(body[8:]),
				sndDelay: binary.BigEndian.Uint16(body[10:]),
			}
			hs.hsRsp = typ == extHSRsp
		case extKMReq, extKMRsp:
			hs.km = body
			hs.kmRsp = typ == extKMRsp
		case extSID:
			if n > maxStreamID {
				return errLongStreamID
			}
			hs.streamID = string(unswapWords(body))
			hs.hasSID = true
		}
	}
	return nil
}

func (hs *handshake) append(dst []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, hs.version)
	dst = binary.BigEndian.AppendUint16(dst, hs.encryption)
	dst = binary.BigEndian.AppendUint16(dst, hs.extField)
	dst = binary.BigEndian.AppendUint32(dst, hs.isn)
	dst = binary.BigEndian.AppendUint32(dst, hs.mtu)
	dst = binary.BigEndian.AppendUint32(dst, hs.flowWindow)
	dst = binary.BigEndian.AppendUint32(dst, hs.typ)
	dst = binary.BigEndian.AppendUint32(dst, hs.socketID)
	dst = binary.BigEndian.AppendUint32(dst, hs.cookie)
	dst = append(dst, hs.peerIP[:]...)
	if hs.hsReq != nil {
		typ := uint16(extHSReq)
		if hs.hsRsp {
			typ = extHSRsp
		}
		dst = binary.BigEndian.AppendUint16(dst, typ)
		dst = binary.BigEndian.AppendUint16(dst, 3)
		dst = binary.BigEndian.AppendUint32(dst, hs.hsReq.version)
		dst = binary.BigEndian.AppendUint32(dst, hs.hsReq.flags)
		dst = binary.BigEndian.AppendUint16(dst, hs.hsReq.rcvDelay)
		dst = binary.BigEndian.AppendUint16(dst, hs.hsReq.sndDelay)
	}
	if hs.km != nil {
		typ := uint16(extKMReq)
		if hs.kmRsp {
			typ = extKMRsp
		}
		dst = binary.BigEndian.AppendUint16(dst, typ)
		dst = binary.BigEndian.AppendUint16(dst, uint16((len(hs.km)+3)/4))
		dst = append(dst, hs.km...)
		for i := len(hs.km); i%4 != 0; i++ {
			dst = append(dst, 0)
		}
	}
	if hs.hasSID {
		sid := swapWords([]byte(hs.streamID))
		dst = binary.BigEndian.AppendUint16(dst, extSID)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(sid)/4))
		dst = append(dst, sid...)
	}
	return dst
}

// swapWords pads b with zeros to whole 32-bit words and reverses the bytes
// of each word: libsrt writes the SID extension as little-endian words,
// which every implementation follows (section 3.2.1.3).
func swapWords(b []byte) []byte {
	out := make([]byte, (len(b)+3)/4*4)
	copy(out, b)
	for i := 0; i < len(out); i += 4 {
		out[i], out[i+1], out[i+2], out[i+3] = out[i+3], out[i+2], out[i+1], out[i]
	}
	return out
}

// unswapWords undoes swapWords, dropping the zero padding.
func unswapWords(b []byte) []byte {
	out := make([]byte, len(b)/4*4)
	copy(out, b)
	for i := 0; i < len(out); i += 4 {
		out[i], out[i+1], out[i+2], out[i+3] = out[i+3], out[i+2], out[i+1], out[i]
	}
	for len(out) > 0 && out[len(out)-1] == 0 {
		out = out[:len(out)-1]
	}
	return out
}

// appendLossList appends the NAK loss list of ranges (section 3.2.5): a
// single sequence number as itself, a range as its first number with the
// top bit set followed by its last.
func appendLossList(dst []byte, ranges []lossRange) []byte {
	for _, r := range ranges {
		if r.from == r.to {
			dst = binary.BigEndian.AppendUint32(dst, r.from)
		} else {
			dst = binary.BigEndian.AppendUint32(dst, r.from|0x80000000)
			dst = binary.BigEndian.AppendUint32(dst, r.to)
		}
	}
	return dst
}

// parseLossList calls fn for every range of a NAK's loss list, stopping at
// a malformed entry.
func parseLossList(b []byte, fn func(from, to uint32)) {
	for len(b) >= 4 {
		w := binary.BigEndian.Uint32(b)
		b = b[4:]
		if w&0x80000000 == 0 {
			fn(w, w)
			continue
		}
		if len(b) < 4 {
			return
		}
		to := binary.BigEndian.Uint32(b) & (maxSeq - 1)
		b = b[4:]
		from := w & (maxSeq - 1)
		if seqDiff(to, from) < 0 {
			return
		}
		fn(from, to)
	}
}

// lossRange is a range of lost sequence numbers, from and to included.
type lossRange struct {
	from, to uint32
}
