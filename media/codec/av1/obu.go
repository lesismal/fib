package av1

import (
	"errors"
	"iter"
	"strconv"

	"github.com/lesismal/fib/media/internal/bitio"
)

var (
	ErrShortOBU = errors.New("av1: OBU too short")
	ErrLEB128   = errors.New("av1: malformed LEB128")
)

// OBUType is the obu_type of an OBU header (section 6.2.2).
type OBUType uint8

// The OBU types of section 6.2.2.
const (
	OBUSequenceHeader       OBUType = 1
	OBUTemporalDelimiter    OBUType = 2
	OBUFrameHeader          OBUType = 3
	OBUTileGroup            OBUType = 4
	OBUMetadata             OBUType = 5
	OBUFrame                OBUType = 6
	OBURedundantFrameHeader OBUType = 7
	OBUTileList             OBUType = 8
	OBUPadding              OBUType = 15
)

var obuNames = [...]string{
	OBUSequenceHeader:       "SEQUENCE_HEADER",
	OBUTemporalDelimiter:    "TEMPORAL_DELIMITER",
	OBUFrameHeader:          "FRAME_HEADER",
	OBUTileGroup:            "TILE_GROUP",
	OBUMetadata:             "METADATA",
	OBUFrame:                "FRAME",
	OBURedundantFrameHeader: "REDUNDANT_FRAME_HEADER",
	OBUTileList:             "TILE_LIST",
	OBUPadding:              "PADDING",
}

// String returns the type's name in the specification, as "FRAME".
func (t OBUType) String() string {
	if int(t) < len(obuNames) && obuNames[t] != "" {
		return obuNames[t]
	}
	return "OBUType(" + strconv.Itoa(int(t)) + ")"
}

// Bits of the first byte of an OBU header.
const (
	extensionFlag = 0x04
	hasSizeField  = 0x02
)

// Type returns the type of the OBU that starts b, 0 when b is empty.
func Type(b []byte) OBUType {
	if len(b) == 0 {
		return 0
	}
	return OBUType(b[0] >> 3 & 0x0f)
}

// OBU is one OBU: its header, of one byte or two with the extension, and
// its payload. Both refer to the bytes the OBU was read from.
type OBU struct {
	Header  []byte
	Payload []byte
}

// Type returns the OBU's type.
func (o OBU) Type() OBUType { return Type(o.Header) }

// NextOBU reads one OBU from the start of b and returns it and the bytes
// after it. An OBU with a size field ends where its size says; one without
// takes the rest of b, as in an RTP OBU element.
func NextOBU(b []byte) (OBU, []byte, error) {
	if len(b) == 0 || b[0]&0x80 != 0 {
		return OBU{}, nil, ErrShortOBU
	}
	h := 1
	if b[0]&extensionFlag != 0 {
		h = 2
	}
	if len(b) < h {
		return OBU{}, nil, ErrShortOBU
	}
	o := OBU{Header: b[:h:h]}
	if b[0]&hasSizeField == 0 {
		o.Payload = b[h:]
		return o, nil, nil
	}
	size, n := ReadLEB128(b[h:])
	if n == 0 {
		return OBU{}, nil, ErrLEB128
	}
	b = b[h+n:]
	if size > uint64(len(b)) {
		return OBU{}, nil, ErrShortOBU
	}
	o.Payload = b[:size:size]
	return o, b[size:], nil
}

// SplitOBUs yields the OBUs of b. It stops at the first that is malformed;
// ValidOBUs or NextOBU tell such data apart.
func SplitOBUs(b []byte) iter.Seq[OBU] {
	return func(yield func(OBU) bool) {
		for len(b) > 0 {
			o, rest, err := NextOBU(b)
			if err != nil || !yield(o) {
				return
			}
			b = rest
		}
	}
}

// ValidOBUs reports whether b is a whole number of OBUs.
func ValidOBUs(b []byte) bool {
	for len(b) > 0 {
		_, rest, err := NextOBU(b)
		if err != nil {
			return false
		}
		b = rest
	}
	return true
}

// AppendOBU appends o to dst, with a size field when withSize is true, as
// the low-overhead bitstream format has it, and without when false, as
// the RTP payload format prefers.
func AppendOBU(dst []byte, o OBU, withSize bool) []byte {
	h := o.Header[0] &^ hasSizeField
	if withSize {
		h |= hasSizeField
	}
	dst = append(dst, h)
	dst = append(dst, o.Header[1:]...)
	if withSize {
		dst = AppendLEB128(dst, uint64(len(o.Payload)))
	}
	return append(dst, o.Payload...)
}

// ReadLEB128 reads the leb128() of section 4.10.5 at the start of b and
// returns it with the number of bytes it took, 0 when b holds no valid one:
// it runs past b, takes more than 8 bytes or is over 2^32-1.
func ReadLEB128(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < 8 && i < len(b); i++ {
		v |= uint64(b[i]&0x7f) << (7 * i)
		if b[i]&0x80 == 0 {
			if v > 1<<32-1 {
				return 0, 0
			}
			return v, i + 1
		}
	}
	return 0, 0
}

// AppendLEB128 appends v to dst as a leb128() of as few bytes as it takes.
func AppendLEB128(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// LEB128Size returns the number of bytes AppendLEB128 writes for v.
func LEB128Size(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// IsKeyframe reports whether the temporal unit tu, a sequence of OBUs,
// starts with a key frame: its first frame header, of an OBU_FRAME or an
// OBU_FRAME_HEADER, is a new frame of frame_type KEY_FRAME, or the
// sequence it carries is a reduced still picture, every frame of which is
// one.
func IsKeyframe(tu []byte) bool {
	reduced := false
	for o := range SplitOBUs(tu) {
		switch o.Type() {
		case OBUSequenceHeader:
			var s SequenceHeader
			reduced = s.parse(o.Payload) == nil && s.ReducedStillPictureHeader
		case OBUFrame, OBUFrameHeader:
			if reduced {
				return true
			}
			r := bitio.NewReader(o.Payload)
			showExisting := r.Flag()
			frameType := r.Bits(2)
			return r.Err() == nil && !showExisting && frameType == 0
		}
	}
	return false
}
