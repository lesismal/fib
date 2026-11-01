package fmp4

import (
	"encoding/binary"
	"errors"
	"iter"
)

var (
	ErrShortBox = errors.New("fmp4: box header runs past the data")
	ErrBoxSize  = errors.New("fmp4: box size smaller than its header or past the data")
)

// BoxType is a box's four-character code, as "moov".
type BoxType [4]byte

// String returns the code as text.
func (t BoxType) String() string { return string(t[:]) }

// Box is one ISO BMFF box (ISO/IEC 14496-12 section 4.2) found in a byte
// slice.
type Box struct {
	Type BoxType
	// HeaderSize is 8, or 16 for a box with a 64-bit largesize.
	HeaderSize int
	// Data is the box's payload, after its header; for a full box it starts
	// with the version and flags. It refers to the slice the box was read
	// from.
	Data []byte
}

// Size returns the box's size, header included.
func (b *Box) Size() int { return b.HeaderSize + len(b.Data) }

// NextBox reads the box at the start of b and returns it and the bytes after
// it. A size of 0, which means the box runs to the end of the file, is taken
// to run to the end of b.
func NextBox(b []byte) (box Box, rest []byte, err error) {
	if len(b) < 8 {
		return box, b, ErrShortBox
	}
	size := uint64(binary.BigEndian.Uint32(b))
	copy(box.Type[:], b[4:8])
	box.HeaderSize = 8
	switch size {
	case 0:
		size = uint64(len(b))
	case 1:
		if len(b) < 16 {
			return box, b, ErrShortBox
		}
		size = binary.BigEndian.Uint64(b[8:])
		box.HeaderSize = 16
	}
	if size < uint64(box.HeaderSize) || size > uint64(len(b)) {
		return box, b, ErrBoxSize
	}
	box.Data = b[box.HeaderSize:size]
	return box, b[size:], nil
}

// Boxes yields the boxes of b in order, and stops after yielding the error of
// the first one that does not parse.
func Boxes(b []byte) iter.Seq2[Box, error] {
	return func(yield func(Box, error) bool) {
		for len(b) > 0 {
			box, rest, err := NextBox(b)
			if !yield(box, err) || err != nil {
				return
			}
			b = rest
		}
	}
}

// begin appends the header of a box of type typ whose size is filled in by
// end, and returns where the box starts.
func begin(b []byte, typ string) ([]byte, int) {
	at := len(b)
	b = append(b, 0, 0, 0, 0)
	return append(b, typ...), at
}

// beginFull begins a full box, with its version and 24-bit flags.
func beginFull(b []byte, typ string, version uint8, flags uint32) ([]byte, int) {
	b, at := begin(b, typ)
	return binary.BigEndian.AppendUint32(b, uint32(version)<<24|flags&0xffffff), at
}

// end fills in the size of the box begun at at.
func end(b []byte, at int) []byte {
	binary.BigEndian.PutUint32(b[at:], uint32(len(b)-at))
	return b
}

// u16, u32 and u64 append big-endian integers.
func u16(b []byte, v uint16) []byte { return binary.BigEndian.AppendUint16(b, v) }
func u32(b []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(b, v) }
func u64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }

// zeros appends n zero bytes.
func zeros(b []byte, n int) []byte {
	for range n {
		b = append(b, 0)
	}
	return b
}

// fullHeader splits a full box's payload into its version, flags and the
// rest.
func fullHeader(b []byte) (version uint8, flags uint32, rest []byte, ok bool) {
	if len(b) < 4 {
		return 0, 0, nil, false
	}
	v := binary.BigEndian.Uint32(b)
	return uint8(v >> 24), v & 0xffffff, b[4:], true
}

// child returns the payload of the first box of type typ among the boxes of
// b.
func child(b []byte, typ string) ([]byte, bool) {
	for box, err := range Boxes(b) {
		if err != nil {
			return nil, false
		}
		if string(box.Type[:]) == typ {
			return box.Data, true
		}
	}
	return nil, false
}
