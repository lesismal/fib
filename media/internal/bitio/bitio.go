// Package bitio reads and writes the MSB-first bit fields and the
// Exp-Golomb codes of H.264 (ITU-T H.264 section 9.1), H.265 and the MPEG-4
// audio headers.
//
// A Reader keeps the first error it meets and returns zeros from then on,
// so a parser reads a whole syntax structure and checks Err once, as the
// structure's syntax table reads, instead of after every field.
package bitio

import "errors"

var (
	// ErrShort is the error of a read past the end of the data.
	ErrShort = errors.New("bitio: data too short")
	// ErrGolomb is the error of an Exp-Golomb code too long for 32 bits.
	ErrGolomb = errors.New("bitio: Exp-Golomb code too long")
)

// Reader reads bits from a byte slice, most significant bit first.
type Reader struct {
	b   []byte
	pos int // in bits
	err error
}

// NewReader returns a Reader of b.
func NewReader(b []byte) Reader { return Reader{b: b} }

// Err returns the first error a read met.
func (r *Reader) Err() error { return r.err }

// Pos returns the number of bits read.
func (r *Reader) Pos() int { return r.pos }

// Left returns the number of bits not yet read.
func (r *Reader) Left() int { return len(r.b)*8 - r.pos }

func (r *Reader) fail(err error) {
	if r.err == nil {
		r.err = err
	}
	r.pos = len(r.b) * 8
}

// Bit reads one bit.
func (r *Reader) Bit() uint32 {
	if r.pos >= len(r.b)*8 {
		r.fail(ErrShort)
		return 0
	}
	v := uint32(r.b[r.pos>>3]>>(7-r.pos&7)) & 1
	r.pos++
	return v
}

// Flag reads one bit as a bool.
func (r *Reader) Flag() bool { return r.Bit() == 1 }

// Bits reads n bits, n <= 32, as an unsigned number.
func (r *Reader) Bits(n int) uint32 {
	if n > r.Left() {
		r.fail(ErrShort)
		return 0
	}
	var v uint32
	for ; n > 0; n-- {
		v = v<<1 | uint32(r.b[r.pos>>3]>>(7-r.pos&7))&1
		r.pos++
	}
	return v
}

// Bits64 reads n bits, n <= 64, as an unsigned number.
func (r *Reader) Bits64(n int) uint64 {
	if n > 32 {
		hi := uint64(r.Bits(n - 32))
		return hi<<32 | uint64(r.Bits(32))
	}
	return uint64(r.Bits(n))
}

// Skip skips n bits.
func (r *Reader) Skip(n int) {
	if n < 0 || n > r.Left() {
		r.fail(ErrShort)
		return
	}
	r.pos += n
}

// UE reads an unsigned Exp-Golomb code, ue(v). Codes whose value does not
// fit 32 bits are errors.
func (r *Reader) UE() uint32 {
	zeros := 0
	for r.Bit() == 0 {
		if r.err != nil {
			return 0
		}
		zeros++
		if zeros > 31 {
			r.fail(ErrGolomb)
			return 0
		}
	}
	return (1<<zeros - 1) + r.Bits(zeros)
}

// SE reads a signed Exp-Golomb code, se(v).
func (r *Reader) SE() int32 {
	v := r.UE()
	if v&1 == 1 {
		return int32(v>>1) + 1
	}
	return -int32(v >> 1)
}

// Writer appends bits to a byte slice, most significant bit first.
type Writer struct {
	b []byte
	n int // bits used in the last byte, 0 when it is full
}

// NewWriter returns a Writer appending to dst.
func NewWriter(dst []byte) Writer { return Writer{b: dst} }

// Bits writes the low n bits of v, n <= 64.
func (w *Writer) Bits(v uint64, n int) {
	for n > 0 {
		if w.n == 0 {
			w.b = append(w.b, 0)
		}
		free := 8 - w.n
		take := min(free, n)
		chunk := byte(v>>(n-take)) & (1<<take - 1)
		w.b[len(w.b)-1] |= chunk << (free - take)
		w.n = (w.n + take) & 7
		n -= take
	}
}

// Flag writes one bit.
func (w *Writer) Flag(f bool) {
	if f {
		w.Bits(1, 1)
	} else {
		w.Bits(0, 1)
	}
}

// UE writes v as an unsigned Exp-Golomb code.
func (w *Writer) UE(v uint32) { w.ue(uint64(v)) }

func (w *Writer) ue(v uint64) {
	x := v + 1
	n := 0
	for t := x; t > 1; t >>= 1 {
		n++
	}
	w.Bits(0, n)
	w.Bits(x, n+1)
}

// SE writes v as a signed Exp-Golomb code.
func (w *Writer) SE(v int32) {
	if v > 0 {
		w.ue(uint64(v)*2 - 1)
	} else {
		w.ue(uint64(-int64(v)) * 2)
	}
}

// Bytes returns the bytes written, the last one padded with zero bits.
func (w *Writer) Bytes() []byte { return w.b }
