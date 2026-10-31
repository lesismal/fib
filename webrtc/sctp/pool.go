package sctp

import "math/bits"

// bufPool recycles the buffers that hold queued messages and received
// chunks, by power-of-two size class, so steady traffic allocates nothing.
// It belongs to one association and needs no lock. Each class keeps up to
// 512 KiB of buffers, and at least 32, which covers a sender that keeps
// about that much buffered.
type bufPool struct {
	classes [poolClasses][][]byte
}

const (
	poolMinShift = 6
	poolClasses  = 14 // 64 bytes to 512 KiB
	poolKeep     = 32
	poolKeepSize = 512 << 10
)

func poolClass(n int) int {
	if n <= 1<<poolMinShift {
		return 0
	}
	return bits.Len(uint(n-1)) - poolMinShift
}

// get returns a buffer of length n.
func (p *bufPool) get(n int) []byte {
	c := poolClass(n)
	if c >= poolClasses {
		return make([]byte, n)
	}
	if l := p.classes[c]; len(l) > 0 {
		b := l[len(l)-1]
		p.classes[c] = l[:len(l)-1]
		return b[:n]
	}
	return make([]byte, n, 1<<(c+poolMinShift))
}

// put takes back a buffer get returned.
func (p *bufPool) put(b []byte) {
	n := cap(b)
	if n < 1<<poolMinShift || n&(n-1) != 0 {
		return
	}
	c := poolClass(n)
	if c < poolClasses && len(p.classes[c]) < max(poolKeep, poolKeepSize>>(c+poolMinShift)) {
		p.classes[c] = append(p.classes[c], b[:0])
	}
}

// grow returns b with room for n more bytes, moving it to a larger pooled
// buffer when it has none.
func (p *bufPool) grow(b []byte, n int) []byte {
	if len(b)+n <= cap(b) {
		return b
	}
	nb := p.get(max(2*cap(b), len(b)+n))[:len(b)]
	copy(nb, b)
	p.put(b)
	return nb
}
