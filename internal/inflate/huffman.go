package inflate

// fastBits is how many bits of a code the lookup table resolves at once;
// longer codes, which are rare, are decoded a bit at a time.
const fastBits = 9

// symInvalid is what decode reports for bits that are no code.
const symInvalid = -1

// huffman is a canonical Huffman code (RFC 1951 section 3.2.2): count[n] is
// how many codes are n bits long, and symbol lists the symbols in the order
// of their codes. fast maps the next fastBits bits of input to the symbol
// whose code they begin with, as symbol<<4 | length, or to zero when its code
// is longer.
type huffman struct {
	count  [16]uint16
	symbol [288]uint16
	fast   [1 << fastBits]uint16
}

// build makes the code whose symbols have lengths, where zero is a symbol
// that has none. It returns zero for a complete code, or one with no codes at
// all; more than zero for an incomplete one, which the caller may refuse; and
// less than zero for one with more codes than the lengths allow, which it
// leaves unusable.
func (h *huffman) build(lengths []uint8) int {
	h.count = [16]uint16{}
	h.fast = [1 << fastBits]uint16{}
	for _, n := range lengths {
		h.count[n]++
	}
	if int(h.count[0]) == len(lengths) {
		return 0
	}
	left := 1
	for n := 1; n < 16; n++ {
		left <<= 1
		left -= int(h.count[n])
		if left < 0 {
			return left
		}
	}
	var offsets, next [16]int
	code := 0
	for n := 1; n < 16; n++ {
		if n < 15 {
			offsets[n+1] = offsets[n] + int(h.count[n])
		}
		if n > 1 {
			code = (code + int(h.count[n-1])) << 1
		}
		next[n] = code
	}
	for sym, n := range lengths {
		if n == 0 {
			continue
		}
		h.symbol[offsets[n]] = uint16(sym)
		offsets[n]++
		code := next[n]
		next[n]++
		if n > fastBits {
			continue
		}
		// DEFLATE sends a code's bits most significant first, into a
		// stream read least significant first: reversed, as the table is
		// indexed.
		reversed := 0
		for i := uint8(0); i < n; i++ {
			reversed |= (code >> i & 1) << (n - 1 - i)
		}
		for i := reversed; i < len(h.fast); i += 1 << n {
			h.fast[i] = uint16(sym)<<4 | uint16(n)
		}
	}
	return left
}

// decode reads a symbol from the front of bits, of which nbits are input. It
// returns the symbol and the length of its code, or a zero length with
// symInvalid for bits that are no code, and with anything else when the code
// is longer than nbits.
func (h *huffman) decode(bits uint64, nbits uint) (int, uint) {
	if e := h.fast[bits&(1<<fastBits-1)]; e != 0 {
		// Bits past nbits are zero, so an entry found through them is the
		// symbol only when its code is within nbits.
		if n := uint(e & 15); n <= nbits {
			return int(e >> 4), n
		}
		return 0, 0
	}
	// Bit by bit, as zlib's puff does.
	code, first, index := 0, 0, 0
	for n := uint(1); n < 16; n++ {
		if n > nbits {
			return 0, 0
		}
		code |= int(bits>>(n-1)) & 1
		count := int(h.count[n])
		if code-count < first {
			return int(h.symbol[index+code-first]), n
		}
		index += count
		first += count
		first <<= 1
		code <<= 1
	}
	return symInvalid, 0
}

// The codes of a fixed-code block.
var fixedLit, fixedDist huffman

func init() {
	var lengths [288]uint8
	for i := range lengths {
		switch {
		case i < 144:
			lengths[i] = 8
		case i < 256:
			lengths[i] = 9
		case i < 280:
			lengths[i] = 7
		default:
			lengths[i] = 8
		}
	}
	fixedLit.build(lengths[:])
	var dist [30]uint8
	for i := range dist {
		dist[i] = 5
	}
	fixedDist.build(dist[:])
}
