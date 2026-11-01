package inflate

import (
	"bytes"
	"errors"
	"testing"
)

// clCode is the code length code the dynamic blocks below are written with:
// eight symbols of three bits each, a complete code. canonical order gives
// them the codes 0 to 7 in the order of their symbols.
var clCode = []int{0, 1, 2, 8, 9, 16, 17, 18}

// dynamicBlock writes a final dynamic block header for nlen literal/length
// and ndist distance codes, with the code length code of clLengths (by
// symbol, in the order the format sends them, of which the first hclen go),
// and then the symbols of syms, each a code length code symbol and, for 16,
// 17 and 18, its extra bits.
type clSym struct {
	sym   int
	extra uint64
}

func dynamicBlock(w *bitWriter, nlen, ndist, hclen int, clLengths [19]uint8, syms []clSym) {
	w.write(1, 1) // final
	w.write(2, 2) // dynamic codes
	w.write(uint64(nlen-257), 5)
	w.write(uint64(ndist-1), 5)
	w.write(uint64(hclen-4), 4)
	for _, s := range codeLengthOrder[:hclen] {
		w.write(uint64(clLengths[s]), 3)
	}
	for _, s := range syms {
		code := -1
		for i, c := range clCode {
			if c == s.sym {
				code = i
			}
		}
		w.writeCode(uint64(code), 3)
		switch s.sym {
		case 16:
			w.write(s.extra, 2)
		case 17:
			w.write(s.extra, 3)
		case 18:
			w.write(s.extra, 7)
		}
	}
}

// completeCL is clCode's lengths.
func completeCL() [19]uint8 {
	var l [19]uint8
	for _, s := range clCode {
		l[s] = 3
	}
	return l
}

// lengthsOf turns literal/length and distance code lengths, each 0, 1, 2, 8
// or 9, into code length code symbols, runs of zeros as 17 and 18.
func lengthsOf(lengths []int) []clSym {
	var out []clSym
	for i := 0; i < len(lengths); {
		run := 0
		for i+run < len(lengths) && lengths[i+run] == 0 && run < 138 {
			run++
		}
		switch {
		case run >= 11:
			out = append(out, clSym{18, uint64(run - 11)})
			i += run
		case run >= 3:
			out = append(out, clSym{17, uint64(run - 3)})
			i += run
		default:
			out = append(out, clSym{sym: lengths[i]})
			i++
		}
	}
	return out
}

// litLengths is n literal/length code lengths with those of set as given.
func litLengths(n int, set map[int]int) []int {
	l := make([]int, n)
	for s, n := range set {
		l[s] = n
	}
	return l
}

// TestDynamicBlocks decodes hand-made dynamic blocks: ones a compressor would
// not write, but the format allows, and every way the header of one can be
// wrong.
func TestDynamicBlocks(t *testing.T) {
	block := func(nlen, ndist, hclen int, cl [19]uint8, syms []clSym, codes func(w *bitWriter)) []byte {
		var w bitWriter
		dynamicBlock(&w, nlen, ndist, hclen, cl, syms)
		if codes != nil {
			codes(&w)
		}
		// Room for the decoder to look ahead by, as it does a whole symbol.
		w.write(0, 32)
		return w.bytes()
	}
	// 'a' and the end of the block, one bit each; dist as given.
	ab := litLengths(257, map[int]int{'a': 1, 256: 1})
	withDist := func(lit []int, dist ...int) []clSym { return lengthsOf(append(append([]int(nil), lit...), dist...)) }

	cases := []struct {
		name string
		src  []byte
		want error
		out  string
	}{
		{
			// A block with no distance codes at all is fine while it
			// copies nothing.
			"no distance codes", block(257, 1, 19, completeCL(), withDist(ab, 0), func(w *bitWriter) {
				w.writeCode(0, 1) // 'a'
				w.writeCode(1, 1) // end of block
			}), nil, "a",
		},
		{
			"a match with no distance codes", block(258, 1, 19, completeCL(),
				lengthsOf(append(litLengths(258, map[int]int{'a': 2, 256: 2, 257: 1}), 0)), func(w *bitWriter) {
					// 257 has the one-bit code, 'a' and 256 the two-bit ones.
					w.writeCode(0b10, 2) // 'a'
					w.writeCode(0b0, 1)  // length 3, then no distance
				}), ErrCorrupt, "",
		},
		{
			// A single one-bit code is allowed incomplete; the bit it
			// leaves unused is no symbol.
			"an unused code", block(257, 1, 19, completeCL(), withDist(litLengths(257, map[int]int{256: 1}), 1), func(w *bitWriter) {
				w.writeCode(1, 1)
				w.write(0xffff, 16)
			}), ErrCorrupt, "",
		},
		{"too many literal codes", block(288, 1, 19, completeCL(), nil, nil), ErrCorrupt, ""},
		{"too many distance codes", block(257, 31, 19, completeCL(), nil, nil), ErrCorrupt, ""},
		{"an incomplete code length code", block(257, 1, 4, [19]uint8{16: 3}, nil, nil), ErrCorrupt, ""},
		{"no code length code", block(257, 1, 4, [19]uint8{}, nil, nil), ErrCorrupt, ""},
		{"an oversubscribed code length code", block(257, 1, 4, [19]uint8{16: 1, 17: 1, 18: 1}, nil, nil), ErrCorrupt, ""},
		{"a repeat with nothing before it", block(257, 1, 19, completeCL(), []clSym{{16, 0}}, nil), ErrCorrupt, ""},
		{"a repeat past the end", block(257, 1, 19, completeCL(), []clSym{{18, 127}, {18, 127}, {18, 127}}, nil), ErrCorrupt, ""},
		{"no end of block", block(257, 1, 19, completeCL(), withDist(litLengths(257, map[int]int{'a': 1, 'b': 1}), 1), nil), ErrCorrupt, ""},
		{"an oversubscribed literal code", block(257, 1, 19, completeCL(), withDist(litLengths(257, map[int]int{'a': 1, 'b': 1, 256: 1}), 1), nil), ErrCorrupt, ""},
		{"an incomplete literal code", block(257, 1, 19, completeCL(), withDist(litLengths(257, map[int]int{'a': 1, 256: 2}), 1), nil), ErrCorrupt, ""},
		{"an oversubscribed distance code", block(257, 3, 19, completeCL(), withDist(ab, 1, 1, 1), nil), ErrCorrupt, ""},
	}
	for _, c := range cases {
		for _, in := range []int{1, 1 << 20} {
			out, err := decodeSplit(Raw, c.src, in, 64)
			if !errors.Is(err, c.want) && !(c.want == nil && errors.Is(err, ErrTrailingData)) {
				t.Errorf("%s, pieces of %d: %v, want %v", c.name, in, err, c.want)
			}
			if c.want == nil && !bytes.Equal(out, []byte(c.out)) {
				t.Errorf("%s, pieces of %d: %q, want %q", c.name, in, out, c.out)
			}
		}
	}
}

// Fixed codes have literal/length symbols 286 and 287 and distance symbols
// 30 and 31, which stand for nothing.
func TestFixedCodesUnusedSymbols(t *testing.T) {
	var lit, dist bitWriter
	for _, w := range []*bitWriter{&lit, &dist} {
		w.write(1, 1) // final
		w.write(1, 2) // fixed codes
	}
	lit.writeCode(0b11000110, 8) // 286
	dist.writeCode(0b0000001, 7) // length 3
	dist.writeCode(0b11110, 5)   // distance symbol 30
	for name, src := range map[string][]byte{"length 286": lit.bytes(), "distance 30": dist.bytes()} {
		src = append(src, 0, 0, 0, 0, 0, 0)
		if _, err := decodeSplit(Raw, src, 1<<20, 64); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
}
