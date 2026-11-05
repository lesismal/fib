package tls

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"testing"
)

// referenceSpans is what the spans of fragments at the given [start, end)
// ranges should be: sorted, merged where they overlap or touch.
func referenceSpans(ranges [][2]int) [][2]int {
	var out [][2]int
	for _, r := range slices.SortedFunc(slices.Values(ranges), func(a, b [2]int) int { return a[0] - b[0] }) {
		if n := len(out); n > 0 && r[0] <= out[n-1][1] {
			out[n-1][1] = max(out[n-1][1], r[1])
		} else {
			out = append(out, r)
		}
	}
	return out
}

// Fragments merged where they fall leave the spans that sorting and merging
// them all would, and the bytes each brought.
func TestDTLSMessageSpansMerge(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for round := 0; round < 2000; round++ {
		length := 1 + rng.IntN(300)
		want := make([]byte, length)
		for i := range want {
			want[i] = byte(rng.Uint32())
		}
		m := &dtlsMessage{body: make([]byte, length)}
		var ranges [][2]int
		for k := 0; k < 1+rng.IntN(40); k++ {
			off := rng.IntN(length)
			end := off + rng.IntN(min(length-off, 1+rng.IntN(20))+1)
			m.add(off, want[off:end])
			if end > off {
				ranges = append(ranges, [2]int{off, end})
			}
		}
		if ref := referenceSpans(ranges); !slices.Equal(m.spans, ref) {
			t.Fatalf("round %d: spans %v, want %v (from %v)", round, m.spans, ref, ranges)
		}
		for _, s := range m.spans {
			if !bytes.Equal(m.body[s[0]:s[1]], want[s[0]:s[1]]) {
				t.Fatalf("round %d: the bytes of %v differ", round, s)
			}
		}
		if m.complete() != (len(m.spans) == 1 && m.spans[0] == [2]int{0, length}) {
			t.Fatalf("round %d: complete %v with %v", round, m.complete(), m.spans)
		}
	}
}

// A message cut into fragments one byte apart keeps no more than dtlsMaxSpans
// of them; one that fills a gap or extends a span is still taken, and the
// message completes once its gaps are filled.
func TestDTLSMessageSpansBounded(t *testing.T) {
	const length = 4 * dtlsMaxSpans
	body := bytes.Repeat([]byte{7}, length)
	m := &dtlsMessage{body: make([]byte, length)}
	for off := 0; off < length; off += 2 {
		m.add(off, body[off:off+1])
	}
	if len(m.spans) != dtlsMaxSpans {
		t.Fatalf("%d spans", len(m.spans))
	}
	for off := 1; off < length; off += 2 {
		m.add(off, body[off:off+1])
	}
	m.add(0, body)
	if !m.complete() || !bytes.Equal(m.body, body) {
		t.Fatalf("not complete: %v", m.spans)
	}
}
