package qpack

// The dynamic table (RFC 9204 section 3.2), as the decoder and the encoder
// each keep it: entries in the order they were inserted, the oldest evicted
// first, each known by its absolute index, which counts insertions from
// zero.

// entryOverhead is what an entry weighs beyond its name and value.
const entryOverhead = 32

// table is a dynamic table. Its entries are a ring, so that evicting the
// oldest moves nothing.
type table struct {
	ring []HeaderField
	// head is where in ring the oldest entry is, and n how many there are.
	head, n int
	// inserted is the insert count: the absolute index the next entry gets.
	inserted uint64
	// size is what the entries weigh; capacity what they may.
	size, capacity uint64
}

// entrySize is what a field weighs in the table.
func entrySize(name, value string) uint64 {
	return uint64(len(name)+len(value)) + entryOverhead
}

// dropped is the absolute index of the oldest entry, which is the number of
// entries evicted.
func (t *table) dropped() uint64 { return t.inserted - uint64(t.n) }

// entry is the entry of absolute index abs, which the caller has checked is
// in the table.
func (t *table) entry(abs uint64) HeaderField {
	return t.ring[(t.head+int(abs-t.dropped()))%len(t.ring)]
}

// has reports whether the entry of absolute index abs is in the table.
func (t *table) has(abs uint64) bool { return abs < t.inserted && abs >= t.dropped() }

// evict drops the oldest entry.
func (t *table) evict() HeaderField {
	f := t.ring[t.head]
	t.ring[t.head] = HeaderField{}
	t.head = (t.head + 1) % len(t.ring)
	t.n--
	t.size -= entrySize(f.Name, f.Value)
	return f
}

// add inserts f, which the caller has made room for.
func (t *table) add(f HeaderField) {
	if t.n == len(t.ring) {
		// Every entry weighs at least entryOverhead, so the ring never
		// needs more than capacity/entryOverhead places; it grows to that
		// as the table fills.
		ring := make([]HeaderField, max(8, 2*len(t.ring)))
		for i := 0; i < t.n; i++ {
			ring[i] = t.ring[(t.head+i)%len(t.ring)]
		}
		t.ring, t.head = ring, 0
	}
	t.ring[(t.head+t.n)%len(t.ring)] = f
	t.n++
	t.inserted++
	t.size += entrySize(f.Name, f.Value)
}

// parseInt reads an integer with an n-bit prefix from the front of b (RFC
// 7541 section 5.1), reporting how many bytes it took, or zero if it is not
// all there yet; one too large to hold is an error.
func parseInt(b []byte, n uint8) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, nil
	}
	mask := uint64(1)<<n - 1
	v := uint64(b[0]) & mask
	if v < mask {
		return v, 1, nil
	}
	var shift uint
	for i := 1; i < len(b); i++ {
		v += uint64(b[i]&0x7f) << shift
		if b[i]&0x80 == 0 {
			return v, i + 1, nil
		}
		if shift += 7; shift >= 56 {
			return 0, 0, ErrDecompression
		}
	}
	return 0, 0, nil
}

// parseString reads a string whose length has an n-bit prefix, with the
// Huffman flag in the bit just above it, from the front of b, as parseInt
// does: zero bytes taken means it is not all there yet. A string whose
// encoding is longer than maxRaw, or which decodes to more than maxLen, is
// an error before any of it need arrive.
func parseString(b []byte, n uint8, maxRaw uint64, maxLen int) (string, int, error) {
	length, k, err := parseInt(b, n)
	if err != nil || k == 0 {
		return "", 0, err
	}
	if length > maxRaw {
		return "", 0, ErrTooLarge
	}
	if uint64(len(b)-k) < length {
		return "", 0, nil
	}
	s, _, err := readString(b[:k+int(length)], n, maxLen)
	if err != nil {
		return "", 0, err
	}
	return s, k + int(length), nil
}
