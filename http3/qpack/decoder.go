package qpack

// decoderTable is a Decoder's dynamic table, which the peer's encoder
// stream fills, and what the decoder owes the encoder on its own decoder
// stream (RFC 9204 sections 4.3 and 4.4).
type decoderTable struct {
	table
	// maxCapacity is the SETTINGS_QPACK_MAX_TABLE_CAPACITY this side sent,
	// the most the encoder may set the capacity to, and maxEntries what
	// the required insert count of a section wraps at (section 4.5.1.1).
	maxCapacity, maxEntries uint64
	// maxBlocked is the SETTINGS_QPACK_BLOCKED_STREAMS this side sent, and
	// blocked the streams waiting on the encoder stream now.
	maxBlocked int
	blocked    map[uint64]struct{}
	// told is the insert count the encoder has been told of, by Section
	// Acknowledgment or Insert Count Increment.
	told uint64
	// in holds the start of an encoder instruction that has not all
	// arrived, and out the decoder instructions not yet taken.
	in, out []byte
}

// EnableDynamic lets the peer's encoder use a dynamic table of up to
// maxCapacity bytes, what this side sends as SETTINGS_QPACK_MAX_TABLE_CAPACITY,
// and have up to maxBlocked streams wait for entries,
// SETTINGS_QPACK_BLOCKED_STREAMS. It is called once, before anything is
// decoded; a capacity of zero leaves the table off.
func (d *Decoder) EnableDynamic(maxCapacity uint64, maxBlocked int) {
	if maxCapacity == 0 {
		return
	}
	d.dyn = &decoderTable{
		maxCapacity: maxCapacity,
		maxEntries:  maxCapacity / entryOverhead,
		maxBlocked:  maxBlocked,
		blocked:     make(map[uint64]struct{}),
	}
}

// Dynamic reports whether EnableDynamic turned the table on.
func (d *Decoder) Dynamic() bool { return d.dyn != nil }

// Blocked reports how many streams wait on the encoder stream.
func (d *Decoder) Blocked() int {
	if d.dyn == nil {
		return 0
	}
	return len(d.dyn.blocked)
}

// AppendInstructions appends what the decoder has to send on its decoder
// stream, and forgets it.
func (d *Decoder) AppendInstructions(dst []byte) []byte {
	if d.dyn == nil || len(d.dyn.out) == 0 {
		return dst
	}
	dst = append(dst, d.dyn.out...)
	d.dyn.out = d.dyn.out[:0]
	return dst
}

// Cancel is a stream that is reset, or whose reading is abandoned, before
// its field sections were all decoded: it no longer waits, and the encoder
// is told with a Stream Cancellation, so that it can let go of the entries
// its sections refer to (RFC 9204 section 4.4.2).
func (d *Decoder) Cancel(streamID uint64) {
	if d.dyn == nil {
		return
	}
	delete(d.dyn.blocked, streamID)
	d.dyn.out = appendInt(d.dyn.out, 0x40, 6, streamID)
}

// FeedEncoder carries out the instructions of the peer's encoder stream in
// data, keeping the start of one that has not all arrived for the next
// call, and owes the encoder an Insert Count Increment for what it inserted.
// An instruction it cannot carry out is ErrEncoderStream.
func (d *Decoder) FeedEncoder(data []byte) error {
	t := d.dyn
	if t == nil {
		return ErrEncoderStream
	}
	buf := data
	if len(t.in) > 0 {
		t.in = append(t.in, data...)
		buf = t.in
	}
	for len(buf) > 0 {
		n, err := t.instruction(buf)
		if err != nil {
			return ErrEncoderStream
		}
		if n == 0 {
			break
		}
		buf = buf[n:]
	}
	t.in = append(t.in[:0], buf...)
	if t.inserted > t.told {
		t.out = appendInt(t.out, 0x00, 6, t.inserted-t.told)
		t.told = t.inserted
	}
	return nil
}

// instruction carries out the encoder instruction at the front of b,
// reporting how many bytes it took, or zero if it has not all arrived.
func (t *decoderTable) instruction(b []byte) (int, error) {
	// No string of an entry that fits weighs more than the capacity, and
	// Huffman coding takes at most four bytes for each one it codes.
	maxLen := int(t.maxCapacity)
	maxRaw := 4 * t.maxCapacity
	switch c := b[0]; {
	case c&0x80 != 0:
		// Insert With Name Reference.
		i, k, err := parseInt(b, 6)
		if err != nil || k == 0 {
			return 0, err
		}
		var name string
		if c&0x40 != 0 {
			if i >= uint64(len(staticTable)) {
				return 0, ErrEncoderStream
			}
			name = staticTable[i].Name
		} else {
			if i >= t.inserted || !t.has(t.inserted-1-i) {
				return 0, ErrEncoderStream
			}
			name = t.entry(t.inserted - 1 - i).Name
		}
		value, n, err := parseString(b[k:], 7, maxRaw, maxLen)
		if err != nil || n == 0 {
			return 0, err
		}
		return k + n, t.insert(HeaderField{name, value})
	case c&0xc0 == 0x40:
		// Insert With Literal Name.
		name, k, err := parseString(b, 5, maxRaw, maxLen)
		if err != nil || k == 0 {
			return 0, err
		}
		value, n, err := parseString(b[k:], 7, maxRaw, maxLen)
		if err != nil || n == 0 {
			return 0, err
		}
		return k + n, t.insert(HeaderField{name, value})
	case c&0xe0 == 0x20:
		// Set Dynamic Table Capacity.
		capacity, k, err := parseInt(b, 5)
		if err != nil || k == 0 {
			return 0, err
		}
		if capacity > t.maxCapacity {
			return 0, ErrEncoderStream
		}
		t.capacity = capacity
		for t.size > t.capacity {
			t.evict()
		}
		return k, nil
	default:
		// Duplicate.
		i, k, err := parseInt(b, 5)
		if err != nil || k == 0 {
			return 0, err
		}
		if i >= t.inserted || !t.has(t.inserted-1-i) {
			return 0, ErrEncoderStream
		}
		return k, t.insert(t.entry(t.inserted - 1 - i))
	}
}

// insert adds f, evicting the oldest entries to make room for it. The name
// or entry it was copied from may be among them, which is why it is copied
// first.
func (t *decoderTable) insert(f HeaderField) error {
	size := entrySize(f.Name, f.Value)
	if size > t.capacity {
		return ErrEncoderStream
	}
	for t.size+size > t.capacity {
		t.evict()
	}
	t.add(f)
	return nil
}

// requiredInsertCount decodes a section's encoded required insert count,
// which is not zero (RFC 9204 section 4.5.1.1).
func (t *decoderTable) requiredInsertCount(encoded uint64) (uint64, error) {
	full := 2 * t.maxEntries
	if encoded > full {
		return 0, ErrDecompression
	}
	maxValue := t.inserted + t.maxEntries
	ric := maxValue/full*full + encoded - 1
	if ric > maxValue {
		if ric <= full {
			return 0, ErrDecompression
		}
		ric -= full
	}
	if ric == 0 {
		return 0, ErrDecompression
	}
	return ric, nil
}

// block is a section of streamID that has to wait for entries: it is
// ErrBlocked, unless the stream would be one more waiting than this side
// allowed, which is a decompression failure (RFC 9204 section 2.1.2).
func (t *decoderTable) block(streamID uint64) error {
	if _, ok := t.blocked[streamID]; !ok {
		if len(t.blocked) >= t.maxBlocked {
			return ErrDecompression
		}
		t.blocked[streamID] = struct{}{}
	}
	return ErrBlocked
}

// relative is the entry a field line's relative index i refers to, from
// the section's base, which has to be below its required insert count;
// largest goes up to one past it.
func (t *decoderTable) relative(base, i, ric uint64, largest *uint64) (HeaderField, error) {
	if t == nil || i >= base {
		return HeaderField{}, ErrDecompression
	}
	return t.referred(base-1-i, ric, largest)
}

// postBase is the entry a field line's post-base index i refers to.
func (t *decoderTable) postBase(base, i, ric uint64, largest *uint64) (HeaderField, error) {
	if t == nil || i >= ric {
		return HeaderField{}, ErrDecompression
	}
	return t.referred(base+i, ric, largest)
}

func (t *decoderTable) referred(abs, ric uint64, largest *uint64) (HeaderField, error) {
	if abs >= ric || !t.has(abs) {
		return HeaderField{}, ErrDecompression
	}
	*largest = max(*largest, abs+1)
	return t.entry(abs), nil
}

// acknowledge ends a section of streamID that referred to the table, whose
// required insert count was ric: it waits no more, and the encoder is told
// with a Section Acknowledgment (RFC 9204 section 4.4.1).
func (t *decoderTable) acknowledge(streamID, ric uint64) {
	if t == nil || ric == 0 {
		return
	}
	delete(t.blocked, streamID)
	t.out = appendInt(t.out, 0x80, 7, streamID)
	t.told = max(t.told, ric)
}
