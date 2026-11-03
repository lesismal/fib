package qpack

import (
	"hash/maphash"
	"strings"
	"sync"
)

// Encoder encodes the field sections of one connection with a dynamic
// table, which it fills on its encoder stream. It inserts a field the
// second time it sees it, as long as it fits, and from then on sends an
// index for it. An entry the peer's decoder has not acknowledged yet may
// have the peer's stream wait for the encoder stream: the encoder refers
// to one only on as many streams at once as the peer's
// SETTINGS_QPACK_BLOCKED_STREAMS allows, and with none allowed, waits for
// the acknowledgment, which leaves the field sent whole a third time. A
// sensitive field, which its caller marks never-indexed, is sent as before,
// never indexed, unless it is a cookie long enough not to be guessed (see
// indexable).
//
// An Encoder is safe for concurrent use; a section holds it from Begin to
// End. A nil *Encoder encodes without a dynamic table, as AppendField does.
type Encoder struct {
	mu sync.Mutex
	t  table
	// maxCapacity is the most this side lets its table take, and
	// peerEntries what the required insert count of a section wraps at,
	// from the capacity the peer allows (RFC 9204 section 4.5.1.1). The
	// table is used once SetPeerSettings has set its capacity above zero.
	maxCapacity, peerEntries uint64
	// known is the insert count the peer has acknowledged: the entries
	// below it are the ones a section may refer to without its stream
	// waiting. peerBlocked is how many streams the peer lets wait, and
	// maxRIC the largest required insert count of a section sent: none
	// waits once known has reached it.
	known, peerBlocked, maxRIC uint64
	// fields and names find the newest entry with a field and with a name.
	fields map[HeaderField]uint64
	names  map[string]uint64
	// unacked holds, by stream, the sections sent that referred to the
	// table and are not acknowledged yet, oldest first: their entries
	// cannot be evicted. pending counts them.
	unacked map[uint64][]sentSection
	pending int
	// seen remembers the fields encoded lately, by hash, which is how a
	// field is inserted only once it repeats: a hash may be kept in either
	// of its set's two ways, and a field seen for the first time takes the
	// place of one picked by evict, pseudo-randomly, so that no fields that
	// repeat in step can keep pushing each other out.
	seen  [seenSets][2]uint64
	evict uint64
	// out is what is to be written on the encoder stream, which flushing
	// says a goroutine is doing; write writes it. in holds the start of a
	// decoder instruction that has not all arrived.
	out, spare []byte
	flushing   bool
	write      func([]byte) error
	in         []byte
}

// sentSection is a section that referred to the table: its required insert
// count, and the oldest entry it referred to.
type sentSection struct {
	ric, oldest uint64
}

const (
	// seenSets is how many sets of two fields an Encoder remembers having
	// seen.
	seenSets = 128
	// maxPending bounds the sections a connection has unacknowledged that
	// refer to the table; past it, sections refer to none until the peer
	// acknowledges some, which bounds what a peer that never does costs.
	maxPending = 1024
	// minIndexedCookie is the shortest cookie or set-cookie value the
	// table takes when its caller marks it never-indexed; see indexable.
	minIndexedCookie = 20
)

var seenSeed = maphash.MakeSeed()

// NewEncoder returns an Encoder whose table takes up to maxCapacity bytes,
// once the peer allows that much, and which writes its encoder stream with
// write: in order, and never while it holds the Encoder.
func NewEncoder(maxCapacity uint64, write func([]byte) error) *Encoder {
	return &Encoder{
		maxCapacity: maxCapacity,
		fields:      make(map[HeaderField]uint64),
		names:       make(map[string]uint64),
		unacked:     make(map[uint64][]sentSection),
		write:       write,
	}
}

// SetPeerSettings is the peer's SETTINGS_QPACK_MAX_TABLE_CAPACITY and
// SETTINGS_QPACK_BLOCKED_STREAMS: the table takes the lesser of the first
// and the Encoder's own maximum, which the encoder stream announces. It is
// called once.
func (e *Encoder) SetPeerSettings(peerMax, peerBlocked uint64) {
	e.mu.Lock()
	capacity := min(e.maxCapacity, peerMax)
	if capacity == 0 || e.t.capacity != 0 {
		e.mu.Unlock()
		return
	}
	e.t.capacity, e.peerBlocked = capacity, peerBlocked
	e.peerEntries = peerMax / entryOverhead
	e.out = appendInt(e.out, 0x20, 5, capacity)
	e.flush()
}

// Counts reports the entries the Encoder has inserted, its insert count,
// and how many of them the peer has acknowledged, its known received count
// (RFC 9204 section 2.1.4).
func (e *Encoder) Counts() (inserted, known uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.t.inserted, e.known
}

// Section is a field section an Encoder is encoding.
type Section struct {
	e *Encoder
	// start is where the section's prefix is in the block, and base the
	// insert count the section's references count from: back for the
	// entries below it, on for those inserted since, which only a section
	// that may block refers to.
	start int
	base  uint64
	// ric is one past the newest entry referred to, oldest the oldest.
	ric, oldest uint64
	// streamID is the stream the section is sent on.
	streamID uint64
	// refer says the section may refer to the table, and block that it may
	// refer to entries the peer has not acknowledged.
	refer, block bool
}

// Begin starts the section of streamID on dst, holding the Encoder until
// End.
func (e *Encoder) Begin(dst []byte, streamID uint64) (Section, []byte) {
	if e == nil {
		return Section{}, append(dst, Prefix...)
	}
	e.mu.Lock()
	s := Section{e: e, start: len(dst), base: e.known, oldest: ^uint64(0), streamID: streamID}
	s.refer = e.t.capacity > 0 && e.pending < maxPending
	if s.refer && e.mayBlock(streamID) {
		s.block, s.base = true, e.t.inserted
	}
	return s, append(dst, Prefix...)
}

// mayBlock reports whether a section of streamID may have the stream wait:
// it is already one that may, or fewer streams may than the peer allows.
func (e *Encoder) mayBlock(streamID uint64) bool {
	if e.peerBlocked == 0 || e.maxRIC <= e.known {
		return e.peerBlocked > 0
	}
	var blocking uint64
	for id, sections := range e.unacked {
		for _, sent := range sections {
			if sent.ric > e.known {
				if id == streamID {
					return true
				}
				blocking++
				break
			}
		}
	}
	return blocking < e.peerBlocked
}

// Append appends one field line, as AppendField does, referring to the
// table where it can.
func (s *Section) Append(dst []byte, name, value string, neverIndex bool) []byte {
	e := s.e
	if e == nil || e.t.capacity == 0 {
		return AppendField(dst, name, value, neverIndex)
	}
	static, _ := staticNameIndex.Get(name)
	if !neverIndex {
		for _, i := range static {
			if staticTable[i].Value == value {
				return appendInt(dst, 0xc0, 6, uint64(i))
			}
		}
	}
	indexable := indexable(name, value, neverIndex)
	f := HeaderField{name, value}
	abs, inTable := e.fields[f]
	if !inTable && indexable && e.repeated(name, value) && e.insert(f, static, s) {
		abs, inTable = e.t.inserted-1, true
	}
	if inTable && indexable && s.mayRefer(abs) {
		if abs < s.base {
			return appendInt(dst, 0x80, 6, s.base-1-abs)
		}
		// Indexed field line with a post-base index.
		return appendInt(dst, 0x10, 4, abs-s.base)
	}
	n := byte(0)
	if neverIndex {
		n = 0x20
	}
	if len(static) > 0 {
		dst = appendInt(dst, 0x50|n, 4, uint64(static[0]))
		return appendString(dst, 0, 7, value)
	}
	if abs, ok := e.names[name]; ok && s.mayRefer(abs) {
		if abs < s.base {
			dst = appendInt(dst, 0x40|n, 4, s.base-1-abs)
		} else {
			// Literal field line with a post-base name reference.
			dst = appendInt(dst, n>>2, 3, abs-s.base)
		}
		return appendString(dst, 0, 7, value)
	}
	return appendString(appendString(dst, 0x20|n>>1, 3, name), 0, 7, value)
}

// indexable reports whether the table may take a field its caller marked
// neverIndex or not. A field in the table is sent as an index, which an
// attacker who can add fields of its own to the connection's requests can
// probe for, a guess at a time, by how long the sections come out (RFC 9204
// section 7.1). So credentials, authorization and proxy-authorization, are
// never indexed, and a cookie or set-cookie only when it is long enough not
// to be guessed whole, the bound browsers put on the cookies they index;
// any other field marked never-indexed is never indexed either.
func indexable(name, value string, neverIndex bool) bool {
	if !neverIndex {
		return true
	}
	return (name == "cookie" || name == "set-cookie") && len(value) >= minIndexedCookie
}

// mayRefer reports whether the section may refer to the entry abs, which
// the table has, and notes that it does.
func (s *Section) mayRefer(abs uint64) bool {
	if !s.refer || !s.block && abs >= s.base {
		return false
	}
	s.ric = max(s.ric, abs+1)
	s.oldest = min(s.oldest, abs)
	return true
}

// End finishes the section, writes its prefix, lets go of the Encoder and
// writes what the section inserted on the encoder stream.
func (s *Section) End(dst []byte) []byte {
	e := s.e
	if e == nil {
		return dst
	}
	if s.ric > 0 {
		e.unacked[s.streamID] = append(e.unacked[s.streamID], sentSection{ric: s.ric, oldest: s.oldest})
		e.pending++
		e.maxRIC = max(e.maxRIC, s.ric)
		var prefix [20]byte
		p := appendInt(prefix[:0], 0, 8, s.ric%(2*e.peerEntries)+1)
		if s.base >= s.ric {
			p = appendInt(p, 0, 7, s.base-s.ric)
		} else {
			// A base below the required insert count, with the sign bit.
			p = appendInt(p, 0x80, 7, s.ric-s.base-1)
		}
		if grow := len(p) - len(Prefix); grow > 0 {
			dst = append(dst, p[:grow]...)
			copy(dst[s.start+len(p):], dst[s.start+len(Prefix):len(dst)-grow])
		}
		copy(dst[s.start:], p)
	}
	e.flush()
	return dst
}

// repeated reports whether the field was seen lately, and remembers it.
func (e *Encoder) repeated(name, value string) bool {
	h := seenHash(name, value)
	set := &e.seen[h%seenSets]
	if set[0] == h || set[1] == h {
		return true
	}
	// A free way first, else one by the top bit of a linear congruential
	// generator's next value.
	way := 0
	if set[0] != 0 {
		way = 1
		if set[1] != 0 {
			e.evict = e.evict*6364136223846793005 + 1442695040888963407
			way = int(e.evict >> 63)
		}
	}
	set[way] = h
	return false
}

// seenHash is the hash a field is remembered by in an Encoder's seen.
func seenHash(name, value string) uint64 {
	return maphash.String(seenSeed, name)*31 ^ maphash.String(seenSeed, value)
}

// insert adds f to the table, if room can be made for it by evicting
// entries no unacknowledged section refers to, nor s, writes the insertion
// on the encoder stream and reports whether it did. static is the static
// table's entries with f's name.
func (e *Encoder) insert(f HeaderField, static []uint8, s *Section) bool {
	size := entrySize(f.Name, f.Value)
	if size > e.t.capacity {
		return false
	}
	if e.t.size+size > e.t.capacity {
		// The oldest entry a section in flight still needs.
		keep := min(s.oldest, e.t.inserted)
		for _, sections := range e.unacked {
			for _, sent := range sections {
				keep = min(keep, sent.oldest)
			}
		}
		// Only what can all go is evicted.
		need, abs := e.t.size+size-e.t.capacity, e.t.dropped()
		for freed := uint64(0); freed < need; abs++ {
			if abs >= keep {
				return false
			}
			old := e.t.entry(abs)
			freed += entrySize(old.Name, old.Value)
		}
		for e.t.dropped() < abs {
			old, at := e.t.evict(), e.t.dropped()-1
			if e.fields[old] == at {
				delete(e.fields, old)
			}
			if e.names[old.Name] == at {
				delete(e.names, old.Name)
			}
		}
	}
	if len(static) > 0 {
		e.out = appendInt(e.out, 0xc0, 6, uint64(static[0]))
	} else {
		e.out = appendString(e.out, 0x40, 5, f.Name)
	}
	e.out = appendString(e.out, 0, 7, f.Value)
	// The table keeps copies: the caller's strings may be its own buffers,
	// which Append, like AppendField, does not hold on to.
	kept := HeaderField{strings.Clone(f.Name), strings.Clone(f.Value)}
	e.fields[kept] = e.t.inserted
	e.names[kept.Name] = e.t.inserted
	e.t.add(kept)
	return true
}

// flush lets go of the Encoder, which the caller holds, and writes out on
// the encoder stream, unless another goroutine is writing already: that
// one writes it next, so the stream carries the instructions in the order
// they were made, and a write that runs the connection's callbacks on this
// goroutine, an encoding among them, finds no lock held.
func (e *Encoder) flush() {
	if e.flushing || len(e.out) == 0 {
		e.mu.Unlock()
		return
	}
	e.flushing = true
	for len(e.out) > 0 {
		out := e.out
		e.out, e.spare = e.spare[:0], nil
		e.mu.Unlock()
		_ = e.write(out)
		e.mu.Lock()
		if e.spare == nil {
			e.spare = out[:0]
		}
	}
	e.flushing = false
	e.mu.Unlock()
}

// FeedDecoder carries out the instructions of the peer's decoder stream in
// data, keeping the start of one that has not all arrived for the next
// call. An instruction that does not fit what was sent is
// ErrDecoderStream.
func (e *Encoder) FeedDecoder(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	buf := data
	if len(e.in) > 0 {
		e.in = append(e.in, data...)
		buf = e.in
	}
	for len(buf) > 0 {
		c, prefix := buf[0], uint8(6)
		if c&0x80 != 0 {
			prefix = 7
		}
		v, k, err := parseInt(buf, prefix)
		if err != nil {
			return ErrDecoderStream
		}
		if k == 0 {
			break
		}
		buf = buf[k:]
		switch {
		case c&0x80 != 0:
			// Section Acknowledgment: the stream's oldest section.
			sections := e.unacked[v]
			if len(sections) == 0 {
				return ErrDecoderStream
			}
			e.known = max(e.known, sections[0].ric)
			if len(sections) == 1 {
				delete(e.unacked, v)
			} else {
				e.unacked[v] = sections[1:]
			}
			e.pending--
		case c&0x40 != 0:
			// Stream Cancellation.
			e.pending -= len(e.unacked[v])
			delete(e.unacked, v)
		default:
			// Insert Count Increment.
			if v == 0 || v > e.t.inserted-e.known {
				return ErrDecoderStream
			}
			e.known += v
		}
	}
	e.in = append(e.in[:0], buf...)
	return nil
}
