package qpack

import (
	"bytes"
	"encoding/hex"
	"errors"
	"hash/maphash"
	"slices"
	"strings"
	"testing"
)

// pair is an Encoder and the Decoder on the other end of a connection,
// with what the encoder stream carries between them.
type pair struct {
	enc     *Encoder
	dec     Decoder
	encoder []byte
}

func newPair(capacity uint64, blocked int) *pair {
	p := &pair{}
	p.enc = NewEncoder(capacity, func(b []byte) error {
		p.encoder = append(p.encoder, b...)
		return nil
	})
	p.dec.EnableDynamic(capacity, blocked)
	p.enc.SetPeerSettings(capacity, uint64(blocked))
	return p
}

// distinctSeenSlots sets seenSeed, for the rest of t, to one under which the
// fields each take a slot of their own in an Encoder's seen, so that a test
// that counts on every repeated field being indexed does not depend on the
// seed the package started with.
func distinctSeenSlots(t *testing.T, fields []HeaderField) {
	t.Helper()
	old := seenSeed
	t.Cleanup(func() { seenSeed = old })
	for range 1000 {
		slots := make(map[uint64]bool, len(fields))
		for _, f := range fields {
			slots[seenHash(f.Name, f.Value)%seenSlots] = true
		}
		if len(slots) == len(fields) {
			return
		}
		seenSeed = maphash.MakeSeed()
	}
	t.Fatal("no seed found that keeps the fields apart")
}

// sensitive is what package http3 marks never-indexed.
func sensitive(name string) bool {
	return name == "authorization" || name == "proxy-authorization" || name == "cookie" || name == "set-cookie"
}

// neverIndexed reports whether a field line, a literal, has its
// never-indexed bit set.
func neverIndexed(line byte) bool {
	if line&0xc0 == 0x40 {
		return line&0x20 != 0
	}
	return line&0xe0 == 0x20 && line&0x10 != 0
}

func (p *pair) encode(fields []HeaderField, streamID uint64) []byte {
	s, block := p.enc.Begin(nil, streamID)
	for _, f := range fields {
		block = s.Append(block, f.Name, f.Value, sensitive(f.Name))
	}
	return s.End(block)
}

// carry delivers the encoder stream to the decoder, and the decoder's
// instructions back to the encoder.
func (p *pair) carry(t *testing.T) {
	t.Helper()
	p.deliver(t)
	if err := p.enc.FeedDecoder(p.dec.AppendInstructions(nil)); err != nil {
		t.Fatalf("decoder stream: %v", err)
	}
}

// deliver delivers the encoder stream to the decoder.
func (p *pair) deliver(t *testing.T) {
	t.Helper()
	if err := p.dec.FeedEncoder(p.encoder); err != nil {
		t.Fatalf("encoder stream: %v", err)
	}
	p.encoder = nil
}

// decodeLate decodes a section that may wait on the encoder stream, which
// is then delivered.
func (p *pair) decodeLate(t *testing.T, block []byte, streamID uint64) ([]HeaderField, bool) {
	t.Helper()
	fields, err := p.dec.AppendSection(nil, block, 0, streamID)
	if !errors.Is(err, ErrBlocked) {
		if err != nil {
			t.Fatalf("decoding: %v", err)
		}
		return fields, false
	}
	p.deliver(t)
	return p.decode(t, block, streamID), true
}

func (p *pair) decode(t *testing.T, block []byte, streamID uint64) []HeaderField {
	t.Helper()
	fields, err := p.dec.AppendSection(nil, block, 0, streamID)
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	return fields
}

func sameFields(t *testing.T, got, want []HeaderField) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d fields %q, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("field %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestDynamicExample decodes RFC 9204 appendix B.2 to B.5, which fill the
// dynamic table, refer to it by post-base and relative index, duplicate an
// entry and insert one with a dynamic name reference.
func TestDynamicExample(t *testing.T) {
	var d Decoder
	d.EnableDynamic(220, 1)
	// B.2: two insertions, and a section that refers to both after its
	// base.
	if err := d.FeedEncoder(unhex(t, "3fbd01 c00f7777772e6578616d706c652e636f6d c10c2f73616d706c652f70617468")); err != nil {
		t.Fatal(err)
	}
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x02}) {
		t.Fatalf("B.2 decoder stream %x, want an increment of 2", got)
	}
	fields, err := d.AppendSection(nil, unhex(t, "0381 10 11"), 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	sameFields(t, fields, []HeaderField{{":authority", "www.example.com"}, {":path", "/sample/path"}})
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x84}) {
		t.Fatalf("B.2 decoder stream %x, want 84", got)
	}
	// B.3: an insertion with a literal name.
	if err := d.FeedEncoder(unhex(t, "4a637573746f6d2d6b65790c637573746f6d2d76616c7565")); err != nil {
		t.Fatal(err)
	}
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x01}) {
		t.Fatalf("B.3 decoder stream %x, want 01", got)
	}
	// B.4: a duplicate, and a section that refers to entries before its
	// base; the stream is then cancelled.
	if err := d.FeedEncoder([]byte{0x02}); err != nil {
		t.Fatal(err)
	}
	fields, err = d.AppendSection(nil, unhex(t, "0500 80 c1 81"), 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	sameFields(t, fields, []HeaderField{{":authority", "www.example.com"}, {":path", "/"}, {"custom-key", "custom-value"}})
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x01, 0x88}) {
		t.Fatalf("B.4 decoder stream %x, want 01 88", got)
	}
	d.Cancel(8)
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x48}) {
		t.Fatalf("B.4 decoder stream %x, want 48", got)
	}
	// B.5: an insertion naming a dynamic entry, which evicts the oldest.
	if err := d.FeedEncoder(unhex(t, "810d637573746f6d2d76616c756532")); err != nil {
		t.Fatal(err)
	}
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x01}) {
		t.Fatalf("B.5 decoder stream %x, want 01", got)
	}
	if d.dyn.dropped() != 1 || d.dyn.size != 215 {
		t.Fatalf("table holds %d bytes from entry %d, want 215 from 1", d.dyn.size, d.dyn.dropped())
	}
	if f := d.dyn.entry(4); f != (HeaderField{"custom-key", "custom-value2"}) {
		t.Fatalf("entry 4 is %q", f)
	}
}

// TestDynamicRoundTrip has an Encoder and a Decoder carry the same request
// over and over: the third and later sections refer to the table, and each
// decodes to what was encoded.
func TestDynamicRoundTrip(t *testing.T) {
	fields := []HeaderField{
		{":method", "GET"},
		{":scheme", "https"},
		{":authority", "api.example.com"},
		{":path", "/v1/items"},
		{"authorization", "Bearer " + strings.Repeat("t0k3n", 30)},
		{"cookie", "session=" + strings.Repeat("c", 400)},
		{"x-rpc-service", "inventory.v1.Items/List"},
		{"x-request-id", "1"},
		{"user-agent", "Go-http-client/3"},
	}
	// The encoder remembers what it has seen in one slot a hash, so a
	// field whose slot the changing request ID shares is never seen twice
	// and never indexed, as the seed falls.
	seen := slices.Clone(fields)
	for i := 0; i < 5; i++ {
		seen = append(seen, HeaderField{"x-request-id", string(rune('a' + i))})
	}
	distinctSeenSlots(t, seen)
	p := newPair(4096, 0)
	plain := encode(fields)
	var sizes []int
	for i := 0; i < 5; i++ {
		fields[7].Value = string(rune('a' + i))
		id := uint64(4 * i)
		block := p.encode(fields, id)
		sizes = append(sizes, len(block))
		sameFields(t, p.decode(t, block, id), fields)
		p.carry(t)
	}
	if sizes[0] < len(plain)-8 || sizes[1] < len(plain)-8 {
		t.Fatalf("first sections take %v bytes, want about %d", sizes[:2], len(plain))
	}
	// All but the bearer token, which is never indexed, and the request
	// ID, which changes.
	token := len(AppendField(nil, fields[4].Name, fields[4].Value, true))
	if sizes[2] > token+40 || sizes[4] > token+40 {
		t.Fatalf("later sections take %v bytes, want all but the %d-byte token indexed", sizes, token)
	}
	if p.enc.pending != 0 {
		t.Fatalf("%d sections unacknowledged", p.enc.pending)
	}
	if inserted, known := p.enc.Counts(); inserted == 0 || known != inserted {
		t.Fatalf("%d entries inserted, %d acknowledged", inserted, known)
	}
}

// TestDynamicBlockingEncoder: with blocked streams allowed, a field is
// indexed as soon as it is inserted, by a post-base index, and so is the
// name of another value of it; the section waits for the encoder stream,
// and only as many streams wait as the peer allows.
func TestDynamicBlockingEncoder(t *testing.T) {
	p := newPair(4096, 1)
	one := HeaderField{"x-custom-header", "one"}
	p.decode(t, p.encode([]HeaderField{one}, 0), 0)
	both := []HeaderField{one, {"x-custom-header", "two"}}
	block := p.encode(both, 4)
	// A required insert count of one, a base of zero, a post-base index
	// and a post-base name reference.
	if !bytes.Equal(block[:4], []byte{0x02, 0x80, 0x10, 0x00}) {
		t.Fatalf("encoded as %x", block)
	}
	fields, waited := p.decodeLate(t, block, 4)
	if !waited {
		t.Fatal("decoded before the encoder stream came")
	}
	sameFields(t, fields, both)
	// Unacknowledged, the entry may have stream 4 wait again, but no other
	// stream, as the peer allows one.
	again := p.encode([]HeaderField{one}, 4)
	other := p.encode([]HeaderField{one}, 8)
	if again[0] == 0 || other[0] != 0 {
		t.Fatalf("stream 4 encoded %x, stream 8 %x", again, other)
	}
	sameFields(t, p.decode(t, again, 4), []HeaderField{one})
	sameFields(t, p.decode(t, other, 8), []HeaderField{one})
	p.carry(t)
	// Acknowledged, it blocks nothing, and every stream refers to it.
	if block = p.encode([]HeaderField{one}, 12); len(block) != 3 {
		t.Fatalf("encoded as %x", block)
	}
	sameFields(t, p.decode(t, block, 12), []HeaderField{one})
	p.carry(t)
}

// TestDynamicSecrets: credentials and short cookies are never indexed,
// however often and however long they are sent; a long cookie is.
func TestDynamicSecrets(t *testing.T) {
	p := newPair(4096, 0)
	long := []HeaderField{{"cookie", strings.Repeat("s", minIndexedCookie)}}
	for _, never := range []HeaderField{
		{"authorization", "Basic YTpi"},
		{"authorization", "Bearer " + strings.Repeat("t", 200)},
		{"proxy-authorization", strings.Repeat("p", 100)},
		{"cookie", strings.Repeat("s", minIndexedCookie-1)},
		{"set-cookie", strings.Repeat("s", minIndexedCookie-1)},
	} {
		for i := 0; i < 4; i++ {
			block := p.encode([]HeaderField{never}, 0)
			if block[0] != 0 || !neverIndexed(block[2]) {
				t.Fatalf("%s encoded as %x, want a never-indexed literal", never.Name, block)
			}
			sameFields(t, p.decode(t, block, 0), []HeaderField{never})
			p.carry(t)
		}
	}
	if inserted, _ := p.enc.Counts(); inserted != 0 {
		t.Fatalf("%d secrets inserted", inserted)
	}
	var last []byte
	for i := 0; i < 4; i++ {
		last = p.encode(long, 4)
		sameFields(t, p.decode(t, last, 4), long)
		p.carry(t)
	}
	if len(last) != 3 {
		t.Fatalf("long secret encoded as %x, want an index", last)
	}
}

// TestDynamicNameReference: once a custom field is in the table, another
// value of it refers to the entry's name.
func TestDynamicNameReference(t *testing.T) {
	p := newPair(4096, 0)
	f := []HeaderField{{"x-custom-header-name", "one"}}
	for i := 0; i < 3; i++ {
		sameFields(t, p.decode(t, p.encode(f, 0), 0), f)
		p.carry(t)
	}
	other := []HeaderField{{"x-custom-header-name", "two"}}
	block := p.encode(other, 4)
	if block[2]&0xf0 != 0x40 {
		t.Fatalf("encoded as %x, want a dynamic name reference", block)
	}
	sameFields(t, p.decode(t, block, 4), other)
}

// TestDynamicEviction fills a small table with more fields than it holds:
// old entries are evicted as new ones come, never one an unacknowledged
// section refers to, and everything still decodes.
func TestDynamicEviction(t *testing.T) {
	p := newPair(200, 0)
	for round := 0; round < 3; round++ {
		for i := 0; i < 10; i++ {
			name := "x-field"
			if round == 0 {
				// Names of their own, which eviction forgets with them.
				name += string(rune('a' + i))
			}
			f := []HeaderField{{name, strings.Repeat(string(rune('a'+i)), 60)}}
			for j := 0; j < 3; j++ {
				id := uint64(4 * (round*30 + i*3 + j))
				sameFields(t, p.decode(t, p.encode(f, id), id), f)
				p.carry(t)
			}
		}
	}
	if p.enc.t.size > 200 || p.dec.dyn.size > 200 {
		t.Fatalf("tables at %d and %d bytes, past 200", p.enc.t.size, p.dec.dyn.size)
	}
	// A section the peer has not acknowledged keeps its entries: a new
	// field that needs their room is sent as a literal, not inserted.
	f := []HeaderField{{"x-field", strings.Repeat("j", 60)}}
	held := p.encode(f, 1000)
	if held[0] == 0 {
		t.Fatalf("encoded as %x, want a reference", held)
	}
	before := p.enc.t.inserted
	for i := 0; i < 3; i++ {
		g := []HeaderField{{"x-other", strings.Repeat("k", 120)}}
		p.decode(t, p.encode(g, 2000), 2000)
	}
	if p.enc.t.inserted != before {
		t.Fatalf("inserted %d entries over one an unacknowledged section holds", p.enc.t.inserted-before)
	}
	sameFields(t, p.decode(t, held, 1000), f)
	p.carry(t)
}

// TestDynamicBlocked: a section that refers to entries still to come
// waits, up to the blocked streams allowed, and decodes once they come; a
// stream given up on is cancelled.
func TestDynamicBlocked(t *testing.T) {
	var d Decoder
	d.EnableDynamic(4096, 1)
	// One entry, ":authority: a.example", and a section that needs it.
	insert := append([]byte{0x3f, 0xe1, 0x1f}, appendString([]byte{0xc0}, 0, 7, "a.example")...)
	section := []byte{0x02, 0x00, 0x80}
	if _, err := d.AppendSection(nil, section, 0, 0); !errors.Is(err, ErrBlocked) {
		t.Fatalf("got %v, want ErrBlocked", err)
	}
	if _, err := d.AppendSection(nil, section, 0, 0); !errors.Is(err, ErrBlocked) {
		t.Fatalf("again: got %v, want ErrBlocked", err)
	}
	if d.Blocked() != 1 {
		t.Fatalf("%d streams blocked, want 1", d.Blocked())
	}
	if _, err := d.AppendSection(nil, section, 0, 4); !errors.Is(err, ErrDecompression) {
		t.Fatalf("a second blocked stream: got %v, want ErrDecompression", err)
	}
	if err := d.FeedEncoder(insert[:4]); err != nil {
		t.Fatal(err)
	}
	if err := d.FeedEncoder(insert[4:]); err != nil {
		t.Fatal(err)
	}
	fields, err := d.AppendSection(nil, section, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sameFields(t, fields, []HeaderField{{":authority", "a.example"}})
	if d.Blocked() != 0 {
		t.Fatalf("%d streams blocked, want 0", d.Blocked())
	}
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x01, 0x80}) {
		t.Fatalf("decoder stream %x, want 01 80", got)
	}
	// Cancelled while blocked: the stream no longer counts.
	section = []byte{0x03, 0x00, 0x80}
	if _, err := d.AppendSection(nil, section, 0, 8); !errors.Is(err, ErrBlocked) {
		t.Fatalf("got %v, want ErrBlocked", err)
	}
	d.Cancel(8)
	if d.Blocked() != 0 {
		t.Fatalf("%d streams blocked after Cancel", d.Blocked())
	}
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x48}) {
		t.Fatalf("decoder stream %x, want 48", got)
	}
	if _, err := d.AppendSection(nil, section, 0, 12); !errors.Is(err, ErrBlocked) {
		t.Fatalf("another stream: got %v, want ErrBlocked", err)
	}
	// Too large is given up on, and acknowledged like one decoded.
	if err := d.FeedEncoder(append([]byte{0xc0}, appendString(nil, 0, 7, strings.Repeat("b", 100))...)); err != nil {
		t.Fatal(err)
	}
	d.AppendInstructions(nil)
	if _, err := d.AppendSection(nil, section, 50, 12); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
	if got := d.AppendInstructions(nil); !bytes.Equal(got, []byte{0x8c}) || d.Blocked() != 0 {
		t.Fatalf("decoder stream %x with %d blocked, want 8c and none", got, d.Blocked())
	}
}

// TestDynamicCapacity: the encoder may lower the capacity, which evicts,
// but not raise it past the maximum this side allows.
func TestDynamicCapacity(t *testing.T) {
	var d Decoder
	d.EnableDynamic(100, 0)
	ins := append([]byte{0x3f, 0x45}, appendString([]byte{0xc0}, 0, 7, "x")...)
	if err := d.FeedEncoder(ins); err != nil {
		t.Fatal(err)
	}
	if d.dyn.n != 1 {
		t.Fatalf("%d entries", d.dyn.n)
	}
	// Down to 10, which evicts it; a reference to it then fails.
	if err := d.FeedEncoder([]byte{0x2a}); err != nil {
		t.Fatal(err)
	}
	if d.dyn.n != 0 {
		t.Fatalf("%d entries after the capacity went down", d.dyn.n)
	}
	if _, err := d.AppendSection(nil, []byte{0x02, 0x00, 0x80}, 0, 0); !errors.Is(err, ErrDecompression) {
		t.Fatalf("reference to an evicted entry: %v", err)
	}
	// An entry larger than the capacity.
	if err := d.FeedEncoder(ins[2:]); !errors.Is(err, ErrEncoderStream) {
		t.Fatalf("entry past the capacity: %v", err)
	}
	var e Decoder
	e.EnableDynamic(100, 0)
	if err := e.FeedEncoder([]byte{0x3f, 0x46}); !errors.Is(err, ErrEncoderStream) {
		t.Fatalf("capacity past the maximum: %v", err)
	}
}

// TestDynamicDecodeErrors: references the table cannot satisfy.
func TestDynamicDecodeErrors(t *testing.T) {
	var d Decoder
	d.EnableDynamic(4096, 4)
	if err := d.FeedEncoder(append([]byte{0x3f, 0xe1, 0x1f}, appendString([]byte{0xc0}, 0, 7, "a")...)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		block []byte
	}{
		{"encoded insert count past the range", []byte{0xff, 0x7f, 0x00}},
		{"insert count of zero", []byte{0x01, 0x00}},
		{"negative base", []byte{0x02, 0x81, 0x80}},
		{"relative index past the base", []byte{0x02, 0x00, 0x81}},
		{"insert count above the references", []byte{0x02, 0x00}},
		{"reference at the insert count", []byte{0x02, 0x01, 0x80}},
		{"post-base past the insert count", []byte{0x02, 0x80, 0x11}},
		{"name reference past the base", []byte{0x02, 0x00, 0x41, 0x00}},
		{"post-base name past the insert count", []byte{0x02, 0x80, 0x01, 0x00}},
		{"dynamic reference with no count", []byte{0x00, 0x00, 0x80}},
		{"dynamic name with no count", []byte{0x00, 0x00, 0x40, 0x00}},
		{"post-base with no count", []byte{0x00, 0x00, 0x10}},
		{"truncated delta", []byte{0x02}},
		{"truncated delta continuation", []byte{0x02, 0x7f}},
		{"insert count past what the table holds", []byte{0x83, 0x00}},
		{"truncated index", []byte{0x02, 0x00, 0xbf}},
		{"truncated name index", []byte{0x02, 0x00, 0x4f}},
		{"truncated post-base", []byte{0x02, 0x80, 0x1f}},
		{"truncated post-base name", []byte{0x02, 0x80, 0x07}},
		{"post-base name without value", []byte{0x02, 0x80, 0x00}},
		{"truncated encoded count", []byte{0xff}},
	} {
		if _, err := d.AppendSection(nil, tc.block, 0, 0); !errors.Is(err, ErrDecompression) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	// A decoder without a table refuses any reference to one.
	var plain Decoder
	if _, err := plain.AppendSection(nil, []byte{0x02, 0x00, 0x80}, 0, 0); !errors.Is(err, ErrDecompression) {
		t.Fatalf("no table: %v", err)
	}
	if err := plain.FeedEncoder([]byte{0x20}); !errors.Is(err, ErrEncoderStream) {
		t.Fatalf("no table: %v", err)
	}
	plain.EnableDynamic(0, 0)
	if plain.Dynamic() || plain.Blocked() != 0 || plain.AppendInstructions(nil) != nil {
		t.Fatal("a capacity of zero enabled the table")
	}
	plain.Cancel(0)
	// A post-base reference decodes, and a post-base name reference.
	fields, err := d.AppendSection(nil, []byte{0x02, 0x80, 0x10, 0x00, 0x01, 'b'}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sameFields(t, fields, []HeaderField{{":authority", "a"}, {":authority", "b"}})
}

// TestEncoderStreamErrors: instructions the decoder cannot carry out.
func TestEncoderStreamErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"static name past the table", []byte{0xff, 0x24, 0x00}},
		{"dynamic name of an empty table", []byte{0x80, 0x00}},
		{"duplicate of an empty table", []byte{0x00}},
		{"entry past the capacity", []byte{0x40 | 3, 'a', 'b', 'c', 0x00}},
		{"length past any entry", []byte{0x5f, 0xff, 0xff, 0x7f}},
		{"bad huffman", []byte{0x3f, 0xe1, 0x1f, 0x61, 0xff, 0x00}},
		{"overflowing integer", []byte{0x3f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"overflowing name index", []byte{0xbf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"overflowing duplicate", []byte{0x1f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"literal name past the capacity", append([]byte{0x3f, 0x01}, appendString(appendString([]byte{0x40}, 0, 5, "ab"), 0, 7, "")...)},
	} {
		var d Decoder
		d.EnableDynamic(4096, 0)
		if err := d.FeedEncoder(tc.data); !errors.Is(err, ErrEncoderStream) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	// A dynamic name reference and a duplicate of entries that are there.
	var d Decoder
	d.EnableDynamic(4096, 0)
	data := append([]byte{0x3f, 0xe1, 0x1f}, appendString(appendString([]byte{}, 0x40, 5, "x-a"), 0, 7, "1")...)
	data = append(data, appendString([]byte{0x80}, 0, 7, "2")...)
	data = append(data, 0x01)
	for i := range data {
		// Byte by byte, as a stream may deliver it.
		if err := d.FeedEncoder(data[i : i+1]); err != nil {
			t.Fatal(err)
		}
	}
	if d.dyn.n != 3 || d.dyn.entry(1) != (HeaderField{"x-a", "2"}) || d.dyn.entry(2) != (HeaderField{"x-a", "1"}) {
		t.Fatalf("table %q", d.dyn.ring[:d.dyn.n])
	}
}

// TestDecoderStreamErrors: instructions the encoder cannot account for.
func TestDecoderStreamErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"acknowledgment of nothing", []byte{0x84}},
		{"increment of zero", []byte{0x00}},
		{"increment past the insertions", []byte{0x01}},
		{"overflowing integer", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	} {
		e := NewEncoder(4096, func([]byte) error { return nil })
		if err := e.FeedDecoder(tc.data); !errors.Is(err, ErrDecoderStream) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	// Cancellation drops a stream's unacknowledged sections, and an
	// instruction may arrive a byte at a time.
	p := newPair(4096, 0)
	f := []HeaderField{{"x-long-field", strings.Repeat("v", 200)}}
	for i := 0; i < 3; i++ {
		p.decode(t, p.encode(f, 0), 0)
		p.carry(t)
	}
	p.encode(f, 0x1234)
	p.encode(f, 0x1234)
	if p.enc.pending != 2 {
		t.Fatalf("%d pending", p.enc.pending)
	}
	cancel := appendInt(nil, 0x40, 6, 0x1234)
	for i := range cancel {
		if err := p.enc.FeedDecoder(cancel[i : i+1]); err != nil {
			t.Fatal(err)
		}
	}
	if p.enc.pending != 0 || len(p.enc.unacked) != 0 {
		t.Fatalf("%d pending after cancellation", p.enc.pending)
	}
	// Two sections of one stream are acknowledged in turn.
	first, second := p.encode(f, 8), p.encode(f, 8)
	p.decode(t, first, 8)
	p.carry(t)
	if p.enc.pending != 1 {
		t.Fatalf("%d pending after one acknowledgment, want 1", p.enc.pending)
	}
	p.decode(t, second, 8)
	p.carry(t)
	if p.enc.pending != 0 {
		t.Fatalf("%d pending after both, want 0", p.enc.pending)
	}
}

// TestRequiredInsertCountWraps: the encoded required insert count wraps at
// twice the entries the table can hold, and decodes back to the count
// nearest the decoder's own (RFC 9204 section 4.5.1.1). A table of 100
// bytes holds one of these entries at a time, and each section refers to
// the newest.
func TestRequiredInsertCountWraps(t *testing.T) {
	p := newPair(100, 0)
	for i := 0; i < 20; i++ {
		f := []HeaderField{{"x-w", strings.Repeat("w", 30) + string(rune('a'+i))}}
		for j := 0; j < 3; j++ {
			sameFields(t, p.decode(t, p.encode(f, 0), 0), f)
			p.carry(t)
		}
	}
	if p.dec.dyn.inserted != 20 {
		t.Fatalf("%d inserted, want 20", p.dec.dyn.inserted)
	}
}

// TestEncoderBounds: a peer that never acknowledges leaves the encoder
// referring to the table for at most maxPending sections, and a nil or
// unconfigured Encoder encodes as AppendField does.
func TestEncoderBounds(t *testing.T) {
	p := newPair(4096, 0)
	f := []HeaderField{{"x-long-field", strings.Repeat("v", 200)}}
	for i := 0; i < 3; i++ {
		p.decode(t, p.encode(f, 0), 0)
		p.carry(t)
	}
	for i := 0; i < maxPending+10; i++ {
		block := p.encode(f, uint64(4*i))
		if i >= maxPending && block[0] != 0 {
			t.Fatalf("section %d refers to the table past maxPending", i)
		}
	}
	plain := encode(f)
	var e *Encoder
	s, block := e.Begin(nil, 0)
	for _, field := range f {
		block = s.Append(block, field.Name, field.Value, false)
	}
	if block = s.End(block); !bytes.Equal(block, plain) {
		t.Fatalf("nil Encoder wrote %x, want %x", block, plain)
	}
	off := NewEncoder(0, nil)
	off.SetPeerSettings(4096, 0)
	s, block = off.Begin(nil, 0)
	block = s.Append(block, f[0].Name, f[0].Value, false)
	if block = s.End(block); !bytes.Equal(block, plain) {
		t.Fatalf("Encoder of no capacity wrote %x, want %x", block, plain)
	}
	// A field larger than the table is never inserted.
	small := newPair(64, 0)
	for i := 0; i < 3; i++ {
		small.decode(t, small.encode(f, 0), 0)
		small.carry(t)
	}
	if small.enc.t.inserted != 0 {
		t.Fatal("inserted a field larger than the table")
	}
}

// TestEncoderPrefixGrows: a required insert count past what one byte holds
// widens the section's prefix, and the fields after it move along.
func TestEncoderPrefixGrows(t *testing.T) {
	p := newPair(64<<10, 0)
	var fields []HeaderField
	for i := 0; i < 260; i++ {
		// Each twice, so that it repeats before the encoder forgets it.
		f := HeaderField{"x-n", strings.Repeat("v", 10) + string(rune('A'+i%50)) + string(rune('A'+i/50))}
		fields = append(fields, f, f)
	}
	for i := 0; i < 3; i++ {
		block := p.encode(fields, 0)
		sameFields(t, p.decode(t, block, 0), fields)
		p.carry(t)
	}
}
