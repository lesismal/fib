package hpack

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func decodeAll(t *testing.T, d *Decoder, block []byte) []HeaderField {
	t.Helper()
	var fields []HeaderField
	if err := d.Decode(block, func(f HeaderField) error {
		fields = append(fields, f)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return fields
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestDecodeRFCExamples decodes RFC 7541 Appendix C.4: three requests on one
// connection, Huffman coded, sharing a dynamic table.
func TestDecodeRFCExamples(t *testing.T) {
	d := NewDecoder(DefaultTableSize)
	steps := []struct {
		block string
		want  []HeaderField
		size  int
	}{
		{"8286 8441 8cf1 e3c2 e5f2 3a6b a0ab 90f4 ff", []HeaderField{
			{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "www.example.com"},
		}, 57},
		{"8286 84be 5886 a8eb 1064 9cbf", []HeaderField{
			{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "www.example.com"},
			{"cache-control", "no-cache"},
		}, 110},
		{"8287 85bf 4088 25a8 49e9 5ba9 7d7f 8925 a849 e95b b8e8 b4bf", []HeaderField{
			{":method", "GET"}, {":scheme", "https"}, {":path", "/index.html"}, {":authority", "www.example.com"},
			{"custom-key", "custom-value"},
		}, 164},
	}
	for i, step := range steps {
		got := decodeAll(t, d, unhex(t, step.block))
		if !reflect.DeepEqual(got, step.want) {
			t.Fatalf("step %d: got %v, want %v", i, got, step.want)
		}
		if d.table.size != step.size {
			t.Fatalf("step %d: table size %d, want %d", i, d.table.size, step.size)
		}
	}
}

// TestDecodeEviction decodes RFC 7541 Appendix C.6, whose 256-byte table
// evicts entries as the responses go.
func TestDecodeEviction(t *testing.T) {
	d := NewDecoder(256)
	d.table.setMaxSize(256)
	blocks := []string{
		"4882 6402 5885 aec3 771a 4b61 96d0 7abe 9410 54d4 44a8 2005 9504 0b81 66e0 82a6 2d1b ff6e 919d 29ad 1718 63c7 8f0b 97c8 e9ae 82ae 43d3",
		"4883 640e ff c1 c0 bf",
		"88c1 6196 d07a be94 1054 d444 a820 0595 040b 8166 e084 a62d 1bff c05a 839b d9ab 77ad 94e7 821d d7f2 e6c7 b335 dfdf cd5b 3960 d5af 2708 7f36 72c1 ab27 0fb5 291f 9587 3160 65c0 03ed 4ee5 b106 3d50 07",
	}
	last := []HeaderField{
		{":status", "200"}, {"cache-control", "private"}, {"date", "Mon, 21 Oct 2013 20:13:22 GMT"},
		{"location", "https://www.example.com"}, {"content-encoding", "gzip"},
		{"set-cookie", "foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1"},
	}
	var got []HeaderField
	for _, block := range blocks {
		got = decodeAll(t, d, unhex(t, block))
	}
	if !reflect.DeepEqual(got, last) {
		t.Fatalf("got %v, want %v", got, last)
	}
	if d.table.size != 215 {
		t.Fatalf("table size %d, want 215", d.table.size)
	}
}

func TestRoundTrip(t *testing.T) {
	e := NewEncoder()
	d := NewDecoder(DefaultTableSize)
	rng := rand.New(rand.NewSource(1))
	names := []string{":status", "content-type", "x-custom", "set-cookie", "cache-control", "x-big"}
	for round := 0; round < 200; round++ {
		if round == 50 {
			e.SetMaxTableSize(100)
		}
		if round == 51 {
			e.SetMaxTableSize(DefaultTableSize)
		}
		var want []HeaderField
		block := e.Begin(nil)
		for i := rng.Intn(8); i >= 0; i-- {
			name := names[rng.Intn(len(names))]
			value := strings.Repeat(string(rune('a'+rng.Intn(26))), rng.Intn(40))
			if name == "x-big" {
				value = strings.Repeat("z", 3000)
			}
			want = append(want, HeaderField{name, value})
			block = e.AppendField(block, name, value, name == "set-cookie" && round%2 == 0)
		}
		got := decodeAll(t, d, block)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: got %v, want %v", round, got, want)
		}
	}
}

func TestHuffmanRoundTrip(t *testing.T) {
	var all []byte
	for i := 0; i < 256; i++ {
		all = append(all, byte(i))
	}
	for _, s := range []string{"", "a", "www.example.com", "no-cache", string(all)} {
		encoded := huffmanAppend(nil, s)
		if len(encoded) != huffmanLen(s) {
			t.Fatalf("%q: length %d, want %d", s, len(encoded), huffmanLen(s))
		}
		got, err := huffmanDecode(encoded, 0)
		if err != nil || got != s {
			t.Fatalf("%q: got %q, %v", s, got, err)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	cases := map[string][]byte{
		"index zero":          {0x80},
		"index past table":    {0xff, 0x00},
		"truncated string":    {0x40, 0x05, 'a'},
		"long padding":        {0x00, 0x81, 'a' | 0x80, 0x82, 0xff, 0xff},
		"size update late":    {0x82, 0x20},
		"size update too big": {0x3f, 0xe2, 0x1f},
	}
	for name, block := range cases {
		d := NewDecoder(DefaultTableSize)
		if err := d.Decode(block, func(HeaderField) error { return nil }); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

// literalBlock encodes fields as literals without indexing whose names are in
// the static table, the way a peer that replays one encoded request does,
// Huffman coding the values when huffman is set.
func literalBlock(huffman bool, fields ...HeaderField) []byte {
	var block []byte
	for _, f := range fields {
		block = appendInt(block, 0x00, 4, uint64(staticNameIndex[f.Name]))
		if huffman {
			block = appendInt(block, 0x80, 7, uint64(huffmanLen(f.Value)))
			block = huffmanAppend(block, f.Value)
		} else {
			block = appendInt(block, 0x00, 7, uint64(len(f.Value)))
			block = append(block, f.Value...)
		}
	}
	return block
}

// TestDecodeInternsRepeatedLiterals checks that a literal a peer sends on
// every block without indexing it is only made a string once, and that the
// strings handed out stay what they were.
func TestDecodeInternsRepeatedLiterals(t *testing.T) {
	want := []HeaderField{{":authority", "127.0.0.1:21001"}, {":path", "/echo"}, {"content-length", "1024"}}
	for _, huffman := range []bool{false, true} {
		d := NewDecoder(DefaultTableSize)
		block := literalBlock(huffman, want...)
		first := decodeAll(t, d, block)
		if !reflect.DeepEqual(first, want) {
			t.Fatalf("huffman=%v: got %v, want %v", huffman, first, want)
		}
		fields := make([]HeaderField, 0, len(want))
		allocs := testing.AllocsPerRun(100, func() {
			fields = fields[:0]
			_ = d.Decode(block, func(f HeaderField) error {
				fields = append(fields, f)
				return nil
			})
		})
		if allocs != 0 {
			t.Fatalf("huffman=%v: a block seen before took %v allocations", huffman, allocs)
		}
		if !reflect.DeepEqual(fields, want) {
			t.Fatalf("huffman=%v: got %v, want %v", huffman, fields, want)
		}

		// Other values of the same length, decoded through the same scratch
		// buffer, leave the strings already handed out alone.
		other := decodeAll(t, d, literalBlock(huffman,
			HeaderField{":authority", "10.20.30.40:9999"}, HeaderField{":path", "/ohce"}, HeaderField{"content-length", "4201"}))
		if other[1].Value != "/ohce" || other[2].Value != "4201" {
			t.Fatalf("huffman=%v: got %v", huffman, other)
		}
		if !reflect.DeepEqual(first, want) {
			t.Fatalf("huffman=%v: strings decoded earlier changed to %v", huffman, first)
		}

		// Past what is remembered, a literal is still decoded whole.
		long := HeaderField{":path", "/" + strings.Repeat("x", 2*maxRecentString)}
		if got := decodeAll(t, d, literalBlock(huffman, long)); !reflect.DeepEqual(got, []HeaderField{long}) {
			t.Fatalf("huffman=%v: long literal decoded as %v", huffman, got)
		}
	}
}

// TestDecodeRemembersALongLiteral checks that a literal past what recent
// holds, a bearer token a peer never indexes, is decoded and made a string
// once while it repeats, that one of the same length that differs is decoded
// anew, and that MaxStringLength still holds for one remembered.
func TestDecodeRemembersALongLiteral(t *testing.T) {
	token := HeaderField{"authorization", "Bearer " + strings.Repeat("eyJhbGciOiJIUzI1NiJ9.", 14)}
	other := HeaderField{"authorization", token.Value[:len(token.Value)-1] + "!"}
	for _, huffman := range []bool{false, true} {
		d := NewDecoder(DefaultTableSize)
		block := literalBlock(huffman, token)
		first := decodeAll(t, d, block)
		if !reflect.DeepEqual(first, []HeaderField{token}) {
			t.Fatalf("huffman=%v: got %v", huffman, first)
		}
		allocs := testing.AllocsPerRun(100, func() {
			_ = d.Decode(block, func(HeaderField) error { return nil })
		})
		if allocs != 0 {
			t.Fatalf("huffman=%v: a long literal seen before took %v allocations", huffman, allocs)
		}
		if got := decodeAll(t, d, literalBlock(huffman, other)); !reflect.DeepEqual(got, []HeaderField{other}) {
			t.Fatalf("huffman=%v: a different literal of the same length decoded as %v", huffman, got)
		}
		if got := decodeAll(t, d, literalBlock(!huffman, other)); !reflect.DeepEqual(got, []HeaderField{other}) {
			t.Fatalf("huffman=%v: the literal coded the other way decoded as %v", huffman, got)
		}
		if !reflect.DeepEqual(first, []HeaderField{token}) {
			t.Fatalf("huffman=%v: a string decoded earlier changed to %v", huffman, first)
		}
		d.MaxStringLength = len(token.Value) - 1
		if err := d.Decode(literalBlock(!huffman, other), func(HeaderField) error { return nil }); err != ErrStringTooLong {
			t.Fatalf("huffman=%v: past MaxStringLength: %v, want ErrStringTooLong", huffman, err)
		}
	}
}

// BenchmarkDecodeRepeatedToken decodes the block of a client that sends a
// bearer token unindexed on every request, as curl and h2load do.
func BenchmarkDecodeRepeatedToken(b *testing.B) {
	d := NewDecoder(DefaultTableSize)
	block := literalBlock(true, HeaderField{":path", "/api/items"},
		HeaderField{"authorization", "Bearer " + strings.Repeat("eyJhbGciOiJIUzI1NiJ9.", 14)})
	b.ReportAllocs()
	for b.Loop() {
		_ = d.Decode(block, func(HeaderField) error { return nil })
	}
}

// BenchmarkDecodeRepeatedHuffman decodes the block a load generator sends on
// every request: the same Huffman-coded literals, none of them indexed.
func BenchmarkDecodeRepeatedHuffman(b *testing.B) {
	d := NewDecoder(DefaultTableSize)
	block := literalBlock(true, HeaderField{":path", "/baseline2?a=1&b=1"},
		HeaderField{":authority", "localhost:8082"}, HeaderField{"user-agent", "h2load nghttp2/1.59.0"})
	b.ReportAllocs()
	for b.Loop() {
		_ = d.Decode(block, func(HeaderField) error { return nil })
	}
}

// benchResponse is the header a typical response carries: a status, the
// content headers, a date, a server and a few fields of the application's.
var benchResponse = []HeaderField{
	{":status", "200"}, {"content-type", "application/json; charset=utf-8"},
	{"content-length", "1234"}, {"date", "Sat, 24 Oct 2026 12:00:00 GMT"},
	{"server", "fib"}, {"cache-control", "no-cache"}, {"vary", "accept-encoding"},
	{"x-frame-options", "DENY"}, {"x-content-type-options", "nosniff"},
}

func benchEncode(b *testing.B, fields func(i int) []HeaderField) {
	e := NewEncoder()
	var block []byte
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		block = e.Begin(block[:0])
		for _, f := range fields(i) {
			block = e.AppendField(block, f.Name, f.Value, false)
		}
		i++
	}
}

// BenchmarkEncodeResponseRepeated encodes the same response header again and
// again, which after the first block is all indexed.
func BenchmarkEncodeResponseRepeated(b *testing.B) {
	benchEncode(b, func(int) []HeaderField { return benchResponse })
}

// BenchmarkEncodeResponseVaried encodes the typical response header with a
// content-length taken from a few hundred sizes and a date that moves on now
// and then, so the table fills with lengths and evicts the oldest.
func BenchmarkEncodeResponseVaried(b *testing.B) {
	var lengths, dates []string
	for i := range 300 {
		lengths = append(lengths, strconv.Itoa(100+i*37))
	}
	for i := range 60 {
		dates = append(dates, fmt.Sprintf("Sat, 24 Oct 2026 12:00:%02d GMT", i))
	}
	fields := append([]HeaderField(nil), benchResponse...)
	benchEncode(b, func(i int) []HeaderField {
		fields[2].Value = lengths[(i*7)%len(lengths)]
		fields[3].Value = dates[(i/1000)%len(dates)]
		return fields
	})
}

// BenchmarkEncodeDistinct encodes fields that never repeat while they are in
// the table, so every one is added and the full table evicts on every add.
func BenchmarkEncodeDistinct(b *testing.B) {
	var blocks [][]HeaderField
	for i := range 64 {
		var fields []HeaderField
		for j := range 8 {
			fields = append(fields, HeaderField{fmt.Sprintf("x-custom-%d", j), fmt.Sprintf("value-%d-%d", i, j)})
		}
		blocks = append(blocks, fields)
	}
	benchEncode(b, func(i int) []HeaderField { return blocks[i%len(blocks)] })
}
