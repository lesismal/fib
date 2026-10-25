package hpack

import (
	"bytes"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

// appendFieldLinear is AppendField as it was before the index, searching the
// dynamic table from the newest entry, kept to check the index against. It
// runs on an encoder without an index.
func appendFieldLinear(e *Encoder, dst []byte, name, value string, sensitive bool) []byte {
	nameIndex := 0
	if i, ok := staticPairIndex[HeaderField{name, value}]; ok {
		return appendInt(dst, 0x80, 7, uint64(i))
	}
	if i, ok := staticNameIndex[name]; ok {
		nameIndex = i
	}
	for i := len(e.table.entries) - 1; i >= 0; i-- {
		f := e.table.entries[i]
		if f.Name != name {
			continue
		}
		index := len(staticTable) + len(e.table.entries) - i
		if f.Value == value {
			return appendInt(dst, 0x80, 7, uint64(index))
		}
		if nameIndex == 0 {
			nameIndex = index
		}
	}
	f := HeaderField{name, value}
	if sensitive {
		dst = appendInt(dst, 0x10, 4, uint64(nameIndex))
	} else if f.size() > e.table.maxSize/2 {
		dst = appendInt(dst, 0x00, 4, uint64(nameIndex))
	} else {
		dst = appendInt(dst, 0x40, 6, uint64(nameIndex))
		e.table.add(f)
	}
	if nameIndex == 0 {
		dst = appendString(dst, name)
	}
	return appendString(dst, value)
}

// TestIndexMatchesLinearSearch encodes long random sequences of blocks with
// the index and with the linear search it replaced, and checks every block
// comes out byte for byte the same and decodes to what was encoded. The
// fields repeat often, mix static names with others, and include large ones
// and nearly empty ones that fill the table with as many entries as it
// holds; the table size changes between blocks, so entries are evicted all
// the time. It runs with the default limit on the table and with one that
// lets the table hold thousands of entries, past what the index's entry
// numbers wrap at, and grow past the size its index was made for.
func TestIndexMatchesLinearSearch(t *testing.T) {
	for _, limit := range []int{DefaultTableSize, 1 << 17} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) { testIndexMatchesLinearSearch(t, limit) })
	}
}

func testIndexMatchesLinearSearch(t *testing.T, limit int) {
	names := []string{"", ":path", "content-length", "content-type", "date", "server",
		"x-a", "x-b", "x-request-id", "set-cookie", "cookie", "vary"}
	values := []string{"", "0", "1", "/", "gzip", "text/html", "no-cache", "fib"}
	sizes := []int{0, 31, 32, 64, 100, 256, 1000, 2048, 4000, DefaultTableSize, 1 << 15, 1 << 17, 1 << 20}
	most := 0
	seeds, blocks := uint64(20), 1000
	if limit > DefaultTableSize {
		// The linear search is slow over a large table.
		seeds, blocks = 4, 400
	}
	for seed := range seeds {
		r := rand.New(rand.NewPCG(seed, 7))
		indexed, linear := NewEncoder(), NewEncoder()
		indexed.SetTableSizeLimit(limit)
		linear.SetTableSizeLimit(limit)
		d := NewDecoder(limit)
		var a, b []byte
		for range blocks {
			if r.IntN(20) == 0 {
				n := sizes[r.IntN(len(sizes))]
				indexed.SetMaxTableSize(n)
				linear.SetMaxTableSize(n)
			}
			a, b = indexed.Begin(a[:0]), linear.Begin(b[:0])
			var want []HeaderField
			n, tiny := r.IntN(16), r.IntN(10) == 0
			if tiny {
				n = max(200, limit/20)
			}
			for range n {
				f := HeaderField{names[r.IntN(len(names))], values[r.IntN(len(values))]}
				if tiny {
					f = HeaderField{"", strconv.Itoa(r.IntN(100000))}
				}
				switch r.IntN(8) {
				case 0:
					f.Value = strconv.Itoa(r.IntN(500))
				case 1:
					f.Name = "x-" + strconv.Itoa(r.IntN(200))
				case 2:
					f.Value = strings.Repeat("v", r.IntN(3000))
				case 3:
					f = HeaderField{"", strconv.Itoa(r.IntN(300))}
				}
				sensitive := r.IntN(10) == 0
				want = append(want, f)
				a = indexed.AppendField(a, f.Name, f.Value, sensitive)
				b = appendFieldLinear(linear, b, f.Name, f.Value, sensitive)
			}
			most = max(most, len(indexed.table.entries))
			if !bytes.Equal(a, b) {
				t.Fatalf("seed %d: indexed % x, linear % x", seed, a, b)
			}
			got := decodeAll(t, d, a)
			if len(got) != len(want) {
				t.Fatalf("seed %d: decoded %d fields, want %d", seed, len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("seed %d: field %d decoded as %v, want %v", seed, i, got[i], want[i])
				}
			}
		}
		if linear.table.index != nil {
			t.Fatal("the linear encoder made an index")
		}
	}
	if most < min(limit/40, 300) {
		t.Fatalf("the table held at most %d entries", most)
	}
}

// TestEncoderTableSizeLimit checks that an encoder uses a larger table than
// the default only when both its limit and the peer allow it, announces each
// change of size at the start of the next block, and that a decoder allowed
// as much decodes what it sends.
func TestEncoderTableSizeLimit(t *testing.T) {
	e := NewEncoder()
	d := NewDecoder(1 << 16)
	e.SetMaxTableSize(1 << 16)
	if e.table.maxSize != DefaultTableSize {
		t.Fatalf("table %d before a limit above the default", e.table.maxSize)
	}
	e.SetTableSizeLimit(1 << 15)
	if e.table.maxSize != 1<<15 {
		t.Fatalf("table %d with a limit of 32768 and an allowance of 65536", e.table.maxSize)
	}
	// Enough distinct fields that the default table could not keep them.
	var block []byte
	for round := range 2 {
		block = e.Begin(block[:0])
		if round == 0 && (len(block) == 0 || block[0]&0xe0 != 0x20) {
			t.Fatal("the first block does not announce the larger table")
		}
		for i := range 300 {
			block = e.AppendField(block, "x-field-"+strconv.Itoa(i), "value-"+strconv.Itoa(i), false)
		}
		got := decodeAll(t, d, block)
		if len(got) != 300 || got[299] != (HeaderField{"x-field-299", "value-299"}) {
			t.Fatalf("round %d decoded %d fields, last %v", round, len(got), got[len(got)-1])
		}
	}
	// The second round found every field in the table.
	if len(block) > 3*300 {
		t.Fatalf("second block %d bytes for 300 indexed fields", len(block))
	}
	// The peer allowing less shrinks the table below the limit.
	e.SetMaxTableSize(100)
	if e.table.maxSize != 100 {
		t.Fatalf("table %d with an allowance of 100", e.table.maxSize)
	}
	// A limit below the default shrinks a table the peer allows more of.
	f := NewEncoder()
	f.SetTableSizeLimit(0)
	block = f.Begin(nil)
	block = f.AppendField(block, "x-a", "b", false)
	g := NewDecoder(DefaultTableSize)
	decodeAll(t, g, block)
	if len(g.table.entries) != 0 || g.table.maxSize != 0 {
		t.Fatalf("decoder table %d entries of at most %d bytes", len(g.table.entries), g.table.maxSize)
	}
	if e.SetTableSizeLimit(1 << 30); e.limit != MaxEncoderTableSize {
		t.Fatalf("limit %d", e.limit)
	}
}

// TestDecoderAllowedMaxSize checks that once the allowance drops below the
// peer's table, the peer's next block has to begin by shrinking the table.
func TestDecoderAllowedMaxSize(t *testing.T) {
	e, d := NewEncoder(), NewDecoder(DefaultTableSize)
	block := e.AppendField(e.Begin(nil), "x-a", "b", false)
	decodeAll(t, d, block)
	d.SetAllowedMaxSize(100)
	plain := e.AppendField(e.Begin(nil), "x-a", "b", false)
	if err := d.Decode(plain, func(HeaderField) error { return nil }); err != ErrInvalidSizeUpdate {
		t.Fatalf("block without the update: %v", err)
	}
	e.SetMaxTableSize(100)
	shrunk := e.AppendField(e.Begin(nil), "x-a", "b", false)
	if got := decodeAll(t, d, shrunk); len(got) != 1 || d.mustShrink {
		t.Fatalf("decoded %v, mustShrink %v", got, d.mustShrink)
	}
	// An allowance at or above the table's size asks nothing of the peer.
	d.SetAllowedMaxSize(200)
	if d.mustShrink {
		t.Fatal("a larger allowance requires an update")
	}
}
