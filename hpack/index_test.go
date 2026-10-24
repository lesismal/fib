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
// the time.
func TestIndexMatchesLinearSearch(t *testing.T) {
	names := []string{"", ":path", "content-length", "content-type", "date", "server",
		"x-a", "x-b", "x-request-id", "set-cookie", "cookie", "vary"}
	values := []string{"", "0", "1", "/", "gzip", "text/html", "no-cache", "fib"}
	sizes := []int{0, 31, 32, 64, 100, 256, 1000, 2048, 4000, DefaultTableSize, 1 << 20}
	most := 0
	for seed := range uint64(20) {
		r := rand.New(rand.NewPCG(seed, 7))
		indexed, linear := NewEncoder(), NewEncoder()
		d := NewDecoder(DefaultTableSize)
		var a, b []byte
		for range 1000 {
			if r.IntN(20) == 0 {
				n := sizes[r.IntN(len(sizes))]
				indexed.SetMaxTableSize(n)
				linear.SetMaxTableSize(n)
			}
			a, b = indexed.Begin(a[:0]), linear.Begin(b[:0])
			var want []HeaderField
			n, tiny := r.IntN(16), r.IntN(10) == 0
			if tiny {
				n = 200
			}
			for range n {
				f := HeaderField{names[r.IntN(len(names))], values[r.IntN(len(values))]}
				if tiny {
					f = HeaderField{"", strconv.Itoa(r.IntN(1000))}
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
	if most < 100 {
		t.Fatalf("the table held at most %d entries", most)
	}
}
