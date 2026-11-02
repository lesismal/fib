package strtab

import (
	"strconv"
	"testing"
)

var names = []string{
	"accept", "accept-encoding", "accept-language", "accept-ranges", "age",
	"authorization", "cache-control", "content-encoding", "content-length",
	"content-type", "cookie", "date", "etag", "host", "location", "server",
	"set-cookie", "user-agent", "vary", "x-forwarded-for", "x-request-id",
}

func TestTable(t *testing.T) {
	vals := make([]int, len(names))
	for i := range vals {
		vals[i] = i
	}
	tab := New(names, vals)
	for i, name := range names {
		if v, ok := tab.Get(name); !ok || v != i {
			t.Fatalf("Get(%q) = %d, %v; want %d, true", name, v, ok, i)
		}
	}
	for _, miss := range []string{"", "a", "accepT", "content-lengtH", "x-forwarded-fo", "x-forwarded-forx"} {
		if v, ok := tab.Get(miss); ok {
			t.Fatalf("Get(%q) = %d, true; want a miss", miss, v)
		}
	}
	// A table full of keys that hash alike still finds every one.
	var many []string
	for i := 0; i < 500; i++ {
		many = append(many, "k"+strconv.Itoa(i))
	}
	big := New(many, many)
	for _, k := range many {
		if v, ok := big.Get(k); !ok || v != k {
			t.Fatalf("Get(%q) = %q, %v", k, v, ok)
		}
	}
	if _, ok := big.Get("k500"); ok {
		t.Fatal("found a key never added")
	}
}

func BenchmarkGet(b *testing.B) {
	tab := New(names, names)
	m := make(map[string]string)
	for _, n := range names {
		m[n] = n
	}
	keys := []string{"content-type", "content-length", "date", "server", "x-custom"}
	b.Run("table", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tab.Get(keys[i%len(keys)])
		}
	})
	b.Run("map", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = m[keys[i%len(keys)]]
		}
	})
}
