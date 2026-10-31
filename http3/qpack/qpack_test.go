package qpack

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func encode(fields []HeaderField) []byte {
	b := append([]byte(nil), Prefix...)
	for _, f := range fields {
		b = AppendField(b, f.Name, f.Value, f.Name == "authorization")
	}
	return b
}

func decode(t *testing.T, block []byte, maxSize int) ([]HeaderField, error) {
	t.Helper()
	var fields []HeaderField
	err := Decode(block, maxSize, func(f HeaderField) error {
		fields = append(fields, f)
		return nil
	})
	return fields, err
}

func TestRoundTrip(t *testing.T) {
	fields := []HeaderField{
		{":method", "GET"},                    // static pair
		{":path", "/index.html?q=1"},          // static name
		{":status", "200"},                    // static pair
		{"content-type", "text/html; x=y"},    // static name, other value
		{"x-custom", "value"},                 // literal name
		{"authorization", "Bearer secret"},    // never indexed
		{"x-empty", ""},                       // empty value
		{"x-long", strings.Repeat("ab", 300)}, // long, multi-byte length
	}
	block := encode(fields)
	got, err := decode(t, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(fields) {
		t.Fatalf("got %d fields", len(got))
	}
	for i := range fields {
		if got[i] != fields[i] {
			t.Fatalf("field %d: got %v, want %v", i, got[i], fields[i])
		}
	}
	// A static pair takes one byte.
	if b := AppendField(nil, ":method", "GET", false); !bytes.Equal(b, []byte{0xc0 | 17}) {
		t.Fatalf(":method GET encoded as %x", b)
	}
}

// TestDecodeExample decodes RFC 9204 appendix B.1, a field section that
// uses only the static table.
func TestDecodeExample(t *testing.T) {
	block := []byte{0x00, 0x00, 0x51, 0x0b, '/', 'i', 'n', 'd', 'e', 'x', '.', 'h', 't', 'm', 'l'}
	got, err := decode(t, block, 0)
	if err != nil || len(got) != 1 || got[0] != (HeaderField{":path", "/index.html"}) {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestDecodeErrors(t *testing.T) {
	cases := map[string][]byte{
		"dynamic insert count": {0x01, 0x00},
		"dynamic index":        {0x00, 0x00, 0x80},
		"post-base index":      {0x00, 0x00, 0x10},
		"static out of range":  {0x00, 0x00, 0xff, 0x40},
		"truncated string":     {0x00, 0x00, 0x51, 0x05, 'a'},
		"empty":                {},
	}
	for name, block := range cases {
		if _, err := decode(t, block, 0); !errors.Is(err, ErrDecompression) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	big := encode([]HeaderField{{"x-a", strings.Repeat("v", 100)}})
	if _, err := decode(t, big, 64); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("size limit: got %v", err)
	}
}

// A Decoder decodes what Decode does, and a section it has seen before costs
// it no allocation.
func TestDecoderRemembersStrings(t *testing.T) {
	block := encode([]HeaderField{
		{":method", "POST"},
		{":path", "/echo"},
		{":authority", "127.0.0.1:28001"},
		{"content-length", "1024"},
		{"user-agent", "benchmark/1.0"},
		{"x-custom", "value"},
		{"x-long", strings.Repeat("ab", 300)}, // too long to remember
	})
	want, err := decode(t, block, 0)
	if err != nil {
		t.Fatal(err)
	}
	var d Decoder
	var fields []HeaderField
	for i := 0; i < 3; i++ {
		if fields, err = d.AppendFields(fields[:0], block, 0); err != nil {
			t.Fatal(err)
		}
		if len(fields) != len(want) {
			t.Fatalf("decoded %d fields, want %d", len(fields), len(want))
		}
		for j := range want {
			if fields[j] != want[j] {
				t.Fatalf("field %d is %q, want %q", j, fields[j], want[j])
			}
		}
	}
	short := encode(want[:6])
	allocs := testing.AllocsPerRun(100, func() {
		fields, _ = d.AppendFields(fields[:0], short, 0)
	})
	if allocs != 0 {
		t.Fatalf("a section seen before took %v allocations", allocs)
	}
	// A limit the remembered string is past still fails the field.
	if _, err := d.AppendFields(nil, short, 10); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
}

// A Decoder remembers a browser's request, whose literals outnumber what it
// would remember were they not kept by their place, and whose cookie is
// long: a request that repeats it costs no allocation, and one with another
// path decodes the path alone anew, and keeps it.
func TestDecoderRemembersABrowsersRequest(t *testing.T) {
	fields := func(path string) []HeaderField {
		return []HeaderField{
			{":method", "GET"}, {":scheme", "https"}, {":authority", "www.example.com"}, {":path", path},
			{"user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"},
			{"accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"},
			{"accept-language", "en-US,en;q=0.9,zh-CN;q=0.8"},
			{"accept-encoding", "gzip, deflate, br, zstd"},
			{"referer", "https://www.example.com/products/catalog?page=2"},
			{"cookie", "session=" + strings.Repeat("s", 48) + "; cart=" + strings.Repeat("c", 32) + "; _ga=GA1.2.1418925463.1729700000; consent=all"},
			{"sec-fetch-site", "same-origin"}, {"sec-fetch-mode", "navigate"}, {"sec-fetch-dest", "document"},
			{"x-forwarded-proto", "https"},
		}
	}
	first, second := encode(fields("/site/a/one.html")), encode(fields("/site/b/two.html"))
	var d Decoder
	got, err := d.AppendFields(nil, first, 0)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		got, _ = d.AppendFields(got[:0], first, 0)
	})
	if allocs != 0 {
		t.Fatalf("a browser's request seen before took %v allocations", allocs)
	}
	want := fields("/site/b/two.html")
	if got, err = d.AppendFields(got[:0], second, 0); err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("field %d is %q, want %q", i, got[i], want[i])
		}
	}
	allocs = testing.AllocsPerRun(100, func() {
		block := first
		if len(got) > 0 && got[3].Value == "/site/a/one.html" {
			block = second
		}
		got, _ = d.AppendFields(got[:0], block, 0)
	})
	// The path's string, and its encoding kept for the next request.
	if allocs > 2 {
		t.Fatalf("a request with another path took %v allocations, want the path's alone", allocs)
	}
}

// A field in the static table is sent as its index, the first one it has,
// and a field whose name alone is there refers to the first entry with that
// name; one marked never to be indexed only ever refers to the name.
func TestAppendFieldStaticTable(t *testing.T) {
	for i, f := range staticTable {
		first, firstName := -1, -1
		for j, g := range staticTable {
			if first < 0 && g == f {
				first = j
			}
			if firstName < 0 && g.Name == f.Name {
				firstName = j
			}
		}
		if got, want := AppendField(nil, f.Name, f.Value, false), appendInt(nil, 0xc0, 6, uint64(first)); !bytes.Equal(got, want) {
			t.Fatalf("entry %d %v: % x, want % x", i, f, got, want)
		}
		want := appendString(appendInt(nil, 0x70, 4, uint64(firstName)), 0, 7, f.Value)
		if got := AppendField(nil, f.Name, f.Value, true); !bytes.Equal(got, want) {
			t.Fatalf("entry %d %v never indexed: % x, want % x", i, f, got, want)
		}
		want = appendString(appendInt(nil, 0x50, 4, uint64(firstName)), 0, 7, "x-other")
		if got := AppendField(nil, f.Name, "x-other", false); !bytes.Equal(got, want) {
			t.Fatalf("entry %d %v with another value: % x, want % x", i, f, got, want)
		}
	}
}
