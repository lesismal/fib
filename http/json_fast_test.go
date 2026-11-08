//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	json "encoding/json/v2"
	"math"
	"math/rand/v2"
	"testing"
	"time"
	"unicode/utf8"
)

type jfRating struct {
	Score int `json:"score"`
	Count int `json:"count"`
}

type jfItem struct {
	ID       int      `json:"id"`
	Name     string   `json:"name"`
	Category string   `json:"category"`
	Price    int      `json:"price"`
	Quantity int      `json:"quantity"`
	Active   bool     `json:"active"`
	Tags     []string `json:"tags"`
	Rating   jfRating `json:"rating"`
}

type jfProcessed struct {
	jfItem
	Total int `json:"total"`
}

type jfResponse struct {
	Items []jfProcessed `json:"items"`
	Count int           `json:"count"`
}

type jfNamedInt int32
type jfNamedString string

type jfAll struct {
	B      bool
	I      int
	I8     int8
	I16    int16
	I32    int32
	I64    int64
	U      uint
	U8     uint8
	U16    uint16
	U32    uint32
	U64    uint64
	S      string
	NI     jfNamedInt
	NS     jfNamedString
	P      *int
	PP     **string
	Arr    [3]int16
	Sl     []uint32
	Nil    []string
	Empty  []string
	Nest   []*jfRating
	Skip   string `json:"-"`
	Space  string `json:"with space"`
	Empty2 string `json:""`
	hidden int
}

type jfInner struct {
	A string
	B int `json:"bee"`
}

type jfinner struct {
	C bool
}

type jfEmbed struct {
	jfInner
	jfinner
	D []jfInner
}

type jfMarshaler struct{ X int }

func (*jfMarshaler) MarshalJSON() ([]byte, error) { return []byte(`"m"`), nil }

type jfText int

func (jfText) MarshalText() ([]byte, error) { return []byte("t"), nil }

type jfPromotes struct {
	time.Time
	N int
}

type jfRecursive struct {
	V    int
	Next *jfRecursive
}

type jfOmit struct {
	A int `json:"a,omitempty"`
}

type jfDup struct {
	jfInner
	A string
}

type jfPtrEmbed struct {
	*jfInner
}

type jfOnlyHidden struct {
	a int
}

type jfSinglePtr struct {
	P *int
}

type jfQuoteName struct {
	A int `json:"a\"b"`
}

type jfUnicodeName struct {
	A int `json:"é"`
}

type jfIface struct {
	V any
}

func jfFast(v any) bool {
	_, ok := appendFastJSON(nil, v)
	return ok
}

// checkJSON compares appendJSON's output, through the compiled encoders where
// they take the value, with encoding/json/v2's, and whether the compiled
// encoders took it with fast.
func checkJSON(t *testing.T, name string, v any, fast bool) {
	t.Helper()
	want, wantErr := json.Marshal(v)
	got, gotErr := appendJSON([]byte("prefix"), v)
	if (wantErr == nil) != (gotErr == nil) {
		t.Fatalf("%s: error %v, v2 %v", name, gotErr, wantErr)
	}
	if wantErr == nil && !bytes.Equal(got, append([]byte("prefix"), want...)) {
		t.Fatalf("%s:\n got %s\nwant prefix%s", name, got, want)
	}
	if gotErr != nil && string(got) != "prefix" {
		t.Fatalf("%s: error left %q", name, got)
	}
	if took := jfFast(v); took != fast {
		t.Fatalf("%s: compiled encoder took it %v, want %v", name, took, fast)
	}
}

func TestFastJSONMatchesV2(t *testing.T) {
	n := 7
	s := "pp"
	ps := &s
	all := jfAll{
		B: true, I: math.MinInt, I8: math.MinInt8, I16: math.MaxInt16, I32: math.MinInt32, I64: math.MaxInt64,
		U: math.MaxUint, U8: math.MaxUint8, U16: math.MaxUint16, U32: math.MaxUint32, U64: math.MaxUint64,
		S: "a\"b\\c\n\r\t\b\f\x00\x1f\x7f<>&\u2028\u2029é😀\ufffd", NI: -3, NS: "named",
		P: &n, PP: &ps, Arr: [3]int16{1, -2, 3}, Sl: []uint32{4, 5}, Empty: []string{},
		Nest: []*jfRating{{1, 2}, nil}, Skip: "skip", Space: "sp", Empty2: "e", hidden: 9,
	}
	items := []jfProcessed{
		{jfItem{1, "Alpha Widget", "electronics", 328, 15, true, []string{"sale", "popular"}, jfRating{48, 53}}, 4920},
		{jfItem{2, "Pro Valve", "tools", 347, 95, false, nil, jfRating{}}, 0},
	}
	cases := []struct {
		name string
		v    any
		fast bool
	}{
		{"bool", true, true},
		{"int", -42, true},
		{"uint64", uint64(math.MaxUint64), true},
		{"string", "x\"y\u2028", true},
		{"named int", jfNamedInt(5), true},
		{"all", all, true},
		{"all pointer", &all, true},
		{"zero all", jfAll{}, true},
		{"response", jfResponse{Items: items, Count: 2}, true},
		{"response pointer", &jfResponse{Items: items, Count: 2}, true},
		{"embed", jfEmbed{jfInner{"a", 1}, jfinner{true}, []jfInner{{"b", 2}}}, true},
		{"empty struct", struct{}{}, true},
		{"nil slice", []int(nil), true},
		{"slice of pointers", []*int{&n, nil}, true},
		{"array", [2]string{"a", "b"}, true},
		{"space name", struct {
			A int `json:"a b"`
			B int
		}{1, 2}, true},
		{"nil pointer", (*jfAll)(nil), true},
		{"pointer to pointer", &ps, true},

		{"nil", nil, false},
		{"float", 1.5, false},
		{"map", map[string]int{"a": 1}, false},
		{"bytes", []byte("abc"), false},
		{"byte array", [3]byte{1, 2, 3}, false},
		{"named bytes", []uint8{1}, false},
		{"time", time.Unix(1, 0).UTC(), false},
		{"duration", time.Second, false},
		{"duration field", struct{ D time.Duration }{time.Second}, false},
		{"pointer marshaler", jfMarshaler{1}, false},
		{"pointer marshaler pointer", &jfMarshaler{1}, false},
		{"marshaler in slice", []jfMarshaler{{1}}, false},
		{"text marshaler", jfText(1), false},
		{"promoted marshaler", jfPromotes{time.Unix(1, 0).UTC(), 1}, false},
		{"recursive", jfRecursive{1, &jfRecursive{V: 2}}, false},
		{"omitempty", jfOmit{}, false},
		{"duplicate name", jfDup{jfInner{"a", 1}, "b"}, false},
		{"pointer embed", jfPtrEmbed{&jfInner{"a", 1}}, false},
		{"only hidden", jfOnlyHidden{1}, false},
		{"single pointer struct", jfSinglePtr{&n}, false},
		{"quote name", jfQuoteName{1}, false},
		{"unicode name", jfUnicodeName{1}, false},
		{"interface", jfIface{1}, false},
		{"invalid utf8", "a\xffb", false},
		{"invalid utf8 field", jfInner{A: "\xc3("}, false},
	}
	for _, c := range cases {
		checkJSON(t, c.name, c.v, c.fast)
	}
}

// TestFastJSONStrings compares the quoting of random strings, mostly
// printable, with control bytes, multi-byte runes and invalid UTF-8 mixed in.
func TestFastJSONStrings(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	pieces := []string{"a", "Z", " ", "\"", "\\", "\n", "\x00", "\x1f", "\x7f", "<", "&", "é", "€", "😀",
		"\u2028", "\ufffd", "\xff", "\xc3", "\xe2\x82", "\xed\xa0\x80", "\xf4\x90\x80\x80"}
	for i := 0; i < 20000; i++ {
		var b []byte
		for j := r.IntN(12); j > 0; j-- {
			b = append(b, pieces[r.IntN(len(pieces))]...)
		}
		s := string(b)
		checkJSON(t, "string", struct{ S string }{s}, utf8.ValidString(s))
	}
}

func jfBenchResponse() jfResponse {
	items := make([]jfProcessed, 25)
	for i := range items {
		items[i] = jfProcessed{jfItem{i + 1, "Alpha Widget", "electronics", 328 + i, 15, i%2 == 0,
			[]string{"sale", "heavy-duty", "popular"}, jfRating{48, 53 + i}}, (328 + i) * 15 * 3}
	}
	return jfResponse{Items: items, Count: len(items)}
}

func BenchmarkJSONResponse(b *testing.B) {
	v := jfBenchResponse()
	b.Run("compiled", func(b *testing.B) {
		var dst []byte
		b.ReportAllocs()
		for b.Loop() {
			dst, _ = appendJSON(dst[:0], v)
		}
	})
	b.Run("v2", func(b *testing.B) {
		w := &appendWriter{}
		b.ReportAllocs()
		for b.Loop() {
			w.out = w.out[:0]
			_ = json.MarshalWrite(w, v)
		}
	})
}
