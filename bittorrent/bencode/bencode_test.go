package bencode

import (
	"bytes"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

type inner struct {
	X int `bencode:"x"`
}

type Embedded struct {
	E string `bencode:"e"`
}

type sample struct {
	Embedded
	Name    string            `bencode:"name"`
	Len     int64             `bencode:"length"`
	Pieces  []byte            `bencode:"pieces,omitempty"`
	Hash    [4]byte           `bencode:"hash"`
	Private bool              `bencode:"private,omitempty"`
	List    []string          `bencode:"list,omitempty"`
	Ptr     *inner            `bencode:"ptr"`
	Skip    string            `bencode:"-"`
	Map     map[string]int    `bencode:"map,omitempty"`
	Raw     RawMessage        `bencode:"raw"`
	Any     any               `bencode:"any,omitempty"`
	ByHash  map[[2]byte]inner `bencode:"by hash,omitempty"`
	U8      uint8             `bencode:"u8"`
	NoTag   int
}

func TestRoundTrip(t *testing.T) {
	in := sample{
		Embedded: Embedded{E: "emb"},
		Name:     "a.txt",
		Len:      -5,
		Pieces:   []byte{0, 1, 2},
		Hash:     [4]byte{'a', 'b', 'c', 'd'},
		Private:  true,
		List:     []string{"x", "yy"},
		Ptr:      &inner{X: 7},
		Skip:     "never",
		Map:      map[string]int{"b": 2, "a": 1},
		Raw:      RawMessage("li1ei2ee"),
		Any:      map[string]any{"k": []any{int64(1), "v"}},
		ByHash:   map[[2]byte]inner{{'z', 'z'}: {1}, {'a', 'a'}: {2}},
		U8:       255,
		NoTag:    3,
	}
	b, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	want := "d5:NoTagi3e3:anyd1:kli1e1:vee7:by hashd2:aad1:xi2ee2:zzd1:xi1eee1:e3:emb4:hash4:abcd" +
		"6:lengthi-5e4:listl1:x2:yye3:mapd1:ai1e1:bi2ee4:name5:a.txt6:pieces3:\x00\x01\x02" +
		"7:privatei1e3:ptrd1:xi7ee3:rawli1ei2ee2:u8i255ee"
	if string(b) != want {
		t.Fatalf("Marshal\n got %q\nwant %q", b, want)
	}
	var out sample
	if err := Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	in.Skip = ""
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("Unmarshal\n got %+v\nwant %+v", out, in)
	}
	// Canonical input decodes strictly too.
	d := NewBytesDecoder(b)
	d.Strict()
	if err := d.Decode(&out); err != nil {
		t.Fatal(err)
	}
}

func TestOmitted(t *testing.T) {
	type T struct {
		A string     `bencode:"a,omitempty"`
		B int        `bencode:"b,omitempty"`
		C *inner     `bencode:"c"`
		D RawMessage `bencode:"d"`
		E any        `bencode:"e"`
		F []int      `bencode:"f"`
		G []byte     `bencode:"g"`
	}
	b, err := Marshal(T{})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "d1:fle1:g0:e" {
		t.Fatalf("got %q", b)
	}
	if _, err := Marshal([]any{nil}); err == nil {
		t.Fatal("nil in a list encoded")
	}
	if _, err := Marshal(1.5); err == nil {
		t.Fatal("float encoded")
	}
	if _, err := Marshal(map[int]int{1: 1}); err == nil {
		t.Fatal("int-keyed map encoded")
	}
	type cyc struct {
		P *cyc `bencode:"p"`
	}
	c := &cyc{}
	c.P = c
	if _, err := Marshal(c); err == nil {
		t.Fatal("cycle encoded")
	}
}

func TestIntegers(t *testing.T) {
	var i64 int64
	if err := Unmarshal([]byte("i-9223372036854775808e"), &i64); err != nil || i64 != math.MinInt64 {
		t.Fatal(i64, err)
	}
	var u64 uint64
	if err := Unmarshal([]byte("i18446744073709551615e"), &u64); err != nil || u64 != math.MaxUint64 {
		t.Fatal(u64, err)
	}
	var te *UnmarshalTypeError
	for _, c := range []struct {
		in string
		v  any
	}{
		{"i9223372036854775808e", new(int64)},
		{"i18446744073709551616e", new(uint64)},
		{"i99999999999999999999999e", new(uint64)},
		{"i-1e", new(uint)},
		{"i128e", new(int8)},
		{"i256e", new(uint8)},
		{"i1e", new(string)},
		{"1:a", new(int)},
		{"3:abc", new([4]byte)},
		{"le", new(int)},
		{"de", new([]int)},
	} {
		if err := Unmarshal([]byte(c.in), c.v); !errors.As(err, &te) {
			t.Errorf("%s into %T: %v, want a type error", c.in, c.v, err)
		}
	}
	var b bool
	if err := Unmarshal([]byte("i2e"), &b); err != nil || !b {
		t.Fatal(b, err)
	}
	var x any
	if err := Unmarshal([]byte("i99999999999999999999e"), &x); !errors.As(err, &te) {
		t.Fatal(err)
	}
}

func TestSyntaxErrors(t *testing.T) {
	deep := strings.Repeat("l", defaultMaxDepth+1) + strings.Repeat("e", defaultMaxDepth+1)
	for _, in := range []string{
		"", "i", "ie", "i-e", "i-0e", "i03e", "i1", "i1x", "03:abc", "4:abc", "1", "1x", "x",
		"l", "li1e", "d", "di1ei2ee", "d1:ae", "d1:a", "e", "i1ei2e", "le1", deep,
		"99999999999999999999999999:x",
	} {
		var v any
		var se *SyntaxError
		if err := Unmarshal([]byte(in), &v); !errors.As(err, &se) {
			t.Errorf("%q: %v, want a syntax error", in, err)
		}
		if Valid([]byte(in)) {
			t.Errorf("%q: Valid", in)
		}
	}
	ok := strings.Repeat("l", defaultMaxDepth) + strings.Repeat("e", defaultMaxDepth)
	var v any
	if err := Unmarshal([]byte(ok), &v); err != nil {
		t.Fatal(err)
	}
}

func TestTolerantAndStrict(t *testing.T) {
	type msg struct {
		T string `bencode:"t"`
		Y string `bencode:"y"`
		V int    `bencode:"v"`
		Q string `bencode:"q"`
	}
	// Unsorted, repeated and unknown keys, and a value of the wrong type.
	in := []byte("d1:y1:q1:t2:aa1:xd1:ali1eee1:v3:abc1:q4:ping1:y1:qe")
	var m msg
	var te *UnmarshalTypeError
	if err := Unmarshal(in, &m); !errors.As(err, &te) {
		t.Fatalf("got %v, want a type error", err)
	}
	if m != (msg{T: "aa", Y: "q", Q: "ping"}) {
		t.Fatalf("got %+v", m)
	}
	d := NewBytesDecoder(in)
	d.Strict()
	var se *SyntaxError
	if err := d.Decode(&m); !errors.As(err, &se) {
		t.Fatalf("strict: %v", err)
	}
	d = NewBytesDecoder([]byte("d1:ai1e1:ai2ee"))
	d.Strict()
	if err := d.Decode(&map[string]int{}); !errors.As(err, &se) {
		t.Fatalf("strict duplicate: %v", err)
	}
}

type upper string

func (u upper) MarshalBencode() ([]byte, error) {
	return Marshal(strings.ToUpper(string(u)))
}

func (u *upper) UnmarshalBencode(b []byte) error {
	var s string
	if err := Unmarshal(b, &s); err != nil {
		return err
	}
	*u = upper(strings.ToLower(s))
	return nil
}

type badMarshaler struct{}

func (badMarshaler) MarshalBencode() ([]byte, error) { return []byte("i1ei2e"), nil }

func TestMarshaler(t *testing.T) {
	type T struct {
		U  upper  `bencode:"u"`
		UP *upper `bencode:"up"`
	}
	up := upper("q")
	b, err := Marshal(T{U: "abc", UP: &up})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "d1:u3:ABC2:up1:Qe" {
		t.Fatalf("got %q", b)
	}
	var out T
	if err := Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.U != "abc" || out.UP == nil || *out.UP != "q" {
		t.Fatalf("got %+v", out)
	}
	var me *MarshalerError
	if _, err := Marshal(badMarshaler{}); !errors.As(err, &me) {
		t.Fatal(err)
	}
	if _, err := Marshal(RawMessage("i1")); err == nil {
		t.Fatal("invalid RawMessage encoded")
	}
}

func TestDecoderBytes(t *testing.T) {
	// A ut_metadata message: a dictionary, then the piece's raw bytes.
	msg := []byte("d8:msg_typei1e5:piecei0e10:total_sizei5ee" + "hello")
	type meta struct {
		Type  int `bencode:"msg_type"`
		Piece int `bencode:"piece"`
		Total int `bencode:"total_size"`
	}
	var m meta
	d := NewBytesDecoder(msg)
	if err := d.Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Total != 5 || string(msg[d.InputOffset():]) != "hello" {
		t.Fatalf("%+v %d", m, d.InputOffset())
	}
	if err := Unmarshal(msg, &m); err == nil {
		t.Fatal("Unmarshal accepted trailing data")
	}

	// NoCopy aliases the input.
	in := []byte("l3:abc3:defe")
	var raw RawMessage
	var bs [][]byte
	d = NewBytesDecoder(append(in, in...))
	d.NoCopy()
	if err := d.Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if err := d.Decode(&bs); err != nil {
		t.Fatal(err)
	}
	if d.More() || d.Decode(&raw) != io.EOF {
		t.Fatal("not at EOF")
	}
	if &raw[0] != &d.data[0] || &bs[0][0] != &d.data[len(in)+3] {
		t.Fatal("NoCopy copied")
	}
	var cp []byte
	src := []byte("3:abc")
	if err := Unmarshal(src, &cp); err != nil || &cp[0] == &src[2] {
		t.Fatal("Unmarshal aliased its input", err)
	}
}

func TestDecoderReader(t *testing.T) {
	in := "d1:ai1ee" + "l3:abce" + "i-3e" + "5:hello"
	d := NewDecoder(iotest.OneByteReader(strings.NewReader(in)))
	var got []any
	for {
		var v any
		err := d.Decode(&v)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []any{map[string]any{"a": int64(1)}, []any{"abc"}, int64(-3), "hello"}
	if !reflect.DeepEqual(got, want) || d.InputOffset() != int64(len(in)) {
		t.Fatalf("got %#v at %d", got, d.InputOffset())
	}

	var v any
	if err := NewDecoder(strings.NewReader("l3:ab")).Decode(&v); err != io.ErrUnexpectedEOF {
		t.Fatalf("cut short: %v", err)
	}
	d = NewDecoder(strings.NewReader("100000:x"))
	d.SetLimits(Limits{MaxSize: 1000})
	if err := d.Decode(&v); err == nil {
		t.Fatal("over MaxSize")
	}
	d = NewDecoder(strings.NewReader("lllee"))
	d.SetLimits(Limits{MaxDepth: 2})
	if err := d.Decode(&v); err == nil {
		t.Fatal("over MaxDepth")
	}
	d = NewDecoder(strings.NewReader("i1e3:abcrest"))
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	rest, _ := io.ReadAll(d.Buffered())
	if string(rest) != "rest" {
		t.Fatalf("Buffered %q", rest)
	}
	// Syntax errors carry stream offsets.
	d = NewDecoder(strings.NewReader("i1ei-0e"))
	_ = d.Decode(&v)
	var se *SyntaxError
	if err := d.Decode(&v); !errors.As(err, &se) || se.Offset != 3 {
		t.Fatalf("got %v", err)
	}
}

func TestLookupSplit(t *testing.T) {
	torrent := []byte("d8:announce3:url4:infod6:lengthi1e4:name1:a12:piece lengthi16384eee")
	info, err := Lookup(torrent, "info")
	if err != nil || string(info) != "d6:lengthi1e4:name1:a12:piece lengthi16384ee" {
		t.Fatalf("%q %v", info, err)
	}
	name, err := Lookup(torrent, "info", "name")
	if err != nil || string(name) != "1:a" {
		t.Fatalf("%q %v", name, err)
	}
	if _, err := Lookup(torrent, "nope"); err != ErrNotFound {
		t.Fatal(err)
	}
	if _, err := Lookup(torrent, "announce", "x"); err == nil {
		t.Fatal("lookup in a string")
	}
	v, rest, err := Split([]byte("li1eetail"))
	if err != nil || string(v) != "li1ee" || string(rest) != "tail" {
		t.Fatal(v, rest, err)
	}
}

func TestReuseSlice(t *testing.T) {
	type T struct {
		L []*inner `bencode:"l"`
	}
	v := T{L: []*inner{{1}, {2}, {3}}}
	if err := Unmarshal([]byte("d1:lld1:xi9eeee"), &v); err != nil {
		t.Fatal(err)
	}
	if len(v.L) != 1 || v.L[0].X != 9 {
		t.Fatalf("%+v", v.L)
	}
	var arr [2]int
	var te *UnmarshalTypeError
	if err := Unmarshal([]byte("li1ei2ei3ee"), &arr); !errors.As(err, &te) || arr != [2]int{1, 2} {
		t.Fatal(arr, err)
	}
	var empty []int
	if err := Unmarshal([]byte("le"), &empty); err != nil || empty == nil {
		t.Fatal(empty, err)
	}
}

func TestInvalidUnmarshal(t *testing.T) {
	var ie *InvalidUnmarshalError
	var x int
	if err := Unmarshal([]byte("i1e"), x); !errors.As(err, &ie) {
		t.Fatal(err)
	}
	if err := Unmarshal([]byte("i1e"), nil); !errors.As(err, &ie) {
		t.Fatal(err)
	}
}

func BenchmarkUnmarshalStruct(b *testing.B) {
	in, _ := Marshal(sample{Name: "a.txt", Len: 1 << 30, Pieces: make([]byte, 2000), List: []string{"a", "b"}, Raw: RawMessage("i1e")})
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		var s sample
		if err := Unmarshal(in, &s); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalStruct(b *testing.B) {
	s := sample{Name: "a.txt", Len: 1 << 30, Pieces: make([]byte, 2000), List: []string{"a", "b"}, Raw: RawMessage("i1e")}
	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		var err error
		if buf, err = Append(buf[:0], s); err != nil {
			b.Fatal(err)
		}
	}
	_ = bytes.Clone(buf)
}
