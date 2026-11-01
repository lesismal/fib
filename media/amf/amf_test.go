package amf

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// connectHex is the connect command ffmpeg 5.1 sends to publish
// rtmp://127.0.0.1/live/s, captured from the wire.
const connectHex = "020007636f6e6e656374003ff00000000000000300036170700200046c69766500047479706502000a6e6f6e707269766174650008666c617368566572020024464d4c452f332e302028636f6d70617469626c653b204c61766635392e32372e313030290005746355726c02001a72746d703a2f2f3132372e302e302e313a313933352f6c697665000009"

func TestDecodeConnect(t *testing.T) {
	b, err := hex.DecodeString(connectHex)
	if err != nil {
		t.Fatal(err)
	}
	values, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || values[0] != "connect" || values[1] != 1.0 {
		t.Fatalf("values %#v", values)
	}
	obj := values[2].(Object)
	if app, _ := obj.String("app"); app != "live" {
		t.Fatalf("app %q", app)
	}
	if tc, _ := obj.String("tcUrl"); tc != "rtmp://127.0.0.1:1935/live" {
		t.Fatalf("tcUrl %q", tc)
	}
	if obj[0].Key != "app" {
		t.Fatal("order lost")
	}
	// Encoding the decoded values gives back the bytes.
	out, err := Append(nil, values...)
	if err != nil || !bytes.Equal(out, b) {
		t.Fatalf("%v\n%x\n%x", err, out, b)
	}
}

func TestRoundTrip(t *testing.T) {
	long := strings.Repeat("x", 70000)
	values := []any{
		1.5, true, false, "hello", long, nil, Undefined{}, Unsupported{},
		Object{{"a", 1.0}, {"b", Object{{"c", "d"}}}},
		ECMAArray{{"duration", 0.0}, {"width", 1280.0}},
		[]any{1.0, "two", nil},
		Date{Millis: 1e12, TimeZone: -60},
		XMLDocument("<a/>"),
		TypedObject{Class: "flex.Foo", Object: Object{{"x", 2.0}}},
		Object(nil),
	}
	b, err := Append(nil, values...)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, values) {
		t.Fatalf("got %#v", got)
	}
}

func TestEncodeConversions(t *testing.T) {
	now := time.UnixMilli(1700000000123)
	in := []any{int(1), int8(2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10), float32(11), now,
		map[string]any{"b": 2, "a": "x"}}
	b, err := Append(nil, in...)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 11 {
		if got[i] != float64(i+1) {
			t.Fatalf("%d: %v", i, got[i])
		}
	}
	if d := got[11].(Date); !d.Time().Equal(now) {
		t.Fatalf("date %v", d.Time())
	}
	if arr := got[12].(ECMAArray); arr[0].Key != "a" || arr[1].Value != 2.0 {
		t.Fatalf("map %#v", arr)
	}
	if n, ok := got[12].(ECMAArray).Number("b"); !ok || n != 2 {
		t.Fatal("ECMAArray.Number")
	}
	if s, ok := got[12].(ECMAArray).String("a"); !ok || s != "x" {
		t.Fatal("ECMAArray.String")
	}
	if _, ok := got[12].(ECMAArray).Get("z"); ok {
		t.Fatal("ECMAArray.Get")
	}
	if _, ok := (Object{{"a", 1.0}}).Get("a"); !ok {
		t.Fatal("Object.Get")
	}
	if _, ok := (Object{{"a", 1.0}}).Number("a"); !ok {
		t.Fatal("Object.Number")
	}
}

func TestEncodeErrors(t *testing.T) {
	if _, err := Append(nil, struct{}{}); !errors.Is(err, ErrType) {
		t.Fatal(err)
	}
	if _, err := Append(nil, []any{struct{}{}}); !errors.Is(err, ErrType) {
		t.Fatal(err)
	}
	if _, err := Append(nil, Object{{"a", struct{}{}}}); !errors.Is(err, ErrType) {
		t.Fatal(err)
	}
	long := strings.Repeat("k", 70000)
	if _, err := Append(nil, Object{{long, 1}}); !errors.Is(err, ErrKeyTooLong) {
		t.Fatal(err)
	}
	if _, err := Append(nil, TypedObject{Class: long}); !errors.Is(err, ErrKeyTooLong) {
		t.Fatal(err)
	}
	var deep any = 1.0
	for range MaxDepth + 2 {
		deep = []any{deep}
	}
	if _, err := Append(nil, deep); !errors.Is(err, ErrDepth) {
		t.Fatal(err)
	}
}

func TestReference(t *testing.T) {
	obj := Object{{"a", 1.0}}
	b, _ := Append(nil, obj)
	b = append(b, MarkerReference, 0, 0)
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got[1], obj) {
		t.Fatalf("%#v", got)
	}
	if _, err := Decode([]byte{MarkerReference, 0, 1}); !errors.Is(err, ErrReference) {
		t.Fatal(err)
	}
	if _, err := Decode([]byte{MarkerReference, 0}); !errors.Is(err, ErrShort) {
		t.Fatal(err)
	}
}

func TestECMAArrayWithoutEnd(t *testing.T) {
	// Some encoders end an ECMA array with the message, without the end
	// marker.
	b := []byte{MarkerECMAArray, 0, 0, 0, 1, 0, 1, 'a', MarkerNumber, 0, 0, 0, 0, 0, 0, 0, 0}
	got, err := Decode(b)
	if err != nil || len(got[0].(ECMAArray)) != 1 {
		t.Fatal(got, err)
	}
	// An empty key that is not the end marker, cut short.
	b = []byte{MarkerECMAArray, 0, 0, 0, 1, 0, 0}
	if got, err := Decode(b); err != nil || len(got[0].(ECMAArray)) != 0 {
		t.Fatal(got, err)
	}
	// An empty key followed by a value.
	b = []byte{MarkerObject, 0, 0, MarkerNull, 0, 0, MarkerObjectEnd}
	if got, err := Decode(b); err != nil || len(got[0].(Object)) != 1 {
		t.Fatal(got, err)
	}
}

func TestDecoderReuse(t *testing.T) {
	b, _ := Append(nil, Object{{"a", 1.0}}, "x")
	d := NewDecoder(b)
	if _, err := d.Decode(); err != nil {
		t.Fatal(err)
	}
	if !d.More() || len(d.Rest()) != 4 {
		t.Fatal("rest")
	}
	d.Reset([]byte{MarkerReference, 0, 0})
	if _, err := d.Decode(); !errors.Is(err, ErrReference) {
		t.Fatal("references kept across Reset", err)
	}
}

func TestDecodeErrors(t *testing.T) {
	cases := [][]byte{
		{MarkerNumber, 0},
		{MarkerBoolean},
		{MarkerString, 0, 5, 'a'},
		{MarkerString, 0},
		{MarkerLongString, 0, 0, 0, 9, 'a'},
		{MarkerLongString, 0, 0},
		{MarkerXMLDocument, 0, 0, 0, 2},
		{MarkerDate, 0, 0, 0, 0, 0, 0, 0, 0},
		{MarkerDate, 0},
		{MarkerObject, 0, 1, 'a'},
		{MarkerObject, 0, 1},
		{MarkerTypedObject, 0, 1},
		{MarkerTypedObject, 0, 1, 'c', 0},
		{MarkerECMAArray, 0, 0},
		{MarkerStrictArray, 0, 0},
		{MarkerStrictArray, 0xff, 0xff, 0xff, 0xff},
		{MarkerStrictArray, 0, 0, 0, 1, MarkerNumber},
		{MarkerMovieClip},
		{MarkerRecordSet},
		{0x20},
		{MarkerAVMPlus},
	}
	for _, c := range cases {
		if _, err := Decode(c); err == nil {
			t.Errorf("%x decoded", c)
		}
	}
	var deep []byte
	for range MaxDepth + 2 {
		deep = append(deep, MarkerStrictArray, 0, 0, 0, 1)
	}
	deep = append(deep, MarkerNull)
	if _, err := Decode(deep); !errors.Is(err, ErrDepth) {
		t.Fatal(err)
	}
}

func u29(v uint32) []byte {
	switch {
	case v < 0x80:
		return []byte{byte(v)}
	case v < 0x4000:
		return []byte{byte(v>>7) | 0x80, byte(v & 0x7f)}
	case v < 0x200000:
		return []byte{byte(v>>14) | 0x80, byte(v>>7) | 0x80, byte(v & 0x7f)}
	}
	return []byte{byte(v>>22) | 0x80, byte(v>>15) | 0x80, byte(v>>8) | 0x80, byte(v)}
}

func str3(s string) []byte { return append(u29(uint32(len(s))<<1|1), s...) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestAMF3(t *testing.T) {
	putF := func(f float64) []byte {
		b := make([]byte, 8)
		u := math.Float64bits(f)
		for i := range 8 {
			b[i] = byte(u >> (56 - 8*i))
		}
		return b
	}
	b := cat(
		[]byte{marker3Undefined, marker3Null, marker3False, marker3True},
		[]byte{marker3Integer}, u29(5),
		[]byte{marker3Integer}, u29(0x1fffffff), // -1
		[]byte{marker3Integer}, u29(300),
		[]byte{marker3Integer}, u29(0x100000),
		[]byte{marker3Double}, putF(2.5),
		[]byte{marker3String}, str3("hello"),
		[]byte{marker3String}, u29(0), // reference to "hello"
		[]byte{marker3String}, str3(""),
		[]byte{marker3XMLDoc}, str3("<a/>"),
		[]byte{marker3XML}, u29(0), // reference to the XML doc
		[]byte{marker3ByteArray}, str3("\x01\x02"),
		[]byte{marker3Date}, u29(1), putF(1000),
		[]byte{marker3Date}, u29(2<<1), // reference to the date
		// A dense array.
		[]byte{marker3Array}, u29(2<<1|1), str3(""), []byte{marker3Integer}, u29(1), []byte{marker3Null},
		// A mixed array.
		[]byte{marker3Array}, u29(1<<1|1), str3("k"), []byte{marker3True}, str3(""), []byte{marker3False},
		// An anonymous dynamic object with one sealed member.
		[]byte{marker3Object}, u29(1<<4|0x08|0x03), str3(""), str3("s"), []byte{marker3Integer}, u29(7), str3("d"), []byte{marker3Null}, str3(""),
		// An object of the same traits, by reference.
		[]byte{marker3Object}, u29(0<<2|0x01), []byte{marker3Integer}, u29(8), str3(""),
		// A typed object.
		[]byte{marker3Object}, u29(0<<4|0x03), str3("Foo"),
		// The first object by reference.
		[]byte{marker3Object}, u29(5<<1),
		[]byte{marker3VectorInt}, u29(2<<1|1), []byte{0}, []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 1},
		[]byte{marker3VectorUint}, u29(1<<1|1), []byte{0}, []byte{0xff, 0xff, 0xff, 0xff},
		[]byte{marker3VectorDouble}, u29(1<<1|1), []byte{0}, putF(0.5),
		[]byte{marker3VectorObject}, u29(1<<1|1), []byte{0}, str3("Foo"), []byte{marker3Null},
		[]byte{marker3VectorInt}, u29(8<<1), // reference to the first vector
		[]byte{marker3Dictionary}, u29(1<<1|1), []byte{0}, []byte{marker3Integer}, u29(1), []byte{marker3True},
		[]byte{marker3Dictionary}, u29(12<<1),
		[]byte{marker3Array}, u29(4<<1),
	)
	got, err := Decode3(b)
	if err != nil {
		t.Fatalf("%v after %#v", err, got)
	}
	want := []any{
		Undefined{}, nil, false, true, 5.0, -1.0, 300.0, float64(0x100000), 2.5, "hello", "hello", "",
		XMLDocument("<a/>"), XMLDocument("<a/>"), []byte{1, 2}, Date{Millis: 1000}, Date{Millis: 1000},
		[]any{1.0, nil},
		ECMAArray{{"k", true}, {"0", false}},
		Object{{"s", 7.0}, {"d", nil}},
		Object{{"s", 8.0}},
		TypedObject{Class: "Foo", Object: Object{}},
		Object{{"s", 7.0}, {"d", nil}},
		[]any{-1.0, 1.0},
		[]any{float64(0xffffffff)},
		[]any{0.5},
		[]any{nil},
		[]any{-1.0, 1.0},
		Dictionary{{1.0, true}},
		Dictionary{{1.0, true}},
		ECMAArray{{"k", true}, {"0", false}},
	}
	if !reflect.DeepEqual(got, want) {
		for i := range min(len(got), len(want)) {
			if !reflect.DeepEqual(got[i], want[i]) {
				t.Errorf("%d: got %#v want %#v", i, got[i], want[i])
			}
		}
		t.Fatalf("len %d want %d", len(got), len(want))
	}
}

func TestAMF0SwitchToAMF3(t *testing.T) {
	b := cat([]byte{MarkerString}, []byte{0, 1, 'x'}, []byte{MarkerAVMPlus, marker3String}, str3("y"),
		[]byte{MarkerAVMPlus, marker3String}, u29(0))
	got, err := Decode(b)
	if err != nil || !reflect.DeepEqual(got, []any{"x", "y", "y"}) {
		t.Fatal(got, err)
	}
}

func TestAMF3Errors(t *testing.T) {
	cases := [][]byte{
		{},
		{marker3Integer},
		{marker3Integer, 0x80},
		{marker3Double, 0},
		{marker3String, 5, 'a'},
		{marker3String, 2},
		{marker3String},
		{marker3XMLDoc, 2},
		{marker3XMLDoc},
		{marker3XMLDoc, 5, 1},
		{marker3Date},
		{marker3Date, 1},
		{marker3Date, 0},
		{marker3Array},
		{marker3Array, 0},
		{marker3Array, 3},
		{marker3Array, 3, 3, 'k'},
		{marker3Array, 5, 1},
		{marker3Array, 5, 1, marker3Integer},
		{marker3Object},
		{marker3Object, 0},
		{marker3Object, 1},
		{marker3Object, 7},
		{marker3Object, 3},
		{marker3Object, 0x13, 1},
		{marker3Object, 0x13, 1, 3},
		{marker3Object, 0x13, 1, 3, 's'},
		{marker3Object, 0x0b, 1, 3, 'k'},
		{marker3Object, 0x0b, 1, 3, 'k', marker3Null},
		{marker3Object, 0x0b, 1, 3},
		{marker3VectorInt},
		{marker3VectorInt, 0},
		{marker3VectorInt, 3},
		{marker3VectorInt, 3, 0, 1},
		{marker3VectorObject, 3, 0},
		{marker3VectorObject, 3, 0, 1},
		{marker3Dictionary},
		{marker3Dictionary, 0},
		{marker3Dictionary, 3},
		{marker3Dictionary, 3, 0, marker3Null},
		{marker3Dictionary, 3, 0, 0x20, 0},
		{0x20},
	}
	for _, c := range cases {
		if v, err := Decode3(c); err == nil && len(c) > 0 {
			t.Errorf("%x decoded to %#v", c, v)
		}
	}
	var deep []byte
	for range MaxDepth + 2 {
		deep = append(deep, marker3Array, 3, 1)
	}
	deep = append(deep, marker3Null)
	if _, err := Decode3(deep); !errors.Is(err, ErrDepth) {
		t.Fatal(err)
	}
}
