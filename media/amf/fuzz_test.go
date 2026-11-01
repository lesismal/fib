package amf

import (
	"encoding/hex"
	"reflect"
	"testing"
)

// FuzzDecode decodes arbitrary bytes as AMF0: it either fails or yields
// values that encode, and decode back to the same.
func FuzzDecode(f *testing.F) {
	b, _ := hex.DecodeString(connectHex)
	f.Add(b)
	seed, _ := Append(nil, "onMetaData", ECMAArray{{"width", 1280.0}}, []any{true, nil, Undefined{}}, Date{}, XMLDocument("x"),
		TypedObject{Class: "c"})
	f.Add(seed)
	f.Add([]byte{MarkerObject, 0, 1, 'a', MarkerReference, 0, 0, 0, 0, MarkerObjectEnd})
	f.Add(cat([]byte{MarkerAVMPlus, marker3Object}, u29(1<<4|0x08|0x03), str3(""), str3("s"), []byte{marker3Integer}, u29(7), str3("")))
	f.Fuzz(func(t *testing.T, b []byte) {
		values, err := Decode(b)
		if err != nil {
			return
		}
		out, err := Append(nil, values...)
		if err != nil {
			// A reference to an object being decoded resolves to nil,
			// and AMF3-only types have no AMF0 encoding.
			return
		}
		again, err := Decode(out)
		if err != nil {
			t.Fatalf("%x -> %#v -> %x: %v", b, values, out, err)
		}
		if !reflect.DeepEqual(normalize(values), normalize(again)) {
			t.Fatalf("%#v != %#v", values, again)
		}
	})
}

// normalize maps what AMF0 cannot tell apart after a round trip (a NaN is
// not equal to itself) to comparable values.
func normalize(v any) any {
	switch v := v.(type) {
	case float64:
		if v != v {
			return "NaN"
		}
	case Date:
		if v.Millis != v.Millis {
			return "NaNDate"
		}
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = normalize(e)
		}
		return out
	case Object:
		return normalizeProps(v)
	case ECMAArray:
		return ECMAArray(normalizeProps(v))
	case TypedObject:
		return TypedObject{v.Class, normalizeProps(v.Object)}
	}
	return v
}

func normalizeProps(props []Property) Object {
	out := make(Object, len(props))
	for i, p := range props {
		out[i] = Property{p.Key, normalize(p.Value)}
	}
	return out
}

// FuzzDecode3 decodes arbitrary bytes as AMF3; it must never panic.
func FuzzDecode3(f *testing.F) {
	f.Add(cat([]byte{marker3Array}, u29(1<<1|1), str3("k"), []byte{marker3True}, str3(""), []byte{marker3False}))
	f.Add(cat([]byte{marker3VectorObject}, u29(1<<1|1), []byte{0}, str3("Foo"), []byte{marker3Null}))
	f.Add([]byte{marker3Dictionary, 3, 0, marker3Integer, 1, marker3True})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = Decode3(b)
	})
}
