package amf

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"time"
)

// AMF0 type markers.
const (
	MarkerNumber      = 0x00
	MarkerBoolean     = 0x01
	MarkerString      = 0x02
	MarkerObject      = 0x03
	MarkerMovieClip   = 0x04
	MarkerNull        = 0x05
	MarkerUndefined   = 0x06
	MarkerReference   = 0x07
	MarkerECMAArray   = 0x08
	MarkerObjectEnd   = 0x09
	MarkerStrictArray = 0x0a
	MarkerDate        = 0x0b
	MarkerLongString  = 0x0c
	MarkerUnsupported = 0x0d
	MarkerRecordSet   = 0x0e
	MarkerXMLDocument = 0x0f
	MarkerTypedObject = 0x10
	MarkerAVMPlus     = 0x11
)

// MaxDepth bounds how deeply values may nest, so that a hostile message
// cannot exhaust the stack.
const MaxDepth = 64

// Errors decoding and encoding report.
var (
	ErrShort          = errors.New("amf: value cut short")
	ErrMarker         = errors.New("amf: unknown or reserved type marker")
	ErrDepth          = errors.New("amf: values nested too deeply")
	ErrReference      = errors.New("amf: reference to no value")
	ErrExternalizable = errors.New("amf: AMF3 externalizable object")
	ErrType           = errors.New("amf: Go type has no AMF0 encoding")
	ErrKeyTooLong     = errors.New("amf: property name longer than 65535 bytes")
)

// Property is one name and value of an Object or ECMAArray.
type Property struct {
	Key   string
	Value any
}

// Object is an anonymous AMF object: its properties in the order they were
// written.
type Object []Property

// ECMAArray is an associative array: properties in order, as Object, but
// written with the ECMA array marker that FLV's onMetaData uses.
type ECMAArray []Property

// TypedObject is an object with a class name.
type TypedObject struct {
	Class  string
	Object Object
}

// Undefined is AMF's undefined value, distinct from null (nil).
type Undefined struct{}

// Unsupported is AMF0's unsupported marker.
type Unsupported struct{}

// Date is a point in time: milliseconds since the Unix epoch, UTC, and a
// time zone offset AMF0 carries but says to ignore.
type Date struct {
	Millis   float64
	TimeZone int16
}

// Time returns d as a time.Time.
func (d Date) Time() time.Time { return time.UnixMilli(int64(d.Millis)).UTC() }

// XMLDocument is an XML document, carried as its text.
type XMLDocument string

// Dictionary is an AMF3 dictionary: keys of any type, in order.
type Dictionary []DictionaryEntry

// DictionaryEntry is one key and value of a Dictionary.
type DictionaryEntry struct {
	Key, Value any
}

func lookup(props []Property, key string) (any, bool) {
	for _, p := range props {
		if p.Key == key {
			return p.Value, true
		}
	}
	return nil, false
}

// Get returns the value of the first property named key.
func (o Object) Get(key string) (any, bool) { return lookup(o, key) }

// String returns the property named key when it is a string.
func (o Object) String(key string) (string, bool) {
	v, _ := lookup(o, key)
	s, ok := v.(string)
	return s, ok
}

// Number returns the property named key when it is a number.
func (o Object) Number(key string) (float64, bool) {
	v, _ := lookup(o, key)
	n, ok := v.(float64)
	return n, ok
}

// Get returns the value of the first property named key.
func (a ECMAArray) Get(key string) (any, bool) { return lookup(a, key) }

// String returns the property named key when it is a string.
func (a ECMAArray) String(key string) (string, bool) { return Object(a).String(key) }

// Number returns the property named key when it is a number.
func (a ECMAArray) Number(key string) (float64, bool) { return Object(a).Number(key) }

// Append appends the AMF0 encoding of each value to dst.
func Append(dst []byte, values ...any) ([]byte, error) {
	var err error
	for _, v := range values {
		if dst, err = appendValue(dst, v, 0); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

// AppendNumber, AppendString and the rest append one value of a known type,
// without the type switch Append goes through.

// AppendNumber appends n as an AMF0 number.
func AppendNumber(dst []byte, n float64) []byte {
	dst = append(dst, MarkerNumber)
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(n))
}

// AppendBool appends b as an AMF0 boolean.
func AppendBool(dst []byte, b bool) []byte {
	if b {
		return append(dst, MarkerBoolean, 1)
	}
	return append(dst, MarkerBoolean, 0)
}

// AppendString appends s as an AMF0 string, or a long string when it is
// longer than 65535 bytes.
func AppendString(dst []byte, s string) []byte {
	if len(s) > math.MaxUint16 {
		dst = append(dst, MarkerLongString)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
		return append(dst, s...)
	}
	dst = append(dst, MarkerString)
	return appendUTF8(dst, s)
}

func appendUTF8(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func appendProps(dst []byte, props []Property, depth int) ([]byte, error) {
	var err error
	for _, p := range props {
		if len(p.Key) > math.MaxUint16 {
			return dst, ErrKeyTooLong
		}
		dst = appendUTF8(dst, p.Key)
		if dst, err = appendValue(dst, p.Value, depth+1); err != nil {
			return dst, err
		}
	}
	return append(dst, 0, 0, MarkerObjectEnd), nil
}

func appendValue(dst []byte, v any, depth int) ([]byte, error) {
	if depth > MaxDepth {
		return dst, ErrDepth
	}
	switch v := v.(type) {
	case nil:
		return append(dst, MarkerNull), nil
	case Undefined:
		return append(dst, MarkerUndefined), nil
	case Unsupported:
		return append(dst, MarkerUnsupported), nil
	case bool:
		return AppendBool(dst, v), nil
	case float64:
		return AppendNumber(dst, v), nil
	case float32:
		return AppendNumber(dst, float64(v)), nil
	case int:
		return AppendNumber(dst, float64(v)), nil
	case int8:
		return AppendNumber(dst, float64(v)), nil
	case int16:
		return AppendNumber(dst, float64(v)), nil
	case int32:
		return AppendNumber(dst, float64(v)), nil
	case int64:
		return AppendNumber(dst, float64(v)), nil
	case uint:
		return AppendNumber(dst, float64(v)), nil
	case uint8:
		return AppendNumber(dst, float64(v)), nil
	case uint16:
		return AppendNumber(dst, float64(v)), nil
	case uint32:
		return AppendNumber(dst, float64(v)), nil
	case uint64:
		return AppendNumber(dst, float64(v)), nil
	case string:
		return AppendString(dst, v), nil
	case XMLDocument:
		dst = append(dst, MarkerXMLDocument)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
		return append(dst, v...), nil
	case Date:
		dst = append(dst, MarkerDate)
		dst = binary.BigEndian.AppendUint64(dst, math.Float64bits(v.Millis))
		return binary.BigEndian.AppendUint16(dst, uint16(v.TimeZone)), nil
	case time.Time:
		return appendValue(dst, Date{Millis: float64(v.UnixMilli())}, depth)
	case Object:
		return appendProps(append(dst, MarkerObject), v, depth)
	case TypedObject:
		if len(v.Class) > math.MaxUint16 {
			return dst, ErrKeyTooLong
		}
		dst = appendUTF8(append(dst, MarkerTypedObject), v.Class)
		return appendProps(dst, v.Object, depth)
	case ECMAArray:
		dst = append(dst, MarkerECMAArray)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
		return appendProps(dst, v, depth)
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		arr := make(ECMAArray, 0, len(keys))
		for _, k := range keys {
			arr = append(arr, Property{k, v[k]})
		}
		return appendValue(dst, arr, depth)
	case []any:
		dst = append(dst, MarkerStrictArray)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
		var err error
		for _, e := range v {
			if dst, err = appendValue(dst, e, depth+1); err != nil {
				return dst, err
			}
		}
		return dst, nil
	}
	return dst, ErrType
}

// Decoder decodes the AMF0 values of one message. References (AMF0's, and
// AMF3's inside AVM+ values) refer to values earlier in the same message, so
// one Decoder decodes one message and is then reset or dropped.
type Decoder struct {
	b    []byte
	off  int
	refs []any
	amf3 decoder3
}

// NewDecoder returns a Decoder of the values in b.
func NewDecoder(b []byte) *Decoder { return &Decoder{b: b} }

// Reset makes d decode b, forgetting the references of what it decoded
// before.
func (d *Decoder) Reset(b []byte) {
	d.b, d.off = b, 0
	clear(d.refs)
	d.refs = d.refs[:0]
	d.amf3.reset()
}

// More reports whether bytes are left to decode.
func (d *Decoder) More() bool { return d.off < len(d.b) }

// Rest returns the bytes not decoded yet.
func (d *Decoder) Rest() []byte { return d.b[d.off:] }

// Decode decodes the next value.
func (d *Decoder) Decode() (any, error) { return d.value(0) }

// Decode decodes every AMF0 value in b.
func Decode(b []byte) ([]any, error) {
	d := Decoder{b: b}
	var values []any
	for d.More() {
		v, err := d.Decode()
		if err != nil {
			return values, err
		}
		values = append(values, v)
	}
	return values, nil
}

func (d *Decoder) need(n int) ([]byte, error) {
	if n < 0 || len(d.b)-d.off < n {
		return nil, ErrShort
	}
	b := d.b[d.off : d.off+n]
	d.off += n
	return b, nil
}

func (d *Decoder) u16() (int, error) {
	b, err := d.need(2)
	if err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint16(b)), nil
}

func (d *Decoder) u32() (uint32, error) {
	b, err := d.need(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (d *Decoder) utf8(long bool) (string, error) {
	var n int
	if long {
		u, err := d.u32()
		if err != nil {
			return "", err
		}
		if int64(u) > int64(len(d.b)-d.off) {
			return "", ErrShort
		}
		n = int(u)
	} else {
		var err error
		if n, err = d.u16(); err != nil {
			return "", err
		}
	}
	b, err := d.need(n)
	return string(b), err
}

func (d *Decoder) number() (float64, error) {
	b, err := d.need(8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
}

// props decodes properties up to the object end marker. An ECMA array may
// end with the input instead, which some encoders write.
func (d *Decoder) props(depth int, endOptional bool) ([]Property, error) {
	var props []Property
	for {
		if endOptional && d.off == len(d.b) {
			return props, nil
		}
		key, err := d.utf8(false)
		if err != nil {
			return props, err
		}
		if key == "" {
			if d.off < len(d.b) && d.b[d.off] == MarkerObjectEnd {
				d.off++
				return props, nil
			}
			if endOptional && d.off == len(d.b) {
				return props, nil
			}
		}
		v, err := d.value(depth + 1)
		if err != nil {
			return props, err
		}
		props = append(props, Property{key, v})
	}
}

// ref reserves the reference index of a complex value about to be decoded.
func (d *Decoder) ref() int {
	d.refs = append(d.refs, nil)
	return len(d.refs) - 1
}

func (d *Decoder) value(depth int) (any, error) {
	if depth > MaxDepth {
		return nil, ErrDepth
	}
	m, err := d.need(1)
	if err != nil {
		return nil, err
	}
	switch m[0] {
	case MarkerNumber:
		return d.number()
	case MarkerBoolean:
		b, err := d.need(1)
		if err != nil {
			return nil, err
		}
		return b[0] != 0, nil
	case MarkerString:
		return d.utf8(false)
	case MarkerLongString:
		return d.utf8(true)
	case MarkerXMLDocument:
		s, err := d.utf8(true)
		return XMLDocument(s), err
	case MarkerNull:
		return nil, nil
	case MarkerUndefined:
		return Undefined{}, nil
	case MarkerUnsupported:
		return Unsupported{}, nil
	case MarkerDate:
		ms, err := d.number()
		if err != nil {
			return nil, err
		}
		tz, err := d.u16()
		return Date{Millis: ms, TimeZone: int16(tz)}, err
	case MarkerReference:
		i, err := d.u16()
		if err != nil {
			return nil, err
		}
		if i >= len(d.refs) {
			return nil, ErrReference
		}
		return d.refs[i], nil
	case MarkerObject:
		r := d.ref()
		props, err := d.props(depth, false)
		d.refs[r] = Object(props)
		return Object(props), err
	case MarkerTypedObject:
		class, err := d.utf8(false)
		if err != nil {
			return nil, err
		}
		r := d.ref()
		props, err := d.props(depth, false)
		v := TypedObject{Class: class, Object: props}
		d.refs[r] = v
		return v, err
	case MarkerECMAArray:
		if _, err := d.u32(); err != nil {
			return nil, err
		}
		r := d.ref()
		props, err := d.props(depth, true)
		d.refs[r] = ECMAArray(props)
		return ECMAArray(props), err
	case MarkerStrictArray:
		n, err := d.u32()
		if err != nil {
			return nil, err
		}
		// Every element takes at least a byte, which bounds what a count
		// may make the decoder allocate.
		if int64(n) > int64(len(d.b)-d.off) {
			return nil, ErrShort
		}
		r := d.ref()
		arr := make([]any, 0, n)
		for range n {
			v, err := d.value(depth + 1)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v)
		}
		d.refs[r] = arr
		return arr, nil
	case MarkerAVMPlus:
		d.amf3.b, d.amf3.off = d.b, d.off
		v, err := d.amf3.value(depth)
		d.off = d.amf3.off
		return v, err
	}
	return nil, ErrMarker
}
