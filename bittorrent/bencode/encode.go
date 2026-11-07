package bencode

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
)

// Marshaler is a type that encodes itself. MarshalBencode must return
// exactly one valid bencoded value.
type Marshaler interface {
	MarshalBencode() ([]byte, error)
}

// UnsupportedTypeError is returned by Marshal for a type bencoding has no
// form for, such as float64, a channel, or a map with integer keys.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "bencode: unsupported type " + e.Type.String()
}

// MarshalerError wraps an error returned by a Marshaler, or reports that
// one returned an invalid value.
type MarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e *MarshalerError) Error() string {
	return "bencode: error calling MarshalBencode for type " + e.Type.String() + ": " + e.Err.Error()
}

func (e *MarshalerError) Unwrap() error { return e.Err }

// maxEncodeDepth stops a cyclic value (a pointer to itself) from
// recursing until the stack runs out.
const maxEncodeDepth = 1000

var (
	marshalerType   = reflect.TypeFor[Marshaler]()
	unmarshalerType = reflect.TypeFor[Unmarshaler]()
	rawMessageType  = reflect.TypeFor[RawMessage]()
)

// Marshal returns the bencoding of v.
func Marshal(v any) ([]byte, error) {
	return Append(nil, v)
}

// Append appends the bencoding of v to dst and returns the extended
// buffer. On error dst is returned unchanged in length.
func Append(dst []byte, v any) ([]byte, error) {
	e := encodeState{buf: dst}
	if err := e.value(reflect.ValueOf(v), 0); err != nil {
		return dst[:len(dst):len(dst)], err
	}
	return e.buf, nil
}

// An Encoder writes bencoded values to a stream.
type Encoder struct {
	w   io.Writer
	buf []byte
}

// NewEncoder returns an Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// Encode writes the bencoding of v to the stream.
func (enc *Encoder) Encode(v any) error {
	b, err := Append(enc.buf[:0], v)
	if err != nil {
		return err
	}
	enc.buf = b
	_, err = enc.w.Write(b)
	return err
}

type encodeState struct {
	buf []byte
}

func (e *encodeState) value(v reflect.Value, depth int) error {
	if depth > maxEncodeDepth {
		return fmt.Errorf("bencode: value nested more than %d deep (a cycle?)", maxEncodeDepth)
	}
	if !v.IsValid() {
		return fmt.Errorf("bencode: cannot encode nil")
	}
	t := v.Type()
	if t == rawMessageType {
		raw := v.Bytes()
		if len(raw) == 0 {
			return fmt.Errorf("bencode: cannot encode an empty RawMessage")
		}
		if n, err := scanValue(raw, defaultMaxDepth); err != nil || n != len(raw) {
			return fmt.Errorf("bencode: invalid RawMessage")
		}
		e.buf = append(e.buf, raw...)
		return nil
	}
	if t.Implements(marshalerType) {
		if t.Kind() == reflect.Pointer && v.IsNil() {
			return fmt.Errorf("bencode: cannot encode nil %s", t)
		}
		return e.marshaler(v.Interface().(Marshaler), t)
	}
	if t.Kind() != reflect.Pointer && v.CanAddr() && reflect.PointerTo(t).Implements(marshalerType) {
		return e.marshaler(v.Addr().Interface().(Marshaler), t)
	}

	switch t.Kind() {
	case reflect.Bool:
		if v.Bool() {
			e.buf = append(e.buf, "i1e"...)
		} else {
			e.buf = append(e.buf, "i0e"...)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.buf = append(e.buf, 'i')
		e.buf = strconv.AppendInt(e.buf, v.Int(), 10)
		e.buf = append(e.buf, 'e')
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		e.buf = append(e.buf, 'i')
		e.buf = strconv.AppendUint(e.buf, v.Uint(), 10)
		e.buf = append(e.buf, 'e')
	case reflect.String:
		e.str(v.String())
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			e.bytes(v.Bytes())
			return nil
		}
		return e.list(v, depth)
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			e.byteArray(v)
			return nil
		}
		return e.list(v, depth)
	case reflect.Map:
		return e.mapValue(v, depth)
	case reflect.Struct:
		return e.structValue(v, depth)
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return fmt.Errorf("bencode: cannot encode nil %s", t)
		}
		return e.value(v.Elem(), depth+1)
	default:
		return &UnsupportedTypeError{Type: t}
	}
	return nil
}

func (e *encodeState) marshaler(m Marshaler, t reflect.Type) error {
	b, err := m.MarshalBencode()
	if err != nil {
		return &MarshalerError{Type: t, Err: err}
	}
	if n, err := scanValue(b, defaultMaxDepth); err != nil || n != len(b) {
		if err == nil {
			err = fmt.Errorf("%d bytes after the value", len(b)-n)
		}
		return &MarshalerError{Type: t, Err: err}
	}
	e.buf = append(e.buf, b...)
	return nil
}

func (e *encodeState) str(s string) {
	e.buf = strconv.AppendInt(e.buf, int64(len(s)), 10)
	e.buf = append(e.buf, ':')
	e.buf = append(e.buf, s...)
}

func (e *encodeState) bytes(b []byte) {
	e.buf = strconv.AppendInt(e.buf, int64(len(b)), 10)
	e.buf = append(e.buf, ':')
	e.buf = append(e.buf, b...)
}

func (e *encodeState) byteArray(v reflect.Value) {
	n := v.Len()
	e.buf = strconv.AppendInt(e.buf, int64(n), 10)
	e.buf = append(e.buf, ':')
	if v.CanAddr() {
		e.buf = append(e.buf, v.Bytes()...)
		return
	}
	for i := 0; i < n; i++ {
		e.buf = append(e.buf, byte(v.Index(i).Uint()))
	}
}

func (e *encodeState) list(v reflect.Value, depth int) error {
	e.buf = append(e.buf, 'l')
	for i, n := 0, v.Len(); i < n; i++ {
		if err := e.value(v.Index(i), depth+1); err != nil {
			return err
		}
	}
	e.buf = append(e.buf, 'e')
	return nil
}

// omitted reports whether a dictionary leaves v out: nil pointers,
// interfaces and empty RawMessages always (bencoding has no null), other
// empty values when the field says omitempty.
func omitted(v reflect.Value, omitEmpty bool) bool {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return true
		}
	case reflect.Slice:
		if v.Type() == rawMessageType && v.Len() == 0 {
			return true
		}
	}
	if !omitEmpty {
		return false
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Map, reflect.String:
		return v.Len() == 0
	}
	return v.IsZero()
}

func (e *encodeState) mapValue(v reflect.Value, depth int) error {
	t := v.Type()
	kt := t.Key()
	if kt.Kind() != reflect.String && (kt.Kind() != reflect.Array || kt.Elem().Kind() != reflect.Uint8) {
		return &UnsupportedTypeError{Type: t}
	}
	type entry struct {
		key string
		val reflect.Value
	}
	entries := make([]entry, 0, v.Len())
	iter := v.MapRange()
	for iter.Next() {
		val := iter.Value()
		if omitted(val, false) {
			continue
		}
		k := iter.Key()
		var ks string
		if kt.Kind() == reflect.String {
			ks = k.String()
		} else {
			b := make([]byte, k.Len())
			for i := range b {
				b[i] = byte(k.Index(i).Uint())
			}
			ks = string(b)
		}
		entries = append(entries, entry{ks, val})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	e.buf = append(e.buf, 'd')
	for _, en := range entries {
		e.str(en.key)
		if err := e.value(en.val, depth+1); err != nil {
			return err
		}
	}
	e.buf = append(e.buf, 'e')
	return nil
}

func (e *encodeState) structValue(v reflect.Value, depth int) error {
	si := cachedStructInfo(v.Type())
	e.buf = append(e.buf, 'd')
	for i := range si.fields {
		f := &si.fields[i]
		fv, ok := fieldByIndex(v, f.index)
		if !ok || omitted(fv, f.omitEmpty) {
			continue
		}
		e.str(f.name)
		if err := e.value(fv, depth+1); err != nil {
			return err
		}
	}
	e.buf = append(e.buf, 'e')
	return nil
}

// fieldByIndex is v.FieldByIndex that reports false, rather than
// panicking, at a nil embedded pointer.
func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v, true
}
