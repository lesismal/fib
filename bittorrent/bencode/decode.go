package bencode

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
)

// Unmarshaler is a type that decodes itself. UnmarshalBencode gets one
// complete, valid value; it must copy the bytes to keep them after
// returning.
type Unmarshaler interface {
	UnmarshalBencode([]byte) error
}

// UnmarshalTypeError is a value that does not fit the Go value it is
// decoded into: a string for an int, an integer out of range, a string of
// the wrong length for a byte array. Decoding goes on past it, skipping
// the value, and Unmarshal returns the first such error after decoding
// everything else, so a message with one odd key still yields the rest.
type UnmarshalTypeError struct {
	Value  string // "integer 300", "string", "list", "dictionary"
	Type   reflect.Type
	Offset int64
}

func (e *UnmarshalTypeError) Error() string {
	return "bencode: cannot decode " + e.Value + " into Go value of type " + e.Type.String() +
		" at offset " + strconv.FormatInt(e.Offset, 10)
}

// InvalidUnmarshalError is a nil or non-pointer argument to Unmarshal.
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "bencode: Unmarshal(nil)"
	}
	if e.Type.Kind() != reflect.Pointer {
		return "bencode: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "bencode: Unmarshal(nil " + e.Type.String() + ")"
}

const (
	defaultMaxDepth = 256
	defaultMaxSize  = 64 << 20
)

// Limits bound what decoding accepts from hostile input. Zero fields take
// the defaults: MaxDepth 256 and MaxSize 64 MiB.
type Limits struct {
	// MaxDepth is how deeply lists and dictionaries may nest.
	MaxDepth int
	// MaxSize is the longest value a Decoder reads from an io.Reader. A
	// byte slice is bounded by its own length.
	MaxSize int
}

func (l Limits) withDefaults() Limits {
	if l.MaxDepth <= 0 {
		l.MaxDepth = defaultMaxDepth
	}
	if l.MaxSize <= 0 {
		l.MaxSize = defaultMaxSize
	}
	return l
}

// Unmarshal decodes the single bencoded value in data into the value v
// points to. Data after the value is an error; Decoder decodes a value
// that something else follows. []byte and RawMessage values are copies, so
// data may be reused afterwards.
func Unmarshal(data []byte, v any) error {
	d := decodeState{data: data, maxDepth: defaultMaxDepth}
	if err := d.unmarshal(v); err != nil {
		return err
	}
	if d.off != len(data) {
		return syntaxErr(d.off, "data after the value")
	}
	return d.typeErr
}

type decodeState struct {
	data     []byte
	off      int
	depth    int
	maxDepth int
	noCopy   bool
	strict   bool
	// base is the offset of data in a stream, for the offsets of errors.
	base    int64
	typeErr error
}

func (d *decodeState) unmarshal(v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return &InvalidUnmarshalError{Type: reflect.TypeOf(v)}
	}
	return d.value(rv)
}

func (d *decodeState) syntax(off int, msg string) error {
	return &SyntaxError{Offset: d.base + int64(off), msg: msg}
}

func (d *decodeState) rebase(err error) error {
	var se *SyntaxError
	if d.base != 0 && errors.As(err, &se) {
		return &SyntaxError{Offset: se.Offset + d.base, msg: se.msg}
	}
	return err
}

// mismatch records that the value at off does not fit t and skips it.
func (d *decodeState) mismatch(what string, t reflect.Type, off int) error {
	if d.typeErr == nil {
		d.typeErr = &UnmarshalTypeError{Value: what, Type: t, Offset: d.base + int64(off)}
	}
	d.off = off
	return d.skip()
}

// skip moves past the value at d.off, checking it.
func (d *decodeState) skip() error {
	end, err := scanValueAt(d.data, d.off, d.maxDepth-d.depth)
	if err != nil {
		return d.rebase(err)
	}
	d.off = end
	return nil
}

// span checks the value at d.off, moves past it and returns its bytes.
func (d *decodeState) span() ([]byte, error) {
	start := d.off
	if err := d.skip(); err != nil {
		return nil, err
	}
	return d.data[start:d.off:d.off], nil
}

func (d *decodeState) value(v reflect.Value) error {
	if d.off >= len(d.data) {
		return d.syntax(d.off, "unexpected end of input")
	}
	// Go through pointers, allocating, until a value that decodes itself
	// or one the bencoded value goes into.
	for {
		t := v.Type()
		if t == rawMessageType {
			raw, err := d.span()
			if err != nil {
				return err
			}
			if !d.noCopy {
				raw = bytes.Clone(raw)
			}
			v.SetBytes(raw)
			return nil
		}
		if t.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(t.Elem()))
			}
			if t.Elem() != rawMessageType && t.Implements(unmarshalerType) {
				return d.unmarshaler(v.Interface().(Unmarshaler))
			}
			v = v.Elem()
			continue
		}
		if v.CanAddr() && reflect.PointerTo(t).Implements(unmarshalerType) {
			return d.unmarshaler(v.Addr().Interface().(Unmarshaler))
		}
		if t.Kind() != reflect.Interface && t.Implements(unmarshalerType) {
			return d.unmarshaler(v.Interface().(Unmarshaler))
		}
		if t.Kind() == reflect.Interface && !v.IsNil() {
			if e := v.Elem(); e.Kind() == reflect.Pointer && !e.IsNil() {
				v = e
				continue
			}
		}
		break
	}

	switch c := d.data[d.off]; {
	case c == 'i':
		return d.integer(v)
	case c >= '0' && c <= '9':
		return d.str(v)
	case c == 'l':
		return d.list(v)
	case c == 'd':
		return d.dict(v)
	default:
		return d.syntax(d.off, "invalid byte "+strconv.QuoteRune(rune(c)))
	}
}

func (d *decodeState) unmarshaler(u Unmarshaler) error {
	raw, err := d.span()
	if err != nil {
		return err
	}
	return u.UnmarshalBencode(raw)
}

func isEmptyInterface(v reflect.Value) bool {
	return v.Kind() == reflect.Interface && v.NumMethod() == 0
}

// parseDigits parses an integer's digits ("-12") into its sign and
// magnitude, reporting false if the magnitude does not fit a uint64.
func parseDigits(b []byte) (neg bool, u uint64, ok bool) {
	if len(b) > 0 && b[0] == '-' {
		neg = true
		b = b[1:]
	}
	for _, c := range b {
		if u > (1<<64-1)/10 {
			return neg, 0, false
		}
		u *= 10
		if u+uint64(c-'0') < u {
			return neg, 0, false
		}
		u += uint64(c - '0')
	}
	return neg, u, true
}

func toInt64(neg bool, u uint64) (int64, bool) {
	if neg {
		if u > 1<<63 {
			return 0, false
		}
		return int64(-u), true
	}
	if u > 1<<63-1 {
		return 0, false
	}
	return int64(u), true
}

func (d *decodeState) integer(v reflect.Value) error {
	start := d.off
	end, err := scanInt(d.data, d.off)
	if err != nil {
		return d.rebase(err)
	}
	digits := d.data[start+1 : end-1]
	d.off = end
	neg, u, ok := parseDigits(digits)
	bad := func() error {
		return d.mismatch("integer "+string(digits), v.Type(), start)
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, fits := toInt64(neg, u)
		if !ok || !fits || v.OverflowInt(n) {
			return bad()
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !ok || neg && u != 0 || v.OverflowUint(u) {
			return bad()
		}
		v.SetUint(u)
	case reflect.Bool:
		v.SetBool(u != 0 || !ok)
	case reflect.Interface:
		n, fits := toInt64(neg, u)
		if !isEmptyInterface(v) || !ok || !fits {
			return bad()
		}
		v.Set(reflect.ValueOf(n))
	default:
		return bad()
	}
	return nil
}

func (d *decodeState) str(v reflect.Value) error {
	start := d.off
	s, e, err := scanString(d.data, d.off)
	if err != nil {
		return d.rebase(err)
	}
	raw := d.data[s:e:e]
	d.off = e
	switch v.Kind() {
	case reflect.String:
		v.SetString(string(raw))
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return d.mismatch("string", v.Type(), start)
		}
		if !d.noCopy {
			raw = bytes.Clone(raw)
		}
		v.SetBytes(raw)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 || v.Len() != len(raw) {
			return d.mismatch("string of length "+strconv.Itoa(len(raw)), v.Type(), start)
		}
		if v.CanAddr() {
			copy(v.Bytes(), raw)
		} else {
			for i, c := range raw {
				v.Index(i).SetUint(uint64(c))
			}
		}
	case reflect.Interface:
		if !isEmptyInterface(v) {
			return d.mismatch("string", v.Type(), start)
		}
		v.Set(reflect.ValueOf(string(raw)))
	default:
		return d.mismatch("string", v.Type(), start)
	}
	return nil
}

func (d *decodeState) enter() error {
	if d.depth >= d.maxDepth {
		return d.syntax(d.off, "nesting too deep")
	}
	d.depth++
	d.off++ // 'l' or 'd'
	return nil
}

// more reports whether the list or dictionary being decoded has another
// item, consuming its 'e' if not.
func (d *decodeState) more() (bool, error) {
	if d.off >= len(d.data) {
		return false, d.syntax(d.off, "unexpected end of input")
	}
	if d.data[d.off] == 'e' {
		d.off++
		d.depth--
		return false, nil
	}
	return true, nil
}

func (d *decodeState) list(v reflect.Value) error {
	start := d.off
	switch v.Kind() {
	case reflect.Slice:
	case reflect.Array:
	case reflect.Interface:
		if !isEmptyInterface(v) {
			return d.mismatch("list", v.Type(), start)
		}
		x, err := d.anyValue()
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(x))
		return nil
	default:
		return d.mismatch("list", v.Type(), start)
	}
	if err := d.enter(); err != nil {
		return err
	}
	i := 0
	for {
		more, err := d.more()
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if v.Kind() == reflect.Slice {
			if i >= v.Cap() {
				v.Grow(1)
			}
			v.SetLen(i + 1)
			// The slot may hold a value from before the slice was reused.
			v.Index(i).SetZero()
		} else if i >= v.Len() {
			if err := d.mismatch("list longer than "+strconv.Itoa(v.Len()), v.Type(), d.off); err != nil {
				return err
			}
			i++
			continue
		}
		if err := d.value(v.Index(i)); err != nil {
			return err
		}
		i++
	}
	switch {
	case v.Kind() == reflect.Array:
		for ; i < v.Len(); i++ {
			v.Index(i).SetZero()
		}
	case v.IsNil():
		v.Set(reflect.MakeSlice(v.Type(), 0, 0))
	default:
		v.SetLen(i)
	}
	return nil
}

// key reads a dictionary key, checking order in strict mode.
func (d *decodeState) key(prev []byte) ([]byte, error) {
	if c := d.data[d.off]; c < '0' || c > '9' {
		return nil, d.syntax(d.off, "dictionary key is not a string")
	}
	s, e, err := scanString(d.data, d.off)
	if err != nil {
		return nil, d.rebase(err)
	}
	k := d.data[s:e:e]
	if d.strict && prev != nil && bytes.Compare(prev, k) >= 0 {
		return nil, d.syntax(d.off, "dictionary keys not sorted and unique")
	}
	d.off = e
	if d.off >= len(d.data) {
		return nil, d.syntax(d.off, "dictionary key without a value")
	}
	if d.data[d.off] == 'e' {
		return nil, d.syntax(d.off, "dictionary key without a value")
	}
	return k, nil
}

func (d *decodeState) dict(v reflect.Value) error {
	start := d.off
	switch v.Kind() {
	case reflect.Struct:
		return d.structValue(v)
	case reflect.Map:
		kt := v.Type().Key()
		if kt.Kind() == reflect.String || kt.Kind() == reflect.Array && kt.Elem().Kind() == reflect.Uint8 {
			return d.mapValue(v)
		}
	case reflect.Interface:
		if isEmptyInterface(v) {
			x, err := d.anyValue()
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(x))
			return nil
		}
	}
	return d.mismatch("dictionary", v.Type(), start)
}

func (d *decodeState) structValue(v reflect.Value) error {
	si := cachedStructInfo(v.Type())
	if err := d.enter(); err != nil {
		return err
	}
	var prev []byte
	for {
		more, err := d.more()
		if err != nil || !more {
			return err
		}
		k, err := d.key(prev)
		if err != nil {
			return err
		}
		prev = k
		i, ok := si.byName[string(k)]
		if !ok {
			if err := d.skip(); err != nil {
				return err
			}
			continue
		}
		if err := d.value(fieldForDecode(v, si.fields[i].index)); err != nil {
			return err
		}
	}
}

// fieldForDecode is v.FieldByIndex, allocating nil embedded pointers.
func fieldForDecode(v reflect.Value, index []int) reflect.Value {
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v
}

func (d *decodeState) mapValue(v reflect.Value) error {
	t := v.Type()
	kt, et := t.Key(), t.Elem()
	if err := d.enter(); err != nil {
		return err
	}
	if v.IsNil() {
		v.Set(reflect.MakeMap(t))
	}
	var prev []byte
	for {
		more, err := d.more()
		if err != nil || !more {
			return err
		}
		keyOff := d.off
		k, err := d.key(prev)
		if err != nil {
			return err
		}
		prev = k
		var kv reflect.Value
		if kt.Kind() == reflect.String {
			kv = reflect.ValueOf(string(k)).Convert(kt)
		} else {
			if len(k) != kt.Len() {
				if err := d.mismatch("key of length "+strconv.Itoa(len(k)), kt, keyOff); err != nil {
					return err
				}
				// mismatch skipped the key; skip its value too.
				if err := d.skip(); err != nil {
					return err
				}
				continue
			}
			kv = reflect.New(kt).Elem()
			copy(kv.Bytes(), k)
		}
		ev := reflect.New(et).Elem()
		if err := d.value(ev); err != nil {
			return err
		}
		v.SetMapIndex(kv, ev)
	}
}

// anyValue decodes the value at d.off into the interface{} form: int64,
// string, []any or map[string]any.
func (d *decodeState) anyValue() (any, error) {
	if d.off >= len(d.data) {
		return nil, d.syntax(d.off, "unexpected end of input")
	}
	switch c := d.data[d.off]; {
	case c == 'i':
		start := d.off
		end, err := scanInt(d.data, d.off)
		if err != nil {
			return nil, d.rebase(err)
		}
		digits := d.data[start+1 : end-1]
		neg, u, ok := parseDigits(digits)
		n, fits := toInt64(neg, u)
		if !ok || !fits {
			// An integer no int64 holds is left nil, recorded as a type
			// error, so that the rest of the value still decodes.
			return nil, d.mismatch("integer "+string(digits), reflect.TypeFor[int64](), start)
		}
		d.off = end
		return n, nil
	case c >= '0' && c <= '9':
		s, e, err := scanString(d.data, d.off)
		if err != nil {
			return nil, d.rebase(err)
		}
		d.off = e
		return string(d.data[s:e]), nil
	case c == 'l':
		if err := d.enter(); err != nil {
			return nil, err
		}
		list := []any{}
		for {
			more, err := d.more()
			if err != nil {
				return nil, err
			}
			if !more {
				return list, nil
			}
			x, err := d.anyValue()
			if err != nil {
				return nil, err
			}
			list = append(list, x)
		}
	case c == 'd':
		if err := d.enter(); err != nil {
			return nil, err
		}
		m := map[string]any{}
		var prev []byte
		for {
			more, err := d.more()
			if err != nil {
				return nil, err
			}
			if !more {
				return m, nil
			}
			k, err := d.key(prev)
			if err != nil {
				return nil, err
			}
			prev = k
			x, err := d.anyValue()
			if err != nil {
				return nil, err
			}
			m[string(k)] = x
		}
	default:
		return nil, d.syntax(d.off, "invalid byte "+strconv.QuoteRune(rune(c)))
	}
}
