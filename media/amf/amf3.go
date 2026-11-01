package amf

import (
	"encoding/binary"
	"math"
	"strconv"
)

// AMF3 type markers.
const (
	marker3Undefined    = 0x00
	marker3Null         = 0x01
	marker3False        = 0x02
	marker3True         = 0x03
	marker3Integer      = 0x04
	marker3Double       = 0x05
	marker3String       = 0x06
	marker3XMLDoc       = 0x07
	marker3Date         = 0x08
	marker3Array        = 0x09
	marker3Object       = 0x0a
	marker3XML          = 0x0b
	marker3ByteArray    = 0x0c
	marker3VectorInt    = 0x0d
	marker3VectorUint   = 0x0e
	marker3VectorDouble = 0x0f
	marker3VectorObject = 0x10
	marker3Dictionary   = 0x11
)

// Decode3 decodes every AMF3 value in b, with one set of reference tables,
// as an AMF3 stream that does not start in AMF0 is written.
func Decode3(b []byte) ([]any, error) {
	d := decoder3{b: b}
	var values []any
	for d.off < len(d.b) {
		v, err := d.value(0)
		if err != nil {
			return values, err
		}
		values = append(values, v)
	}
	return values, nil
}

// traits is the description an AMF3 object's class shares with the others
// of its class, which later objects refer to by index.
type traits struct {
	class   string
	dynamic bool
	sealed  []string
}

// decoder3 decodes AMF3, keeping the three reference tables the format has.
type decoder3 struct {
	b       []byte
	off     int
	strings []string
	objects []any
	traits  []*traits
}

func (d *decoder3) reset() {
	d.b, d.off = nil, 0
	d.strings = d.strings[:0]
	clear(d.objects)
	d.objects = d.objects[:0]
	d.traits = d.traits[:0]
}

func (d *decoder3) need(n int) ([]byte, error) {
	if n < 0 || len(d.b)-d.off < n {
		return nil, ErrShort
	}
	b := d.b[d.off : d.off+n]
	d.off += n
	return b, nil
}

// u29 reads AMF3's variable-length 29-bit unsigned integer.
func (d *decoder3) u29() (uint32, error) {
	var v uint32
	for i := range 4 {
		if d.off >= len(d.b) {
			return 0, ErrShort
		}
		c := d.b[d.off]
		d.off++
		if i == 3 {
			return v<<8 | uint32(c), nil
		}
		v = v<<7 | uint32(c&0x7f)
		if c&0x80 == 0 {
			break
		}
	}
	return v, nil
}

// header reads the U29 that starts a value which may be a reference: it
// returns the reference index when the low bit is clear, and the count or
// length that follows otherwise.
func (d *decoder3) header() (n uint32, inline bool, err error) {
	u, err := d.u29()
	if err != nil {
		return 0, false, err
	}
	return u >> 1, u&1 == 1, nil
}

// bounded checks that n elements of at least size bytes each fit in what is
// left, so that a count cannot make the decoder allocate more than the
// message could fill.
func (d *decoder3) bounded(n uint32, size int) error {
	if int64(n)*int64(size) > int64(len(d.b)-d.off) {
		return ErrShort
	}
	return nil
}

func (d *decoder3) str() (string, error) {
	n, inline, err := d.header()
	if err != nil {
		return "", err
	}
	if !inline {
		if int(n) >= len(d.strings) {
			return "", ErrReference
		}
		return d.strings[n], nil
	}
	b, err := d.need(int(n))
	if err != nil {
		return "", err
	}
	s := string(b)
	if s != "" {
		d.strings = append(d.strings, s)
	}
	return s, nil
}

func (d *decoder3) objectRef(n uint32) (any, error) {
	if int(n) >= len(d.objects) {
		return nil, ErrReference
	}
	return d.objects[n], nil
}

func (d *decoder3) reserve() int {
	d.objects = append(d.objects, nil)
	return len(d.objects) - 1
}

func (d *decoder3) value(depth int) (any, error) {
	if depth > MaxDepth {
		return nil, ErrDepth
	}
	m, err := d.need(1)
	if err != nil {
		return nil, err
	}
	switch m[0] {
	case marker3Undefined:
		return Undefined{}, nil
	case marker3Null:
		return nil, nil
	case marker3False:
		return false, nil
	case marker3True:
		return true, nil
	case marker3Integer:
		u, err := d.u29()
		// Sign-extend the 29 bits.
		return float64(int32(u<<3) >> 3), err
	case marker3Double:
		b, err := d.need(8)
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
	case marker3String:
		return d.str()
	case marker3XMLDoc, marker3XML, marker3ByteArray:
		n, inline, err := d.header()
		if err != nil {
			return nil, err
		}
		if !inline {
			return d.objectRef(n)
		}
		b, err := d.need(int(n))
		if err != nil {
			return nil, err
		}
		var v any = XMLDocument(b)
		if m[0] == marker3ByteArray {
			v = append([]byte(nil), b...)
		}
		d.objects = append(d.objects, v)
		return v, nil
	case marker3Date:
		n, inline, err := d.header()
		if err != nil {
			return nil, err
		}
		if !inline {
			return d.objectRef(n)
		}
		b, err := d.need(8)
		if err != nil {
			return nil, err
		}
		v := Date{Millis: math.Float64frombits(binary.BigEndian.Uint64(b))}
		d.objects = append(d.objects, v)
		return v, nil
	case marker3Array:
		return d.array(depth)
	case marker3Object:
		return d.object(depth)
	case marker3VectorInt, marker3VectorUint, marker3VectorDouble, marker3VectorObject:
		return d.vector(m[0], depth)
	case marker3Dictionary:
		n, inline, err := d.header()
		if err != nil {
			return nil, err
		}
		if !inline {
			return d.objectRef(n)
		}
		if err := d.bounded(n, 2); err != nil {
			return nil, err
		}
		if _, err := d.need(1); err != nil { // weak keys
			return nil, err
		}
		r := d.reserve()
		dict := make(Dictionary, 0, n)
		for range n {
			k, err := d.value(depth + 1)
			if err != nil {
				return nil, err
			}
			v, err := d.value(depth + 1)
			if err != nil {
				return nil, err
			}
			dict = append(dict, DictionaryEntry{k, v})
		}
		d.objects[r] = dict
		return dict, nil
	}
	return nil, ErrMarker
}

func (d *decoder3) array(depth int) (any, error) {
	n, inline, err := d.header()
	if err != nil {
		return nil, err
	}
	if !inline {
		return d.objectRef(n)
	}
	r := d.reserve()
	var assoc ECMAArray
	for {
		key, err := d.str()
		if err != nil {
			return nil, err
		}
		if key == "" {
			break
		}
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		assoc = append(assoc, Property{key, v})
	}
	if err := d.bounded(n, 1); err != nil {
		return nil, err
	}
	dense := make([]any, 0, n)
	for range n {
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		dense = append(dense, v)
	}
	if assoc == nil {
		d.objects[r] = dense
		return dense, nil
	}
	for i, v := range dense {
		assoc = append(assoc, Property{strconv.Itoa(i), v})
	}
	d.objects[r] = assoc
	return assoc, nil
}

func (d *decoder3) object(depth int) (any, error) {
	u, err := d.u29()
	if err != nil {
		return nil, err
	}
	if u&1 == 0 {
		return d.objectRef(u >> 1)
	}
	var t *traits
	if u&2 == 0 {
		i := int(u >> 2)
		if i >= len(d.traits) {
			return nil, ErrReference
		}
		t = d.traits[i]
	} else {
		if u&4 != 0 {
			return nil, ErrExternalizable
		}
		t = &traits{dynamic: u&8 != 0}
		if t.class, err = d.str(); err != nil {
			return nil, err
		}
		n := u >> 4
		if err := d.bounded(n, 1); err != nil {
			return nil, err
		}
		t.sealed = make([]string, 0, n)
		for range n {
			s, err := d.str()
			if err != nil {
				return nil, err
			}
			t.sealed = append(t.sealed, s)
		}
		d.traits = append(d.traits, t)
	}
	r := d.reserve()
	obj := make(Object, 0, len(t.sealed))
	for _, key := range t.sealed {
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		obj = append(obj, Property{key, v})
	}
	for t.dynamic {
		key, err := d.str()
		if err != nil {
			return nil, err
		}
		if key == "" {
			break
		}
		v, err := d.value(depth + 1)
		if err != nil {
			return nil, err
		}
		obj = append(obj, Property{key, v})
	}
	var v any = obj
	if t.class != "" {
		v = TypedObject{Class: t.class, Object: obj}
	}
	d.objects[r] = v
	return v, nil
}

func (d *decoder3) vector(marker byte, depth int) (any, error) {
	n, inline, err := d.header()
	if err != nil {
		return nil, err
	}
	if !inline {
		return d.objectRef(n)
	}
	if _, err := d.need(1); err != nil { // fixed-length flag
		return nil, err
	}
	size := 1
	switch marker {
	case marker3VectorInt, marker3VectorUint:
		size = 4
	case marker3VectorDouble:
		size = 8
	}
	if err := d.bounded(n, size); err != nil {
		return nil, err
	}
	if marker == marker3VectorObject {
		if _, err := d.str(); err != nil { // the element type's name
			return nil, err
		}
	}
	r := d.reserve()
	vec := make([]any, 0, n)
	for range n {
		var v any
		switch marker {
		case marker3VectorInt:
			b, _ := d.need(4)
			v = float64(int32(binary.BigEndian.Uint32(b)))
		case marker3VectorUint:
			b, _ := d.need(4)
			v = float64(binary.BigEndian.Uint32(b))
		case marker3VectorDouble:
			b, _ := d.need(8)
			v = math.Float64frombits(binary.BigEndian.Uint64(b))
		default:
			if v, err = d.value(depth + 1); err != nil {
				return nil, err
			}
		}
		vec = append(vec, v)
	}
	d.objects[r] = vec
	return vec, nil
}
