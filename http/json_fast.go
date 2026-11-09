//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"reflect"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"
)

// The default JSONEncoder answers the shapes handlers return most - structs
// of strings, integers, booleans, slices, arrays and pointers of them - with
// an encoder compiled once per type, and leaves everything else to
// encoding/json/v2. For the types it takes, its output is byte for byte what
// json.MarshalWrite writes with no options: it is a faster path to the same
// encoding, not another encoding. encoding/json/v2 walks each value through
// reflect.Value and per-field option checks, which costs about half again
// as much as encoding/json did before it was built on v2 (Go 1.26): a
// response of 25 small structs takes 8.8 µs there and 5.6 µs in Go 1.26's
// encoder.
//
// A type is taken only when every type reachable from it is one of:
//
//   - bool, the signed and unsigned integers (not uintptr), and string,
//     including named types of those kinds;
//   - a slice or array of a taken type whose kind is not uint8, since v2
//     encodes bytes as base64;
//   - a pointer to a taken type;
//   - a struct whose serialized fields are all taken, where a field's json
//     tag, if any, is "-" or a plain name with no options, unexported fields
//     carry no tag, embedded fields without a tag are structs (not pointers)
//     that v2 inlines, and no two serialized fields share a name.
//
// and no type, or pointer to it, has a method v2 would call instead of
// encoding it by kind (MarshalJSON, MarshalJSONTo, MarshalText, AppendText),
// nor is time.Duration, which v2 encodes in a format of its own. Recursive
// types, floats, maps, interfaces and the rest go to v2 whole, as does a
// value with a string that is not valid UTF-8, so that v2 reports the error.

// jsonEncodeFunc appends the JSON for the value at p to dst. It returns false
// when the value has to go to encoding/json/v2 after all.
type jsonEncodeFunc func(dst []byte, p unsafe.Pointer) ([]byte, bool)

// jsonPlan is a type's compiled encoder, nil for a type left to v2.
type jsonPlan struct {
	encode jsonEncodeFunc
}

var jsonPlans sync.Map // reflect.Type -> *jsonPlan

// appendFastJSON appends v's JSON to dst when its type is one the compiled
// encoders take, and reports whether it did; when it did not, dst is
// returned as it was.
func appendFastJSON(dst []byte, v any) ([]byte, bool) {
	if v == nil {
		return dst, false
	}
	t := reflect.TypeOf(v)
	plan := jsonPlanFor(t)
	if plan.encode == nil {
		return dst, false
	}
	data := (*[2]unsafe.Pointer)(unsafe.Pointer(&v))[1]
	if t.Kind() == reflect.Pointer {
		// The interface holds the pointer itself; its plan encodes what the
		// pointer points to.
		if data == nil {
			return append(dst, "null"...), true
		}
	} else if jsonDirectIface(t) {
		return dst, false
	}
	out, ok := plan.encode(dst, data)
	if !ok {
		return dst, false
	}
	return out, true
}

// jsonDirectIface reports whether an interface holding a t keeps the value
// itself rather than a pointer to it: pointer-shaped types, which besides
// pointers are structs and arrays wrapping exactly one. Only the latter can
// reach it here, and they go to v2.
func jsonDirectIface(t reflect.Type) bool {
	return (t.Kind() == reflect.Struct || t.Kind() == reflect.Array) && t.Size() == unsafe.Sizeof(uintptr(0))
}

// jsonPlanFor returns t's plan, compiling it on first use. For a pointer type
// the plan encodes the element, the interface holding the pointer itself.
func jsonPlanFor(t reflect.Type) *jsonPlan {
	if plan, ok := jsonPlans.Load(t); ok {
		return plan.(*jsonPlan)
	}
	c := jsonCompiler{inProgress: map[reflect.Type]bool{}}
	var encode jsonEncodeFunc
	if t.Kind() == reflect.Pointer {
		if c.hasMethods(t) {
			encode = nil
		} else {
			encode = c.compile(t.Elem())
		}
	} else {
		encode = c.compile(t)
	}
	plan, _ := jsonPlans.LoadOrStore(t, &jsonPlan{encode: encode})
	return plan.(*jsonPlan)
}

type jsonCompiler struct {
	inProgress map[reflect.Type]bool
}

var durationType = reflect.TypeFor[time.Duration]()

// jsonMethodNames are the methods through which v2 encodes or decodes a type
// instead of by its kind. Decoding methods matter for embedded structs, which
// v2 refuses to inline when they have any.
var (
	jsonMarshalMethods = []string{"MarshalJSON", "MarshalJSONTo", "MarshalText", "AppendText"}
	jsonAllMethods     = []string{"MarshalJSON", "MarshalJSONTo", "MarshalText", "AppendText",
		"UnmarshalJSON", "UnmarshalJSONFrom", "UnmarshalText"}
)

func (c *jsonCompiler) hasMethodsIn(t reflect.Type, names []string) bool {
	for _, typ := range []reflect.Type{t, reflect.PointerTo(t)} {
		if typ.NumMethod() == 0 {
			continue
		}
		for _, name := range names {
			if _, ok := typ.MethodByName(name); ok {
				return true
			}
		}
	}
	return false
}

func (c *jsonCompiler) hasMethods(t reflect.Type) bool {
	return c.hasMethodsIn(t, jsonMarshalMethods)
}

// compile returns the encoder for a value of type t, or nil when t, or any
// type reachable from it, is left to v2.
func (c *jsonCompiler) compile(t reflect.Type) jsonEncodeFunc {
	if t == durationType || c.hasMethods(t) {
		return nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return encodeJSONBool
	case reflect.Int:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendInt(dst, int64(*(*int)(p)), 10), true
		}
	case reflect.Int8:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendInt(dst, int64(*(*int8)(p)), 10), true
		}
	case reflect.Int16:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendInt(dst, int64(*(*int16)(p)), 10), true
		}
	case reflect.Int32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendInt(dst, int64(*(*int32)(p)), 10), true
		}
	case reflect.Int64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendInt(dst, *(*int64)(p), 10), true
		}
	case reflect.Uint:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendUint(dst, uint64(*(*uint)(p)), 10), true
		}
	case reflect.Uint8:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendUint(dst, uint64(*(*uint8)(p)), 10), true
		}
	case reflect.Uint16:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendUint(dst, uint64(*(*uint16)(p)), 10), true
		}
	case reflect.Uint32:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendUint(dst, uint64(*(*uint32)(p)), 10), true
		}
	case reflect.Uint64:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return strconv.AppendUint(dst, *(*uint64)(p), 10), true
		}
	case reflect.String:
		return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
			return appendJSONString(dst, *(*string)(p))
		}
	case reflect.Pointer:
		return c.compilePointer(t)
	case reflect.Slice:
		return c.compileSlice(t)
	case reflect.Array:
		return c.compileArray(t)
	case reflect.Struct:
		return c.compileStruct(t)
	}
	return nil
}

func encodeJSONBool(dst []byte, p unsafe.Pointer) ([]byte, bool) {
	if *(*bool)(p) {
		return append(dst, "true"...), true
	}
	return append(dst, "false"...), true
}

// enter marks t as being compiled, refusing a type that reaches itself.
func (c *jsonCompiler) enter(t reflect.Type) bool {
	if c.inProgress[t] {
		return false
	}
	c.inProgress[t] = true
	return true
}

func (c *jsonCompiler) leave(t reflect.Type) { delete(c.inProgress, t) }

func (c *jsonCompiler) compilePointer(t reflect.Type) jsonEncodeFunc {
	if !c.enter(t) {
		return nil
	}
	defer c.leave(t)
	elem := c.compile(t.Elem())
	if elem == nil {
		return nil
	}
	return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
		q := *(*unsafe.Pointer)(p)
		if q == nil {
			return append(dst, "null"...), true
		}
		return elem(dst, q)
	}
}

func (c *jsonCompiler) compileSlice(t reflect.Type) jsonEncodeFunc {
	if t.Elem().Kind() == reflect.Uint8 || !c.enter(t) {
		return nil
	}
	defer c.leave(t)
	elem := c.compile(t.Elem())
	if elem == nil {
		return nil
	}
	size := t.Elem().Size()
	return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
		// A nil slice is [] too: v2 writes null for it only when asked to.
		s := (*struct {
			data unsafe.Pointer
			n    int
			c    int
		})(p)
		return appendJSONElems(dst, s.data, s.n, size, elem)
	}
}

func (c *jsonCompiler) compileArray(t reflect.Type) jsonEncodeFunc {
	if t.Elem().Kind() == reflect.Uint8 || !c.enter(t) {
		return nil
	}
	defer c.leave(t)
	elem := c.compile(t.Elem())
	if elem == nil {
		return nil
	}
	size, n := t.Elem().Size(), t.Len()
	return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
		return appendJSONElems(dst, p, n, size, elem)
	}
}

func appendJSONElems(dst []byte, data unsafe.Pointer, n int, size uintptr, elem jsonEncodeFunc) ([]byte, bool) {
	dst = append(dst, '[')
	for i := 0; i < n; i++ {
		if i > 0 {
			dst = append(dst, ',')
		}
		var ok bool
		if dst, ok = elem(dst, unsafe.Add(data, uintptr(i)*size)); !ok {
			return dst, false
		}
	}
	return append(dst, ']'), true
}

// jsonField is one serialized field of a struct, its embedded structs'
// fields inlined: the member name, quoted and preceded by the delimiter, and
// the field's offset from the start of the outermost struct.
type jsonField struct {
	key    string
	offset uintptr
	encode jsonEncodeFunc
}

func (c *jsonCompiler) compileStruct(t reflect.Type) jsonEncodeFunc {
	if !c.enter(t) {
		return nil
	}
	defer c.leave(t)
	var fields []jsonField
	names := map[string]bool{}
	if !c.collectFields(t, 0, &fields, names) {
		return nil
	}
	if len(fields) == 0 {
		return func(dst []byte, _ unsafe.Pointer) ([]byte, bool) {
			return append(dst, "{}"...), true
		}
	}
	for i := range fields {
		if i == 0 {
			fields[i].key = "{" + fields[i].key
		} else {
			fields[i].key = "," + fields[i].key
		}
	}
	return func(dst []byte, p unsafe.Pointer) ([]byte, bool) {
		var ok bool
		for i := range fields {
			f := &fields[i]
			dst = append(dst, f.key...)
			if dst, ok = f.encode(dst, unsafe.Add(p, f.offset)); !ok {
				return dst, false
			}
		}
		return append(dst, '}'), true
	}
}

// collectFields appends t's serialized fields, at base from the outermost
// struct, in v2's depth-first order, and reports whether v2 would encode t as
// the fields say: every name distinct, every option and embedding one the
// compiled encoders follow.
func (c *jsonCompiler) collectFields(t reflect.Type, base uintptr, fields *[]jsonField, names map[string]bool) bool {
	var anyTag, anyField bool
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		tag, hasTag := sf.Tag.Lookup("json")
		anyTag = anyTag || hasTag
		if tag == "-" {
			continue
		}
		if !sf.IsExported() && !sf.Anonymous {
			if hasTag {
				return false // v2 rejects a tag on an unexported field
			}
			continue
		}
		anyField = true
		name := sf.Name
		if hasTag {
			if !jsonPlainName(tag) {
				return false // options, or a name that takes quoting
			}
			if tag != "" {
				name = tag
			}
		}
		if sf.Anonymous && tag == "" {
			// Inlined by v2: only a struct, without methods v2 would refuse
			// it for, is followed here.
			if sf.Type.Kind() != reflect.Struct || c.hasMethodsIn(sf.Type, jsonAllMethods) {
				return false
			}
			if !c.enter(sf.Type) {
				return false
			}
			ok := c.collectFields(sf.Type, base+sf.Offset, fields, names)
			c.leave(sf.Type)
			if !ok {
				return false
			}
			continue
		}
		if !sf.IsExported() {
			return false // unexported embedded with a name: v2 refuses it
		}
		if names[name] {
			return false // v2 drops or picks between same-named fields
		}
		names[name] = true
		encode := c.compile(sf.Type)
		if encode == nil {
			return false
		}
		key, _ := appendJSONString(nil, name)
		*fields = append(*fields, jsonField{key: string(key) + ":", offset: base + sf.Offset, encode: encode})
	}
	if t.NumField() > 0 && !anyTag && !anyField {
		return false // v2: a struct with fields but none exported is an error
	}
	return true
}

// jsonPlainName reports whether a json tag is a bare member name, possibly
// empty, that needs no quoting and has no options.
func jsonPlainName(tag string) bool {
	for i := 0; i < len(tag); i++ {
		c := tag[i]
		if c < 0x20 || c >= utf8.RuneSelf || jsonEscapeASCII[c] || c == ',' || c == '\'' || c == '`' {
			return false
		}
	}
	return true
}

// jsonEscapeASCII marks the ASCII bytes v2 escapes in a string with no
// options: control characters, '"' and '\\'. '<', '>' and '&' are escaped
// only for HTML, which v2 leaves off by default.
var jsonEscapeASCII = func() (t [utf8.RuneSelf]bool) {
	for c := 0; c < 0x20; c++ {
		t[c] = true
	}
	t['"'], t['\\'] = true, true
	return t
}()

// appendJSONString appends s quoted as v2's jsonwire.AppendQuote quotes it
// with no options, and reports false for a string that is not valid UTF-8,
// for which v2 returns an error.
func appendJSONString(dst []byte, s string) ([]byte, bool) {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			if !jsonEscapeASCII[c] {
				continue
			}
			dst = append(dst, s[start:i-1]...)
			switch c {
			case '"', '\\':
				dst = append(dst, '\\', c)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			}
			start = i
			continue
		}
		r, n := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && n == 1 {
			return dst, false
		}
		i += n
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"'), true
}
