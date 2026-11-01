// Package amf encodes and decodes the Action Message Format that RTMP's
// commands and FLV's script data are written in: AMF0 (Adobe's "Action
// Message Format -- AMF 0" specification) both ways, and AMF3 ("AMF 3
// Specification", 2013) decoded, which is all an RTMP server needs of it.
//
// Values map to Go types:
//
//	Number              float64 (any Go integer or float encodes as one)
//	Boolean             bool
//	String, Long String string (encoded as a long string past 65535 bytes)
//	Object              Object, an ordered list of properties
//	Typed Object        TypedObject
//	ECMA Array          ECMAArray (a map[string]any encodes as one, sorted)
//	Strict Array        []any
//	Null                nil
//	Undefined           Undefined
//	Unsupported         Unsupported
//	Date                Date (a time.Time encodes as one)
//	XML Document        XMLDocument
//
// Objects keep the order their properties came in, since peers are known to
// care about it (connect's command object puts app first) and a map would
// lose it. Reference markers (0x07) resolve to the value they refer to, which
// is shared, not copied; a reference to an object still being decoded, which
// only a cyclic value has, resolves to nil.
//
// The AVM+ marker (0x11) switches one value to AMF3, which RTMP's AMF3
// command and data messages (types 17 and 15) use. AMF3 values decode to
// the same types: integers and doubles to float64, arrays to []any when they
// have no associative part and ECMAArray when they do, objects to Object
// or TypedObject, byte arrays to []byte, vectors to []any and dictionaries
// to Dictionary. String, object and trait references are resolved.
//
// Left out: encoding AMF3 (RTMP peers accept AMF0 replies to AMF3 commands),
// AMF3's externalizable objects, whose bytes only the class that wrote them
// can read (rejected with ErrExternalizable), and AMF0's movie clip, record
// set and recordset markers, which the specification reserves.
//
// Decoding never panics on hostile input: nesting is bounded by MaxDepth,
// counts by the bytes left, and every error is a typed one.
package amf
