// Package qpack implements QPACK (RFC 9204) for HTTP/3: field sections
// encoded with the static table and literals, and decoded likewise, and,
// when a connection opts in, the dynamic table, which the Encoder fills on
// the encoder stream and the Decoder follows. Without it, which is the
// default, both sides advertise a dynamic table capacity of zero, so a peer
// may not refer to one, and neither the encoder nor the decoder stream
// carries anything.
package qpack

import (
	"errors"

	"github.com/lesismal/fib/hpack"
)

// HeaderField is one field line, with its name lowercased.
type HeaderField struct {
	Name, Value string
}

var (
	// ErrDecompression is a field section that cannot be decoded, which
	// HTTP/3 treats as QPACK_DECOMPRESSION_FAILED.
	ErrDecompression = errors.New("qpack: decompression failed")
	// ErrTooLarge is a field section beyond the decoder's limit.
	ErrTooLarge = errors.New("qpack: field section too large")
	// ErrBlocked is a field section that refers to dynamic table entries
	// the encoder stream has not brought yet: the stream it came on waits
	// for them, and the section is decoded again once they are in.
	ErrBlocked = errors.New("qpack: field section blocked on the encoder stream")
	// ErrEncoderStream is an encoder stream instruction that cannot be
	// carried out, which HTTP/3 treats as QPACK_ENCODER_STREAM_ERROR.
	ErrEncoderStream = errors.New("qpack: encoder stream error")
	// ErrDecoderStream is a decoder stream instruction that cannot be
	// carried out, which HTTP/3 treats as QPACK_DECODER_STREAM_ERROR.
	ErrDecoderStream = errors.New("qpack: decoder stream error")
)

// AppendField appends one field line; a section starts with Prefix.
func AppendField(dst []byte, name, value string, neverIndex bool) []byte {
	if entries := staticNameIndex[name]; len(entries) > 0 {
		if !neverIndex {
			for _, i := range entries {
				if staticTable[i].Value == value {
					// Indexed field line, static table.
					return appendInt(dst, 0xc0, 6, uint64(i))
				}
			}
		}
		// Literal field line with a static name reference.
		first := byte(0x50)
		if neverIndex {
			first |= 0x20
		}
		dst = appendInt(dst, first, 4, uint64(entries[0]))
		return appendString(dst, 0, 7, value)
	}
	// Literal field line with a literal name.
	first := byte(0x20)
	if neverIndex {
		first |= 0x10
	}
	dst = appendString(dst, first, 3, name)
	return appendString(dst, 0, 7, value)
}

// Prefix is the section prefix AppendField expects to follow.
var Prefix = []byte{0, 0}

// Decode decodes a field section, calling emit for each field in order. It
// stops with ErrTooLarge once the fields' size, counted as RFC 9114 section
// 4.2.2 does, would pass maxSize, if maxSize is positive.
func Decode(block []byte, maxSize int, emit func(HeaderField) error) error {
	var d *Decoder
	return d.decode(block, maxSize, emit, nil, 0)
}

// Decoder decodes the field sections of one connection. A connection's
// requests repeat most of their fields — the authority, the paths, the
// client's own fields — in the same order, so it remembers each string of
// the last section by its place in it, and a string the same as the one in
// its place before costs neither decoding nor an allocation: a path that
// changes misses its own place and no other. A Decoder is used by one
// goroutine at a time. The zero value is ready, and a nil *Decoder decodes
// as Decode does, remembering nothing.
type Decoder struct {
	recent [recentStrings]recentString
	// place is the place in its section of the next string read.
	place int
	// dyn is the dynamic table, once EnableDynamic has allowed one.
	dyn *decoderTable
}

// recentString is a string as a field line encoded it, with a prefix of n
// bits, and what it decoded to. The same bytes read with another prefix are
// another string.
type recentString struct {
	encoded, decoded string
	n                uint8
}

const (
	// recentStrings is how many places of a section a Decoder remembers,
	// more than the literals a browser's request has: its path, its
	// authority and some ten fields the static table has no value for, as
	// HTTP/3 clients that leave QPACK's dynamic table alone send every one
	// on every request.
	recentStrings = 16
	// maxRecentString bounds the encoded length of a string worth
	// remembering, a browser's cookie and user agent among them, which
	// bounds what a connection keeps to a few kilobytes.
	maxRecentString = 256
)

// AppendFields decodes a field section as Decode does, appending its fields
// to dst.
func (d *Decoder) AppendFields(dst []HeaderField, block []byte, maxSize int) ([]HeaderField, error) {
	err := d.decode(block, maxSize, nil, &dst, 0)
	return dst, err
}

// AppendSection is AppendFields for the field section of a stream, which
// may refer to the dynamic table: one that refers to entries still to come
// fails with ErrBlocked, and decodes when given again once they have come.
// A section that referred to the table is acknowledged on the decoder
// stream; see AppendInstructions.
func (d *Decoder) AppendSection(dst []HeaderField, block []byte, maxSize int, streamID uint64) ([]HeaderField, error) {
	err := d.decode(block, maxSize, nil, &dst, streamID)
	return dst, err
}

// decode decodes a field section into emit, or onto dst when emit is nil.
func (d *Decoder) decode(block []byte, maxSize int, emit func(HeaderField) error, dst *[]HeaderField, streamID uint64) error {
	encoded, rest, err := readInt(block, 8)
	if err != nil {
		return err
	}
	var dyn *decoderTable
	var ric, base uint64
	if encoded != 0 {
		if d == nil || d.dyn == nil {
			// A reference to a dynamic table this side said it does not
			// have.
			return ErrDecompression
		}
		dyn = d.dyn
		if ric, err = dyn.requiredInsertCount(encoded); err != nil {
			return err
		}
	}
	if len(rest) == 0 {
		return ErrDecompression
	}
	sign := rest[0]&0x80 != 0
	var delta uint64
	if delta, rest, err = readInt(rest, 7); err != nil {
		return err
	}
	if dyn != nil {
		switch {
		case !sign:
			base = ric + delta
		case delta >= ric:
			return ErrDecompression
		default:
			base = ric - delta - 1
		}
		if ric > dyn.inserted {
			return dyn.block(streamID)
		}
	}
	if d != nil {
		d.place = 0
	}
	// largest is one past the largest absolute index referred to, which
	// has to be the required insert count the prefix declared.
	var largest uint64
	size := 0
	for len(rest) > 0 {
		var f HeaderField
		b := rest[0]
		switch {
		case b&0x80 != 0:
			// Indexed field line.
			var i uint64
			if i, rest, err = readInt(rest, 6); err != nil {
				return err
			}
			if b&0x40 == 0 {
				if f, err = dyn.relative(base, i, ric, &largest); err != nil {
					return err
				}
				break
			}
			if i >= uint64(len(staticTable)) {
				return ErrDecompression
			}
			f = staticTable[i]
		case b&0xc0 == 0x40:
			// Literal field line with a name reference.
			var i uint64
			if i, rest, err = readInt(rest, 4); err != nil {
				return err
			}
			if b&0x10 == 0 {
				var e HeaderField
				if e, err = dyn.relative(base, i, ric, &largest); err != nil {
					return err
				}
				f.Name = e.Name
			} else {
				if i >= uint64(len(staticTable)) {
					return ErrDecompression
				}
				f.Name = staticTable[i].Name
			}
			if f.Value, rest, err = d.readString(rest, 7, maxSize); err != nil {
				return err
			}
		case b&0xe0 == 0x20:
			// Literal field line with a literal name.
			if f.Name, rest, err = d.readString(rest, 3, maxSize); err != nil {
				return err
			}
			if f.Value, rest, err = d.readString(rest, 7, maxSize); err != nil {
				return err
			}
		case b&0xf0 == 0x10:
			// Indexed field line with a post-base index.
			var i uint64
			if i, rest, err = readInt(rest, 4); err != nil {
				return err
			}
			if f, err = dyn.postBase(base, i, ric, &largest); err != nil {
				return err
			}
		default:
			// Literal field line with a post-base name reference.
			var i uint64
			if i, rest, err = readInt(rest, 3); err != nil {
				return err
			}
			var e HeaderField
			if e, err = dyn.postBase(base, i, ric, &largest); err != nil {
				return err
			}
			f.Name = e.Name
			if f.Value, rest, err = d.readString(rest, 7, maxSize); err != nil {
				return err
			}
		}
		size += len(f.Name) + len(f.Value) + 32
		if maxSize > 0 && size > maxSize {
			// Given up on, as one decoded whole would be: the encoder
			// hears the section is done with either way.
			dyn.acknowledge(streamID, ric)
			return ErrTooLarge
		}
		if emit == nil {
			*dst = append(*dst, f)
		} else if err := emit(f); err != nil {
			return err
		}
	}
	if largest != ric {
		// A required insert count above what the section refers to
		// (RFC 9204 section 4.5.1.1).
		return ErrDecompression
	}
	dyn.acknowledge(streamID, ric)
	return nil
}

// readInt reads an integer with an n-bit prefix (RFC 7541 section 5.1).
func readInt(b []byte, n uint8) (uint64, []byte, error) {
	if len(b) == 0 {
		return 0, nil, ErrDecompression
	}
	mask := uint64(1)<<n - 1
	v := uint64(b[0]) & mask
	b = b[1:]
	if v < mask {
		return v, b, nil
	}
	var shift uint
	for i, c := range b {
		v += uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return v, b[i+1:], nil
		}
		if shift += 7; shift >= 56 {
			return 0, nil, ErrDecompression
		}
	}
	return 0, nil, ErrDecompression
}

func appendInt(dst []byte, first byte, n uint8, v uint64) []byte {
	mask := uint64(1)<<n - 1
	if v < mask {
		return append(dst, first|byte(v))
	}
	dst = append(dst, first|byte(mask))
	v -= mask
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

// readString reads a string whose length has an n-bit prefix, with the
// Huffman flag in the bit just above it, and remembers it for the string in
// the same place of the next section, if that encodes it the same way.
func (d *Decoder) readString(b []byte, n uint8, maxLen int) (string, []byte, error) {
	if d == nil || d.place >= recentStrings {
		return readString(b, n, maxLen)
	}
	r := &d.recent[d.place]
	d.place++
	// A remembered string is taken only where readString would have taken
	// it, raw length limit and all.
	if length, after, err := readInt(b, n); err == nil && length <= uint64(len(after)) && (maxLen <= 0 || length <= uint64(maxLen)) {
		if end := len(b) - len(after) + int(length); end <= maxRecentString {
			if r.n == n && r.encoded == string(b[:end]) && (maxLen <= 0 || len(r.decoded) <= maxLen) {
				return r.decoded, b[end:], nil
			}
		}
	}
	s, rest, err := readString(b, n, maxLen)
	if err != nil {
		return s, rest, err
	}
	if encoded := b[:len(b)-len(rest)]; len(encoded) <= maxRecentString {
		*r = recentString{encoded: string(encoded), decoded: s, n: n}
	}
	return s, rest, nil
}

// readString reads a string whose length has an n-bit prefix, with the
// Huffman flag in the bit just above it.
func readString(b []byte, n uint8, maxLen int) (string, []byte, error) {
	if len(b) == 0 {
		return "", nil, ErrDecompression
	}
	huffman := b[0]&(1<<n) != 0
	length, rest, err := readInt(b, n)
	if err != nil {
		return "", nil, err
	}
	if length > uint64(len(rest)) {
		return "", nil, ErrDecompression
	}
	raw := rest[:length]
	rest = rest[length:]
	if maxLen > 0 && len(raw) > maxLen {
		return "", nil, ErrTooLarge
	}
	if !huffman {
		return string(raw), rest, nil
	}
	s, err := hpack.HuffmanDecode(raw, maxLen)
	if errors.Is(err, hpack.ErrStringTooLong) {
		return "", nil, ErrTooLarge
	}
	if err != nil {
		return "", nil, ErrDecompression
	}
	return s, rest, nil
}

// appendString appends s with an n-bit length prefix after the bits of
// first, Huffman coded when that is shorter.
func appendString(dst []byte, first byte, n uint8, s string) []byte {
	if h := hpack.HuffmanLen(s); h < len(s) {
		dst = appendInt(dst, first|1<<n, n, uint64(h))
		return hpack.AppendHuffman(dst, s)
	}
	dst = appendInt(dst, first, n, uint64(len(s)))
	return append(dst, s...)
}

// staticNameIndex maps a name to the static table's entries with it, in
// order: one lookup finds both the entry a field can be indexed as, among a
// few values, and the first one, which a literal refers to for its name.
var staticNameIndex = make(map[string][]uint8, len(staticTable))

func init() {
	for i, f := range staticTable {
		staticNameIndex[f.Name] = append(staticNameIndex[f.Name], uint8(i))
	}
}

// staticTable is RFC 9204 appendix A.
var staticTable = [...]HeaderField{
	{":authority", ""},
	{":path", "/"},
	{"age", "0"},
	{"content-disposition", ""},
	{"content-length", "0"},
	{"cookie", ""},
	{"date", ""},
	{"etag", ""},
	{"if-modified-since", ""},
	{"if-none-match", ""},
	{"last-modified", ""},
	{"link", ""},
	{"location", ""},
	{"referer", ""},
	{"set-cookie", ""},
	{":method", "CONNECT"},
	{":method", "DELETE"},
	{":method", "GET"},
	{":method", "HEAD"},
	{":method", "OPTIONS"},
	{":method", "POST"},
	{":method", "PUT"},
	{":scheme", "http"},
	{":scheme", "https"},
	{":status", "103"},
	{":status", "200"},
	{":status", "304"},
	{":status", "404"},
	{":status", "503"},
	{"accept", "*/*"},
	{"accept", "application/dns-message"},
	{"accept-encoding", "gzip, deflate, br"},
	{"accept-ranges", "bytes"},
	{"access-control-allow-headers", "cache-control"},
	{"access-control-allow-headers", "content-type"},
	{"access-control-allow-origin", "*"},
	{"cache-control", "max-age=0"},
	{"cache-control", "max-age=2592000"},
	{"cache-control", "max-age=604800"},
	{"cache-control", "no-cache"},
	{"cache-control", "no-store"},
	{"cache-control", "public, max-age=31536000"},
	{"content-encoding", "br"},
	{"content-encoding", "gzip"},
	{"content-type", "application/dns-message"},
	{"content-type", "application/javascript"},
	{"content-type", "application/json"},
	{"content-type", "application/x-www-form-urlencoded"},
	{"content-type", "image/gif"},
	{"content-type", "image/jpeg"},
	{"content-type", "image/png"},
	{"content-type", "text/css"},
	{"content-type", "text/html; charset=utf-8"},
	{"content-type", "text/plain"},
	{"content-type", "text/plain;charset=utf-8"},
	{"range", "bytes=0-"},
	{"strict-transport-security", "max-age=31536000"},
	{"strict-transport-security", "max-age=31536000; includesubdomains"},
	{"strict-transport-security", "max-age=31536000; includesubdomains; preload"},
	{"vary", "accept-encoding"},
	{"vary", "origin"},
	{"x-content-type-options", "nosniff"},
	{"x-xss-protection", "1; mode=block"},
	{":status", "100"},
	{":status", "204"},
	{":status", "206"},
	{":status", "302"},
	{":status", "400"},
	{":status", "403"},
	{":status", "421"},
	{":status", "425"},
	{":status", "500"},
	{"accept-language", ""},
	{"access-control-allow-credentials", "FALSE"},
	{"access-control-allow-credentials", "TRUE"},
	{"access-control-allow-headers", "*"},
	{"access-control-allow-methods", "get"},
	{"access-control-allow-methods", "get, post, options"},
	{"access-control-allow-methods", "options"},
	{"access-control-expose-headers", "content-length"},
	{"access-control-request-headers", "content-type"},
	{"access-control-request-method", "get"},
	{"access-control-request-method", "post"},
	{"alt-svc", "clear"},
	{"authorization", ""},
	{"content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"},
	{"early-data", "1"},
	{"expect-ct", ""},
	{"forwarded", ""},
	{"if-range", ""},
	{"origin", ""},
	{"purpose", "prefetch"},
	{"server", ""},
	{"timing-allow-origin", "*"},
	{"upgrade-insecure-requests", "1"},
	{"user-agent", ""},
	{"x-forwarded-for", ""},
	{"x-frame-options", "deny"},
	{"x-frame-options", "sameorigin"},
}
