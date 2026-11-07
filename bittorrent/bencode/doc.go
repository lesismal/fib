// Package bencode implements bencoding, the serialization of BitTorrent
// (BEP 3): .torrent files, tracker responses, DHT messages and the
// extension messages of the peer wire protocol.
//
// Bencoding has four kinds of values: integers (i42e), byte strings
// (4:spam), lists (l...e) and dictionaries (d...e) with byte-string keys.
// Marshal and Unmarshal map them onto Go values much as encoding/json does:
//
//   - integers to and from every int and uint kind, with range checks, and
//     bool as 0 or 1 (any non-zero integer decodes as true);
//   - byte strings to and from string, []byte and byte arrays such as an
//     info hash [20]byte (whose length must then match);
//   - lists to and from slices and arrays;
//   - dictionaries to and from structs and maps whose keys are strings or
//     byte arrays. Struct fields take their key from a `bencode:"key"` tag
//     (or the field name), `bencode:"key,omitempty"` leaves out empty
//     values, and `bencode:"-"` skips the field. Keys are written sorted,
//     as the format requires; keys a struct has no field for are skipped
//     when decoding, so a message with extra keys still decodes.
//   - into an interface{}, integers become int64, strings string, lists
//     []any and dictionaries map[string]any.
//
// Bencoding has no null, so nil pointers, nil interfaces and empty
// RawMessages in a dictionary leave their key out rather than failing.
//
// RawMessage keeps a value's bytes exactly as they arrived: a torrent's
// info hash is the SHA-1 of the info dictionary as encoded in the file,
// which re-encoding would change for any file that is not canonical.
// Types implementing Marshaler and Unmarshaler encode themselves.
//
// Decoding is strict about syntax (no leading zeros, no "-0", nothing
// after the value for Unmarshal) and limited against hostile input:
// nesting deeper than Limits.MaxDepth and values longer than
// Limits.MaxSize fail rather than exhausting the stack or memory, and
// integers out of the target's range are errors. Decoder.Strict also
// requires the canonical form (dictionary keys sorted and unique); by
// default unsorted and repeated keys are accepted, the last one winning,
// because real-world torrents have them.
//
// A Decoder decodes a sequence of values from a byte slice or an
// io.Reader. Over a byte slice it decodes in place: with NoCopy, []byte
// and RawMessage values alias the input instead of copying it, and
// InputOffset says where the next value starts, which is what a ut_metadata
// message (a dictionary followed by raw data) needs. Split and Lookup find
// values without decoding them.
//
//	type Peer struct {
//		IP   string `bencode:"ip"`
//		Port int    `bencode:"port"`
//	}
//	type Response struct {
//		Interval int    `bencode:"interval"`
//		Peers    []Peer `bencode:"peers"`
//		Failure  string `bencode:"failure reason,omitempty"`
//	}
//
//	var resp Response
//	if err := bencode.Unmarshal(body, &resp); err != nil { ... }
//	b, err := bencode.Marshal(resp)
//
//	info, err := bencode.Lookup(torrent, "info") // the info dict's bytes
//	hash := sha1.Sum(info)
package bencode
