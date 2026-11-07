package utp

import (
	"encoding/binary"
	"time"
)

// Packet types, the high nibble of a packet's first byte.
const (
	stData  = 0 // data, which takes a sequence number
	stFin   = 1 // the end of the sender's data, which takes one too
	stState = 2 // an acknowledgement, which takes none
	stReset = 3 // the connection is gone
	stSyn   = 4 // a connection's start, which takes a sequence number
)

const (
	// version is the protocol version, the low nibble of the first byte.
	version = 1
	// HeaderSize is the size of a packet's header without extensions.
	HeaderSize = 20
	// extSACK is the selective acknowledgement extension.
	extSACK = 1
	// sackBytes is the size of the selective acknowledgement mask sent: 32
	// packets past the first one missing, as libutp sends.
	sackBytes = 4
)

// IsUTP reports whether b looks like a uTP packet: at least a header long,
// version 1, and of a known type. It is the test a UDP socket shared with
// the DHT (whose bencoded messages begin with 'd', version 4 as uTP reads
// it) and with UDP trackers (whose messages begin with a zero byte) sorts
// what it reads by.
func IsUTP(b []byte) bool {
	return len(b) >= HeaderSize && b[0]&0x0f == version && b[0]>>4 <= stSyn
}

// header is a packet's fixed header, big-endian on the wire:
//
//	0       4       8               16              24              32
//	+-------+-------+---------------+---------------+---------------+
//	| type  | ver   | extension     | connection_id                 |
//	+-------+-------+---------------+---------------+---------------+
//	| timestamp_microseconds                                        |
//	+---------------+---------------+---------------+---------------+
//	| timestamp_difference_microseconds                             |
//	+---------------+---------------+---------------+---------------+
//	| wnd_size                                                      |
//	+---------------+---------------+---------------+---------------+
//	| seq_nr                        | ack_nr                        |
//	+---------------+---------------+---------------+---------------+
type header struct {
	typ    uint8
	ext    uint8
	connID uint16
	ts     uint32
	tsDiff uint32
	wnd    uint32
	seq    uint16
	ack    uint16
}

// parse reads a packet: its header, the mask of its selective
// acknowledgement extension (nil without one), and its payload. Extensions
// it does not know are skipped, as libutp skips them; a packet whose
// extensions run past its end is not uTP.
func parse(b []byte) (h header, sack, payload []byte, ok bool) {
	if !IsUTP(b) {
		return h, nil, nil, false
	}
	h = header{
		typ:    b[0] >> 4,
		ext:    b[1],
		connID: binary.BigEndian.Uint16(b[2:]),
		ts:     binary.BigEndian.Uint32(b[4:]),
		tsDiff: binary.BigEndian.Uint32(b[8:]),
		wnd:    binary.BigEndian.Uint32(b[12:]),
		seq:    binary.BigEndian.Uint16(b[16:]),
		ack:    binary.BigEndian.Uint16(b[18:]),
	}
	p := b[HeaderSize:]
	for ext := h.ext; ext != 0; {
		if len(p) < 2 || len(p)-2 < int(p[1]) {
			return h, nil, nil, false
		}
		next, n := p[0], int(p[1])
		if ext == extSACK && sack == nil {
			sack = p[2 : 2+n : 2+n]
		}
		ext, p = next, p[2+n:]
	}
	return h, sack, p, true
}

// putHeader writes h into b, which is at least HeaderSize long.
func putHeader(b []byte, h header) {
	b[0] = h.typ<<4 | version
	b[1] = h.ext
	binary.BigEndian.PutUint16(b[2:], h.connID)
	binary.BigEndian.PutUint32(b[4:], h.ts)
	binary.BigEndian.PutUint32(b[8:], h.tsDiff)
	binary.BigEndian.PutUint32(b[12:], h.wnd)
	binary.BigEndian.PutUint16(b[16:], h.seq)
	binary.BigEndian.PutUint16(b[18:], h.ack)
}

// seqLess reports whether sequence number a comes before b, the two being
// within half the 16-bit space of each other.
func seqLess(a, b uint16) bool { return int16(a-b) < 0 }

// tsLess is seqLess for 32-bit timestamps and delays, which wrap too.
func tsLess(a, b uint32) bool { return int32(a-b) < 0 }

// epoch is when the clock micros reads starts, so that a timestamp is
// never far from zero and the monotonic clock is the one read.
var epoch = time.Now()

// micros is the time in microseconds since epoch, the clock timestamps,
// RTTs and every deadline of a connection are kept in.
func micros() int64 { return int64(time.Since(epoch) / time.Microsecond) }
