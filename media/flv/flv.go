package flv

import (
	"encoding/binary"
	"errors"
	"time"
)

// Tag types.
const (
	TagAudio  = 8
	TagVideo  = 9
	TagScript = 18
)

// Sizes of the fixed parts of an FLV file.
const (
	HeaderSize    = 9
	TagHeaderSize = 11
	// MaxTagDataSize is the largest body the 24-bit size field can carry.
	MaxTagDataSize = 1<<24 - 1
)

// DefaultMaxTagSize bounds the body of one tag Demuxer holds when its
// config leaves the bound 0.
const DefaultMaxTagSize = 8 << 20

// Errors the package reports.
var (
	ErrShort       = errors.New("flv: tag body cut short")
	ErrHeader      = errors.New("flv: not an FLV header")
	ErrTagTooLarge = errors.New("flv: tag larger than the limit")
	ErrTagType     = errors.New("flv: unknown tag type")
	ErrMultitrack  = errors.New("flv: multitrack tags are not supported")
	ErrCodec       = errors.New("flv: codec not carried in FLV")
	ErrConfig      = errors.New("flv: track has no usable codec configuration")
)

// Tag is one FLV tag: its type, its timestamp in milliseconds, and its
// body.
type Tag struct {
	Type      uint8
	Timestamp uint32
	// StreamID is always 0 in a file; RTMP uses the field for its message
	// stream.
	StreamID uint32
	Data     []byte
}

// Header is the FLV file header's flags.
type Header struct {
	HasAudio, HasVideo bool
}

// AppendHeader appends the FLV file header and the zero previous-tag-size
// that follows it.
func AppendHeader(dst []byte, h Header) []byte {
	var flags byte
	if h.HasAudio {
		flags |= 4
	}
	if h.HasVideo {
		flags |= 1
	}
	return append(dst, 'F', 'L', 'V', 1, flags, 0, 0, 0, HeaderSize, 0, 0, 0, 0)
}

// AppendTag appends a tag with the given body, and its previous-tag-size
// trailer.
func AppendTag(dst []byte, typ uint8, timestamp uint32, data []byte) []byte {
	dst, start := beginTag(dst, typ, timestamp)
	dst = append(dst, data...)
	return endTag(dst, start)
}

// beginTag appends a tag header whose size endTag fills in once the body
// has been appended after it, so that a body is built in place.
func beginTag(dst []byte, typ uint8, timestamp uint32) ([]byte, int) {
	start := len(dst)
	dst = append(dst, typ, 0, 0, 0,
		byte(timestamp>>16), byte(timestamp>>8), byte(timestamp), byte(timestamp>>24),
		0, 0, 0)
	return dst, start
}

func endTag(dst []byte, start int) []byte {
	n := len(dst) - start - TagHeaderSize
	dst[start+1], dst[start+2], dst[start+3] = byte(n>>16), byte(n>>8), byte(n)
	return binary.BigEndian.AppendUint32(dst, uint32(n+TagHeaderSize))
}

// Millis converts d to the 32-bit millisecond timestamps FLV and RTMP use,
// rounded, wrapping as they do.
func Millis(d time.Duration) uint32 {
	return uint32((d + time.Millisecond/2).Milliseconds())
}

// DemuxerConfig configures a Demuxer.
type DemuxerConfig struct {
	// MaxTagSize bounds the body of one tag: a larger one fails the stream
	// with ErrTagTooLarge, since holding it would let a peer make the
	// demuxer allocate up to 16 MiB. 0 means DefaultMaxTagSize.
	MaxTagSize int
	// NoHeader starts at the first tag, for a stream whose file header has
	// been read elsewhere.
	NoHeader bool
}

// Demuxer splits an FLV byte stream into tags. It is fed the stream in
// pieces of any size and holds what it needs to complete one tag, never
// more than one tag's worth.
type Demuxer struct {
	maxTag int
	header Header
	// state: 0 file header, 1 skipping the rest of the header and the
	// first previous-tag-size, 2 tags.
	state int
	skip  int
	buf   []byte
}

// NewDemuxer returns a Demuxer.
func NewDemuxer(config DemuxerConfig) *Demuxer {
	d := &Demuxer{maxTag: config.MaxTagSize}
	if d.maxTag <= 0 {
		d.maxTag = DefaultMaxTagSize
	}
	if config.NoHeader {
		d.state = 2
	}
	return d
}

// Header returns the file header's flags, once it has been read.
func (d *Demuxer) Header() (Header, bool) { return d.header, d.state > 0 }

// Feed reads b, calling fn for every tag it completes. The tag's data is
// valid only during the call. An error from fn stops Feed and is returned;
// a malformed stream fails, and the Demuxer is no good after either.
func (d *Demuxer) Feed(b []byte, fn func(Tag) error) error {
	for len(b) > 0 {
		switch d.state {
		case 0:
			need := HeaderSize - len(d.buf)
			if len(b) < need {
				d.buf = append(d.buf, b...)
				return nil
			}
			h := append(d.buf, b[:need]...)
			b = b[need:]
			if h[0] != 'F' || h[1] != 'L' || h[2] != 'V' {
				return ErrHeader
			}
			off := binary.BigEndian.Uint32(h[5:])
			if off < HeaderSize || off > 1<<16 {
				return ErrHeader
			}
			d.header = Header{HasAudio: h[4]&4 != 0, HasVideo: h[4]&1 != 0}
			d.skip = int(off) - HeaderSize + 4
			d.buf = d.buf[:0]
			d.state = 1
		case 1:
			n := min(d.skip, len(b))
			d.skip -= n
			b = b[n:]
			if d.skip == 0 {
				d.state = 2
			}
		default:
			var err error
			if b, err = d.tags(b, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// tags reads the tags of b, each with its trailer, holding a partial one.
func (d *Demuxer) tags(b []byte, fn func(Tag) error) ([]byte, error) {
	if len(d.buf) > 0 {
		// Complete the header first, to learn the tag's size.
		if len(d.buf) < TagHeaderSize {
			n := min(TagHeaderSize-len(d.buf), len(b))
			d.buf = append(d.buf, b[:n]...)
			b = b[n:]
			if len(d.buf) < TagHeaderSize {
				return b, nil
			}
		}
		size, err := d.tagSize(d.buf)
		if err != nil {
			return nil, err
		}
		n := min(size-len(d.buf), len(b))
		d.buf = append(d.buf, b[:n]...)
		b = b[n:]
		if len(d.buf) < size {
			return b, nil
		}
		err = fn(parseTag(d.buf))
		d.buf = d.buf[:0]
		if err != nil {
			return nil, err
		}
	}
	for len(b) >= TagHeaderSize {
		size, err := d.tagSize(b)
		if err != nil {
			return nil, err
		}
		if len(b) < size {
			break
		}
		if err := fn(parseTag(b[:size])); err != nil {
			return nil, err
		}
		b = b[size:]
	}
	d.buf = append(d.buf, b...)
	return nil, nil
}

// tagSize checks a tag header and returns the bytes of the tag with its
// trailer.
func (d *Demuxer) tagSize(h []byte) (int, error) {
	switch h[0] & 0x1f {
	case TagAudio, TagVideo, TagScript:
	default:
		return 0, ErrTagType
	}
	if h[0]&0x20 != 0 {
		// The filter bit marks an encrypted tag, which nothing here reads.
		return 0, ErrTagType
	}
	n := int(h[1])<<16 | int(h[2])<<8 | int(h[3])
	if n > d.maxTag {
		return 0, ErrTagTooLarge
	}
	return TagHeaderSize + n + 4, nil
}

func parseTag(b []byte) Tag {
	return Tag{
		Type:      b[0] & 0x1f,
		Timestamp: uint32(b[7])<<24 | uint32(b[4])<<16 | uint32(b[5])<<8 | uint32(b[6]),
		StreamID:  uint32(b[8])<<16 | uint32(b[9])<<8 | uint32(b[10]),
		Data:      b[TagHeaderSize : len(b)-4],
	}
}
