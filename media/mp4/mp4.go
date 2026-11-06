package mp4

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/fmp4"
)

var (
	// ErrNoMoov is returned for a file with no moov box among its top-level
	// boxes, or one cut short before it.
	ErrNoMoov = errors.New("mp4: no moov")
	// ErrMalformed is returned for a box that does not parse, or tables that
	// do not agree with each other.
	ErrMalformed = errors.New("mp4: malformed box")
	// ErrFragmented is returned for a file whose moov has an mvex: its
	// samples are in movie fragments, which media/fmp4 reads.
	ErrFragmented = errors.New("mp4: fragmented file")
	// ErrTooLarge is returned for a file past one of the reader's limits:
	// MaxTracks, MaxBoxes, MaxSampleEntries, MaxEdits or MaxSamples.
	ErrTooLarge = errors.New("mp4: past the reader's limits")
	// ErrSampleData is returned by ReadTable for a chunk whose samples lie
	// outside the file.
	ErrSampleData = errors.New("mp4: sample data outside the file")
)

// The reader's limits. Open reads no more than MaxBoxes box headers and
// MaxSampleEntries bytes of each track's stsd, so the work and memory it
// takes are bounded however the file is made.
const (
	MaxTracks        = 256
	MaxBoxes         = 1 << 16
	MaxSampleEntries = 1 << 20
	MaxEdits         = 1024
	// MaxSamples is the most samples a track may have: sample indexes are
	// kept in 32 bits.
	MaxSamples = math.MaxInt32
)

// File is a parsed MP4 file: its movie header and tracks. It holds no
// reference to the reader it was opened from.
type File struct {
	// Size is the file's size, as given to Open.
	Size int64
	// MajorBrand is the ftyp's major brand, as "isom"; empty without ftyp.
	MajorBrand string
	// Timescale is the movie's clock, in which Edit durations are counted.
	Timescale uint32
	// Duration is the movie's duration from mvhd, 0 if unknown.
	Duration time.Duration
	// MoovOffset and MoovSize locate the moov box. A moov that comes
	// before the media data (written with ffmpeg's -movflags +faststart)
	// lets a player start before it has the end of the file.
	MoovOffset, MoovSize int64
	// Tracks are in the order of the trak boxes.
	Tracks []*Track
}

// Edit is one entry of a track's edit list (ISO/IEC 14496-12 section
// 8.6.6).
type Edit struct {
	// Duration is in the movie's timescale.
	Duration int64
	// MediaTime is where the edit starts in the media, in the track's
	// timescale; -1 for an empty edit, which delays the media after it.
	MediaTime int64
	// Rate is the media rate as 16.16 fixed point: 1<<16 normally, 0 for a
	// dwell.
	Rate int32
}

// Track is one track of a File: what it carries, its clock and edits, and
// where its sample table lies in the file, which ReadTable reads.
type Track struct {
	// ID is the track_ID of tkhd.
	ID uint32
	// Handler is the hdlr's handler type: "vide", "soun", "subt", "text",
	// "sbtl" and so on.
	Handler string
	// Language is the ISO 639-2/T code of mdhd, "und" when unset.
	Language string
	// Timescale is the media's clock: every time of its samples is in
	// ticks of it.
	Timescale uint32
	// Duration is the media's duration from mdhd, 0 if unknown.
	Duration time.Duration
	// Track describes the codec from the first sample entry, as
	// fmp4.ParseSampleEntry reads it: av.CodecUnknown for a format fib does
	// not carry. Its Config is a copy, not a part of the file.
	Track *av.Track
	// Edits is the edit list, if any.
	Edits []Edit
	// Shift is what Edits add to a sample's media time to give its
	// presentation time, in ticks of Timescale: the empty edits before the
	// first one that plays, less where that one starts in the media. With
	// B-frames ffmpeg starts the edit at the first frame's composition
	// offset, so that the first frame shown is at 0; with AAC at the
	// encoder's priming samples. Later edits are not applied: they are in
	// Edits for a caller that needs them.
	Shift int64
	// Samples and Chunks are the counts of samples and chunks.
	Samples, Chunks int

	fileSize int64
	sizes    sizeBox
	chunks   tableBox // stco (entry 4) or co64 (entry 8)
	stsc     tableBox
	stts     tableBox
	ctts     tableBox
	stss     tableBox
}

// tableBox is where a table's entries start in the file and how many there
// are; n < 0 when the box is absent.
type tableBox struct {
	off   int64
	n     int
	entry int // bytes an entry takes
}

// sizeBox is a track's stsz or stz2: a size for every sample (field bits
// each), or one for them all (constant).
type sizeBox struct {
	off      int64
	constant uint32
	field    int // 0 with a constant, else 4, 8, 16 or 32 bits a sample
}

// Open reads the boxes of an MP4 file (ISO/IEC 14496-12, the ISO base media
// file format) of size bytes through r, and returns its tracks. It reads box
// headers to find the moov, wherever it is in the file, and then the boxes
// of the moov but for the sample tables, of which it reads the entry counts
// alone; ReadTable reads a track's table later. Open never reads media
// data, and holds nothing of the file but the codec configurations once it
// returns.
//
// A file whose moov has an mvex is fragmented: Open returns ErrFragmented,
// for media/fmp4 to read it. An error from r is returned as it is.
func Open(r io.ReaderAt, size int64) (*File, error) {
	o := &opener{r: r}
	f := &File{Size: size, MoovOffset: -1}
	for off := int64(0); off < size; {
		h, err := o.header(off, size)
		if err != nil {
			if err == ErrMalformed {
				// A file cut short ends in a box running past its end: the
				// moov is lost with it if it was not found before.
				return nil, ErrNoMoov
			}
			return nil, err
		}
		switch h.typ {
		case "ftyp":
			p, err := o.payload(h, 4)
			if err != nil {
				return nil, err
			}
			if len(p) == 4 {
				f.MajorBrand = string(p)
			}
		case "moov":
			f.MoovOffset, f.MoovSize = h.off, h.end-h.off
			if err := o.moov(f, h); err != nil {
				return nil, err
			}
			return f, nil
		case "moof":
			return nil, ErrFragmented
		}
		off = h.end
	}
	return nil, ErrNoMoov
}

// opener reads box headers and small boxes through r, counting the headers.
type opener struct {
	r     io.ReaderAt
	boxes int
	buf   [16]byte
}

// boxHeader is a box found in the file: its type, where its payload starts
// and where the box ends.
type boxHeader struct {
	typ           string
	off, at, end  int64 // the box's start, its payload's, and its end
	payloadLength int64
}

// header reads the header of the box at off, which must end by end.
func (o *opener) header(off, end int64) (boxHeader, error) {
	var h boxHeader
	if o.boxes++; o.boxes > MaxBoxes {
		return h, ErrTooLarge
	}
	n := int64(16)
	if end-off < n {
		n = end - off
	}
	if n < 8 {
		return h, ErrMalformed
	}
	if err := o.readAt(o.buf[:n], off); err != nil {
		return h, err
	}
	size := int64(binary.BigEndian.Uint32(o.buf[:]))
	h.typ, h.off, h.at = string(o.buf[4:8]), off, off+8
	switch size {
	case 0:
		size = end - off
	case 1:
		if n < 16 {
			return h, ErrMalformed
		}
		large := binary.BigEndian.Uint64(o.buf[8:])
		if large > math.MaxInt64 {
			return h, ErrMalformed
		}
		size, h.at = int64(large), off+16
	}
	if size < h.at-off || size > end-off {
		return h, ErrMalformed
	}
	h.end = off + size
	h.payloadLength = h.end - h.at
	return h, nil
}

// readAt fills b from off, as io.ReadFull does.
func (o *opener) readAt(b []byte, off int64) error {
	n, err := o.r.ReadAt(b, off)
	if n == len(b) {
		return nil
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

// payload reads the first max bytes of h's payload, or all of it if
// shorter.
func (o *opener) payload(h boxHeader, max int64) ([]byte, error) {
	n := min(h.payloadLength, max)
	b := make([]byte, n)
	if err := o.readAt(b, h.at); err != nil {
		return nil, err
	}
	return b, nil
}

// children calls fn for each box in h's payload, from skip bytes into it.
func (o *opener) children(h boxHeader, skip int64, fn func(boxHeader) error) error {
	for off := h.at + skip; off < h.end; {
		c, err := o.header(off, h.end)
		if err != nil {
			return err
		}
		if err := fn(c); err != nil {
			return err
		}
		off = c.end
	}
	return nil
}

// fullHeader splits a full box's payload into its version and the rest.
func fullHeader(p []byte) (uint8, []byte, bool) {
	if len(p) < 4 {
		return 0, nil, false
	}
	return p[0], p[4:], true
}

func (o *opener) moov(f *File, moov boxHeader) error {
	err := o.children(moov, 0, func(h boxHeader) error {
		switch h.typ {
		case "mvhd":
			p, err := o.payload(h, 32)
			if err != nil {
				return err
			}
			v, p, ok := fullHeader(p)
			ts, dur, ok := times(v, p, ok)
			if !ok || ts == 0 {
				return ErrMalformed
			}
			f.Timescale = ts
			f.Duration = av.FromTicks(dur, int(ts))
		case "trak":
			if len(f.Tracks) == MaxTracks {
				return ErrTooLarge
			}
			t := &Track{fileSize: f.Size}
			if err := o.trak(t, h); err != nil {
				return err
			}
			f.Tracks = append(f.Tracks, t)
		case "mvex":
			return ErrFragmented
		}
		return nil
	})
	if err != nil {
		return err
	}
	if f.Timescale == 0 {
		return ErrMalformed
	}
	for _, t := range f.Tracks {
		t.Shift = shift(t.Edits, f.Timescale, t.Timescale)
	}
	return nil
}

// times reads the timescale and duration of an mvhd or mdhd, after their
// version 0 or 1 creation and modification times. An unknown duration (all
// ones) is 0.
func times(v uint8, p []byte, ok bool) (timescale uint32, duration int64, _ bool) {
	if !ok {
		return 0, 0, false
	}
	if v == 1 {
		if len(p) < 28 {
			return 0, 0, false
		}
		d := binary.BigEndian.Uint64(p[20:])
		if d > math.MaxInt64 {
			d = 0
		}
		return binary.BigEndian.Uint32(p[16:]), int64(d), true
	}
	if len(p) < 16 {
		return 0, 0, false
	}
	d := binary.BigEndian.Uint32(p[12:])
	if d == math.MaxUint32 {
		d = 0
	}
	return binary.BigEndian.Uint32(p[8:]), int64(d), true
}

func (o *opener) trak(t *Track, trak boxHeader) error {
	t.sizes.field = -1
	for _, b := range []*tableBox{&t.chunks, &t.stsc, &t.stts, &t.ctts, &t.stss} {
		b.n = -1
	}
	var haveHeader, haveMedia bool
	err := o.children(trak, 0, func(h boxHeader) error {
		switch h.typ {
		case "tkhd":
			p, err := o.payload(h, 24)
			if err != nil {
				return err
			}
			v, p, ok := fullHeader(p)
			at := 8
			if v == 1 {
				at = 16
			}
			if !ok || len(p) < at+4 {
				return ErrMalformed
			}
			t.ID, haveHeader = binary.BigEndian.Uint32(p[at:]), true
		case "edts":
			return o.children(h, 0, func(h boxHeader) error {
				if h.typ == "elst" {
					return o.elst(t, h)
				}
				return nil
			})
		case "mdia":
			haveMedia = true
			return o.mdia(t, h)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !haveHeader || !haveMedia || t.Timescale == 0 || t.Track == nil || t.sizes.field < 0 {
		return ErrMalformed
	}
	if t.Samples > 0 && (t.chunks.n < 0 || t.stsc.n < 0 || t.stts.n < 0) {
		return ErrMalformed
	}
	t.Chunks = max(t.chunks.n, 0)
	if t.Track.Codec.IsAudio() && t.Track.SampleRate == 0 {
		t.Track.SampleRate = int(t.Timescale)
	}
	return nil
}

func (o *opener) elst(t *Track, h boxHeader) error {
	p, err := o.payload(h, 8)
	if err != nil {
		return err
	}
	v, p, ok := fullHeader(p)
	if !ok || len(p) < 4 || v > 1 {
		return ErrMalformed
	}
	n, entry := int64(binary.BigEndian.Uint32(p)), int64(12)
	if v == 1 {
		entry = 20
	}
	if n > MaxEdits {
		return ErrTooLarge
	}
	if 8+n*entry > h.payloadLength {
		return ErrMalformed
	}
	p, err = o.payload(h, 8+n*entry)
	if err != nil {
		return err
	}
	p = p[8:]
	t.Edits = make([]Edit, n)
	for i := range t.Edits {
		e := &t.Edits[i]
		if v == 1 {
			e.Duration = int64(binary.BigEndian.Uint64(p) & math.MaxInt64)
			e.MediaTime = int64(binary.BigEndian.Uint64(p[8:]))
			p = p[16:]
		} else {
			e.Duration = int64(binary.BigEndian.Uint32(p))
			e.MediaTime = int64(int32(binary.BigEndian.Uint32(p[4:])))
			p = p[8:]
		}
		e.Rate = int32(binary.BigEndian.Uint32(p))
		p = p[4:]
	}
	return nil
}

func (o *opener) mdia(t *Track, mdia boxHeader) error {
	return o.children(mdia, 0, func(h boxHeader) error {
		switch h.typ {
		case "mdhd":
			p, err := o.payload(h, 36)
			if err != nil {
				return err
			}
			v, p, ok := fullHeader(p)
			ts, dur, ok := times(v, p, ok)
			at := 16
			if v == 1 {
				at = 28
			}
			if !ok || ts == 0 || len(p) < at+2 {
				return ErrMalformed
			}
			t.Timescale, t.Duration = ts, av.FromTicks(dur, int(ts))
			t.Language = language(binary.BigEndian.Uint16(p[at:]))
		case "hdlr":
			p, err := o.payload(h, 12)
			if err != nil {
				return err
			}
			if len(p) < 12 {
				return ErrMalformed
			}
			t.Handler = string(p[8:12])
		case "minf":
			return o.children(h, 0, func(h boxHeader) error {
				if h.typ == "stbl" {
					return o.stbl(t, h)
				}
				return nil
			})
		}
		return nil
	})
}

// language decodes mdhd's packed ISO 639-2/T code: three letters of five
// bits each, offset by 0x60. A Macintosh language code (below 0x400) or a
// code that is not three letters is "und".
func language(v uint16) string {
	if v < 0x400 {
		return "und"
	}
	b := []byte{byte(v>>10&31) + 0x60, byte(v>>5&31) + 0x60, byte(v&31) + 0x60}
	for _, c := range b {
		if c < 'a' || c > 'z' {
			return "und"
		}
	}
	return string(b)
}

func (o *opener) stbl(t *Track, stbl boxHeader) error {
	return o.children(stbl, 0, func(h boxHeader) error {
		switch h.typ {
		case "stsd":
			return o.stsd(t, h)
		case "stts":
			return o.table(&t.stts, h, 8)
		case "ctts":
			return o.table(&t.ctts, h, 8)
		case "stss":
			return o.table(&t.stss, h, 4)
		case "stsc":
			return o.table(&t.stsc, h, 12)
		case "stco":
			return o.table(&t.chunks, h, 4)
		case "co64":
			return o.table(&t.chunks, h, 8)
		case "stsz", "stz2":
			return o.sizeTable(t, h)
		}
		return nil
	})
}

// table reads the entry count of a table of entry bytes an entry.
func (o *opener) table(b *tableBox, h boxHeader, entry int) error {
	if b.n >= 0 {
		return ErrMalformed
	}
	p, err := o.payload(h, 8)
	if err != nil {
		return err
	}
	_, p, ok := fullHeader(p)
	if !ok || len(p) < 4 {
		return ErrMalformed
	}
	n := int64(binary.BigEndian.Uint32(p))
	if n*int64(entry) > h.payloadLength-8 {
		return ErrMalformed
	}
	b.off, b.n, b.entry = h.at+8, int(n), entry
	return nil
}

func (o *opener) sizeTable(t *Track, h boxHeader) error {
	if t.sizes.field >= 0 {
		return ErrMalformed
	}
	p, err := o.payload(h, 12)
	if err != nil {
		return err
	}
	_, p, ok := fullHeader(p)
	if !ok || len(p) < 8 {
		return ErrMalformed
	}
	n := int64(binary.BigEndian.Uint32(p[4:]))
	if n > MaxSamples {
		return ErrTooLarge
	}
	field := 32
	if h.typ == "stz2" {
		field = int(p[3])
		if field != 4 && field != 8 && field != 16 {
			return ErrMalformed
		}
	} else if t.sizes.constant = binary.BigEndian.Uint32(p); t.sizes.constant != 0 {
		field = 0
	}
	if (n*int64(field)+7)/8 > h.payloadLength-12 {
		return ErrMalformed
	}
	t.sizes.off, t.sizes.field, t.Samples = h.at+12, field, int(n)
	return nil
}

func (o *opener) stsd(t *Track, h boxHeader) error {
	if t.Track != nil {
		return ErrMalformed
	}
	if h.payloadLength > MaxSampleEntries {
		return ErrTooLarge
	}
	p, err := o.payload(h, h.payloadLength)
	if err != nil {
		return err
	}
	if len(p) < 8 {
		return ErrMalformed
	}
	t.Track = &av.Track{}
	if binary.BigEndian.Uint32(p[4:]) == 0 {
		return nil
	}
	e, _, err := fmp4.NextBox(p[8:])
	if err != nil {
		return ErrMalformed
	}
	t.Track = fmp4.ParseSampleEntry(e)
	t.Track.Config = append([]byte(nil), t.Track.Config...)
	return nil
}

// shift gives the ticks of a media clock that edits add to the media's
// times: the empty edits before the first that plays, rescaled from the
// movie's clock, less where that one starts.
// Both are kept within ±limit, so that their difference is in range.
func shift(edits []Edit, movie, media uint32) int64 {
	var delay, start int64
	for _, e := range edits {
		if e.MediaTime == -1 {
			delay = min(delay+rescale(e.Duration, int64(media), int64(movie)), limit)
			continue
		}
		start = max(min(e.MediaTime, limit), -limit)
		break
	}
	return delay - start
}

// limit bounds the times shift adds up.
const limit = math.MaxInt64 / 4

// rescale returns a*b/c rounded to the nearest, for a, b >= 0 and c > 0,
// saturating at limit.
func rescale(a, b, c int64) int64 {
	if a < 0 || b < 0 || c <= 0 {
		return 0
	}
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	lo, carry := bits.Add64(lo, uint64(c)/2, 0)
	hi += carry
	if hi >= uint64(c) {
		return limit
	}
	q, _ := bits.Div64(hi, lo, uint64(c))
	if q > limit {
		return limit
	}
	return int64(q)
}
