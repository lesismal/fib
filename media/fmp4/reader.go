package fmp4

import (
	"encoding/binary"
	"errors"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/opus"
)

var (
	ErrNoMoov     = errors.New("fmp4: no moov")
	ErrMalformed  = errors.New("fmp4: malformed box")
	ErrNoMoof     = errors.New("fmp4: no moof")
	ErrTrack      = errors.New("fmp4: fragment of a track the init segment does not have")
	ErrSampleData = errors.New("fmp4: sample data outside the fragment")
)

// InitTrack is one track of a parsed init segment.
type InitTrack struct {
	ID        uint32
	Timescale uint32
	// Track describes the codec, from the sample entry; a codec this
	// package does not know has av.CodecUnknown.
	Track *av.Track
	// The trex defaults of the track's fragments.
	DefaultDuration uint32
	DefaultSize     uint32
	DefaultFlags    uint32
}

// Init is a parsed init segment: what reading its fragments takes.
type Init struct {
	Tracks []InitTrack
}

// ParseInit parses an init segment, or any file with a moov, for its tracks.
// Track configs refer to b.
func ParseInit(b []byte) (*Init, error) {
	moov, ok := child(b, "moov")
	if !ok {
		return nil, ErrNoMoov
	}
	init := &Init{}
	for box, err := range Boxes(moov) {
		if err != nil {
			return nil, ErrMalformed
		}
		if box.Type.String() != "trak" {
			continue
		}
		t, err := parseTrak(box.Data)
		if err != nil {
			return nil, err
		}
		init.Tracks = append(init.Tracks, t)
	}
	if len(init.Tracks) == 0 {
		return nil, ErrNoTracks
	}
	if mvex, ok := child(moov, "mvex"); ok {
		for box, err := range Boxes(mvex) {
			if err != nil {
				return nil, ErrMalformed
			}
			if box.Type.String() != "trex" {
				continue
			}
			_, _, p, ok := fullHeader(box.Data)
			if !ok || len(p) < 20 {
				return nil, ErrMalformed
			}
			if i := init.index(binary.BigEndian.Uint32(p)); i >= 0 {
				t := &init.Tracks[i]
				t.DefaultDuration = binary.BigEndian.Uint32(p[8:])
				t.DefaultSize = binary.BigEndian.Uint32(p[12:])
				t.DefaultFlags = binary.BigEndian.Uint32(p[16:])
			}
		}
	}
	return init, nil
}

// index returns the index of the track with track_ID id, or -1.
func (init *Init) index(id uint32) int {
	for i := range init.Tracks {
		if init.Tracks[i].ID == id {
			return i
		}
	}
	return -1
}

func parseTrak(trak []byte) (InitTrack, error) {
	var t InitTrack
	tkhd, ok := child(trak, "tkhd")
	if !ok {
		return t, ErrMalformed
	}
	v, _, p, ok := fullHeader(tkhd)
	idAt := 8
	if v == 1 {
		idAt = 16
	}
	if !ok || len(p) < idAt+4 {
		return t, ErrMalformed
	}
	t.ID = binary.BigEndian.Uint32(p[idAt:])
	mdia, ok1 := child(trak, "mdia")
	mdhd, ok2 := child(mdia, "mdhd")
	v, _, p, ok3 := fullHeader(mdhd)
	tsAt := 8
	if v == 1 {
		tsAt = 16
	}
	if !ok1 || !ok2 || !ok3 || len(p) < tsAt+4 {
		return t, ErrMalformed
	}
	t.Timescale = binary.BigEndian.Uint32(p[tsAt:])
	minf, _ := child(mdia, "minf")
	stbl, _ := child(minf, "stbl")
	stsd, ok := child(stbl, "stsd")
	_, _, p, ok1 = fullHeader(stsd)
	if !ok || !ok1 || len(p) < 4 {
		return t, ErrMalformed
	}
	entry, _, err := NextBox(p[4:])
	if err != nil {
		return t, ErrMalformed
	}
	t.Track = parseEntry(entry)
	if t.Track.Codec.IsAudio() && t.Track.SampleRate == 0 {
		t.Track.SampleRate = int(t.Timescale)
	}
	return t, nil
}

// ParseSampleEntry reads the codec of a sample entry, one of the boxes after
// an stsd's entry count, as ParseInit reads each track's first one: avc1 and
// avc3, hvc1 and hev1, av01, mp4a and Opus. A format it does not know, or an
// entry it cannot read, gives a track whose codec is av.CodecUnknown, never
// an error. The Config refers to e.Data. An audio track's SampleRate is the
// AudioSpecificConfig's or the entry's; where it is 0 the media's timescale
// is the rate to take, as ParseInit does. Package media/mp4 reads the sample
// entries of whole files with it.
func ParseSampleEntry(e Box) *av.Track { return parseEntry(e) }

// parseEntry reads a sample entry. What it cannot read leaves the track's
// codec unknown rather than failing, as a player skips a track it cannot
// play.
func parseEntry(e Box) *av.Track {
	t := &av.Track{}
	format := e.Type.String()
	switch format {
	case "avc1", "avc3", "hvc1", "hev1", "av01":
		if len(e.Data) < 78 {
			return t
		}
		t.Width = int(binary.BigEndian.Uint16(e.Data[24:]))
		t.Height = int(binary.BigEndian.Uint16(e.Data[26:]))
		var configType string
		switch format {
		case "avc1", "avc3":
			t.Codec, configType = av.CodecH264, "avcC"
		case "hvc1", "hev1":
			t.Codec, configType = av.CodecH265, "hvcC"
		default:
			t.Codec, configType = av.CodecAV1, "av1C"
		}
		t.Config, _ = child(e.Data[78:], configType)
	case "mp4a", "Opus":
		if len(e.Data) < 28 {
			return t
		}
		t.Channels = int(binary.BigEndian.Uint16(e.Data[16:]))
		t.SampleRate = int(binary.BigEndian.Uint32(e.Data[24:]) >> 16)
		if format == "mp4a" {
			parseESDS(t, e.Data[28:])
		} else {
			parseDOps(t, e.Data[28:])
		}
	}
	return t
}

func parseESDS(t *av.Track, children []byte) {
	esds, ok := child(children, "esds")
	_, _, p, ok1 := fullHeader(esds)
	if !ok || !ok1 {
		return
	}
	asc := findDescriptor(p, 0x05, 0)
	if asc == nil {
		return
	}
	var c aac.Config
	if c.Unmarshal(asc) != nil {
		return
	}
	t.Codec, t.Config = av.CodecAAC, asc
	t.SampleRate = c.SampleRate
	if n := c.Channels(); n > 0 {
		t.Channels = n
	}
}

// findDescriptor returns the payload of the first descriptor of tag in the
// descriptors of b, looking inside ES_Descriptor and DecoderConfigDescriptor.
func findDescriptor(b []byte, tag byte, depth int) []byte {
	for len(b) >= 2 && depth < 4 {
		d := b[0]
		n, i := 0, 1
		for ; i < len(b) && i <= 4; i++ {
			n = n<<7 | int(b[i]&0x7f)
			if b[i]&0x80 == 0 {
				break
			}
		}
		i++
		if i > len(b) || n > len(b)-i {
			return nil
		}
		payload := b[i : i+n]
		b = b[i+n:]
		switch {
		case d == tag:
			return payload
		case d == 0x03 && len(payload) >= 3:
			// ES_ID, then flags that may announce optional fields.
			flags, skip := payload[2], 3
			if flags&0x80 != 0 {
				skip += 2
			}
			if flags&0x40 != 0 && skip < len(payload) {
				skip += 1 + int(payload[skip])
			}
			if flags&0x20 != 0 {
				skip += 2
			}
			if skip <= len(payload) {
				if r := findDescriptor(payload[skip:], tag, depth+1); r != nil {
					return r
				}
			}
		case d == 0x04 && len(payload) >= 13:
			if r := findDescriptor(payload[13:], tag, depth+1); r != nil {
				return r
			}
		}
	}
	return nil
}

func parseDOps(t *av.Track, children []byte) {
	p, ok := child(children, "dOps")
	if !ok || len(p) < 11 {
		return
	}
	h := opus.Head{
		Channels:        p[1],
		PreSkip:         binary.BigEndian.Uint16(p[2:]),
		InputSampleRate: binary.BigEndian.Uint32(p[4:]),
		OutputGain:      int16(binary.BigEndian.Uint16(p[8:])),
		MappingFamily:   p[10],
	}
	if h.MappingFamily != 0 {
		if len(p) < 13+int(h.Channels) {
			return
		}
		h.StreamCount, h.CoupledCount = p[11], p[12]
		h.ChannelMapping = p[13 : 13+int(h.Channels)]
	}
	config, err := h.Append(nil)
	if err != nil {
		return
	}
	t.Codec, t.Config = av.CodecOpus, config
	t.Channels, t.SampleRate = int(h.Channels), 48000
}

// ParseFragment reads one fragment, a moof and the mdat after it (with any
// styp, sidx or other boxes before), into packets: Track is the index into
// init.Tracks, DTS and PTS are the decode times from tfdt and the sample
// durations converted from each track's timescale, and Data refers to b.
// It returns the fragment's sequence number and the bytes after the mdat.
func (init *Init) ParseFragment(b []byte) (seq uint32, pkts []*av.Packet, rest []byte, err error) {
	var moof Box
	at := 0
	for {
		box, r, err := NextBox(b[at:])
		if err != nil {
			return 0, nil, b, ErrNoMoof
		}
		if box.Type.String() == "moof" {
			moof = box
			rest = r
			break
		}
		at += box.Size()
	}
	moofStart := b[at:]
	// The mdat, if any, follows; sample data is addressed from the moof
	// and must stay within moof and mdat.
	limit := moof.Size()
	if box, r, err := NextBox(rest); err == nil && box.Type.String() == "mdat" {
		limit += box.Size()
		rest = r
	}
	data := moofStart[:limit]
	for box, err := range Boxes(moof.Data) {
		if err != nil {
			return 0, nil, b, ErrMalformed
		}
		switch box.Type.String() {
		case "mfhd":
			_, _, p, ok := fullHeader(box.Data)
			if !ok || len(p) < 4 {
				return 0, nil, b, ErrMalformed
			}
			seq = binary.BigEndian.Uint32(p)
		case "traf":
			if pkts, err = init.parseTraf(box.Data, data, pkts); err != nil {
				return 0, nil, b, err
			}
		}
	}
	return seq, pkts, rest, nil
}

func (init *Init) parseTraf(traf, data []byte, pkts []*av.Packet) ([]*av.Packet, error) {
	tfhd, ok := child(traf, "tfhd")
	_, flags, p, ok1 := fullHeader(tfhd)
	if !ok || !ok1 || len(p) < 4 {
		return pkts, ErrMalformed
	}
	index := init.index(binary.BigEndian.Uint32(p))
	if index < 0 {
		return pkts, ErrTrack
	}
	t := &init.Tracks[index]
	p = p[4:]
	defDur, defSize, defFlags := t.DefaultDuration, t.DefaultSize, t.DefaultFlags
	var fields = []struct {
		flag uint32
		size int
		dst  *uint32
	}{
		{0x01, 8, nil}, {0x02, 4, nil}, {0x08, 4, &defDur}, {0x10, 4, &defSize}, {0x20, 4, &defFlags},
	}
	for _, f := range fields {
		if flags&f.flag == 0 {
			continue
		}
		if len(p) < f.size {
			return pkts, ErrMalformed
		}
		// A base data offset (0x01) is from the start of the file, which a
		// fragment read on its own cannot place; data offsets are taken from
		// the start of the moof, as default-base-is-moof has them.
		if f.dst != nil {
			*f.dst = binary.BigEndian.Uint32(p)
		}
		p = p[f.size:]
	}
	var dec uint64
	if tfdt, ok := child(traf, "tfdt"); ok {
		v, _, p, ok := fullHeader(tfdt)
		switch {
		case ok && v == 1 && len(p) >= 8:
			dec = binary.BigEndian.Uint64(p)
		case ok && v == 0 && len(p) >= 4:
			dec = uint64(binary.BigEndian.Uint32(p))
		default:
			return pkts, ErrMalformed
		}
	}
	pos := uint64(0)
	for box, err := range Boxes(traf) {
		if err != nil {
			return pkts, ErrMalformed
		}
		if box.Type.String() != "trun" {
			continue
		}
		pkts, pos, dec, err = parseTrun(box.Data, data, pkts, index, t, pos, dec, defDur, defSize, defFlags)
		if err != nil {
			return pkts, err
		}
	}
	return pkts, nil
}

func parseTrun(trun, data []byte, pkts []*av.Packet, index int, t *InitTrack, pos, dec uint64, defDur, defSize, defFlags uint32) ([]*av.Packet, uint64, uint64, error) {
	v, flags, p, ok := fullHeader(trun)
	if !ok || len(p) < 4 {
		return pkts, pos, dec, ErrMalformed
	}
	count := binary.BigEndian.Uint32(p)
	p = p[4:]
	if flags&0x01 != 0 {
		if len(p) < 4 {
			return pkts, pos, dec, ErrMalformed
		}
		off := int32(binary.BigEndian.Uint32(p))
		if off < 0 {
			return pkts, pos, dec, ErrSampleData
		}
		pos = uint64(off)
		p = p[4:]
	}
	firstFlags, hasFirst := uint32(0), flags&0x04 != 0
	if hasFirst {
		if len(p) < 4 {
			return pkts, pos, dec, ErrMalformed
		}
		firstFlags = binary.BigEndian.Uint32(p)
		p = p[4:]
	}
	per := 0
	for _, f := range []uint32{0x100, 0x200, 0x400, 0x800} {
		if flags&f != 0 {
			per += 4
		}
	}
	// A run's fields must be there, and no fragment has more samples than
	// bytes: together they bound the packets by what b holds, however many
	// runs claim the same bytes.
	if uint64(count)*uint64(per) > uint64(len(p)) || uint64(len(pkts))+uint64(count) > uint64(len(data)) {
		return pkts, pos, dec, ErrMalformed
	}
	for i := range count {
		dur, size, sflags, cts := defDur, defSize, defFlags, int64(0)
		if flags&0x100 != 0 {
			dur, p = binary.BigEndian.Uint32(p), p[4:]
		}
		if flags&0x200 != 0 {
			size, p = binary.BigEndian.Uint32(p), p[4:]
		}
		if flags&0x400 != 0 {
			sflags, p = binary.BigEndian.Uint32(p), p[4:]
		} else if i == 0 && hasFirst {
			sflags = firstFlags
		}
		if flags&0x800 != 0 {
			if v == 0 {
				cts = int64(binary.BigEndian.Uint32(p))
			} else {
				cts = int64(int32(binary.BigEndian.Uint32(p)))
			}
			p = p[4:]
		}
		if pos+uint64(size) > uint64(len(data)) {
			return pkts, pos, dec, ErrSampleData
		}
		ts := int(t.Timescale)
		dts := av.FromTicks(int64(dec), ts)
		pkts = append(pkts, &av.Packet{
			Track:    index,
			DTS:      dts,
			PTS:      av.FromTicks(int64(dec)+cts, ts),
			Keyframe: sflags&0x00010000 == 0,
			Data:     data[pos : pos+uint64(size)],
		})
		pos += uint64(size)
		dec += uint64(dur)
	}
	return pkts, pos, dec, nil
}
