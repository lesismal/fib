package mp4

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/fmp4"
)

// synth describes a track of a file the tests build, with every table
// written as the options say.
type synth struct {
	timescale uint32
	handler   string
	stsd      []byte // the stsd box; none is written when nil
	samples   []synthSample
	perChunk  []int // samples a chunk, cycled
	co64      bool
	// sizes: 32 for stsz, 4, 8 or 16 for stz2, 0 for a constant stsz.
	sizes  int
	ctts   int    // 0 none, 1 version 0, 2 version 1
	stss   string // "list", "none", "empty" or "reversed"
	edits  []Edit
	editV1 bool
	noSTTS bool
}

// synthChunk is a chunk of a built file: where it is in the mdat's payload,
// and its samples.
type synthChunk struct {
	at    int
	count int
}

type synthSample struct {
	size     uint32
	duration uint32
	cts      int32
	sync     bool
}

// box returns a box of type typ holding the concatenated payloads.
func box(typ string, payloads ...[]byte) []byte {
	n := 8
	for _, p := range payloads {
		n += len(p)
	}
	b := binary.BigEndian.AppendUint32(make([]byte, 0, n), uint32(n))
	b = append(b, typ...)
	for _, p := range payloads {
		b = append(b, p...)
	}
	return b
}

// full returns a full box's version and flags.
func full(v uint8, flags uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(v)<<24|flags)
}

func be32(vs ...uint32) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.BigEndian.AppendUint32(b, v)
	}
	return b
}

// sampleByte is the byte every sample's data is made of: its track's and
// its own index, so that ReadSample can be checked.
func sampleByte(track, i int) byte { return byte(track*37 + i) }

// buildFile returns a file of the tracks: ftyp, then the moov and an mdat
// in the order moovFirst says, the mdat with a 64-bit size if large.
func buildFile(tracks []synth, moovFirst, large bool) []byte {
	ftyp := box("ftyp", []byte("isom"), be32(512), []byte("isomiso2mp41"))
	// The media: each track's chunks in turn.
	var mdat []byte
	chunks := make([][]synthChunk, len(tracks))
	for ti, tr := range tracks {
		for i, k := 0, 0; i < len(tr.samples); k++ {
			n := min(tr.perChunk[k%len(tr.perChunk)], len(tr.samples)-i)
			chunks[ti] = append(chunks[ti], synthChunk{len(mdat), n})
			for j := i; j < i+n; j++ {
				for range tr.samples[j].size {
					mdat = append(mdat, sampleByte(ti, j))
				}
			}
			i += n
		}
	}
	mdatHeader := 8
	if large {
		mdatHeader = 16
	}
	moov := func(mdatAt int) []byte {
		var traks [][]byte
		for ti, tr := range tracks {
			var offsets []int64
			for _, c := range chunks[ti] {
				offsets = append(offsets, int64(mdatAt+mdatHeader+c.at))
			}
			traks = append(traks, tr.trak(uint32(ti+1), offsets, chunks[ti]))
		}
		mvhd := box("mvhd", full(0, 0), be32(0, 0, 1000, 1000), make([]byte, 80))
		return box("moov", append([][]byte{mvhd}, traks...)...)
	}
	var mdatBox []byte
	if large {
		mdatBox = append(be32(1), "mdat"...)
		mdatBox = binary.BigEndian.AppendUint64(mdatBox, uint64(16+len(mdat)))
		mdatBox = append(mdatBox, mdat...)
	} else {
		mdatBox = box("mdat", mdat)
	}
	if moovFirst {
		m := moov(0) // its size, the offsets being of a fixed width
		m = moov(len(ftyp) + len(m))
		return append(append(ftyp, m...), mdatBox...)
	}
	b := append(ftyp, mdatBox...)
	return append(b, moov(len(ftyp))...)
}

func (tr *synth) trak(id uint32, offsets []int64, chunks []synthChunk) []byte {
	var stbl [][]byte
	if tr.stsd != nil {
		stbl = append(stbl, tr.stsd)
	}
	// stts and ctts, run-length coded.
	var stts, ctts []byte
	var sttsN, cttsN uint32
	for i, s := range tr.samples {
		if i == 0 || s.duration != tr.samples[i-1].duration {
			stts = append(stts, be32(1, s.duration)...)
			sttsN++
		} else {
			binary.BigEndian.PutUint32(stts[len(stts)-8:], binary.BigEndian.Uint32(stts[len(stts)-8:])+1)
		}
		if i == 0 || s.cts != tr.samples[i-1].cts {
			ctts = append(ctts, be32(1, uint32(s.cts))...)
			cttsN++
		} else {
			binary.BigEndian.PutUint32(ctts[len(ctts)-8:], binary.BigEndian.Uint32(ctts[len(ctts)-8:])+1)
		}
	}
	if !tr.noSTTS {
		stbl = append(stbl, box("stts", full(0, 0), be32(sttsN), stts))
	}
	if tr.ctts > 0 {
		stbl = append(stbl, box("ctts", full(uint8(tr.ctts-1), 0), be32(cttsN), ctts))
	}
	var stss []uint32
	for i, s := range tr.samples {
		if s.sync {
			stss = append(stss, uint32(i+1))
		}
	}
	switch tr.stss {
	case "reversed":
		for i, j := 0, len(stss)-1; i < j; i, j = i+1, j-1 {
			stss[i], stss[j] = stss[j], stss[i]
		}
		fallthrough
	case "list":
		stbl = append(stbl, box("stss", full(0, 0), be32(uint32(len(stss))), be32(stss...)))
	case "empty":
		stbl = append(stbl, box("stss", full(0, 0), be32(0)))
	}
	// stsc, with an entry where the samples a chunk change.
	var stsc []byte
	var stscN uint32
	for c, ch := range chunks {
		if c == 0 || ch.count != chunks[c-1].count {
			stsc = append(stsc, be32(uint32(c+1), uint32(ch.count), 1)...)
			stscN++
		}
	}
	stbl = append(stbl, box("stsc", full(0, 0), be32(stscN), stsc))
	n := uint32(len(tr.samples))
	switch tr.sizes {
	case 0:
		size := uint32(0)
		if n > 0 {
			size = tr.samples[0].size
		}
		stbl = append(stbl, box("stsz", full(0, 0), be32(size, n)))
	case 32:
		var sizes []byte
		for _, s := range tr.samples {
			sizes = binary.BigEndian.AppendUint32(sizes, s.size)
		}
		stbl = append(stbl, box("stsz", full(0, 0), be32(0, n), sizes))
	default:
		var sizes []byte
		for i, s := range tr.samples {
			switch tr.sizes {
			case 16:
				sizes = binary.BigEndian.AppendUint16(sizes, uint16(s.size))
			case 8:
				sizes = append(sizes, byte(s.size))
			case 4:
				if i%2 == 0 {
					sizes = append(sizes, byte(s.size<<4))
				} else {
					sizes[len(sizes)-1] |= byte(s.size & 15)
				}
			}
		}
		stbl = append(stbl, box("stz2", full(0, 0), be32(uint32(tr.sizes), n), sizes))
	}
	var co []byte
	for _, off := range offsets {
		if tr.co64 {
			co = binary.BigEndian.AppendUint64(co, uint64(off))
		} else {
			co = binary.BigEndian.AppendUint32(co, uint32(off))
		}
	}
	if tr.co64 {
		stbl = append(stbl, box("co64", full(0, 0), be32(uint32(len(offsets))), co))
	} else {
		stbl = append(stbl, box("stco", full(0, 0), be32(uint32(len(offsets))), co))
	}

	tkhd := box("tkhd", full(0, 3), be32(0, 0, id, 0, 0), make([]byte, 60))
	var trak [][]byte
	trak = append(trak, tkhd)
	if tr.edits != nil {
		var elst []byte
		for _, e := range tr.edits {
			if tr.editV1 {
				elst = binary.BigEndian.AppendUint64(elst, uint64(e.Duration))
				elst = binary.BigEndian.AppendUint64(elst, uint64(e.MediaTime))
			} else {
				elst = append(elst, be32(uint32(e.Duration), uint32(e.MediaTime))...)
			}
			elst = append(elst, be32(uint32(e.Rate))...)
		}
		v := uint8(0)
		if tr.editV1 {
			v = 1
		}
		trak = append(trak, box("edts", box("elst", full(v, 0), be32(uint32(len(tr.edits))), elst)))
	}
	// mdhd's language "eng": 5 bits a letter, offset by 0x60.
	lang := uint32('e'-0x60)<<10 | uint32('n'-0x60)<<5 | uint32('g'-0x60)
	mdhd := box("mdhd", full(0, 0), be32(0, 0, tr.timescale, 0), be32(lang<<16))
	hdlr := box("hdlr", full(0, 0), be32(0), []byte(tr.handler), make([]byte, 13))
	minf := box("minf", box("stbl", stbl...))
	trak = append(trak, box("mdia", mdhd, hdlr, minf))
	return box("trak", trak...)
}

// stsdOf returns the stsd box fmp4 writes for track.
func stsdOf(t testing.TB, track *av.Track) []byte {
	t.Helper()
	w, err := fmp4.NewWriter([]*av.Track{track})
	if err != nil {
		t.Fatal(err)
	}
	b := w.Init()
	for _, typ := range []string{"moov", "trak", "mdia", "minf", "stbl", "stsd"} {
		found := false
		for bx, err := range fmp4.Boxes(b) {
			if err != nil {
				t.Fatal(err)
			}
			if bx.Type.String() == typ {
				if typ == "stsd" {
					return box("stsd", bx.Data)
				}
				b, found = bx.Data, true
				break
			}
		}
		if !found {
			t.Fatalf("no %s", typ)
		}
	}
	return nil
}

func h264Track(t testing.TB) *av.Track {
	config, err := os.ReadFile("../fmp4/testdata/h264_high_720.avcc")
	if err != nil {
		t.Fatal(err)
	}
	return &av.Track{Codec: av.CodecH264, Config: config}
}

// aacConfig is the AudioSpecificConfig of AAC LC at 48 kHz, mono.
var aacConfig = []byte{0x11, 0x88}

// videoSamples returns n samples of a 25 fps video at a 12800 Hz clock with
// B-frames in a pattern I P B B, and a sync sample every gop.
func videoSamples(n, gop int) []synthSample {
	s := make([]synthSample, n)
	for i := range s {
		cts := []int32{1024, 2048, 512, 512}[i%4]
		s[i] = synthSample{size: uint32(100 + i%7*13), duration: 512, cts: cts, sync: i%gop == 0}
	}
	return s
}

// replaceBox returns b with the first box of type typ, depth first,
// replaced by one with the payload, and the sizes of the boxes holding it
// fixed. The media data does not move if the moov is after it.
func replaceBox(t testing.TB, b []byte, typ string, payload []byte) []byte {
	t.Helper()
	out, ok := replaceIn(b, typ, payload)
	if !ok {
		t.Fatalf("no %s", typ)
	}
	return out
}

func replaceIn(b []byte, typ string, payload []byte) ([]byte, bool) {
	var out []byte
	done := false
	for len(b) >= 8 {
		size := int(binary.BigEndian.Uint32(b))
		if size < 8 || size > len(b) {
			break
		}
		name, data := string(b[4:8]), b[8:size]
		switch {
		case done:
			out = append(out, b[:size]...)
		case name == typ:
			out, done = append(out, box(typ, payload)...), true
		case name == "moov" || name == "trak" || name == "mdia" || name == "minf" || name == "stbl" || name == "edts":
			var inner []byte
			inner, done = replaceIn(data, typ, payload)
			out = append(out, box(name, inner)...)
		default:
			out = append(out, b[:size]...)
		}
		b = b[size:]
	}
	return append(out, b...), done
}

// audioSamples returns n AAC frames at 48 kHz.
func audioSamples(n int) []synthSample {
	s := make([]synthSample, n)
	for i := range s {
		s[i] = synthSample{size: uint32(5 + i%9), duration: 1024, cts: int32(i/40)*512 - 512, sync: true}
	}
	return s
}
