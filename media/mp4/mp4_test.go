package mp4

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/media/av"
)

// variant is one way of writing the tables of a built file.
type variant struct {
	moovFirst, large, co64 bool
	videoSizes, audioSizes int
	ctts                   int
	stss                   string
	perChunk               []int
}

func (v variant) String() string {
	return fmt.Sprintf("front=%v large=%v co64=%v sizes=%d,%d ctts=%d stss=%s chunks=%v",
		v.moovFirst, v.large, v.co64, v.videoSizes, v.audioSizes, v.ctts, v.stss, v.perChunk)
}

func (v variant) tracks(t testing.TB) []synth {
	video := synth{timescale: 12800, handler: "vide", stsd: stsdOf(t, h264Track(t)), samples: videoSamples(57, 10),
		perChunk: v.perChunk, co64: v.co64, sizes: v.videoSizes, ctts: v.ctts, stss: v.stss,
		edits: []Edit{{Duration: 2280, MediaTime: 1024, Rate: 1 << 16}}}
	audio := synth{timescale: 48000, handler: "soun", stsd: stsdOf(t, &av.Track{Codec: av.CodecAAC, Config: aacConfig}),
		samples: audioSamples(107), perChunk: []int{5, 3}, co64: v.co64, sizes: v.audioSizes, ctts: v.ctts, stss: "none",
		edits: []Edit{{Duration: 100, MediaTime: -1, Rate: 1 << 16}, {Duration: 2280, MediaTime: 0, Rate: 1 << 16}}}
	if v.videoSizes == 0 {
		for i := range video.samples {
			video.samples[i].size = 77
		}
	}
	return []synth{video, audio}
}

func variants() []variant {
	var vs []variant
	for _, front := range []bool{true, false} {
		for _, sizes := range [][2]int{{32, 32}, {16, 8}, {0, 4}} {
			for ctts := range 3 {
				for _, stss := range []string{"list", "none", "empty", "reversed"} {
					vs = append(vs, variant{moovFirst: front, large: ctts == 1, co64: stss == "list" || stss == "empty",
						videoSizes: sizes[0], audioSizes: sizes[1], ctts: ctts, stss: stss,
						perChunk: [][]int{{1}, {4}, {3, 1, 7}}[(len(vs))%3]})
				}
			}
		}
	}
	return vs
}

// TestTables builds files of every way of writing the tables and checks
// that every sample reads back, each lookup agreeing with a walk of all of
// them, and that each table takes the bytes TableBytes said.
func TestTables(t *testing.T) {
	for _, v := range variants() {
		t.Run(v.String(), func(t *testing.T) {
			tracks := v.tracks(t)
			b := buildFile(tracks, v.moovFirst, v.large)
			r := bytes.NewReader(b)
			f, err := Open(r, int64(len(b)))
			if err != nil {
				t.Fatal(err)
			}
			if len(f.Tracks) != 2 || f.MajorBrand != "isom" || f.Timescale != 1000 || f.Duration != time.Second {
				t.Fatalf("%+v", f)
			}
			if front := f.MoovOffset < int64(len(b)/2); front != v.moovFirst {
				t.Fatalf("moov at %d of %d", f.MoovOffset, len(b))
			}
			for ti, tr := range f.Tracks {
				checkTrack(t, r, ti, tr, &tracks[ti])
			}
			if v := f.Tracks[0]; v.Track.Codec != av.CodecH264 || v.Track.Width != 1280 || v.Handler != "vide" || v.Language != "eng" || v.ID != 1 || v.Shift != -1024 {
				t.Fatalf("%+v %+v", v, v.Track)
			}
			// The audio's empty edit of 100 ms of the movie's clock delays
			// it by 4800 ticks of its own.
			if a := f.Tracks[1]; a.Track.Codec != av.CodecAAC || a.Track.SampleRate != 48000 || a.Track.Channels != 1 || a.ID != 2 || a.Shift != 4800 {
				t.Fatalf("%+v %+v", a, a.Track)
			}
		})
	}
}

func checkTrack(t *testing.T, r io.ReaderAt, ti int, tr *Track, want *synth) {
	t.Helper()
	if tr.Samples != len(want.samples) || tr.Timescale != want.timescale {
		t.Fatalf("track %d: %d samples, timescale %d", ti, tr.Samples, tr.Timescale)
	}
	tb, err := tr.ReadTable(r)
	if err != nil {
		t.Fatalf("track %d: %v", ti, err)
	}
	if tb.Bytes() != tr.TableBytes() {
		t.Fatalf("track %d: %d bytes, TableBytes %d", ti, tb.Bytes(), tr.TableBytes())
	}
	var dts int64
	var keyframes []int
	var data []byte
	var total int64
	for i, s := range tb.Samples(0) {
		w := want.samples[i]
		sync := w.sync
		switch want.stss {
		case "none":
			sync = true
		case "empty":
			sync = false
		}
		cts := w.cts
		if want.ctts == 0 {
			cts = 0
		}
		if s.Size != w.size || s.Duration != w.duration || s.DTS != dts || s.CTS != cts || s.Keyframe != sync {
			t.Fatalf("track %d sample %d: %+v, want %+v at %d", ti, i, s, w, dts)
		}
		if s != tb.Sample(i) {
			t.Fatalf("track %d sample %d: walked %+v, looked up %+v", ti, i, s, tb.Sample(i))
		}
		if data, err = ReadSample(r, s, data); err != nil || len(data) != int(w.size) || bytes.Count(data, []byte{sampleByte(ti, i)}) != len(data) {
			t.Fatalf("track %d sample %d: %v %x", ti, i, err, data)
		}
		if sync {
			keyframes = append(keyframes, i)
		}
		dts += int64(w.duration)
		total += int64(w.size)
	}
	minCTS := int32(0)
	for _, s := range want.samples {
		if want.ctts > 0 {
			minCTS = min(minCTS, s.cts)
		}
	}
	if tb.MinCTS() != minCTS {
		t.Fatalf("track %d: MinCTS %d, want %d", ti, tb.MinCTS(), minCTS)
	}
	if tb.End() != dts || tb.DataSize() != total || tb.Len() != len(want.samples) {
		t.Fatalf("track %d: end %d, %d bytes, want %d, %d", ti, tb.End(), tb.DataSize(), dts, total)
	}
	checkLookups(t, tb, keyframes)
}

// checkLookups checks the table's lookups against a walk of every sample.
func checkLookups(t *testing.T, tb *Table, keyframes []int) {
	t.Helper()
	var all []Sample
	for _, s := range tb.Samples(0) {
		all = append(all, s)
	}
	for from := range len(all) + 1 {
		n := 0
		for i, s := range tb.Samples(from) {
			if i != from+n || s != all[i] {
				t.Fatalf("Samples(%d) yielded %d %+v", from, i, s)
			}
			n++
		}
		if n != len(all)-from {
			t.Fatalf("Samples(%d) yielded %d", from, n)
		}
	}
	if tb.Keyframes() != len(keyframes) {
		t.Fatalf("%d keyframes, want %d", tb.Keyframes(), len(keyframes))
	}
	for k, i := range keyframes {
		if tb.Keyframe(k) != i {
			t.Fatalf("keyframe %d at %d, want %d", k, tb.Keyframe(k), i)
		}
	}
	for i := -1; i <= len(all); i++ {
		before, after := -1, -1
		for _, k := range keyframes {
			if k <= i {
				before = k
			}
			if k >= i && after < 0 && i >= 0 {
				after = k
			}
		}
		if i < 0 && len(keyframes) > 0 {
			after = keyframes[0]
		}
		if got := tb.KeyframeBefore(i); got != before {
			t.Fatalf("KeyframeBefore(%d) = %d, want %d", i, got, before)
		}
		if got := tb.KeyframeAfter(i); got != after {
			t.Fatalf("KeyframeAfter(%d) = %d, want %d", i, got, after)
		}
	}
	// SeekKeyframe takes sync samples to be presented in decode order;
	// built tracks whose every sample is one have B-frames among them.
	ordered := slices.IsSortedFunc(keyframes, func(a, b int) int { return int(all[a].PTS() - all[b].PTS()) })
	// Times every half tick, from before the first sample to past the
	// end, more sparsely in the middle.
	step := tb.Time(1) - tb.Time(0)
	for d := tb.Time(0) - 3*step; d <= tb.Time(tb.End())+3*step; d += step / 2 {
		ticks := tb.Ticks(d)
		index := -1
		for i, s := range all {
			if s.DTS <= ticks {
				index = i
			}
		}
		if got := tb.IndexAt(d); got != index {
			t.Fatalf("IndexAt(%v) = %d, want %d", d, got, index)
		}
		seek := -1
		for _, k := range keyframes {
			if all[k].PTS() <= ticks || seek < 0 {
				seek = k
			}
		}
		if got := tb.SeekKeyframe(d); ordered && got != seek {
			t.Fatalf("SeekKeyframe(%v) = %d, want %d", d, got, seek)
		}
		if d > tb.Time(0)+5*step && d < tb.Time(tb.End())-5*step {
			d += 97 * step // fewer in the middle
		}
	}
}

// TestRewrittenFFmpeg rewrites the tables of ffmpeg's h264_aac.mp4, whose
// moov is at the end, with co64 and stz2 in place of stco and stsz, and
// checks every sample against ffprobe's reading of the original.
func TestRewrittenFFmpeg(t *testing.T) {
	r, f := openFile(t, "testdata/h264_aac.mp4")
	b := make([]byte, f.Size)
	if _, err := r.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	moov := b[f.MoovOffset : f.MoovOffset+f.MoovSize]
	field := map[int]int{0: 16, 1: 8} // stz2 fields of the tracks' sizes
	track := -1
	var rewrite func(typ string, payload []byte) []byte
	rewrite = func(typ string, payload []byte) []byte {
		switch typ {
		case "moov", "trak", "mdia", "minf", "stbl", "edts":
			var children [][]byte
			for len(payload) > 0 {
				size := int(be(payload[:4]))
				if typ == "moov" && string(payload[4:8]) == "trak" {
					track++
				}
				children = append(children, rewrite(string(payload[4:8]), payload[8:size]))
				payload = payload[size:]
			}
			return box(typ, children...)
		case "stco":
			n := int(be(payload[4:8]))
			out := slices.Clone(payload[:8])
			for i := range n {
				out = append(out, 0, 0, 0, 0)
				out = append(out, payload[8+4*i:12+4*i]...)
			}
			return box("co64", out)
		case "stsz":
			n := int(be(payload[8:12]))
			if be(payload[4:8]) != 0 {
				t.Fatal("constant sizes")
			}
			out := append(full(0, 0), be32(uint32(field[track]), uint32(n))...)
			for i := range n {
				v := be(payload[12+4*i:])
				if v >= 1<<field[track] {
					t.Fatalf("size %d", v)
				}
				if field[track] == 16 {
					out = append(out, byte(v>>8))
				}
				out = append(out, byte(v))
			}
			return box("stz2", out)
		}
		return box(typ, payload)
	}
	nb := append(slices.Clone(b[:f.MoovOffset]), rewrite("moov", moov[8:])...)
	nb = append(nb, b[f.MoovOffset+f.MoovSize:]...)
	nr := bytes.NewReader(nb)
	nf, err := Open(nr, int64(len(nb)))
	if err != nil {
		t.Fatal(err)
	}
	p := readProbe(t, "testdata/h264_aac.probe")
	for i, tr := range nf.Tracks {
		if tr.sizes.field != field[i] || tr.chunks.entry != 8 {
			t.Fatalf("track %d: not rewritten: %+v %+v", i, tr.sizes, tr.chunks)
		}
		tb, err := tr.ReadTable(nr)
		if err != nil {
			t.Fatal(err)
		}
		if tb.Bytes() != tr.TableBytes() || tb.Bytes() >= f.Tracks[i].TableBytes() {
			t.Fatalf("track %d: %d bytes, %d before", i, tb.Bytes(), f.Tracks[i].TableBytes())
		}
		checkSamples(t, i, tb, p.raw[i], p.pkt[i])
	}
}

func be(b []byte) uint32 { return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]) }

// TestEdits checks the shift of edit lists of both versions: empty edits
// before the first that plays, rescaled from the movie's clock, and edits
// after it ignored.
func TestEdits(t *testing.T) {
	for _, c := range []struct {
		edits []Edit
		v1    bool
		shift int64
	}{
		{nil, false, 0},
		{[]Edit{{Duration: 1000, MediaTime: 2048, Rate: 1 << 16}}, false, -2048},
		{[]Edit{{Duration: 1000, MediaTime: 2048, Rate: 1 << 16}}, true, -2048},
		{[]Edit{{Duration: 250, MediaTime: -1}, {Duration: 1000, MediaTime: 0}}, false, 3200},
		{[]Edit{{Duration: 250, MediaTime: -1}, {Duration: 1, MediaTime: -1}, {Duration: 1000, MediaTime: 512}, {Duration: 5, MediaTime: -1}}, true, 3200 + 13 - 512},
		{[]Edit{{Duration: 250, MediaTime: -1}}, false, 3200},
		{[]Edit{{Duration: 1 << 62, MediaTime: -1}, {Duration: 1, MediaTime: -1 << 62}}, true, limit + limit},
	} {
		tr := synth{timescale: 12800, handler: "vide", stsd: stsdOf(t, h264Track(t)), samples: videoSamples(4, 2),
			perChunk: []int{2}, sizes: 32, stss: "list", edits: c.edits, editV1: c.v1}
		b := buildFile([]synth{tr}, true, false)
		f, err := Open(bytes.NewReader(b), int64(len(b)))
		if err != nil {
			t.Fatal(err)
		}
		if got := f.Tracks[0]; got.Shift != c.shift || !slices.Equal(got.Edits, c.edits) && len(c.edits) > 0 {
			t.Fatalf("%+v: shift %d, edits %+v; want %d", c.edits, got.Shift, got.Edits, c.shift)
		}
	}
}

// errReader fails every read past at.
type errReader struct {
	r  io.ReaderAt
	at int64
}

var errRead = errors.New("read failed")

func (e errReader) ReadAt(b []byte, off int64) (int, error) {
	if off+int64(len(b)) > e.at {
		return 0, errRead
	}
	return e.r.ReadAt(b, off)
}

// TestErrors checks that files broken in each way fail with their error,
// from Open or from ReadTable.
func TestErrors(t *testing.T) {
	good := func() []synth {
		return variant{moovFirst: true, videoSizes: 32, audioSizes: 32, ctts: 1, stss: "list", perChunk: []int{4}}.tracks(t)
	}
	build := func(edit func([]synth), moovFirst bool) []byte {
		tracks := good()
		if edit != nil {
			edit(tracks)
		}
		return buildFile(tracks, moovFirst, false)
	}
	replace := func(b []byte, typ string, payload []byte) []byte { return replaceBox(t, b, typ, payload) }
	front := build(nil, true)
	back := build(nil, false)
	ftyp := box("ftyp", []byte("isom"), be32(512))
	mvex := box("moov", box("mvhd", full(0, 0), be32(0, 0, 1000, 0), make([]byte, 80)), box("mvex"))
	manyTraks := [][]byte{box("mvhd", full(0, 0), be32(0, 0, 1000, 0), make([]byte, 80))}
	trakAt := bytes.Index(front, []byte("trak")) - 4
	firstTrak := front[trakAt : trakAt+int(be(front[trakAt:]))]
	for range MaxTracks + 1 {
		manyTraks = append(manyTraks, firstTrak)
	}
	// A track of 2^31-1 samples of a byte, whose decode times run past
	// the clock.
	pastClock := replace(front, "stsz", append(full(0, 0), be32(1, 1<<31-1)...))
	pastClock = replace(pastClock, "stts", append(full(0, 0), be32(1, 1<<31-1, 1<<32-1)...))
	pastClock = replace(pastClock, "stsc", append(full(0, 0), be32(1, 1, 1<<31-1, 1)...))
	pastClock = replace(pastClock, "stco", append(full(0, 0), be32(1, 100)...))
	manyBoxes := slices.Clone(ftyp)
	for range MaxBoxes {
		manyBoxes = append(manyBoxes, box("free")...)
	}

	for _, c := range []struct {
		name  string
		b     []byte
		open  error // Open's error, or nil
		table error // the first track's ReadTable's
	}{
		{"empty", nil, ErrNoMoov, nil},
		{"ftyp only", ftyp, ErrNoMoov, nil},
		{"short box", append(slices.Clone(ftyp), 0, 0, 0), ErrNoMoov, nil},
		{"cut before moov", back[:len(back)-100], ErrNoMoov, nil},
		{"fragmented", append(slices.Clone(ftyp), mvex...), ErrFragmented, nil},
		{"moof", append(slices.Clone(ftyp), box("moof")...), ErrFragmented, nil},
		{"many boxes", manyBoxes, ErrTooLarge, nil},
		{"many tracks", append(slices.Clone(ftyp), box("moov", manyTraks...)...), ErrTooLarge, nil},
		{"empty trak", append(slices.Clone(ftyp), box("moov", manyTraks[0], box("trak"))...), ErrMalformed, nil},
		{"no mvhd", append(slices.Clone(ftyp), box("moov")...), ErrMalformed, nil},
		{"mvhd timescale 0", replace(front, "mvhd", append(full(0, 0), make([]byte, 96)...)), ErrMalformed, nil},
		{"short mvhd", replace(front, "mvhd", full(1, 0)), ErrMalformed, nil},
		{"short tkhd", replace(front, "tkhd", full(0, 0)), ErrMalformed, nil},
		{"short mdhd", replace(front, "mdhd", full(0, 0)), ErrMalformed, nil},
		{"short hdlr", replace(front, "hdlr", full(0, 0)), ErrMalformed, nil},
		{"short stsd", replace(front, "stsd", full(0, 0)), ErrMalformed, nil},
		{"bad sample entry", replace(front, "stsd", append(full(0, 0), be32(1, 3)...)), ErrMalformed, nil},
		{"large stsd", replace(front, "stsd", make([]byte, MaxSampleEntries+1)), ErrTooLarge, nil},
		{"two stsd", replace(front, "stsd", append(full(0, 0), be32(0)...)), nil, nil},
		{"many edits", replace(front, "elst", append(full(0, 0), be32(MaxEdits+1)...)), ErrTooLarge, nil},
		{"edits past box", replace(front, "elst", append(full(0, 0), be32(2, 1, 1, 1)...)), ErrMalformed, nil},
		{"elst version 2", replace(front, "elst", append(full(2, 0), be32(0)...)), ErrMalformed, nil},
		{"stts count past box", replace(front, "stts", append(full(0, 0), be32(2, 1, 1)...)), ErrMalformed, nil},
		{"short stts", replace(front, "stts", full(0, 0)), ErrMalformed, nil},
		{"stsz count past box", replace(front, "stsz", append(full(0, 0), be32(0, 3, 1, 1)...)), ErrMalformed, nil},
		{"stz2 of 12 bits", replace(front, "stsz", append(full(0, 0), be32(0, 0)...)), nil, nil},
		{"short stsz", replace(front, "stsz", full(0, 0)), ErrMalformed, nil},
		{"too many samples", replace(front, "stsz", append(full(0, 0), be32(1, 1<<31)...)), ErrTooLarge, nil},
		{"stts short of the samples", replace(front, "stts", append(full(0, 0), be32(1, 56, 512)...)), nil, ErrMalformed},
		{"stts past the clock", pastClock, nil, ErrMalformed},
		{"stsc short of the samples", replace(front, "stsc", append(full(0, 0), be32(1, 1, 1, 1)...)), nil, ErrMalformed},
		{"stsc not ascending", replace(front, "stsc", append(full(0, 0), be32(2, 1, 4, 1, 1, 4, 1)...)), nil, ErrMalformed},
		{"chunk past the file", replace(front, "stco", append(full(0, 0), be32(1, 1<<30)...)), nil, ErrSampleData},
		{"chunk running past the file", replace(replace(front, "stsc", append(full(0, 0), be32(1, 1, 57, 1)...)), "stco", append(full(0, 0), be32(1, uint32(len(front)-10))...)), nil, ErrSampleData},
		{"constant size past the file", replace(front, "stsz", append(full(0, 0), be32(1<<20, 57)...)), nil, ErrSampleData},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := bytes.NewReader(c.b)
			f, err := Open(r, int64(len(c.b)))
			if c.name == "two stsd" || c.name == "stz2 of 12 bits" {
				// Built by hand below.
				return
			}
			if !errors.Is(err, c.open) {
				t.Fatalf("Open: %v, want %v", err, c.open)
			}
			if err != nil {
				return
			}
			if _, err := f.Tracks[0].ReadTable(r); !errors.Is(err, c.table) {
				t.Fatalf("ReadTable: %v, want %v", err, c.table)
			}
		})
	}

	// Two tables of a kind, a missing one, an stz2 of a field size there is
	// none of, and a track without stsd or stsz.
	for name, edit := range map[string]func(s []synth){
		"no stsd": func(s []synth) { s[0].stsd = nil },
		"no stts": func(s []synth) { s[0].noSTTS = true },
	} {
		b := build(edit, true)
		f, err := Open(bytes.NewReader(b), int64(len(b)))
		if !errors.Is(err, ErrMalformed) {
			t.Fatalf("%s: %v %+v", name, err, f)
		}
	}
	stbl := bytes.Index(front, []byte("stsz")) - 4
	dup := replace(front, "stss", slices.Clone(front[stbl+8:stbl+int(be(front[stbl:]))]))
	dup = bytes.Replace(dup, []byte("stss"), []byte("stsz"), 1)
	if _, err := Open(bytes.NewReader(dup), int64(len(dup))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("two stsz: %v", err)
	}
	stz2 := bytes.Replace(replace(front, "stsz", append(full(0, 0), be32(12, 0)...)), []byte("stsz"), []byte("stz2"), 1)
	if _, err := Open(bytes.NewReader(stz2), int64(len(stz2))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("stz2 of 12 bits: %v", err)
	}
	dupSTCO := replace(front, "stss", slices.Clone(front[bytes.Index(front, []byte("stco"))+4:bytes.Index(front, []byte("stco"))-4+int(be(front[bytes.Index(front, []byte("stco"))-4:]))]))
	dupSTCO = bytes.Replace(dupSTCO, []byte("stss"), []byte("co64"), 1)
	if _, err := Open(bytes.NewReader(dupSTCO), int64(len(dupSTCO))); !errors.Is(err, ErrMalformed) {
		t.Fatalf("stco and co64: %v", err)
	}

	// A reader that fails: Open and ReadTable return its error.
	for at := int64(0); at < int64(len(front)); at += 7 {
		r := errReader{bytes.NewReader(front), at}
		f, err := Open(r, int64(len(front)))
		if err != nil {
			if !errors.Is(err, errRead) && !errors.Is(err, ErrNoMoov) {
				t.Fatalf("at %d: %v", at, err)
			}
			continue
		}
		for _, tr := range f.Tracks {
			if _, err := tr.ReadTable(r); err != nil && !errors.Is(err, errRead) {
				t.Fatalf("at %d: %v", at, err)
			}
		}
	}
	// A reader shorter than the size given: io.ErrUnexpectedEOF.
	f, err := Open(bytes.NewReader(front), int64(len(front)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Tracks[0].ReadTable(bytes.NewReader(front[:f.MoovOffset+200])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short reader: %v", err)
	}
	if _, err := Open(bytes.NewReader(front[:100]), int64(len(front))); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short reader: %v", err)
	}
	if _, err := ReadSample(bytes.NewReader(front), Sample{Offset: int64(len(front)) - 2, Size: 4}, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadSample past the end: %v", err)
	}
}

// TestLenientTables reads tables that are off in ways players accept: a
// ctts short of the samples, whose others have no offset; stts entries of
// no samples and past the last; sync samples numbered 0, past the last,
// repeated and out of order.
func TestLenientTables(t *testing.T) {
	tr := synth{timescale: 12800, handler: "vide", stsd: stsdOf(t, h264Track(t)), samples: videoSamples(20, 5),
		perChunk: []int{3}, sizes: 32, ctts: 1, stss: "list"}
	b := buildFile([]synth{tr}, false, false)
	b = replaceBox(t, b, "ctts", append(full(0, 0), be32(2, 0, 1, 4, 1024, 1, 7, 0)...))
	b = replaceBox(t, b, "stts", append(full(0, 0), be32(3, 0, 7, 19, 512, 9, 256)...))
	b = replaceBox(t, b, "stss", append(full(0, 0), be32(6, 0, 11, 1, 99, 11, 6)...))
	r := bytes.NewReader(b)
	f, err := Open(r, int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	tb, err := f.Tracks[0].ReadTable(r)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range tb.Samples(0) {
		cts := int32(0)
		if i < 4 {
			cts = 1024
		}
		duration, dts := uint32(512), int64(i)*512
		if i == 19 {
			duration = 256
		}
		key := i == 0 || i == 5 || i == 10
		if s.CTS != cts || s.Duration != duration || s.DTS != dts || s.Keyframe != key || s != tb.Sample(i) {
			t.Fatalf("sample %d: %+v", i, s)
		}
	}
	if tb.End() != 19*512+256 || tb.Keyframes() != 3 {
		t.Fatal(tb.End(), tb.Keyframes())
	}
}

// TestEmptyTrack reads a track without samples.
func TestEmptyTrack(t *testing.T) {
	tr := synth{timescale: 48000, handler: "soun", stsd: stsdOf(t, &av.Track{Codec: av.CodecAAC, Config: aacConfig}), perChunk: []int{1}, sizes: 32, stss: "none", noSTTS: true}
	b := buildFile([]synth{tr}, true, false)
	r := bytes.NewReader(b)
	f, err := Open(r, int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	tb, err := f.Tracks[0].ReadTable(r)
	if err != nil || tb.Len() != 0 || tb.Bytes() > f.Tracks[0].TableBytes() || tb.IndexAt(0) != -1 || tb.SeekKeyframe(0) != -1 || tb.KeyframeBefore(0) != -1 || tb.KeyframeAfter(0) != -1 {
		t.Fatal(err, tb)
	}
	for range tb.Samples(0) {
		t.Fatal("a sample")
	}
}

// TestUnknownCodec reads a track of a format fib does not carry, and one
// with an stsd of no entries.
func TestUnknownCodec(t *testing.T) {
	entry := box("tx3g", make([]byte, 40))
	tr := synth{timescale: 1000, handler: "text", stsd: box("stsd", full(0, 0), be32(1), entry), samples: audioSamples(3), perChunk: []int{1}, sizes: 32, stss: "none"}
	empty := tr
	empty.stsd = box("stsd", full(0, 0), be32(0))
	b := buildFile([]synth{tr, empty}, false, false)
	r := bytes.NewReader(b)
	f, err := Open(r, int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range f.Tracks {
		if tr.Track.Codec != av.CodecUnknown || tr.Handler != "text" {
			t.Fatalf("%+v", tr.Track)
		}
		if tb, err := tr.ReadTable(r); err != nil || tb.Len() != 3 {
			t.Fatal(err)
		}
	}
}

// TestVersion1Headers reads version 1 mvhd, tkhd and mdhd, whose times are
// 64 bits, and an unknown duration of all ones.
func TestVersion1Headers(t *testing.T) {
	tr := synth{timescale: 12800, handler: "vide", stsd: stsdOf(t, h264Track(t)), samples: videoSamples(4, 2), perChunk: []int{2}, sizes: 32, stss: "list"}
	b := buildFile([]synth{tr}, false, false)
	u64 := func(v uint64) []byte { return be32(uint32(v>>32), uint32(v)) }
	b = replaceBox(t, b, "mvhd", slices.Concat(full(1, 0), u64(1), u64(2), be32(600), u64(1200), make([]byte, 80)))
	b = replaceBox(t, b, "tkhd", slices.Concat(full(1, 3), u64(1), u64(2), be32(9, 0), u64(1200), make([]byte, 60)))
	lang := uint32('f'-0x60)<<10 | uint32('r'-0x60)<<5 | uint32('a'-0x60)
	b = replaceBox(t, b, "mdhd", slices.Concat(full(1, 0), u64(1), u64(2), be32(90000), u64(1<<64-1), be32(lang<<16)))
	f, err := Open(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	if v := f.Tracks[0]; f.Timescale != 600 || f.Duration != 2*time.Second || v.ID != 9 || v.Timescale != 90000 || v.Duration != 0 || v.Language != "fra" {
		t.Fatalf("%+v %+v", f, v)
	}
	b = replaceBox(t, b, "mdhd", slices.Concat(full(0, 0), be32(1, 2, 90000, 1<<32-1, lang<<16)))
	if f, err = Open(bytes.NewReader(b), int64(len(b))); err != nil || f.Tracks[0].Duration != 0 {
		t.Fatal(err, f.Tracks[0].Duration)
	}
}

func TestLanguage(t *testing.T) {
	for v, want := range map[uint16]string{0: "und", 0x3ff: "und", 0x7fff: "und", 0x55c4: "und", 0x15c7: "eng", 0x2a0e: "jpn"} {
		if got := language(v); got != want {
			t.Errorf("language(%#x) = %q, want %q", v, got, want)
		}
	}
}

// TestTableBound reads the table of a long track, an hour of video at 60
// fps with B-frames, and checks that it takes what TableBytes said, and
// that reading it allocates no more than that and its buffer.
func TestTableBound(t *testing.T) {
	const n = 3600 * 60
	tr := synth{timescale: 90000, handler: "vide", stsd: stsdOf(t, h264Track(t)), samples: videoSamples(n, 120),
		perChunk: []int{30}, sizes: 32, ctts: 1, stss: "list"}
	for i := range tr.samples {
		tr.samples[i].size = 1 // a small file holding the long table
	}
	b := buildFile([]synth{tr}, false, false)
	r := bytes.NewReader(b)
	f, err := Open(r, int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	track := f.Tracks[0]
	bound := track.TableBytes()
	// 4 bytes a sample for sizes and 4 for composition offsets (ctts has
	// an entry for most samples), 12 a chunk, 16 an stts entry, 4 a sync
	// sample.
	want := tableBaseBytes + 4*n + 4*n + 12*n/30 + 16 + 4*n/120
	if bound != want {
		t.Fatalf("TableBytes %d, want %d", bound, want)
	}
	if sttsRunBytes != 16 || cttsRunBytes != 8 || chunkBytes != 12 || unsafe.Sizeof(Table{}) > 512 {
		t.Fatal(sttsRunBytes, cttsRunBytes, chunkBytes, unsafe.Sizeof(Table{}))
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	tb, err := track.ReadTable(r)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	allocated := int64(after.TotalAlloc - before.TotalAlloc)
	// The heap rounds a large object up to its pages: 8 KiB for each of
	// the table's slices at most.
	if tb.Bytes() != bound || allocated > bound+readBuffer+8*8<<10 {
		t.Fatalf("table %d bytes, allocated %d, bound %d", tb.Bytes(), allocated, bound)
	}
	t.Logf("%d samples: table %d bytes (%.1f a sample), allocated %d", n, bound, float64(bound)/n, allocated)
	runtime.KeepAlive(tb)
}
