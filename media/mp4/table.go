package mp4

import (
	"encoding/binary"
	"io"
	"iter"
	"math"
	"slices"
	"sort"
	"time"
	"unsafe"

	"github.com/lesismal/fib/media/av"
)

// readBuffer is the size of the buffer ReadTable reads the tables through.
const readBuffer = 16 << 10

// Table is a track's sample table, read by Track.ReadTable: for every
// sample, in decode order, its offset and size in the file, its decode time
// and composition offset, and whether it is a sync sample. It keeps the
// tables of the file in a compact form rather than an entry a sample, so its
// size, Bytes, is the Track's TableBytes, known before it is read. A Table
// is immutable and safe to use from several goroutines.
type Table struct {
	// Timescale and Shift are the Track's: times are in ticks of
	// Timescale, and Shift turns a media time into a presentation time.
	Timescale uint32
	Shift     int64

	n        int
	constant uint32   // every sample's size, when sizes32 and sizes16 are nil
	sizes32  []uint32 // stsz
	sizes16  []uint16 // stz2 of 4, 8 or 16 bits
	// chunkOffset[c] is chunk c's offset in the file, and chunkFirst[c]
	// the index of its first sample: the next chunk's when it has none.
	chunkOffset []int64
	chunkFirst  []uint32
	stts        []sttsRun
	cttsRuns    []cttsRun // with a last run of offset 0 from the end of ctts
	cttsSamples []int32   // instead of cttsRuns when it takes less memory
	stss        []uint32  // sync sample indexes, ascending
	allSync     bool      // no stss: every sample is a sync sample
	minCTS      int32     // the least composition offset, if negative
	dataSize    int64
	end         int64 // the decode time after the last sample
}

// sttsRun is a run of samples of one duration: the first sample's index and
// decode time.
type sttsRun struct {
	first    uint32
	duration uint32
	dts      int64
}

// cttsRun is a run of samples of one composition offset from its first.
type cttsRun struct {
	first  uint32
	offset int32
}

// The bytes a table entry of each kind takes in a Table.
const (
	sttsRunBytes   = int64(unsafe.Sizeof(sttsRun{}))
	cttsRunBytes   = int64(unsafe.Sizeof(cttsRun{}))
	chunkBytes     = int64(unsafe.Sizeof(int64(0)) + unsafe.Sizeof(uint32(0)))
	tableBaseBytes = int64(unsafe.Sizeof(Table{}))
)

// TableBytes returns the bytes the track's Table takes once ReadTable has
// read it, from the entry counts Open read: 4 bytes a sample for stsz
// sizes (2 for stz2, none when every sample has one size), 12 a chunk, 16
// an stts entry, 4 a sync sample, the smaller of 8 a ctts entry and 4 a
// sample for composition offsets, and the Table itself. A caller bounds the
// memory of the tables it keeps by this before it reads them; reading one
// takes a buffer of 16 KiB more while it runs.
func (t *Track) TableBytes() int64 {
	n := int64(t.Samples)
	b := tableBaseBytes
	switch t.sizes.field {
	case 32:
		b += 4 * n
	case 4, 8, 16:
		b += 2 * n
	}
	b += chunkBytes*int64(max(t.chunks.n, 0)) + sttsRunBytes*int64(max(t.stts.n, 0)) + 4*int64(max(t.stss.n, 0))
	if t.ctts.n >= 0 {
		b += min(cttsRunBytes*int64(t.ctts.n+1), 4*n)
	}
	return b
}

// tableReader reads a table's entries in order through a buffer.
type tableReader struct {
	r        io.ReaderAt
	off, end int64
	buf, b   []byte
}

func (rd *tableReader) reset(off int64, length int64) {
	rd.off, rd.end, rd.b = off, off+length, rd.buf[:0]
}

// next returns the next k bytes of the table, valid until the next call.
func (rd *tableReader) next(k int) ([]byte, error) {
	if len(rd.b) < k {
		n := copy(rd.buf, rd.b)
		want := min(int64(len(rd.buf)-n), rd.end-rd.off)
		if int64(n)+want < int64(k) {
			return nil, ErrMalformed
		}
		m, err := rd.r.ReadAt(rd.buf[n:n+int(want)], rd.off)
		if int64(m) < want {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		rd.off += want
		rd.b = rd.buf[:n+int(want)]
	}
	p := rd.b[:k]
	rd.b = rd.b[k:]
	return p, nil
}

func (rd *tableReader) u32() (uint32, error) {
	p, err := rd.next(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(p), nil
}

// ReadTable reads the track's sample table through r, which must read the
// file Open read, and checks that its tables agree: that stts and stsc
// cover every sample, and that every chunk's samples lie within the file.
// It reads each table once, in order, through a buffer of 16 KiB, and
// allocates TableBytes; it does not read media data. An error from r is
// returned as it is.
func (t *Track) ReadTable(r io.ReaderAt) (*Table, error) {
	tb := &Table{Timescale: t.Timescale, Shift: t.Shift, n: t.Samples, allSync: t.stss.n < 0}
	if t.Samples == 0 {
		return tb, nil
	}
	rd := &tableReader{r: r, buf: make([]byte, readBuffer)}
	if err := tb.readSizes(rd, t); err != nil {
		return nil, err
	}
	if err := tb.readSTTS(rd, t); err != nil {
		return nil, err
	}
	if err := tb.readCTTS(rd, t); err != nil {
		return nil, err
	}
	if err := tb.readSTSS(rd, t); err != nil {
		return nil, err
	}
	if err := tb.readChunks(rd, t); err != nil {
		return nil, err
	}
	return tb, nil
}

func (tb *Table) readSizes(rd *tableReader, t *Track) error {
	n := t.Samples
	rd.reset(t.sizes.off, (int64(n)*int64(t.sizes.field)+7)/8)
	switch t.sizes.field {
	case 0:
		tb.constant = t.sizes.constant
	case 32:
		tb.sizes32 = make([]uint32, n)
		for i := range tb.sizes32 {
			v, err := rd.u32()
			if err != nil {
				return err
			}
			tb.sizes32[i] = v
		}
	case 16:
		tb.sizes16 = make([]uint16, n)
		for i := range tb.sizes16 {
			p, err := rd.next(2)
			if err != nil {
				return err
			}
			tb.sizes16[i] = binary.BigEndian.Uint16(p)
		}
	case 8:
		tb.sizes16 = make([]uint16, n)
		for i := range tb.sizes16 {
			p, err := rd.next(1)
			if err != nil {
				return err
			}
			tb.sizes16[i] = uint16(p[0])
		}
	case 4:
		tb.sizes16 = make([]uint16, n)
		for i := 0; i < n; i += 2 {
			p, err := rd.next(1)
			if err != nil {
				return err
			}
			tb.sizes16[i] = uint16(p[0] >> 4)
			if i+1 < n {
				tb.sizes16[i+1] = uint16(p[0] & 15)
			}
		}
	}
	return nil
}

// size returns sample i's size.
func (tb *Table) size(i int) uint32 {
	switch {
	case tb.sizes32 != nil:
		return tb.sizes32[i]
	case tb.sizes16 != nil:
		return uint32(tb.sizes16[i])
	}
	return tb.constant
}

// sizes returns the sum of the sizes of samples [from, to).
func (tb *Table) sizes(from, to int) uint64 {
	var s uint64
	switch {
	case tb.sizes32 != nil:
		for _, v := range tb.sizes32[from:to] {
			s += uint64(v)
		}
	case tb.sizes16 != nil:
		for _, v := range tb.sizes16[from:to] {
			s += uint64(v)
		}
	default:
		s = uint64(to-from) * uint64(tb.constant)
	}
	return s
}

// readChunks reads the chunk offsets and stsc, and checks that every chunk
// lies within the file.
func (tb *Table) readChunks(rd *tableReader, t *Track) error {
	chunks := t.chunks.n
	tb.chunkOffset = make([]int64, chunks)
	tb.chunkFirst = make([]uint32, chunks)
	rd.reset(t.chunks.off, int64(chunks)*int64(t.chunks.entry))
	for c := range tb.chunkOffset {
		p, err := rd.next(t.chunks.entry)
		if err != nil {
			return err
		}
		var off uint64
		if t.chunks.entry == 8 {
			off = binary.BigEndian.Uint64(p)
		} else {
			off = uint64(binary.BigEndian.Uint32(p))
		}
		if off > uint64(t.fileSize) {
			return ErrSampleData
		}
		tb.chunkOffset[c] = int64(off)
	}

	// Each stsc entry gives the samples a chunk of the chunks from its
	// first_chunk to the next entry's has.
	n := uint64(t.Samples)
	sample, c, perChunk := uint64(0), 0, uint64(0)
	fill := func(to int) {
		for ; c < to && c < chunks; c++ {
			tb.chunkFirst[c] = uint32(min(sample, n))
			sample = min(sample+perChunk, n+1)
		}
	}
	rd.reset(t.stsc.off, int64(t.stsc.n)*12)
	prev := uint32(0)
	for range t.stsc.n {
		p, err := rd.next(12)
		if err != nil {
			return err
		}
		first := binary.BigEndian.Uint32(p)
		if first <= prev {
			return ErrMalformed
		}
		fill(int(min(first-1, uint32(math.MaxInt32))))
		prev, perChunk = first, uint64(binary.BigEndian.Uint32(p[4:]))
	}
	fill(chunks)
	if sample < n {
		return ErrMalformed
	}

	// Every chunk's samples must lie within the file.
	for c := range chunks {
		from, to := int(tb.chunkFirst[c]), t.Samples
		if c+1 < chunks {
			to = int(tb.chunkFirst[c+1])
		}
		size := tb.sizes(from, to)
		if size > uint64(t.fileSize-tb.chunkOffset[c]) {
			return ErrSampleData
		}
		tb.dataSize += int64(size)
	}
	return nil
}

func (tb *Table) readSTTS(rd *tableReader, t *Track) error {
	tb.stts = make([]sttsRun, 0, t.stts.n)
	rd.reset(t.stts.off, int64(t.stts.n)*8)
	n := uint64(t.Samples)
	var sample, dts uint64
	for range t.stts.n {
		p, err := rd.next(8)
		if err != nil {
			return err
		}
		count, duration := uint64(binary.BigEndian.Uint32(p)), binary.BigEndian.Uint32(p[4:])
		if count == 0 || sample >= n {
			continue
		}
		tb.stts = append(tb.stts, sttsRun{first: uint32(sample), duration: duration, dts: int64(dts)})
		count = min(count, n-sample)
		sample += count
		d := count * uint64(duration)
		if d > limit-dts {
			return ErrMalformed
		}
		dts += d
	}
	if sample < n {
		return ErrMalformed
	}
	tb.end = int64(dts)
	return nil
}

func (tb *Table) readCTTS(rd *tableReader, t *Track) error {
	if t.ctts.n < 0 {
		return nil
	}
	n := uint64(t.Samples)
	perSample := 4*int64(n) < cttsRunBytes*int64(t.ctts.n+1)
	if perSample {
		tb.cttsSamples = make([]int32, n)
	} else {
		tb.cttsRuns = make([]cttsRun, 0, t.ctts.n+1)
	}
	rd.reset(t.ctts.off, int64(t.ctts.n)*8)
	var sample uint64
	for range t.ctts.n {
		p, err := rd.next(8)
		if err != nil {
			return err
		}
		// Version 0 offsets are unsigned, but writers put negative ones in
		// them too; like ffmpeg, both versions are read as signed.
		count, offset := uint64(binary.BigEndian.Uint32(p)), int32(binary.BigEndian.Uint32(p[4:]))
		if count == 0 || sample >= n {
			continue
		}
		count = min(count, n-sample)
		tb.minCTS = min(tb.minCTS, offset)
		if perSample {
			for i := range count {
				tb.cttsSamples[sample+i] = offset
			}
		} else {
			tb.cttsRuns = append(tb.cttsRuns, cttsRun{first: uint32(sample), offset: offset})
		}
		sample += count
	}
	if !perSample {
		tb.cttsRuns = append(tb.cttsRuns, cttsRun{first: uint32(sample)})
	}
	return nil
}

func (tb *Table) readSTSS(rd *tableReader, t *Track) error {
	if t.stss.n < 0 {
		return nil
	}
	tb.stss = make([]uint32, 0, t.stss.n)
	rd.reset(t.stss.off, int64(t.stss.n)*4)
	sorted := true
	for range t.stss.n {
		v, err := rd.u32()
		if err != nil {
			return err
		}
		// Sample numbers count from 1.
		if v == 0 || uint64(v) > uint64(t.Samples) {
			continue
		}
		if l := len(tb.stss); l > 0 && tb.stss[l-1] >= v-1 {
			sorted = false
		}
		tb.stss = append(tb.stss, v-1)
	}
	if !sorted {
		slices.Sort(tb.stss)
		tb.stss = slices.Compact(tb.stss)
	}
	return nil
}

// Sample is one sample of a Table.
type Sample struct {
	// Offset and Size are where the sample's data lies in the file.
	Offset int64
	Size   uint32
	// DTS is the decode time in ticks of the media's timescale, from the
	// start of the media: the times ffprobe gives with -ignore_editlist.
	// Table.Time turns it into a presentation time.
	DTS int64
	// CTS is the composition offset: the sample is presented at DTS+CTS.
	CTS int32
	// Duration is the time to the next sample's DTS, from stts.
	Duration uint32
	// Keyframe marks a sync sample: one a decoder can start from.
	Keyframe bool
}

// PTS returns the sample's presentation time in media ticks, DTS + CTS.
func (s Sample) PTS() int64 { return s.DTS + int64(s.CTS) }

// Len returns the number of samples.
func (tb *Table) Len() int { return tb.n }

// Bytes returns the memory the table takes: the Track's TableBytes, or less
// for a track without samples, whose tables ReadTable does not read.
func (tb *Table) Bytes() int64 {
	return tableBaseBytes + 4*int64(cap(tb.sizes32)) + 2*int64(cap(tb.sizes16)) +
		8*int64(cap(tb.chunkOffset)) + 4*int64(cap(tb.chunkFirst)) +
		sttsRunBytes*int64(cap(tb.stts)) + cttsRunBytes*int64(cap(tb.cttsRuns)) +
		4*int64(cap(tb.cttsSamples)) + 4*int64(cap(tb.stss))
}

// MinCTS returns the least composition offset of the samples when it is
// negative (ctts version 1, or ffmpeg's -movflags +negative_cts_offsets), and 0
// otherwise. A muxer that needs every DTS at or before its PTS, as MPEG-TS
// does, decodes the samples -MinCTS ticks earlier, as ffmpeg's reader does.
func (tb *Table) MinCTS() int32 { return tb.minCTS }

// DataSize returns the sum of the samples' sizes.
func (tb *Table) DataSize() int64 { return tb.dataSize }

// End returns the decode time after the last sample, in media ticks: the
// sum of the samples' durations.
func (tb *Table) End() int64 { return tb.end }

// Time returns the presentation time of a media time in ticks, as a player
// takes it after the edit list: FromTicks(ticks + Shift). For a sample,
// Time(s.DTS) and Time(s.PTS()) are the times ffprobe gives.
func (tb *Table) Time(ticks int64) time.Duration {
	return av.FromTicks(ticks+tb.Shift, int(tb.Timescale))
}

// Ticks returns the media time of a presentation time, the inverse of Time
// rounded to the nearest tick.
func (tb *Table) Ticks(d time.Duration) int64 {
	return av.ToTicks(d, int(tb.Timescale)) - tb.Shift
}

// chunkOf returns the chunk of sample i: the last whose first sample is at
// or before i.
func (tb *Table) chunkOf(i int) int {
	return sort.Search(len(tb.chunkFirst), func(c int) bool { return int(tb.chunkFirst[c]) > i }) - 1
}

// sttsOf returns the stts run of sample i.
func (tb *Table) sttsOf(i int) int {
	return sort.Search(len(tb.stts), func(r int) bool { return int(tb.stts[r].first) > i }) - 1
}

// cttsOf returns the ctts run of sample i, -1 before the first.
func (tb *Table) cttsOf(i int) int {
	return sort.Search(len(tb.cttsRuns), func(r int) bool { return int(tb.cttsRuns[r].first) > i }) - 1
}

// isSync reports whether sample i is a sync sample.
func (tb *Table) isSync(i int) bool {
	if tb.allSync {
		return true
	}
	_, ok := slices.BinarySearch(tb.stss, uint32(i))
	return ok
}

// cts returns sample i's composition offset.
func (tb *Table) cts(i int) int32 {
	if tb.cttsSamples != nil {
		return tb.cttsSamples[i]
	}
	if r := tb.cttsOf(i); r >= 0 {
		return tb.cttsRuns[r].offset
	}
	return 0
}

// Sample returns sample i, for i in [0, Len()), in O(log n) of the tables'
// entries and the samples of one chunk. Samples walks them in order at a
// constant cost each.
func (tb *Table) Sample(i int) Sample {
	if i < 0 || i >= tb.n {
		panic("mp4: sample index out of range")
	}
	c := tb.chunkOf(i)
	r := tb.stts[tb.sttsOf(i)]
	return Sample{
		Offset:   tb.chunkOffset[c] + int64(tb.sizes(int(tb.chunkFirst[c]), i)),
		Size:     tb.size(i),
		DTS:      r.dts + int64(i-int(r.first))*int64(r.duration),
		CTS:      tb.cts(i),
		Duration: r.duration,
		Keyframe: tb.isSync(i),
	}
}

// Samples yields the samples from index from to the last, in decode order,
// with their indexes.
func (tb *Table) Samples(from int) iter.Seq2[int, Sample] {
	return func(yield func(int, Sample) bool) {
		from = max(from, 0)
		if from >= tb.n {
			return
		}
		s := tb.Sample(from)
		c := tb.chunkOf(from)
		r := tb.sttsOf(from)
		k := 0 // the next sync sample in stss
		if !tb.allSync {
			k, _ = slices.BinarySearch(tb.stss, uint32(from))
		}
		cr := tb.cttsOf(from)
		for i := from; ; {
			if !yield(i, s) {
				return
			}
			if i++; i == tb.n {
				return
			}
			s.Offset += int64(s.Size)
			for c+1 < len(tb.chunkFirst) && int(tb.chunkFirst[c+1]) <= i {
				c++
				s.Offset = tb.chunkOffset[c]
			}
			s.Size = tb.size(i)
			s.DTS += int64(s.Duration)
			if r+1 < len(tb.stts) && int(tb.stts[r+1].first) == i {
				r++
				s.Duration = tb.stts[r].duration
			}
			if tb.cttsSamples != nil {
				s.CTS = tb.cttsSamples[i]
			} else if cr+1 < len(tb.cttsRuns) && int(tb.cttsRuns[cr+1].first) == i {
				cr++
				s.CTS = tb.cttsRuns[cr].offset
			}
			if tb.allSync {
				s.Keyframe = true
			} else {
				if k < len(tb.stss) && tb.stss[k] < uint32(i) {
					k++
				}
				s.Keyframe = k < len(tb.stss) && tb.stss[k] == uint32(i)
			}
		}
	}
}

// Keyframes returns the number of sync samples.
func (tb *Table) Keyframes() int {
	if tb.allSync {
		return tb.n
	}
	return len(tb.stss)
}

// Keyframe returns the index of sync sample k, for k in [0, Keyframes()).
func (tb *Table) Keyframe(k int) int {
	if tb.allSync {
		if k < 0 || k >= tb.n {
			panic("mp4: keyframe index out of range")
		}
		return k
	}
	return int(tb.stss[k])
}

// KeyframeBefore returns the index of the last sync sample at or before
// sample i, or -1 if there is none.
func (tb *Table) KeyframeBefore(i int) int {
	if i < 0 || tb.n == 0 {
		return -1
	}
	i = min(i, tb.n-1)
	if tb.allSync {
		return i
	}
	k := sort.Search(len(tb.stss), func(k int) bool { return int(tb.stss[k]) > i }) - 1
	if k < 0 {
		return -1
	}
	return int(tb.stss[k])
}

// KeyframeAfter returns the index of the first sync sample at or after
// sample i, or -1 if there is none.
func (tb *Table) KeyframeAfter(i int) int {
	i = max(i, 0)
	if i >= tb.n {
		return -1
	}
	if tb.allSync {
		return i
	}
	k := sort.Search(len(tb.stss), func(k int) bool { return int(tb.stss[k]) >= i })
	if k == len(tb.stss) {
		return -1
	}
	return int(tb.stss[k])
}

// IndexAt returns the index of the last sample whose decode time, as a
// presentation time (Time(s.DTS)), is at or before d, or -1 if d is before
// the first sample.
func (tb *Table) IndexAt(d time.Duration) int {
	if tb.n == 0 {
		return -1
	}
	ticks := tb.Ticks(d)
	r := sort.Search(len(tb.stts), func(r int) bool { return tb.stts[r].dts > ticks }) - 1
	if r < 0 {
		return -1
	}
	run := tb.stts[r]
	last := tb.n - 1
	if r+1 < len(tb.stts) {
		last = int(tb.stts[r+1].first) - 1
	}
	if run.duration == 0 {
		return last
	}
	return int(min(int64(run.first)+(ticks-run.dts)/int64(run.duration), int64(last)))
}

// SeekKeyframe returns the index of the sync sample a player seeking to d
// starts from: the last whose presentation time is at or before d, or the
// first if all are after d; -1 if the track has none. Sync samples are
// taken to be presented in decode order, as they are in every codec fib
// carries.
func (tb *Table) SeekKeyframe(d time.Duration) int {
	n := tb.Keyframes()
	if n == 0 {
		return -1
	}
	ticks := tb.Ticks(d)
	k := sort.Search(n, func(k int) bool {
		i := tb.Keyframe(k)
		return tb.dts(i)+int64(tb.cts(i)) > ticks
	}) - 1
	return tb.Keyframe(max(k, 0))
}

// dts returns sample i's decode time.
func (tb *Table) dts(i int) int64 {
	r := tb.stts[tb.sttsOf(i)]
	return r.dts + int64(i-int(r.first))*int64(r.duration)
}

// ReadSample reads sample s's data through r into buf, grown if it is too
// small, and returns it.
func ReadSample(r io.ReaderAt, s Sample, buf []byte) ([]byte, error) {
	buf = slices.Grow(buf[:0], int(s.Size))[:s.Size]
	n, err := r.ReadAt(buf, s.Offset)
	if n == len(buf) {
		return buf, nil
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return nil, err
}
