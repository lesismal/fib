package mpegts

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/opus"
)

// capture is what a Demuxer gave back.
type capture struct {
	tracks  [][]*av.Track
	packets []*av.Packet
	codecs  []av.CodecID // the codec of each packet, as announced then
}

func (c *capture) config() DemuxerConfig {
	return DemuxerConfig{
		OnTracks: func(t []*av.Track) { c.tracks = append(c.tracks, t) },
		OnPacket: func(p *av.Packet) {
			c.packets = append(c.packets, p)
			c.codecs = append(c.codecs, c.last()[p.Track].Codec)
		},
	}
}

func (c *capture) last() []*av.Track {
	if len(c.tracks) == 0 {
		return nil
	}
	return c.tracks[len(c.tracks)-1]
}

func (c *capture) count(track int) int {
	n := 0
	for _, p := range c.packets {
		if p.Track == track {
			n++
		}
	}
	return n
}

func (c *capture) countCodec(codec av.CodecID) int {
	n := 0
	for _, x := range c.codecs {
		if x == codec {
			n++
		}
	}
	return n
}

func demux(t testing.TB, b []byte, chunk int) *capture {
	t.Helper()
	var c capture
	d := NewDemuxer(c.config())
	for len(b) > 0 {
		n := min(chunk, len(b))
		d.Write(b[:n])
		b = b[n:]
	}
	d.Flush()
	return &c
}

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mux(t testing.TB, c *capture, cfg MuxerConfig) []byte {
	t.Helper()
	m, err := NewMuxer(c.last(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	for _, p := range c.packets {
		if out, err = m.WritePacket(out, p); err != nil {
			t.Fatal(err)
		}
	}
	return m.Flush(out)
}

// TestFixtures demuxes what ffmpeg muxed: 1 s of 25 fps video with B-frames
// and audio.
func TestFixtures(t *testing.T) {
	cases := []struct {
		file   string
		codecs []av.CodecID
		counts []int
	}{
		{"h264_aac.ts", []av.CodecID{av.CodecH264, av.CodecAAC}, []int{25, 48}},
		{"h265_opus.ts", []av.CodecID{av.CodecH265, av.CodecOpus}, []int{25, 51}},
		{"aac_only.ts", []av.CodecID{av.CodecAAC}, []int{45}},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			c := demux(t, fixture(t, tc.file), 1000)
			if len(c.tracks) != 1 {
				t.Fatalf("OnTracks %d times", len(c.tracks))
			}
			tracks := c.last()
			if len(tracks) != len(tc.codecs) {
				t.Fatalf("%d tracks", len(tracks))
			}
			for i, codec := range tc.codecs {
				if tracks[i].Codec != codec {
					t.Fatalf("track %d codec %v", i, tracks[i].Codec)
				}
				if n := c.count(i); n != tc.counts[i] {
					t.Fatalf("track %d: %d packets, want %d", i, n, tc.counts[i])
				}
				if codec.IsVideo() && (tracks[i].Width != 64 || tracks[i].Height != 48) {
					t.Fatalf("size %dx%d", tracks[i].Width, tracks[i].Height)
				}
			}
			keys := 0
			var lastDTS = map[int]time.Duration{}
			for _, p := range c.packets {
				if p.Keyframe && tracks[p.Track].Codec.IsVideo() {
					keys++
				}
				if p.PTS < p.DTS {
					t.Fatalf("PTS %v before DTS %v", p.PTS, p.DTS)
				}
				if l, ok := lastDTS[p.Track]; ok && p.DTS <= l {
					t.Fatalf("track %d DTS %v after %v", p.Track, p.DTS, l)
				}
				lastDTS[p.Track] = p.DTS
				if tracks[p.Track].Codec.IsVideo() && !h264.ValidAVCC(p.Data, 4) {
					t.Fatal("video not AVCC")
				}
			}
			if tc.codecs[0].IsVideo() && keys != 3 {
				t.Fatalf("%d keyframes, want 3 (GOP of 10)", keys)
			}
			if c.packets[0].DTS < 0 || c.packets[0].DTS > 100*time.Millisecond {
				t.Fatalf("first DTS %v", c.packets[0].DTS)
			}
		})
	}
}

// TestRoundTrip muxes what it demuxed and demuxes it again: the same
// tracks and packets come back, whatever the chunking and grouping.
func TestRoundTrip(t *testing.T) {
	for _, file := range []string{"h264_aac.ts", "h265_opus.ts", "aac_only.ts"} {
		for _, group := range []int{1, 4} {
			src := demux(t, fixture(t, file), 7)
			ts := mux(t, src, MuxerConfig{AudioFramesPerPES: group})
			if len(ts)%PacketSize != 0 {
				t.Fatalf("output of %d bytes", len(ts))
			}
			for i := 0; i < len(ts); i += PacketSize {
				if ts[i] != syncByte {
					t.Fatalf("no sync byte at %d", i)
				}
			}
			back := demux(t, ts, 1+rand.IntN(500))
			comparePackets(t, src, back)
		}
	}
}

func comparePackets(t *testing.T, a, b *capture) {
	t.Helper()
	ta, tb := a.last(), b.last()
	if len(ta) != len(tb) {
		t.Fatalf("%d tracks, then %d", len(ta), len(tb))
	}
	for i := range ta {
		if ta[i].Codec != tb[i].Codec || !bytes.Equal(ta[i].Config, tb[i].Config) {
			t.Fatalf("track %d: %v %x, then %v %x", i, ta[i].Codec, ta[i].Config, tb[i].Codec, tb[i].Config)
		}
	}
	if len(a.packets) != len(b.packets) {
		t.Fatalf("%d packets, then %d", len(a.packets), len(b.packets))
	}
	// Packets of different tracks may come back in another order; compare
	// track by track.
	byTrack := func(c *capture) map[int][]*av.Packet {
		m := map[int][]*av.Packet{}
		for _, p := range c.packets {
			m[p.Track] = append(m[p.Track], p)
		}
		return m
	}
	pa, pb := byTrack(a), byTrack(b)
	shift := a.packets[0].DTS - b.packets[0].DTS
	for tr, ps := range pa {
		for i, p := range ps {
			q := pb[tr][i]
			if p.DTS-q.DTS != shift && p.DTS-q.DTS != 0 || p.PTS-p.DTS != q.PTS-q.DTS {
				t.Fatalf("track %d packet %d: DTS %v PTS %v, then %v %v", tr, i, p.DTS, p.PTS, q.DTS, q.PTS)
			}
			if p.Keyframe != q.Keyframe || !bytes.Equal(p.Data, q.Data) {
				t.Fatalf("track %d packet %d differs", tr, i)
			}
		}
	}
}

// TestGarbage resynchronises after bytes that are not packets.
func TestGarbage(t *testing.T) {
	src := fixture(t, "h264_aac.ts")
	clean := demux(t, src, len(src))
	var dirty []byte
	dirty = append(dirty, 0x47, 1, 2, 3, 0x47)
	dirty = append(dirty, src[:PacketSize*20]...)
	dirty = append(dirty, bytes.Repeat([]byte{0x47}, 3)...)
	dirty = append(dirty, src[PacketSize*20:]...)
	got := demux(t, dirty, 333)
	// The junk lands between two packets, so nothing is lost.
	comparePackets(t, clean, got)
}

// TestLoss drops a packet of the video PID: the access unit it was part of
// is dropped, and nothing else.
func TestLoss(t *testing.T) {
	src := fixture(t, "h264_aac.ts")
	clean := demux(t, src, len(src))
	var lossy []byte
	dropped := false
	videoSeen := 0
	for i := 0; i < len(src); i += PacketSize {
		p := src[i : i+PacketSize]
		pid := binary.BigEndian.Uint16(p[1:]) & 0x1fff
		if pid == 0x100 {
			videoSeen++
			if videoSeen >= 30 && p[1]&0x40 == 0 && !dropped {
				dropped = true
				continue
			}
		}
		lossy = append(lossy, p...)
	}
	got := demux(t, lossy, len(lossy))
	if got.count(0) != clean.count(0)-1 || got.count(1) != clean.count(1) {
		t.Fatalf("video %d audio %d, from %d %d", got.count(0), got.count(1), clean.count(0), clean.count(1))
	}
}

// TestDuplicate ignores a packet sent twice, as ISO/IEC 13818-1 allows.
func TestDuplicate(t *testing.T) {
	src := fixture(t, "h264_aac.ts")
	clean := demux(t, src, len(src))
	var dup []byte
	for i := 0; i < len(src); i += PacketSize {
		dup = append(dup, src[i:i+PacketSize]...)
		if i/PacketSize%5 == 3 {
			dup = append(dup, src[i:i+PacketSize]...)
		}
	}
	comparePackets(t, clean, demux(t, dup, 1000))
}

func h264Track(t testing.TB) *av.Track {
	t.Helper()
	c := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	return c.last()[0]
}

func aacTrack() *av.Track {
	cfg := aac.Config{ObjectType: aac.ObjectTypeLC, SampleRate: 48000, ChannelConfig: 2}
	b, _ := cfg.Append(nil)
	return &av.Track{Codec: av.CodecAAC, Config: b, SampleRate: 48000, Channels: 2}
}

// packetsOf splits a transport stream into its packets by PID.
func packetsOf(ts []byte) map[uint16][][]byte {
	m := map[uint16][][]byte{}
	for i := 0; i+PacketSize <= len(ts); i += PacketSize {
		p := ts[i : i+PacketSize]
		pid := binary.BigEndian.Uint16(p[1:]) & 0x1fff
		m[pid] = append(m[pid], p)
	}
	return m
}

// TestMuxerLayout checks the tables, the PCR, the random access indicator
// and the stuffing of what the muxer writes.
func TestMuxerLayout(t *testing.T) {
	vt := h264Track(t)
	m, err := NewMuxer([]*av.Track{vt, {Codec: av.CodecPCMU}, aacTrack()}, MuxerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !m.Supported(0) || m.Supported(1) || !m.Supported(2) || m.Supported(3) {
		t.Fatal("Supported")
	}
	if m.PID(0) != 0x100 || m.PID(2) != 0x101 || m.PID(1) != 0 {
		t.Fatal("PIDs", m.PID(0), m.PID(2))
	}
	src := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	key := src.packets[0]
	if !key.Keyframe {
		t.Fatal("first packet not a keyframe")
	}
	// The fixture's keyframes carry no parameter sets (the demuxer strips
	// them): the muxer must add them.
	out, err := m.WritePacket(nil, &av.Packet{Track: 0, Keyframe: true, Data: key.Data})
	if err != nil {
		t.Fatal(err)
	}
	pk := packetsOf(out)
	if len(pk[0]) != 1 || len(pk[DefaultPMTPID]) != 1 {
		t.Fatal("tables not written first")
	}
	pat := pk[0][0]
	if crc32MPEG(pat[5:5+3+int(pat[7])]) != 0 {
		t.Fatal("PAT CRC")
	}
	first := pk[0x100][0]
	if first[1]&0x40 == 0 || first[3]&0x20 == 0 || first[5]&0x50 != 0x50 {
		t.Fatalf("first video packet %x: want PUSI, adaptation field, PCR and RAI", first[:12])
	}
	// The parameter sets went in band.
	var es []byte
	for _, p := range pk[0x100] {
		pl := p[4:]
		if p[3]&0x20 != 0 {
			pl = pl[1+int(pl[0]):]
		}
		es = append(es, pl...)
	}
	types := []h264.NALUType{}
	for n := range h264.SplitAnnexB(es[9+int(es[8]):]) {
		types = append(types, h264.Type(n))
	}
	if len(types) < 4 || types[0] != h264.NALAUD || types[1] != h264.NALSPS || types[2] != h264.NALPPS {
		t.Fatalf("NAL units %v", types)
	}

	// A packet that is not AVCC.
	if _, err := m.WritePacket(nil, &av.Packet{Track: 0, Data: []byte{0, 0, 0, 9, 1}}); !errors.Is(err, ErrBadData) {
		t.Fatal(err)
	}
	if _, err := m.WritePacket(nil, &av.Packet{Track: 1}); !errors.Is(err, ErrTrack) {
		t.Fatal(err)
	}
	// Audio 30 ms later is not due tables, but a PCR is due, and goes on
	// the video PID in a packet of its own.
	out, _ = m.WritePacket(nil, &av.Packet{Track: 2, DTS: 50 * time.Millisecond, PTS: 50 * time.Millisecond, Keyframe: true, Data: make([]byte, 20)})
	pk = packetsOf(out)
	if len(pk[0]) != 0 || len(pk[0x100]) != 1 || len(pk[0x101]) != 1 {
		t.Fatalf("PIDs %v", len(pk))
	}
	pcr := pk[0x100][0]
	if pcr[3]&0x30 != 0x20 || pcr[4] != 183 || pcr[5]&0x10 == 0 {
		t.Fatalf("PCR packet %x", pcr[:12])
	}
	// 200 ms on, the tables are due again.
	out, _ = m.WritePacket(nil, &av.Packet{Track: 2, DTS: 300 * time.Millisecond, PTS: 300 * time.Millisecond, Data: make([]byte, 20)})
	if len(packetsOf(out)[0]) != 1 {
		t.Fatal("tables not repeated")
	}
	// WriteTables right after a packet writes them; twice in a row only
	// once.
	out = m.WriteTables(nil)
	out = m.WriteTables(out)
	if len(out) != 2*PacketSize {
		t.Fatalf("WriteTables wrote %d bytes", len(out))
	}
	m.ResetContinuity()
	if m.patCC != 0 || m.order[0].cc != 0 {
		t.Fatal("ResetContinuity")
	}
}

// TestPCRContinuity checks that a PCR-only packet repeats the counter of the
// packet before it, as ffmpeg's demuxer checks.
func TestPCRContinuity(t *testing.T) {
	src := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	m, err := NewMuxer(src.last(), MuxerConfig{PCRInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	for _, p := range src.packets {
		if out, err = m.WritePacket(out, p); err != nil {
			t.Fatal(err)
		}
	}
	for pid, ps := range packetsOf(out) {
		last := -1
		for _, p := range ps {
			cc := int(p[3] & 0x0f)
			payload := p[3]&0x10 != 0
			switch {
			case last < 0:
			case payload && cc != (last+1)&0x0f:
				t.Fatalf("PID %#x: counter %d after %d", pid, cc, last)
			case !payload && cc != last:
				t.Fatalf("PID %#x: payloadless counter %d after %d", pid, cc, last)
			}
			last = cc
		}
	}
	// And the whole demuxes cleanly.
	comparePackets(t, src, demux(t, out, 4096))
}

func TestMuxerErrors(t *testing.T) {
	if _, err := NewMuxer([]*av.Track{nil, {Codec: av.CodecPCMA}, {Codec: av.CodecAAC}}, MuxerConfig{}); !errors.Is(err, ErrNoTracks) {
		t.Fatal(err)
	}
	var many []*av.Track
	for range 40 {
		many = append(many, &av.Track{Codec: av.CodecOpus})
	}
	if _, err := NewMuxer(many, MuxerConfig{}); !errors.Is(err, ErrTooManyTracks) {
		t.Fatal(err)
	}
	// An AAC track without config but with a rate and channels gets one.
	m, err := NewMuxer([]*av.Track{{Codec: av.CodecAAC, SampleRate: 44100, Channels: 2}}, MuxerConfig{TimeOffset: -1, PCRLead: -1, TablesInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	out, err := m.WritePacket(nil, &av.Packet{Data: make([]byte, 5000)})
	if err != nil {
		t.Fatal(err)
	}
	c := demux(t, out, len(out))
	if len(c.packets) != 1 || len(c.packets[0].Data) != 5000 || c.last()[0].SampleRate != 44100 {
		t.Fatal("AAC without config")
	}
	// An ADTS frame cannot carry more than 8191 bytes.
	if _, err := m.WritePacket(nil, &av.Packet{Data: make([]byte, 9000)}); err == nil {
		t.Fatal("oversized ADTS frame")
	}
}

// TestTimeline checks offsets, the PCR lead and 33-bit wrapping: a stream
// that crosses 2^33 ticks keeps rising.
func TestTimeline(t *testing.T) {
	m, err := NewMuxer([]*av.Track{aacTrack()}, MuxerConfig{TimeOffset: time.Second, PCRLead: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	wrap := av.FromTicks(1<<33, clock90k)
	var out []byte
	start := wrap - 5*time.Second
	for i := range 400 {
		d := start + time.Duration(i)*25*time.Millisecond
		out, _ = m.WritePacket(out, &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{1, 2, 3}})
	}
	c := demux(t, out, 188*3)
	if len(c.packets) != 400 {
		t.Fatalf("%d packets", len(c.packets))
	}
	for i, p := range c.packets {
		if want := time.Duration(i) * 25 * time.Millisecond; p.DTS-want > time.Millisecond || want-p.DTS > time.Millisecond {
			t.Fatalf("packet %d at %v, want %v", i, p.DTS, want)
		}
	}
	// The first PCR is the DTS less the lead.
	p := packetsOf(out)[0x100][0]
	base := int64(p[6])<<25 | int64(p[7])<<17 | int64(p[8])<<9 | int64(p[9])<<1 | int64(p[10]>>7)
	want := (av.ToTicks(start+time.Second-100*time.Millisecond, clock90k)) & tsMask
	if base != want {
		t.Fatalf("PCR base %d, want %d", base, want)
	}
}

// TestPCRMonotonic: a PES whose DTS is a little behind the last PCR's does
// not move the PCR back; a jump back of more than a second is a new
// timeline and does.
func TestPCRMonotonic(t *testing.T) {
	m, _ := NewMuxer([]*av.Track{aacTrack()}, MuxerConfig{PCRInterval: time.Millisecond, TimeOffset: 10 * time.Second})
	a := m.nextPCR(90000)
	if b := m.nextPCR(89000); b != a {
		t.Fatalf("PCR back from %d to %d", a, b)
	}
	if b := m.nextPCR(90000 - 3*clock90k); b == a {
		t.Fatal("PCR did not follow a jump back")
	}
}

func opusHead(t testing.TB, h opus.Head) []byte {
	b, err := h.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestOpus checks the channel configuration codes and the control header.
func TestOpus(t *testing.T) {
	cases := []struct {
		track *av.Track
		code  byte
		chans int
	}{
		{&av.Track{Codec: av.CodecOpus}, 2, 2},
		{&av.Track{Codec: av.CodecOpus, Channels: 1}, 1, 1},
		{&av.Track{Codec: av.CodecOpus, Channels: 6}, 0xff, 2},
		{&av.Track{Codec: av.CodecOpus, Config: opusHead(t, opus.Head{Channels: 2, PreSkip: 0})}, 2, 2},
		{&av.Track{Codec: av.CodecOpus, Config: opusHead(t, opus.Head{Channels: 6, PreSkip: 100, MappingFamily: 1, StreamCount: 4, CoupledCount: 2, ChannelMapping: []byte{0, 4, 1, 2, 3, 5}})}, 6, 6},
		{&av.Track{Codec: av.CodecOpus, Config: opusHead(t, opus.Head{Channels: 2, MappingFamily: 1, StreamCount: 2, CoupledCount: 0, ChannelMapping: []byte{0, 1}})}, 0xff, 2},
	}
	for i, tc := range cases {
		if got := opusChannelCode(tc.track); got != tc.code {
			t.Fatalf("case %d: code %#x, want %#x", i, got, tc.code)
		}
		m, err := NewMuxer([]*av.Track{tc.track}, MuxerConfig{AudioFramesPerPES: 3})
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		// 20 ms CELT packets of 300 bytes, which take two size bytes.
		pkt := append([]byte{0xfc}, bytes.Repeat([]byte{7}, 299)...)
		for j := range 7 {
			d := time.Duration(j) * 20 * time.Millisecond
			out, _ = m.WritePacket(out, &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: pkt})
		}
		out = m.Flush(out)
		c := demux(t, out, len(out))
		if len(c.packets) != 7 || c.last()[0].Channels != tc.chans {
			t.Fatalf("case %d: %d packets, %d channels", i, len(c.packets), c.last()[0].Channels)
		}
		for j, p := range c.packets {
			if !bytes.Equal(p.Data, pkt) || p.DTS != time.Duration(j)*20*time.Millisecond {
				t.Fatalf("case %d packet %d at %v", i, j, p.DTS)
			}
		}
		var h opus.Head
		if err := h.Unmarshal(c.last()[0].Config); err != nil {
			t.Fatal(err)
		}
		if tc.track.Config != nil {
			var src opus.Head
			_ = src.Unmarshal(tc.track.Config)
			if src.PreSkip > 0 && h.PreSkip != src.PreSkip {
				t.Fatalf("case %d: pre-skip %d", i, h.PreSkip)
			}
		}
	}
}

// TestOpusControlHeaders reads control headers with trims and extensions,
// and stops at a malformed one.
func TestOpusControlHeaders(t *testing.T) {
	var c capture
	d := NewDemuxer(c.config())
	s := &demuxStream{codec: av.CodecOpus, opusCode: 2, idx: -1}
	d.order = []*demuxStream{s}
	pkt := []byte{0xf8, 1, 2}
	var es []byte
	es = append(es, 0x7f, 0xe0|0x10|0x08|0x04, 3, 0, 50, 0, 10, 2, 9, 9)
	es = append(es, pkt...)
	es = append(es, 0x7f, 0xe0, 3)
	es = append(es, 0xff, 1, 2) // a packet with a bad TOC still passes
	es = append(es, 0x7f, 0xe0, 200)
	d.opusPackets(s, 0, es)
	if len(c.packets) != 2 || !bytes.Equal(c.packets[0].Data, pkt) {
		t.Fatalf("%d packets", len(c.packets))
	}
	var h opus.Head
	_ = h.Unmarshal(c.last()[0].Config)
	if h.PreSkip != 50 {
		t.Fatalf("pre-skip %d", h.PreSkip)
	}
	for _, bad := range [][]byte{
		{0x7f, 0xf0, 3},       // trim cut short
		{0x7f, 0xe4, 1},       // extension cut short
		{0x7f, 0xe0, 0},       // empty
		{0x00, 0xe0, 1, 1},    // no prefix
		{0x7f, 0xe0, 0xff, 1}, // longer than the data
	} {
		n := len(c.packets)
		d.opusPackets(s, 0, bad)
		if len(c.packets) != n {
			t.Fatalf("% x gave a packet", bad)
		}
	}
	// Channel codes the demuxer maps.
	for code, chans := range map[byte]uint8{0: 2, 1: 1, 2: 2, 5: 5, 8: 8, 0x81: 2, 0xff: 2} {
		var h opus.Head
		if err := h.Unmarshal(opusTrack(code, 0).Config); err != nil || h.Channels != chans {
			t.Fatalf("code %#x: %d channels, %v", code, h.Channels, err)
		}
	}
}

// TestPMTUpdate switches the program from H.264 + AAC to AAC alone and back:
// the tracks are announced again each time.
func TestPMTUpdate(t *testing.T) {
	src := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	vt, at := src.last()[0], src.last()[1]
	both, _ := NewMuxer([]*av.Track{vt, at}, MuxerConfig{})
	// The same PIDs, the PMT listing only the audio.
	audio, _ := NewMuxer([]*av.Track{at}, MuxerConfig{FirstPID: 0x101})
	audio.tables[len(audio.tables)-4-5-12+5] |= 1 << 1 // a new version
	audio.tables = finishTablesAgain(audio.tables)
	var out []byte
	for _, p := range src.packets[:20] {
		out, _ = both.WritePacket(out, p)
	}
	for _, p := range src.packets[20:] {
		if p.Track == 1 {
			q := *p
			q.Track = 0
			out, _ = audio.WritePacket(out, &q)
		}
	}
	c := demux(t, out, 1000)
	if len(c.tracks) != 2 || len(c.tracks[1]) != 1 || c.tracks[1][0].Codec != av.CodecAAC {
		t.Fatalf("tracks %v", c.tracks)
	}
}

// finishTablesAgain recomputes the PMT CRC of a muxer's tables after a
// test changed them.
func finishTablesAgain(tables []byte) []byte {
	patLen := int(tables[1]&0x0f)<<8 | int(tables[2]) + 3
	pmt := tables[patLen : len(tables)-4]
	pmt[1] &^= 0x0f
	pmt[2] = 0
	return append(tables[:patLen:patLen], finishSection(append([]byte(nil), pmt...))...)
}

// TestPending: a track that never gets its configuration is left out once
// MaxPending packets wait, and joins with a new OnTracks when it does.
func TestPending(t *testing.T) {
	src := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	m, _ := NewMuxer(src.last(), MuxerConfig{})
	var out []byte
	// Audio first, video only after 20 audio frames.
	var audio, video []*av.Packet
	for _, p := range src.packets {
		if p.Track == 1 {
			audio = append(audio, p)
		} else {
			video = append(video, p)
		}
	}
	for _, p := range audio[:20] {
		out, _ = m.WritePacket(out, p)
	}
	for _, p := range video {
		out, _ = m.WritePacket(out, p)
	}
	var c capture
	cfg := c.config()
	cfg.MaxPending = 10
	d := NewDemuxer(cfg)
	d.Write(out)
	d.Flush()
	if len(c.tracks) != 2 || len(c.tracks[0]) != 1 || c.tracks[0][0].Codec != av.CodecAAC || len(c.tracks[1]) != 2 {
		t.Fatalf("tracks %d", len(c.tracks))
	}
	if len(d.Tracks()) != 2 {
		t.Fatal("Tracks")
	}
	if c.countCodec(av.CodecAAC) != 20 {
		t.Fatalf("%d audio packets, want 20", c.countCodec(av.CodecAAC))
	}
}

// TestConfigChange: new parameter sets mid-stream make a new track.
func TestConfigChange(t *testing.T) {
	h264src := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	vt := h264src.last()[0]
	var avc h264.AVCConfig
	if err := avc.Unmarshal(vt.Config); err != nil {
		t.Fatal(err)
	}
	// The same SPS with another level, a different configuration.
	sps2 := append([]byte(nil), avc.SPS[0]...)
	sps2[3] = 0x1f
	var out []byte
	m, _ := NewMuxer([]*av.Track{vt}, MuxerConfig{})
	for i, p := range h264src.packets {
		if p.Track != 0 {
			continue
		}
		q := *p
		if i > 30 && q.Keyframe {
			q.Data = h264.AppendAVCC(h264.AppendAVCC(nil, sps2), avc.PPS[0])
			q.Data = append(q.Data, p.Data...)
		}
		out, _ = m.WritePacket(out, &q)
	}
	c := demux(t, out, 4000)
	if len(c.tracks) != 2 || c.tracks[1][0].Config[3] != 0x1f {
		t.Fatalf("OnTracks %d times", len(c.tracks))
	}
}

// TestMaxPESBuffers checks that a PMT listing some two hundred streams,
// each sent a PES that never ends, cannot have the demuxer hold more than
// MaxPESBuffers in all, and that the buffers of streams a new PMT drops
// stop counting.
func TestMaxPESBuffers(t *testing.T) {
	const streams, maxPES, maxBufs = 200, 64 << 10, 256 << 10
	var c capture
	cfg := c.config()
	cfg.MaxPESSize, cfg.MaxPESBuffers = maxPES, maxBufs
	d := NewDemuxer(cfg)
	m, _ := NewMuxer([]*av.Track{aacTrack()}, MuxerConfig{})
	patLen := int(m.tables[1]&0x0f)<<8 | int(m.tables[2]) + 3
	d.Write(tsPacket(0, true, 0, append([]byte{0}, m.tables[:patLen]...)))
	sec := []byte{2, 0xb0, 0, 0, 1, 0xc3, 0, 0, 0xe1, 0x00, 0xf0, 0x00}
	for i := range streams {
		pid := 0x100 + i
		sec = append(sec, StreamTypeH264, 0xe0|byte(pid>>8), byte(pid), 0xf0, 0)
	}
	pmt := append([]byte{0}, finishSectionRaw(sec)...)
	for cc := byte(0); len(pmt) > 0; cc++ {
		n := min(len(pmt), 184)
		d.Write(tsPacket(DefaultPMTPID, cc == 0, cc, pmt[:n]))
		pmt = pmt[n:]
	}
	if len(d.order) != streams {
		t.Fatalf("%d streams", len(d.order))
	}
	start := []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0x80, 5, 0, 0, 0, 0, 0}
	putTS(start[9:], 0x20, 9000)
	fill := bytes.Repeat([]byte{0xff}, 184)
	ccs := make([]byte, streams)
	for i := range streams {
		pid := uint16(0x100 + i)
		d.Write(tsPacket(pid, true, ccs[i], start))
		for range maxPES / 184 {
			ccs[i]++
			d.Write(tsPacket(pid, false, ccs[i]&0x0f, fill))
		}
		ccs[i]++
		held := 0
		for _, s := range d.order {
			held += cap(s.pes)
		}
		if held != d.pesBufs || held > maxBufs {
			t.Fatalf("after stream %d: buffers of %d bytes, counted %d", i, held, d.pesBufs)
		}
	}
	if d.pesBufs < maxBufs-maxPES {
		t.Fatalf("buffers of %d bytes: the limit was never reached", d.pesBufs)
	}
	// A new PMT that keeps one stream lets the others' buffers go.
	sec = append(sec[:12], StreamTypeH264, 0xe1, 0x00+streams-1, 0xf0, 0)
	sec[5] = 0xc5
	d.Write(tsPacket(DefaultPMTPID, true, 0, append([]byte{0}, finishSectionRaw(sec)...)))
	if len(d.order) != 1 || d.pesBufs > maxPES {
		t.Fatalf("%d streams, %d bytes of buffers", len(d.order), d.pesBufs)
	}
}

// TestMaxPESSize drops an access unit larger than the bound.
func TestMaxPESSize(t *testing.T) {
	src := demux(t, fixture(t, "h264_aac.ts"), 1<<20)
	ts := mux(t, src, MuxerConfig{})
	var c capture
	cfg := c.config()
	cfg.MaxPESSize = 600
	d := NewDemuxer(cfg)
	d.Write(ts)
	d.Flush()
	big := 0
	for _, p := range src.packets {
		if p.Track == 0 && len(p.Data) > 500 {
			big++
		}
	}
	if n := c.countCodec(av.CodecH264); big == 0 || n > src.count(0)-big {
		t.Fatalf("%d video packets, %d of %d too big", n, big, src.count(0))
	}
}

// TestPSIEdges feeds sections split over packets, several in one packet,
// and malformed ones.
func TestPSIEdges(t *testing.T) {
	var c capture
	d := NewDemuxer(c.config())
	m, _ := NewMuxer([]*av.Track{aacTrack()}, MuxerConfig{})
	patLen := int(m.tables[1]&0x0f)<<8 | int(m.tables[2]) + 3
	pat, pmt := m.tables[:patLen], m.tables[patLen:]
	// A PAT split over two packets, with a network PID entry first.
	netPAT := append([]byte(nil), pat[:8]...)
	netPAT = append(netPAT, 0, 0, 0xe0, 0x10)
	netPAT = append(netPAT, pat[8:len(pat)-4]...)
	netPAT[2] += 4
	netPAT = finishSectionRaw(netPAT)
	pkt1 := tsPacket(0, true, 0, append([]byte{0}, netPAT[:6]...))
	pkt2 := tsPacket(0, false, 1, netPAT[6:])
	d.Write(pkt1)
	d.Write(pkt2)
	if d.pmtPID != DefaultPMTPID {
		t.Fatalf("PMT PID %#x", d.pmtPID)
	}
	// A PMT whose pointer_field skips the tail of a section never started,
	// then the PMT followed by stuffing.
	d.Write(tsPacket(DefaultPMTPID, true, 0, append(append([]byte{2, 0xaa, 0xbb}, pmt...), 0xff)))
	if len(d.order) != 1 {
		t.Fatalf("%d streams", len(d.order))
	}
	// Garbage sections: bad CRC, a too-long length, a short one, a pointer
	// past the end, a payload-less PUSI.
	bad := append([]byte(nil), pmt...)
	bad[len(bad)-1] ^= 1
	d.Write(tsPacket(DefaultPMTPID, true, 1, append([]byte{0}, bad...)))
	d.Write(tsPacket(DefaultPMTPID, true, 2, []byte{0, 2, 0xbf, 0xff}))
	d.Write(tsPacket(DefaultPMTPID, true, 3, []byte{0, 2, 0xb0, 5}))
	d.Write(tsPacket(DefaultPMTPID, true, 4, []byte{200}))
	d.Write(tsPacket(DefaultPMTPID, true, 5, nil))
	// Scrambled, errored and adaptation-only packets are skipped.
	sc := tsPacket(0x100, true, 0, []byte{0, 0, 1})
	sc[3] |= 0x80
	d.Write(sc)
	te := tsPacket(0x100, true, 0, []byte{0, 0, 1})
	te[1] |= 0x80
	d.Write(te)
	af := tsPacket(0x100, true, 0, nil)
	af[3] = 0x20
	af[4] = 200 // longer than the packet
	d.Write(af)
	if len(d.order) != 1 || len(c.tracks) != 0 {
		t.Fatal("garbage changed the program")
	}
	// A PMT of another program is ignored; a PAT naming another PMT PID
	// restarts the program.
	other := append([]byte(nil), pmt...)
	other[4] = 9
	d.Write(tsPacket(DefaultPMTPID, true, 6, append([]byte{0}, finishSectionRaw(other[:len(other)-4])...)))
	pat2 := append([]byte(nil), pat[:len(pat)-4]...)
	pat2[11] = 0x20
	d.Write(tsPacket(0, true, 2, append([]byte{0}, finishSectionRaw(pat2)...)))
	if d.pmtPID != 0x1020 || d.pmtVersion != -1 {
		t.Fatalf("PMT PID %#x", d.pmtPID)
	}
	// PMT entries cut short, with odd stream types and descriptors.
	var body []byte
	body = append(body, 0xe1, 0x00, 0xf0, 0x00)                          // PCR PID, no program info
	body = append(body, 0x06, 0xe1, 0x00, 0xf0, 4, 0x05, 2, 'x', 'y')    // private, not Opus
	body = append(body, 0x02, 0xe1, 0x01, 0xf0, 0)                       // MPEG-2 video
	body = append(body, 0x06, 0xe1, 0x02, 0xf0, 6, 0x05, 4, 'O', 'p', 0) // descriptor cut short
	body = append(body, 0x0f, 0xe1, 0x03, 0xf0, 40)                      // ES info past the end
	sec := append([]byte{2, 0xb0, 0, 0, 1, 0xc3, 0, 0}, body...)
	d.Write(tsPacket(0x1020, true, 0, append([]byte{0}, finishSectionRaw(sec)...)))
	if len(d.order) != 0 {
		t.Fatalf("%d streams", len(d.order))
	}
	d.readPMT([]byte{0, 0})
	d.readPMT([]byte{0, 0, 0xf0, 9})
}

func finishSectionRaw(s []byte) []byte {
	s = append([]byte(nil), s...)
	s[1] &^= 0x0f
	s[2] = 0
	return finishSection(s)
}

// tsPacket builds one transport packet with payload, stuffed in its
// adaptation field.
func tsPacket(pid uint16, pusi bool, cc byte, payload []byte) []byte {
	p, _ := appendTSPacket(nil, pid, pusi, &cc, 0, -1, payload)
	return p
}

// TestPESEdges feeds malformed PES packets: none gives a packet.
func TestPESEdges(t *testing.T) {
	var c capture
	d := NewDemuxer(c.config())
	m, _ := NewMuxer([]*av.Track{aacTrack()}, MuxerConfig{})
	d.Write(m.WriteTables(nil))
	s := d.streams[0x100]
	if s == nil {
		t.Fatal("no stream")
	}
	for _, pes := range [][]byte{
		{0, 0, 2, 0xc0, 0, 0, 0x80, 0x80, 5},             // bad start code
		{0, 0, 1, 0xc0, 0, 0, 0x80, 0x80, 40},            // header past the end
		{0, 0, 1, 0xc0, 0, 0, 0x80, 0x80, 2, 0, 0},       // PTS cut short
		{0, 0, 1, 0xc0, 0, 0, 0x80, 0xc0, 5, 0, 0, 0, 0}, // DTS cut short
		{0, 0, 1, 0xc0, 0, 0, 0x80, 0x00, 0, 1, 2},       // no PTS, none before
		{0, 0, 1, 0xc0, 0, 50, 0x80, 0x00, 0, 1, 2},      // shorter than its length
	} {
		s.inPES, s.broken, s.pesLen = true, false, 0
		s.pes = append(s.pes[:0], pes...)
		if len(pes) >= 6 {
			if l := int(binary.BigEndian.Uint16(pes[4:])); l > 0 {
				s.pesLen = 6 + l
			}
		}
		d.finishPES(s)
	}
	if len(c.packets) != 0 {
		t.Fatalf("%d packets", len(c.packets))
	}
	// A PES without a PTS after one with: it takes the last PTS.
	hdr := []byte{0, 0, 1, 0xc0, 0, 0, 0x80, 0x80, 5, 0, 0, 0, 0, 0}
	putTS(hdr[9:], 0x20, 9000)
	frame, _ := (&aac.Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}).AppendADTS(nil, 4)
	frame = append(frame, 1, 2, 3, 4)
	for _, h := range [][]byte{hdr, {0, 0, 1, 0xc0, 0, 0, 0x80, 0, 0}} {
		s.inPES, s.broken, s.pesLen = true, false, 0
		s.pes = append(append(s.pes[:0], h...), frame...)
		d.finishPES(s)
	}
	d.Flush()
	if len(c.packets) != 2 || c.packets[1].PTS != c.packets[0].PTS {
		t.Fatalf("%d packets", len(c.packets))
	}
	// ADTS garbage after the frame ends the PES quietly.
	s.inPES, s.pes = true, append(append(append(s.pes[:0], hdr...), frame...), 0xff, 0xf1, 0, 0)
	d.finishPES(s)
	// An unknown codec does nothing.
	s.codec = av.CodecMP3
	s.inPES, s.pes = true, append(s.pes[:0], hdr...)
	d.finishPES(s)
}

// TestVideoWithoutConfig drops access units until the parameter sets come,
// and H.265 needs all three.
func TestVideoWithoutConfig(t *testing.T) {
	var c capture
	d := NewDemuxer(c.config())
	for _, codec := range []av.CodecID{av.CodecH264, av.CodecH265} {
		s := &demuxStream{codec: codec, idx: -1}
		d.order = []*demuxStream{s}
		d.video(s, 0, 0, []byte{0, 0, 0, 1, 0x65, 0x88, 0, 0, 1, 0x26, 0x01, 0xaf})
		d.video(s, 0, 0, []byte{0, 0, 0, 1, 0x09, 0xf0}) // only an AUD
		// Parameter sets that do not parse.
		d.video(s, 0, 0, []byte{0, 0, 0, 1, 0x67, 0, 0, 0, 1, 0x68, 0, 0, 0, 1, 0x40, 1, 0, 0, 1, 0x42, 1, 0, 0, 1, 0x44, 1})
		if s.track != nil {
			t.Fatal("track from garbage")
		}
	}
	if len(c.packets) != 0 {
		t.Fatalf("%d packets", len(c.packets))
	}
}

// TestNilCallbacks: a demuxer without callbacks still demuxes.
func TestNilCallbacks(t *testing.T) {
	d := NewDemuxer(DemuxerConfig{})
	d.Write(fixture(t, "h265_opus.ts"))
	d.Flush()
	if len(d.Tracks()) != 2 {
		t.Fatal("tracks")
	}
}

func TestCRC(t *testing.T) {
	// The CRC of "123456789" in the MPEG-2 variant.
	if got := crc32MPEG([]byte("123456789")); got != 0x0376e6e7 {
		t.Fatalf("%#x", got)
	}
	if wrapDiff(0, tsMask) != 1 || wrapDiff(tsMask, 0) != -1 {
		t.Fatal("wrapDiff")
	}
}
