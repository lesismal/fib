package hls

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/mpegts"
)

// avcc makes the video packets of pkts valid AVCC, one NAL unit each, an
// IDR or a non-IDR slice, which package mpegts requires.
func avcc(pkts []*av.Packet) []*av.Packet {
	for _, p := range pkts {
		if p.Track != 0 || len(p.Data) == 0 {
			continue
		}
		nal := byte(0x41)
		if p.Keyframe {
			nal = 0x65
		}
		d := binary.BigEndian.AppendUint32(nil, uint32(len(p.Data)))
		d = append(d, nal)
		p.Data = append(d, p.Data[1:]...)
	}
	return pkts
}

// demux reads a transport stream back into tracks and packets.
func demux(t testing.TB, ts []byte) ([]*av.Track, []*av.Packet) {
	t.Helper()
	if len(ts)%mpegts.PacketSize != 0 {
		t.Fatalf("%d bytes is not whole packets", len(ts))
	}
	var tracks []*av.Track
	var pkts []*av.Packet
	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
		OnTracks: func(tr []*av.Track) { tracks = tr },
		OnPacket: func(p *av.Packet) { pkts = append(pkts, p) },
	})
	d.Write(ts)
	d.Flush()
	return tracks, pkts
}

// pid returns the PID of the transport packet at the start of b.
func pid(b []byte) uint16 { return binary.BigEndian.Uint16(b[1:3]) & 0x1fff }

func TestMuxerTS(t *testing.T) {
	m := NewMuxer(Config{Format: MPEGTS, PlaylistSize: 3, KeepSegments: 1})
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t), {Codec: av.CodecPCMU}})
	src := &source{video: true, audio: 1, gop: 50}
	src.frame = 10
	feed(m, avcc(src.until(time.Second)))
	// A packet the muxer refuses, video that is not AVCC, is left out.
	feed(m, []*av.Packet{{Track: 2, Data: []byte{1}}, {Track: 0, DTS: time.Second, Data: []byte{0, 0, 0, 9, 1}}})
	feed(m, avcc(src.until(13*time.Second)))
	p := media(t, m)
	if p.tags["#EXT-X-VERSION"] != "3" || len(p.maps) != 0 || len(p.segs) != 3 || p.tags["#EXT-X-MEDIA-SEQUENCE"] != "2" {
		t.Fatalf("%+v", p)
	}
	var all []byte
	for _, name := range p.segs {
		if !strings.HasSuffix(name, ".ts") {
			t.Fatal(name)
		}
		data, ok := m.File(name)
		if !ok {
			t.Fatal(name)
		}
		// Every segment starts with the PAT and the PMT, and stands
		// alone: it demuxes by itself, from a keyframe.
		if len(data) < 2*mpegts.PacketSize || data[0] != 0x47 || pid(data) != 0 || pid(data[mpegts.PacketSize:]) != mpegts.DefaultPMTPID {
			t.Fatal(name, "does not start with PAT and PMT")
		}
		tracks, pkts := demux(t, data)
		if len(tracks) != 2 || tracks[0].Codec != av.CodecH264 || tracks[1].Codec != av.CodecAAC || len(pkts) == 0 {
			t.Fatal(name, tracks, len(pkts))
		}
		for _, pk := range pkts {
			if pk.Track == 0 {
				if !pk.Keyframe {
					t.Fatal(name, "first video is not a keyframe")
				}
				break
			}
		}
		all = append(all, data...)
	}
	if _, ok := m.File(strings.Replace(p.segs[0], "seg-", "init-", 1)); ok {
		t.Fatal("TS has no init section")
	}
	_, pkts := demux(t, all)
	var v, a int
	for _, pk := range pkts {
		if pk.Track == 0 {
			v++
		} else {
			a++
		}
	}
	if v != 150 || a < 278 || a > 284 {
		t.Fatal(v, a)
	}
	b, ok := m.MultivariantPlaylist(nil, "media.m3u8")
	if !ok || !bytes.Contains(b, []byte("#EXT-X-VERSION:3\n")) || !bytes.Contains(b, []byte(`CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=1280x720`)) {
		t.Fatalf("%s", b)
	}
	m.Close()
	if p := media(t, m); !p.ended {
		t.Fatal("no ENDLIST")
	}
}

func TestMuxerTSParts(t *testing.T) {
	m := NewMuxer(Config{Format: MPEGTS, PartDuration: 500 * time.Millisecond})
	m.OnTracks([]*av.Track{h265Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 50}
	feed(m, avcc(src.until(4100*time.Millisecond)))
	b, ok := m.MediaPlaylist(nil)
	if !ok {
		t.Fatal("no playlist")
	}
	p := parsePlaylist(t, b)
	if p.tags["#EXT-X-VERSION"] != "3" || len(p.segs) != 2 || len(p.parts) < 8 || p.hint == "" || !strings.HasSuffix(p.hint, ".ts") {
		t.Fatalf("%s", b)
	}
	// Parts are runs of whole transport packets that add up to their
	// segment; the first of each segment starts at its keyframe and is
	// INDEPENDENT, the others are not (the GOP is the segment).
	var joined []byte
	prefix := "part-" + m.session + "-0-"
	for _, name := range p.parts {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		data, ok := m.File(name)
		if !ok || len(data)%mpegts.PacketSize != 0 {
			t.Fatal(name, len(data))
		}
		joined = append(joined, data...)
	}
	seg, _ := m.File(p.segs[0])
	if !bytes.Equal(joined, seg) {
		t.Fatal("parts do not add up to the segment")
	}
	if n := strings.Count(string(b), "INDEPENDENT=YES"); n != 2 || !strings.Contains(string(b), "-0-0.ts\",INDEPENDENT=YES") {
		// Each complete segment starts with one; the open one has no
		// part yet.
		t.Fatalf("%d independent parts:\n%s", n, b)
	}
	mv, _ := m.MultivariantPlaylist(nil, "media.m3u8")
	if !bytes.Contains(mv, []byte(`CODECS="hvc1.`)) {
		t.Fatalf("%s", mv)
	}
}

func TestMuxerTSAudioOnly(t *testing.T) {
	m := NewMuxer(Config{Format: MPEGTS, PartDuration: 300 * time.Millisecond})
	m.OnTracks([]*av.Track{aacTrack(t)})
	src := &source{audio: 0}
	feed(m, src.until(5*time.Second))
	b, _ := m.MediaPlaylist(nil)
	p := parsePlaylist(t, b)
	if len(p.parts) == 0 || strings.Count(string(b), "INDEPENDENT=YES") != strings.Count(string(b), "#EXT-X-PART:") {
		t.Fatalf("%s", b)
	}
	if _, ok := m.MultivariantPlaylist(nil, "media.m3u8"); !ok {
		t.Fatal("no multivariant playlist")
	}
}

func TestMuxerTSNoTracks(t *testing.T) {
	// A stream of nothing TS carries writes nothing.
	m := NewMuxer(Config{Format: MPEGTS})
	m.OnTracks([]*av.Track{{Codec: av.CodecAV1}})
	feed(m, []*av.Packet{{Track: 0, Keyframe: true, Data: []byte{1}}})
	if m.writer != nil || m.Bytes() != 0 {
		t.Fatal("writer for no tracks")
	}
	if _, err := MPEGTS.New([]*av.Track{{Codec: av.CodecPCMA}}); err == nil {
		t.Fatal("no error")
	}
}
