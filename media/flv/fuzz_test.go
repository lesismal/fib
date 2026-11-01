package flv

import (
	"testing"

	"github.com/lesismal/fib/media/av"
)

// FuzzDemux feeds arbitrary bytes to a Demuxer, in two pieces, and every
// tag it yields to a Decoder: neither may panic, and every packet the
// Decoder makes must index a track it reported.
func FuzzDemux(f *testing.F) {
	for _, name := range []string{"h264_aac.flv", "hevc_aac.flv", "av1.flv", "mp3.flv", "pcma.flv"} {
		b := readFixture(f, name)
		f.Add(b[:min(len(b), 4096)], 100)
	}
	f.Fuzz(func(t *testing.T, b []byte, split int) {
		var tracks []*av.Track
		d := NewDecoder(DecoderConfig{
			OnTracks: func(tr []*av.Track) { tracks = tr },
			OnPacket: func(p *av.Packet) {
				if p.Track < 0 || p.Track >= len(tracks) || tracks[p.Track] == nil {
					t.Fatalf("packet of track %d of %d", p.Track, len(tracks))
				}
			},
			MaxPending: 8,
		})
		dm := NewDemuxer(DemuxerConfig{MaxTagSize: 1 << 16})
		fn := func(tag Tag) error {
			_ = d.WriteTag(tag.Type, tag.Timestamp, tag.Data)
			return nil
		}
		if split < 0 || split > len(b) {
			split = len(b) / 2
		}
		if dm.Feed(b[:split], fn) == nil {
			_ = dm.Feed(b[split:], fn)
		}
		d.Flush()
	})
}

// FuzzTags parses arbitrary bytes as video and audio tag bodies: a body
// that parses as a codec this package writes is written back and parses
// to the same header.
func FuzzTags(f *testing.F) {
	tags, _ := demuxAll(f, readFixture(f, "hevc_aac.flv"), 1<<20)
	for _, tag := range tags[:min(len(tags), 20)] {
		f.Add(tag.Data)
	}
	f.Add([]byte{0x80 | FrameKey<<4 | PacketModEx, 2, 0, 0, 1, PacketCodedFrames, 'h', 'v', 'c', '1', 0, 0, 1, 2})
	f.Fuzz(func(t *testing.T, b []byte) {
		if v, err := ParseVideoTag(b); err == nil && (v.Codec == av.CodecH264 || v.Codec == av.CodecH265) &&
			v.FrameType != FrameCommand && v.PacketType <= PacketSequenceEnd {
			out, err := AppendVideoTag(nil, &v)
			if err != nil {
				t.Fatal(err)
			}
			w, err := ParseVideoTag(out)
			if err != nil || w.Codec != v.Codec || w.CTS != v.CTS || w.PacketType != v.PacketType || string(w.Data) != string(v.Data) {
				t.Fatalf("%x -> %+v -> %x -> %+v %v", b, v, out, w, err)
			}
		}
		if a, err := ParseAudioTag(b); err == nil && a.Codec != av.CodecUnknown && a.Codec != av.CodecMP3 {
			if _, err := AppendAudioTag(nil, &a); err != nil {
				t.Fatal(err)
			}
		}
		_, _, _ = ParseScript(b)
	})
}
