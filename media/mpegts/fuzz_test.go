package mpegts

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h264"
)

// FuzzDemuxer feeds arbitrary bytes to a Demuxer in two chunks: it never
// panics, video comes out as AVCC, and every packet indexes the tracks
// announced before it.
func FuzzDemuxer(f *testing.F) {
	names, _ := filepath.Glob("testdata/*.ts")
	for _, n := range names {
		b, err := os.ReadFile(n)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b[:min(len(b), 40*PacketSize)], 1000)
		f.Add(b, 4096)
	}
	f.Fuzz(func(t *testing.T, b []byte, split int) {
		var tracks []*av.Track
		d := NewDemuxer(DemuxerConfig{
			MaxPESSize: 1 << 16,
			OnTracks:   func(t []*av.Track) { tracks = t },
			OnPacket: func(p *av.Packet) {
				if p.Track < 0 || p.Track >= len(tracks) {
					t.Fatalf("packet of track %d of %d", p.Track, len(tracks))
				}
				if tracks[p.Track].Codec.IsVideo() && !h264.ValidAVCC(p.Data, 4) {
					t.Fatal("video packet not AVCC")
				}
			},
		})
		if split < 0 {
			split = -split
		}
		if len(b) > 0 {
			split %= len(b)
		} else {
			split = 0
		}
		d.Write(b[:split])
		d.Write(b[split:])
		d.Flush()
	})
}

// FuzzMuxer muxes arbitrary data as packets of every codec: it never panics
// and writes whole transport packets.
func FuzzMuxer(f *testing.F) {
	f.Add([]byte{0, 0, 0, 2, 0x65, 0x88}, true, int64(0))
	f.Add([]byte{0, 0, 0, 9}, false, int64(-1))
	f.Add(make([]byte, 400), true, int64(1<<40))
	f.Fuzz(func(t *testing.T, data []byte, key bool, ts int64) {
		tracks := []*av.Track{{Codec: av.CodecH264}, {Codec: av.CodecH265}, aacTrack(), {Codec: av.CodecOpus}}
		m, err := NewMuxer(tracks, MuxerConfig{AudioFramesPerPES: 2})
		if err != nil {
			t.Fatal(err)
		}
		var out []byte
		for i := range tracks {
			out, _ = m.WritePacket(out, &av.Packet{Track: i, DTS: time.Duration(ts), PTS: time.Duration(ts), Keyframe: key, Data: data})
		}
		out = m.Flush(out)
		if len(out)%PacketSize != 0 {
			t.Fatalf("%d bytes", len(out))
		}
		NewDemuxer(DemuxerConfig{}).Write(out)
	})
}
