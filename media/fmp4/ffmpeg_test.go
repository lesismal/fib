package fmp4

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/opus"
)

// TestReadFFmpeg reads fragmented MP4 files ffmpeg 5.1 wrote (-movflags
// frag_keyframe+empty_moov+default_base_moof): 64x48 H.264 with B-frames at
// 25 fps in GOPs of 5 with 48 kHz AAC, and mono Opus.
func TestReadFFmpeg(t *testing.T) {
	b := readFile(t, "ffmpeg_h264_aac.mp4")
	init, err := ParseInit(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(init.Tracks) != 2 || init.Tracks[0].Track.Codec != av.CodecH264 || init.Tracks[0].Track.Width != 64 || init.Tracks[1].Track.Codec != av.CodecAAC || init.Tracks[1].Track.SampleRate != 48000 {
		t.Fatalf("%+v %+v", init.Tracks[0].Track, init.Tracks[1].Track)
	}
	_, rest, _ := NextBox(b) // ftyp
	_, rest, _ = NextBox(rest)
	var video, audio []*av.Packet
	frags := 0
	for len(rest) > 0 {
		var pkts []*av.Packet
		_, pkts, rest, err = init.ParseFragment(rest)
		if err == ErrNoMoof && frags > 0 {
			break // the mfra ffmpeg ends the file with
		}
		if err != nil {
			t.Fatal(err)
		}
		frags++
		for _, p := range pkts {
			if p.Track == 0 {
				video = append(video, p)
			} else {
				audio = append(audio, p)
			}
		}
	}
	if len(video) != 10 || frags < 2 || len(audio) < 15 {
		t.Fatal(len(video), len(audio), frags)
	}
	reordered := false
	for i, p := range video {
		if p.Keyframe != (i%5 == 0) || p.DTS != time.Duration(i)*40*time.Millisecond+video[0].DTS || !h264.ValidAVCC(p.Data, 4) {
			t.Fatalf("%d: %+v", i, p)
		}
		reordered = reordered || p.PTS-p.DTS != video[0].PTS-video[0].DTS
	}
	if !reordered {
		t.Fatal("no B-frames read")
	}

	b = readFile(t, "ffmpeg_opus.mp4")
	init, err = ParseInit(b)
	if err != nil || init.Tracks[0].Track.Codec != av.CodecOpus || init.Tracks[0].Timescale != 48000 {
		t.Fatal(err)
	}
	var h opus.Head
	if err := h.Unmarshal(init.Tracks[0].Track.Config); err != nil || h.Channels != 1 {
		t.Fatal(h, err)
	}
}
