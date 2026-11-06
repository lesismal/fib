package fmp4

import (
	"bytes"
	"testing"

	"github.com/lesismal/fib/media/av"
)

// TestParseSampleEntry reads the sample entries AppendInit writes back with
// ParseSampleEntry, and an entry of a format it does not know.
func TestParseSampleEntry(t *testing.T) {
	for _, track := range []*av.Track{h264Track(t), h265Track(t), aacTrack(t, 44100), opusTrack(t, nil)} {
		init := mustInit(t, []*av.Track{track})
		at := bytes.Index(init, []byte("stsd"))
		if at < 0 {
			t.Fatal("no stsd")
		}
		e, _, err := NextBox(init[at+12:])
		if err != nil {
			t.Fatal(err)
		}
		got := ParseSampleEntry(e)
		if got.Codec != track.Codec || track.Codec.IsAudio() && got.Channels != track.Channels ||
			track.Codec == av.CodecAAC && (!bytes.Equal(got.Config, track.Config) || got.SampleRate != 44100) {
			t.Fatalf("%v: %+v", track.Codec, got)
		}
	}
	if got := ParseSampleEntry(Box{Type: BoxType{'t', 'x', '3', 'g'}, Data: make([]byte, 40)}); got.Codec != av.CodecUnknown {
		t.Fatalf("%+v", got)
	}
}
