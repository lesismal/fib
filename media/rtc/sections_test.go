//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc"
)

var pcmuCodec = webrtc.Codec{MimeType: "audio/PCMU", ClockRate: 8000, PayloadType: 0}

// playTwoAudio publishes an Opus and a PCMU track at path and plays them
// with a viewer whose offer has the given audio sections, each offering
// the codecs listed; it returns the codecs the viewer receives packets
// in, by mid, once every section that is to get one has.
func playTwoAudio(t *testing.T, ev *env, path string, sections [][]webrtc.Codec, want int) map[string]string {
	t.Helper()
	tracks := []*av.Track{{Codec: av.CodecOpus, SampleRate: 48000, Channels: 2}, {Codec: av.CodecPCMU, SampleRate: 8000, Channels: 1}}
	pub, err := ev.hub.Publish(path, tracks, hub.PublishInfo{Protocol: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	pace(t, func(k int) {
		d := time.Duration(k) * frameDur
		pub.WritePacket(&av.Packet{Track: 0, DTS: d, PTS: d, Keyframe: true, Data: frameData(k, 40)})
		pub.WritePacket(&av.Packet{Track: 1, DTS: d, PTS: d, Keyframe: true, Data: frameData(k, 160)})
	})

	var all []webrtc.Codec
	for _, s := range sections {
		for _, c := range s {
			if !containsCodec(all, c) {
				all = append(all, c)
			}
		}
	}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Engine: ev.engine, ListenAddr: "127.0.0.1:0", Codecs: all})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	var mu sync.Mutex
	got := map[string]string{}
	pc.OnTrack(func(tr *webrtc.TrackRemote) {
		mid := tr.Transceiver().Mid()
		tr.OnRTP(func([]byte) {
			mu.Lock()
			got[mid] = tr.Codec().Name()
			mu.Unlock()
		})
	})
	for range sections {
		if _, err := pc.AddTransceiver(webrtc.MediaKindAudio, webrtc.DirectionRecvOnly); err != nil {
			t.Fatal(err)
		}
	}
	ev.connectWith(t, pc, "/whep/"+path, func(sdp string) string { return keepCodecs(sdp, sections) })
	waitFor(t, "audio", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= want
	})
	mu.Lock()
	defer mu.Unlock()
	out := map[string]string{}
	for k, v := range got {
		out[k] = v
	}
	return out
}

// keepCodecs edits an offer of audio sections so that the i-th offers only
// the codecs of sections[i].
func keepCodecs(offer string, sections [][]webrtc.Codec) string {
	lines := strings.Split(offer, "\r\n")
	var out []string
	sec := -1
	keep := func(pt string) bool {
		for _, c := range sections[sec] {
			if pt == strconv.Itoa(int(c.PayloadType)) {
				return true
			}
		}
		return false
	}
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "m=audio "):
			sec++
			f := strings.Fields(l)
			m := f[:3]
			for _, pt := range f[3:] {
				if keep(pt) {
					m = append(m, pt)
				}
			}
			l = strings.Join(m, " ")
		case sec >= 0 && (strings.HasPrefix(l, "a=rtpmap:") || strings.HasPrefix(l, "a=fmtp:") || strings.HasPrefix(l, "a=rtcp-fb:")):
			pt, _, _ := strings.Cut(l[strings.IndexByte(l, ':')+1:], " ")
			if !keep(pt) {
				continue
			}
		}
		out = append(out, l)
	}
	return strings.Join(out, "\r\n")
}

// TestWHEPTwoAudioTracks plays a stream of an Opus and a PCMU track to a
// viewer with two audio sections: each track goes in a section of its own.
func TestWHEPTwoAudioTracks(t *testing.T) {
	ev := newEnv(t, nil)
	both := []webrtc.Codec{opusCodec, pcmuCodec}
	got := playTwoAudio(t, ev, "two", [][]webrtc.Codec{both, both}, 2)
	if len(got) != 2 || got["0"] != "opus" || got["1"] != "PCMU" {
		t.Fatalf("%v", got)
	}
}

// TestWHEPOneAudioSection plays the same stream to a viewer with one
// audio section: it gets the first track, and the second is logged as
// left out.
func TestWHEPOneAudioSection(t *testing.T) {
	ev := newEnv(t, nil)
	got := playTwoAudio(t, ev, "one", [][]webrtc.Codec{{opusCodec, pcmuCodec}}, 1)
	if len(got) != 1 || got["0"] != "opus" {
		t.Fatalf("%v", got)
	}
	if !ev.srv.loggedNoSection[av.CodecPCMU].Load() {
		t.Fatal("PCMU not logged")
	}
}

// TestWHEPSectionsByCodec has the first section offer only PCMU and the
// second only Opus: each track goes in the section that takes its codec.
func TestWHEPSectionsByCodec(t *testing.T) {
	ev := newEnv(t, nil)
	got := playTwoAudio(t, ev, "bycodec", [][]webrtc.Codec{{pcmuCodec}, {opusCodec}}, 2)
	if len(got) != 2 || got["0"] != "PCMU" || got["1"] != "opus" {
		t.Fatalf("%v", got)
	}
}
