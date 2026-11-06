//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/hub"
)

// channelItems returns two items of a channel on one clock, of B-frame
// video (track 0, decoded 80 ms before it is shown) and audio (track 1):
// the first from 920 ms (the video's DTS) and 1 s (the audio's) to 2 s, the
// second on from there, its first keyframe decoded before the first item's
// last audio ends. Each item starts with its keyframe.
func channelItems() (first, second []*av.Packet) {
	item := func(at time.Duration) []*av.Packet {
		var pkts []*av.Packet
		v, a := 0, 0
		for v < 25 || a < 50 {
			vt := at + time.Duration(v)*40*time.Millisecond - 80*time.Millisecond
			ats := at + time.Duration(a)*20*time.Millisecond
			if v < 25 && (a >= 50 || vt <= ats) {
				nal := append([]byte{0x41}, bytes.Repeat([]byte{byte(v)}, 300)...)
				if v == 0 {
					nal[0] = 0x65
				}
				pkts = append(pkts, &av.Packet{Track: 0, DTS: vt, PTS: vt + 80*time.Millisecond, Keyframe: v == 0, Data: h264.AppendAVCC(nil, nal)})
				v++
				continue
			}
			pkts = append(pkts, &av.Packet{Track: 1, DTS: ats, PTS: ats, Keyframe: true, Data: make([]byte, 100)})
			a++
		}
		return pkts
	}
	return item(time.Second), item(2 * time.Second)
}

// A player of a stream whose tracks change on the same clock, as a
// channel's next item or a codec change makes, gets RTP timestamps that
// follow the publisher's times as they are, though the next item's video
// is decoded before the last item's audio ends; a republish restarting
// the clock is still moved on after the last time.
func TestPlayerSameClock(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, PlayWait: 5 * time.Second})
	tracks := testTracks(t)
	pub, err := h.Publish("ch", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	r := dialRaw(t, base)
	expect(t, r.do(MethodDescribe, base+"ch"), StatusOK)
	expect(t, r.do(MethodSetup, base+"ch/trackID=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1"), StatusOK)
	expect(t, r.do(MethodSetup, base+"ch/trackID=1", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3"), StatusOK)
	expect(t, r.do(MethodPlay, base+"ch"), StatusOK)
	// The subscription starts once the PLAY response has gone.
	time.Sleep(100 * time.Millisecond)
	first, second := channelItems()
	for _, p := range first {
		pub.WritePacket(p)
	}
	// The next item: a video track of its own.
	next := slices.Clone(tracks)
	v := *next[0]
	next[0] = &v
	pub.SetTracks(next)
	for _, p := range second {
		pub.WritePacket(p)
	}
	pub.Close()
	// A republish from 0.
	pub, err = h.Publish("ch", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for _, p := range first {
		q := *p
		q.DTS -= 920 * time.Millisecond
		q.PTS -= 920 * time.Millisecond
		pub.WritePacket(&q)
	}
	// The RTP timestamps of each track: a frame's fragments share one.
	var video, audio []uint32
	for len(video) < 75 || len(audio) < 150 {
		m, ok := r.next(true).(*Interleaved)
		if !ok {
			t.Fatalf("no frame after %d video, %d audio", len(video), len(audio))
		}
		ts := binary.BigEndian.Uint32(m.Data[4:])
		switch m.Channel {
		case 0:
			if len(video) == 0 || video[len(video)-1] != ts {
				video = append(video, ts)
			}
		case 2:
			audio = append(audio, ts)
		}
	}
	sent := append(slices.Clone(first), second...)
	var sentVideo, sentAudio []time.Duration
	for _, p := range sent {
		if p.Track == 0 {
			sentVideo = append(sentVideo, p.PTS)
		} else {
			sentAudio = append(sentAudio, p.PTS)
		}
	}
	check := func(what string, got []uint32, sent []time.Duration, rate int) {
		t.Helper()
		for i, d := range sent {
			if want := uint32(av.ToTicks(d-sent[0], rate)); got[i]-got[0] != want {
				t.Fatalf("%s %d at %d, sent at %d", what, i, got[i]-got[0], want)
			}
		}
		// The republish: after the last, by 20 ms to a second.
		n := len(sent)
		if d := int32(got[n] - got[n-1]); d <= 0 || d > int32(rate) {
			t.Fatalf("%s republished %d ticks on", what, d)
		}
		for i := n + 1; i < len(got); i++ {
			if int32(got[i]-got[i-1]) <= 0 {
				t.Fatalf("%s %d back", what, i)
			}
		}
	}
	check("video", video, sentVideo, 90000)
	check("audio", audio, sentAudio, 48000)
}
