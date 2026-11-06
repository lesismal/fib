package rtmp

import (
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// A player waiting through a republish whose clock starts at 0 again sees
// its timestamps carry on rather than go back, which ffmpeg reported for
// every packet after it.
func TestRepublishTimestamps(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	url := serve(t, ServerConfig{Hub: h, PlayWait: 5 * time.Second})
	e := run(t)
	got := newRecorder(4)
	if _, err := play(t, e, url+"/live/s", ClientConfig{}, got); err != nil {
		t.Fatal(err)
	}
	tracks := []*av.Track{{Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}}
	pkt := func(d time.Duration) *av.Packet {
		return &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{1}}
	}
	pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	pub.WritePacket(pkt(5 * time.Second))
	pub.WritePacket(pkt(5020 * time.Millisecond))
	pub.Close()
	pub, err = h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	pub.WritePacket(pkt(0))
	pub.WritePacket(pkt(20 * time.Millisecond))
	got.wait(t)
	got.mu.Lock()
	defer got.mu.Unlock()
	for i, p := range got.packets[1:] {
		if p.DTS <= got.packets[i].DTS {
			t.Fatalf("packet %d at %v after %v", i+1, p.DTS, got.packets[i].DTS)
		}
	}
}

// channelItems returns two items of a channel on one clock, of B-frame
// video (track video, decoded 80 ms before it is shown) and audio (track
// audio): the first from 920 ms (the video's DTS) and 1 s (the audio's) to
// 2 s, the second on from there, its first keyframe decoded before the
// first item's last audio ends. Each item starts with its keyframe.
func channelItems(video, audio int, key, delta, sound []byte) (first, second []*av.Packet) {
	item := func(at time.Duration) []*av.Packet {
		var pkts []*av.Packet
		v, a := 0, 0
		for v < 25 || a < 50 {
			vt := at + time.Duration(v)*40*time.Millisecond - 80*time.Millisecond
			ats := at + time.Duration(a)*20*time.Millisecond
			if v < 25 && (a >= 50 || vt <= ats) {
				p := &av.Packet{Track: video, DTS: vt, PTS: vt + 80*time.Millisecond, Keyframe: v == 0, Data: delta}
				if v == 0 {
					p.Data = key
				}
				pkts = append(pkts, p)
				v++
				continue
			}
			pkts = append(pkts, &av.Packet{Track: audio, DTS: ats, PTS: ats, Keyframe: true, Data: sound})
			a++
		}
		return pkts
	}
	return item(time.Second), item(2 * time.Second)
}

// A player of a stream whose tracks change on the same clock, as a
// channel's next item or a codec change makes, gets the publisher's
// timestamps as they are, though the next item's video is decoded before
// the last item's audio ends; a republish restarting the clock is still
// moved on after the last timestamp.
func TestSameClockTimestamps(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	url := serve(t, ServerConfig{Hub: h, PlayWait: 5 * time.Second})
	e := run(t)
	tracks, packets := stream(t)
	video, audio := 0, 1
	if !tracks[0].Codec.IsVideo() {
		video, audio = 1, 0
	}
	var key, delta, sound []byte
	for _, p := range packets {
		switch {
		case p.Track == video && p.Keyframe && key == nil:
			key = p.Data
		case p.Track == video && !p.Keyframe && delta == nil:
			delta = p.Data
		case p.Track == audio && sound == nil:
			sound = p.Data
		}
	}
	first, second := channelItems(video, audio, key, delta, sound)
	got := newRecorder(3 * len(first))
	if _, err := play(t, e, url+"/live/s", ClientConfig{}, got); err != nil {
		t.Fatal(err)
	}
	pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range first {
		pub.WritePacket(p)
	}
	// The next item: a video track of its own.
	next := slices.Clone(tracks)
	v := *next[video]
	next[video] = &v
	pub.SetTracks(next)
	for _, p := range second {
		pub.WritePacket(p)
	}
	pub.Close()
	// A republish from 0.
	pub, err = h.Publish("live/s", tracks, hub.PublishInfo{})
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
	got.wait(t)
	got.mu.Lock()
	defer got.mu.Unlock()
	sent := append(slices.Clone(first), second...)
	for i, w := range sent {
		p := got.packets[i]
		if p.DTS != w.DTS || p.PTS != w.PTS || p.Keyframe != w.Keyframe {
			t.Fatalf("packet %d at %v/%v, sent at %v/%v", i, p.DTS, p.PTS, w.DTS, w.PTS)
		}
	}
	var last [2]time.Duration
	for _, p := range got.packets[:len(sent)] {
		last[p.Track] = max(last[p.Track], p.DTS)
	}
	end := max(last[0], last[1])
	if p := got.packets[len(sent)]; p.DTS < end+20*time.Millisecond || p.DTS > end+time.Second {
		t.Fatalf("republish at %v after %v", p.DTS, end)
	}
	for i, p := range got.packets[len(sent):] {
		if p.DTS < last[p.Track] {
			t.Fatalf("republished packet %d of track %d at %v, back from %v", i, p.Track, p.DTS, last[p.Track])
		}
		last[p.Track] = p.DTS
	}
}
