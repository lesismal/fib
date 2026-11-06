package httpflv

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

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

// An HTTP-FLV player of a stream whose tracks change on the same clock, as
// a channel's next item or a codec change makes, gets the publisher's
// timestamps as they are (from 0), though the next item's video is decoded
// before the last item's audio ends; a republish restarting the clock is
// still moved on after the last timestamp.
func TestSameClock(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	addr := serve(t, Config{Hub: h, Wait: 5 * time.Second}, nil)
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
	resp, err := http.Get("http://" + addr + "/live/s.flv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitFor(t, func() bool { return subscribers(h, "live/s") == 1 })
	p := newPlayer()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			p.feed(buf[:n])
			if err != nil {
				return
			}
		}
	}()
	pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pkt := range first {
		pub.WritePacket(pkt)
	}
	// The next item: a video track of its own.
	next := slices.Clone(tracks)
	v := *next[video]
	next[video] = &v
	pub.SetTracks(next)
	for _, pkt := range second {
		pub.WritePacket(pkt)
	}
	pub.Close()
	// A republish from 0.
	pub, err = h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for _, pkt := range first {
		q := *pkt
		q.DTS -= 920 * time.Millisecond
		q.PTS -= 920 * time.Millisecond
		pub.WritePacket(&q)
	}
	sent := append(slices.Clone(first), second...)
	waitFor(t, func() bool { return p.count() == len(sent)+len(first) })
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		t.Fatal(p.err)
	}
	base := first[0].DTS
	for i, w := range sent {
		got := p.packets[i]
		if got.DTS != w.DTS-base || got.PTS != w.PTS-base || got.Keyframe != w.Keyframe {
			t.Fatalf("packet %d at %v/%v, sent at %v/%v", i, got.DTS, got.PTS, w.DTS-base, w.PTS-base)
		}
	}
	var last [2]time.Duration
	for _, got := range p.packets[:len(sent)] {
		last[got.Track] = max(last[got.Track], got.DTS)
	}
	end := max(last[0], last[1])
	if got := p.packets[len(sent)]; got.DTS < end+20*time.Millisecond || got.DTS > end+time.Second {
		t.Fatalf("republish at %v after %v", got.DTS, end)
	}
	for i, got := range p.packets[len(sent):] {
		if got.DTS < last[got.Track] {
			t.Fatalf("republished packet %d of track %d at %v, back from %v", i, got.Track, got.DTS, last[got.Track])
		}
		last[got.Track] = got.DTS
	}
}
