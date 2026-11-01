package rtmp

import (
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
