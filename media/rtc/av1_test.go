//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/webrtc"
)

// loadAV1 reads the AV1 fixture of package av1: 1.2 s of 160x120 at 25
// fps, a keyframe every 10 frames.
func loadAV1(t testing.TB) *media {
	t.Helper()
	b, err := os.ReadFile("../codec/av1/testdata/av1.obu")
	if err != nil {
		t.Fatal(err)
	}
	var tus [][]byte
	for o := range av1.SplitOBUs(b) {
		if o.Type() == av1.OBUTemporalDelimiter {
			tus = append(tus, nil)
			continue
		}
		tus[len(tus)-1] = av1.AppendOBU(tus[len(tus)-1], o, true)
	}
	cfg, err := av1.NewConfig(tus[0])
	if err != nil {
		t.Fatal(err)
	}
	m := &media{tracks: []*av.Track{{Codec: av.CodecAV1, Config: cfg.Append(nil), Width: 160, Height: 120}}}
	for i, tu := range tus {
		ts := time.Duration(i) * 40 * time.Millisecond
		m.packets = append(m.packets, &av.Packet{DTS: ts, PTS: ts, Keyframe: av1.IsKeyframe(tu), Data: tu})
	}
	return m
}

// TestAV1WHIPAndWHEP publishes AV1 with a WHIP client and plays it back
// with a WHEP client: the temporal units come through as they were, and
// the viewer learns the track's configuration from the sequence headers.
func TestAV1WHIPAndWHEP(t *testing.T) {
	ev := newEnv(t, nil)
	m := loadAV1(t)
	cfg := ev.clientConfig()
	ready := make(chan struct{})
	cfg.OnReady = func() { close(ready) }
	push, err := Push(cfg, ev.base+"/whip/av1", m.tracks)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	stop := make(chan struct{})
	defer close(stop)
	go feed(m, func(p *av.Packet) { _ = push.WritePacket(p) }, stop)
	select {
	case <-ready:
	case <-time.After(waitTimeout):
		t.Fatal("not connected")
	}
	if len(push.Tracks()) != 1 {
		t.Fatalf("pushing %v", push.Tracks())
	}

	deadline := time.Now().Add(waitTimeout)
	for len(ev.hub.Streams()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	col := newCollector()
	pull, err := Pull(col.config(ev.clientConfig()), ev.base+"/whep/av1")
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	col.wait(t, "AV1", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, keys := counts(tracks, packets)
		return n[av.CodecAV1] >= 30 && keys >= 2
	})
	col.mu.Lock()
	tracks, packets := col.tracks, col.packets
	col.mu.Unlock()
	if len(tracks) != 1 || tracks[0].Codec != av.CodecAV1 || tracks[0].Width != 160 || !bytes.Equal(tracks[0].Config, m.tracks[0].Config) {
		t.Fatalf("tracks %+v", tracks)
	}
	if !packets[0].Keyframe {
		t.Fatal("first packet is not a keyframe")
	}
	for i, p := range packets {
		if !contains(m.packets, p.Data) {
			t.Fatalf("packet %d not as published", i)
		}
	}
}

func TestAV1Codecs(t *testing.T) {
	m := loadAV1(t)
	main := m.tracks[0]
	c, ok := sendCodec(main, nil)
	if !ok || c.Name() != "AV1" || c.Fmtp != "level-idx=0;profile=0;tier=0" {
		t.Fatalf("%+v", c)
	}
	offered := []webrtc.Codec{
		{MimeType: "video/AV1", ClockRate: 90000, PayloadType: 41, Fmtp: "profile=1"},
		{MimeType: "video/AV1", ClockRate: 90000, PayloadType: 45},
		{MimeType: "video/AV1", ClockRate: 90000, PayloadType: 47, Fmtp: "level-idx=5;profile=0;tier=0"},
	}
	if c, ok := sendCodec(main, offered); !ok || c.PayloadType != 45 {
		t.Fatalf("%+v", c)
	}
	if c, ok := sendCodec(main, offered[2:]); !ok || c.PayloadType != 47 {
		t.Fatalf("%+v", c)
	}
	high := &av.Track{Codec: av.CodecAV1, Config: []byte{0x81, 0x20, 0, 0}}
	if c, ok := sendCodec(high, offered); !ok || c.PayloadType != 41 {
		t.Fatalf("%+v", c)
	}
	if _, ok := sendCodec(high, offered[1:]); ok {
		t.Fatal("high profile sent as main")
	}
	// Without a configuration, main profile at the default level.
	if c, _ := sendCodec(&av.Track{Codec: av.CodecAV1}, nil); c.Fmtp != "level-idx=5;profile=0;tier=0" {
		t.Fatal(c.Fmtp)
	}
	if !receivable(offered[0]) {
		t.Fatal("AV1 not receivable")
	}
	if receivable(webrtc.Codec{MimeType: "audio/MP4A-LATM", ClockRate: 44100}) {
		t.Fatal("AAC receivable")
	}
	var found bool
	for _, c := range DefaultReceiveCodecs() {
		found = found || c.Name() == "AV1"
	}
	if !found {
		t.Fatal("AV1 not offered")
	}
}
