//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/media/hub"
)

// av1Stream returns the track and temporal units of the AV1 fixture of
// package av1: 160x120 at 25 fps, a keyframe every 10 frames.
func av1Stream(t *testing.T) (*av.Track, []*av.Packet) {
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
	var pkts []*av.Packet
	for i, tu := range tus {
		ts := time.Duration(i) * 40 * time.Millisecond
		pkts = append(pkts, &av.Packet{DTS: ts, PTS: ts, Keyframe: av1.IsKeyframe(tu), Data: tu})
	}
	return &av.Track{Codec: av.CodecAV1, Config: cfg.Append(nil), Width: 160, Height: 120}, pkts
}

// waitConfigured waits until every track of the stream at path has its
// configuration.
func waitConfigured(t *testing.T, h *hub.Hub, path string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		for _, st := range h.Streams() {
			if st.Path == path && len(st.Tracks) > 0 && !slices.ContainsFunc(st.Tracks, func(tr *av.Track) bool { return len(tr.Config) == 0 }) {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s has a track with no configuration", path)
}

// TestAV1Session records AV1 into a Server with a Client and plays it back
// with another: the SDP names it AV1/90000 with the track's profile, level
// and tier, and the temporal units and the track's config come through.
func TestAV1Session(t *testing.T) {
	track, pkts := av1Stream(t)
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	ce := run(t)
	ready := make(chan struct{})
	pub, err := Publish(ClientConfig{Engine: ce, OnReady: func() { close(ready) }}, base+"av1", []*av.Track{track})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	select {
	case <-ready:
	case <-time.After(testTimeout):
		t.Fatal("publish not ready")
	}
	for _, p := range pkts[:15] {
		if err := pub.WritePacket(p); err != nil {
			t.Fatal(err)
		}
	}
	waitPublished(t, h, "av1")
	// AV1's configuration comes in band, with the first keyframe, which the
	// server may not have read yet when the stream is published; until it
	// has, DESCRIBE rightly has no fmtp to give.
	waitConfigured(t, h, "av1")

	r := dialRaw(t, base)
	resp := r.do("DESCRIBE", base+"av1", "Accept: application/sdp")
	expect(t, resp, StatusOK)
	if body := string(resp.Body); !strings.Contains(body, "a=rtpmap:96 AV1/90000") || !strings.Contains(body, "level-idx=0;profile=0;tier=0") {
		t.Fatal(body)
	}

	s := newSink()
	cfg := s.config()
	cfg.Engine = ce
	player, err := Play(cfg, base+"av1")
	if err != nil {
		t.Fatal(err)
	}
	defer player.Close()
	s.wait(t, "tracks", func(s *sink) bool { return len(s.tracks) > 0 })
	for _, p := range pkts[15:] {
		if err := pub.WritePacket(p); err != nil {
			t.Fatal(err)
		}
	}
	// The player starts at a keyframe, whose data is unlike any other's,
	// and has every temporal unit from there on.
	s.wait(t, "packets", func(s *sink) bool {
		if len(s.pkts) == 0 {
			return false
		}
		i := slices.IndexFunc(pkts, func(p *av.Packet) bool { return bytes.Equal(p.Data, s.pkts[0].Data) })
		return len(s.pkts) >= len(pkts)-i
	})
	s.mu.Lock()
	got := slices.Clone(s.pkts)
	tracks := s.tracks[len(s.tracks)-1]
	s.mu.Unlock()
	if len(tracks) != 1 || tracks[0].Codec != av.CodecAV1 || !bytes.Equal(tracks[0].Config, track.Config) || tracks[0].Width != 160 {
		t.Fatalf("%+v", tracks[0])
	}
	if !got[0].Keyframe {
		t.Fatal("does not start with a keyframe")
	}
	i := slices.IndexFunc(pkts, func(p *av.Packet) bool { return bytes.Equal(p.Data, got[0].Data) })
	if i < 0 || len(got) != len(pkts)-i {
		t.Fatalf("started at %d with %d packets", i, len(got))
	}
	for j, p := range got {
		w := pkts[i+j]
		if !bytes.Equal(p.Data, w.Data) || p.Keyframe != w.Keyframe || p.PTS-got[0].PTS != w.PTS-pkts[i].PTS {
			t.Fatalf("packet %d differs", j)
		}
	}
}
