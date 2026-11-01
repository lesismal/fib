//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/rtp"
)

// frameDur is the duration of the test streams' frames, each of which
// carries its number k in its first two bytes.
const frameDur = 20 * time.Millisecond

func frameData(k, n int) []byte {
	b := make([]byte, n)
	b[0], b[1] = byte(k>>8), byte(k)
	return b
}

func frameOf(b []byte) int { return int(b[0])<<8 | int(b[1]) }

// connect has pc, with the tracks it sends or receives added, publish to
// or play the env's path by WHIP or WHEP, and waits until it connects.
func (ev *env) connect(t *testing.T, pc *webrtc.PeerConnection, path string) {
	t.Helper()
	connected := make(chan struct{})
	var once sync.Once
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			once.Do(func() { close(connected) })
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	offer, err := webrtc.Offer(ctx, pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, answer := ev.request(t, "POST", path, "application/sdp", offer.SDP, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("%s: %d %s", path, resp.StatusCode, answer)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-time.After(waitTimeout):
		t.Fatal("not connected")
	}
}

// pace calls frame with k = 0, 1, ... every frameDur from now until the
// test ends.
func pace(t *testing.T, frame func(k int)) {
	stop := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { close(stop); <-done })
	start := time.Now()
	go func() {
		defer close(done)
		for k := 0; ; k++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Until(start.Add(time.Duration(k) * frameDur))):
			}
			frame(k)
		}
	}()
}

// TestWHIPLipSync publishes two tracks with a PeerConnection whose
// sender reports place them together, the second track's packets sent
// 300 ms after the first's of the same moment and its timestamps
// wrapping: the hub stream has them lined up once the reports come.
func TestWHIPLipSync(t *testing.T) {
	ev := newEnv(t, nil)
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Engine: ev.engine, ListenAddr: "127.0.0.1:0",
		Codecs: []webrtc.Codec{opusCodec}, RTCPReportInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	locals := []*webrtc.TrackLocal{webrtc.NewTrackLocal(opusCodec, "a", "s"), webrtc.NewTrackLocal(opusCodec, "b", "s")}
	for _, tr := range locals {
		s, err := pc.AddTrack(tr)
		if err != nil {
			t.Fatal(err)
		}
		s.Transceiver().SetDirection(webrtc.DirectionSendOnly)
	}
	ev.connect(t, pc, "/whip/sync")
	col := newCollector()
	sub, err := ev.hub.Subscribe("sync", col, hub.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	// Frame k of either track is sampled at wall0 + k*frameDur.
	const lag = 15 // frames, 300 ms
	ts := [2]uint32{1000, 1<<32 - 960*30}
	wall0 := time.Now()
	for i, tr := range locals {
		tr.SetReportClock(wall0, ts[i])
	}
	send := func(i, k int) {
		_ = locals[i].WritePacket(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(k), Timestamp: ts[i] + uint32(960*k)},
			Payload: frameData(k, 40)})
	}
	pace(t, func(k int) {
		send(0, k)
		if k >= lag {
			send(1, k-lag)
		}
	})
	// skews returns how much later track 1 presents each frame than track 0.
	skews := func(packets []*av.Packet) []time.Duration {
		var at [2]map[int]time.Duration
		at[0], at[1] = map[int]time.Duration{}, map[int]time.Duration{}
		for _, p := range packets {
			at[p.Track][frameOf(p.Data)] = p.PTS
		}
		var out []time.Duration
		for k := range 1 << 16 {
			if a, ok := at[0][k]; ok {
				if b, ok := at[1][k]; ok {
					out = append(out, b-a)
				}
			}
		}
		return out
	}
	col.wait(t, "the tracks lined up", func(tracks []*av.Track, packets []*av.Packet) bool {
		s := skews(packets)
		if len(s) == 0 {
			return false
		}
		if s[0] < 200*time.Millisecond {
			t.Fatalf("the first frames are only %v apart: the test shows nothing", s[0])
		}
		last := s[len(s)-1]
		return last >= -time.Millisecond && last <= time.Millisecond
	})
}

// TestWHEPSenderReports plays two tracks whose packets reach the hub, and
// so the viewer, 100 ms apart from the frames of the same moment: fib's
// sender reports still put them on one clock, as a browser needs to line
// them up.
func TestWHEPSenderReports(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.WebRTC.RTCPReportInterval = 20 * time.Millisecond })
	opus := &av.Track{Codec: av.CodecOpus, SampleRate: 48000, Channels: 2}
	tracks := []*av.Track{opus, opus}
	pub, err := ev.hub.Publish("sync", tracks, hub.PublishInfo{Protocol: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	const lead = 5 // frames, 100 ms
	pace(t, func(k int) {
		d := time.Duration(k) * frameDur
		pub.WritePacket(&av.Packet{Track: 0, DTS: d, PTS: d, Keyframe: true, Data: frameData(k, 40)})
		d += lead * frameDur
		pub.WritePacket(&av.Packet{Track: 1, DTS: d, PTS: d, Keyframe: true, Data: frameData(k+lead, 40)})
	})

	// The viewer keeps, per SSRC, a packet's timestamp and frame, and the
	// offset of the wall clock from the media time of each sender report.
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Engine: ev.engine, ListenAddr: "127.0.0.1:0",
		Codecs: []webrtc.Codec{opusCodec}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	type stream struct {
		rate    int
		ts      uint32
		frame   int
		offsets []time.Duration
	}
	var mu sync.Mutex
	streams := map[uint32]*stream{}
	pc.OnTrack(func(tr *webrtc.TrackRemote) {
		st := &stream{rate: int(tr.Codec().ClockRate), frame: -1}
		mu.Lock()
		streams[tr.SSRC()] = st
		mu.Unlock()
		tr.OnRTP(func(b []byte) {
			var p rtp.Packet
			if p.Unmarshal(b) == nil && len(p.Payload) >= 2 {
				mu.Lock()
				st.ts, st.frame = p.Timestamp, frameOf(p.Payload)
				mu.Unlock()
			}
		})
	})
	pc.OnRTCP(func(b []byte) {
		mu.Lock()
		defer mu.Unlock()
		for len(b) > 0 {
			h, p, rest, err := rtp.NextRTCP(b)
			if err != nil {
				return
			}
			b = rest
			var sr rtp.SenderReport
			if h.Type != rtp.TypeSR || sr.Unmarshal(p) != nil {
				continue
			}
			if st := streams[sr.SSRC]; st != nil && st.frame >= 0 {
				media := time.Duration(st.frame)*frameDur + av.FromTicks(int64(int32(sr.RTPTime-st.ts)), st.rate)
				st.offsets = append(st.offsets, rtp.TimeFromNTP(sr.NTPTime).Sub(time.Unix(0, 0))-media)
			}
		}
	})
	for range 2 {
		if _, err := pc.AddTransceiver(webrtc.MediaKindAudio, webrtc.DirectionRecvOnly); err != nil {
			t.Fatal(err)
		}
	}
	ev.connect(t, pc, "/whep/sync")
	waitFor(t, "sender reports on one clock", func() bool {
		mu.Lock()
		defer mu.Unlock()
		var last []time.Duration
		for _, st := range streams {
			if len(st.offsets) < 5 {
				return false
			}
			last = append(last, st.offsets[len(st.offsets)-1])
		}
		// NTP times have 2^-32 s of resolution, the conversions round.
		return len(last) == 2 && last[0]-last[1] <= time.Millisecond && last[1]-last[0] <= time.Millisecond
	})
}
