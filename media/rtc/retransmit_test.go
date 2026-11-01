//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/rtp"
)

// TestWHEPRetransmit answers a viewer that offers NACK and RTX with RTX,
// and resends what it asks for again; with RetransmitPackets negative it
// offers no RTX.
func TestWHEPRetransmit(t *testing.T) {
	t.Run("default", func(t *testing.T) { whepRetransmit(t, 0) })
	t.Run("off", func(t *testing.T) { whepRetransmit(t, -1) })
}

func whepRetransmit(t *testing.T, packets int) {
	ev := newEnv(t, func(c *Config) { c.RetransmitPackets = packets })
	ev.publish(t, "live/a", loadTS(t, "testdata/h264_opus.ts"))

	codecs := DefaultReceiveCodecs()
	for i := range codecs {
		if codecs[i].Kind() == webrtc.MediaKindVideo {
			codecs[i].Feedback = append([]string{"nack"}, codecs[i].Feedback...)
		}
	}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Engine: ev.engine, ListenAddr: "127.0.0.1:0", Codecs: codecs,
		Retransmit: webrtc.RetransmitConfig{Packets: 64}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	_, _ = pc.AddTransceiver(webrtc.MediaKindVideo, webrtc.DirectionRecvOnly)
	_, _ = pc.AddTransceiver(webrtc.MediaKindAudio, webrtc.DirectionRecvOnly)
	var video atomic.Pointer[webrtc.TrackRemote]
	var seq atomic.Int32
	seq.Store(-1)
	pc.OnTrack(func(tr *webrtc.TrackRemote) {
		if tr.Kind() != webrtc.MediaKindVideo {
			return
		}
		tr.OnRTP(func(b []byte) {
			var h rtp.Header
			if _, err := h.Unmarshal(b); err == nil {
				seq.Store(int32(h.SequenceNumber))
			}
		})
		video.Store(tr)
	})
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	offer, err := webrtc.Offer(ctx, pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, answer := ev.request(t, "POST", "/whep/live/a", "application/sdp", offer.SDP, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("play: %d %s", resp.StatusCode, answer)
	}
	rtx := strings.Contains(answer, " rtx/90000") && strings.Contains(answer, "a=ssrc-group:FID ")
	if rtx != (packets >= 0) || strings.Contains(answer, "rtx/48000") {
		t.Fatalf("answer has RTX: %v\n%s", rtx, answer)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "a video packet", func() bool { return video.Load() != nil && seq.Load() >= 0 })

	resent := func() (n uint64) {
		ev.srv.mu.Lock()
		defer ev.srv.mu.Unlock()
		for _, sess := range ev.srv.sessions {
			if sess.snd == nil {
				continue
			}
			sess.snd.mu.Lock()
			for _, o := range sess.snd.tracks {
				p, _ := o.rtp.Retransmitted()
				n += p
			}
			sess.snd.mu.Unlock()
		}
		return n
	}
	nack := func() {
		pkt, _ := (&rtp.NACK{SenderSSRC: 1, MediaSSRC: video.Load().SSRC(), Pairs: []rtp.NACKPair{{PacketID: uint16(seq.Load())}}}).Append(nil)
		_ = pc.WriteRTCP(pkt)
	}
	if packets < 0 {
		nack()
		if n := resent(); n != 0 {
			t.Fatalf("%d packets resent with no history", n)
		}
		return
	}
	waitFor(t, "a retransmission", func() bool {
		nack()
		return resent() > 0
	})
}
