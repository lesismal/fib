//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc/rtp"
)

// With PlayWait a player outlives its publisher: the next one, with the
// same codec, carries on in the same session, the RTP timestamps going on
// from where they were though its clock starts at 0 again. One with
// another codec ends the player.
func TestPlayerRepublish(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, PlayWait: 5 * time.Second})
	pcmu := []*av.Track{{Codec: av.CodecPCMU, SampleRate: 8000, Channels: 1}}
	pkt := func(d time.Duration) *av.Packet {
		return &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{1, 2, 3, 4}}
	}
	pub, err := h.Publish("rep", pcmu, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	r := dialRaw(t, base)
	expect(t, r.do(MethodDescribe, base+"rep"), StatusOK)
	expect(t, r.do(MethodSetup, base+"rep/trackID=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1"), StatusOK)
	expect(t, r.do(MethodPlay, base+"rep"), StatusOK)
	var stamps []uint32
	next := func() {
		t.Helper()
		for {
			m, ok := r.next(true).(*Interleaved)
			if !ok {
				t.Fatal("no frame")
			}
			if m.Channel == 0 {
				stamps = append(stamps, binary.BigEndian.Uint32(m.Data[4:]))
				return
			}
		}
	}
	// The subscription starts once the PLAY response has gone.
	time.Sleep(100 * time.Millisecond)
	pub.WritePacket(pkt(5 * time.Second))
	pub.WritePacket(pkt(5020 * time.Millisecond))
	next()
	next()
	pub.Close()
	pub, err = h.Publish("rep", pcmu, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	pub.WritePacket(pkt(0))
	pub.WritePacket(pkt(20 * time.Millisecond))
	next()
	next()
	// 160 ticks a 20 ms packet at 8 kHz; the new clock starts after the
	// last packet by as long as the stream was quiet, 20 ms or more.
	if d := stamps[2] - stamps[1]; stamps[1]-stamps[0] != 160 || d < 160 || d > 8000 || stamps[3]-stamps[2] != 160 {
		t.Fatalf("RTP timestamps %v", stamps)
	}
	pub.Close()
	// Another codec: the player ends.
	pub, err = h.Publish("rep", []*av.Track{{Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}}, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	pub.WritePacket(pkt(0))
	for {
		if _, ok := r.next(true).(error); ok {
			break
		}
	}
}

// Sender reports map wall time to RTP time the same way across a
// republish, as ffmpeg stamps packets by them: a player that joined with
// a cached GOP runs ahead of the wall clock, and restarting the mapping at
// the new clock moved ffmpeg's audio a second and more back.
func TestPlayerRepublishReports(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, PlayWait: 5 * time.Second, SenderReportInterval: 20 * time.Millisecond})
	pcmu := []*av.Track{{Codec: av.CodecPCMU, SampleRate: 8000, Channels: 1}}
	pkt := func(d time.Duration) *av.Packet {
		return &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{1, 2, 3, 4}}
	}
	pub, err := h.Publish("rep", pcmu, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	r := dialRaw(t, base)
	expect(t, r.do(MethodDescribe, base+"rep"), StatusOK)
	expect(t, r.do(MethodSetup, base+"rep/trackID=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1"), StatusOK)
	expect(t, r.do(MethodPlay, base+"rep"), StatusOK)
	time.Sleep(100 * time.Millisecond)
	// Two seconds of media at once, as a cached GOP comes.
	pub.WritePacket(pkt(5 * time.Second))
	pub.WritePacket(pkt(7 * time.Second))
	var skew []time.Duration
	var rtp0 uint32
	var ntp0 time.Time
	var media int
	reports := func(n int) {
		t.Helper()
		for len(skew) < n {
			m, ok := r.next(true).(*Interleaved)
			if !ok {
				t.Fatal("no frame after", len(skew), "reports")
			}
			if m.Channel != 1 {
				media++
				continue
			}
			var sr rtp.SenderReport
			if _, first, _, err := rtp.NextRTCP(m.Data); err != nil || sr.Unmarshal(first) != nil {
				t.Fatal("not a sender report", err)
			}
			at := rtp.TimeFromNTP(sr.NTPTime)
			if len(skew) == 0 {
				rtp0, ntp0 = sr.RTPTime, at
			}
			skew = append(skew, av.FromTicks(int64(int32(sr.RTPTime-rtp0)), 8000)-at.Sub(ntp0))
		}
	}
	reports(2)
	pub.Close()
	time.Sleep(100 * time.Millisecond)
	pub, err = h.Publish("rep", pcmu, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	pub.WritePacket(pkt(0))
	// The reports sent while the stream was quiet, then three after the
	// new publisher's first packet.
	for media < 3 {
		reports(len(skew) + 1)
	}
	reports(len(skew) + 3)
	for _, d := range skew {
		if d > 10*time.Millisecond || d < -10*time.Millisecond {
			t.Fatalf("sender reports moved: %v", skew)
		}
	}
}
