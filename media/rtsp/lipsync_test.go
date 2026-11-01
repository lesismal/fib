//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"encoding/binary"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc/rtp"
)

// twoAudioSDP announces two G.711 streams, which are published as soon as
// a packet arrives, so their start times show how they were lined up.
const twoAudioSDP = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=x\r\nt=0 0\r\n" +
	"m=audio 0 RTP/AVP 0\r\na=control:streamid=0\r\n" +
	"m=audio 0 RTP/AVP 8\r\na=control:streamid=1\r\n"

func testSR(t *testing.T, wall time.Time, ts uint32) []byte {
	t.Helper()
	b, err := rtp.AppendRTCP(nil,
		&rtp.SenderReport{SSRC: 1, NTPTime: rtp.NTPTime(wall), RTPTime: ts},
		&rtp.SourceDescription{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func g711(pt byte, seq uint16, ts uint32) []byte {
	b := []byte{0x80, pt, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 4}
	binary.BigEndian.PutUint16(b[2:], seq)
	binary.BigEndian.PutUint32(b[4:], ts)
	return b
}

// A recorder's streams start where its sender reports place them, not
// where their first packets happen to arrive: ffmpeg's video reaches the
// server a quarter of a second after the audio sampled with it, which
// arrival times turned into a lasting A/V offset.
func TestRecordLipSync(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	r := dialRaw(t, base)
	expect(t, r.do(MethodAnnounce, base+"sync", "Content-Type: application/sdp", "Body: "+twoAudioSDP), StatusOK)
	expect(t, r.do(MethodSetup, base+"sync/streamid=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=record"), StatusOK)
	expect(t, r.do(MethodSetup, base+"sync/streamid=1", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3;mode=record"), StatusOK)
	expect(t, r.do(MethodRecord, base+"sync"), StatusOK)
	s := newSink()
	if _, err := h.Subscribe("sync", s, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	wall := time.Now()
	var frames []byte
	frames = AppendInterleaved(frames, 1, testSR(t, wall, 1000))
	frames = AppendInterleaved(frames, 3, testSR(t, wall, 50000))
	// Stream 1 first, at its report's time plus 100 ms...
	frames = AppendInterleaved(frames, 2, g711(8, 1, 50000+800))
	r.send(string(frames))
	s.wait(t, "stream 1", func(s *sink) bool { return len(s.pkts) == 1 })
	// ...and stream 0's sample of the same moment 300 ms later.
	time.Sleep(300 * time.Millisecond)
	r.send(string(AppendInterleaved(nil, 0, g711(0, 1, 1000+800))))
	s.wait(t, "stream 0", func(s *sink) bool { return len(s.pkts) == 2 })
	if a, b := s.pkts[0], s.pkts[1]; a.Track != 1 || b.Track != 0 || a.PTS != b.PTS {
		t.Fatalf("stream 1 at %v, stream 0 at %v: not lined up", a.PTS, b.PTS)
	}
}

// The same over UDP, the reports on the client's RTCP ports.
func TestRecordLipSyncUDP(t *testing.T) {
	h := hub.New(hub.Config{})
	base, srv, _ := serve(t, ServerConfig{Hub: h, ReorderDelay: time.Millisecond})
	rtpPort, rtcpPort := srv.UDPPorts()
	listen := func() (*net.UDPConn, string) {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c, strconv.Itoa(c.LocalAddr().(*net.UDPAddr).Port)
	}
	rtp0, p0 := listen()
	rtcp0, c0 := listen()
	rtp1, p1 := listen()
	rtcp1, c1 := listen()
	r := dialRaw(t, base)
	expect(t, r.do(MethodAnnounce, base+"syncu", "Content-Type: application/sdp", "Body: "+twoAudioSDP), StatusOK)
	expect(t, r.do(MethodSetup, base+"syncu/streamid=0", "Transport: RTP/AVP;unicast;client_port="+p0+"-"+c0+";mode=record"), StatusOK)
	expect(t, r.do(MethodSetup, base+"syncu/streamid=1", "Transport: RTP/AVP;unicast;client_port="+p1+"-"+c1+";mode=record"), StatusOK)
	expect(t, r.do(MethodRecord, base+"syncu"), StatusOK)
	s := newSink()
	if _, err := h.Subscribe("syncu", s, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	srvRTP := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rtpPort}
	srvRTCP := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rtcpPort}
	wall := time.Now()
	_, _ = rtcp0.WriteToUDP(testSR(t, wall, 1000), srvRTCP)
	_, _ = rtcp1.WriteToUDP(testSR(t, wall, 50000), srvRTCP)
	time.Sleep(50 * time.Millisecond)
	_, _ = rtp1.WriteToUDP(g711(8, 1, 50000+800), srvRTP)
	s.wait(t, "stream 1", func(s *sink) bool { return len(s.pkts) == 1 })
	time.Sleep(300 * time.Millisecond)
	_, _ = rtp0.WriteToUDP(g711(0, 1, 1000+800), srvRTP)
	s.wait(t, "stream 0", func(s *sink) bool { return len(s.pkts) == 2 })
	if a, b := s.pkts[0], s.pkts[1]; a.PTS != b.PTS {
		t.Fatalf("stream 1 at %v, stream 0 at %v: not lined up", a.PTS, b.PTS)
	}
}

func TestLipSyncOrigin(t *testing.T) {
	wall := time.Unix(1700000000, 0)
	var none senderClock
	var l lipSync
	if at := l.origin(none, 5, 8000, 7*time.Second); at != 7*time.Second || l.set {
		t.Fatal("no report: fallback, no anchor", at)
	}
	a := senderClock{ok: true, ntp: wall, rtp: 1000}
	if at := l.origin(a, 1000, 0, time.Second); at != time.Second || l.set {
		t.Fatal("no clock rate: fallback", at)
	}
	// The first stream with a report anchors at its fallback.
	if at := l.origin(a, 1000+8000, 8000, 3*time.Second); at != 3*time.Second || !l.set {
		t.Fatal("anchor", at)
	}
	// RTP timestamps before the report's, and wrapping, count backwards.
	b := senderClock{ok: true, ntp: wall.Add(500 * time.Millisecond), rtp: 10}
	if at := l.origin(b, 10-90000+1<<32, 90000, 9*time.Second); at != 1500*time.Millisecond {
		t.Fatal("placed", at)
	}
	// Reports placing a stream before zero or too far off are ignored.
	far := senderClock{ok: true, ntp: wall.Add(-time.Hour), rtp: 0}
	if at := l.origin(far, 0, 8000, time.Second); at != time.Second {
		t.Fatal("before zero", at)
	}
	late := senderClock{ok: true, ntp: wall.Add(time.Hour), rtp: 0}
	if at := l.origin(late, 0, 8000, time.Second); at != time.Second {
		t.Fatal("too late", at)
	}
	early := senderClock{ok: true, ntp: wall.Add(-time.Second), rtp: 0}
	if at := l.origin(early, 0, 8000, 20*time.Second); at != 20*time.Second {
		t.Fatal("too early", at)
	}
}

func TestSenderClockUpdate(t *testing.T) {
	var c senderClock
	rr, _ := rtp.AppendRTCP(nil, &rtp.ReceiverReport{SSRC: 1})
	c.update(rr)
	c.update([]byte{0x80, 200, 0, 9})             // length past the end
	c.update([]byte{0x80, 200, 0, 1, 0, 0, 0, 1}) // too short a report
	zero, _ := rtp.AppendRTCP(nil, &rtp.SenderReport{SSRC: 1})
	c.update(zero)
	if c.ok {
		t.Fatal("no sender report taken", c)
	}
	wall := time.Unix(1700000000, 0)
	c.update(append(rr, testSR(t, wall, 77)...))
	if !c.ok || c.rtp != 77 || !c.ntp.Equal(wall) {
		t.Fatal(c)
	}
}

// A client playing from a server that sends no RTP-Info lines the
// streams up by the server's sender reports, rather than starting each at
// the same time whenever its first packet comes.
func TestClientLipSync(t *testing.T) {
	ce := run(t)
	wall := time.Now()
	url := fakeServer(t, func(req *Request) []byte {
		switch req.Method {
		case MethodDescribe:
			return reply(req, 200, nil, twoAudioSDP)
		case MethodSetup:
			ch := "0-1"
			if strings.HasSuffix(req.URL, "streamid=1") {
				ch = "2-3"
			}
			return reply(req, 200, Header{{"Transport", "RTP/AVP/TCP;unicast;interleaved=" + ch}, {"Session", "s"}}, "")
		case MethodPlay:
			b := reply(req, 200, Header{{"Session", "s"}}, "")
			b = AppendInterleaved(b, 1, testSR(t, wall, 1000))
			b = AppendInterleaved(b, 3, testSR(t, wall, 50000))
			b = AppendInterleaved(b, 2, g711(8, 1, 50000+800))
			// Stream 0's first packet, 100 ms after stream 1's by the
			// sender's clock.
			return AppendInterleaved(b, 0, g711(0, 1, 1000+1600))
		}
		return reply(req, 200, Header{{"Session", "s"}}, "")
	})
	s := newSink()
	cfg := s.config()
	cfg.Engine = ce
	if _, err := Play(cfg, url); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "packets", func(s *sink) bool { return len(s.pkts) == 2 })
	if a, b := s.pkts[0], s.pkts[1]; b.PTS-a.PTS != 100*time.Millisecond {
		t.Fatalf("stream 1 at %v, stream 0 at %v: want 100 ms apart", a.PTS, b.PTS)
	}
}
