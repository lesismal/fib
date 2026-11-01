package rtc

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

func TestH264Profiles(t *testing.T) {
	cases := []struct {
		id, class string
	}{
		{"42e01f", "cb"}, {"42001f", "42"}, {"4d401f", "cb"}, {"4d001f", "4d"}, {"64001f", "64"},
		{"f4000a", "f4"}, {"42e0", ""}, {"zzzzzz", ""},
	}
	for _, c := range cases {
		if got := h264Class(c.id); got != c.class {
			t.Errorf("%s: %q, want %q", c.id, got, c.class)
		}
	}
	decodes := []struct {
		offered, stream string
		ok              bool
	}{
		{"cb", "cb", true}, {"42", "cb", true}, {"4d", "cb", true}, {"64", "cb", true},
		{"64", "4d", true}, {"cb", "4d", false}, {"4d", "64", false}, {"42", "f4", false},
	}
	for _, d := range decodes {
		if h264Decodes(d.offered, d.stream) != d.ok {
			t.Errorf("%s decodes %s: %v", d.offered, d.stream, !d.ok)
		}
	}
	if profileLevelID(nil) != defaultProfileLevelID || profileLevelID(&av.Track{Config: []byte{1, 0x64, 0, 0x28}}) != "640028" {
		t.Fatal("profileLevelID")
	}
}

func h264(pt uint8, mode, profile string) webrtc.Codec {
	c := webrtc.Codec{MimeType: "video/H264", ClockRate: 90000, PayloadType: pt, Fmtp: "packetization-mode=" + mode}
	if profile != "" {
		c.Fmtp += ";profile-level-id=" + profile
	}
	return c
}

func TestSendCodec(t *testing.T) {
	high := &av.Track{Codec: av.CodecH264, Config: []byte{1, 0x64, 0, 0x28}}
	cb := &av.Track{Codec: av.CodecH264, Config: []byte{1, 0x42, 0xc0, 0x1e}}
	offered := []webrtc.Codec{vp8Codec, h264(100, "0", "64001f"), h264(101, "1", ""), h264(102, "1", "4d001f"), h264(103, "1", "64001f"), h264(104, "1", "42e01f")}
	if c, ok := sendCodec(high, offered); !ok || c.PayloadType != 103 {
		t.Fatalf("high: %+v %v", c, ok)
	}
	if c, ok := sendCodec(cb, offered); !ok || c.PayloadType != 104 {
		t.Fatalf("cb: %+v %v", c, ok)
	}
	// Without its own profile, constrained baseline goes in the first that
	// contains it.
	if c, ok := sendCodec(cb, offered[:4]); !ok || c.PayloadType != 101 {
		t.Fatalf("cb fallback: %+v %v", c, ok)
	}
	if _, ok := sendCodec(high, offered[:3]); ok {
		t.Fatal("high sent as main")
	}
	for _, codec := range []av.CodecID{av.CodecH265, av.CodecOpus, av.CodecPCMU, av.CodecPCMA} {
		tr := &av.Track{Codec: codec}
		c, ok := sendCodec(tr, nil)
		if !ok {
			t.Fatalf("%v not sendable", codec)
		}
		if got, ok := sendCodec(tr, []webrtc.Codec{c}); !ok || got.MimeType != c.MimeType {
			t.Fatalf("%v: not matched", codec)
		}
		if _, ok := sendCodec(tr, []webrtc.Codec{}); ok {
			t.Fatalf("%v: matched nothing", codec)
		}
	}
	for _, codec := range []av.CodecID{av.CodecAAC, av.CodecAV1, av.CodecMP3} {
		if _, ok := sendCodec(&av.Track{Codec: codec}, nil); ok {
			t.Fatalf("%v sendable", codec)
		}
	}
	var dropped []av.CodecID
	outs, codecs := planEgress([]*av.Track{nil, {Codec: av.CodecAAC}, cb, {Codec: av.CodecOpus}}, nil, 1200,
		func(c av.CodecID) { dropped = append(dropped, c) })
	if len(outs) != 2 || len(codecs) != 2 || len(dropped) != 1 || outs[0].hub != 2 {
		t.Fatalf("plan: %d %d %v", len(outs), len(codecs), dropped)
	}
	if outs, _ := planEgress([]*av.Track{cb}, nil, 10, nil); len(outs) != 0 {
		t.Fatal("tiny MTU accepted")
	}
}

func TestReceiveCodecs(t *testing.T) {
	sd := sdp.New(1, 1)
	video := sdp.NewMedia("video", 9, "UDP/TLS/RTP/SAVPF", "96", "97", "98", "99", "100")
	video.AddCodec(toSDP(vp8Codec))
	video.AddCodec(toSDP(h264(97, "1", "42e01f")))
	video.AddCodec(toSDP(h264(98, "2", "42e01f")))
	video.AddCodec(toSDP(h264(99, "1", "42e01f")))
	video.AddCodec(sdp.Codec{PayloadType: 100, Name: "H264", ClockRate: 90000, Fmtp: "sprop-parameter-sets=!!"})
	sd.AddMedia(video)
	audio := sdp.NewMedia("audio", 9, "UDP/TLS/RTP/SAVPF", "111", "0")
	audio.AddCodec(toSDP(opusCodec))
	audio.AddCodec(sdp.Codec{PayloadType: 0, Name: "PCMU", ClockRate: 8000})
	sd.AddMedia(audio)
	sd.AddMedia(sdp.NewDataChannelMedia())
	got := receiveCodecs(sd)
	if len(got) != 3 || got[0].PayloadType != 97 || got[1].PayloadType != 111 || got[2].PayloadType != 0 {
		t.Fatalf("%+v", got)
	}
	if !containsCodec(got, got[0]) || containsCodec(got, vp8Codec) {
		t.Fatal("containsCodec")
	}
}

func TestParseFragment(t *testing.T) {
	f, err := parseFragment("a=ice-options:trickle\r\na=ice-ufrag:abc\r\na=ice-pwd:def\r\n\r\n" +
		"m=audio 9 RTP/AVP 0\r\na=mid:0\r\na=candidate:1 1 udp 1 1.2.3.4 5 typ host\r\na=end-of-candidates\r\n" +
		"m=video 9 RTP/AVP 0\r\na=mid:1\r\na=candidate:2 1 udp 1 1.2.3.4 6 typ host\r\n")
	if err != nil {
		t.Fatal(err)
	}
	want := []webrtc.ICECandidateInit{
		{Candidate: "candidate:1 1 udp 1 1.2.3.4 5 typ host", SDPMid: "0", UsernameFragment: "abc"},
		{SDPMid: "0", UsernameFragment: "abc"},
		{Candidate: "candidate:2 1 udp 1 1.2.3.4 6 typ host", SDPMid: "1", UsernameFragment: "abc"},
	}
	if f.ufrag != "abc" || f.pwd != "def" || len(f.candidates) != len(want) {
		t.Fatalf("%+v", f)
	}
	for i := range want {
		if f.candidates[i] != want[i] {
			t.Fatalf("candidate %d: %+v", i, f.candidates[i])
		}
	}
	f, err = parseFragment("a=end-of-candidates\n")
	if err != nil || len(f.candidates) != 1 || f.candidates[0].SDPMid != "" {
		t.Fatalf("session end: %+v %v", f, err)
	}
	for _, bad := range []string{"x", "ab=c", "a"} {
		if _, err := parseFragment(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	many := ""
	for range maxFragmentCandidates + 1 {
		many += "a=end-of-candidates\r\n"
	}
	if _, err := parseFragment(many); err == nil {
		t.Fatal("too many candidates accepted")
	}
}

func FuzzParseFragment(f *testing.F) {
	f.Add("a=ice-ufrag:abc\r\na=ice-pwd:def\r\nm=audio 9 RTP/AVP 0\r\na=mid:0\r\na=candidate:1 1 udp 1 1.2.3.4 5 typ host\r\na=end-of-candidates\r\n")
	f.Add("a=end-of-candidates")
	f.Add("m=\na=mid:\na=candidate:")
	f.Fuzz(func(t *testing.T, s string) {
		frag, err := parseFragment(s)
		if err == nil && len(frag.candidates) > maxFragmentCandidates {
			t.Fatal("unbounded")
		}
	})
}

func TestHeaders(t *testing.T) {
	for in, want := range map[string]string{"Bearer abc": "abc", "bearer  abc ": "abc", "Basic abc": "", "": "", "Bearer": ""} {
		if got := bearer(in); got != want {
			t.Errorf("bearer(%q) = %q", in, got)
		}
	}
	if a, b := newID(), newID(); len(a) != 32 || a == b {
		t.Fatal("newID")
	}
	if (&StatusError{StatusCode: 404, Body: "x"}).Error() != "rtc: server answered 404: x" {
		t.Fatal("StatusError")
	}
}

// testSender is a sender of an H.264 and an Opus track, with tracks of
// its own that no PeerConnection sends.
func testSender(t *testing.T, limit int) (*sender, []*av.Track) {
	t.Helper()
	streams := []*av.Track{{Codec: av.CodecH264, Config: []byte{1, 0x42, 0xc0, 0x1e}}, {Codec: av.CodecOpus, SampleRate: 48000, Channels: 2}}
	outs, _ := planEgress(streams, nil, 1200, nil)
	s := newSender(limit)
	s.OnTracks(streams)
	s.setTracks(outs)
	return s, streams
}

func avcc(n int) []byte {
	b := make([]byte, 4+n)
	b[3] = byte(n)
	b[4] = 0x41
	return b
}

func TestSenderHold(t *testing.T) {
	s, _ := testSender(t, 100)
	key := &av.Packet{Track: 0, Keyframe: true, Data: avcc(40)}
	delta := &av.Packet{Track: 0, Data: avcc(40)}
	audio := &av.Packet{Track: 1, Keyframe: true, Data: make([]byte, 30)}
	s.OnPacket(&av.Packet{Track: 5})
	s.OnPacket(delta)
	s.OnPacket(key)
	s.OnPacket(audio)
	if len(s.held) != 2 {
		t.Fatalf("held %d", len(s.held))
	}
	// Past the limit, what is held goes and video waits for a keyframe.
	s.OnPacket(delta)
	if len(s.held) != 0 || !s.waitKey {
		t.Fatalf("held %d, waitKey %v", len(s.held), s.waitKey)
	}
	s.OnPacket(delta)
	s.OnPacket(audio)
	if len(s.held) != 1 {
		t.Fatalf("held %d", len(s.held))
	}
	s.OnPacket(key)
	s.OnPacket(audio)
	s.OnPacket(&av.Packet{Track: 1, Keyframe: true, Data: make([]byte, 90)})
	if len(s.held) != 1 || !s.waitKey {
		t.Fatalf("held %d after audio overflow", len(s.held))
	}
	s.OnPacket(key)
	s.start()
	s.start()
	if !s.live || len(s.held) != 0 || s.tracks[0].pk.Sequence() == 0 && s.tracks[0].pk.HasOrigin() == false {
		t.Fatal("not started")
	}
	closed := make(chan error, 1)
	s.onClose = func(err error) { closed <- err }
	s.OnClose(nil)
	<-closed
	s.OnPacket(key)
	s.start()
}

func TestSenderTimes(t *testing.T) {
	s, streams := testSender(t, 1<<20)
	s.start()
	at := func(d time.Duration, track int) {
		s.OnPacket(&av.Packet{Track: track, DTS: d, PTS: d, Keyframe: true, Data: avcc(10)})
	}
	at(5*time.Second, 0)
	if s.lastOut != sendEpoch {
		t.Fatalf("first at %v", s.lastOut)
	}
	at(5*time.Second+40*time.Millisecond, 0)
	// A republish that starts over is rebased onto what was sent.
	s.OnTracks(streams)
	at(0, 0)
	if s.lastOut < sendEpoch+40*time.Millisecond || s.lastOut > sendEpoch+time.Second {
		t.Fatalf("rebased to %v", s.lastOut)
	}
	// A change of tracks that keeps the times keeps them.
	off := s.offset
	s.OnTracks(streams)
	at(40*time.Millisecond, 1)
	if s.offset != off {
		t.Fatal("rebased without a jump")
	}
	// Tracks of another codec go to the track of their codec, or none.
	s.OnTracks([]*av.Track{streams[1], {Codec: av.CodecAAC}, streams[0]})
	if s.byHub[0] != 1 || s.byHub[1] != -1 || s.byHub[2] != 0 {
		t.Fatalf("mapped %v", s.byHub)
	}
	at(time.Second, 1)
	s.close()
	at(time.Second, 0)
}

func TestSenderRTCP(t *testing.T) {
	s, _ := testSender(t, 1<<20)
	ours := func(ssrc uint32) bool { return ssrc == 7 }
	rr := &rtp.ReceiverReport{SSRC: 1, Reports: []rtp.ReceptionReport{{SSRC: 7, FractionLost: 99}, {SSRC: 8, FractionLost: 200}}}
	sr := &rtp.SenderReport{SSRC: 1, Reports: []rtp.ReceptionReport{{SSRC: 7, FractionLost: 3}}}
	b, err := rtp.AppendRTCP(nil, rr)
	if err != nil {
		t.Fatal(err)
	}
	s.onRTCP(ours, b)
	if s.backlog() != 99 {
		t.Fatalf("lost %d", s.backlog())
	}
	b, _ = rtp.AppendRTCP(nil, &rtp.PLI{SenderSSRC: 1, MediaSSRC: 7}, sr)
	s.onRTCP(ours, b)
	if s.backlog() != 3 {
		t.Fatalf("lost %d", s.backlog())
	}
	// Malformed reports are skipped.
	bad := []byte{0x81, rtp.TypeRR, 0, 1, 0, 0, 0, 1}
	s.onRTCP(ours, append(bad, 0x81, rtp.TypeSR, 0, 1, 0, 0, 0, 1))
	s.onRTCP(ours, []byte{1, 2})
	if s.backlog() != 3 {
		t.Fatalf("lost %d", s.backlog())
	}
}

// rtpOf packetizes the av.Packets of track as payload type pt.
func rtpOf(t *testing.T, track *av.Track, pt uint8, pkts ...*av.Packet) [][]byte {
	t.Helper()
	f, err := rtpcodec.NewFormat(track, pt)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := rtpcodec.NewPacketizer(f, rtpcodec.PacketizerConfig{Sequence: 100})
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for _, p := range pkts {
		_ = pk.Packetize(p, func(rp *rtp.Packet) error {
			b, err := rp.Append(nil)
			out = append(out, b)
			return err
		})
	}
	return out
}

func TestReceiver(t *testing.T) {
	m := loadTS(t, "testdata/h264_opus.ts")
	desc, err := sdp.ParseString("v=0\r\no=- 1 1 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 96 102 103\r\na=mid:0\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\n" +
		"a=rtpmap:102 H264/90000\r\na=fmtp:102 packetization-mode=1\r\na=rtpmap:103 H264/90000\r\na=fmtp:103 packetization-mode=1;profile-level-id=64001f\r\n" +
		"m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:1\r\na=recvonly\r\na=rtpmap:111 opus/48000/2\r\n" +
		"m=audio 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:2\r\na=sendonly\r\na=rtpmap:96 VP8/90000\r\n" +
		"m=audio 0 UDP/TLS/RTP/SAVPF 0\r\na=mid:3\r\na=sendonly\r\n")
	if err != nil {
		t.Fatal(err)
	}
	r := newReceiver(recvConfig{pliInterval: time.Millisecond, keyFrameInterval: time.Hour}, desc, true)
	if tr := r.Tracks(); len(tr) != 1 || tr[0].Codec != av.CodecH264 {
		t.Fatalf("tracks %v", tr)
	}
	var tracks [][]*av.Track
	var got []*av.Packet
	r.onTracks = func(tr []*av.Track) { tracks = append(tracks, tr) }
	r.onPacket = func(p *av.Packet) { got = append(got, p) }
	st := r.streams[0]
	f, _ := formatOf(st.codecs[0])
	st.dp, _ = rtpcodec.NewDepacketizer(f, rtpcodec.DepacketizerConfig{})
	st.pt = st.codecs[0].PayloadType
	st.reorder = rtpcodec.NewReorder(0, 0)
	var video []*av.Packet
	for _, p := range m.packets {
		if p.Track == 0 {
			video = append(video, p)
		}
	}
	// Sent with the other payload type the answer has, the packets get a
	// depacketizer of it; one lost has the stream wait for a keyframe.
	pkts := rtpOf(t, m.tracks[0], 103, video...)
	for i, b := range pkts {
		if i == 5 {
			continue
		}
		r.receive(st, b)
	}
	r.receive(st, []byte{1})
	time.Sleep(rtpcodec.DefaultReorderDelay + 20*time.Millisecond)
	r.onTimer()
	snapshot := func() (uint8, int, int, uint64, bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		return st.pt, len(got), len(tracks), st.lost, st.needKey
	}
	pt, n, nt, lost, _ := snapshot()
	if pt != 103 || n < 10 || nt < 1 || lost == 0 {
		t.Fatalf("pt %d, %d packets, %d tracks, lost %d", pt, n, nt, lost)
	}
	if tracks[len(tracks)-1][0].Width != 160 {
		t.Fatalf("track %+v", tracks[len(tracks)-1][0])
	}
	// A payload type the answer does not have is dropped.
	bad := rtpOf(t, m.tracks[0], 120, video[0])
	r.receive(st, bad[0])
	if _, n2, _, _, _ := snapshot(); n2 != n {
		t.Fatal("unknown payload type depacketized")
	}
	r.requestKeyFrame()
	r.startPeriodic()
	r.onTimer()
	r.close()
	r.close()
	r.receive(st, pkts[0])
	r.requestKeyFrame()
	r.startPeriodic()
	r.onTimer()
}

// TestConfigDefaults holds the defaults the Config fields' comments state:
// zero values become the Default constants, and the values that mean
// something when negative keep them.
func TestConfigDefaults(t *testing.T) {
	var c Config
	c.defaults()
	if c.WHIPPrefix != "/whip/" || c.WHEPPrefix != "/whep/" || c.ResourcePrefix != "/rtc/session/" {
		t.Fatalf("prefixes %q %q %q", c.WHIPPrefix, c.WHEPPrefix, c.ResourcePrefix)
	}
	if c.GatherTimeout != 5*time.Second || c.ConnectTimeout != 15*time.Second || c.RepublishWait != 10*time.Second {
		t.Fatalf("timeouts %v %v %v", c.GatherTimeout, c.ConnectTimeout, c.RepublishWait)
	}
	if c.MaxHeldBytes != 8<<20 || c.MaxLoss != 0.25 || c.MTU != 1200 || c.PLIInterval != 500*time.Millisecond || c.MaxOfferBytes != 64<<10 {
		t.Fatalf("limits %d %v %d %v %d", c.MaxHeldBytes, c.MaxLoss, c.MTU, c.PLIInterval, c.MaxOfferBytes)
	}
	c = Config{RepublishWait: -1, MaxLoss: -1}
	c.defaults()
	if c.RepublishWait != -1 || c.MaxLoss != -1 {
		t.Fatalf("negatives became %v %v", c.RepublishWait, c.MaxLoss)
	}
}
