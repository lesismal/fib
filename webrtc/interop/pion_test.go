package interop

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib"
	fibrtp "github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/turn"
	"github.com/lesismal/fib/webrtc/turn/turnserver"

	fibwebrtc "github.com/lesismal/fib/webrtc"
	"github.com/pion/ice/v4"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const waitTimeout = 20 * time.Second

func startEngine(t testing.TB) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.LogSettings = false
	e, err := fib.New(config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	t.Cleanup(func() {
		e.Stop()
		<-done
		_ = e.Close()
	})
	return e
}

// fibPC is a full ICE agent on a loopback port of its own.
func fibPC(t *testing.T, e *fib.Engine, tune func(*fibwebrtc.Configuration)) *fibwebrtc.PeerConnection {
	t.Helper()
	config := fibwebrtc.Configuration{Engine: e, ListenAddr: "127.0.0.1:0"}
	if tune != nil {
		tune(&config)
	}
	pc, err := fibwebrtc.NewPeerConnection(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// pionPC gathers on loopback over UDP only, as fib's side does, without
// mDNS.
func pionPC(t *testing.T, config webrtc.Configuration) *webrtc.PeerConnection {
	t.Helper()
	var se webrtc.SettingEngine
	se.SetIncludeLoopbackCandidate(true)
	se.SetInterfaceFilter(func(name string) bool { return strings.HasPrefix(name, "lo") })
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	var me webrtc.MediaEngine
	if err := me.RegisterDefaultCodecs(); err != nil {
		t.Fatal(err)
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(se), webrtc.WithMediaEngine(&me))
	pc, err := api.NewPeerConnection(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

func wait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// closer closes its channel once.
type closer struct {
	ch   chan struct{}
	once sync.Once
}

func newCloser() *closer             { return &closer{ch: make(chan struct{})} }
func (c *closer) close()             { c.once.Do(func() { close(c.ch) }) }
func (c *closer) C() <-chan struct{} { return c.ch }

func fibConnected(pc *fibwebrtc.PeerConnection) <-chan struct{} {
	c := newCloser()
	pc.OnConnectionStateChange(func(s fibwebrtc.PeerConnectionState) {
		if s == fibwebrtc.PeerConnectionStateConnected {
			c.close()
		}
	})
	return c.C()
}

func pionConnected(pc *webrtc.PeerConnection) <-chan struct{} {
	c := newCloser()
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			c.close()
		}
	})
	return c.C()
}

// pionEcho has pc echo every message on the data channels its peer opens.
func pionEcho(pc *webrtc.PeerConnection) {
	pc.OnDataChannel(func(d *webrtc.DataChannel) {
		d.OnMessage(func(m webrtc.DataChannelMessage) {
			if m.IsString {
				_ = d.SendText(string(m.Data))
			} else {
				_ = d.Send(m.Data)
			}
		})
	})
}

func fibEcho(pc *fibwebrtc.PeerConnection) {
	pc.OnDataChannel(func(d *fibwebrtc.DataChannel) {
		d.OnMessage(func(m fibwebrtc.DataChannelMessage) {
			if m.IsString {
				_ = d.SendText(string(m.Data))
			} else {
				_ = d.Send(m.Data)
			}
		})
	})
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// messages are what each data channel test sends, up to 60 KiB.
func messages() [][]byte {
	return [][]byte{[]byte("hello pion"), randomBytes(1000), randomBytes(16 << 10), randomBytes(60 << 10)}
}

// fibRoundTrip sends each message on d, open, and checks it comes back.
func fibRoundTrip(t *testing.T, d *fibwebrtc.DataChannel) {
	t.Helper()
	msgs := messages()
	got := make(chan []byte, len(msgs))
	d.OnMessage(func(m fibwebrtc.DataChannelMessage) { got <- m.Data })
	for _, m := range msgs {
		if err := d.Send(m); err != nil {
			t.Fatalf("Send %d bytes: %v", len(m), err)
		}
	}
	for i, m := range msgs {
		select {
		case g := <-got:
			if !bytes.Equal(g, m) {
				t.Fatalf("message %d: got %d bytes, want %d", i, len(g), len(m))
			}
		case <-time.After(waitTimeout):
			t.Fatalf("message %d of %d bytes did not come back", i, len(m))
		}
	}
}

// fibOpen opens a data channel on pc and reports when it is open.
func fibOpen(t *testing.T, pc *fibwebrtc.PeerConnection, label string) (*fibwebrtc.DataChannel, <-chan struct{}) {
	t.Helper()
	d, err := pc.CreateDataChannel(label, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := newCloser()
	d.OnOpen(c.close)
	return d, c.C()
}

// fibSend writes VP8-ish packets to track until stop closes, one every
// 10 ms, as a camera would.
func fibSend(track *fibwebrtc.TrackLocal, stop <-chan struct{}) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for seq := uint16(1); ; seq++ {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		p := fibrtp.Packet{Header: fibrtp.Header{PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 900, SSRC: 1, Marker: true},
			Payload: append([]byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a}, bytes.Repeat([]byte{byte(seq)}, 500)...)}
		b, _ := p.Append(nil)
		_ = track.WriteRTP(b)
	}
}

// pionSend writes Opus-sized packets to track until stop closes, one
// every 20 ms.
func pionSend(track *webrtc.TrackLocalStaticRTP, stop <-chan struct{}) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for seq := uint16(1); ; seq++ {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		_ = track.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: uint32(seq) * 960, SSRC: 1},
			Payload: bytes.Repeat([]byte{byte(seq)}, 80)})
	}
}

// fibReceived counts what fib receives on its first remote track.
type fibReceived struct {
	ssrc, pt atomic.Uint32
	n        atomic.Int64
	kind     atomic.Value
	enough   *closer
}

func fibCollect(pc *fibwebrtc.PeerConnection) *fibReceived {
	r := &fibReceived{enough: newCloser()}
	pc.OnTrack(func(tr *fibwebrtc.TrackRemote) {
		r.kind.Store(tr.Kind())
		tr.OnRTP(func(b []byte) {
			var p fibrtp.Packet
			if p.Unmarshal(b) != nil || len(p.Payload) != 80 {
				return
			}
			r.ssrc.Store(p.SSRC)
			r.pt.Store(uint32(p.PayloadType))
			if r.n.Add(1) >= 50 {
				r.enough.close()
			}
		})
	})
	return r
}

// pionReceived counts what pion receives on its first remote track.
type pionReceived struct {
	mu     sync.Mutex
	codec  webrtc.RTPCodecParameters
	ssrc   webrtc.SSRC
	pt     uint8
	n      int
	enough *closer
}

func pionCollect(pc *webrtc.PeerConnection) *pionReceived {
	r := &pionReceived{enough: newCloser()}
	pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			p, _, err := tr.ReadRTP()
			if err != nil {
				return
			}
			if len(p.Payload) != 506 {
				continue
			}
			r.mu.Lock()
			r.codec, r.ssrc, r.pt = tr.Codec(), tr.SSRC(), p.PayloadType
			r.n++
			if r.n >= 50 {
				r.enough.close()
			}
			r.mu.Unlock()
		}
	})
	return r
}

// media is the fib video track to pion and the pion audio track to fib
// of one test.
type media struct {
	fibTrack  *fibwebrtc.TrackLocal
	fibSender *fibwebrtc.RTPSender
	pionTrack *webrtc.TrackLocalStaticRTP
	pionSSRC  webrtc.SSRC
	atFib     *fibReceived
	atPion    *pionReceived
}

// check sends both tracks until each side has 50 packets, then checks
// each arrived with the SSRC its sender signaled and the payload type
// the answer gave its codec.
func (m *media) check(t *testing.T, pionSender *webrtc.RTPSender) {
	t.Helper()
	stop := make(chan struct{})
	defer close(stop)
	go fibSend(m.fibTrack, stop)
	go pionSend(m.pionTrack, stop)
	wait(t, m.atPion.enough.C(), "50 VP8 packets at pion")
	wait(t, m.atFib.enough.C(), "50 Opus packets at fib")
	m.atPion.mu.Lock()
	defer m.atPion.mu.Unlock()
	if !strings.EqualFold(m.atPion.codec.MimeType, webrtc.MimeTypeVP8) || uint32(m.atPion.ssrc) != m.fibSender.SSRC() ||
		m.atPion.pt != uint8(m.atPion.codec.PayloadType) {
		t.Fatalf("pion got %s pt %d ssrc %x; fib sends ssrc %x", m.atPion.codec.MimeType, m.atPion.pt, m.atPion.ssrc, m.fibSender.SSRC())
	}
	ssrc := pionSender.GetParameters().Encodings[0].SSRC
	if got := m.atFib.ssrc.Load(); got != uint32(ssrc) || m.atFib.kind.Load() != fibwebrtc.MediaKindAudio {
		t.Fatalf("fib got ssrc %x kind %v; pion sends ssrc %x", got, m.atFib.kind.Load(), ssrc)
	}
	if pt := m.atFib.pt.Load(); pt != 111 {
		t.Fatalf("fib got Opus at payload type %d, want 111", pt)
	}
}

func newPionAudio(t *testing.T) *webrtc.TrackLocalStaticRTP {
	t.Helper()
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "pion")
	if err != nil {
		t.Fatal(err)
	}
	return track
}

// TestFibOffersPionAnswers has fib offer a data channel, a VP8 track and
// to receive audio, without trickle: the offer and the answer carry every
// candidate. pion answers with an Opus track and echoes the data channel.
func TestFibOffersPionAnswers(t *testing.T) {
	e := startEngine(t)
	f := fibPC(t, e, nil)
	p := pionPC(t, webrtc.Configuration{})
	pionEcho(p)
	m := &media{fibTrack: fibwebrtc.NewTrackLocal(fibwebrtc.Codec{MimeType: "video/VP8"}, "video", "fib"), pionTrack: newPionAudio(t)}
	var err error
	if m.fibSender, err = f.AddTrack(m.fibTrack); err != nil {
		t.Fatal(err)
	}
	if _, err := f.AddTransceiver(fibwebrtc.MediaKindAudio, fibwebrtc.DirectionRecvOnly); err != nil {
		t.Fatal(err)
	}
	m.atFib, m.atPion = fibCollect(f), pionCollect(p)
	d, open := fibOpen(t, f, "echo")
	fc, pc := fibConnected(f), pionConnected(p)

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	offer, err := fibwebrtc.Offer(ctx, f, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer.SDP}); err != nil {
		t.Fatalf("pion SetRemoteDescription: %v\n%s", err, offer.SDP)
	}
	pionSender, err := p.AddTrack(m.pionTrack)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := p.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(p)
	if err := p.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	wait(t, gathered, "pion's candidates")
	if err := f.SetRemoteDescription(fibwebrtc.SessionDescription{Type: fibwebrtc.SDPTypeAnswer, SDP: p.LocalDescription().SDP}); err != nil {
		t.Fatalf("fib SetRemoteDescription: %v\n%s", err, p.LocalDescription().SDP)
	}
	wait(t, fc, "fib connected")
	wait(t, pc, "pion connected")
	wait(t, open, "data channel open")
	fibRoundTrip(t, d)
	m.check(t, pionSender)
}

// TestPionOffersFibAnswers has pion offer a data channel, an Opus track
// and to receive video, trickling its candidates, as fib trickles its own
// back. fib answers with a VP8 track and echoes the data channel.
func TestPionOffersFibAnswers(t *testing.T) {
	e := startEngine(t)
	f := fibPC(t, e, nil)
	p := pionPC(t, webrtc.Configuration{})
	ready := trickle(t, f, p)
	fibEcho(f)
	m := &media{fibTrack: fibwebrtc.NewTrackLocal(fibwebrtc.Codec{MimeType: "video/VP8"}, "video", "fib"), pionTrack: newPionAudio(t)}
	pionSender, err := p.AddTrack(m.pionTrack)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	m.atFib, m.atPion = fibCollect(f), pionCollect(p)
	d, err := p.CreateDataChannel("echo", nil)
	if err != nil {
		t.Fatal(err)
	}
	echoed := make(chan []byte, 4)
	d.OnMessage(func(msg webrtc.DataChannelMessage) { echoed <- msg.Data })
	open := newCloser()
	d.OnOpen(open.close)
	fc, pc := fibConnected(f), pionConnected(p)

	offer, err := p.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if err := f.SetRemoteDescription(fibwebrtc.SessionDescription{Type: fibwebrtc.SDPTypeOffer, SDP: offer.SDP}); err != nil {
		t.Fatalf("fib SetRemoteDescription: %v\n%s", err, offer.SDP)
	}
	if m.fibSender, err = f.AddTrack(m.fibTrack); err != nil {
		t.Fatal(err)
	}
	answer, err := f.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	if err := p.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.SDP}); err != nil {
		t.Fatalf("pion SetRemoteDescription: %v\n%s", err, answer.SDP)
	}
	ready()
	wait(t, fc, "fib connected")
	wait(t, pc, "pion connected")
	wait(t, open.C(), "data channel open")
	for _, msg := range messages() {
		if err := d.Send(msg); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-echoed:
			if !bytes.Equal(got, msg) {
				t.Fatalf("got %d bytes back, want %d", len(got), len(msg))
			}
		case <-time.After(waitTimeout):
			t.Fatalf("%d bytes did not come back", len(msg))
		}
	}
	m.check(t, pionSender)
}

// trickle has each side signal its candidates to the other as they come.
// pion takes none before its remote description, unlike fib, so fib's
// wait until ready is called, after pion has it.
func trickle(t *testing.T, f *fibwebrtc.PeerConnection, p *webrtc.PeerConnection) (ready func()) {
	var mu sync.Mutex
	var pending []webrtc.ICECandidateInit
	held := true
	add := func(c webrtc.ICECandidateInit) {
		if err := p.AddICECandidate(c); err != nil {
			t.Errorf("pion AddICECandidate %q: %v", c.Candidate, err)
		}
	}
	f.OnICECandidate(func(c *fibwebrtc.ICECandidateInit) {
		if c == nil {
			return
		}
		mid, index := c.SDPMid, c.SDPMLineIndex
		init := webrtc.ICECandidateInit{Candidate: c.Candidate, SDPMid: &mid, SDPMLineIndex: &index}
		mu.Lock()
		if held {
			pending = append(pending, init)
			mu.Unlock()
			return
		}
		mu.Unlock()
		add(init)
	})
	p.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		fc := fibwebrtc.ICECandidateInit{Candidate: init.Candidate}
		if init.SDPMid != nil {
			fc.SDPMid = *init.SDPMid
		}
		if init.SDPMLineIndex != nil {
			fc.SDPMLineIndex = *init.SDPMLineIndex
		}
		if err := f.AddICECandidate(fc); err != nil {
			t.Errorf("fib AddICECandidate %q: %v", fc.Candidate, err)
		}
	})
	return func() {
		mu.Lock()
		held = false
		list := pending
		pending = nil
		mu.Unlock()
		for _, c := range list {
			add(c)
		}
	}
}

// TestTURNRelay has fib and pion gather relay candidates only, through
// fib's turnserver, so that everything between them goes through it: the
// data channel and both tracks.
func TestTURNRelay(t *testing.T) {
	te := startEngine(t)
	cfg := turnserver.Config{RelayIP: netip.MustParseAddr("127.0.0.1")}
	cfg.Realm = "fib.test"
	cfg.Auth = turn.StaticAuth(cfg.Realm, map[string]string{"user": "pass"})
	cfg.AllowedPeers = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	server, err := turnserver.Listen(te, "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	addr, err := te.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	url := fmt.Sprintf("turn:%s", addr.AddrPort())

	e := startEngine(t)
	f := fibPC(t, e, func(c *fibwebrtc.Configuration) {
		c.ICEServers = []fibwebrtc.ICEServer{{URLs: []string{url}, Username: "user", Credential: "pass"}}
		c.ICETransportPolicy = fibwebrtc.ICETransportPolicyRelay
	})
	p := pionPC(t, webrtc.Configuration{
		ICEServers:         []webrtc.ICEServer{{URLs: []string{url}, Username: "user", Credential: "pass"}},
		ICETransportPolicy: webrtc.ICETransportPolicyRelay,
	})
	ready := trickle(t, f, p)
	pionEcho(p)
	m := &media{fibTrack: fibwebrtc.NewTrackLocal(fibwebrtc.Codec{MimeType: "video/VP8"}, "video", "fib"), pionTrack: newPionAudio(t)}
	if m.fibSender, err = f.AddTrack(m.fibTrack); err != nil {
		t.Fatal(err)
	}
	if _, err := f.AddTransceiver(fibwebrtc.MediaKindAudio, fibwebrtc.DirectionRecvOnly); err != nil {
		t.Fatal(err)
	}
	m.atFib, m.atPion = fibCollect(f), pionCollect(p)
	d, open := fibOpen(t, f, "relayed")
	fc, pc := fibConnected(f), pionConnected(p)

	offer, err := f.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if err := p.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer.SDP}); err != nil {
		t.Fatal(err)
	}
	ready()
	pionSender, err := p.AddTrack(m.pionTrack)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := p.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	if err := f.SetRemoteDescription(fibwebrtc.SessionDescription{Type: fibwebrtc.SDPTypeAnswer, SDP: answer.SDP}); err != nil {
		t.Fatal(err)
	}
	wait(t, fc, "fib connected through the relay")
	wait(t, pc, "pion connected through the relay")
	wait(t, open, "data channel open")
	fibRoundTrip(t, d)
	m.check(t, pionSender)
	local, remote, ok := f.SelectedCandidatePair()
	if !ok || !strings.Contains(local, "typ relay") || !strings.Contains(remote, "typ relay") {
		t.Fatalf("selected pair %q -> %q, want relay to relay", local, remote)
	}
	if n := server.Allocations(); n != 2 {
		t.Fatalf("%d allocations, want 2", n)
	}
}
