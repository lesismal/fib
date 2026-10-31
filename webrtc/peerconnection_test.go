//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

const waitTimeout = 20 * time.Second

func startEngine(t testing.TB) *fib.Engine { return startEngineWith(t, nil) }

// startEngineWith runs an engine whose configuration tune changes first.
func startEngineWith(t testing.TB, tune func(*fib.Config)) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.LogSettings = false
	if tune != nil {
		tune(&config)
	}
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

func loopbackMux(t testing.TB, e *fib.Engine) *Mux {
	t.Helper()
	m, err := NewMux(e, MuxConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func newPC(t testing.TB, config Configuration) *PeerConnection {
	t.Helper()
	pc, err := NewPeerConnection(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// trickle has each side signal its candidates to the other as they come.
func trickle(a, b *PeerConnection) {
	a.OnICECandidate(func(c *ICECandidateInit) {
		if c != nil {
			_ = b.AddICECandidate(*c)
		}
	})
	b.OnICECandidate(func(c *ICECandidateInit) {
		if c != nil {
			_ = a.AddICECandidate(*c)
		}
	})
}

// negotiate runs one offer and answer from a to b.
func negotiate(t testing.TB, a, b *PeerConnection, opts *OfferOptions) {
	t.Helper()
	offer, err := a.CreateOffer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRemoteDescription(offer); err != nil {
		t.Fatalf("answerer SetRemoteDescription: %v\n%s", err, offer.SDP)
	}
	answer, err := b.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	if err := a.SetRemoteDescription(answer); err != nil {
		t.Fatalf("offerer SetRemoteDescription: %v\n%s", err, answer.SDP)
	}
}

// connectedCh reports when pc's PeerConnectionState is connected.
func connectedCh(pc *PeerConnection) <-chan struct{} {
	ch := make(chan struct{})
	var once sync.Once
	pc.OnConnectionStateChange(func(s PeerConnectionState) {
		if s == PeerConnectionStateConnected {
			once.Do(func() { close(ch) })
		}
	})
	return ch
}

func wait(t testing.TB, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// pair makes two connected-to-be PeerConnections, on one engine with a Mux
// each or on two engines.
func pair(t *testing.T, twoEngines bool) (a, b *PeerConnection) {
	e1 := startEngine(t)
	e2 := e1
	if twoEngines {
		e2 = startEngine(t)
	}
	a = newPC(t, Configuration{Mux: loopbackMux(t, e1)})
	b = newPC(t, Configuration{Engine: e2, ListenAddr: "127.0.0.1:0"})
	trickle(a, b)
	return a, b
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// echoChannels has b echo every message on the data channels a opens.
func echoChannels(b *PeerConnection) {
	b.OnDataChannel(func(d *DataChannel) {
		d.OnMessage(func(m DataChannelMessage) {
			if m.IsString {
				_ = d.SendText(string(m.Data))
			} else {
				_ = d.Send(m.Data)
			}
		})
	})
}

// roundTrip sends each message on d and checks it comes back.
func roundTrip(t *testing.T, d *DataChannel, msgs [][]byte) {
	t.Helper()
	got := make(chan []byte, len(msgs))
	d.OnMessage(func(m DataChannelMessage) { got <- m.Data })
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

func openChannel(t *testing.T, pc *PeerConnection, label string) *DataChannel {
	t.Helper()
	d, err := pc.CreateDataChannel(label, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func waitOpen(t *testing.T, d *DataChannel) {
	t.Helper()
	open := make(chan struct{})
	var once sync.Once
	d.OnOpen(func() { once.Do(func() { close(open) }) })
	wait(t, open, "data channel open")
}

func TestDataChannels(t *testing.T) {
	for _, two := range []bool{false, true} {
		t.Run(fmt.Sprintf("twoEngines=%v", two), func(t *testing.T) {
			a, b := pair(t, two)
			echoChannels(b)
			echoChannels(a)
			ca, cb := connectedCh(a), connectedCh(b)
			d := openChannel(t, a, "a")
			negotiate(t, a, b, nil)
			wait(t, ca, "offerer connected")
			wait(t, cb, "answerer connected")
			waitOpen(t, d)
			msgs := [][]byte{[]byte("hello"), randomBytes(1200), randomBytes(64 << 10), randomBytes(200 << 10)}
			roundTrip(t, d, msgs)
			// The answerer opens one too, which the offerer echoes.
			d2 := openChannel(t, b, "b")
			waitOpen(t, d2)
			roundTrip(t, d2, msgs)
			if d.Label() != "a" || d2.Label() != "b" {
				t.Fatal("labels")
			}
		})
	}
}

// fakeRTP writes VP8-ish packets to track until stop closes.
func fakeRTP(track *TrackLocal, pt uint8, stop <-chan struct{}) {
	var seq uint16
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		seq++
		p := rtp.Packet{Header: rtp.Header{PayloadType: pt, SequenceNumber: seq, Timestamp: uint32(seq) * 3000, SSRC: 0x1234, Marker: true},
			Payload: append([]byte{0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a}, bytes.Repeat([]byte{byte(seq)}, 500)...)}
		b, _ := p.Append(nil)
		_ = track.WriteRTP(b)
	}
}

type received struct {
	ssrc  uint32
	pt    uint8
	count atomic.Int64
	first chan struct{}
	once  sync.Once
	track *TrackRemote
}

func collect(pc *PeerConnection) chan *received {
	out := make(chan *received, 4)
	pc.OnTrack(func(tr *TrackRemote) {
		r := &received{first: make(chan struct{}), track: tr}
		tr.OnRTP(func(b []byte) {
			var p rtp.Packet
			if p.Unmarshal(b) != nil {
				return
			}
			r.ssrc, r.pt = p.SSRC, p.PayloadType
			if r.count.Add(1) >= 20 {
				r.once.Do(func() { close(r.first) })
			}
		})
		out <- r
	})
	return out
}

func nextTrack(t *testing.T, ch chan *received) *received {
	t.Helper()
	select {
	case r := <-ch:
		wait(t, r.first, "20 RTP packets")
		return r
	case <-time.After(waitTimeout):
		t.Fatal("no track")
	}
	return nil
}

func TestMedia(t *testing.T) {
	a, b := pair(t, true)
	ta := NewTrackLocal(Codec{MimeType: "video/VP8"}, "va", "sa")
	tb := NewTrackLocal(Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 2}, "ab", "sb")
	sa, err := a.AddTrack(ta)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := b.AddTrack(tb)
	if err != nil {
		t.Fatal(err)
	}
	ra, rb := collect(a), collect(b)
	ca, cb := connectedCh(a), connectedCh(b)
	// b offers here, a has no audio of its own and receives b's on a new
	// transceiver; b receives a's video likewise.
	negotiate(t, b, a, nil)
	// a's video was added before the offer; renegotiate so b gets it.
	wait(t, ca, "a connected")
	wait(t, cb, "b connected")
	negotiate(t, a, b, nil)
	stop := make(chan struct{})
	defer close(stop)
	go fakeRTP(ta, 100, stop)
	go fakeRTP(tb, 100, stop)
	gotB := nextTrack(t, rb)
	gotA := nextTrack(t, ra)
	if gotB.ssrc != sa.SSRC() || gotB.pt != 96 || gotB.track.Kind() != MediaKindVideo || gotB.track.StreamID() != "sa" || gotB.track.ID() != "va" {
		t.Fatalf("b got ssrc %x pt %d kind %s ids %s/%s; a sends ssrc %x", gotB.ssrc, gotB.pt, gotB.track.Kind(), gotB.track.StreamID(), gotB.track.ID(), sa.SSRC())
	}
	if gotA.ssrc != sb.SSRC() || gotA.pt != 111 || gotA.track.Kind() != MediaKindAudio {
		t.Fatalf("a got ssrc %x pt %d; b sends ssrc %x", gotA.ssrc, gotA.pt, sb.SSRC())
	}

	// RTCP: a PLI from b reaches a's sender, and receiver reports flow.
	kf := make(chan struct{}, 1)
	sa.OnKeyFrameRequest(func() {
		select {
		case kf <- struct{}{}:
		default:
		}
	})
	rtcpSeen := make(chan struct{}, 1)
	a.OnRTCP(func(b []byte) {
		for rest := b; len(rest) > 0; {
			h, _, r, err := rtp.NextRTCP(rest)
			if err != nil {
				return
			}
			rest = r
			if h.Type == rtp.TypeRR || h.Type == rtp.TypeSR {
				select {
				case rtcpSeen <- struct{}{}:
				default:
				}
			}
		}
	})
	if err := gotB.track.RequestKeyFrame(); err != nil {
		t.Fatal(err)
	}
	wait(t, kf, "PLI at the sender")
	wait(t, rtcpSeen, "a report")
}

func TestRenegotiateAddTrack(t *testing.T) {
	a, b := pair(t, false)
	echoChannels(b)
	d := openChannel(t, a, "x")
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected")
	waitOpen(t, d)
	rb := collect(b)
	track := NewTrackLocal(Codec{MimeType: "video/VP8"}, "", "")
	s, err := a.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	negotiate(t, a, b, nil)
	stop := make(chan struct{})
	defer close(stop)
	go fakeRTP(track, 96, stop)
	r := nextTrack(t, rb)
	if r.ssrc != s.SSRC() {
		t.Fatalf("ssrc %x, want %x", r.ssrc, s.SSRC())
	}
	// A second track, offered by the other side this time.
	ra := collect(a)
	t2 := NewTrackLocal(Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 2}, "", "")
	if _, err := b.AddTrack(t2); err != nil {
		t.Fatal(err)
	}
	negotiate(t, b, a, nil)
	go fakeRTP(t2, 111, stop)
	nextTrack(t, ra)
	roundTrip(t, d, [][]byte{[]byte("still here")})
}

func TestICERestart(t *testing.T) {
	a, b := pair(t, false)
	echoChannels(b)
	d := openChannel(t, a, "x")
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected")
	waitOpen(t, d)
	before := a.RemoteDescription().SDP
	selections := func() int {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.selections
	}
	selected := selections()
	negotiate(t, a, b, &OfferOptions{ICERestart: true})
	u1, _ := sessionCredentialsOf(t, before)
	u2, _ := sessionCredentialsOf(t, a.RemoteDescription().SDP)
	if u1 == u2 {
		t.Fatal("answer kept its ufrag across the restart")
	}
	deadline := time.Now().Add(waitTimeout)
	for {
		_, remote, ok := a.SelectedCandidatePair()
		if ok && remote != "" && selections() > selected && b.ICEConnectionState() == ICEConnectionStateConnected && a.ICEConnectionState() == ICEConnectionStateConnected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ICE did not reconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	roundTrip(t, d, [][]byte{[]byte("after restart"), randomBytes(50 << 10)})
	// Restart again from the other side.
	negotiate(t, b, a, &OfferOptions{ICERestart: true})
	roundTrip(t, d, [][]byte{[]byte("after second restart")})
}

func TestClose(t *testing.T) {
	a, b := pair(t, false)
	echoChannels(b)
	d := openChannel(t, a, "x")
	cb := connectedCh(b)
	negotiate(t, a, b, nil)
	wait(t, cb, "connected")
	waitOpen(t, d)
	closed := make(chan struct{})
	var once sync.Once
	b.OnConnectionStateChange(func(s PeerConnectionState) {
		if s == PeerConnectionStateClosed {
			once.Do(func() { close(closed) })
		}
	})
	dclosed := make(chan struct{})
	d.OnClose(func() { close(dclosed) })
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	wait(t, dclosed, "local channel closed")
	wait(t, closed, "the peer to see the close")
	if err := d.SendText("x"); err == nil {
		t.Fatal("Send on a closed channel succeeded")
	}
	if err := b.SetRemoteDescription(SessionDescription{Type: SDPTypeAnswer, SDP: "v=0"}); err != ErrSignalingState && err != ErrClosed {
		t.Fatalf("answer in the stable state: %v", err)
	}
	if _, err := a.CreateOffer(nil); err != ErrClosed {
		t.Fatalf("CreateOffer after Close: %v", err)
	}
}

// TestManyPeersOneMux has 200 PeerConnections share one port, as a server
// does, each with a client on a port of its own, and echoes on each.
func TestManyPeersOneMux(t *testing.T) {
	n := 200
	if testing.Short() {
		n = 40
	}
	server := startEngine(t)
	clients := startEngine(t)
	mux := loopbackMux(t, server)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		s := newPC(t, Configuration{Mux: mux, ICELite: true})
		c := newPC(t, Configuration{Engine: clients, ListenAddr: "127.0.0.1:0"})
		echoChannels(s)
		d := openChannel(t, c, "x")
		got := make(chan []byte, 1)
		d.OnMessage(func(m DataChannelMessage) { got <- m.Data })
		d.OnOpen(func() { _ = d.SendText(fmt.Sprint("peer ", i)) })
		wg.Add(1)
		go func() {
			defer wg.Done()
			offer, err := Offer(t.Context(), c, nil)
			if err != nil {
				errs <- err
				return
			}
			answer, err := Answer(t.Context(), s, offer)
			if err != nil {
				errs <- err
				return
			}
			if err := c.SetRemoteDescription(answer); err != nil {
				errs <- err
				return
			}
			select {
			case m := <-got:
				if string(m) != fmt.Sprint("peer ", i) {
					errs <- fmt.Errorf("peer %d got %q", i, m)
				}
			case <-time.After(waitTimeout):
				errs <- fmt.Errorf("peer %d: no echo", i)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := mux.Len(); got != n {
		t.Fatalf("mux serves %d PeerConnections, want %d", got, n)
	}
}

func sessionCredentialsOf(t *testing.T, s string) (string, string) {
	t.Helper()
	sd, err := sdp.ParseString(s)
	if err != nil {
		t.Fatal(err)
	}
	return sessionCredentials(sd)
}
