//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// startEngine runs an engine with the given bindings until the test ends.
func startEngine(t *testing.T, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	e, err := fib.New(cfg, bindings...)
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

// startServer runs a Server on a loopback UDP port and returns its address.
func startServer(t *testing.T, cfg ServerConfig) (*Server, string, *fib.Engine) {
	t.Helper()
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e := startEngine(t, fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: srv})
	addrs, err := e.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	return srv, addrs[0].(*net.UDPAddr).String(), e
}

// source is a stream of av packets from an ffmpeg capture.
func source(t *testing.T) ([]*av.Track, []*av.Packet) {
	t.Helper()
	b, err := os.ReadFile("../mpegts/testdata/h264_aac.ts")
	if err != nil {
		t.Fatal(err)
	}
	var tracks []*av.Track
	var pkts []*av.Packet
	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
		OnTracks: func(t []*av.Track) { tracks = t },
		OnPacket: func(p *av.Packet) { pkts = append(pkts, p) },
	})
	d.Write(b)
	d.Flush()
	return tracks, pkts
}

// recorder is a hub.Sink that keeps what it hears.
type recorder struct {
	mu      sync.Mutex
	tracks  []*av.Track
	packets []*av.Packet
	closed  error
	ch      chan struct{}
}

func newRecorder() *recorder { return &recorder{ch: make(chan struct{}, 1)} }

func (r *recorder) OnTracks(t []*av.Track) {
	r.mu.Lock()
	r.tracks = t
	r.mu.Unlock()
}

func (r *recorder) OnPacket(p *av.Packet) {
	r.mu.Lock()
	r.packets = append(r.packets, p)
	r.mu.Unlock()
	select {
	case r.ch <- struct{}{}:
	default:
	}
}

func (r *recorder) OnClose(err error) {
	r.mu.Lock()
	r.closed = err
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.packets)
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type closeWait struct {
	ch chan error
}

func newCloseWait() *closeWait { return &closeWait{ch: make(chan error, 1)} }

func (c *closeWait) fn(err error) { c.ch <- err }

func (c *closeWait) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-c.ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("no OnClose")
		return nil
	}
}

// TestPublishPlay pushes a stream into the server with Publish, plays it
// back with Play, and checks every packet makes the round trip, in the
// clear and encrypted.
func TestPublishPlay(t *testing.T) {
	for _, pass := range []string{"", "0123456789abc"} {
		t.Run("passphrase="+pass, func(t *testing.T) {
			serverHub := hub.New(hub.Config{})
			srv, addr, _ := startServer(t, ServerConfig{Hub: serverHub, Passphrase: pass, PBKeyLen: 32, Latency: 40 * time.Millisecond, PlayWait: 5 * time.Second})
			client := startEngine(t)
			tracks, pkts := source(t)

			srcHub := hub.New(hub.Config{})
			pub, err := srcHub.Publish("src", tracks, hub.PublishInfo{})
			if err != nil {
				t.Fatal(err)
			}
			atServer := newRecorder()
			if _, err := serverHub.Subscribe("live/s", atServer, hub.SubscribeOptions{Wait: 5 * time.Second}); err != nil {
				t.Fatal(err)
			}
			connected := make(chan *Session, 1)
			pusher, err := Publish(client, addr, srcHub, "src", ClientConfig{
				StreamID: "#!::r=live/s,m=publish", Passphrase: pass, Latency: 40 * time.Millisecond,
				OnConnect: func(s *Session) { connected <- s },
			})
			if err != nil {
				t.Fatal(err)
			}
			// Half the packets before the handshake ends, which the session
			// holds, half after.
			half := len(pkts) / 2
			for _, p := range pkts[:half] {
				pub.WritePacket(p)
			}
			<-connected
			for _, p := range pkts[half:] {
				pub.WritePacket(p)
			}
			waitFor(t, "the published packets", func() bool { return atServer.count() >= len(pkts)-1 })

			// Play it back into another hub.
			playHub := hub.New(hub.Config{})
			played := newRecorder()
			if _, err := playHub.Subscribe("copy", played, hub.SubscribeOptions{Wait: 5 * time.Second}); err != nil {
				t.Fatal(err)
			}
			player, err := Play(client, addr, playHub, "copy", ClientConfig{StreamID: "live/s", Passphrase: pass, Latency: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, "the player", func() bool { return len(srv.Sessions()) == 2 })
			// The player starts from the server's GOP cache; then more.
			for range 3 {
				for _, p := range pkts {
					q := *p
					q.DTS += 2 * time.Second
					q.PTS += 2 * time.Second
					pub.WritePacket(&q)
				}
			}
			waitFor(t, "the played packets", func() bool { return played.count() >= 2*len(pkts) })
			played.mu.Lock()
			if len(played.tracks) != 2 || played.tracks[0].Codec != av.CodecH264 || played.tracks[1].Codec != av.CodecAAC {
				t.Fatalf("tracks %v", played.tracks)
			}
			played.mu.Unlock()
			for _, s := range srv.Sessions() {
				if s.Remote().Port() == 0 || s.Latency() != 40*time.Millisecond {
					t.Fatalf("session %v latency %v", s.Remote(), s.Latency())
				}
				if s.Stats().PacketsSent+s.Stats().PacketsReceived == 0 {
					t.Fatal("session stats")
				}
			}
			if pusher.StreamID().Mode != ModePublish || player.StreamID().Resource != "live/s" {
				t.Fatal("client stream IDs")
			}
			if pusher.Backlog() < 0 {
				t.Fatal("backlog")
			}

			// The publisher leaving ends the player's stream on the
			// server; closing the server closes the player.
			pusher.Close()
			waitFor(t, "the publisher to leave", func() bool { return len(srv.Sessions()) == 1 })
			srv.Close()
			waitFor(t, "the server to close", func() bool { return len(srv.Sessions()) == 0 })
			pub.Close()
		})
	}
}

// TestRejected covers the handshake refusals a client hears.
func TestRejected(t *testing.T) {
	serverHub := hub.New(hub.Config{})
	_, addr, _ := startServer(t, ServerConfig{
		Hub:      serverHub,
		MaxConns: 1,
		OnConnect: func(req *Request, id StreamID) error {
			switch id.User {
			case "mallory":
				return RejectUnauthorized
			case "eve":
				return errors.New("no")
			}
			return nil
		},
	})
	client := startEngine(t)
	cases := []struct {
		sid, pass string
		want      error
	}{
		{"live/missing", "", RejectNotFound},
		{"#!::r=a,u=mallory", "", RejectUnauthorized},
		{"#!::r=a,u=eve", "", RejectForbidden},
		{"#!::m=publish", "", RejectBadRequest},
		{"#!::r=a,m=bidirectional", "", RejectBadMode},
		{"#!::r=a,m=publish", "0123456789abc", RejectUnsecure},
	}
	for _, tc := range cases {
		cw := newCloseWait()
		if _, err := Dial(client, addr, ClientConfig{StreamID: tc.sid, Passphrase: tc.pass, OnClose: cw.fn}); err != nil {
			t.Fatal(err)
		}
		if err := cw.wait(t); err != tc.want {
			t.Fatalf("%s: %v, want %v", tc.sid, err, tc.want)
		}
	}
	// MaxConns: one publisher holds the only place.
	cw := newCloseWait()
	ok := make(chan struct{})
	first, err := Dial(client, addr, ClientConfig{StreamID: "#!::r=a,m=publish", OnConnect: func(*Session) { close(ok) }})
	if err != nil {
		t.Fatal(err)
	}
	<-ok
	if _, err := Dial(client, addr, ClientConfig{StreamID: "#!::r=b,m=publish", OnClose: cw.fn}); err != nil {
		t.Fatal(err)
	}
	if err := cw.wait(t); err != RejectOverload {
		t.Fatal(err)
	}
	first.Close()
	// A publisher of a busy stream is rejected in the handshake.
	_, err = serverHub.Publish("busy", []*av.Track{{Codec: av.CodecAAC}}, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	cw = newCloseWait()
	if _, err := Dial(client, addr, ClientConfig{StreamID: "#!::r=busy,m=publish", OnClose: cw.fn}); err != nil {
		t.Fatal(err)
	}
	if err := cw.wait(t); err != RejectConflict {
		t.Fatal(err)
	}
}

// TestPublishReserved covers a second SRT publisher of a path whose first
// has not sent its tracks yet: the first reserved the path in its
// handshake, so the second is rejected in its own, and the first goes
// live once it sends.
func TestPublishReserved(t *testing.T) {
	serverHub := hub.New(hub.Config{})
	_, addr, _ := startServer(t, ServerConfig{Hub: serverHub})
	client := startEngine(t)
	ok := make(chan struct{})
	first, err := Dial(client, addr, ClientConfig{StreamID: "#!::r=live/r,m=publish", OnConnect: func(*Session) { close(ok) }})
	if err != nil {
		t.Fatal(err)
	}
	<-ok
	if infos := serverHub.Streams(); len(infos) != 1 || !infos[0].Reserved || infos[0].Publishing {
		t.Fatalf("%+v", infos)
	}
	cw := newCloseWait()
	if _, err := Dial(client, addr, ClientConfig{StreamID: "#!::r=live/r,m=publish", OnClose: cw.fn}); err != nil {
		t.Fatal(err)
	}
	if err := cw.wait(t); err != RejectConflict {
		t.Fatal(err)
	}
	tracks, pkts := source(t)
	m, _ := mpegts.NewMuxer(tracks, mpegts.MuxerConfig{})
	var ts []byte
	for _, p := range pkts {
		ts, _ = m.WritePacket(ts, p)
	}
	for len(ts) > 0 {
		n := min(len(ts), TSPayloadSize)
		_ = first.Write(ts[:n])
		ts = ts[n:]
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		infos := serverHub.Streams()
		if len(infos) == 1 && infos[0].Publishing && len(infos[0].Tracks) == len(tracks) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%+v", infos)
		}
		time.Sleep(10 * time.Millisecond)
	}
	first.Close()
	deadline = time.Now().Add(5 * time.Second)
	for len(serverHub.Streams()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%+v", serverHub.Streams())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestPublishTaken covers a publisher OnSession takes: the path it
// reserved is released.
func TestPublishTaken(t *testing.T) {
	serverHub := hub.New(hub.Config{})
	taken := make(chan struct{})
	_, addr, _ := startServer(t, ServerConfig{Hub: serverHub, OnSession: func(*Session) bool {
		close(taken)
		return true
	}})
	client := startEngine(t)
	if _, err := Dial(client, addr, ClientConfig{StreamID: "#!::r=t,m=publish"}); err != nil {
		t.Fatal(err)
	}
	<-taken
	deadline := time.Now().Add(5 * time.Second)
	for len(serverHub.Streams()) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%+v", serverHub.Streams())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPublishReject(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want RejectReason
	}{
		{hub.ErrStreamBusy, RejectConflict},
		{hub.ErrLimit, RejectOverload},
		{hub.ErrClosed, RejectClose},
		{RejectUnauthorized, RejectUnauthorized},
		{errors.New("no"), RejectForbidden},
	} {
		if got := publishReject(tc.err); got != tc.want {
			t.Fatal(tc.err, got)
		}
	}
}

// TestOnSession runs an echo server on raw sessions.
func TestOnSession(t *testing.T) {
	_, addr, _ := startServer(t, ServerConfig{
		Latency: 20 * time.Millisecond,
		OnSession: func(s *Session) bool {
			s.SetOnData(func(b []byte) {
				// Echo from a goroutine: OnData runs with the session's
				// mutex held.
				b = append([]byte(nil), b...)
				go func() { _ = s.Write(b) }()
			})
			return true
		},
	})
	client := startEngine(t)
	got := make(chan string, 4)
	ok := make(chan struct{})
	cw := newCloseWait()
	s, err := Dial(client, addr, ClientConfig{
		StreamID:  "#!::r=echo,m=bidirectional",
		OnConnect: func(*Session) { close(ok) },
		OnData:    func(b []byte) { got <- string(b) },
		OnClose:   cw.fn,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("early")); err != ErrNotConnected {
		t.Fatal(err)
	}
	<-ok
	if err := s.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m != "hello" {
			t.Fatal(m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no echo")
	}
	s.Close()
	if err := cw.wait(t); err != nil {
		t.Fatal(err)
	}
}

// TestPlayCallbacks plays into Demuxer callbacks rather than a hub, and
// covers the client's argument errors.
func TestPlayCallbacks(t *testing.T) {
	serverHub := hub.New(hub.Config{})
	_, addr, _ := startServer(t, ServerConfig{Hub: serverHub})
	client := startEngine(t)
	tracks, pkts := source(t)
	pub, err := serverHub.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var n int
	s, err := Play(client, addr, nil, "", ClientConfig{StreamID: "live/s", Demuxer: mpegts.DemuxerConfig{
		OnTracks: func([]*av.Track) {},
		OnPacket: func(*av.Packet) { mu.Lock(); n++; mu.Unlock() },
	}})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the player", func() bool { return s.Stats().PacketsReceived > 0 || true })
	time.Sleep(200 * time.Millisecond)
	for _, p := range pkts {
		pub.WritePacket(p)
	}
	waitFor(t, "the packets", func() bool { mu.Lock(); defer mu.Unlock(); return n > len(pkts)/2 })
	pub.Close() // the player's session ends with the stream
	waitFor(t, "the session to end", func() bool { return s.Write([]byte{1}) == ErrClosed })

	if _, err := Play(client, addr, nil, "", ClientConfig{}); err != errNoPlayTarget {
		t.Fatal(err)
	}
	if _, err := Dial(client, "no such host:x", ClientConfig{}); err == nil {
		t.Fatal("bad address")
	}
	if _, err := Dial(client, addr, ClientConfig{Passphrase: "short"}); err == nil {
		t.Fatal("bad passphrase")
	}
	if _, err := Publish(client, addr, serverHub, "missing", ClientConfig{}); !errors.Is(err, hub.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := NewServer(ServerConfig{}); err != errNoHub {
		t.Fatal(err)
	}
}
