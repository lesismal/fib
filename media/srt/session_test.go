//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"net"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// connectedSession returns a session around the caller Conn of a connected
// simulation, without a fib connection: what it sends is dropped.
func connectedSession(t *testing.T) (*Session, *sim) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	sess := newSession(s.addr, 1<<20)
	sess.conn = s.caller.conn
	sess.conn.cfg.Output = sess.output
	sess.conn.cfg.OnClose = sess.closedConn
	sess.conn.cfg.OnData = sess.data
	return sess, s
}

// TestPlaySinkEdges drives a player's sink directly: packets held while
// connecting (bounded, then from the next keyframe), new tracks mid-stream,
// and packets of tracks MPEG-TS cannot carry.
func TestPlaySinkEdges(t *testing.T) {
	sess, _ := connectedSession(t)
	sink := playSink{sess}
	tracks, pkts := source(t)
	sink.OnTracks(tracks)
	// Not ready: held, up to maxPending, then from the next keyframe.
	for i := range maxPending + 5 {
		p := *pkts[1]
		p.Keyframe = false
		if i == maxPending+2 {
			p = *pkts[0] // a video keyframe
		}
		sink.OnPacket(&p)
	}
	if len(sess.pending) != 3 || !sess.pending[0].Keyframe {
		t.Fatalf("%d pending", len(sess.pending))
	}
	sess.lock()
	sess.startPlaying(time.Now())
	sess.unlock()
	if st := sess.conn.Stats(); st.PacketsSent == 0 {
		t.Fatalf("nothing sent: %+v mux %v", st, sess.mux != nil)
	}
	// New tracks while playing: a new muxer; a track of PCMU only is not
	// carried at all.
	sink.OnTracks([]*av.Track{{Codec: av.CodecPCMU}})
	if sess.mux != nil {
		t.Fatal("muxer for PCMU")
	}
	sent := sess.conn.Stats().PacketsSent
	sink.OnPacket(&av.Packet{Data: []byte{1}})
	sink.OnTracks(tracks)
	sink.OnPacket(&av.Packet{Track: 0, Data: []byte{0, 0, 0, 9}}) // not AVCC
	if sess.conn.Stats().PacketsSent != sent {
		t.Fatal("sent what cannot be muxed")
	}
	// A stream that ends closes the session.
	sink.OnClose(hub.ErrUnpublished)
	if sess.conn.State() != StateClosed {
		t.Fatal("not closed")
	}
	sink.OnPacket(pkts[0]) // after close: ignored
}

// TestPublishEdges: new tracks from the demuxer republish them, and a
// session closed meanwhile does not publish.
func TestPublishEdges(t *testing.T) {
	sess, _ := connectedSession(t)
	h := hub.New(hub.Config{})
	sess.startPublish(h, "p", hub.PublishInfo{}, mpegts.DemuxerConfig{})
	tracks, pkts := source(t)
	sess.lock()
	sess.pubTracks(tracks)
	sess.pubPacket(pkts[0])
	sess.pubTracks(tracks[:1]) // SetTracks
	sess.unlock()
	if st := h.Streams(); len(st) != 1 || len(st[0].Tracks) != 1 {
		t.Fatal("tracks not replaced")
	}
	sess.Close()
	sess.lock()
	sess.pub = nil
	sess.pubTracks(tracks)
	sess.unlock()
	if len(h.Streams()) != 0 {
		t.Fatal("published after close")
	}
	// A session that never got a Conn gives up its subscription.
	other := newSession(sess.remote, 0)
	pub, _ := h.Publish("q", tracks, hub.PublishInfo{})
	other.sub, _ = h.Subscribe("q", other.sink(), hub.SubscribeOptions{})
	other.abandon()
	if h.Streams()[0].Subscribers != 0 {
		t.Fatal("subscription kept")
	}
	// The sink of a session without a Conn closes it once it has one.
	other.sub, _ = h.Subscribe("q", other.sink(), hub.SubscribeOptions{})
	pub.Close()
	if !other.closeSet {
		t.Fatal("close not remembered")
	}
}

// TestCallerRestart: a caller that starts again from the same address
// with a new socket replaces its old session.
func TestCallerRestart(t *testing.T) {
	h := hub.New(hub.Config{})
	srv, addr, _ := startServer(t, ServerConfig{Hub: h})
	ua, _ := net.ResolveUDPAddr("udp", addr)
	uc, err := net.DialUDP("udp", nil, ua)
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	connect := func(seed byte) *Conn {
		var c *Conn
		connected := false
		c, err := NewCaller(Config{
			StreamID: "#!::r=x,m=publish", Rand: newRand(seed),
			Output:    func(b []byte) { _, _ = uc.Write(b) },
			OnConnect: func() { connected = true },
		}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 2000)
		for !connected {
			_ = uc.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
			n, err := uc.Read(buf)
			if err != nil {
				c.HandleTimeout(time.Now())
				if c.State() == StateClosed {
					t.Fatal(c.Err())
				}
				continue
			}
			c.Handle(buf[:n], time.Now())
		}
		return c
	}
	first := connect(10)
	waitFor(t, "the first session", func() bool { return len(srv.Sessions()) == 1 })
	old := srv.Sessions()[0]
	second := connect(11)
	if first.SocketID() == second.SocketID() {
		t.Fatal("same socket")
	}
	waitFor(t, "the second session", func() bool {
		ss := srv.Sessions()
		return len(ss) == 1 && ss[0] != old
	})
	// fib closing the connection ends the session.
	sess := srv.Sessions()[0]
	sess.lock()
	fc := sess.fc
	sess.unlock()
	srv.OnClose(fc, net.ErrClosed)
	waitFor(t, "the session to end", func() bool { return len(srv.Sessions()) == 0 })
	srv.OnClose(fc, nil) // no session: nothing to do
	srv.OnOpen(nil)
	srv.OnPriorityData(nil, nil)
	clientHandler{}.OnOpen(nil)
	clientHandler{}.OnPriorityData(nil, nil)
}
