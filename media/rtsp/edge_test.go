//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"net"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

func TestClientCloseEarly(t *testing.T) {
	ce := run(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	for range 5 {
		s := newSink()
		cfg := s.config()
		cfg.Engine = ce
		c, err := Play(cfg, "rtsp://"+ln.Addr().String()+"/s")
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
		s.wait(t, "close", func(s *sink) bool { return len(s.closed) == 1 })
		if s.closed[0] != ErrClientClosed {
			t.Fatal(s.closed[0])
		}
	}
	// A dial that times out.
	s := newSink()
	cfg := s.config()
	cfg.Engine, cfg.DialTimeout = ce, 50*time.Millisecond
	if _, err := Play(cfg, "rtsp://10.255.255.1/s"); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "dial failure", func(s *sink) bool { return len(s.closed) == 1 })
}

func TestRelayRefused(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	pub.WritePacket(testPackets(1)[0])
	ce := run(t)
	// The relay's path is taken: the client closes.
	local := hub.New(hub.Config{})
	taken, _ := local.Publish("copy", testTracks(t), hub.PublishInfo{})
	defer taken.Close()
	s := newSink()
	cfg := s.config()
	cfg.Engine = ce
	if _, err := PlayToHub(cfg, base+"s", local, "copy"); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "close", func(s *sink) bool { return len(s.closed) == 1 })
	if _, err := PlayToHub(ClientConfig{}, base+"s", local, "x"); err == nil {
		t.Fatal("no engine taken")
	}
	// A push whose client cannot start.
	if _, err := PublishFromHub(ClientConfig{Engine: ce}, "http://x/y", h, "s", hub.SubscribeOptions{}); err == nil {
		t.Fatal("bad URL taken")
	}
	if _, err := PublishFromHub(ClientConfig{Engine: ce}, base+"x", h, "missing", hub.SubscribeOptions{}); err == nil {
		t.Fatal("missing stream taken")
	}
	// A push that waits for its stream, closed before it comes.
	p, err := PublishFromHub(ClientConfig{Engine: ce}, base+"x", h, "later", hub.SubscribeOptions{Wait: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if p.Client() != nil {
		t.Fatal("client before tracks")
	}
	p.Close()
	later, _ := h.Publish("later", testTracks(t), hub.PublishInfo{})
	later.WritePacket(&av.Packet{Keyframe: true, Data: []byte{0, 0, 0, 1, 0x65}})
	later.Close()
}

func TestIsToken(t *testing.T) {
	for _, s := range []string{"", "a b", "a\x7f", "a:b"} {
		if isToken(s) {
			t.Errorf("%q", s)
		}
	}
}
