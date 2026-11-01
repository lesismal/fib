//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// TestPlayReserves covers Play into a hub: the path is reserved before
// the handshake, with the stream ID's query in the PublishInfo, so a busy
// path fails Play at once; the stream goes live with the tracks, and a
// session that never got a Conn gives the reservation back.
func TestPlayReserves(t *testing.T) {
	serverHub := hub.New(hub.Config{})
	_, addr, _ := startServer(t, ServerConfig{Hub: serverHub, PlayWait: 5 * time.Second})
	client := startEngine(t)

	var mu sync.Mutex
	var infos []hub.PublishInfo
	playHub := hub.New(hub.Config{OnPublish: func(path string, info hub.PublishInfo) error {
		mu.Lock()
		infos = append(infos, info)
		mu.Unlock()
		return nil
	}})
	player, err := Play(client, addr, playHub, "copy", ClientConfig{StreamID: "live/s?token=abc"})
	if err != nil {
		t.Fatal(err)
	}
	st := playHub.Streams()
	if len(st) != 1 || !st[0].Reserved || st[0].Publishing {
		t.Fatalf("streams after Play: %+v", st)
	}
	mu.Lock()
	if len(infos) != 1 || infos[0].Protocol != "srt" || infos[0].Query != "token=abc" || !infos[0].Remote.IsValid() {
		t.Fatalf("publish infos %+v", infos)
	}
	mu.Unlock()

	// A second pull of the same path is refused before it dials.
	if _, err := Play(client, addr, playHub, "copy", ClientConfig{StreamID: "live/s"}); !errors.Is(err, hub.ErrStreamBusy) {
		t.Fatalf("second Play: %v", err)
	}
	// So is a publish of it, while the player has no tracks yet.
	if _, err := playHub.Publish("copy", []*av.Track{{Codec: av.CodecAAC}}, hub.PublishInfo{}); !errors.Is(err, hub.ErrStreamBusy) {
		t.Fatalf("Publish of a reserved path: %v", err)
	}

	// The stream goes live once the server sends the tracks.
	tracks, pkts := source(t)
	pub, err := serverHub.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the player to go live", func() bool {
		for range 2 {
			for _, p := range pkts {
				pub.WritePacket(p)
			}
		}
		st := playHub.Streams()
		return len(st) == 1 && st[0].Publishing
	})
	player.Close()
	waitFor(t, "the reservation to go", func() bool { return len(playHub.Streams()) == 0 })
	pub.Close()

	// A session that never got a Conn gives the reservation up.
	s := newSession(player.remote, 0)
	s.pub, err = playHub.Reserve("lost", hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	s.abandon()
	if len(playHub.Streams()) != 0 || s.pub != nil {
		t.Fatal("reservation kept")
	}
}

// TestPublishQuery covers Publish's SubscribeInfo: the stream ID's query
// reaches the source hub's OnSubscribe, which can refuse the push.
func TestPublishQuery(t *testing.T) {
	serverHub := hub.New(hub.Config{})
	_, addr, _ := startServer(t, ServerConfig{Hub: serverHub})
	client := startEngine(t)
	errDenied := errors.New("denied")
	var got hub.SubscribeInfo
	srcHub := hub.New(hub.Config{OnSubscribe: func(path string, info hub.SubscribeInfo) error {
		got = info
		if info.Query != "token=abc" {
			return errDenied
		}
		return nil
	}})
	pub, err := srcHub.Publish("src", []*av.Track{{Codec: av.CodecAAC, SampleRate: 48000, Channels: 2, Config: []byte{0x11, 0x90}}}, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	s, err := Publish(client, addr, srcHub, "src", ClientConfig{StreamID: "live/q?token=abc"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Protocol != "srt" || got.Query != "token=abc" || !got.Remote.IsValid() {
		t.Fatalf("subscribe info %+v", got)
	}
	s.Close()
	if _, err := Publish(client, addr, srcHub, "src", ClientConfig{StreamID: "live/q?token=bad"}); !errors.Is(err, errDenied) {
		t.Fatalf("refused push: %v", err)
	}
}
