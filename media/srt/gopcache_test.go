//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// A player starts at the next keyframe, not with the hub's cached GOP,
// whose burst a receiver's UDP buffer could not take, unless
// PlayGOPCache asks for it.
func TestPlayGOPCache(t *testing.T) {
	for _, gop := range []bool{false, true} {
		serverHub := hub.New(hub.Config{})
		_, addr, _ := startServer(t, ServerConfig{Hub: serverHub, PlayGOPCache: gop})
		client := startEngine(t)
		tracks, pkts := source(t)
		pub, err := serverHub.Publish("live/s", tracks, hub.PublishInfo{})
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pkts {
			pub.WritePacket(p)
		}
		var n atomic.Int64
		s, err := Play(client, addr, nil, "", ClientConfig{StreamID: "live/s", Demuxer: mpegts.DemuxerConfig{
			OnTracks: func([]*av.Track) {},
			OnPacket: func(*av.Packet) { n.Add(1) },
		}})
		if err != nil {
			t.Fatal(err)
		}
		if gop {
			waitFor(t, "the cached GOP", func() bool { return n.Load() > 0 })
		} else {
			time.Sleep(500 * time.Millisecond)
			if got := n.Load(); got != 0 {
				t.Fatalf("%d cached packets played", got)
			}
		}
		s.Close()
		pub.Close()
	}
}
