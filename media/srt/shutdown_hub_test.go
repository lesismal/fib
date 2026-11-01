//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"os"
	"testing"
	"time"

	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// A publisher's last frame of each stream, which the demuxer holds until
// the next PES starts, reaches the hub when the publisher leaves.
func TestPublishCloseFlushes(t *testing.T) {
	_, want := source(t)
	sess, _ := connectedSession(t)
	h := hub.New(hub.Config{})
	pub, err := h.Reserve("p", hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	sess.startPublish(h, "p", hub.PublishInfo{}, mpegts.DemuxerConfig{})
	sess.pub = pub
	r := newRecorder()
	if _, err := h.Subscribe("p", r, hub.SubscribeOptions{Wait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("../mpegts/testdata/h264_aac.ts")
	if err != nil {
		t.Fatal(err)
	}
	sess.lock()
	sess.data(b)
	sess.closedConn(ErrPeerClosed)
	sess.unlock()
	if got := r.count(); got != len(want) {
		t.Fatalf("%d packets of %d", got, len(want))
	}
}
