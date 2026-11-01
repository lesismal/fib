//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/mpegts"
)

// A player waiting through a republish whose clock starts again sees its
// MPEG-TS times carry on, rather than step back with no discontinuity
// signalled, which ffmpeg reported as non-monotonic DTS.
func TestPlayRepublishTimeline(t *testing.T) {
	sess, sim := connectedSession(t)
	sess.conn.cfg.Output = func(b []byte) { sim.send(true, b) }
	sink := playSink{sess}
	tracks, pkts := source(t)
	sess.lock()
	sess.startPlaying(sim.now)
	sess.unlock()
	sink.OnTracks(tracks)
	var end time.Duration
	for _, p := range pkts {
		sink.OnPacket(p)
		end = max(end, p.DTS)
	}
	// The next publisher, its clock from the start again.
	sink.OnTracks(tracks)
	for _, p := range pkts {
		sink.OnPacket(p)
	}
	sim.run(time.Second)
	var last [2]time.Duration
	var seen [2]bool
	n := 0
	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
		OnTracks: func([]*av.Track) {},
		OnPacket: func(p *av.Packet) {
			n++
			if seen[p.Track] && p.DTS < last[p.Track] {
				t.Fatalf("packet %d of track %d back from %v to %v", n, p.Track, last[p.Track], p.DTS)
			}
			seen[p.Track], last[p.Track] = true, p.DTS
		},
	})
	for _, b := range sim.server.data {
		d.Write(b)
	}
	d.Flush()
	if n < len(pkts) || last[0] <= end {
		t.Fatalf("%d packets, ending at %v", n, last[0])
	}
}
