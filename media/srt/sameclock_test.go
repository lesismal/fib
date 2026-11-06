//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/mpegts"
)

// channelItems returns two items of a channel on one clock, of B-frame
// video (track video, decoded 80 ms before it is shown) and audio (track
// audio): the first from 920 ms (the video's DTS) and 1 s (the audio's) to
// 2 s, the second on from there, its first keyframe decoded before the
// first item's last audio ends. Each item starts with its keyframe.
func channelItems(video, audio int, key, delta, sound []byte) (first, second []*av.Packet) {
	item := func(at time.Duration) []*av.Packet {
		var pkts []*av.Packet
		v, a := 0, 0
		for v < 25 || a < 50 {
			vt := at + time.Duration(v)*40*time.Millisecond - 80*time.Millisecond
			ats := at + time.Duration(a)*20*time.Millisecond
			if v < 25 && (a >= 50 || vt <= ats) {
				p := &av.Packet{Track: video, DTS: vt, PTS: vt + 80*time.Millisecond, Keyframe: v == 0, Data: delta}
				if v == 0 {
					p.Data = key
				}
				pkts = append(pkts, p)
				v++
				continue
			}
			pkts = append(pkts, &av.Packet{Track: audio, DTS: ats, PTS: ats, Keyframe: true, Data: sound})
			a++
		}
		return pkts
	}
	return item(time.Second), item(2 * time.Second)
}

// A player of a stream whose tracks change on the same clock, as a
// channel's next item or a codec change makes, gets the publisher's times
// as they are, though the next item's video is decoded before the last
// item's audio ends; a republish restarting the clock is still moved on
// after the last time.
func TestPlaySameClock(t *testing.T) {
	sess, sim := connectedSession(t)
	sess.conn.cfg.Output = func(b []byte) { sim.send(true, b) }
	sink := playSink{sess}
	tracks, pkts := source(t)
	video, audio := 0, 1
	if !tracks[0].Codec.IsVideo() {
		video, audio = 1, 0
	}
	var key, delta, sound []byte
	for _, p := range pkts {
		switch {
		case p.Track == video && p.Keyframe && key == nil:
			key = p.Data
		case p.Track == video && !p.Keyframe && delta == nil:
			delta = p.Data
		case p.Track == audio && sound == nil:
			sound = p.Data
		}
	}
	first, second := channelItems(video, audio, key, delta, sound)
	sess.lock()
	sess.startPlaying(sim.now)
	sess.unlock()
	sink.OnTracks(tracks)
	for _, p := range first {
		sink.OnPacket(p)
	}
	// The next item: a video track of its own.
	next := slices.Clone(tracks)
	v := *next[video]
	next[video] = &v
	sink.OnTracks(next)
	for _, p := range second {
		sink.OnPacket(p)
	}
	// A republish from 0.
	sink.OnTracks(tracks)
	for _, p := range first {
		q := *p
		q.DTS -= 920 * time.Millisecond
		q.PTS -= 920 * time.Millisecond
		sink.OnPacket(&q)
	}
	sim.run(time.Second)
	var got []*av.Packet
	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
		OnTracks: func([]*av.Track) {},
		OnPacket: func(p *av.Packet) { got = append(got, p) },
	})
	for _, b := range sim.server.data {
		d.Write(b)
	}
	d.Flush()
	sent := append(slices.Clone(first), second...)
	if len(got) < len(sent)+len(first)-2 {
		t.Fatalf("%d packets of %d", len(got), len(sent)+len(first))
	}
	// The demuxer gives each track's packets in order; the times sent
	// are the publisher's, as the demuxer starts them (at off).
	byTrack := func(pkts []*av.Packet, track int) []*av.Packet {
		var out []*av.Packet
		for _, p := range pkts {
			if p.Track == track {
				out = append(out, p)
			}
		}
		return out
	}
	gotVideo := byTrack(got, video)
	gotAudio := byTrack(got, audio)
	sentVideo := byTrack(sent, video)
	sentAudio := byTrack(sent, audio)
	off := gotVideo[0].DTS - sentVideo[0].DTS
	for i, w := range sentVideo {
		if g := gotVideo[i]; g.DTS != w.DTS+off || g.PTS != w.PTS+off {
			t.Fatalf("video %d at %v/%v, sent at %v/%v", i, g.DTS, g.PTS, w.DTS+off, w.PTS+off)
		}
	}
	for i, w := range sentAudio {
		if g := gotAudio[i]; g.DTS != w.DTS+off {
			t.Fatalf("audio %d at %v, sent at %v", i, g.DTS, w.DTS+off)
		}
	}
	end := max(sentVideo[len(sentVideo)-1].DTS, sentAudio[len(sentAudio)-1].DTS) + off
	if g := gotVideo[len(sentVideo)]; g.DTS < end+20*time.Millisecond || g.DTS > end+time.Second {
		t.Fatalf("republish at %v after %v", g.DTS, end)
	}
	for _, list := range [][]*av.Packet{gotVideo, gotAudio} {
		for i := 1; i < len(list); i++ {
			if list[i].DTS <= list[i-1].DTS {
				t.Fatalf("track %d back from %v to %v", list[i].Track, list[i-1].DTS, list[i].DTS)
			}
		}
	}
}
