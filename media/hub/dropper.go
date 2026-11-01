package hub

import "github.com/lesismal/fib/media/av"

// NewDropper wraps next with the slow-consumer policy every egress uses.
// backlog reports how much the consumer has queued and not yet sent, as a
// fib connection's queued bytes; while it is over limit, packets are
// dropped instead of queued. Once a video packet has been dropped, video
// is held back until the next keyframe, since a decoder cannot use the
// frames that depend on a dropped one; audio resumes as soon as the
// backlog is back under the limit. OnTracks and OnClose always pass.
//
// The policy keeps a sink's memory bounded by limit plus one packet
// without ever blocking the publisher; an egress that would rather drop
// the consumer altogether checks its backlog against a second, higher
// limit and closes it.
func NewDropper(next Sink, backlog func() int64, limit int64) Sink {
	return &dropper{next: next, backlog: backlog, limit: limit}
}

type dropper struct {
	next      Sink
	backlog   func() int64
	limit     int64
	video     []bool
	videoWait bool
}

func (d *dropper) OnTracks(tracks []*av.Track) {
	d.video = d.video[:0]
	for _, t := range tracks {
		d.video = append(d.video, t != nil && t.Codec.IsVideo())
	}
	d.next.OnTracks(tracks)
}

func (d *dropper) OnPacket(pkt *av.Packet) {
	video := pkt.Track >= 0 && pkt.Track < len(d.video) && d.video[pkt.Track]
	if d.backlog() > d.limit {
		if video {
			d.videoWait = true
		}
		return
	}
	if video && d.videoWait {
		if !pkt.Keyframe {
			return
		}
		d.videoWait = false
	}
	d.next.OnPacket(pkt)
}

func (d *dropper) OnClose(err error) { d.next.OnClose(err) }
