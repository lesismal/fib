package flv

import "github.com/lesismal/fib/media/av"

// Muxer writes an av stream as an FLV file: the file header, onMetaData
// and the sequence headers when the stream starts, then a tag per packet.
// It appends to a buffer the caller gives and owns, so a server streaming
// to many clients reuses one buffer per client and allocates nothing per
// packet.
//
// FLV files start at timestamp 0, and players such as flv.js wait for
// the gap otherwise: the Muxer subtracts the first packet's decode time
// from every timestamp, and keeps them increasing across a republish (see
// Timeline).
type Muxer struct {
	enc Encoder
	tl  Timeline
}

// NewMuxer returns a Muxer.
func NewMuxer(config EncoderConfig) *Muxer {
	return &Muxer{enc: Encoder{config: config, video: -1, audio: -1}, tl: Timeline{FromZero: true}}
}

// Encoder returns the Encoder the Muxer writes tags with.
func (m *Muxer) Encoder() *Encoder { return &m.enc }

// AppendHeader appends the file header and the tags a player needs before
// the packets, for the stream's tracks, and returns the indices of the
// tracks FLV leaves out.
func (m *Muxer) AppendHeader(dst []byte, tracks []*av.Track) ([]byte, []int) {
	dropped := m.enc.SetTracks(tracks)
	video, audio := m.enc.Tracks()
	dst = AppendHeader(dst, Header{HasAudio: audio != nil, HasVideo: video != nil})
	return m.appendHeaders(dst), dropped
}

// AppendTracks appends the tags that announce tracks that changed
// mid-stream, each at the time of the last packet of its track (metadata
// at the last of either), and returns the indices of the tracks FLV leaves
// out. The tracks may come from a new publisher, whose clock the packets
// after them follow (Timeline.Restart).
func (m *Muxer) AppendTracks(dst []byte, tracks []*av.Track) ([]byte, []int) {
	dropped := m.enc.SetTracks(tracks)
	m.tl.Restart()
	return m.appendHeaders(dst), dropped
}

func (m *Muxer) appendHeaders(dst []byte) []byte {
	m.enc.Headers(func(typ uint8, body []byte) {
		dst = AppendTag(dst, typ, m.tl.HeaderStamp(typ), body)
	})
	return dst
}

// AppendPacket appends the tag of pkt, or nothing for a packet of a track
// the Muxer leaves out.
func (m *Muxer) AppendPacket(dst []byte, pkt *av.Packet) ([]byte, error) {
	if pkt.Track != m.enc.video && pkt.Track != m.enc.audio || pkt.Track < 0 {
		return dst, nil
	}
	ts := m.tl.Stamp(pkt.Track == m.enc.video, pkt.DTS)
	start := len(dst)
	dst, _ = beginTag(dst, 0, ts)
	dst, typ, err := m.enc.AppendPacket(dst, pkt)
	if err != nil {
		return dst[:start], err
	}
	dst[start] = typ
	return endTag(dst, start), nil
}
