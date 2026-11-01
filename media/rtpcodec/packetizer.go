package rtpcodec

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/webrtc/rtp"
)

// DefaultMTU is the RTP packet size, header included, a Packetizer keeps
// to when PacketizerConfig.MTU is zero: what fits an Ethernet frame inside
// IP, UDP and a tunnel or two, as ffmpeg's 1460 for RTSP over TCP and
// WebRTC's 1200 bracket.
const DefaultMTU = 1400

// minMTU is the smallest MTU a Packetizer takes: room for the header and a
// fragmentation unit's few bytes of headers with payload to spare.
const minMTU = rtp.HeaderSize + 16

// ErrPacket is what a Packetizer reports for an access unit it cannot
// packetize, such as H.264 data that is not AVCC.
var ErrPacket = errors.New("rtpcodec: malformed access unit")

// PacketizerConfig configures a Packetizer.
type PacketizerConfig struct {
	// MTU bounds each RTP packet, header included; 0 means DefaultMTU.
	MTU int
	// SSRC, Sequence and Timestamp are the source, the first sequence
	// number and the RTP timestamp of the media time origin. Zero draws
	// one at random, as RFC 3550 section 5.1 asks of the last two.
	SSRC      uint32
	Sequence  uint16
	Timestamp uint32
}

// payloader splits one access unit into RTP payloads of at most limit bytes.
type payloader interface {
	payload(p *Packetizer, pkt *av.Packet, limit int, emit func(payload []byte, marker bool) error) error
}

// Packetizer turns the av.Packets of one track into RTP packets of its
// format. It is not safe for concurrent use.
//
// A packet's RTP timestamp is the Timestamp of the configuration plus its
// PTS, from the origin, in ticks of the clock rate, modulo 2^32. The origin
// is the PTS of the first packet unless SetOrigin set it before: a sender of
// several tracks sets one origin on all of them, so that their timestamps
// are of one clock and the RTP-Info and sender reports it gives line up.
type Packetizer struct {
	f      *Format
	pl     payloader
	mtu    int
	header rtp.Header

	ts        uint32
	origin    time.Duration
	hasOrigin bool

	packets, octets uint32
	buf             []byte
}

// NewPacketizer returns a Packetizer of format f.
func NewPacketizer(f *Format, config PacketizerConfig) (*Packetizer, error) {
	p := &Packetizer{f: f, mtu: config.MTU, ts: config.Timestamp}
	if p.mtu == 0 {
		p.mtu = DefaultMTU
	}
	if p.mtu < minMTU {
		return nil, errors.New("rtpcodec: MTU too small")
	}
	switch f.Track.Codec {
	case av.CodecH264:
		p.pl = &h264Payloader{}
	case av.CodecH265:
		p.pl = &h265Payloader{}
	case av.CodecAV1:
		p.pl = &av1Payloader{}
	case av.CodecAAC:
		if f.LATM {
			p.pl = latmPayloader{}
		} else {
			p.pl = &aacPayloader{}
		}
	case av.CodecOpus:
		p.pl = framePayloader{}
	case av.CodecPCMU, av.CodecPCMA:
		p.pl = g711Payloader{}
	default:
		return nil, ErrUnsupported
	}
	p.header.PayloadType = f.PayloadType
	p.header.SSRC = config.SSRC
	if p.header.SSRC == 0 {
		p.header.SSRC = randUint32()
	}
	p.header.SequenceNumber = config.Sequence
	if config.Sequence == 0 {
		p.header.SequenceNumber = uint16(randUint32())
	}
	if p.ts == 0 {
		p.ts = randUint32()
	}
	if err := p.SetTrack(f.Track); err != nil {
		return nil, err
	}
	return p, nil
}

func randUint32() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}

// SetTrack replaces the track the packets are of, as when the hub reports
// new tracks after a codec configuration change: H.264 and H.265 take their
// parameter sets from it to send ahead of keyframes. The codec must stay the
// same.
func (p *Packetizer) SetTrack(t *av.Track) error {
	if t.Codec != p.f.Track.Codec {
		return ErrUnsupported
	}
	if s, ok := p.pl.(interface{ setTrack(*av.Track) }); ok {
		s.setTrack(t)
	}
	return nil
}

// SSRC returns the synchronization source of the packets.
func (p *Packetizer) SSRC() uint32 { return p.header.SSRC }

// Sequence returns the sequence number of the next packet.
func (p *Packetizer) Sequence() uint16 { return p.header.SequenceNumber }

// SetOrigin sets the media time whose RTP timestamp is the configured
// Timestamp.
func (p *Packetizer) SetOrigin(t time.Duration) {
	p.origin, p.hasOrigin = t, true
}

// HasOrigin reports whether the origin is set, by SetOrigin or the first
// packet.
func (p *Packetizer) HasOrigin() bool { return p.hasOrigin }

// Timestamp returns the RTP timestamp of media time t.
func (p *Packetizer) Timestamp(t time.Duration) uint32 {
	return p.ts + uint32(av.ToTicks(t-p.origin, p.f.ClockRate))
}

// Packetize turns pkt into RTP packets and calls emit with each, in order.
// The packet, its payload included, is only valid during the call; emit
// marshals it (rtp.Packet.Append) or copies it. An error from emit stops
// Packetize, which returns it.
func (p *Packetizer) Packetize(pkt *av.Packet, emit func(*rtp.Packet) error) error {
	if !p.hasOrigin {
		p.SetOrigin(pkt.PTS)
	}
	p.header.Timestamp = p.Timestamp(pkt.PTS)
	out := rtp.Packet{}
	return p.pl.payload(p, pkt, p.mtu-rtp.HeaderSize, func(payload []byte, marker bool) error {
		out.Header = p.header
		out.Marker = marker
		out.Payload = payload
		p.header.SequenceNumber++
		p.packets++
		p.octets += uint32(len(payload))
		return emit(&out)
	})
}

// Counts returns the packets and the payload octets sent, as an RTCP
// sender report gives them.
func (p *Packetizer) Counts() (packets, octets uint32) { return p.packets, p.octets }

// SenderReport returns the RTCP sender report of the source at wall time
// now, when the media time being sent is t: what ties the RTP timestamps of
// every track to one clock for a receiver.
func (p *Packetizer) SenderReport(now time.Time, t time.Duration) *rtp.SenderReport {
	return &rtp.SenderReport{
		SSRC:        p.header.SSRC,
		NTPTime:     rtp.NTPTime(now),
		RTPTime:     p.Timestamp(t),
		PacketCount: p.packets,
		OctetCount:  p.octets,
	}
}

// scratch returns a buffer of n bytes the payloaders build payloads in,
// reused from one packet to the next.
func (p *Packetizer) scratch(n int) []byte {
	if cap(p.buf) < n {
		p.buf = make([]byte, n)
	}
	return p.buf[:n]
}

// framePayloader sends each frame in one packet, as RFC 7587 has Opus: a
// frame too large for the MTU is sent anyway, since Opus packets are far
// smaller than any MTU and the format has no fragmentation.
type framePayloader struct{}

func (framePayloader) payload(_ *Packetizer, pkt *av.Packet, _ int, emit func([]byte, bool) error) error {
	return emit(pkt.Data, true)
}

// g711Payloader splits samples over as many packets as the MTU takes, each
// timestamped by its first sample, as RFC 3551 section 4.5.14 has them.
type g711Payloader struct{}

func (g711Payloader) payload(p *Packetizer, pkt *av.Packet, limit int, emit func([]byte, bool) error) error {
	channels := max(p.f.Track.Channels, 1)
	limit -= limit % channels
	data := pkt.Data
	ts := p.header.Timestamp
	for len(data) > 0 {
		n := min(len(data), limit)
		p.header.Timestamp = ts
		if err := emit(data[:n], true); err != nil {
			return err
		}
		ts += uint32(n / channels)
		data = data[n:]
	}
	return nil
}
