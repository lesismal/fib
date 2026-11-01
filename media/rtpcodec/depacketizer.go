package rtpcodec

import (
	"errors"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/webrtc/rtp"
)

// DefaultMaxAccessUnit bounds the access unit a Depacketizer reassembles
// when DepacketizerConfig.MaxAccessUnit is zero: room for an intra frame of
// 4K video at a high bit rate.
const DefaultMaxAccessUnit = 8 << 20

// ErrPayloadType is what Depacketize reports for a packet of another
// payload type than its format's.
var ErrPayloadType = errors.New("rtpcodec: unexpected payload type")

// errPayload is what a depayloader reports for a payload it cannot read.
var errPayload = errors.New("rtpcodec: malformed payload")

// DepacketizerConfig configures a Depacketizer.
type DepacketizerConfig struct {
	// MaxAccessUnit bounds an access unit being reassembled, in bytes; one
	// that grows past it is dropped. Zero means DefaultMaxAccessUnit.
	MaxAccessUnit int
}

// depayloader turns the payloads of one format into access units.
type depayloader interface {
	// depayload takes the payload of p, whose media time is t.
	depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error
	// loss tells the depayloader packets were lost before the next.
	loss()
}

// Depacketizer turns the RTP packets of one format into av.Packets: it
// reassembles access units, recovers their times from RTP timestamps,
// and for video waits for a keyframe at the start and after a loss, since
// a decoder can use nothing until then. It is not safe for concurrent use.
//
// Packets are expected in sequence order, as TCP delivers them or a Reorder
// buffer puts them; a gap in the sequence numbers is a loss, and a packet
// older than the last is dropped as a duplicate.
//
// Media times count from an origin: the RTP timestamp SetOrigin gives, or
// that of the first packet, is media time zero (or the time SetOrigin
// gives), and timestamps are unwrapped from 32 bits from there, so a stream
// may run for as long as it likes. Video decode times are recovered from
// presentation times through the reordering depth the SPS signals (see
// the package documentation); they are never negative, so a receiver that
// wants them exact from the first frame sets an origin later than zero.
type Depacketizer struct {
	f      *Format
	track  *av.Track
	dp     depayloader
	maxAU  int
	emit   func(*av.Packet)
	video  *videoAssembler
	lost   uint64
	seqOK  bool
	seq    uint16
	tsOK   bool
	lastTS uint32
	ext    int64
	origin time.Duration
}

// NewDepacketizer returns a Depacketizer of format f.
func NewDepacketizer(f *Format, config DepacketizerConfig) (*Depacketizer, error) {
	d := &Depacketizer{f: f, track: f.Track, maxAU: config.MaxAccessUnit}
	if d.maxAU <= 0 {
		d.maxAU = DefaultMaxAccessUnit
	}
	switch f.Track.Codec {
	case av.CodecH264:
		d.video = newVideoAssembler(d, h264Codec{})
		d.dp = &h264Depayloader{v: d.video}
	case av.CodecH265:
		d.video = newVideoAssembler(d, h265Codec{})
		d.dp = &h265Depayloader{v: d.video}
	case av.CodecAV1:
		d.dp = newAV1Depayloader(d)
	case av.CodecAAC:
		if f.LATM {
			d.dp = &latmDepayloader{d: d}
		} else {
			if f.SizeLength <= 0 {
				return nil, ErrFormat
			}
			d.dp = &aacDepayloader{d: d}
		}
	case av.CodecOpus, av.CodecPCMU, av.CodecPCMA:
		d.dp = frameDepayloader{}
	default:
		return nil, ErrUnsupported
	}
	return d, nil
}

// Track returns the track of the packets: the format's, until in-band
// parameter sets or an in-band StreamMuxConfig change the configuration,
// when it is a new track. A caller that publishes compares it with the one
// it published after each Depacketize, and publishes the new one before
// the packets Depacketize gave, which are its.
func (d *Depacketizer) Track() *av.Track { return d.track }

// Lost returns how many packets the sequence numbers say were lost.
func (d *Depacketizer) Lost() uint64 { return d.lost }

// SetOrigin makes RTP timestamp ts media time at, as the rtptime of RTSP's
// RTP-Info or the RTCP sender reports of several tracks align them. It
// is called before the first packet.
func (d *Depacketizer) SetOrigin(ts uint32, at time.Duration) {
	d.tsOK, d.lastTS, d.ext, d.origin = true, ts, 0, at
}

// Depacketize takes the next RTP packet and calls emit with each access
// unit it completes; the av.Packet and its data are new, and belong to
// emit. Its Track is 0, for the caller to set. It reports ErrPayloadType for
// a packet of another payload type, and an error for a payload it cannot
// read, which it drops; neither affects what follows.
func (d *Depacketizer) Depacketize(p *rtp.Packet, emit func(*av.Packet)) error {
	if p.PayloadType != d.f.PayloadType {
		return ErrPayloadType
	}
	if d.seqOK {
		diff := int16(p.SequenceNumber - d.seq)
		if diff <= 0 {
			return nil
		}
		if diff > 1 {
			d.lost += uint64(diff - 1)
			d.dp.loss()
		}
	}
	d.seqOK, d.seq = true, p.SequenceNumber
	d.emit = emit
	defer func() { d.emit = nil }()
	return d.dp.depayload(d, p, d.time(p.Timestamp))
}

// Flush emits what is held of an access unit whose marker never came, as
// at the end of a stream.
func (d *Depacketizer) Flush(emit func(*av.Packet)) {
	var f interface{ flush() }
	switch {
	case d.video != nil:
		f = d.video
	case d.f.Track.Codec == av.CodecAV1:
		f = d.dp.(*av1Depayloader)
	default:
		return
	}
	d.emit = emit
	f.flush()
	d.emit = nil
}

// time returns the media time of RTP timestamp ts.
func (d *Depacketizer) time(ts uint32) time.Duration {
	if !d.tsOK {
		d.SetOrigin(ts, 0)
	}
	d.ext += int64(int32(ts - d.lastTS))
	d.lastTS = ts
	return d.origin + av.FromTicks(d.ext, d.f.ClockRate)
}

// emitAudio emits a copy of one audio frame.
func (d *Depacketizer) emitAudio(data []byte, t time.Duration) {
	d.emit(&av.Packet{DTS: t, PTS: t, Keyframe: true, Data: data})
}

// frameDepayloader takes each payload as one frame: Opus and G.711.
type frameDepayloader struct{}

func (frameDepayloader) depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error {
	if len(p.Payload) == 0 {
		return nil
	}
	d.emitAudio(append([]byte(nil), p.Payload...), t)
	return nil
}

func (frameDepayloader) loss() {}
