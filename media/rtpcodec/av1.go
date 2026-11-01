package rtpcodec

import (
	"bytes"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/webrtc/rtp"
)

// Bits of the aggregation header of the AV1 RTP payload format (section
// 4.4).
const (
	av1Z = 0x80 // the first OBU element continues a fragment
	av1Y = 0x40 // the last OBU element is continued in the next packet
	av1N = 0x08 // the packet starts a coded video sequence
)

// av1MinFragment is the least room left in a packet that a fragment of
// an OBU is put in rather than in a new packet.
const av1MinFragment = 8

// av1Payloader packetizes AV1 as the AV1 RTP payload format (AOM, v1.0)
// has it: the OBUs of a temporal unit without their size fields, the
// temporal delimiter, tile list and padding OBUs left out, as many to a
// packet as fit and fragmented over packets when they do not, with the
// marker on the last packet of the temporal unit. The sequence header of
// the track goes ahead of every keyframe that does not carry its own, and
// the first packet of a keyframe sets N.
type av1Payloader struct {
	seq   []byte
	buf   []byte
	ends  []int
	elems [][]byte
}

func (a *av1Payloader) setTrack(t *av.Track) {
	a.seq = nil
	var c av1.Config
	if c.Unmarshal(t.Config) == nil {
		a.seq = c.SequenceHeader()
	}
}

func (a *av1Payloader) payload(p *Packetizer, pkt *av.Packet, limit int, emit func([]byte, bool) error) error {
	if !av1.ValidOBUs(pkt.Data) {
		return ErrPacket
	}
	a.buf, a.ends = a.buf[:0], a.ends[:0]
	hasSeq := false
	for o := range av1.SplitOBUs(pkt.Data) {
		hasSeq = hasSeq || o.Type() == av1.OBUSequenceHeader
	}
	if pkt.Keyframe && !hasSeq && a.seq != nil {
		a.add(a.seq)
		hasSeq = true
	}
	for o := range av1.SplitOBUs(pkt.Data) {
		switch o.Type() {
		case av1.OBUTemporalDelimiter, av1.OBUTileList, av1.OBUPadding:
			continue
		}
		a.buf = av1.AppendOBU(a.buf, o, false)
		a.ends = append(a.ends, len(a.buf))
	}
	if len(a.ends) == 0 {
		return nil
	}
	return a.pack(p, limit, pkt.Keyframe && hasSeq, emit)
}

// add appends an OBU to the temporal unit being sent.
func (a *av1Payloader) add(obu []byte) {
	o, _, _ := av1.NextOBU(obu)
	a.buf = av1.AppendOBU(a.buf, o, false)
	a.ends = append(a.ends, len(a.buf))
}

// pack cuts the OBUs of the temporal unit into packets.
func (a *av1Payloader) pack(p *Packetizer, limit int, n bool, emit func([]byte, bool) error) error {
	a.elems = a.elems[:0]
	// size counts the aggregation header and every element with its
	// length, though the last of up to three elements goes without, so a
	// packet may come out a byte or two short of the limit.
	size, z := 1, false
	send := func(y, marker bool) error {
		var h byte
		w := len(a.elems)
		if w <= 3 {
			h = byte(w) << 4
		} else {
			w = 0
		}
		if z {
			h |= av1Z
		}
		if y {
			h |= av1Y
		}
		if n {
			h |= av1N
		}
		buf := append(p.scratch(limit)[:0], h)
		for i, e := range a.elems {
			if w == 0 || i < w-1 {
				buf = av1.AppendLEB128(buf, uint64(len(e)))
			}
			buf = append(buf, e...)
		}
		a.elems, size, z, n = a.elems[:0], 1, y, false
		return emit(buf, marker)
	}
	start := 0
	for _, end := range a.ends {
		obu := a.buf[start:end]
		start = end
		for len(obu) > 0 {
			free := limit - size
			if need := av1.LEB128Size(uint64(len(obu))) + len(obu); need <= free {
				a.elems = append(a.elems, obu)
				size += need
				break
			}
			if len(a.elems) > 0 && free < av1MinFragment {
				if err := send(false, false); err != nil {
					return err
				}
				continue
			}
			k := free - av1.LEB128Size(uint64(free))
			a.elems = append(a.elems, obu[:k])
			obu = obu[k:]
			if err := send(true, false); err != nil {
				return err
			}
		}
	}
	return send(false, true)
}

// av1Depayloader reassembles the temporal units of the AV1 RTP payload
// format into low-overhead OBU streams, every OBU with its size field and
// no temporal delimiter. A temporal unit ends with the marker, or a change
// of timestamp when the marker is lost. The sequence headers it carries
// make the track's configuration, so nothing is emitted before the first.
type av1Depayloader struct {
	d *Depacketizer

	tu      []byte
	auTS    uint32
	auTime  time.Duration
	open    bool
	broken  bool
	waitKey bool
	seq     []byte // the sequence header the temporal unit carried

	frag   []byte
	inFrag bool
}

func newAV1Depayloader(d *Depacketizer) *av1Depayloader {
	return &av1Depayloader{d: d, waitKey: true}
}

func (a *av1Depayloader) loss() {
	a.inFrag = false
	a.lose()
}

// lose drops the temporal unit being assembled, and waits for the next
// keyframe.
func (a *av1Depayloader) lose() {
	if a.open {
		a.broken = true
	}
	a.waitKey = true
}

func (a *av1Depayloader) depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error {
	if len(p.Payload) == 0 {
		return nil
	}
	if a.open && p.Timestamp != a.auTS {
		a.flush()
	}
	if !a.open {
		a.open, a.auTS, a.auTime = true, p.Timestamp, t
	}
	err := a.read(p.Payload)
	if err != nil {
		a.inFrag = false
		a.lose()
	}
	if p.Marker {
		a.flush()
	}
	return err
}

// read takes the OBU elements of one payload.
func (a *av1Depayloader) read(b []byte) error {
	h := b[0]
	z, y, w := h&av1Z != 0, h&av1Y != 0, int(h>>4&3)
	if !z && a.inFrag {
		// The fragment's end never came, though no packet was lost.
		a.inFrag = false
		a.lose()
	}
	b = b[1:]
	for i := 0; len(b) > 0; i++ {
		var e []byte
		if w == 0 || i < w-1 {
			n, k := av1.ReadLEB128(b)
			if k == 0 || n > uint64(len(b)-k) {
				return errPayload
			}
			e, b = b[k:k+int(n)], b[k+int(n):]
		} else {
			e, b = b, nil
		}
		first, last := i == 0, len(b) == 0
		switch {
		case first && z:
			if !a.inFrag {
				// The rest of a fragment whose start was lost.
				continue
			}
			if len(a.frag)+len(e) > a.d.maxAU {
				a.inFrag = false
				return errPayload
			}
			a.frag = append(a.frag, e...)
			if last && y {
				continue
			}
			a.inFrag = false
			if err := a.element(a.frag); err != nil {
				return err
			}
		case last && y:
			if len(e) > a.d.maxAU {
				return errPayload
			}
			a.frag = append(a.frag[:0], e...)
			a.inFrag = true
		default:
			if err := a.element(e); err != nil {
				return err
			}
		}
	}
	return nil
}

// element adds the OBUs of one whole OBU element to the temporal unit:
// one OBU, which may or may not have its size field.
func (a *av1Depayloader) element(e []byte) error {
	for len(e) > 0 {
		o, rest, err := av1.NextOBU(e)
		if err != nil {
			return errPayload
		}
		e = rest
		switch o.Type() {
		case av1.OBUTemporalDelimiter, av1.OBUTileList, av1.OBUPadding:
			continue
		}
		if a.broken {
			continue
		}
		n := len(a.tu)
		a.tu = av1.AppendOBU(a.tu, o, true)
		if len(a.tu) > a.d.maxAU {
			a.broken = true
			continue
		}
		if o.Type() == av1.OBUSequenceHeader {
			a.seq = a.tu[n:]
		}
	}
	return nil
}

// flush ends the temporal unit and emits it when it is whole and usable.
func (a *av1Depayloader) flush() {
	if !a.open {
		return
	}
	tu, broken, seq, t := a.tu, a.broken, a.seq, a.auTime
	a.tu, a.broken, a.seq, a.open = nil, false, nil, false
	if broken {
		a.waitKey = true
		return
	}
	d := a.d
	if seq != nil && !bytes.Equal(seq, d.track.Config[min(len(d.track.Config), 4):]) {
		if c, err := av1.NewConfig(seq); err == nil {
			var s av1.SequenceHeader
			_ = s.Unmarshal(seq) // checked by NewConfig
			t := *d.track
			t.Config, t.Width, t.Height = c.Append(nil), s.Width, s.Height
			d.track = &t
		}
	}
	if len(tu) == 0 || d.track.Config == nil {
		return
	}
	key := av1.IsKeyframe(tu)
	if a.waitKey {
		if !key {
			return
		}
		a.waitKey = false
	}
	d.emit(&av.Packet{DTS: t, PTS: t, Keyframe: key, Data: tu})
}
