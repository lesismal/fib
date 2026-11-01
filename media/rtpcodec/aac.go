package rtpcodec

import (
	"bytes"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/internal/bitio"
	"github.com/lesismal/fib/webrtc/rtp"
)

// aacFrameDuration returns the duration of one access unit of the AAC
// config, or that of 1024 samples at the clock rate without one.
func aacFrameDuration(config []byte, clockRate int) time.Duration {
	var c aac.Config
	if c.Unmarshal(config) == nil {
		if d := c.FrameDuration(); d > 0 {
			return d
		}
	}
	return av.FromTicks(1024, clockRate)
}

// aacPayloader packetizes AAC as mpeg4-generic (RFC 3640) with the AU
// headers of the format, AAC-hbr's 13-bit size and 3-bit index: one access
// unit a packet, or the fragments of one that does not fit, each with the
// AU header of the whole and the marker on the last.
type aacPayloader struct{}

func (aacPayloader) payload(p *Packetizer, pkt *av.Packet, limit int, emit func([]byte, bool) error) error {
	f := p.f
	bits := f.SizeLength + f.IndexLength
	hdr := 2 + (bits+7)/8
	if len(pkt.Data) >= 1<<f.SizeLength || hdr >= limit {
		return ErrPacket
	}
	w := bitio.NewWriter(p.scratch(limit)[:0])
	w.Bits(uint64(bits), 16)
	w.Bits(uint64(len(pkt.Data)), f.SizeLength)
	w.Bits(0, f.IndexLength)
	head := w.Bytes()
	data := pkt.Data
	for {
		n := min(len(data), limit-hdr)
		b := append(head[:hdr], data[:n]...)
		data = data[n:]
		if err := emit(b, len(data) == 0); err != nil {
			return err
		}
		if len(data) == 0 {
			return nil
		}
	}
}

// aacDepayloader reads mpeg4-generic AAC: the AU headers of the format,
// several access units a packet, and an access unit fragmented over
// packets. The CTS and DTS deltas, random access and stream state fields
// and the auxiliary section are skipped; interleaving by AU index is not
// undone.
type aacDepayloader struct {
	d     *Depacketizer
	frame time.Duration
	sizes []int
	// frag is a fragmented access unit, of fragSize bytes, its fragments
	// timestamped fragTS.
	frag     []byte
	fragSize int
	fragTS   uint32
	fragTime time.Duration
	inFrag   bool
}

func (a *aacDepayloader) loss() { a.inFrag = false }

func (a *aacDepayloader) depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error {
	if a.frame == 0 {
		a.frame = aacFrameDuration(d.track.Config, d.f.ClockRate)
	}
	f := d.f
	r := bitio.NewReader(p.Payload)
	headersLen := int(r.Bits(16))
	if r.Err() != nil || headersLen > r.Left() {
		a.inFrag = false
		return errPayload
	}
	a.sizes = a.sizes[:0]
	for read := 0; read < headersLen; {
		start := r.Pos()
		size := int(r.Bits(f.SizeLength))
		if len(a.sizes) == 0 {
			r.Skip(f.IndexLength)
		} else {
			r.Skip(f.IndexDeltaLength)
		}
		if f.CTSDeltaLength > 0 && r.Flag() {
			r.Skip(f.CTSDeltaLength)
		}
		if f.DTSDeltaLength > 0 && r.Flag() {
			r.Skip(f.DTSDeltaLength)
		}
		if f.RandomAccessIndication {
			r.Skip(1)
		}
		r.Skip(f.StreamStateIndication)
		read += r.Pos() - start
		if r.Err() != nil || read > headersLen || len(a.sizes) == 64 {
			a.inFrag = false
			return errPayload
		}
		a.sizes = append(a.sizes, size)
	}
	off := 2 + (headersLen+7)/8
	if f.AuxDataSizeLength > 0 {
		ar := bitio.NewReader(p.Payload[off:])
		aux := int(ar.Bits(f.AuxDataSizeLength))
		off += (f.AuxDataSizeLength + aux + 7) / 8
		if ar.Err() != nil || off > len(p.Payload) {
			return errPayload
		}
	}
	data := p.Payload[off:]
	if len(a.sizes) == 1 && (a.sizes[0] > len(data) || a.inFrag) {
		return a.fragment(d, p, t, data)
	}
	a.inFrag = false
	for i, size := range a.sizes {
		if size > len(data) {
			return errPayload
		}
		d.emitAudio(bytes.Clone(data[:size]), t+time.Duration(i)*a.frame)
		data = data[size:]
	}
	return nil
}

// fragment takes one fragment of an access unit too large for a packet.
func (a *aacDepayloader) fragment(d *Depacketizer, p *rtp.Packet, t time.Duration, data []byte) error {
	size := a.sizes[0]
	if !a.inFrag || p.Timestamp != a.fragTS || size != a.fragSize {
		if size > d.maxAU {
			a.inFrag = false
			return errPayload
		}
		a.frag, a.fragSize, a.fragTS, a.fragTime, a.inFrag = a.frag[:0], size, p.Timestamp, t, true
	}
	a.frag = append(a.frag, data...)
	if len(a.frag) < a.fragSize && !p.Marker {
		return nil
	}
	a.inFrag = false
	if len(a.frag) != a.fragSize {
		return errPayload
	}
	d.emitAudio(bytes.Clone(a.frag), a.fragTime)
	return nil
}

// latmPayloader packetizes AAC as MP4A-LATM with cpresent=0: an
// AudioMuxElement of one access unit a packet, fragmented over packets when
// it does not fit, with the marker on the last.
type latmPayloader struct{}

func (latmPayloader) payload(p *Packetizer, pkt *av.Packet, limit int, emit func([]byte, bool) error) error {
	el := appendAudioMuxElement(p.scratch(len(pkt.Data) + len(pkt.Data)/255 + 1)[:0], pkt.Data)
	for {
		n := min(len(el), limit)
		b := el[:n]
		el = el[n:]
		if err := emit(b, len(el) == 0); err != nil {
			return err
		}
		if len(el) == 0 {
			return nil
		}
	}
}

// latmDepayloader reads MP4A-LATM: an AudioMuxElement over one or more
// packets up to the marker, holding one access unit for each subframe.
// With cpresent=1 a StreamMuxConfig in band sets the track's config.
type latmDepayloader struct {
	d      *Depacketizer
	config *streamMuxConfig
	el     []byte
	elTS   uint32
	elTime time.Duration
	open   bool
}

func (l *latmDepayloader) loss() { l.open = false }

func (l *latmDepayloader) depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error {
	if l.config == nil && !d.f.CPresent {
		// The StreamMuxConfig of config=, or for a format made by
		// NewLATMFormat the one it implies.
		l.config = d.f.smc
		if l.config == nil {
			l.config = &streamMuxConfig{asc: d.track.Config}
		}
	}
	if !l.open || p.Timestamp != l.elTS {
		l.el, l.elTS, l.elTime, l.open = l.el[:0], p.Timestamp, t, true
	}
	if len(l.el)+len(p.Payload) > d.maxAU {
		l.open = false
		return errPayload
	}
	l.el = append(l.el, p.Payload...)
	if !p.Marker {
		return nil
	}
	l.open = false
	var aus [][]byte
	err := readAudioMuxElement(l.el, d.f.CPresent, &l.config, func(au []byte) { aus = append(aus, au) })
	if err != nil {
		return err
	}
	if !bytes.Equal(l.config.asc, d.track.Config) {
		var c aac.Config
		_ = c.Unmarshal(l.config.asc) // checked by readStreamMuxConfig
		d.track = aacTrack(l.config.asc, &c, d.track.Channels)
	}
	frame := aacFrameDuration(d.track.Config, d.f.ClockRate)
	for i, au := range aus {
		d.emitAudio(au, l.elTime+time.Duration(i)*frame)
	}
	return nil
}
