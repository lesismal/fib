package rtpcodec

import (
	"encoding/binary"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/webrtc/rtp"
)

// The packet types of RFC 7798 section 4.4.
const (
	h265AP   = 48
	h265FU   = 49
	h265PACI = 50
)

// h265Payloader packetizes H.265 as RFC 7798 has it without DONL fields
// (sprop-max-don-diff 0): single NAL unit packets, aggregation packets
// and fragmentation units, with the parameter sets of the track ahead of
// every keyframe that does not carry its own.
type h265Payloader struct {
	sets  [][]byte
	nalus [][]byte
}

func (h *h265Payloader) setTrack(t *av.Track) {
	h.sets = nil
	var c h265.HEVCConfig
	if c.Unmarshal(t.Config) == nil {
		for _, typ := range []h265.NALUType{h265.NALVPS, h265.NALSPS, h265.NALPPS} {
			h.sets = append(h.sets, c.NALUs(typ)...)
		}
	}
}

func (h *h265Payloader) payload(p *Packetizer, pkt *av.Packet, limit int, emit func([]byte, bool) error) error {
	nalus, err := splitAU(h.nalus[:0], pkt, h.sets, func(nal []byte) (bool, bool) {
		t := h265.Type(nal)
		return t == h265.NALVPS || t == h265.NALSPS, t == h265.NALAUD || len(nal) < 2
	})
	h.nalus = nalus
	if err != nil {
		return err
	}
	return aggregate(p, nalus, limit, 2, true, emit, h265Aggregate, h265Fragment)
}

// h265Aggregate builds an aggregation packet: a payload header with the F
// bit of any NAL unit and the lowest layer and temporal IDs, then each NAL
// unit with its 16-bit size.
func h265Aggregate(buf []byte, nalus [][]byte) []byte {
	var f byte
	layer, tid := byte(63), byte(7)
	for _, n := range nalus {
		f |= n[0] & 0x80
		layer = min(layer, (n[0]&1)<<5|n[1]>>3)
		tid = min(tid, n[1]&7)
	}
	buf = append(buf, f|h265AP<<1|layer>>5, (layer&0x1f)<<3|tid)
	for _, n := range nalus {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(n)))
		buf = append(buf, n...)
	}
	return buf
}

// h265Fragment cuts nal into fragmentation units.
func h265Fragment(p *Packetizer, nal []byte, limit int, last bool, emit func([]byte, bool) error) error {
	h0, h1 := nal[0]&0x81|h265FU<<1, nal[1]
	typ := byte(h265.Type(nal))
	data := nal[2:]
	chunk := limit - 3
	buf := p.scratch(limit)
	for start := true; len(data) > 0; start = false {
		n := min(len(data), chunk)
		header := typ
		if start {
			header |= 0x80
		}
		end := n == len(data)
		if end {
			header |= 0x40
		}
		b := append(append(buf[:0], h0, h1, header), data[:n]...)
		if err := emit(b, end && last); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// h265Depayloader reads single NAL unit packets, aggregation packets and
// fragmentation units without DONL fields, and skips PACI packets.
type h265Depayloader struct {
	v    *videoAssembler
	fu   []byte
	inFU bool
}

func (h *h265Depayloader) loss() {
	h.inFU = false
	h.v.lose()
}

func (h *h265Depayloader) depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error {
	b := p.Payload
	if len(b) == 0 {
		return nil
	}
	v := h.v
	v.begin(p.Timestamp, t)
	err := h.read(d, b)
	if err != nil {
		v.lose()
	}
	if p.Marker {
		v.flush()
	}
	return err
}

func (h *h265Depayloader) read(d *Depacketizer, b []byte) error {
	if len(b) < 2 {
		return errPayload
	}
	v := h.v
	switch h265.Type(b) {
	case h265AP:
		return readSized(b[2:], 0, func(nal []byte) {
			if len(nal) >= 2 {
				v.add(nal)
			}
		})
	case h265FU:
		if len(b) < 3 {
			return errPayload
		}
		fh := b[2]
		start, end := fh&0x80 != 0, fh&0x40 != 0
		if start {
			h.fu = append(h.fu[:0], b[0]&0x81|(fh&0x3f)<<1, b[1])
			h.inFU = true
		} else if !h.inFU {
			return nil
		}
		if len(h.fu)+len(b)-3 > d.maxAU {
			h.inFU = false
			return errPayload
		}
		h.fu = append(h.fu, b[3:]...)
		if end {
			h.inFU = false
			v.add(h.fu)
		}
		return nil
	case h265PACI:
		return nil
	}
	v.add(b)
	return nil
}
