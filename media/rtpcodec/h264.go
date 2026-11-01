package rtpcodec

import (
	"encoding/binary"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/webrtc/rtp"
)

// The packet types of RFC 6184 section 5.4.
const (
	h264STAPA  = 24
	h264STAPB  = 25
	h264MTAP16 = 26
	h264MTAP24 = 27
	h264FUA    = 28
	h264FUB    = 29
)

// h264Payloader packetizes H.264 in packetization mode 1: a NAL unit that
// fits goes alone or aggregated with the ones after it into a STAP-A, and
// one that does not is cut into FU-As. The parameter sets of the track go
// ahead of every keyframe that does not carry its own, so that a receiver
// that joins late, or ignores sprop-parameter-sets, can decode.
type h264Payloader struct {
	sets  [][]byte
	nalus [][]byte
}

func (h *h264Payloader) setTrack(t *av.Track) {
	h.sets = nil
	var c h264.AVCConfig
	if c.Unmarshal(t.Config) == nil {
		h.sets = append(append(h.sets, c.SPS...), c.PPS...)
	}
}

func (h *h264Payloader) payload(p *Packetizer, pkt *av.Packet, limit int, emit func([]byte, bool) error) error {
	nalus, err := splitAU(h.nalus[:0], pkt, h.sets, func(nal []byte) (bool, bool) {
		t := h264.Type(nal)
		return t == h264.NALSPS, t == h264.NALAUD
	})
	h.nalus = nalus
	if err != nil {
		return err
	}
	mode1 := p.f.PacketizationMode != 0
	return aggregate(p, nalus, limit, 1, mode1, emit, h264STAP, h264FU)
}

// splitAU returns the NAL units of pkt's AVCC data, without those skip
// says to leave out, and with sets ahead of a keyframe when none of its
// NAL units is a parameter set.
func splitAU(dst [][]byte, pkt *av.Packet, sets [][]byte, classify func([]byte) (ps, skip bool)) ([][]byte, error) {
	if !h264.ValidAVCC(pkt.Data, 4) {
		return dst, ErrPacket
	}
	hasPS := false
	for nal := range h264.SplitAVCC(pkt.Data, 4) {
		ps, skip := classify(nal)
		if len(nal) == 0 || skip {
			continue
		}
		hasPS = hasPS || ps
		dst = append(dst, nal)
	}
	if pkt.Keyframe && !hasPS && len(sets) > 0 {
		dst = append(sets[:len(sets):len(sets)], dst...)
	}
	return dst, nil
}

// aggregate sends NAL units with a header of hdr bytes: singly, aggregated
// by stap when allowed, or fragmented by fu, with the marker on the last
// packet.
func aggregate(p *Packetizer, nalus [][]byte, limit, hdr int, allowSTAP bool,
	emit func([]byte, bool) error,
	stap func(buf []byte, nalus [][]byte) []byte,
	fu func(p *Packetizer, nal []byte, limit int, last bool, emit func([]byte, bool) error) error) error {
	for i := 0; i < len(nalus); {
		nal := nalus[i]
		if len(nal) > limit {
			if err := fu(p, nal, limit, i == len(nalus)-1, emit); err != nil {
				return err
			}
			i++
			continue
		}
		j, size := i+1, hdr+2+len(nal)
		for allowSTAP && j < len(nalus) && size+2+len(nalus[j]) <= limit {
			size += 2 + len(nalus[j])
			j++
		}
		payload := nal
		if j > i+1 {
			payload = stap(p.scratch(size)[:0], nalus[i:j])
		}
		if err := emit(payload, j == len(nalus)); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// h264STAP builds a STAP-A of nalus: a header with the highest NRI and the
// F bit of any of them, then each with its 16-bit size.
func h264STAP(buf []byte, nalus [][]byte) []byte {
	var f, nri byte
	for _, n := range nalus {
		f |= n[0] & 0x80
		nri = max(nri, n[0]&0x60)
	}
	buf = append(buf, f|nri|h264STAPA)
	for _, n := range nalus {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(n)))
		buf = append(buf, n...)
	}
	return buf
}

// h264FU cuts nal into FU-As.
func h264FU(p *Packetizer, nal []byte, limit int, last bool, emit func([]byte, bool) error) error {
	indicator := nal[0]&0xe0 | h264FUA
	typ := nal[0] & 0x1f
	data := nal[1:]
	chunk := limit - 2
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
		b := append(append(buf[:0], indicator, header), data[:n]...)
		if err := emit(b, end && last); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// h264Depayloader reads the packet types of RFC 6184: single NAL units,
// STAP-A, STAP-B, MTAP16, MTAP24, FU-A and FU-B. Decoding order numbers are
// skipped, not acted on: the interleaved mode's reordering is not done.
type h264Depayloader struct {
	v    *videoAssembler
	fu   []byte
	inFU bool
}

func (h *h264Depayloader) loss() {
	h.inFU = false
	h.v.lose()
}

func (h *h264Depayloader) depayload(d *Depacketizer, p *rtp.Packet, t time.Duration) error {
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

func (h *h264Depayloader) read(d *Depacketizer, b []byte) error {
	v := h.v
	switch typ := b[0] & 0x1f; typ {
	case h264STAPA, h264STAPB:
		b = b[1:]
		if typ == h264STAPB {
			if len(b) < 2 {
				return errPayload
			}
			b = b[2:]
		}
		return readSized(b, 0, v.add)
	case h264MTAP16, h264MTAP24:
		if len(b) < 3 {
			return errPayload
		}
		skip := 3
		if typ == h264MTAP24 {
			skip = 4
		}
		return readSized(b[3:], skip, v.add)
	case h264FUA, h264FUB:
		if len(b) < 2 {
			return errPayload
		}
		start, end := b[1]&0x80 != 0, b[1]&0x40 != 0
		data := b[2:]
		if start {
			if typ == h264FUB {
				if len(data) < 2 {
					return errPayload
				}
				data = data[2:]
			}
			h.fu = append(h.fu[:0], b[0]&0xe0|b[1]&0x1f)
			h.inFU = true
		} else if !h.inFU {
			// The start was lost: the loss has the access unit dropped.
			return nil
		}
		if len(h.fu)+len(data) > d.maxAU {
			h.inFU = false
			return errPayload
		}
		h.fu = append(h.fu, data...)
		if end {
			h.inFU = false
			v.add(h.fu)
		}
		return nil
	case 0, 30, 31:
		return errPayload
	}
	v.add(b)
	return nil
}

// readSized reads the NAL units of an aggregation packet, each after a
// 16-bit size and skip bytes of fields that the size does not count.
func readSized(b []byte, skip int, add func([]byte)) error {
	for len(b) > 0 {
		if len(b) < 2+skip {
			return errPayload
		}
		n := int(binary.BigEndian.Uint16(b))
		b = b[2+skip:]
		if n > len(b) {
			return errPayload
		}
		add(b[:n])
		b = b[n:]
	}
	return nil
}
