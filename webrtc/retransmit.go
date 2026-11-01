package webrtc

import (
	"encoding/binary"
	"math/bits"
	"strconv"
	"strings"
	"time"

	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// RetransmitConfig turns on loss repair for the tracks a PeerConnection
// sends: the latest packets of each are kept, and those the peer reports
// lost in a generic NACK (RFC 4585) are sent again, as RTX (RFC 4588) on a
// stream of their own when the peer negotiated it, or else as they were.
// Its zero value keeps nothing: NACKs go unanswered and RTX is not
// offered.
//
// The history is the TrackLocal's, shared by every PeerConnection it is
// sent on: a forwarder keeps one per track, not one per peer. Each sender
// adds a record of 24 bytes per packet of what it sent.
type RetransmitConfig struct {
	// Packets is how many of a track's latest packets are kept, rounded up
	// to a power of two, at most 4096; zero keeps none. Each costs a
	// buffer of about 1.5 KiB once written. Only tracks whose codec the
	// peer negotiated "nack" for keep any.
	Packets int
	// MaxAge is how old a packet may be and still be resent; 1s when
	// zero.
	MaxAge time.Duration
	// MaxBytesPerSecond bounds what each sender resends, with bursts of a
	// quarter of a second's worth; 256 KiB/s when zero, unbounded when
	// negative. A packet over it is not resent: retransmission never
	// waits.
	MaxBytesPerSecond int
}

const (
	maxRetransmitPackets      = 4096
	defaultRetransmitAge      = time.Second
	defaultRetransmitRate     = 256 << 10
	defaultRetransmitInterval = 100 * time.Millisecond
	// minRetransmitBurst is the least burst a sender may resend, so that
	// a low rate still lets a whole packet through.
	minRetransmitBurst = 2 * 1500
)

func (c *RetransmitConfig) defaults() {
	if c.Packets <= 0 {
		c.Packets = 0
		return
	}
	n := min(c.Packets, maxRetransmitPackets)
	c.Packets = 1 << bits.Len(uint(n-1))
	if c.MaxAge <= 0 {
		c.MaxAge = defaultRetransmitAge
	}
	if c.MaxBytesPerSecond == 0 {
		c.MaxBytesPerSecond = defaultRetransmitRate
	}
}

// hasNACK reports whether rtcp-fb values include the generic NACK.
func hasNACK(feedback []string) bool {
	for _, f := range feedback {
		if strings.EqualFold(strings.TrimSpace(f), "nack") {
			return true
		}
	}
	return false
}

func isRTX(name string) bool { return strings.EqualFold(name, "rtx") }

// rtxPayloadTypes picks a payload type for the RTX format of each codec
// with NACK feedback: the one after the codec's when it is free, as Chrome
// numbers them, or else the first free dynamic one.
func rtxPayloadTypes(codecs []Codec) map[uint8]uint8 {
	used := make(map[int]bool)
	for _, c := range codecs {
		used[int(c.PayloadType)] = true
	}
	free := func(pt int) bool { return (pt >= 96 && pt <= 127 || pt >= 35 && pt <= 63) && !used[pt] }
	out := make(map[uint8]uint8)
	for _, c := range codecs {
		if _, ok := out[c.PayloadType]; ok || !hasNACK(c.Feedback) || isRTX(c.Name()) {
			continue
		}
		pt := int(c.PayloadType) + 1
		for p := 96; !free(pt) && p != 64; p++ {
			if p == 128 {
				p = 35
			}
			pt = p
		}
		if !free(pt) {
			continue
		}
		used[pt] = true
		out[c.PayloadType] = uint8(pt)
	}
	return out
}

// rtxSDP is the RTX format of payload type pt that repairs payload type
// apt, of clock rate rate.
func rtxSDP(pt, apt uint8, rate uint32) sdp.Codec {
	return sdp.Codec{PayloadType: pt, Name: "rtx", ClockRate: rate, Fmtp: "apt=" + strconv.Itoa(int(apt))}
}

// remoteRTX returns the RTX formats of a section's codecs, by the payload
// type each repairs.
func remoteRTX(codecs []sdp.Codec) map[uint8]uint8 {
	var out map[uint8]uint8
	for _, c := range codecs {
		if !isRTX(c.Name) {
			continue
		}
		v, ok := c.Param("apt")
		if !ok {
			continue
		}
		apt, err := strconv.ParseUint(v, 10, 7)
		if err != nil {
			continue
		}
		if out == nil {
			out = make(map[uint8]uint8)
		}
		out[uint8(apt)] = c.PayloadType
	}
	return out
}

// aptOf returns the payload type the RTX format of payload type pt
// repairs, as this side receives them.
func (t *Transceiver) aptOf(pt uint8) (uint8, bool) {
	for _, c := range t.codecs {
		if c.rtxRecv != 0 && c.rtxRecv == pt {
			return c.recvPT, true
		}
	}
	return 0, false
}

// history is the ring of a TrackLocal's latest packets, as written, by
// sequence number. Its buffers come from rtpPool and go back to it when
// no sender uses it any longer.
type history struct {
	slots  []histSlot
	mask   uint16
	users  int
	nextID uint64
}

type histSlot struct {
	// id tells the packet apart from every other the track has written,
	// so that a sender's record finds the very packet it sent.
	id  uint64
	at  int64
	src *[256]string
	buf *[]byte
}

func (h *history) free() {
	for i := range h.slots {
		if b := h.slots[i].buf; b != nil {
			rtpPool.Put(b)
		}
	}
	h.slots = nil
}

// retain makes the track keep its latest n packets, or more if another
// sender asked for more, until release.
func (t *TrackLocal) retain(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hist
	if h == nil {
		h = &history{}
		t.hist = h
	}
	h.users++
	if len(h.slots) < n {
		h.free()
		h.slots = make([]histSlot, n)
		h.mask = uint16(n - 1)
	}
	t.histOn.Store(true)
}

// release undoes a retain; the history goes with its last user.
func (t *TrackLocal) release() {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hist
	if h == nil {
		return
	}
	if h.users--; h.users == 0 {
		h.free()
		t.hist = nil
		t.histOn.Store(false)
	}
}

// remember keeps a copy of p, if the track keeps a history, and returns
// the id it is kept under, 0 when it is not.
func (t *TrackLocal) remember(p *rtp.Packet, src *[256]string) uint64 {
	if !t.histOn.Load() {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hist
	if h == nil {
		return 0
	}
	sl := &h.slots[p.SequenceNumber&h.mask]
	if sl.buf == nil {
		sl.buf = rtpPool.Get().(*[]byte)
	}
	b, err := p.Append((*sl.buf)[:0])
	*sl.buf = b
	if err != nil {
		sl.id = 0
		return 0
	}
	h.nextID++
	sl.id, sl.at, sl.src = h.nextID, time.Now().UnixNano(), src
	return sl.id
}

// lookup appends to dst the packet of sequence number seq kept under id,
// and returns with it the header extension URIs it was written with and
// when.
func (t *TrackLocal) lookup(seq uint16, id uint64, dst []byte) ([]byte, *[256]string, int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h := t.hist
	if h == nil || id == 0 {
		return dst, nil, 0, false
	}
	sl := &h.slots[seq&h.mask]
	if sl.id != id {
		return dst, nil, 0, false
	}
	return append(dst, *sl.buf...), sl.src, sl.at, true
}

// sentRecord is what a sender keeps of a packet it sent, to send it again:
// the track's id for it, the abs-send-time it went with, so that a plain
// retransmission is the very same packet, and when it was last resent.
type sentRecord struct {
	id     uint64
	abs    uint32
	resent int64
}

// setHistory starts or stops keeping what the sender sends, as its
// negotiation says. It runs under the PeerConnection's mutex. The records
// are cleared either way: they hold the track's ids and depend on the
// header extension ids and payload type negotiated.
func (s *RTPSender) setHistory(on bool) {
	on = on && s.track != nil
	if s.histTrack != nil && (!on || s.histTrack != s.track) {
		s.histTrack.release()
		s.histTrack = nil
	}
	n := s.pc.cfg.Retransmit.Packets
	if on && s.histTrack == nil {
		s.track.retain(n)
		s.histTrack = s.track
	}
	switch {
	case !on:
		s.sent = nil
	case s.sent == nil:
		s.sent = make([]sentRecord, n)
		s.smask = uint16(n - 1)
	default:
		clear(s.sent)
	}
}

// Retransmitted returns how many packets the sender has sent again, and
// their bytes.
func (s *RTPSender) Retransmitted() (packets, bytes uint64) {
	s.pc.mu.Lock()
	defer s.pc.mu.Unlock()
	return s.rtxPackets, s.rtxBytes
}

// nacked takes a generic NACK, p: the packets it reports lost are sent
// again, as far as the sender keeps them and its rate allows.
func (pc *PeerConnection) nacked(h rtp.RTCPHeader, p []byte, now time.Time) {
	ssrc := binary.BigEndian.Uint32(p[8:])
	s := pc.senders[ssrc]
	if s == nil || s.ssrc != ssrc || s.sent == nil || pc.closed || pc.srtpOut == nil || !s.hasPT {
		return
	}
	if pad := int(p[len(p)-1]); h.Padding && pad <= len(p)-12 {
		p = p[:len(p)-pad]
	}
	for i := 12; i+4 <= len(p); i += 4 {
		pair := rtp.NACKPair{PacketID: binary.BigEndian.Uint16(p[i:]), LostPackets: binary.BigEndian.Uint16(p[i+2:])}
		for seq := range pair.All {
			pc.retransmit(s, seq, now)
		}
	}
}

// retransmit sends the packet of sequence number seq again on s, unless
// the sender no longer has it, it is too old, it was resent less than a
// round trip ago, or the sender's rate is spent.
func (pc *PeerConnection) retransmit(s *RTPSender, seq uint16, now time.Time) {
	rec := &s.sent[seq&s.smask]
	if rec.id == 0 || s.track == nil {
		return
	}
	at := now.UnixNano()
	interval := s.rtt
	if interval <= 0 {
		interval = defaultRetransmitInterval
	}
	if rec.resent != 0 && at-rec.resent < int64(interval) {
		return
	}
	b, src, sentAt, ok := s.track.lookup(seq, rec.id, pc.hbuf[:0])
	pc.hbuf = b[:0]
	if !ok || at-sentAt > int64(pc.cfg.Retransmit.MaxAge) {
		return
	}
	p := &pc.hpkt
	if p.Unmarshal(b) != nil || p.SequenceNumber != seq {
		return
	}
	size := len(b) + 2
	if !s.spend(size, at) {
		return
	}
	var buf []byte
	var err error
	if s.rtxPT != 0 {
		buf, err = pc.appendRTP(pc.wbuf[:0], s, p, src, s.rtxSSRC, s.rtxPT, s.rtxSeq, rtp.AbsSendTime(now), true)
	} else {
		buf, err = pc.appendRTP(pc.wbuf[:0], s, p, src, s.ssrc, s.pt, seq, rec.abs, false)
	}
	if err != nil {
		return
	}
	pc.wbuf = buf[:0]
	out, err := pc.srtpOut.ProtectRTP(pc.sbuf[:0], buf)
	if err != nil {
		return
	}
	pc.sbuf = out[:0]
	if s.rtxPT != 0 {
		s.rtxSeq++
	}
	rec.resent = at
	s.rtxPackets++
	s.rtxBytes += uint64(len(buf))
	pc.sendPeer(out)
}

// spend takes n bytes from the sender's retransmission budget, a token
// bucket, if it has them.
func (s *RTPSender) spend(n int, at int64) bool {
	rate := s.pc.cfg.Retransmit.MaxBytesPerSecond
	if rate < 0 {
		return true
	}
	burst := max(float64(rate)/4, minRetransmitBurst)
	if s.tokenAt == 0 {
		s.tokens = burst
	} else if d := at - s.tokenAt; d > 0 {
		s.tokens = min(burst, s.tokens+float64(d)*float64(rate)/1e9)
	}
	s.tokenAt = at
	if s.tokens < float64(n) {
		return false
	}
	s.tokens -= float64(n)
	return true
}

// reportBlocks takes the reception report blocks of a receiver or sender
// report: the round trip time to the peer of each sender that keeps a
// history, from the last sender report it echoes (RFC 3550 section
// 6.4.1).
func (pc *PeerConnection) reportBlocks(blocks []byte, count uint8, now time.Time) {
	blocks = blocks[:min(len(blocks), 24*int(count))]
	mid := uint32(rtp.NTPTime(now) >> 16)
	for ; len(blocks) >= 24; blocks = blocks[24:] {
		ssrc := binary.BigEndian.Uint32(blocks)
		s := pc.senders[ssrc]
		if s == nil || s.ssrc != ssrc || s.sent == nil {
			continue
		}
		lsr, dlsr := binary.BigEndian.Uint32(blocks[16:]), binary.BigEndian.Uint32(blocks[20:])
		if lsr == 0 {
			continue
		}
		if d := mid - lsr - dlsr; d < 10<<16 {
			s.rtt = max(time.Duration(d)*time.Second>>16, time.Millisecond)
		}
	}
}

// rtxTrack finds the stream an RTX stream of the transceiver t repairs:
// by the FID group the remote description pairs them in, by the
// repaired-rid header extension, or as the transceiver's only stream.
func (pc *PeerConnection) rtxTrack(t *Transceiver, h *rtp.Header) *TrackRemote {
	if primary, ok := pc.rtxPrimary[h.SSRC]; ok {
		if tr := pc.tracks[primary]; tr != nil && tr.t == t {
			return tr
		}
		return nil
	}
	if id := pc.extIDs[rtp.ExtensionURIRepairedRid]; id != 0 {
		if rid := h.GetExtension(id); rid != nil {
			for _, tr := range t.tracks {
				if tr.rid == string(rid) {
					return tr
				}
			}
			return nil
		}
	}
	if len(t.tracks) == 1 {
		return t.tracks[0]
	}
	return nil
}

// unwrapRTX turns the decrypted RTX packet pkt, whose header h is n bytes,
// back into the packet it repairs on the stream tr, in place: its original
// sequence number, the payload type apt, and tr's SSRC. It returns nil for
// a packet with no original sequence number, as the padding a sender
// probes with is.
func unwrapRTX(pkt []byte, h *rtp.Header, n int, apt uint8, tr *TrackRemote) []byte {
	end := len(pkt)
	if h.Padding {
		end -= int(pkt[end-1])
	}
	if end-n < 2 {
		return nil
	}
	osn := binary.BigEndian.Uint16(pkt[n:])
	copy(pkt[2:n+2], pkt[:n])
	pkt = pkt[2:]
	pkt[1] = pkt[1]&0x80 | apt
	binary.BigEndian.PutUint16(pkt[2:], osn)
	binary.BigEndian.PutUint32(pkt[8:], tr.ssrc)
	return pkt
}
