package webrtc

import (
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/webrtc/rtp"
)

// ErrNotConnected is what writing RTCP returns before DTLS has keyed SRTP.
var ErrNotConnected = errors.New("webrtc: media transport not connected")

// rtpPool holds the buffers RTP packets are queued for TrackRemote.OnRTP
// in.
var rtpPool = sync.Pool{New: func() any { b := make([]byte, 0, 1500); return &b }}

// Transceiver is an RTCRtpTransceiver: one media section, with the sender
// of a local track and the receiver of the peer's.
type Transceiver struct {
	pc        *PeerConnection
	mid       string
	midBytes  []byte
	kind      MediaKind
	direction Direction
	current   Direction
	remoteDir Direction
	stopped   bool
	sender    *RTPSender
	tracks    []*TrackRemote

	codecs       []negotiatedCodec
	extIDs       map[string]uint8
	rids         []string
	remoteStream string
	remoteTrack  string
}

func (pc *PeerConnection) newTransceiver(kind MediaKind, dir Direction) *Transceiver {
	t := &Transceiver{pc: pc, kind: kind, direction: dir, current: DirectionInactive, extIDs: make(map[string]uint8)}
	t.sender = &RTPSender{pc: pc, t: t, ssrc: randomUint32()}
	for pc.senders[t.sender.ssrc] != nil {
		t.sender.ssrc = randomUint32()
	}
	pc.senders[t.sender.ssrc] = t.sender
	pc.transceivers = append(pc.transceivers, t)
	return t
}

func (pc *PeerConnection) transceiverByMid(mid string) *Transceiver {
	for _, t := range pc.transceivers {
		if t.mid == mid && mid != "" {
			return t
		}
	}
	return nil
}

// claimTransceiver returns a transceiver AddTrack made that no section has
// been negotiated for, for a section of the remote offer.
func (pc *PeerConnection) claimTransceiver(kind MediaKind) *Transceiver {
	for _, t := range pc.transceivers {
		if t.mid == "" && t.kind == kind && !t.stopped {
			return t
		}
	}
	return nil
}

// codecsOffered are the codecs this side offers or accepts for t: those
// negotiated before, or the configured ones of its kind.
func (t *Transceiver) codecsOffered(all []Codec) []Codec {
	var out []Codec
	for _, c := range all {
		if c.Kind() == t.kind {
			out = append(out, c)
		}
	}
	return out
}

// resolve settles which way media flows once both sides have spoken, and
// the payload type the sender writes.
func (pc *PeerConnection) resolve(t *Transceiver) {
	send := t.direction.sends() && t.sender.hasTrack() && t.remoteDir.receives()
	recv := t.direction.receives() && t.remoteDir.sends()
	t.current = directionOf(send, recv)
	if len(t.codecs) == 0 || t.stopped {
		t.current = DirectionInactive
	}
	t.midBytes = []byte(t.mid)
	t.sender.setPayloadType(0)
	if t.current.sends() {
		for _, c := range t.codecs {
			if t.sender.track.codecMatches(c.Codec) {
				t.sender.setPayloadType(c.sendPT)
				t.sender.clockRate = c.ClockRate
				break
			}
		}
	}
}

// Mid returns the transceiver's mid, once it has been negotiated.
func (t *Transceiver) Mid() string {
	t.pc.mu.Lock()
	defer t.pc.mu.Unlock()
	return t.mid
}

// Kind returns the transceiver's media kind.
func (t *Transceiver) Kind() MediaKind { return t.kind }

// Direction returns the direction this side wants.
func (t *Transceiver) Direction() Direction {
	t.pc.mu.Lock()
	defer t.pc.mu.Unlock()
	return t.direction
}

// CurrentDirection returns the direction negotiated.
func (t *Transceiver) CurrentDirection() Direction {
	t.pc.mu.Lock()
	defer t.pc.mu.Unlock()
	return t.current
}

// SetDirection sets the direction this side wants, which takes effect at
// the next negotiation.
func (t *Transceiver) SetDirection(d Direction) {
	t.pc.mu.Lock()
	if t.direction != d {
		t.direction = d
		t.pc.negotiationNeeded()
	}
	t.pc.unlock()
}

// Sender returns the transceiver's sender.
func (t *Transceiver) Sender() *RTPSender { return t.sender }

// Tracks returns the remote tracks received on the transceiver: one, or
// one for each simulcast layer.
func (t *Transceiver) Tracks() []*TrackRemote {
	t.pc.mu.Lock()
	defer t.pc.mu.Unlock()
	return append([]*TrackRemote(nil), t.tracks...)
}

// Stop stops the transceiver: the next offer rejects its section.
func (t *Transceiver) Stop() {
	t.pc.mu.Lock()
	if !t.stopped {
		t.stopped = true
		t.direction = DirectionInactive
		t.current = DirectionInactive
		t.sender.setPayloadType(0)
		t.pc.negotiationNeeded()
	}
	t.pc.unlock()
}

// Transceivers returns the PeerConnection's transceivers.
func (pc *PeerConnection) Transceivers() []*Transceiver {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return append([]*Transceiver(nil), pc.transceivers...)
}

// AddTransceiver adds a transceiver of kind with no track, to receive the
// peer's media: dir is usually DirectionRecvOnly.
func (pc *PeerConnection) AddTransceiver(kind MediaKind, dir Direction) (*Transceiver, error) {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return nil, ErrClosed
	}
	if kind != MediaKindAudio && kind != MediaKindVideo {
		return nil, ErrConfiguration
	}
	t := pc.newTransceiver(kind, dir)
	pc.negotiationNeeded()
	return t, nil
}

// AddTrack sends a local track to the peer: on a transceiver of its kind
// that sends nothing yet, as a remote offer makes, or on a new one. It takes
// effect at the next negotiation.
func (pc *PeerConnection) AddTrack(track *TrackLocal) (*RTPSender, error) {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return nil, ErrClosed
	}
	kind := track.Kind()
	if kind != MediaKindAudio && kind != MediaKindVideo {
		return nil, ErrConfiguration
	}
	var t *Transceiver
	for _, x := range pc.transceivers {
		if x.kind == kind && !x.stopped && !x.sender.hasTrack() && x.direction != DirectionSendOnly {
			t = x
			break
		}
	}
	if t == nil {
		t = pc.newTransceiver(kind, DirectionSendRecv)
	}
	t.direction = directionOf(true, true)
	t.sender.track = track
	track.addSender(t.sender)
	pc.negotiationNeeded()
	return t.sender, nil
}

// RemoveTrack stops sending the sender's track; its transceiver goes on
// receiving. It takes effect at the next negotiation.
func (pc *PeerConnection) RemoveTrack(s *RTPSender) error {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return ErrClosed
	}
	if s.pc != pc || s.track == nil {
		return nil
	}
	s.detach()
	s.t.direction = directionOf(false, s.t.direction.receives())
	pc.negotiationNeeded()
	return nil
}

// RTPSender is an RTCRtpSender: what a transceiver sends, under an SSRC of
// its own.
type RTPSender struct {
	pc        *PeerConnection
	t         *Transceiver
	track     *TrackLocal
	ssrc      uint32
	pt        uint8
	clockRate uint32

	packets, octets uint32
	lastTS          uint32
	lastAt          time.Time

	onKeyFrame atomic.Pointer[func()]
}

// SSRC returns the SSRC the sender's packets carry.
func (s *RTPSender) SSRC() uint32 { return s.ssrc }

// Transceiver returns the sender's transceiver.
func (s *RTPSender) Transceiver() *Transceiver { return s.t }

// Track returns the track the sender sends, if any.
func (s *RTPSender) Track() *TrackLocal {
	s.pc.mu.Lock()
	defer s.pc.mu.Unlock()
	return s.track
}

// ReplaceTrack sends another track of the same kind, or nothing, without
// negotiating: the new track must have a codec the transceiver
// negotiated, or its packets are dropped.
func (s *RTPSender) ReplaceTrack(track *TrackLocal) error {
	if track != nil && track.Kind() != s.t.kind {
		return ErrConfiguration
	}
	s.pc.mu.Lock()
	defer s.pc.unlock()
	if s.pc.closed {
		return ErrClosed
	}
	s.detach()
	if track != nil {
		s.track = track
		track.addSender(s)
		if s.t.mid != "" {
			cur := s.t.current
			s.pc.resolve(s.t)
			s.t.current = cur
		}
	}
	return nil
}

// OnKeyFrameRequest sets the callback for the peer's requests for a key
// frame of this sender's stream, its PLIs and FIRs: an SFU passes them on
// to the publisher with TrackRemote.RequestKeyFrame.
func (s *RTPSender) OnKeyFrameRequest(fn func()) { s.onKeyFrame.Store(&fn) }

func (s *RTPSender) hasTrack() bool { return s != nil && s.track != nil }

func (s *RTPSender) setPayloadType(pt uint8) { s.pt = pt }

// detach stops sending the track. It runs under the PeerConnection's
// mutex.
func (s *RTPSender) detach() {
	if s.track != nil {
		s.track.removeSender(s)
		s.track = nil
	}
	s.pt = 0
}

// TrackLocal is a track of RTP packets the application writes: from a
// capture and an encoder, or forwarded from a TrackRemote. It may be added
// to any number of PeerConnections, each of which sends it under its own
// SSRC, payload type and header extension ids.
type TrackLocal struct {
	id, streamID string
	codec        Codec
	senders      atomic.Pointer[[]*RTPSender]
	mu           sync.Mutex
	srcExt       atomic.Pointer[[256]string]
}

// NewTrackLocal returns a track of codec, whose MimeType at least is set,
// "video/VP8", with the ids of a MediaStreamTrack and its MediaStream.
func NewTrackLocal(codec Codec, id, streamID string) *TrackLocal {
	if id == "" {
		id = randomString(16)
	}
	if streamID == "" {
		streamID = randomString(16)
	}
	t := &TrackLocal{id: id, streamID: streamID, codec: codec}
	t.senders.Store(&[]*RTPSender{})
	return t
}

// ID returns the track id.
func (t *TrackLocal) ID() string { return t.id }

// StreamID returns the stream id.
func (t *TrackLocal) StreamID() string { return t.streamID }

// Kind returns the track's media kind.
func (t *TrackLocal) Kind() MediaKind { return t.codec.Kind() }

// Codec returns the track's codec.
func (t *TrackLocal) Codec() Codec { return t.codec }

// SetHeaderExtensions says which header extension each id in the packets
// written to the track is, by URI, so that a PeerConnection can carry
// those it negotiated under its own ids: a track that forwards a
// TrackRemote takes the remote's HeaderExtensions. Extensions of ids it
// does not name are dropped. The PeerConnection writes the MID and the
// abs-send-time itself.
func (t *TrackLocal) SetHeaderExtensions(ids map[string]uint8) {
	var m [256]string
	for uri, id := range ids {
		m[id] = uri
	}
	t.srcExt.Store(&m)
}

func (t *TrackLocal) codecMatches(c Codec) bool {
	if t == nil {
		return false
	}
	if t.codec.ClockRate == 0 {
		return strings.EqualFold(t.codec.MimeType, c.MimeType)
	}
	return t.codec.matches(c)
}

func (t *TrackLocal) addSender(s *RTPSender) {
	t.mu.Lock()
	old := *t.senders.Load()
	n := append(append(make([]*RTPSender, 0, len(old)+1), old...), s)
	t.senders.Store(&n)
	t.mu.Unlock()
}

func (t *TrackLocal) removeSender(s *RTPSender) {
	t.mu.Lock()
	old := *t.senders.Load()
	n := make([]*RTPSender, 0, len(old))
	for _, x := range old {
		if x != s {
			n = append(n, x)
		}
	}
	t.senders.Store(&n)
	t.mu.Unlock()
}

// WriteRTP sends an RTP packet on every PeerConnection the track is on.
// Each rewrites the SSRC and payload type to its own, and the header
// extensions to the ids it negotiated (see SetHeaderExtensions), then
// protects it with SRTP; the sequence number, timestamp, marker and
// payload go as written. A PeerConnection that is not connected yet, or
// did not negotiate the track's codec, drops it. pkt is not retained.
func (t *TrackLocal) WriteRTP(pkt []byte) error {
	var p rtp.Packet
	if err := p.Unmarshal(pkt); err != nil {
		return err
	}
	return t.WritePacket(&p)
}

// WritePacket is WriteRTP for a parsed packet.
func (t *TrackLocal) WritePacket(p *rtp.Packet) error {
	src := t.srcExt.Load()
	for _, s := range *t.senders.Load() {
		pc := s.pc
		pc.mu.Lock()
		pc.writeRTP(s, p, src)
		pc.unlock()
	}
	return nil
}

// writeRTP sends p on the sender s.
func (pc *PeerConnection) writeRTP(s *RTPSender, p *rtp.Packet, src *[256]string) {
	if pc.closed || pc.srtpOut == nil || s.pt == 0 || s.track == nil {
		return
	}
	t := s.t
	now := time.Now()
	w := &pc.whdr
	w.Padding = false
	w.Marker = p.Marker
	w.PayloadType = s.pt
	w.SequenceNumber = p.SequenceNumber
	w.Timestamp = p.Timestamp
	w.SSRC = s.ssrc
	w.CSRC = p.CSRC
	w.Extension, w.ExtensionProfile, w.ExtensionData = false, 0, nil
	if id := t.extIDs[rtp.ExtensionURIMid]; id != 0 && len(t.midBytes) > 0 {
		_ = w.SetExtension(id, t.midBytes)
	}
	if id := t.extIDs[rtp.ExtensionURIAbsSendTime]; id != 0 {
		var b [3]byte
		_ = w.SetExtension(id, rtp.AppendAbsSendTime(b[:0], rtp.AbsSendTime(now)))
	}
	if src != nil && p.Extension {
		for id, payload := range p.Extensions() {
			switch uri := src[id]; uri {
			case "", rtp.ExtensionURIMid, rtp.ExtensionURIRid, rtp.ExtensionURIRepairedRid,
				rtp.ExtensionURIAbsSendTime, rtp.ExtensionURITransportCC:
			default:
				if nid := t.extIDs[uri]; nid != 0 {
					_ = w.SetExtension(nid, payload)
				}
			}
		}
	}
	buf, err := w.Append(pc.wbuf[:0])
	if err != nil {
		return
	}
	buf = append(buf, p.Payload...)
	if p.PaddingSize > 0 {
		for range p.PaddingSize - 1 {
			buf = append(buf, 0)
		}
		buf = append(buf, p.PaddingSize)
		buf[0] |= 0x20
	}
	pc.wbuf = buf[:0]
	out, err := pc.srtpOut.ProtectRTP(pc.sbuf[:0], buf)
	if err != nil {
		return
	}
	pc.sbuf = out[:0]
	pc.sendPeer(out)
	s.packets++
	s.octets += uint32(len(p.Payload))
	s.lastTS, s.lastAt = p.Timestamp, now
}

// TrackRemote is a stream of RTP packets the peer sends: an
// RTCRtpReceiver's track, or one simulcast layer of it, by RID.
type TrackRemote struct {
	pc    *PeerConnection
	t     *Transceiver
	ssrc  uint32
	rid   string
	codec negotiatedCodec
	ready atomic.Bool
	onRTP atomic.Pointer[func([]byte)]

	// Reception statistics, for receiver reports (RFC 3550 section 6.4.1
	// and appendix A).
	unwrap        rtp.SeqUnwrapper
	base, max     int64
	received      int64
	expectedPrior int64
	receivedPrior int64
	transit       int64
	jitter        float64
	lastSR        uint32
	lastSRAt      time.Time
}

// SSRC returns the stream's SSRC.
func (r *TrackRemote) SSRC() uint32 { return r.ssrc }

// RID returns the simulcast layer's RID, or "" without simulcast.
func (r *TrackRemote) RID() string { return r.rid }

// Kind returns the track's media kind.
func (r *TrackRemote) Kind() MediaKind { return r.t.kind }

// Codec returns the codec of the stream's first packet, with the payload
// type it was received with.
func (r *TrackRemote) Codec() Codec {
	c := r.codec.Codec
	c.PayloadType = r.codec.recvPT
	return c
}

// ID returns the track id the peer's msid gives, if any.
func (r *TrackRemote) ID() string { return r.t.remoteTrack }

// StreamID returns the stream id the peer's msid gives, if any.
func (r *TrackRemote) StreamID() string { return r.t.remoteStream }

// Transceiver returns the transceiver the track is received on.
func (r *TrackRemote) Transceiver() *Transceiver { return r.t }

// HeaderExtensions returns the header extension ids negotiated for the
// track, by URI: what TrackLocal.SetHeaderExtensions takes to forward it.
func (r *TrackRemote) HeaderExtensions() map[string]uint8 {
	r.pc.mu.Lock()
	defer r.pc.mu.Unlock()
	m := make(map[string]uint8, len(r.t.extIDs))
	for k, v := range r.t.extIDs {
		m[k] = v
	}
	return m
}

// OnRTP sets the callback that receives each RTP packet, decrypted, as
// bytes valid only during the call; rtp.Packet.Unmarshal parses them in
// place. Set it in OnTrack to miss none.
func (r *TrackRemote) OnRTP(fn func(pkt []byte)) { r.onRTP.Store(&fn) }

// RequestKeyFrame asks the peer for a key frame with a PLI.
func (r *TrackRemote) RequestKeyFrame() error {
	pli := rtp.PLI{SenderSSRC: r.pc.rtcpSSRC, MediaSSRC: r.ssrc}
	b, _ := pli.Append(nil)
	return r.pc.WriteRTCP(b)
}

// RequestFIR asks the peer for a key frame with a FIR (RFC 5104), which
// some senders honor where they ignore a PLI.
func (r *TrackRemote) RequestFIR() error {
	r.pc.mu.Lock()
	r.pc.firSeq++
	seq := r.pc.firSeq
	r.pc.mu.Unlock()
	fir := rtp.FIR{SenderSSRC: r.pc.rtcpSSRC, Entries: []rtp.FIREntry{{SSRC: r.ssrc, SequenceNumber: seq}}}
	b, _ := fir.Append(nil)
	return r.pc.WriteRTCP(b)
}

func (r *TrackRemote) deliver(b []byte) {
	if fn := r.onRTP.Load(); fn != nil {
		(*fn)(b)
	}
}

// update counts a packet for the receiver reports.
func (r *TrackRemote) update(h *rtp.Header, now time.Time) {
	e := r.unwrap.Unwrap(h.SequenceNumber)
	if r.received == 0 {
		r.base, r.max = e, e
	} else if e > r.max {
		r.max = e
	}
	r.received++
	if rate := int64(r.codec.ClockRate); rate > 0 {
		arrival := now.UnixNano() / 1000 * rate / 1e6
		transit := arrival - int64(h.Timestamp)
		if r.received > 1 {
			d := transit - r.transit
			if d < 0 {
				d = -d
			}
			r.jitter += (float64(d) - r.jitter) / 16
		}
		r.transit = transit
	}
}

// OnTrack sets the callback for each stream the peer starts sending: called
// with its first packet, which the track's OnRTP, set in the callback,
// receives.
func (pc *PeerConnection) OnTrack(fn func(*TrackRemote)) {
	pc.mu.Lock()
	pc.onTrack = fn
	pc.mu.Unlock()
}

// OnRTCP sets the callback that receives each compound RTCP packet the
// peer sends, decrypted, as bytes valid only during the call;
// rtp.NextRTCP walks it.
func (pc *PeerConnection) OnRTCP(fn func(pkt []byte)) {
	pc.mu.Lock()
	pc.onRTCP = fn
	pc.mu.Unlock()
}

// WriteRTCP sends a compound RTCP packet, which rtp's packet types build,
// protected with SRTCP.
func (pc *PeerConnection) WriteRTCP(pkt []byte) error {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return ErrClosed
	}
	if pc.srtpOut == nil {
		return ErrNotConnected
	}
	out, err := pc.srtpOut.ProtectRTCP(pc.rtcpBuf[:0], pkt)
	if err != nil {
		return err
	}
	pc.rtcpBuf = out[:0]
	pc.sendPeer(out)
	return nil
}

// handleRTP takes an SRTP packet.
func (pc *PeerConnection) handleRTP(b []byte, now time.Time) {
	if len(b) < rtp.HeaderSize {
		return
	}
	ssrc := binary.BigEndian.Uint32(b[8:])
	tr := pc.tracks[ssrc]
	if tr == nil && len(pc.tracks) >= pc.cfg.MaxRemoteSSRCs {
		return
	}
	pkt, err := pc.srtpIn.UnprotectRTP(b[:0], b)
	if err != nil {
		return
	}
	h := &pc.rhdr
	if _, err := h.Unmarshal(pkt); err != nil {
		return
	}
	if tr == nil {
		if tr = pc.newRemoteTrack(h); tr == nil {
			pc.srtpIn.RemoveSSRC(ssrc)
			return
		}
	}
	tr.update(h, now)
	if tr.ready.Load() && tr.onRTP.Load() == nil {
		return
	}
	buf := rtpPool.Get().(*[]byte)
	*buf = append((*buf)[:0], pkt...)
	pc.events = append(pc.events, event{track: tr, buf: buf})
}

// newRemoteTrack finds the transceiver of a new SSRC: by the MID header
// extension, by the SSRCs the remote description lists, or as the first
// receiving transceiver with no track that negotiated its payload type.
func (pc *PeerConnection) newRemoteTrack(h *rtp.Header) *TrackRemote {
	var t *Transceiver
	if pc.extMid != 0 {
		if v := h.GetExtension(pc.extMid); v != nil {
			t = pc.transceiverByMid(string(v))
		}
	}
	if t == nil {
		if mid, ok := pc.remoteSSRCs[h.SSRC]; ok {
			t = pc.transceiverByMid(mid)
		}
	}
	if t == nil {
		for _, x := range pc.transceivers {
			if x.current.receives() && len(x.tracks) == 0 && x.codecByRecvPT(h.PayloadType) != nil {
				t = x
				break
			}
		}
	}
	if t == nil || !t.current.receives() {
		return nil
	}
	c := t.codecByRecvPT(h.PayloadType)
	if c == nil {
		return nil
	}
	tr := &TrackRemote{pc: pc, t: t, ssrc: h.SSRC, codec: *c}
	if pc.extRid != 0 {
		tr.rid = string(h.GetExtension(pc.extRid))
	}
	t.tracks = append(t.tracks, tr)
	pc.tracks[h.SSRC] = tr
	if fn := pc.onTrack; fn != nil {
		pc.queue(func() {
			fn(tr)
			tr.ready.Store(true)
		})
	} else {
		tr.ready.Store(true)
	}
	return tr
}

func (t *Transceiver) codecByRecvPT(pt uint8) *negotiatedCodec {
	for i := range t.codecs {
		if t.codecs[i].recvPT == pt {
			return &t.codecs[i]
		}
	}
	return nil
}

// handleRTCP takes an SRTCP packet: sender reports feed the receiver
// reports, PLIs and FIRs go to their sender's OnKeyFrameRequest, and the
// whole of it to OnRTCP.
func (pc *PeerConnection) handleRTCP(b []byte, now time.Time) {
	out, err := pc.srtpIn.UnprotectRTCP(b[:0], b)
	if err != nil {
		return
	}
	for rest := out; len(rest) > 0; {
		h, p, r, err := rtp.NextRTCP(rest)
		if err != nil {
			break
		}
		rest = r
		switch {
		case h.Type == rtp.TypeSR && len(p) >= 16:
			if tr := pc.tracks[binary.BigEndian.Uint32(p[4:])]; tr != nil {
				tr.lastSR = uint32(binary.BigEndian.Uint64(p[8:]) >> 16)
				tr.lastSRAt = now
			}
		case h.Type == rtp.TypePSFB && h.Count == rtp.FormatPLI && len(p) >= 12:
			pc.keyFrameRequested(binary.BigEndian.Uint32(p[8:]))
		case h.Type == rtp.TypePSFB && h.Count == rtp.FormatFIR:
			for i := 12; i+8 <= len(p); i += 8 {
				pc.keyFrameRequested(binary.BigEndian.Uint32(p[i:]))
			}
		}
	}
	if fn := pc.onRTCP; fn != nil {
		cp := append([]byte(nil), out...)
		pc.queue(func() { fn(cp) })
	}
}

func (pc *PeerConnection) keyFrameRequested(ssrc uint32) {
	if s := pc.senders[ssrc]; s != nil {
		if fn := s.onKeyFrame.Load(); fn != nil && *fn != nil {
			pc.queue(*fn)
		}
	}
}

func (pc *PeerConnection) scheduleReports(now time.Time) {
	if pc.cfg.RTCPReportInterval > 0 {
		pc.nextReport = now.Add(pc.cfg.RTCPReportInterval)
	}
}

// sendReports sends a sender report for each sender that has sent, and a
// receiver report for the streams received, at most 31 of them.
func (pc *PeerConnection) sendReports(now time.Time) {
	pc.scheduleReports(now)
	if pc.srtpOut == nil {
		return
	}
	buf := pc.rtcpBuf[:0]
	for _, t := range pc.transceivers {
		s := t.sender
		if s.packets == 0 || s.pt == 0 {
			continue
		}
		rtpTime := s.lastTS + uint32(now.Sub(s.lastAt).Seconds()*float64(s.clockRate))
		sr := rtp.SenderReport{SSRC: s.ssrc, NTPTime: rtp.NTPTime(now), RTPTime: rtpTime, PacketCount: s.packets, OctetCount: s.octets}
		buf, _ = sr.Append(buf)
	}
	rr := rtp.ReceiverReport{SSRC: pc.rtcpSSRC}
	for _, tr := range pc.tracks {
		if len(rr.Reports) == 31 {
			break
		}
		if tr.received == 0 {
			continue
		}
		expected := tr.max - tr.base + 1
		lost := expected - tr.received
		expInt := expected - tr.expectedPrior
		recvInt := tr.received - tr.receivedPrior
		tr.expectedPrior, tr.receivedPrior = expected, tr.received
		var fraction uint8
		if lostInt := expInt - recvInt; expInt > 0 && lostInt > 0 {
			fraction = uint8(min(lostInt<<8/expInt, 255))
		}
		var dlsr uint32
		if !tr.lastSRAt.IsZero() {
			dlsr = uint32(now.Sub(tr.lastSRAt).Seconds() * 65536)
		}
		rr.Reports = append(rr.Reports, rtp.ReceptionReport{
			SSRC: tr.ssrc, FractionLost: fraction, TotalLost: int32(max(min(lost, 1<<23-1), -1<<23)),
			LastSequence: uint32(tr.max), Jitter: uint32(tr.jitter), LastSenderReport: tr.lastSR, Delay: dlsr,
		})
	}
	if len(rr.Reports) > 0 || len(buf) > 0 {
		buf, _ = rr.Append(buf)
	}
	if len(buf) == 0 {
		return
	}
	out, err := pc.srtpOut.ProtectRTCP(pc.sbuf[:0], buf)
	pc.rtcpBuf = buf[:0]
	if err != nil {
		return
	}
	pc.sbuf = out[:0]
	pc.sendPeer(out)
}
