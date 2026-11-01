package rtsp

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/internal/lipsync"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc/rtp"
)

// recordEpoch is the media time a recorded stream starts at: later than
// zero, so that the decode times a depacketizer recovers ahead of the
// first frame's presentation time are not cut off.
const recordEpoch = time.Second

type sessionMode uint8

const (
	modeNone sessionMode = iota
	modeAnnounced
	modeRecording
	modePlayReady
	modePlaying
	modePaused
)

// session is a client's RTSP session: the streams it set up, and what it
// plays or records. It belongs to its connection, and its fields to the
// connection's lock, but for the player, which the hub calls.
type session struct {
	sc          *ServerConn
	id          string
	path, query string
	mode        sessionMode
	base        string
	streams     []*stream

	// desc is what is played; player sends it, while sub feeds it.
	desc   *description
	player *player
	sub    *hub.Subscription
	nextSR time.Time

	// announced is what is recorded; pub publishes it, as pubTracks.
	announced   []*rtpcodec.Format
	pub         *hub.Publisher
	pubTracks   []*av.Track
	recordStart time.Time
	sync        lipsync.Session
}

// stream is one stream of a session, as SETUP set it up.
type stream struct {
	index  int
	format *rtpcodec.Format
	tcp    bool
	// channel is the RTP channel of TCP; RTCP's is the one after.
	channel uint8
	// The UDP addresses of the client, and what sends to them.
	clientRTP, clientRTCP netip.AddrPort
	rtpOut, rtcpOut       *udpSender

	// pk sends a played stream; ts is its first RTP timestamp, and
	// srSent says a sender report has gone, which a receiver needs to
	// line the streams up.
	pk     *rtpcodec.Packetizer
	ts     uint32
	srSent bool
	// Over UDP, videoWait holds video back until the next keyframe once a
	// datagram of it was lost, and lossSince is when the receiver reports
	// began saying the player loses more than MaxPlayerLoss.
	videoWait bool
	lossSince time.Time

	// dp receives a recorded stream, through reorder over UDP; pubIndex
	// is its track in the hub.
	dp        *rtpcodec.Depacketizer
	reorder   *rtpcodec.Reorder
	pubIndex  int
	originSet bool
	sr        lipsync.Clock
	pkt       rtp.Packet
}

func newSession(sc *ServerConn, path, query string) *session {
	return &session{sc: sc, id: newSessionID(), path: path, query: query}
}

func (s *session) header() string {
	return Session{ID: s.id, Timeout: int(s.sc.s.config.SessionTimeout / time.Second)}.String()
}

// pickTransport returns the first transport the server can do.
func (s *session) pickTransport(transports []Transport) (Transport, bool) {
	udp := s.sc.s.udpInfo()
	for _, t := range transports {
		if t.Multicast {
			continue
		}
		if t.TCP || (udp != nil && t.ClientPort[0] >= 0) {
			return t, true
		}
	}
	return Transport{}, false
}

// prepare sets up the transport of st as t asks, and returns the transport
// to answer with.
func (s *session) prepare(st *stream, t Transport) (Transport, bool) {
	reply := Transport{Interleaved: [2]int{-1, -1}, ClientPort: [2]int{-1, -1}, ServerPort: [2]int{-1, -1}}
	if t.TCP {
		ch := t.Interleaved[0]
		if ch < 0 || ch > 254 || s.channelTaken(ch) {
			ch = 0
			for s.channelTaken(ch) {
				ch += 2
			}
		}
		st.tcp, st.channel = true, uint8(ch)
		reply.TCP, reply.Interleaved = true, [2]int{ch, ch + 1}
		return reply, ch <= 254
	}
	udp := s.sc.s.udpInfo()
	ip := s.sc.c.RemoteAddrPort().Addr().Unmap()
	rtcpPort := t.ClientPort[1]
	if rtcpPort < 0 {
		rtcpPort = t.ClientPort[0] + 1
	}
	st.clientRTP = netip.AddrPortFrom(ip, uint16(t.ClientPort[0]))
	st.clientRTCP = netip.AddrPortFrom(ip, uint16(rtcpPort))
	limit := int(s.sc.s.config.MaxQueuedBytes)
	st.rtpOut = newUDPSender(udp.engine, udp.rtp, st.clientRTP, limit)
	st.rtcpOut = newUDPSender(udp.engine, udp.rtcp, st.clientRTCP, 64<<10)
	reply.ClientPort = [2]int{t.ClientPort[0], rtcpPort}
	reply.ServerPort = [2]int{udp.rtpPort, udp.rtcpPort}
	return reply, true
}

func (s *session) channelTaken(ch int) bool {
	for _, st := range s.streams {
		if st.tcp && (int(st.channel) == ch || int(st.channel)+1 == ch || int(st.channel) == ch+1) {
			return true
		}
	}
	return false
}

// setupPlay sets up track of the described stream for playing.
func (s *session) setupPlay(req *Request, transports []Transport, track int) *Response {
	if s.mode != modePlayReady && s.mode != modePaused {
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	for _, st := range s.streams {
		if st.index == track {
			return &Response{StatusCode: StatusMethodNotValidInState}
		}
	}
	t, ok := s.pickTransport(transports)
	if !ok {
		return &Response{StatusCode: StatusUnsupportedTransport}
	}
	st := &stream{index: track, format: s.desc.formats[track]}
	for st.ts == 0 {
		st.ts = randUint32()
	}
	pk, err := rtpcodec.NewPacketizer(st.format, rtpcodec.PacketizerConfig{MTU: s.sc.s.config.MTU, Timestamp: st.ts})
	if err != nil {
		return &Response{StatusCode: StatusUnsupportedMediaType}
	}
	st.pk = pk
	reply, ok := s.prepare(st, t)
	if !ok {
		return &Response{StatusCode: StatusUnsupportedTransport}
	}
	reply.SSRC, reply.HasSSRC = pk.SSRC(), true
	s.streams = append(s.streams, st)
	s.addRoutes(st)
	return &Response{StatusCode: StatusOK, Header: Header{{"Transport", reply.String()}, {"Session", s.header()}}}
}

// setupRecord sets up a stream of the announced description for
// recording.
func (s *session) setupRecord(req *Request, transports []Transport) *Response {
	if s.mode != modeAnnounced {
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	idx := -1
	used := func(i int) bool {
		return slices.ContainsFunc(s.streams, func(st *stream) bool { return st.index == i })
	}
	for i, f := range s.announced {
		if !used(i) && matchControl(s.base, f.Control, req.URL) {
			idx = i
			break
		}
	}
	if idx < 0 {
		for i := range s.announced {
			if !used(i) {
				idx = i
				break
			}
		}
	}
	if idx < 0 {
		return &Response{StatusCode: StatusNotFound}
	}
	t, ok := s.pickTransport(transports)
	if !ok || (t.Mode != "" && t.Mode != "RECORD" && t.Mode != "RECEIVE") {
		return &Response{StatusCode: StatusUnsupportedTransport}
	}
	f := s.announced[idx]
	dp, err := rtpcodec.NewDepacketizer(f, rtpcodec.DepacketizerConfig{})
	if err != nil {
		return &Response{StatusCode: StatusUnsupportedMediaType}
	}
	st := &stream{index: idx, format: f, dp: dp}
	reply, ok := s.prepare(st, t)
	if !ok {
		return &Response{StatusCode: StatusUnsupportedTransport}
	}
	if !st.tcp {
		st.reorder = rtpcodec.NewReorder(s.sc.s.config.ReorderSize, s.sc.s.config.ReorderDelay)
	}
	reply.Mode = "record"
	s.streams = append(s.streams, st)
	s.addRoutes(st)
	if t.HasSSRC && !st.tcp {
		s.sc.s.addSSRCRoute(t.SSRC, &udpRoute{sc: s.sc, sess: s, st: st})
	}
	return &Response{StatusCode: StatusOK, Header: Header{{"Transport", reply.String()}, {"Session", s.header()}}}
}

func (s *session) addRoutes(st *stream) {
	if st.tcp {
		return
	}
	s.sc.s.addRoute(st.clientRTP, &udpRoute{sc: s.sc, sess: s, st: st})
	s.sc.s.addRoute(st.clientRTCP, &udpRoute{sc: s.sc, sess: s, st: st, rtcp: true})
}

// play starts playing; the subscription starts once the response has
// gone, in afterResponse, so that the packets follow it.
func (s *session) play(req *Request) *Response {
	switch s.mode {
	case modePlayReady, modePaused, modePlaying:
	default:
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	if len(s.streams) == 0 {
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	if s.player == nil {
		s.player = newPlayer(s)
	}
	var infos []string
	for _, st := range s.streams {
		info := RTPInfo{URL: s.desc.base + "trackID=" + strconv.Itoa(st.index), HasSeq: true, HasTime: true}
		info.Seq, info.RTPTime = s.player.position(st)
		infos = append(infos, info.String())
	}
	s.mode = modePlaying
	resp := &Response{StatusCode: StatusOK, Header: Header{{"Range", "npt=0.000-"}}}
	resp.Header.Add("RTP-Info", strings.Join(infos, ","))
	return resp
}

// afterResponse acts once the response to a request has gone.
func (s *session) afterResponse(method string) {
	if method != MethodPlay || s.mode != modePlaying || s.sub != nil {
		return
	}
	cfg := &s.sc.s.config
	var sink hub.Sink = s.player
	if slices.ContainsFunc(s.streams, func(st *stream) bool { return st.tcp }) && outputwait.Queued != nil {
		c := s.sc.c
		sink = hub.NewDropper(s.player, func() int64 { return outputwait.Queued(c) }, cfg.MaxQueuedBytes)
	}
	// Over UDP, nothing paces or resends the cached GOP's burst: a
	// 4 Mbit/s stream's megabyte overflows the player's socket buffer and
	// its start is lost. UDP players start at the next keyframe instead.
	udp := slices.ContainsFunc(s.streams, func(st *stream) bool { return !st.tcp })
	sub, err := cfg.Hub.Subscribe(s.path, sink, hub.SubscribeOptions{Info: s.sc.subscribeInfo(s.query), Wait: cfg.PlayWait, NoGOPCache: udp})
	if err != nil {
		// The stream went between DESCRIBE and PLAY.
		s.sc.closeLocked()
		s.sc.c.CloseAfterSend()
		return
	}
	s.sub = sub
	s.nextSR = time.Now().Add(cfg.SenderReportInterval)
	s.sc.schedule(s.nextSR)
}

// pause stops playing, keeping the streams set up.
func (s *session) pause() *Response {
	switch s.mode {
	case modePlaying, modePaused:
	default:
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	if s.sub != nil {
		s.sub.Close()
		s.sub = nil
	}
	s.mode = modePaused
	return &Response{StatusCode: StatusOK}
}

// record publishes the announced streams set up.
func (s *session) record(_ *Request, now time.Time) *Response {
	if s.mode != modeAnnounced || len(s.streams) == 0 {
		return &Response{StatusCode: StatusMethodNotValidInState}
	}
	h := s.sc.s.config.Hub
	// Publish in the order announced.
	slices.SortFunc(s.streams, func(a, b *stream) int { return a.index - b.index })
	s.pubTracks = make([]*av.Track, len(s.streams))
	for i, st := range s.streams {
		st.pubIndex = i
		s.pubTracks[i] = st.format.Track
	}
	pub, err := h.Publish(s.path, slices.Clone(s.pubTracks), hub.PublishInfo{Protocol: "rtsp", Remote: s.sc.c.RemoteAddrPort(), Query: s.query})
	if err != nil {
		return hubError(err)
	}
	s.pub = pub
	s.mode = modeRecording
	s.recordStart = now
	return &Response{StatusCode: StatusOK}
}

// onInterleaved takes an RTP or RTCP packet sent on the connection.
func (s *session) onInterleaved(now time.Time, m *Interleaved) {
	for _, st := range s.streams {
		if st.tcp && m.Channel == st.channel {
			s.receiveRTP(st, now, m.Data)
			return
		}
		if st.tcp && m.Channel == st.channel+1 {
			s.receiveRTCP(st, now, m.Data)
			return
		}
	}
}

// receiveRTCP takes an RTCP packet of a stream. A recorder's sender
// reports line its streams up (lipsync.Session); the rest, and a player's receiver
// reports, only show the client is there.
func (s *session) receiveRTCP(st *stream, now time.Time, b []byte) {
	if st.dp != nil {
		st.sr.Update(b)
	} else if s.player != nil && !st.tcp {
		s.player.receiverReport(st, b, now)
	}
}

// receiveRTP takes an RTP packet of a recorded stream.
func (s *session) receiveRTP(st *stream, now time.Time, b []byte) {
	if s.mode != modeRecording || st.dp == nil {
		return
	}
	if st.pkt.Unmarshal(b) != nil {
		return
	}
	if st.reorder == nil {
		s.depacketize(st, now, &st.pkt)
		return
	}
	st.reorder.Push(now, &st.pkt, func(p *rtp.Packet) { s.depacketize(st, now, p) })
	if at, ok := st.reorder.NextTimeout(); ok {
		s.sc.schedule(at)
	}
}

func (s *session) depacketize(st *stream, now time.Time, p *rtp.Packet) {
	if !st.originSet {
		st.originSet = true
		at := s.sync.Origin(st.sr, p.Timestamp, st.format.ClockRate, recordEpoch+now.Sub(s.recordStart))
		st.dp.SetOrigin(p.Timestamp, at)
	}
	_ = st.dp.Depacketize(p, func(pkt *av.Packet) {
		if t := st.dp.Track(); t != s.pubTracks[st.pubIndex] {
			s.pubTracks[st.pubIndex] = t
			s.pub.SetTracks(slices.Clone(s.pubTracks))
		}
		pkt.Track = st.pubIndex
		s.pub.WritePacket(pkt)
	})
}

// onTimer sends sender reports and expires reorder buffers, and returns
// when it is next due.
func (s *session) onTimer(now time.Time) (time.Time, bool) {
	var next time.Time
	soonest := func(at time.Time) {
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	if s.mode == modePlaying && s.player != nil {
		if !now.Before(s.nextSR) {
			s.player.sendReports(now)
			s.nextSR = now.Add(s.sc.s.config.SenderReportInterval)
		}
		soonest(s.nextSR)
	}
	for _, st := range s.streams {
		if st.reorder == nil {
			continue
		}
		st.reorder.Expire(now, func(p *rtp.Packet) { s.depacketize(st, now, p) })
		if at, ok := st.reorder.NextTimeout(); ok {
			soonest(at)
		}
	}
	return next, !next.IsZero()
}

// close ends the session: what it plays or publishes, its routes and its
// UDP peers.
func (s *session) close() {
	if s.sub != nil {
		s.sub.Close()
		s.sub = nil
	}
	if s.player != nil {
		s.player.close()
	}
	if s.pub != nil {
		for _, st := range s.streams {
			if st.dp != nil {
				st.dp.Flush(func(pkt *av.Packet) {
					pkt.Track = st.pubIndex
					s.pub.WritePacket(pkt)
				})
			}
		}
		s.pub.Close()
		s.pub = nil
	}
	s.sc.s.removeRoutes(s)
	for _, st := range s.streams {
		if st.rtpOut != nil {
			st.rtpOut.close()
			st.rtcpOut.close()
		}
	}
	s.mode = modeNone
}

// player is the hub.Sink of a playing session: it packetizes what the hub
// gives and sends it to the client. The hub calls it under the stream's
// lock, so it never takes the connection's.
type player struct {
	mu        sync.Mutex
	c         *fib.Connection
	streams   []*stream // by track index
	tracks    []*av.Track
	hardLimit int64
	// maxLoss is MaxPlayerLoss as RTCP's fraction lost, out of 256, and
	// lossWindow PlayerLossWindow; maxLoss is 0 when loss closes nothing.
	maxLoss    int
	lossWindow time.Duration
	rr         rtp.ReceiverReport
	// origin is set by the first packet: its DTS is media time originAt,
	// and it went at wall time wall0. Sender reports map wall time to
	// media time from there for every stream alike, which is all a
	// receiver needs to line them up.
	origin   bool
	originAt time.Duration
	wall0    time.Time
	lastPTS  time.Duration
	// After a republish (restart), the next publisher's clock may start
	// anywhere: offset moves its times on from lastDTS, the largest sent
	// (at lastAt), into shifted, as flv.Timeline does for RTMP.
	restart bool
	offset  time.Duration
	lastDTS time.Duration
	lastAt  time.Time
	shifted av.Packet
	closed  bool
	buf     []byte
}

// maxClockJump is how far past the last packet the first one after a
// republish may be and still be taken as the same clock carrying on. A
// new clock starts after the last packet by as long as the stream was
// quiet, at least minRestartGap and at most maxClockJump.
const (
	maxClockJump  = 10 * time.Second
	minRestartGap = 20 * time.Millisecond
)

func newPlayer(s *session) *player {
	cfg := &s.sc.s.config
	p := &player{c: s.sc.c, tracks: s.desc.tracks, streams: make([]*stream, len(s.desc.tracks)),
		hardLimit: 2 * cfg.MaxQueuedBytes, lossWindow: cfg.PlayerLossWindow}
	if cfg.MaxPlayerLoss > 0 && cfg.MaxPlayerLoss < 1 {
		p.maxLoss = int(cfg.MaxPlayerLoss * 256)
	}
	for _, st := range s.streams {
		p.streams[st.index] = st
	}
	return p
}

// position returns the sequence number and RTP timestamp of the next
// packet of st, as RTP-Info gives them.
func (p *player) position(st *stream) (uint16, uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// Streams set up after a pause join in.
	if p.streams[st.index] == nil {
		p.streams[st.index] = st
		if p.origin {
			st.pk.SetOrigin(p.originAt)
		}
	}
	if !st.pk.HasOrigin() {
		return st.pk.Sequence(), st.ts
	}
	return st.pk.Sequence(), st.pk.Timestamp(p.lastPTS)
}

func (p *player) OnTracks(tracks []*av.Track) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(tracks) != len(p.tracks) {
		p.closeLocked()
		return
	}
	for i, st := range p.streams {
		if st == nil {
			continue
		}
		if tracks[i] == nil || tracks[i].Codec != p.tracks[i].Codec || st.pk.SetTrack(tracks[i]) != nil {
			// What the client set up is gone.
			p.closeLocked()
			return
		}
	}
	p.restart = p.origin
}

func (p *player) OnPacket(pkt *av.Packet) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || pkt.Track < 0 || pkt.Track >= len(p.streams) || p.streams[pkt.Track] == nil {
		return
	}
	now := time.Now()
	if !p.origin {
		p.origin, p.originAt, p.wall0, p.lastDTS = true, pkt.DTS, now, pkt.DTS
		for _, st := range p.streams {
			if st != nil {
				st.pk.SetOrigin(pkt.DTS)
			}
		}
	}
	if p.restart {
		p.restart = false
		if d := pkt.DTS + p.offset; d <= p.lastDTS || d > p.lastDTS+maxClockJump {
			// As long after the last packet as the stream was quiet:
			// the sender reports' mapping of wall time to media time,
			// which a player stamps packets by (ffmpeg does), stays as
			// true as it was.
			at := p.lastDTS + max(min(now.Sub(p.lastAt), maxClockJump), minRestartGap)
			p.offset = at - pkt.DTS
		}
	}
	if p.offset != 0 {
		p.shifted = *pkt
		p.shifted.DTS += p.offset
		p.shifted.PTS += p.offset
		pkt = &p.shifted
	}
	p.lastDTS, p.lastAt = max(p.lastDTS, pkt.DTS), now
	st := p.streams[pkt.Track]
	p.lastPTS = pkt.PTS
	defer func() {
		if !st.srSent && !p.closed {
			p.sendReport(st, now)
		}
	}()
	if st.tcp {
		buf := p.buf[:0]
		_ = st.pk.Packetize(pkt, func(rp *rtp.Packet) error {
			at := len(buf)
			buf = append(buf, '$', st.channel, 0, 0)
			var err error
			buf, err = rp.Append(buf)
			n := len(buf) - at - 4
			buf[at+2], buf[at+3] = byte(n>>8), byte(n)
			return err
		})
		p.buf = buf
		if err := p.c.Send(buf); err != nil {
			p.closed = true
			return
		}
		if len(p.buf) > 1<<20 {
			p.buf = nil
		}
		if outputwait.Queued != nil && outputwait.Queued(p.c) > p.hardLimit {
			// Too far behind even with dropping: let it go.
			p.closeLocked()
		}
		return
	}
	video := p.tracks[pkt.Track] != nil && p.tracks[pkt.Track].Codec.IsVideo()
	if video && st.videoWait {
		if !pkt.Keyframe {
			return
		}
		st.videoWait = false
	}
	_ = st.pk.Packetize(pkt, func(rp *rtp.Packet) error {
		b, err := rp.Append(make([]byte, 0, rp.MarshalSize()))
		if err != nil {
			return err
		}
		if !st.rtpOut.send(b) && video {
			// The frames after this one need it: wait for a keyframe,
			// and send no more of this one.
			st.videoWait = true
			return errDatagramLost
		}
		return nil
	})
}

// errDatagramLost stops packetizing a frame one of whose datagrams the
// socket refused.
var errDatagramLost = errors.New("rtsp: datagram lost")

// receiverReport takes a compound RTCP packet from the client of st, a UDP
// stream it plays, and closes the player once its receiver reports have
// said for lossWindow that it loses more than maxLoss of the stream.
func (p *player) receiverReport(st *stream, b []byte, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.maxLoss == 0 {
		return
	}
	for len(b) > 0 {
		h, pkt, rest, err := rtp.NextRTCP(b)
		if err != nil {
			return
		}
		b = rest
		if h.Type != rtp.TypeRR || p.rr.Unmarshal(pkt) != nil {
			continue
		}
		for _, r := range p.rr.Reports {
			if r.SSRC != st.pk.SSRC() {
				continue
			}
			if int(r.FractionLost) <= p.maxLoss {
				st.lossSince = time.Time{}
				continue
			}
			if st.lossSince.IsZero() {
				st.lossSince = now
			} else if now.Sub(st.lossSince) >= p.lossWindow {
				p.closeLocked()
				return
			}
		}
	}
}

// OnClose ends the client's connection: the stream is gone.
func (p *player) OnClose(error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeLocked()
}

func (p *player) closeLocked() {
	if !p.closed {
		p.closed = true
		_ = p.c.Close()
	}
}

// close stops the player without closing the connection.
func (p *player) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

// sendReports sends an RTCP sender report for each stream that has sent.
func (p *player) sendReports(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.origin {
		return
	}
	for _, st := range p.streams {
		if st == nil {
			continue
		}
		if n, _ := st.pk.Counts(); n > 0 {
			p.sendReport(st, now)
		}
	}
}

// sendReport sends the sender report of st at wall time now.
func (p *player) sendReport(st *stream, now time.Time) {
	b := senderReport(st.pk, now, p.originAt+now.Sub(p.wall0))
	if st.tcp {
		_ = p.c.Send(AppendInterleaved(nil, st.channel+1, b))
	} else {
		st.rtcpOut.send(b)
	}
	st.srSent = true
}

// senderReport returns the compound RTCP packet of a sender report and the
// source's CNAME, as RFC 3550 section 6.1 has a sender send it.
func senderReport(pk *rtpcodec.Packetizer, now time.Time, at time.Duration) []byte {
	sr := pk.SenderReport(now, at)
	sdes := &rtp.SourceDescription{Chunks: []rtp.SDESChunk{{Source: sr.SSRC, Items: []rtp.SDESItem{{Type: rtp.SDESCNAME, Text: []byte("fib")}}}}}
	b, _ := rtp.AppendRTCP(nil, sr, sdes) // neither can fail to marshal
	return b
}

func randUint32() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:])
}
