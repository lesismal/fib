package mpegts

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/codec/opus"
)

// Demuxer defaults.
const (
	// DefaultMaxPESSize bounds the PES a Demuxer reassembles, which for
	// video is a whole access unit.
	DefaultMaxPESSize = 4 << 20
	// DefaultMaxPending bounds the packets a Demuxer holds while it waits
	// for the configuration of every track.
	DefaultMaxPending = 256
)

// maxSection is the largest PSI section: 1021 bytes of section_length and
// the 3 bytes before it.
const maxSection = 1024

// DemuxerConfig configures a Demuxer.
type DemuxerConfig struct {
	// OnTracks hears the program's tracks: once every stream has its
	// configuration, and again whenever the set of tracks or a
	// configuration changes. Packets after it index these tracks.
	OnTracks func(tracks []*av.Track)
	// OnPacket hears every packet. The packet and its data belong to the
	// callee.
	OnPacket func(pkt *av.Packet)
	// MaxPESSize is DefaultMaxPESSize when zero; a larger PES is dropped.
	MaxPESSize int
	// MaxPending is DefaultMaxPending when zero: once that many packets
	// (or MaxPESSize bytes) wait for the configuration of a track still
	// without one, the tracks that have one are announced without it, and
	// it joins them, with a new OnTracks, when its configuration comes.
	MaxPending int
}

// Demuxer reads one program of a transport stream. Its methods must not be
// called concurrently.
type Demuxer struct {
	cfg DemuxerConfig

	part  [PacketSize]byte
	partN int

	pmtPID     int // -1 until the PAT names one
	program    uint16
	pmtVersion int // -1 until a PMT is read
	pat, pmt   section

	streams map[uint16]*demuxStream
	order   []*demuxStream

	tracks    []*av.Track
	announced bool
	pending   []pendingPacket
	pendBytes int

	haveRef  bool
	ref      int64 // the last unwrapped timestamp
	haveBase bool
	base     int64 // the unwrapped DTS packet times count from
}

// section reassembles the PSI sections of one PID.
type section struct {
	buf    []byte
	active bool
}

type demuxStream struct {
	pid   uint16
	typ   uint8
	codec av.CodecID

	cc      int // -1 before the first packet
	pes     []byte
	inPES   bool
	broken  bool
	pesLen  int // 0 for a PES of unbounded length
	lastPTS int64
	hasPTS  bool

	opusCode byte
	vps      []byte
	sps      []byte
	pps      []byte
	config   []byte
	track    *av.Track
	idx      int  // index in the announced tracks, -1 when not among them
	changed  bool // the track changed since it was announced
}

type pendingPacket struct {
	s        *demuxStream
	pts, dts int64
	key      bool
	data     []byte
}

// NewDemuxer returns a Demuxer.
func NewDemuxer(cfg DemuxerConfig) *Demuxer {
	if cfg.MaxPESSize <= 0 {
		cfg.MaxPESSize = DefaultMaxPESSize
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = DefaultMaxPending
	}
	if cfg.OnTracks == nil {
		cfg.OnTracks = func([]*av.Track) {}
	}
	if cfg.OnPacket == nil {
		cfg.OnPacket = func(*av.Packet) {}
	}
	return &Demuxer{cfg: cfg, pmtPID: -1, pmtVersion: -1, streams: map[uint16]*demuxStream{}}
}

// Tracks returns the tracks last announced.
func (d *Demuxer) Tracks() []*av.Track { return d.tracks }

// Write reads the next bytes of the stream, which may start and end
// anywhere in a packet. Bytes that are not transport packets are skipped
// until the packets resume.
func (d *Demuxer) Write(b []byte) {
	for len(b) > 0 {
		if d.partN > 0 {
			k := copy(d.part[d.partN:], b)
			d.partN += k
			b = b[k:]
			if d.partN < PacketSize {
				return
			}
			d.partN = 0
			d.packet(d.part[:])
			continue
		}
		if b[0] != syncByte || (len(b) > PacketSize && b[PacketSize] != syncByte) {
			// Lost: the next sync byte with another a packet after it, or
			// at the end of what there is, is where packets resume.
			i := 1
			for ; i < len(b); i++ {
				if b[i] == syncByte && (i+PacketSize >= len(b) || b[i+PacketSize] == syncByte) {
					break
				}
			}
			b = b[i:]
			continue
		}
		if len(b) < PacketSize {
			d.partN = copy(d.part[:], b)
			return
		}
		d.packet(b[:PacketSize])
		b = b[PacketSize:]
	}
}

// Flush ends the stream: the PES each stream has in progress, which a PES
// of unbounded length ends only with the next, is read, and the tracks are
// announced if they were still waiting.
func (d *Demuxer) Flush() {
	for _, s := range d.order {
		d.finishPES(s)
	}
	if !d.announced && len(d.pending) > 0 {
		d.announce()
	}
}

// packet reads one transport packet.
func (d *Demuxer) packet(p []byte) {
	if p[1]&0x80 != 0 || p[3]&0xc0 != 0 {
		// transport_error_indicator, or scrambled.
		return
	}
	pusi := p[1]&0x40 != 0
	pid := uint16(p[1]&0x1f)<<8 | uint16(p[2])
	afc := p[3] >> 4 & 3
	cc := int(p[3] & 0x0f)
	payload := p[4:]
	discontinuity := false
	if afc&2 != 0 {
		n := int(payload[0])
		if n > len(payload)-1 {
			return
		}
		if n > 0 {
			discontinuity = payload[1]&0x80 != 0
		}
		payload = payload[1+n:]
	}
	if afc&1 == 0 {
		return
	}
	switch {
	case pid == pidPAT:
		d.psi(&d.pat, pid, pusi, payload)
	case int(pid) == d.pmtPID:
		d.psi(&d.pmt, pid, pusi, payload)
	default:
		if s := d.streams[pid]; s != nil {
			d.pesPacket(s, pusi, cc, discontinuity, payload)
		}
	}
}

// psi reassembles the sections of a PSI PID.
func (d *Demuxer) psi(sec *section, pid uint16, pusi bool, payload []byte) {
	if pusi {
		if len(payload) == 0 || int(payload[0]) >= len(payload) {
			sec.active = false
			return
		}
		ptr := int(payload[0])
		payload = payload[1:]
		if sec.active {
			sec.buf = append(sec.buf, payload[:ptr]...)
			d.sections(sec, pid)
		}
		sec.buf = append(sec.buf[:0], payload[ptr:]...)
		sec.active = true
	} else if sec.active {
		sec.buf = append(sec.buf, payload...)
	}
	d.sections(sec, pid)
}

// sections reads the complete sections at the start of sec's buffer.
func (d *Demuxer) sections(sec *section, pid uint16) {
	for sec.active {
		b := sec.buf
		if len(b) > 0 && b[0] == 0xff {
			sec.active = false // stuffing to the end of the packet
			return
		}
		if len(b) < 3 {
			return
		}
		n := 3 + (int(b[1]&0x0f)<<8 | int(b[2]))
		if n > maxSection || n < 12 {
			sec.active = false
			return
		}
		if len(b) < n {
			return
		}
		d.section(pid, b[:n])
		if sec.buf = b[n:]; len(sec.buf) == 0 {
			sec.active = false
		}
	}
}

// section reads one complete PSI section.
func (d *Demuxer) section(pid uint16, s []byte) {
	if s[1]&0x80 == 0 || crc32MPEG(s) != 0 || s[5]&1 == 0 {
		// Not long-form, corrupt, or not yet current.
		return
	}
	body := s[8 : len(s)-4]
	switch {
	case pid == pidPAT && s[0] == 0x00:
		for ; len(body) >= 4; body = body[4:] {
			prog := binary.BigEndian.Uint16(body)
			if prog == 0 {
				continue // the network PID
			}
			pmt := int(binary.BigEndian.Uint16(body[2:]) & 0x1fff)
			if pmt != d.pmtPID || prog != d.program {
				d.pmtPID, d.program, d.pmtVersion = pmt, prog, -1
				d.pmt = section{}
			}
			return
		}
	case int(pid) == d.pmtPID && s[0] == 0x02:
		if binary.BigEndian.Uint16(s[3:]) != d.program {
			return
		}
		if v := int(s[5] >> 1 & 0x1f); v != d.pmtVersion {
			d.pmtVersion = v
			d.readPMT(body)
		}
	}
}

// readPMT reads the stream loop of a PMT, keeping the streams it still
// lists and starting the ones it adds.
func (d *Demuxer) readPMT(b []byte) {
	if len(b) < 4 {
		return
	}
	n := int(binary.BigEndian.Uint16(b[2:]) & 0x0fff)
	if 4+n > len(b) {
		return
	}
	b = b[4+n:]
	var order []*demuxStream
	for len(b) >= 5 {
		typ := b[0]
		pid := binary.BigEndian.Uint16(b[1:]) & 0x1fff
		n := int(binary.BigEndian.Uint16(b[3:]) & 0x0fff)
		if 5+n > len(b) {
			break
		}
		desc := b[5 : 5+n]
		b = b[5+n:]
		codec, code := streamCodec(typ, desc)
		if codec == av.CodecUnknown {
			continue
		}
		s := d.streams[pid]
		if s == nil || s.typ != typ || s.codec != codec || s.opusCode != code {
			s = &demuxStream{pid: pid, typ: typ, codec: codec, cc: -1, idx: -1, opusCode: code}
		}
		order = append(order, s)
	}
	same := len(order) == len(d.order)
	for i := 0; same && i < len(order); i++ {
		same = order[i] == d.order[i]
	}
	if same {
		return
	}
	d.streams = make(map[uint16]*demuxStream, len(order))
	for _, s := range order {
		d.streams[s.pid] = s
	}
	d.order = order
	if d.announced {
		// Tracks the PMT dropped leave, and those it added join once they
		// have a configuration; announce what is left now.
		d.announce()
	} else {
		d.pending, d.pendBytes = d.pending[:0], 0
	}
}

// streamCodec returns the codec of a PMT entry and, for Opus, the channel
// configuration code of its extension descriptor.
func streamCodec(typ uint8, desc []byte) (av.CodecID, byte) {
	switch typ {
	case StreamTypeH264:
		return av.CodecH264, 0
	case StreamTypeH265:
		return av.CodecH265, 0
	case StreamTypeAAC:
		return av.CodecAAC, 0
	case StreamTypePrivate:
		opus, code := false, byte(2)
		for len(desc) >= 2 {
			tag, n := desc[0], int(desc[1])
			if 2+n > len(desc) {
				break
			}
			body := desc[2 : 2+n]
			desc = desc[2+n:]
			switch {
			case tag == descRegistration && string(body) == "Opus":
				opus = true
			case tag == descExtension && n >= 2 && body[0] == extOpus:
				code = body[1]
			}
		}
		if opus {
			return av.CodecOpus, code
		}
	}
	return av.CodecUnknown, 0
}

// pesPacket adds one transport packet to its stream's PES.
func (d *Demuxer) pesPacket(s *demuxStream, pusi bool, cc int, discontinuity bool, payload []byte) {
	if s.cc >= 0 && !discontinuity {
		if cc == s.cc {
			return // a duplicate packet
		}
		if cc != (s.cc+1)&0x0f {
			s.broken = true // a packet is missing from the PES
		}
	}
	s.cc = cc
	if pusi {
		d.finishPES(s)
		s.inPES, s.broken, s.pesLen = true, false, 0
		s.pes = s.pes[:0]
	}
	if !s.inPES || s.broken {
		return
	}
	if len(s.pes)+len(payload) > d.cfg.MaxPESSize {
		s.broken = true
		s.pes = s.pes[:0]
		return
	}
	s.pes = append(s.pes, payload...)
	if s.pesLen == 0 && len(s.pes) >= 6 {
		if l := int(binary.BigEndian.Uint16(s.pes[4:])); l > 0 {
			s.pesLen = 6 + l
		}
	}
	if s.pesLen > 0 && len(s.pes) >= s.pesLen {
		d.finishPES(s)
	}
}

// finishPES reads the PES s has gathered, if it is whole.
func (d *Demuxer) finishPES(s *demuxStream) {
	if !s.inPES {
		return
	}
	s.inPES = false
	p := s.pes
	if s.broken || len(p) < 9 || p[0] != 0 || p[1] != 0 || p[2] != 1 {
		return
	}
	if s.pesLen > 0 {
		if len(p) < s.pesLen || s.pesLen < 9 {
			return
		}
		p = p[:s.pesLen]
	}
	hl := 9 + int(p[8])
	if hl > len(p) {
		return
	}
	var pts, dts int64
	switch p[7] >> 6 {
	case 2:
		if hl < 14 {
			return
		}
		pts = d.unwrap(readTS(p[9:]))
		dts = pts
	case 3:
		if hl < 19 {
			return
		}
		pts = d.unwrap(readTS(p[9:]))
		dts = d.unwrap(readTS(p[14:]))
	default:
		if !s.hasPTS {
			return
		}
		pts, dts = s.lastPTS, s.lastPTS
	}
	s.lastPTS, s.hasPTS = pts, true
	data := p[hl:]
	switch s.codec {
	case av.CodecH264, av.CodecH265:
		d.video(s, pts, dts, data)
	case av.CodecAAC:
		d.aacFrames(s, pts, data)
	case av.CodecOpus:
		d.opusPackets(s, pts, data)
	}
}

// readTS reads a 33-bit PTS or DTS.
func readTS(b []byte) int64 {
	return int64(b[0]>>1&7)<<30 | int64(b[1])<<22 | int64(b[2]>>1)<<15 | int64(b[3])<<7 | int64(b[4]>>1)
}

// unwrap extends a 33-bit timestamp to the 64-bit timeline of the stream,
// taking it as the nearest such time to the last one read, whichever track
// that was from.
func (d *Demuxer) unwrap(ts int64) int64 {
	if !d.haveRef {
		d.ref, d.haveRef = ts, true
		return ts
	}
	d.ref += wrapDiff(ts, d.ref&tsMask)
	return d.ref
}

// video reads one access unit in Annex B.
func (d *Demuxer) video(s *demuxStream, pts, dts int64, data []byte) {
	hevc := s.codec == av.CodecH265
	setsChanged := false
	for n := range h264.SplitAnnexB(data) {
		var slot *[]byte
		if hevc {
			switch h265.Type(n) {
			case h265.NALVPS:
				slot = &s.vps
			case h265.NALSPS:
				slot = &s.sps
			case h265.NALPPS:
				slot = &s.pps
			}
		} else {
			switch h264.Type(n) {
			case h264.NALSPS:
				slot = &s.sps
			case h264.NALPPS:
				slot = &s.pps
			}
		}
		if slot != nil && !bytes.Equal(*slot, n) {
			*slot = append((*slot)[:0:0], n...)
			setsChanged = true
		}
	}
	if setsChanged {
		d.videoConfig(s, hevc)
	}
	au := h264.AnnexBToAVCC(nil, data, func(n []byte) bool {
		if len(n) == 0 {
			return false
		}
		return !isAUD(n, hevc) && !isParamSet(n, hevc)
	})
	if len(au) == 0 {
		return
	}
	var key bool
	if hevc {
		key = h265.IsKeyframe(au)
	} else {
		key = h264.IsKeyframe(au)
	}
	d.emit(s, pts, dts, key, au)
}

// videoConfig builds the configuration record of the parameter sets s has
// seen, and a new track when it differs from the last.
func (d *Demuxer) videoConfig(s *demuxStream, hevc bool) {
	var (
		config []byte
		err    error
		w, h   int
	)
	if hevc {
		if s.vps == nil || s.sps == nil || s.pps == nil {
			return
		}
		var c *h265.HEVCConfig
		if c, err = h265.NewHEVCConfig([][]byte{s.vps}, [][]byte{s.sps}, [][]byte{s.pps}); err == nil {
			config, err = c.Append(nil)
		}
		var sps h265.SPS
		if sps.Unmarshal(s.sps) == nil {
			w, h = sps.Width, sps.Height
		}
	} else {
		if s.sps == nil || s.pps == nil {
			return
		}
		var c *h264.AVCConfig
		if c, err = h264.NewAVCConfig([][]byte{s.sps}, [][]byte{s.pps}); err == nil {
			config, err = c.Append(nil)
		}
		var sps h264.SPS
		if sps.Unmarshal(s.sps) == nil {
			w, h = sps.Width, sps.Height
		}
	}
	if err != nil || bytes.Equal(config, s.config) {
		return
	}
	s.config = config
	d.setTrack(s, &av.Track{Codec: s.codec, Config: config, Width: w, Height: h})
}

// setTrack gives s a new track, to be announced before s's next packet.
func (d *Demuxer) setTrack(s *demuxStream, t *av.Track) {
	s.track = t
	s.changed = true
}

// aacFrames reads the ADTS frames of one PES.
func (d *Demuxer) aacFrames(s *demuxStream, pts int64, data []byte) {
	for len(data) > 0 {
		h, au, rest, err := aac.NextADTS(data)
		if err != nil {
			return
		}
		data = rest
		cfg := h.Config()
		config, err := cfg.Append(nil)
		if err != nil {
			return
		}
		if !bytes.Equal(config, s.config) {
			s.config = config
			d.setTrack(s, &av.Track{Codec: av.CodecAAC, Config: config, SampleRate: cfg.SampleRate, Channels: cfg.Channels()})
		}
		d.emit(s, pts, pts, true, append([]byte(nil), au...))
		pts += av.ToTicks(cfg.FrameDuration(), clock90k)
	}
}

// opusPackets reads the Opus packets of one PES, each after its control
// header.
func (d *Demuxer) opusPackets(s *demuxStream, pts int64, data []byte) {
	for len(data) >= 3 {
		if data[0] != 0x7f || data[1]&0xe0 != 0xe0 {
			return
		}
		flags := data[1]
		i, size := 2, 0
		for i < len(data) {
			b := data[i]
			i++
			size += int(b)
			if b != 0xff {
				break
			}
		}
		var trim int
		if flags&0x10 != 0 {
			if i+2 > len(data) {
				return
			}
			trim = int(binary.BigEndian.Uint16(data[i:]) & 0x1fff)
			i += 2
		}
		if flags&0x08 != 0 {
			i += 2
		}
		if flags&0x04 != 0 {
			if i >= len(data) {
				return
			}
			i += 1 + int(data[i])
		}
		if i+size > len(data) || size == 0 {
			return
		}
		pkt := data[i : i+size]
		data = data[i+size:]
		if s.track == nil {
			d.setTrack(s, opusTrack(s.opusCode, trim))
		}
		d.emit(s, pts, pts, true, append([]byte(nil), pkt...))
		dur, err := opus.PacketDuration(pkt)
		if err != nil {
			dur = 20 * time.Millisecond
		}
		pts += av.ToTicks(dur, clock90k)
	}
}

// opusTrack returns the track of an Opus stream of channel configuration
// code, with the OpusHead the code stands for and the encoder delay of the
// first packet's start_trim, or ffmpeg's default of 312 samples.
func opusTrack(code byte, trim int) *av.Track {
	h := opus.Head{Version: 1, Channels: 2, InputSampleRate: 48000, PreSkip: 312}
	if trim > 0 {
		h.PreSkip = uint16(trim)
	}
	if code >= 1 && code <= 8 {
		h.Channels = code
		if code > 2 {
			h.MappingFamily = 1
			h.StreamCount = opusStreams[code]
			h.CoupledCount = opusCoupled[code]
			h.ChannelMapping = opusMapping[code]
		}
	}
	config, _ := h.Append(nil)
	return &av.Track{Codec: av.CodecOpus, Config: config, SampleRate: 48000, Channels: int(h.Channels)}
}

// emit delivers one packet, or holds it until the tracks are announced.
func (d *Demuxer) emit(s *demuxStream, pts, dts int64, key bool, data []byte) {
	if s.track == nil {
		return // no configuration yet: a decoder could not use it
	}
	if d.announced {
		if s.idx < 0 || s.changed {
			d.announce()
		}
		d.deliver(s, pts, dts, key, data)
		return
	}
	d.pending = append(d.pending, pendingPacket{s: s, pts: pts, dts: dts, key: key, data: data})
	d.pendBytes += len(data)
	ready := true
	for _, o := range d.order {
		ready = ready && o.track != nil
	}
	if ready || len(d.pending) >= d.cfg.MaxPending || d.pendBytes >= d.cfg.MaxPESSize {
		d.announce()
	}
}

// announce gives OnTracks the tracks of the streams that have one, and
// then the packets held for them.
func (d *Demuxer) announce() {
	d.tracks = nil
	for _, s := range d.order {
		s.idx = -1
		if s.track != nil {
			s.idx = len(d.tracks)
			s.changed = false
			d.tracks = append(d.tracks, s.track)
		}
	}
	if !d.haveBase {
		for i, p := range d.pending {
			if i == 0 || wrapDiff(p.dts, d.base) < 0 {
				d.base = p.dts
			}
		}
		d.haveBase = len(d.pending) > 0
	}
	d.announced = true
	d.cfg.OnTracks(d.tracks)
	pending := d.pending
	d.pending, d.pendBytes = nil, 0
	for _, p := range pending {
		if p.s.idx >= 0 {
			d.deliver(p.s, p.pts, p.dts, p.key, p.data)
		}
	}
}

func (d *Demuxer) deliver(s *demuxStream, pts, dts int64, key bool, data []byte) {
	if !d.haveBase {
		d.base, d.haveBase = dts, true
	}
	d.cfg.OnPacket(&av.Packet{
		Track:    s.idx,
		DTS:      av.FromTicks(dts-d.base, clock90k),
		PTS:      av.FromTicks(pts-d.base, clock90k),
		Keyframe: key,
		Data:     data,
	})
}
