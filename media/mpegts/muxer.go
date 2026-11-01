package mpegts

import (
	"encoding/binary"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/codec/opus"
)

// Muxer defaults.
const (
	// DefaultTablesInterval is how often the PAT and PMT are repeated,
	// in stream time, besides before every video keyframe.
	DefaultTablesInterval = 100 * time.Millisecond
	// DefaultPCRInterval is how often the PCR is sent; ISO/IEC 13818-1
	// asks for at least every 100 ms.
	DefaultPCRInterval = 40 * time.Millisecond
	// DefaultTimeOffset is added to every PTS and DTS, as ffmpeg's
	// muxdelay is, so the PCR can run ahead of the DTS and a stream whose
	// presentation starts before its decoding does not start below zero.
	DefaultTimeOffset = 1400 * time.Millisecond
	// DefaultPCRLead is how far the PCR runs ahead of the DTS of the PES it
	// arrives with: the time a decoder has to receive an access unit
	// before it is due.
	DefaultPCRLead = 700 * time.Millisecond
	// DefaultPMTPID and DefaultFirstPID are the PIDs ffmpeg uses.
	DefaultPMTPID   = 0x1000
	DefaultFirstPID = 0x100
)

// MuxerConfig configures a Muxer. The zero value is ffmpeg's layout.
type MuxerConfig struct {
	// ProgramNumber is the program of the PAT and PMT; 1 when zero.
	ProgramNumber uint16
	// TransportStreamID is the PAT's transport_stream_id; 1 when zero.
	TransportStreamID uint16
	// PMTPID is the PMT's PID, DefaultPMTPID when zero; the tracks take
	// PIDs from FirstPID on, DefaultFirstPID when zero, in track order.
	PMTPID   uint16
	FirstPID uint16
	// TablesInterval is DefaultTablesInterval when zero; PCRInterval is
	// DefaultPCRInterval when zero.
	TablesInterval time.Duration
	PCRInterval    time.Duration
	// TimeOffset is added to every timestamp: DefaultTimeOffset when zero,
	// none when negative. PCRLead is DefaultPCRLead when zero, none when
	// negative.
	TimeOffset time.Duration
	PCRLead    time.Duration
	// AudioFramesPerPES groups that many consecutive frames of an audio
	// track into one PES, which saves most of the PES and stuffing
	// overhead of short frames at the cost of holding frames back; 1 (the
	// default) sends every frame at once. Flush sends a partial group.
	AudioFramesPerPES int
}

// Muxer writes the tracks of one program as a transport stream. Its methods
// must not be called concurrently.
type Muxer struct {
	cfg     MuxerConfig
	offset  int64 // 90 kHz ticks
	lead    int64 // 90 kHz ticks
	tables  []byte
	streams []*muxStream // by track index; nil for a track left out
	order   []*muxStream // the streams carried, in PID order
	pmtCC   uint8
	patCC   uint8
	pcr     *muxStream

	tablesDue  bool  // nothing written since the last tables
	lastTables int64 // 90 kHz time of the last tables
	haveTables bool
	lastPCRAt  int64 // 90 kHz DTS of the PES the last PCR came with
	lastPCR    int64 // 90 kHz PCR base
	havePCR    bool
	scratch    []byte
}

type muxStream struct {
	pid      uint16
	typ      uint8
	streamID uint8
	codec    av.CodecID
	cc       uint8
	avc      *h264.AVCConfig
	hevc     *h265.HEVCConfig
	aac      aac.Config
	haveAAC  bool
	opusCode byte
	preSkip  uint16
	opusSent bool

	// The audio frames grouped into the next PES.
	group    []byte
	groupPTS int64
	groupN   int
}

// NewMuxer returns a Muxer of tracks, leaving out those of codecs MPEG-TS
// does not carry; it fails with ErrNoTracks when that leaves none.
func NewMuxer(tracks []*av.Track, cfg MuxerConfig) (*Muxer, error) {
	if cfg.ProgramNumber == 0 {
		cfg.ProgramNumber = 1
	}
	if cfg.TransportStreamID == 0 {
		cfg.TransportStreamID = 1
	}
	if cfg.PMTPID == 0 {
		cfg.PMTPID = DefaultPMTPID
	}
	if cfg.FirstPID == 0 {
		cfg.FirstPID = DefaultFirstPID
	}
	if cfg.TablesInterval <= 0 {
		cfg.TablesInterval = DefaultTablesInterval
	}
	if cfg.PCRInterval <= 0 {
		cfg.PCRInterval = DefaultPCRInterval
	}
	if cfg.AudioFramesPerPES <= 0 {
		cfg.AudioFramesPerPES = 1
	}
	m := &Muxer{cfg: cfg, streams: make([]*muxStream, len(tracks))}
	switch {
	case cfg.TimeOffset == 0:
		m.offset = av.ToTicks(DefaultTimeOffset, clock90k)
	case cfg.TimeOffset > 0:
		m.offset = av.ToTicks(cfg.TimeOffset, clock90k)
	}
	switch {
	case cfg.PCRLead == 0:
		m.lead = av.ToTicks(DefaultPCRLead, clock90k)
	case cfg.PCRLead > 0:
		m.lead = av.ToTicks(cfg.PCRLead, clock90k)
	}
	pid := cfg.FirstPID
	for i, t := range tracks {
		if t == nil {
			continue
		}
		typ := streamTypeOf(t.Codec)
		if typ == 0 {
			continue
		}
		s := &muxStream{pid: pid, typ: typ, codec: t.Codec}
		switch t.Codec {
		case av.CodecH264:
			s.streamID = streamIDVideo
			var c h264.AVCConfig
			if c.Unmarshal(t.Config) == nil {
				s.avc = &c
			}
		case av.CodecH265:
			s.streamID = streamIDVideo
			var c h265.HEVCConfig
			if c.Unmarshal(t.Config) == nil {
				s.hevc = &c
			}
		case av.CodecAAC:
			s.streamID = streamIDAudio
			if s.aac.Unmarshal(t.Config) == nil {
				s.haveAAC = true
			} else if idx, ok := aac.SampleRateIndex(t.SampleRate); ok && t.Channels > 0 && t.Channels <= 7 {
				// A track without a config: AAC LC of its rate and
				// channels is the best guess, and the ADTS header needs
				// one.
				s.aac = aac.Config{ObjectType: aac.ObjectTypeLC, SampleRate: aac.SampleRates[idx], ChannelConfig: uint8(t.Channels)}
				s.haveAAC = true
			}
		case av.CodecOpus:
			s.streamID = streamIDPrivate1
			s.opusCode = opusChannelCode(t)
			var h opus.Head
			if h.Unmarshal(t.Config) == nil {
				s.preSkip = h.PreSkip
			}
		}
		if t.Codec == av.CodecAAC && !s.haveAAC {
			// ADTS needs the config, and a decoder needs ADTS.
			continue
		}
		m.streams[i] = s
		m.order = append(m.order, s)
		pid++
	}
	if len(m.order) == 0 {
		return nil, ErrNoTracks
	}
	m.pcr = m.order[0]
	for _, s := range m.order {
		if s.streamID == streamIDVideo {
			m.pcr = s
			break
		}
	}
	m.tables = m.buildTables()
	if len(m.tables) == 0 {
		return nil, ErrTooManyTracks
	}
	return m, nil
}

// opusChannelCode returns the channel_config_code of the Opus extension
// descriptor for t: the channel count for the layouts the code stands for
// (mapping family 0, or family 1 in Vorbis order), 0xff otherwise.
func opusChannelCode(t *av.Track) byte {
	var h opus.Head
	if h.Unmarshal(t.Config) != nil {
		switch {
		case t.Channels == 0:
			return 2
		case t.Channels <= 2:
			return byte(t.Channels)
		}
		return 0xff
	}
	n := int(h.Channels)
	switch {
	case h.MappingFamily == 0 && n >= 1 && n <= 2:
		return byte(n)
	case h.MappingFamily == 1 && n >= 1 && n <= 8 &&
		h.StreamCount == opusStreams[n] && h.CoupledCount == opusCoupled[n] &&
		string(h.ChannelMapping) == string(opusMapping[n]):
		return byte(n)
	}
	return 0xff
}

// Supported reports whether the muxer carries track i.
func (m *Muxer) Supported(i int) bool {
	return i >= 0 && i < len(m.streams) && m.streams[i] != nil
}

// PID returns the PID of track i, or 0 for a track the muxer leaves out.
func (m *Muxer) PID(i int) uint16 {
	if !m.Supported(i) {
		return 0
	}
	return m.streams[i].pid
}

// ResetContinuity sets every continuity counter back to zero, as an HLS
// segmenter may do at the start of each segment; leaving them running is
// as valid, since a segment starts with a random access point anyway.
func (m *Muxer) ResetContinuity() {
	m.patCC, m.pmtCC = 0, 0
	for _, s := range m.order {
		s.cc = 0
	}
}

// buildTables builds the PAT and PMT sections, without the transport
// packet framing, or returns nil when the PMT would not fit one packet.
func (m *Muxer) buildTables() []byte {
	pat := []byte{0x00, 0xb0, 0, // table_id, syntax, length later
		byte(m.cfg.TransportStreamID >> 8), byte(m.cfg.TransportStreamID),
		0xc1, 0, 0, // version 0, current; section 0 of 0
		byte(m.cfg.ProgramNumber >> 8), byte(m.cfg.ProgramNumber),
		0xe0 | byte(m.cfg.PMTPID>>8), byte(m.cfg.PMTPID)}
	pat = finishSection(pat)
	pmt := []byte{0x02, 0xb0, 0,
		byte(m.cfg.ProgramNumber >> 8), byte(m.cfg.ProgramNumber),
		0xc1, 0, 0,
		0xe0 | byte(m.pcr.pid>>8), byte(m.pcr.pid),
		0xf0, 0} // no program descriptors
	for _, s := range m.order {
		var desc []byte
		if s.codec == av.CodecOpus {
			desc = []byte{descRegistration, 4, 'O', 'p', 'u', 's', descExtension, 2, extOpus, s.opusCode}
		}
		pmt = append(pmt, s.typ, 0xe0|byte(s.pid>>8), byte(s.pid), 0xf0|byte(len(desc)>>8), byte(len(desc)))
		pmt = append(pmt, desc...)
	}
	pmt = finishSection(pmt)
	if len(pmt)+1 > PacketSize-4 {
		return nil
	}
	return append(pat, pmt...)
}

// finishSection fills in section_length and appends the CRC.
func finishSection(s []byte) []byte {
	n := len(s) - 3 + 4
	s[1] |= byte(n >> 8)
	s[2] = byte(n)
	return binary.BigEndian.AppendUint32(s, crc32MPEG(s))
}

// WriteTables appends the PAT and PMT to dst, as the start of an HLS
// segment needs. They are not written again before the next packet, when
// that packet would bring them anyway.
func (m *Muxer) WriteTables(dst []byte) []byte {
	if m.tablesDue {
		return dst
	}
	patLen := int(m.tables[1]&0x0f)<<8 | int(m.tables[2]) + 3
	dst = appendSectionPacket(dst, pidPAT, &m.patCC, m.tables[:patLen])
	dst = appendSectionPacket(dst, m.cfg.PMTPID, &m.pmtCC, m.tables[patLen:])
	m.tablesDue = true
	return dst
}

// appendSectionPacket appends one transport packet carrying section, which
// fits it, with pointer_field 0 and 0xff filling the rest.
func appendSectionPacket(dst []byte, pid uint16, cc *uint8, section []byte) []byte {
	dst = append(dst, syncByte, 0x40|byte(pid>>8), byte(pid), 0x10|*cc, 0)
	*cc = (*cc + 1) & 0x0f
	dst = append(dst, section...)
	for range PacketSize - 5 - len(section) {
		dst = append(dst, 0xff)
	}
	return dst
}

// WritePacket appends the transport packets of pkt to dst. A packet of an
// audio track grouped by AudioFramesPerPES may append nothing until its
// group is complete.
func (m *Muxer) WritePacket(dst []byte, pkt *av.Packet) ([]byte, error) {
	if !m.Supported(pkt.Track) {
		return dst, ErrTrack
	}
	s := m.streams[pkt.Track]
	dts := (av.ToTicks(pkt.DTS, clock90k) + m.offset) & tsMask
	pts := (av.ToTicks(pkt.PTS, clock90k) + m.offset) & tsMask
	key := s.streamID == streamIDVideo && pkt.Keyframe
	if !m.haveTables || key || wrapDiff(dts, m.lastTables) >= av.ToTicks(m.cfg.TablesInterval, clock90k) {
		dst = m.WriteTables(dst)
		m.haveTables = true
		m.lastTables = dts
	}
	switch s.codec {
	case av.CodecH264, av.CodecH265:
		es, err := m.annexB(s, pkt)
		if err != nil {
			return dst, err
		}
		dst = m.writePES(dst, s, es, pts, dts, true, key)
	case av.CodecAAC:
		if s.groupN == 0 {
			s.groupPTS = pts
		}
		var err error
		if s.group, err = s.aac.AppendADTS(s.group, len(pkt.Data)); err != nil {
			return dst, err
		}
		s.group = append(s.group, pkt.Data...)
		s.groupN++
	case av.CodecOpus:
		if s.groupN == 0 {
			s.groupPTS = pts
		}
		s.group = s.appendOpus(s.group, pkt.Data)
		s.groupN++
	}
	if s.groupN >= m.cfg.AudioFramesPerPES {
		dst = m.flushStream(dst, s)
	}
	return dst, nil
}

// Flush appends the PES of every partly grouped audio track to dst.
func (m *Muxer) Flush(dst []byte) []byte {
	for _, s := range m.order {
		dst = m.flushStream(dst, s)
	}
	return dst
}

func (m *Muxer) flushStream(dst []byte, s *muxStream) []byte {
	if s.groupN == 0 {
		return dst
	}
	dst = m.writePES(dst, s, s.group, s.groupPTS, s.groupPTS, false, false)
	s.group, s.groupN = s.group[:0], 0
	return dst
}

// appendOpus appends one Opus packet with its control header (section 5 of
// the Opus in MPEG-TS draft): the 11-bit prefix, the trim flags, the size in
// 255-byte steps, and on the first packet the encoder delay as start_trim,
// as ffmpeg writes it.
func (s *muxStream) appendOpus(dst, pkt []byte) []byte {
	flags := byte(0xe0)
	trim := !s.opusSent && s.preSkip > 0
	if trim {
		flags |= 0x10
	}
	s.opusSent = true
	dst = append(dst, 0x7f, flags)
	n := len(pkt)
	for ; n >= 255; n -= 255 {
		dst = append(dst, 0xff)
	}
	dst = append(dst, byte(n))
	if trim {
		dst = append(dst, byte(s.preSkip>>8)&0x1f, byte(s.preSkip))
	}
	return append(dst, pkt...)
}

// annexB converts the AVCC access unit of pkt to Annex B, with an access
// unit delimiter first and, for a keyframe without them, the parameter sets
// of the track's configuration.
func (m *Muxer) annexB(s *muxStream, pkt *av.Packet) ([]byte, error) {
	if !h264.ValidAVCC(pkt.Data, 4) {
		return nil, ErrBadData
	}
	out := m.scratch[:0]
	hevc := s.codec == av.CodecH265
	if hevc {
		out = append(out, 0, 0, 0, 1, 0x46, 0x01, 0x50)
	} else {
		out = append(out, 0, 0, 0, 1, 0x09, 0xf0)
	}
	if pkt.Keyframe {
		var haveSets bool
		for n := range h264.SplitAVCC(pkt.Data, 4) {
			if isParamSet(n, hevc) {
				haveSets = true
				break
			}
		}
		if !haveSets {
			switch {
			case s.avc != nil:
				for _, n := range s.avc.SPS {
					out = appendNALU(out, n)
				}
				for _, n := range s.avc.PPS {
					out = appendNALU(out, n)
				}
			case s.hevc != nil:
				for _, t := range []h265.NALUType{h265.NALVPS, h265.NALSPS, h265.NALPPS} {
					for _, n := range s.hevc.NALUs(t) {
						out = appendNALU(out, n)
					}
				}
			}
		}
	}
	for n := range h264.SplitAVCC(pkt.Data, 4) {
		if len(n) == 0 || isAUD(n, hevc) {
			continue
		}
		out = appendNALU(out, n)
	}
	m.scratch = out
	return out, nil
}

func appendNALU(dst, n []byte) []byte {
	return append(append(dst, 0, 0, 0, 1), n...)
}

func isAUD(n []byte, hevc bool) bool {
	if hevc {
		return h265.Type(n) == h265.NALAUD
	}
	return h264.Type(n) == h264.NALAUD
}

func isParamSet(n []byte, hevc bool) bool {
	if hevc {
		t := h265.Type(n)
		return t == h265.NALVPS || t == h265.NALSPS || t == h265.NALPPS
	}
	t := h264.Type(n)
	return t == h264.NALSPS || t == h264.NALPPS
}

// wrapDiff returns a-b of two 33-bit times, taking the shorter way round.
func wrapDiff(a, b int64) int64 {
	d := (a - b) & tsMask
	if d >= 1<<32 {
		d -= 1 << 33
	}
	return d
}

// writePES appends es as one PES on s's PID.
func (m *Muxer) writePES(dst []byte, s *muxStream, es []byte, pts, dts int64, video, key bool) []byte {
	m.tablesDue = false
	pcr := int64(-1)
	if d := wrapDiff(dts, m.lastPCRAt); !m.havePCR || d >= av.ToTicks(m.cfg.PCRInterval, clock90k) || d < -clock90k {
		m.lastPCRAt = dts
		pcr = m.nextPCR(dts) * 300
		if s != m.pcr {
			// A PCR-only packet on the PCR PID, which carries no payload
			// and leaves its continuity counter alone.
			dst, _ = appendTSPacket(dst, m.pcr.pid, false, &m.pcr.cc, 0, pcr, nil)
			pcr = -1
		}
	}

	hdr := [19]byte{0, 0, 1, s.streamID}
	n := 9
	hdr[6] = 0x80
	if video {
		hdr[6] |= 0x04 // data_alignment_indicator
	}
	if pts != dts {
		hdr[7] = 0xc0
		hdr[8] = 10
		putTS(hdr[9:], 0x30, pts)
		putTS(hdr[14:], 0x10, dts)
		n += 10
	} else {
		hdr[7] = 0x80
		hdr[8] = 5
		putTS(hdr[9:], 0x20, pts)
		n += 5
	}
	if l := n - 6 + len(es); !video && l <= 0xffff {
		binary.BigEndian.PutUint16(hdr[4:], uint16(l))
	}
	payload := hdr[:n]
	first := true
	for len(payload) > 0 || len(es) > 0 {
		var flags byte
		p := int64(-1)
		if first && key {
			flags |= 0x40 // random_access_indicator
		}
		if first {
			p = pcr
		}
		var used int
		if len(payload) > 0 {
			// The PES header and the start of the data share the first
			// packet, which has room for the whole header: join them in a
			// buffer of one packet's payload.
			var buf [PacketSize]byte
			k := copy(buf[:], payload)
			k += copy(buf[k:PacketSize-4], es)
			dst, used = appendTSPacket(dst, s.pid, first, &s.cc, flags, p, buf[:k])
			es = es[used-len(payload):]
			payload = nil
		} else {
			dst, used = appendTSPacket(dst, s.pid, first, &s.cc, flags, p, es)
			es = es[used:]
		}
		first = false
	}
	return dst
}

// nextPCR returns the PCR base for a PES of DTS dts, the lead ahead of it
// and never earlier than the last one but after a jump back of more than a
// second, which is a new timeline rather than jitter between tracks.
func (m *Muxer) nextPCR(dts int64) int64 {
	pcr := (dts - m.lead) & tsMask
	if d := wrapDiff(pcr, m.lastPCR); m.havePCR && d < 0 && d >= -clock90k {
		pcr = m.lastPCR
	}
	m.lastPCR, m.havePCR = pcr, true
	return pcr
}

// putTS writes a 33-bit PTS or DTS with its 4-bit prefix and marker bits.
func putTS(b []byte, prefix byte, ts int64) {
	b[0] = prefix | byte(ts>>29)&0x0e | 1
	b[1] = byte(ts >> 22)
	b[2] = byte(ts>>14) | 1
	b[3] = byte(ts >> 7)
	b[4] = byte(ts<<1) | 1
}

// appendTSPacket appends one transport packet of pid with as much of
// payload as fits, an adaptation field when flags or a PCR (pcr >= 0) need
// one, and stuffing in the adaptation field when payload is short. It
// returns how much of payload went in.
func appendTSPacket(dst []byte, pid uint16, pusi bool, cc *uint8, flags byte, pcr int64, payload []byte) ([]byte, int) {
	af := 0
	if flags != 0 || pcr >= 0 {
		af = 2
		if pcr >= 0 {
			af += 6
		}
	}
	n := min(len(payload), PacketSize-4-af)
	af = PacketSize - 4 - n
	b1 := byte(pid>>8) & 0x1f
	if pusi {
		b1 |= 0x40
	}
	var afc byte
	switch {
	case af == 0:
		afc = 0x10
	case n == 0:
		afc = 0x20
	default:
		afc = 0x30
	}
	if n > 0 {
		dst = append(dst, syncByte, b1, byte(pid), afc|*cc)
		*cc = (*cc + 1) & 0x0f
	} else {
		// A packet without payload repeats the counter of the last one
		// with payload.
		dst = append(dst, syncByte, b1, byte(pid), afc|(*cc-1)&0x0f)
	}
	if af > 0 {
		dst = append(dst, byte(af-1))
		if af > 1 {
			if pcr >= 0 {
				flags |= 0x10
			}
			dst = append(dst, flags)
			k := 2
			if pcr >= 0 {
				base, ext := pcr/300, pcr%300
				dst = append(dst, byte(base>>25), byte(base>>17), byte(base>>9), byte(base>>1),
					byte(base<<7)|0x7e|byte(ext>>8), byte(ext))
				k += 6
			}
			for ; k < af; k++ {
				dst = append(dst, 0xff)
			}
		}
	}
	return append(dst, payload[:n]...), n
}
