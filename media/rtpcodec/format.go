package rtpcodec

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/webrtc/sdp"
)

// Errors of the format layer.
var (
	// ErrUnsupported is what ParseMedia and NewFormat report for a codec,
	// or a mode of one, the package has no payload format for. A session
	// that offers such a media section is still usable without it.
	ErrUnsupported = errors.New("rtpcodec: unsupported payload format")
	// ErrFormat is what ParseMedia reports for format parameters it cannot
	// make sense of, such as a config= that is not hex.
	ErrFormat = errors.New("rtpcodec: malformed format parameters")
)

// Static payload types of RFC 3551 the package knows.
const (
	PayloadTypePCMU = 0
	PayloadTypePCMA = 8
)

// Format is one RTP payload format of a media section: the payload type
// and clock rate, the track it carries, and the parameters of the payload
// format a packetizer and a depacketizer need. ParseMedia reads one from a
// media section and NewFormat makes one for a track; Media writes it back.
type Format struct {
	PayloadType uint8
	ClockRate   int
	// Track is the track the format carries. For a format parsed from SDP
	// its Config holds the out-of-band configuration the SDP gave
	// (sprop-parameter-sets, config=), nil when it gave none.
	Track *av.Track
	// Control is the a=control attribute: the URL, absolute or relative to
	// the session's, a stream is set up by in RTSP.
	Control string

	// PacketizationMode is H.264's packetization-mode: 0 (single NAL unit)
	// or 1 (non-interleaved), which is what the packetizer writes. Mode 2
	// (interleaved) is read but not written.
	PacketizationMode int

	// LATM says AAC travels as MP4A-LATM (RFC 6416) rather than
	// mpeg4-generic (RFC 3640), and CPresent that its StreamMuxConfig is
	// in band (cpresent=1) rather than in config=.
	LATM     bool
	CPresent bool

	// The AU-header fields of mpeg4-generic (RFC 3640 section 3.3.6), in
	// bits: SizeLength, IndexLength and IndexDeltaLength of every AU-header,
	// CTSDeltaLength and DTSDeltaLength of its optional time offsets, and
	// RandomAccessIndication, StreamStateIndication and AuxDataSizeLength
	// of what the depacketizer skips. NewFormat writes AAC-hbr: 13, 3, 3.
	SizeLength             int
	IndexLength            int
	IndexDeltaLength       int
	CTSDeltaLength         int
	DTSDeltaLength         int
	RandomAccessIndication bool
	StreamStateIndication  int
	AuxDataSizeLength      int

	// smc is the StreamMuxConfig of config=, for MP4A-LATM with cpresent=0.
	smc *streamMuxConfig
}

// NewFormat returns the format a track is sent in with payload type pt,
// which a G.711 track ignores for its static type. H.264 is sent in
// packetization mode 1, H.265 per RFC 7798 without DONL, AAC as
// mpeg4-generic AAC-hbr, Opus per RFC 7587 and G.711 per RFC 3551. Other
// codecs report ErrUnsupported, as does an AAC track without a valid
// AudioSpecificConfig, which the format cannot do without.
func NewFormat(track *av.Track, pt uint8) (*Format, error) {
	f := &Format{PayloadType: pt, Track: track}
	switch track.Codec {
	case av.CodecH264:
		f.ClockRate, f.PacketizationMode = 90000, 1
	case av.CodecH265:
		f.ClockRate = 90000
	case av.CodecAAC:
		var c aac.Config
		if err := c.Unmarshal(track.Config); err != nil {
			return nil, ErrUnsupported
		}
		f.ClockRate = track.SampleRate
		if f.ClockRate <= 0 {
			f.ClockRate = c.SampleRate
		}
		f.SizeLength, f.IndexLength, f.IndexDeltaLength = 13, 3, 3
	case av.CodecOpus:
		f.ClockRate = 48000
	case av.CodecPCMU, av.CodecPCMA:
		f.ClockRate = 8000
		f.PayloadType = PayloadTypePCMU
		if track.Codec == av.CodecPCMA {
			f.PayloadType = PayloadTypePCMA
		}
	default:
		return nil, ErrUnsupported
	}
	return f, nil
}

// NewLATMFormat returns the format an AAC track is sent in as MP4A-LATM
// (RFC 6416) with its StreamMuxConfig out of band, cpresent=0, which some
// receivers take where they do not take mpeg4-generic.
func NewLATMFormat(track *av.Track, pt uint8) (*Format, error) {
	if track.Codec != av.CodecAAC {
		return nil, ErrUnsupported
	}
	f, err := NewFormat(track, pt)
	if err != nil {
		return nil, err
	}
	f.LATM = true
	f.SizeLength, f.IndexLength, f.IndexDeltaLength = 0, 0, 0
	return f, nil
}

// Media returns the media section that describes the format: its m= line
// with port 0 and RTP/AVP, as RTSP writes them, its a=rtpmap and a=fmtp,
// and a=control when Control is set. An H.264 or H.265 track's parameter
// sets go into sprop-parameter-sets or sprop-vps/sps/pps, an AAC track's
// AudioSpecificConfig into config=.
func (f *Format) Media() (*sdp.MediaDescription, error) {
	kind := "audio"
	if f.Track.Codec.IsVideo() {
		kind = "video"
	}
	md := sdp.NewMedia(kind, 0, "RTP/AVP")
	c := sdp.Codec{PayloadType: f.PayloadType, ClockRate: uint32(f.ClockRate)}
	switch f.Track.Codec {
	case av.CodecH264:
		c.Name = "H264"
		c.Fmtp = h264Fmtp(f)
	case av.CodecH265:
		c.Name = "H265"
		c.Fmtp = h265Fmtp(f.Track.Config)
	case av.CodecAAC:
		var cfg aac.Config
		if err := cfg.Unmarshal(f.Track.Config); err != nil {
			return nil, ErrUnsupported
		}
		c.Channels = uint16(max(f.Track.Channels, cfg.Channels(), 1))
		if f.LATM {
			c.Name = "MP4A-LATM"
			smc, err := appendStreamMuxConfig(nil, f.Track.Config)
			if err != nil {
				return nil, err
			}
			c.Fmtp = "profile-level-id=30;object=" + strconv.Itoa(int(cfg.ObjectType)) +
				";cpresent=0;config=" + hex.EncodeToString(smc)
		} else {
			c.Name = "MPEG4-GENERIC"
			c.Fmtp = "profile-level-id=1;mode=AAC-hbr;sizelength=" + strconv.Itoa(f.SizeLength) +
				";indexlength=" + strconv.Itoa(f.IndexLength) +
				";indexdeltalength=" + strconv.Itoa(f.IndexDeltaLength) +
				";config=" + hex.EncodeToString(f.Track.Config)
		}
	case av.CodecOpus:
		c.Name, c.Channels = "opus", 2
		if f.Track.Channels == 2 {
			c.Fmtp = "sprop-stereo=1"
		}
	case av.CodecPCMU:
		c.Name = "PCMU"
	case av.CodecPCMA:
		c.Name = "PCMA"
	default:
		return nil, ErrUnsupported
	}
	md.AddCodec(c)
	if f.Control != "" {
		md.AddAttribute("control", f.Control)
	}
	return md, nil
}

func h264Fmtp(f *Format) string {
	s := "packetization-mode=" + strconv.Itoa(f.PacketizationMode)
	var c h264.AVCConfig
	if c.Unmarshal(f.Track.Config) != nil || len(c.SPS) == 0 || len(c.SPS[0]) < 4 {
		return s
	}
	s += ";profile-level-id=" + strings.ToUpper(hex.EncodeToString(c.SPS[0][1:4]))
	var sets []string
	for _, ps := range append(c.SPS[:len(c.SPS):len(c.SPS)], c.PPS...) {
		sets = append(sets, base64.StdEncoding.EncodeToString(ps))
	}
	return s + ";sprop-parameter-sets=" + strings.Join(sets, ",")
}

func h265Fmtp(config []byte) string {
	var c h265.HEVCConfig
	if c.Unmarshal(config) != nil {
		return ""
	}
	var parts []string
	for _, p := range []struct {
		name string
		t    h265.NALUType
	}{{"sprop-vps", h265.NALVPS}, {"sprop-sps", h265.NALSPS}, {"sprop-pps", h265.NALPPS}} {
		var sets []string
		for _, ps := range c.NALUs(p.t) {
			sets = append(sets, base64.StdEncoding.EncodeToString(ps))
		}
		if len(sets) > 0 {
			parts = append(parts, p.name+"="+strings.Join(sets, ","))
		}
	}
	return strings.Join(parts, ";")
}

// ParseSession returns the format of every media section of sd the package
// can carry, in order, skipping the others.
func ParseSession(sd *sdp.SessionDescription) []*Format {
	var fs []*Format
	for _, md := range sd.Media {
		if f, err := ParseMedia(md); err == nil {
			fs = append(fs, f)
		}
	}
	return fs
}

// ParseMedia returns the format of the first payload type of md the
// package can carry, with the track it describes: the codec, the clock
// rate and channels of a=rtpmap (or of a static payload type), and the
// configuration of a=fmtp. It reports ErrUnsupported when md has none, and
// ErrFormat when the one it has comes with parameters it cannot read.
func ParseMedia(md *sdp.MediaDescription) (*Format, error) {
	control, _ := md.Attribute("control")
	err := ErrUnsupported
	for _, c := range md.Codecs() {
		var f *Format
		f, err = parseCodec(c)
		if err == nil {
			f.Control = control
			return f, nil
		}
		if err != ErrUnsupported {
			return nil, err
		}
	}
	return nil, err
}

func parseCodec(c sdp.Codec) (*Format, error) {
	name := strings.ToUpper(c.Name)
	if name == "" {
		switch c.PayloadType {
		case PayloadTypePCMU:
			name, c.ClockRate = "PCMU", 8000
		case PayloadTypePCMA:
			name, c.ClockRate = "PCMA", 8000
		}
	}
	f := &Format{PayloadType: c.PayloadType, ClockRate: int(c.ClockRate)}
	if f.ClockRate <= 0 {
		return nil, ErrUnsupported
	}
	channels := max(int(c.Channels), 1)
	switch name {
	case "H264":
		return f, parseH264(f, c)
	case "H265":
		return f, parseH265(f, c)
	case "MPEG4-GENERIC":
		return f, parseGeneric(f, c, channels)
	case "MP4A-LATM":
		return f, parseLATM(f, c, channels)
	case "OPUS":
		f.Track = &av.Track{Codec: av.CodecOpus, SampleRate: 48000, Channels: 2}
		if v, ok := c.Param("sprop-stereo"); ok && v == "0" {
			f.Track.Channels = 1
		}
		return f, nil
	case "PCMU", "PCMA":
		f.Track = &av.Track{Codec: av.CodecPCMU, SampleRate: f.ClockRate, Channels: channels}
		if name == "PCMA" {
			f.Track.Codec = av.CodecPCMA
		}
		return f, nil
	}
	return nil, ErrUnsupported
}

func parseH264(f *Format, c sdp.Codec) error {
	f.Track = &av.Track{Codec: av.CodecH264}
	if v, ok := c.Param("packetization-mode"); ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 2 {
			return ErrFormat
		}
		f.PacketizationMode = n
	}
	v, ok := c.Param("sprop-parameter-sets")
	if !ok {
		return nil
	}
	var sps, pps [][]byte
	for _, s := range strings.Split(v, ",") {
		ps, err := decodeBase64(s)
		if err != nil {
			return ErrFormat
		}
		switch h264.Type(ps) {
		case h264.NALSPS:
			sps = append(sps, ps)
		case h264.NALPPS:
			pps = append(pps, ps)
		}
	}
	// A config the parameter sets do not make is learned from the stream
	// instead, as when they are absent.
	setH264Config(f.Track, sps, pps)
	return nil
}

// setH264Config sets the track's config and size from parameter sets, and
// reports whether they made one.
func setH264Config(t *av.Track, sps, pps [][]byte) bool {
	if len(sps) == 0 || len(pps) == 0 {
		return false
	}
	var s h264.SPS
	if s.Unmarshal(sps[0]) != nil {
		return false
	}
	cfg, err := h264.NewAVCConfig(sps, pps)
	if err != nil {
		return false
	}
	b, err := cfg.Append(nil)
	if err != nil {
		return false
	}
	t.Config, t.Width, t.Height = b, s.Width, s.Height
	return true
}

func parseH265(f *Format, c sdp.Codec) error {
	f.Track = &av.Track{Codec: av.CodecH265}
	if v, ok := c.Param("sprop-max-don-diff"); ok && v != "0" {
		// DONL fields in every packet, which the depacketizer does not
		// read.
		return ErrUnsupported
	}
	var sets [3][][]byte
	for i, name := range []string{"sprop-vps", "sprop-sps", "sprop-pps"} {
		v, ok := c.Param(name)
		if !ok {
			continue
		}
		for _, s := range strings.Split(v, ",") {
			ps, err := decodeBase64(s)
			if err != nil {
				return ErrFormat
			}
			sets[i] = append(sets[i], ps)
		}
	}
	setH265Config(f.Track, sets[0], sets[1], sets[2])
	return nil
}

// setH265Config is setH264Config for H.265.
func setH265Config(t *av.Track, vps, sps, pps [][]byte) bool {
	if len(vps) == 0 || len(sps) == 0 || len(pps) == 0 {
		return false
	}
	var s h265.SPS
	if s.Unmarshal(sps[0]) != nil {
		return false
	}
	cfg, err := h265.NewHEVCConfig(vps, sps, pps)
	if err != nil {
		return false
	}
	b, err := cfg.Append(nil)
	if err != nil {
		return false
	}
	t.Config, t.Width, t.Height = b, s.Width, s.Height
	return true
}

func parseGeneric(f *Format, c sdp.Codec, channels int) error {
	if mode, _ := c.Param("mode"); !strings.EqualFold(mode, "AAC-hbr") && !strings.EqualFold(mode, "AAC-lbr") {
		return ErrUnsupported
	}
	if v, ok := c.Param("streamtype"); ok && v != "5" {
		return ErrUnsupported
	}
	cfgHex, _ := c.Param("config")
	config, err := hex.DecodeString(cfgHex)
	if err != nil {
		return ErrFormat
	}
	var cfg aac.Config
	if cfg.Unmarshal(config) != nil {
		return ErrFormat
	}
	ints := []struct {
		name string
		dst  *int
	}{
		{"sizelength", &f.SizeLength}, {"indexlength", &f.IndexLength},
		{"indexdeltalength", &f.IndexDeltaLength}, {"ctsdeltalength", &f.CTSDeltaLength},
		{"dtsdeltalength", &f.DTSDeltaLength}, {"streamstateindication", &f.StreamStateIndication},
		{"auxiliarydatasizelength", &f.AuxDataSizeLength},
	}
	for _, p := range ints {
		if v, ok := c.Param(p.name); ok {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 || n > 32 {
				return ErrFormat
			}
			*p.dst = n
		}
	}
	if v, _ := c.Param("randomaccessindication"); v == "1" {
		f.RandomAccessIndication = true
	}
	if f.SizeLength == 0 {
		return ErrFormat
	}
	f.Track = aacTrack(config, &cfg, channels)
	return nil
}

func aacTrack(config []byte, cfg *aac.Config, channels int) *av.Track {
	t := &av.Track{Codec: av.CodecAAC, Config: config, SampleRate: cfg.OutputSampleRate(), Channels: cfg.Channels()}
	if t.Channels == 0 {
		t.Channels = channels
	}
	return t
}

func parseLATM(f *Format, c sdp.Codec, channels int) error {
	f.LATM = true
	if v, ok := c.Param("cpresent"); !ok || v != "0" {
		// The default is 1: the StreamMuxConfig travels in band, and the
		// track's config is learned from it.
		f.CPresent = true
	}
	if v, ok := c.Param("config"); ok && !f.CPresent {
		smc, err := hex.DecodeString(v)
		if err != nil {
			return ErrFormat
		}
		config, err := parseStreamMuxConfig(smc)
		if err != nil {
			return ErrFormat
		}
		var cfg aac.Config
		_ = cfg.Unmarshal(config.asc) // checked by parseStreamMuxConfig
		f.Track = aacTrack(config.asc, &cfg, channels)
		f.smc = config
		return nil
	}
	if !f.CPresent {
		return ErrFormat
	}
	f.Track = &av.Track{Codec: av.CodecAAC, SampleRate: f.ClockRate, Channels: channels}
	return nil
}

// decodeBase64 decodes a parameter set, with or without padding, as
// encoders write it both ways.
func decodeBase64(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) > 0 {
		return b, nil
	}
	b, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(s, "="))
	if err != nil || len(b) == 0 {
		return nil, ErrFormat
	}
	return b, nil
}
