package rtc

import (
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/rtpcodec"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/sdp"
)

// The rtcp-fb values the codecs this package sends and receives are
// offered with. There is no "nack": a client neither sends NACKs nor keeps
// the history a retransmission would come from. A WHEP session answers
// the NACKs a viewer's offer asks for (Config.RetransmitPackets).
var (
	videoFeedback = []string{"ccm fir", "nack pli"}
	audioFeedback []string
)

// defaultProfileLevelID is the H.264 profile-level-id of a track whose
// configuration is not known yet: constrained baseline, level 3.1, which
// every browser decodes.
const defaultProfileLevelID = "42e01f"

// h264Fmtp returns the fmtp of H.264 in packetization mode 1 with the
// given profile-level-id.
func h264Fmtp(profileLevelID string) string {
	return "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=" + profileLevelID
}

// profileLevelID returns the profile-level-id of an H.264 track, from the
// profile, constraint flags and level of its AVCDecoderConfigurationRecord.
func profileLevelID(t *av.Track) string {
	if t == nil || len(t.Config) < 4 {
		return defaultProfileLevelID
	}
	return hex.EncodeToString(t.Config[1:4])
}

// h264Class names the profile of a profile-level-id as WebRTC matches
// them: its profile_idc, or "cb" for the constrained baseline browsers set
// apart (RFC 6184 section 8.1), as package webrtc does.
func h264Class(id string) string {
	id = strings.ToLower(id)
	if len(id) != 6 {
		return ""
	}
	b, err := hex.DecodeString(id)
	if err != nil {
		return ""
	}
	idc := id[:2]
	if (idc == "42" || idc == "4d" || idc == "58") && b[1]&0x40 != 0 {
		return "cb"
	}
	return idc
}

// h264Decodes reports whether a decoder of the offered class decodes a
// stream of the stream class: its own profile, or one the offered profile
// contains. Constrained baseline is a subset of baseline, main and high,
// and main of high.
func h264Decodes(offered, stream string) bool {
	switch {
	case offered == stream:
		return true
	case stream == "cb":
		return offered == "42" || offered == "4d" || offered == "64"
	case stream == "4d":
		return offered == "64"
	}
	return false
}

// toSDP returns c as an SDP codec.
func toSDP(c webrtc.Codec) sdp.Codec {
	return sdp.Codec{PayloadType: c.PayloadType, Name: c.Name(), ClockRate: c.ClockRate,
		Channels: c.Channels, Fmtp: c.Fmtp, Feedback: c.Feedback}
}

// fromSDP returns the SDP codec c of a section of kind as a webrtc codec.
func fromSDP(kind string, c sdp.Codec) webrtc.Codec {
	return webrtc.Codec{MimeType: kind + "/" + c.Name, ClockRate: c.ClockRate, Channels: c.Channels,
		Fmtp: c.Fmtp, Feedback: c.Feedback, PayloadType: c.PayloadType}
}

// formatOf returns the RTP payload format of codec c, with the track it
// carries, when package rtpcodec can depacketize it.
func formatOf(c webrtc.Codec) (*rtpcodec.Format, error) {
	md := sdp.NewMedia(string(c.Kind()), 9, "UDP/TLS/RTP/SAVPF", strconv.Itoa(int(c.PayloadType)))
	md.AddCodec(toSDP(c))
	return rtpcodec.ParseMedia(md)
}

// receivable reports whether a codec the peer sends can be published:
// H.264 in packetization mode 0 or 1, H.265, Opus and G.711.
func receivable(c webrtc.Codec) bool {
	f, err := formatOf(c)
	if err != nil {
		return false
	}
	switch f.Track.Codec {
	case av.CodecH264:
		return f.PacketizationMode < 2
	case av.CodecH265, av.CodecOpus, av.CodecPCMU, av.CodecPCMA:
		return true
	}
	return false
}

// receiveCodecs returns the codecs of the offer's media sections this
// package can receive, as the PeerConnection answering it is configured
// with: exactly what was offered, so that whatever the sender prefers among
// them is accepted.
func receiveCodecs(offer *sdp.SessionDescription) []webrtc.Codec {
	var out []webrtc.Codec
	for _, md := range offer.Media {
		kind := md.Kind()
		if kind != "audio" && kind != "video" {
			continue
		}
		for _, sc := range md.Codecs() {
			c := fromSDP(kind, sc)
			if sc.Name == "" || !receivable(c) || containsCodec(out, c) {
				continue
			}
			out = append(out, c)
		}
	}
	return out
}

func containsCodec(list []webrtc.Codec, c webrtc.Codec) bool {
	for _, x := range list {
		if strings.EqualFold(x.MimeType, c.MimeType) && x.ClockRate == c.ClockRate &&
			x.Channels == c.Channels && x.Fmtp == c.Fmtp {
			return true
		}
	}
	return false
}

// DefaultReceiveCodecs are the codecs a client that plays a stream (Pull)
// offers: H.264 in packetization mode 1 in the profiles browsers send,
// H.265, Opus and G.711.
func DefaultReceiveCodecs() []webrtc.Codec {
	h264 := func(pt uint8, profile string) webrtc.Codec {
		return webrtc.Codec{MimeType: "video/H264", ClockRate: 90000, PayloadType: pt, Feedback: videoFeedback,
			Fmtp: h264Fmtp(profile)}
	}
	return []webrtc.Codec{
		{MimeType: "audio/opus", ClockRate: 48000, Channels: 2, PayloadType: 111, Fmtp: "minptime=10;useinbandfec=1"},
		{MimeType: "audio/PCMU", ClockRate: 8000, PayloadType: 0},
		{MimeType: "audio/PCMA", ClockRate: 8000, PayloadType: 8},
		h264(102, "42001f"),
		h264(108, "42e01f"),
		h264(112, "64001f"),
		h264(127, "4d001f"),
		{MimeType: "video/H265", ClockRate: 90000, PayloadType: 49, Feedback: videoFeedback},
	}
}

// sendCodec returns the codec a track is sent in, when WebRTC can carry it
// and rtpcodec packetize it: H.264, H.265, Opus and G.711. offered, when
// not nil, are the codecs of the peer's media section of the track's kind:
// the codec is then the one among them that suits the track (for H.264,
// a profile whose decoder decodes the track's), with the peer's payload
// type and parameters, and none when the peer offers none that does.
func sendCodec(t *av.Track, offered []webrtc.Codec) (webrtc.Codec, bool) {
	var c webrtc.Codec
	switch t.Codec {
	case av.CodecH264:
		c = webrtc.Codec{MimeType: "video/H264", ClockRate: 90000, PayloadType: 102, Feedback: videoFeedback,
			Fmtp: h264Fmtp(profileLevelID(t))}
	case av.CodecH265:
		c = webrtc.Codec{MimeType: "video/H265", ClockRate: 90000, PayloadType: 49, Feedback: videoFeedback}
	case av.CodecOpus:
		c = webrtc.Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 2, PayloadType: 111,
			Fmtp: "minptime=10;useinbandfec=1"}
	case av.CodecPCMU:
		c = webrtc.Codec{MimeType: "audio/PCMU", ClockRate: 8000, PayloadType: 0}
	case av.CodecPCMA:
		c = webrtc.Codec{MimeType: "audio/PCMA", ClockRate: 8000, PayloadType: 8}
	default:
		return webrtc.Codec{}, false
	}
	if offered == nil {
		return c, true
	}
	if t.Codec == av.CodecH264 {
		return pickH264(offered, h264Class(profileLevelID(t)))
	}
	for _, o := range offered {
		if strings.EqualFold(o.MimeType, c.MimeType) && o.ClockRate == c.ClockRate {
			return o, true
		}
	}
	return webrtc.Codec{}, false
}

// pickH264 returns the offered H.264 codec in packetization mode 1 that
// decodes a stream of class: of the stream's own profile if there is one,
// or else the first that contains it.
func pickH264(offered []webrtc.Codec, class string) (webrtc.Codec, bool) {
	var fallback *webrtc.Codec
	for i, o := range offered {
		if !strings.EqualFold(o.Name(), "H264") || o.ClockRate != 90000 {
			continue
		}
		if pm, _ := sdp.FmtpParam(o.Fmtp, "packetization-mode"); pm != "1" {
			continue
		}
		id, ok := sdp.FmtpParam(o.Fmtp, "profile-level-id")
		if !ok {
			id = "42001f"
		}
		oc := h264Class(id)
		if oc == class {
			return o, true
		}
		if fallback == nil && h264Decodes(oc, class) {
			fallback = &offered[i]
		}
	}
	if fallback != nil {
		return *fallback, true
	}
	return webrtc.Codec{}, false
}

// offeredCodecs returns the codecs of every media section of kind in the
// offer.
func offeredCodecs(offer *sdp.SessionDescription, kind webrtc.MediaKind) []webrtc.Codec {
	out := []webrtc.Codec{}
	for _, md := range offer.Media {
		if md.Kind() != string(kind) || md.Rejected() {
			continue
		}
		for _, sc := range md.Codecs() {
			if sc.Name != "" {
				out = append(out, fromSDP(md.Kind(), sc))
			}
		}
	}
	return out
}

// kindOf returns the WebRTC media kind of a codec.
func kindOf(c av.CodecID) webrtc.MediaKind {
	if c.IsVideo() {
		return webrtc.MediaKindVideo
	}
	return webrtc.MediaKindAudio
}
