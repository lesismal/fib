package webrtc

import (
	"strings"

	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// MediaKind is the kind of a media section and of the tracks in it.
type MediaKind string

const (
	MediaKindAudio MediaKind = "audio"
	MediaKindVideo MediaKind = "video"
)

// Codec is an RTP payload format a PeerConnection may negotiate. The
// package carries RTP packets and never looks inside their payloads, so a
// codec here is only what SDP says of it.
type Codec struct {
	// MimeType is the kind and the encoding name, "video/VP8" or
	// "audio/opus"; names compare without regard to case.
	MimeType  string
	ClockRate uint32
	// Channels is 2 for Opus and 0 for codecs that do not say.
	Channels uint16
	// Fmtp holds the format parameters, "minptime=10;useinbandfec=1".
	Fmtp string
	// Feedback lists the rtcp-fb values, "nack", "nack pli", "ccm fir".
	Feedback []string
	// PayloadType is the payload type an offer gives the codec. An answer
	// takes the offer's, so a negotiated codec may have another.
	PayloadType uint8
}

// Kind returns the codec's media kind, from its MimeType.
func (c Codec) Kind() MediaKind {
	kind, _, _ := strings.Cut(c.MimeType, "/")
	return MediaKind(strings.ToLower(kind))
}

// Name returns the encoding name, the part of MimeType after the slash.
func (c Codec) Name() string {
	_, name, _ := strings.Cut(c.MimeType, "/")
	return name
}

var videoFeedback = []string{"goog-remb", "ccm fir", "nack", "nack pli"}

// DefaultCodecs returns the codecs a PeerConnection offers and accepts
// unless its Configuration names others: Opus, VP8, VP9 profile 0, H.264
// with packetization-mode=1 in the constrained baseline, baseline, main and
// high profiles browsers offer, and AV1, at the payload types Chrome gives
// them. FEC formats are left out, and so are RTX formats, which a
// PeerConnection adds for the codecs with NACK feedback when its
// Configuration.Retransmit keeps a history to repair from.
func DefaultCodecs() []Codec {
	h264 := func(pt uint8, profile string) Codec {
		return Codec{MimeType: "video/H264", ClockRate: 90000, PayloadType: pt, Feedback: videoFeedback,
			Fmtp: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=" + profile}
	}
	return []Codec{
		{MimeType: "audio/opus", ClockRate: 48000, Channels: 2, PayloadType: 111, Fmtp: "minptime=10;useinbandfec=1"},
		{MimeType: "video/VP8", ClockRate: 90000, PayloadType: 96, Feedback: videoFeedback},
		{MimeType: "video/VP9", ClockRate: 90000, PayloadType: 98, Fmtp: "profile-id=0", Feedback: videoFeedback},
		h264(102, "42001f"),
		h264(108, "42e01f"),
		h264(112, "64001f"),
		h264(127, "4d001f"),
		{MimeType: "video/AV1", ClockRate: 90000, PayloadType: 45, Feedback: videoFeedback},
	}
}

// HeaderExtension is an RTP header extension a PeerConnection may
// negotiate, by its URI, for the media kinds it applies to.
type HeaderExtension struct {
	URI  string
	Kind MediaKind // "" for both
}

// DefaultHeaderExtensions returns the header extensions a PeerConnection
// negotiates unless its Configuration names others: the MID, which routes
// packets to their transceiver before their SSRC is known, the RID of
// simulcast layers, the sender's absolute send time, and audio levels.
// Transport-wide congestion control is left out, as the feedback it asks
// for is not sent.
func DefaultHeaderExtensions() []HeaderExtension {
	return []HeaderExtension{
		{URI: rtp.ExtensionURIMid},
		{URI: rtp.ExtensionURIAbsSendTime},
		{URI: rtp.ExtensionURIRid, Kind: MediaKindVideo},
		{URI: rtp.ExtensionURIAudioLevel, Kind: MediaKindAudio},
	}
}

// codecFromSDP turns an SDP codec of a section of kind into a Codec.
func codecFromSDP(kind string, c sdp.Codec) Codec {
	return Codec{MimeType: kind + "/" + c.Name, ClockRate: c.ClockRate, Channels: c.Channels,
		Fmtp: c.Fmtp, Feedback: c.Feedback, PayloadType: c.PayloadType}
}

// toSDP turns a codec into an SDP one with payload type pt.
func (c Codec) toSDP(pt uint8) sdp.Codec {
	return sdp.Codec{PayloadType: pt, Name: c.Name(), ClockRate: c.ClockRate, Channels: c.Channels,
		Fmtp: c.Fmtp, Feedback: c.Feedback}
}

// matches reports whether the remote codec r is the local codec l: the
// same name, clock rate and channels, and the parameters that make two
// streams of the codec incompatible: H.264's packetization mode and profile
// (its level may differ), VP9's and AV1's profile.
func (l Codec) matches(r Codec) bool {
	if !strings.EqualFold(l.MimeType, r.MimeType) || l.ClockRate != r.ClockRate {
		return false
	}
	if l.Channels != r.Channels && !(l.Channels <= 1 && r.Channels <= 1) {
		return false
	}
	param := func(c Codec, key, def string) string {
		if v, ok := sdp.FmtpParam(c.Fmtp, key); ok {
			return strings.ToLower(v)
		}
		return def
	}
	switch strings.ToLower(l.Name()) {
	case "h264":
		if param(l, "packetization-mode", "0") != param(r, "packetization-mode", "0") {
			return false
		}
		lp, rp := param(l, "profile-level-id", "42001f"), param(r, "profile-level-id", "42001f")
		return len(lp) == 6 && len(rp) == 6 && h264Profile(lp) == h264Profile(rp)
	case "vp9":
		return param(l, "profile-id", "0") == param(r, "profile-id", "0")
	case "av1":
		return param(l, "profile", "0") == param(r, "profile", "0")
	}
	return true
}

// h264Profile names the profile of a profile-level-id: its profile_idc,
// and for baseline, whether the constraint flags make it constrained
// baseline, which is what browsers distinguish (RFC 6184 section 8.1).
func h264Profile(id string) string {
	idc, iop := id[:2], id[2:4]
	if idc == "42" || idc == "4d" || idc == "58" {
		// Constrained baseline is baseline, main or extended with
		// constraint_set1 (0x40) set.
		var b byte
		for _, c := range iop {
			b <<= 4
			switch {
			case c >= '0' && c <= '9':
				b |= byte(c - '0')
			case c >= 'a' && c <= 'f':
				b |= byte(c-'a') + 10
			}
		}
		if b&0x40 != 0 {
			return "cb"
		}
	}
	return idc
}

// negotiatedCodec is a codec both sides accepted, with the payload type
// each side receives it on: the remote one is what this side sends with.
// rtxSend and rtxRecv are those of its RTX format, 0 when there is none.
type negotiatedCodec struct {
	Codec
	sendPT, recvPT   uint8
	rtxSend, rtxRecv uint8
}

// intersectCodecs returns the remote codecs, in the remote's order of
// preference, that match one of local, with the remote's parameters.
func intersectCodecs(kind string, remote []sdp.Codec, local []Codec) []negotiatedCodec {
	var out []negotiatedCodec
	for _, rc := range remote {
		if rc.Name == "" || isRTX(rc.Name) {
			continue
		}
		r := codecFromSDP(kind, rc)
		for _, l := range local {
			if l.Kind() == MediaKind(kind) && l.matches(r) {
				out = append(out, negotiatedCodec{Codec: r, sendPT: r.PayloadType, recvPT: r.PayloadType})
				break
			}
		}
	}
	return out
}
