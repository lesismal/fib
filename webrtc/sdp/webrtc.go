package sdp

import (
	"crypto/rand"
	"encoding/binary"
	"strconv"
	"strings"
)

// The protocols and format of WebRTC's media sections.
const (
	ProtoRTP          = "UDP/TLS/RTP/SAVPF"
	ProtoSCTP         = "UDP/DTLS/SCTP"
	FormatDataChannel = "webrtc-datachannel"
)

// DefaultSCTPPort is the SCTP port browsers use on both sides (RFC 8841).
const DefaultSCTPPort = 5000

// New returns a description with the session-level lines JSEP writes
// (RFC 9429 section 5.2.1): "v=0", an o= line with the given session ID and
// version, "s=-" and "t=0 0". Media sections and attributes are added to it.
func New(sessionID, sessionVersion uint64) *SessionDescription {
	sd := &SessionDescription{}
	sd.Lines = []Line{
		{'v', "0"},
		{'o', Origin{"-", sessionID, sessionVersion, "IN", "IP4", "127.0.0.1"}.String()},
		{'s', "-"},
		{'t', "0 0"},
	}
	return sd
}

// NewSessionID returns a random session ID below 2^63, as RFC 9429
// section 5.2.1 asks of an o= line.
func NewSessionID() uint64 {
	var b [8]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint64(b[:]) >> 1
}

// NewMedia returns a media section with an m= line for kind ("audio",
// "video" or "application"), port, proto and formats, and the
// "c=IN IP4 0.0.0.0" line JSEP writes. A section needs at least one format,
// which AddCodec adds, before it is sent. A WebRTC section's port is 9 (the
// discard port) until ICE picks an address, and 0 when it is rejected.
func NewMedia(kind string, port int, proto string, formats ...string) *MediaDescription {
	md := &MediaDescription{}
	md.Lines = []Line{
		{'m', MediaName{Media: kind, Port: port, Proto: proto, Formats: formats}.String()},
		{'c', "IN IP4 0.0.0.0"},
	}
	return md
}

// NewDataChannelMedia returns the media section that carries data
// channels: "m=application 9 UDP/DTLS/SCTP webrtc-datachannel", its c=
// line and "a=sctp-port:5000".
func NewDataChannelMedia() *MediaDescription {
	md := NewMedia("application", 9, ProtoSCTP, FormatDataChannel)
	md.SetSCTPPort(DefaultSCTPPort)
	return md
}

// Origin returns the parsed o= line.
func (sd *SessionDescription) Origin() (Origin, bool) {
	v, ok := sd.Line('o')
	if !ok {
		return Origin{}, false
	}
	o, err := ParseOrigin(v)
	return o, err == nil
}

// SetOrigin replaces the o= line.
func (sd *SessionDescription) SetOrigin(o Origin) { sd.SetLine('o', o.String()) }

// AddMedia appends a media section and returns it.
func (sd *SessionDescription) AddMedia(md *MediaDescription) *MediaDescription {
	sd.Media = append(sd.Media, md)
	return md
}

// MediaByMid returns the media section with the given mid.
func (sd *SessionDescription) MediaByMid(mid string) *MediaDescription {
	for _, md := range sd.Media {
		if m, ok := md.Mid(); ok && m == mid {
			return md
		}
	}
	return nil
}

// Groups returns the a=group attributes, skipping malformed ones.
func (sd *SessionDescription) Groups() []Group {
	var out []Group
	for _, v := range sd.Attributes("group") {
		if g, err := ParseGroup(v); err == nil {
			out = append(out, g)
		}
	}
	return out
}

// Bundle returns the mids of the first BUNDLE group, the first of which
// tags the section whose transport the others share (RFC 9143), and whether
// there is a BUNDLE group.
func (sd *SessionDescription) Bundle() ([]string, bool) {
	for _, g := range sd.Groups() {
		if g.Semantics == "BUNDLE" {
			return g.IDs, true
		}
	}
	return nil, false
}

// SetGroup replaces the a=group attribute with the same semantics, or adds
// one.
func (sd *SessionDescription) SetGroup(g Group) {
	for i, l := range sd.Lines {
		if !isAttr(l, "group") {
			continue
		}
		_, v, _ := l.Attribute()
		if old, err := ParseGroup(v); err == nil && old.Semantics == g.Semantics {
			sd.Lines[i].Value = "group:" + g.String()
			return
		}
	}
	sd.AddAttribute("group", g.String())
}

// SetBundle sets the BUNDLE group to the given mids.
func (sd *SessionDescription) SetBundle(mids ...string) {
	sd.SetGroup(Group{Semantics: "BUNDLE", IDs: mids})
}

// ICELite reports whether the session has a=ice-lite: the peer only answers
// checks and never sends its own (RFC 8445 section 2.5).
func (sd *SessionDescription) ICELite() bool { return sd.HasAttribute("ice-lite") }

// SetICELite adds or removes a=ice-lite.
func (sd *SessionDescription) SetICELite(on bool) { sd.SetFlag("ice-lite", on) }

// Lookup returns the attribute of md if it has it and the session's
// otherwise, which is where an attribute such as fingerprint or ice-ufrag
// written at session level applies to every media section.
func (sd *SessionDescription) Lookup(md *MediaDescription, name string) (string, bool) {
	if v, ok := md.Attribute(name); ok {
		return v, true
	}
	return sd.Attribute(name)
}

// MediaDirection returns md's direction, the session's if md has none, and
// sendrecv, the default of RFC 8866 section 6.7, if neither has.
func (sd *SessionDescription) MediaDirection(md *MediaDescription) Direction {
	if d, ok := md.Direction(); ok {
		return d
	}
	if d, ok := sd.Direction(); ok {
		return d
	}
	return SendRecv
}

// Connection returns the parsed c= line.
func (s *Section) Connection() (Connection, bool) {
	v, ok := s.Line('c')
	if !ok {
		return Connection{}, false
	}
	c, err := ParseConnection(v)
	return c, err == nil
}

// SetConnection replaces the c= line, or adds one ahead of the attributes.
func (s *Section) SetConnection(c Connection) { s.SetLine('c', c.String()) }

// Fingerprints returns the a=fingerprint attributes, skipping malformed
// ones. There may be one for each certificate hash the peer offers.
func (s *Section) Fingerprints() []Fingerprint {
	var out []Fingerprint
	for _, v := range s.Attributes("fingerprint") {
		if f, err := ParseFingerprint(v); err == nil {
			out = append(out, f)
		}
	}
	return out
}

// AddFingerprint adds an a=fingerprint attribute.
func (s *Section) AddFingerprint(f Fingerprint) { s.AddAttribute("fingerprint", f.String()) }

// Setup returns the a=setup attribute.
func (s *Section) Setup() (Setup, bool) {
	v, ok := s.Attribute("setup")
	return Setup(v), ok
}

// SetSetup replaces or adds the a=setup attribute.
func (s *Section) SetSetup(v Setup) { s.SetAttribute("setup", string(v)) }

// ICEUfrag returns the a=ice-ufrag attribute.
func (s *Section) ICEUfrag() (string, bool) { return s.Attribute("ice-ufrag") }

// ICEPwd returns the a=ice-pwd attribute.
func (s *Section) ICEPwd() (string, bool) { return s.Attribute("ice-pwd") }

// SetICECredentials replaces or adds the a=ice-ufrag and a=ice-pwd
// attributes, which an ICE restart changes.
func (s *Section) SetICECredentials(ufrag, pwd string) {
	s.SetAttribute("ice-ufrag", ufrag)
	s.SetAttribute("ice-pwd", pwd)
}

// ICEOptions returns the options of the a=ice-options attributes, such as
// "trickle".
func (s *Section) ICEOptions() []string {
	var out []string
	for _, v := range s.Attributes("ice-options") {
		out = append(out, strings.Fields(v)...)
	}
	return out
}

// SetICEOptions replaces or adds the a=ice-options attribute, or removes
// it if there are no options.
func (s *Section) SetICEOptions(options ...string) {
	if len(options) == 0 {
		s.DeleteAttribute("ice-options")
		return
	}
	s.SetAttribute("ice-options", strings.Join(options, " "))
}

// Candidates returns each a=candidate attribute as its text after "a=",
// "candidate:1 1 udp ...", which is the form RTCIceCandidate.candidate
// takes and the ICE agent parses.
func (s *Section) Candidates() []string {
	var out []string
	for _, l := range s.Lines {
		if isAttr(l, "candidate") {
			out = append(out, l.Value)
		}
	}
	return out
}

// AddCandidate adds an a=candidate attribute. c may be written with or
// without the leading "a=" and "candidate:". It goes before an
// a=end-of-candidates attribute if the section has one.
func (s *Section) AddCandidate(c string) {
	c = strings.TrimPrefix(c, "a=")
	if !strings.HasPrefix(c, "candidate:") {
		c = "candidate:" + c
	}
	l := Line{Type: 'a', Value: c}
	for i := range s.Lines {
		if isAttr(s.Lines[i], "end-of-candidates") {
			s.Lines = append(s.Lines, Line{})
			copy(s.Lines[i+1:], s.Lines[i:])
			s.Lines[i] = l
			return
		}
	}
	s.Lines = append(s.Lines, l)
}

// EndOfCandidates reports whether the section has a=end-of-candidates.
func (s *Section) EndOfCandidates() bool { return s.HasAttribute("end-of-candidates") }

// SetEndOfCandidates adds or removes a=end-of-candidates.
func (s *Section) SetEndOfCandidates(on bool) { s.SetFlag("end-of-candidates", on) }

// Direction returns which of sendrecv, sendonly, recvonly or inactive the
// section has.
func (s *Section) Direction() (Direction, bool) {
	for _, l := range s.Lines {
		if l.Type == 'a' {
			if d := Direction(l.Value); d.valid() {
				return d, true
			}
		}
	}
	return "", false
}

// SetDirection replaces the section's direction attribute where it stands,
// or adds one.
func (s *Section) SetDirection(d Direction) {
	s.replace(Line{Type: 'a', Value: string(d)}, string(SendRecv), string(SendOnly), string(RecvOnly), string(Inactive))
}

// ExtMaps returns the a=extmap attributes, skipping malformed ones.
func (s *Section) ExtMaps() []ExtMap {
	var out []ExtMap
	for _, v := range s.Attributes("extmap") {
		if e, err := ParseExtMap(v); err == nil {
			out = append(out, e)
		}
	}
	return out
}

// AddExtMap adds an a=extmap attribute.
func (s *Section) AddExtMap(e ExtMap) { s.AddAttribute("extmap", e.String()) }

// MediaName returns the parsed m= line. It reads one that lists no format
// yet, as NewMedia writes before a codec is added, although Parse would
// reject it.
func (md *MediaDescription) MediaName() (MediaName, bool) {
	v, ok := md.Line('m')
	if !ok {
		return MediaName{}, false
	}
	m, err := parseMediaName(v, 0)
	return m, err == nil
}

// SetMediaName replaces the m= line.
func (md *MediaDescription) SetMediaName(m MediaName) { md.SetLine('m', m.String()) }

// Kind returns the media type of the m= line: "audio", "video",
// "application" or another.
func (md *MediaDescription) Kind() string {
	m, _ := md.MediaName()
	return m.Media
}

// Rejected reports whether the m= line's port is 0, which in an answer
// rejects the section and in an offer stops it (RFC 9429 section 5.2.2).
func (md *MediaDescription) Rejected() bool {
	m, ok := md.MediaName()
	return ok && m.Port == 0
}

// SetPort replaces the port of the m= line. Port 0 rejects the section.
func (md *MediaDescription) SetPort(port int) {
	if m, ok := md.MediaName(); ok {
		m.Port = port
		md.SetMediaName(m)
	}
}

// IsDataChannel reports whether the section carries SCTP, as the data
// channel section does.
func (md *MediaDescription) IsDataChannel() bool {
	m, ok := md.MediaName()
	return ok && m.Media == "application" && strings.HasSuffix(m.Proto, "SCTP")
}

// Mid returns the a=mid attribute.
func (md *MediaDescription) Mid() (string, bool) { return md.Attribute("mid") }

// SetMid replaces or adds the a=mid attribute.
func (md *MediaDescription) SetMid(mid string) { md.SetAttribute("mid", mid) }

// RTCPMux reports whether the section has a=rtcp-mux, so RTCP shares
// RTP's transport (RFC 5761), as WebRTC requires.
func (md *MediaDescription) RTCPMux() bool { return md.HasAttribute("rtcp-mux") }

// SetRTCPMux adds or removes a=rtcp-mux.
func (md *MediaDescription) SetRTCPMux(on bool) { md.SetFlag("rtcp-mux", on) }

// RTCPRsize reports whether the section has a=rtcp-rsize, allowing reduced
// size RTCP (RFC 5506).
func (md *MediaDescription) RTCPRsize() bool { return md.HasAttribute("rtcp-rsize") }

// SetRTCPRsize adds or removes a=rtcp-rsize.
func (md *MediaDescription) SetRTCPRsize(on bool) { md.SetFlag("rtcp-rsize", on) }

// RTPMaps returns the a=rtpmap attributes, skipping malformed ones.
func (md *MediaDescription) RTPMaps() []RTPMap {
	var out []RTPMap
	for _, v := range md.Attributes("rtpmap") {
		if m, err := ParseRTPMap(v); err == nil {
			out = append(out, m)
		}
	}
	return out
}

// Fmtps returns the a=fmtp attributes, skipping malformed ones.
func (md *MediaDescription) Fmtps() []Fmtp {
	var out []Fmtp
	for _, v := range md.Attributes("fmtp") {
		if f, err := ParseFmtp(v); err == nil {
			out = append(out, f)
		}
	}
	return out
}

// RTCPFeedbacks returns the a=rtcp-fb attributes, skipping malformed ones.
func (md *MediaDescription) RTCPFeedbacks() []RTCPFeedback {
	var out []RTCPFeedback
	for _, v := range md.Attributes("rtcp-fb") {
		if f, err := ParseRTCPFeedback(v); err == nil {
			out = append(out, f)
		}
	}
	return out
}

// Codecs returns a Codec for each numeric format of the m= line, in its
// order, which is the order of preference. A payload type without an
// rtpmap, as a static one may be, has an empty Name.
func (md *MediaDescription) Codecs() []Codec {
	m, ok := md.MediaName()
	if !ok {
		return nil
	}
	var out []Codec
	for _, f := range m.Formats {
		pt, err := parseInt(f, 0, 127)
		if err == nil {
			out = append(out, Codec{PayloadType: uint8(pt)})
		}
	}
	find := func(pt uint8) *Codec {
		for i := range out {
			if out[i].PayloadType == pt {
				return &out[i]
			}
		}
		return nil
	}
	for _, l := range md.Lines {
		if l.Type != 'a' {
			continue
		}
		name, v, _ := l.Attribute()
		switch name {
		case "rtpmap":
			if r, err := ParseRTPMap(v); err == nil {
				if c := find(r.PayloadType); c != nil {
					c.Name, c.ClockRate, c.Channels = r.EncodingName, r.ClockRate, r.Channels
				}
			}
		case "fmtp":
			if f, err := ParseFmtp(v); err == nil {
				if c := find(f.PayloadType); c != nil {
					c.Fmtp = f.Params
				}
			}
		case "rtcp-fb":
			fb, err := ParseRTCPFeedback(v)
			if err != nil {
				continue
			}
			_, value, _ := strings.Cut(v, " ")
			value = strings.TrimSpace(value)
			if fb.PayloadType == "*" {
				for i := range out {
					out[i].Feedback = append(out[i].Feedback, value)
				}
			} else if pt, err := parseInt(fb.PayloadType, 0, 127); err == nil {
				if c := find(uint8(pt)); c != nil {
					c.Feedback = append(c.Feedback, value)
				}
			}
		}
	}
	return out
}

// Codec returns the codec of payload type pt.
func (md *MediaDescription) Codec(pt uint8) (Codec, bool) {
	for _, c := range md.Codecs() {
		if c.PayloadType == pt {
			return c, true
		}
	}
	return Codec{}, false
}

// AddCodec adds c's payload type to the m= line's formats and appends its
// a=rtpmap, a=rtcp-fb and a=fmtp lines, in the order Chrome writes them.
// Channels is written only when it is not 0.
func (md *MediaDescription) AddCodec(c Codec) {
	if m, ok := md.MediaName(); ok {
		m.Formats = append(m.Formats, strconv.Itoa(int(c.PayloadType)))
		md.SetMediaName(m)
	}
	md.AddAttribute("rtpmap", RTPMap{c.PayloadType, c.Name, c.ClockRate, c.Channels}.String())
	pt := strconv.Itoa(int(c.PayloadType))
	for _, fb := range c.Feedback {
		md.AddAttribute("rtcp-fb", pt+" "+fb)
	}
	if c.Fmtp != "" {
		md.AddAttribute("fmtp", pt+" "+c.Fmtp)
	}
}

// SSRCs returns the a=ssrc attributes, skipping malformed ones.
func (md *MediaDescription) SSRCs() []SSRC {
	var out []SSRC
	for _, v := range md.Attributes("ssrc") {
		if s, err := ParseSSRC(v); err == nil {
			out = append(out, s)
		}
	}
	return out
}

// AddSSRC adds an a=ssrc attribute.
func (md *MediaDescription) AddSSRC(s SSRC) { md.AddAttribute("ssrc", s.String()) }

// SSRCGroups returns the a=ssrc-group attributes, skipping malformed ones.
func (md *MediaDescription) SSRCGroups() []SSRCGroup {
	var out []SSRCGroup
	for _, v := range md.Attributes("ssrc-group") {
		if g, err := ParseSSRCGroup(v); err == nil {
			out = append(out, g)
		}
	}
	return out
}

// AddSSRCGroup adds an a=ssrc-group attribute.
func (md *MediaDescription) AddSSRCGroup(g SSRCGroup) { md.AddAttribute("ssrc-group", g.String()) }

// MSIDs returns the a=msid attributes, skipping malformed ones. A track
// in several streams has one for each.
func (md *MediaDescription) MSIDs() []MSID {
	var out []MSID
	for _, v := range md.Attributes("msid") {
		if m, err := ParseMSID(v); err == nil {
			out = append(out, m)
		}
	}
	return out
}

// AddMSID adds an a=msid attribute.
func (md *MediaDescription) AddMSID(m MSID) { md.AddAttribute("msid", m.String()) }

// RIDs returns the a=rid attributes, skipping malformed ones.
func (md *MediaDescription) RIDs() []RID {
	var out []RID
	for _, v := range md.Attributes("rid") {
		if r, err := ParseRID(v); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// AddRID adds an a=rid attribute.
func (md *MediaDescription) AddRID(r RID) { md.AddAttribute("rid", r.String()) }

// Simulcast returns the parsed a=simulcast attribute.
func (md *MediaDescription) Simulcast() (Simulcast, bool) {
	v, ok := md.Attribute("simulcast")
	if !ok {
		return Simulcast{}, false
	}
	s, err := ParseSimulcast(v)
	return s, err == nil
}

// SetSimulcast replaces or adds the a=simulcast attribute.
func (md *MediaDescription) SetSimulcast(s Simulcast) { md.SetAttribute("simulcast", s.String()) }

// SCTPPort returns the a=sctp-port attribute, or for a section in the
// older form "DTLS/SCTP 5000" its format.
func (md *MediaDescription) SCTPPort() (int, bool) {
	if v, ok := md.Attribute("sctp-port"); ok {
		n, err := parseInt(v, 0, 65535)
		return int(n), err == nil
	}
	if m, ok := md.MediaName(); ok && m.Proto == "DTLS/SCTP" && len(m.Formats) > 0 {
		n, err := parseInt(m.Formats[0], 0, 65535)
		return int(n), err == nil
	}
	return 0, false
}

// SetSCTPPort replaces or adds the a=sctp-port attribute.
func (md *MediaDescription) SetSCTPPort(port int) { md.SetAttribute("sctp-port", strconv.Itoa(port)) }

// MaxMessageSize returns the a=max-message-size attribute: the largest
// message the peer accepts, 0 for no limit. RFC 8841 section 6 has a
// peer assume 64 KiB when there is none.
func (md *MediaDescription) MaxMessageSize() (uint64, bool) {
	v, ok := md.Attribute("max-message-size")
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	return n, err == nil
}

// SetMaxMessageSize replaces or adds the a=max-message-size attribute.
func (md *MediaDescription) SetMaxMessageSize(n uint64) {
	md.SetAttribute("max-message-size", strconv.FormatUint(n, 10))
}
