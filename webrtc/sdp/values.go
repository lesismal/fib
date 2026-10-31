package sdp

import (
	"fmt"
	"strconv"
	"strings"
)

// Origin is the value of the o= line. A peer keeps SessionID for the life
// of a session and raises SessionVersion each time it changes what it
// offers (RFC 9429 section 5.2.1).
type Origin struct {
	Username       string
	SessionID      uint64
	SessionVersion uint64
	NetType        string
	AddrType       string
	Address        string
}

// ParseOrigin parses the value of an o= line.
func ParseOrigin(v string) (Origin, error) {
	f := strings.Fields(v)
	if len(f) != 6 {
		return Origin{}, fmt.Errorf("%w: %d fields", ErrInvalidOrigin, len(f))
	}
	id, err1 := strconv.ParseUint(f[1], 10, 64)
	version, err2 := strconv.ParseUint(f[2], 10, 64)
	if err1 != nil || err2 != nil {
		return Origin{}, fmt.Errorf("%w: session id or version is not a number", ErrInvalidOrigin)
	}
	return Origin{f[0], id, version, f[3], f[4], f[5]}, nil
}

func (o Origin) String() string {
	return o.Username + " " + strconv.FormatUint(o.SessionID, 10) + " " +
		strconv.FormatUint(o.SessionVersion, 10) + " " + o.NetType + " " + o.AddrType + " " + o.Address
}

// Connection is the value of a c= line. WebRTC writes "IN IP4 0.0.0.0",
// since ICE, not this line, says where media goes.
type Connection struct {
	NetType  string
	AddrType string
	Address  string
}

// ParseConnection parses the value of a c= line.
func ParseConnection(v string) (Connection, error) {
	f := strings.Fields(v)
	if len(f) != 3 {
		return Connection{}, fmt.Errorf("%w: c=%s", ErrInvalidAttribute, truncate(v))
	}
	return Connection{f[0], f[1], f[2]}, nil
}

func (c Connection) String() string { return c.NetType + " " + c.AddrType + " " + c.Address }

// MediaName is the value of an m= line. NumPorts is 0 when the line gives
// none.
type MediaName struct {
	Media    string
	Port     int
	NumPorts int
	Proto    string
	Formats  []string
}

// ParseMediaName parses the value of an m= line, which must list at least
// one format.
func ParseMediaName(v string) (MediaName, error) {
	return parseMediaName(v, 1)
}

// parseMediaName lets the builders read an m= line that lists fewer than
// minFormats formats, as one does until its first codec is added.
func parseMediaName(v string, minFormats int) (MediaName, error) {
	f := strings.Fields(v)
	if len(f) < 3+minFormats {
		return MediaName{}, fmt.Errorf("%w: %d fields", ErrInvalidMedia, len(f))
	}
	m := MediaName{Media: f[0], Proto: f[2], Formats: f[3:]}
	port, count, hasCount := strings.Cut(f[1], "/")
	p, err := parseInt(port, 0, 65535)
	if err != nil {
		return MediaName{}, fmt.Errorf("%w: port %q", ErrInvalidMedia, truncate(f[1]))
	}
	m.Port = int(p)
	if hasCount {
		c, err := parseInt(count, 1, 65535)
		if err != nil {
			return MediaName{}, fmt.Errorf("%w: port %q", ErrInvalidMedia, truncate(f[1]))
		}
		m.NumPorts = int(c)
	}
	return m, nil
}

func (m MediaName) String() string {
	var b strings.Builder
	b.WriteString(m.Media)
	b.WriteByte(' ')
	b.WriteString(strconv.Itoa(m.Port))
	if m.NumPorts > 0 {
		b.WriteByte('/')
		b.WriteString(strconv.Itoa(m.NumPorts))
	}
	b.WriteByte(' ')
	b.WriteString(m.Proto)
	for _, f := range m.Formats {
		b.WriteByte(' ')
		b.WriteString(f)
	}
	return b.String()
}

// parseInt parses a decimal number from lo to hi, without the sign
// strconv would accept. It returns an int64 so that hi may be 2^32-1 where
// int has 32 bits.
func parseInt(s string, lo, hi int64) (int64, error) {
	if s == "" || s[0] == '+' || s[0] == '-' {
		return 0, strconv.ErrSyntax
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if n < lo || n > hi {
		return 0, strconv.ErrRange
	}
	return n, nil
}

// Group is the value of an a=group line (RFC 5888), such as BUNDLE and the
// mids it bundles.
type Group struct {
	Semantics string
	IDs       []string
}

// ParseGroup parses the value of an a=group attribute.
func ParseGroup(v string) (Group, error) {
	f := strings.Fields(v)
	if len(f) == 0 {
		return Group{}, fmt.Errorf("%w: empty group", ErrInvalidAttribute)
	}
	return Group{Semantics: f[0], IDs: f[1:]}, nil
}

func (g Group) String() string {
	if len(g.IDs) == 0 {
		return g.Semantics
	}
	return g.Semantics + " " + strings.Join(g.IDs, " ")
}

// Fingerprint is the value of an a=fingerprint attribute (RFC 8122): the
// hash function's name, such as "sha-256", and the certificate's hash as
// uppercase hex bytes separated by colons.
type Fingerprint struct {
	Algorithm string
	Value     string
}

// ParseFingerprint parses the value of an a=fingerprint attribute.
func ParseFingerprint(v string) (Fingerprint, error) {
	alg, value, ok := strings.Cut(v, " ")
	value = strings.TrimSpace(value)
	if !ok || alg == "" || value == "" || strings.ContainsRune(value, ' ') {
		return Fingerprint{}, fmt.Errorf("%w: fingerprint %q", ErrInvalidAttribute, truncate(v))
	}
	return Fingerprint{alg, value}, nil
}

func (f Fingerprint) String() string { return f.Algorithm + " " + f.Value }

// Setup is the value of an a=setup attribute (RFC 4145): which side opens
// the DTLS connection. An offer says actpass; the answer picks active or
// passive (RFC 8842).
type Setup string

const (
	SetupActive   Setup = "active"
	SetupPassive  Setup = "passive"
	SetupActpass  Setup = "actpass"
	SetupHoldconn Setup = "holdconn"
)

// Direction is one of the four attributes that say which way media flows.
type Direction string

const (
	SendRecv Direction = "sendrecv"
	SendOnly Direction = "sendonly"
	RecvOnly Direction = "recvonly"
	Inactive Direction = "inactive"
)

// Reverse returns the direction the other side of a stream sees: an
// answer to sendonly is recvonly.
func (d Direction) Reverse() Direction {
	switch d {
	case SendOnly:
		return RecvOnly
	case RecvOnly:
		return SendOnly
	}
	return d
}

func (d Direction) valid() bool {
	return d == SendRecv || d == SendOnly || d == RecvOnly || d == Inactive
}

// RTPMap is the value of an a=rtpmap attribute. Channels is 0 when the
// line gives none, which for audio means one.
type RTPMap struct {
	PayloadType  uint8
	EncodingName string
	ClockRate    uint32
	Channels     uint16
}

// ParseRTPMap parses the value of an a=rtpmap attribute, such as
// "111 opus/48000/2".
func ParseRTPMap(v string) (RTPMap, error) {
	pt, rest, ok := strings.Cut(v, " ")
	if !ok {
		return RTPMap{}, fmt.Errorf("%w: rtpmap %q", ErrInvalidAttribute, truncate(v))
	}
	var m RTPMap
	p, err := parseInt(pt, 0, 127)
	if err != nil {
		return RTPMap{}, fmt.Errorf("%w: rtpmap payload type %q", ErrInvalidAttribute, truncate(pt))
	}
	m.PayloadType = uint8(p)
	parts := strings.Split(strings.TrimSpace(rest), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" {
		return RTPMap{}, fmt.Errorf("%w: rtpmap %q", ErrInvalidAttribute, truncate(v))
	}
	m.EncodingName = parts[0]
	rate, err := parseInt(parts[1], 0, 1<<32-1)
	if err != nil {
		return RTPMap{}, fmt.Errorf("%w: rtpmap clock rate %q", ErrInvalidAttribute, truncate(parts[1]))
	}
	m.ClockRate = uint32(rate)
	if len(parts) == 3 {
		ch, err := parseInt(parts[2], 0, 65535)
		if err != nil {
			return RTPMap{}, fmt.Errorf("%w: rtpmap channels %q", ErrInvalidAttribute, truncate(parts[2]))
		}
		m.Channels = uint16(ch)
	}
	return m, nil
}

func (m RTPMap) String() string {
	s := strconv.Itoa(int(m.PayloadType)) + " " + m.EncodingName + "/" + strconv.FormatUint(uint64(m.ClockRate), 10)
	if m.Channels > 0 {
		s += "/" + strconv.Itoa(int(m.Channels))
	}
	return s
}

// Fmtp is the value of an a=fmtp attribute: a payload type and its format
// parameters, kept as written ("minptime=10;useinbandfec=1").
type Fmtp struct {
	PayloadType uint8
	Params      string
}

// ParseFmtp parses the value of an a=fmtp attribute.
func ParseFmtp(v string) (Fmtp, error) {
	pt, params, ok := strings.Cut(v, " ")
	p, err := parseInt(pt, 0, 127)
	if !ok || err != nil {
		return Fmtp{}, fmt.Errorf("%w: fmtp %q", ErrInvalidAttribute, truncate(v))
	}
	return Fmtp{uint8(p), params}, nil
}

func (f Fmtp) String() string { return strconv.Itoa(int(f.PayloadType)) + " " + f.Params }

// FmtpParam finds key among format parameters written as fmtp writes them,
// "key=value" pairs separated by ';', and returns its value. A parameter
// written without '=' is found with an empty value.
func FmtpParam(params, key string) (string, bool) {
	for params != "" {
		var p string
		p, params, _ = strings.Cut(params, ";")
		k, v, _ := strings.Cut(strings.TrimSpace(p), "=")
		if strings.EqualFold(k, key) {
			return v, true
		}
	}
	return "", false
}

// RTCPFeedback is the value of an a=rtcp-fb attribute (RFC 4585), such as
// "96 nack pli". PayloadType is "*" for one that applies to every format of
// the section.
type RTCPFeedback struct {
	PayloadType string
	Type        string
	Param       string
}

// ParseRTCPFeedback parses the value of an a=rtcp-fb attribute.
func ParseRTCPFeedback(v string) (RTCPFeedback, error) {
	pt, rest, ok := strings.Cut(v, " ")
	if pt != "*" {
		if _, err := parseInt(pt, 0, 127); err != nil {
			ok = false
		}
	}
	typ, param, _ := strings.Cut(strings.TrimSpace(rest), " ")
	if !ok || typ == "" {
		return RTCPFeedback{}, fmt.Errorf("%w: rtcp-fb %q", ErrInvalidAttribute, truncate(v))
	}
	return RTCPFeedback{pt, typ, param}, nil
}

func (f RTCPFeedback) String() string {
	if f.Param == "" {
		return f.PayloadType + " " + f.Type
	}
	return f.PayloadType + " " + f.Type + " " + f.Param
}

// ExtMap is the value of an a=extmap attribute (RFC 8285): an RTP header
// extension's ID, an optional direction, its URI and any attributes.
type ExtMap struct {
	ID         int
	Direction  Direction
	URI        string
	Attributes string
}

// ParseExtMap parses the value of an a=extmap attribute.
func ParseExtMap(v string) (ExtMap, error) {
	id, rest, ok := strings.Cut(v, " ")
	id, dir, hasDir := strings.Cut(id, "/")
	var e ExtMap
	n, err := parseInt(id, 1, 4095)
	if err != nil {
		ok = false
	}
	e.ID = int(n)
	if hasDir {
		e.Direction = Direction(dir)
		if !e.Direction.valid() {
			ok = false
		}
	}
	e.URI, e.Attributes, _ = strings.Cut(strings.TrimSpace(rest), " ")
	if !ok || e.URI == "" {
		return ExtMap{}, fmt.Errorf("%w: extmap %q", ErrInvalidAttribute, truncate(v))
	}
	return e, nil
}

func (e ExtMap) String() string {
	s := strconv.Itoa(e.ID)
	if e.Direction != "" {
		s += "/" + string(e.Direction)
	}
	s += " " + e.URI
	if e.Attributes != "" {
		s += " " + e.Attributes
	}
	return s
}

// SSRC is the value of an a=ssrc attribute (RFC 5576): a source and one of
// its attributes, such as "cname" and its value.
type SSRC struct {
	SSRC      uint32
	Attribute string
	Value     string
}

// ParseSSRC parses the value of an a=ssrc attribute.
func ParseSSRC(v string) (SSRC, error) {
	id, rest, ok := strings.Cut(v, " ")
	n, err := parseInt(id, 0, 1<<32-1)
	if !ok || err != nil {
		return SSRC{}, fmt.Errorf("%w: ssrc %q", ErrInvalidAttribute, truncate(v))
	}
	attr, value, _ := strings.Cut(rest, ":")
	if attr == "" {
		return SSRC{}, fmt.Errorf("%w: ssrc %q", ErrInvalidAttribute, truncate(v))
	}
	return SSRC{uint32(n), attr, value}, nil
}

func (s SSRC) String() string {
	v := strconv.FormatUint(uint64(s.SSRC), 10) + " " + s.Attribute
	if s.Value != "" {
		v += ":" + s.Value
	}
	return v
}

// SSRCGroup is the value of an a=ssrc-group attribute, such as "FID" and
// the SSRCs of a stream and its retransmissions.
type SSRCGroup struct {
	Semantics string
	SSRCs     []uint32
}

// ParseSSRCGroup parses the value of an a=ssrc-group attribute.
func ParseSSRCGroup(v string) (SSRCGroup, error) {
	f := strings.Fields(v)
	if len(f) == 0 {
		return SSRCGroup{}, fmt.Errorf("%w: empty ssrc-group", ErrInvalidAttribute)
	}
	g := SSRCGroup{Semantics: f[0], SSRCs: make([]uint32, 0, len(f)-1)}
	for _, s := range f[1:] {
		n, err := parseInt(s, 0, 1<<32-1)
		if err != nil {
			return SSRCGroup{}, fmt.Errorf("%w: ssrc-group %q", ErrInvalidAttribute, truncate(v))
		}
		g.SSRCs = append(g.SSRCs, uint32(n))
	}
	return g, nil
}

func (g SSRCGroup) String() string {
	s := g.Semantics
	for _, n := range g.SSRCs {
		s += " " + strconv.FormatUint(uint64(n), 10)
	}
	return s
}

// MSID is the value of an a=msid attribute (RFC 8830): the media stream a
// track belongs to, "-" for none, and optionally the track's ID.
type MSID struct {
	Stream string
	Track  string
}

// ParseMSID parses the value of an a=msid attribute.
func ParseMSID(v string) (MSID, error) {
	f := strings.Fields(v)
	switch len(f) {
	case 1:
		return MSID{Stream: f[0]}, nil
	case 2:
		return MSID{f[0], f[1]}, nil
	}
	return MSID{}, fmt.Errorf("%w: msid %q", ErrInvalidAttribute, truncate(v))
}

func (m MSID) String() string {
	if m.Track == "" {
		return m.Stream
	}
	return m.Stream + " " + m.Track
}

// RID is the value of an a=rid attribute (RFC 8851): a restriction
// identifier, whether it applies to what this side sends or receives, and
// its restrictions as written ("pt=96;max-width=1280").
type RID struct {
	ID           string
	Direction    string
	Restrictions string
}

// The directions of a rid.
const (
	RIDSend = "send"
	RIDRecv = "recv"
)

// ParseRID parses the value of an a=rid attribute.
func ParseRID(v string) (RID, error) {
	f := strings.SplitN(v, " ", 3)
	if len(f) < 2 || !validRIDID(f[0]) || (f[1] != RIDSend && f[1] != RIDRecv) {
		return RID{}, fmt.Errorf("%w: rid %q", ErrInvalidAttribute, truncate(v))
	}
	r := RID{ID: f[0], Direction: f[1]}
	if len(f) == 3 {
		r.Restrictions = f[2]
	}
	return r, nil
}

// validRIDID reports whether id is a rid-id of RFC 8851 section 10:
// letters, digits, '-' and '_'.
func validRIDID(id string) bool {
	if id == "" {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func (r RID) String() string {
	if r.Restrictions == "" {
		return r.ID + " " + r.Direction
	}
	return r.ID + " " + r.Direction + " " + r.Restrictions
}

// Simulcast is the value of an a=simulcast attribute (RFC 8853). Each of
// Send and Recv lists streams in order of preference; each stream lists
// rids that are alternatives to one another.
type Simulcast struct {
	Send [][]SimulcastRID
	Recv [][]SimulcastRID
}

// SimulcastRID is a rid in a simulcast stream; Paused when it was written
// with a leading '~'.
type SimulcastRID struct {
	ID     string
	Paused bool
}

// ParseSimulcast parses the value of an a=simulcast attribute. It accepts
// the "rid=" prefix of the drafts before RFC 8853, which old Firefox
// versions write.
func ParseSimulcast(v string) (Simulcast, error) {
	var s Simulcast
	f := strings.Fields(v)
	if len(f) == 0 || len(f)%2 != 0 || len(f) > 4 {
		return Simulcast{}, fmt.Errorf("%w: simulcast %q", ErrInvalidAttribute, truncate(v))
	}
	for i := 0; i < len(f); i += 2 {
		var dst *[][]SimulcastRID
		switch f[i] {
		case "send":
			dst = &s.Send
		case "recv":
			dst = &s.Recv
		default:
			return Simulcast{}, fmt.Errorf("%w: simulcast %q", ErrInvalidAttribute, truncate(v))
		}
		if *dst != nil {
			return Simulcast{}, fmt.Errorf("%w: simulcast %q", ErrInvalidAttribute, truncate(v))
		}
		for _, stream := range strings.Split(strings.TrimPrefix(f[i+1], "rid="), ";") {
			var alts []SimulcastRID
			for _, id := range strings.Split(stream, ",") {
				r := SimulcastRID{ID: id}
				if strings.HasPrefix(id, "~") {
					r = SimulcastRID{ID: id[1:], Paused: true}
				}
				if !validRIDID(r.ID) {
					return Simulcast{}, fmt.Errorf("%w: simulcast %q", ErrInvalidAttribute, truncate(v))
				}
				alts = append(alts, r)
			}
			*dst = append(*dst, alts)
		}
	}
	return s, nil
}

func (s Simulcast) String() string {
	var b strings.Builder
	write := func(dir string, streams [][]SimulcastRID) {
		if len(streams) == 0 {
			return
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(dir)
		b.WriteByte(' ')
		for i, alts := range streams {
			if i > 0 {
				b.WriteByte(';')
			}
			for j, r := range alts {
				if j > 0 {
					b.WriteByte(',')
				}
				if r.Paused {
					b.WriteByte('~')
				}
				b.WriteString(r.ID)
			}
		}
	}
	write("send", s.Send)
	write("recv", s.Recv)
	return b.String()
}

// Codec gathers what a media section says of one RTP payload type: its
// rtpmap, its fmtp parameters and its rtcp-fb lines, as "nack pli" and the
// like, including those written for every format with "*".
type Codec struct {
	PayloadType uint8
	Name        string
	ClockRate   uint32
	Channels    uint16
	Fmtp        string
	Feedback    []string
}

// Param returns one of the codec's format parameters.
func (c Codec) Param(key string) (string, bool) { return FmtpParam(c.Fmtp, key) }
