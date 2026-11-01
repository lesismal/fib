package rtsp

import (
	"strconv"
	"strings"
)

// Transport is one transport specification of a Transport header (RFC 2326
// section 12.39): how the RTP and RTCP of one stream travel. The package
// speaks RTP/AVP over UDP unicast and RTP/AVP/TCP interleaved on the RTSP
// connection.
type Transport struct {
	// Protocol is the transport protocol as written, "RTP/AVP",
	// "RTP/AVP/UDP" or "RTP/AVP/TCP".
	Protocol string
	// TCP says RTP is interleaved on the RTSP connection.
	TCP bool
	// Multicast says the client asked for multicast, which the package
	// does not do; unicast is the default.
	Multicast bool
	// Interleaved are the RTP and RTCP channels of TCP, ClientPort and
	// ServerPort the RTP and RTCP ports of UDP; -1 where not given.
	Interleaved [2]int
	ClientPort  [2]int
	ServerPort  [2]int
	// SSRC is the ssrc parameter, valid when HasSSRC.
	SSRC    uint32
	HasSSRC bool
	// Mode is "PLAY", "RECORD" or another mode, upper-cased; "" when not
	// given, which means PLAY.
	Mode string
	// Destination and Source are the addresses of those parameters.
	Destination string
	Source      string
}

// ParseTransports parses a Transport header: its transport specifications
// in the client's order of preference. Specifications that are not RTP's
// are skipped; parameters the package does not know are ignored.
func ParseTransports(v string) []Transport {
	var ts []Transport
	for _, spec := range strings.Split(v, ",") {
		if t, ok := parseTransport(spec); ok {
			ts = append(ts, t)
		}
	}
	return ts
}

func parseTransport(spec string) (Transport, bool) {
	params := strings.Split(strings.TrimSpace(spec), ";")
	t := Transport{Protocol: strings.TrimSpace(params[0]), Interleaved: [2]int{-1, -1},
		ClientPort: [2]int{-1, -1}, ServerPort: [2]int{-1, -1}}
	switch strings.ToUpper(t.Protocol) {
	case "RTP/AVP", "RTP/AVP/UDP":
	case "RTP/AVP/TCP":
		t.TCP = true
	default:
		return t, false
	}
	for _, p := range params[1:] {
		k, v, _ := strings.Cut(strings.TrimSpace(p), "=")
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "multicast":
			t.Multicast = true
		case "unicast":
			t.Multicast = false
		case "interleaved":
			t.Interleaved = parseRange(v, 255)
		case "client_port":
			t.ClientPort = parseRange(v, 65535)
		case "server_port":
			t.ServerPort = parseRange(v, 65535)
		case "ssrc":
			n, err := strconv.ParseUint(strings.TrimSpace(v), 16, 32)
			t.SSRC, t.HasSSRC = uint32(n), err == nil
		case "mode":
			t.Mode = strings.ToUpper(strings.Trim(strings.TrimSpace(v), `"`))
		case "destination":
			t.Destination = v
		case "source":
			t.Source = v
		}
	}
	return t, true
}

// parseRange parses "a-b" or "a" of numbers up to limit; b is a+1 when
// not given, and both are -1 when malformed.
func parseRange(v string, limit int) [2]int {
	a, b, hasB := strings.Cut(strings.TrimSpace(v), "-")
	x, err := strconv.Atoi(a)
	if err != nil || x < 0 || x > limit {
		return [2]int{-1, -1}
	}
	y := x + 1
	if hasB {
		if y, err = strconv.Atoi(b); err != nil || y < 0 || y > limit {
			return [2]int{-1, -1}
		}
	}
	if y > limit {
		y = -1
	}
	return [2]int{x, y}
}

// String writes the specification as a Transport header carries it.
func (t Transport) String() string {
	var b strings.Builder
	switch {
	case t.TCP:
		b.WriteString("RTP/AVP/TCP")
	default:
		b.WriteString("RTP/AVP")
	}
	if t.Multicast {
		b.WriteString(";multicast")
	} else {
		b.WriteString(";unicast")
	}
	if t.Destination != "" {
		b.WriteString(";destination=" + t.Destination)
	}
	if t.Source != "" {
		b.WriteString(";source=" + t.Source)
	}
	writeRange := func(name string, r [2]int) {
		if r[0] < 0 {
			return
		}
		b.WriteString(";" + name + "=" + strconv.Itoa(r[0]))
		if r[1] >= 0 {
			b.WriteString("-" + strconv.Itoa(r[1]))
		}
	}
	writeRange("interleaved", t.Interleaved)
	writeRange("client_port", t.ClientPort)
	writeRange("server_port", t.ServerPort)
	if t.HasSSRC {
		b.WriteString(";ssrc=" + strings.ToUpper(strconv.FormatUint(uint64(t.SSRC)|1<<32, 16)[1:]))
	}
	if t.Mode != "" {
		b.WriteString(";mode=" + t.Mode)
	}
	return b.String()
}

// Session is the value of a Session header: the session's ID and its
// timeout in seconds, 0 when not given.
type Session struct {
	ID      string
	Timeout int
}

// ParseSession parses a Session header.
func ParseSession(v string) Session {
	id, params, _ := strings.Cut(v, ";")
	s := Session{ID: strings.TrimSpace(id)}
	for _, p := range strings.Split(params, ";") {
		k, val, _ := strings.Cut(strings.TrimSpace(p), "=")
		if strings.EqualFold(strings.TrimSpace(k), "timeout") {
			s.Timeout, _ = strconv.Atoi(strings.TrimSpace(val))
		}
	}
	return s
}

// String writes the header's value.
func (s Session) String() string {
	if s.Timeout > 0 {
		return s.ID + ";timeout=" + strconv.Itoa(s.Timeout)
	}
	return s.ID
}

// RTPInfo is one stream's entry of an RTP-Info header (RFC 2326 section
// 12.33): the stream's URL and the sequence number and RTP timestamp of
// the first packet after the PLAY.
type RTPInfo struct {
	URL     string
	Seq     uint16
	HasSeq  bool
	RTPTime uint32
	HasTime bool
}

// ParseRTPInfo parses an RTP-Info header.
func ParseRTPInfo(v string) []RTPInfo {
	var infos []RTPInfo
	for _, entry := range strings.Split(v, ",") {
		var info RTPInfo
		for _, p := range strings.Split(entry, ";") {
			k, val, _ := strings.Cut(strings.TrimSpace(p), "=")
			val = strings.TrimSpace(val)
			switch strings.ToLower(strings.TrimSpace(k)) {
			case "url":
				info.URL = strings.Trim(val, `"`)
			case "seq":
				n, err := strconv.ParseUint(val, 10, 16)
				info.Seq, info.HasSeq = uint16(n), err == nil
			case "rtptime":
				n, err := strconv.ParseUint(val, 10, 32)
				info.RTPTime, info.HasTime = uint32(n), err == nil
			}
		}
		if info.URL != "" {
			infos = append(infos, info)
		}
	}
	return infos
}

// String writes the entry.
func (i RTPInfo) String() string {
	s := "url=" + i.URL
	if i.HasSeq {
		s += ";seq=" + strconv.Itoa(int(i.Seq))
	}
	if i.HasTime {
		s += ";rtptime=" + strconv.FormatUint(uint64(i.RTPTime), 10)
	}
	return s
}
