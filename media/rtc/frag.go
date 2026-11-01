package rtc

import (
	"errors"
	"strings"

	"github.com/lesismal/fib/webrtc"
)

// errFragment is what parseFragment reports for a body that is not an SDP
// fragment.
var errFragment = errors.New("rtc: malformed SDP fragment")

// maxFragmentCandidates bounds the candidates one PATCH may carry.
const maxFragmentCandidates = 64

// fragment is what a trickle-ice-sdpfrag body (RFC 8840) says: the ICE
// credentials it was gathered under and its candidates, each with the mid
// of its media section, and whether gathering is complete.
type fragment struct {
	ufrag, pwd string
	candidates []webrtc.ICECandidateInit
}

// parseFragment reads an application/trickle-ice-sdpfrag body: a=ice-ufrag
// and a=ice-pwd, at session level or in a media section, and per media
// section (m= then a=mid) its a=candidate and a=end-of-candidates lines.
// Other lines are skipped.
func parseFragment(body string) (*fragment, error) {
	f := &fragment{}
	mid, inMedia := "", false
	for line := range strings.Lines(body) {
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		if len(line) < 2 || line[1] != '=' {
			return nil, errFragment
		}
		switch {
		case line[0] == 'm':
			mid, inMedia = "", true
		case strings.HasPrefix(line, "a=mid:"):
			mid = line[len("a=mid:"):]
		case strings.HasPrefix(line, "a=ice-ufrag:"):
			f.ufrag = line[len("a=ice-ufrag:"):]
		case strings.HasPrefix(line, "a=ice-pwd:"):
			f.pwd = line[len("a=ice-pwd:"):]
		case strings.HasPrefix(line, "a=candidate:"), line == "a=end-of-candidates":
			if len(f.candidates) >= maxFragmentCandidates {
				return nil, errFragment
			}
			c := webrtc.ICECandidateInit{SDPMid: mid}
			if line != "a=end-of-candidates" {
				c.Candidate = line[len("a="):]
			} else if !inMedia {
				c.SDPMid = ""
			}
			f.candidates = append(f.candidates, c)
		}
	}
	for i := range f.candidates {
		f.candidates[i].UsernameFragment = f.ufrag
	}
	return f, nil
}
