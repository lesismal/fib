package sdp

import (
	"bytes"
	"reflect"
	"testing"
)

// FuzzParse checks that whatever the parser accepts it writes in a form it
// parses again to the same lines, and that no accessor panics on it.
func FuzzParse(f *testing.F) {
	for _, b := range readTestdata(f) {
		f.Add(b)
	}
	f.Add([]byte("v=0\no=- 1 2 IN IP4 0.0.0.0\ns=-\nt=0 0\nm=audio 9/2 RTP/AVP 0\na=rtpmap:0 PCMU/8000\n"))
	f.Add([]byte("v=0\r\no=- 1 2 IN IP4 0.0.0.0\r\ns=\r\nt=0 0\r\nm=application 9 DTLS/SCTP 5000\r\n\r\n"))
	f.Fuzz(func(t *testing.T, in []byte) {
		sd, err := Parse(in)
		if err != nil {
			return
		}
		out := sd.Marshal()
		again, err := Parse(out)
		if err != nil {
			t.Fatalf("output does not parse: %v\n%q", err, out)
		}
		if !bytes.Equal(again.Marshal(), out) {
			t.Fatalf("output changes on a second round:\n%q", out)
		}
		if !bytes.Contains(in, []byte("\r")) && bytes.HasSuffix(in, []byte("\n")) && !bytes.HasSuffix(in, []byte("\n\n")) &&
			!bytes.Equal(bytes.ReplaceAll(out, []byte("\r\n"), []byte("\n")), in) {
			t.Fatalf("LF input not kept:\n%q\n%q", in, out)
		}
		touch(sd)
	})
}

func touch(sd *SessionDescription) {
	sd.Origin()
	sd.Groups()
	sd.Bundle()
	sd.ICELite()
	sd.Fingerprints()
	sd.ICEOptions()
	for _, md := range sd.Media {
		sd.MediaDirection(md)
		sd.Lookup(md, "setup")
		md.MediaName()
		md.Kind()
		md.Rejected()
		md.IsDataChannel()
		md.Mid()
		md.Setup()
		md.Connection()
		md.Candidates()
		md.Codecs()
		md.RTCPFeedbacks()
		md.Fmtps()
		md.RTPMaps()
		md.ExtMaps()
		md.SSRCs()
		md.SSRCGroups()
		md.MSIDs()
		md.RIDs()
		md.Simulcast()
		md.SCTPPort()
		md.MaxMessageSize()
	}
}

// FuzzValues checks that each attribute value the parsers accept is
// written by its String method in a form that parses to the same value.
func FuzzValues(f *testing.F) {
	for _, s := range []string{
		"- 1 2 IN IP4 127.0.0.1", "audio 9/2 RTP/AVP 0", "BUNDLE 0 1", "sha-256 AB:CD",
		"111 opus/48000/2", "111 minptime=10", "96 nack pli", "* ccm fir", "2/recvonly urn:x a",
		"1 cname:x", "FID 1 2", "- t", "f send pt=96", "send a,~b;c recv d", "IN IP4 0.0.0.0",
	} {
		f.Add(s)
	}
	type parser func(string) (any, error)
	parsers := []parser{
		func(s string) (any, error) { return ParseOrigin(s) },
		func(s string) (any, error) { return ParseConnection(s) },
		func(s string) (any, error) { return ParseMediaName(s) },
		func(s string) (any, error) { return ParseGroup(s) },
		func(s string) (any, error) { return ParseFingerprint(s) },
		func(s string) (any, error) { return ParseRTPMap(s) },
		func(s string) (any, error) { return ParseFmtp(s) },
		func(s string) (any, error) { return ParseRTCPFeedback(s) },
		func(s string) (any, error) { return ParseExtMap(s) },
		func(s string) (any, error) { return ParseSSRC(s) },
		func(s string) (any, error) { return ParseSSRCGroup(s) },
		func(s string) (any, error) { return ParseMSID(s) },
		func(s string) (any, error) { return ParseRID(s) },
		func(s string) (any, error) { return ParseSimulcast(s) },
	}
	f.Fuzz(func(t *testing.T, in string) {
		for i, parse := range parsers {
			v, err := parse(in)
			if err != nil {
				continue
			}
			s := v.(interface{ String() string }).String()
			again, err := parse(s)
			if err != nil {
				t.Fatalf("parser %d: %q written as %q, which fails: %v", i, in, s, err)
			}
			if !reflect.DeepEqual(again, v) {
				t.Fatalf("parser %d: %q is %+v, written as %q, read back as %+v", i, in, v, s, again)
			}
		}
	})
}
