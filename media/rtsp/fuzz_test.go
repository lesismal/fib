package rtsp

import (
	"testing"
	"time"
)

// ffmpegAnnounce is what ffmpeg 5.1 sends to publish H.264 and AAC.
const ffmpegAnnounce = "ANNOUNCE rtsp://127.0.0.1:8554/live/s RTSP/1.0\r\nContent-Type: application/sdp\r\nCSeq: 2\r\n" +
	"User-Agent: Lavf59.27.100\r\nContent-Length: 475\r\n\r\n" +
	"v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=No Name\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\na=tool:libavformat 59.27.100\r\n" +
	"m=video 0 RTP/AVP 96\r\nb=AS:200\r\na=rtpmap:96 H264/90000\r\n" +
	"a=fmtp:96 packetization-mode=1; sprop-parameter-sets=Z2QAHqzZQKAv+XARAAADAAEAAAMAMg8WLZY=,aOvjyyLA; profile-level-id=64001E\r\n" +
	"a=control:streamid=0\r\nm=audio 0 RTP/AVP 97\r\nb=AS:64\r\na=rtpmap:97 MPEG4-GENERIC/48000/2\r\n" +
	"a=fmtp:97 profile-level-id=1;mode=AAC-hbr;sizelength=13;indexlength=3;indexdeltalength=3; config=119056E500\r\n" +
	"a=control:streamid=1\r\n" +
	"SETUP rtsp://127.0.0.1:8554/live/s/streamid=0 RTSP/1.0\r\nTransport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=record\r\nCSeq: 3\r\n\r\n"

// FuzzParser feeds the input to a Parser in pieces of every size up to
// 7; whatever it makes of the bytes, it must not panic, and must make the
// same of them however they are cut.
func FuzzParser(f *testing.F) {
	f.Add([]byte(ffmpegAnnounce))
	f.Add(AppendInterleaved([]byte("RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n"), 1, []byte{0x80, 200, 0, 1}))
	f.Add([]byte("OPTIONS * RTSP/1.0\nCSeq: 1\n folded\n\n$\x00\x00"))
	f.Fuzz(func(t *testing.T, data []byte) {
		count := func(step int) (int, error) {
			p := Parser{MaxHeaderBytes: 512, MaxBodyBytes: 1024}
			n := 0
			var err error
			for b := data; len(b) > 0 && err == nil; {
				k := min(step, len(b))
				err = p.Feed(b[:k], func(m any) error {
					n++
					switch m := m.(type) {
					case *Request:
						_ = m.Append(nil)
						_ = ParseTransports(m.Header.Get("Transport"))
						_ = ParseSession(m.Header.Get("Session"))
						_ = parseAuthParams(m.Header.Get("Authorization"))
					case *Response:
						_ = m.Append(nil)
						_ = ParseRTPInfo(m.Header.Get("RTP-Info"))
					}
					return nil
				})
				b = b[k:]
			}
			return n, err
		}
		n, err := count(len(data) + 1)
		for step := 1; step <= 7 && err == nil; step++ {
			if m, err2 := count(step); err2 == nil && m != n {
				t.Fatalf("%d messages whole, %d in pieces of %d", n, m, step)
			}
		}
	})
}

// FuzzTransport parses Transport values and writes them back: what is
// written parses to the same.
func FuzzTransport(f *testing.F) {
	f.Add("RTP/AVP/TCP;unicast;interleaved=0-1;mode=record")
	f.Add("RTP/AVP;unicast;client_port=5000-5001;server_port=8000-8001;ssrc=1234ABCD")
	f.Fuzz(func(t *testing.T, s string) {
		for _, tr := range ParseTransports(s) {
			back := ParseTransports(tr.String())
			if len(back) != 1 || back[0].TCP != tr.TCP || back[0].Interleaved != tr.Interleaved || back[0].ClientPort != tr.ClientPort ||
				back[0].HasSSRC != tr.HasSSRC || back[0].SSRC != tr.SSRC {
				t.Fatalf("%q -> %q -> %+v", s, tr.String(), back)
			}
		}
	})
}

// FuzzDigest checks credentials of any shape against a server: none that
// the fuzzer makes up passes.
func FuzzDigest(f *testing.F) {
	a := newAuthServer("r", AuthDigest|AuthBasic, 0)
	now := time.Unix(1_700_000_000, 0)
	c, _ := newClientAuth("u", "p", a.challenges(now, false))
	f.Add(c.authorization("DESCRIBE", "rtsp://h/s"))
	f.Add("Basic dTp4")
	f.Fuzz(func(t *testing.T, authz string) {
		if _, ok, _ := a.verify("DESCRIBE", authz, now, func(string) (string, bool) { return "secret password", true }); ok {
			t.Fatalf("%q passed", authz)
		}
	})
}
