package rtsp

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func feedAll(t *testing.T, p *Parser, data []byte, step int) ([]any, error) {
	t.Helper()
	var msgs []any
	fn := func(m any) error {
		switch m := m.(type) {
		case *Request:
			c := *m
			c.Body = bytes.Clone(m.Body)
			msgs = append(msgs, &c)
		case *Response:
			c := *m
			c.Body = bytes.Clone(m.Body)
			msgs = append(msgs, &c)
		case *Interleaved:
			msgs = append(msgs, &Interleaved{Channel: m.Channel, Data: bytes.Clone(m.Data)})
		}
		return nil
	}
	for len(data) > 0 {
		n := min(step, len(data))
		if err := p.Feed(data[:n], fn); err != nil {
			return msgs, err
		}
		data = data[n:]
	}
	return msgs, nil
}

func TestParser(t *testing.T) {
	req := &Request{Method: MethodAnnounce, URL: "rtsp://h/live/s", Header: Header{{"CSeq", "2"}, {"Content-Type", "application/sdp"}}, Body: []byte("v=0\r\n")}
	resp := &Response{StatusCode: StatusOK, Header: Header{{"CSeq", "2"}, {"Session", "ABC;timeout=60"}}}
	var stream []byte
	stream = req.Append(stream)
	stream = append(stream, "\r\n"...)
	stream = AppendInterleaved(stream, 3, []byte{1, 2, 3})
	stream = resp.Append(stream)
	stream = append(stream, "OPTIONS * RTSP/1.0\nCSeq: 3\nX-Long: a\n b\n\n"...)
	for _, step := range []int{1, 3, 7, len(stream)} {
		var p Parser
		msgs, err := feedAll(t, &p, stream, step)
		if err != nil || len(msgs) != 4 || p.Buffered() != 0 {
			t.Fatalf("step %d: %v %d", step, err, len(msgs))
		}
		r := msgs[0].(*Request)
		if r.Method != MethodAnnounce || string(r.Body) != "v=0\r\n" || r.CSeq() != 2 || r.Header.Get("content-type") != "application/sdp" {
			t.Fatalf("request %+v", r)
		}
		if il := msgs[1].(*Interleaved); il.Channel != 3 || !bytes.Equal(il.Data, []byte{1, 2, 3}) {
			t.Fatalf("interleaved %+v", il)
		}
		if rs := msgs[2].(*Response); rs.StatusCode != 200 || rs.Reason != "OK" || rs.CSeq() != 2 {
			t.Fatalf("response %+v", rs)
		}
		if o := msgs[3].(*Request); o.URL != "*" || o.Header.Get("X-Long") != "a b" {
			t.Fatalf("options %+v", o)
		}
	}
	// Errors.
	for _, tc := range []struct {
		in  string
		err error
	}{
		{"GET\r\n\r\n", ErrMalformed},
		{"OPTIONS * HTTP/1.1\r\n\r\n", ErrMalformed},
		{"OPT@ONS * RTSP/1.0\r\n\r\n", ErrMalformed},
		{"RTSP/1.0 2x OK\r\n\r\n", ErrMalformed},
		{"RTSP/2.0 200 OK\r\n\r\n", ErrMalformed},
		{"OPTIONS * RTSP/1.0\r\nbad line\r\n\r\n", ErrMalformed},
		{"OPTIONS * RTSP/1.0\r\n continued\r\n\r\n", ErrMalformed},
		{"OPTIONS * RTSP/1.0\r\nContent-Length: x\r\n\r\n", ErrMalformed},
		{"OPTIONS * RTSP/1.0\r\nContent-Length: 999999999\r\n\r\n", ErrTooLarge},
		{"OPTIONS * RTSP/1.0\r\nX: " + strings.Repeat("a", 100) + "\r\n\r\n", ErrTooLarge},
		{"OPTIONS * RTSP/1.0\r\nX: " + strings.Repeat("a", 100), ErrTooLarge},
	} {
		p := Parser{MaxHeaderBytes: 64}
		if _, err := feedAll(t, &p, []byte(tc.in), len(tc.in)); err != tc.err {
			t.Errorf("%q: %v, want %v", tc.in, err, tc.err)
		}
	}
	// An error from fn stops the feed.
	stop := errors.New("stop")
	var p Parser
	if err := p.Feed(stream, func(any) error { return stop }); err != stop {
		t.Fatal(err)
	}
	if err := p.Feed(AppendInterleaved(nil, 0, nil), func(any) error { return stop }); err != stop {
		t.Fatal(err)
	}
	// An incomplete frame and body wait for more.
	p = Parser{}
	if err := p.Feed([]byte{'$', 0, 0, 5, 1}, func(any) error { t.Fatal("early"); return nil }); err != nil || p.Buffered() != 5 {
		t.Fatal(err, p.Buffered())
	}
	p = Parser{}
	if err := p.Feed([]byte("OPTIONS * RTSP/1.0\r\nContent-Length: 4\r\n\r\nab"), func(any) error { t.Fatal("early"); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestHeaderAndAppend(t *testing.T) {
	var h Header
	h.Add("A", "1")
	h.Add("a", "2")
	h.Set("B", "3")
	if h.Get("A") != "1" || len(h.Values("a")) != 2 || h.Get("missing") != "" {
		t.Fatal(h)
	}
	h.Set("a", "4")
	if v := h.Values("A"); len(v) != 1 || v[0] != "4" {
		t.Fatal(v)
	}
	h.Del("b")
	if _, ok := h.Lookup("B"); ok {
		t.Fatal("deleted")
	}
	resp := &Response{StatusCode: 999, Reason: "Custom", Header: Header{{"Content-Length", "99"}}, Body: []byte("x")}
	if s := string(resp.Append(nil)); s != "RTSP/1.0 999 Custom\r\nContent-Length: 1\r\n\r\nx" {
		t.Fatalf("%q", s)
	}
	if StatusText(StatusUnsupportedTransport) != "Unsupported Transport" || StatusText(1) != "" {
		t.Fatal("status text")
	}
	if (&Request{}).CSeq() != -1 {
		t.Fatal("cseq")
	}
}

func TestTransport(t *testing.T) {
	ts := ParseTransports(`RTP/AVP/TCP;unicast;interleaved=4-5;mode="record", RTP/AVP;unicast;client_port=5000-5001;server_port=6000;ssrc=0000ABCD;destination=1.2.3.4;source=5.6.7.8, RAW/RAW/UDP;unicast, RTP/AVP/UDP;multicast;client_port=x`)
	if len(ts) != 3 {
		t.Fatalf("%+v", ts)
	}
	if !ts[0].TCP || ts[0].Interleaved != [2]int{4, 5} || ts[0].Mode != "RECORD" {
		t.Fatalf("%+v", ts[0])
	}
	if ts[1].TCP || ts[1].ClientPort != [2]int{5000, 5001} || ts[1].ServerPort != [2]int{6000, 6001} || !ts[1].HasSSRC || ts[1].SSRC != 0xabcd || ts[1].Destination != "1.2.3.4" || ts[1].Source != "5.6.7.8" {
		t.Fatalf("%+v", ts[1])
	}
	if !ts[2].Multicast || ts[2].ClientPort[0] != -1 {
		t.Fatalf("%+v", ts[2])
	}
	for _, tr := range ts {
		back := ParseTransports(tr.String())
		if len(back) != 1 || back[0] != tr && !(tr.Protocol != back[0].Protocol) {
			t.Errorf("%s: %+v", tr.String(), back)
		}
	}
	if r := parseRange("65535", 65535); r != [2]int{65535, -1} {
		t.Fatal(r)
	}
	if r := parseRange("1-x", 65535); r[0] != -1 {
		t.Fatal(r)
	}
	s := ParseSession("ABC ; timeout = 30")
	if s.ID != "ABC" || s.Timeout != 30 || s.String() != "ABC;timeout=30" || (Session{ID: "X"}).String() != "X" {
		t.Fatalf("%+v", s)
	}
	infos := ParseRTPInfo(`url="rtsp://h/s/trackID=0";seq=10;rtptime=4294967295, url=rtsp://h/s/trackID=1;seq=x, seq=3`)
	if len(infos) != 2 || infos[0].Seq != 10 || infos[0].RTPTime != 1<<32-1 || infos[1].HasSeq || infos[1].HasTime {
		t.Fatalf("%+v", infos)
	}
	if infos[0].String() != "url=rtsp://h/s/trackID=0;seq=10;rtptime=4294967295" || infos[1].String() != "url=rtsp://h/s/trackID=1" {
		t.Fatal(infos[0].String(), infos[1].String())
	}
}

func TestAuth(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	a := newAuthServer("r", AuthDigest|AuthBasic, 0)
	pass := func(u string) (string, bool) { return "secret", u == "alice" }
	ch := a.challenges(now, true)
	if len(ch) != 2 || !strings.Contains(ch[0], "stale=TRUE") || !strings.HasPrefix(ch[1], "Basic") {
		t.Fatal(ch)
	}
	for _, qop := range []bool{false, true} {
		c, ok := newClientAuth("alice", "secret", []string{`Basic realm="r"`, strings.Replace(ch[0], "stale=TRUE", `qop="auth,auth-int", opaque="o"`, 1)})
		if !ok || c.basic {
			t.Fatal("digest not picked")
		}
		c.qop = qop
		authz := c.authorization("DESCRIBE", "rtsp://h/s")
		if u, ok, stale := a.verify("DESCRIBE", authz, now, pass); !ok || stale || u != "alice" {
			t.Fatalf("qop %v: %v %v", qop, ok, stale)
		}
		if _, ok, _ := a.verify("PLAY", authz, now, pass); ok {
			t.Fatal("method not covered")
		}
		if _, ok, stale := a.verify("DESCRIBE", authz, now.Add(time.Hour), pass); ok || !stale {
			t.Fatal("old nonce taken")
		}
	}
	c, _ := newClientAuth("alice", "wrong", ch)
	if _, ok, _ := a.verify("DESCRIBE", c.authorization("DESCRIBE", "u"), now, pass); ok {
		t.Fatal("wrong password taken")
	}
	c, _ = newClientAuth("bob", "secret", ch)
	if _, ok, _ := a.verify("DESCRIBE", c.authorization("DESCRIBE", "u"), now, pass); ok {
		t.Fatal("unknown user taken")
	}
	for _, authz := range []string{
		`Digest username="alice", realm="other", nonce="` + a.nonce(now) + `", uri="u", response="x"`,
		`Digest username="alice", realm="r", nonce="00", uri="u", response="x"`,
		`Digest username="alice", realm="r", nonce="zz` + strings.Repeat("0", 38) + `", uri="u", response="x"`,
		`Digest username="alice", realm="r", nonce="` + a.nonce(now) + `", uri="u", response="x", algorithm=SHA-256`,
		`Basic !!!`, `Basic ` + "YWxpY2U6d3Jvbmc=", `Basic YWxpY2U=`, `Bearer x`, ``,
	} {
		if _, ok, _ := a.verify("DESCRIBE", authz, now, pass); ok {
			t.Errorf("%q taken", authz)
		}
	}
	if u, ok, _ := a.verify("X", "Basic YWxpY2U6c2VjcmV0", now, pass); !ok || u != "alice" {
		t.Fatal("basic")
	}
	b, ok := newClientAuth("alice", "secret", []string{`Digest realm="r", algorithm=SHA-256`, `Basic realm="r"`})
	if !ok || !b.basic || b.authorization("X", "u") != "Basic YWxpY2U6c2VjcmV0" {
		t.Fatal("basic fallback")
	}
	if _, ok := newClientAuth("a", "b", []string{"Bearer"}); ok {
		t.Fatal("unknown scheme answered")
	}
	// A nonce from the future is not fresh.
	if _, fresh := a.nonceValid(a.nonce(now.Add(time.Hour)), now); fresh {
		t.Fatal("future nonce")
	}
	p := parseAuthParams(`a="x\"y,z", b=plain , c="unterminated`)
	if p["a"] != `x"y,z` || p["b"] != "plain" || p["c"] != "unterminated" {
		t.Fatal(p)
	}
	if d := newAuthServer("r", 0, 0); d.methods != AuthDigest || d.lifetime != DefaultNonceLifetime {
		t.Fatal("defaults")
	}
}

func TestURL(t *testing.T) {
	if p, q, ok := streamPath("rtsp://h:1/live/s/?tok=1"); !ok || p != "live/s" || q != "tok=1" {
		t.Fatal(p, q, ok)
	}
	// A control resolved against a base URL with a query.
	if p, q, ok := streamPath("rtsp://h:1/live/s?tok=1/trackID=2"); !ok || p != "live/s/trackID=2" || q != "tok=1" {
		t.Fatal(p, q, ok)
	}
	if p, q, ok := streamPath("rtsp://h:1/live/s?tok=a/b"); !ok || p != "live/s" || q != "tok=a/b" {
		t.Fatal(p, q, ok)
	}
	if _, _, ok := streamPath("http://h/x"); ok {
		t.Fatal("http taken")
	}
	if _, _, ok := streamPath("rtsp://h/%zz"); ok {
		t.Fatal("bad escape taken")
	}
	for _, tc := range []struct {
		in   string
		path string
		n    int
	}{{"live/s/trackID=1", "live/s", 1}, {"trackID=2", "", 2}, {"live/s", "live/s", -1}, {"live/trackID=x", "live/trackID=x", -1}} {
		if p, n := splitTrackID(tc.in); p != tc.path || n != tc.n {
			t.Errorf("%s: %s %d", tc.in, p, n)
		}
	}
	if resolveControl("rtsp://h/s", "trackID=0") != "rtsp://h/s/trackID=0" || resolveControl("rtsp://h/s/", "*") != "rtsp://h/s/" ||
		resolveControl("b", "rtsp://x/y") != "rtsp://x/y" {
		t.Fatal("resolve")
	}
	if !matchControl("rtsp://h/s", "streamid=0", "rtsp://h:554/s/streamid=0") || matchControl("rtsp://h/s", "", "rtsp://h/s") ||
		matchControl("rtsp://h/s", "streamid=0", "rtsp://h/s/streamid=1") {
		t.Fatal("match")
	}
	if u, user, pass, err := stripUserinfo("rtsp://a:b@h/s"); err != nil || u != "rtsp://h/s" || user != "a" || pass != "b" {
		t.Fatal(u, user, pass, err)
	}
	if _, _, _, err := stripUserinfo("rtsp://h/%zz"); err == nil {
		t.Fatal("bad URL")
	}
}
