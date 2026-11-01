package rtmp

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/lesismal/fib/media/amf"
)

// pipe joins a client and a server Conn in memory. What each outputs is
// queued and fed to the other by pump, so that no Feed runs inside
// another.
type pipe struct {
	client, server       *Conn
	toServer, toClient   []byte
	clientMsgs, srvMsgs  []Message
	chunk                int
	clientErr, serverErr error
}

func copyMsg(m *Message) Message {
	c := *m
	c.Payload = append([]byte(nil), m.Payload...)
	return c
}

func newPipe(t testing.TB, simple bool) *pipe {
	p := &pipe{chunk: 1 << 20}
	p.client = NewConn(ConnConfig{Client: true, SimpleHandshake: simple,
		Output:    func(b []byte) error { p.toServer = append(p.toServer, b...); return nil },
		OnMessage: func(m *Message) error { p.clientMsgs = append(p.clientMsgs, copyMsg(m)); return nil }})
	p.server = NewConn(ConnConfig{
		Output:    func(b []byte) error { p.toClient = append(p.toClient, b...); return nil },
		OnMessage: func(m *Message) error { p.srvMsgs = append(p.srvMsgs, copyMsg(m)); return nil }})
	if err := p.client.Start(); err != nil {
		t.Fatal(err)
	}
	if err := p.server.Start(); err != nil {
		t.Fatal(err)
	}
	p.pump(t)
	if !p.client.HandshakeDone() || !p.server.HandshakeDone() {
		t.Fatal("handshake not done")
	}
	return p
}

func (p *pipe) pump(t testing.TB) {
	t.Helper()
	for len(p.toServer) > 0 || len(p.toClient) > 0 {
		if len(p.toServer) > 0 {
			b := p.toServer
			p.toServer = nil
			for len(b) > 0 {
				n := min(len(b), p.chunk)
				if err := p.server.Feed(b[:n]); err != nil {
					t.Fatal("server: ", err)
				}
				b = b[n:]
			}
		}
		if len(p.toClient) > 0 {
			b := p.toClient
			p.toClient = nil
			for len(b) > 0 {
				n := min(len(b), p.chunk)
				if err := p.client.Feed(b[:n]); err != nil {
					t.Fatal("client: ", err)
				}
				b = b[n:]
			}
		}
	}
}

func TestHandshakes(t *testing.T) {
	for _, simple := range []bool{true, false} {
		newPipe(t, simple)
	}
	// The digest handshake signs what it should.
	hello := clientHello(false)
	scheme, d, ok := findDigest(hello[1:], fpKeyText)
	if !ok || scheme != 1 {
		t.Fatal("client digest not found")
	}
	reply, err := serverHandshake(hello)
	if err != nil {
		t.Fatal(err)
	}
	s1, s2 := reply[1:1+handshakeSize], reply[1+handshakeSize:]
	sScheme, sDigest, ok := findDigest(s1, fmsKeyText)
	if !ok || sScheme != scheme {
		t.Fatal("server digest not found")
	}
	tmp := hmac.New(sha256.New, fmsKey)
	tmp.Write(d)
	mac := hmac.New(sha256.New, tmp.Sum(nil))
	mac.Write(s2[:handshakeSize-32])
	if !bytes.Equal(mac.Sum(nil), s2[handshakeSize-32:]) {
		t.Fatal("S2 not signed for C1's digest")
	}
	c2, err := clientAnswer(reply)
	if err != nil {
		t.Fatal(err)
	}
	tmp = hmac.New(sha256.New, fpKey)
	tmp.Write(sDigest)
	mac = hmac.New(sha256.New, tmp.Sum(nil))
	mac.Write(c2[:handshakeSize-32])
	if !bytes.Equal(mac.Sum(nil), c2[handshakeSize-32:]) {
		t.Fatal("C2 not signed for S1's digest")
	}
	// Scheme 0 digests are found too.
	c1 := make([]byte, handshakeSize)
	signPacket(c1, clientVersion, fpKeyText, 0)
	if s, _, ok := findDigest(c1, fpKeyText); !ok || s != 0 {
		t.Fatal("scheme 0")
	}
	// A C1 with a version but no valid digest gets the simple handshake.
	bad := clientHello(false)
	bad[100] ^= 0xff
	bad[800] ^= 0xff
	reply, _ = serverHandshake(bad)
	if !bytes.Equal(reply[1+handshakeSize:], bad[1:]) {
		t.Fatal("S2 does not echo C1")
	}
	// A simple S1 is echoed in C2.
	c2, _ = clientAnswer(reply)
	if !bytes.Equal(c2, reply[1:1+handshakeSize]) {
		t.Fatal("C2 does not echo S1")
	}
	// Other versions are refused.
	hello[0] = 6
	if _, err := serverHandshake(hello); !errors.Is(err, ErrHandshake) {
		t.Fatal(err)
	}
	if _, err := clientAnswer(hello); !errors.Is(err, ErrHandshake) {
		t.Fatal(err)
	}
	s := NewConn(ConnConfig{Output: func([]byte) error { return nil }})
	if err := s.Feed(hello); !errors.Is(err, ErrHandshake) {
		t.Fatal(err)
	}
	c := NewConn(ConnConfig{Client: true, Output: func([]byte) error { return nil }})
	if err := c.Feed(make([]byte, 1+2*handshakeSize)); !errors.Is(err, ErrHandshake) {
		t.Fatal(err)
	}
}

func TestHandshakeOutputErrors(t *testing.T) {
	fail := errors.New("fail")
	s := NewConn(ConnConfig{Output: func([]byte) error { return fail }})
	if err := s.Feed(clientHello(true)); err != fail {
		t.Fatal(err)
	}
	c := NewConn(ConnConfig{Client: true, Output: func([]byte) error { return fail }})
	if err := c.Start(); err != fail {
		t.Fatal(err)
	}
	reply, _ := serverHandshake(clientHello(true))
	if err := c.Feed(reply); err != fail {
		t.Fatal(err)
	}
}

func TestMessagesRoundTrip(t *testing.T) {
	for _, chunk := range []int{1, 3, 17, 1 << 20} {
		for _, size := range []int{1, 128, 300} {
			p := newPipe(t, false)
			p.chunk = chunk
			if err := p.client.SetChunkSize(size); err != nil {
				t.Fatal(err)
			}
			rng := rand.New(rand.NewPCG(1, uint64(size)))
			var want []Message
			ts := map[uint8]uint32{TypeAudio: 0, TypeVideo: 0xfffff0}
			for i := range 200 {
				typ := uint8(TypeAudio)
				if i%3 == 0 {
					typ = TypeVideo
				}
				// Steady deltas give type 3 headers, changing ones type 2,
				// changing lengths type 1, and a timestamp going back type
				// 0; video crosses into extended timestamps.
				switch {
				case i%50 == 49:
					ts[typ] -= 10
				case i%7 == 0:
					ts[typ] += uint32(rng.IntN(100))
				default:
					ts[typ] += 23
				}
				n := 10
				if i%5 == 0 {
					n = rng.IntN(1000)
				}
				payload := make([]byte, n)
				for j := range payload {
					payload[j] = byte(rng.Uint32())
				}
				m := Message{Type: typ, StreamID: 1, Timestamp: ts[typ], Payload: payload}
				if err := p.client.WriteMessage(&m); err != nil {
					t.Fatal(err)
				}
				want = append(want, m)
			}
			// A chunk stream id needing two and three bytes.
			for _, csid := range []uint32{70, 400} {
				m := Message{Type: TypeDataAMF0, StreamID: 1, Timestamp: 5, ChunkStreamID: csid, Payload: []byte{1, 2, 3}}
				_ = p.client.WriteMessage(&m)
				want = append(want, m)
			}
			p.pump(t)
			if len(p.srvMsgs) != len(want) {
				t.Fatalf("chunk %d size %d: %d messages, want %d", chunk, size, len(p.srvMsgs), len(want))
			}
			for i, m := range p.srvMsgs {
				w := want[i]
				if m.Type != w.Type || m.Timestamp != w.Timestamp || m.StreamID != w.StreamID || !bytes.Equal(m.Payload, w.Payload) {
					t.Fatalf("chunk %d size %d message %d: %+v want %+v", chunk, size, i, m, w)
				}
			}
		}
	}
}

// chunkHeader builds a basic and message header by hand.
func chunkHeader(format uint8, csid uint32, ts, length uint32, typ uint8, streamID uint32) []byte {
	b := appendBasicHeader(nil, format, csid)
	if format <= 2 {
		b = append(b, byte(ts>>16), byte(ts>>8), byte(ts))
	}
	if format <= 1 {
		b = append(b, byte(length>>16), byte(length>>8), byte(length), typ)
	}
	if format == 0 {
		b = binary.LittleEndian.AppendUint32(b, streamID)
	}
	return b
}

func serverConn(t testing.TB, onMessage func(*Message) error, out *[]byte) *Conn {
	t.Helper()
	c := NewConn(ConnConfig{OnMessage: onMessage, MaxMessageSize: 1000,
		Output: func(b []byte) error {
			if out != nil {
				*out = append(*out, b...)
			}
			return nil
		}})
	hello := clientHello(true)
	if err := c.Feed(hello); err != nil {
		t.Fatal(err)
	}
	if err := c.Feed(make([]byte, handshakeSize)); err != nil {
		t.Fatal(err)
	}
	if !c.HandshakeDone() {
		t.Fatal("handshake")
	}
	if out != nil {
		*out = (*out)[:0]
	}
	return c
}

func TestExtendedTimestampQuirk(t *testing.T) {
	var got []Message
	c := serverConn(t, func(m *Message) error { got = append(got, copyMsg(m)); return nil }, nil)
	payload := bytes.Repeat([]byte{7}, 200)
	// The continuation chunk repeats the extended timestamp, as the
	// specification says.
	b := chunkHeader(0, 4, extended, 200, TypeAudio, 1)
	b = binary.BigEndian.AppendUint32(b, 0x01000000)
	b = append(b, payload[:128]...)
	b = append(b, 0xc4)
	b = binary.BigEndian.AppendUint32(b, 0x01000000)
	b = append(b, payload[128:]...)
	// The next one leaves it out of the continuation chunk.
	b = append(b, chunkHeader(0, 4, extended, 200, TypeAudio, 1)...)
	b = binary.BigEndian.AppendUint32(b, 0x01000010)
	b = append(b, payload[:128]...)
	b = append(b, 0xc4)
	b = append(b, payload[128:]...)
	// A type 3 header that starts a new message carries it, and adds the
	// delta: here the last type 0 timestamp.
	b = append(b, 0xc4)
	b = binary.BigEndian.AppendUint32(b, 0x01000010)
	b = append(b, payload[:128]...)
	b = append(b, 0xc4)
	b = binary.BigEndian.AppendUint32(b, 0x01000010)
	b = append(b, payload[128:]...)
	if err := c.Feed(b); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("%d messages", len(got))
	}
	for i, ts := range []uint32{0x01000000, 0x01000010, 0x02000020} {
		if got[i].Timestamp != ts || !bytes.Equal(got[i].Payload, payload) {
			t.Fatalf("%d: ts %x len %d", i, got[i].Timestamp, len(got[i].Payload))
		}
	}
}

func TestControlMessages(t *testing.T) {
	var got []Message
	var out []byte
	c := serverConn(t, func(m *Message) error { got = append(got, copyMsg(m)); return nil }, &out)
	write := func(typ uint8, payload []byte) {
		t.Helper()
		b := append(chunkHeader(0, 2, 0, uint32(len(payload)), typ, 0), payload...)
		if err := c.Feed(b); err != nil {
			t.Fatal(err)
		}
	}
	// A window makes the Conn acknowledge.
	write(TypeWindowAckSize, []byte{0, 0, 0, 20})
	write(TypeAck, []byte{0, 0, 0, 1})
	if len(out) == 0 || out[7] != TypeAck {
		t.Fatalf("no ack: %x", out)
	}
	write(TypeWindowAckSize, []byte{0x40, 0, 0, 0})
	out = out[:0]
	// Ping requests are answered, other events delivered.
	write(TypeUserControl, []byte{0, EventPingRequest, 0, 0, 0, 9})
	if !bytes.Contains(out, []byte{0, EventPingResponse, 0, 0, 0, 9}) {
		t.Fatalf("no ping response: %x", out)
	}
	write(TypeUserControl, []byte{0, EventStreamBegin, 0, 0, 0, 1})
	if len(got) != 1 || got[0].Type != TypeUserControl {
		t.Fatal(got)
	}
	// A peer bandwidth other than our window is answered with our window.
	out = out[:0]
	write(TypeSetPeerBandwidth, []byte{0, 1, 0, 0, 2})
	if len(out) == 0 || out[7] != TypeWindowAckSize {
		t.Fatalf("%x", out)
	}
	out = out[:0]
	write(TypeSetPeerBandwidth, []byte{0, 1, 0, 0, 2})
	if len(out) != 0 {
		t.Fatal("answered the same bandwidth twice")
	}
	// An abort drops the message being assembled on another chunk stream.
	b := append(chunkHeader(0, 5, 0, 200, TypeAudio, 1), make([]byte, 128)...)
	if err := c.Feed(b); err != nil {
		t.Fatal(err)
	}
	write(TypeAbort, []byte{0, 0, 0, 5})
	b = append(chunkHeader(0, 5, 0, 3, TypeAudio, 1), 1, 2, 3)
	if err := c.Feed(b); err != nil {
		t.Fatal(err)
	}
	if m := got[len(got)-1]; len(m.Payload) != 3 {
		t.Fatalf("%+v", m)
	}
	// A new header in the middle of a message drops it too.
	b = append(chunkHeader(0, 5, 0, 200, TypeAudio, 1), make([]byte, 128)...)
	b = append(b, chunkHeader(1, 5, 0, 2, TypeAudio, 0)...)
	b = append(b, 9, 9)
	if err := c.Feed(b); err != nil {
		t.Fatal(err)
	}
	if m := got[len(got)-1]; !bytes.Equal(m.Payload, []byte{9, 9}) {
		t.Fatalf("%+v", m)
	}
	// Chunk size changes.
	write(TypeSetChunkSize, []byte{0, 0, 1, 0})
	b = append(chunkHeader(0, 6, 0, 256, TypeVideo, 1), make([]byte, 256)...)
	if err := c.Feed(b); err != nil {
		t.Fatal(err)
	}
	if m := got[len(got)-1]; len(m.Payload) != 256 {
		t.Fatalf("%+v", m)
	}
	if c.BytesIn() == 0 {
		t.Fatal("BytesIn")
	}
	// Aggregate messages are split.
	var agg []byte
	for i, ts := range []uint32{1000, 1040} {
		agg = append(agg, TypeVideo, 0, 0, 2, byte(ts>>16), byte(ts>>8), byte(ts), byte(ts>>24), 0, 0, 0, byte(i), 0xaa, 0, 0, 0, 13)
	}
	n := len(got)
	b = append(chunkHeader(0, 7, 500, uint32(len(agg)), TypeAggregate, 1), agg...)
	if err := c.Feed(b); err != nil {
		t.Fatal(err)
	}
	if len(got) != n+2 || got[n].Timestamp != 500 || got[n+1].Timestamp != 540 || got[n+1].Payload[0] != 1 {
		t.Fatalf("%+v", got[n:])
	}
	// A cut aggregate fails.
	b = append(chunkHeader(0, 7, 500, 12, TypeAggregate, 1), agg[:12]...)
	if err := c.Feed(b); !errors.Is(err, ErrChunk) {
		t.Fatal(err)
	}
}

func TestChunkErrors(t *testing.T) {
	stop := errors.New("stop")
	cases := []struct {
		name string
		b    []byte
		err  error
	}{
		{"type 1 first", chunkHeader(1, 4, 0, 1, TypeAudio, 0), ErrChunk},
		{"too large", chunkHeader(0, 4, 0, 1001, TypeAudio, 1), ErrMessageTooLarge},
		{"chunk size 0", append(chunkHeader(0, 2, 0, 4, TypeSetChunkSize, 0), 0, 0, 0, 0), ErrChunk},
		{"chunk size short", append(chunkHeader(0, 2, 0, 2, TypeSetChunkSize, 0), 0, 1), ErrChunk},
		{"handler error", append(chunkHeader(0, 4, 0, 1, TypeAudio, 1), 1), stop},
	}
	for _, tc := range cases {
		c := serverConn(t, func(*Message) error { return stop }, nil)
		if err := c.Feed(tc.b); !errors.Is(err, tc.err) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	// Partial messages beyond the limit, across chunk streams.
	c := serverConn(t, nil, nil)
	var b []byte
	for csid := uint32(3); csid < 13; csid++ {
		b = append(b, chunkHeader(0, csid, 0, 900, TypeAudio, 1)...)
		b = append(b, make([]byte, 128)...)
	}
	if err := c.Feed(b); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatal(err)
	}
	// Too many chunk streams.
	c = serverConn(t, nil, nil)
	b = nil
	for csid := uint32(3); csid < 100; csid++ {
		b = append(b, chunkHeader(0, csid, 0, 1, TypeAudio, 1)...)
		b = append(b, 1)
	}
	if err := c.Feed(b); !errors.Is(err, ErrChunk) {
		t.Fatal(err)
	}
	// A closed Conn neither reads nor writes.
	c.Close()
	if err := c.Feed([]byte{1}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := c.WriteMessage(&Message{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := c.SetChunkSize(10); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := c.WriteCommand(0, "x", 0, struct{}{}); err == nil {
		t.Fatal("unencodable command")
	}
}

func TestFeedPieces(t *testing.T) {
	// Every split of the input gives the same messages.
	p := newPipe(t, true)
	_ = p.client.SetChunkSize(64)
	for i := range 5 {
		_ = p.client.WriteMessage(&Message{Type: TypeVideo, StreamID: 1, Timestamp: uint32(i) * 0x800000, Payload: bytes.Repeat([]byte{byte(i)}, 150)})
	}
	_ = p.client.WriteMessage(&Message{Type: TypeDataAMF0, ChunkStreamID: 500, Payload: []byte{1}})
	whole := p.toServer
	p.toServer = nil
	var ref []Message
	c := serverConn(t, func(m *Message) error { ref = append(ref, copyMsg(m)); return nil }, nil)
	if err := c.Feed(whole); err != nil {
		t.Fatal(err)
	}
	for split := 1; split < len(whole); split += 7 {
		var got []Message
		c := serverConn(t, func(m *Message) error { got = append(got, copyMsg(m)); return nil }, nil)
		if err := c.Feed(whole[:split]); err != nil {
			t.Fatal(err)
		}
		for i := split; i < len(whole); i++ {
			if err := c.Feed(whole[i : i+1]); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("split %d", split)
		}
	}
}

func TestCommands(t *testing.T) {
	p := newPipe(t, true)
	if err := p.client.WriteCommand(0, "connect", 1, amf.Object{{Key: "app", Value: "live"}}, "extra"); err != nil {
		t.Fatal(err)
	}
	p.pump(t)
	cmd, err := ParseCommand(&p.srvMsgs[0])
	if err != nil || cmd.Name != "connect" || cmd.TransactionID != 1 || len(cmd.Args) != 1 {
		t.Fatalf("%+v %v", cmd, err)
	}
	// AMF3 commands carry a format byte first.
	payload, _ := amf.Append([]byte{0}, "play", 0.0, nil, "s")
	cmd, err = ParseCommand(&Message{Type: TypeCommandAMF3, Payload: payload})
	if err != nil || cmd.Name != "play" || cmd.Args[0] != "s" {
		t.Fatalf("%+v %v", cmd, err)
	}
	two, _ := amf.Append(nil, "x", 1.0)
	if cmd, err := ParseCommand(&Message{Type: TypeCommandAMF0, Payload: two}); err != nil || cmd.Object != nil {
		t.Fatal(cmd, err)
	}
	for _, bad := range [][]any{{"x"}, {1.0, 1.0}, {"x", "y"}} {
		b, _ := amf.Append(nil, bad...)
		if _, err := ParseCommand(&Message{Type: TypeCommandAMF0, Payload: b}); err == nil {
			t.Errorf("%v parsed", bad)
		}
	}
	if _, err := ParseCommand(&Message{Type: TypeCommandAMF0, Payload: []byte{0x20}}); err == nil {
		t.Fatal("garbage parsed")
	}
	if b := DataPayload(&Message{Type: TypeDataAMF3, Payload: []byte{0, 5}}); !bytes.Equal(b, []byte{5}) {
		t.Fatal(b)
	}
	if b := DataPayload(&Message{Type: TypeDataAMF0, Payload: []byte{0, 5}}); len(b) != 2 {
		t.Fatal(b)
	}
	// The other writers.
	p.srvMsgs = nil
	_ = p.client.WritePeerBandwidth(100, 2)
	_ = p.client.WriteUserControl(EventStreamBegin, 1)
	p.pump(t)
	if len(p.srvMsgs) != 1 || p.srvMsgs[0].Type != TypeUserControl {
		t.Fatal(p.srvMsgs)
	}
	if defaultChunkStream(TypeAudio) != csidAudio || defaultChunkStream(TypeCommandAMF0) != csidCommand {
		t.Fatal("defaultChunkStream")
	}
}

func TestURL(t *testing.T) {
	u, err := ParseURL("rtmp://example.com/live/s?token=1")
	if err != nil || u.Host != "example.com:1935" || u.App != "live" || u.Stream != "s" || u.Query != "token=1" {
		t.Fatalf("%+v %v", u, err)
	}
	if u.TcURL() != "rtmp://example.com:1935/live" || u.StreamName() != "s?token=1" || u.Path() != "live/s" {
		t.Fatal(u.TcURL(), u.StreamName(), u.Path())
	}
	u, err = ParseURL("RTMPS://h:444/a/b/c")
	if err != nil || u.Host != "h:444" || u.Stream != "b/c" || u.StreamName() != "b/c" {
		t.Fatalf("%+v %v", u, err)
	}
	if u, _ := ParseURL("rtmps://h/a/b"); u.Host != "h:443" {
		t.Fatal(u.Host)
	}
	for _, bad := range []string{"http://h/a/b", "rtmp:///a/b", "rtmp://h/a", "rtmp://h//b", "rtmp://h/a/", "%zz"} {
		if _, err := ParseURL(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
	cases := []struct{ app, name, path, query string }{
		{"live", "s", "live/s", ""},
		{"live?vhost=x", "s?token=1", "live/s", "token=1&vhost=x"},
		{"live?vhost=x", "s", "live/s", "vhost=x"},
		{"/live/", "/s", "live/s", ""},
		{"", "s", "s", ""},
	}
	for _, c := range cases {
		if path, query := streamPath(c.app, c.name); path != c.path || query != c.query {
			t.Errorf("%q %q: %q %q", c.app, c.name, path, query)
		}
	}
}

func TestStatusCode(t *testing.T) {
	level, code, desc := statusCode(amf.ECMAArray(status("error", "c", "d")))
	if level != "error" || code != "c" || desc != "d" {
		t.Fatal(level, code, desc)
	}
	if l, _, _ := statusCode(nil); l != "" {
		t.Fatal(l)
	}
	e := &StatusError{Code: "c", Description: "d"}
	if !errors.Is(e, ErrRejected) || e.Error() == "" {
		t.Fatal(e)
	}
}
