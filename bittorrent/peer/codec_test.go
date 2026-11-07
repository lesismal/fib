package peer

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"
	"unsafe"
)

func TestHandshakeRoundTrip(t *testing.T) {
	var r Reserved
	r.SetExtended(true)
	r.SetFast(true)
	r.SetDHT(true)
	r.SetV2(true)
	if r != (Reserved{0, 0, 0, 0, 0, 0x10, 0, 0x15}) {
		t.Fatalf("reserved %x", r)
	}
	h := Handshake{Reserved: r, InfoHash: [20]byte{1, 2, 3}, PeerID: NewPeerID("")}
	b := AppendHandshake(nil, &h)
	if len(b) != HandshakeLen || b[0] != 19 || string(b[1:20]) != Protocol {
		t.Fatalf("encoded %q", b)
	}
	got, err := ParseHandshake(b)
	if err != nil || got != h {
		t.Fatalf("got %+v %v", got, err)
	}
	if !got.Reserved.Extended() || !got.Reserved.Fast() || !got.Reserved.DHT() || !got.Reserved.V2() {
		t.Fatal("bits lost")
	}
	r.SetFast(false)
	if r.Fast() || !r.DHT() {
		t.Fatal("SetFast(false)")
	}
	b[5] = 'x'
	if _, err := ParseHandshake(b); !errors.Is(err, ErrHandshake) {
		t.Fatalf("bad protocol: %v", err)
	}
	if _, err := ParseHandshake(b[:30]); err == nil {
		t.Fatal("short handshake")
	}
}

func TestPeerID(t *testing.T) {
	id := NewPeerID("")
	if string(id[:8]) != DefaultPeerIDPrefix {
		t.Fatalf("id %q", id)
	}
	c, v, ok := PeerIDClient(id)
	if !ok || c != "FB" || v != "0001" {
		t.Fatalf("client %q %q %v", c, v, ok)
	}
	if NewPeerID("") == id {
		t.Fatal("peer IDs repeat")
	}
	if _, _, ok := PeerIDClient([20]byte{'M', '4', '-'}); ok {
		t.Fatal("mainline style taken as Azureus")
	}
}

// allMessages has one message of every type.
func allMessages() []Message {
	root := [32]byte{9, 8, 7}
	return []Message{
		{Keepalive: true},
		{Type: MsgChoke}, {Type: MsgUnchoke}, {Type: MsgInterested}, {Type: MsgNotInterested},
		{Type: MsgHave, Index: 42},
		{Type: MsgBitfield, Data: []byte{0xff, 0x80}},
		{Type: MsgRequest, Index: 1, Begin: 16384, Length: 16384},
		{Type: MsgPiece, Index: 2, Begin: 32768, Data: bytes.Repeat([]byte{7}, 1000)},
		{Type: MsgCancel, Index: 1, Begin: 0, Length: 16384},
		{Type: MsgPort, Port: 6881},
		{Type: MsgSuggest, Index: 5}, {Type: MsgHaveAll}, {Type: MsgHaveNone},
		{Type: MsgRejectRequest, Index: 3, Begin: 4, Length: 5},
		{Type: MsgAllowedFast, Index: 6},
		{Type: MsgExtended, ExtendedID: 0, Data: []byte("d1:md11:ut_metadatai1eee")},
		{Type: MsgExtended, ExtendedID: 3, Data: nil},
		{Type: MsgHashRequest, PiecesRoot: root, BaseLayer: 0, Index: 0, Length: 512, ProofLayers: 3},
		{Type: MsgHashes, PiecesRoot: root, BaseLayer: 0, Index: 0, Length: 2, ProofLayers: 0, Data: make([]byte, 64)},
		{Type: MsgHashReject, PiecesRoot: root, Length: 512, ProofLayers: 1},
		{Type: 99, Data: []byte("unknown extension message")},
	}
}

func normalize(m Message) Message {
	if len(m.Data) == 0 {
		m.Data = nil
	}
	return m
}

func TestMessageRoundTrip(t *testing.T) {
	msgs := allMessages()
	var stream []byte
	for i := range msgs {
		before := len(stream)
		stream = AppendMessage(stream, &msgs[i])
		if len(stream)-before != EncodedLen(&msgs[i]) {
			t.Fatalf("%v: EncodedLen %d, appended %d", &msgs[i], EncodedLen(&msgs[i]), len(stream)-before)
		}
	}
	for _, maxChunk := range []int{1, 2, 3, 5, 17, 1000, len(stream)} {
		r := rand.New(rand.NewPCG(uint64(maxChunk), 1))
		var p Parser
		var got []Message
		for b := stream; len(b) > 0; {
			n := min(len(b), 1+r.IntN(maxChunk))
			err := p.Feed(b[:n], func(m *Message) error {
				c := *m
				c.Data = append([]byte(nil), m.Data...)
				got = append(got, c)
				return nil
			})
			if err != nil {
				t.Fatalf("chunk %d: %v", maxChunk, err)
			}
			b = b[n:]
		}
		if len(got) != len(msgs) {
			t.Fatalf("chunk %d: %d messages, want %d", maxChunk, len(got), len(msgs))
		}
		for i := range msgs {
			if !reflect.DeepEqual(normalize(got[i]), normalize(msgs[i])) {
				t.Fatalf("chunk %d message %d: got %+v want %+v", maxChunk, i, got[i], msgs[i])
			}
		}
		if p.Buffered() != 0 {
			t.Fatalf("buffered %d", p.Buffered())
		}
	}
}

func TestParserZeroCopy(t *testing.T) {
	block := bytes.Repeat([]byte{1}, BlockSize)
	stream := AppendMessage(nil, &Message{Type: MsgPiece, Index: 1, Begin: 0, Data: block})
	var p Parser
	var aliased bool
	_ = p.Feed(stream, func(m *Message) error {
		start := uintptr(unsafe.Pointer(&stream[0]))
		d := uintptr(unsafe.Pointer(&m.Data[0]))
		aliased = d >= start && d < start+uintptr(len(stream))
		return nil
	})
	if !aliased {
		t.Fatal("a whole piece message was copied")
	}
}

func TestParserErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []byte
	}{
		{"have short", []byte{0, 0, 0, 2, 4, 1}},
		{"choke long", []byte{0, 0, 0, 2, 0, 0}},
		{"request short", []byte{0, 0, 0, 5, 6, 0, 0, 0, 1}},
		{"piece short", []byte{0, 0, 0, 5, 7, 0, 0, 0, 1}},
		{"port long", []byte{0, 0, 0, 4, 9, 0, 0, 0}},
		{"extended empty", []byte{0, 0, 0, 1, 20}},
		{"hashes ragged", append([]byte{0, 0, 0, 50, 22}, make([]byte, 49)...)},
		{"too long", []byte{0, 0x20, 0, 0, 7}},
	} {
		var p Parser
		err := p.Feed(tc.in, func(*Message) error { return nil })
		var me *MessageError
		if !errors.As(err, &me) {
			t.Errorf("%s: %v", tc.name, err)
		}
		if err := p.Feed([]byte{0, 0, 0, 0}, func(*Message) error { return nil }); !errors.Is(err, ErrParserFailed) {
			t.Errorf("%s: after failure: %v", tc.name, err)
		}
	}
	p := Parser{MaxLength: 100}
	if err := p.Feed([]byte{0, 0, 0, 101}, nil); err == nil {
		t.Fatal("MaxLength not applied")
	}
	stop := errors.New("stop")
	var q Parser
	stream := AppendMessage(AppendMessage(nil, &Message{Type: MsgChoke}), &Message{Type: MsgUnchoke})
	n := 0
	if err := q.Feed(stream, func(*Message) error { n++; return stop }); err != stop || n != 1 {
		t.Fatalf("callback error: %v after %d", err, n)
	}
}

func TestAppendPieceHeader(t *testing.T) {
	block := []byte("abcdef")
	a := append(AppendPieceHeader(nil, 3, 9, len(block)), block...)
	b := AppendMessage(nil, &Message{Type: MsgPiece, Index: 3, Begin: 9, Data: block})
	if !bytes.Equal(a, b) {
		t.Fatalf("%x != %x", a, b)
	}
}

func TestBitfield(t *testing.T) {
	f := NewBitfield(11)
	if f.Len() != 11 || len(f.Bytes()) != 2 || !f.None() {
		t.Fatal("new")
	}
	f.Set(0)
	f.Set(10)
	f.Set(11) // out of range: ignored
	if !f.Has(0) || !f.Has(10) || f.Has(1) || f.Has(11) || f.Count() != 2 {
		t.Fatalf("%08b", f.Bytes())
	}
	if !bytes.Equal(f.Bytes(), []byte{0x80, 0x20}) {
		t.Fatalf("%08b", f.Bytes())
	}
	g, err := ParseBitfield(f.Bytes(), 11)
	if err != nil || !g.Has(10) {
		t.Fatal(err)
	}
	if _, err := ParseBitfield([]byte{0x80, 0x10}, 11); !errors.Is(err, ErrBitfield) {
		t.Fatal("spare bit accepted")
	}
	if _, err := ParseBitfield([]byte{0x80}, 11); !errors.Is(err, ErrBitfield) {
		t.Fatal("short accepted")
	}
	h := NewBitfield(11)
	h.SetAll()
	if !h.All() || h.Count() != 11 || h.Bytes()[1] != 0xe0 {
		t.Fatalf("SetAll %08b", h.Bytes())
	}
	if got := f.Missing(nil, h); !slices.Equal(got, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("missing %v", got)
	}
	f.Clear(0)
	if f.Has(0) || f.Clone().Count() != 1 {
		t.Fatal("clear")
	}
	h.ClearAll()
	if !h.None() {
		t.Fatal("ClearAll")
	}
}

// TestAllowedFastSet checks the example of BEP 6.
func TestAllowedFastSet(t *testing.T) {
	var ih [20]byte
	for i := range ih {
		ih[i] = 0xaa
	}
	addr := netip.MustParseAddr("80.4.4.200")
	if got := AllowedFastSet(addr, ih, 1313, 7); !slices.Equal(got, []uint32{1059, 431, 808, 1217, 287, 376, 1188}) {
		t.Fatalf("k=7: %v", got)
	}
	if got := AllowedFastSet(addr, ih, 1313, 9); !slices.Equal(got, []uint32{1059, 431, 808, 1217, 287, 376, 1188, 353, 508}) {
		t.Fatalf("k=9: %v", got)
	}
	// The /24 is what counts.
	if got := AllowedFastSet(netip.MustParseAddr("80.4.4.1"), ih, 1313, 7); got[0] != 1059 {
		t.Fatal("not masked to /24")
	}
	if got := AllowedFastSet(addr, ih, 3, 10); len(got) != 3 {
		t.Fatalf("k > pieces: %v", got)
	}
	v6 := AllowedFastSet(netip.MustParseAddr("2001:db8:1::1"), ih, 1000, 5)
	if !slices.Equal(v6, AllowedFastSet(netip.MustParseAddr("2001:db8:1:ffff::2"), ih, 1000, 5)) {
		t.Fatal("IPv6 not masked to /48")
	}
}

func TestExtendedHandshake(t *testing.T) {
	h := &ExtendedHandshake{
		M:    map[string]int{ExtMetadata: 3, ExtPex: 0},
		Port: 6881, Version: "fib 1", YourIP: CompactAddr(netip.MustParseAddr("10.0.0.1")),
		Reqq: 500, MetadataSize: 31235, UploadOnly: 1,
	}
	b, err := h.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseExtendedHandshake(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, h) {
		t.Fatalf("got %+v", got)
	}
	if got.ID(ExtMetadata) != 3 || got.ID(ExtPex) != 0 || got.ID("x") != 0 {
		t.Fatal("ID")
	}
	if got.YourAddr() != netip.MustParseAddr("10.0.0.1") {
		t.Fatal("YourAddr")
	}
	b, _ = (&ExtendedHandshake{}).Marshal()
	if string(b) != "d1:mdee" {
		t.Fatalf("empty: %s", b)
	}
	// Unknown keys and types are skipped.
	if _, err := ParseExtendedHandshake([]byte("d1:md6:ut_pexi1ee1:xli1eee")); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataMessage(t *testing.T) {
	for _, m := range []MetadataMessage{
		{Type: MetadataRequest, Piece: 0},
		{Type: MetadataReject, Piece: 3},
		{Type: MetadataData, Piece: 1, TotalSize: 20000, Data: []byte("the piece's bytes")},
	} {
		b := AppendMetadataMessage(nil, &m)
		got, err := ParseMetadataMessage(b)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(normalizeMeta(*got), normalizeMeta(m)) {
			t.Fatalf("got %+v want %+v", got, m)
		}
	}
	if b := AppendMetadataMessage(nil, &MetadataMessage{}); string(b) != "d8:msg_typei0e5:piecei0ee" {
		t.Fatalf("request: %s", b)
	}
	for _, bad := range []string{"d8:msg_typei5e5:piecei0ee", "d8:msg_typei0e5:piecei-1ee", "d8:msg_typei0e5:piecei0eexx", "garbage"} {
		if _, err := ParseMetadataMessage([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func normalizeMeta(m MetadataMessage) MetadataMessage {
	if len(m.Data) == 0 {
		m.Data = nil
	}
	return m
}

func TestPex(t *testing.T) {
	p := &Pex{
		Added: []PexPeer{
			{Addr: netip.MustParseAddrPort("1.2.3.4:6881"), Flags: PexEncryption | PexSeed},
			{Addr: netip.MustParseAddrPort("[2001:db8::1]:51413"), Flags: PexUTP},
		},
		Dropped: []netip.AddrPort{netip.MustParseAddrPort("5.6.7.8:1"), netip.MustParseAddrPort("[::1]:2")},
	}
	b, err := p.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePex(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, p) {
		t.Fatalf("got %+v", got)
	}
	if _, err := ParsePex([]byte("d5:added5:12345e")); !errors.Is(err, ErrPex) {
		t.Fatalf("ragged: %v", err)
	}
	// Flags may be missing.
	got, err = ParsePex([]byte("d5:added6:\x01\x02\x03\x04\x00\x50e"))
	if err != nil || len(got.Added) != 1 || got.Added[0].Flags != 0 || got.Added[0].Addr.Port() != 80 {
		t.Fatalf("no flags: %+v %v", got, err)
	}
}

func TestPexState(t *testing.T) {
	var s PexState
	now := time.Unix(1000, 0)
	var peers []PexPeer
	for i := range 70 {
		peers = append(peers, PexPeer{Addr: netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, 0, byte(i)}), 1)})
	}
	p, ok := s.Next(now, peers)
	if !ok || len(p.Added) != PexMaxPeers {
		t.Fatalf("first: %v %d", ok, len(p.Added))
	}
	if _, ok := s.Next(now.Add(30*time.Second), peers); ok {
		t.Fatal("sent within the interval")
	}
	now = now.Add(PexInterval)
	p, ok = s.Next(now, peers[10:])
	if !ok || len(p.Added) != 20 || len(p.Dropped) != 10 {
		t.Fatalf("second: %v %d %d", ok, len(p.Added), len(p.Dropped))
	}
	now = now.Add(PexInterval)
	if _, ok := s.Next(now, peers[10:]); ok {
		t.Fatal("sent with nothing changed")
	}

	big := &Pex{Added: peers}
	if !s.Received(now, big) || len(big.Added) != PexMaxPeers {
		t.Fatalf("received: %d", len(big.Added))
	}
	if s.Received(now.Add(10*time.Second), &Pex{}) {
		t.Fatal("heeded a message 10s after the last")
	}
	if !s.Received(now.Add(PexInterval), &Pex{}) {
		t.Fatal("ignored a message a minute after the last")
	}
}

func FuzzParser(f *testing.F) {
	var stream []byte
	for _, m := range allMessages() {
		stream = AppendMessage(stream, &m)
	}
	f.Add(stream, uint8(7))
	f.Add([]byte{0, 0, 0, 5, 7, 0, 0, 0}, uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		p := Parser{MaxLength: 1 << 16}
		var whole []Message
		_ = p.Feed(data, func(m *Message) error {
			c := *m
			c.Data = append([]byte(nil), m.Data...)
			whole = append(whole, c)
			// What parses re-encodes to the same bytes.
			b := AppendMessage(nil, m)
			var q Parser
			n := 0
			if err := q.Feed(b, func(m2 *Message) error {
				n++
				if !reflect.DeepEqual(normalize(*m2), normalize(c)) {
					t.Fatalf("re-parse %+v != %+v", m2, c)
				}
				return nil
			}); err != nil || n != 1 {
				t.Fatalf("re-parse: %v, %d messages", err, n)
			}
			return nil
		})
		// Fed in chunks, the same messages come out.
		q := Parser{MaxLength: 1 << 16}
		var parts []Message
		step := int(chunk%32) + 1
		for b := data; len(b) > 0; {
			n := min(step, len(b))
			if q.Feed(b[:n], func(m *Message) error {
				c := *m
				c.Data = append([]byte(nil), m.Data...)
				parts = append(parts, c)
				return nil
			}) != nil {
				break
			}
			b = b[n:]
		}
		if len(parts) != len(whole) {
			t.Fatalf("chunked %d messages, whole %d", len(parts), len(whole))
		}
	})
}

func FuzzHandshake(f *testing.F) {
	f.Add(AppendHandshake(nil, &Handshake{PeerID: NewPeerID("")}))
	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := ParseHandshake(data)
		if err != nil {
			return
		}
		if b := AppendHandshake(nil, &h); !bytes.Equal(b, data[:HandshakeLen]) {
			t.Fatalf("re-encoded %x, was %x", b, data[:HandshakeLen])
		}
	})
}

func FuzzExtensionMessages(f *testing.F) {
	f.Add([]byte("d1:md11:ut_metadatai1ee13:metadata_sizei100ee"))
	f.Add([]byte("d8:msg_typei1e5:piecei0e10:total_sizei3eeabc"))
	f.Add([]byte("d5:added6:\x01\x02\x03\x04\x00\x507:added.f1:\x01e"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ParseExtendedHandshake(data)
		if m, err := ParseMetadataMessage(data); err == nil && m.Type == MetadataData {
			if a, err := NewMetadataAssembler(m.TotalSize, 1<<20); err == nil {
				_ = a.Add(m)
			}
		}
		if p, err := ParsePex(data); err == nil {
			var s PexState
			s.Received(time.Now(), p)
			if _, err := p.Marshal(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
