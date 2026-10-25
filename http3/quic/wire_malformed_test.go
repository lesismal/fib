package quic

import (
	"bytes"
	"crypto/tls"
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// longHeader is the start of a long header packet: first byte, version and
// connection IDs.
func longHeader(first byte, version uint32, dcid, scid []byte) []byte {
	b := []byte{first, byte(version >> 24), byte(version >> 16), byte(version >> 8), byte(version)}
	b = append(b, byte(len(dcid)))
	b = append(b, dcid...)
	b = append(b, byte(len(scid)))
	return append(b, scid...)
}

// Whatever arrives is read as a header only as far as it is one: a header
// cut short, with a connection ID longer than version 1 allows, without the
// fixed bit, or with a token or length running past the datagram is
// malformed, and a version other than 1 is told apart from that.
func TestParseHeaderMalformed(t *testing.T) {
	cid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	long21 := bytes.Repeat([]byte{9}, 21)
	cases := []struct {
		name string
		b    []byte
		want error
	}{
		{"empty", nil, errBadPacket},
		{"short header shorter than its connection ID", []byte{0x40, 1, 2}, errBadPacket},
		{"long header cut short", []byte{0xc0, 0, 0, 0, 1, 0}, errBadPacket},
		{"destination connection ID too long", longHeader(0xc0, Version1, long21, nil), errBadPacket},
		{"source connection ID too long", longHeader(0xc0, Version1, cid, long21), errBadPacket},
		{"source connection ID cut short", longHeader(0xc0, Version1, cid, cid)[:16], errBadPacket},
		{"fixed bit clear", append(longHeader(0x80, Version1, cid, cid), 0, 0), errBadPacket},
		{"token past the end", append(longHeader(0xc0, Version1, cid, cid), 5, 1), errBadPacket},
		{"no length", longHeader(0xe0, Version1, cid, cid), errBadPacket},
		{"length past the end", append(longHeader(0xe0, Version1, cid, cid), 5, 1), errBadPacket},
		{"another version", append(longHeader(0xc0, 0x6b3343cf, long21, cid), 0), errUnknownVersion},
		{"version negotiation", append(longHeader(0x80, 0, cid, cid), 0, 0, 0, 1), errUnknownVersion},
		{"retry", append(longHeader(0xf0, Version1, cid, cid), "token and tag"...), nil},
		{"handshake", append(longHeader(0xe0, Version1, cid, cid), 2, 0xaa, 0xbb), nil},
	}
	for _, tc := range cases {
		h, err := parseHeader(tc.b, cidLen)
		if err != tc.want {
			t.Fatalf("%s: %v, want %v", tc.name, err, tc.want)
		}
		if err == nil && (h.end > len(tc.b) || h.pnOffset > h.end) {
			t.Fatalf("%s: packet number at %d, end at %d of %d", tc.name, h.pnOffset, h.end, len(tc.b))
		}
	}
	if h, _ := parseHeader(append(longHeader(0xf0, Version1, cid, cid), "tok"...), cidLen); h.typ != packetRetry || string(h.token) != "tok" {
		t.Fatalf("retry read as type %d with token %q", h.typ, h.token)
	}
}

// A datagram's destination connection ID is found by its header's form, and
// nothing is found in one too short to carry it.
func TestDatagramDCID(t *testing.T) {
	cid := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	cases := []struct {
		name string
		b    []byte
		want []byte
	}{
		{"empty", nil, nil},
		{"short header", append([]byte{0x40}, cid...), cid},
		{"short header cut short", []byte{0x40, 1, 2}, nil},
		{"long header", longHeader(0xc0, Version1, cid[:4], cid), cid[:4]},
		{"long header cut short", []byte{0xc0, 0, 0, 0, 1}, nil},
		{"connection ID cut short", []byte{0xc0, 0, 0, 0, 1, 8, 1, 2}, nil},
	}
	for _, tc := range cases {
		if got := DatagramDCID(tc.b); !bytes.Equal(got, tc.want) {
			t.Fatalf("%s: %x, want %x", tc.name, got, tc.want)
		}
	}
}

// Packet numbers are sent in as few bytes as tell them apart from those
// not yet acknowledged, and read back from them whichever side of the
// expected number they fall (RFC 9000 appendix A).
func TestPacketNumberEncoding(t *testing.T) {
	for _, tc := range []struct {
		pn      uint64
		largest int64
		want    int
	}{
		{0, -1, 1}, {200, 100, 1}, {1 << 10, -1, 2}, {1 << 20, -1, 3}, {1 << 30, -1, 4}, {1<<30 + 5, 1 << 30, 1},
	} {
		if got := pnLen(tc.pn, tc.largest); got != tc.want {
			t.Fatalf("pnLen(%d, %d) = %d, want %d", tc.pn, tc.largest, got, tc.want)
		}
	}
	for _, tc := range []struct {
		largest   int64
		truncated uint64
		n         int
		want      uint64
	}{
		{0xa82f30ea, 0x9b32, 2, 0xa82f9b32},
		// Just below the expected number, across the window's edge.
		{0x1ff, 0xff, 1, 0x1ff},
		// Just above it, across the edge the other way.
		{0x2fe, 0x01, 1, 0x301},
	} {
		if got := decodePN(tc.largest, tc.truncated, tc.n); got != tc.want {
			t.Fatalf("decodePN(%x, %x, %d) = %x, want %x", tc.largest, tc.truncated, tc.n, got, tc.want)
		}
	}
	// A packet too short to sample for header protection is not one.
	_, rx := initialKeys([]byte{1, 2, 3, 4, 5, 6, 7, 8}, true)
	if _, _, ok := unprotect(make([]byte, 20), 9, rx.hp, -1); ok {
		t.Fatal("unprotected a packet too short to sample")
	}
}

// paramBlob builds transport parameters from id and value pairs.
func paramBlob(pairs ...any) []byte {
	var b []byte
	for i := 0; i < len(pairs); i += 2 {
		id := uint64(pairs[i].(int))
		switch v := pairs[i+1].(type) {
		case int:
			b = appendParamVarint(b, id, uint64(v))
		case uint64:
			b = appendParamVarint(b, id, v)
		case []byte:
			b = appendParamBytes(b, id, v)
		}
	}
	return b
}

// The peer's transport parameters are checked as RFC 9000 section 18.2
// requires: each at most once, each value in its range and of its size,
// and those only a server may send not from a client.
func TestParseParamsInvalid(t *testing.T) {
	cases := []struct {
		name     string
		b        []byte
		isClient bool
	}{
		{"truncated", []byte{paramMaxIdleTimeout}, false},
		{"duplicate", paramBlob(paramMaxIdleTimeout, 10, paramMaxIdleTimeout, 10), false},
		{"integer with bytes after it", paramBlob(paramMaxIdleTimeout, []byte{0x05, 0x00}), false},
		{"reset token of the wrong size", paramBlob(paramStatelessResetToken, []byte{1, 2, 3}), true},
		{"datagram size below 1200", paramBlob(paramMaxUDPPayloadSize, 1199), false},
		{"bidirectional streams above 2^60", paramBlob(paramInitialMaxStreamsBidi, uint64(1<<60+1)), false},
		{"unidirectional streams above 2^60", paramBlob(paramInitialMaxStreamsUni, uint64(1<<60+1)), false},
		{"ack delay exponent above 20", paramBlob(paramAckDelayExponent, 21), false},
		{"max ack delay of 2^14 ms", paramBlob(paramMaxAckDelay, 1<<14), false},
		{"disable_active_migration with a value", paramBlob(paramDisableMigration, []byte{1}), false},
		{"connection ID limit below 2", paramBlob(paramActiveCIDLimit, 1), false},
		{"preferred_address from a client", paramBlob(paramPreferredAddress, []byte{1, 2, 3}), false},
		{"original_destination_connection_id from a client", paramBlob(paramOriginalDCID, []byte{1}), false},
		{"retry_source_connection_id from a client", paramBlob(paramRetrySCID, []byte{1}), false},
	}
	for _, tc := range cases {
		_, err := parseParams(tc.b, tc.isClient)
		if te, ok := err.(*TransportError); !ok || te.Code != errTransportParameter {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
	// A server's preferred address is allowed, and ignored.
	if _, err := parseParams(paramBlob(paramPreferredAddress, []byte{1, 2, 3}), true); err != nil {
		t.Fatalf("preferred_address from a server: %v", err)
	}
}

// What a server sends after a Retry names the connection ID the Retry
// chose, and the client reads it back.
func TestParamsRetrySCIDRoundTrip(t *testing.T) {
	p := defaultParams()
	p.retrySCID, p.hasRetrySCID = []byte("retry-id"), true
	got, err := parseParams(p.encode(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !got.hasRetrySCID || string(got.retrySCID) != "retry-id" {
		t.Fatalf("retry_source_connection_id %q", got.retrySCID)
	}
}

// Limits remembered with a session ticket that cannot be read allow no
// early data, on either side.
func TestRememberedLimitsUnreadable(t *testing.T) {
	c := frameConn(t)
	for _, extra := range [][][]byte{nil, {{2}}, {{1, 0x01}}} {
		if c.earlyAllowed(extra) {
			t.Fatalf("server allowed early data on %x", extra)
		}
		if _, ok := parseRemembered(extra); ok {
			t.Fatalf("client remembered limits from %x", extra)
		}
	}
	if !c.earlyAllowed([][]byte{c.rememberedLimits()}) {
		t.Fatal("the server's own limits are not allowed")
	}
}

// Packets are protected with each cipher suite TLS 1.3 may agree; one it
// does not have is refused, not guessed at.
func TestPacketProtectionSuites(t *testing.T) {
	for _, suite := range []uint16{tls.TLS_AES_128_GCM_SHA256, tls.TLS_AES_256_GCM_SHA384, tls.TLS_CHACHA20_POLY1305_SHA256} {
		secret := bytes.Repeat([]byte{byte(suite)}, suiteHash(suite)().Size())
		tx, err := newKeys(suite, secret)
		if err != nil {
			t.Fatalf("suite %x: %v", suite, err)
		}
		rx, _ := newKeys(suite, secret)
		for _, k := range []*keys{tx, rx} {
			if k.aead.NonceSize() != 12 || k.aead.Overhead() != aeadOverhead {
				t.Fatalf("suite %x: nonce %d, overhead %d", suite, k.aead.NonceSize(), k.aead.Overhead())
			}
		}
		// A short header with a one-byte packet number, and room for the tag.
		pkt := append(append([]byte{0x40}, make([]byte, cidLen)...), 7)
		pkt = append(pkt, "payload of a protected packet"...)
		n := len(pkt)
		pkt = append(pkt, make([]byte, aeadOverhead)...)[:n]
		sealed := tx.seal(pkt, 1+cidLen, 1, 7)
		pn, n, ok := unprotect(sealed, 1+cidLen, rx.hp, 6)
		if !ok || pn != 7 {
			t.Fatalf("suite %x: packet number %d", suite, pn)
		}
		payload, err := rx.open(sealed, 1+cidLen+n, pn)
		if err != nil || string(payload) != "payload of a protected packet" {
			t.Fatalf("suite %x: %q, %v", suite, payload, err)
		}
	}
	if _, err := newKeys(0x1399, make([]byte, 32)); err == nil {
		t.Fatal("keys for an unknown suite")
	}
	if _, err := newChaCha20Poly1305(make([]byte, 16)); err == nil {
		t.Fatal("ChaCha20-Poly1305 with a 16-byte key")
	}
	a, _ := newChaCha20Poly1305(make([]byte, chachaKeySize))
	if _, err := a.Open(nil, make([]byte, 12), make([]byte, 15), nil); err == nil {
		t.Fatal("opened a ciphertext shorter than its tag")
	}
	// ChaCha20-Poly1305 has limits of its own (RFC 9001 section 6.6).
	if confidentialityLimit(tls.TLS_CHACHA20_POLY1305_SHA256) != 1<<62 || integrityLimit(tls.TLS_CHACHA20_POLY1305_SHA256) != 1<<36 {
		t.Fatal("ChaCha20-Poly1305 limits")
	}
}

// Received packet numbers are kept as ranges: one just below a range
// extends it down, and once the most ranges are kept, one older than all
// of them is too old to tell from a repeat.
func TestRangeSetEdges(t *testing.T) {
	var s rangeSet
	s.add(9)
	s.add(8)
	if !slices.Equal(s, rangeSet{{8, 9}}) {
		t.Fatalf("got %v", s)
	}
	s = nil
	for i := 0; i < maxAckRanges; i++ {
		s.add(uint64(10 + 2*i))
	}
	if s.add(0) {
		t.Fatal("a packet older than every range kept was taken")
	}
	if len(s) != maxAckRanges || s[0].lo != 10 {
		t.Fatalf("%d ranges from %d", len(s), s[0].lo)
	}
}

// The buffers ignore what adds nothing: an empty span, data already
// delivered or of no length, and a lost range asked for with no room.
func TestBuffersIgnoreNothing(t *testing.T) {
	spans := addSpan(nil, span{5, 5})
	if len(spans) != 0 {
		t.Fatalf("spans %v", spans)
	}
	var r recvBuffer
	r.take(0, []byte("abc"))
	r.push(1, []byte("b"))
	r.push(3, nil)
	if len(r.segments) != 0 || r.buffered != 0 {
		t.Fatalf("%d segments of %d bytes", len(r.segments), r.buffered)
	}
	var b sendBuffer
	b.write([]byte("0123"))
	b.popNew(4)
	b.onLost(0, 4)
	if off, data := b.popLost(0); off != 0 || data != nil {
		t.Fatalf("%d %q with no room", off, data)
	}
	// Owned data adopted after other data is kept behind it as it is, and
	// both go once the peer has acknowledged them.
	owned := bufferpool.Get(4)
	copy(owned, "4567")
	b.adopt(owned)
	if off, data := b.popNew(4); off != 4 || string(data) != "4567" {
		t.Fatalf("%d %q after the data written", off, data)
	}
	b.onAck(0, 8)
	if !b.allAcked() || len(b.pieces) != 0 {
		t.Fatalf("%d pieces of %d bytes after the acknowledgement", len(b.pieces), b.size)
	}
}

// pathBudget allows nothing more once three times what arrived has gone.
func TestPathBudgetSpent(t *testing.T) {
	if got := pathBudget(100, 300); got != 0 {
		t.Fatalf("budget %d", got)
	}
	if got := pathBudget(100, 100); got != 200 {
		t.Fatalf("budget %d", got)
	}
}

// A packet that does not fit what was set aside for it is still sealed,
// into memory of its own.
func TestSealPlanGrows(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &c.spaces[spaceApp]
	headerLen := c.headerLen(spaceApp, 1, false)
	payload := []byte{framePing, 0, 0, 0}
	// Room for the packet but not its tag.
	out := make([]byte, headerLen, headerLen+len(payload)+aeadOverhead-1)
	out = append(out, payload...)
	plan := packetPlan{space: spaceApp, pn: s.nextPN, pnLen: 1, payload: out[headerLen:], sp: &sentPacket{}}
	if out = c.sealPlan(out, &plan); len(out) != c.packetSize(&plan) {
		t.Fatalf("sealed %d bytes, want %d", len(out), c.packetSize(&plan))
	}
	// Without peer parameters the peer is taken to hold acknowledgements
	// back as long as RFC 9000 allows by default.
	c.peerParams = nil
	if d := c.peerMaxAckDelay(); d != 25*time.Millisecond {
		t.Fatalf("max ack delay %v", d)
	}
}
