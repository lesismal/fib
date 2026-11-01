package srt

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"
)

func TestSeq(t *testing.T) {
	if seqAdd(maxSeq-1, 1) != 0 || seqAdd(0, -1) != maxSeq-1 {
		t.Fatal("seqAdd")
	}
	if seqDiff(0, maxSeq-1) != 1 || seqDiff(maxSeq-1, 0) != -1 || seqDiff(10, 3) != 7 {
		t.Fatal("seqDiff")
	}
	if socketIDOf(0) != 1 || socketIDOf(1<<30) != 1 || socketIDOf(5) != 5 {
		t.Fatal("socketIDOf")
	}
}

// TestHandshakeCodec round-trips a conclusion with every extension.
func TestHandshakeCodec(t *testing.T) {
	hs := handshake{
		version: 5, encryption: 4, extField: 7, isn: 12345, mtu: 1500, flowWindow: 8192,
		typ: hsConclusion, socketID: 99, cookie: 0xdeadbeef,
		hsReq:    &hsExt{version: srtVersion, flags: 0x3f, rcvDelay: 120, sndDelay: 200},
		km:       []byte{1, 2, 3, 4, 5}, // padded on the wire
		streamID: "#!::r=live/stream,m=publish", hasSID: true,
	}
	b := hs.append(nil)
	var got handshake
	if err := got.unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if got.streamID != hs.streamID || *got.hsReq != *hs.hsReq || got.hsRsp || !bytes.Equal(got.km, []byte{1, 2, 3, 4, 5, 0, 0, 0}) ||
		got.cookie != hs.cookie || got.isn != hs.isn || got.encryption != 4 {
		t.Fatalf("%+v", got)
	}
	// libsrt's SID byte order: "abcd" travels as "dcba".
	if s := string(swapWords([]byte("abcdef"))); s != "dcba\x00\x00fe" {
		t.Fatalf("%q", s)
	}
	for _, bad := range [][]byte{
		b[:40],
		append(append([]byte(nil), b[:48]...), 0, 1, 0, 9),             // extension past the end
		append(append([]byte(nil), b[:48]...), 0, 1, 0, 1, 0, 0, 0, 0), // HSREQ too short
		append(append([]byte(nil), b[:48]...), append([]byte{0, 5, 0, 129}, make([]byte, 516)...)...),
	} {
		if got.unmarshal(bad) == nil {
			t.Fatalf("% x accepted", bad[40:])
		}
	}
	var h header
	if h.unmarshal([]byte{1}) == nil {
		t.Fatal("short header")
	}
}

// TestKeyWrap checks RFC 3394's test vectors of section 4.1, 4.3 and 4.6.
func TestKeyWrap(t *testing.T) {
	cases := []struct{ kek, key, wrapped string }{
		{"000102030405060708090A0B0C0D0E0F", "00112233445566778899AABBCCDDEEFF", "1FA68B0A8112B447AEF34BD8FB5A7B829D3E862371D2CFE5"},
		{"000102030405060708090A0B0C0D0E0F1011121314151617", "00112233445566778899AABBCCDDEEFF", "96778B25AE6CA435F92B5B97C050AED2468AB8A17AD84E5D"},
		{"000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F", "00112233445566778899AABBCCDDEEFF000102030405060708090A0B0C0D0E0F", "28C9F404C4B810F4CBCCB35CFB87F8263F5786E2D80ED326CBC7F0E71A99F43BFB988B9B7A02DD21"},
	}
	for _, tc := range cases {
		kek, _ := hex.DecodeString(tc.kek)
		key, _ := hex.DecodeString(tc.key)
		want, _ := hex.DecodeString(tc.wrapped)
		got, err := keyWrap(kek, key)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("wrap %X, %v", got, err)
		}
		back, err := keyUnwrap(kek, got)
		if err != nil || !bytes.Equal(back, key) {
			t.Fatalf("unwrap %X, %v", back, err)
		}
		got[0] ^= 1
		if _, err := keyUnwrap(kek, got); err != errBadSecret {
			t.Fatal(err)
		}
	}
	if _, err := keyWrap(make([]byte, 5), make([]byte, 16)); err == nil {
		t.Fatal("bad KEK")
	}
	if _, err := keyUnwrap(make([]byte, 5), make([]byte, 24)); err == nil {
		t.Fatal("bad KEK")
	}
	if _, err := keyUnwrap(make([]byte, 16), make([]byte, 20)); err != errUnwrapShort {
		t.Fatal(err)
	}
}

// TestKM round-trips keying material with one and two keys, and refuses
// malformed messages.
func TestKM(t *testing.T) {
	const pass = "0123456789abc"
	km, err := newKeyMaterial(newRand(7), 24)
	if err != nil {
		t.Fatal(err)
	}
	km.odd = bytes.Repeat([]byte{3}, 24)
	b, err := km.marshal(pass)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != kmHeaderSize+kmSaltSize+8+48 || b[3] != 3 || b[15] != 6 {
		t.Fatalf("% x", b[:16])
	}
	got, err := unmarshalKM(b, pass)
	if err != nil || !bytes.Equal(got.even, km.even) || !bytes.Equal(got.odd, km.odd) || got.salt != km.salt {
		t.Fatal(err)
	}
	if _, err := unmarshalKM(b, "another passphrase"); err != errBadSecret {
		t.Fatal(err)
	}
	for i, mutate := range []func(b []byte) []byte{
		func(b []byte) []byte { return b[:20] },
		func(b []byte) []byte { b[0] = 0x13; return b },
		func(b []byte) []byte { b[3] = 0; return b },
		func(b []byte) []byte { b[8] = 3; return b },
		func(b []byte) []byte { b[15] = 5; return b },
		func(b []byte) []byte { return b[:len(b)-8] },
	} {
		c := mutate(append([]byte(nil), b...))
		if _, err := unmarshalKM(c, pass); err != errBadKM {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	if _, err := newKeyMaterial(newRand(1), 20); err != errKeyLen {
		t.Fatal(err)
	}
	if _, err := newKeyMaterial(newRandFail(), 16); err == nil {
		t.Fatal("no randomness for the salt")
	}
	if _, err := newKeyMaterial(newRandFailAfter(16), 16); err == nil {
		t.Fatal("no randomness for the key")
	}
	if _, err := (&keyMaterial{keyLen: 20}).marshal(pass); err == nil {
		t.Fatal("marshal of a bad key length")
	}
	if _, err := newCryptor(&keyMaterial{even: make([]byte, 5)}); err == nil {
		t.Fatal("bad even key")
	}
	if _, err := newCryptor(&keyMaterial{odd: make([]byte, 5)}); err == nil {
		t.Fatal("bad odd key")
	}
	c, _ := newCryptor(&keyMaterial{even: make([]byte, 16)})
	if c.xor(kkOdd, 1, make([]byte, 4)) {
		t.Fatal("odd key absent")
	}
}

// TestCTRVector pins the IV derivation: AES-CTR keystream of a known key,
// salt and sequence number, as libsrt computes it.
func TestCTRVector(t *testing.T) {
	km := &keyMaterial{even: make([]byte, 16), keyLen: 16}
	for i := range km.salt {
		km.salt[i] = byte(i)
	}
	c, _ := newCryptor(km)
	b := make([]byte, 16)
	c.xor(kkEven, 0x01020304, b)
	// AES-128 with a zero key of the block 00 01 .. 09 0a^01 0b^02 0c^03
	// 0d^04 00 00 (checked with openssl enc -aes-128-ecb).
	if got := hex.EncodeToString(b); got != "1b5a278308cb9e43c17204d764f64f80" {
		t.Fatalf("keystream %s", got)
	}
	d := append([]byte(nil), b...)
	c.xor(kkEven, 0x01020304, d)
	if !bytes.Equal(d, make([]byte, 16)) {
		t.Fatal("CTR does not invert")
	}
}

func TestParseStreamID(t *testing.T) {
	cases := []struct {
		in   string
		want StreamID
		err  bool
	}{
		{in: "live/s", want: StreamID{Resource: "live/s"}},
		{in: "/live/s?token=x", want: StreamID{Resource: "live/s", Query: "token=x"}},
		{in: "#!::r=live/s,m=publish,u=alice,s=1,h=example.com,t=stream,x=y",
			want: StreamID{Resource: "live/s", Mode: ModePublish, User: "alice", Session: "1", Host: "example.com", Type: "stream"}},
		{in: "#!::m=request,r=/a", want: StreamID{Resource: "a"}},
		{in: "#!::r=a,m=bidirectional", want: StreamID{Resource: "a", Mode: ModeBidirectional}},
		{in: "", err: true},
		{in: "?x", err: true},
		{in: "#!::m=publish", err: true},
		{in: "#!::r=a,m=push", err: true},
		{in: "#!::r=a,junk", err: true},
	}
	for _, tc := range cases {
		got, err := ParseStreamID(tc.in)
		if (err != nil) != tc.err {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if tc.err {
			continue
		}
		if got.Resource != tc.want.Resource || got.Mode != tc.want.Mode || got.User != tc.want.User || got.Query != tc.want.Query ||
			got.Session != tc.want.Session || got.Host != tc.want.Host || got.Type != tc.want.Type {
			t.Fatalf("%q: %+v", tc.in, got)
		}
	}
	if id, _ := ParseStreamID("#!::r=a,x=y"); id.Params["x"] != "y" {
		t.Fatal("Params")
	}
	if ModePublish.String() != "publish" || ModeRequest.String() != "request" || ModeBidirectional.String() != "bidirectional" {
		t.Fatal("Mode.String")
	}
}

// TestUnwrapTS covers timestamps that arrive late across the wrap.
func TestUnwrapTS(t *testing.T) {
	c := &Conn{}
	if c.unwrapTS(0xfffffff0) != 0xfffffff0 {
		t.Fatal("first")
	}
	if got := c.unwrapTS(0x10); got != 1<<32+0x10 {
		t.Fatalf("%x", got)
	}
	if got := c.unwrapTS(0xffffffff); got != 0xffffffff {
		t.Fatalf("late %x", got)
	}
}

// TestRTT checks the smoothing of ACKACK samples and stale ACKACKs.
func TestRTT(t *testing.T) {
	c := &Conn{rtt: 100 * time.Millisecond, rttVar: 50 * time.Millisecond}
	now := time.Unix(0, 0)
	c.acks[3] = ackRecord{no: 3, seq: 9, sent: now}
	c.handleACKACK(3, now.Add(20*time.Millisecond))
	if c.rtt != 90*time.Millisecond || c.ackAcked != 9 {
		t.Fatalf("rtt %v", c.rtt)
	}
	c.handleACKACK(3, now) // answered already
	c.handleACKACK(67, now)
	if c.rtt != 90*time.Millisecond {
		t.Fatal("stale ACKACK used")
	}
	c.acks[4] = ackRecord{no: 4, sent: now}
	c.handleACKACK(4, now.Add(500*time.Millisecond))
	if c.rtt <= 90*time.Millisecond {
		t.Fatal("rtt did not rise")
	}
}
