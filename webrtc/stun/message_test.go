package stun

import (
	"bytes"
	"encoding/hex"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// vector turns a test vector as RFC 5769 prints it, four hex bytes a line
// followed by a comment, into bytes.
func vector(s string) []byte {
	var b []byte
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 4 {
			continue
		}
		x, err := hex.DecodeString(strings.Join(f[:4], ""))
		if err != nil {
			panic(err)
		}
		b = append(b, x...)
	}
	return b
}

// RFC 5769 section 2.1.
var sampleRequest = vector(`
00 01 00 58     Request type and message length
21 12 a4 42     Magic cookie
b7 e7 a7 01  }
bc 34 d6 86  }  Transaction ID
fa 87 df ae  }
80 22 00 10     SOFTWARE attribute header
53 54 55 4e  }
20 74 65 73  }  User-agent...
74 20 63 6c  }  ...name
69 65 6e 74  }
00 24 00 04     PRIORITY attribute header
6e 00 01 ff     ICE priority value
80 29 00 08     ICE-CONTROLLED attribute header
93 2f f9 b1  }  Pseudo-random tie breaker...
51 26 3b 36  }   ...for ICE control
00 06 00 09     USERNAME attribute header
65 76 74 6a  }
3a 68 36 76  }  Username (9 bytes) and padding (3 bytes)
59 20 20 20  }
00 08 00 14     MESSAGE-INTEGRITY attribute header
9a ea a7 0c  }
bf d8 cb 56  }
78 1e f2 b5  }  HMAC-SHA1 fingerprint
b2 d3 f2 49  }
c1 b5 71 a2  }
80 28 00 04     FINGERPRINT attribute header
e5 7a 3b cf     CRC32 fingerprint
`)

// RFC 5769 section 2.2.
var sampleIPv4Response = vector(`
01 01 00 3c     Response type and message length
21 12 a4 42     Magic cookie
b7 e7 a7 01  }
bc 34 d6 86  }  Transaction ID
fa 87 df ae  }
80 22 00 0b     SOFTWARE attribute header
74 65 73 74  }
20 76 65 63  }  UTF-8 server name
74 6f 72 20  }
00 20 00 08     XOR-MAPPED-ADDRESS attribute header
00 01 a1 47     Address family (IPv4) and xor'd mapped port number
e1 12 a6 43     Xor'd mapped IPv4 address
00 08 00 14     MESSAGE-INTEGRITY attribute header
2b 91 f5 99  }
fd 9e 90 c3  }
8c 74 89 f9  }  HMAC-SHA1 fingerprint
2a f9 ba 53  }
f0 6b e7 d7  }
80 28 00 04     FINGERPRINT attribute header
c0 7d 4c 96     CRC32 fingerprint
`)

// RFC 5769 section 2.3.
var sampleIPv6Response = vector(`
01 01 00 48     Response type and message length
21 12 a4 42     Magic cookie
b7 e7 a7 01  }
bc 34 d6 86  }  Transaction ID
fa 87 df ae  }
80 22 00 0b     SOFTWARE attribute header
74 65 73 74  }
20 76 65 63  }  UTF-8 server name
74 6f 72 20  }
00 20 00 14     XOR-MAPPED-ADDRESS attribute header
00 02 a1 47     Address family (IPv6) and xor'd mapped port number
01 13 a9 fa  }
a5 d3 f1 79  }  Xor'd mapped IPv6 address
bc 25 f4 b5  }
be d2 b9 d9  }
00 08 00 14     MESSAGE-INTEGRITY attribute header
a3 82 95 4e  }
4b e6 7b f1  }
17 84 c9 7c  }  HMAC-SHA1 fingerprint
82 92 c2 75  }
bf e3 ed 41  }
80 28 00 04     FINGERPRINT attribute header
c8 fb 0b 4c     CRC32 fingerprint
`)

// RFC 5769 section 2.4.
var sampleLongTermRequest = vector(`
00 01 00 60     Request type and message length
21 12 a4 42     Magic cookie
78 ad 34 33  }
c6 ad 72 c0  }  Transaction ID
29 da 41 2e  }
00 06 00 12     USERNAME attribute header
e3 83 9e e3  }
83 88 e3 83  }
aa e3 83 83  }  Username value (18 bytes) and padding (2 bytes)
e3 82 af e3  }
82 b9 00 00  }
00 15 00 1c     NONCE attribute header
66 2f 2f 34  }
39 39 6b 39  }
35 34 64 36  }
4f 4c 33 34  }  Nonce value
6f 4c 39 46  }
53 54 76 79  }
36 34 73 41  }
00 14 00 0b     REALM attribute header
65 78 61 6d  }
70 6c 65 2e  }  Realm value (11 bytes) and padding (1 byte)
6f 72 67 00  }
00 08 00 14     MESSAGE-INTEGRITY attribute header
f6 70 24 65  }
6d d6 4a 3e  }
02 b8 e0 71  }  HMAC-SHA1 fingerprint
2e 85 c9 a2  }
8c a8 96 66  }
`)

const (
	samplePassword  = "VOkJxbRl1RmTxUk/WvJxBt"
	sampleUsername  = "マトリックス"
	sampleRealm     = "example.org"
	samplePassword2 = "TheMatrIX" // "The­MªtrⅨ" after SASLprep
)

func mustDecode(t *testing.T, b []byte) *Message {
	t.Helper()
	m := new(Message)
	if err := m.Decode(b); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return m
}

func TestRFC5769SampleRequest(t *testing.T) {
	if !IsMessage(sampleRequest) {
		t.Fatal("IsMessage = false")
	}
	m := mustDecode(t, sampleRequest)
	if m.Type != BindingRequest {
		t.Fatalf("type %v", m.Type)
	}
	if err := m.CheckIntegrity([]byte(samplePassword)); err != nil {
		t.Fatal(err)
	}
	if err := m.CheckFingerprint(); err != nil {
		t.Fatal(err)
	}
	if err := m.CheckIntegrity([]byte("wrong")); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("wrong key: %v", err)
	}
	if v, _ := m.Get(AttrSoftware); string(v) != "STUN test client" {
		t.Fatalf("SOFTWARE %q", v)
	}
	if v, _ := m.Get(AttrUsername); string(v) != "evtj:h6vY" {
		t.Fatalf("USERNAME %q", v)
	}
	if p, err := m.Uint32(AttrPriority); err != nil || p != 0x6e0001ff {
		t.Fatalf("PRIORITY %x %v", p, err)
	}
	if tb, err := m.Uint64(AttrICEControlled); err != nil || tb != 0x932ff9b151263b36 {
		t.Fatalf("ICE-CONTROLLED %x %v", tb, err)
	}

	// The same message built here verifies too, and so does a copy whose
	// USERNAME padding is changed: padding is covered by the HMAC.
	b := new(Message)
	b.Build(BindingRequest, m.TransactionID)
	b.AddString(AttrSoftware, "STUN test client")
	b.AddUint32(AttrPriority, 0x6e0001ff)
	b.AddUint64(AttrICEControlled, 0x932ff9b151263b36)
	b.AddString(AttrUsername, "evtj:h6vY")
	b.AddIntegrity([]byte(samplePassword))
	b.AddFingerprint()
	if len(b.Raw) != len(sampleRequest) {
		t.Fatalf("built %d bytes, want %d", len(b.Raw), len(sampleRequest))
	}
	padded := bytes.Clone(b.Raw)
	copy(padded[len(padded)-32-3:], "   ")
	// With the RFC's space padding the encoding is byte for byte the sample.
	if !bytes.Equal(padded[:len(padded)-32], sampleRequest[:len(sampleRequest)-32]) {
		t.Fatalf("built\n%x\nwant\n%x", padded, sampleRequest)
	}
	d := mustDecode(t, b.Raw)
	if err := d.CheckIntegrity([]byte(samplePassword)); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckFingerprint(); err != nil {
		t.Fatal(err)
	}
}

func TestRFC5769SampleResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    []byte
		addr netip.AddrPort
	}{
		{"IPv4", sampleIPv4Response, netip.MustParseAddrPort("192.0.2.1:32853")},
		{"IPv6", sampleIPv6Response, netip.MustParseAddrPort("[2001:db8:1234:5678:11:2233:4455:6677]:32853")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := mustDecode(t, tc.b)
			if m.Type != BindingSuccess {
				t.Fatalf("type %v", m.Type)
			}
			if err := m.CheckIntegrity([]byte(samplePassword)); err != nil {
				t.Fatal(err)
			}
			if err := m.CheckFingerprint(); err != nil {
				t.Fatal(err)
			}
			a, err := m.XORAddress(AttrXORMappedAddress)
			if err != nil || a != tc.addr {
				t.Fatalf("XOR-MAPPED-ADDRESS %v %v", a, err)
			}
			if v, _ := m.Get(AttrSoftware); string(v) != "test vector" {
				t.Fatalf("SOFTWARE %q", v)
			}

			b := new(Message)
			b.Build(BindingSuccess, m.TransactionID)
			b.AddString(AttrSoftware, "test vector")
			b.AddXORAddress(AttrXORMappedAddress, tc.addr)
			b.Raw[HeaderSize+4+11] = ' ' // the RFC pads SOFTWARE with a space
			b.AddIntegrity([]byte(samplePassword))
			b.AddFingerprint()
			if !bytes.Equal(b.Raw, tc.b) {
				t.Fatalf("built\n%x\nwant\n%x", b.Raw, tc.b)
			}
		})
	}
}

func TestRFC5769LongTermRequest(t *testing.T) {
	m := mustDecode(t, sampleLongTermRequest)
	key := LongTermKey(sampleUsername, sampleRealm, samplePassword2)
	if err := m.CheckIntegrity(key); err != nil {
		t.Fatal(err)
	}
	if err := m.CheckFingerprint(); !errors.Is(err, ErrAttrNotFound) {
		t.Fatalf("CheckFingerprint = %v", err)
	}
	if v, _ := m.Get(AttrUsername); string(v) != sampleUsername {
		t.Fatalf("USERNAME %q", v)
	}
	if v, _ := m.Get(AttrNonce); string(v) != "f//499k954d6OL34oL9FSTvy64sA" {
		t.Fatalf("NONCE %q", v)
	}
	if v, _ := m.Get(AttrRealm); string(v) != sampleRealm {
		t.Fatalf("REALM %q", v)
	}

	b := new(Message)
	b.Build(BindingRequest, m.TransactionID)
	b.AddString(AttrUsername, sampleUsername)
	b.AddString(AttrNonce, "f//499k954d6OL34oL9FSTvy64sA")
	b.AddString(AttrRealm, sampleRealm)
	b.AddIntegrity(key)
	if !bytes.Equal(b.Raw, sampleLongTermRequest) {
		t.Fatalf("built\n%x\nwant\n%x", b.Raw, sampleLongTermRequest)
	}
}

// RFC 8489 appendix B.1.
var sampleSHA256Request = vector(`
00 01 00 9c      Request type and message length
21 12 a4 42      Magic cookie
78 ad 34 33   }
c6 ad 72 c0   }  Transaction ID
29 da 41 2e   }
00 1e 00 20      USERHASH attribute header
4a 3c f3 8f   }
ef 69 92 bd   }
a9 52 c6 78   }
04 17 da 0f   }  Userhash value (32 bytes)
24 81 94 15   }
56 9e 60 b2   }
05 c4 6e 41   }
40 7f 17 04   }
00 15 00 29      NONCE attribute header
6f 62 4d 61   }
74 4a 6f 73   }
32 41 41 41   }
43 66 2f 2f   }
34 39 39 6b   }  Nonce value and padding (3 bytes)
39 35 34 64   }
36 4f 4c 33   }
34 6f 4c 39   }
46 53 54 76   }
79 36 34 73   }
41 00 00 00   }
00 14 00 0b      REALM attribute header
65 78 61 6d   }
70 6c 65 2e   }  Realm value (11 bytes) and padding (1 byte)
6f 72 67 00   }
00 1c 00 20      MESSAGE-INTEGRITY-SHA256 attribute header
e4 68 6c 8f   }
0e de b5 90   }
13 e0 70 90   }
01 0a 93 ef   }  HMAC-SHA256 value
cc bc cc 54   }
4c 0a 45 d9   }
f8 30 aa 6d   }
6f 73 5a 01   }
`)

// TestRFC8489SHA256Request checks what can be checked of RFC 8489's vector:
// as printed, its length counts 20 bytes the listing leaves out, so neither
// it nor its HMAC can be reproduced (the vector is known to be broken; see
// the RFC's errata). Its USERHASH is right, and the decoder rejects the
// length; a message built the same way verifies with the long-term key.
func TestRFC8489SHA256Request(t *testing.T) {
	var m Message
	if err := m.Decode(sampleSHA256Request); !errors.Is(err, ErrTruncated) {
		t.Fatalf("Decode = %v", err)
	}
	b := bytes.Clone(sampleSHA256Request)
	b[3] = byte(len(b) - HeaderSize)
	d := mustDecode(t, b)
	uh := Userhash(sampleUsername, sampleRealm)
	if v, _ := d.Get(AttrUserhash); !bytes.Equal(v, uh[:]) {
		t.Fatalf("USERHASH %x, want %x", v, uh)
	}

	key := LongTermKey(sampleUsername, sampleRealm, samplePassword2)
	r := new(Message)
	r.Build(BindingRequest, d.TransactionID)
	r.Add(AttrUserhash, uh[:])
	r.AddString(AttrNonce, "obMatJos2AAACf//499k954d6OL34oL9FSTvy64sA")
	r.AddString(AttrRealm, sampleRealm)
	r.AddIntegritySHA256(key)
	if !bytes.Equal(r.Raw[4:len(r.Raw)-32], b[4:len(b)-32]) {
		t.Fatalf("built\n%x\nwant\n%x", r.Raw, b)
	}
	if err := mustDecode(t, r.Raw).CheckIntegritySHA256(key); err != nil {
		t.Fatal(err)
	}
}

func TestMessageType(t *testing.T) {
	for m := Method(0); m < 0x1000; m++ {
		for c := ClassRequest; c <= ClassErrorResponse; c++ {
			mt := MessageType{m, c}
			if got := ParseMessageType(mt.Value()); got != mt {
				t.Fatalf("%v -> %#x -> %v", mt, mt.Value(), got)
			}
		}
	}
	for _, tc := range []struct {
		t MessageType
		v uint16
	}{
		{BindingRequest, 0x0001},
		{BindingSuccess, 0x0101},
		{BindingError, 0x0111},
		{BindingIndication, 0x0011},
		{MessageType{MethodAllocate, ClassRequest}, 0x0003},
		{MessageType{MethodData, ClassIndication}, 0x0017},
		{MessageType{MethodChannelBind, ClassSuccessResponse}, 0x0109},
	} {
		if tc.t.Value() != tc.v {
			t.Errorf("%v = %#04x, want %#04x", tc.t, tc.t.Value(), tc.v)
		}
	}
}

func TestAttributesRoundTrip(t *testing.T) {
	id := NewTransactionID()
	m := new(Message)
	m.Build(MessageType{MethodAllocate, ClassErrorResponse}, id)
	v4 := netip.MustParseAddrPort("203.0.113.9:4000")
	v6 := netip.MustParseAddrPort("[2001:db8::1]:65535")
	m.AddAddress(AttrMappedAddress, v4)
	m.AddXORAddress(AttrXORMappedAddress, v6)
	m.AddXORAddress(AttrXORPeerAddress, v4)
	m.AddXORAddress(AttrXORRelayedAddress, netip.MustParseAddrPort("[::ffff:198.51.100.1]:1"))
	m.AddErrorCode(CodeRoleConflict, "Role Conflict")
	m.AddUnknownAttributes([]AttrType{0x0031, 0x0032, 0x0033})
	m.AddChannelNumber(0x4001)
	m.AddLifetime(10 * time.Minute)
	m.AddRequestedTransport(ProtoUDP)
	m.AddFlag(AttrDontFragment)
	m.AddUint64(AttrReservationToken, 0x0102030405060708)
	m.Add(AttrData, []byte{1, 2, 3, 4, 5})
	m.AddFlag(AttrUseCandidate)
	m.AddIntegrity([]byte("k"))
	m.AddIntegritySHA256([]byte("k"))
	m.AddFingerprint()

	d := mustDecode(t, m.Append(nil))
	if d.Type != m.Type || d.TransactionID != id || d.Len() != m.Len() {
		t.Fatalf("decoded %v %x %d attrs", d.Type, d.TransactionID, d.Len())
	}
	if a, err := d.Address(AttrMappedAddress); err != nil || a != v4 {
		t.Fatalf("MAPPED-ADDRESS %v %v", a, err)
	}
	if a, err := d.XORAddress(AttrXORMappedAddress); err != nil || a != v6 {
		t.Fatalf("XOR-MAPPED-ADDRESS %v %v", a, err)
	}
	if a, err := d.XORAddress(AttrXORPeerAddress); err != nil || a != v4 {
		t.Fatalf("XOR-PEER-ADDRESS %v %v", a, err)
	}
	if a, err := d.XORAddress(AttrXORRelayedAddress); err != nil || a != netip.MustParseAddrPort("198.51.100.1:1") {
		t.Fatalf("XOR-RELAYED-ADDRESS %v %v", a, err)
	}
	if c, r, err := d.ErrorCode(); err != nil || c != 487 || string(r) != "Role Conflict" {
		t.Fatalf("ERROR-CODE %d %q %v", c, r, err)
	}
	if u, err := d.UnknownAttributes(nil); err != nil || len(u) != 3 || u[2] != 0x0033 {
		t.Fatalf("UNKNOWN-ATTRIBUTES %v %v", u, err)
	}
	if n, err := d.ChannelNumber(); err != nil || n != 0x4001 {
		t.Fatalf("CHANNEL-NUMBER %x %v", n, err)
	}
	if l, err := d.Lifetime(); err != nil || l != 10*time.Minute {
		t.Fatalf("LIFETIME %v %v", l, err)
	}
	if p, err := d.RequestedTransport(); err != nil || p != ProtoUDP {
		t.Fatalf("REQUESTED-TRANSPORT %v %v", p, err)
	}
	if !d.Contains(AttrDontFragment) || !d.Contains(AttrUseCandidate) {
		t.Fatal("flags missing")
	}
	if v, _ := d.Get(AttrData); !bytes.Equal(v, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("DATA %x", v)
	}
	if err := d.CheckIntegrity([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckIntegritySHA256([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckFingerprint(); err != nil {
		t.Fatal(err)
	}
	if typ, _ := d.Attr(0); typ != AttrMappedAddress || typ.String() != "MAPPED-ADDRESS" || AttrType(0x7777).String() != "AttrType(0x7777)" {
		t.Fatal("Attr/String")
	}
	if !AttrUseCandidate.Required() || AttrSoftware.Required() {
		t.Fatal("Required")
	}
}

func TestIgnoreAfterIntegrity(t *testing.T) {
	m := new(Message)
	m.Build(BindingRequest, NewTransactionID())
	m.AddString(AttrUsername, "a:b")
	m.AddIntegrity([]byte("pw"))
	m.AddUint32(AttrPriority, 1) // not covered: must be ignored
	m.AddFingerprint()
	d := mustDecode(t, m.Raw)
	if d.Contains(AttrPriority) {
		t.Fatal("PRIORITY after MESSAGE-INTEGRITY was kept")
	}
	if err := d.CheckIntegrity([]byte("pw")); err != nil {
		t.Fatal(err)
	}
	if err := d.CheckFingerprint(); err != nil {
		t.Fatal(err)
	}
}

func TestTamper(t *testing.T) {
	for i := HeaderSize; i < len(sampleRequest)-8; i++ {
		b := bytes.Clone(sampleRequest)
		b[i] ^= 0x01
		m := new(Message)
		if m.Decode(b) != nil {
			continue
		}
		if m.CheckIntegrity([]byte(samplePassword)) == nil && m.CheckFingerprint() == nil {
			t.Fatalf("flipping byte %d went unnoticed", i)
		}
	}
}

func TestDecodeMalformed(t *testing.T) {
	ok := bytes.Clone(sampleIPv4Response)
	for _, tc := range []struct {
		name string
		b    []byte
		err  error
	}{
		{"empty", nil, ErrTruncated},
		{"short header", ok[:19], ErrTruncated},
		{"top bits", append([]byte{0x80}, ok[1:]...), ErrNotSTUN},
		{"cookie", append(append(bytes.Clone(ok[:4]), 0, 0, 0, 0), ok[8:]...), ErrNotSTUN},
		{"length not 4n", append([]byte{1, 1, 0, 0x3d}, ok[4:]...), ErrBadLength},
		{"body short", ok[:len(ok)-4], ErrTruncated},
		{"attr header cut", []byte{0, 1, 0, 4, 0x21, 0x12, 0xa4, 0x42, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 6, 0, 8}, ErrTruncated},
		{"attr value overruns", []byte{0, 1, 0, 4, 0x21, 0x12, 0xa4, 0x42, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 6, 0, 1}, ErrTruncated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var m Message
			if err := m.Decode(tc.b); !errors.Is(err, tc.err) {
				t.Fatalf("Decode = %v, want %v", err, tc.err)
			}
		})
	}
	// A message followed by more bytes, as on a stream, decodes to itself.
	var m Message
	if err := m.Decode(append(bytes.Clone(ok), 9, 9, 9)); err != nil || len(m.Raw) != len(ok) {
		t.Fatalf("trailing bytes: %v %d", err, len(m.Raw))
	}
	if err := m.CheckFingerprint(); err != nil {
		t.Fatal(err)
	}
	if IsMessage(ok[:19]) || IsMessage([]byte{0x16, 0xfe, 0xfd, 0, 0x21, 0x12, 0xa4, 0x42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("IsMessage accepted a non-STUN packet")
	}
}

func TestBadAttributeValues(t *testing.T) {
	m := new(Message)
	m.Build(BindingSuccess, NewTransactionID())
	m.Add(AttrXORMappedAddress, []byte{0, 3, 0, 0, 1, 2, 3, 4})
	m.Add(AttrMappedAddress, []byte{0, 1, 0, 0, 1, 2, 3})
	m.Add(AttrPriority, []byte{1})
	m.Add(AttrErrorCode, []byte{1})
	m.Add(AttrUnknownAttributes, []byte{1})
	m.Add(AttrMessageIntegrity, []byte{1})
	d := mustDecode(t, m.Raw)
	if _, err := d.XORAddress(AttrXORMappedAddress); !errors.Is(err, ErrBadFamily) {
		t.Fatal(err)
	}
	if _, err := d.Address(AttrMappedAddress); !errors.Is(err, ErrAttrSize) {
		t.Fatal(err)
	}
	if _, err := d.Address(AttrOtherAddress); !errors.Is(err, ErrAttrNotFound) {
		t.Fatal(err)
	}
	if _, err := d.Uint32(AttrPriority); !errors.Is(err, ErrAttrSize) {
		t.Fatal(err)
	}
	if _, _, err := d.ErrorCode(); !errors.Is(err, ErrAttrSize) {
		t.Fatal(err)
	}
	if _, err := d.UnknownAttributes(nil); !errors.Is(err, ErrAttrSize) {
		t.Fatal(err)
	}
	if err := d.CheckIntegrity([]byte("x")); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	if err := d.CheckIntegritySHA256([]byte("x")); !errors.Is(err, ErrIntegrityMissing) {
		t.Fatal(err)
	}
}

func TestDecodeReuseNoAlloc(t *testing.T) {
	var m Message
	_ = m.Decode(sampleRequest)
	n := testing.AllocsPerRun(100, func() {
		if err := m.Decode(sampleRequest); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Uint32(AttrPriority); err != nil {
			t.Fatal(err)
		}
		_ = m.CheckFingerprint()
	})
	if n != 0 {
		t.Fatalf("%v allocations a decode", n)
	}
}
