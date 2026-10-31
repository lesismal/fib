package srtp

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var profiles = []Profile{ProfileAES128CMHMACSHA1_80, ProfileAES128CMHMACSHA1_32, ProfileAEADAES128GCM, ProfileAEADAES256GCM}

// TestKeyDerivation checks the session keys RFC 3711 Appendix B.3 derives
// from its master key and salt.
func TestKeyDerivation(t *testing.T) {
	masterKey := unhex(t, "E1F97A0D3E018BE0D64FA32C06DE4139")
	masterSalt := unhex(t, "0EC675AD498AFEEBB6960B3AABE6")
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		label byte
		want  string
	}{
		{labelRTPEncryption, "C61E7A93744F39EE10734AFE3FF7A087"},
		{labelRTPSalt, "30CBBC08863D8C85D49DB34A9AE1"},
		{labelRTPAuth, "CEBE321F6FF7716B6FD4AB49AF256A156D38BAA4"},
	} {
		want := unhex(t, tc.want)
		if got := deriveKey(block, masterSalt, tc.label, len(want)); !bytes.Equal(got, want) {
			t.Errorf("label %d: got %X, want %X", tc.label, got, want)
		}
	}
}

// TestKeystream checks AES-CM against RFC 3711 Appendix B.2, and the
// AES-256 vector of RFC 6188 section 7.1, from the start of the key stream.
func TestKeystream(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{"2B7E151628AED2A6ABF7158809CF4F3C", `E03EAD0935C95E80E166B16DD92B4EB4
			D23513162B02D0F72A43A2FE4A5F97AB 41E95B3BB0A2E8DD477901E4FCA894C0`},
		{"57f82fe3613fd170a85ec93c40b1f0922ec4cb0dc025b58272147cc438944a98", `92bdd28a93c3f52511c677d08b5515a4
			9da71b2378a854f67050756ded165bac 63c4868b7096d88421b563b8c94c9a31`},
	} {
		block, err := aes.NewCipher(unhex(t, tc.key))
		if err != nil {
			t.Fatal(err)
		}
		c := &Context{}
		copy(c.iv[:], unhex(t, "F0F1F2F3F4F5F6F7F8F9FAFBFCFD0000"))
		want := unhex(t, tc.want)
		got := make([]byte, len(want))
		c.xorCTR(block, got, got)
		if !bytes.Equal(got, want) {
			t.Errorf("key %s: got %X, want %X", tc.key, got, want)
		}
	}
}

// setSessionKeys replaces a Context's derived keys with session keys, as
// RFC 7714's test vectors give them.
func setSessionKeys(t *testing.T, c *Context, key, salt []byte) {
	t.Helper()
	for _, k := range []*sessionKeys{&c.rtp, &c.rtcp} {
		block, err := aes.NewCipher(key)
		if err != nil {
			t.Fatal(err)
		}
		if k.gcm, err = cipher.NewGCM(block); err != nil {
			t.Fatal(err)
		}
		copy(k.salt[:], salt)
	}
}

// TestGCMVectors checks AEAD_AES_128_GCM and AEAD_AES_256_GCM against RFC
// 7714 sections 16 and 17.
func TestGCMVectors(t *testing.T) {
	plainRTP := `8040f17b 8041f8d3 5501a0b2 47616c6c 69612065 7374206f 6d6e6973 20646976
		69736120 696e2070 61727465 73207472 6573`
	plainRTCP := `81c8000d 4d617273 4e545031 4e545032 52545020 0000042a 0000e930 4c756e61
		deadbeef deadbeef deadbeef deadbeef deadbeef`
	for _, tc := range []struct {
		profile             Profile
		key                 string
		rtp, rtcp, rtcpAuth string
	}{
		{ProfileAEADAES128GCM, "000102030405060708090a0b0c0d0e0f",
			`8040f17b 8041f8d3 5501a0b2 f24de3a3 fb34de6c acba861c 9d7e4bca be633bd5
			0d294e6f 42a5f47a 51c7d19b 36de3adf 8833899d 7f27beb1 6a9152cf 765ee439 0cce`,
			`81c8000d 4d617273 63e94885 dcdab67c a727d766 2f6b7e99 7ff5c0f7 6c06f32d
			c676a5f1 730d6fda 4ce09b46 86303ded 0bb9275b c84aa458 96cf4d2f c5abf872 45d9eade 800005d4`,
			`81c8000d 4d617273 4e545031 4e545032 52545020 0000042a 0000e930 4c756e61
			deadbeef deadbeef deadbeef deadbeef deadbeef 841dd968 3dd78ec9 2ae58790 125f62b3 000005d4`},
		{ProfileAEADAES256GCM, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			`8040f17b 8041f8d3 5501a0b2 32b1de78 a822fe12 ef9f78fa 332e33aa b1801238
			9a58e2f3 b50b2a02 76ffae0f 1ba63799 b87b7aa3 db36dfff d6b0f9bb 7878d7a7 6c13`,
			`81c8000d 4d617273 d50ae4d1 f5ce5d30 4ba297e4 7d470c28 2c3ece5d bffe0a50
			a2eaa5c1 110555be 8415f658 c61de047 6f1b6fad 1d1eb30c 4446839f 57ff6f6c b26ac3be 800005d4`,
			`81c8000d 4d617273 4e545031 4e545032 52545020 0000042a 0000e930 4c756e61
			deadbeef deadbeef deadbeef deadbeef deadbeef 91db4afb feee5a97 8fab4393 ed2615fe 000005d4`},
	} {
		t.Run(tc.profile.String(), func(t *testing.T) {
			newCtx := func() *Context {
				c, err := NewContext(tc.profile, make([]byte, tc.profile.KeyLen()), make([]byte, 12), nil)
				if err != nil {
					t.Fatal(err)
				}
				setSessionKeys(t, c, unhex(t, tc.key), unhex(t, "517569642070726f2071756f"))
				return c
			}
			sender, receiver := newCtx(), newCtx()
			got, err := sender.ProtectRTP(nil, unhex(t, plainRTP))
			if err != nil || !bytes.Equal(got, unhex(t, tc.rtp)) {
				t.Fatalf("ProtectRTP: %v\n got %x\nwant %x", err, got, unhex(t, tc.rtp))
			}
			got, err = receiver.UnprotectRTP(nil, unhex(t, tc.rtp))
			if err != nil || !bytes.Equal(got, unhex(t, plainRTP)) {
				t.Fatalf("UnprotectRTP: %v %x", err, got)
			}
			sender.rtcpOut[0x4d617273] = 0x5d3
			got, err = sender.ProtectRTCP(nil, unhex(t, plainRTCP))
			if err != nil || !bytes.Equal(got, unhex(t, tc.rtcp)) {
				t.Fatalf("ProtectRTCP: %v\n got %x\nwant %x", err, got, unhex(t, tc.rtcp))
			}
			got, err = receiver.UnprotectRTCP(nil, unhex(t, tc.rtcp))
			if err != nil || !bytes.Equal(got, unhex(t, plainRTCP)) {
				t.Fatalf("UnprotectRTCP: %v %x", err, got)
			}
			got, err = newCtx().UnprotectRTCP(nil, unhex(t, tc.rtcpAuth))
			if err != nil || !bytes.Equal(got, unhex(t, plainRTCP)) {
				t.Fatalf("UnprotectRTCP of an unencrypted packet: %v %x", err, got)
			}
		})
	}
}

func rtpPacket(ssrc uint32, seq uint16, payload int) []byte {
	b := []byte{0x90, 96, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0xbe, 0xde, 0, 1, 0x10, 0xaa, 0xbb, 0}
	binary.BigEndian.PutUint16(b[2:], seq)
	binary.BigEndian.PutUint32(b[4:], uint32(seq)*960)
	binary.BigEndian.PutUint32(b[8:], ssrc)
	for i := range payload {
		b = append(b, byte(i+int(seq)))
	}
	return b
}

func rtcpPacket(ssrc uint32, n byte) []byte {
	b := []byte{0x80, 201, 0, 3, 0, 0, 0, 0, 1, 2, 3, n, 0x81, 202, 0, 2, 0, 0, 0, 0, 1, 1, 'x', 0}
	binary.BigEndian.PutUint32(b[4:], ssrc)
	binary.BigEndian.PutUint32(b[16:], ssrc)
	return b
}

func newPair(t testing.TB, p Profile, opts *Options) (*Context, *Context) {
	t.Helper()
	material := make([]byte, p.KeyingMaterialLen())
	for i := range material {
		material[i] = byte(i * 7)
	}
	client, _, err := NewContexts(material, p, true, opts)
	if err != nil {
		t.Fatal(err)
	}
	_, server, err := NewContexts(material, p, false, opts)
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

// TestRoundTrip protects and unprotects through the sequence number's
// rollover, with packets reordered around it, for every profile.
func TestRoundTrip(t *testing.T) {
	for _, p := range profiles {
		t.Run(p.String(), func(t *testing.T) {
			tx, rx := newPair(t, p, nil)
			seqs := []uint16{65530, 65531, 65533, 65532, 65534, 65535, 1, 0, 2, 3, 20000, 40000, 60000, 1000, 999, 1001}
			for _, seq := range seqs {
				pkt := rtpPacket(7, seq, 100)
				enc, err := tx.ProtectRTP(nil, pkt)
				if err != nil {
					t.Fatal(err)
				}
				if len(enc) != len(pkt)+p.RTPOverhead() {
					t.Fatalf("seq %d: protected %d bytes", seq, len(enc))
				}
				dec, err := rx.UnprotectRTP(nil, enc)
				if err != nil || !bytes.Equal(dec, pkt) {
					t.Fatalf("seq %d: %v", seq, err)
				}
				if _, err := rx.UnprotectRTP(nil, enc); !errors.Is(err, ErrReplayed) {
					t.Fatalf("seq %d replayed: %v", seq, err)
				}
			}
			// 60000 then 1000 wrapped a second time.
			if roc, _ := rx.ROC(7); roc != 2 {
				t.Errorf("receiver ROC %d, want 2", roc)
			}
			for i := range 5 {
				pkt := rtcpPacket(9, byte(i))
				enc, err := tx.ProtectRTCP(nil, pkt)
				if err != nil {
					t.Fatal(err)
				}
				dec, err := rx.UnprotectRTCP(nil, enc)
				if err != nil || !bytes.Equal(dec, pkt) {
					t.Fatalf("RTCP %d: %v", i, err)
				}
				if _, err := rx.UnprotectRTCP(nil, enc); !errors.Is(err, ErrReplayed) {
					t.Fatalf("RTCP %d replayed: %v", i, err)
				}
			}
		})
	}
}

// TestInPlace protects and unprotects in the packet's own buffer.
func TestInPlace(t *testing.T) {
	for _, p := range profiles {
		tx, rx := newPair(t, p, nil)
		want := rtpPacket(1, 10, 50)
		buf := make([]byte, len(want), len(want)+32)
		copy(buf, want)
		enc, err := tx.ProtectRTP(buf[:0], buf)
		if err != nil || &enc[0] != &buf[0] {
			t.Fatalf("%v: ProtectRTP in place: %v", p, err)
		}
		dec, err := rx.UnprotectRTP(enc[:0], enc)
		if err != nil || !bytes.Equal(dec, want) {
			t.Fatalf("%v: UnprotectRTP in place: %v", p, err)
		}
		want = rtcpPacket(1, 1)
		buf = append(buf[:0], want...)
		enc, err = tx.ProtectRTCP(buf[:0], buf)
		if err != nil {
			t.Fatal(err)
		}
		dec, err = rx.UnprotectRTCP(enc[:0], enc)
		if err != nil || !bytes.Equal(dec, want) {
			t.Fatalf("%v: UnprotectRTCP in place: %v", p, err)
		}
		// Appending after what dst holds keeps it.
		dec, err = rx.UnprotectRTP([]byte("prefix"), mustProtect(t, tx, rtpPacket(1, 11, 5)))
		if err != nil || string(dec[:6]) != "prefix" || !bytes.Equal(dec[6:], rtpPacket(1, 11, 5)) {
			t.Fatalf("%v: append after a prefix: %v", p, err)
		}
	}
}

func mustProtect(t testing.TB, c *Context, pkt []byte) []byte {
	t.Helper()
	enc, err := c.ProtectRTP(nil, pkt)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

// TestTamper flips each bit of protected packets: every change fails
// authentication and leaves the receiver as it was.
func TestTamper(t *testing.T) {
	for _, p := range profiles {
		tx, rx := newPair(t, p, nil)
		enc := mustProtect(t, tx, rtpPacket(3, 5, 20))
		for i := range len(enc) * 8 {
			bad := bytes.Clone(enc)
			bad[i/8] ^= 1 << (i % 8)
			if _, err := rx.UnprotectRTP(nil, bad); err == nil {
				t.Fatalf("%v: RTP bit %d flipped was accepted", p, i)
			}
		}
		if _, err := rx.UnprotectRTP(nil, enc); err != nil {
			t.Fatalf("%v: the intact packet: %v", p, err)
		}
		encRTCP, _ := tx.ProtectRTCP(nil, rtcpPacket(3, 1))
		for i := range len(encRTCP) * 8 {
			bad := bytes.Clone(encRTCP)
			bad[i/8] ^= 1 << (i % 8)
			if _, err := rx.UnprotectRTCP(nil, bad); err == nil {
				t.Fatalf("%v: RTCP bit %d flipped was accepted", p, i)
			}
		}
		if _, err := rx.UnprotectRTCP(nil, encRTCP); err != nil {
			t.Fatalf("%v: the intact RTCP packet: %v", p, err)
		}
		for n := range enc {
			if _, err := rx.UnprotectRTP(nil, enc[:n]); err == nil {
				t.Fatalf("%v: RTP truncated to %d accepted", p, n)
			}
		}
	}
}

// TestReplayWindow checks the window's edge, for the default size and a
// larger one.
func TestReplayWindow(t *testing.T) {
	for _, size := range []int{0, 200} {
		want := size
		if want == 0 {
			want = DefaultReplayWindow
		}
		tx, rx := newPair(t, ProfileAES128CMHMACSHA1_80, &Options{ReplayWindow: size})
		var pkts [][]byte
		for seq := range 400 {
			pkts = append(pkts, mustProtect(t, tx, rtpPacket(1, uint16(seq), 4)))
		}
		if _, err := rx.UnprotectRTP(nil, pkts[399]); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.UnprotectRTP(nil, pkts[399-want]); !errors.Is(err, ErrReplayed) {
			t.Errorf("window %d: a packet %d behind: %v", want, want, err)
		}
		if _, err := rx.UnprotectRTP(nil, pkts[399-want+1]); err != nil {
			t.Errorf("window %d: a packet %d behind: %v", want, want-1, err)
		}
	}
	var w replayWindow
	w.init(64)
	for _, tc := range []struct {
		index uint64
		ok    bool
	}{{100, true}, {100, false}, {37, true}, {36, false}, {99, true}, {99, false}, {300, true}, {237, true}, {236, false}, {100, false}} {
		if got := w.check(tc.index); got != tc.ok {
			t.Fatalf("check(%d) = %v, want %v", tc.index, got, tc.ok)
		}
		if tc.ok {
			w.accept(tc.index)
		}
	}
}

// TestSetROC joins a stream whose sequence numbers wrapped before.
func TestSetROC(t *testing.T) {
	tx, rx := newPair(t, ProfileAEADAES128GCM, nil)
	tx.SetROC(5, 3)
	rx.SetROC(5, 3)
	enc := mustProtect(t, tx, rtpPacket(5, 100, 10))
	if _, err := rx.UnprotectRTP(nil, enc); err != nil {
		t.Fatal(err)
	}
	if roc, ok := rx.ROC(5); !ok || roc != 3 {
		t.Fatalf("ROC %d %v", roc, ok)
	}
}

// TestNewContextsLayout checks that each side protects with its own write
// key and unprotects with the peer's, and that bad inputs are refused.
func TestNewContextsLayout(t *testing.T) {
	p := ProfileAES128CMHMACSHA1_80
	material := make([]byte, p.KeyingMaterialLen())
	for i := range material {
		material[i] = byte(i)
	}
	clientLocal, clientRemote, err := NewContexts(material, p, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	serverLocal, serverRemote, err := NewContexts(material, p, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := NewContext(p, material[:16], material[32:46], nil)
	if err != nil {
		t.Fatal(err)
	}
	pkt := rtpPacket(1, 1, 10)
	a, b := mustProtect(t, clientLocal, pkt), mustProtect(t, direct, pkt)
	if !bytes.Equal(a, b) {
		t.Fatal("the client does not protect with client_write_key and client_write_salt")
	}
	if _, err := serverRemote.UnprotectRTP(nil, a); err != nil {
		t.Fatal(err)
	}
	if _, err := clientRemote.UnprotectRTP(nil, mustProtect(t, serverLocal, pkt)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := NewContexts(material[1:], p, true, nil); !errors.Is(err, ErrKeyLength) {
		t.Fatal(err)
	}
	if _, _, err := NewContexts(material, 5, true, nil); !errors.Is(err, ErrProfile) {
		t.Fatal(err)
	}
}

// TestNoAllocs checks that a warmed Context protects and unprotects
// without allocating.
func TestNoAllocs(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	for _, p := range profiles {
		tx, rx := newPair(t, p, nil)
		pkt := rtpPacket(1, 0, 1200)
		enc := make([]byte, 0, 1500)
		dec := make([]byte, 0, 1500)
		seq := uint16(0)
		allocs := testing.AllocsPerRun(100, func() {
			seq++
			binary.BigEndian.PutUint16(pkt[2:], seq)
			e, err := tx.ProtectRTP(enc[:0], pkt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rx.UnprotectRTP(dec[:0], e); err != nil {
				t.Fatal(err)
			}
			e, _ = tx.ProtectRTCP(enc[:0], rtcpPacket(1, 1))
			if _, err := rx.UnprotectRTCP(dec[:0], e); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 {
			t.Errorf("%v: %v allocations per packet", p, allocs)
		}
	}
}

func BenchmarkProtectRTP(b *testing.B) {
	for _, p := range profiles {
		b.Run(p.String(), func(b *testing.B) {
			tx, _ := newPair(b, p, nil)
			pkt := rtpPacket(1, 0, 1200-20)
			out := make([]byte, 0, 1500)
			b.SetBytes(int64(len(pkt)))
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				binary.BigEndian.PutUint16(pkt[2:], uint16(i))
				if _, err := tx.ProtectRTP(out[:0], pkt); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnprotectRTP(b *testing.B) {
	for _, p := range profiles {
		b.Run(p.String(), func(b *testing.B) {
			tx, rx := newPair(b, p, nil)
			pkts := make([][]byte, 256)
			for i := range pkts {
				pkts[i] = mustProtect(b, tx, rtpPacket(1, uint16(i), 1200-20))
			}
			out := make([]byte, 0, 1500)
			b.SetBytes(1200)
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				if i > 0 && i%len(pkts) == 0 {
					// Start over as a new stream rather than replay.
					rx.RemoveSSRC(1)
				}
				if _, err := rx.UnprotectRTP(out[:0], pkts[i%len(pkts)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestContextErrors checks the inputs a Context refuses, and its
// accessors.
func TestContextErrors(t *testing.T) {
	if _, err := NewContext(9, make([]byte, 16), make([]byte, 14), nil); !errors.Is(err, ErrProfile) {
		t.Fatal(err)
	}
	if _, err := NewContext(ProfileAEADAES256GCM, make([]byte, 16), make([]byte, 12), nil); !errors.Is(err, ErrKeyLength) {
		t.Fatal(err)
	}
	if s := Profile(9).String(); s != "Profile(9)" {
		t.Fatal(s)
	}
	tx, rx := newPair(t, ProfileAES128CMHMACSHA1_32, nil)
	if tx.Profile() != ProfileAES128CMHMACSHA1_32 {
		t.Fatal(tx.Profile())
	}
	if _, err := tx.ProtectRTP(nil, []byte{0x80, 96, 0}); !errors.Is(err, ErrMalformed) {
		t.Fatal(err)
	}
	for _, pkt := range [][]byte{{0x80, 201, 0, 1}, {0x40, 201, 0, 1, 0, 0, 0, 1}} {
		want := ErrShortPacket
		if len(pkt) == 8 {
			want = ErrMalformed
		}
		if _, err := tx.ProtectRTCP(nil, pkt); !errors.Is(err, want) {
			t.Fatalf("protect %x: %v", pkt, err)
		}
		if _, err := rx.UnprotectRTCP(nil, pkt); !errors.Is(err, want) {
			t.Fatalf("unprotect %x: %v", pkt, err)
		}
	}
	// A header and sender SSRC but no room for the index and tag.
	enc, err := tx.ProtectRTCP(nil, rtcpPacket(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rx.UnprotectRTCP(nil, enc[:8+4+9]); !errors.Is(err, ErrShortPacket) {
		t.Fatal(err)
	}
	if _, err := rx.UnprotectRTCP(nil, enc); err != nil {
		t.Fatal(err)
	}
}

// TestSRTCPIndexExhausted checks that a sender whose SRTCP index reached
// its 31-bit limit stops, as it must be rekeyed, and that other SSRCs and
// forgotten ones go on.
func TestSRTCPIndexExhausted(t *testing.T) {
	tx, _ := newPair(t, ProfileAEADAES128GCM, nil)
	tx.rtcpOut[1] = maxSRTCPIndex - 1
	if _, err := tx.ProtectRTCP(nil, rtcpPacket(1, 1)); err != nil {
		t.Fatal(err)
	}
	if b, err := tx.ProtectRTCP([]byte{7}, rtcpPacket(1, 1)); !errors.Is(err, ErrIndexExhaust) || len(b) != 1 {
		t.Fatal(b, err)
	}
	if _, err := tx.ProtectRTCP(nil, rtcpPacket(2, 1)); err != nil {
		t.Fatal(err)
	}
	tx.RemoveSSRC(1)
	if _, err := tx.ProtectRTCP(nil, rtcpPacket(1, 1)); err != nil {
		t.Fatal(err)
	}
}

// TestRemoveSSRC forgets a receiver's state: its rollover counter and the
// replay windows, so that a packet seen before is taken again.
func TestRemoveSSRC(t *testing.T) {
	tx, rx := newPair(t, ProfileAES128CMHMACSHA1_80, nil)
	if _, ok := rx.ROC(4); ok {
		t.Fatal("ROC of an unknown SSRC")
	}
	enc := mustProtect(t, tx, rtpPacket(4, 10, 8))
	encRTCP, _ := tx.ProtectRTCP(nil, rtcpPacket(4, 1))
	for range 2 {
		if _, err := rx.UnprotectRTP(nil, enc); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.UnprotectRTCP(nil, encRTCP); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.UnprotectRTP(nil, enc); !errors.Is(err, ErrReplayed) {
			t.Fatal(err)
		}
		if _, err := rx.UnprotectRTCP(nil, encRTCP); !errors.Is(err, ErrReplayed) {
			t.Fatal(err)
		}
		if _, ok := rx.ROC(4); !ok {
			t.Fatal("no ROC")
		}
		rx.RemoveSSRC(4)
		if _, ok := rx.ROC(4); ok {
			t.Fatal("ROC kept")
		}
	}
}

// TestLateBeforeRollover receives a packet from before the sequence
// number wrapped after packets from after it: it is taken under the
// previous rollover counter, which stays where it was.
func TestLateBeforeRollover(t *testing.T) {
	tx, rx := newPair(t, ProfileAES128CMHMACSHA1_80, nil)
	pkts := map[uint16][]byte{}
	for _, seq := range []uint16{65534, 65535, 0, 1} {
		pkts[seq] = mustProtect(t, tx, rtpPacket(6, seq, 4))
	}
	for _, seq := range []uint16{65534, 0, 1, 65535} {
		out, err := rx.UnprotectRTP(nil, pkts[seq])
		if err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		if !bytes.Equal(out, rtpPacket(6, seq, 4)) {
			t.Fatalf("seq %d: payload", seq)
		}
	}
	if roc, _ := rx.ROC(6); roc != 1 {
		t.Fatalf("ROC %d", roc)
	}
}

// TestRTCPReplayWindow checks the SRTCP window's edge, for the default
// size and Options.RTCPReplayWindow.
func TestRTCPReplayWindow(t *testing.T) {
	for _, size := range []int{0, 100} {
		want := size
		if want == 0 {
			want = DefaultReplayWindow
		}
		tx, rx := newPair(t, ProfileAES128CMHMACSHA1_80, &Options{RTCPReplayWindow: size})
		var pkts [][]byte
		for range 200 {
			enc, err := tx.ProtectRTCP(nil, rtcpPacket(1, 1))
			if err != nil {
				t.Fatal(err)
			}
			pkts = append(pkts, enc)
		}
		if _, err := rx.UnprotectRTCP(nil, pkts[199]); err != nil {
			t.Fatal(err)
		}
		if _, err := rx.UnprotectRTCP(nil, pkts[199-want]); !errors.Is(err, ErrReplayed) {
			t.Errorf("window %d: a packet %d behind: %v", want, want, err)
		}
		if _, err := rx.UnprotectRTCP(nil, pkts[199-want+1]); err != nil {
			t.Errorf("window %d: a packet %d behind: %v", want, want-1, err)
		}
	}
}
