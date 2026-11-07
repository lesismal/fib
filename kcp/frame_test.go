package kcp

import (
	"bytes"
	"crypto/aes"
	"math/rand/v2"
	"testing"
)

func testCrypts(t *testing.T) map[string]*Crypt {
	t.Helper()
	key := bytes.Repeat([]byte{7}, 32)
	out := map[string]*Crypt{}
	for _, name := range []string{"null", "none", "xor", "aes", "aes-128", "aes-192", "3des", "aes-128-gcm"} {
		c, err := NewCrypt(name, key)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		out[name] = c
	}
	return out
}

func TestCryptRoundTrip(t *testing.T) {
	for name, c := range testCrypts(t) {
		for _, n := range []int{0, 1, 7, 8, 15, 16, 17, 100, 1400} {
			plain := make([]byte, n)
			for i := range plain {
				plain[i] = byte(i * 13)
			}
			b := make([]byte, c.headerSize(), c.headerSize()+n+c.trailerSize())
			b = append(b, plain...)
			sealed := c.seal(b)
			if c != nil && n >= 8 && bytes.Contains(sealed, plain) && name != "none" {
				t.Fatalf("%s: %d bytes sent in the clear", name, n)
			}
			got, err := c.open(append([]byte(nil), sealed...))
			if err != nil || !bytes.Equal(got, plain) {
				t.Fatalf("%s %d: got %d bytes, %v", name, n, len(got), err)
			}
			if c == nil || n == 0 {
				continue
			}
			sealed[len(sealed)-1] ^= 1
			if _, err := c.open(sealed); err == nil {
				t.Fatalf("%s %d: a corrupted datagram opened", name, n)
			}
		}
	}
	if _, err := NewCrypt("salsa20", make([]byte, 32)); err == nil {
		t.Fatal("salsa20 is not supported")
	}
	if _, err := NewCrypt("aes", make([]byte, 16)); err == nil {
		t.Fatal("aes takes 32 bytes")
	}
	b, _ := aes.NewCipher(make([]byte, 16))
	if _, err := NewBlockCrypt(b); err != nil {
		t.Fatal(err)
	}
}

// TestCFBMatchesStandardCFB checks kcp-go's CFB against the standard
// library's on whole blocks, which it is, with a fixed IV.
func TestCFBMatchesWholeBlocks(t *testing.T) {
	block, _ := aes.NewCipher(bytes.Repeat([]byte{1}, 16))
	plain := make([]byte, 64)
	for i := range plain {
		plain[i] = byte(i)
	}
	got := append([]byte(nil), plain...)
	cfbEncrypt(block, got)
	// By hand: c0 = p0 ^ E(IV), ci = pi ^ E(ci-1).
	want := make([]byte, 64)
	var ks [16]byte
	block.Encrypt(ks[:], initialVector[:])
	for i := 0; i < 64; i += 16 {
		for j := range 16 {
			want[i+j] = plain[i+j] ^ ks[j]
		}
		block.Encrypt(ks[:], want[i:i+16])
	}
	if !bytes.Equal(got, want) {
		t.Fatal("CFB differs")
	}
	cfbDecrypt(block, got)
	if !bytes.Equal(got, plain) {
		t.Fatal("CFB does not decrypt")
	}
}

func TestRSRecovers(t *testing.T) {
	for _, dp := range [][2]int{{1, 1}, {3, 2}, {10, 3}, {20, 10}, {128, 128}} {
		rs, err := newRS(dp[0], dp[1])
		if err != nil {
			t.Fatal(err)
		}
		rnd := rand.New(rand.NewPCG(uint64(dp[0]), uint64(dp[1])))
		shards := make([][]byte, dp[0]+dp[1])
		for i := range shards {
			shards[i] = make([]byte, 50)
			if i < dp[0] {
				for j := range shards[i] {
					shards[i][j] = byte(rnd.Uint32())
				}
			}
		}
		rs.encode(shards)
		orig := make([][]byte, len(shards))
		for i := range shards {
			orig[i] = append([]byte(nil), shards[i]...)
		}
		// Lose as many as there is parity, data first.
		for _, i := range rnd.Perm(len(shards))[:dp[1]] {
			shards[i] = nil
		}
		if err := rs.reconstructData(shards, 50); err != nil {
			t.Fatal(err)
		}
		for i := range dp[0] {
			if !bytes.Equal(shards[i], orig[i]) {
				t.Fatalf("%v: shard %d differs", dp, i)
			}
		}
	}
	if _, err := newRS(200, 57); err == nil {
		t.Fatal("257 shards")
	}
}

func TestFECFraming(t *testing.T) {
	for name, c := range testCrypts(t) {
		for _, dp := range [][2]int{{0, 0}, {10, 3}, {2, 1}} {
			enc, err := newFramer(c, dp[0], dp[1])
			if err != nil {
				t.Fatal(err)
			}
			dec, _ := newFramer(c, dp[0], dp[1])
			rnd := rand.New(rand.NewPCG(3, 4))
			var sent, got [][]byte
			var dgrams [][]byte
			for i := range 200 {
				p := make([]byte, Overhead+rnd.IntN(1000))
				for j := range p {
					p[j] = byte(i + j)
				}
				sent = append(sent, p)
				enc.encode(p, int64(i), func(d []byte) { dgrams = append(dgrams, append([]byte(nil), d...)) })
			}
			if dp[0] > 0 && len(dgrams) != 200+200/dp[0]*dp[1] {
				t.Fatalf("%s %v: %d datagrams", name, dp, len(dgrams))
			}
			total := dp[0] + dp[1]
			for i, d := range dgrams {
				// Lose one shard of each set.
				if total > 0 && i%total == 1 {
					continue
				}
				o := open(c, d)
				if o == nil {
					t.Fatalf("%s %v: datagram %d does not open", name, dp, i)
				}
				dec.decode(o, func(p []byte) { got = append(got, append([]byte(nil), p...)) })
			}
			if dp[0] == 0 {
				if len(got) != len(sent) {
					t.Fatalf("%s: %d of %d", name, len(got), len(sent))
				}
				continue
			}
			if len(got) != len(sent) {
				t.Fatalf("%s %v: %d of %d packets, the lost ones not recovered", name, dp, len(got), len(sent))
			}
			seen := map[string]bool{}
			for _, p := range got {
				seen[string(p)] = true
			}
			for i, p := range sent {
				if !seen[string(p)] {
					t.Fatalf("%s %v: packet %d missing", name, dp, i)
				}
			}
		}
	}
}

func TestFECSkipsParityAfterAPause(t *testing.T) {
	f, _ := newFramer(nil, 2, 1)
	n := 0
	emit := func([]byte) { n++ }
	p := make([]byte, Overhead)
	f.encode(p, 0, emit)
	f.encode(p, 10, emit) // a set, quick: parity
	f.encode(p, 20, emit)
	f.encode(p, 20+fecEncodeLatency, emit) // a set after a pause: none
	if n != 5 {
		t.Fatalf("%d datagrams, want 5", n)
	}
	if f.enc.next != 6 {
		t.Fatalf("next seqid %d, want 6: the skipped parity keeps its seqid", f.enc.next)
	}
}
