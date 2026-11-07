package mse

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	mrand "math/rand/v2"
	"net"
	"testing"
	"time"
)

func randKey() (k [20]byte) {
	_, _ = rand.Read(k[:])
	return k
}

// chunks splits b into pieces of random sizes, single bytes among them.
func chunks(r *mrand.Rand, b []byte, maxChunk int) [][]byte {
	var out [][]byte
	for len(b) > 0 {
		n := 1 + r.IntN(maxChunk)
		n = min(n, len(b))
		out = append(out, b[:n])
		b = b[n:]
	}
	return out
}

// run drives an initiator and a receiver against each other, each side's
// output delivered to the other in chunks of up to maxChunk bytes, plus
// extra bytes each sends right behind the handshake, encrypted as the
// outcome says.
func run(t *testing.T, a *Initiator, b *Receiver, maxChunk int, seed uint64) (*Result, *Result, error) {
	t.Helper()
	r := mrand.New(mrand.NewPCG(seed, seed))
	toB, err := a.Start()
	if err != nil {
		return nil, nil, err
	}
	var toA []byte
	aDone, bDone := false, false
	for i := 0; i < 1000 && !(aDone && bDone); i++ {
		if len(toB) > 0 && !bDone {
			in := toB
			toB = nil
			for _, c := range chunks(r, in, maxChunk) {
				if bDone {
					// What follows the handshake belongs to the payload.
					toB = append(toB, c...)
					continue
				}
				out, done, err := b.Feed(c)
				if err != nil {
					return nil, nil, err
				}
				toA = append(toA, out...)
				bDone = done
			}
		}
		if len(toA) > 0 && !aDone {
			in := toA
			toA = nil
			for _, c := range chunks(r, in, maxChunk) {
				if aDone {
					toA = append(toA, c...)
					continue
				}
				out, done, err := a.Feed(c)
				if err != nil {
					return nil, nil, err
				}
				toB = append(toB, out...)
				aDone = done
			}
		}
	}
	if !aDone || !bDone {
		t.Fatalf("handshake did not finish: a %v b %v", aDone, bDone)
	}
	return a.Result(), b.Result(), nil
}

func TestHandshakeRoles(t *testing.T) {
	for _, tc := range []struct {
		name    string
		provide CryptoMethod
		policy  Policy
		want    CryptoMethod
	}{
		{"both-allow", Both, Allow, Plaintext},
		{"both-prefer", Both, Prefer, RC4},
		{"both-require", Both, Require, RC4},
		{"rc4-allow", RC4, Allow, RC4},
		{"plain-prefer", Plaintext, Prefer, Plaintext},
	} {
		for _, maxChunk := range []int{1, 7, 100, 4096} {
			t.Run(tc.name, func(t *testing.T) {
				skey := randKey()
				var keys KeyTable
				keys.Add(skey)
				ia := []byte("\x13BitTorrent protocol initial payload")
				a := NewInitiator(skey, tc.provide, ia)
				b := NewReceiver(&keys, tc.policy)
				ra, rb, err := run(t, a, b, maxChunk, uint64(maxChunk))
				if err != nil {
					t.Fatal(err)
				}
				if ra.Method != tc.want || rb.Method != tc.want {
					t.Fatalf("methods %v %v, want %v", ra.Method, rb.Method, tc.want)
				}
				if rb.SKey != skey || !bytes.Equal(rb.IA, ia) {
					t.Fatalf("receiver got skey %x ia %q", rb.SKey, rb.IA)
				}
				if (ra.Encrypt != nil) != (tc.want == RC4) {
					t.Fatal("ciphers do not match the method")
				}
				if tc.want == RC4 {
					// The payload continues each keystream where the
					// handshake left it.
					msg := []byte("payload from A to B")
					enc := append([]byte(nil), msg...)
					ra.Encrypt.XORKeyStream(enc, enc)
					rb.Decrypt.XORKeyStream(enc, enc)
					if !bytes.Equal(enc, msg) {
						t.Fatal("A->B payload does not decrypt")
					}
					msg = []byte("payload from B to A")
					enc = append([]byte(nil), msg...)
					rb.Encrypt.XORKeyStream(enc, enc)
					ra.Decrypt.XORKeyStream(enc, enc)
					if !bytes.Equal(enc, msg) {
						t.Fatal("B->A payload does not decrypt")
					}
				}
			})
		}
	}
}

func TestReceiverFindsKeyAmongMany(t *testing.T) {
	var keys KeyTable
	var all [][20]byte
	for range 2000 {
		k := randKey()
		keys.Add(k)
		all = append(all, k)
	}
	if keys.Len() != 2000 {
		t.Fatalf("len %d", keys.Len())
	}
	for _, i := range []int{0, 999, 1999} {
		a := NewInitiator(all[i], Both, nil)
		b := NewReceiver(&keys, Prefer)
		_, rb, err := run(t, a, b, 512, uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		if rb.SKey != all[i] {
			t.Fatalf("found %x, want %x", rb.SKey, all[i])
		}
	}
	keys.Remove(all[5])
	a := NewInitiator(all[5], Both, nil)
	b := NewReceiver(&keys, Prefer)
	if _, _, err := run(t, a, b, 512, 5); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("removed key: %v", err)
	}
}

func TestReceiverRefusals(t *testing.T) {
	var keys KeyTable
	skey := randKey()
	keys.Add(skey)

	// An initiator that provides only plaintext, to a receiver requiring
	// RC4.
	a := NewInitiator(skey, Plaintext, nil)
	b := NewReceiver(&keys, Require)
	if _, _, err := run(t, a, b, 64, 1); !errors.Is(err, ErrNoMethod) {
		t.Fatalf("plaintext-only to Require: %v", err)
	}

	// Garbage never synchronizes.
	b = NewReceiver(&keys, Allow)
	junk := make([]byte, 2000)
	_, _ = rand.Read(junk)
	junk[0] = 0
	if _, _, err := b.Feed(junk); !errors.Is(err, ErrNoSync) && !errors.Is(err, ErrBadKey) {
		t.Fatalf("junk: %v", err)
	}
}

func TestPlaintextDetection(t *testing.T) {
	hs := append([]byte("\x13BitTorrent protocol"), make([]byte, 48)...)
	for _, policy := range []Policy{Allow, Prefer, Require} {
		b := NewReceiver(new(KeyTable), policy)
		var done bool
		var err error
		for i := range hs {
			if _, done, err = b.Feed(hs[i : i+1]); err != nil || done {
				break
			}
		}
		if policy == Require {
			if !errors.Is(err, ErrPlaintextRefused) {
				t.Fatalf("Require: %v", err)
			}
			continue
		}
		if err != nil || !done {
			t.Fatalf("%v: done %v err %v", policy, done, err)
		}
		r := b.Result()
		if !r.PlainHandshake || r.Method != 0 || !bytes.Equal(r.Rest, hs[:20]) {
			t.Fatalf("%v: result %+v", policy, r)
		}
	}
	// A public key that begins like the header, but stops matching, is MSE.
	b := NewReceiver(new(KeyTable), Allow)
	if _, done, err := b.Feed([]byte("\x13BitTorrent protocoX")); err != nil || done {
		t.Fatalf("near miss: done %v err %v", done, err)
	}
	if b.state != rYa {
		t.Fatalf("state %d, want rYa", b.state)
	}
}

func TestPolicySelect(t *testing.T) {
	for _, tc := range []struct {
		p        Policy
		provided CryptoMethod
		want     CryptoMethod
	}{
		{Allow, Both, Plaintext}, {Allow, RC4, RC4}, {Allow, 0, 0},
		{Prefer, Both, RC4}, {Prefer, Plaintext, Plaintext},
		{Require, Both, RC4}, {Require, Plaintext, 0},
	} {
		if got := tc.p.Select(tc.provided); got != tc.want {
			t.Errorf("%v.Select(%v) = %v, want %v", tc.p, tc.provided, got, tc.want)
		}
	}
	if Require.Provide() != RC4 || Prefer.Provide() != Both || Allow.Provide() != Both {
		t.Fatal("Provide")
	}
}

func TestNetConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	skey := randKey()
	var keys KeyTable
	keys.Add(randKey())
	keys.Add(skey)
	for _, tc := range []struct {
		name    string
		provide CryptoMethod
		policy  Policy
		plain   bool
	}{
		{"rc4", Both, Prefer, false},
		{"plaintext-method", Both, Allow, false},
		{"plain-handshake", 0, Allow, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := make(chan error, 1)
			go func() {
				conn, err := ln.Accept()
				if err != nil {
					srv <- err
					return
				}
				defer conn.Close()
				c, err := Server(conn, &keys, tc.policy, 5*time.Second)
				if err != nil {
					srv <- err
					return
				}
				if c.Result.PlainHandshake != tc.plain {
					srv <- errors.New("PlainHandshake wrong")
					return
				}
				buf := make([]byte, 5+20)
				if _, err := io.ReadFull(c, buf); err != nil {
					srv <- err
					return
				}
				_, err = c.Write(bytes.ToUpper(buf))
				srv <- err
			}()
			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var rw io.ReadWriter = conn
			if tc.plain {
				_, _ = conn.Write([]byte("\x13BitTorrent protocol"))
			} else {
				c, err := Client(conn, skey, tc.provide, []byte("\x13BitTorrent protocol"), 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				rw = c
			}
			if _, err := rw.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 25)
			if _, err := io.ReadFull(rw, buf); err != nil {
				t.Fatal(err)
			}
			if string(buf) != "\x13BITTORRENT PROTOCOLHELLO" {
				t.Fatalf("echo %q", buf)
			}
			if err := <-srv; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBadPublicKey(t *testing.T) {
	a := NewInitiator(randKey(), Both, nil)
	if _, err := a.Start(); err != nil {
		t.Fatal(err)
	}
	one := make([]byte, KeySize)
	one[KeySize-1] = 1
	if _, _, err := a.Feed(one); !errors.Is(err, ErrBadKey) {
		t.Fatalf("Y=1: %v", err)
	}
}

func FuzzReceiver(f *testing.F) {
	f.Add([]byte("\x13BitTorrent protocol"))
	f.Add(make([]byte, 200))
	f.Add(bytes.Repeat([]byte{0xff}, 700))
	var keys KeyTable
	keys.Add(randKey())
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, p := range []Policy{Allow, Require} {
			b := NewReceiver(&keys, p)
			data := data
			for len(data) > 0 {
				n := min(len(data), 1+int(data[0])%64)
				_, done, err := b.Feed(data[:n])
				data = data[n:]
				if err != nil || done {
					break
				}
			}
		}
	})
}
