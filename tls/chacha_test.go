//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// The ChaCha20-Poly1305 suites are the record layer's own, with the AEAD of
// RFC 8439 that the QUIC half shares: TLS 1.3's, whose records are protected
// as AES-GCM's are, only with another AEAD, and RFC 7905's TLS 1.2 ones, whose
// nonce is the key block's 12-byte salt XORed with the sequence number, with no
// explicit part in the record.
func TestChaCha20SuiteLookup(t *testing.T) {
	for _, tc := range []struct {
		version uint16
		id      uint16
		saltLen int
	}{
		{stdtls.VersionTLS13, stdtls.TLS_CHACHA20_POLY1305_SHA256, 0},
		{stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, 12},
		{stdtls.VersionTLS12, stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256, 12},
	} {
		s, ok := lookupSuite(tc.version, tc.id)
		if !ok {
			t.Fatalf("%#04x %#04x: no suite", tc.version, tc.id)
		}
		if s.keyLen != 32 || s.hash().Size() != 32 || s.newAEAD == nil || s.saltLen() != tc.saltLen {
			t.Fatalf("%#04x %#04x: suite %+v", tc.version, tc.id, s)
		}
		if _, err := s.aead(make([]byte, 31)); err == nil {
			t.Fatalf("%#04x %#04x: a 31-byte key was taken", tc.version, tc.id)
		}
		aead, err := s.aead(make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
		if aead.NonceSize() != 12 || aead.Overhead() != gcmTagLen {
			t.Fatalf("%#04x %#04x: AEAD sizes %d, %d", tc.version, tc.id, aead.NonceSize(), aead.Overhead())
		}
	}
	// The suites the record layer protected before keep AES-GCM.
	if s, _ := lookupSuite(stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256); s.newAEAD != nil {
		t.Fatal("an AES-GCM suite builds another AEAD")
	}
}

// A record of every length around the tag and a record's plaintext seals and
// opens whole over either version, with the shape RFC 8446 and RFC 7905 give
// it, and any bit flipped in one fails to open.
func TestChaCha20Records(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint16
		suite   uint16
		// inner says the record's type is the last byte of its plaintext, as
		// TLS 1.3's is, and not its outer type.
		inner bool
	}{
		{"TLS13", stdtls.VersionTLS13, stdtls.TLS_CHACHA20_POLY1305_SHA256, true},
		{"TLS12", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, false},
		{"TLS12-RSA", stdtls.VersionTLS12, stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seal, open := recordPair(t, tc.version, tc.suite)
			inner := 0
			if tc.inner {
				inner = 1
			}
			if seal.prefixLen() != recordHeaderLen || seal.suffixLen() != inner+gcmTagLen {
				t.Fatalf("prefix %d, suffix %d", seal.prefixLen(), seal.suffixLen())
			}
			for _, n := range []int{0, 1, 15, 16, 17, 64, 255, 1000, maxPlaintext} {
				payload := bytes.Repeat([]byte{byte(n)}, n)
				record, err := seal.seal(nil, recordTypeApplicationData, payload[:n/2], payload[n/2:])
				if err != nil {
					t.Fatal(err)
				}
				if len(record) != recordHeaderLen+n+inner+gcmTagLen {
					t.Fatalf("%d bytes: record is %d bytes, want %d", n, len(record), recordHeaderLen+n+inner+gcmTagLen)
				}
				typ, plain, alert, ok := open.open(bytes.Clone(record))
				if !ok || typ != recordTypeApplicationData || !bytes.Equal(plain, payload) {
					t.Fatalf("%d bytes: type %d, %d bytes, alert %d, open %v", n, typ, len(plain), alert, ok)
				}
			}
			// The type the caller sealed under is what comes back, for a
			// TLS 1.2 record, and is hidden inside for a TLS 1.3 one.
			for _, typ := range []byte{recordTypeAlert, recordTypeHandshake, recordTypeApplicationData} {
				record, err := seal.seal(nil, typ, []byte{1, 2, 3}, nil)
				if err != nil {
					t.Fatal(err)
				}
				got, plain, _, ok := open.open(bytes.Clone(record))
				if !ok || len(plain) != 3 {
					t.Fatalf("type %d: open %v, %d bytes", typ, ok, len(plain))
				}
				if !tc.inner && got != typ {
					t.Fatalf("type %d came back as %d", typ, got)
				}
				if tc.inner && record[0] != recordTypeApplicationData {
					t.Fatalf("a TLS 1.3 record's outer type is %d", record[0])
				}
			}
			// One bit flipped in the record fails it, but for the length
			// bytes of a TLS 1.2 record: those are the framing's, and what
			// that record authenticates is the length it was given, as
			// crypto/tls's TLS 1.2 records do. A frame whose length was
			// altered reads other bytes, and fails with them.
			record, err := seal.seal(nil, recordTypeApplicationData, bytes.Repeat([]byte{9}, 100), nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := range record {
				if !tc.inner && i >= 3 && i < recordHeaderLen {
					continue
				}
				bad := bytes.Clone(record)
				bad[i] ^= 0x80
				if _, _, _, ok := open.open(bad); ok {
					t.Fatalf("a record with bit 7 of byte %d flipped opened", i)
				}
			}
		})
	}
}

// A record sealed at one sequence number does not open at another, nor out of
// order: the nonce is the salt and the sequence number together.
func TestChaCha20RecordSequence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint16
		suite   uint16
	}{
		{"TLS13", stdtls.VersionTLS13, stdtls.TLS_CHACHA20_POLY1305_SHA256},
		{"TLS12", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seal, open := recordPair(t, tc.version, tc.suite)
			first, err := seal.seal(nil, recordTypeApplicationData, []byte("first"), nil)
			if err != nil {
				t.Fatal(err)
			}
			second, err := seal.seal(nil, recordTypeApplicationData, []byte("second"), nil)
			if err != nil {
				t.Fatal(err)
			}
			// The receiver's sequence number is the sender's.
			open.seq = 1
			if _, plain, _, ok := open.open(bytes.Clone(first)); ok {
				t.Fatalf("the record of sequence 0 opened at 1: %q", plain)
			}
			if _, plain, _, ok := open.open(bytes.Clone(second)); !ok || string(plain) != "second" {
				t.Fatal("the record of sequence 1 did not open at 1")
			}
			open.seq = 0
			if _, plain, _, ok := open.open(bytes.Clone(second)); ok {
				t.Fatalf("the record of sequence 1 opened at 0: %q", plain)
			}
		})
	}
}

// A KeyUpdate derives the ChaCha20-Poly1305 keys of the next generation, which
// open what they seal and not what the generation before them sealed.
func TestChaCha20KeyUpdate(t *testing.T) {
	s, _ := lookupSuite(stdtls.VersionTLS13, stdtls.TLS_CHACHA20_POLY1305_SHA256)
	secret := bytes.Repeat([]byte{7}, s.hash().Size())
	old, err := newKeys13(s, s.hash, secret)
	if err != nil {
		t.Fatal(err)
	}
	next, err := old.next()
	if err != nil {
		t.Fatal(err)
	}
	if next.mode != old.mode || next.keyLen != old.keyLen || next.newAEAD == nil || bytes.Equal(next.key, old.key) {
		t.Fatalf("the next generation is %+v", next)
	}
	record, err := next.seal(nil, recordTypeApplicationData, []byte("updated"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := old.open(bytes.Clone(record)); ok {
		t.Fatal("a record sealed under the next keys opened under the old ones")
	}
	receiver, err := newKeys13(s, s.hash, secret)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err = receiver.next()
	if err != nil {
		t.Fatal(err)
	}
	if _, plain, _, ok := receiver.open(bytes.Clone(record)); !ok || string(plain) != "updated" {
		t.Fatal("the next generation's keys did not open their record")
	}
}

// Two engines whose handshakes run without a worker settle on ChaCha20-Poly1305
// when the CPU has no AES-GCM to prefer, over TLS 1.3 and 1.2, and carry data
// both ways under it, across a KeyUpdate.
func TestNativeChaCha20Handshake(t *testing.T) {
	old := aesGCMHardware
	defer func() { aesGCMHardware = old }()
	aesGCMHardware = false
	for _, tc := range []struct {
		name    string
		version uint16
		suite   uint16
	}{
		{"TLS13", stdtls.VersionTLS13, stdtls.TLS_CHACHA20_POLY1305_SHA256},
		{"TLS12", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverConfig, clientConfig := tlsConfigs(t)
			serverConfig.MinVersion, serverConfig.MaxVersion = tc.version, tc.version
			clientConfig.MinVersion, clientConfig.MaxVersion = tc.version, tc.version
			serverLayers := make(layerOf, 2)
			_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, serverLayers.handler()))
			client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
			clientLayers := make(chan *layer, 1)
			var mu sync.Mutex
			var got []byte
			received := make(chan struct{}, 16)
			handler := &settledHandler{HandlerFuncs: fib.HandlerFuncs{Data: func(_ *fib.Connection, b []byte) {
				mu.Lock()
				got = append(got, b...)
				mu.Unlock()
				received <- struct{}{}
			}}, layers: clientLayers}
			if err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, handler, nil); err != nil {
				t.Fatal(err)
			}
			cl, sl := <-clientLayers, <-serverLayers
			for _, l := range []*layer{cl, sl} {
				state, ok := ConnectionState(l.c)
				if !ok || state.Version != tc.version || state.CipherSuite != tc.suite {
					t.Fatalf("settled on suite %#04x version %#04x, want %#04x %#04x",
						state.CipherSuite, state.Version, tc.suite, tc.version)
				}
				if l.conn != nil || l.tx == nil || l.rx == nil {
					t.Fatal("the handshake did not run without crypto/tls")
				}
			}
			want := []byte{}
			for i := 0; i < 3; i++ {
				if i == 1 && tc.version == stdtls.VersionTLS13 {
					// Asked for, and answered, under the new keys. TLS 1.2 has
					// no KeyUpdate.
					requestKeyUpdate(t, cl)
				}
				msg := bytes.Repeat([]byte{byte('a' + i)}, 20000)
				want = append(want, msg...)
				if err := cl.c.Send(msg); err != nil {
					t.Fatal(err)
				}
				waitFor(t, &mu, &got, len(want), received)
			}
			mu.Lock()
			defer mu.Unlock()
			if !bytes.Equal(got, want) {
				t.Fatal("echo mismatch across the key update")
			}
		})
	}
}

// No kernel's TLS has a cipher for ChaCha20-Poly1305, so a file sent over it
// is sealed here and never handed to the kernel, which would seal it with
// AES-GCM instead and the peer would refuse it. AES-GCM, which shares TLS
// 1.3's mode, still fits.
func TestChaCha20NotKernelTLS(t *testing.T) {
	for _, tc := range []struct {
		version uint16
		suite   uint16
	}{
		{stdtls.VersionTLS13, stdtls.TLS_CHACHA20_POLY1305_SHA256},
		{stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256},
		{stdtls.VersionTLS12, stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256},
	} {
		k, _ := recordPair(t, tc.version, tc.suite)
		if k.kernelAESGCM() || kernelTLSFits(k) {
			t.Fatalf("%#04x %#04x: the kernel would seal ChaCha20-Poly1305", tc.version, tc.suite)
		}
	}
	for _, tc := range []struct {
		version uint16
		suite   uint16
	}{
		{stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256},
		{stdtls.VersionTLS13, stdtls.TLS_AES_256_GCM_SHA384},
		{stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
	} {
		k, _ := recordPair(t, tc.version, tc.suite)
		if !k.kernelAESGCM() {
			t.Fatalf("%#04x %#04x: the kernel would not seal AES-GCM", tc.version, tc.suite)
		}
	}
}
