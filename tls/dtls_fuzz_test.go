package tls

import (
	"bytes"
	"testing"
	"time"
)

// dtlsFuzzSeeds are the datagrams of a handshake and of some application
// data, each way.
func dtlsFuzzSeeds(f *testing.F) [][]byte {
	cc, sc := webrtcConfigs(f, "ecdsa", "ecdsa")
	n := newDTLSNet(f, 1)
	var seeds [][]byte
	n.drop = func(_ int, b []byte) bool {
		seeds = append(seeds, bytes.Clone(b))
		return false
	}
	profiles := DTLSOptions{SRTPProfiles: []uint16{SRTP_AEAD_AES_128_GCM}, MTU: 300}
	n.pair(cc, sc, profiles, profiles)
	n.handshake(time.Minute)
	n.conns[0].Write([]byte("ping"))
	n.conns[1].Write([]byte("pong"))
	n.conns[0].Close()
	return seeds
}

// FuzzDTLSDatagram hands arbitrary datagrams to a server that has yet to hear
// from its client, with and without the cookie exchange, and to a client
// waiting for the server's first answer: nothing may panic, and a server keeps
// nothing until a hello with its cookie.
func FuzzDTLSDatagram(f *testing.F) {
	for _, s := range dtlsFuzzSeeds(f) {
		f.Add(byte(0), s)
		f.Add(byte(1), s)
		f.Add(byte(2), s)
	}
	f.Add(byte(0), []byte{22, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	cc, sc := webrtcConfigs(f, "ecdsa", "ecdsa")
	f.Fuzz(func(t *testing.T, mode byte, data []byte) {
		now := time.Unix(1_000_000, 0)
		out := func([]byte) {}
		opts := DTLSOptions{Output: out, SRTPProfiles: []uint16{SRTP_AES128_CM_HMAC_SHA1_80, SRTP_AEAD_AES_128_GCM}}
		switch mode % 3 {
		case 0:
			s, _ := NewDTLSServer(sc, opts)
			s.HandleDatagram(data, now)
			if s.srv != nil {
				t.Fatal("server keeps state for a hello without its cookie")
			}
		case 1:
			opts.InsecureSkipHelloVerify = true
			s, _ := NewDTLSServer(sc, opts)
			s.HandleDatagram(bytes.Clone(data), now)
			s.HandleDatagram(data, now)
			s.HandleTimeout(now.Add(time.Hour))
		case 2:
			c, _ := NewDTLSClient(cc, opts)
			c.Start(now)
			c.HandleDatagram(bytes.Clone(data), now)
			c.HandleDatagram(data, now)
			c.HandleTimeout(now.Add(time.Hour))
		}
	})
}

// FuzzDTLSHandshake runs a handshake and alters one of its datagrams, the
// one after step steps, by XORing patch into it at offset: whatever comes of
// it, nothing may panic, and a side that completes agrees with the other.
func FuzzDTLSHandshake(f *testing.F) {
	f.Add(uint8(0), uint16(13), []byte{1})
	f.Add(uint8(2), uint16(25), []byte{0xff, 0xff})
	f.Add(uint8(3), uint16(40), []byte{0x80})
	f.Add(uint8(4), uint16(1), []byte{0x01})
	f.Add(uint8(5), uint16(11), []byte{0xff, 0xff, 0xff})
	f.Add(uint8(6), uint16(30), []byte{0, 0, 0, 0, 0, 0, 1})
	f.Add(uint8(7), uint16(9), []byte{2})
	cc, sc := webrtcConfigs(f, "ecdsa", "ecdsa")
	f.Fuzz(func(t *testing.T, step uint8, offset uint16, patch []byte) {
		if len(patch) == 0 {
			return
		}
		n := newDTLSNet(t, uint64(step))
		opts := DTLSOptions{SRTPProfiles: []uint16{SRTP_AEAD_AES_128_GCM}, MTU: 300}
		n.pair(cc, sc, opts, opts)
		altered := false
		n.drop = func(to int, b []byte) bool {
			if step > 0 {
				step--
				return false
			}
			if altered {
				return false
			}
			altered = true
			at := int(offset) % len(b)
			for i, p := range patch {
				if at+i < len(b) {
					b[at+i] ^= p
				}
			}
			return false
		}
		n.handshake(10 * time.Minute)
		for i := 0; i < 200 && n.step(); i++ {
		}
		if n.errs[0] == nil && n.errs[1] == nil && n.conns[0].HandshakeComplete() && n.conns[1].HandshakeComplete() {
			ck, _ := n.conns[0].ExportKeyingMaterial("EXTRACTOR-dtls_srtp", nil, 32)
			sk, _ := n.conns[1].ExportKeyingMaterial("EXTRACTOR-dtls_srtp", nil, 32)
			if !bytes.Equal(ck, sk) {
				t.Fatal("both sides complete with different keys")
			}
		}
	})
}

// TestDTLSMalformed hands a server and a client records that are cut short,
// too long, of unknown epochs and types, and fragments that claim more than
// they carry.
func TestDTLSMalformed(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	hs := func(typ byte, length, seq, off, n int, body []byte) []byte {
		b := []byte{typ, byte(length >> 16), byte(length >> 8), byte(length), byte(seq >> 8), byte(seq),
			byte(off >> 16), byte(off >> 8), byte(off), byte(n >> 16), byte(n >> 8), byte(n)}
		return append(b, body...)
	}
	rec := func(typ byte, epoch uint16, body []byte) []byte {
		b := []byte{typ, 0xfe, 0xfd, byte(epoch >> 8), byte(epoch), 0, 0, 0, 0, 0, 1, byte(len(body) >> 8), byte(len(body))}
		return append(b, body...)
	}
	datagrams := [][]byte{
		{},
		{22},
		rec(22, 0, nil)[:12],
		append(rec(22, 0, hs(1, 10, 0, 0, 10, make([]byte, 10))), 0xff),
		rec(22, 0, hs(1, 100, 0, 0, 200, make([]byte, 10))),
		rec(22, 0, hs(1, 1<<20, 0, 0, 10, make([]byte, 10))),
		rec(22, 0, hs(1, 10, 0, 5, 10, make([]byte, 10))),
		rec(22, 0, hs(2, 10, 0, 0, 10, make([]byte, 10))),
		rec(22, 0, hs(3, 3, 0, 0, 3, []byte{0xfe, 0xff, 0})),
		rec(22, 0, hs(20, 12, 0, 0, 12, make([]byte, 12))),
		rec(22, 1, make([]byte, 40)),
		rec(22, 7, make([]byte, 40)),
		rec(23, 0, []byte("data")),
		rec(21, 0, []byte{2}),
		rec(20, 0, []byte{1}),
		rec(99, 0, []byte{1}),
		{22, 0x03, 0x03, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for i, d := range datagrams {
		s, _ := NewDTLSServer(sc, DTLSOptions{Output: func([]byte) {}, InsecureSkipHelloVerify: true})
		if err := s.HandleDatagram(bytes.Clone(d), time.Now()); err != nil {
			t.Errorf("server, datagram %d: %v", i, err)
		}
		var out int
		c, _ := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) { out++ }})
		c.Start(time.Now())
		err := c.HandleDatagram(bytes.Clone(d), time.Now())
		if i == 8 {
			// An empty HelloVerifyRequest is one: the hello goes again.
			if err != nil || out != 2 {
				t.Errorf("client, empty HelloVerifyRequest: %v, %d datagrams", err, out)
			}
			continue
		}
		if i == 3 || i == 7 {
			// A ClientHello, or a ServerHello that does not parse, in the
			// place of the server's first message fails the handshake.
			if err == nil {
				t.Errorf("client took datagram %d", i)
			}
			continue
		}
		if err != nil {
			t.Errorf("client, datagram %d: %v", i, err)
		}
	}
	// A fatal alert in the clear fails a handshake that has begun.
	c, _ := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) {}})
	c.Start(time.Now())
	if err := c.HandleDatagram(rec(21, 0, []byte{2, 40}), time.Now()); err == nil {
		t.Fatal("client ignored a fatal alert")
	}
}
