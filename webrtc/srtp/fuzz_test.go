package srtp

import (
	"bytes"
	"testing"
)

// FuzzUnprotectRTP feeds arbitrary packets to every profile's
// UnprotectRTP: none panics, and none but an authentic packet passes.
func FuzzUnprotectRTP(f *testing.F) {
	for _, p := range profiles {
		tx, _ := newPair(f, p, nil)
		f.Add(mustProtect(f, tx, rtpPacket(1, 1, 30)))
	}
	f.Add(rtpPacket(1, 1, 0))
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, p := range profiles {
			tx, rx := newPair(t, p, nil)
			if _, err := rx.UnprotectRTP(nil, b); err == nil {
				// Only the seed packets are authentic; anything else
				// passing is a forgery.
				enc, _ := tx.ProtectRTP(nil, rtpPacket(1, 1, 30))
				if !bytes.Equal(enc, b) {
					t.Fatalf("%v: forged packet accepted: %x", p, b)
				}
			}
			// In place, over a copy, must not panic either.
			c := bytes.Clone(b)
			rx.UnprotectRTP(c[:0], c)
		}
	})
}

// FuzzUnprotectRTCP is FuzzUnprotectRTP for SRTCP.
func FuzzUnprotectRTCP(f *testing.F) {
	for _, p := range profiles {
		tx, _ := newPair(f, p, nil)
		enc, _ := tx.ProtectRTCP(nil, rtcpPacket(1, 1))
		f.Add(enc)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, p := range profiles {
			tx, rx := newPair(t, p, nil)
			if _, err := rx.UnprotectRTCP(nil, b); err == nil {
				enc, _ := tx.ProtectRTCP(nil, rtcpPacket(1, 1))
				if !bytes.Equal(enc, b) {
					t.Fatalf("%v: forged packet accepted: %x", p, b)
				}
			}
			c := bytes.Clone(b)
			rx.UnprotectRTCP(c[:0], c)
		}
	})
}

// FuzzRoundTrip protects arbitrary RTP payloads with every profile and
// unprotects them again.
func FuzzRoundTrip(f *testing.F) {
	f.Add(uint16(65535), []byte("payload"))
	f.Fuzz(func(t *testing.T, seq uint16, payload []byte) {
		pkt := append(rtpPacket(2, seq, 0), payload...)
		for _, p := range profiles {
			tx, rx := newPair(t, p, nil)
			enc, err := tx.ProtectRTP(nil, pkt)
			if err != nil {
				t.Fatal(err)
			}
			dec, err := rx.UnprotectRTP(nil, enc)
			if err != nil || !bytes.Equal(dec, pkt) {
				t.Fatalf("%v: %v", p, err)
			}
			enc, err = tx.ProtectRTCP(nil, append(rtcpPacket(2, 1), payload...))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := rx.UnprotectRTCP(nil, enc); err != nil {
				t.Fatalf("%v: RTCP: %v", p, err)
			}
		}
	})
}
