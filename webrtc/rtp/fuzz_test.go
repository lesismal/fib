package rtp

import (
	"bytes"
	"testing"
)

// FuzzPacket parses arbitrary bytes as RTP: whatever it is given, it
// either fails or yields a packet that marshals back to the same bytes,
// whose extensions can be walked and rewritten.
func FuzzPacket(f *testing.F) {
	f.Add(unhex(f, chromePacket))
	f.Add([]byte{0x80, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3})
	f.Add([]byte{0xb1, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0x10, 0x00, 0x00, 0x01, 1, 2, 9, 9, 0, 2})
	f.Fuzz(func(t *testing.T, b []byte) {
		var p Packet
		if err := p.Unmarshal(b); err != nil {
			return
		}
		out, err := p.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		// Padding bytes other than the count need not be zero.
		if n := len(out) - int(p.PaddingSize); !bytes.Equal(out[:n], b[:n]) || len(out) != len(b) {
			t.Fatalf("round trip\n got %x\nwant %x", out, b)
		}
		for id, payload := range p.Extensions() {
			if got := p.GetExtension(id); got == nil && len(payload) > 0 {
				t.Fatalf("extension %d yielded but not found", id)
			}
		}
		if err := p.SetExtension(7, []byte{1, 2}); err == nil {
			if string(p.GetExtension(7)) != "\x01\x02" {
				t.Fatal("extension set but not found")
			}
			if _, err := p.Append(nil); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// FuzzRTCP parses arbitrary bytes as a compound RTCP packet: whatever it
// is given, it either fails or yields packets that marshal and parse back.
func FuzzRTCP(f *testing.F) {
	b, _ := AppendRTCP(nil, samplePackets()...)
	f.Add(b)
	for _, p := range samplePackets() {
		b, _ := p.Append(nil)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		pkts, err := UnmarshalRTCP(b)
		if err != nil {
			return
		}
		for _, p := range pkts {
			out, err := p.Append(nil)
			if err != nil {
				// Values a parse can yield but a marshal refuses, such as
				// SDES items of type END, are fine to refuse.
				continue
			}
			if len(out) != p.MarshalSize() {
				t.Fatalf("%T: marshaled %d bytes, MarshalSize %d", p, len(out), p.MarshalSize())
			}
			again, err := UnmarshalRTCP(out)
			if err != nil || len(again) != 1 {
				t.Fatalf("%T: reparse %v\n%x", p, err, out)
			}
			out2, err := again[0].Append(nil)
			if err != nil || !bytes.Equal(out, out2) {
				t.Fatalf("%T: not stable\n%x\n%x", p, out, out2)
			}
		}
	})
}

// FuzzTransportCC builds feedback from arbitrary status symbols and deltas
// and reads it back.
func FuzzTransportCC(f *testing.F) {
	f.Add([]byte{1, 1, 0, 2}, []byte{0, 4, 9, 9, 3})
	f.Fuzz(func(t *testing.T, status []byte, deltaBytes []byte) {
		p := TransportCC{BaseSeq: 9}
		for i, s := range status {
			s %= 3
			p.Status = append(p.Status, s)
			var d int16
			if i < len(deltaBytes) {
				d = int16(deltaBytes[i])
			}
			switch s {
			case TCCSmallDelta:
				p.Deltas = append(p.Deltas, d)
			case TCCLargeDelta:
				p.Deltas = append(p.Deltas, -d*100-1)
			}
		}
		b, err := p.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		var q TransportCC
		if err := q.Unmarshal(b); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(q.Status, p.Status) || len(q.Deltas) != len(p.Deltas) {
			t.Fatalf("status %v\n got %v", p.Status, q.Status)
		}
		for i := range p.Deltas {
			if q.Deltas[i] != p.Deltas[i] {
				t.Fatalf("delta %d: %d, want %d", i, q.Deltas[i], p.Deltas[i])
			}
		}
	})
}
