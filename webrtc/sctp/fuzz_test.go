package sctp

import (
	"encoding/binary"
	"testing"
	"time"
)

// FuzzParse runs the chunk and parameter parsers over arbitrary chunk
// bytes: each returns an error or values, and never panics.
func FuzzParse(f *testing.F) {
	f.Add([]byte{chunkData, 3, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 51, 'x'})
	f.Add([]byte{chunkSack, 0, 0, 20, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 3})
	f.Add([]byte{chunkInit, 0, 0, 28, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1, 0x80, 0x08, 0, 6, 130, 192, 0, 0})
	f.Add([]byte{chunkReconfig, 0, 0, 22, 0, 13, 0, 18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		for len(b) > 0 {
			_, _, v, rest, err := nextChunk(b)
			if err != nil {
				return
			}
			b = rest
			var ic initChunk
			_ = parseInit(v, &ic)
			var s sackChunk
			_ = parseSack(v, &s)
			for p := v; len(p) > 0; {
				_, _, r, err := nextParam(p)
				if err != nil {
					break
				}
				p = r
			}
		}
	})
}

// FuzzAssociation feeds an established association arbitrary chunks in a
// packet with a correct tag and checksum, and then checks it still works
// or has closed cleanly.
func FuzzAssociation(f *testing.F) {
	f.Add([]byte{chunkData, 3, 0, 17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 51, 'x'})
	f.Add([]byte{chunkData, 2, 0, 17, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 51, 'x', 0, 0, 0, chunkData, 1, 0, 17, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 51, 'y'})
	f.Add([]byte{chunkForwardTSN, 0, 0, 12, 0, 0, 0, 9, 0, 0, 0, 3})
	f.Add([]byte{chunkSack, 0, 0, 20, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 3})
	f.Add([]byte{chunkReconfig, 0, 0, 22, 0, 13, 0, 18, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})
	f.Add([]byte{chunkHeartbeat, 0, 0, 8, 0, 1, 0, 4})
	f.Add([]byte{chunkShutdown, 0, 0, 8, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, chunks []byte) {
		l := newLink(t, 1, func(i int, c *Config) {
			c.OnMessage = func(uint16, uint32, []byte) {}
			c.OnDataChannel = func(*DataChannel) {}
		})
		l.connect(false)
		b := l.ends[1]
		// Relative TSNs in DATA and FORWARD TSN chunks would rarely hit
		// the window by chance; offset them by the peer's.
		p := make([]byte, headerSize, headerSize+len(chunks))
		p = append(p, chunks...)
		for c := p[headerSize:]; len(c) >= 8; {
			typ, _, v, rest, err := nextChunk(c)
			if err != nil {
				break
			}
			if (typ == chunkData || typ == chunkForwardTSN) && len(v) >= 4 {
				binary.BigEndian.PutUint32(v, binary.BigEndian.Uint32(v)+b.peerCum)
			}
			if typ == chunkSack && len(v) >= 4 {
				binary.BigEndian.PutUint32(v, binary.BigEndian.Uint32(v)+b.cumAck)
			}
			c = rest
		}
		binary.BigEndian.PutUint32(p[4:], b.localTag)
		binary.LittleEndian.PutUint32(p[8:], checksum(p))
		_ = b.HandlePacket(p, l.now)
		l.run(time.Minute, nil)
		if l.closed[1] {
			return
		}
		got := false
		b.cfg.OnMessage = func(_ uint16, ppid uint32, _ []byte) { got = got || ppid == 1234 }
		// The probe goes on a stream the input opened no channel on.
		probe := uint16(100)
		for b.inUse(probe) {
			probe++
		}
		_ = l.ends[0].Send(probe, 1234, []byte("probe"), SendOptions{Unordered: true})
		l.run(10*time.Minute, func() bool { return got || l.closed[1] || l.closed[0] })
		if !got && !l.closed[1] && !l.closed[0] {
			t.Fatalf("association stopped delivering: %v %v", l.ends[0].State(), b.State())
		}
	})
}
