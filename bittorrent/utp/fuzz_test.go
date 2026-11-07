package utp

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"
)

func FuzzParse(f *testing.F) {
	var b [HeaderSize + 6 + 3]byte
	putHeader(b[:], header{typ: stState, ext: extSACK, connID: 7, ts: 1, tsDiff: 2, wnd: 3, seq: 4, ack: 5})
	b[20], b[21] = 0, 4
	f.Add(b[:])
	f.Add([]byte("d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"))
	f.Add(make([]byte, 16))
	f.Fuzz(func(t *testing.T, b []byte) {
		h, sack, payload, ok := parse(b)
		if !ok {
			return
		}
		if !IsUTP(b) {
			t.Fatal("parsed what IsUTP refuses")
		}
		var re [HeaderSize]byte
		putHeader(re[:], h)
		if !bytes.Equal(re[:], b[:HeaderSize]) {
			t.Fatalf("header round trip: %x != %x", re, b[:HeaderSize])
		}
		if len(payload) > len(b)-HeaderSize || len(sack) > len(b) {
			t.Fatal("slices past the packet")
		}
	})
}

// FuzzConn hands an established connection packets of any content, from
// its peer's address and with its connection ID, to check that nothing a
// peer sends breaks it.
func FuzzConn(f *testing.F) {
	pkt := func(typ uint8, seq, ack uint16, ext []byte, payload string) []byte {
		b := make([]byte, HeaderSize, HeaderSize+len(ext)+len(payload))
		h := header{typ: typ, connID: 0, ts: 99, tsDiff: 50, wnd: 1 << 16, seq: seq, ack: ack}
		if len(ext) > 0 {
			h.ext = extSACK
		}
		putHeader(b, h)
		b = append(b, ext...)
		return append(b, payload...)
	}
	f.Add(pkt(stData, 101, 1000, nil, "hello"), pkt(stState, 101, 1002, []byte{0, 4, 0xff, 0, 0, 1}, ""))
	f.Add(pkt(stFin, 105, 999, nil, ""), pkt(stData, 103, 1001, nil, "x"))
	f.Add(pkt(stReset, 1, 1, nil, ""), pkt(stSyn, 1, 1, nil, ""))
	f.Fuzz(func(t *testing.T, p1, p2 []byte) {
		peer := netip.MustParseAddrPort("10.0.0.2:1")
		m := NewMux(Config{}, nil, func(netip.AddrPort, [][]byte) {})
		defer m.Close()
		c := m.newConn(peer, 0, 1, true)
		c.mu.Lock()
		c.state = stateConnected
		c.seqNr, c.ackNr = 1000, 100
		for i := range 5 {
			p := &outPacket{seq: c.seqNr, typ: stData, payload: []byte("abc")}
			c.push(p, int64(i))
			c.transmit(p, int64(i))
		}
		c.mu.Unlock()
		m.mu.Lock()
		m.conns[connKey{peer, 0}] = c
		m.mu.Unlock()
		for _, p := range [][]byte{p1, p2, p1} {
			if len(p) >= 4 {
				binary.BigEndian.PutUint16(p[2:], 0) // our receive ID
			}
			m.HandleDatagram(peer, p)
			m.Flush()
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.inflightPkts < 0 || c.curWindow < 0 || c.reorderCount < 0 || c.reorderBytes < 0 {
			t.Fatalf("inflight %d window %d reorder %d/%d", c.inflightPkts, c.curWindow, c.reorderCount, c.reorderBytes)
		}
	})
}
