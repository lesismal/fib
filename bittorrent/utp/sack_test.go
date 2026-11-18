package utp

import "testing"

// A selective acknowledgement is counted from the acknowledgement its
// packet carries. The keepalive carries one short of the real one, and a
// mask counted from the real one told the peer the missing packet had
// arrived: libutp then never sent it again, and the transfer stopped
// (TestUTPLibutpDialsFib's lossy run, once in a few hundred).
func TestSelectiveAckCountsFromTheCarriedAck(t *testing.T) {
	c := &Conn{cfg: &Config{RecvBuffer: 1 << 20}}
	c.ackNr = 100
	// 101 is missing; 102 and 104 arrived ahead of it.
	c.in.put(102, []byte{1}, 101)
	c.in.put(104, []byte{1}, 101)
	c.reorderCount = 2

	for _, ack := range []uint16{c.ackNr, c.ackNr - 1} {
		c.dgrams, c.arena = nil, nil
		c.writePacket(stState, 7, ack, nil, 1)
		h, sack, _, ok := parse(c.dgrams[0])
		if !ok || sack == nil || h.ack != ack {
			t.Fatalf("ack %d: parsed %+v, sack %v, ok %v", ack, h, sack, ok)
		}
		for seq := uint16(101); seq <= 105; seq++ {
			i := seq - (h.ack + 2)
			if seq-h.ack < 2 {
				continue // ack+1 is the one missing by definition
			}
			got := sack[i/8]&(1<<(i%8)) != 0
			want := seq == 102 || seq == 104
			if got != want {
				t.Errorf("ack %d: the mask says %d arrived: %v, want %v", ack, seq, got, want)
			}
		}
	}
}
