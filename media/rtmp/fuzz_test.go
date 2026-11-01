package rtmp

import (
	"testing"

	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/flv"
)

// FuzzConnFeed feeds arbitrary chunks, in two pieces, to a server Conn
// past its handshake, with every message going on to the command parser
// and an FLV decoder as the server's would: nothing may panic, and no
// message may exceed the size limit.
func FuzzConnFeed(f *testing.F) {
	p := newPipe(f, true)
	_ = p.client.SetChunkSize(50)
	_ = p.client.WriteCommand(0, "connect", 1, amf.Object{{Key: "app", Value: "live"}})
	_ = p.client.WriteMessage(&Message{Type: TypeVideo, StreamID: 1, Timestamp: 0x1000000, Payload: make([]byte, 120)})
	_ = p.client.WriteMessage(&Message{Type: TypeAudio, StreamID: 1, Timestamp: 5, ChunkStreamID: 400, Payload: []byte{0xaf, 1, 2}})
	_ = p.client.WriteWindowAckSize(10)
	_ = p.client.WriteUserControl(EventPingRequest, 1)
	f.Add(p.toServer, 7)
	agg := []byte{TypeVideo, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0xaa, 0, 0, 0, 12}
	f.Add(append(chunkHeader(0, 7, 0, uint32(len(agg)), TypeAggregate, 1), agg...), 3)
	f.Fuzz(func(t *testing.T, b []byte, split int) {
		dec := flv.NewDecoder(flv.DecoderConfig{})
		c := serverConn(t, func(m *Message) error {
			if len(m.Payload) > 1000 {
				t.Fatalf("message of %d bytes", len(m.Payload))
			}
			_, _ = ParseCommand(m)
			ingestMessage(dec, m)
			return nil
		}, nil)
		if split < 0 || split > len(b) {
			split = len(b) / 2
		}
		if c.Feed(b[:split]) == nil {
			_ = c.Feed(b[split:])
		}
	})
}

// FuzzHandshake feeds arbitrary bytes as C0, C1 and C2 to a server, and as
// S0, S1 and S2 to a client.
func FuzzHandshake(f *testing.F) {
	f.Add(clientHello(false))
	reply, _ := serverHandshake(clientHello(false))
	f.Add(reply)
	f.Fuzz(func(t *testing.T, b []byte) {
		s := NewConn(ConnConfig{Output: func([]byte) error { return nil }})
		_ = s.Feed(b)
		c := NewConn(ConnConfig{Client: true, Output: func([]byte) error { return nil }})
		_ = c.Feed(b)
	})
}
