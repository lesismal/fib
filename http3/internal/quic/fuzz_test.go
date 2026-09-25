package quic

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/tlstest"
)

// FuzzParseHeader reads arbitrary datagrams as packet headers, which is the
// first thing an endpoint does with whatever arrives.
func FuzzParseHeader(f *testing.F) {
	f.Add([]byte{0xc0, 0x00, 0x00, 0x00, 0x01, 0x08, 1, 2, 3, 4, 5, 6, 7, 8, 0x00, 0x00, 0x44, 0x00})
	f.Add([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 0x00})
	f.Add([]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x04, 1, 2, 3, 4, 0x04, 5, 6, 7, 8})
	f.Add(make([]byte, minInitialDatagram))
	f.Fuzz(func(t *testing.T, datagram []byte) {
		h, err := parseHeader(datagram, cidLen)
		if err != nil {
			return
		}
		switch {
		case h.end > len(datagram) || h.end < 0:
			t.Fatalf("packet ends at %d of %d bytes", h.end, len(datagram))
		case h.pnOffset > h.end && h.typ != packetRetry:
			t.Fatalf("packet number at %d ends at %d", h.pnOffset, h.end)
		case len(h.dcid) > maxCIDLen || len(h.scid) > maxCIDLen:
			t.Fatalf("connection IDs of %d and %d bytes", len(h.dcid), len(h.scid))
		}
		// Whatever it holds, deciding how to answer it must not panic.
		_ = IsInitial(datagram)
		_ = VersionNegotiation(datagram)
		key := NewResetKey()
		_ = key.StatelessReset(datagram)
	})
}

// FuzzParseParams reads arbitrary transport parameters, which arrive inside
// the peer's handshake.
func FuzzParseParams(f *testing.F) {
	params := defaultParams()
	params.initialSCID, params.hasInitialSCID = []byte("12345678"), true
	f.Add(params.encode())
	f.Add([]byte{0x01, 0x04, 0x80, 0x00, 0x75, 0x30})
	f.Add([]byte{0x0f, 0x08, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, isClient := range []bool{false, true} {
			p, err := parseParams(data, isClient)
			if err != nil {
				continue
			}
			if p.ackDelayExponent > 20 || p.maxAckDelay >= 1<<14*time.Millisecond ||
				p.activeCIDLimit < 2 || p.maxUDPPayloadSize < 1200 {
				t.Fatalf("accepted parameters out of range: %+v", p)
			}
		}
	})
}

// FuzzFrames hands arbitrary 1-RTT payloads to a connection: frames it
// cannot parse end the connection with an error, and nothing panics.
func FuzzFrames(f *testing.F) {
	f.Add([]byte{framePing})
	f.Add([]byte{frameAck, 0x02, 0x00, 0x00, 0x00})
	f.Add([]byte{frameStream | 0x02, 0x00, 0x04, 'b', 'o', 'd', 'y'})
	f.Add([]byte{frameMaxStreamsBidi, 0x41, 0x00})
	f.Add([]byte{frameNewConnectionID, 0x01, 0x00, 0x08, 1, 2, 3, 4, 5, 6, 7, 8,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Add([]byte{frameConnectionCloseApp, 0x41, 0x00, 0x02, 'n', 'o'})
	f.Fuzz(func(t *testing.T, payload []byte) {
		c := fuzzConn(t)
		defer c.tls.Close()
		if _, err := c.handleFrames(spaceApp, payload, time.Now()); err != nil {
			// An error is a connection error, which the caller turns into
			// a CONNECTION_CLOSE; it must carry a code.
			var te *TransportError
			if !asTransportError(err, &te) {
				t.Fatalf("frames failed with %v", err)
			}
		}
		// Whatever state the frames left, building a packet from it is
		// still allowed to run.
		c.mu.Lock()
		c.wantFlush = true
		c.mu.Unlock()
		c.dispatch()
	})
}

func asTransportError(err error, target **TransportError) bool {
	te, ok := err.(*TransportError)
	if ok {
		*target = te
	}
	return ok
}

// fuzzConn is a server connection with its handshake behind it, for frames
// that arrive in 1-RTT packets.
func fuzzConn(t *testing.T) *Conn {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.NextProtos = []string{"fuzz"}
	c := newConn(fuzzPacketConn{}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
		Config{TLSConfig: serverTLS}, fuzzHandler{}, false)
	c.tls = tls.QUICServer(&tls.QUICConfig{TLSConfig: c.config.TLSConfig})
	if err := c.tls.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	params := defaultParams()
	params.initialMaxData = 1 << 20
	params.maxStreamDataBidiL = 1 << 16
	params.maxStreamDataBidiR = 1 << 16
	params.maxStreamDataUni = 1 << 16
	params.maxStreamsBidi = 16
	params.maxStreamsUni = 16
	c.peerParams = &params
	c.peerMaxData = params.initialMaxData
	c.peerMaxBidi, c.peerMaxUni = params.maxStreamsBidi, params.maxStreamsUni
	c.handshakeComplete, c.handshakeConfirmed, c.addressValidated = true, true, true
	return c
}

type fuzzPacketConn struct{}

func (fuzzPacketConn) Send([]byte) error { return nil }
func (fuzzPacketConn) Close() error      { return nil }

type fuzzHandler struct{}

func (fuzzHandler) OnHandshake(*Conn)                  {}
func (fuzzHandler) OnStreamData(*Stream, []byte, bool) {}
func (fuzzHandler) OnStreamReset(*Stream, uint64)      {}
func (fuzzHandler) OnStopSending(*Stream, uint64)      {}
func (fuzzHandler) OnStreamsAvailable(*Conn)           {}
func (fuzzHandler) OnClose(*Conn, error)               {}
