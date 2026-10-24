package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/tlstest"
)

// scriptTLS is a TLS handshake that plays back the events it is given, and
// fails where it is told to, for the ways a handshake can go wrong that a
// real one does not readily produce.
type scriptTLS struct {
	startErr, handleErr, ticketErr error
	events                         []tls.QUICEvent
	params                         []byte
	closed                         bool
}

func (s *scriptTLS) Start(context.Context) error                          { return s.startErr }
func (s *scriptTLS) HandleData(tls.QUICEncryptionLevel, []byte) error     { return s.handleErr }
func (s *scriptTLS) SetTransportParameters(p []byte)                      { s.params = p }
func (s *scriptTLS) SendSessionTicket(tls.QUICSessionTicketOptions) error { return s.ticketErr }
func (s *scriptTLS) ConnectionState() tls.ConnectionState                 { return tls.ConnectionState{} }
func (s *scriptTLS) Close() error                                         { s.closed = true; return nil }
func (s *scriptTLS) NextEvent() tls.QUICEvent {
	if len(s.events) == 0 {
		return tls.QUICEvent{Kind: tls.QUICNoEvent}
	}
	e := s.events[0]
	s.events = s.events[1:]
	return e
}

// storingTLS is a scriptTLS that stores the sessions it resumes, as the
// handshake of package fib/tls does, and fails to.
type storingTLS struct {
	scriptTLS
	storeErr error
}

func (s *storingTLS) StoreSession(*tls.SessionState) error { return s.storeErr }

// sinkConn is a PacketConn that keeps what is sent through it.
type sinkConn struct {
	mu   sync.Mutex
	sent [][]byte
}

func (s *sinkConn) Send(d []byte) error {
	s.mu.Lock()
	s.sent = append(s.sent, append([]byte(nil), d...))
	s.mu.Unlock()
	return nil
}
func (s *sinkConn) Close() error { return nil }

func (s *sinkConn) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

var testAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}

// testTLSConfigs are the TLS configurations of a test client and server.
func testTLSConfigs(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	server, client, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	server.NextProtos = []string{"test"}
	client.NextProtos = []string{"test"}
	return server, client
}

// dialSink dials a client whose datagrams go nowhere, so that what it is
// sent is up to the test. It is ended with the test.
func dialSink(t *testing.T, configure func(*Config)) (*Conn, *sinkConn, *testHandler) {
	t.Helper()
	_, clientTLS := testTLSConfigs(t)
	config := Config{TLSConfig: clientTLS}
	if configure != nil {
		configure(&config)
	}
	sink, h := &sinkConn{}, newTestHandler()
	c, err := Dial(sink, testAddr, config, h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Abort(errTestOver) })
	return c, sink, h
}

// clientInitial is the first datagram of a client, which a server is
// accepted from.
func clientInitial(t *testing.T) []byte {
	t.Helper()
	_, sink, _ := dialSink(t, nil)
	if sink.count() == 0 {
		t.Fatal("the client sent nothing")
	}
	return sink.sent[0]
}

func wantCode(t *testing.T, err error, code uint64) {
	t.Helper()
	var te *TransportError
	if !errors.As(err, &te) || te.Code != code {
		t.Fatalf("failed with %v, want code 0x%x", err, code)
	}
}

// A connection that cannot start is not returned: Dial says why, Accept
// returns nil, and the TLS handshake made for it is let go.
func TestStartRefused(t *testing.T) {
	serverTLS, clientTLS := testTLSConfigs(t)
	if _, err := Dial(&sinkConn{}, testAddr, Config{}, newTestHandler()); err == nil {
		t.Fatal("dialed without a TLS configuration")
	}
	failing := &scriptTLS{startErr: errors.New("no start")}
	withTLS := func(t TLSConn) func(bool, *tls.Config) TLSConn {
		return func(bool, *tls.Config) TLSConn { return t }
	}
	if _, err := Dial(&sinkConn{}, testAddr, Config{TLSConfig: clientTLS, NewTLS: withTLS(failing)}, newTestHandler()); err == nil || !failing.closed {
		t.Fatalf("dialed with a handshake that does not start: %v, closed %v", err, failing.closed)
	}
	// A handshake that fails at once fails Dial with the alert it sent,
	// or an internal error when it sent none.
	alerting := &scriptTLS{events: []tls.QUICEvent{{Kind: tls.QUICErrorEvent, Err: tls.AlertError(40)}}}
	_, err := Dial(&sinkConn{}, testAddr, Config{TLSConfig: clientTLS, NewTLS: withTLS(alerting)}, newTestHandler())
	wantCode(t, err, errCryptoBase+40)
	broken := &scriptTLS{events: []tls.QUICEvent{{Kind: tls.QUICErrorEvent, Err: errors.New("broken")}}}
	_, err = Dial(&sinkConn{}, testAddr, Config{TLSConfig: clientTLS, NewTLS: withTLS(broken)}, newTestHandler())
	wantCode(t, err, errInternal)

	initial := clientInitial(t)
	if c := Accept(&sinkConn{}, testAddr, Config{}, newTestHandler(), initial); c != nil {
		t.Fatal("accepted without a TLS configuration")
	}
	if c := Accept(&sinkConn{}, testAddr, Config{TLSConfig: serverTLS}, newTestHandler(), initial[:20]); c != nil {
		t.Fatal("accepted from a datagram that is not an Initial packet")
	}
	failing = &scriptTLS{startErr: errors.New("no start")}
	if c := Accept(&sinkConn{}, testAddr, Config{TLSConfig: serverTLS, NewTLS: withTLS(failing)}, newTestHandler(), initial); c != nil || !failing.closed {
		t.Fatalf("accepted with a handshake that does not start: closed %v", failing.closed)
	}
}

// What the TLS handshake reports is checked before it is acted on: keys of
// a suite there is no protection for, transport parameters that are
// malformed or name the wrong connection IDs, a handshake over without
// them, limits below those early data was sent under, and a session that
// cannot be stored or a ticket that cannot be sent all end the connection,
// while events that leave nothing to do are passed over.
func TestTLSEventsChecked(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	odcid, scid, dcid := []byte("original"), []byte("server-1"), []byte("client-1")
	params := func(f func(p *transportParams)) []byte {
		p := defaultParams()
		p.initialSCID, p.hasInitialSCID = scid, true
		p.originalDCID, p.hasOriginalDCID = odcid, true
		if f != nil {
			f(&p)
		}
		return p.encode()
	}
	paramsEvent := func(f func(p *transportParams)) tls.QUICEvent {
		return tls.QUICEvent{Kind: tls.QUICTransportParameters, Data: params(f)}
	}
	resume := &tls.SessionState{EarlyData: true}
	cases := []struct {
		name     string
		isClient bool
		store    error
		ticket   error
		setup    func(c *Conn)
		events   []tls.QUICEvent
		want     uint64
		check    func(t *testing.T, c *Conn, s *scriptTLS)
	}{
		{name: "keys of an unknown suite", isClient: true,
			events: []tls.QUICEvent{{Kind: tls.QUICSetReadSecret, Level: tls.QUICEncryptionLevelHandshake, Suite: 0x1399, Data: make([]byte, 32)}},
			want:   errInternal},
		{name: "resumption without a session", isClient: true,
			events: []tls.QUICEvent{{Kind: tls.QUICResumeSession}}},
		{name: "resumption with early data and no limits remembered", isClient: true,
			setup:  func(c *Conn) { c.config.EarlyData = true },
			events: []tls.QUICEvent{{Kind: tls.QUICResumeSession, SessionState: resume}},
			check: func(t *testing.T, c *Conn, _ *scriptTLS) {
				if resume.EarlyData || c.remembered != nil {
					t.Fatal("early data offered with no limits to keep to")
				}
			}},
		{name: "session that cannot be stored", isClient: true, store: errors.New("full"),
			setup:  func(c *Conn) { c.peerParamsRaw = params(nil) },
			events: []tls.QUICEvent{{Kind: tls.QUICStoreSession, SessionState: &tls.SessionState{}}},
			want:   errInternal},
		{name: "malformed transport parameters", isClient: true,
			events: []tls.QUICEvent{{Kind: tls.QUICTransportParameters, Data: []byte{paramMaxIdleTimeout}}},
			want:   errTransportParameter},
		{name: "transport parameters asked for",
			events: []tls.QUICEvent{{Kind: tls.QUICTransportParametersRequired}},
			check: func(t *testing.T, c *Conn, s *scriptTLS) {
				if s.params == nil {
					t.Fatal("no transport parameters given")
				}
			}},
		{name: "handshake over without transport parameters", isClient: true,
			events: []tls.QUICEvent{{Kind: tls.QUICHandshakeDone}}, want: errTransportParameter},
		{name: "server's initial_source_connection_id not the one it used", isClient: true,
			setup: func(c *Conn) { c.dcid = []byte("other-id") }, events: []tls.QUICEvent{paramsEvent(nil)},
			want: errTransportParameter},
		{name: "client's initial_source_connection_id not the one it used",
			setup: func(c *Conn) { c.dcid = dcid }, events: []tls.QUICEvent{paramsEvent(func(p *transportParams) {
				p.hasOriginalDCID = false
			})},
			want: errTransportParameter},
		{name: "original_destination_connection_id not the one sent to", isClient: true,
			events: []tls.QUICEvent{paramsEvent(func(p *transportParams) { p.originalDCID = []byte("not-sent") })},
			want:   errTransportParameter},
		{name: "retry_source_connection_id with no Retry", isClient: true,
			events: []tls.QUICEvent{paramsEvent(func(p *transportParams) {
				p.retrySCID, p.hasRetrySCID = []byte("no-retry"), true
			})},
			want: errTransportParameter},
		{name: "limits below those early data was sent under", isClient: true,
			setup: func(c *Conn) { c.remembered = &transportParams{initialMaxData: 1 << 20} },
			events: []tls.QUICEvent{paramsEvent(func(p *transportParams) { p.initialMaxData = 1 << 10 }),
				{Kind: tls.QUICHandshakeDone}},
			want: errProtocolViolation},
		{name: "session ticket that cannot be sent", ticket: errors.New("no ticket"),
			setup: func(c *Conn) { c.dcid = dcid },
			events: []tls.QUICEvent{paramsEvent(func(p *transportParams) {
				p.initialSCID, p.hasOriginalDCID = dcid, false
			}), {Kind: tls.QUICHandshakeDone}},
			want: errInternal},
		{name: "early data turned away on a server",
			events: []tls.QUICEvent{{Kind: tls.QUICRejectedEarlyData}},
			check: func(t *testing.T, c *Conn, _ *scriptTLS) {
				if c.earlyRejected {
					t.Fatal("a server took its own rejection as the client's")
				}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newConn(fuzzPacketConn{}, testAddr, Config{TLSConfig: serverTLS}, fuzzHandler{}, tc.isClient)
			st := &storingTLS{scriptTLS: scriptTLS{events: tc.events, ticketErr: tc.ticket}, storeErr: tc.store}
			c.tls = st
			c.odcid = odcid
			c.dcid = scid
			if tc.setup != nil {
				tc.setup(c)
			}
			c.mu.Lock()
			err := c.processTLSEvents()
			c.mu.Unlock()
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("failed with %v", err)
				}
			} else {
				wantCode(t, err, tc.want)
			}
			if tc.check != nil {
				tc.check(t, c, &st.scriptTLS)
			}
		})
	}
}

// Handshake data TLS will not take, or that leaves TLS failed, ends the
// connection; so does a handshake that fails on its own, once a callback it
// waited for returns.
func TestHandshakeDataRefused(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	for _, st := range []*scriptTLS{
		{handleErr: tls.AlertError(50)},
		{events: []tls.QUICEvent{{Kind: tls.QUICErrorEvent, Err: tls.AlertError(50)}}},
	} {
		c := newConn(fuzzPacketConn{}, testAddr, Config{TLSConfig: serverTLS}, fuzzHandler{}, false)
		c.tls = st
		c.mu.Lock()
		_, err := c.handleFrames(spaceInitial, frame(frameCrypto, 0, 4, "data"), time.Now())
		c.mu.Unlock()
		wantCode(t, err, errCryptoBase+50)
	}
	h := newTestHandler()
	c := newConn(fuzzPacketConn{}, testAddr, Config{TLSConfig: serverTLS}, h, false)
	c.tls = &scriptTLS{events: []tls.QUICEvent{{Kind: tls.QUICErrorEvent, Err: tls.AlertError(50)}}}
	c.wakeTLS()
	select {
	case err := <-h.closed:
		wantCode(t, err, errCryptoBase+50)
	default:
		t.Fatal("the failed handshake left the connection open")
	}
}

// A client whose handshake does not finish in time gives up with
// ErrHandshakeTimeout.
func TestHandshakeTimeout(t *testing.T) {
	_, _, h := dialSink(t, func(c *Config) { c.HandshakeTimeout = 20 * time.Millisecond })
	select {
	case err := <-h.closed:
		if err != ErrHandshakeTimeout {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake did not time out")
	}
}

// Streams wait for the handshake, unless early data goes before it, and
// for a connection that is still open.
func TestOpenStreamRefused(t *testing.T) {
	c, _, _ := dialSink(t, nil)
	if _, err := c.OpenStream(); err == nil || err == ErrStreamLimit {
		t.Fatalf("opened a stream before the handshake: %v", err)
	}
	// A stream of the peer's that turns up before its limits are known
	// may not be sent on.
	c.mu.Lock()
	s, err := c.peerStream(1, true)
	c.mu.Unlock()
	if err != nil || s.sendMax != 0 {
		t.Fatalf("peer's stream: %v, send limit %d", err, s.sendMax)
	}
	c.Abort(errTestOver)
	if _, err := c.OpenUniStream(); err != ErrClosed {
		t.Fatalf("opened a stream on a closed connection: %v", err)
	}
}

// versionNegotiation is the version negotiation packet a server that does
// not speak version 1 answers c's first packets with.
func versionNegotiation(c *Conn, versions ...uint32) []byte {
	b := longHeader(0x80, 0, c.scid, c.odcid)
	for _, v := range versions {
		b = append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
	return b
}

// A client gives up on a server that answers with version negotiation (RFC
// 9000 section 6.2), unless the answer cannot be genuine: it is for other
// connection IDs, lists version 1, or comes after the server has answered
// in version 1. Datagrams of other versions, or that are no packets at all,
// are dropped.
func TestVersionNegotiationReceived(t *testing.T) {
	c, _, h := dialSink(t, nil)
	other := versionNegotiation(c, 0x6b3343cf)
	copy(other[6:], "notmine!")
	for _, d := range [][]byte{
		other,
		versionNegotiation(c, 0x6b3343cf, Version1),
		append(longHeader(0xc0, 0x6b3343cf, c.scid, c.odcid), make([]byte, 20)...),
		{0xc0, 0, 0, 0, 1, 0},
	} {
		c.HandleDatagram(d)
	}
	select {
	case err := <-h.closed:
		t.Fatalf("closed with %v", err)
	default:
	}
	heard, _, heardH := dialSink(t, nil)
	heard.mu.Lock()
	heard.heardPeer = true
	heard.mu.Unlock()
	heard.HandleDatagram(versionNegotiation(heard, 0x6b3343cf))
	select {
	case err := <-heardH.closed:
		t.Fatalf("closed after hearing the server with %v", err)
	default:
	}
	c.HandleDatagram(versionNegotiation(c, 0x6b3343cf))
	select {
	case err := <-h.closed:
		if err != ErrVersionNegotiation {
			t.Fatalf("closed with %v", err)
		}
	default:
		t.Fatal("the client did not give up")
	}
}

// retryPacket is a Retry a server sends c, naming scid and carrying token.
func retryPacket(c *Conn, scid []byte, token string) []byte {
	b := append(longHeader(0xf0, Version1, c.scid, scid), token...)
	return append(b, retryTag(c.odcid, b)...)
}

// A Retry starts the client's handshake again towards the connection ID it
// names, with its token, and its 0-RTT packets are sent again too. Only the
// first authentic Retry counts.
func TestRetry(t *testing.T) {
	c, sink, _ := dialSink(t, nil)
	c.mu.Lock()
	sentBefore := c.spaces[spaceInitial].sent.len()
	c.earlyTx, _ = initialKeys([]byte("early-keys"), true)
	app := &c.spaces[spaceApp]
	app.nextPN = 2
	app.sent.reset()
	app.sent.push(&sentPacket{pn: 0})
	app.sent.push(&sentPacket{pn: 1, early: true, ackEliciting: true, inFlight: true, size: 100})
	app.ackElicitingInFlight = 1
	c.cc.bytesInFlight += 100
	c.mu.Unlock()
	if sentBefore == 0 {
		t.Fatal("no Initial packets in flight")
	}
	for _, d := range [][]byte{
		// A tag that does not match, and a short token.
		append(retryPacket(c, []byte("server-1"), "token")[:40], make([]byte, 16)...),
		retryPacket(c, []byte("server-1"), ""),
		// The connection ID it already sends to.
		retryPacket(c, c.dcid, "token"),
	} {
		c.HandleDatagram(d)
	}
	c.mu.Lock()
	if c.retrySCID != nil {
		c.mu.Unlock()
		t.Fatal("took a Retry that is not one")
	}
	c.mu.Unlock()
	before := sink.count()
	c.HandleDatagram(retryPacket(c, []byte("server-1"), "token"))
	c.HandleDatagram(retryPacket(c, []byte("server-2"), "token"))
	c.mu.Lock()
	defer c.mu.Unlock()
	if string(c.dcid) != "server-1" || string(c.token) != "token" {
		t.Fatalf("dcid %q, token %q", c.dcid, c.token)
	}
	if app.sent.len() != 1 || app.sent.take(0) == nil || app.ackElicitingInFlight != 0 {
		t.Fatalf("%d 1-RTT packets in flight, %d ack-eliciting", app.sent.len(), app.ackElicitingInFlight)
	}
	if sink.count() == before {
		t.Fatal("the Initial was not sent again")
	}
	h, err := parseHeader(sink.sent[before], cidLen)
	if err != nil || h.typ != packetInitial || string(h.dcid) != "server-1" || string(h.token) != "token" {
		t.Fatalf("sent again as %+v, %v", h, err)
	}
}

// Loss recovery at the edges of the handshake: a space with nothing
// acknowledged has nothing to declare lost, a probe timeout with nothing in
// flight prompts the server in the Initial space, a probe takes handshake data
// still to send with it, a probe timeout with nothing in flight prompts
// the server in the Handshake space once there are keys for it, and a
// space already discarded is not discarded again.
func TestRecoveryHandshakeEdges(t *testing.T) {
	c, _, _ := dialSink(t, nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	hs := &c.spaces[spaceHandshake]
	c.detectLost(spaceHandshake, now)
	// With nothing in flight and no Handshake keys yet, the probe is an
	// Initial one.
	initial := &c.spaces[spaceInitial]
	for i := range c.spaces {
		c.spaces[i].ackElicitingInFlight = 0
		c.spaces[i].lossTime = time.Time{}
	}
	if _, space := c.ptoTimeSpace(now); space != spaceInitial {
		t.Fatalf("probe timeout for space %d", space)
	}
	c.onLossDetectionTimeout(now)
	if initial.probes != 1 {
		t.Fatalf("%d initial probes", initial.probes)
	}
	initial.probes = 0
	hs.tx, _ = initialKeys([]byte("handshake"), true)
	hs.cryptoSend.write([]byte("finished"))
	c.probe(spaceInitial)
	if hs.probes != 1 {
		t.Fatalf("%d handshake probes", hs.probes)
	}
	for i := range c.spaces {
		c.spaces[i].probes = 0
		c.spaces[i].ackElicitingInFlight = 0
		c.spaces[i].lossTime = time.Time{}
	}
	if _, space := c.ptoTimeSpace(now); space != spaceHandshake {
		t.Fatalf("probe timeout for space %d", space)
	}
	c.onLossDetectionTimeout(now)
	if hs.probes != 1 || c.spaces[spaceInitial].probes != 0 {
		t.Fatalf("probes: handshake %d, initial %d", hs.probes, c.spaces[spaceInitial].probes)
	}
	c.discardSpace(spaceInitial)
	c.ptoCount = 3
	c.discardSpace(spaceInitial)
	if c.ptoCount != 3 {
		t.Fatal("discarding a discarded space reset the probe count")
	}
}
