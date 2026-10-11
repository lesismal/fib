package tls

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	stdtls "crypto/tls"
	"errors"
	"slices"
)

// The TLS 1.3 handshake of a QUIC connection (RFC 9001), run by the state
// machines of this package.
//
// QUIC does not put TLS in records. The handshake messages travel in CRYPTO
// frames at the encryption level they belong to, the secrets they produce go to
// QUIC to protect its packets, and the peer's transport parameters ride in an
// extension. crypto/tls offers that as QUICConn, whose handshake runs on a
// goroutine of its own that waits for the peer's messages, one for every
// connection still handshaking. QUICConn here has the same methods and
// events, so package quic can use either, but nothing in it waits: HandleData
// takes the messages that have arrived and the events they cause are there to
// be read at once.
//
// It is TLS 1.3 as the TCP handshakes are (client13.go and server13.go, whose
// code it shares), without what QUIC does not use: records, the compatibility
// change_cipher_spec and session ID, the suites' record protection, which is
// QUIC's own and so also lets ChaCha20-Poly1305 be used. 0-RTT is not supported,
// as it is not in package quic. A server hands to crypto/tls the clients it
// cannot serve, before it has sent anything.

// extQUICParams is the quic_transport_parameters extension.
const extQUICParams = 0x39

// quicSuites are the TLS 1.3 suites of a QUIC handshake: all three, since
// TLS protects no records there and only the hash of the suite matters to it.
var quicSuites = []uint16{
	stdtls.TLS_AES_128_GCM_SHA256, stdtls.TLS_AES_256_GCM_SHA384, stdtls.TLS_CHACHA20_POLY1305_SHA256,
}

func quicSuite(id uint16) (suite, bool) {
	switch id {
	case stdtls.TLS_AES_128_GCM_SHA256, stdtls.TLS_CHACHA20_POLY1305_SHA256:
		return suite{hash: sha256.New}, true
	case stdtls.TLS_AES_256_GCM_SHA384:
		return suite{hash: sha512.New384}, true
	}
	return suite{}, false
}

// quicCtx is what a handshake in QUIC mode reports through: events, in the
// order they happened, and the transport parameters of this side.
type quicCtx struct {
	events      []stdtls.QUICEvent
	localParams []byte
	haveLocal   bool
}

func (q *quicCtx) write(level stdtls.QUICEncryptionLevel, data []byte) {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICWriteData, Level: level, Data: bytes.Clone(data)})
}

func (q *quicCtx) setRead(level stdtls.QUICEncryptionLevel, suite uint16, secret []byte) {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICSetReadSecret, Level: level, Suite: suite, Data: bytes.Clone(secret)})
}

func (q *quicCtx) setWrite(level stdtls.QUICEncryptionLevel, suite uint16, secret []byte) {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICSetWriteSecret, Level: level, Suite: suite, Data: bytes.Clone(secret)})
}

func (q *quicCtx) peer(params []byte) {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICTransportParameters, Data: bytes.Clone(params)})
}

func (q *quicCtx) done() {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICHandshakeDone})
}

// finishQUIC ends a QUIC client's handshake once the server's Finished has been
// verified: the client's certificate if one was asked for, none, and its
// Finished go to the server at the handshake level, and both directions move
// to the application keys.
func (h *clientHS) finishQUIC(exporter []byte) *hsError {
	if h.certReq {
		// No certificate to offer; see finished.
		cert := []byte{hsCertificate, 0, 0, byte(1 + len(h.reqCtx) + 3)}
		cert = append(cert, byte(len(h.reqCtx)))
		cert = append(cert, h.reqCtx...)
		cert = append(cert, 0, 0, 0)
		h.transcript = append(h.transcript, cert...)
		h.quic.write(stdtls.QUICEncryptionLevelHandshake, cert)
	}
	verify, err := h.finishedMAC(h.clientHS)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	fin := append([]byte{hsFinished, 0, 0, byte(len(verify))}, verify...)
	h.transcript = append(h.transcript, fin...)
	h.quic.write(stdtls.QUICEncryptionLevelHandshake, fin)
	h.keyLog("CLIENT_TRAFFIC_SECRET_0", h.appClient)
	h.keyLog("SERVER_TRAFFIC_SECRET_0", h.appServer)
	h.keyLog("EXPORTER_SECRET", exporter)
	h.quic.setRead(stdtls.QUICEncryptionLevelApplication, h.suiteID, h.appServer)
	h.quic.setWrite(stdtls.QUICEncryptionLevelApplication, h.suiteID, h.appClient)
	if verify := h.config.VerifyConnection; verify != nil {
		if err := verify(h.connectionState()); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.state = hsDone
	h.flightEnd = true
	h.quic.done()
	return nil
}

// nativeQUICClient reports whether a QUIC client handshake with config can be
// run by clientHS: the Config of nativeClient, with QUIC's TLS 1.3 alone in
// place of a range of versions.
func nativeQUICClient(config *stdtls.Config) bool {
	if config == nil || config.Renegotiation != stdtls.RenegotiateNever {
		return false
	}
	if config.MaxVersion != 0 && config.MaxVersion < stdtls.VersionTLS13 || config.MinVersion > stdtls.VersionTLS13 {
		return false
	}
	if len(config.Certificates) != 0 || config.GetClientCertificate != nil || config.ClientSessionCache != nil ||
		config.EncryptedClientHelloConfigList != nil {
		return false
	}
	if len(config.CurvePreferences) != 0 {
		return slices.ContainsFunc(config.CurvePreferences, func(id stdtls.CurveID) bool { return curveOf(id) != nil })
	}
	return true
}

// QUICConn is the TLS handshake of one QUIC connection, with the methods and
// events of crypto/tls's QUICConn: package quic calls Start on a client, hands
// HandleData what arrives in its CRYPTO frames, and reads NextEvent until it
// reports no event. It is not safe for concurrent use.
type QUICConn struct {
	q      *quicCtx
	client *clientHS
	server *serverHS
	config *stdtls.Config
	// fallback is crypto/tls's, once a server has found that it cannot serve
	// its client.
	fallback *stdtls.QUICConn
	// in holds the handshake bytes of each level not yet taken as messages.
	in      [3][]byte
	err     error
	started bool
	// suites narrows what a client offers, for tests.
	suites []uint16
}

// NewQUICClient returns the handshake of a QUIC client, or nil if config asks
// for something it does not do (see nativeClient), in which case crypto/tls's
// QUICClient is the one to use. Set the transport parameters before Start.
func NewQUICClient(config *stdtls.Config) *QUICConn {
	if !nativeQUICClient(config) {
		return nil
	}
	return &QUICConn{q: &quicCtx{}, config: config}
}

// QUICServer makes the handshakes of the QUIC connections of one server,
// which share the keys their session tickets are sealed under.
type QUICServer struct {
	config  *stdtls.Config
	tickets *ticketKeys
}

// NewQUICServer returns a maker of handshakes for a server with config, or nil
// if the Config asks for something it does not do (see nativeServer), in which
// case crypto/tls's QUICServer is the one to use.
func NewQUICServer(config *stdtls.Config) *QUICServer {
	if !nativeServer(config) {
		return nil
	}
	if allow13, _ := serverVersions(config); !allow13 && config.MinVersion != 0 {
		return nil
	}
	return &QUICServer{config: config, tickets: newTicketKeys(config)}
}

// NewConn returns the handshake of one connection. Set the transport
// parameters before Start.
func (s *QUICServer) NewConn() *QUICConn {
	c := &QUICConn{q: &quicCtx{}, config: s.config}
	c.server = newServerHSFor(s.config, nil, s.tickets, c.q)
	return c
}

// SetTransportParameters sets the transport parameters sent to the peer. They
// must be set before Start.
func (c *QUICConn) SetTransportParameters(params []byte) {
	c.q.localParams, c.q.haveLocal = bytes.Clone(params), true
	if c.fallback != nil {
		c.fallback.SetTransportParameters(params)
	}
}

// Start begins the handshake: a client's first flight is then to be read from
// NextEvent. It is called with the transport parameters set.
func (c *QUICConn) Start(ctx context.Context) error {
	if c.started {
		return errors.New("tls: Start called more than once")
	}
	c.started = true
	if !c.q.haveLocal {
		return errors.New("tls: Start called before SetTransportParameters")
	}
	if c.server != nil {
		return nil
	}
	h, err := newClientHSFor(c.config, c.q)
	if err != nil {
		return err
	}
	c.client = h
	if c.suites != nil {
		h.suites = c.suites
	}
	hello := h.clientHello()
	h.transcript = append(h.transcript, hello...)
	c.q.write(stdtls.QUICEncryptionLevelInitial, hello)
	return nil
}

// NextEvent returns the next event of the handshake, or an event of kind
// QUICNoEvent if there is none.
func (c *QUICConn) NextEvent() stdtls.QUICEvent {
	if c.fallback != nil {
		return c.fallback.NextEvent()
	}
	if len(c.q.events) == 0 {
		return stdtls.QUICEvent{Kind: stdtls.QUICNoEvent}
	}
	e := c.q.events[0]
	c.q.events[0] = stdtls.QUICEvent{}
	c.q.events = c.q.events[1:]
	return e
}

// HandleData takes handshake data received at level, in order. The data may
// end anywhere in a message: what does not make a whole one is kept for the next
// call.
func (c *QUICConn) HandleData(level stdtls.QUICEncryptionLevel, data []byte) error {
	if c.fallback != nil {
		return c.fallback.HandleData(level, data)
	}
	if c.err != nil {
		return c.err
	}
	if !c.started {
		return errors.New("tls: HandleData called before Start")
	}
	var at int
	switch level {
	case stdtls.QUICEncryptionLevelInitial:
		at = 0
	case stdtls.QUICEncryptionLevelHandshake:
		at = 1
	case stdtls.QUICEncryptionLevelApplication:
		at = 2
	default:
		return errors.New("tls: HandleData at an unexpected level")
	}
	for i := range c.in {
		if i != at && len(c.in[i]) != 0 {
			// A message does not span encryption levels (RFC 9001 section 4.1.3).
			c.err = stdtls.AlertError(alertUnexpectedMessage)
			return c.err
		}
	}
	c.in[at] = append(c.in[at], data...)
	for {
		buf := c.in[at]
		if len(buf) < 4 {
			return nil
		}
		size := 4 + (int(buf[1])<<16 | int(buf[2])<<8 | int(buf[3]))
		if size > maxHandshakeMessage {
			c.err = stdtls.AlertError(alertInternalError)
			return c.err
		}
		if len(buf) < size {
			return nil
		}
		msg := buf[:size:size]
		herr := c.message(at, msg)
		if herr == errFallback {
			return c.fallBack()
		}
		if herr != nil {
			c.err = quicError(herr)
			return c.err
		}
		c.in[at] = buf[size:]
		if len(c.in[at]) == 0 {
			c.in[at] = nil
		}
	}
}

// quicError is the error QUIC turns into a connection error carrying alert.
func quicError(herr *hsError) error {
	if herr.alert != 0 {
		return stdtls.AlertError(herr.alert)
	}
	return herr.err
}

// message handles one handshake message received at level (0 initial, 1
// handshake, 2 application).
func (c *QUICConn) message(level int, msg []byte) *hsError {
	if c.client != nil {
		h := c.client
		h.flightEnd = false
		var want int
		switch h.state {
		case hsExpectServerHello:
			want = 0
		case hsDone:
			want = 2
		default:
			want = 1
		}
		if level != want {
			return fail(alertUnexpectedMessage, "handshake message %d at the wrong encryption level", msg[0])
		}
		if h.state == hsDone {
			// The server's tickets, which this client has no use for. Anything
			// else, a KeyUpdate in particular, is not allowed in QUIC.
			if msg[0] != hsNewSessionTicket {
				return fail(alertUnexpectedMessage, "unexpected handshake message %d", msg[0])
			}
			return nil
		}
		if herr := h.message(msg, nil); herr != nil {
			return herr
		}
		if h.flightEnd && len(c.in[level]) != len(msg) {
			return fail(alertUnexpectedMessage, "handshake data after the last message of a flight")
		}
		return nil
	}
	h := c.server
	var want int
	switch h.state {
	case srvAwaitHello, srvAwaitHello2:
		want = 0
	case srvAwaitFinished:
		want = 1
	default:
		want = 2
	}
	if level != want {
		return fail(alertUnexpectedMessage, "handshake message %d at the wrong encryption level", msg[0])
	}
	switch {
	case (h.state == srvAwaitHello || h.state == srvAwaitHello2) && msg[0] == hsClientHello:
		if h.state == srvAwaitHello {
			// The server has sent nothing yet, so crypto/tls can still take over.
			ch, ok := parseClientHello(msg)
			if !ok || !h.servesQUIC(ch) {
				return errFallback
			}
		}
		if !c.q.haveLocal {
			return fail(alertInternalError, "no transport parameters to send")
		}
		if herr := h.clientHello(msg, nil); herr != nil {
			return herr
		}
		if len(c.in[level]) != len(msg) {
			return fail(alertUnexpectedMessage, "handshake data after the ClientHello")
		}
	case h.state == srvAwaitFinished && msg[0] == hsFinished:
		if herr := h.finished(msg, nil); herr != nil {
			return herr
		}
		if len(c.in[level]) != len(msg) {
			return fail(alertUnexpectedMessage, "handshake data after the Finished")
		}
	default:
		return fail(alertUnexpectedMessage, "unexpected handshake message %d", msg[0])
	}
	return nil
}

// fallBack hands a server connection whose client this cannot serve to
// crypto/tls, which is given the ClientHello as if it had read it itself.
func (c *QUICConn) fallBack() error {
	if nativeFellBack != nil {
		nativeFellBack()
	}
	std := stdtls.QUICServer(&stdtls.QUICConfig{TLSConfig: c.config})
	std.SetTransportParameters(c.q.localParams)
	if err := std.Start(context.Background()); err != nil {
		c.err = err
		return err
	}
	received := c.in[0]
	c.in = [3][]byte{}
	c.fallback, c.server, c.q.events = std, nil, nil
	return std.HandleData(stdtls.QUICEncryptionLevelInitial, received)
}

// SendSessionTicket sends the client a ticket to resume from, once the
// handshake of a server is over. Early data is not supported.
func (c *QUICConn) SendSessionTicket(opts stdtls.QUICSessionTicketOptions) error {
	if c.fallback != nil {
		return c.fallback.SendSessionTicket(opts)
	}
	if c.server == nil || c.server.state != srvDone {
		return errors.New("tls: SendSessionTicket called before the handshake of a server is over")
	}
	msg, herr := c.server.ticketMessage()
	if herr != nil {
		return quicError(herr)
	}
	if msg != nil {
		c.q.write(stdtls.QUICEncryptionLevelApplication, msg)
	}
	return nil
}

// ConnectionState reports the connection's TLS parameters.
func (c *QUICConn) ConnectionState() stdtls.ConnectionState {
	switch {
	case c.fallback != nil:
		return c.fallback.ConnectionState()
	case c.client != nil:
		return c.client.connectionState()
	case c.server != nil && c.server.ch != nil:
		return c.server.connectionState()
	}
	return stdtls.ConnectionState{}
}

// Close lets the handshake go.
func (c *QUICConn) Close() error {
	if c.fallback != nil {
		return c.fallback.Close()
	}
	c.in = [3][]byte{}
	return nil
}
