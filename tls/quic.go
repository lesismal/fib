package tls

import (
	"bytes"
	"context"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"hash"
	"slices"
	"time"

	"github.com/lesismal/fib/sidepool"
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
// QUIC's own and so also lets ChaCha20-Poly1305 be used. Resumption and 0-RTT
// are as crypto/tls's QUICConn has them, through the same events: a server that
// has a client's ticket asks the QUIC layer, with QUICResumeSession, whether to
// accept the early data, and so does a client before it offers any; a client
// that is turned away is told by QUICRejectedEarlyData. A server hands to
// crypto/tls the clients it cannot serve, before it has sent anything.

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

// resume asks the QUIC layer about the session a client resumes, whose
// EarlyData it may clear to decline the client's 0-RTT data.
func (q *quicCtx) resume(state *stdtls.SessionState) {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICResumeSession, SessionState: state})
}

func (q *quicCtx) peer(params []byte) {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICTransportParameters, Data: bytes.Clone(params)})
}

func (q *quicCtx) done() {
	q.events = append(q.events, stdtls.QUICEvent{Kind: stdtls.QUICHandshakeDone})
}

// finishQUIC ends a QUIC client's handshake once the server's Finished has been
// verified: the client's certificate if one was asked for, and its Finished go
// to the server at the handshake level, and both directions move to the
// application keys.
func (h *clientHS) finishQUIC(exporter []byte) *hsError {
	if h.certReq {
		msgs, herr := h.certificateMessages13()
		if herr != nil {
			return herr
		}
		for _, m := range msgs {
			h.quic.write(stdtls.QUICEncryptionLevelHandshake, m)
		}
	}
	verify, err := h.finishedMAC(h.clientHS)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	fin := append([]byte{hsFinished, 0, 0, byte(len(verify))}, verify...)
	h.transcript = append(h.transcript, fin...)
	h.quic.write(stdtls.QUICEncryptionLevelHandshake, fin)
	if h.sessions != nil {
		// The tickets that follow resume from the secret that covers the
		// handshake through the client's Finished.
		if h.resumption, err = deriveSecret(h.suite.hash, h.master, "res master", h.transcriptHash()); err != nil {
			return fail(alertInternalError, "%v", err)
		}
	}
	h.keyLog("CLIENT_TRAFFIC_SECRET_0", h.appClient)
	h.keyLog("SERVER_TRAFFIC_SECRET_0", h.appServer)
	h.keyLog("EXPORTER_SECRET", exporter)
	h.quic.setRead(stdtls.QUICEncryptionLevelApplication, h.suiteID, h.appServer)
	h.quic.setWrite(stdtls.QUICEncryptionLevelApplication, h.suiteID, h.appClient)
	h.state = hsDone
	h.flightEnd = true
	h.quic.done()
	return nil
}

// ticketSink is what takes the tickets a server sends a client after its
// handshake into the client's cache: the handshake's end, with only what that
// needs of it.
type ticketSink struct {
	cache      QUICSessionCache
	key        string
	hash       func() hash.Hash
	resumption []byte
	suite      uint16
	alpn       string
	certs      [][]byte
	chains     [][]*x509.Certificate
	// quicParams are the server's transport parameters, which a QUIC client
	// keeps with the session; nil over TCP.
	quicParams []byte
	quic       bool
	now        func() time.Time
}

// sink makes the handshake's ticket sink once its Finished is sent.
func (h *clientHS) sink() *ticketSink {
	if h.sessions == nil || h.resumption == nil {
		return nil
	}
	return &ticketSink{cache: h.sessions, key: h.config.ServerName, hash: h.suite.hash, resumption: h.resumption,
		suite: h.suiteID, alpn: h.protocol, certs: h.rawCerts, chains: h.chains, quicParams: h.peerParams, quic: h.quic != nil, now: h.now}
}

// newSessionTicket takes a ticket the server sends after the handshake into the
// cache, to resume from (RFC 8446 section 4.6.1).
func (h *clientHS) newSessionTicket(msg []byte) *hsError {
	if k := h.sink(); k != nil {
		return k.store(msg)
	}
	return nil
}

func (k *ticketSink) store(msg []byte) *hsError {
	r := newReader(msg[4:])
	lifetime := r.u32()
	ageAdd := r.u32()
	nonce := r.vec8()
	ticket := r.vec16()
	extensions := r.vec16()
	if !r.empty() || len(ticket) == 0 {
		return fail(alertDecodeError, "malformed NewSessionTicket")
	}
	exts, herr := parseExtensions(extensions)
	if herr != nil {
		return herr
	}
	var maxEarly uint32
	if data, ok := exts[extEarlyData]; ok {
		d := newReader(data)
		size := d.u32()
		// QUIC allows all of the early data or none (RFC 9001 section 4.6.1).
		if !d.empty() || k.quic && size != 0xffffffff {
			return fail(alertIllegalParameter, "invalid max_early_data_size in a session ticket")
		}
		maxEarly = size
	}
	if lifetime == 0 {
		return nil
	}
	psk, err := expandLabelContext(k.hash, k.resumption, "resumption", nonce, k.hash().Size())
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	session := &QUICSession{
		ticket: bytes.Clone(ticket), psk: psk, suite: k.suite, received: k.now(),
		lifetime: min(time.Duration(lifetime)*time.Second, ticketLifetime),
		ageAdd:   ageAdd, alpn: k.alpn, earlyData: maxEarly != 0, maxEarly: maxEarly, certs: k.certs,
		chains: k.chains,
	}
	if k.quic && maxEarly != 0 {
		// What the layer works to when it sends early data on this session.
		session.extra = [][]byte{append([]byte{1}, k.quicParams...)}
	}
	k.cache.Put(k.key, session)
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
	if config.ClientSessionCache != nil || config.EncryptedClientHelloConfigList != nil {
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
	// sessions is where a client's tickets go and come from.
	sessions QUICSessionCache
	// wake, if the QUIC layer set it, has the layer ask for the events of a
	// handshake that has gone on by itself: once a certificate callback,
	// which ran on another goroutine, has returned.
	wake func()
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

// NewQUICClientWithSessions is NewQUICClient for a client that resumes sessions
// from sessions, and puts those the server sends into it. With a session that
// allows it, early data is offered if the QUIC layer says so, by clearing
// EarlyData of the QUICResumeSession event it is asked about.
func NewQUICClientWithSessions(config *stdtls.Config, sessions QUICSessionCache) *QUICConn {
	c := NewQUICClient(config)
	if c != nil {
		c.sessions = sessions
	}
	return c
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

// SetWake lets the handshake call the certificate callbacks of the Config,
// GetCertificate and GetClientCertificate, which may block, on a goroutine of
// their own, and a client verify the server's certificate and call
// VerifyPeerCertificate and VerifyConnection there too: the handshake then
// waits, with no events to read, until one has returned, and calls wake from
// that goroutine, after which the QUIC layer reads the events that follow from
// NextEvent, as it does after HandleData, under whatever lock it holds for
// that. Without it, the callbacks are called from HandleData, as crypto/tls's
// QUICConn does. It is set before Start.
func (c *QUICConn) SetWake(wake func()) {
	c.wake = wake
	if c.server != nil {
		c.server.pick.async = c.async()
	}
}

// async is how the handshake runs a certificate callback once the layer has
// set wake: on a worker of the handshake pool, which wakes the layer once it
// has returned.
func (c *QUICConn) async() func(call func()) bool {
	wake := c.wake
	if wake == nil {
		return nil
	}
	return func(call func()) bool {
		return sidepool.Handshake().TryGo(func() {
			call()
			wake()
		})
	}
}

// SetTransportParameters sets the transport parameters sent to the peer. They
// must be set before Start.
func (c *QUICConn) SetTransportParameters(params []byte) {
	c.q.localParams, c.q.haveLocal = bytes.Clone(params), true
	if c.fallback != nil {
		c.fallback.SetTransportParameters(params)
	}
}

// SetCipherSuites narrows the TLS 1.3 cipher suites a client's ClientHello
// offers, in the order to prefer them, by ID; it must be called before Start.
// Empty keeps the default order, AES-GCM first where the CPU computes it in
// hardware and ChaCha20-Poly1305 first where it does not, as crypto/tls orders
// them, which is what a server that follows its client's preference takes. A
// server handshake ignores it: a TLS 1.3 server settles on one of the suites
// its client offered.
func (c *QUICConn) SetCipherSuites(suites []uint16) {
	if len(suites) == 0 {
		c.suites = nil
		return
	}
	c.suites = slices.Clone(suites)
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
	h.pick.async = c.async()
	h.check.async = h.pick.async
	if c.suites != nil {
		h.suites = c.suites
	}
	h.sessions = c.sessions
	if h.pickSession() && h.session.earlyData {
		// The QUIC layer is asked whether to offer early data with it, and the
		// hello is made once it has answered (see NextEvent).
		h.resumeState = &stdtls.SessionState{EarlyData: true, Extra: h.session.extra}
		c.q.resume(h.resumeState)
		h.pending = func() *hsError {
			h.offerEarly = h.resumeState.EarlyData
			h.sendHello()
			return nil
		}
		return nil
	}
	h.sendHello()
	return nil
}

// pickSession takes the session to offer, if the cache has one that is still
// good for this connection, out of the cache.
func (h *clientHS) pickSession() bool {
	if h.sessions == nil || h.quic == nil && !h.offers(stdtls.VersionTLS13) {
		return false
	}
	key := h.config.ServerName
	s := h.sessions.Get(key)
	if s == nil {
		return false
	}
	// A session is used once, whether or not the server takes it.
	h.sessions.Put(key, nil)
	if _, ok := quicSuite(s.suite); !ok || !s.usable(h.now()) || !slices.Contains(h.suites, s.suite) ||
		len(h.config.NextProtos) > 0 && !slices.Contains(h.config.NextProtos, s.alpn) || !h.sessionTrusted(s) {
		return false
	}
	h.session = s
	return true
}

// sessionTrusted reports whether the server a session was made with is still
// one the Config trusts, as crypto/tls decides before it offers a session: a
// session may come from a connection that did not verify the server, with
// InsecureSkipVerify, or with other RootCAs, since sessions are kept by server
// name alone, in a cache that may be shared between Configs. It is not unless
// the server's chain was verified, its certificates are all still current, the
// leaf is the ServerName's, and the root is among the RootCAs if the Config
// names them; the system's roots do not change under a running process, and
// what this build's crypto/x509 distrusts it did not verify in the first
// place. Nothing here verifies a signature, so it is cheap enough for the
// goroutine that drives the handshake, which a verification is not (see
// checkThen).
func (h *clientHS) sessionTrusted(s *QUICSession) bool {
	if h.config.InsecureSkipVerify {
		return true
	}
	if len(s.chains) == 0 {
		return false
	}
	now := h.now()
	if s.chains[0][0].VerifyHostname(h.config.ServerName) != nil {
		return false
	}
	for _, chain := range s.chains {
		if slices.ContainsFunc(chain, func(c *x509.Certificate) bool {
			return now.Before(c.NotBefore) || now.After(c.NotAfter)
		}) {
			continue
		}
		if h.config.RootCAs == nil {
			return true
		}
		// A root among the Roots verifies as its own chain without a
		// signature checked, which is how a pool is asked what it holds.
		root := chain[len(chain)-1]
		if _, err := root.Verify(x509.VerifyOptions{Roots: h.config.RootCAs, CurrentTime: now, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err == nil {
			return true
		}
	}
	return false
}

// sendHello writes the ClientHello, and the keys early data goes under if it is
// offered.
func (h *clientHS) sendHello() {
	h.earlyActive = h.offerEarly && h.session != nil
	hello := h.clientHello()
	h.transcript = append(h.transcript, hello...)
	h.quic.write(stdtls.QUICEncryptionLevelInitial, hello)
	if !h.earlyActive {
		return
	}
	f := h.sessionHash()
	early, err := hkdf.Extract(f, h.session.psk, nil)
	if err == nil {
		var secret []byte
		if secret, err = deriveSecret(f, early, "c e traffic", hashBytes(f, h.transcript)); err == nil {
			h.keyLog("CLIENT_EARLY_TRAFFIC_SECRET", secret)
			h.quic.setWrite(stdtls.QUICEncryptionLevelEarly, h.session.suite, secret)
			return
		}
	}
	h.earlyActive = false
}

// NextEvent returns the next event of the handshake, or an event of kind
// QUICNoEvent if there is none.
func (c *QUICConn) NextEvent() stdtls.QUICEvent {
	if c.fallback != nil {
		return c.fallback.NextEvent()
	}
	if len(c.q.events) == 0 {
		c.resumePending()
	}
	if len(c.q.events) == 0 {
		return stdtls.QUICEvent{Kind: stdtls.QUICNoEvent}
	}
	e := c.q.events[0]
	c.q.events[0] = stdtls.QUICEvent{}
	c.q.events = c.q.events[1:]
	return e
}

// resumePending goes on with the handshake of a server that stopped to ask
// the QUIC layer whether to accept the early data of a resuming client: the
// layer has had the QUICResumeSession event, and answers by what it did to
// the session before asking for the next event.
func (c *QUICConn) resumePending() {
	var pending func() *hsError
	switch {
	case c.server != nil && c.server.pending != nil:
		pending, c.server.pending = c.server.pending, nil
	case c.client != nil && c.client.pending != nil:
		pending, c.client.pending = c.client.pending, nil
	default:
		c.resumePicked()
		return
	}
	if herr := pending(); herr != nil {
		c.err = quicError(herr)
		c.q.events = append(c.q.events, stdtls.QUICEvent{Kind: stdtls.QUICErrorEvent, Err: c.err})
	}
}

// waiting reports whether the handshake waits for a certificate callback, or
// a client for a check of the server's.
func (c *QUICConn) waiting() bool {
	return c.server != nil && c.server.pick.waiting() || c.client != nil && c.client.waiting()
}

// resumePicked goes on with a handshake that waited for a certificate
// callback, once it has answered, and with the messages that arrived
// meanwhile.
func (c *QUICConn) resumePicked() {
	if c.err != nil || !c.waiting() {
		return
	}
	var waiting bool
	var herr *hsError
	if c.server != nil {
		waiting, herr = c.server.pick.proceed(nil)
	} else {
		waiting, herr = c.client.proceed(nil)
	}
	if waiting {
		return
	}
	if herr != nil {
		c.err = quicError(herr)
	} else {
		for at := range c.in {
			if len(c.in[at]) > 0 {
				if err := c.drain(at); err != nil && c.err == nil {
					c.err = err
				}
				break
			}
		}
	}
	if c.err != nil {
		c.q.events = append(c.q.events, stdtls.QUICEvent{Kind: stdtls.QUICErrorEvent, Err: c.err})
	}
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
	c.resumePending()
	if c.err != nil {
		return c.err
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
	return c.drain(at)
}

// drain handles the messages that are whole in what has arrived at level at
// (0 initial, 1 handshake, 2 application), until the handshake waits for a
// certificate callback, which leaves the rest for when it has answered.
func (c *QUICConn) drain(at int) error {
	for !c.waiting() {
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
	return nil
}

// quicError is the error QUIC turns into a connection error carrying alert.
func quicError(herr *hsError) error {
	if herr.alert != 0 {
		if herr.err == nil {
			return stdtls.AlertError(herr.alert)
		}
		return &alertCause{alert: stdtls.AlertError(herr.alert), err: herr.err}
	}
	return herr.err
}

// alertCause is an alert together with what went wrong, as crypto/tls
// reports a failed QUIC handshake: it reads as the cause ("tls: failed to
// verify certificate: ..."), and errors.As finds the AlertError in it.
type alertCause struct {
	alert stdtls.AlertError
	err   error
}

func (e *alertCause) Error() string   { return e.err.Error() }
func (e *alertCause) Unwrap() []error { return []error{e.alert, e.err} }

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
			return h.newSessionTicket(msg)
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
	switch {
	case h.state == srvAwaitHello || h.state == srvAwaitHello2:
		want = 0
	case h.awaitsClientFlight():
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
	case h.awaitsClientFlight():
		// The client's certificate if it was asked for one, and its Finished.
		if herr := h.message(msg, nil); herr != nil {
			return herr
		}
		if h.state == srvDone && len(c.in[level]) != len(msg) {
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
// handshake of a server is over; with opts.EarlyData the ticket may be used
// for 0-RTT, and opts.Extra is kept with it for the QUIC layer to see again.
func (c *QUICConn) SendSessionTicket(opts stdtls.QUICSessionTicketOptions) error {
	if c.fallback != nil {
		return c.fallback.SendSessionTicket(opts)
	}
	if c.server == nil || c.server.state != srvDone {
		return errors.New("tls: SendSessionTicket called before the handshake of a server is over")
	}
	msg, herr := c.server.ticketMessage(opts.EarlyData, opts.Extra)
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
