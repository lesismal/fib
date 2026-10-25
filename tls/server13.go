package tls

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"time"
)

// The TLS server handshake as a state machine, the counterpart of clientHS:
// TLS 1.3 here (RFC 8446) and TLS 1.2 in server12.go (RFC 5246). OnData hands
// process what has arrived, and it returns what to send and whether the
// handshake is over, never waiting. A server accepting thousands of
// connections at once holds no goroutine for any of them while they handshake.
//
// A server, unlike a client, can see what its peer wants before it commits to
// anything, so it can hand a connection it cannot serve to crypto/tls: process
// reads the ClientHello first, and if the client is one this does not handle,
// it consumes nothing and reports errFallback, and the layer gives crypto/tls
// every byte received so far, as if it had read them itself. That is a client
// that offers no version below, nor TLS 1.3 or 1.2 within, the Config's range
// (an old client crypto/tls still serves if the Config allows it), asks for
// 0-RTT early data, offers no suite, group or signature scheme this implements
// that the Config and its certificate allow, or sends a hello that does not
// parse, for which crypto/tls finds the alert to send. A Config this does not
// qualify for never starts here; see nativeServer.
//
// Session resumption is its own (ticket.go): after a full handshake it sends a
// ticket, sealed under a key of the Handler's, and a client that comes back
// with one resumes, with a PSK in TLS 1.3 and with the ticket's master secret
// in TLS 1.2, without a certificate. Tickets that crypto/tls issued, to a
// client the fallback served, are not recognised, and the client then
// handshakes in full.

// errFallback is the hsError that sends a connection to crypto/tls instead.
var errFallback = &hsError{err: errors.New("tls: handled by crypto/tls")}

// nativeServer reports whether a server handshake with config can be run by
// serverHS. The Config needs certificates, its own or GetCertificate's (see
// auth.go), and may authenticate clients. GetConfigForClient, which may change
// anything about the handshake once the client has spoken, encrypted client
// hello, and keys that cannot sign by themselves are crypto/tls's.
func nativeServer(config *stdtls.Config) bool {
	if config == nil || config.GetConfigForClient != nil || config.EncryptedClientHelloKeys != nil ||
		len(config.Certificates) == 0 && config.GetCertificate == nil ||
		config.ClientAuth < stdtls.NoClientCert || config.ClientAuth > stdtls.RequireAndVerifyClientCert ||
		config.Renegotiation != stdtls.RenegotiateNever {
		return false
	}
	if allow13, allow12 := serverVersions(config); !allow13 && !allow12 {
		return false
	}
	for i := range config.Certificates {
		if _, ok := config.Certificates[i].PrivateKey.(crypto.Signer); !ok || len(config.Certificates[i].Certificate) == 0 {
			return false
		}
	}
	if len(config.CurvePreferences) != 0 {
		for _, id := range config.CurvePreferences {
			if curveOf(id) != nil {
				return true
			}
		}
		return false
	}
	return true
}

// serverVersions are the versions a server with config can serve without a
// worker: the ones within its range, TLS 1.2 up by default.
func serverVersions(config *stdtls.Config) (allow13, allow12 bool) {
	lowest, highest := config.MinVersion, config.MaxVersion
	if lowest == 0 {
		lowest = stdtls.VersionTLS12
	}
	if highest == 0 {
		highest = stdtls.VersionTLS13
	}
	return lowest <= stdtls.VersionTLS13 && highest >= stdtls.VersionTLS13,
		lowest <= stdtls.VersionTLS12 && highest >= stdtls.VersionTLS12
}

type srvState uint8

const (
	srvAwaitHello srvState = iota
	srvAwaitHello2
	// The client's certificate and its CertificateVerify, if it was asked for
	// one.
	srvAwaitClientCert
	srvAwaitClientCertVerify
	srvAwaitFinished
	// TLS 1.2: the client's certificate if it was asked for one, its key
	// exchange, its CertificateVerify if it sent a certificate, its
	// change_cipher_spec, and its Finished.
	srv12AwaitClientCert
	srv12AwaitKeyExchange
	srv12AwaitClientCertVerify
	srv12AwaitCCS
	srv12AwaitFinished
	srvDone
)

// serverHS is one server handshake.
type serverHS struct {
	config *stdtls.Config
	conn   net.Conn
	state  srvState
	// decided says the ClientHello has been read and is one this serves.
	decided bool

	ch      *clientHelloMsg
	random  [32]byte
	groups  []stdtls.CurveID
	retried bool
	// group is what the key exchange will use, chosen when a share for it is
	// at hand.
	group stdtls.CurveID

	transcript []byte
	hsBuf      []byte
	ccsSeen    int

	// version is what the handshake serves, once the ClientHello has said,
	// and allow13 and allow12 what the Config lets it serve. plan is what
	// serves worked out for a TLS 1.2 client.
	version uint16
	allow13 bool
	allow12 bool
	plan    *plan12
	suiteID uint16
	suite   suite
	cert    *stdtls.Certificate
	alg     uint16
	// pick is Config.GetCertificate, called off the handshake's goroutine
	// where there is a layer to wake it again (see auth.go).
	pick certCall

	// The client's certificates, if it was asked for them and sent any, as
	// sent and parsed, and the chains they were verified with.
	peerRaw    [][]byte
	peerCerts  []*x509.Certificate
	peerChains [][]*x509.Certificate

	// TLS 1.2: the ECDHE key, the master secret, and whether it is the
	// extended one; the Finished messages, which are the connection's
	// tls-unique; and whether the handshake resumed a ticket.
	ecdhe          *ecdh.PrivateKey
	ems            bool
	clientFinished []byte
	serverFinished []byte

	clientHS  []byte
	serverHS  []byte
	master    []byte
	appClient []byte
	appServer []byte
	// rx and tx are the keys in use: reading the client's, writing ours.
	// appRx is what reading switches to once the client's Finished is in.
	rx, tx *recordKeys
	appRx  *recordKeys
	rxOn   bool
	txOn   bool
	// ccsSent says the change_cipher_spec for middleboxes has gone out.
	ccsSent bool

	protocol string

	// tickets seal and open session tickets, or are nil when the Config
	// disables them. psk is the secret of the ticket the handshake resumed,
	// if it did.
	tickets *ticketKeys
	psk     []byte
	resumed bool

	// quic is set for a QUIC handshake, which delivers its messages and
	// secrets as events instead of records (RFC 9001).
	quic       *quicCtx
	peerParams []byte

	// 0-RTT, for QUIC: whether the client's early data is accepted, the hash
	// of the ClientHello its keys derive from, and, while the QUIC layer
	// decides whether to accept it, what remains of the handshake and the
	// session the layer is asked about.
	earlyData bool
	earlyHash []byte
	// skipEarly is how many bytes of a client's early data over TCP, which is
	// not accepted, may still be skipped.
	skipEarly   int
	pending     func() *hsError
	resumeState *stdtls.SessionState
	// Early data over TCP that is accepted (RFC 8446 section 4.2.10): whether
	// the Handler takes it and how much, the keys it is read under and the
	// handshake keys the client's Finished is, whether the client's
	// EndOfEarlyData is still to come, and the plaintext so far and its size.
	// Once the flight is sent the layer sends under tx, and the ticket that
	// follows the client's Finished is left to it.
	tcpEarly   bool
	maxEarly   uint32
	earlyRx    *recordKeys
	hsRx       *recordKeys
	earlyOn    bool
	earlyBuf   []byte
	earlyTotal uint32
	ticketMsg  []byte
}

func (h *serverHS) now() time.Time {
	if h.config.Time != nil {
		return h.config.Time()
	}
	return time.Now()
}

// newServerHS prepares a handshake with config, which nativeServer qualified,
// for the connection conn.
func newServerHS(config *stdtls.Config, conn net.Conn, tickets *ticketKeys) *serverHS {
	return newServerHSFor(config, conn, tickets, nil)
}

// newServerHSFor is newServerHS for a handshake that is QUIC's if q is not nil:
// TLS 1.3 alone.
func newServerHSFor(config *stdtls.Config, conn net.Conn, tickets *ticketKeys, q *quicCtx) *serverHS {
	h := &serverHS{config: config, conn: conn, tickets: tickets, quic: q}
	h.allow13, h.allow12 = serverVersions(config)
	if q != nil {
		h.allow13, h.allow12 = true, false
	}
	h.groups = handshakeCurves
	if len(config.CurvePreferences) != 0 {
		h.groups = nil
		for _, id := range config.CurvePreferences {
			if curveOf(id) != nil && !slices.Contains(h.groups, id) {
				h.groups = append(h.groups, id)
			}
		}
	}
	return h
}

func (h *serverHS) rander() io.Reader {
	if h.config.Rand != nil {
		return h.config.Rand
	}
	return rand.Reader
}

type keyShare struct {
	group stdtls.CurveID
	data  []byte
}

// clientHelloMsg is a ClientHello, parsed.
type clientHelloMsg struct {
	raw           []byte
	legacy        uint16
	random        []byte
	sessionID     []byte
	suites        []uint16
	compression   []byte
	exts          map[uint16][]byte
	serverName    string
	versions      []uint16
	groups        []stdtls.CurveID
	shares        []keyShare
	sigAlgs       []uint16
	protos        []string
	earlyData     bool
	statusRequest bool
	sct           bool
	// the pre_shared_key extension, which ends the message: the tickets
	// offered, their binders, and how many bytes the binders take.
	psks       []clientPSK
	binders    [][]byte
	bindersLen int
	pskModes   []byte
	// extBlock is the extensions as they were sent, in their order.
	extBlock []byte
	// TLS 1.2: whether the client offers the extended master secret, the
	// session ticket extension and the ticket it holds, the point formats it
	// reads, and whether it signalled secure renegotiation.
	ems         bool
	ticketExt   bool
	ticket      []byte
	pointFormat []byte
	havePoints  bool
	renegotiate bool
}

// extensionIDs are the types of the extensions in the order they were sent.
func (ch *clientHelloMsg) extensionIDs() []uint16 {
	ids := make([]uint16, 0, len(ch.exts))
	r := newReader(ch.extBlock)
	for len(r.b) > 0 && r.ok {
		ids = append(ids, r.u16())
		r.vec16()
	}
	return ids
}

type clientPSK struct {
	identity []byte
	age      uint32
}

// parseClientHello reads a ClientHello message, header included. It reports
// false for one that does not parse, which crypto/tls then answers.
func parseClientHello(msg []byte) (*clientHelloMsg, bool) {
	if len(msg) < 4 || msg[0] != hsClientHello {
		return nil, false
	}
	r := newReader(msg[4:])
	ch := &clientHelloMsg{raw: msg}
	ch.legacy = r.u16()
	ch.random = r.take(32)
	ch.sessionID = r.vec8()
	suites := newReader(r.vec16())
	ch.compression = r.vec8()
	extensions := r.vec16()
	if !r.empty() || len(ch.sessionID) > 32 || !suites.ok || len(suites.b)%2 != 0 || len(ch.compression) == 0 {
		return nil, false
	}
	for len(suites.b) > 0 {
		ch.suites = append(ch.suites, suites.u16())
	}
	exts, herr := parseExtensions(extensions)
	if herr != nil {
		return nil, false
	}
	ch.exts, ch.extBlock = exts, extensions
	for typ, data := range exts {
		d := newReader(data)
		switch typ {
		case extServerName:
			list := newReader(d.vec16())
			for len(list.b) > 0 && list.ok {
				nameType := list.u8()
				name := list.vec16()
				if nameType == 0 && ch.serverName == "" {
					ch.serverName = string(name)
				}
			}
			if !d.empty() || !list.ok {
				return nil, false
			}
		case extSupportedVersions:
			list := newReader(d.vec8())
			for len(list.b) > 0 && list.ok {
				ch.versions = append(ch.versions, list.u16())
			}
			if !d.empty() || !list.ok {
				return nil, false
			}
		case extSupportedGroups:
			list := newReader(d.vec16())
			for len(list.b) > 0 && list.ok {
				ch.groups = append(ch.groups, stdtls.CurveID(list.u16()))
			}
			if !d.empty() || !list.ok {
				return nil, false
			}
		case extKeyShare:
			list := newReader(d.vec16())
			for len(list.b) > 0 && list.ok {
				group := stdtls.CurveID(list.u16())
				ch.shares = append(ch.shares, keyShare{group, list.vec16()})
			}
			if !d.empty() || !list.ok {
				return nil, false
			}
		case extSignatureAlgs:
			list := newReader(d.vec16())
			for len(list.b) > 0 && list.ok {
				ch.sigAlgs = append(ch.sigAlgs, list.u16())
			}
			if !d.empty() || !list.ok {
				return nil, false
			}
		case extALPN:
			list := newReader(d.vec16())
			for len(list.b) > 0 && list.ok {
				ch.protos = append(ch.protos, string(list.vec8()))
			}
			if !d.empty() || !list.ok {
				return nil, false
			}
		case 42: // early_data
			ch.earlyData = true
		case extExtendedMasterSec:
			ch.ems = len(data) == 0
		case extSessionTicket:
			ch.ticketExt, ch.ticket = true, data
		case extECPointFormats:
			ch.havePoints, ch.pointFormat = true, d.vec8()
			if !d.empty() {
				return nil, false
			}
		case extRenegotiationInfo:
			ch.renegotiate = true
		case extPSKModes:
			ch.pskModes = d.vec8()
			if !d.empty() {
				return nil, false
			}
		case extPreSharedKey:
			// It has to be the last extension: the binders are computed over
			// the message up to them.
			if !bytes.HasSuffix(msg, data) {
				return nil, false
			}
			identities := newReader(d.vec16())
			for len(identities.b) > 0 && identities.ok {
				id := identities.vec16()
				ch.psks = append(ch.psks, clientPSK{id, identities.u32()})
			}
			bindersAt := len(d.b)
			binders := newReader(d.vec16())
			for len(binders.b) > 0 && binders.ok {
				ch.binders = append(ch.binders, binders.vec8())
			}
			ch.bindersLen = bindersAt - len(d.b)
			if !d.empty() || !identities.ok || !binders.ok || len(ch.psks) != len(ch.binders) || len(ch.psks) == 0 {
				return nil, false
			}
		case extStatusRequest:
			ch.statusRequest = true
		case 18: // signed_certificate_timestamp
			ch.sct = true
		}
	}
	if slices.Contains(ch.suites, 0x00ff) {
		ch.renegotiate = true
	}
	if len(ch.versions) == 0 && ch.legacy >= stdtls.VersionTLS12 {
		// A client without supported_versions offers the version it names.
		ch.versions = []uint16{ch.legacy}
	}
	return ch, true
}

// choose says which version this handshake serves ch with: TLS 1.3 if the
// client offers it and the Config allows it, else TLS 1.2 if the client can use
// it and there is a certificate, suite and group to serve it with, else zero,
// and crypto/tls gets the connection. A client that offers TLS 1.3 is not
// offered 1.2 instead when it asks for something 1.3 here does not do, as
// crypto/tls would not. TLS 1.2 needs the certificate to decide, and an error
// is GetCertificate's, or errCertWait while it has not answered.
func (h *serverHS) choose(ch *clientHelloMsg) (uint16, *hsError) {
	if h.allow13 && slices.Contains(ch.versions, stdtls.VersionTLS13) {
		if !slices.Contains(ch.compression, 0) {
			return 0, nil
		}
		if !slices.ContainsFunc(ch.suites, func(id uint16) bool { _, ok := h.suite13(id); return ok }) {
			return 0, nil
		}
		for _, g := range h.groups {
			if slices.Contains(ch.groups, g) {
				return stdtls.VersionTLS13, nil
			}
		}
		return 0, nil
	}
	if h.allow12 && slices.Contains(ch.versions, stdtls.VersionTLS12) {
		plan, herr := h.plan12(ch)
		if herr != nil {
			return 0, herr
		}
		if plan != nil {
			h.plan = plan
			return stdtls.VersionTLS12, nil
		}
	}
	return 0, nil
}

// serves reports whether this handshake can serve ch, or that it cannot tell
// before GetCertificate has answered: peekServe, peekFallback or peekMore. A
// client that GetCertificate fails is served, to be told so.
func (h *serverHS) serves(ch *clientHelloMsg) int {
	if h.quic != nil {
		if h.servesQUIC(ch) {
			return peekServe
		}
		return peekFallback
	}
	switch v, herr := h.choose(ch); {
	case herr == errCertWait:
		return peekMore
	case herr != nil || v != 0:
		return peekServe
	}
	return peekFallback
}

// servesQUIC is serves for QUIC, whose ClientHello must offer TLS 1.3 and
// nothing older, an application protocol the server has, and its transport
// parameters. A client that does not is answered by crypto/tls, which has the
// alerts for it.
func (h *serverHS) servesQUIC(ch *clientHelloMsg) bool {
	if !slices.Contains(ch.versions, stdtls.VersionTLS13) || !slices.Contains(ch.compression, 0) || len(ch.sessionID) != 0 {
		return false
	}
	for _, v := range ch.versions {
		if v < stdtls.VersionTLS13 {
			return false
		}
	}
	if _, ok := ch.exts[extQUICParams]; !ok {
		return false
	}
	if len(h.config.NextProtos) > 0 && !slices.ContainsFunc(ch.protos, func(p string) bool { return slices.Contains(h.config.NextProtos, p) }) {
		return false
	}
	if !slices.ContainsFunc(ch.suites, func(id uint16) bool { _, ok := quicSuite(id); return ok }) {
		return false
	}
	for _, g := range h.groups {
		if slices.Contains(ch.groups, g) {
			return true
		}
	}
	return false
}

// suite13 looks up a TLS 1.3 suite: those the record layer protects, or, for
// QUIC, any of the three, since only their hash is needed.
func (h *serverHS) suite13(id uint16) (suite, bool) {
	if h.quic != nil {
		return quicSuite(id)
	}
	return lookupSuite(stdtls.VersionTLS13, id)
}

// peek reads the ClientHello at the start of in without consuming anything,
// and decides whether to serve the connection.
func (h *serverHS) peek(in []byte) (status int) {
	const (
		needMore = peekMore
		fallback = peekFallback
	)
	var msg []byte
	for len(in) >= recordHeaderLen {
		if in[0] != recordTypeHandshake || in[1] != 3 {
			return fallback
		}
		length := int(in[3])<<8 | int(in[4])
		if length > maxPlaintext {
			return fallback
		}
		if len(in) < recordHeaderLen+length {
			return needMore
		}
		msg = append(msg, in[recordHeaderLen:recordHeaderLen+length]...)
		in = in[recordHeaderLen+length:]
		if len(msg) >= 4 {
			size := 4 + (int(msg[1])<<16 | int(msg[2])<<8 | int(msg[3]))
			if size > maxHandshakeMessage {
				return fallback
			}
			if len(msg) >= size {
				ch, ok := parseClientHello(msg[:size])
				if !ok {
					return fallback
				}
				return h.serves(ch)
			}
		}
	}
	return needMore
}

const extPSKModes = 45

const (
	peekMore = iota
	peekServe
	peekFallback
)

// process consumes the records in in that are complete, as clientHS.process
// does, and reports how much of it that was, what to send the peer, and
// whether the handshake is over. Until the ClientHello has arrived and been
// accepted it consumes nothing. While GetCertificate has not answered, it
// consumes nothing either; once it has, a call goes on from where the
// handshake stopped, with more of in or none.
func (h *serverHS) process(in []byte) (consumed int, out []byte, done bool, err *hsError) {
	if !h.decided {
		switch h.peek(in) {
		case peekMore:
			return 0, nil, false, nil
		case peekFallback:
			return 0, nil, false, errFallback
		}
		h.decided = true
	}
	if waiting, herr := h.pick.proceed(&out); waiting || herr != nil {
		return 0, out, false, herr
	}
	for h.state != srvDone && !h.pick.waiting() && len(in)-consumed >= recordHeaderLen {
		rec := in[consumed:]
		length := int(rec[3])<<8 | int(rec[4])
		limit := maxCiphertext13
		if h.version == stdtls.VersionTLS12 {
			limit = maxCiphertext12
		}
		if length > limit {
			return consumed, out, false, fail(alertRecordOverflow, "oversized record")
		}
		if len(rec) < recordHeaderLen+length {
			break
		}
		record := rec[:recordHeaderLen+length]
		consumed += len(record)
		if herr := h.record(record, &out); herr != nil {
			return consumed, out, false, herr
		}
	}
	return consumed, out, h.state == srvDone, nil
}

// maxSkippedEarly bounds the early data of a client that is skipped: a ticket
// this server made allows none, so a client that sends any went by a ticket of
// another server's, and not much of it is to be expected.
const maxSkippedEarly = 64 << 10

// skipEarlyRecord drops a record of early data, if the handshake has been told
// to expect some and this is where it is: it is protected, and not under the
// handshake keys. It reports whether it did.
func (h *serverHS) skipEarlyRecord(record []byte) bool {
	if h.skipEarly <= 0 {
		return false
	}
	h.skipEarly -= len(record) - recordHeaderLen
	return true
}

func (h *serverHS) record(record []byte, out *[]byte) *hsError {
	typ, body := record[0], record[recordHeaderLen:]
	if record[1] != 3 {
		return fail(alertProtocolVersion, "unsupported record version %#x%02x", record[1], record[2])
	}
	if h.version == stdtls.VersionTLS12 {
		return h.record12(record, out)
	}
	switch typ {
	case recordTypeChangeCipherSpec:
		// Sent by a client for middleboxes, and ignored (RFC 8446 section 5).
		h.ccsSeen++
		if len(body) != 1 || body[0] != 1 || h.ccsSeen > 2 {
			return fail(alertUnexpectedMessage, "unexpected change_cipher_spec")
		}
		return nil
	case recordTypeAlert:
		if h.rxOn {
			return fail(alertUnexpectedMessage, "unprotected alert after the ServerHello")
		}
		return remoteAlert(body)
	case recordTypeHandshake:
		if h.rxOn {
			return fail(alertUnexpectedMessage, "unprotected handshake record after the ServerHello")
		}
		return h.handshakeData(body, out)
	case recordTypeApplicationData:
		if !h.rxOn {
			if h.skipEarlyRecord(record) {
				// Early data after a HelloRetryRequest.
				return nil
			}
			return fail(alertUnexpectedMessage, "protected record before the ServerHello")
		}
		inner, plain, alert, ok := h.rx.open(record)
		if !ok {
			if h.awaitsClientFlight() && h.skipEarlyRecord(record) {
				return nil
			}
			return fail(alert, "bad record")
		}
		switch inner {
		case recordTypeHandshake:
			return h.handshakeData(plain, out)
		case recordTypeAlert:
			return remoteAlert(plain)
		case recordTypeApplicationData:
			if h.earlyOn {
				if h.earlyTotal += uint32(len(plain)); h.earlyTotal > h.maxEarly {
					return fail(alertUnexpectedMessage, "more early data than the ticket allows")
				}
				h.earlyBuf = append(h.earlyBuf, plain...)
				return nil
			}
		}
		return fail(alertUnexpectedMessage, "unexpected record type %d during the handshake", inner)
	}
	return fail(alertUnexpectedMessage, "unexpected record type %d", typ)
}

func (h *serverHS) handshakeData(data []byte, out *[]byte) *hsError {
	h.hsBuf = append(h.hsBuf, data...)
	for len(h.hsBuf) >= 4 {
		size := 4 + (int(h.hsBuf[1])<<16 | int(h.hsBuf[2])<<8 | int(h.hsBuf[3]))
		if size > maxHandshakeMessage {
			return fail(alertInternalError, "handshake message too large")
		}
		if len(h.hsBuf) < size {
			break
		}
		msg := h.hsBuf[:size:size]
		if herr := h.message(msg, out); herr != nil {
			return herr
		}
		h.hsBuf = h.hsBuf[size:]
		if len(h.hsBuf) != 0 && msg[0] == hsClientHello {
			// The keys change after the ClientHello, or it is our turn.
			return fail(alertUnexpectedMessage, "handshake data after the last message of a flight")
		}
		if h.state == srvDone && len(h.hsBuf) != 0 {
			return fail(alertUnexpectedMessage, "handshake data after the Finished")
		}
	}
	if len(h.hsBuf) == 0 {
		h.hsBuf = nil
	}
	return nil
}

// message handles one handshake message from the client.
func (h *serverHS) message(msg []byte, out *[]byte) *hsError {
	switch typ := msg[0]; {
	case (h.state == srvAwaitHello || h.state == srvAwaitHello2) && typ == hsClientHello:
		return h.clientHello(msg, out)
	case h.state == srvAwaitFinished && h.earlyOn && typ == hsEndOfEarlyData:
		return h.endOfEarlyData(msg)
	case h.state == srvAwaitClientCert && typ == hsCertificate:
		return h.clientCertificate13(msg)
	case h.state == srvAwaitClientCertVerify && typ == hsCertificateVerify:
		return h.clientCertificateVerify13(msg)
	case h.state == srvAwaitFinished && !h.earlyOn && typ == hsFinished:
		return h.finished(msg, out)
	case h.state == srv12AwaitClientCert && typ == hsCertificate:
		return h.clientCertificate12(msg)
	case h.state == srv12AwaitKeyExchange && typ == hsClientKeyExchange:
		return h.clientKeyExchange(msg)
	case h.state == srv12AwaitClientCertVerify && typ == hsCertificateVerify:
		return h.clientCertificateVerify12(msg)
	case h.state == srv12AwaitFinished && typ == hsFinished:
		return h.finished12(msg, out)
	}
	return fail(alertUnexpectedMessage, "unexpected handshake message %d", msg[0])
}

// awaitsClientFlight reports whether a TLS 1.3 handshake waits for the
// client's second flight, which ends in its Finished.
func (h *serverHS) awaitsClientFlight() bool {
	return h.state == srvAwaitClientCert || h.state == srvAwaitClientCertVerify || h.state == srvAwaitFinished
}

// requestsCert says whether the handshake asks the client for a certificate:
// whenever the Config authenticates clients, except when it resumes a session,
// which carries the certificates the client sent before.
func (h *serverHS) requestsCert() bool {
	return h.config.ClientAuth != stdtls.NoClientCert && !h.resumed
}

func (h *serverHS) transcriptHash() []byte { return hashBytes(h.suite.hash, h.transcript) }

// clientHello handles the ClientHello, or the second one after a
// HelloRetryRequest, and answers with the server's flight.
func (h *serverHS) clientHello(msg []byte, out *[]byte) *hsError {
	ch, ok := parseClientHello(msg)
	if !ok {
		return fail(alertDecodeError, "malformed ClientHello")
	}
	if h.state == srvAwaitHello2 {
		first := h.ch
		if !bytes.Equal(ch.random, first.random) || !bytes.Equal(ch.sessionID, first.sessionID) ||
			!slices.Equal(ch.suites, first.suites) {
			return fail(alertIllegalParameter, "the second ClientHello differs from the first")
		}
	} else if h.quic != nil {
		if !h.servesQUIC(ch) {
			return fail(alertProtocolVersion, "client offers no handshake this server serves over QUIC")
		}
		h.version = stdtls.VersionTLS13
		h.peerParams = bytes.Clone(ch.exts[extQUICParams])
		h.quic.peer(h.peerParams)
	} else {
		v, herr := h.choose(ch)
		if herr == errCertWait {
			// peek waited for the answer, so this cannot be.
			return fail(alertInternalError, "GetCertificate has not answered")
		}
		if herr != nil {
			return herr
		}
		switch v {
		case stdtls.VersionTLS12:
			h.version = stdtls.VersionTLS12
			return h.clientHello12(ch, msg, out)
		case stdtls.VersionTLS13:
			h.version = stdtls.VersionTLS13
		default:
			return fail(alertProtocolVersion, "client offers no version this server serves")
		}
	}
	h.ch = ch
	if h.state == srvAwaitHello {
		// The suites the record layer protects, in the server's order of
		// preference: AES-GCM first where the CPU has it, and ChaCha20-Poly1305
		// first where it has not or the client puts it first, as crypto/tls
		// orders them. For QUIC, where nothing but the hash depends on the
		// suite, the client's among the three.
		preferred := suites13(!peerPrefersAESGCM(ch.suites))
		if h.quic != nil {
			preferred = ch.suites
		}
		for _, id := range preferred {
			if _, ok := h.suite13(id); ok && slices.Contains(ch.suites, id) {
				h.suiteID = id
				break
			}
		}
		h.suite, _ = h.suite13(h.suiteID)
	}
	// The transcript before this message, which a PSK binder covers with the
	// message up to the binders.
	prior := len(h.transcript)
	h.transcript = append(h.transcript, msg...)

	// The key exchange: the first group the server prefers that the client
	// sent a share for, or else a request for the first it supports.
	var share *keyShare
	var want stdtls.CurveID
	for _, g := range h.groups {
		if !slices.Contains(ch.groups, g) {
			continue
		}
		if i := slices.IndexFunc(ch.shares, func(s keyShare) bool { return s.group == g }); i >= 0 {
			share = &ch.shares[i]
			break
		}
		if want == 0 {
			want = g
		}
	}
	if ch.earlyData && len(ch.psks) == 0 {
		return fail(alertIllegalParameter, "early_data without pre_shared_key")
	}
	if share == nil {
		if h.retried || want == 0 {
			return fail(alertHandshakeFailure, "no key exchange group in common with the client")
		}
		if h.quic == nil && ch.earlyData {
			// A HelloRetryRequest rejects the early data, which the client
			// sends before it has the answer, and which is skipped.
			h.skipEarly = maxSkippedEarly
		}
		return h.helloRetryRequest(want, out)
	}
	// Everything that can fail is done before the ServerHello is sent.
	if herr := h.selectProtocol(); herr != nil {
		return herr
	}
	h.psk, h.resumed = nil, false
	var pskIndex int
	var ticket ticketState
	if psk, index, state, ok := h.resume(ch, h.transcript[:prior]); ok {
		h.psk, h.resumed, pskIndex, ticket = psk, true, index, state
	}
	if h.quic == nil && ch.earlyData {
		if h.resumed && h.tcpEarly && h.earlyAcceptable(ch, ticket, pskIndex) {
			h.earlyData = true
			h.earlyHash = h.transcriptHash()
		} else if h.skipEarly == 0 {
			// Early data that is not accepted is still sent by a client
			// that offered it, after the ClientHello and before it has the
			// answer, in records that are read under no key this side has,
			// and which are skipped (RFC 8446 section 4.2.10).
			h.skipEarly = maxSkippedEarly
		}
	}
	rest := func() *hsError { return h.serve(ch, share, pskIndex, out) }
	if h.quic != nil && ch.earlyData && h.resumed && h.earlyAcceptable(ch, ticket, pskIndex) {
		// The QUIC layer has the last word, since it holds what the early
		// data is bound by: it is asked, and the handshake goes on once it
		// has answered (see QUICConn.NextEvent).
		h.resumeState = &stdtls.SessionState{EarlyData: true, Extra: ticket.extra}
		h.quic.resume(h.resumeState)
		h.pending = func() *hsError {
			if h.earlyData = h.resumeState.EarlyData; h.earlyData {
				h.earlyHash = h.transcriptHash()
			}
			return rest()
		}
		return nil
	}
	return rest()
}

// serve is the rest of the answer to a ClientHello, once it is known whether
// it resumes a session: the certificate, the key exchange, the flight. Until
// GetCertificate has answered, it is set aside, and runs again once it has.
func (h *serverHS) serve(ch *clientHelloMsg, share *keyShare, pskIndex int, out *[]byte) *hsError {
	if !h.resumed {
		cert, herr := h.selectCertificate(ch)
		if herr == errCertWait {
			h.pick.wait(func(out *[]byte) *hsError { return h.serve(ch, share, pskIndex, out) })
			return nil
		}
		if herr != nil {
			return herr
		}
		h.cert = cert
		if h.alg, herr = h.selectSignature(cert, ch, false); herr != nil {
			return herr
		}
	}
	curve := curveOf(share.group)
	peer, err := curve.NewPublicKey(share.data)
	if err != nil {
		return fail(alertIllegalParameter, "invalid client key share: %v", err)
	}
	key, err := curve.GenerateKey(h.rander())
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	shared, err := key.ECDH(peer)
	if err != nil {
		return fail(alertIllegalParameter, "key exchange failed: %v", err)
	}
	h.group = share.group
	return h.flight(key, shared, pskIndex, out)
}

// earlyWindow is how far the age a client reports for its ticket may be from
// the ticket's age here for the client's early data to be taken as fresh (RFC
// 8446 section 8.3).
const earlyWindow = 10 * time.Second

// earlyAcceptable says whether the client's early data can be accepted with
// the ticket it resumed: a ticket made for early data, the same suite and
// protocol, the first ClientHello, a ticket age that fits, and a ticket that
// has not carried early data before.
func (h *serverHS) earlyAcceptable(ch *clientHelloMsg, ticket ticketState, index int) bool {
	if !ticket.earlyData || index != 0 || h.retried || ticket.suite != h.suiteID || ticket.alpn != h.protocol {
		return false
	}
	now := h.now()
	reported := time.Duration(ch.psks[0].age-ticket.ageAdd) * time.Millisecond
	if d := now.Sub(ticket.created) - reported; d > earlyWindow || d < -earlyWindow {
		return false
	}
	return h.tickets.firstUse(ch.psks[0].identity, now)
}

// resume looks for a ticket among the ones the client offered that this
// handshake can resume from, and returns its secret. A ticket is used only if
// it is ours and unaltered, no older than it may be, for the same server name
// and a suite with the hash of the one chosen now, and its binder, which
// proves the client holds the secret, is right over the transcript and the
// ClientHello as far as the binders (RFC 8446 section 4.2.11.2).
func (h *serverHS) resume(ch *clientHelloMsg, prior []byte) (psk []byte, index int, state ticketState, ok bool) {
	if h.tickets == nil || len(ch.psks) == 0 || !slices.Contains(ch.pskModes, 1) {
		return nil, 0, ticketState{}, false
	}
	f := h.suite.hash
	truncated := ch.raw[:len(ch.raw)-ch.bindersLen]
	hashed := hashBytes(f, append(append([]byte(nil), prior...), truncated...))
	for i, id := range ch.psks {
		state, found := h.tickets.open(id.identity, h.now())
		if !found || state.server != ch.serverName {
			continue
		}
		if s, valid := h.suite13(state.suite); !valid || s.hash().Size() != f().Size() {
			continue
		}
		certs, chains, usable := h.sessionPeer(state.certs, state.chains)
		if !usable {
			continue
		}
		early, err := hkdf.Extract(f, state.psk, nil)
		if err != nil {
			continue
		}
		binderKey, err := deriveSecret(f, early, "res binder", hashBytes(f, nil))
		if err != nil {
			continue
		}
		key, err := expandLabelContext(f, binderKey, "finished", nil, f().Size())
		if err != nil {
			continue
		}
		mac := hmac.New(f, key)
		mac.Write(hashed)
		if hmac.Equal(mac.Sum(nil), ch.binders[i]) {
			h.peerRaw, h.peerCerts, h.peerChains = state.certs, certs, chains
			return state.psk, i, state, true
		}
	}
	return nil, 0, ticketState{}, false
}

// selectProtocol picks the ALPN protocol as crypto/tls does: the first of the
// server's that the client offered, no protocol for a client that offered none,
// and an HTTP/1.1 client is let in to an h2 server as if it had not offered
// any.
func (h *serverHS) selectProtocol() *hsError {
	server, client := h.config.NextProtos, h.ch.protos
	if len(server) == 0 || len(client) == 0 {
		return nil
	}
	http11 := false
	for _, s := range server {
		for _, c := range client {
			if s == c {
				h.protocol = s
				return nil
			}
			if s == "h2" && c == "http/1.1" {
				http11 = true
			}
		}
	}
	if http11 {
		return nil
	}
	return fail(alertNoApplicationProtocol, "client requested unsupported application protocols (%s)", client)
}

// hello is what a certificate is chosen against, and what GetCertificate is
// handed.
func (h *serverHS) hello(ch *clientHelloMsg) *stdtls.ClientHelloInfo {
	groups := make([]stdtls.CurveID, 0, len(ch.groups))
	groups = append(groups, ch.groups...)
	schemes := make([]stdtls.SignatureScheme, len(ch.sigAlgs))
	for i, alg := range ch.sigAlgs {
		schemes[i] = stdtls.SignatureScheme(alg)
	}
	points := []uint8{0}
	if ch.havePoints {
		points = bytes.Clone(ch.pointFormat)
	}
	info := &stdtls.ClientHelloInfo{
		CipherSuites:      slices.Clone(ch.suites),
		ServerName:        ch.serverName,
		SupportedCurves:   groups,
		SupportedPoints:   points,
		SignatureSchemes:  schemes,
		SupportedProtos:   slices.Clone(ch.protos),
		SupportedVersions: slices.Clone(ch.versions),
		Extensions:        ch.extensionIDs(),
		Conn:              h.conn,
	}
	setContext(info, helloCtxOffset, context.Background())
	return info
}

// selectCertificate picks the certificate for the client's hello as
// crypto/tls's Config does: GetCertificate's, if it has one for a client that
// names a server or the Config has no certificates of its own, or else the
// only one, or the first that supports the hello, or the first. errCertWait
// means that GetCertificate has yet to answer.
func (h *serverHS) selectCertificate(ch *clientHelloMsg) (*stdtls.Certificate, *hsError) {
	certs := h.config.Certificates
	if get := h.config.GetCertificate; get != nil && (len(certs) == 0 || ch.serverName != "") {
		a, ok := h.pick.ask(func() (*stdtls.Certificate, error) { return get(h.hello(ch)) })
		if !ok {
			return nil, errCertWait
		}
		if a.err != nil {
			return nil, certError(a.err)
		}
		if a.cert != nil {
			if herr := usableCertificate(a.cert); herr != nil {
				return nil, herr
			}
			return a.cert, nil
		}
	}
	if len(certs) == 0 {
		return nil, fail(alertUnrecognizedName, "no certificates configured")
	}
	if len(certs) == 1 {
		return &certs[0], nil
	}
	info := h.hello(ch)
	for i := range certs {
		if info.SupportsCertificate(&certs[i]) == nil {
			return &certs[i], nil
		}
	}
	return &certs[0], nil
}

// selectSignature picks the scheme the certificate's key signs with that the
// client accepts: in the client's order in TLS 1.3, and in the server's in
// TLS 1.2, which also signs a key exchange with PKCS #1.
func (h *serverHS) selectSignature(cert *stdtls.Certificate, ch *clientHelloMsg, tls12 bool) (uint16, *hsError) {
	if alg, ok := chooseSignature(cert.PrivateKey.(crypto.Signer).Public(), ch.sigAlgs, tls12, tls12); ok {
		return alg, nil
	}
	return 0, fail(alertHandshakeFailure, "client doesn't support any of the certificate's signature algorithms")
}

// helloRetryRequest asks the client for a share of group.
func (h *serverHS) helloRetryRequest(group stdtls.CurveID, out *[]byte) *hsError {
	var b builder
	b.u8(hsServerHello)
	b.u24(0)
	b.u16(0x0303)
	b.raw(helloRetryRandom[:])
	b.vec8(func(b *builder) { b.raw(h.ch.sessionID) })
	b.u16(h.suiteID)
	b.u8(0)
	b.vec16(func(b *builder) {
		b.u16(extSupportedVersions)
		b.vec16(func(b *builder) { b.u16(stdtls.VersionTLS13) })
		b.u16(extKeyShare)
		b.vec16(func(b *builder) { b.u16(uint16(group)) })
	})
	msg := b.b
	msg[1], msg[2], msg[3] = byte((len(msg)-4)>>16), byte((len(msg)-4)>>8), byte(len(msg)-4)
	// The first ClientHello is replaced in the transcript by its hash.
	first := hashBytes(h.suite.hash, h.transcript)
	h.transcript = append([]byte{hsMessageHash, 0, 0, byte(len(first))}, first...)
	h.transcript = append(h.transcript, msg...)
	if h.quic != nil {
		h.quic.write(stdtls.QUICEncryptionLevelInitial, msg)
	} else {
		*out = appendRecord(*out, recordTypeHandshake, 0x0303, msg)
		h.sendCCS(out)
	}
	h.retried = true
	h.state = srvAwaitHello2
	return nil
}

func (h *serverHS) sendCCS(out *[]byte) {
	if len(h.ch.sessionID) != 0 && !h.ccsSent {
		h.ccsSent = true
		*out = append(*out, recordTypeChangeCipherSpec, 3, 3, 0, 1, 1)
	}
}

// keyLog writes a secret to Config.KeyLogWriter in the NSS format.
func (h *serverHS) keyLog(label string, secret []byte) {
	if h.config.KeyLogWriter == nil {
		return
	}
	line := fmt.Sprintf("%s %s %s\n", label, hex.EncodeToString(h.ch.random), hex.EncodeToString(secret))
	_, _ = io.WriteString(h.config.KeyLogWriter, line)
}

// flight sends the ServerHello and, under the handshake keys, the rest of the
// server's side: EncryptedExtensions, Certificate, CertificateVerify and
// Finished.
func (h *serverHS) flight(key *ecdh.PrivateKey, shared []byte, pskIndex int, out *[]byte) *hsError {
	f := h.suite.hash
	var b builder
	b.u8(hsServerHello)
	b.u24(0)
	b.u16(0x0303)
	if _, err := io.ReadFull(h.rander(), h.random[:]); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	b.raw(h.random[:])
	b.vec8(func(b *builder) { b.raw(h.ch.sessionID) })
	b.u16(h.suiteID)
	b.u8(0)
	b.vec16(func(b *builder) {
		b.u16(extSupportedVersions)
		b.vec16(func(b *builder) { b.u16(stdtls.VersionTLS13) })
		b.u16(extKeyShare)
		b.vec16(func(b *builder) {
			b.u16(uint16(h.group))
			b.vec16(func(b *builder) { b.raw(key.PublicKey().Bytes()) })
		})
		if h.resumed {
			b.u16(extPreSharedKey)
			b.vec16(func(b *builder) { b.u16(uint16(pskIndex)) })
		}
	})
	hello := b.b
	hello[1], hello[2], hello[3] = byte((len(hello)-4)>>16), byte((len(hello)-4)>>8), byte(len(hello)-4)
	h.transcript = append(h.transcript, hello...)
	if h.quic != nil {
		h.quic.write(stdtls.QUICEncryptionLevelInitial, hello)
	} else {
		*out = appendRecord(*out, recordTypeHandshake, 0x0303, hello)
		h.sendCCS(out)
	}

	// Key schedule to the handshake traffic secrets (RFC 8446 section 7.1).
	zeros := make([]byte, f().Size())
	earlySecret := zeros
	if h.resumed {
		earlySecret = h.psk
	}
	early, err := hkdf.Extract(f, earlySecret, nil)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.earlyData {
		// The client's 0-RTT packets are protected under a secret that
		// covers its ClientHello alone (RFC 8446 section 7.1).
		secret, err := deriveSecret(f, early, "c e traffic", h.earlyHash)
		if err != nil {
			return fail(alertInternalError, "%v", err)
		}
		h.keyLog("CLIENT_EARLY_TRAFFIC_SECRET", secret)
		if h.quic != nil {
			h.quic.setRead(stdtls.QUICEncryptionLevelEarly, h.suiteID, secret)
		} else if h.earlyRx, err = newKeys13(h.suite, f, secret); err != nil {
			return fail(alertInternalError, "%v", err)
		}
	}
	derived, err := deriveSecret(f, early, "derived", hashBytes(f, nil))
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	handshake, err := hkdf.Extract(f, shared, derived)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	t := h.transcriptHash()
	if h.clientHS, err = deriveSecret(f, handshake, "c hs traffic", t); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.serverHS, err = deriveSecret(f, handshake, "s hs traffic", t); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if derived, err = deriveSecret(f, handshake, "derived", hashBytes(f, nil)); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.master, err = hkdf.Extract(f, zeros, derived); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.keyLog("CLIENT_HANDSHAKE_TRAFFIC_SECRET", h.clientHS)
	h.keyLog("SERVER_HANDSHAKE_TRAFFIC_SECRET", h.serverHS)
	if h.quic != nil {
		h.quic.setWrite(stdtls.QUICEncryptionLevelHandshake, h.suiteID, h.serverHS)
		h.quic.setRead(stdtls.QUICEncryptionLevelHandshake, h.suiteID, h.clientHS)
	} else {
		if h.rx, err = newKeys13(h.suite, f, h.clientHS); err != nil {
			return fail(alertInternalError, "%v", err)
		}
		if h.tx, err = newKeys13(h.suite, f, h.serverHS); err != nil {
			return fail(alertInternalError, "%v", err)
		}
		h.rxOn, h.txOn = true, true
		if h.earlyRx != nil {
			// The client's early data comes first, under its own keys, and
			// then its EndOfEarlyData; the handshake keys follow.
			h.hsRx, h.rx = h.rx, h.earlyRx
			h.earlyOn = true
		}
	}

	var sealed []byte
	send := func(msg []byte) *hsError {
		h.transcript = append(h.transcript, msg...)
		if h.quic != nil {
			h.quic.write(stdtls.QUICEncryptionLevelHandshake, msg)
			return nil
		}
		// A message longer than a record is split across records.
		for len(msg) > 0 {
			n := min(len(msg), maxPlaintext)
			var serr error
			if sealed, serr = h.tx.seal(sealed, recordTypeHandshake, msg[:n], nil); serr != nil {
				return fail(alertInternalError, "%v", serr)
			}
			msg = msg[n:]
		}
		return nil
	}

	// EncryptedExtensions: the ALPN protocol, if one was chosen.
	var ee builder
	ee.u8(hsEncryptedExtensions)
	ee.u24(0)
	ee.vec16(func(b *builder) {
		if h.protocol != "" {
			b.u16(extALPN)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.raw([]byte(h.protocol)) }) })
			})
		}
		if h.quic != nil {
			b.u16(extQUICParams)
			b.vec16(func(b *builder) { b.raw(h.quic.localParams) })
		}
		if h.earlyData {
			b.u16(extEarlyData)
			b.vec16(func(b *builder) {})
		}
	})
	eem := ee.b
	eem[1], eem[2], eem[3] = byte((len(eem)-4)>>16), byte((len(eem)-4)>>8), byte(len(eem)-4)
	if herr := send(eem); herr != nil {
		return herr
	}
	if h.requestsCert() {
		if herr := send(h.requestCertificate(false)); herr != nil {
			return herr
		}
	}

	if !h.resumed {
		// Certificate: the chain, with the OCSP response and the signed
		// timestamps for a client that asked for them.
		var cm builder
		cm.u8(hsCertificate)
		cm.u24(0)
		cm.u8(0) // certificate_request_context
		cm.u24(0)
		listAt := len(cm.b) - 3
		for i, der := range h.cert.Certificate {
			cm.u24(len(der))
			cm.raw(der)
			cm.vec16(func(b *builder) {
				if i != 0 {
					return
				}
				if h.ch.statusRequest && len(h.cert.OCSPStaple) > 0 {
					b.u16(extStatusRequest)
					b.vec16(func(b *builder) {
						b.u8(1) // ocsp
						b.u24(len(h.cert.OCSPStaple))
						b.raw(h.cert.OCSPStaple)
					})
				}
				if h.ch.sct && len(h.cert.SignedCertificateTimestamps) > 0 {
					b.u16(18)
					b.vec16(func(b *builder) {
						b.vec16(func(b *builder) {
							for _, sct := range h.cert.SignedCertificateTimestamps {
								b.vec16(func(b *builder) { b.raw(sct) })
							}
						})
					})
				}
			})
		}
		certMsg := cm.b
		certMsg[1], certMsg[2], certMsg[3] = byte((len(certMsg)-4)>>16), byte((len(certMsg)-4)>>8), byte(len(certMsg)-4)
		listLen := len(certMsg) - listAt - 3
		certMsg[listAt], certMsg[listAt+1], certMsg[listAt+2] = byte(listLen>>16), byte(listLen>>8), byte(listLen)
		if herr := send(certMsg); herr != nil {
			return herr
		}

		// CertificateVerify.
		signature, herr := h.sign(certificateVerifyContent(true, h.transcriptHash()))
		if herr != nil {
			return herr
		}
		if herr := send(appendCertificateVerify(h.alg, signature)); herr != nil {
			return herr
		}

	}

	// Finished.
	verify, err := h.finishedMAC(h.serverHS)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if herr := send(append([]byte{hsFinished, 0, 0, byte(len(verify))}, verify...)); herr != nil {
		return herr
	}
	if h.quic == nil {
		*out = append(*out, sealed...)
	}

	// Application secrets cover the transcript through the server's Finished.
	t = h.transcriptHash()
	if h.appClient, err = deriveSecret(f, h.master, "c ap traffic", t); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.appServer, err = deriveSecret(f, h.master, "s ap traffic", t); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	exporter, err := deriveSecret(f, h.master, "exp master", t)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.keyLog("CLIENT_TRAFFIC_SECRET_0", h.appClient)
	h.keyLog("SERVER_TRAFFIC_SECRET_0", h.appServer)
	h.keyLog("EXPORTER_SECRET", exporter)
	if h.quic != nil {
		// The server writes under its application keys from here; the
		// client's Finished is still read under its handshake keys.
		h.quic.setWrite(stdtls.QUICEncryptionLevelApplication, h.suiteID, h.appServer)
		h.state = h.clientFlightState()
		return nil
	}
	if h.appRx, err = newKeys13(h.suite, f, h.appClient); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	// What the server writes from here on is under its application keys; the
	// client's Finished is still read under its handshake keys.
	if h.tx, err = newKeys13(h.suite, f, h.appServer); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.state = h.clientFlightState()
	return nil
}

// clientFlightState is what a TLS 1.3 handshake waits for once its flight is
// out: the client's certificate if it asked for one, else its Finished.
func (h *serverHS) clientFlightState() srvState {
	if h.requestsCert() {
		return srvAwaitClientCert
	}
	return srvAwaitFinished
}

// sign signs the CertificateVerify content, or in TLS 1.2 the key exchange,
// with the certificate's key.
func (h *serverHS) sign(signed []byte) ([]byte, *hsError) {
	signature, err := signHandshake(h.cert.PrivateKey.(crypto.Signer), h.alg, h.rander(), signed)
	if err != nil {
		return nil, fail(alertInternalError, "failed to sign handshake: %v", err)
	}
	return signature, nil
}

func (h *serverHS) finishedMAC(base []byte) ([]byte, error) {
	key, err := expandLabelContext(h.suite.hash, base, "finished", nil, h.suite.hash().Size())
	if err != nil {
		return nil, err
	}
	mac := hmac.New(h.suite.hash, key)
	mac.Write(h.transcriptHash())
	return mac.Sum(nil), nil
}

// endOfEarlyData takes the client's EndOfEarlyData, which ends its early data:
// what it sends next is under the handshake keys.
func (h *serverHS) endOfEarlyData(msg []byte) *hsError {
	if len(msg) != 4 || len(h.hsBuf) != 4 {
		return fail(alertDecodeError, "malformed EndOfEarlyData")
	}
	h.transcript = append(h.transcript, msg...)
	h.rx, h.earlyOn = h.hsRx, false
	return nil
}

// takeEarly returns the early data that has arrived since the last call.
func (h *serverHS) takeEarly() []byte {
	b := h.earlyBuf
	h.earlyBuf = nil
	return b
}

// earlyWriteKeys are the keys the server sends under from the end of its
// flight on, if it takes early data: it may answer before the client's
// Finished has come, which the client does not send before it has the flight
// (RFC 8446 section 2.3). Once the layer sends with them, the ticket that
// follows the Finished is the layer's to seal.
func (h *serverHS) earlyWriteKeys() *recordKeys {
	if h.earlyData && h.quic == nil && h.state == srvAwaitFinished {
		return h.tx
	}
	return nil
}

// takeTicket returns the ticket message that was left to the layer to send.
func (h *serverHS) takeTicket() []byte {
	m := h.ticketMsg
	h.ticketMsg = nil
	return m
}

// finished checks the client's Finished, which completes the handshake.
func (h *serverHS) finished(msg []byte, out *[]byte) *hsError {
	want, err := h.finishedMAC(h.clientHS)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if !hmac.Equal(want, msg[4:]) {
		return fail(alertDecryptError, "invalid client finished hash")
	}
	h.transcript = append(h.transcript, msg...)
	if h.quic != nil {
		h.quic.setRead(stdtls.QUICEncryptionLevelApplication, h.suiteID, h.appClient)
	} else {
		h.rx = h.appRx
		if herr := h.sendTicket(out); herr != nil {
			return herr
		}
	}
	if verify := h.config.VerifyConnection; verify != nil {
		if err := verify(h.connectionState()); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.state = srvDone
	if h.quic != nil {
		h.quic.done()
	}
	return nil
}

// sendTicket sends the client a ticket to resume from (RFC 8446 section
// 4.6.1), under the keys the connection goes on with, if the Config allows
// tickets and the client said it can use them.
func (h *serverHS) sendTicket(out *[]byte) *hsError {
	msg, herr := h.ticketMessage(h.tcpEarly, nil)
	if herr != nil || msg == nil {
		return herr
	}
	if h.earlyData {
		// The layer sends under tx by now.
		h.ticketMsg = msg
		return nil
	}
	// The ticket carries the client's certificates, so it may be longer than
	// a record: it is split across records as the flight's messages are.
	sealed := *out
	for len(msg) > 0 {
		n := min(len(msg), maxPlaintext)
		var err error
		if sealed, err = h.tx.seal(sealed, recordTypeHandshake, msg[:n], nil); err != nil {
			return fail(alertInternalError, "%v", err)
		}
		msg = msg[n:]
	}
	*out = sealed
	return nil
}

// ticketMessage builds the NewSessionTicket for the session, or returns nil if
// the Config disables tickets or the client cannot use them.
func (h *serverHS) ticketMessage(earlyData bool, extra [][]byte) ([]byte, *hsError) {
	if h.tickets == nil || !slices.Contains(h.ch.pskModes, 1) {
		return nil, nil
	}
	f := h.suite.hash
	resumption, err := deriveSecret(f, h.master, "res master", h.transcriptHash())
	if err != nil {
		return nil, fail(alertInternalError, "%v", err)
	}
	nonce := []byte{0}
	psk, err := expandLabelContext(f, resumption, "resumption", nonce, f().Size())
	if err != nil {
		return nil, fail(alertInternalError, "%v", err)
	}
	var ageAdd [4]byte
	if _, err := io.ReadFull(h.rander(), ageAdd[:]); err != nil {
		return nil, fail(alertInternalError, "%v", err)
	}
	now := h.now()
	ticket, ok := h.tickets.seal(ticketState{
		suite: h.suiteID, created: now, ageAdd: binary.BigEndian.Uint32(ageAdd[:]), psk: psk, server: h.ch.serverName,
		earlyData: earlyData, alpn: h.protocol, extra: extra, certs: h.peerRaw, chains: rawChains(h.peerChains),
	}, now)
	if !ok {
		return nil, nil
	}
	var b builder
	b.u8(hsNewSessionTicket)
	b.u24(0)
	b.u32(uint32(ticketLifetime / time.Second))
	b.u32(binary.BigEndian.Uint32(ageAdd[:]))
	b.vec8(func(b *builder) { b.raw(nonce) })
	b.vec16(func(b *builder) { b.raw(ticket) })
	b.vec16(func(b *builder) {
		if earlyData {
			// The ticket may be used for 0-RTT; QUIC always says "all of it"
			// (RFC 9001 section 4.6.1).
			b.u16(extEarlyData)
			b.vec16(func(b *builder) {
				if h.quic != nil {
					b.u32(0xffffffff)
				} else {
					b.u32(h.maxEarly)
				}
			})
		}
	})
	msg := b.b
	msg[1], msg[2], msg[3] = byte((len(msg)-4)>>16), byte((len(msg)-4)>>8), byte(len(msg)-4)
	return msg, nil
}

func (h *serverHS) connectionState() stdtls.ConnectionState {
	if h.version == stdtls.VersionTLS12 {
		return h.connectionState12()
	}
	return stdtls.ConnectionState{
		Version:                    stdtls.VersionTLS13,
		HandshakeComplete:          true,
		DidResume:                  h.resumed,
		CipherSuite:                h.suiteID,
		NegotiatedProtocol:         h.protocol,
		NegotiatedProtocolIsMutual: h.protocol != "",
		ServerName:                 h.ch.serverName,
		PeerCertificates:           h.peerCerts,
		VerifiedChains:             h.peerChains,
	}
}

func (h *serverHS) recordKeys() (rx, tx *recordKeys) { return h.rx, h.tx }

// alertRecord is the record that tells the peer the handshake has failed with
// alert: under our keys once there are any.
func (h *serverHS) alertRecord(alert uint8) []byte {
	body := []byte{alertLevelError, alert}
	if !h.txOn || h.state == srvDone {
		return appendRecord(nil, recordTypeAlert, 0x0303, body)
	}
	record, err := h.tx.seal(nil, recordTypeAlert, body, nil)
	if err != nil {
		return nil
	}
	return record
}
