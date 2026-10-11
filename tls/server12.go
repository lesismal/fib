package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rsa"
	stdtls "crypto/tls"
	"io"
	"slices"
	"time"
)

// The TLS 1.2 half of the server handshake (RFC 5246, with RFC 7627, RFC 5746
// and RFC 8422): ECDHE key exchange signed by the certificate, the extended
// master secret, and session tickets (RFC 5077) for resumption. It follows the
// shape of the TLS 1.3 half in server13.go, and of clientHS's TLS 1.2 half in
// client12.go.
//
// A client is served with TLS 1.2 only if everything it needs is here, which
// plan12 settles before anything is sent: an ECDHE suite the certificate's key
// can be used with and the record layer protects, a group, and a signature
// scheme the key signs with. Key exchange by RSA, which crypto/tls no longer
// offers by default either, and the rest stay with crypto/tls.

// serverSuites12 are the TLS 1.2 suites in the server's order of preference,
// as crypto/tls ranks the ones the record layer protects, and whether each
// signs with an ECDSA (or Ed25519) key and not an RSA one.
var serverSuites12 = []struct {
	id    uint16
	ecdsa bool
}{
	{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, true},
	{stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, false},
	{stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, true},
	{stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, false},
	{stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, false},
	{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, true},
	{stdtls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, false},
	{stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, true},
	{stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, false},
	{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, true},
}

// plan12 is what a TLS 1.2 handshake with a client will use.
type plan12 struct {
	cert  *stdtls.Certificate
	alg   uint16
	suite uint16
	group stdtls.CurveID
}

// allowsSuite12 reports whether the Config lets id be used.
func (h *serverHS) allowsSuite12(id uint16) bool {
	return len(h.config.CipherSuites) == 0 || slices.Contains(h.config.CipherSuites, id)
}

// plan12 works out how to serve ch with TLS 1.2, or reports that it cannot.
func (h *serverHS) plan12(ch *clientHelloMsg) *plan12 {
	if !slices.Contains(ch.compression, 0) || len(ch.sigAlgs) == 0 || len(ch.groups) == 0 {
		return nil
	}
	if ch.havePoints && !slices.Contains(ch.pointFormat, 0) {
		return nil
	}
	cert, herr := h.selectCertificate(ch)
	if herr != nil {
		return nil
	}
	alg, herr := h.selectSignature(cert, ch, true)
	if herr != nil {
		return nil
	}
	var ecdsaKey bool
	switch cert.PrivateKey.(crypto.Signer).Public().(type) {
	case *ecdsa.PublicKey, ed25519.PublicKey:
		ecdsaKey = true
	case *rsa.PublicKey:
	default:
		return nil
	}
	p := &plan12{cert: cert, alg: alg}
	for _, s := range serverSuites12 {
		if s.ecdsa != ecdsaKey || !slices.Contains(ch.suites, s.id) || !h.allowsSuite12(s.id) {
			continue
		}
		if _, ok := lookupSuite(stdtls.VersionTLS12, s.id); ok {
			p.suite = s.id
			break
		}
	}
	if p.suite == 0 {
		return nil
	}
	for _, g := range h.groups {
		if slices.Contains(ch.groups, g) {
			p.group = g
			break
		}
	}
	if p.group == 0 {
		return nil
	}
	return p
}

// sendPlain sends a handshake message in the clear, split across records if
// it is longer than one, and adds it to the transcript.
func (h *serverHS) sendPlain(out *[]byte, msg []byte) {
	h.transcript = append(h.transcript, msg...)
	for len(msg) > 0 {
		n := min(len(msg), maxPlaintext)
		*out = appendRecord(*out, recordTypeHandshake, 0x0303, msg[:n])
		msg = msg[n:]
	}
}

// finishMessage sets the length in the header of the message b builds.
func finishMessage(b *builder) []byte {
	msg := b.b
	msg[1], msg[2], msg[3] = byte((len(msg)-4)>>16), byte((len(msg)-4)>>8), byte(len(msg)-4)
	return msg
}

// newRandom makes the server random. A server that also speaks TLS 1.3 marks
// it, when it answers with 1.2, so that a client that offered 1.3 can tell a
// downgrade made by someone else (RFC 8446 section 4.1.3).
func (h *serverHS) newRandom() *hsError {
	if _, err := io.ReadFull(h.rander(), h.random[:]); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.allow13 {
		copy(h.random[24:], downgradeTLS12)
	}
	return nil
}

// serverHello12 builds the ServerHello. extra adds the extensions that only a
// full handshake has.
func (h *serverHS) serverHello12(sessionID []byte, extra func(*builder)) []byte {
	ch := h.ch
	var b builder
	b.u8(hsServerHello)
	b.u24(0)
	b.u16(0x0303)
	b.raw(h.random[:])
	b.vec8(func(b *builder) { b.raw(sessionID) })
	b.u16(h.suiteID)
	b.u8(0)
	b.vec16(func(b *builder) {
		if ch.renegotiate {
			b.u16(extRenegotiationInfo)
			b.vec16(func(b *builder) { b.vec8(func(b *builder) {}) })
		}
		if h.ems {
			b.u16(extExtendedMasterSec)
			b.vec16(func(b *builder) {})
		}
		if ch.havePoints {
			b.u16(extECPointFormats)
			b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.u8(0) }) })
		}
		if h.protocol != "" {
			b.u16(extALPN)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.raw([]byte(h.protocol)) }) })
			})
		}
		if extra != nil {
			extra(b)
		}
	})
	return finishMessage(&b)
}

// clientHello12 handles a ClientHello that is to be served with TLS 1.2, and
// answers with the server's flight: the whole of a full handshake up to the
// ServerHelloDone, or all of a resumed one.
func (h *serverHS) clientHello12(ch *clientHelloMsg, msg []byte, out *[]byte) *hsError {
	h.ch = ch
	h.transcript = append(h.transcript, msg...)
	if ri, ok := ch.exts[extRenegotiationInfo]; ok && (len(ri) != 1 || ri[0] != 0) {
		return fail(alertHandshakeFailure, "unsafe renegotiation attempt")
	}
	if herr := h.selectProtocol(); herr != nil {
		return herr
	}
	h.ems = ch.ems
	if herr := h.newRandom(); herr != nil {
		return herr
	}
	if state, ok := h.resume12(ch); ok {
		return h.flightResume12(state, out)
	}
	return h.flightFull12(h.plan, out)
}

// resume12 looks for a ticket in the ClientHello that this handshake can
// resume from: one that is ours and unaltered, no older than it may be, for the
// same server name, with a suite the client offers and the Config allows, and
// the same use of the extended master secret as now (RFC 7627 section 5.3).
func (h *serverHS) resume12(ch *clientHelloMsg) (ticketState12, bool) {
	if h.tickets == nil || len(ch.ticket) == 0 {
		return ticketState12{}, false
	}
	state, ok := h.tickets.open12(ch.ticket, h.now())
	if !ok || state.server != ch.serverName || state.ems != ch.ems || !slices.Contains(ch.suites, state.suite) ||
		!h.allowsSuite12(state.suite) {
		return ticketState12{}, false
	}
	if _, ok := lookupSuite(stdtls.VersionTLS12, state.suite); !ok {
		return ticketState12{}, false
	}
	return state, true
}

// flightResume12 answers a ClientHello whose ticket was accepted: the abbreviated
// handshake, in which the server speaks first.
func (h *serverHS) flightResume12(state ticketState12, out *[]byte) *hsError {
	h.resumed = true
	h.suiteID = state.suite
	h.suite, _ = lookupSuite(stdtls.VersionTLS12, state.suite)
	h.master = bytes.Clone(state.master)
	// The client's session ID, echoed, is how it knows the ticket was taken.
	h.sendPlain(out, h.serverHello12(h.ch.sessionID, nil))
	if herr := h.keys12(); herr != nil {
		return herr
	}
	if herr := h.sendFinished12(out); herr != nil {
		return herr
	}
	h.state = srv12AwaitCCS
	return nil
}

// flightFull12 answers a ClientHello with the server's side of a full
// handshake, ending in the ServerHelloDone.
func (h *serverHS) flightFull12(p *plan12, out *[]byte) *hsError {
	ch := h.ch
	h.cert, h.alg, h.suiteID, h.group = p.cert, p.alg, p.suite, p.group
	h.suite, _ = lookupSuite(stdtls.VersionTLS12, p.suite)
	key, err := curveOf(p.group).GenerateKey(h.rander())
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.ecdhe = key

	hello := h.serverHello12(nil, func(b *builder) {
		if h.tickets != nil && ch.ticketExt {
			b.u16(extSessionTicket)
			b.vec16(func(b *builder) {})
		}
		if ch.statusRequest && len(h.cert.OCSPStaple) > 0 {
			b.u16(extStatusRequest)
			b.vec16(func(b *builder) {})
		}
		if ch.sct && len(h.cert.SignedCertificateTimestamps) > 0 {
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
	h.sendPlain(out, hello)

	var cm builder
	cm.u8(hsCertificate)
	cm.u24(0)
	cm.u24(0)
	listAt := len(cm.b) - 3
	for _, der := range h.cert.Certificate {
		cm.u24(len(der))
		cm.raw(der)
	}
	certMsg := finishMessage(&cm)
	listLen := len(certMsg) - listAt - 3
	certMsg[listAt], certMsg[listAt+1], certMsg[listAt+2] = byte(listLen>>16), byte(listLen>>8), byte(listLen)
	h.sendPlain(out, certMsg)

	if ch.statusRequest && len(h.cert.OCSPStaple) > 0 {
		var sm builder
		sm.u8(22) // certificate_status
		sm.u24(0)
		sm.u8(1) // ocsp
		sm.u24(len(h.cert.OCSPStaple))
		sm.raw(h.cert.OCSPStaple)
		h.sendPlain(out, finishMessage(&sm))
	}

	// ServerKeyExchange: the ECDHE parameters, signed with the randoms.
	public := key.PublicKey().Bytes()
	params := []byte{3, byte(p.group >> 8), byte(p.group), byte(len(public))}
	params = append(params, public...)
	signed := make([]byte, 0, 64+len(params))
	signed = append(append(append(signed, ch.random...), h.random[:]...), params...)
	signature, herr := h.sign(signed)
	if herr != nil {
		return herr
	}
	var kx builder
	kx.u8(hsServerKeyExchange)
	kx.u24(0)
	kx.raw(params)
	kx.u16(h.alg)
	kx.vec16(func(b *builder) { b.raw(signature) })
	h.sendPlain(out, finishMessage(&kx))

	h.sendPlain(out, []byte{hsServerHelloDone, 0, 0, 0})
	h.state = srv12AwaitKeyExchange
	return nil
}

// clientKeyExchange takes the client's share, and with it the master secret.
func (h *serverHS) clientKeyExchange(msg []byte) *hsError {
	r := newReader(msg[4:])
	public := r.vec8()
	if !r.empty() {
		return fail(alertDecodeError, "malformed ClientKeyExchange")
	}
	curve := curveOf(h.group)
	peer, err := curve.NewPublicKey(public)
	if err != nil {
		return fail(alertIllegalParameter, "invalid client key share: %v", err)
	}
	premaster, err := h.ecdhe.ECDH(peer)
	if err != nil {
		return fail(alertIllegalParameter, "key exchange failed: %v", err)
	}
	h.transcript = append(h.transcript, msg...)
	f := h.suite.hash
	if h.ems {
		h.master = prf12(f, premaster, "extended master secret", h.transcriptHash(), 48)
	} else {
		seed := append(append([]byte(nil), h.ch.random...), h.random[:]...)
		h.master = prf12(f, premaster, "master secret", seed, 48)
	}
	if herr := h.keys12(); herr != nil {
		return herr
	}
	h.state = srv12AwaitCCS
	return nil
}

// keys12 derives the record keys from the master secret: ours are in use once
// our Finished goes out, and the client's once its change_cipher_spec comes.
func (h *serverHS) keys12() *hsError {
	h.keyLog("CLIENT_RANDOM", h.master)
	client, server, err := newKeysFromMaster(stdtls.VersionTLS12, h.suite, h.master, h.ch.random, h.random[:])
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.rx, h.tx = client, server
	return nil
}

// sendFinished12 sends the end of the server's side: a ticket if the client
// asked for one and this is a full handshake, the change_cipher_spec, and the
// Finished.
func (h *serverHS) sendFinished12(out *[]byte) *hsError {
	if !h.resumed && h.tickets != nil && h.ch.ticketExt {
		now := h.now()
		if ticket, ok := h.tickets.seal12(ticketState12{
			suite: h.suiteID, created: now, master: h.master, ems: h.ems, server: h.ch.serverName,
		}, now); ok {
			var b builder
			b.u8(hsNewSessionTicket)
			b.u24(0)
			b.u32(uint32(ticketLifetime / time.Second))
			b.vec16(func(b *builder) { b.raw(ticket) })
			h.sendPlain(out, finishMessage(&b))
		}
	}
	*out = append(*out, recordTypeChangeCipherSpec, 3, 3, 0, 1, 1)
	h.txOn = true
	verify := prf12(h.suite.hash, h.master, "server finished", h.transcriptHash(), 12)
	finished := append([]byte{hsFinished, 0, 0, 12}, verify...)
	h.transcript = append(h.transcript, finished...)
	sealed, err := h.tx.seal(*out, recordTypeHandshake, finished, nil)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	*out = sealed
	h.serverFinished = verify
	return nil
}

// record12 handles one record of a TLS 1.2 handshake.
func (h *serverHS) record12(record []byte, out *[]byte) *hsError {
	typ, body := record[0], record[recordHeaderLen:]
	switch typ {
	case recordTypeChangeCipherSpec:
		if h.state != srv12AwaitCCS || len(body) != 1 || body[0] != 1 || len(h.hsBuf) != 0 {
			return fail(alertUnexpectedMessage, "unexpected change_cipher_spec")
		}
		h.rxOn = true
		h.state = srv12AwaitFinished
		return nil
	case recordTypeAlert, recordTypeHandshake:
		if !h.rxOn {
			if typ == recordTypeAlert {
				return remoteAlert(body)
			}
			return h.handshakeData(body, out)
		}
		inner, plain, alert, ok := h.rx.open(record)
		if !ok {
			return fail(alert, "bad record")
		}
		if inner != typ {
			return fail(alertUnexpectedMessage, "unexpected record type %d", inner)
		}
		if typ == recordTypeAlert {
			return remoteAlert(plain)
		}
		return h.handshakeData(plain, out)
	}
	return fail(alertUnexpectedMessage, "unexpected record type %d during the handshake", typ)
}

// finished12 checks the client's Finished. For a full handshake the server's
// side follows, and the handshake is over; a resumed one sent its Finished
// first.
func (h *serverHS) finished12(msg []byte, out *[]byte) *hsError {
	want := prf12(h.suite.hash, h.master, "client finished", h.transcriptHash(), 12)
	if !hmac.Equal(want, msg[4:]) {
		return fail(alertDecryptError, "invalid client finished hash")
	}
	h.transcript = append(h.transcript, msg...)
	h.clientFinished = want
	if !h.resumed {
		if herr := h.sendFinished12(out); herr != nil {
			return herr
		}
	}
	if verify := h.config.VerifyConnection; verify != nil {
		if err := verify(h.connectionState12()); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.state = srvDone
	return nil
}

func (h *serverHS) connectionState12() stdtls.ConnectionState {
	// tls-unique is the first Finished sent, which a resumed handshake has the
	// server send.
	unique := h.clientFinished
	if h.resumed {
		unique = h.serverFinished
	}
	return stdtls.ConnectionState{
		Version:                    stdtls.VersionTLS12,
		HandshakeComplete:          true,
		DidResume:                  h.resumed,
		CipherSuite:                h.suiteID,
		NegotiatedProtocol:         h.protocol,
		NegotiatedProtocolIsMutual: h.protocol != "",
		ServerName:                 h.ch.serverName,
		TLSUnique:                  bytes.Clone(unique),
	}
}
