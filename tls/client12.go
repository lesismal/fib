package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rsa"
	stdtls "crypto/tls"
	"slices"
)

// The TLS 1.2 half of the client handshake (RFC 5246, with RFC 7627 and RFC
// 8422): ECDHE key exchange, the server's parameters signed by its
// certificate, and the master secret that newKeysFromMaster turns into the
// record layer's keys. It follows the same shape as the TLS 1.3 half in
// client13.go.

// downgradeTLS12 and downgradeTLS11 end the random of a server that supports
// TLS 1.3 and has been made to answer with an older version (RFC 8446 section
// 4.1.3).
var (
	downgradeTLS12 = []byte("DOWNGRD\x01")
	downgradeTLS11 = []byte("DOWNGRD\x00")
)

// serverHello12 handles a ServerHello that chose TLS 1.2.
func (h *clientHS) serverHello12(msg []byte, random, sessionID []byte, cipherSuite uint16, exts map[uint16][]byte) *hsError {
	if h.offers(stdtls.VersionTLS13) && (bytes.Equal(random[24:], downgradeTLS12) || bytes.Equal(random[24:], downgradeTLS11)) {
		return fail(alertIllegalParameter, "downgrade attempt detected, possibly due to a MitM attack or a broken middlebox")
	}
	if !slices.Contains(h.suites12, cipherSuite) {
		return fail(alertIllegalParameter, "server selected a cipher suite that was not offered")
	}
	s, ok := lookupSuite(stdtls.VersionTLS12, cipherSuite)
	if !ok {
		return fail(alertIllegalParameter, "unsupported cipher suite")
	}
	if bytes.Equal(sessionID, h.sessionID) {
		// The ID is made up, only there for TLS 1.3 middleboxes, so a server
		// that resumes it is confused.
		return fail(alertIllegalParameter, "server resumed a session this client does not have")
	}
	for typ, data := range exts {
		d := newReader(data)
		switch typ {
		case extRenegotiationInfo:
			if info := d.vec8(); len(info) != 0 || !d.empty() {
				return fail(alertHandshakeFailure, "unexpected renegotiation extension")
			}
		case extExtendedMasterSec:
			if len(data) != 0 {
				return fail(alertDecodeError, "malformed extended_master_secret extension")
			}
			h.ems = true
		case extECPointFormats:
			if formats := d.vec8(); !d.empty() || !bytes.Contains(formats, []byte{0}) {
				return fail(alertIllegalParameter, "server does not support uncompressed points")
			}
		case extALPN:
			list := newReader(d.vec16())
			name := list.vec8()
			if !d.empty() || !list.empty() || len(name) == 0 {
				return fail(alertDecodeError, "malformed ALPN extension")
			}
			if !slices.Contains(h.config.NextProtos, string(name)) {
				return fail(alertNoApplicationProtocol, "server selected an unadvertised ALPN protocol")
			}
			h.protocol = string(name)
		case extServerName:
			// The server acknowledges the name it was sent.
			if len(data) != 0 {
				return fail(alertDecodeError, "malformed server_name extension")
			}
		case extSessionTicket, extStatusRequest:
			// A ticket follows the Finished, if it does; nothing here uses it.
			if len(data) != 0 {
				return fail(alertDecodeError, "malformed extension %d", typ)
			}
		default:
			return fail(alertUnsupportedExtension, "server sent an unsolicited extension %d", typ)
		}
		if !d.ok {
			return fail(alertDecodeError, "malformed ServerHello extension")
		}
	}
	h.version = stdtls.VersionTLS12
	copy(h.serverRandom[:], random)
	h.suiteID, h.suite = cipherSuite, s
	h.transcript = append(h.transcript, msg...)
	h.state = hs12ExpectCertificate
	return nil
}

func (h *clientHS) certificate12(msg []byte) *hsError {
	r := newReader(msg[4:])
	list := newReader(r.vec24())
	if !r.empty() || !list.ok {
		return fail(alertDecodeError, "malformed Certificate")
	}
	var ders [][]byte
	for len(list.b) > 0 {
		der := list.vec24()
		if !list.ok || len(der) == 0 {
			return fail(alertDecodeError, "malformed Certificate")
		}
		ders = append(ders, der)
	}
	if herr := h.acceptCertificates(ders); herr != nil {
		return herr
	}
	h.transcript = append(h.transcript, msg...)
	h.state = hs12ExpectServerKeyExchange
	return nil
}

// serverKeyExchange checks the ECDHE parameters the server signed.
func (h *clientHS) serverKeyExchange(msg []byte) *hsError {
	body := msg[4:]
	r := newReader(body)
	curveType := r.u8()
	curveID := stdtls.CurveID(r.u16())
	pub := r.vec8()
	params := body[:len(body)-len(r.b)]
	alg := r.u16()
	signature := r.vec16()
	if !r.empty() {
		return fail(alertDecodeError, "malformed ServerKeyExchange")
	}
	if curveType != 3 || !slices.Contains(h.groups, curveID) || curveOf(curveID) == nil {
		return fail(alertIllegalParameter, "server chose a key exchange group that was not offered")
	}
	if _, err := curveOf(curveID).NewPublicKey(pub); err != nil {
		return fail(alertIllegalParameter, "invalid server key share: %v", err)
	}
	if !slices.Contains(clientSignatureAlgs12, alg) {
		return fail(alertIllegalParameter, "server signed with a scheme that was not offered")
	}
	signed := make([]byte, 0, 64+len(params))
	signed = append(append(append(signed, h.random[:]...), h.serverRandom[:]...), params...)
	if err := verifySignature12(alg, h.certs[0].PublicKey, signed, signature); err != nil {
		return fail(alertDecryptError, "invalid signature by the server certificate: %v", err)
	}
	h.skxCurve, h.skxPub = curveID, bytes.Clone(pub)
	h.transcript = append(h.transcript, msg...)
	h.state = hs12ExpectCertificateRequestOrDone
	return nil
}

// verifySignature12 checks a TLS 1.2 signature over signed.
func verifySignature12(alg uint16, pub crypto.PublicKey, signed, signature []byte) error {
	ch, ok := signatureHash(alg)
	if !ok {
		return errUnsupportedSignature
	}
	if alg == 0x0807 {
		key, isEd := pub.(ed25519.PublicKey)
		if !isEd || !ed25519.Verify(key, signed, signature) {
			return errBadSignature
		}
		return nil
	}
	digest := hashBytes(ch.New, signed)
	switch alg {
	case 0x0401, 0x0501, 0x0601:
		key, isRSA := pub.(*rsa.PublicKey)
		if !isRSA {
			return errUnsupportedSignature
		}
		return rsa.VerifyPKCS1v15(key, ch, digest, signature)
	case 0x0804, 0x0805, 0x0806:
		key, isRSA := pub.(*rsa.PublicKey)
		if !isRSA {
			return errUnsupportedSignature
		}
		return rsa.VerifyPSS(key, ch, digest, signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case 0x0403, 0x0503, 0x0603:
		key, isECDSA := pub.(*ecdsa.PublicKey)
		if !isECDSA {
			return errUnsupportedSignature
		}
		if !ecdsa.VerifyASN1(key, digest, signature) {
			return errBadSignature
		}
		return nil
	}
	return errUnsupportedSignature
}

type signatureError string

func (e signatureError) Error() string { return string(e) }

const (
	errBadSignature         = signatureError("signature verification failure")
	errUnsupportedSignature = signatureError("signature scheme does not match the certificate's key")
)

func (h *clientHS) certificateRequest12(msg []byte) *hsError {
	r := newReader(msg[4:])
	types := r.vec8()
	algs := r.vec16()
	authorities := r.vec16()
	if !r.empty() || types == nil || algs == nil || authorities == nil {
		return fail(alertDecodeError, "malformed CertificateRequest")
	}
	h.certReq = true
	h.transcript = append(h.transcript, msg...)
	h.state = hs12ExpectServerHelloDone
	return nil
}

// serverHelloDone ends the server's flight, and is answered with ours: the
// certificate it asked for, if any, our key share, and the change_cipher_spec
// and Finished that switch to the keys they make.
func (h *clientHS) serverHelloDone(msg []byte, out *[]byte) *hsError {
	if len(msg) != 4 {
		return fail(alertDecodeError, "malformed ServerHelloDone")
	}
	h.transcript = append(h.transcript, msg...)
	curve := curveOf(h.skxCurve)
	key, err := curve.GenerateKey(h.rander())
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	peer, err := curve.NewPublicKey(h.skxPub)
	if err != nil {
		return fail(alertIllegalParameter, "invalid server key share: %v", err)
	}
	premaster, err := key.ECDH(peer)
	if err != nil {
		return fail(alertIllegalParameter, "key exchange failed: %v", err)
	}

	var flight []byte
	if h.certReq {
		// No certificate to offer; see certificate in client13.go.
		cert := []byte{hsCertificate, 0, 0, 3, 0, 0, 0}
		h.transcript = append(h.transcript, cert...)
		flight = appendRecord(flight, recordTypeHandshake, 0x0303, cert)
	}
	public := key.PublicKey().Bytes()
	exchange := append([]byte{hsClientKeyExchange, 0, 0, byte(1 + len(public)), byte(len(public))}, public...)
	h.transcript = append(h.transcript, exchange...)
	flight = appendRecord(flight, recordTypeHandshake, 0x0303, exchange)

	f := h.suite.hash
	if h.ems {
		h.master = prf12(f, premaster, "extended master secret", h.transcriptHash(), 48)
	} else {
		seed := append(append([]byte(nil), h.random[:]...), h.serverRandom[:]...)
		h.master = prf12(f, premaster, "master secret", seed, 48)
	}
	h.keyLog("CLIENT_RANDOM", h.master)
	if h.tx, h.rx, err = newKeysFromMaster(stdtls.VersionTLS12, h.suite, h.master, h.random[:], h.serverRandom[:]); err != nil {
		return fail(alertInternalError, "%v", err)
	}

	flight = append(flight, recordTypeChangeCipherSpec, 3, 3, 0, 1, 1)
	h.txOn = true
	verify := prf12(f, h.master, "client finished", h.transcriptHash(), 12)
	finished := append([]byte{hsFinished, 0, 0, 12}, verify...)
	h.transcript = append(h.transcript, finished...)
	if flight, err = h.tx.seal(flight, recordTypeHandshake, finished, nil); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.clientFinished = verify
	*out = append(*out, flight...)
	h.state = hs12ExpectCCS
	h.flightEnd = true
	return nil
}

// serverChangeCipherSpec switches reading to the server's keys: what follows
// it is protected.
func (h *clientHS) serverChangeCipherSpec(body []byte) *hsError {
	if h.state != hs12ExpectCCS || len(body) != 1 || body[0] != 1 || len(h.hsBuf) != 0 {
		return fail(alertUnexpectedMessage, "unexpected change_cipher_spec")
	}
	h.rxOn = true
	h.state = hs12ExpectFinished
	return nil
}

func (h *clientHS) finished12(msg []byte) *hsError {
	want := prf12(h.suite.hash, h.master, "server finished", h.transcriptHash(), 12)
	if !hmac.Equal(want, msg[4:]) {
		return fail(alertDecryptError, "invalid server finished hash")
	}
	if verify := h.config.VerifyConnection; verify != nil {
		if err := verify(h.connectionState()); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.state = hsDone
	h.flightEnd = true
	return nil
}
