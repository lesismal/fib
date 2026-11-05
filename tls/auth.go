package tls

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"reflect"
	"slices"
	"sync/atomic"
	"unsafe"
)

// Certificates chosen by callbacks, and certificates on both sides: what the
// handshakes that run without a worker share for a server's GetCertificate
// and client authentication (ClientAuth, ClientCAs), and a client's
// Certificates and GetClientCertificate.
//
// The callbacks are the user's, and may block: autocert's GetCertificate
// asks an ACME server for a certificate it does not have yet, which takes
// seconds, and others read a disk, a database or a secret store. The state
// machines run on whatever goroutine drives them, an engine's loop or worker
// for TCP, or a QUIC connection's datagram handler holding its lock, where a
// call that blocks stalls every connection that goroutine serves. So a
// callback is not called there, and neither is a client's verification of
// the server's certificate, nor its VerifyPeerCertificate and VerifyConnection
// (see clientHS.checkThen), nor a server's verification of the client's and
// its VerifyPeerCertificate (see serverHS.acceptClientThen): it is handed to a worker of the handshake
// pool, the handshake sets the rest aside, and once the callback returns the
// worker drives the handshake on (see certCall). The cost is a hop to the pool
// and back for the full handshakes that need a callback, which is small next
// to their key exchange and signatures, and nothing for the rest; a worker is
// held only for as long as the callback runs, and not, as it is when
// crypto/tls runs the handshake, for every round trip with the peer. A QUIC
// layer that cannot be woken (see QUICConn.SetWake) has the callback called
// where the handshake is, as crypto/tls's QUICConn would.

const (
	alertUnsupportedCertificate = 43
	alertCertificateExpired     = 45
	alertUnknownCA              = 48
	alertUnrecognizedName       = 112
	alertCertificateRequired    = 116

	extCertAuthorities = 47
)

// certAnswer is what a certificate callback returned.
type certAnswer struct {
	cert *stdtls.Certificate
	err  error
}

// errCertWait is what a step of a handshake reports while the answer of a
// certificate callback is still to come; it never leaves the state machine.
var errCertWait = &hsError{err: errors.New("tls: waiting for a certificate callback")}

// certCall is a certificate callback of a handshake, called once.
type certCall struct {
	// async runs call off the goroutine that drives the handshake, and has
	// the handshake driven again once call has returned; it reports false if
	// it cannot. nil means call runs where the handshake does.
	async  func(call func()) bool
	asked  bool
	answer atomic.Pointer[certAnswer]
	// resume is the rest of the handshake, set aside until the answer comes.
	resume func(out *[]byte) *hsError
}

// ask calls fn, the first time, and returns its answer, or false while it is
// still to come.
func (c *certCall) ask(fn func() (*stdtls.Certificate, error)) (*certAnswer, bool) {
	if a := c.answer.Load(); a != nil {
		return a, true
	}
	if c.asked {
		return nil, false
	}
	c.asked = true
	call := func() {
		cert, err := fn()
		c.answer.Store(&certAnswer{cert: cert, err: err})
	}
	if c.async == nil {
		call()
	} else if !c.async(call) {
		c.answer.Store(&certAnswer{err: errNoHandshakeWorker})
	} else {
		return nil, false
	}
	return c.answer.Load(), true
}

// wait sets rest aside until the answer has come.
func (c *certCall) wait(rest func(out *[]byte) *hsError) { c.resume = rest }

// waiting reports whether the handshake has something set aside.
func (c *certCall) waiting() bool { return c.resume != nil }

// reset makes the call ready to be asked again, once its answer is taken.
func (c *certCall) reset() {
	c.asked = false
	c.answer.Store(nil)
	c.resume = nil
}

// proceed runs what was set aside if the answer has come, and reports
// whether the handshake still waits.
func (c *certCall) proceed(out *[]byte) (bool, *hsError) {
	if c.resume == nil {
		return false, nil
	}
	if c.answer.Load() == nil {
		return true, nil
	}
	rest := c.resume
	c.resume = nil
	return false, rest(out)
}

// ctxOffsets locate the context crypto/tls keeps unexported in what it hands
// the callbacks, which Context returns and callbacks may use; it is filled in
// here if crypto/tls still has the field, and is nil otherwise.
var (
	helloCtxOffset   = ctxOffset(reflect.TypeFor[stdtls.ClientHelloInfo]())
	requestCtxOffset = ctxOffset(reflect.TypeFor[stdtls.CertificateRequestInfo]())
)

func ctxOffset(t reflect.Type) uintptr {
	f, ok := t.FieldByName("ctx")
	if !ok || f.Type != reflect.TypeFor[context.Context]() {
		return ^uintptr(0)
	}
	return f.Offset
}

func setContext[T any](info *T, offset uintptr, ctx context.Context) {
	if offset != ^uintptr(0) {
		*(*context.Context)(unsafe.Add(unsafe.Pointer(info), offset)) = ctx
	}
}

// usableCertificate checks a certificate a callback returned: the state
// machines sign with its key, which must be able to do so by itself.
func usableCertificate(cert *stdtls.Certificate) *hsError {
	if len(cert.Certificate) == 0 {
		return fail(alertInternalError, "certificate callback returned a certificate without a chain")
	}
	if _, ok := cert.PrivateKey.(crypto.Signer); !ok {
		return fail(alertInternalError, "certificate private key of type %T does not implement crypto.Signer", cert.PrivateKey)
	}
	return nil
}

// signatureSchemes are the schemes a key signs handshakes with, in the order
// this side prefers them. TLS 1.2 also signs with PKCS #1 for RSA.
func signatureSchemes(pub crypto.PublicKey, tls12 bool) []uint16 {
	var schemes []uint16
	switch key := pub.(type) {
	case *rsa.PublicKey:
		for _, alg := range []uint16{0x0804, 0x0805, 0x0806} {
			// PSS needs room for the digest and the salt twice, plus padding.
			if hash, _ := signatureHash(alg); key.Size() >= 2*hash.Size()+2 {
				schemes = append(schemes, alg)
			}
		}
		if tls12 {
			schemes = append(schemes, 0x0401, 0x0501, 0x0601)
		}
	case *ecdsa.PublicKey:
		switch key.Curve {
		case elliptic.P256():
			schemes = []uint16{0x0403}
		case elliptic.P384():
			schemes = []uint16{0x0503}
		case elliptic.P521():
			schemes = []uint16{0x0603}
		}
	case ed25519.PublicKey:
		schemes = []uint16{0x0807}
	}
	return schemes
}

// chooseSignature picks the scheme pub signs with among those the peer
// accepts: in this side's order with ours, else in the peer's.
func chooseSignature(pub crypto.PublicKey, peer []uint16, tls12, ours bool) (uint16, bool) {
	schemes := signatureSchemes(pub, tls12)
	if ours {
		for _, alg := range schemes {
			if slices.Contains(peer, alg) {
				return alg, true
			}
		}
		return 0, false
	}
	for _, alg := range peer {
		if slices.Contains(schemes, alg) {
			return alg, true
		}
	}
	return 0, false
}

// signHandshake signs what a CertificateVerify or ServerKeyExchange covers
// with alg: Ed25519 signs it whole, the others its digest.
func signHandshake(signer crypto.Signer, alg uint16, rand io.Reader, signed []byte) ([]byte, error) {
	h, _ := signatureHash(alg)
	switch alg {
	case 0x0807:
		return signer.Sign(rand, signed, crypto.Hash(0))
	case 0x0804, 0x0805, 0x0806:
		return signer.Sign(rand, hashBytes(h.New, signed), &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: h})
	}
	return signer.Sign(rand, hashBytes(h.New, signed), h)
}

// verify13 checks the signature of a TLS 1.3 CertificateVerify by pub, the
// peer's key, over signed (RFC 8446 section 4.4.3). peer names it in errors.
func verify13(alg uint16, pub crypto.PublicKey, signed, signature []byte, peer string) *hsError {
	ch, _ := signatureHash(alg)
	var err error
	switch alg {
	case 0x0804, 0x0805, 0x0806:
		key, isRSA := pub.(*rsa.PublicKey)
		if !isRSA {
			return fail(alertIllegalParameter, "RSA-PSS signature with a key of another type")
		}
		err = rsa.VerifyPSS(key, ch, hashBytes(ch.New, signed), signature,
			&rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash})
	case 0x0403, 0x0503, 0x0603:
		key, isECDSA := pub.(*ecdsa.PublicKey)
		want := map[uint16]elliptic.Curve{0x0403: elliptic.P256(), 0x0503: elliptic.P384(), 0x0603: elliptic.P521()}[alg]
		if !isECDSA || key.Curve != want {
			return fail(alertIllegalParameter, "ECDSA signature with a key of another type or curve")
		}
		if !ecdsa.VerifyASN1(key, hashBytes(ch.New, signed), signature) {
			err = errors.New("ECDSA verification failure")
		}
	case 0x0807:
		key, isEd := pub.(ed25519.PublicKey)
		if !isEd {
			return fail(alertIllegalParameter, "Ed25519 signature with a key of another type")
		}
		if !ed25519.Verify(key, signed, signature) {
			err = errors.New("Ed25519 verification failure")
		}
	default:
		return fail(alertIllegalParameter, "%s signed with an unsupported scheme", peer)
	}
	if err != nil {
		return fail(alertDecryptError, "invalid signature by the %s certificate: %v", peer, err)
	}
	return nil
}

// certificateVerifyContent is what a TLS 1.3 CertificateVerify signs: the
// transcript hash, after a prefix and the context string of the side that
// signs.
func certificateVerifyContent(server bool, transcriptHash []byte) []byte {
	signed := bytes.Repeat([]byte{0x20}, 64)
	if server {
		signed = append(signed, "TLS 1.3, server CertificateVerify"...)
	} else {
		signed = append(signed, "TLS 1.3, client CertificateVerify"...)
	}
	signed = append(signed, 0)
	return append(signed, transcriptHash...)
}

// appendCertificateVerify appends a CertificateVerify message.
func appendCertificateVerify(alg uint16, signature []byte) []byte {
	var b builder
	b.u8(hsCertificateVerify)
	b.u24(0)
	b.u16(alg)
	b.vec16(func(b *builder) { b.raw(signature) })
	return finishMessage(&b)
}

// requiresClientCert reports whether a client without a certificate is
// turned away.
func requiresClientCert(auth stdtls.ClientAuthType) bool {
	return auth == stdtls.RequireAnyClientCert || auth == stdtls.RequireAndVerifyClientCert
}

// maxRSAKeySize is the largest RSA key a peer's certificate may carry, as
// crypto/tls bounds it: verifying with a larger one costs more than it should.
const maxRSAKeySize = 8192

// requestCertificate builds the server's CertificateRequest: the signature
// schemes this side verifies, and the authorities of Config.ClientCAs.
func (h *serverHS) requestCertificate(tls12 bool) []byte {
	cas := h.clientCAs()
	var b builder
	b.u8(hsCertificateRequest)
	b.u24(0)
	if tls12 {
		b.vec8(func(b *builder) { b.u8(1); b.u8(64) }) // rsa_sign, ecdsa_sign
		b.vec16(func(b *builder) {
			for _, alg := range clientSignatureAlgs12 {
				b.u16(alg)
			}
		})
		b.vec16(func(b *builder) {
			for _, dn := range cas {
				b.vec16(func(b *builder) { b.raw(dn) })
			}
		})
		return finishMessage(&b)
	}
	b.u8(0) // certificate_request_context
	b.vec16(func(b *builder) {
		b.u16(extSignatureAlgs)
		b.vec16(func(b *builder) {
			b.vec16(func(b *builder) {
				for _, alg := range clientSignatureAlgs {
					b.u16(alg)
				}
			})
		})
		if len(cas) > 0 {
			b.u16(extCertAuthorities)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					for _, dn := range cas {
						b.vec16(func(b *builder) { b.raw(dn) })
					}
				})
			})
		}
	})
	return finishMessage(&b)
}

// clientCAs are the names of the authorities in Config.ClientCAs, as far as
// they fit a CertificateRequest.
func (h *serverHS) clientCAs() [][]byte {
	if h.config.ClientCAs == nil {
		return nil
	}
	//lint:ignore SA1019 crypto/tls lists them the same way, and the pool is not the system's
	cas := h.config.ClientCAs.Subjects()
	size := 0
	for i, dn := range cas {
		if size += 2 + len(dn); size > 0xfff0 {
			return cas[:i]
		}
	}
	return cas
}

// acceptClientCertificates takes the chain of the client's Certificate as
// crypto/tls does: parsed, required if Config.ClientAuth requires one,
// verified against Config.ClientCAs if it says so, and handed to
// VerifyPeerCertificate.
func (h *serverHS) acceptClientCertificates(ders [][]byte) *hsError {
	certs := make([]*x509.Certificate, len(ders))
	for i, der := range ders {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return fail(alertDecodeError, "failed to parse client certificate: %v", err)
		}
		if key, ok := cert.PublicKey.(*rsa.PublicKey); ok && key.N.BitLen() > maxRSAKeySize {
			return fail(alertBadCertificate, "client sent certificate containing RSA key larger than %d bits", maxRSAKeySize)
		}
		certs[i] = cert
	}
	auth := h.config.ClientAuth
	if len(certs) == 0 && requiresClientCert(auth) {
		alert := uint8(alertCertificateRequired)
		if h.version == stdtls.VersionTLS12 {
			alert = alertHandshakeFailure
		}
		return &hsError{alert: alert, err: errors.New("tls: client didn't provide a certificate")}
	}
	var chains [][]*x509.Certificate
	if auth >= stdtls.VerifyClientCertIfGiven && len(certs) > 0 {
		opts := x509.VerifyOptions{
			Roots:         h.config.ClientCAs,
			CurrentTime:   h.now(),
			Intermediates: x509.NewCertPool(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		var err error
		if chains, err = certs[0].Verify(opts); err != nil {
			alert := uint8(alertBadCertificate)
			var unknown x509.UnknownAuthorityError
			var invalid x509.CertificateInvalidError
			if errors.As(err, &unknown) {
				alert = alertUnknownCA
			} else if errors.As(err, &invalid) && invalid.Reason == x509.Expired {
				alert = alertCertificateExpired
			}
			return &hsError{alert: alert, err: &stdtls.CertificateVerificationError{UnverifiedCertificates: certs, Err: err}}
		}
	}
	if len(certs) > 0 {
		switch certs[0].PublicKey.(type) {
		case *ecdsa.PublicKey, *rsa.PublicKey, ed25519.PublicKey:
		default:
			return fail(alertUnsupportedCertificate, "client certificate contains an unsupported public key of type %T", certs[0].PublicKey)
		}
	}
	if verify := h.config.VerifyPeerCertificate; verify != nil {
		if err := verify(ders, chains); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.peerCerts, h.peerChains = certs, chains
	h.peerRaw = make([][]byte, len(ders))
	for i, der := range ders {
		h.peerRaw[i] = bytes.Clone(der)
	}
	return nil
}

// acceptClientThen runs acceptClientCertificates on ders, and then rest,
// unless it failed. Verifying a chain takes a while, longer where the
// platform's verifier does it, and VerifyPeerCertificate is the user's and may
// block, so where there is one of them to run and a layer to wake the
// handshake again, it runs as a client's check of the server's does (see
// clientHS.checkThen): on a worker of the handshake pool, with the messages
// that came behind the Certificate set aside until it has returned.
func (h *serverHS) acceptClientThen(ders [][]byte, rest func()) *hsError {
	verifies := h.config.ClientAuth >= stdtls.VerifyClientCertIfGiven && len(ders) > 0
	if h.check.async == nil || !verifies && h.config.VerifyPeerCertificate == nil {
		if herr := h.acceptClientCertificates(ders); herr != nil {
			return herr
		}
		rest()
		return nil
	}
	if _, ok := h.check.ask(func() (*stdtls.Certificate, error) {
		if herr := h.acceptClientCertificates(ders); herr != nil {
			return nil, herr
		}
		return nil, nil
	}); !ok {
		h.check.wait(func(out *[]byte) *hsError {
			if herr := h.checked(); herr != nil {
				return herr
			}
			rest()
			if h.quic != nil {
				// QUIC's layer drains the messages that wait; see
				// QUICConn.resumePicked.
				return nil
			}
			return h.handshakeData(nil, out)
		})
		return nil
	}
	if herr := h.checked(); herr != nil {
		return herr
	}
	rest()
	return nil
}

// checked is what the check of the client's certificate answered, with the
// call made ready for the next.
func (h *serverHS) checked() *hsError {
	a := h.check.answer.Load()
	h.check.reset()
	if a.err == nil {
		return nil
	}
	if herr, ok := a.err.(*hsError); ok {
		return herr
	}
	return &hsError{alert: alertInternalError, err: a.err}
}

// sessionPeerChecked, set by tests, is told of each ticket whose client
// certificates sessionPeer reads back.
var sessionPeerChecked func()

// sessionPeer reads back the client certificates a ticket carries, and
// reports whether the session can be resumed with them under the Config: not
// without one if one is required, not with one if none is asked for, not once
// it has expired, and not unverified if the Config verifies them.
//
// If the Config verifies them they are verified again, as in a full
// handshake, rather than taken on the chains the ticket keeps. A ticket can
// outlive the trust it was issued under: the ClientCAs may have changed since,
// an intermediate expired, or the ticket been sealed, under a SessionTicketKey
// shared between servers or kept across restarts, by a build whose
// crypto/x509 still trusted what a later one distrusts (CVE-2026-111943).
// crypto/tls checks only that a kept chain's certificates are current and its
// root still among the ClientCAs; the cost of more is one verification, what a
// full handshake with a client certificate pays.
func (h *serverHS) sessionPeer(raw [][]byte, rawChains [][][]byte) (certs []*x509.Certificate, chains [][]*x509.Certificate, ok bool) {
	if sessionPeerChecked != nil {
		sessionPeerChecked()
	}
	auth := h.config.ClientAuth
	if len(raw) == 0 {
		return nil, nil, !requiresClientCert(auth)
	}
	if auth == stdtls.NoClientCert {
		return nil, nil, false
	}
	for _, der := range raw {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, nil, false
		}
		certs = append(certs, c)
	}
	if h.now().After(certs[0].NotAfter) {
		return nil, nil, false
	}
	if auth < stdtls.VerifyClientCertIfGiven {
		// Chains the ticket keeps were verified under a Config that has
		// changed since; a full handshake under this one would have none.
		return certs, nil, true
	}
	if len(rawChains) == 0 {
		return nil, nil, false
	}
	opts := x509.VerifyOptions{
		Roots:         h.config.ClientCAs,
		CurrentTime:   h.now(),
		Intermediates: x509.NewCertPool(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	for _, c := range certs[1:] {
		opts.Intermediates.AddCert(c)
	}
	chains, err := certs[0].Verify(opts)
	if err != nil {
		return nil, nil, false
	}
	return certs, chains, true
}

// rawChains are the verified chains in the form a ticket keeps them.
func rawChains(chains [][]*x509.Certificate) [][][]byte {
	if len(chains) == 0 {
		return nil
	}
	out := make([][][]byte, len(chains))
	for i, chain := range chains {
		for _, c := range chain {
			out[i] = append(out[i], c.Raw)
		}
	}
	return out
}

// clientCertificate13 handles the client's Certificate in TLS 1.3.
func (h *serverHS) clientCertificate13(msg []byte) *hsError {
	r := newReader(msg[4:])
	ctx := r.vec8()
	list := newReader(r.vec24())
	if !r.empty() || !list.ok || len(ctx) != 0 {
		return fail(alertDecodeError, "malformed Certificate")
	}
	var ders [][]byte
	for len(list.b) > 0 {
		der := list.vec24()
		extensions := list.vec16()
		if !list.ok || len(der) == 0 || extensions == nil {
			return fail(alertDecodeError, "malformed Certificate")
		}
		ders = append(ders, der)
	}
	return h.acceptClientThen(ders, func() {
		h.transcript = append(h.transcript, msg...)
		h.state = srvAwaitFinished
		if len(h.peerCerts) > 0 {
			h.state = srvAwaitClientCertVerify
		}
	})
}

// clientCertificateVerify13 checks the client's CertificateVerify in TLS 1.3,
// which signs the transcript through its Certificate.
func (h *serverHS) clientCertificateVerify13(msg []byte) *hsError {
	r := newReader(msg[4:])
	alg := r.u16()
	signature := r.vec16()
	if !r.empty() {
		return fail(alertDecodeError, "malformed CertificateVerify")
	}
	if !slices.Contains(clientSignatureAlgs, alg) {
		return fail(alertIllegalParameter, "client certificate used with invalid signature algorithm")
	}
	signed := certificateVerifyContent(false, h.transcriptHash())
	if herr := verify13(alg, h.peerCerts[0].PublicKey, signed, signature, "client"); herr != nil {
		return herr
	}
	h.transcript = append(h.transcript, msg...)
	h.state = srvAwaitFinished
	return nil
}

// clientCertificate12 handles the client's Certificate in TLS 1.2, which it
// sends, empty or not, when it is asked for one.
func (h *serverHS) clientCertificate12(msg []byte) *hsError {
	ders, herr := parseCertificates12(msg)
	if herr != nil {
		return herr
	}
	return h.acceptClientThen(ders, func() {
		h.transcript = append(h.transcript, msg...)
		h.state = srv12AwaitKeyExchange
	})
}

// clientCertificateVerify12 checks the client's CertificateVerify in TLS 1.2,
// which signs every handshake message before it.
func (h *serverHS) clientCertificateVerify12(msg []byte) *hsError {
	if herr := h.checkCertificateVerify12(msg, h.transcript); herr != nil {
		return herr
	}
	h.transcript = append(h.transcript, msg...)
	h.state = srv12AwaitCCS
	return nil
}

// checkCertificateVerify12 checks the client's CertificateVerify in TLS 1.2,
// a signature over signed, the handshake messages before it.
func (h *serverHS) checkCertificateVerify12(msg, signed []byte) *hsError {
	r := newReader(msg[4:])
	alg := r.u16()
	signature := r.vec16()
	if !r.empty() {
		return fail(alertDecodeError, "malformed CertificateVerify")
	}
	if !slices.Contains(clientSignatureAlgs12, alg) {
		return fail(alertIllegalParameter, "client certificate used with invalid signature algorithm")
	}
	if err := verifySignature12(alg, h.peerCerts[0].PublicKey, signed, signature); err != nil {
		return fail(alertDecryptError, "invalid signature by the client certificate: %v", err)
	}
	return nil
}

// parseCertificateRequest13 reads the server's CertificateRequest in TLS 1.3:
// the schemes it verifies and the authorities it trusts.
func (h *clientHS) parseCertificateRequest13(msg []byte) *hsError {
	r := newReader(msg[4:])
	ctx := r.vec8()
	extensions := r.vec16()
	if !r.empty() || extensions == nil {
		return fail(alertDecodeError, "malformed CertificateRequest")
	}
	exts, herr := parseExtensions(extensions)
	if herr != nil {
		return herr
	}
	if data, ok := exts[extSignatureAlgs]; ok {
		d := newReader(data)
		list := newReader(d.vec16())
		for len(list.b) > 0 && list.ok {
			h.reqAlgs = append(h.reqAlgs, list.u16())
		}
		if !d.empty() || !list.ok {
			return fail(alertDecodeError, "malformed CertificateRequest")
		}
	}
	if len(h.reqAlgs) == 0 {
		return fail(alertMissingExtension, "server sent a CertificateRequest without signature_algorithms")
	}
	if data, ok := exts[extCertAuthorities]; ok {
		cas, ok := parseAuthorities(data)
		if !ok {
			return fail(alertDecodeError, "malformed CertificateRequest")
		}
		h.reqCAs = cas
	}
	h.certReq, h.reqCtx = true, bytes.Clone(ctx)
	return nil
}

// parseAuthorities reads the extension that lists distinguished names.
func parseAuthorities(data []byte) ([][]byte, bool) {
	d := newReader(data)
	cas, ok := parseNames(d.vec16())
	return cas, ok && d.empty()
}

// parseNames reads a list of distinguished names.
func parseNames(data []byte) ([][]byte, bool) {
	list := newReader(data)
	var cas [][]byte
	for len(list.b) > 0 && list.ok {
		if dn := list.vec16(); list.ok {
			cas = append(cas, bytes.Clone(dn))
		}
	}
	return cas, list.ok
}

// parseCertificateRequest12 reads the server's CertificateRequest in TLS 1.2,
// whose certificate types narrow the schemes it lists.
func (h *clientHS) parseCertificateRequest12(msg []byte) *hsError {
	r := newReader(msg[4:])
	types := r.vec8()
	algs := newReader(r.vec16())
	authorities := r.vec16()
	if !r.empty() || types == nil || !algs.ok || authorities == nil || len(algs.b)%2 != 0 {
		return fail(alertDecodeError, "malformed CertificateRequest")
	}
	rsaOK, ecOK := bytes.IndexByte(types, 1) >= 0, bytes.IndexByte(types, 64) >= 0
	for len(algs.b) > 0 {
		alg := algs.u16()
		switch alg {
		case 0x0403, 0x0503, 0x0603, 0x0807:
			if ecOK {
				h.reqAlgs = append(h.reqAlgs, alg)
			}
		case 0x0401, 0x0501, 0x0601, 0x0804, 0x0805, 0x0806:
			if rsaOK {
				h.reqAlgs = append(h.reqAlgs, alg)
			}
		}
	}
	cas, ok := parseNames(authorities)
	if !ok {
		return fail(alertDecodeError, "malformed CertificateRequest")
	}
	h.reqCAs = cas
	h.certReq = true
	return nil
}

// clientCertificate is the certificate the client answers a CertificateRequest
// with, as crypto/tls picks it: GetClientCertificate's, or else the first of
// Config.Certificates the request allows, or none. errCertWait means that
// GetClientCertificate has yet to answer.
func (h *clientHS) clientCertificate() (*stdtls.Certificate, *hsError) {
	info := &stdtls.CertificateRequestInfo{AcceptableCAs: h.reqCAs, Version: h.version}
	for _, alg := range h.reqAlgs {
		info.SignatureSchemes = append(info.SignatureSchemes, stdtls.SignatureScheme(alg))
	}
	setContext(info, requestCtxOffset, context.Background())
	if get := h.config.GetClientCertificate; get != nil {
		a, ok := h.pick.ask(func() (*stdtls.Certificate, error) { return get(info) })
		if !ok {
			return nil, errCertWait
		}
		if a.err != nil {
			return nil, &hsError{alert: alertInternalError, err: a.err}
		}
		if a.cert == nil || len(a.cert.Certificate) == 0 {
			return &stdtls.Certificate{}, nil
		}
		if herr := usableCertificate(a.cert); herr != nil {
			return nil, herr
		}
		return a.cert, nil
	}
	for i := range h.config.Certificates {
		if info.SupportsCertificate(&h.config.Certificates[i]) == nil {
			return &h.config.Certificates[i], nil
		}
	}
	return &stdtls.Certificate{}, nil
}

// clientFlight runs rest, the client's last flight, once it is known which
// certificate it answers the server's CertificateRequest with, if there was
// one: at once, or when GetClientCertificate has answered.
func (h *clientHS) clientFlight(out *[]byte, rest func(out *[]byte) *hsError) *hsError {
	if h.certReq && h.own == nil {
		cert, herr := h.clientCertificate()
		if herr == errCertWait {
			h.pick.wait(func(out *[]byte) *hsError { return h.clientFlight(out, rest) })
			h.flightEnd = true
			return nil
		}
		if herr != nil {
			return herr
		}
		h.own = cert
	}
	return rest(out)
}

// certificateMessages13 are the client's Certificate and, if it has a
// certificate to send, its CertificateVerify, in TLS 1.3, added to the
// transcript.
func (h *clientHS) certificateMessages13() ([][]byte, *hsError) {
	var b builder
	b.u8(hsCertificate)
	b.u24(0)
	b.vec8(func(b *builder) { b.raw(h.reqCtx) })
	b.vec24(func(b *builder) {
		for _, der := range h.own.Certificate {
			b.vec24(func(b *builder) { b.raw(der) })
			b.u16(0) // no extensions
		}
	})
	cert := finishMessage(&b)
	h.transcript = append(h.transcript, cert...)
	msgs := [][]byte{cert}
	if len(h.own.Certificate) == 0 {
		return msgs, nil
	}
	signer := h.own.PrivateKey.(crypto.Signer)
	alg, ok := chooseSignature(signer.Public(), h.reqAlgs, false, false)
	if !ok {
		return nil, fail(alertHandshakeFailure, "client certificate does not support any of the server's signature algorithms")
	}
	signature, err := signHandshake(signer, alg, h.rander(), certificateVerifyContent(false, h.transcriptHash()))
	if err != nil {
		return nil, fail(alertInternalError, "failed to sign handshake: %v", err)
	}
	cv := appendCertificateVerify(alg, signature)
	h.transcript = append(h.transcript, cv...)
	return append(msgs, cv), nil
}

// certificateMessage12 is the client's Certificate in TLS 1.2.
func (h *clientHS) certificateMessage12() []byte {
	var b builder
	b.u8(hsCertificate)
	b.u24(0)
	b.vec24(func(b *builder) {
		for _, der := range h.own.Certificate {
			b.vec24(func(b *builder) { b.raw(der) })
		}
	})
	return finishMessage(&b)
}

// certificateVerify12 is the client's CertificateVerify in TLS 1.2, over the
// transcript so far, or nil without a certificate.
func (h *clientHS) certificateVerify12() ([]byte, *hsError) {
	if h.own == nil || len(h.own.Certificate) == 0 {
		return nil, nil
	}
	signer := h.own.PrivateKey.(crypto.Signer)
	alg, ok := chooseSignature(signer.Public(), h.reqAlgs, true, false)
	if !ok {
		return nil, fail(alertHandshakeFailure, "client certificate does not support any of the server's signature algorithms")
	}
	signature, err := signHandshake(signer, alg, h.rander(), h.transcript)
	if err != nil {
		return nil, fail(alertInternalError, "failed to sign handshake: %v", err)
	}
	return appendCertificateVerify(alg, signature), nil
}

// certError is the failure a certificate callback's error causes, with the
// alert crypto/tls sends for it.
func certError(err error) *hsError {
	return &hsError{alert: alertInternalError, err: err}
}
