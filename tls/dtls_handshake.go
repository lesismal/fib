package tls

import (
	"bytes"
	"crypto/hmac"
	stdtls "crypto/tls"
	"slices"
	"time"
)

// The flights of a DTLS 1.2 handshake (RFC 6347 section 4.2.4), made of the
// messages of the TLS 1.2 handshake code. A full handshake is six flights:
//
//	client                      server
//	ClientHello          -->                       1
//	                     <--    HelloVerifyRequest 2
//	ClientHello + cookie -->                       3
//	                     <--    ServerHello ...
//	                            ServerHelloDone    4
//	Certificate, ClientKeyExchange, CertificateVerify,
//	ChangeCipherSpec, Finished -->                 5
//	                     <--    ChangeCipherSpec,
//	                            Finished           6
//
// Each side sends a flight again when its timer runs out before the peer's
// next one, or when the peer's last one comes again; the server keeps its
// last for as long as the client may not have it.

// extUseSRTP is the use_srtp extension (RFC 5764 section 4.1.1).
const extUseSRTP = 14

// dtlsSuites are the suites DTLS offers and accepts, in a client's order:
// the ECDHE AEAD suites, whose records need no IV of their own and which
// browsers offer.
var dtlsSuites = []uint16{
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

// dtlsSuitesFor are the suites a client with config offers: ChaCha20-Poly1305
// first where the CPU has no AES-GCM, and only those Config.CipherSuites
// allows, if it lists any.
func dtlsSuitesFor(config *stdtls.Config) []uint16 {
	suites := dtlsSuites
	if !aesGCMHardware {
		suites = chachaFirst(suites, func(id uint16) uint16 { return id })
	}
	if len(config.CipherSuites) == 0 {
		return suites
	}
	return slices.DeleteFunc(slices.Clone(suites), func(id uint16) bool { return !slices.Contains(config.CipherSuites, id) })
}

// sendClientHello sends the client's ClientHello, with the cookie of the
// last HelloVerifyRequest if there was one, as a flight of its own. The
// transcript starts again with it: a ClientHello that was answered with a
// HelloVerifyRequest is not in it, nor is the request (RFC 6347 section
// 4.2.1).
func (c *DTLSConn) sendClientHello(now time.Time) {
	h := c.cli
	var b builder
	b.u8(hsClientHello)
	b.u24(0)
	b.u16(VersionDTLS12)
	b.raw(h.random[:])
	b.u8(0) // no session ID
	b.vec8(func(b *builder) { b.raw(c.cookie) })
	b.vec16(func(b *builder) {
		for _, id := range h.suites12 {
			b.u16(id)
		}
	})
	b.vec8(func(b *builder) { b.u8(0) })
	b.vec16(func(b *builder) {
		if name := hostnameInSNI(c.config.ServerName); name != "" {
			b.u16(extServerName)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					b.u8(0)
					b.vec16(func(b *builder) { b.raw([]byte(name)) })
				})
			})
		}
		b.u16(extExtendedMasterSec)
		b.vec16(func(b *builder) {})
		b.u16(extRenegotiationInfo)
		b.vec16(func(b *builder) { b.vec8(func(b *builder) {}) })
		b.u16(extSupportedGroups)
		b.vec16(func(b *builder) {
			b.vec16(func(b *builder) {
				for _, g := range h.groups {
					b.u16(uint16(g))
				}
			})
		})
		b.u16(extECPointFormats)
		b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.u8(0) }) })
		b.u16(extSignatureAlgs)
		b.vec16(func(b *builder) {
			b.vec16(func(b *builder) {
				for _, alg := range clientSignatureAlgs12 {
					b.u16(alg)
				}
			})
		})
		if len(c.opts.SRTPProfiles) > 0 {
			b.u16(extUseSRTP)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					for _, p := range c.opts.SRTPProfiles {
						b.u16(p)
					}
				})
				b.vec8(func(b *builder) {}) // no MKI
			})
		}
		if len(c.config.NextProtos) > 0 {
			b.u16(extALPN)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					for _, p := range c.config.NextProtos {
						b.vec8(func(b *builder) { b.raw([]byte(p)) })
					}
				})
			})
		}
	})
	h.transcript = h.transcript[:0]
	c.flight = nil
	c.queueMessage(finishMessage(&b), 0)
	c.startFlight(now, true)
}

// clientMessage handles a message of the server's, in order.
func (c *DTLSConn) clientMessage(m *dtlsMessage, now time.Time) *hsError {
	h := c.cli
	msg := m.tlsForm()
	switch {
	case c.state == dtlsAwaitServerHello && m.typ == hsHelloVerifyRequest:
		r := newReader(m.body)
		r.u16() // server_version, which says nothing (RFC 6347 section 4.2.1)
		cookie := r.vec8()
		if !r.empty() {
			return fail(alertDecodeError, "malformed HelloVerifyRequest")
		}
		if c.verifies++; c.verifies > 4 {
			return fail(alertUnexpectedMessage, "too many HelloVerifyRequests")
		}
		c.cookie = bytes.Clone(cookie)
		c.peerLast = int(m.seq)
		c.sendClientHello(now)
		return nil
	case c.state == dtlsAwaitServerHello && m.typ == hsServerHello:
		if herr := c.serverHello(msg); herr != nil {
			return herr
		}
		c.state = dtlsAwaitCertificate
	case c.state == dtlsAwaitCertificate && m.typ == hsCertificate:
		ders, herr := parseCertificates12(msg)
		if herr != nil {
			return herr
		}
		// With no worker to hand them to, the checks of the certificate run
		// here (see clientHS.checkThen).
		var unused []byte
		if herr := h.acceptCertificates(ders, &unused); herr != nil {
			return herr
		}
		c.state = dtlsAwaitServerKeyExchange
	case c.state == dtlsAwaitServerKeyExchange && m.typ == hsServerKeyExchange:
		if herr := h.checkServerKeyExchange(msg); herr != nil {
			return herr
		}
		c.state = dtlsAwaitCertificateRequestOrDone
	case c.state == dtlsAwaitCertificateRequestOrDone && m.typ == hsCertificateRequest:
		if herr := h.parseCertificateRequest12(msg); herr != nil {
			return herr
		}
		c.state = dtlsAwaitServerHelloDone
	case (c.state == dtlsAwaitCertificateRequestOrDone || c.state == dtlsAwaitServerHelloDone) && m.typ == hsServerHelloDone:
		if len(m.body) != 0 {
			return fail(alertDecodeError, "malformed ServerHelloDone")
		}
		h.transcript = append(h.transcript, m.dtlsForm()...)
		c.peerLast = int(m.seq)
		return c.clientFlight(now)
	case c.state == dtlsAwaitServerFinished && m.typ == hsFinished:
		want := prf12(h.suite.hash, h.master, "server finished", h.transcriptHash(), 12)
		if !hmac.Equal(want, m.body) {
			return fail(alertDecryptError, "invalid server finished hash")
		}
		h.transcript = append(h.transcript, m.dtlsForm()...)
		if h.config.VerifyConnection != nil {
			if herr := h.verifyConnection(); herr != nil {
				return herr
			}
		}
		c.flight = nil
		c.complete()
		return nil
	default:
		return fail(alertUnexpectedMessage, "unexpected handshake message %d", m.typ)
	}
	h.transcript = append(h.transcript, m.dtlsForm()...)
	return nil
}

// serverHello handles the ServerHello, as serverHello12 does over TCP, with
// the use_srtp extension besides.
func (c *DTLSConn) serverHello(msg []byte) *hsError {
	h := c.cli
	r := newReader(msg[4:])
	version := r.u16()
	random := r.take(32)
	r.vec8() // session ID: there is no resumption
	suiteID := r.u16()
	compression := r.u8()
	var block []byte
	if len(r.b) > 0 {
		block = r.vec16()
	}
	if !r.empty() {
		return fail(alertDecodeError, "malformed ServerHello")
	}
	if version != VersionDTLS12 {
		return fail(alertProtocolVersion, "server selected unsupported protocol version %#04x", version)
	}
	if compression != 0 {
		return fail(alertIllegalParameter, "server selected unsupported compression format")
	}
	s, ok := lookupSuite(stdtls.VersionTLS12, suiteID)
	if !slices.Contains(h.suites12, suiteID) || !ok {
		return fail(alertIllegalParameter, "server selected a cipher suite that was not offered")
	}
	exts, herr := parseExtensions(block)
	if herr != nil {
		return herr
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
			if !slices.Contains(c.config.NextProtos, string(name)) {
				return fail(alertNoApplicationProtocol, "server selected an unadvertised ALPN protocol")
			}
			h.protocol = string(name)
		case extUseSRTP:
			list := newReader(d.vec16())
			profile := list.u16()
			mki := d.vec8()
			if !d.empty() || !list.empty() {
				return fail(alertDecodeError, "malformed use_srtp extension")
			}
			if !slices.Contains(c.opts.SRTPProfiles, profile) || len(mki) != 0 {
				return fail(alertIllegalParameter, "server selected an SRTP profile that was not offered")
			}
			c.srtpProfile, c.srtpOK = profile, true
		case extServerName:
			if len(data) != 0 {
				return fail(alertDecodeError, "malformed server_name extension")
			}
		default:
			return fail(alertUnsupportedExtension, "server sent an unsolicited extension %d", typ)
		}
		if !d.ok {
			return fail(alertDecodeError, "malformed ServerHello extension")
		}
	}
	copy(h.serverRandom[:], random)
	h.suiteID, h.suite = suiteID, s
	return nil
}

// clientFlight sends the client's second flight: its certificate, if the
// server asked for one, the key exchange, the CertificateVerify, and the
// change_cipher_spec and Finished, under the keys the exchange makes.
func (c *DTLSConn) clientFlight(now time.Time) *hsError {
	h := c.cli
	c.flight = nil
	if h.certReq {
		cert, herr := h.clientCertificate()
		if herr != nil {
			return herr
		}
		h.own = cert
		c.queueMessage(h.certificateMessage12(), 0)
	}
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
	public := key.PublicKey().Bytes()
	c.queueMessage(append([]byte{hsClientKeyExchange, 0, 0, byte(1 + len(public)), byte(len(public))}, public...), 0)
	var sessionHash []byte
	if h.ems {
		sessionHash = h.transcriptHash()
	}
	h.master = masterSecret12(h.suite.hash, premaster, h.ems, sessionHash, h.random[:], h.serverRandom[:])
	if h.certReq {
		cv, herr := h.certificateVerify12()
		if herr != nil {
			return herr
		}
		if cv != nil {
			c.queueMessage(cv, 0)
		}
	}
	h.keyLog("CLIENT_RANDOM", h.master)
	tx, rx, err := newKeysFromMaster(stdtls.VersionTLS12, h.suite, h.master, h.random[:], h.serverRandom[:])
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	c.installKeys(rx, tx)
	c.flight = append(c.flight, flightEntry{ccs: true})
	verify := prf12(h.suite.hash, h.master, "client finished", h.transcriptHash(), 12)
	c.queueMessage(append([]byte{hsFinished, 0, 0, 12}, verify...), 1)
	h.clientFinished = verify
	c.txEpoch = 1
	c.state = dtlsAwaitServerFinished
	c.startFlight(now, true)
	return nil
}

// installKeys takes the keys of epoch 1, as DTLS 1.2 writes them.
func (c *DTLSConn) installKeys(rx, tx *recordKeys) {
	rx.version, tx.version = [2]byte{0xfe, 0xfd}, [2]byte{0xfe, 0xfd}
	c.rx, c.tx = rx, tx
}

// serverClientHello handles the ClientHello a server goes on with, in its
// TLS form, and sends the server's flight up to its ServerHelloDone, as the
// TCP handshake does (see serverHS.clientHello12), with use_srtp besides.
func (c *DTLSConn) serverClientHello(msg []byte, now time.Time) *hsError {
	h := c.srv
	ch, ok := parseClientHello(msg)
	if !ok {
		return fail(alertDecodeError, "malformed ClientHello")
	}
	// Versions count down in DTLS: 0xfeff is 1.0, 0xfefd 1.2.
	if ch.legacy > VersionDTLS12 || ch.legacy < 0xfe00 {
		return fail(alertProtocolVersion, "client offered only unsupported versions")
	}
	ch.versions = []uint16{stdtls.VersionTLS12}
	ch.suites = slices.DeleteFunc(ch.suites, func(id uint16) bool { return !slices.Contains(dtlsSuites, id) })
	h.ch = ch
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
	plan, herr := h.plan12(ch)
	if herr != nil {
		return herr
	}
	if plan == nil {
		return fail(alertHandshakeFailure, "no cipher suite, group and signature scheme in common with the client")
	}
	var more func(*builder)
	if data, ok := ch.exts[extUseSRTP]; ok && len(c.opts.SRTPProfiles) > 0 {
		d := newReader(data)
		list := newReader(d.vec16())
		d.vec8() // MKI, which is not used
		if !d.empty() || len(list.b) == 0 || len(list.b)%2 != 0 {
			return fail(alertDecodeError, "malformed use_srtp extension")
		}
		var offered []uint16
		for len(list.b) > 0 {
			offered = append(offered, list.u16())
		}
		for _, p := range c.opts.SRTPProfiles {
			if slices.Contains(offered, p) {
				c.srtpProfile, c.srtpOK = p, true
				break
			}
		}
		if c.srtpOK {
			more = func(b *builder) {
				b.u16(extUseSRTP)
				b.vec16(func(b *builder) {
					b.vec16(func(b *builder) { b.u16(c.srtpProfile) })
					b.vec8(func(b *builder) {})
				})
			}
		}
	}
	c.flight = nil
	herr = h.serverFlight12(plan, more, func(m []byte) {
		if m[0] == hsServerHello {
			m[4], m[5] = byte(VersionDTLS12>>8), byte(VersionDTLS12&0xff)
		}
		c.queueMessage(m, 0)
	})
	if herr != nil {
		return herr
	}
	c.state = dtlsAwaitClientKeyExchange
	if h.requestsCert() {
		c.state = dtlsAwaitClientCertificate
	}
	c.startFlight(now, true)
	return nil
}

// serverMessage handles a message of the client's second flight, in order.
func (c *DTLSConn) serverMessage(m *dtlsMessage, now time.Time) *hsError {
	h := c.srv
	msg := m.tlsForm()
	switch {
	case c.state == dtlsAwaitClientCertificate && m.typ == hsCertificate:
		ders, herr := parseCertificates12(msg)
		if herr != nil {
			return herr
		}
		if herr := h.acceptClientCertificates(ders); herr != nil {
			return herr
		}
		c.state = dtlsAwaitClientKeyExchange
	case c.state == dtlsAwaitClientKeyExchange && m.typ == hsClientKeyExchange:
		premaster, herr := h.clientShare12(msg)
		if herr != nil {
			return herr
		}
		h.transcript = append(h.transcript, m.dtlsForm()...)
		var sessionHash []byte
		if h.ems {
			sessionHash = h.transcriptHash()
		}
		h.master = masterSecret12(h.suite.hash, premaster, h.ems, sessionHash, h.ch.random, h.random[:])
		if herr := h.keys12(); herr != nil {
			return herr
		}
		c.installKeys(h.rx, h.tx)
		c.state = dtlsAwaitClientFinished
		if len(h.peerCerts) > 0 {
			c.state = dtlsAwaitCertificateVerify
		}
		return nil
	case c.state == dtlsAwaitCertificateVerify && m.typ == hsCertificateVerify:
		if herr := h.checkCertificateVerify12(msg, h.transcript); herr != nil {
			return herr
		}
		c.state = dtlsAwaitClientFinished
	case c.state == dtlsAwaitClientFinished && m.typ == hsFinished:
		want := prf12(h.suite.hash, h.master, "client finished", h.transcriptHash(), 12)
		if !hmac.Equal(want, m.body) {
			return fail(alertDecryptError, "invalid client finished hash")
		}
		h.transcript = append(h.transcript, m.dtlsForm()...)
		h.clientFinished = want
		c.peerLast = int(m.seq)
		c.flight = []flightEntry{{ccs: true}}
		verify := prf12(h.suite.hash, h.master, "server finished", h.transcriptHash(), 12)
		c.queueMessage(append([]byte{hsFinished, 0, 0, 12}, verify...), 1)
		h.serverFinished = verify
		c.txEpoch = 1
		if verify := h.config.VerifyConnection; verify != nil {
			if err := verify(h.connectionState12()); err != nil {
				return &hsError{alert: alertBadCertificate, err: err}
			}
		}
		// The flight is kept, and sent again if the client's Finished comes
		// again, until the client's application data shows it arrived.
		c.startFlight(now, false)
		c.complete()
		return nil
	default:
		return fail(alertUnexpectedMessage, "unexpected handshake message %d", m.typ)
	}
	h.transcript = append(h.transcript, m.dtlsForm()...)
	return nil
}

// complete ends the handshake: what was kept for it goes, and the
// application data that came before it is delivered.
func (c *DTLSConn) complete() {
	c.state = dtlsDone
	c.partial, c.future = nil, nil
	if c.isClient {
		c.timerOn = false
	}
	if c.opts.OnHandshakeComplete != nil {
		c.opts.OnHandshakeComplete()
	}
	early := c.early
	c.early = nil
	for _, data := range early {
		if c.err != nil || c.opts.OnData == nil {
			break
		}
		c.opts.OnData(data)
	}
}
