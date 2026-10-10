package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	_ "crypto/sha256"
	_ "crypto/sha512"
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"slices"
	"time"
)

// The TLS 1.3 client handshake as a state machine, for a connection that
// must not park a goroutine on it.
//
// crypto/tls can only run a handshake as a blocking call that reads the peer
// and writes to it, and cannot be resumed once a read has failed, so a
// handshake with it needs a goroutine to wait in. This one is driven by the
// bytes themselves: OnData hands process whatever has arrived, and process
// returns what to send back and whether the handshake is over, without ever
// waiting. Nothing is held between calls but the handshake's own state, and no
// goroutine is spent on a peer that is slow to answer, or that never does.
//
// It implements the client side of RFC 8446 for what a client of this
// package's record layer needs: the AES-GCM suites, X25519, P-256 and P-384
// key exchange, a HelloRetryRequest, ALPN, SNI, the server's certificate
// verified against Config.RootCAs (or not, with InsecureSkipVerify) and its
// signature checked, and an empty Certificate for a server that asks for
// one. What it leaves out stays with crypto/tls, which a Config selects by not
// qualifying; see nativeClient. The record layer keys it ends with are the
// ones fast.go would have taken over from crypto/tls, so an established
// connection is the same either way.

const (
	hsClientHello         = 1
	hsServerHello         = 2
	hsEncryptedExtensions = 8
	hsCertificate         = 11
	hsCertificateRequest  = 13
	hsCertificateVerify   = 15
	hsFinished            = 20
	hsMessageHash         = 254

	extServerName        = 0
	extSupportedGroups   = 10
	extSignatureAlgs     = 13
	extALPN              = 16
	extPreSharedKey      = 41
	extSupportedVersions = 43
	extCookie            = 44
	extKeyShare          = 51

	alertHandshakeFailure      = 40
	alertBadCertificate        = 42
	alertIllegalParameter      = 47
	alertDecodeError           = 50
	alertDecryptError          = 51
	alertInternalError         = 80
	alertMissingExtension      = 109
	alertUnsupportedExtension  = 110
	alertNoApplicationProtocol = 120

	// maxHandshakeMessage bounds a handshake message, as crypto/tls does.
	maxHandshakeMessage = 65536
)

// clientHelloSuites are the suites a client of the record layer offers.
var clientHelloSuites = []uint16{stdtls.TLS_AES_128_GCM_SHA256, stdtls.TLS_AES_256_GCM_SHA384}

// clientSignatureAlgs are the signature schemes a server's CertificateVerify
// may use, in the order they are offered.
var clientSignatureAlgs = []uint16{
	0x0403, // ecdsa_secp256r1_sha256
	0x0804, // rsa_pss_rsae_sha256
	0x0805, // rsa_pss_rsae_sha384
	0x0806, // rsa_pss_rsae_sha512
	0x0807, // ed25519
	0x0503, // ecdsa_secp384r1_sha384
	0x0603, // ecdsa_secp521r1_sha512
}

// handshakeCurves are the key exchange groups, in the order they are offered
// when the Config states no preference.
var handshakeCurves = []stdtls.CurveID{stdtls.X25519, stdtls.CurveP256, stdtls.CurveP384}

func curveOf(id stdtls.CurveID) ecdh.Curve {
	switch id {
	case stdtls.X25519:
		return ecdh.X25519()
	case stdtls.CurveP256:
		return ecdh.P256()
	case stdtls.CurveP384:
		return ecdh.P384()
	}
	return nil
}

// nativeClient reports whether a client handshake with config can be run by
// clientHS, which offers TLS 1.3 alone: only a Config that wants no lower
// version qualifies, since a server that answers with a lower one cannot be
// continued with, and is not retried on this connection. The Config must not
// ask for what clientHS does not do either: client certificates, session
// resumption, encrypted client hello, renegotiation, or a set of groups it
// cannot offer.
func nativeClient(config *stdtls.Config) bool {
	if config == nil || config.MinVersion != stdtls.VersionTLS13 ||
		config.MaxVersion != 0 && config.MaxVersion < stdtls.VersionTLS13 {
		return false
	}
	if len(config.Certificates) != 0 || config.GetClientCertificate != nil || config.ClientSessionCache != nil ||
		config.EncryptedClientHelloConfigList != nil || config.Renegotiation != stdtls.RenegotiateNever {
		return false
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

// hsError is a handshake failure, and the alert to send the peer for it, or
// zero when the peer sent the alert.
type hsError struct {
	alert uint8
	err   error
}

func (e *hsError) Error() string { return e.err.Error() }
func (e *hsError) Unwrap() error { return e.err }

func fail(alert uint8, format string, args ...any) *hsError {
	return &hsError{alert: alert, err: fmt.Errorf("tls: "+format, args...)}
}

type hsState uint8

const (
	hsExpectServerHello hsState = iota
	hsExpectEncryptedExtensions
	hsExpectCertificateOrRequest
	hsExpectCertificate
	hsExpectCertificateVerify
	hsExpectFinished
	hsDone
)

// clientHS is one client handshake.
type clientHS struct {
	config *stdtls.Config
	state  hsState

	random    [32]byte
	sessionID [32]byte
	groups    []stdtls.CurveID
	group     stdtls.CurveID
	key       *ecdh.PrivateKey
	cookie    []byte
	// suites are the suites offered, which tests narrow.
	suites []uint16

	// retried says a HelloRetryRequest has been answered, sentCCS that the
	// change_cipher_spec of RFC 8446 appendix D.4 has been sent.
	retried bool
	sentCCS bool

	// transcript is every handshake message so far, in the form that is
	// hashed: after a HelloRetryRequest, the first ClientHello is replaced by
	// its hash.
	transcript []byte
	hsBuf      []byte
	ccsSeen    int

	suiteID uint16
	suite   suite
	// secrets of the key schedule that outlive the handshake keys.
	master   []byte
	clientHS []byte
	serverHS []byte
	rx, tx   *recordKeys

	certs     []*x509.Certificate
	rawCerts  [][]byte
	chains    [][]*x509.Certificate
	protocol  string
	certReq   bool
	reqCtx    []byte
	appClient []byte
	appServer []byte
}

// newClientHS prepares a handshake with config, which nativeClient qualified.
func newClientHS(config *stdtls.Config) (*clientHS, error) {
	if config.ServerName == "" && !config.InsecureSkipVerify {
		return nil, errors.New("tls: either ServerName or InsecureSkipVerify must be specified in the tls.Config")
	}
	h := &clientHS{config: config, suites: clientHelloSuites}
	rnd := config.Rand
	if rnd == nil {
		rnd = rand.Reader
	}
	if _, err := io.ReadFull(rnd, h.random[:]); err != nil {
		return nil, err
	}
	// Echoed by the server, and a non-empty one asks it to behave like
	// TLS 1.2 to middleboxes (RFC 8446 appendix D.4).
	if _, err := io.ReadFull(rnd, h.sessionID[:]); err != nil {
		return nil, err
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
	if err := h.newKeyShare(h.groups[0]); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *clientHS) rander() io.Reader {
	if h.config.Rand != nil {
		return h.config.Rand
	}
	return rand.Reader
}

func (h *clientHS) newKeyShare(group stdtls.CurveID) error {
	key, err := curveOf(group).GenerateKey(h.rander())
	if err != nil {
		return err
	}
	h.group, h.key = group, key
	return nil
}

// hello returns the record that opens the handshake.
func (h *clientHS) hello() []byte {
	msg := h.clientHello()
	h.transcript = append(h.transcript, msg...)
	return appendRecord(nil, recordTypeHandshake, 0x0301, msg)
}

func appendRecord(dst []byte, typ byte, version uint16, body []byte) []byte {
	dst = append(dst, typ, byte(version>>8), byte(version), byte(len(body)>>8), byte(len(body)))
	return append(dst, body...)
}

// builder appends length-prefixed fields.
type builder struct{ b []byte }

func (b *builder) u8(v uint8)   { b.b = append(b.b, v) }
func (b *builder) u16(v uint16) { b.b = binary.BigEndian.AppendUint16(b.b, v) }
func (b *builder) u24(v int)    { b.b = append(b.b, byte(v>>16), byte(v>>8), byte(v)) }
func (b *builder) raw(p []byte) { b.b = append(b.b, p...) }

// vec16 appends a field of up to 65535 bytes, whose contents fn writes.
func (b *builder) vec16(fn func(*builder)) {
	at := len(b.b)
	b.b = append(b.b, 0, 0)
	fn(b)
	binary.BigEndian.PutUint16(b.b[at:], uint16(len(b.b)-at-2))
}

func (b *builder) vec8(fn func(*builder)) {
	at := len(b.b)
	b.b = append(b.b, 0)
	fn(b)
	b.b[at] = byte(len(b.b) - at - 1)
}

// clientHello builds the ClientHello message.
func (h *clientHS) clientHello() []byte {
	var b builder
	b.u8(hsClientHello)
	b.u24(0) // length, set below
	b.u16(0x0303)
	b.raw(h.random[:])
	b.vec8(func(b *builder) { b.raw(h.sessionID[:]) })
	b.vec16(func(b *builder) {
		for _, id := range h.suites {
			b.u16(id)
		}
	})
	b.vec8(func(b *builder) { b.u8(0) }) // compression: null
	b.vec16(func(b *builder) {
		if name := hostnameInSNI(h.config.ServerName); name != "" {
			b.u16(extServerName)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					b.u8(0) // host_name
					b.vec16(func(b *builder) { b.raw([]byte(name)) })
				})
			})
		}
		b.u16(extSupportedGroups)
		b.vec16(func(b *builder) {
			b.vec16(func(b *builder) {
				for _, id := range h.groups {
					b.u16(uint16(id))
				}
			})
		})
		b.u16(extSignatureAlgs)
		b.vec16(func(b *builder) {
			b.vec16(func(b *builder) {
				for _, alg := range clientSignatureAlgs {
					b.u16(alg)
				}
			})
		})
		if len(h.config.NextProtos) > 0 {
			b.u16(extALPN)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					for _, p := range h.config.NextProtos {
						b.vec8(func(b *builder) { b.raw([]byte(p)) })
					}
				})
			})
		}
		b.u16(extSupportedVersions)
		b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.u16(stdtls.VersionTLS13) }) })
		b.u16(extKeyShare)
		b.vec16(func(b *builder) {
			b.vec16(func(b *builder) {
				b.u16(uint16(h.group))
				b.vec16(func(b *builder) { b.raw(h.key.PublicKey().Bytes()) })
			})
		})
		if h.cookie != nil {
			b.u16(extCookie)
			b.vec16(func(b *builder) { b.vec16(func(b *builder) { b.raw(h.cookie) }) })
		}
	})
	msg := b.b
	msg[1], msg[2], msg[3] = byte((len(msg)-4)>>16), byte((len(msg)-4)>>8), byte(len(msg)-4)
	return msg
}

// hostnameInSNI is the name a ClientHello may carry, which is a host name and
// not an address (RFC 6066 section 3).
func hostnameInSNI(name string) string {
	host := name
	if len(host) > 0 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	if i := bytes.LastIndexByte([]byte(host), '%'); i > 0 {
		host = host[:i]
	}
	if net.ParseIP(host) != nil {
		return ""
	}
	for len(name) > 0 && name[len(name)-1] == '.' {
		name = name[:len(name)-1]
	}
	return name
}

// reader consumes a handshake message's fields. Any read past the end fails
// the whole parse.
type reader struct {
	b  []byte
	ok bool
}

func newReader(b []byte) reader { return reader{b: b, ok: true} }

func (r *reader) take(n int) []byte {
	if !r.ok || n < 0 || len(r.b) < n {
		r.ok = false
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u8() uint8 {
	if p := r.take(1); p != nil {
		return p[0]
	}
	return 0
}

func (r *reader) u16() uint16 {
	if p := r.take(2); p != nil {
		return binary.BigEndian.Uint16(p)
	}
	return 0
}

func (r *reader) u24() int {
	if p := r.take(3); p != nil {
		return int(p[0])<<16 | int(p[1])<<8 | int(p[2])
	}
	return 0
}

func (r *reader) vec8() []byte  { return r.take(int(r.u8())) }
func (r *reader) vec16() []byte { return r.take(int(r.u16())) }
func (r *reader) vec24() []byte { return r.take(r.u24()) }
func (r *reader) empty() bool   { return r.ok && len(r.b) == 0 }

// process consumes the records in in that are complete, and reports how much
// of it that was, what to send the peer, and whether the handshake is over.
// It never waits: when in ends inside a record, it stops there and the caller
// offers the rest again with whatever arrives next. Once it is over, the
// bytes behind the Finished are the connection's first records under the
// application keys, and are left to the caller.
//
// in is overwritten, since records are decrypted where they lie.
func (h *clientHS) process(in []byte) (consumed int, out []byte, done bool, err *hsError) {
	for h.state != hsDone && len(in)-consumed >= recordHeaderLen {
		rec := in[consumed:]
		length := int(rec[3])<<8 | int(rec[4])
		if length > maxCiphertext13 {
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
	return consumed, out, h.state == hsDone, nil
}

// record handles one record of the handshake.
func (h *clientHS) record(record []byte, out *[]byte) *hsError {
	typ, body := record[0], record[recordHeaderLen:]
	if record[1] != 3 {
		return fail(alertProtocolVersion, "unsupported record version %#x%02x", record[1], record[2])
	}
	switch typ {
	case recordTypeChangeCipherSpec:
		// Sent by a server for middleboxes, and ignored (RFC 8446 section 5).
		h.ccsSeen++
		if len(body) != 1 || body[0] != 1 || h.ccsSeen > 2 {
			return fail(alertUnexpectedMessage, "unexpected change_cipher_spec")
		}
		return nil
	case recordTypeAlert:
		if h.rx != nil {
			return fail(alertUnexpectedMessage, "unprotected alert after ServerHello")
		}
		return remoteAlert(body)
	case recordTypeHandshake:
		if h.rx != nil {
			return fail(alertUnexpectedMessage, "unprotected handshake record after ServerHello")
		}
		return h.handshakeData(body, out)
	case recordTypeApplicationData:
		if h.rx == nil {
			return fail(alertUnexpectedMessage, "protected record before ServerHello")
		}
		inner, plain, alert, ok := h.rx.open(record)
		if !ok {
			return fail(alert, "bad record")
		}
		switch inner {
		case recordTypeHandshake:
			return h.handshakeData(plain, out)
		case recordTypeAlert:
			return remoteAlert(plain)
		}
		return fail(alertUnexpectedMessage, "unexpected record type %d during the handshake", inner)
	}
	return fail(alertUnexpectedMessage, "unexpected record type %d", typ)
}

func remoteAlert(body []byte) *hsError {
	if len(body) != 2 {
		return fail(alertDecodeError, "malformed alert")
	}
	return &hsError{err: &net.OpError{Op: "remote error", Err: stdtls.AlertError(body[1])}}
}

// handshakeData adds handshake bytes to the messages being reassembled and
// handles each message that is complete.
func (h *clientHS) handshakeData(data []byte, out *[]byte) *hsError {
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
		wasServerHello := h.state == hsExpectServerHello
		if herr := h.message(msg, out); herr != nil {
			return herr
		}
		h.hsBuf = h.hsBuf[size:]
		if (wasServerHello || h.state == hsDone) && len(h.hsBuf) != 0 {
			// Keys change after the ServerHello and the Finished, so no
			// message may run across the change.
			return fail(alertUnexpectedMessage, "handshake data after the last message of a flight")
		}
	}
	if len(h.hsBuf) == 0 {
		h.hsBuf = nil
	}
	return nil
}

func (h *clientHS) message(msg []byte, out *[]byte) *hsError {
	switch typ := msg[0]; {
	case h.state == hsExpectServerHello && typ == hsServerHello:
		return h.serverHello(msg, out)
	case h.state == hsExpectEncryptedExtensions && typ == hsEncryptedExtensions:
		return h.encryptedExtensions(msg)
	case h.state == hsExpectCertificateOrRequest && typ == hsCertificateRequest:
		return h.certificateRequest(msg)
	case (h.state == hsExpectCertificateOrRequest || h.state == hsExpectCertificate) && typ == hsCertificate:
		return h.certificate(msg)
	case h.state == hsExpectCertificateVerify && typ == hsCertificateVerify:
		return h.certificateVerify(msg)
	case h.state == hsExpectFinished && typ == hsFinished:
		return h.finished(msg, out)
	}
	return fail(alertUnexpectedMessage, "unexpected handshake message %d", msg[0])
}

// transcriptHash is the hash of the handshake so far.
func (h *clientHS) transcriptHash() []byte {
	d := h.suite.hash()
	d.Write(h.transcript)
	return d.Sum(nil)
}

func hashBytes(f func() hash.Hash, b []byte) []byte {
	d := f()
	d.Write(b)
	return d.Sum(nil)
}

// serverHello handles the ServerHello, or a HelloRetryRequest, which has its
// shape.
func (h *clientHS) serverHello(msg []byte, out *[]byte) *hsError {
	r := newReader(msg[4:])
	version := r.u16()
	random := r.take(32)
	sessionID := r.vec8()
	cipherSuite := r.u16()
	compression := r.u8()
	extensions := r.vec16()
	if !r.empty() {
		return fail(alertDecodeError, "malformed ServerHello")
	}
	if version != 0x0303 {
		return fail(alertProtocolVersion, "server selected a protocol this client does not offer")
	}
	if !bytes.Equal(sessionID, h.sessionID[:]) {
		return fail(alertIllegalParameter, "server did not echo the session ID")
	}
	if compression != 0 {
		return fail(alertIllegalParameter, "server selected compression")
	}
	var (
		selected       uint16
		share          []byte
		cookie         []byte
		supported      bool
		versionOK      bool
		haveShare      bool
		haveSelected   bool
		haveCookie     bool
		seen           = map[uint16]bool{}
		hrr            = bytes.Equal(random, helloRetryRandom[:])
		extensionsData = newReader(extensions)
	)
	for extensionsData.ok && len(extensionsData.b) > 0 {
		typ := extensionsData.u16()
		data := extensionsData.vec16()
		if !extensionsData.ok || seen[typ] {
			return fail(alertDecodeError, "malformed ServerHello extensions")
		}
		seen[typ] = true
		d := newReader(data)
		switch typ {
		case extSupportedVersions:
			supported = true
			versionOK = d.u16() == stdtls.VersionTLS13 && d.empty()
		case extKeyShare:
			if hrr {
				selected = d.u16()
				haveSelected = d.empty()
			} else {
				selected = d.u16()
				share = d.vec16()
				haveShare = d.empty()
			}
		case extCookie:
			cookie = d.vec16()
			haveCookie = d.empty() && len(cookie) > 0
		default:
			// Including pre_shared_key, which this client never offered.
			return fail(alertUnsupportedExtension, "server sent an unsolicited extension %d", typ)
		}
		if !d.ok {
			return fail(alertDecodeError, "malformed ServerHello extension")
		}
	}
	if !extensionsData.ok {
		return fail(alertDecodeError, "malformed ServerHello extensions")
	}
	if !supported || !versionOK {
		return fail(alertProtocolVersion, "server does not support TLS 1.3")
	}
	if !slices.Contains(h.suites, cipherSuite) {
		return fail(alertIllegalParameter, "server selected a cipher suite that was not offered")
	}
	s, ok := lookupSuite(stdtls.VersionTLS13, cipherSuite)
	if !ok {
		return fail(alertIllegalParameter, "unsupported cipher suite")
	}
	if hrr {
		return h.helloRetryRequest(msg, cipherSuite, s, selected, haveSelected, seen[extKeyShare], cookie, haveCookie, out)
	}
	if h.retried && cipherSuite != h.suiteID {
		return fail(alertIllegalParameter, "server changed its cipher suite after a HelloRetryRequest")
	}
	h.suiteID, h.suite = cipherSuite, s
	if !haveShare || stdtls.CurveID(selected) != h.group {
		return fail(alertIllegalParameter, "server did not select the key share offered")
	}
	curve := curveOf(h.group)
	peer, err := curve.NewPublicKey(share)
	if err != nil {
		return fail(alertIllegalParameter, "invalid server key share: %v", err)
	}
	shared, err := h.key.ECDH(peer)
	if err != nil {
		return fail(alertIllegalParameter, "key exchange failed: %v", err)
	}
	h.transcript = append(h.transcript, msg...)
	return h.handshakeKeys(shared, out)
}

// helloRetryRequest answers a server that wants another group, or to see a
// cookie, with a second ClientHello.
func (h *clientHS) helloRetryRequest(msg []byte, cipherSuite uint16, s suite, selected uint16, haveSelected, shareExt bool,
	cookie []byte, haveCookie bool, out *[]byte) *hsError {
	if h.retried {
		return fail(alertUnexpectedMessage, "second HelloRetryRequest")
	}
	if shareExt && !haveSelected {
		return fail(alertDecodeError, "malformed HelloRetryRequest")
	}
	if !haveSelected && !haveCookie {
		return fail(alertIllegalParameter, "HelloRetryRequest asks for nothing")
	}
	if haveSelected {
		group := stdtls.CurveID(selected)
		if group == h.group || !slices.Contains(h.groups, group) {
			return fail(alertIllegalParameter, "HelloRetryRequest selected a group that cannot be used")
		}
		if err := h.newKeyShare(group); err != nil {
			return fail(alertInternalError, "%v", err)
		}
	}
	if haveCookie {
		h.cookie = bytes.Clone(cookie)
	}
	h.retried = true
	h.suiteID, h.suite = cipherSuite, s
	// The first ClientHello is replaced in the transcript by its hash.
	first := hashBytes(s.hash, h.transcript)
	h.transcript = append([]byte{hsMessageHash, 0, 0, byte(len(first))}, first...)
	h.transcript = append(h.transcript, msg...)
	second := h.clientHello()
	h.transcript = append(h.transcript, second...)
	h.sendCCS(out)
	*out = appendRecord(*out, recordTypeHandshake, 0x0303, second)
	return nil
}

// sendCCS sends the change_cipher_spec that makes a TLS 1.3 handshake look
// like a resumed TLS 1.2 one to a middlebox, once.
func (h *clientHS) sendCCS(out *[]byte) {
	if !h.sentCCS {
		h.sentCCS = true
		*out = append(*out, recordTypeChangeCipherSpec, 3, 3, 0, 1, 1)
	}
}

// deriveSecret is Derive-Secret (RFC 8446 section 7.1): HKDF-Expand-Label with
// the hash of the transcript as its context.
func deriveSecret(f func() hash.Hash, secret []byte, label string, transcript []byte) ([]byte, error) {
	return expandLabelContext(f, secret, label, transcript, f().Size())
}

// expandLabelContext is HKDF-Expand-Label (RFC 8446 section 7.1).
func expandLabelContext(f func() hash.Hash, secret []byte, label string, context []byte, length int) ([]byte, error) {
	info := make([]byte, 0, 4+6+len(label)+len(context))
	info = binary.BigEndian.AppendUint16(info, uint16(length))
	info = append(info, byte(6+len(label)))
	info = append(info, "tls13 "...)
	info = append(info, label...)
	info = append(info, byte(len(context)))
	info = append(info, context...)
	return hkdf.Expand(f, secret, string(info), length)
}

// handshakeKeys runs the key schedule up to the handshake traffic secrets,
// and installs them: the server's for reading, and ours for what follows.
func (h *clientHS) handshakeKeys(shared []byte, out *[]byte) *hsError {
	f := h.suite.hash
	size := f().Size()
	zeros := make([]byte, size)
	early, err := hkdf.Extract(f, zeros, nil)
	if err != nil {
		return fail(alertInternalError, "%v", err)
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
	derived, err = deriveSecret(f, handshake, "derived", hashBytes(f, nil))
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.master, err = hkdf.Extract(f, zeros, derived); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.rx, err = newKeys13(h.suite.keyLen, f, h.serverHS); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.tx, err = newKeys13(h.suite.keyLen, f, h.clientHS); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.keyLog("CLIENT_HANDSHAKE_TRAFFIC_SECRET", h.clientHS)
	h.keyLog("SERVER_HANDSHAKE_TRAFFIC_SECRET", h.serverHS)
	h.sendCCS(out)
	h.state = hsExpectEncryptedExtensions
	return nil
}

// keyLog writes a secret to Config.KeyLogWriter in the NSS format.
func (h *clientHS) keyLog(label string, secret []byte) {
	if h.config.KeyLogWriter == nil {
		return
	}
	line := fmt.Sprintf("%s %s %s\n", label, hex.EncodeToString(h.random[:]), hex.EncodeToString(secret))
	_, _ = io.WriteString(h.config.KeyLogWriter, line)
}

func (h *clientHS) encryptedExtensions(msg []byte) *hsError {
	r := newReader(msg[4:])
	extensions := newReader(r.vec16())
	if !r.empty() || !extensions.ok {
		return fail(alertDecodeError, "malformed EncryptedExtensions")
	}
	seen := map[uint16]bool{}
	for len(extensions.b) > 0 {
		typ := extensions.u16()
		data := extensions.vec16()
		if !extensions.ok || seen[typ] {
			return fail(alertDecodeError, "malformed EncryptedExtensions")
		}
		seen[typ] = true
		if typ == extALPN {
			d := newReader(data)
			list := newReader(d.vec16())
			name := list.vec8()
			if !d.empty() || !list.empty() || len(name) == 0 {
				return fail(alertDecodeError, "malformed ALPN extension")
			}
			if !slices.Contains(h.config.NextProtos, string(name)) {
				return fail(alertNoApplicationProtocol, "server selected an unadvertised ALPN protocol")
			}
			h.protocol = string(name)
		}
	}
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectCertificateOrRequest
	return nil
}

func (h *clientHS) certificateRequest(msg []byte) *hsError {
	r := newReader(msg[4:])
	ctx := r.vec8()
	extensions := r.vec16()
	if !r.empty() || extensions == nil {
		return fail(alertDecodeError, "malformed CertificateRequest")
	}
	h.certReq, h.reqCtx = true, bytes.Clone(ctx)
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectCertificate
	return nil
}

func (h *clientHS) certificate(msg []byte) *hsError {
	r := newReader(msg[4:])
	ctx := r.vec8()
	list := newReader(r.vec24())
	if !r.empty() || !list.ok || len(ctx) != 0 {
		return fail(alertDecodeError, "malformed Certificate")
	}
	for len(list.b) > 0 {
		der := list.vec24()
		extensions := list.vec16()
		if !list.ok || len(der) == 0 || extensions == nil {
			return fail(alertDecodeError, "malformed Certificate")
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return fail(alertBadCertificate, "failed to parse certificate from server: %v", err)
		}
		h.certs = append(h.certs, cert)
		h.rawCerts = append(h.rawCerts, bytes.Clone(der))
	}
	if len(h.certs) == 0 {
		return fail(alertDecodeError, "server sent an empty certificate chain")
	}
	if !h.config.InsecureSkipVerify {
		now := time.Now()
		if h.config.Time != nil {
			now = h.config.Time()
		}
		opts := x509.VerifyOptions{
			Roots:         h.config.RootCAs,
			CurrentTime:   now,
			DNSName:       h.config.ServerName,
			Intermediates: x509.NewCertPool(),
		}
		for _, c := range h.certs[1:] {
			opts.Intermediates.AddCert(c)
		}
		chains, err := h.certs[0].Verify(opts)
		if err != nil {
			return &hsError{alert: alertBadCertificate, err: &stdtls.CertificateVerificationError{UnverifiedCertificates: h.certs, Err: err}}
		}
		h.chains = chains
	}
	if verify := h.config.VerifyPeerCertificate; verify != nil {
		if err := verify(h.rawCerts, h.chains); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectCertificateVerify
	return nil
}

// signatureHash is the hash a TLS 1.3 signature scheme signs with, or zero
// for Ed25519, which signs the message itself.
func signatureHash(alg uint16) (crypto.Hash, bool) {
	switch alg {
	case 0x0403, 0x0804:
		return crypto.SHA256, true
	case 0x0503, 0x0805:
		return crypto.SHA384, true
	case 0x0603, 0x0806:
		return crypto.SHA512, true
	case 0x0807:
		return 0, true
	}
	return 0, false
}

func (h *clientHS) certificateVerify(msg []byte) *hsError {
	r := newReader(msg[4:])
	alg := r.u16()
	signature := r.vec16()
	if !r.empty() {
		return fail(alertDecodeError, "malformed CertificateVerify")
	}
	ch, ok := signatureHash(alg)
	if !ok || !slices.Contains(clientSignatureAlgs, alg) {
		return fail(alertIllegalParameter, "server signed with a scheme that was not offered")
	}
	// The signature covers the transcript up to the Certificate (RFC 8446
	// section 4.4.3).
	signed := bytes.Repeat([]byte{0x20}, 64)
	signed = append(signed, "TLS 1.3, server CertificateVerify"...)
	signed = append(signed, 0)
	signed = append(signed, h.transcriptHash()...)
	pub := h.certs[0].PublicKey
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
	}
	if err != nil {
		return fail(alertDecryptError, "invalid signature by the server certificate: %v", err)
	}
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectFinished
	return nil
}

// finishedMAC is the verify_data of a Finished (RFC 8446 section 4.4.4).
func (h *clientHS) finishedMAC(base []byte) ([]byte, error) {
	key, err := expandLabelContext(h.suite.hash, base, "finished", nil, h.suite.hash().Size())
	if err != nil {
		return nil, err
	}
	mac := hmac.New(h.suite.hash, key)
	mac.Write(h.transcriptHash())
	return mac.Sum(nil), nil
}

// finished checks the server's Finished and, with it, completes the
// handshake: it answers with ours, under the handshake keys, and moves both
// directions to the application keys.
func (h *clientHS) finished(msg []byte, out *[]byte) *hsError {
	want, err := h.finishedMAC(h.serverHS)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if !hmac.Equal(want, msg[4:]) {
		return fail(alertDecryptError, "invalid server finished hash")
	}
	h.transcript = append(h.transcript, msg...)
	// Application secrets cover the transcript through the server's Finished,
	// and not the client's certificate or Finished that follow.
	t := h.transcriptHash()
	f := h.suite.hash
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

	var flight []byte
	if h.certReq {
		// No certificate to offer: an empty Certificate says so, and the
		// server decides whether it can go on.
		cert := []byte{hsCertificate, 0, 0, byte(1 + len(h.reqCtx) + 3)}
		cert = append(cert, byte(len(h.reqCtx)))
		cert = append(cert, h.reqCtx...)
		cert = append(cert, 0, 0, 0)
		h.transcript = append(h.transcript, cert...)
		var serr error
		if flight, serr = h.tx.seal(flight, recordTypeHandshake, cert, nil); serr != nil {
			return fail(alertInternalError, "%v", serr)
		}
	}
	verify, err := h.finishedMAC(h.clientHS)
	if err != nil {
		return fail(alertInternalError, "%v", err)
	}
	fin := append([]byte{hsFinished, 0, 0, byte(len(verify))}, verify...)
	if flight, err = h.tx.seal(flight, recordTypeHandshake, fin, nil); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.transcript = append(h.transcript, fin...)
	*out = append(*out, flight...)

	if h.rx, err = newKeys13(h.suite.keyLen, f, h.appServer); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.tx, err = newKeys13(h.suite.keyLen, f, h.appClient); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.keyLog("CLIENT_TRAFFIC_SECRET_0", h.appClient)
	h.keyLog("SERVER_TRAFFIC_SECRET_0", h.appServer)
	h.keyLog("EXPORTER_SECRET", exporter)

	if verify := h.config.VerifyConnection; verify != nil {
		if err := verify(h.connectionState()); err != nil {
			return &hsError{alert: alertBadCertificate, err: err}
		}
	}
	h.state = hsDone
	return nil
}

// connectionState is what ConnectionState reports for the connection.
func (h *clientHS) connectionState() stdtls.ConnectionState {
	return stdtls.ConnectionState{
		Version:                    stdtls.VersionTLS13,
		HandshakeComplete:          true,
		CipherSuite:                h.suiteID,
		NegotiatedProtocol:         h.protocol,
		NegotiatedProtocolIsMutual: h.protocol != "",
		ServerName:                 h.config.ServerName,
		PeerCertificates:           h.certs,
		VerifiedChains:             h.chains,
	}
}

// alertRecord is the record that tells the peer the handshake has failed with
// alert: under our keys once there are any.
func (h *clientHS) alertRecord(alert uint8) []byte {
	body := []byte{alertLevelError, alert}
	if h.tx == nil || h.state == hsDone {
		return appendRecord(nil, recordTypeAlert, 0x0303, body)
	}
	record, err := h.tx.seal(nil, recordTypeAlert, body, nil)
	if err != nil {
		return nil
	}
	return record
}
