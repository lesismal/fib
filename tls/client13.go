package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
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

// The TLS client handshake as a state machine, for a connection that must not
// park a goroutine on it. This file has the machinery both versions share and
// TLS 1.3 (RFC 8446); client12.go has TLS 1.2 (RFC 5246).
//
// crypto/tls can only run a handshake as a blocking call that reads the peer
// and writes to it, and cannot be resumed once a read has failed, so a
// handshake with it needs a goroutine to wait in. This one is driven by the
// bytes themselves: OnData hands process whatever has arrived, and process
// returns what to send back and whether the handshake is over, without ever
// waiting. Nothing is held between calls but the handshake's own state, and no
// goroutine is spent on a peer that is slow to answer, or that never does.
//
// It implements the client side of RFC 8446 and RFC 5246 for what a client of
// this package's record layer needs: the AES-GCM and ChaCha20-Poly1305 suites
// of TLS 1.3, and the ECDHE suites of TLS 1.2 that use them or AES-CBC; X25519,
// P-256 and P-384 key exchange, a HelloRetryRequest, ALPN, SNI, the server's
// certificate verified against Config.RootCAs (or not, with InsecureSkipVerify)
// and its signature checked, and for a server that asks for one, the client's
// own certificate, from Config.Certificates or GetClientCertificate (auth.go),
// or none. What it leaves out stays with crypto/tls, which a Config selects by
// not qualifying; see nativeClient. The record layer keys it ends with are the
// ones fast.go would have taken over from crypto/tls, so an established
// connection is the same either way.

const (
	hsClientHello         = 1
	hsServerHello         = 2
	hsEncryptedExtensions = 8
	hsCertificate         = 11
	hsCertificateRequest  = 13
	hsCertificateVerify   = 15
	hsNewSessionTicket    = 4
	hsServerKeyExchange   = 12
	hsServerHelloDone     = 14
	hsClientKeyExchange   = 16
	hsFinished            = 20
	hsMessageHash         = 254

	extServerName        = 0
	extSupportedGroups   = 10
	extSignatureAlgs     = 13
	extALPN              = 16
	extECPointFormats    = 11
	extExtendedMasterSec = 23
	extSessionTicket     = 35
	extStatusRequest     = 5
	extPreSharedKey      = 41
	extEarlyData         = 42
	hsEndOfEarlyData     = 5
	extRenegotiationInfo = 0xff01
	extSupportedVersions = 43
	extCookie            = 44
	extKeyShare          = 51
	extPSKModesClient    = 45

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

// clientSuites12 are the TLS 1.2 suites it offers: the ECDHE ones the record
// layer protects, AEADs first, in the order crypto/tls offers them where the
// CPU has AES-GCM. Key exchange by RSA is not offered, as crypto/tls no longer
// does by default. clientSuites12NoAES puts ChaCha20-Poly1305 first, for a CPU
// without AES-GCM.
var clientSuites12 = []uint16{
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	stdtls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
}

var clientSuites12NoAES = chachaFirst(clientSuites12, func(id uint16) uint16 { return id })

// clientSignatureAlgs are the signature schemes a server's CertificateVerify
// may use in TLS 1.3, in the order they are offered. TLS 1.2 also takes the
// PKCS #1 ones, in clientSignatureAlgs12.
var clientSignatureAlgs = []uint16{
	0x0403, // ecdsa_secp256r1_sha256
	0x0804, // rsa_pss_rsae_sha256
	0x0805, // rsa_pss_rsae_sha384
	0x0806, // rsa_pss_rsae_sha512
	0x0807, // ed25519
	0x0503, // ecdsa_secp384r1_sha384
	0x0603, // ecdsa_secp521r1_sha512
}

var clientSignatureAlgs12 = []uint16{
	0x0403, // ecdsa_secp256r1_sha256
	0x0804, // rsa_pss_rsae_sha256
	0x0401, // rsa_pkcs1_sha256
	0x0503, // ecdsa_secp384r1_sha384
	0x0805, // rsa_pss_rsae_sha384
	0x0501, // rsa_pkcs1_sha384
	0x0806, // rsa_pss_rsae_sha512
	0x0601, // rsa_pkcs1_sha512
	0x0603, // ecdsa_secp521r1_sha512
	0x0807, // ed25519
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

// clientVersions are the protocol versions a handshake with config offers,
// newest first: the ones within the Config's range that clientHS can speak.
// None means the Config is for crypto/tls: it wants a version below TLS 1.2.
func clientVersions(config *stdtls.Config) []uint16 {
	lowest, highest := config.MinVersion, config.MaxVersion
	if lowest == 0 {
		lowest = stdtls.VersionTLS12
	}
	if highest == 0 {
		highest = stdtls.VersionTLS13
	}
	if lowest < stdtls.VersionTLS12 {
		return nil
	}
	var versions []uint16
	for _, v := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		if v >= lowest && v <= highest {
			versions = append(versions, v)
		}
	}
	return versions
}

// suites12 are the TLS 1.2 suites a handshake with config offers: those of
// clientSuites12, or clientSuites12NoAES where the CPU has no AES-GCM, that
// Config.CipherSuites allows, if it lists any.
func suites12(config *stdtls.Config) []uint16 {
	offered := clientSuites12
	if !aesGCMHardware {
		offered = clientSuites12NoAES
	}
	if len(config.CipherSuites) == 0 {
		return offered
	}
	var out []uint16
	for _, id := range offered {
		if slices.Contains(config.CipherSuites, id) {
			out = append(out, id)
		}
	}
	return out
}

// nativeClient reports whether a client handshake with config can be run by
// clientHS, which speaks TLS 1.3 and 1.2: the Config must not want a lower
// version, since a server that answers with one cannot be continued with, and
// is not retried on this connection. It must not ask for what clientHS does
// not do either: crypto/tls's session resumption, encrypted client hello,
// renegotiation, a set of groups or suites it cannot offer. A certificate whose
// key cannot sign by itself is not one to answer a CertificateRequest with,
// for clientHS as for crypto/tls, so it is no reason either.
func nativeClient(config *stdtls.Config) bool {
	if config == nil || config.Renegotiation != stdtls.RenegotiateNever {
		return false
	}
	if config.ClientSessionCache != nil || config.EncryptedClientHelloConfigList != nil {
		return false
	}
	versions := clientVersions(config)
	if len(versions) == 0 {
		return false
	}
	if len(versions) == 1 && versions[0] == stdtls.VersionTLS12 && len(suites12(config)) == 0 {
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
	// TLS 1.2
	hs12ExpectCertificate
	hs12ExpectServerKeyExchange
	hs12ExpectCertificateRequestOrDone
	hs12ExpectServerHelloDone
	hs12ExpectCCS
	hs12ExpectFinished
	hsDone
)

// clientHS is one client handshake.
type clientHS struct {
	config *stdtls.Config
	state  hsState

	random    [32]byte
	sessionID []byte
	groups    []stdtls.CurveID
	group     stdtls.CurveID
	key       *ecdh.PrivateKey
	cookie    []byte
	// versions are the versions offered, newest first, and suites and
	// suites12 the TLS 1.3 and TLS 1.2 suites, which tests narrow.
	versions []uint16
	suites   []uint16
	suites12 []uint16
	// version is the one the server chose, once it has.
	version uint16

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
	// rxOn and txOn say the records read and written are protected, which
	// is so from the ServerHello and the keys that follow it in TLS 1.3, and
	// from each side's change_cipher_spec in TLS 1.2. flightEnd says the
	// message just handled ended a flight, so that nothing may follow it in
	// the same record: the keys change, or it is our turn.
	rxOn, txOn bool
	flightEnd  bool

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
	appClient []byte
	appServer []byte
	exporter  []byte

	// A server's CertificateRequest: whether it sent one, its context, the
	// schemes and authorities it accepts; the certificate the client answers
	// with, an empty one for none, once it is known; and pick, the call of
	// GetClientCertificate (see auth.go).
	certReq bool
	reqCtx  []byte
	reqAlgs []uint16
	reqCAs  [][]byte
	own     *stdtls.Certificate
	pick    certCall
	// check is the verification of the server's certificate, and then
	// VerifyConnection, each run off the goroutine that drives the handshake
	// (see checkThen).
	check certCall

	// TLS 1.2: the server's random, whether the master secret is the
	// extended one, the server's key exchange parameters, and our Finished,
	// which is the connection's tls-unique.
	serverRandom   [32]byte
	ems            bool
	skxCurve       stdtls.CurveID
	skxPub         []byte
	clientFinished []byte

	// quic is set for a QUIC handshake, which delivers its messages and
	// secrets as events instead of records.
	quic       *quicCtx
	peerParams []byte

	// Resumption, for QUIC: where sessions come from and go to, the session
	// this handshake offers, and how far it got. offerEarly says that early
	// data is offered, earlyActive that it still is: until the server turns
	// it away. resumed says the server took the session, accepted that it
	// took the early data. While the QUIC layer decides whether to offer early
	// data, pending is the start of the handshake and resumeState the session
	// the layer was asked about.
	sessions    QUICSessionCache
	session     *QUICSession
	offerEarly  bool
	earlyActive bool
	resumed     bool
	accepted    bool
	resumeState *stdtls.SessionState
	pending     func() *hsError
	// resumption is the resumption master secret, from which the tickets of
	// the server's NewSessionTicket messages give a PSK each.
	resumption []byte
	// Early data over TCP: the keys it is sealed under, which the layer seals
	// with as the application writes; sealEarly seals the EndOfEarlyData that
	// ends it, with the same keys under the layer's lock; decided is told
	// whether the server took it.
	earlyTx      *recordKeys
	sealEarly    func(msg []byte) ([]byte, error)
	decided      func(accepted bool)
	earlyRefused bool
}

// newClientHS prepares a handshake with config, which nativeClient qualified.
func newClientHS(config *stdtls.Config) (*clientHS, error) { return newClientHSFor(config, nil) }

// newClientHSFor is newClientHS for a handshake that is QUIC's if q is not nil
// (RFC 9001): TLS 1.3 alone, with no compatibility mode, any of its three
// suites since TLS protects no records there, and the transport parameters in
// an extension.
func newClientHSFor(config *stdtls.Config, q *quicCtx) (*clientHS, error) {
	if config.ServerName == "" && !config.InsecureSkipVerify {
		return nil, errors.New("tls: either ServerName or InsecureSkipVerify must be specified in the tls.Config")
	}
	h := &clientHS{config: config, suites: suites13(false), suites12: suites12(config), versions: clientVersions(config), quic: q}
	if len(h.suites12) == 0 {
		// Only TLS 1.3 is left to offer.
		h.versions = slices.DeleteFunc(slices.Clone(h.versions), func(v uint16) bool { return v == stdtls.VersionTLS12 })
	}
	rnd := config.Rand
	if rnd == nil {
		rnd = rand.Reader
	}
	if _, err := io.ReadFull(rnd, h.random[:]); err != nil {
		return nil, err
	}
	if q != nil {
		h.suites, h.suites12, h.versions = quicSuites, nil, []uint16{stdtls.VersionTLS13}
	} else {
		// Echoed by the server, and a non-empty one asks it to behave like
		// TLS 1.2 to middleboxes (RFC 8446 appendix D.4).
		h.sessionID = make([]byte, 32)
		if _, err := io.ReadFull(rnd, h.sessionID); err != nil {
			return nil, err
		}
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

// offers reports whether the handshake offers version.
func (h *clientHS) offers(version uint16) bool { return slices.Contains(h.versions, version) }

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
	h.earlyActive = h.offerEarly && h.session != nil
	msg := h.clientHello()
	h.transcript = append(h.transcript, msg...)
	rec := appendRecords(nil, recordTypeHandshake, 0x0301, msg)
	if h.earlyActive {
		secret, err := h.earlySecret()
		if err != nil {
			h.earlyActive = false
			return rec
		}
		if h.earlyTx, err = newKeys13(h.suite13Of(h.session.suite), h.sessionHash(), secret); err != nil {
			h.earlyActive = false
			return rec
		}
		// The change_cipher_spec for middleboxes comes before the early data
		// when there is some (RFC 8446 appendix D.4).
		h.sentCCS = true
		rec = append(rec, recordTypeChangeCipherSpec, 3, 3, 0, 1, 1)
	}
	return rec
}

// suite13Of looks up a TLS 1.3 suite by its ID.
func (h *clientHS) suite13Of(id uint16) suite {
	s, _ := h.suite13(id)
	return s
}

// earlySecret is the secret early data is protected under: it covers the
// ClientHello alone (RFC 8446 section 7.1).
func (h *clientHS) earlySecret() ([]byte, error) {
	f := h.sessionHash()
	early, err := hkdf.Extract(f, h.session.psk, nil)
	if err != nil {
		return nil, err
	}
	secret, err := deriveSecret(f, early, "c e traffic", hashBytes(f, h.transcript))
	if err == nil {
		h.keyLog("CLIENT_EARLY_TRAFFIC_SECRET", secret)
	}
	return secret, err
}

func appendRecord(dst []byte, typ byte, version uint16, body []byte) []byte {
	dst = append(dst, typ, byte(version>>8), byte(version), byte(len(body)>>8), byte(len(body)))
	return append(dst, body...)
}

// appendRecords appends msg in the clear, split across records if it is
// longer than one: a ClientHello that offers a session carries its ticket,
// which carries the certificates the client sent when it made it.
func appendRecords(dst []byte, typ byte, version uint16, msg []byte) []byte {
	for len(msg) > 0 {
		n := min(len(msg), maxPlaintext)
		dst = appendRecord(dst, typ, version, msg[:n])
		msg = msg[n:]
	}
	return dst
}

// builder appends length-prefixed fields.
type builder struct{ b []byte }

func (b *builder) u8(v uint8)   { b.b = append(b.b, v) }
func (b *builder) u16(v uint16) { b.b = binary.BigEndian.AppendUint16(b.b, v) }
func (b *builder) u24(v int)    { b.b = append(b.b, byte(v>>16), byte(v>>8), byte(v)) }
func (b *builder) u32(v uint32) { b.b = binary.BigEndian.AppendUint32(b.b, v) }
func (b *builder) raw(p []byte) { b.b = append(b.b, p...) }

// vec24 appends a field of up to 2^24-1 bytes, whose contents fn writes.
func (b *builder) vec24(fn func(*builder)) {
	at := len(b.b)
	b.b = append(b.b, 0, 0, 0)
	fn(b)
	n := len(b.b) - at - 3
	b.b[at], b.b[at+1], b.b[at+2] = byte(n>>16), byte(n>>8), byte(n)
}

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
	b.vec8(func(b *builder) { b.raw(h.sessionID) })
	b.vec16(func(b *builder) {
		if h.offers(stdtls.VersionTLS13) {
			for _, id := range h.suites {
				b.u16(id)
			}
		}
		if h.offers(stdtls.VersionTLS12) {
			for _, id := range h.suites12 {
				b.u16(id)
			}
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
				algs := clientSignatureAlgs
				if h.offers(stdtls.VersionTLS12) {
					algs = clientSignatureAlgs12
				}
				for _, alg := range algs {
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
		if h.offers(stdtls.VersionTLS12) {
			// uncompressed points only, which is all that is parsed
			b.u16(extECPointFormats)
			b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.u8(0) }) })
			b.u16(extExtendedMasterSec)
			b.vec16(func(b *builder) {})
			b.u16(extRenegotiationInfo)
			b.vec16(func(b *builder) { b.vec8(func(b *builder) {}) })
		}
		if h.offers(stdtls.VersionTLS13) {
			b.u16(extSupportedVersions)
			b.vec16(func(b *builder) {
				b.vec8(func(b *builder) {
					for _, v := range h.versions {
						b.u16(v)
					}
				})
			})
			b.u16(extKeyShare)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					b.u16(uint16(h.group))
					b.vec16(func(b *builder) { b.raw(h.key.PublicKey().Bytes()) })
				})
			})
		}
		if h.quic != nil {
			b.u16(extQUICParams)
			b.vec16(func(b *builder) { b.raw(h.quic.localParams) })
		}
		if h.cookie != nil {
			b.u16(extCookie)
			b.vec16(func(b *builder) { b.vec16(func(b *builder) { b.raw(h.cookie) }) })
		}
		if h.sessions != nil && h.offers(stdtls.VersionTLS13) {
			// Tickets are for clients that say they use them.
			b.u16(extPSKModes)
			b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.u8(1) }) }) // psk_dhe_ke
		}
		if h.earlyActive {
			b.u16(extEarlyData)
			b.vec16(func(b *builder) {})
		}
		if sess := h.session; sess != nil {
			// The pre_shared_key extension is the last, and ends in the
			// binder, whose place is filled once the message is known.
			size := h.sessionHash()().Size()
			age := uint32(h.now().Sub(sess.received)/time.Millisecond) + sess.ageAdd
			b.u16(extPreSharedKey)
			b.vec16(func(b *builder) {
				b.vec16(func(b *builder) {
					b.vec16(func(b *builder) { b.raw(sess.ticket) })
					b.u32(age)
				})
				b.vec16(func(b *builder) { b.vec8(func(b *builder) { b.raw(make([]byte, size)) }) })
			})
		}
	})
	msg := b.b
	msg[1], msg[2], msg[3] = byte((len(msg)-4)>>16), byte((len(msg)-4)>>8), byte(len(msg)-4)
	if h.session != nil {
		f := h.sessionHash()
		size := f().Size()
		binder, err := h.pskBinder(f, msg[:len(msg)-(2+1+size)])
		if err == nil {
			copy(msg[len(msg)-size:], binder)
		}
	}
	return msg
}

func (h *clientHS) now() time.Time {
	if h.config.Time != nil {
		return h.config.Time()
	}
	return time.Now()
}

// sessionHash is the hash of the offered session's suite.
func (h *clientHS) sessionHash() func() hash.Hash {
	s, _ := quicSuite(h.session.suite)
	return s.hash
}

// pskBinder is the binder of the offered session over a ClientHello up to
// its binders (RFC 8446 section 4.2.11.2), after whatever of the handshake
// came before it.
func (h *clientHS) pskBinder(f func() hash.Hash, truncated []byte) ([]byte, error) {
	early, err := hkdf.Extract(f, h.session.psk, nil)
	if err != nil {
		return nil, err
	}
	binderKey, err := deriveSecret(f, early, "res binder", hashBytes(f, nil))
	if err != nil {
		return nil, err
	}
	key, err := expandLabelContext(f, binderKey, "finished", nil, f().Size())
	if err != nil {
		return nil, err
	}
	mac := hmac.New(f, key)
	mac.Write(hashBytes(f, append(bytes.Clone(h.transcript), truncated...)))
	return mac.Sum(nil), nil
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

func (r *reader) u32() uint32 {
	if p := r.take(4); p != nil {
		return binary.BigEndian.Uint32(p)
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
// in is overwritten, since records are decrypted where they lie. While
// GetClientCertificate, or a check of the server's (see checkThen), has not
// answered, it consumes nothing; once it has, a call goes on from where the
// handshake stopped, with more of in or none.
func (h *clientHS) process(in []byte) (consumed int, out []byte, done bool, err *hsError) {
	if waiting, herr := h.proceed(&out); waiting || herr != nil {
		return 0, out, false, herr
	}
	for h.state != hsDone && !h.waiting() && len(in)-consumed >= recordHeaderLen {
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
	return consumed, out, h.state == hsDone, nil
}

// record handles one record of the handshake.
func (h *clientHS) record(record []byte, out *[]byte) *hsError {
	typ, body := record[0], record[recordHeaderLen:]
	if record[1] != 3 {
		return fail(alertProtocolVersion, "unsupported record version %#x%02x", record[1], record[2])
	}
	tls12 := h.version == stdtls.VersionTLS12
	switch typ {
	case recordTypeChangeCipherSpec:
		if tls12 {
			return h.serverChangeCipherSpec(body)
		}
		// Sent by a server for middleboxes, and ignored (RFC 8446 section 5).
		h.ccsSeen++
		if len(body) != 1 || body[0] != 1 || h.ccsSeen > 2 {
			return fail(alertUnexpectedMessage, "unexpected change_cipher_spec")
		}
		return nil
	case recordTypeAlert:
		if h.rxOn && tls12 {
			inner, plain, alert, ok := h.rx.open(record)
			if !ok {
				return fail(alert, "bad record")
			}
			if inner != recordTypeAlert {
				return fail(alertUnexpectedMessage, "unexpected record type")
			}
			return remoteAlert(plain)
		}
		if h.rxOn {
			return fail(alertUnexpectedMessage, "unprotected alert after ServerHello")
		}
		return remoteAlert(body)
	case recordTypeHandshake:
		if h.rxOn && tls12 {
			inner, plain, alert, ok := h.rx.open(record)
			if !ok {
				return fail(alert, "bad record")
			}
			if inner != recordTypeHandshake {
				return fail(alertUnexpectedMessage, "unexpected record type")
			}
			return h.handshakeData(plain, out)
		}
		if h.rxOn {
			return fail(alertUnexpectedMessage, "unprotected handshake record after ServerHello")
		}
		return h.handshakeData(body, out)
	case recordTypeApplicationData:
		if !h.rxOn || tls12 {
			return fail(alertUnexpectedMessage, "protected record before it is expected")
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
	if body[0] == alertLevelWarning && body[1] == alertCloseNotify {
		return &hsError{err: io.ErrUnexpectedEOF}
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
		h.flightEnd = false
		if herr := h.message(msg, out); herr != nil {
			return herr
		}
		h.hsBuf = h.hsBuf[size:]
		if h.flightEnd && len(h.hsBuf) != 0 {
			// Keys change after a flight, or it is our turn, so no message
			// may run across the end.
			return fail(alertUnexpectedMessage, "handshake data after the last message of a flight")
		}
		if h.waiting() {
			// The messages behind it wait with it, and are handled once
			// it goes on.
			return nil
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
		return h.certificate(msg, out)
	case h.state == hsExpectCertificateVerify && typ == hsCertificateVerify:
		return h.certificateVerify(msg)
	case h.state == hsExpectFinished && typ == hsFinished:
		return h.finished(msg, out)
	case h.state == hs12ExpectCertificate && typ == hsCertificate:
		return h.certificate12(msg, out)
	case h.state == hs12ExpectServerKeyExchange && typ == hsServerKeyExchange:
		return h.serverKeyExchange(msg)
	case h.state == hs12ExpectCertificateRequestOrDone && typ == hsCertificateRequest:
		return h.certificateRequest12(msg)
	case (h.state == hs12ExpectCertificateRequestOrDone || h.state == hs12ExpectServerHelloDone) && typ == hsServerHelloDone:
		return h.serverHelloDone(msg, out)
	case h.state == hs12ExpectCCS && typ == hsNewSessionTicket:
		// A server that sends a ticket nobody asked for; there is no
		// resumption to use it for.
		h.transcript = append(h.transcript, msg...)
		return nil
	case h.state == hs12ExpectFinished && typ == hsFinished:
		return h.finished12(msg, out)
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

// parseExtensions splits an extension block, refusing one extension twice.
func parseExtensions(block []byte) (map[uint16][]byte, *hsError) {
	r := newReader(block)
	exts := map[uint16][]byte{}
	for r.ok && len(r.b) > 0 {
		typ := r.u16()
		data := r.vec16()
		if !r.ok {
			break
		}
		if _, dup := exts[typ]; dup {
			return nil, fail(alertIllegalParameter, "duplicate extension %d", typ)
		}
		exts[typ] = data
	}
	if !r.ok {
		return nil, fail(alertDecodeError, "malformed extensions")
	}
	return exts, nil
}

// serverHello handles the ServerHello, or a HelloRetryRequest, which has its
// shape, and with it the choice of version.
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
	exts, herr := parseExtensions(extensions)
	if herr != nil {
		return herr
	}
	if compression != 0 {
		return fail(alertIllegalParameter, "server selected compression")
	}
	hrr := bytes.Equal(random, helloRetryRandom[:])
	if data, ok := exts[extSupportedVersions]; ok {
		d := newReader(data)
		if v := d.u16(); !d.empty() || v != stdtls.VersionTLS13 || !h.offers(stdtls.VersionTLS13) {
			return fail(alertProtocolVersion, "server selected a version this client does not offer")
		}
		return h.serverHello13(msg, version, random, sessionID, cipherSuite, hrr, exts, out)
	}
	if hrr || version != stdtls.VersionTLS12 || !h.offers(stdtls.VersionTLS12) {
		return fail(alertProtocolVersion, "server does not support a version this client offers")
	}
	return h.serverHello12(msg, random, sessionID, cipherSuite, exts)
}

// serverHello13 handles a TLS 1.3 ServerHello, or HelloRetryRequest.
func (h *clientHS) serverHello13(msg []byte, version uint16, random, sessionID []byte, cipherSuite uint16, hrr bool,
	exts map[uint16][]byte, out *[]byte) *hsError {
	if version != 0x0303 {
		return fail(alertProtocolVersion, "server selected a protocol this client does not offer")
	}
	if !bytes.Equal(sessionID, h.sessionID) {
		return fail(alertIllegalParameter, "server did not echo the session ID")
	}
	var (
		selected     uint16
		share        []byte
		cookie       []byte
		haveShare    bool
		haveSelected bool
		haveCookie   bool
		pskIndex     uint16
		havePSK      bool
	)
	for typ, data := range exts {
		d := newReader(data)
		switch typ {
		case extSupportedVersions:
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
			if !hrr {
				return fail(alertUnsupportedExtension, "cookie in a ServerHello")
			}
			cookie = d.vec16()
			haveCookie = d.empty() && len(cookie) > 0
		case extPreSharedKey:
			if hrr || h.session == nil {
				return fail(alertUnsupportedExtension, "server sent an unsolicited extension %d", typ)
			}
			pskIndex = d.u16()
			havePSK = d.empty()
			if !havePSK {
				return fail(alertDecodeError, "malformed ServerHello extension")
			}
		default:
			return fail(alertUnsupportedExtension, "server sent an unsolicited extension %d", typ)
		}
		if !d.ok {
			return fail(alertDecodeError, "malformed ServerHello extension")
		}
	}
	if !slices.Contains(h.suites, cipherSuite) {
		return fail(alertIllegalParameter, "server selected a cipher suite that was not offered")
	}
	s, ok := h.suite13(cipherSuite)
	if !ok {
		return fail(alertIllegalParameter, "unsupported cipher suite")
	}
	if hrr {
		_, shareExt := exts[extKeyShare]
		h.flightEnd = true
		return h.helloRetryRequest(msg, cipherSuite, s, selected, haveSelected, shareExt, cookie, haveCookie, out)
	}
	if h.retried && cipherSuite != h.suiteID {
		return fail(alertIllegalParameter, "server changed its cipher suite after a HelloRetryRequest")
	}
	h.suiteID, h.suite = cipherSuite, s
	if havePSK {
		if pskIndex != 0 {
			return fail(alertIllegalParameter, "server selected a pre-shared key that was not offered")
		}
		if h.sessionHash()().Size() != s.hash().Size() {
			return fail(alertIllegalParameter, "server selected a cipher suite whose hash differs from the session's")
		}
		h.resumed = true
		for _, der := range h.session.certs {
			if cert, err := x509.ParseCertificate(der); err == nil {
				h.certs = append(h.certs, cert)
			}
			h.rawCerts = append(h.rawCerts, der)
		}
		h.chains = h.session.chains
	} else if h.session != nil {
		// The server would rather start over, and takes no early data.
		h.session = nil
		h.rejectEarly()
	}
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
	h.version = stdtls.VersionTLS13
	h.transcript = append(h.transcript, msg...)
	h.flightEnd = true
	return h.handshakeKeys(shared, out)
}

// suite13 looks up a TLS 1.3 suite: those the record layer protects, or, for
// QUIC, any of the three, since only their hash is needed.
func (h *clientHS) suite13(id uint16) (suite, bool) {
	if h.quic != nil {
		return quicSuite(id)
	}
	return lookupSuite(stdtls.VersionTLS13, id)
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
	// A retried handshake takes no early data, and the session is offered
	// again only if its hash is the one the server chose.
	h.rejectEarly()
	if h.session != nil && h.sessionHash()().Size() != s.hash().Size() {
		h.session = nil
	}
	// The first ClientHello is replaced in the transcript by its hash.
	first := hashBytes(s.hash, h.transcript)
	h.transcript = append([]byte{hsMessageHash, 0, 0, byte(len(first))}, first...)
	h.transcript = append(h.transcript, msg...)
	second := h.clientHello()
	h.transcript = append(h.transcript, second...)
	if h.quic != nil {
		h.quic.write(stdtls.QUICEncryptionLevelInitial, second)
		return nil
	}
	h.sendCCS(out)
	*out = appendRecords(*out, recordTypeHandshake, 0x0303, second)
	return nil
}

// sendCCS sends the change_cipher_spec that makes a TLS 1.3 handshake look
// like a resumed TLS 1.2 one to a middlebox, once.
func (h *clientHS) sendCCS(out *[]byte) {
	if !h.sentCCS && h.quic == nil {
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
	earlySecret := zeros
	if h.resumed {
		earlySecret = h.session.psk
	}
	early, err := hkdf.Extract(f, earlySecret, nil)
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
	h.keyLog("CLIENT_HANDSHAKE_TRAFFIC_SECRET", h.clientHS)
	h.keyLog("SERVER_HANDSHAKE_TRAFFIC_SECRET", h.serverHS)
	if h.quic != nil {
		h.quic.setWrite(stdtls.QUICEncryptionLevelHandshake, h.suiteID, h.clientHS)
		h.quic.setRead(stdtls.QUICEncryptionLevelHandshake, h.suiteID, h.serverHS)
		h.state = hsExpectEncryptedExtensions
		return nil
	}
	if h.rx, err = newKeys13(h.suite, f, h.serverHS); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.tx, err = newKeys13(h.suite, f, h.clientHS); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.sendCCS(out)
	h.rxOn, h.txOn = true, true
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
		if typ == extQUICParams && h.quic != nil {
			h.peerParams = bytes.Clone(data)
		}
		if typ == extEarlyData {
			if !h.earlyActive || len(data) != 0 {
				return fail(alertUnsupportedExtension, "server accepted early data that was not offered")
			}
			h.accepted = true
		}
	}
	if h.earlyActive && !h.accepted {
		h.rejectEarly()
	} else if h.accepted && h.quic == nil && h.decided != nil {
		h.decided(true)
	}
	if h.quic != nil {
		// RFC 9001 sections 8.1 and 8.2.
		if h.protocol == "" && len(h.config.NextProtos) > 0 {
			return fail(alertNoApplicationProtocol, "server did not select an ALPN protocol")
		}
		if h.peerParams == nil {
			return fail(alertMissingExtension, "server did not send a quic_transport_parameters extension")
		}
		h.quic.peer(h.peerParams)
	}
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectCertificateOrRequest
	if h.resumed {
		// A resumed session has no certificate to send or verify.
		h.state = hsExpectFinished
	}
	return nil
}

// rejectEarly ends an offer of early data: the server has turned it away, or
// the handshake is one it cannot go with.
func (h *clientHS) rejectEarly() {
	if h.earlyActive {
		h.earlyActive = false
		if h.quic != nil {
			h.quic.events = append(h.quic.events, stdtls.QUICEvent{Kind: stdtls.QUICRejectedEarlyData})
		} else {
			h.earlyRefused = true
			if h.decided != nil {
				h.decided(false)
			}
		}
	}
	h.offerEarly = false
}

func (h *clientHS) certificateRequest(msg []byte) *hsError {
	if herr := h.parseCertificateRequest13(msg); herr != nil {
		return herr
	}
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectCertificate
	return nil
}

func (h *clientHS) certificate(msg []byte, out *[]byte) *hsError {
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
	h.transcript = append(h.transcript, msg...)
	h.state = hsExpectCertificateVerify
	return h.acceptCertificates(ders, out)
}

// acceptCertificates parses the server's chain, and has it verified unless
// the Config says not to, and VerifyPeerCertificate given its say (see
// checkThen); until they have, the handshake waits.
func (h *clientHS) acceptCertificates(ders [][]byte, out *[]byte) *hsError {
	for _, der := range ders {
		// A copy, since a parsed certificate keeps what it was parsed from,
		// and the messages are read where they arrived.
		raw := bytes.Clone(der)
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fail(alertBadCertificate, "failed to parse certificate from server: %v", err)
		}
		h.certs = append(h.certs, cert)
		h.rawCerts = append(h.rawCerts, raw)
	}
	if len(h.certs) == 0 {
		return fail(alertDecodeError, "server sent an empty certificate chain")
	}
	if h.config.InsecureSkipVerify && h.config.VerifyPeerCertificate == nil {
		return nil
	}
	return h.checkThen(h.verifyServer, nil, out)
}

// verifyServer verifies the server's chain unless the Config says not to, and
// gives VerifyPeerCertificate its say.
func (h *clientHS) verifyServer() *hsError {
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
	return nil
}

// signatureHash is the hash a TLS 1.3 signature scheme signs with, or zero
// for Ed25519, which signs the message itself.
func signatureHash(alg uint16) (crypto.Hash, bool) {
	switch alg {
	case 0x0403, 0x0804, 0x0401:
		return crypto.SHA256, true
	case 0x0503, 0x0805, 0x0501:
		return crypto.SHA384, true
	case 0x0603, 0x0806, 0x0601:
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
	if _, ok := signatureHash(alg); !ok || !slices.Contains(clientSignatureAlgs, alg) {
		return fail(alertIllegalParameter, "server signed with a scheme that was not offered")
	}
	// The signature covers the transcript up to the Certificate (RFC 8446
	// section 4.4.3).
	signed := certificateVerifyContent(true, h.transcriptHash())
	if herr := verify13(alg, h.certs[0].PublicKey, signed, signature, "server"); herr != nil {
		return herr
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
// handshake: it answers with ours, under the handshake keys, after the
// certificate the server asked for, and moves both directions to the
// application keys.
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
	if h.exporter, err = deriveSecret(f, h.master, "exp master", t); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.config.VerifyConnection == nil {
		return h.clientFlight(out, h.finish13)
	}
	// VerifyConnection has its say before the client's last flight, as with
	// crypto/tls, so that a connection it turns down gets no Finished, and a
	// QUIC one no application keys.
	h.flightEnd = true
	return h.checkThen(h.verifyConnection, func(out *[]byte) *hsError { return h.clientFlight(out, h.finish13) }, out)
}

// verifyConnection gives Config.VerifyConnection its say.
func (h *clientHS) verifyConnection() *hsError {
	if err := h.config.VerifyConnection(h.connectionState()); err != nil {
		return &hsError{alert: alertBadCertificate, err: err}
	}
	return nil
}

// waiting reports whether the handshake has set its rest aside until a
// callback has answered: GetClientCertificate, or a check of the server's.
func (h *clientHS) waiting() bool { return h.pick.waiting() || h.check.waiting() }

// proceed goes on with what the handshake set aside, if its callback has
// answered, and reports whether it still waits.
func (h *clientHS) proceed(out *[]byte) (bool, *hsError) {
	if h.check.waiting() {
		return h.check.proceed(out)
	}
	return h.pick.proceed(out)
}

// checkThen runs check, a check of the server's: the verification of its
// certificate, VerifyPeerCertificate or VerifyConnection, and then rest, if
// there is one, unless check failed. Verifying a chain takes a while, longer
// where the platform's verifier does it, and the callbacks are the user's and
// may block, so check runs where GetClientCertificate does (see auth.go): on a
// worker of the handshake pool, with the handshake set aside until it has
// returned, and the messages that came behind the one being handled handled
// after rest.
func (h *clientHS) checkThen(check func() *hsError, rest func(out *[]byte) *hsError, out *[]byte) *hsError {
	if _, ok := h.check.ask(func() (*stdtls.Certificate, error) {
		if herr := check(); herr != nil {
			return nil, herr
		}
		return nil, nil
	}); !ok {
		h.check.wait(func(out *[]byte) *hsError {
			herr := h.checked()
			if herr == nil && rest != nil {
				herr = rest(out)
			}
			if herr == nil && !h.waiting() {
				herr = h.handshakeData(nil, out)
			}
			return herr
		})
		return nil
	}
	if herr := h.checked(); herr != nil || rest == nil {
		return herr
	}
	return rest(out)
}

// checked is what the check answered, with the call made ready for the next.
func (h *clientHS) checked() *hsError {
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

// finish13 sends the client's last flight of TLS 1.3: its certificate if the
// server asked for one, and its Finished.
func (h *clientHS) finish13(out *[]byte) *hsError {
	if h.quic != nil {
		return h.finishQUIC(h.exporter)
	}
	f := h.suite.hash
	var err error
	if h.accepted && h.sealEarly != nil {
		// The early data ends with EndOfEarlyData under its keys, and the
		// Finished that follows covers it.
		eoed := []byte{hsEndOfEarlyData, 0, 0, 0}
		h.transcript = append(h.transcript, eoed...)
		rec, serr := h.sealEarly(eoed)
		if serr != nil {
			return fail(alertInternalError, "%v", serr)
		}
		*out = append(*out, rec...)
	}
	var flight []byte
	if h.certReq {
		// The certificate, or an empty Certificate to say there is none, and
		// the server decides whether it can go on.
		msgs, herr := h.certificateMessages13()
		if herr != nil {
			return herr
		}
		for _, m := range msgs {
			// A chain longer than a record is split across records.
			for len(m) > 0 {
				n := min(len(m), maxPlaintext)
				if flight, err = h.tx.seal(flight, recordTypeHandshake, m[:n], nil); err != nil {
					return fail(alertInternalError, "%v", err)
				}
				m = m[n:]
			}
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
	if h.sessions != nil {
		if h.resumption, err = deriveSecret(f, h.master, "res master", h.transcriptHash()); err != nil {
			return fail(alertInternalError, "%v", err)
		}
	}

	if h.rx, err = newKeys13(h.suite, f, h.appServer); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	if h.tx, err = newKeys13(h.suite, f, h.appClient); err != nil {
		return fail(alertInternalError, "%v", err)
	}
	h.keyLog("CLIENT_TRAFFIC_SECRET_0", h.appClient)
	h.keyLog("SERVER_TRAFFIC_SECRET_0", h.appServer)
	h.keyLog("EXPORTER_SECRET", h.exporter)
	h.state = hsDone
	h.flightEnd = true
	return nil
}

// connectionState is what ConnectionState reports for the connection.
func (h *clientHS) connectionState() stdtls.ConnectionState {
	return stdtls.ConnectionState{
		Version:                    h.version,
		HandshakeComplete:          true,
		DidResume:                  h.resumed,
		CipherSuite:                h.suiteID,
		NegotiatedProtocol:         h.protocol,
		NegotiatedProtocolIsMutual: h.protocol != "",
		ServerName:                 h.config.ServerName,
		PeerCertificates:           h.certs,
		VerifiedChains:             h.chains,
		TLSUnique:                  h.clientFinished,
	}
}

func (h *clientHS) recordKeys() (rx, tx *recordKeys) { return h.rx, h.tx }

// nativeHandshake is a handshake that runs without a worker, client or
// server: what the layer needs of it.
type nativeHandshake interface {
	process(in []byte) (consumed int, out []byte, done bool, err *hsError)
	alertRecord(alert uint8) []byte
	connectionState() stdtls.ConnectionState
	recordKeys() (rx, tx *recordKeys)
}

var (
	_ nativeHandshake = (*clientHS)(nil)
	_ nativeHandshake = (*serverHS)(nil)
)

// alertRecord is the record that tells the peer the handshake has failed with
// alert: under our keys once there are any.
func (h *clientHS) alertRecord(alert uint8) []byte {
	body := []byte{alertLevelError, alert}
	if !h.txOn || h.state == hsDone {
		return appendRecord(nil, recordTypeAlert, 0x0303, body)
	}
	record, err := h.tx.seal(nil, recordTypeAlert, body, nil)
	if err != nil {
		return nil
	}
	return record
}
