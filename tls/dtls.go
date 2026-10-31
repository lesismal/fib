package tls

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"time"
)

// DTLS 1.2 (RFC 6347) with the DTLS-SRTP extension (RFC 5764), for WebRTC,
// run by the TLS 1.2 handshakes of this package.
//
// DTLS is TLS 1.2 over datagrams that may be lost, duplicated, reordered or
// cut to a path's MTU. Its handshake messages are TLS 1.2's, with a sequence
// number and a fragment's place in the message added to their header so that
// a receiver can put them back together and in order, and are sent in
// flights that are sent again until the peer's next flight shows they
// arrived. Its records carry their epoch and sequence number, so that each
// can be opened by itself, and a window of the numbers seen turns away
// replays. A server answers a first ClientHello with a HelloVerifyRequest
// carrying a cookie, an HMAC of the hello under a secret of its own, and
// keeps nothing until the client sends the hello again with the cookie: an
// address that cannot receive cannot make a server do the work of a
// handshake.
//
// DTLSConn is sans-IO, as QUICConn is: it never reads or writes a socket,
// starts a goroutine or sleeps. The caller hands it each datagram that
// arrives with HandleDatagram, and it sends what it has to through the Output
// callback of its DTLSOptions, one datagram a call; the caller also asks
// NextTimeout when to call HandleTimeout, which sends a flight again. The
// handshake messages are made and checked by the TLS 1.2 code the TCP
// handshakes use (server12.go, client12.go, auth.go): the ServerHello to
// ServerHelloDone, the key exchange, the extended master secret (RFC 7627),
// certificates and their signatures, and the record keys; only the framing
// is DTLS's, here and in dtls_record.go and dtls_handshake.go.
//
// What it does: the ECDHE suites with AES-GCM and ChaCha20-Poly1305, ECDSA,
// Ed25519 and RSA certificates, X25519, P-256 and P-384, the
// renegotiation_info and extended_master_secret extensions, ALPN, use_srtp
// with the SRTP protection profiles of DTLSOptions, client certificates
// (which WebRTC requires of both sides), and keying material exporters (RFC
// 5705), from which SRTP takes its keys. What it leaves out: DTLS 1.0 and
// 1.3, CBC suites, session resumption, renegotiation (a HelloRequest or a
// second ClientHello is ignored), an SRTP MKI, and the certificate
// callbacks' running off the caller's
// goroutine: GetCertificate and GetClientCertificate, VerifyPeerCertificate
// and VerifyConnection are called from HandleDatagram.
//
// WebRTC peers authenticate each other by the fingerprints of their
// certificates, which their SDP carries (RFC 8122), and not by a chain to a
// root: a client sets InsecureSkipVerify and a server ClientAuth
// RequireAnyClientCert, and both check the peer's certificate against the
// fingerprint in VerifyPeerCertificate; see CertificateFingerprint and
// GenerateWebRTCCertificate.

// VersionDTLS12 is the version DTLS 1.2 is on the wire, the one
// ConnectionState reports.
const VersionDTLS12 = 0xfefd

// The SRTP protection profiles of the use_srtp extension (RFC 5764 section
// 4.1.2, RFC 7714 section 14.2).
const (
	SRTP_AES128_CM_HMAC_SHA1_80 uint16 = 0x0001
	SRTP_AES128_CM_HMAC_SHA1_32 uint16 = 0x0002
	SRTP_AEAD_AES_128_GCM       uint16 = 0x0007
	SRTP_AEAD_AES_256_GCM       uint16 = 0x0008
)

var (
	// ErrDTLSNotEstablished is what Write and ExportKeyingMaterial return
	// before the handshake is complete.
	ErrDTLSNotEstablished = errors.New("tls: DTLS handshake not complete")
	// ErrDTLSClosed is what a connection returns once Close has been called.
	ErrDTLSClosed = errors.New("tls: DTLS connection closed")
	// ErrDTLSRecordTooLarge is what Write returns for more than a record
	// carries.
	ErrDTLSRecordTooLarge = errors.New("tls: DTLS record too large")
	// ErrDTLSConfig is what the constructors return for a Config or
	// DTLSOptions they cannot work with.
	ErrDTLSConfig = errors.New("tls: invalid DTLS configuration")
)

const (
	// DefaultDTLSMTU is the largest datagram a flight is cut into when
	// DTLSOptions.MTU is zero: what WebRTC stacks use, which fits any path
	// that carries IPv6 with room for TURN's framing.
	DefaultDTLSMTU = 1200
	// minDTLSMTU leaves room for a fragment of some size after a record's and
	// a fragment's headers and an AEAD's expansion.
	minDTLSMTU = 256

	// dtlsInitialRTO and dtlsMaxRTO are the flight retransmission timer's
	// first and largest values (RFC 6347 section 4.2.4.1).
	dtlsInitialRTO = time.Second
	dtlsMaxRTO     = 60 * time.Second
)

// DTLSOptions are what a DTLSConn needs besides its Config.
type DTLSOptions struct {
	// Output sends one datagram to the peer. The buffer is only valid
	// during the call. It is required.
	Output func(datagram []byte)
	// OnData receives the application data of one record. The buffer is
	// only valid during the call.
	OnData func(data []byte)
	// OnHandshakeComplete, if set, is called once the handshake is
	// complete, from the HandleDatagram call that completed it.
	OnHandshakeComplete func()
	// MTU bounds the datagrams the handshake's flights are cut into;
	// DefaultDTLSMTU if zero. Each Write is one record in one datagram,
	// whose size is the caller's to keep under the path's MTU.
	MTU int
	// SRTPProfiles are the SRTP protection profiles to negotiate with the
	// use_srtp extension, in order of preference: a client offers them, and
	// a server picks the first its client offers too. None leaves the
	// extension out.
	SRTPProfiles []uint16
	// CookieSecret keys a server's HelloVerifyRequest cookies; a random one
	// is made if it is empty. Servers that share one accept each other's
	// cookies, which CookieContext then tells apart.
	CookieSecret []byte
	// CookieContext binds a server's cookies to the client, typically its
	// address, which RFC 6347 section 4.2.1 has the cookie cover.
	CookieContext []byte
	// InsecureSkipHelloVerify has a server answer a first ClientHello at
	// once, without the cookie exchange, saving a round trip where the
	// client's address has been verified already, as ICE does.
	InsecureSkipHelloVerify bool
}

type dtlsState uint8

const (
	// A client before Start, and a server before a ClientHello it accepts.
	dtlsIdle dtlsState = iota
	// The client: waiting for a HelloVerifyRequest or the ServerHello, then
	// for each of the server's messages up to the ServerHelloDone, then for
	// the server's Finished.
	dtlsAwaitServerHello
	dtlsAwaitCertificate
	dtlsAwaitServerKeyExchange
	dtlsAwaitCertificateRequestOrDone
	dtlsAwaitServerHelloDone
	dtlsAwaitServerFinished
	// The server: waiting for the client's certificate if it asked for one,
	// its key exchange, its CertificateVerify if it sent a certificate, and
	// its Finished.
	dtlsAwaitClientCertificate
	dtlsAwaitClientKeyExchange
	dtlsAwaitCertificateVerify
	dtlsAwaitClientFinished
	dtlsDone
)

// flightEntry is one message of a flight: a handshake message in its DTLS
// form, header included, with the epoch it is sent under, or a
// change_cipher_spec.
type flightEntry struct {
	msg   []byte
	epoch uint16
	ccs   bool
}

// DTLSConn is one side of a DTLS 1.2 connection. It is not safe for
// concurrent use: the caller serializes its calls, and the callbacks of its
// DTLSOptions are called from them.
type DTLSConn struct {
	config   *stdtls.Config
	opts     DTLSOptions
	mtu      int
	isClient bool
	state    dtlsState

	// The handshakes whose code this one runs: their transcripts are DTLS's,
	// and what they derive becomes the record keys here.
	cli *clientHS
	srv *serverHS

	// The record layer: the epoch written under and each epoch's next
	// sequence number, the keys of epoch 1 each way once there are any, a
	// replay window for epoch 1, and the records of epoch 1 that came
	// before their keys.
	txEpoch uint16
	txSeq   [2]uint64
	rx, tx  *recordKeys
	replay  replayWindow
	future  [][]byte

	// The handshake messages: the next message_seq each way, the messages of
	// the peer being put together, and the message_seq of the last message
	// of the peer's flight before the one awaited, whose coming again says
	// that ours was lost.
	txMsgSeq uint16
	rxMsgSeq uint16
	partial  []*dtlsMessage
	peerLast int

	// The last flight sent, the retransmission timer, and whether the flight
	// has been sent again for the datagram being handled.
	flight   []flightEntry
	timerOn  bool
	deadline time.Time
	rto      time.Duration
	resent   bool

	// A server's cookie secret and the ClientHello it is putting together,
	// and a client's cookie and how many HelloVerifyRequests it has
	// answered.
	cookieSecret []byte
	hello        *dtlsMessage
	cookie       []byte
	verifies     int

	srtpProfile uint16
	srtpOK      bool
	// early is application data that came before the handshake was
	// complete, after the keys it was sealed under.
	early [][]byte

	err error
	// out and frag are scratch space for the datagrams sent, wbuf for Write.
	out  []byte
	frag []byte
	wbuf []byte
}

// NewDTLSServer returns the server side of a DTLS connection with one client.
// It needs a certificate in config, Certificates or GetCertificate; set
// ClientAuth to RequireAnyClientCert and VerifyPeerCertificate to check
// WebRTC peers. It keeps no state for a client until the client has answered
// its HelloVerifyRequest, which costs it an HMAC and a datagram.
func NewDTLSServer(config *stdtls.Config, opts DTLSOptions) (*DTLSConn, error) {
	if config == nil || len(config.Certificates) == 0 && config.GetCertificate == nil || config.GetConfigForClient != nil {
		return nil, fmt.Errorf("%w: a server needs Certificates or GetCertificate, and no GetConfigForClient", ErrDTLSConfig)
	}
	for i := range config.Certificates {
		if herr := usableCertificate(&config.Certificates[i]); herr != nil {
			return nil, fmt.Errorf("%w: %v", ErrDTLSConfig, herr.err)
		}
	}
	c, err := newDTLSConn(config, opts)
	if err != nil {
		return nil, err
	}
	c.cookieSecret = opts.CookieSecret
	return c, nil
}

// NewDTLSClient returns the client side of a DTLS connection. The Config needs
// ServerName or InsecureSkipVerify, as for TLS; WebRTC peers set
// InsecureSkipVerify and check the server's certificate in
// VerifyPeerCertificate. Start sends its first flight.
func NewDTLSClient(config *stdtls.Config, opts DTLSOptions) (*DTLSConn, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: nil Config", ErrDTLSConfig)
	}
	c, err := newDTLSConn(config, opts)
	if err != nil {
		return nil, err
	}
	c.isClient = true
	if c.cli, err = newClientHSFor(config, nil); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDTLSConfig, err)
	}
	c.cli.version = stdtls.VersionTLS12
	c.cli.suites12 = dtlsSuitesFor(config)
	if len(c.cli.suites12) == 0 {
		return nil, fmt.Errorf("%w: no DTLS cipher suite in Config.CipherSuites", ErrDTLSConfig)
	}
	return c, nil
}

func newDTLSConn(config *stdtls.Config, opts DTLSOptions) (*DTLSConn, error) {
	if opts.Output == nil {
		return nil, fmt.Errorf("%w: DTLSOptions.Output is required", ErrDTLSConfig)
	}
	mtu := opts.MTU
	if mtu == 0 {
		mtu = DefaultDTLSMTU
	}
	if mtu < minDTLSMTU {
		return nil, fmt.Errorf("%w: MTU %d is below %d", ErrDTLSConfig, mtu, minDTLSMTU)
	}
	for _, p := range opts.SRTPProfiles {
		if p == 0 || p == 0xffff {
			return nil, fmt.Errorf("%w: SRTP profile %#04x", ErrDTLSConfig, p)
		}
	}
	return &DTLSConn{config: config, opts: opts, mtu: mtu, peerLast: -1, rto: dtlsInitialRTO}, nil
}

// Start begins a client's handshake, sending its ClientHello. It does nothing
// for a server, whose client speaks first.
func (c *DTLSConn) Start(now time.Time) error {
	if c.err != nil {
		return c.err
	}
	if !c.isClient || c.state != dtlsIdle {
		return nil
	}
	c.state = dtlsAwaitServerHello
	c.sendClientHello(now)
	return nil
}

// HandleDatagram takes a datagram from the peer: the records in it are
// opened, and what they carry handled, which may send datagrams through
// Output and deliver application data to OnData. Records that do not parse,
// do not authenticate or were seen before are dropped, as RFC 6347 section
// 4.1.2.7 has it. The datagram is decrypted in place. It returns an error
// once the connection has failed, io.EOF once the peer has closed it, and
// ErrDTLSClosed once it has been closed.
func (c *DTLSConn) HandleDatagram(b []byte, now time.Time) error {
	if c.err != nil {
		return c.err
	}
	c.resent = false
	for len(b) >= dtlsRecordHeaderLen && c.err == nil {
		n := dtlsRecordHeaderLen + (int(b[11])<<8 | int(b[12]))
		if n > len(b) {
			break
		}
		c.handleRecord(b[:n], now)
		b = b[n:]
		if c.rx != nil && len(c.future) > 0 && c.err == nil {
			future := c.future
			c.future = nil
			for _, rec := range future {
				if c.err == nil {
					c.handleRecord(rec, now)
				}
			}
		}
	}
	return c.err
}

// NextTimeout reports when HandleTimeout is to be called next, if it is:
// while a flight waits for the peer's answer.
func (c *DTLSConn) NextTimeout() (time.Time, bool) {
	if !c.timerOn || c.err != nil {
		return time.Time{}, false
	}
	return c.deadline, true
}

// HandleTimeout sends the last flight again if its timer has expired, and
// doubles the timer, up to a minute (RFC 6347 section 4.2.4).
func (c *DTLSConn) HandleTimeout(now time.Time) {
	if !c.timerOn || c.err != nil || now.Before(c.deadline) {
		return
	}
	c.rto = min(2*c.rto, dtlsMaxRTO)
	c.sendFlight()
	c.deadline = now.Add(c.rto)
}

// Write sends b to the peer in one application_data record, in one datagram.
func (c *DTLSConn) Write(b []byte) error {
	if c.err != nil {
		return c.err
	}
	if c.state != dtlsDone {
		return ErrDTLSNotEstablished
	}
	if len(b) > maxPlaintext {
		return ErrDTLSRecordTooLarge
	}
	buf, err := c.appendRecord(c.wbuf[:0], 1, recordTypeApplicationData, b)
	if err != nil {
		return err
	}
	c.wbuf = buf
	c.opts.Output(buf)
	return nil
}

// Close sends the peer a close_notify alert and closes the connection.
func (c *DTLSConn) Close() error {
	if c.err != nil {
		return nil
	}
	c.sendAlert(alertLevelWarning, alertCloseNotify)
	c.err = ErrDTLSClosed
	c.timerOn = false
	return nil
}

// HandshakeComplete reports whether the handshake is complete.
func (c *DTLSConn) HandshakeComplete() bool { return c.state == dtlsDone }

// ConnectionState reports the connection's parameters once the handshake is
// complete, with Version VersionDTLS12, and an empty one before.
func (c *DTLSConn) ConnectionState() stdtls.ConnectionState {
	if c.state != dtlsDone {
		return stdtls.ConnectionState{}
	}
	var cs stdtls.ConnectionState
	if c.isClient {
		cs = c.cli.connectionState()
	} else {
		cs = c.srv.connectionState12()
	}
	cs.Version = VersionDTLS12
	return cs
}

// SelectedSRTPProfile reports the SRTP protection profile the handshake
// negotiated, if it did.
func (c *DTLSConn) SelectedSRTPProfile() (uint16, bool) {
	return c.srtpProfile, c.srtpOK && c.state == dtlsDone
}

// ExportKeyingMaterial returns length bytes of keying material derived from
// the master secret under label and, if it is not nil, context (RFC 5705),
// as crypto/tls's does for TLS 1.2. DTLS-SRTP takes its keys with the label
// "EXTRACTOR-dtls_srtp" and no context (RFC 5764 section 4.2). Unlike
// crypto/tls it does not refuse a handshake without the extended master
// secret: what that guards against needs resumption, which DTLSConn does
// not do.
func (c *DTLSConn) ExportKeyingMaterial(label string, context []byte, length int) ([]byte, error) {
	if c.state != dtlsDone {
		return nil, ErrDTLSNotEstablished
	}
	switch label {
	case "client finished", "server finished", "master secret", "key expansion", "extended master secret":
		return nil, fmt.Errorf("tls: reserved ExportKeyingMaterial label: %s", label)
	}
	if context != nil && len(context) >= 1<<16 {
		return nil, errors.New("tls: ExportKeyingMaterial context too long")
	}
	master, clientRandom, serverRandom, f := c.secrets()
	seed := make([]byte, 0, 64+2+len(context))
	seed = append(append(seed, clientRandom...), serverRandom...)
	if context != nil {
		seed = append(seed, byte(len(context)>>8), byte(len(context)))
		seed = append(seed, context...)
	}
	return prf12(f, master, label, seed, length), nil
}

// secrets are the master secret, the randoms and the PRF hash.
func (c *DTLSConn) secrets() (master, clientRandom, serverRandom []byte, f func() hash.Hash) {
	if c.isClient {
		return c.cli.master, c.cli.random[:], c.cli.serverRandom[:], c.cli.suite.hash
	}
	return c.srv.master, c.srv.ch.random, c.srv.random[:], c.srv.suite.hash
}

// fail ends the connection with herr, sending the peer its alert if it has
// one.
func (c *DTLSConn) fail(herr *hsError) {
	if c.err != nil {
		return
	}
	if herr.alert != 0 {
		c.sendAlert(alertLevelError, herr.alert)
	}
	c.err = herr
	c.timerOn = false
}

// sendAlert sends an alert under the epoch written in.
func (c *DTLSConn) sendAlert(level, desc uint8) {
	buf, err := c.appendRecord(c.out[:0], c.txEpoch, recordTypeAlert, []byte{level, desc})
	if err != nil {
		return
	}
	c.out = buf
	c.opts.Output(buf)
}

// peerAlert handles an alert from the peer: close_notify closes the
// connection, a fatal alert fails it, and a warning is ignored.
func (c *DTLSConn) peerAlert(body []byte) {
	if len(body) != 2 {
		return
	}
	if body[1] == alertCloseNotify {
		c.err = io.EOF
		c.timerOn = false
		return
	}
	if body[0] == alertLevelError {
		c.err = &hsError{err: &net.OpError{Op: "remote error", Err: stdtls.AlertError(body[1])}}
		c.timerOn = false
	}
}

// cookieFor is the cookie a server expects in a ClientHello whose body is
// before and after the cookie: an HMAC of them and CookieContext.
func (c *DTLSConn) cookieFor(before, after []byte) []byte {
	if c.cookieSecret == nil {
		c.cookieSecret = make([]byte, 32)
		if _, err := io.ReadFull(c.rander(), c.cookieSecret); err != nil {
			c.cookieSecret = nil
			return nil
		}
	}
	mac := hmac.New(sha256.New, c.cookieSecret)
	mac.Write(c.opts.CookieContext)
	mac.Write(before)
	mac.Write(after)
	return mac.Sum(nil)
}

func (c *DTLSConn) rander() io.Reader {
	if c.config.Rand != nil {
		return c.config.Rand
	}
	return rand.Reader
}
