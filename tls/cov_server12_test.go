//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"reflect"
	"slices"
	"testing"
	"time"
)

// The refusals of the TLS 1.2 server handshake (server12.go) and of the
// client authentication both versions share (auth.go) that the handshakes
// with well-behaved peers never reach.

// covS12Hello is the ClientHello of a TLS 1.2 client of pool, as a record and
// as the parsed message.
func covS12Hello(t *testing.T, pool *x509.CertPool) (record []byte, ch *clientHelloMsg) {
	t.Helper()
	record = newCli12(t, pool, nil).hello()
	ch, ok := parseClientHello(record[recordHeaderLen:])
	if !ok {
		t.Fatal("the client's hello does not parse")
	}
	return record, ch
}

// covS12Started is a server handshake of config that has answered a TLS 1.2
// client's hello with a full handshake, and waits for its key exchange.
func covS12Started(t *testing.T, config *stdtls.Config, pool *x509.CertPool) *serverHS {
	t.Helper()
	hello, _ := covS12Hello(t, pool)
	srv := newSrvHS(config, nil)
	if _, _, _, herr := srv.process(hello); herr != nil {
		t.Fatalf("server: %v", herr)
	}
	if srv.version != stdtls.VersionTLS12 || srv.state != srv12AwaitKeyExchange {
		t.Fatalf("version %x, state %d", srv.version, srv.state)
	}
	return srv
}

// covS12Feed hands srv records of typ, one each, and returns its error.
func covS12Feed(srv *serverHS, typ byte, bodies ...[]byte) *hsError {
	var in []byte
	for _, b := range bodies {
		in = appendRecord(in, typ, 0x0303, b)
	}
	_, _, _, herr := srv.process(in)
	return herr
}

func covS12CKE(public []byte) []byte {
	var b builder
	b.vec8(func(b *builder) { b.raw(public) })
	return hsMsg(hsClientKeyExchange, b.b)
}

// covS12KeyExchanged is covS12Started past a valid ClientKeyExchange and the
// client's change_cipher_spec, with the keys the client would now protect
// its records with.
func covS12KeyExchanged(t *testing.T, config *stdtls.Config, pool *x509.CertPool) (*serverHS, *recordKeys) {
	t.Helper()
	srv := covS12Started(t, config, pool)
	key, err := curveOf(srv.group).GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if herr := covS12Feed(srv, recordTypeHandshake, covS12CKE(key.PublicKey().Bytes())); herr != nil {
		t.Fatalf("key exchange: %v", herr)
	}
	if srv.state != srv12AwaitCCS {
		t.Fatalf("state %d after the key exchange", srv.state)
	}
	if herr := covS12Feed(srv, recordTypeChangeCipherSpec, []byte{1}); herr != nil {
		t.Fatalf("change_cipher_spec: %v", herr)
	}
	client, _, err := newKeysFromMaster(stdtls.VersionTLS12, srv.suite, srv.master, srv.ch.random, srv.random[:])
	if err != nil {
		t.Fatal(err)
	}
	return srv, client
}

func covS12Remote(t *testing.T, name string, herr *hsError, alert stdtls.AlertError) {
	t.Helper()
	var op *net.OpError
	if herr == nil || herr.alert != 0 || !errors.As(herr.err, &op) || op.Err != alert {
		t.Errorf("%s: %v, want the peer's alert %d", name, herr, alert)
	}
}

// plan12 serves no client that lacks what a TLS 1.2 handshake needs: the null
// compression, a signature scheme the certificate signs with, the
// uncompressed point format if it lists any, and a group in common.
func TestCovS12Plan(t *testing.T) {
	config, pool := ecdsaServer(t)
	_, base := covS12Hello(t, pool)
	if p, herr := newSrvHS(config, nil).plan12(base); p == nil || herr != nil {
		t.Fatalf("the unchanged hello is not served: %v", herr)
	}
	for _, tc := range []struct {
		name   string
		change func(*clientHelloMsg)
		groups []stdtls.CurveID
	}{
		{"no null compression", func(ch *clientHelloMsg) { ch.compression = []byte{1} }, nil},
		{"no signature schemes", func(ch *clientHelloMsg) { ch.sigAlgs = nil }, nil},
		{"no groups", func(ch *clientHelloMsg) { ch.groups = nil }, nil},
		{"compressed points only", func(ch *clientHelloMsg) { ch.havePoints, ch.pointFormat = true, []byte{1} }, nil},
		{"no scheme the ECDSA key signs with", func(ch *clientHelloMsg) { ch.sigAlgs = []uint16{0x0807, 0x0804} }, nil},
		{"no group in common", func(ch *clientHelloMsg) { ch.groups = []stdtls.CurveID{stdtls.X25519} },
			[]stdtls.CurveID{stdtls.CurveP384}},
	} {
		ch := *base
		tc.change(&ch)
		h := newSrvHS(config, nil)
		if tc.groups != nil {
			h.groups = tc.groups
		}
		if p, herr := h.plan12(&ch); p != nil || herr != nil {
			t.Errorf("%s: plan %+v, error %v", tc.name, p, herr)
		}
	}
}

// A renegotiation_info that is not empty is an attempt at a renegotiation,
// which a first handshake cannot be.
func TestCovS12UnsafeRenegotiation(t *testing.T) {
	config, pool := ecdsaServer(t)
	record, ch := covS12Hello(t, pool)
	ch.exts[extRenegotiationInfo] = []byte{1, 0}
	var out []byte
	wantAlert(t, "renegotiation", newSrvHS(config, nil).clientHello12(ch, record[recordHeaderLen:], &out), alertHandshakeFailure)
	if len(out) != 0 {
		t.Fatalf("%d bytes sent", len(out))
	}
}

// resume12 takes no ticket for a suite TLS 1.2 has no keys for, nor one with
// client certificates that do not parse.
func TestCovS12ResumeRefusals(t *testing.T) {
	config, pool := ecdsaServer(t)
	_, ch := covS12Hello(t, pool)
	tickets := newTicketKeys(&stdtls.Config{})
	now := time.Now()
	ticket := func(s ticketState12) []byte {
		s.created, s.master, s.ems, s.server = now, bytes.Repeat([]byte{7}, 48), ch.ems, ch.serverName
		b, ok := tickets.seal12(s, now)
		if !ok {
			t.Fatal("the ticket was not sealed")
		}
		return b
	}
	for _, tc := range []struct {
		name  string
		state ticketState12
		ok    bool
	}{
		{"a good ticket", ticketState12{suite: stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}, true},
		{"a TLS 1.3 suite", ticketState12{suite: stdtls.TLS_AES_128_GCM_SHA256}, false},
		{"a certificate that does not parse", ticketState12{suite: stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			certs: [][]byte{{1, 2, 3}}}, false},
	} {
		h := newSrvHS(config, tickets)
		hello := *ch
		hello.ticket = ticket(tc.state)
		hello.suites = append(slices.Clone(ch.suites), stdtls.TLS_AES_128_GCM_SHA256)
		state, ok := h.resume12(&hello)
		if ok != tc.ok || (ok && state.suite != tc.state.suite) {
			t.Errorf("%s: resumed %v with suite %x", tc.name, ok, state.suite)
		}
	}
}

// The ServerHello of a full handshake answers a client that asks for an OCSP
// staple and signed certificate timestamps with the certificate's, and a
// CertificateStatus follows the Certificate.
func TestCovS12StapleAndSCTs(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	cert.OCSPStaple = []byte("covS12 staple")
	cert.SignedCertificateTimestamps = [][]byte{[]byte("covS12 sct one"), []byte("covS12 sct two")}
	config := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
	client := clientFor(pool, func(c *stdtls.Config) { c.MinVersion, c.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12 })
	state, clientErr, serverErr := pumpServer(t, newSrvHS(config, nil), client, 1<<14)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("client %v, server %v", clientErr, serverErr)
	}
	if state.Version != stdtls.VersionTLS12 {
		t.Fatalf("version %x", state.Version)
	}
	if !bytes.Equal(state.OCSPResponse, cert.OCSPStaple) {
		t.Errorf("OCSP response %q", state.OCSPResponse)
	}
	if !reflect.DeepEqual(state.SignedCertificateTimestamps, cert.SignedCertificateTimestamps) {
		t.Errorf("SCTs %q", state.SignedCertificateTimestamps)
	}
}

// A key that fails to sign the ServerKeyExchange fails the handshake.
func TestCovS12SignFails(t *testing.T) {
	config, pool := ecdsaServer(t)
	config.Certificates[0].PrivateKey = failingSigner{config.Certificates[0].PrivateKey.(crypto.Signer)}
	hello, _ := covS12Hello(t, pool)
	_, _, _, herr := newSrvHS(config, nil).process(hello)
	wantAlert(t, "signing", herr, alertInternalError)
}

// A Config.Rand that runs out once the server random is read fails the
// ECDHE key, where the runtime lets a custom Rand be used for it.
func TestCovS12KeyGenerationFails(t *testing.T) {
	t.Setenv("GODEBUG", "cryptocustomrand=1")
	config, pool := ecdsaServer(t)
	config.Rand = &shortRand{n: 32}
	hello, _ := covS12Hello(t, pool)
	srv := newSrvHS(config, nil)
	_, out, _, herr := srv.process(hello)
	wantAlert(t, "key generation", herr, alertInternalError)
	if len(out) != 0 {
		t.Fatalf("%d bytes sent", len(out))
	}
}

// A ClientKeyExchange that is malformed or carries a share that is not a
// point, or one that makes no shared secret, is refused.
func TestCovS12ClientKeyExchange(t *testing.T) {
	config, pool := ecdsaServer(t)
	for _, tc := range []struct {
		name  string
		msg   []byte
		alert uint8
	}{
		{"trailing data", hsMsg(hsClientKeyExchange, append([]byte{32}, make([]byte, 33)...)), alertDecodeError},
		{"a short share", covS12CKE([]byte{1, 2, 3, 4, 5}), alertIllegalParameter},
		{"a low-order point", covS12CKE(make([]byte, 32)), alertIllegalParameter},
	} {
		srv := covS12Started(t, config, pool)
		if srv.group != stdtls.X25519 {
			t.Fatalf("group %v", srv.group)
		}
		wantAlert(t, tc.name, covS12Feed(srv, recordTypeHandshake, tc.msg), tc.alert)
		if srv.master != nil {
			t.Errorf("%s: a master secret was made", tc.name)
		}
	}
}

// Records out of place in a TLS 1.2 handshake: a change_cipher_spec before
// the key exchange, an alert in the clear, application data, a protected
// record that does not open, a protected alert, and a Finished that is wrong.
func TestCovS12Records(t *testing.T) {
	config, pool := ecdsaServer(t)

	srv := covS12Started(t, config, pool)
	wantAlert(t, "early change_cipher_spec", covS12Feed(srv, recordTypeChangeCipherSpec, []byte{1}), alertUnexpectedMessage)

	srv = covS12Started(t, config, pool)
	covS12Remote(t, "alert in the clear", covS12Feed(srv, recordTypeAlert, []byte{2, 42}), 42)

	srv = covS12Started(t, config, pool)
	wantAlert(t, "application data", covS12Feed(srv, recordTypeApplicationData, []byte("data")), alertUnexpectedMessage)

	srv, client := covS12KeyExchanged(t, config, pool)
	finished := append([]byte{hsFinished, 0, 0, 12}, make([]byte, 12)...)
	record, err := client.seal(nil, recordTypeHandshake, finished, nil)
	if err != nil {
		t.Fatal(err)
	}
	record[len(record)-1] ^= 1
	_, _, _, herr := srv.process(record)
	wantAlert(t, "a record that does not open", herr, alertBadRecordMAC)

	srv, client = covS12KeyExchanged(t, config, pool)
	record, err = client.seal(nil, recordTypeAlert, []byte{2, 40}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, herr = srv.process(record)
	covS12Remote(t, "protected alert", herr, 40)

	srv, client = covS12KeyExchanged(t, config, pool)
	if record, err = client.seal(nil, recordTypeHandshake, finished, nil); err != nil {
		t.Fatal(err)
	}
	_, out, _, herr := srv.process(record)
	wantAlert(t, "a wrong Finished", herr, alertDecryptError)
	if len(out) != 0 || srv.state == srvDone {
		t.Errorf("a wrong Finished: %d bytes sent, state %d", len(out), srv.state)
	}
}

// VerifyConnection is asked about a TLS 1.2 handshake once the client's
// Finished is in, and what it refuses is refused with its error.
func TestCovS12VerifyConnection(t *testing.T) {
	config, pool := ecdsaServer(t)
	refused := errors.New("covS12 refused")
	var asked stdtls.ConnectionState
	config.VerifyConnection = func(cs stdtls.ConnectionState) error {
		asked = cs
		return refused
	}
	cli := newCli12(t, pool, nil)
	cliErr, srvErr := converse(t, cli, newSrvHS(config, nil), cli.hello())
	if cliErr != nil {
		t.Fatalf("client: %v", cliErr)
	}
	wantAlert(t, "VerifyConnection", srvErr, alertBadCertificate)
	if srvErr != nil && !errors.Is(srvErr.err, refused) {
		t.Errorf("error %v", srvErr.err)
	}
	if asked.Version != stdtls.VersionTLS12 || !asked.HandshakeComplete || asked.NegotiatedProtocol != "h2" {
		t.Errorf("VerifyConnection saw %+v", asked)
	}
}

// ask runs a callback once: while a worker has it, asking again waits, and
// with no worker to take it the answer is that there is none.
func TestCovS12CertCall(t *testing.T) {
	calls := 0
	fn := func() (*stdtls.Certificate, error) { calls++; return nil, nil }
	var later func()
	c := &certCall{async: func(call func()) bool { later = call; return true }}
	for i := 0; i < 2; i++ {
		if a, ok := c.ask(fn); a != nil || ok {
			t.Fatalf("ask %d: %v, %v before the answer", i, a, ok)
		}
	}
	later()
	if a, ok := c.ask(fn); !ok || a == nil || a.err != nil || calls != 1 {
		t.Fatalf("after the answer: %+v, %v, %d calls", a, ok, calls)
	}

	c = &certCall{async: func(func()) bool { return false }}
	if a, ok := c.ask(fn); !ok || a == nil || a.err != errNoHandshakeWorker || calls != 1 {
		t.Fatalf("without a worker: %+v, %v, %d calls", a, ok, calls)
	}
}

// ctxOffset finds no context in a type without a ctx field of that type.
func TestCovS12CtxOffset(t *testing.T) {
	type otherCtx struct {
		a   int64
		ctx interface{ Done() <-chan struct{} }
	}
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct{ a int }](), reflect.TypeFor[struct{ ctx int }](), reflect.TypeFor[otherCtx](),
	} {
		if off := ctxOffset(typ); off != ^uintptr(0) {
			t.Errorf("%v: offset %d", typ, off)
		}
	}
}

// clientCAs lists none without ClientCAs, and only as many names as fit a
// CertificateRequest.
func TestCovS12ClientCAs(t *testing.T) {
	h := newSrvHS(&stdtls.Config{ClientAuth: stdtls.RequestClientCert}, nil)
	if cas := h.clientCAs(); cas != nil {
		t.Fatalf("%d names without ClientCAs", len(cas))
	}
	pool := x509.NewCertPool()
	for i := 0; i < 3; i++ {
		pool.AddCert(&x509.Certificate{Raw: []byte{byte(i)}, RawSubject: bytes.Repeat([]byte{byte('a' + i)}, 30000)})
	}
	h = newSrvHS(&stdtls.Config{ClientAuth: stdtls.RequestClientCert, ClientCAs: pool}, nil)
	cas := h.clientCAs()
	if len(cas) != 2 || cas[0][0] != 'a' || cas[1][0] != 'b' {
		t.Fatalf("%d names", len(cas))
	}
	if msg := h.requestCertificate(true); len(msg) > 0x10000 {
		t.Fatalf("CertificateRequest of %d bytes", len(msg))
	}
}

// covS12Cert makes a certificate for pub signed by ca, valid for a day from
// an hour ago, for client authentication.
func covS12Cert(t *testing.T, ca *testCA, pub any) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial.Add(1)),
		Subject:      pkix.Name{CommonName: "covS12 client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(23 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// acceptClientCertificates refuses what crypto/tls refuses, with its alerts.
func TestCovS12AcceptClientCertificates(t *testing.T) {
	ca := newTestCA(t, "covS12 CA")
	leaf := ca.issue(t, "ecdsa-p256", "client", true).Certificate[0]
	serverLeaf := ca.issue(t, "ecdsa-p256", "server", false).Certificate[0]
	huge := &rsa.PublicKey{N: new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), 8200), big.NewInt(1)), E: 65537}
	hugeRSA := covS12Cert(t, ca, huge)
	// x509 makes no certificate for an X25519 key, and parses one with no
	// key it knows: this is an Ed25519 one with the key's algorithm changed
	// to X25519, and its signature, which only verifying it would notice,
	// broken by that.
	ed := covS12Cert(t, ca, keyOf(t, "ed25519").Public())
	ed25519OID, x25519OID := []byte{6, 3, 0x2b, 0x65, 0x70}, []byte{6, 3, 0x2b, 0x65, 0x6e}
	if bytes.Count(ed, ed25519OID) != 1 {
		t.Fatal("the Ed25519 algorithm is not where it was expected")
	}
	unsupported := bytes.Replace(ed, ed25519OID, x25519OID, 1)
	if c, err := x509.ParseCertificate(unsupported); err != nil {
		t.Fatal(err)
	} else if c.PublicKey != nil {
		t.Fatalf("a key of type %T", c.PublicKey)
	}
	peerRefused := errors.New("covS12 peer refused")

	for _, tc := range []struct {
		name   string
		auth   stdtls.ClientAuthType
		ders   [][]byte
		config func(*stdtls.Config)
		alert  uint8
	}{
		{"not a certificate", stdtls.RequireAnyClientCert, [][]byte{{1, 2, 3}}, nil, alertDecodeError},
		{"an RSA key too large", stdtls.RequireAnyClientCert, [][]byte{hugeRSA}, nil, alertBadCertificate},
		{"expired", stdtls.RequireAndVerifyClientCert, [][]byte{leaf},
			func(c *stdtls.Config) { c.Time = func() time.Time { return time.Now().Add(48 * time.Hour) } }, alertCertificateExpired},
		{"for servers", stdtls.RequireAndVerifyClientCert, [][]byte{serverLeaf}, nil, alertBadCertificate},
		{"an X25519 key", stdtls.RequireAnyClientCert, [][]byte{unsupported}, nil, alertUnsupportedCertificate},
		{"VerifyPeerCertificate refuses", stdtls.RequireAnyClientCert, [][]byte{leaf},
			func(c *stdtls.Config) {
				c.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error { return peerRefused }
			}, alertBadCertificate},
		{"with the chain sent", stdtls.VerifyClientCertIfGiven, [][]byte{leaf, ca.cert.Raw}, nil, 0},
	} {
		config := &stdtls.Config{ClientAuth: tc.auth, ClientCAs: ca.pool}
		if tc.config != nil {
			tc.config(config)
		}
		h := newSrvHS(config, nil)
		h.version = stdtls.VersionTLS12
		herr := h.acceptClientCertificates(tc.ders)
		if tc.alert == 0 {
			if herr != nil || len(h.peerCerts) != 2 || len(h.peerChains) != 1 || len(h.peerChains[0]) != 2 {
				t.Errorf("%s: %v, %d certificates, chains %v", tc.name, herr, len(h.peerCerts), h.peerChains)
			}
			continue
		}
		wantAlert(t, tc.name, herr, tc.alert)
		if tc.name == "VerifyPeerCertificate refuses" && herr != nil && !errors.Is(herr.err, peerRefused) {
			t.Errorf("%s: %v", tc.name, herr.err)
		}
		if h.peerCerts != nil {
			t.Errorf("%s: certificates kept", tc.name)
		}
	}
}

// sessionPeer resumes no session whose certificates do not parse, have
// expired, were not verified when the Config verifies them, or are not asked
// for; one the Config verifies is verified again, so a root taken out of the
// ClientCAs since the ticket was sealed ends it, and the chains are this
// verification's, whatever the ticket kept; one the Config does not verify
// has none.
func TestCovS12SessionPeer(t *testing.T) {
	ca := newTestCA(t, "covS12 CA")
	other := newTestCA(t, "covS12 other CA")
	leaf := ca.issue(t, "ecdsa-p256", "client", true).Certificate[0]
	chain := [][][]byte{{leaf, ca.cert.Raw}}
	later := func(c *stdtls.Config) { c.Time = func() time.Time { return time.Now().Add(48 * time.Hour) } }
	untrusted := func(c *stdtls.Config) { c.ClientCAs = other.pool }
	for _, tc := range []struct {
		name   string
		auth   stdtls.ClientAuthType
		raw    [][]byte
		chains [][][]byte
		config func(*stdtls.Config)
		ok     bool
		chain  bool
	}{
		{"verified", stdtls.RequireAndVerifyClientCert, [][]byte{leaf}, chain, nil, true, true},
		{"a certificate that does not parse", stdtls.RequestClientCert, [][]byte{leaf, {1, 2}}, nil, nil, false, false},
		{"expired", stdtls.RequestClientCert, [][]byte{leaf}, nil, later, false, false},
		{"not verified", stdtls.VerifyClientCertIfGiven, [][]byte{leaf}, nil, nil, false, false},
		{"not asked for", stdtls.NoClientCert, [][]byte{leaf}, chain, nil, false, false},
		{"a root no longer trusted", stdtls.RequireAndVerifyClientCert, [][]byte{leaf}, chain, untrusted, false, false},
		{"a chain the ticket kept that does not parse", stdtls.RequireAndVerifyClientCert, [][]byte{leaf}, [][][]byte{{leaf, {3}}}, nil, true, true},
		{"no longer verified", stdtls.RequestClientCert, [][]byte{leaf}, chain, nil, true, false},
		{"verified with what the client sent beside its own", stdtls.RequireAndVerifyClientCert, [][]byte{leaf, ca.cert.Raw}, chain, nil, true, true},
	} {
		config := &stdtls.Config{ClientAuth: tc.auth, ClientCAs: ca.pool}
		if tc.config != nil {
			tc.config(config)
		}
		certs, chains, ok := newSrvHS(config, nil).sessionPeer(tc.raw, tc.chains)
		if ok != tc.ok {
			t.Errorf("%s: ok %v", tc.name, ok)
			continue
		}
		if !ok && (certs != nil || chains != nil) {
			t.Errorf("%s: certificates returned", tc.name)
		}
		if !ok {
			continue
		}
		if len(certs) != len(tc.raw) {
			t.Errorf("%s: %d certificates", tc.name, len(certs))
		}
		if !tc.chain && chains != nil {
			t.Errorf("%s: chains %v", tc.name, chains)
		}
		if tc.chain && (len(chains) != 1 || len(chains[0]) != 2 || chains[0][0] != certs[0] || !chains[0][1].Equal(ca.cert)) {
			t.Errorf("%s: chains %v", tc.name, chains)
		}
	}
}

// covS12Vec24 is data with a 24-bit length in front.
func covS12Vec24(data []byte) []byte {
	return append([]byte{byte(len(data) >> 16), byte(len(data) >> 8), byte(len(data))}, data...)
}

// A client's Certificate and CertificateVerify that are malformed, use a
// scheme the server did not offer, or do not verify are refused, in either
// version, and leave the transcript and state as they were.
func TestCovS12ClientCertificateMessages(t *testing.T) {
	ca := newTestCA(t, "covS12 CA")
	leaf := ca.issue(t, "ecdsa-p256", "client", true)
	parsed, err := x509.ParseCertificate(leaf.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	server := func(version uint16) *serverHS {
		h := newSrvHS(&stdtls.Config{ClientAuth: stdtls.RequireAnyClientCert}, nil)
		h.version = version
		id := uint16(stdtls.TLS_AES_128_GCM_SHA256)
		if version == stdtls.VersionTLS12 {
			id = stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
		}
		h.suite, _ = lookupSuite(version, id)
		h.transcript = []byte("covS12 transcript")
		h.peerCerts = []*x509.Certificate{parsed}
		return h
	}
	verify := func(alg uint16, signature []byte, extra ...byte) []byte {
		var b builder
		b.u16(alg)
		b.vec16(func(b *builder) { b.raw(signature) })
		return hsMsg(hsCertificateVerify, append(b.b, extra...))
	}
	badSig := make([]byte, 70)
	untouched := func(name string, h *serverHS) {
		t.Helper()
		if string(h.transcript) != "covS12 transcript" || h.state != 0 {
			t.Errorf("%s: transcript %q, state %d", name, h.transcript, h.state)
		}
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"a context", append([]byte{1, 0}, covS12Vec24(nil)...)},
		{"an empty certificate", append([]byte{0}, covS12Vec24(append(covS12Vec24(nil), 0, 0))...)},
		{"no extensions", append([]byte{0}, covS12Vec24(covS12Vec24(leaf.Certificate[0]))...)},
		{"trailing data", append(append([]byte{0}, covS12Vec24(nil)...), 9)},
	} {
		name := "TLS 1.3 Certificate with " + tc.name
		h := server(stdtls.VersionTLS13)
		wantAlert(t, name, h.clientCertificate13(hsMsg(hsCertificate, tc.body)), alertDecodeError)
		untouched(name, h)
	}
	for _, tc := range []struct {
		name  string
		msg   []byte
		alert uint8
	}{
		{"trailing data", verify(0x0403, badSig, 0), alertDecodeError},
		{"a scheme not offered", verify(0x0203, badSig), alertIllegalParameter},
		{"a bad signature", verify(0x0403, badSig), alertDecryptError},
	} {
		name := "TLS 1.3 CertificateVerify with " + tc.name
		h := server(stdtls.VersionTLS13)
		wantAlert(t, name, h.clientCertificateVerify13(tc.msg), tc.alert)
		untouched(name, h)
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"trailing data", append(covS12Vec24(nil), 9)},
		{"an empty certificate", covS12Vec24(covS12Vec24(nil))},
		{"a truncated certificate", covS12Vec24([]byte{0, 0, 9, 1})},
	} {
		name := "TLS 1.2 Certificate with " + tc.name
		h := server(stdtls.VersionTLS12)
		wantAlert(t, name, h.clientCertificate12(hsMsg(hsCertificate, tc.body)), alertDecodeError)
		untouched(name, h)
	}
	for _, tc := range []struct {
		name  string
		msg   []byte
		alert uint8
	}{
		{"trailing data", verify(0x0403, badSig, 0), alertDecodeError},
		{"a scheme not offered", verify(0x0203, badSig), alertIllegalParameter},
		{"a bad signature", verify(0x0403, badSig), alertDecryptError},
	} {
		name := "TLS 1.2 CertificateVerify with " + tc.name
		h := server(stdtls.VersionTLS12)
		wantAlert(t, name, h.clientCertificateVerify12(tc.msg), tc.alert)
		untouched(name, h)
	}
}
