//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"slices"
	"testing"
	"time"
)

// The TLS 1.2 half of the client handshake against a server that breaks the
// protocol, as client_tamper_test.go does for TLS 1.3: the server's flight is
// in the clear up to its change_cipher_spec, so a test changes its messages
// and hands them to the client.

// flight12 is our server's TLS 1.2 answer to a client's hello: ServerHello,
// Certificate, ServerKeyExchange and ServerHelloDone.
type flight12 struct {
	srv  *serverHS
	msgs [][]byte
	key  crypto.Signer
}

func newFlight12(t *testing.T, cli *clientHS, config *stdtls.Config) *flight12 {
	t.Helper()
	return flight12For(t, cli, config, cli.hello())
}

// flight12For is newFlight12 for a hello already made.
func flight12For(t *testing.T, cli *clientHS, config *stdtls.Config, hello []byte) *flight12 {
	t.Helper()
	srv := newSrvHS(config, nil)
	_, out, _, herr := srv.process(hello)
	if herr != nil {
		t.Fatalf("server: %v", herr)
	}
	if srv.version != stdtls.VersionTLS12 {
		t.Fatalf("the server answered with %x", srv.version)
	}
	f := &flight12{srv: srv}
	f.key, _ = config.Certificates[0].PrivateKey.(crypto.Signer)
	var plain []byte
	for len(out) >= recordHeaderLen {
		size := recordHeaderLen + (int(out[3])<<8 | int(out[4]))
		plain = append(plain, out[recordHeaderLen:size]...)
		out = out[size:]
	}
	for len(plain) >= 4 {
		size := 4 + (int(plain[1])<<16 | int(plain[2])<<8 | int(plain[3]))
		f.msgs = append(f.msgs, bytes.Clone(plain[:size]))
		plain = plain[size:]
	}
	return f
}

// send hands cli msgs, each in a record of its own, in the clear.
func (f *flight12) send(cli *clientHS, msgs ...[]byte) (out []byte, done bool, herr *hsError) {
	var in []byte
	for _, m := range msgs {
		in = appendRecord(in, recordTypeHandshake, 0x0303, m)
	}
	_, out, done, herr = cli.process(in)
	return out, done, herr
}

// ske is a ServerKeyExchange for a share of curve, signed as the server would.
func (f *flight12) ske(t *testing.T, cli *clientHS, curve stdtls.CurveID, pub []byte) []byte {
	t.Helper()
	params := append([]byte{3, byte(curve >> 8), byte(curve), byte(len(pub))}, pub...)
	signed := append(append(append([]byte(nil), cli.random[:]...), f.srv.random[:]...), params...)
	sig, err := signHandshake(f.key, f.srv.alg, rand.Reader, signed)
	if err != nil {
		t.Fatal(err)
	}
	var b builder
	b.raw(params)
	b.u16(f.srv.alg)
	b.vec16(func(b *builder) { b.raw(sig) })
	return hsMsg(hsServerKeyExchange, b.b)
}

// serverKeys12 are the keys the server sends under once its change_cipher_spec
// is out, from their first record.
func serverKeys12(t *testing.T, srv *serverHS) *recordKeys {
	t.Helper()
	_, server, err := newKeysFromMaster(stdtls.VersionTLS12, srv.suite, srv.master, srv.ch.random, srv.random[:])
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func newCli12(t *testing.T, pool *x509.CertPool, configure func(*stdtls.Config)) *clientHS {
	t.Helper()
	return newHS12(t, pool, func(c *stdtls.Config) {
		c.MaxVersion = stdtls.VersionTLS12
		c.NextProtos = []string{"h2"}
		if configure != nil {
			configure(c)
		}
	})
}

// A TLS 1.2 ServerHello that is a downgrade, picks what was not offered, or
// carries extensions it may not, or malformed, is refused.
func TestClientRefusesBadServerHello12(t *testing.T) {
	config, pool := ecdsaServer(t)
	alpn := func(names ...string) []byte {
		var b builder
		b.vec16(func(b *builder) {
			for _, n := range names {
				b.vec8(func(b *builder) { b.raw([]byte(n)) })
			}
		})
		return b.b
	}
	for _, tc := range []struct {
		name  string
		tweak func(*clientHS)
		edit  func(helloFields, *clientHS) helloFields
		alert uint8
	}{
		{"a suite not offered", nil, func(f helloFields, _ *clientHS) helloFields {
			f.suite = stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256
			return f
		}, alertIllegalParameter},
		{"an offered suite the record layer lacks", func(h *clientHS) {
			h.suites12 = append(slices.Clone(h.suites12), stdtls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA)
		}, func(f helloFields, _ *clientHS) helloFields {
			f.suite = stdtls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
			return f
		}, alertIllegalParameter},
		{"the made-up session ID resumed", nil, func(f helloFields, h *clientHS) helloFields { f.session = h.sessionID; return f }, alertIllegalParameter},
		{"a renegotiation in progress", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extRenegotiationInfo, []byte{1, 5}) }, alertHandshakeFailure},
		{"a malformed extended_master_secret", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extExtendedMasterSec, []byte{0}) }, alertDecodeError},
		{"compressed points only", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extECPointFormats, []byte{1, 1}) }, alertIllegalParameter},
		{"a malformed ALPN extension", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extALPN, alpn("h2", "h3")) }, alertDecodeError},
		{"an ALPN protocol not offered", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extALPN, alpn("spdy")) }, alertNoApplicationProtocol},
		{"a malformed server_name", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extServerName, []byte{1}) }, alertDecodeError},
		{"a malformed session_ticket", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extSessionTicket, []byte{1}) }, alertDecodeError},
		{"an unknown extension", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(0x1234, nil) }, alertUnsupportedExtension},
	} {
		cli := newCli12(t, pool, nil)
		if tc.tweak != nil {
			tc.tweak(cli)
		}
		f := newFlight12(t, cli, config)
		_, _, herr := f.send(cli, tc.edit(parseHelloFields(f.msgs[0]), cli).bytes())
		wantAlert(t, tc.name, herr, tc.alert)
	}

	// A client that offers TLS 1.3 refuses a TLS 1.2 answer from a server
	// whose random says it speaks TLS 1.3 too.
	cli12 := newCli12(t, pool, nil)
	f := newFlight12(t, cli12, config)
	cli := newHS12(t, pool, nil)
	cli.hello()
	_, _, herr := f.send(cli, f.msgs[0])
	wantAlert(t, "a downgrade", herr, alertIllegalParameter)

	// Acknowledgements with nothing in them are taken.
	cli = newCli12(t, pool, nil)
	f = newFlight12(t, cli, config)
	sh := parseHelloFields(f.msgs[0]).with(extServerName, nil).with(extStatusRequest, nil)
	if _, _, herr := f.send(cli, sh.bytes()); herr != nil {
		t.Fatalf("empty acknowledgements: %v", herr)
	}
}

// The rest of the server's TLS 1.2 flight, malformed or wrong, is refused.
func TestClientRefusesBadServerFlight12(t *testing.T) {
	config, pool := ecdsaServer(t)
	for _, tc := range []struct {
		name  string
		edit  func(*flight12, *clientHS) [][]byte
		alert uint8
	}{
		{"a Certificate with trailing bytes", func(f *flight12, _ *clientHS) [][]byte {
			return replace(f.msgs, hsCertificate, hsMsg(hsCertificate, []byte{0, 0, 0, 0}))
		}, alertDecodeError},
		{"a Certificate entry with no certificate", func(f *flight12, _ *clientHS) [][]byte {
			return replace(f.msgs, hsCertificate, hsMsg(hsCertificate, []byte{0, 0, 3, 0, 0, 0}))
		}, alertDecodeError},
		{"a ServerKeyExchange with trailing bytes", func(f *flight12, _ *clientHS) [][]byte {
			ske := slices.IndexFunc(f.msgs, func(m []byte) bool { return m[0] == hsServerKeyExchange })
			m := append(bytes.Clone(f.msgs[ske][4:]), 0)
			return replace(f.msgs, hsServerKeyExchange, hsMsg(hsServerKeyExchange, m))
		}, alertDecodeError},
		{"a curve type other than named", func(f *flight12, _ *clientHS) [][]byte {
			ske := slices.IndexFunc(f.msgs, func(m []byte) bool { return m[0] == hsServerKeyExchange })
			m := bytes.Clone(f.msgs[ske])
			m[4] = 1
			return replace(f.msgs, hsServerKeyExchange, m)
		}, alertIllegalParameter},
		{"a share that is no point", func(f *flight12, cli *clientHS) [][]byte {
			return replace(f.msgs, hsServerKeyExchange, f.ske(t, cli, stdtls.X25519, []byte{1, 2, 3}))
		}, alertIllegalParameter},
		{"a scheme not offered", func(f *flight12, _ *clientHS) [][]byte {
			ske := slices.IndexFunc(f.msgs, func(m []byte) bool { return m[0] == hsServerKeyExchange })
			m := bytes.Clone(f.msgs[ske])
			at := 4 + 4 + int(m[7])
			m[at], m[at+1] = 2, 3 // ecdsa_sha1
			return replace(f.msgs, hsServerKeyExchange, m)
		}, alertIllegalParameter},
		{"a signature that does not verify", func(f *flight12, _ *clientHS) [][]byte {
			ske := slices.IndexFunc(f.msgs, func(m []byte) bool { return m[0] == hsServerKeyExchange })
			m := bytes.Clone(f.msgs[ske])
			m[len(m)-1] ^= 1
			return replace(f.msgs, hsServerKeyExchange, m)
		}, alertDecryptError},
		{"a share of low order", func(f *flight12, cli *clientHS) [][]byte {
			return replace(f.msgs, hsServerKeyExchange, f.ske(t, cli, stdtls.X25519, make([]byte, 32)))
		}, alertIllegalParameter},
		{"a CertificateRequest cut short", func(f *flight12, _ *clientHS) [][]byte {
			return insertBefore(f.msgs, hsServerHelloDone, hsMsg(hsCertificateRequest, []byte{2, 1, 64}))
		}, alertDecodeError},
		{"a CertificateRequest with malformed authorities", func(f *flight12, _ *clientHS) [][]byte {
			return insertBefore(f.msgs, hsServerHelloDone, hsMsg(hsCertificateRequest, []byte{2, 1, 64, 0, 2, 4, 3, 0, 3, 0, 5, 1}))
		}, alertDecodeError},
		{"a ServerHelloDone with a body", func(f *flight12, _ *clientHS) [][]byte {
			return replace(f.msgs, hsServerHelloDone, hsMsg(hsServerHelloDone, []byte{0}))
		}, alertDecodeError},
	} {
		cli := newCli12(t, pool, nil)
		f := newFlight12(t, cli, config)
		_, _, herr := f.send(cli, tc.edit(f, cli)...)
		wantAlert(t, tc.name, herr, tc.alert)
	}

	cli := newCli12(t, pool, nil)
	f := newFlight12(t, cli, config)
	in := appendRecord(nil, recordTypeHandshake, 0x0303, f.msgs[0])
	in = append(in, recordTypeChangeCipherSpec, 3, 3, 0, 1, 1)
	_, _, _, herr := cli.process(in)
	wantAlert(t, "a change_cipher_spec in the middle of the flight", herr, alertUnexpectedMessage)
}

// The client's TLS 1.2 certificate cannot be used with the schemes the
// server asks for, or its key fails to sign.
func TestClientCertificateFailures12(t *testing.T) {
	config, pool := ecdsaServer(t)
	ca := newTestCA(t, "client CA")
	clientCert := ca.issue(t, "ecdsa-p256", "client", true)
	broken := clientCert
	broken.PrivateKey = failingSigner{clientCert.PrivateKey.(crypto.Signer)}
	for _, tc := range []struct {
		name  string
		cert  *stdtls.Certificate
		algs  []byte
		alert uint8
	}{
		{"an RSA-only request for an ECDSA certificate", &clientCert, []byte{0, 2, 4, 1}, alertHandshakeFailure},
		{"a key that fails", &broken, []byte{0, 2, 4, 3}, alertInternalError},
	} {
		cli := newCli12(t, pool, func(c *stdtls.Config) {
			c.GetClientCertificate = func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) { return tc.cert, nil }
		})
		f := newFlight12(t, cli, config)
		req := append(append([]byte{2, 1, 64}, tc.algs...), 0, 0)
		_, _, herr := f.send(cli, insertBefore(f.msgs, hsServerHelloDone, hsMsg(hsCertificateRequest, req))...)
		wantAlert(t, tc.name, herr, tc.alert)
	}
}

// After the client's flight, the server's change_cipher_spec, Finished and
// alerts: a ticket nobody asked for is taken and ignored, a protected alert
// is the server's, a record that does not authenticate or a Finished that
// does not verify fails, and VerifyConnection has the last word.
func TestClientServerFinished12(t *testing.T) {
	config, pool := ecdsaServer(t)
	ccs := []byte{recordTypeChangeCipherSpec, 3, 3, 0, 1, 1}
	// setup runs the handshake up to the server's change_cipher_spec.
	setup := func(t *testing.T, configure func(*stdtls.Config)) (*clientHS, *serverHS, []byte) {
		t.Helper()
		cli := newCli12(t, pool, configure)
		f := newFlight12(t, cli, config)
		toServer, _, herr := f.send(cli, f.msgs...)
		if herr != nil {
			t.Fatal(herr)
		}
		_, toClient, _, herr := f.srv.process(toServer)
		if herr != nil {
			t.Fatal(herr)
		}
		return cli, f.srv, toClient
	}

	// The ticket joins the transcript, so the server's Finished, made
	// without it, is not checked here.
	cli, _, _ := setup(t, nil)
	in := appendRecord(nil, recordTypeHandshake, 0x0303, hsMsg(hsNewSessionTicket, []byte{0, 0, 0, 1, 0, 0}))
	before := len(cli.transcript)
	if _, _, done, herr := cli.process(in); herr != nil || done || len(cli.transcript) != before+10 {
		t.Fatalf("an unrequested ticket: done %v, %v", done, herr)
	}

	cli, srv, _ := setup(t, nil)
	rec, _ := serverKeys12(t, srv).seal(bytes.Clone(ccs), recordTypeAlert, []byte{alertLevelError, alertHandshakeFailure}, nil)
	_, _, _, herr := cli.process(rec)
	var alert stdtls.AlertError
	if herr == nil || herr.alert != 0 || !errors.As(herr, &alert) {
		t.Fatalf("a protected alert: %v", herr)
	}

	cli, srv, _ = setup(t, nil)
	rec, _ = serverKeys12(t, srv).seal(bytes.Clone(ccs), recordTypeHandshake, hsMsg(hsFinished, make([]byte, 12)), nil)
	_, _, _, herr = cli.process(rec)
	wantAlert(t, "a Finished that does not verify", herr, alertDecryptError)

	cli, _, toClient := setup(t, nil)
	toClient[len(toClient)-1] ^= 1
	_, _, _, herr = cli.process(toClient)
	wantAlert(t, "a Finished that does not authenticate", herr, alertBadRecordMAC)

	cli, srv, _ = setup(t, nil)
	rec = append(bytes.Clone(ccs), appendRecord(nil, recordTypeAlert, 0x0303, make([]byte, 40))...)
	_, _, _, herr = cli.process(rec)
	wantAlert(t, "a protected alert that does not authenticate", herr, alertBadRecordMAC)
	_ = srv

	refused := errors.New("refused")
	cli, _, toClient = setup(t, func(c *stdtls.Config) {
		c.VerifyConnection = func(stdtls.ConnectionState) error { return refused }
	})
	if _, _, _, herr = cli.process(toClient); !errors.Is(herr, refused) || herr.alert != alertBadCertificate {
		t.Fatalf("VerifyConnection: %v", herr)
	}
}

// withoutHelloExt is the ClientHello record rec without the extension typ,
// and the message it carries.
func withoutHelloExt(t *testing.T, rec []byte, typ uint16) (record, msg []byte) {
	t.Helper()
	ch, ok := parseClientHello(rec[recordHeaderLen:])
	if !ok {
		t.Fatal("the hello does not parse")
	}
	var b builder
	b.u8(hsClientHello)
	b.u24(0)
	b.u16(ch.legacy)
	b.raw(ch.random)
	b.vec8(func(b *builder) { b.raw(ch.sessionID) })
	b.vec16(func(b *builder) {
		for _, s := range ch.suites {
			b.u16(s)
		}
	})
	b.vec8(func(b *builder) { b.raw(ch.compression) })
	r := newReader(ch.extBlock)
	b.vec16(func(b *builder) {
		for len(r.b) > 0 {
			id, data := r.u16(), r.vec16()
			if id != typ {
				b.u16(id)
				b.vec16(func(b *builder) { b.raw(data) })
			}
		}
	})
	msg = finishMessage(&b)
	return appendRecord(nil, recordTypeHandshake, 0x0301, msg), msg
}

// converse runs cli and srv against each other in memory from the client's
// first flight, and returns the first failure of either.
func converse(t *testing.T, cli *clientHS, srv *serverHS, toServer []byte) (cliErr, srvErr *hsError) {
	t.Helper()
	var toClient, inServer, inClient []byte
	for step := 0; step < 100 && !(srv.state == srvDone && cli.state == hsDone); step++ {
		if len(toServer) > 0 {
			inServer = append(inServer, toServer...)
			toServer = nil
			consumed, out, _, herr := srv.process(inServer)
			if herr != nil {
				return nil, herr
			}
			inServer = inServer[consumed:]
			toClient = append(toClient, out...)
		}
		if len(toClient) > 0 {
			inClient = append(inClient, toClient...)
			toClient = nil
			consumed, out, _, herr := cli.process(inClient)
			if herr != nil {
				return herr, nil
			}
			inClient = inClient[consumed:]
			toServer = append(toServer, out...)
		}
	}
	if srv.state != srvDone || cli.state != hsDone {
		t.Fatalf("server state %d, client state %d", srv.state, cli.state)
	}
	return nil, nil
}

// A client that does not offer the extended master secret gets a TLS 1.2
// handshake with the original one, on both sides.
func TestHandshakeWithoutExtendedMasterSecret(t *testing.T) {
	config, pool := ecdsaServer(t)
	cli := newCli12(t, pool, nil)
	hello := cli.hello()
	rec, msg := withoutHelloExt(t, hello, extExtendedMasterSec)
	cli.transcript = append(cli.transcript[:len(cli.transcript)-(len(hello)-recordHeaderLen)], msg...)
	srv := newSrvHS(config, nil)
	if cliErr, srvErr := converse(t, cli, srv, rec); cliErr != nil || srvErr != nil {
		t.Fatalf("client %v, server %v", cliErr, srvErr)
	}
	if cli.ems || srv.ems {
		t.Fatal("the extended master secret was used")
	}
}

// shortRand is a Config.Rand that runs out after n bytes.
type shortRand struct{ n int }

func (r *shortRand) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	k := min(len(p), r.n)
	r.n -= k
	return k, nil
}

// A Config.Rand that fails fails the handshake that reads it, on either side,
// at whichever read it fails.
func TestFailingRand(t *testing.T) {
	config, pool := ecdsaServer(t)
	for _, n := range []int{0, 32} {
		if _, err := newClientHS(&stdtls.Config{ServerName: "localhost", Rand: &shortRand{n: n}}); err == nil {
			t.Fatalf("a Rand that ran out after %d bytes was not noticed", n)
		}
	}
	// One that lasts is the one the handshake uses.
	cli := newHS(t, pool, func(c *stdtls.Config) { c.Rand = rand.Reader; c.Time = time.Now })
	if cli.rander() != rand.Reader {
		t.Fatal("the Config's Rand is not used")
	}

	for _, tc := range []struct {
		name string
		max  uint16
		n    int
	}{
		{"TLS 1.3 server random", stdtls.VersionTLS13, 0},
		{"TLS 1.2 server random", stdtls.VersionTLS12, 0},
		{"TLS 1.3 ticket", stdtls.VersionTLS13, 32},
	} {
		srvConfig := config.Clone()
		srvConfig.Rand = &shortRand{n: tc.n}
		srv := newSrvHS(srvConfig, newTicketKeys(&stdtls.Config{}))
		cli := newHS12(t, pool, func(c *stdtls.Config) { c.MaxVersion = tc.max })
		cli.sessions = NewQUICSessionCache(1)
		cliErr, srvErr := converse(t, cli, srv, cli.hello())
		if cliErr != nil || srvErr == nil || srvErr.alert != alertInternalError {
			t.Errorf("%s: client %v, server %v", tc.name, cliErr, srvErr)
		}
	}
}
