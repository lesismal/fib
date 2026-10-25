//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"slices"
	"testing"
	"time"
)

// The TLS 1.3 server handshake against clients that break the protocol: a
// ClientHello made by hand, or one of our client's taken apart, and records
// sealed under the client's handshake keys, which the server derived and the
// test takes from it.

// s13CH is a ClientHello, to be made by hand and broken as a test needs.
type s13CH struct {
	legacy  uint16
	random  []byte
	session []byte
	suites  []uint16
	comp    []byte
	exts    []msgExt
	trailer []byte
}

// s13KeyShare is a key_share extension with one share.
func s13KeyShare(group stdtls.CurveID, data []byte) []byte {
	var b builder
	b.vec16(func(b *builder) {
		b.u16(uint16(group))
		b.vec16(func(b *builder) { b.raw(data) })
	})
	return b.b
}

// s13ALPN is an ALPN extension offering protos.
func s13ALPN(protos ...string) []byte {
	var b builder
	b.vec16(func(b *builder) {
		for _, p := range protos {
			b.vec8(func(b *builder) { b.raw([]byte(p)) })
		}
	})
	return b.b
}

// s13ServerName is a server_name extension naming name.
func s13ServerName(name string) []byte {
	var b builder
	b.vec16(func(b *builder) {
		b.u8(0)
		b.vec16(func(b *builder) { b.raw([]byte(name)) })
	})
	return b.b
}

// s13Hello is a TLS 1.3 ClientHello of a client that offers X25519 with a
// share, AES-128-GCM, and ECDSA, PSS and Ed25519 signatures.
func s13Hello(t *testing.T) s13CH {
	t.Helper()
	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return s13CH{
		legacy:  0x0303,
		random:  bytes.Repeat([]byte{0x11}, 32),
		session: bytes.Repeat([]byte{0x22}, 32),
		suites:  []uint16{stdtls.TLS_AES_128_GCM_SHA256},
		comp:    []byte{0},
		exts: []msgExt{
			{extSupportedVersions, []byte{2, 3, 4}},
			{extSupportedGroups, u16s(uint16(stdtls.X25519))},
			{extKeyShare, s13KeyShare(stdtls.X25519, key.PublicKey().Bytes())},
			{extSignatureAlgs, u16s(0x0403, 0x0804, 0x0807)},
		},
	}
}

// s13QUICHello is s13Hello as a QUIC client sends it: no session ID, an
// application protocol, and transport parameters.
func s13QUICHello(t *testing.T) s13CH {
	c := s13Hello(t)
	c.session = nil
	return c.with(extALPN, s13ALPN("h2")).with(extQUICParams, []byte{1, 2})
}

// s13Parsed is the ClientHello msg as s13CH, its extensions in their order.
func s13Parsed(t *testing.T, msg []byte) s13CH {
	t.Helper()
	ch, ok := parseClientHello(msg)
	if !ok {
		t.Fatal("the hello does not parse")
	}
	c := s13CH{legacy: ch.legacy, random: bytes.Clone(ch.random), session: bytes.Clone(ch.sessionID),
		suites: slices.Clone(ch.suites), comp: bytes.Clone(ch.compression)}
	r := newReader(ch.extBlock)
	for len(r.b) > 0 && r.ok {
		typ := r.u16()
		c.exts = append(c.exts, msgExt{typ, bytes.Clone(r.vec16())})
	}
	return c
}

func (c s13CH) with(typ uint16, data []byte) s13CH {
	c = c.without(typ)
	c.exts = append(c.exts, msgExt{typ, data})
	return c
}

func (c s13CH) without(typ uint16) s13CH {
	c.exts = slices.DeleteFunc(slices.Clone(c.exts), func(e msgExt) bool { return e.typ == typ })
	return c
}

func (c s13CH) msg() []byte {
	var b builder
	b.u8(hsClientHello)
	b.u24(0)
	b.u16(c.legacy)
	b.raw(c.random)
	b.vec8(func(b *builder) { b.raw(c.session) })
	b.vec16(func(b *builder) {
		for _, s := range c.suites {
			b.u16(s)
		}
	})
	b.vec8(func(b *builder) { b.raw(c.comp) })
	appendExts(&b, c.exts)
	b.raw(c.trailer)
	return finishMessage(&b)
}

func (c s13CH) record() []byte { return appendRecord(nil, recordTypeHandshake, 0x0301, c.msg()) }

// s13AfterFlight has a server with config answer a client of ours, and
// returns it waiting for the client's second flight, with the keys that
// flight is sealed under.
func s13AfterFlight(t *testing.T, config *stdtls.Config, pool *x509.CertPool) (*serverHS, *recordKeys) {
	t.Helper()
	srv := newSrvHS(config, nil)
	cli := newHS(t, pool, nil)
	hello := cli.hello()
	consumed, out, done, herr := srv.process(hello)
	if herr != nil || consumed != len(hello) || done || len(out) == 0 || srv.state != srvAwaitFinished {
		t.Fatalf("server: consumed %d of %d, done %v, state %d, %v", consumed, len(hello), done, srv.state, herr)
	}
	keys, err := newKeys13(srv.suite, srv.suite.hash, srv.clientHS)
	if err != nil {
		t.Fatal(err)
	}
	return srv, keys
}

// s13Finished is the client Finished srv expects.
func s13Finished(t *testing.T, srv *serverHS) []byte {
	t.Helper()
	mac, err := srv.finishedMAC(srv.clientHS)
	if err != nil {
		t.Fatal(err)
	}
	return hsMsg(hsFinished, mac)
}

// s13Header is the header of a record of typ that says it is n bytes long.
func s13Header(typ byte, n int) []byte { return []byte{typ, 3, 3, byte(n >> 8), byte(n)} }

func s13Seal(t *testing.T, keys *recordKeys, typ byte, plain []byte) []byte {
	t.Helper()
	rec, err := keys.seal(nil, typ, plain, nil)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// parseClientHello refuses a ClientHello that is malformed anywhere, with
// which crypto/tls is left to answer, and reads what it can.
func TestS13ParseClientHello(t *testing.T) {
	trailing := func(data []byte) []byte { return append(bytes.Clone(data), 0) }
	base := s13Hello(t)
	pskLast := func(c s13CH, psk []byte) []byte { return c.with(extPreSharedKey, psk).msg() }
	for _, tc := range []struct {
		name string
		msg  []byte
	}{
		{"too short", []byte{hsClientHello, 0, 0}},
		{"not a ClientHello", hsMsg(hsServerHello, base.msg()[4:])},
		{"bytes after the extensions", func() []byte { c := base; c.trailer = []byte{0}; return c.msg() }()},
		{"a session ID longer than 32 bytes", func() []byte { c := base; c.session = make([]byte, 33); return c.msg() }()},
		{"no compression method", func() []byte { c := base; c.comp = nil; return c.msg() }()},
		{"an extension twice", func() []byte {
			c := base
			c.exts = append(slices.Clone(c.exts), msgExt{extALPN, s13ALPN("a")}, msgExt{extALPN, s13ALPN("b")})
			return c.msg()
		}()},
		{"a malformed server_name", base.with(extServerName, trailing(s13ServerName("a.example"))).msg()},
		{"a malformed supported_versions", base.with(extSupportedVersions, []byte{2, 3, 4, 0}).msg()},
		{"a malformed supported_groups", base.with(extSupportedGroups, trailing(u16s(29))).msg()},
		{"a malformed key_share", base.with(extKeyShare, trailing(s13KeyShare(stdtls.X25519, make([]byte, 32)))).msg()},
		{"a malformed signature_algorithms", base.with(extSignatureAlgs, trailing(u16s(0x0403))).msg()},
		{"a malformed ALPN", base.with(extALPN, trailing(s13ALPN("h2"))).msg()},
		{"a malformed ec_point_formats", base.with(extECPointFormats, []byte{1, 0, 0}).msg()},
		{"a malformed psk_key_exchange_modes", base.with(extPSKModes, []byte{1, 1, 0}).msg()},
		{"a pre_shared_key that is not last", func() []byte {
			c := base.with(extPreSharedKey, []byte{0, 0, 0, 0})
			c.exts = append(c.exts, msgExt{extALPN, s13ALPN("h2")})
			return c.msg()
		}()},
		{"a pre_shared_key with no identity", pskLast(base, []byte{0, 0, 0, 0})},
	} {
		if ch, ok := parseClientHello(tc.msg); ok {
			t.Errorf("%s: parsed as %+v", tc.name, ch)
		}
	}

	// The renegotiation SCSV says what the extension would.
	c := base
	c.suites = append(slices.Clone(c.suites), 0x00ff)
	if ch, ok := parseClientHello(c.msg()); !ok || !ch.renegotiate {
		t.Fatalf("the renegotiation SCSV: parsed %v, renegotiate %v", ok, ch != nil && ch.renegotiate)
	}
}

// Before it has taken anything, the server hands crypto/tls a connection
// whose first record or message is not one it reads, or whose ClientHello
// it cannot serve; a ClientHello in two records it puts together and serves.
func TestS13PeekDecides(t *testing.T) {
	config, _ := ecdsaServer(t)
	base := s13Hello(t)
	twoRecords := func() []byte {
		msg := base.msg()
		half := len(msg) / 2
		return append(appendRecord(nil, recordTypeHandshake, 0x0301, msg[:half]),
			appendRecord(nil, recordTypeHandshake, 0x0301, msg[half:])...)
	}()
	for _, tc := range []struct {
		name  string
		in    []byte
		serve bool
	}{
		{"a record longer than a plaintext may be", s13Header(recordTypeHandshake, maxPlaintext+1), false},
		{"a message longer than a handshake message may be", appendRecord(nil, recordTypeHandshake, 0x0301, []byte{hsClientHello, 1, 0, 1}), false},
		{"a ClientHello that does not parse", func() []byte { c := base; c.session = make([]byte, 33); return c.record() }(), false},
		{"TLS 1.3 without the null compression method", func() []byte { c := base; c.comp = []byte{1}; return c.record() }(), false},
		{"a ClientHello in two records", twoRecords, true},
	} {
		srv := newSrvHS(config, nil)
		consumed, out, _, herr := srv.process(tc.in)
		if tc.serve {
			if herr != nil || consumed != len(tc.in) || len(out) == 0 || srv.state != srvAwaitFinished {
				t.Errorf("%s: consumed %d of %d, %d bytes out, state %d, %v", tc.name, consumed, len(tc.in), len(out), srv.state, herr)
			}
			continue
		}
		if herr != errFallback || consumed != 0 || len(out) != 0 {
			t.Errorf("%s: consumed %d, %d bytes out, %v; want the fallback", tc.name, consumed, len(out), herr)
		}
	}
}

// A QUIC server serves only a ClientHello that offers TLS 1.3 alone, a QUIC
// suite, a group and a protocol it has, and transport parameters; any other
// goes to crypto/tls.
func TestS13ServesQUIC(t *testing.T) {
	config, _ := ecdsaServer(t)
	base := s13QUICHello(t)
	for _, tc := range []struct {
		name string
		ch   s13CH
		want int
	}{
		{"a QUIC client", base, peekServe},
		{"TLS 1.2 offered too", base.with(extSupportedVersions, []byte{4, 3, 4, 3, 3}), peekFallback},
		{"no transport parameters", base.without(extQUICParams), peekFallback},
		{"no protocol in common", base.with(extALPN, s13ALPN("http/1.1")), peekFallback},
		{"no QUIC suite", func() s13CH { c := base; c.suites = []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256}; return c }(), peekFallback},
		{"no group in common", base.with(extSupportedGroups, u16s(uint16(stdtls.CurveP521))), peekFallback},
	} {
		srv := newServerHSFor(config, nil, nil, &quicCtx{})
		ch, ok := parseClientHello(tc.ch.msg())
		if !ok {
			t.Fatalf("%s: the hello does not parse", tc.name)
		}
		if got := srv.serves(ch); got != tc.want {
			t.Errorf("%s: serves %d, want %d", tc.name, got, tc.want)
		}
	}
}

// A ClientHello the server takes but cannot complete a handshake with is
// refused with the alert crypto/tls sends.
func TestS13RefusesClientHello(t *testing.T) {
	config, _ := ecdsaServer(t)
	base := s13Hello(t)
	extra := append(base.record(), hsFinished, 0, 0, 0)
	extra[3], extra[4] = byte((len(extra)-recordHeaderLen)>>8), byte(len(extra)-recordHeaderLen)
	for _, tc := range []struct {
		name  string
		in    []byte
		alert uint8
	}{
		{"early_data without pre_shared_key", base.with(extEarlyData, nil).record(), alertIllegalParameter},
		{"no signature scheme the certificate signs with", base.with(extSignatureAlgs, u16s(0x0807)).record(), alertHandshakeFailure},
		{"a key share of the wrong size", base.with(extKeyShare, s13KeyShare(stdtls.X25519, make([]byte, 5))).record(), alertIllegalParameter},
		{"a low-order X25519 share", base.with(extKeyShare, s13KeyShare(stdtls.X25519, make([]byte, 32))).record(), alertIllegalParameter},
		{"a message after the ClientHello in its record", extra, alertUnexpectedMessage},
	} {
		srv := newSrvHS(config, nil)
		_, _, _, herr := srv.process(tc.in)
		wantAlert(t, tc.name, herr, tc.alert)
	}
}

// After a HelloRetryRequest, the server reads records in the clear: the
// client's second ClientHello, which must be the first with the share asked
// for, or an alert; anything else is refused.
func TestS13AfterHelloRetryRequest(t *testing.T) {
	config, pool := ecdsaServer(t)
	config.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256}
	retried := func(hello []byte) *serverHS {
		srv := newSrvHS(config, nil)
		_, out, _, herr := srv.process(hello)
		if herr != nil || srv.state != srvAwaitHello2 || len(out) == 0 {
			t.Fatalf("no HelloRetryRequest: state %d, %v", srv.state, herr)
		}
		return srv
	}
	cli := newHS(t, pool, nil)
	first := cli.hello()
	second := s13Parsed(t, first[recordHeaderLen:])
	for _, tc := range []struct {
		name  string
		in    []byte
		alert uint8
	}{
		{"the first ClientHello again", first, alertHandshakeFailure},
		{"a ClientHello with another random", func() []byte { c := second; c.random = make([]byte, 32); return c.record() }(), alertIllegalParameter},
		{"a ClientHello that does not parse", func() []byte { c := second; c.comp = nil; return c.record() }(), alertDecodeError},
		{"a protected record", appendRecord(nil, recordTypeApplicationData, 0x0303, make([]byte, 30)), alertUnexpectedMessage},
		{"a message too large to take", appendRecord(nil, recordTypeHandshake, 0x0303, []byte{hsClientHello, 1, 0, 0}), alertInternalError},
		{"a record of version 2", appendRecord(nil, recordTypeHandshake, 0x0200, second.msg()), alertProtocolVersion},
	} {
		_, _, _, herr := retried(first).process(tc.in)
		wantAlert(t, tc.name, herr, tc.alert)
	}

	// The client's alert ends the handshake as the client's.
	_, _, _, herr := retried(first).process(appendRecord(nil, recordTypeAlert, 0x0303, []byte{alertLevelError, alertHandshakeFailure}))
	var alert stdtls.AlertError
	if herr == nil || herr.alert != 0 || !errors.As(herr, &alert) || alert != alertHandshakeFailure {
		t.Fatalf("a remote alert: %v", herr)
	}
}

// The client's second flight, which comes under its handshake keys, must be
// its Finished, right, with nothing after it; a record the handshake has no
// place for is refused.
func TestS13ClientSecondFlight(t *testing.T) {
	config, pool := ecdsaServer(t)
	ccs := []byte{recordTypeChangeCipherSpec, 3, 3, 0, 1, 1}
	for _, tc := range []struct {
		name  string
		in    func(srv *serverHS, keys *recordKeys) []byte
		alert uint8
	}{
		{"a record of version 2", func(srv *serverHS, keys *recordKeys) []byte {
			return appendRecord(nil, recordTypeHandshake, 0x0203, []byte{1})
		}, alertProtocolVersion},
		{"a change_cipher_spec that is not one", func(*serverHS, *recordKeys) []byte {
			return appendRecord(nil, recordTypeChangeCipherSpec, 0x0303, []byte{2})
		}, alertUnexpectedMessage},
		{"three change_cipher_specs", func(*serverHS, *recordKeys) []byte {
			return slices.Concat(ccs, ccs, ccs)
		}, alertUnexpectedMessage},
		{"an alert in the clear", func(*serverHS, *recordKeys) []byte {
			return appendRecord(nil, recordTypeAlert, 0x0303, []byte{alertLevelError, alertHandshakeFailure})
		}, alertUnexpectedMessage},
		{"a handshake record in the clear", func(srv *serverHS, _ *recordKeys) []byte {
			return appendRecord(nil, recordTypeHandshake, 0x0303, s13Finished(t, srv))
		}, alertUnexpectedMessage},
		{"a record of an unknown type", func(*serverHS, *recordKeys) []byte {
			return appendRecord(nil, 99, 0x0303, []byte{1})
		}, alertUnexpectedMessage},
		{"a record longer than a TLS 1.3 record may be", func(*serverHS, *recordKeys) []byte {
			return s13Header(recordTypeApplicationData, maxCiphertext13+1)
		}, alertRecordOverflow},
		{"application data before the Finished", func(_ *serverHS, keys *recordKeys) []byte {
			return s13Seal(t, keys, recordTypeApplicationData, []byte("too soon"))
		}, alertUnexpectedMessage},
		{"a change_cipher_spec under the keys", func(_ *serverHS, keys *recordKeys) []byte {
			return s13Seal(t, keys, recordTypeChangeCipherSpec, []byte{1})
		}, alertUnexpectedMessage},
		{"a message the handshake does not expect", func(_ *serverHS, keys *recordKeys) []byte {
			return s13Seal(t, keys, recordTypeHandshake, hsMsg(hsClientKeyExchange, []byte{0}))
		}, alertUnexpectedMessage},
		{"a message too large to take", func(_ *serverHS, keys *recordKeys) []byte {
			return s13Seal(t, keys, recordTypeHandshake, []byte{hsFinished, 1, 0, 0})
		}, alertInternalError},
		{"a wrong Finished", func(_ *serverHS, keys *recordKeys) []byte {
			return s13Seal(t, keys, recordTypeHandshake, hsMsg(hsFinished, make([]byte, 32)))
		}, alertDecryptError},
		{"handshake data after the Finished", func(srv *serverHS, keys *recordKeys) []byte {
			return s13Seal(t, keys, recordTypeHandshake, append(s13Finished(t, srv), hsFinished, 0, 0))
		}, alertUnexpectedMessage},
	} {
		srv, keys := s13AfterFlight(t, config, pool)
		_, _, _, herr := srv.process(tc.in(srv, keys))
		wantAlert(t, tc.name, herr, tc.alert)
	}

	// The two change_cipher_specs a client may send are let through, and
	// the right Finished ends the handshake.
	srv, keys := s13AfterFlight(t, config, pool)
	in := slices.Concat(ccs, ccs, s13Seal(t, keys, recordTypeHandshake, s13Finished(t, srv)))
	if consumed, _, done, herr := srv.process(in); herr != nil || !done || consumed != len(in) {
		t.Fatalf("the Finished: consumed %d of %d, done %v, %v", consumed, len(in), done, herr)
	}
}

// The certificate is GetCertificate's, if it has one, or the first of
// several the client's hello supports, or the first; a certificate that
// cannot be used, or none at all, fails the handshake.
func TestS13SelectCertificate(t *testing.T) {
	cert, _ := issueCert(t, keyOf(t, "ecdsa-p256"))
	other, _ := issueCertFor(t, keyOf(t, "ecdsa-p256"), "other.example")
	hello := s13Hello(t).with(extServerName, s13ServerName("nobody.example")).record()
	get := func(c *stdtls.Certificate) func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		return func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) { return c, nil }
	}
	for _, tc := range []struct {
		name  string
		get   func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error)
		alert uint8
	}{
		{"a certificate without a chain", get(&stdtls.Certificate{PrivateKey: cert.PrivateKey}), alertInternalError},
		{"no certificate at all", get(nil), alertUnrecognizedName},
	} {
		srv := newSrvHS(&stdtls.Config{GetCertificate: tc.get}, nil)
		_, _, _, herr := srv.process(hello)
		wantAlert(t, tc.name, herr, tc.alert)
	}

	// Neither certificate names the server the client asks for: the first.
	srv := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{other, cert}}, nil)
	if _, _, _, herr := srv.process(hello); herr != nil || srv.cert == nil || !bytes.Equal(srv.cert.Certificate[0], other.Certificate[0]) {
		t.Fatalf("no certificate for the name: %v", herr)
	}

	// A key that fails to sign fails the CertificateVerify.
	broken := cert
	broken.PrivateKey = failingSigner{cert.PrivateKey.(crypto.Signer)}
	srv = newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{broken}}, nil)
	_, _, _, herr := srv.process(hello)
	wantAlert(t, "a key that cannot sign", herr, alertInternalError)
}

// A client that asked for them is sent the certificate's OCSP response and
// signed certificate timestamps with the leaf.
func TestS13StapledOCSPAndSCTs(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	cert.OCSPStaple = []byte("an OCSP response")
	cert.SignedCertificateTimestamps = [][]byte{[]byte("sct one"), []byte("sct two")}
	hs := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{cert}}, nil)
	state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, nil), 4096)
	if clientErr != nil || serverErr != nil {
		t.Fatalf("client %v, server %v", clientErr, serverErr)
	}
	if !bytes.Equal(state.OCSPResponse, cert.OCSPStaple) || len(state.SignedCertificateTimestamps) != 2 ||
		!bytes.Equal(state.SignedCertificateTimestamps[1], cert.SignedCertificateTimestamps[1]) {
		t.Fatalf("OCSP %q, SCTs %q", state.OCSPResponse, state.SignedCertificateTimestamps)
	}
}

// A client that does not support the server's first group gets the first it
// has a share for, without a HelloRetryRequest.
func TestS13SkipsGroupsTheClientLacks(t *testing.T) {
	config, pool := ecdsaServer(t)
	srv := newSrvHS(config, nil)
	cli := newHS(t, pool, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} })
	if cliErr, srvErr := converse(t, cli, srv, cli.hello()); cliErr != nil || srvErr != nil {
		t.Fatalf("client %v, server %v", cliErr, srvErr)
	}
	if srv.group != stdtls.CurveP256 || srv.retried {
		t.Fatalf("group %v, retried %v", srv.group, srv.retried)
	}
}

// s13Session runs a full handshake of a client of ours, changed by tweak,
// with a server with config whose tickets are keys, and which takes early
// data over TCP if early, and returns the cache that holds the ticket the
// client was sent.
func s13Session(t *testing.T, config *stdtls.Config, pool *x509.CertPool, keys *ticketKeys, early bool, tweak func(*clientHS)) QUICSessionCache {
	t.Helper()
	cache := NewQUICSessionCache(1)
	srv := newSrvHS(config, keys)
	srv.tcpEarly, srv.maxEarly = early, 10
	cli := newHS(t, pool, nil)
	cli.sessions = cache
	if tweak != nil {
		tweak(cli)
	}
	if s13Converse(t, cli, srv, cli.hello()) != 1 || cache.Get("localhost") == nil {
		t.Fatal("no ticket")
	}
	return cache
}

// s13Converse runs cli and srv against each other in memory until the
// server's handshake is over, and hands the client the tickets the server
// sends then, which over TCP the layer takes; it returns how many there were.
func s13Converse(t *testing.T, cli *clientHS, srv *serverHS, toServer []byte) (tickets int) {
	t.Helper()
	for step := 0; step < 10; step++ {
		consumed, out, done, herr := srv.process(toServer)
		if herr != nil || consumed != len(toServer) {
			t.Fatalf("server: consumed %d of %d, %v", consumed, len(toServer), herr)
		}
		if done {
			for len(out) > 0 {
				size := recordHeaderLen + (int(out[3])<<8 | int(out[4]))
				typ, plain, _, ok := cli.rx.open(out[:size])
				if !ok || typ != recordTypeHandshake || plain[0] != hsNewSessionTicket {
					t.Fatalf("after the handshake: type %d, %v", typ, ok)
				}
				if herr := cli.newSessionTicket(plain); herr != nil {
					t.Fatalf("client: %v", herr)
				}
				out = out[size:]
				tickets++
			}
			return tickets
		}
		consumed, toServer, _, herr = cli.process(out)
		if herr != nil || consumed != len(out) {
			t.Fatalf("client: consumed %d of %d, %v", consumed, len(out), herr)
		}
	}
	t.Fatalf("server state %d, client state %d", srv.state, cli.state)
	return 0
}

// s13Resuming is a client of ours that offers the session in cache, with
// early data if early, and its first flight.
func s13Resuming(t *testing.T, pool *x509.CertPool, cache QUICSessionCache, early bool) (*clientHS, []byte) {
	t.Helper()
	cli := newHS(t, pool, nil)
	cli.sessions = cache
	if !cli.pickSession() {
		t.Fatal("no session to offer")
	}
	cli.offerEarly = early
	return cli, cli.hello()
}

// Early data over TCP is taken up to what the ticket allows, and ends with an
// EndOfEarlyData that is that alone; more, or a malformed one, is refused.
func TestS13EarlyDataOverTCP(t *testing.T) {
	config, pool := ecdsaServer(t)
	keys := newTicketKeys(&stdtls.Config{})
	for _, tc := range []struct {
		name  string
		early func(tx *recordKeys) []byte
		alert uint8
	}{
		{"what the ticket allows", func(tx *recordKeys) []byte {
			return append(s13Seal(t, tx, recordTypeApplicationData, []byte("0123456789")),
				s13Seal(t, tx, recordTypeHandshake, hsMsg(hsEndOfEarlyData, nil))...)
		}, 0},
		{"more than the ticket allows", func(tx *recordKeys) []byte {
			return s13Seal(t, tx, recordTypeApplicationData, []byte("0123456789a"))
		}, alertUnexpectedMessage},
		{"an EndOfEarlyData with a body", func(tx *recordKeys) []byte {
			return s13Seal(t, tx, recordTypeHandshake, hsMsg(hsEndOfEarlyData, []byte{0}))
		}, alertDecodeError},
		{"a message after the EndOfEarlyData in its record", func(tx *recordKeys) []byte {
			return s13Seal(t, tx, recordTypeHandshake, append(hsMsg(hsEndOfEarlyData, nil), hsFinished, 0, 0))
		}, alertDecodeError},
	} {
		// A ticket carries early data once: each case has its own.
		cli, hello := s13Resuming(t, pool, s13Session(t, config, pool, keys, true, nil), true)
		srv := newSrvHS(config, keys)
		srv.tcpEarly, srv.maxEarly = true, 10
		in := append(hello, tc.early(cli.earlyTx)...)
		consumed, _, _, herr := srv.process(in)
		if !srv.resumed || !srv.earlyData {
			t.Errorf("%s: resumed %v, early data %v", tc.name, srv.resumed, srv.earlyData)
		}
		if tc.alert != 0 {
			wantAlert(t, tc.name, herr, tc.alert)
			continue
		}
		if herr != nil || consumed != len(in) || srv.earlyOn || string(srv.takeEarly()) != "0123456789" {
			t.Errorf("%s: consumed %d of %d, still early %v, %v", tc.name, consumed, len(in), srv.earlyOn, herr)
		}
	}

	// A ticket made without early data resumes, and the early data that
	// comes with it is skipped.
	_, hello := s13Resuming(t, pool, s13Session(t, config, pool, keys, false, nil), true)
	srv := newSrvHS(config, keys)
	srv.tcpEarly, srv.maxEarly = true, 10
	if _, _, _, herr := srv.process(hello); herr != nil || !srv.resumed || srv.earlyData || srv.skipEarly != maxSkippedEarly {
		t.Fatalf("a ticket without early data: resumed %v, early data %v, skipping %d, %v", srv.resumed, srv.earlyData, srv.skipEarly, herr)
	}
}

// A ticket is not resumed from if the suite chosen now has another hash, or
// the Config now requires a client certificate the session lacks: the
// handshake is a full one.
func TestS13ResumeRefusesTicket(t *testing.T) {
	config, pool := ecdsaServer(t)
	keys := newTicketKeys(&stdtls.Config{})

	// The session is AES-256-GCM's, with SHA-384; the server prefers
	// AES-128-GCM, with SHA-256.
	cache := s13Session(t, config, pool, keys, false, func(c *clientHS) { c.suites = []uint16{stdtls.TLS_AES_256_GCM_SHA384} })
	cli, hello := s13Resuming(t, pool, cache, false)
	srv := newSrvHS(config, keys)
	if cliErr, srvErr := converse(t, cli, srv, hello); cliErr != nil || srvErr != nil {
		t.Fatalf("another hash: client %v, server %v", cliErr, srvErr)
	}
	if srv.resumed || cli.resumed || srv.suiteID != stdtls.TLS_AES_128_GCM_SHA256 {
		t.Fatalf("another hash: server resumed %v, client resumed %v, suite %#x", srv.resumed, cli.resumed, srv.suiteID)
	}

	required := config.Clone()
	required.ClientAuth = stdtls.RequireAnyClientCert
	_, hello = s13Resuming(t, pool, s13Session(t, config, pool, keys, false, nil), false)
	srv = newSrvHS(required, keys)
	if _, _, _, herr := srv.process(hello); herr != nil || srv.resumed || srv.state != srvAwaitClientCert {
		t.Fatalf("a certificate now required: resumed %v, state %d, %v", srv.resumed, srv.state, herr)
	}
}

// A client whose chain has more certificates than a ticket holds, 255, is
// sent no ticket; one with a single certificate is.
func TestS13NoTicketForLongClientChain(t *testing.T) {
	config, pool := ecdsaServer(t)
	config.ClientAuth = stdtls.RequestClientCert
	keys := newTicketKeys(&stdtls.Config{})
	// About the smallest certificate there is, so that 256 of them fit in
	// one handshake message.
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 256} {
		chain := &stdtls.Certificate{Certificate: slices.Repeat([][]byte{der}, n), PrivateKey: key}
		cli := newHS(t, pool, func(c *stdtls.Config) {
			c.GetClientCertificate = func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) { return chain, nil }
		})
		cli.sessions = NewQUICSessionCache(1)
		srv := newSrvHS(config, keys)
		want := 1
		if n > 255 {
			want = 0
		}
		if got := s13Converse(t, cli, srv, cli.hello()); len(srv.peerRaw) != n || got != want {
			t.Errorf("%d certificates: %d received, %d tickets, want %d", n, len(srv.peerRaw), got, want)
		}
	}
}

// What clientHello does with a ClientHello that peek would not have let
// through: it refuses it, as a handshake that cannot go on.
func TestS13ClientHelloPeekRuledOut(t *testing.T) {
	config, pool := ecdsaServer(t)
	var out []byte

	// No version to serve the client with.
	c := s13Hello(t)
	c.comp = []byte{1}
	herr := newSrvHS(config, nil).clientHello(c.msg(), &out)
	wantAlert(t, "no version", herr, alertProtocolVersion)

	// A QUIC handshake with a client that is not QUIC's.
	herr = newServerHSFor(config, nil, nil, &quicCtx{}).clientHello(s13Hello(t).msg(), &out)
	wantAlert(t, "not a QUIC client", herr, alertProtocolVersion)

	// GetCertificate, which TLS 1.2 needs to choose, has not answered.
	srv := newSrvHS(&stdtls.Config{GetCertificate: func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		return nil, errors.New("not called")
	}}, nil)
	srv.pick.async = func(func()) bool { return true }
	hello := helloOf(t, pool, func(c *stdtls.Config) { c.MaxVersion = stdtls.VersionTLS12 }, nil)
	herr = srv.clientHello(hello[recordHeaderLen:], &out)
	wantAlert(t, "GetCertificate waited for", herr, alertInternalError)
	if len(out) != 0 {
		t.Fatalf("%d bytes out", len(out))
	}
}
