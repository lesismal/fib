//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"slices"
	"testing"
	"time"
)

// The client handshake against a server that breaks the protocol: our own
// server makes a real answer, which a test takes apart, changes and hands the
// client, so that each check the client makes of what a server sends is met
// by a server that fails it.

// msgExt is an extension of a hand-made handshake message.
type msgExt struct {
	typ  uint16
	data []byte
}

func appendExts(b *builder, exts []msgExt) {
	b.vec16(func(b *builder) {
		for _, e := range exts {
			b.u16(e.typ)
			b.vec16(func(b *builder) { b.raw(e.data) })
		}
	})
}

// hsMsg is a handshake message of typ with body.
func hsMsg(typ byte, body []byte) []byte {
	return append([]byte{typ, byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))}, body...)
}

// u16s is the vector of 16-bit values with a 16-bit length a few extensions
// carry.
func u16s(values ...uint16) []byte {
	var b builder
	b.vec16(func(b *builder) {
		for _, v := range values {
			b.u16(v)
		}
	})
	return b.b
}

// helloFields are a ServerHello's, to be changed and made again.
type helloFields struct {
	version uint16
	random  []byte
	session []byte
	suite   uint16
	comp    byte
	exts    []msgExt
	trailer []byte
	// extBlock, if set, is the extension block as sent, in place of exts.
	extBlock []byte
}

func parseHelloFields(msg []byte) helloFields {
	r := newReader(msg[4:])
	f := helloFields{version: r.u16(), random: bytes.Clone(r.take(32)), session: bytes.Clone(r.vec8()), suite: r.u16(), comp: r.u8()}
	exts := newReader(r.vec16())
	for len(exts.b) > 0 && exts.ok {
		typ := exts.u16()
		f.exts = append(f.exts, msgExt{typ, bytes.Clone(exts.vec16())})
	}
	return f
}

func (f helloFields) bytes() []byte {
	var b builder
	b.u8(hsServerHello)
	b.u24(0)
	b.u16(f.version)
	b.raw(f.random)
	b.vec8(func(b *builder) { b.raw(f.session) })
	b.u16(f.suite)
	b.u8(f.comp)
	if f.extBlock != nil {
		b.vec16(func(b *builder) { b.raw(f.extBlock) })
	} else {
		appendExts(&b, f.exts)
	}
	b.raw(f.trailer)
	return finishMessage(&b)
}

// with is f with the extension typ set to data, in place of any it had.
func (f helloFields) with(typ uint16, data []byte) helloFields {
	f = f.without(typ)
	f.exts = append(f.exts, msgExt{typ, data})
	return f
}

func (f helloFields) without(typ uint16) helloFields {
	f.exts = slices.DeleteFunc(slices.Clone(f.exts), func(e msgExt) bool { return e.typ == typ })
	return f
}

// helloRetry is a HelloRetryRequest made from the fields of a ServerHello.
func (f helloFields) helloRetry(exts ...msgExt) helloFields {
	f.random = helloRetryRandom[:]
	f.exts = append([]msgExt{{extSupportedVersions, []byte{3, 4}}}, exts...)
	return f
}

// flight13 is our server's TLS 1.3 answer to a client's hello: the ServerHello,
// and the messages it protects, opened.
type flight13 struct {
	srv   *serverHS
	base  []byte // the transcript through the ServerHello
	hello []byte
	msgs  [][]byte
	key   crypto.Signer
}

// newFlight13 has a server with config answer cli's hello.
func newFlight13(t *testing.T, cli *clientHS, config *stdtls.Config) *flight13 {
	t.Helper()
	srv := newSrvHS(config, nil)
	hello := cli.hello()
	_, out, _, herr := srv.process(hello)
	if herr != nil {
		t.Fatalf("server: %v", herr)
	}
	f := &flight13{srv: srv}
	if len(config.Certificates) > 0 {
		f.key, _ = config.Certificates[0].PrivateKey.(crypto.Signer)
	}
	keys := f.keys()
	var plain []byte
	for len(out) >= recordHeaderLen {
		size := recordHeaderLen + (int(out[3])<<8 | int(out[4]))
		rec := out[:size]
		out = out[size:]
		switch rec[0] {
		case recordTypeHandshake:
			f.hello = bytes.Clone(rec[recordHeaderLen:])
		case recordTypeApplicationData:
			_, p, _, ok := keys.open(rec)
			if !ok {
				t.Fatal("the server's flight did not open")
			}
			plain = append(plain, p...)
		}
	}
	for len(plain) >= 4 {
		size := 4 + (int(plain[1])<<16 | int(plain[2])<<8 | int(plain[3]))
		f.msgs = append(f.msgs, bytes.Clone(plain[:size]))
		plain = plain[size:]
	}
	f.base = append(bytes.Clone(hello[recordHeaderLen:]), f.hello...)
	return f
}

// keys are the server's handshake keys, from the first record on.
func (f *flight13) keys() *recordKeys {
	k, _ := newKeys13(f.srv.suite, f.srv.suite.hash, f.srv.serverHS)
	return k
}

// records seals each message in a record of its own under fresh handshake
// keys.
func (f *flight13) records(msgs ...[]byte) []byte {
	keys := f.keys()
	var out []byte
	for _, m := range msgs {
		out, _ = keys.seal(out, recordTypeHandshake, m, nil)
	}
	return out
}

// resign makes the CertificateVerify and Finished among msgs right again for
// the messages before them, as a server whose flight that was would have.
func (f *flight13) resign(t *testing.T, msgs [][]byte) [][]byte {
	t.Helper()
	h := f.srv.suite.hash
	transcript := bytes.Clone(f.base)
	out := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		switch m[0] {
		case hsCertificateVerify:
			sig, err := signHandshake(f.key, f.srv.alg, rand.Reader, certificateVerifyContent(true, hashBytes(h, transcript)))
			if err != nil {
				t.Fatal(err)
			}
			m = appendCertificateVerify(f.srv.alg, sig)
		case hsFinished:
			key, _ := expandLabelContext(h, f.srv.serverHS, "finished", nil, h().Size())
			mac := hmac.New(h, key)
			mac.Write(hashBytes(h, transcript))
			m = hsMsg(hsFinished, mac.Sum(nil))
		}
		transcript = append(transcript, m...)
		out = append(out, m)
	}
	return out
}

// send hands cli the ServerHello hello in the clear and then protected, and
// returns what it made of them.
func (f *flight13) send(cli *clientHS, hello, protected []byte) (out []byte, done bool, herr *hsError) {
	in := append(appendRecord(nil, recordTypeHandshake, 0x0303, hello), protected...)
	_, out, done, herr = cli.process(in)
	return out, done, herr
}

// replace is msgs with the message of typ replaced by m, or m put in front
// of the message of type before if there is none of typ.
func replace(msgs [][]byte, typ byte, m []byte) [][]byte {
	out := slices.Clone(msgs)
	for i := range out {
		if out[i][0] == typ {
			out[i] = m
			return out
		}
	}
	return out
}

// insertBefore is msgs with m put in front of the first message of typ.
func insertBefore(msgs [][]byte, typ byte, m []byte) [][]byte {
	i := slices.IndexFunc(msgs, func(x []byte) bool { return x[0] == typ })
	return slices.Insert(slices.Clone(msgs), i, m)
}

func wantAlert(t *testing.T, name string, herr *hsError, alert uint8) {
	t.Helper()
	if herr == nil {
		t.Errorf("%s: accepted", name)
		return
	}
	if herr.alert != alert {
		t.Errorf("%s: alert %d (%v), want %d", name, herr.alert, herr, alert)
	}
}

func ecdsaServer(t *testing.T) (*stdtls.Config, *x509.CertPool) {
	t.Helper()
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	return &stdtls.Config{Certificates: []stdtls.Certificate{cert}, NextProtos: []string{"h2"}}, pool
}

// A ServerHello that is malformed, picks what was not offered, or carries what
// a ServerHello may not is refused with the alert crypto/tls sends for it.
func TestClientRefusesBadServerHello(t *testing.T) {
	config, pool := ecdsaServer(t)
	keyShare := func(group stdtls.CurveID, share []byte) []byte {
		var b builder
		b.u16(uint16(group))
		b.vec16(func(b *builder) { b.raw(share) })
		return b.b
	}
	for _, tc := range []struct {
		name  string
		tweak func(*clientHS)
		edit  func(helloFields, *clientHS) helloFields
		alert uint8
	}{
		{"trailing bytes", nil, func(f helloFields, _ *clientHS) helloFields { f.trailer = []byte{0}; return f }, alertDecodeError},
		{"an extension twice", nil, func(f helloFields, _ *clientHS) helloFields { f.exts = append(f.exts, f.exts[0]); return f }, alertIllegalParameter},
		{"a malformed extension block", nil, func(f helloFields, _ *clientHS) helloFields { f.extBlock = []byte{0, 43, 0}; return f }, alertDecodeError},
		{"compression", nil, func(f helloFields, _ *clientHS) helloFields { f.comp = 1; return f }, alertIllegalParameter},
		{"TLS 1.2 in supported_versions", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extSupportedVersions, []byte{3, 3}) }, alertProtocolVersion},
		{"no supported_versions to a TLS 1.3 client", nil, func(f helloFields, _ *clientHS) helloFields { return f.without(extSupportedVersions) }, alertProtocolVersion},
		{"a legacy version other than TLS 1.2", nil, func(f helloFields, _ *clientHS) helloFields { f.version = 0x0302; return f }, alertProtocolVersion},
		{"the session ID not echoed", nil, func(f helloFields, _ *clientHS) helloFields { f.session = nil; return f }, alertIllegalParameter},
		{"a cookie", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extCookie, []byte{0, 1, 7}) }, alertUnsupportedExtension},
		{"pre_shared_key with no session offered", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extPreSharedKey, []byte{0, 0}) }, alertUnsupportedExtension},
		{"an unknown extension", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(0x1234, nil) }, alertUnsupportedExtension},
		{"a malformed key share", nil, func(f helloFields, _ *clientHS) helloFields { return f.with(extKeyShare, []byte{0, 0x1d, 0}) }, alertDecodeError},
		// TLS_AES_128_CCM_SHA256 (RFC 8446 appendix B.4), which this client
		// does not offer and the record layer has no AEAD for.
		{"a suite not offered", nil, func(f helloFields, _ *clientHS) helloFields { f.suite = 0x1304; return f }, alertIllegalParameter},
		{"an offered suite the record layer lacks", func(h *clientHS) { h.suites = append(slices.Clone(h.suites), 0x1304) },
			func(f helloFields, _ *clientHS) helloFields { f.suite = 0x1304; return f }, alertIllegalParameter},
		{"a share of another group", nil, func(f helloFields, _ *clientHS) helloFields {
			return f.with(extKeyShare, keyShare(stdtls.CurveP256, make([]byte, 65)))
		}, alertIllegalParameter},
		{"a share that is no point", nil, func(f helloFields, _ *clientHS) helloFields {
			return f.with(extKeyShare, keyShare(stdtls.X25519, []byte{1, 2, 3}))
		}, alertIllegalParameter},
		{"a share of low order", nil, func(f helloFields, _ *clientHS) helloFields {
			return f.with(extKeyShare, keyShare(stdtls.X25519, make([]byte, 32)))
		}, alertIllegalParameter},
	} {
		cli := newHS(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"} })
		if tc.tweak != nil {
			tc.tweak(cli)
		}
		f := newFlight13(t, cli, config)
		_, _, herr := f.send(cli, tc.edit(parseHelloFields(f.hello), cli).bytes(), nil)
		wantAlert(t, tc.name, herr, tc.alert)
	}
}

// A HelloRetryRequest that is malformed, asks for nothing or for what cannot
// be given, or comes twice is refused; one with a cookie has it sent back; a
// ServerHello after it may not change the suite; and a session whose hash
// differs from the suite the server chose is offered no more.
func TestClientRefusesBadHelloRetryRequest(t *testing.T) {
	config, pool := ecdsaServer(t)
	group := func(id stdtls.CurveID) msgExt { return msgExt{extKeyShare, []byte{byte(id >> 8), byte(id)}} }
	retry := func(t *testing.T, tweak func(*clientHS), exts ...msgExt) (*clientHS, *flight13, []byte, *hsError) {
		cli := newHS(t, pool, nil)
		if tweak != nil {
			tweak(cli)
		}
		f := newFlight13(t, cli, config)
		hrr := parseHelloFields(f.hello).helloRetry(exts...)
		hrr.suite = stdtls.TLS_AES_128_GCM_SHA256
		out, _, herr := f.send(cli, hrr.bytes(), nil)
		return cli, f, out, herr
	}

	_, _, _, herr := retry(t, nil, msgExt{extKeyShare, []byte{0, 0x17, 0}})
	wantAlert(t, "a malformed key_share", herr, alertDecodeError)
	_, _, _, herr = retry(t, nil)
	wantAlert(t, "nothing asked for", herr, alertIllegalParameter)
	_, _, _, herr = retry(t, nil, group(stdtls.X25519))
	wantAlert(t, "the group already offered", herr, alertIllegalParameter)
	_, _, _, herr = retry(t, nil, group(stdtls.CurveP521))
	wantAlert(t, "a group not offered", herr, alertIllegalParameter)

	cookie := []byte("a cookie of the server's")
	cli, f, out, herr := retry(t, nil, group(stdtls.CurveP256), msgExt{extCookie, append([]byte{0, byte(len(cookie))}, cookie...)})
	if herr != nil || !bytes.Contains(out, cookie) {
		t.Fatalf("cookie: %v, sent back: %v", herr, bytes.Contains(out, cookie))
	}
	again := parseHelloFields(f.hello).helloRetry(group(stdtls.CurveP384))
	_, _, herr = f.send(cli, again.bytes(), nil)
	wantAlert(t, "a second HelloRetryRequest", herr, alertUnexpectedMessage)

	cli, f, _, herr = retry(t, nil, group(stdtls.CurveP256))
	if herr != nil {
		t.Fatal(herr)
	}
	sh := parseHelloFields(f.hello)
	sh.suite = stdtls.TLS_AES_256_GCM_SHA384
	_, _, herr = f.send(cli, sh.bytes(), nil)
	wantAlert(t, "another suite after the HelloRetryRequest", herr, alertIllegalParameter)

	session := func(h *clientHS) {
		h.sessions = NewQUICSessionCache(1)
		h.session = &QUICSession{ticket: []byte("ticket"), psk: make([]byte, 48), suite: stdtls.TLS_AES_256_GCM_SHA384,
			received: time.Now(), lifetime: time.Hour}
	}
	cli, _, _, herr = retry(t, session, group(stdtls.CurveP256))
	if herr != nil || cli.session != nil {
		t.Fatalf("%v: the session with the other hash is still offered: %v", herr, cli.session != nil)
	}
}

// A ServerHello that takes a session takes the one offered, with a suite of
// its hash.
func TestClientRefusesBadPreSharedKey(t *testing.T) {
	config, pool := ecdsaServer(t)
	for _, tc := range []struct {
		name  string
		index uint16
		suite uint16
	}{
		{"an index not offered", 1, stdtls.TLS_AES_256_GCM_SHA384},
		{"a suite of another hash", 0, stdtls.TLS_AES_128_GCM_SHA256},
	} {
		cli := newHS(t, pool, func(c *stdtls.Config) {})
		cli.sessions = NewQUICSessionCache(1)
		cli.session = &QUICSession{ticket: []byte("ticket"), psk: make([]byte, 48), suite: stdtls.TLS_AES_256_GCM_SHA384,
			received: time.Now(), lifetime: time.Hour}
		f := newFlight13(t, cli, config)
		sh := parseHelloFields(f.hello).with(extPreSharedKey, []byte{byte(tc.index >> 8), byte(tc.index)})
		sh.suite = tc.suite
		_, _, herr := f.send(cli, sh.bytes(), nil)
		wantAlert(t, tc.name, herr, alertIllegalParameter)
	}
}

// What follows the ServerHello must be protected, whole, in order, and well
// formed: each way a server can get that wrong is refused.
func TestClientRefusesBadServerFlight(t *testing.T) {
	config, pool := ecdsaServer(t)
	ee := func(exts ...msgExt) []byte {
		var b builder
		appendExts(&b, exts)
		return hsMsg(hsEncryptedExtensions, b.b)
	}
	alpn := func(names ...string) []byte {
		var b builder
		b.vec16(func(b *builder) {
			for _, n := range names {
				b.vec8(func(b *builder) { b.raw([]byte(n)) })
			}
		})
		return b.b
	}
	cr := func(ctx []byte, exts ...msgExt) []byte {
		var b builder
		b.vec8(func(b *builder) { b.raw(ctx) })
		appendExts(&b, exts)
		return hsMsg(hsCertificateRequest, b.b)
	}
	sigAlgs := msgExt{extSignatureAlgs, u16s(0x0403)}
	type step struct {
		name      string
		protected func(f *flight13) []byte
		alert     uint8
		remote    bool
	}
	msgs := func(edit func(f *flight13) [][]byte) func(f *flight13) []byte {
		return func(f *flight13) []byte { return f.records(edit(f)...) }
	}
	for _, tc := range []step{
		{"a record that does not authenticate", func(f *flight13) []byte {
			r := f.records(f.msgs...)
			r[len(r)-1] ^= 1
			return r
		}, alertBadRecordMAC, false},
		{"an alert under the keys", func(f *flight13) []byte {
			r, _ := f.keys().seal(nil, recordTypeAlert, []byte{alertLevelError, alertHandshakeFailure}, nil)
			return r
		}, 0, true},
		{"application data during the handshake", func(f *flight13) []byte {
			r, _ := f.keys().seal(nil, recordTypeApplicationData, []byte("early"), nil)
			return r
		}, alertUnexpectedMessage, false},
		{"an alert in the clear", func(*flight13) []byte {
			return appendRecord(nil, recordTypeAlert, 0x0303, []byte{alertLevelError, alertHandshakeFailure})
		}, alertUnexpectedMessage, false},
		{"a handshake record in the clear", func(f *flight13) []byte {
			return appendRecord(nil, recordTypeHandshake, 0x0303, f.msgs[0])
		}, alertUnexpectedMessage, false},
		{"an unknown record type", func(*flight13) []byte { return appendRecord(nil, 99, 0x0303, []byte{1}) }, alertUnexpectedMessage, false},
		{"a record version that is not TLS's", func(*flight13) []byte { return appendRecord(nil, recordTypeApplicationData, 0x0203, []byte{1}) }, alertProtocolVersion, false},
		{"a change_cipher_spec of two bytes", func(*flight13) []byte { return appendRecord(nil, recordTypeChangeCipherSpec, 0x0303, []byte{1, 1}) }, alertUnexpectedMessage, false},
		{"a message larger than allowed", func(f *flight13) []byte {
			r, _ := f.keys().seal(nil, recordTypeHandshake, []byte{hsEncryptedExtensions, 1, 0, 1}, nil)
			return r
		}, alertInternalError, false},
		{"a message out of order", msgs(func(f *flight13) [][]byte { return f.msgs[1:] }), alertUnexpectedMessage, false},
		{"EncryptedExtensions with trailing bytes", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsEncryptedExtensions, hsMsg(hsEncryptedExtensions, []byte{0, 0, 0}))
		}), alertDecodeError, false},
		{"EncryptedExtensions with an extension twice", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsEncryptedExtensions, ee(msgExt{0x1234, nil}, msgExt{0x1234, nil}))
		}), alertDecodeError, false},
		{"a malformed ALPN extension", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsEncryptedExtensions, ee(msgExt{extALPN, alpn("h2", "h3")}))
		}), alertDecodeError, false},
		{"an ALPN protocol not offered", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsEncryptedExtensions, ee(msgExt{extALPN, alpn("spdy")}))
		}), alertNoApplicationProtocol, false},
		{"early data that was not offered", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsEncryptedExtensions, ee(msgExt{extEarlyData, nil}))
		}), alertUnsupportedExtension, false},
		{"a CertificateRequest cut short", msgs(func(f *flight13) [][]byte {
			return insertBefore(f.msgs, hsCertificate, hsMsg(hsCertificateRequest, []byte{0}))
		}), alertDecodeError, false},
		{"a CertificateRequest with an extension twice", msgs(func(f *flight13) [][]byte {
			return insertBefore(f.msgs, hsCertificate, cr(nil, sigAlgs, sigAlgs))
		}), alertIllegalParameter, false},
		{"a CertificateRequest with malformed schemes", msgs(func(f *flight13) [][]byte {
			return insertBefore(f.msgs, hsCertificate, cr(nil, msgExt{extSignatureAlgs, []byte{0, 3, 4, 3, 0}}))
		}), alertDecodeError, false},
		{"a CertificateRequest without schemes", msgs(func(f *flight13) [][]byte {
			return insertBefore(f.msgs, hsCertificate, cr(nil))
		}), alertMissingExtension, false},
		{"a CertificateRequest with malformed authorities", msgs(func(f *flight13) [][]byte {
			return insertBefore(f.msgs, hsCertificate, cr(nil, sigAlgs, msgExt{extCertAuthorities, []byte{0, 3, 0, 9, 1}}))
		}), alertDecodeError, false},
		{"a Certificate with a context", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificate, hsMsg(hsCertificate, []byte{1, 7, 0, 0, 0}))
		}), alertDecodeError, false},
		{"a Certificate entry with no certificate", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificate, hsMsg(hsCertificate, []byte{0, 0, 0, 5, 0, 0, 0, 0, 0}))
		}), alertDecodeError, false},
		{"a certificate that does not parse", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificate, hsMsg(hsCertificate, []byte{0, 0, 0, 8, 0, 0, 3, 1, 2, 3, 0, 0}))
		}), alertBadCertificate, false},
		{"an empty certificate chain", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificate, hsMsg(hsCertificate, []byte{0, 0, 0, 0}))
		}), alertDecodeError, false},
		{"a CertificateVerify with trailing bytes", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificateVerify, hsMsg(hsCertificateVerify, []byte{4, 3, 0, 0, 0}))
		}), alertDecodeError, false},
		{"a CertificateVerify with a scheme not offered", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificateVerify, hsMsg(hsCertificateVerify, []byte{4, 1, 0, 0}))
		}), alertIllegalParameter, false},
		{"a CertificateVerify with an unknown scheme", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificateVerify, hsMsg(hsCertificateVerify, []byte{9, 9, 0, 0}))
		}), alertIllegalParameter, false},
		{"a signature that does not verify", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsCertificateVerify, appendCertificateVerify(0x0403, []byte{0x30, 0}))
		}), alertDecryptError, false},
		{"a Finished that does not verify", msgs(func(f *flight13) [][]byte {
			return replace(f.msgs, hsFinished, hsMsg(hsFinished, make([]byte, 32)))
		}), alertDecryptError, false},
	} {
		cli := newHS(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"} })
		f := newFlight13(t, cli, config)
		_, _, herr := f.send(cli, f.hello, tc.protected(f))
		if tc.remote {
			var alert stdtls.AlertError
			if herr == nil || herr.alert != 0 || !errors.As(herr, &alert) {
				t.Errorf("%s: %v, want the server's alert", tc.name, herr)
			}
			continue
		}
		wantAlert(t, tc.name, herr, tc.alert)
	}
}

// A server's flight may split a message across records, and a message may
// not run on past the end of a flight. Alerts in the clear before the
// ServerHello are the server's, or malformed.
func TestClientReassemblesServerFlight(t *testing.T) {
	config, pool := ecdsaServer(t)
	cli := newHS(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"} })
	f := newFlight13(t, cli, config)
	ee := f.msgs[0]
	split := append([][]byte{ee[:5], ee[5:]}, f.msgs[1:]...)
	if _, done, herr := f.send(cli, f.hello, f.records(split...)); herr != nil || !done {
		t.Fatalf("a message split across records: done %v, %v", done, herr)
	}

	cli = newHS(t, pool, nil)
	f = newFlight13(t, cli, config)
	in := appendRecord(nil, recordTypeHandshake, 0x0303, append(bytes.Clone(f.hello), hsEncryptedExtensions, 0))
	_, _, _, herr := cli.process(in)
	wantAlert(t, "a message behind the ServerHello in its record", herr, alertUnexpectedMessage)

	for _, tc := range []struct {
		name  string
		alert []byte
	}{
		{"close_notify", []byte{alertLevelWarning, alertCloseNotify}},
		{"a malformed alert", []byte{alertLevelError}},
	} {
		cli = newHS(t, pool, nil)
		cli.hello()
		_, _, _, herr := cli.process(appendRecord(nil, recordTypeAlert, 0x0303, tc.alert))
		if herr == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
	cli = newHS(t, pool, nil)
	cli.hello()
	_, _, _, herr = cli.process(appendRecord(nil, recordTypeApplicationData, 0x0303, []byte{1}))
	wantAlert(t, "a protected record before the ServerHello", herr, alertUnexpectedMessage)
}

// failingSigner is a key whose signatures fail, as a hardware key's can.
type failingSigner struct{ crypto.Signer }

func (failingSigner) Sign(io.Reader, []byte, crypto.SignerOpts) ([]byte, error) {
	return nil, errors.New("the key is gone")
}

// The client's side of client authentication, against a CertificateRequest
// its certificate cannot answer, with a key that fails, or with
// GetClientCertificate returning nothing; and a VerifyConnection that refuses.
func TestClientCertificateFailures13(t *testing.T) {
	config, pool := ecdsaServer(t)
	ca := newTestCA(t, "client CA")
	clientCert := ca.issue(t, "ecdsa-p256", "client", true)
	cr := func(algs ...uint16) []byte {
		var b builder
		b.vec8(func(b *builder) {})
		appendExts(&b, []msgExt{{extSignatureAlgs, u16s(algs...)}})
		return hsMsg(hsCertificateRequest, b.b)
	}
	run := func(name string, get func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error), algs []uint16) ([]byte, bool, *hsError) {
		cli := newHS(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"}; c.GetClientCertificate = get })
		f := newFlight13(t, cli, config)
		msgs := f.resign(t, insertBefore(f.msgs, hsCertificate, cr(algs...)))
		return f.send(cli, f.hello, f.records(msgs...))
	}

	_, _, herr := run("no scheme for the key", func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
		return &clientCert, nil
	}, []uint16{0x0804})
	wantAlert(t, "a certificate the request does not allow", herr, alertHandshakeFailure)

	broken := clientCert
	broken.PrivateKey = failingSigner{clientCert.PrivateKey.(crypto.Signer)}
	_, _, herr = run("a key that fails", func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
		return &broken, nil
	}, []uint16{0x0403})
	wantAlert(t, "a key that cannot sign", herr, alertInternalError)

	_, _, herr = run("a certificate without a key", func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
		return &stdtls.Certificate{Certificate: clientCert.Certificate, PrivateKey: "no key"}, nil
	}, []uint16{0x0403})
	wantAlert(t, "a certificate whose key is not a signer", herr, alertInternalError)

	out, done, herr := run("no certificate", func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
		return nil, nil
	}, []uint16{0x0403})
	if herr != nil || !done || len(out) == 0 {
		t.Fatalf("GetClientCertificate with no certificate: done %v, %v", done, herr)
	}

	refused := errors.New("refused")
	cli := newHS(t, pool, func(c *stdtls.Config) {
		c.VerifyConnection = func(stdtls.ConnectionState) error { return refused }
	})
	f := newFlight13(t, cli, config)
	if _, _, herr = f.send(cli, f.hello, f.records(f.msgs...)); herr == nil || !errors.Is(herr, refused) || herr.alert != alertBadCertificate {
		t.Fatalf("VerifyConnection: %v", herr)
	}
}

// A chain with an intermediate is verified through it.
func TestClientVerifiesThroughIntermediates(t *testing.T) {
	ca := newTestCA(t, "root")
	leaf := ca.issue(t, "ecdsa-p256", "localhost", false)
	leaf.Certificate = append(leaf.Certificate, ca.cert.Raw)
	cli := newHS(t, ca.pool, nil)
	f := newFlight13(t, cli, &stdtls.Config{Certificates: []stdtls.Certificate{leaf}})
	if _, done, herr := f.send(cli, f.hello, f.records(f.msgs...)); herr != nil || !done || len(cli.chains) == 0 {
		t.Fatalf("done %v, %v, %d chains", done, herr, len(cli.chains))
	}
}
