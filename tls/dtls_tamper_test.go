package tls

import (
	"bytes"
	"crypto"
	stdtls "crypto/tls"
	"errors"
	"testing"
	"time"
)

// dtlsAlert is the alert a DTLS connection failed with, or -1.
func dtlsAlert(err error) int {
	var herr *hsError
	if errors.As(err, &herr) {
		return int(herr.alert)
	}
	return -1
}

// failingReader is a Rand that has nothing to give.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("no entropy") }

// dtlsRecords splits a datagram into its records.
func dtlsRecords(b []byte) [][]byte {
	var recs [][]byte
	for len(b) >= dtlsRecordHeaderLen {
		n := dtlsRecordHeaderLen + (int(b[11])<<8 | int(b[12]))
		recs = append(recs, b[:n])
		b = b[n:]
	}
	return recs
}

// withoutEpoch1 is b without its records of epoch 1: a flight without its
// Finished.
func withoutEpoch1(b []byte) []byte {
	var out []byte
	for _, rec := range dtlsRecords(b) {
		if rec[3] != 0 || rec[4] != 0 {
			continue
		}
		out = append(out, rec...)
	}
	return out
}

// hsRecord is an epoch 0 record carrying one handshake fragment.
func hsRecord(recSeq byte, typ byte, length int, msgSeq uint16, off int, frag []byte) []byte {
	n := len(frag)
	body := []byte{typ, byte(length >> 16), byte(length >> 8), byte(length), byte(msgSeq >> 8), byte(msgSeq),
		byte(off >> 16), byte(off >> 8), byte(off), byte(n >> 16), byte(n >> 8), byte(n)}
	body = append(body, frag...)
	rec := []byte{recordTypeHandshake, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, recSeq, byte(len(body) >> 8), byte(len(body))}
	return append(rec, body...)
}

func ext(typ uint16, data ...byte) []byte {
	return append([]byte{byte(typ >> 8), byte(typ), byte(len(data) >> 8), byte(len(data))}, data...)
}

// serverHelloMsg is a ServerHello in its TLS form, with the extension block
// exts if it is not nil.
func serverHelloMsg(version, suite uint16, compression byte, exts []byte) []byte {
	b := []byte{byte(version >> 8), byte(version)}
	b = append(b, make([]byte, 32)...)
	b = append(b, 0, byte(suite>>8), byte(suite), compression)
	if exts != nil {
		b = append(b, byte(len(exts)>>8), byte(len(exts)))
		b = append(b, exts...)
	}
	n := len(b)
	return append([]byte{hsServerHello, byte(n >> 16), byte(n >> 8), byte(n)}, b...)
}

// TestDTLSServerHelloChecks checks that a client refuses a ServerHello that
// does not answer what it offered, with the alert crypto/tls sends.
func TestDTLSServerHelloChecks(t *testing.T) {
	const suite = stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
	cat := func(bs ...[]byte) []byte { return bytes.Join(bs, nil) }
	cases := []struct {
		name  string
		msg   []byte
		alert int
	}{
		{"trailing bytes", append(serverHelloMsg(VersionDTLS12, suite, 0, nil), 0), alertDecodeError},
		{"TLS 1.2", serverHelloMsg(stdtls.VersionTLS12, suite, 0, nil), alertProtocolVersion},
		{"compression", serverHelloMsg(VersionDTLS12, suite, 1, nil), alertIllegalParameter},
		{"suite not offered", serverHelloMsg(VersionDTLS12, stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256, 0, nil), alertIllegalParameter},
		{"duplicate extension", serverHelloMsg(VersionDTLS12, suite, 0, cat(ext(extExtendedMasterSec), ext(extExtendedMasterSec))), alertIllegalParameter},
		{"renegotiation", serverHelloMsg(VersionDTLS12, suite, 0, ext(extRenegotiationInfo, 1, 0)), alertHandshakeFailure},
		{"extended master secret with data", serverHelloMsg(VersionDTLS12, suite, 0, ext(extExtendedMasterSec, 0)), alertDecodeError},
		{"compressed points only", serverHelloMsg(VersionDTLS12, suite, 0, ext(extECPointFormats, 1, 1)), alertIllegalParameter},
		{"empty ALPN protocol", serverHelloMsg(VersionDTLS12, suite, 0, ext(extALPN, 0, 1, 0)), alertDecodeError},
		{"ALPN not offered", serverHelloMsg(VersionDTLS12, suite, 0, ext(extALPN, 0, 2, 1, 'x')), alertNoApplicationProtocol},
		{"use_srtp without MKI", serverHelloMsg(VersionDTLS12, suite, 0, ext(extUseSRTP, 0, 2, 0, 7)), alertDecodeError},
		{"SRTP profile not offered", serverHelloMsg(VersionDTLS12, suite, 0, ext(extUseSRTP, 0, 2, 0, 1, 0)), alertIllegalParameter},
		{"SRTP MKI", serverHelloMsg(VersionDTLS12, suite, 0, ext(extUseSRTP, 0, 2, 0, 7, 1, 9)), alertIllegalParameter},
		{"server_name with data", serverHelloMsg(VersionDTLS12, suite, 0, ext(extServerName, 0)), alertDecodeError},
		{"unsolicited extension", serverHelloMsg(VersionDTLS12, suite, 0, ext(extSessionTicket)), alertUnsupportedExtension},
		{"accepted", serverHelloMsg(VersionDTLS12, suite, 0, cat(ext(extServerName), ext(extALPN, 0, 2, 1, 'a'),
			ext(extUseSRTP, 0, 2, 0, 7, 0), ext(extECPointFormats, 1, 0))), -1},
	}
	cc, _ := webrtcConfigs(t, "ecdsa", "ecdsa")
	cc.NextProtos = []string{"a"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) {}, SRTPProfiles: []uint16{SRTP_AEAD_AES_128_GCM}})
			if err != nil {
				t.Fatal(err)
			}
			c.Start(time.Now())
			herr := c.serverHello(tc.msg)
			if got := -1; herr != nil {
				got = int(herr.alert)
				if got != tc.alert {
					t.Fatalf("alert %d (%v), want %d", got, herr, tc.alert)
				}
			} else if tc.alert != -1 {
				t.Fatalf("accepted, want alert %d", tc.alert)
			}
			if tc.alert == -1 && (c.cli.protocol != "a" || !c.srtpOK || c.srtpProfile != SRTP_AEAD_AES_128_GCM) {
				t.Fatalf("protocol %q, SRTP %v %#04x", c.cli.protocol, c.srtpOK, c.srtpProfile)
			}
		})
	}
}

// TestDTLSClientMessageChecks checks the client's refusals of the server's
// messages out of turn or malformed.
func TestDTLSClientMessageChecks(t *testing.T) {
	cc, _ := webrtcConfigs(t, "ecdsa", "ecdsa")
	newClient := func(t *testing.T, state dtlsState) *DTLSConn {
		c, err := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) {}})
		if err != nil {
			t.Fatal(err)
		}
		c.Start(time.Now())
		c.state = state
		return c
	}
	cases := []struct {
		name  string
		state dtlsState
		typ   byte
		body  []byte
		alert int
	}{
		{"malformed HelloVerifyRequest", dtlsAwaitServerHello, hsHelloVerifyRequest, []byte{0xfe, 0xff, 4, 1}, alertDecodeError},
		{"malformed Certificate", dtlsAwaitCertificate, hsCertificate, []byte{0, 0, 9}, alertDecodeError},
		{"malformed ServerKeyExchange", dtlsAwaitServerKeyExchange, hsServerKeyExchange, []byte{3, 0}, alertDecodeError},
		{"malformed CertificateRequest", dtlsAwaitCertificateRequestOrDone, hsCertificateRequest, []byte{9}, alertDecodeError},
		{"ServerHelloDone with a body", dtlsAwaitServerHelloDone, hsServerHelloDone, []byte{0}, alertDecodeError},
		{"Certificate out of turn", dtlsAwaitServerHello, hsCertificate, nil, alertUnexpectedMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, tc.state)
			herr := c.clientMessage(&dtlsMessage{typ: tc.typ, body: tc.body}, time.Now())
			if herr == nil || int(herr.alert) != tc.alert {
				t.Fatalf("got %v, want alert %d", herr, tc.alert)
			}
		})
	}
	t.Run("HelloVerifyRequest loop", func(t *testing.T) {
		// A server that asks for a cookie again and again is given up on.
		var hellos int
		c, _ := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) { hellos++ }})
		c.Start(time.Now())
		var herr *hsError
		for i := 0; i < 10 && herr == nil; i++ {
			herr = c.clientMessage(&dtlsMessage{typ: hsHelloVerifyRequest, body: []byte{0xfe, 0xff, 1, byte(i)}}, time.Now())
		}
		if herr == nil || herr.alert != alertUnexpectedMessage || hellos != 5 {
			t.Fatalf("after %d hellos: %v", hellos, herr)
		}
	})
	t.Run("server key share", func(t *testing.T) {
		for _, tc := range []struct {
			curve stdtls.CurveID
			pub   []byte
		}{
			{stdtls.CurveP256, []byte{4, 1, 2, 3}},
			// The X25519 point of order one makes no shared secret.
			{stdtls.X25519, make([]byte, 32)},
		} {
			c := newClient(t, dtlsAwaitServerHelloDone)
			c.cli.suite, _ = lookupSuite(stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
			c.cli.skxCurve, c.cli.skxPub = tc.curve, tc.pub
			if herr := c.clientFlight(time.Now()); herr == nil || herr.alert != alertIllegalParameter {
				t.Fatalf("%v: %v", tc.curve, herr)
			}
		}
	})
}

// TestDTLSFinishedChecks checks that each side refuses a Finished that does
// not hash the transcript, and anything other than it at its turn.
func TestDTLSFinishedChecks(t *testing.T) {
	for side := range 2 {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		n := newDTLSNet(t, 20)
		// The peer's Finished never arrives.
		n.drop = func(to int, b []byte) bool {
			if to == side {
				if rest := withoutEpoch1(b); len(rest) != len(b) {
					if len(rest) > 0 {
						n.queue = append(n.queue, dtlsPacket{to: to, at: n.now.Add(n.latency), data: rest})
					}
					return true
				}
			}
			return false
		}
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		want := []dtlsState{dtlsAwaitServerFinished, dtlsAwaitClientFinished}[side]
		for n.conns[side].state != want && n.step() {
		}
		c := n.conns[side]
		if c.state != want {
			t.Fatalf("side %d stuck in state %v", side, c.state)
		}
		handle := c.serverMessage
		if side == 0 {
			handle = c.clientMessage
		}
		if herr := handle(&dtlsMessage{typ: hsServerHelloDone}, n.now); herr == nil || herr.alert != alertUnexpectedMessage {
			t.Fatalf("side %d, a message out of turn: %v", side, herr)
		}
		if herr := handle(&dtlsMessage{typ: hsFinished, body: make([]byte, 12)}, n.now); herr == nil || herr.alert != alertDecryptError {
			t.Fatalf("side %d, a forged Finished: %v", side, herr)
		}
	}
}

// TestDTLSVerifyConnection checks that either side's VerifyConnection can
// refuse the connection at the end of the handshake.
func TestDTLSVerifyConnection(t *testing.T) {
	refuse := func(stdtls.ConnectionState) error { return errors.New("not this one") }
	for side := range 2 {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		[]*stdtls.Config{cc, sc}[side].VerifyConnection = refuse
		n := newDTLSNet(t, 21)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		if n.errs[side] == nil || n.errs[side].Error() != "not this one" || n.conns[side].HandshakeComplete() {
			t.Fatalf("side %d: %v", side, n.errs[side])
		}
	}
}

// TestDTLSSigningFails checks that a side whose key cannot sign fails the
// handshake rather than sending a flight without its signature.
func TestDTLSSigningFails(t *testing.T) {
	for side := range 2 {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		cfg := []*stdtls.Config{cc, sc}[side]
		cert := cfg.Certificates[0]
		cert.PrivateKey = failingSigner{cert.PrivateKey.(crypto.Signer)}
		cfg.Certificates = []stdtls.Certificate{cert}
		n := newDTLSNet(t, 22)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		if dtlsAlert(n.errs[side]) != alertInternalError {
			t.Fatalf("side %d: %v", side, n.errs[side])
		}
	}
}

// TestDTLSClientCertificateVerify checks that a server refuses a client
// whose CertificateVerify is not signed by its certificate's key.
func TestDTLSClientCertificateVerify(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	other := dtlsCert(t, "ecdsa")
	cert := cc.Certificates[0]
	cert.PrivateKey = other.PrivateKey
	cc.Certificates = []stdtls.Certificate{cert}
	n := newDTLSNet(t, 23)
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	n.handshake(time.Minute)
	if n.errs[1] == nil || n.conns[1].HandshakeComplete() {
		t.Fatalf("server: %v", n.errs[1])
	}
	if n.drain(); n.errs[0] == nil {
		t.Fatal("client did not hear of the failure")
	}
}

// TestDTLSServerClientHelloChecks checks the server's refusals of a
// ClientHello that carries its cookie.
func TestDTLSServerClientHelloChecks(t *testing.T) {
	const suite = stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
	hello := func(version uint16, suites []uint16, exts []byte) []byte {
		b := []byte{byte(version >> 8), byte(version)}
		b = append(b, make([]byte, 32)...)
		b = append(b, 0, 0) // no session ID, no cookie
		b = append(b, 0, byte(2*len(suites)))
		for _, s := range suites {
			b = append(b, byte(s>>8), byte(s))
		}
		b = append(b, 1, 0)
		if exts != nil {
			b = append(b, byte(len(exts)>>8), byte(len(exts)))
			b = append(b, exts...)
		}
		return b
	}
	groups := ext(extSupportedGroups, 0, 2, 0, byte(stdtls.CurveP256))
	algs := ext(extSignatureAlgs, 0, 2, 4, 3)
	points := ext(extECPointFormats, 1, 0)
	usual := bytes.Join([][]byte{groups, algs, points}, nil)
	_, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	cases := []struct {
		name  string
		body  []byte
		cfg   func(c *stdtls.Config)
		alert int
	}{
		{"no suites", hello(VersionDTLS12, nil, nil)[:36], nil, alertDecodeError},
		{"TLS 1.2", hello(stdtls.VersionTLS12, []uint16{suite}, usual), nil, alertProtocolVersion},
		{"renegotiation", hello(VersionDTLS12, []uint16{suite}, append(ext(extRenegotiationInfo, 1, 0), usual...)), nil, alertHandshakeFailure},
		{"ALPN", hello(VersionDTLS12, []uint16{suite}, append(ext(extALPN, 0, 2, 1, 'b'), usual...)),
			func(c *stdtls.Config) { c.NextProtos = []string{"a"} }, alertNoApplicationProtocol},
		{"no common suite", hello(VersionDTLS12, []uint16{stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256}, usual), nil, alertHandshakeFailure},
		{"certificate", hello(VersionDTLS12, []uint16{suite}, usual), func(c *stdtls.Config) {
			c.Certificates = nil
			c.GetCertificate = func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) { return nil, errors.New("none today") }
		}, alertInternalError},
		{"use_srtp", hello(VersionDTLS12, []uint16{suite}, append(ext(extUseSRTP, 0, 1, 7, 0), usual...)), nil, alertDecodeError},
		{"random", hello(VersionDTLS12, []uint16{suite}, usual), func(c *stdtls.Config) { c.Rand = failingReader{} }, alertInternalError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sc.Clone()
			if tc.cfg != nil {
				tc.cfg(cfg)
			}
			var alerts [][]byte
			s, err := NewDTLSServer(cfg, DTLSOptions{
				Output:                  func(b []byte) { alerts = append(alerts, bytes.Clone(b)) },
				InsecureSkipHelloVerify: true,
				SRTPProfiles:            []uint16{SRTP_AEAD_AES_128_GCM},
			})
			if err != nil {
				t.Fatal(err)
			}
			body := tc.body
			err = s.HandleDatagram(hsRecord(0, hsClientHello, len(body), 0, 0, body), time.Now())
			if got := dtlsAlert(err); got != tc.alert {
				t.Fatalf("alert %d (%v), want %d", got, err, tc.alert)
			}
			if len(alerts) != 1 || alerts[0][0] != recordTypeAlert || alerts[0][14] != byte(tc.alert) {
				t.Fatalf("sent %x", alerts)
			}
		})
	}
}

// TestDTLSServerMessageChecks checks the server's refusals of the client's
// second flight, malformed.
func TestDTLSServerMessageChecks(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 24)
	// The client's second flight never arrives.
	n.drop = func(to int, b []byte) bool { return to == 1 && n.conns[0].txEpoch == 1 }
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	for n.conns[1].state != dtlsAwaitClientCertificate && n.step() {
	}
	s := n.conns[1]
	if s.state != dtlsAwaitClientCertificate {
		t.Fatalf("server in state %v", s.state)
	}
	if herr := s.serverMessage(&dtlsMessage{typ: hsCertificate, body: []byte{0, 0, 9}}, n.now); herr == nil || herr.alert != alertDecodeError {
		t.Fatalf("malformed Certificate: %v", herr)
	}
	s.state = dtlsAwaitClientKeyExchange
	if herr := s.serverMessage(&dtlsMessage{typ: hsClientKeyExchange, body: []byte{3, 1, 2, 3}}, n.now); herr == nil {
		t.Fatal("malformed ClientKeyExchange accepted")
	}
}

// TestDTLSRecordsDropped checks that records and fragments that cannot be
// taken are dropped without harm: the handshake goes on when the real ones
// come.
func TestDTLSRecordsDropped(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 25)
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	c := n.conns[0]
	junk := [][]byte{
		// Longer than any record may be.
		append([]byte{recordTypeHandshake, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 1, 0x48, 0x01}, make([]byte, 0x4801)...),
		// A handshake record too short for a fragment's header.
		{recordTypeHandshake, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 2, 0, 3, 1, 2, 3},
		// A message too far ahead.
		hsRecord(3, hsCertificate, 4, dtlsMaxAhead+1, 0, []byte{1, 2, 3, 4}),
		// Messages that together would take more than may be buffered: the
		// last of them is dropped.
		hsRecord(4, hsCertificate, maxHandshakeMessage, 2, 0, []byte{1}),
		hsRecord(5, hsCertificate, maxHandshakeMessage, 3, 0, []byte{1}),
		hsRecord(6, hsCertificate, maxHandshakeMessage, 4, 0, []byte{1}),
		hsRecord(7, hsCertificate, maxHandshakeMessage, 5, 0, []byte{1}),
		hsRecord(8, hsServerKeyExchange, 100, 6, 0, []byte{1}),
		// A fragment whose type its message does not have.
		hsRecord(9, hsServerKeyExchange, maxHandshakeMessage, 2, 1, []byte{1}),
	}
	for _, rec := range junk {
		if err := c.HandleDatagram(rec, n.now); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.partial) != 4 || c.state != dtlsAwaitServerHello {
		t.Fatalf("buffered %d messages in state %v", len(c.partial), c.state)
	}
	for _, m := range c.partial {
		if m.typ != hsCertificate || m.spans[0] != [2]int{0, 1} {
			t.Fatalf("message %d of type %d, %v", m.seq, m.typ, m.spans)
		}
	}
	c.partial = nil
	n.handshake(time.Minute)
	checkEstablished(t, n, 0, 0)
}

// TestDTLSFlightResentOnce checks that a peer's last flight, again in one
// datagram but in fragments, has ours sent again once, not once a fragment.
func TestDTLSFlightResentOnce(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	var hello []byte
	client, _ := NewDTLSClient(cc, DTLSOptions{Output: func(b []byte) { hello = bytes.Clone(b) }})
	client.Start(time.Now())
	sent := 0
	server, _ := NewDTLSServer(sc, DTLSOptions{Output: func([]byte) { sent++ }, InsecureSkipHelloVerify: true})
	if err := server.HandleDatagram(bytes.Clone(hello), time.Now()); err != nil || server.state != dtlsAwaitClientCertificate {
		t.Fatalf("%v, state %v", err, server.state)
	}
	flight := sent
	msg := hello[dtlsRecordHeaderLen:]
	body := msg[dtlsHandshakeHeaderLen:]
	again := append(hsRecord(1, hsClientHello, len(body), 0, 0, body[:40]), hsRecord(2, hsClientHello, len(body), 0, 40, body[40:])...)
	if err := server.HandleDatagram(again, time.Now()); err != nil {
		t.Fatal(err)
	}
	if sent != 2*flight {
		t.Fatalf("a flight of %d datagrams, then %d sent again", flight, sent-flight)
	}
}

// TestDTLSSequenceExhausted checks that a connection out of record sequence
// numbers sends nothing more under them.
func TestDTLSSequenceExhausted(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 27)
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	n.handshake(time.Minute)
	checkEstablished(t, n, 0, 0)
	c := n.conns[0]
	c.txSeq[1] = maxDTLSSeq + 1
	sent := n.sent[0]
	if err := c.Write([]byte("x")); !errors.Is(err, errDTLSSeqExhausted) || n.sent[0] != sent {
		t.Fatalf("Write: %v, %d sent", err, n.sent[0]-sent)
	}

	// A flight that cannot be sent again fails the handshake, at its
	// change_cipher_spec as at a handshake message.
	for _, ccs := range []bool{false, true} {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		n := newDTLSNet(t, 28)
		n.drop = func(to int, b []byte) bool { return to == 0 }
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		c := n.conns[0]
		if ccs {
			for c.state != dtlsAwaitServerFinished && n.step() {
				n.drop = nil
			}
			c.flight = c.flight[len(c.flight)-2:]
		}
		c.txSeq[0] = maxDTLSSeq + 1
		d, _ := c.NextTimeout()
		c.HandleTimeout(d)
		if err := c.HandleDatagram(nil, d); dtlsAlert(err) != alertInternalError {
			t.Fatalf("ccs %v: %v", ccs, err)
		}
	}
}

// TestDTLSMTUs checks that a handshake completes under every MTU from the
// smallest allowed, with no datagram larger than it.
func TestDTLSMTUs(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	for mtu := minDTLSMTU; mtu <= 1200; mtu += 7 {
		n := newDTLSNet(t, uint64(mtu))
		largest := 0
		n.drop = func(to int, b []byte) bool {
			largest = max(largest, len(b))
			return false
		}
		n.pair(cc, sc, DTLSOptions{MTU: mtu}, DTLSOptions{MTU: mtu})
		n.handshake(time.Minute)
		if !n.conns[0].HandshakeComplete() || !n.conns[1].HandshakeComplete() || largest > mtu {
			t.Fatalf("MTU %d: complete %v %v, largest datagram %d", mtu, n.conns[0].HandshakeComplete(), n.conns[1].HandshakeComplete(), largest)
		}
	}
}

// TestDTLSConnAPI checks the corners of the API: the constructors' checks,
// Start and Close more than once, the limits of Write and of
// ExportKeyingMaterial, and the alerts a connection ignores.
func TestDTLSConnAPI(t *testing.T) {
	out := DTLSOptions{Output: func([]byte) {}}
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	t.Run("config", func(t *testing.T) {
		bad := sc.Clone()
		bad.Certificates = []stdtls.Certificate{{}}
		if _, err := NewDTLSServer(bad, out); !errors.Is(err, ErrDTLSConfig) {
			t.Fatalf("server without a chain: %v", err)
		}
		if _, err := NewDTLSServer(sc, DTLSOptions{}); !errors.Is(err, ErrDTLSConfig) {
			t.Fatalf("server without Output: %v", err)
		}
		if _, err := NewDTLSClient(nil, out); !errors.Is(err, ErrDTLSConfig) {
			t.Fatalf("nil Config: %v", err)
		}
		rsaOnly := cc.Clone()
		rsaOnly.CipherSuites = []uint16{stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256}
		if _, err := NewDTLSClient(rsaOnly, out); !errors.Is(err, ErrDTLSConfig) {
			t.Fatalf("no DTLS suite: %v", err)
		}
		for _, p := range []uint16{0, 0xffff} {
			if _, err := NewDTLSClient(cc, DTLSOptions{Output: out.Output, SRTPProfiles: []uint16{p}}); !errors.Is(err, ErrDTLSConfig) {
				t.Fatalf("SRTP profile %#x: %v", p, err)
			}
		}
	})
	t.Run("ChaCha20 first", func(t *testing.T) {
		old := aesGCMHardware
		defer func() { aesGCMHardware = old }()
		aesGCMHardware = false
		suites := dtlsSuitesFor(cc)
		if suites[0] != stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 || len(suites) != len(dtlsSuites) {
			t.Fatalf("suites %x", suites)
		}
	})
	t.Run("server name", func(t *testing.T) {
		named := cc.Clone()
		named.ServerName = "peer.example"
		var server *DTLSConn
		var hello []byte
		c, _ := NewDTLSClient(named, DTLSOptions{Output: func(b []byte) { hello = bytes.Clone(b) }})
		c.Start(time.Now())
		server, _ = NewDTLSServer(sc, DTLSOptions{Output: func([]byte) {}, InsecureSkipHelloVerify: true})
		server.HandleDatagram(hello, time.Now())
		if server.srv == nil || server.srv.ch.serverName != "peer.example" {
			t.Fatal("the server did not get the client's server_name")
		}
	})
	t.Run("lifecycle", func(t *testing.T) {
		var sent int
		s, _ := NewDTLSServer(sc, DTLSOptions{Output: func([]byte) { sent++ }})
		if err := s.Start(time.Now()); err != nil || sent != 0 {
			t.Fatalf("a server's Start: %v, %d sent", err, sent)
		}
		c, _ := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) { sent++ }})
		c.Start(time.Now())
		if err := c.Start(time.Now()); err != nil || sent != 1 {
			t.Fatalf("a second Start: %v, %d sent", err, sent)
		}
		// A warning other than close_notify, and an alert of the wrong
		// length, change nothing.
		c.peerAlert([]byte{alertLevelWarning, alertNoRenegotiation})
		c.peerAlert([]byte{alertLevelError})
		if c.err != nil {
			t.Fatal(c.err)
		}
		c.Close()
		c.fail(fail(alertInternalError, "after the close"))
		if err := c.Close(); err != nil || sent != 2 || c.err != ErrDTLSClosed {
			t.Fatalf("Close again: %v, %d sent, %v", err, sent, c.err)
		}
		if err := c.Start(time.Now()); err != ErrDTLSClosed {
			t.Fatalf("Start after Close: %v", err)
		}
	})
	t.Run("limits", func(t *testing.T) {
		n := newDTLSNet(t, 29)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		checkEstablished(t, n, 0, 0)
		c := n.conns[0]
		if err := c.Write(make([]byte, maxPlaintext+1)); err != ErrDTLSRecordTooLarge {
			t.Fatalf("Write too much: %v", err)
		}
		if _, err := c.ExportKeyingMaterial("master secret", nil, 32); err == nil {
			t.Fatal("exported under a reserved label")
		}
		if _, err := c.ExportKeyingMaterial("EXTRACTOR-x", make([]byte, 1<<16), 32); err == nil {
			t.Fatal("exported with a context too long")
		}
	})
	t.Run("no entropy", func(t *testing.T) {
		// A server that cannot make its cookie secret answers nothing, and
		// keeps nothing.
		var hello []byte
		c, _ := NewDTLSClient(cc, DTLSOptions{Output: func(b []byte) { hello = bytes.Clone(b) }})
		c.Start(time.Now())
		dry := sc.Clone()
		dry.Rand = failingReader{}
		var sent int
		s, _ := NewDTLSServer(dry, DTLSOptions{Output: func([]byte) { sent++ }})
		if err := s.HandleDatagram(hello, time.Now()); err != nil || sent != 0 || s.cookieSecret != nil || s.srv != nil {
			t.Fatalf("%v, %d sent", err, sent)
		}
	})
}
