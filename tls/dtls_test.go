package tls

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	mrand "math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	dtlsRSAOnce sync.Once
	dtlsRSACert stdtls.Certificate
)

// dtlsCert returns a self-signed certificate with a key of kind: "ecdsa",
// "rsa" or "ed25519".
func dtlsCert(t testing.TB, kind string) stdtls.Certificate {
	t.Helper()
	switch kind {
	case "ecdsa":
		cert, err := GenerateWebRTCCertificate()
		if err != nil {
			t.Fatal(err)
		}
		return cert
	case "rsa":
		dtlsRSAOnce.Do(func() {
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				panic(err)
			}
			dtlsRSACert = selfSigned(key, &key.PublicKey)
		})
		return dtlsRSACert
	case "ed25519":
		pub, key, _ := ed25519.GenerateKey(rand.Reader)
		return selfSigned(key, pub)
	case "p384":
		key, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		return selfSigned(key, &key.PublicKey)
	}
	t.Fatalf("unknown key kind %s", kind)
	return stdtls.Certificate{}
}

func selfSigned(key, pub any) stdtls.Certificate {
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "dtls test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		panic(err)
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// fingerprintCheck is a VerifyPeerCertificate that accepts only the
// certificate whose fingerprint is want, as a WebRTC peer checks its peer's
// against its SDP.
func fingerprintCheck(want string) func([][]byte, [][]*x509.Certificate) error {
	return func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 || CertificateFingerprint(raw[0]) != want {
			return errors.New("fingerprint mismatch")
		}
		return nil
	}
}

// dtlsPacket is a datagram on its way to side to (0 the client, 1 the
// server), due at at.
type dtlsPacket struct {
	to   int
	at   time.Time
	data []byte
}

// dtlsNet connects a client and a server through a simulated path under a
// fake clock, which may lose, duplicate and reorder datagrams.
type dtlsNet struct {
	t       testing.TB
	rng     *mrand.Rand
	now     time.Time
	latency time.Duration
	// loss, dup and reorder are probabilities; reorder delays a datagram by
	// up to ten times the latency.
	loss, dup, reorder float64
	// drop, if set, drops what it reports true for, before the rest.
	drop    func(to int, data []byte) bool
	queue   []dtlsPacket
	conns   [2]*DTLSConn
	errs    [2]error
	data    [2][][]byte
	sent    [2]int
	settled bool
}

func newDTLSNet(t testing.TB, seed uint64) *dtlsNet {
	return &dtlsNet{t: t, rng: mrand.New(mrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), now: time.Unix(1_000_000, 0),
		latency: 10 * time.Millisecond}
}

func (n *dtlsNet) output(from int) func([]byte) {
	return func(b []byte) {
		to := 1 - from
		n.sent[from]++
		if n.drop != nil && n.drop(to, b) {
			return
		}
		if n.rng.Float64() < n.loss {
			return
		}
		copies := 1
		if n.rng.Float64() < n.dup {
			copies = 2
		}
		for range copies {
			at := n.now.Add(n.latency)
			if n.rng.Float64() < n.reorder {
				at = at.Add(time.Duration(n.rng.Int64N(int64(10 * n.latency))))
			}
			n.queue = append(n.queue, dtlsPacket{to: to, at: at, data: bytes.Clone(b)})
		}
	}
}

func (n *dtlsNet) options(side int, opts DTLSOptions) DTLSOptions {
	opts.Output = n.output(side)
	opts.OnData = func(b []byte) { n.data[side] = append(n.data[side], bytes.Clone(b)) }
	return opts
}

// pair makes the client and the server, and starts the client.
func (n *dtlsNet) pair(clientConfig, serverConfig *stdtls.Config, clientOpts, serverOpts DTLSOptions) {
	n.t.Helper()
	var err error
	if n.conns[0], err = NewDTLSClient(clientConfig, n.options(0, clientOpts)); err != nil {
		n.t.Fatal(err)
	}
	if n.conns[1], err = NewDTLSServer(serverConfig, n.options(1, serverOpts)); err != nil {
		n.t.Fatal(err)
	}
	if err := n.conns[0].Start(n.now); err != nil {
		n.t.Fatal(err)
	}
}

// step delivers the next datagram due or fires the next timer, moving the
// clock on to it. It reports false once nothing is left to happen.
func (n *dtlsNet) step() bool {
	next := -1
	for i, p := range n.queue {
		if next < 0 || p.at.Before(n.queue[next].at) {
			next = i
		}
	}
	timer, side := time.Time{}, -1
	for i, c := range n.conns {
		if c == nil || n.errs[i] != nil {
			continue
		}
		if d, ok := c.NextTimeout(); ok && (side < 0 || d.Before(timer)) {
			timer, side = d, i
		}
	}
	switch {
	case next >= 0 && (side < 0 || !n.queue[next].at.After(timer)):
		p := n.queue[next]
		n.queue = append(n.queue[:next], n.queue[next+1:]...)
		if p.at.After(n.now) {
			n.now = p.at
		}
		if n.errs[p.to] == nil {
			n.errs[p.to] = n.conns[p.to].HandleDatagram(p.data, n.now)
		}
	case side >= 0:
		if timer.After(n.now) {
			n.now = timer
		}
		n.conns[side].HandleTimeout(n.now)
	default:
		return false
	}
	return true
}

// handshake runs until both sides are done or failed, or until limit of
// fake time has passed.
func (n *dtlsNet) handshake(limit time.Duration) {
	end := n.now.Add(limit)
	for n.now.Before(end) {
		if n.conns[0].HandshakeComplete() && n.conns[1].HandshakeComplete() {
			return
		}
		if n.errs[0] != nil || n.errs[1] != nil {
			return
		}
		if !n.step() {
			return
		}
	}
}

// drain delivers everything in flight, with no more loss.
func (n *dtlsNet) drain() {
	n.loss, n.dup, n.reorder, n.drop = 0, 0, 0, nil
	for i := 0; i < 1000 && len(n.queue) > 0; i++ {
		n.step()
	}
}

type dtlsCase struct {
	suite    uint16
	key      string
	profiles []uint16
}

func webrtcConfigs(t testing.TB, clientKey, serverKey string) (client, server *stdtls.Config) {
	cc, sc := dtlsCert(t, clientKey), dtlsCert(t, serverKey)
	client = &stdtls.Config{
		Certificates:          []stdtls.Certificate{cc},
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: fingerprintCheck(CertificateFingerprint(sc.Certificate[0])),
	}
	server = &stdtls.Config{
		Certificates:          []stdtls.Certificate{sc},
		ClientAuth:            stdtls.RequireAnyClientCert,
		VerifyPeerCertificate: fingerprintCheck(CertificateFingerprint(cc.Certificate[0])),
	}
	return client, server
}

// checkEstablished checks that both sides agree on what they negotiated and
// on their keying material, and exchanges application data both ways.
func checkEstablished(t *testing.T, n *dtlsNet, suite uint16, profile uint16) {
	t.Helper()
	c, s := n.conns[0], n.conns[1]
	if n.errs[0] != nil || n.errs[1] != nil {
		t.Fatalf("handshake failed: client %v, server %v", n.errs[0], n.errs[1])
	}
	if !c.HandshakeComplete() || !s.HandshakeComplete() {
		t.Fatalf("handshake not complete after %v: client %v, server %v", n.now.Sub(time.Unix(1_000_000, 0)), c.state, s.state)
	}
	cs, ss := c.ConnectionState(), s.ConnectionState()
	if cs.Version != VersionDTLS12 || cs.CipherSuite != ss.CipherSuite || suite != 0 && cs.CipherSuite != suite {
		t.Fatalf("suites: client %#04x, server %#04x, want %#04x", cs.CipherSuite, ss.CipherSuite, suite)
	}
	if len(cs.PeerCertificates) != 1 || len(ss.PeerCertificates) != 1 {
		t.Fatalf("peer certificates: client %d, server %d", len(cs.PeerCertificates), len(ss.PeerCertificates))
	}
	cp, cok := c.SelectedSRTPProfile()
	sp, sok := s.SelectedSRTPProfile()
	if cp != sp || cok != sok || cok != (profile != 0) || cp != profile {
		t.Fatalf("SRTP profiles: client %#04x %v, server %#04x %v, want %#04x", cp, cok, sp, sok, profile)
	}
	ck, err := c.ExportKeyingMaterial("EXTRACTOR-dtls_srtp", nil, 60)
	if err != nil {
		t.Fatal(err)
	}
	sk, err := s.ExportKeyingMaterial("EXTRACTOR-dtls_srtp", nil, 60)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ck, sk) {
		t.Fatalf("exported keying material differs:\n%x\n%x", ck, sk)
	}
	ck2, _ := c.ExportKeyingMaterial("EXTRACTOR-dtls_srtp", []byte{}, 60)
	if bytes.Equal(ck, ck2) {
		t.Fatal("an empty context exports the same keying material as none")
	}
	n.data = [2][][]byte{}
	n.loss, n.dup, n.reorder, n.drop = 0, 0, 0, nil
	if err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	n.drain()
	if len(n.data[1]) != 1 || string(n.data[1][0]) != "ping" || len(n.data[0]) != 1 || string(n.data[0][0]) != "pong" {
		t.Fatalf("application data: server got %q, client got %q", n.data[1], n.data[0])
	}
}

func TestDTLSHandshakeSuites(t *testing.T) {
	cases := []dtlsCase{
		{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, "ecdsa", []uint16{SRTP_AEAD_AES_128_GCM}},
		{stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, "ecdsa", []uint16{SRTP_AEAD_AES_256_GCM}},
		{stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, "ecdsa", []uint16{SRTP_AES128_CM_HMAC_SHA1_80}},
		{stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, "rsa", []uint16{SRTP_AES128_CM_HMAC_SHA1_32}},
		{stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, "rsa", nil},
		{stdtls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256, "rsa", []uint16{SRTP_AEAD_AES_128_GCM, SRTP_AES128_CM_HMAC_SHA1_80}},
		{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, "ed25519", []uint16{SRTP_AES128_CM_HMAC_SHA1_80}},
		{stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, "p384", []uint16{SRTP_AES128_CM_HMAC_SHA1_80}},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%s", stdtls.CipherSuiteName(tc.suite), tc.key), func(t *testing.T) {
			cc, sc := webrtcConfigs(t, tc.key, tc.key)
			cc.CipherSuites = []uint16{tc.suite}
			n := newDTLSNet(t, 1)
			n.pair(cc, sc, DTLSOptions{SRTPProfiles: tc.profiles}, DTLSOptions{SRTPProfiles: tc.profiles})
			n.handshake(time.Minute)
			var want uint16
			if len(tc.profiles) > 0 {
				want = tc.profiles[0]
			}
			checkEstablished(t, n, tc.suite, want)
		})
	}
}

func TestDTLSCurvesAndProfiles(t *testing.T) {
	for _, curve := range []stdtls.CurveID{stdtls.X25519, stdtls.CurveP256, stdtls.CurveP384} {
		t.Run(curve.String(), func(t *testing.T) {
			cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
			cc.CurvePreferences = []stdtls.CurveID{curve}
			n := newDTLSNet(t, 2)
			// The server picks by its own preference among what the client
			// offers.
			n.pair(cc, sc, DTLSOptions{SRTPProfiles: []uint16{SRTP_AES128_CM_HMAC_SHA1_80, SRTP_AEAD_AES_128_GCM}},
				DTLSOptions{SRTPProfiles: []uint16{SRTP_AEAD_AES_256_GCM, SRTP_AEAD_AES_128_GCM}})
			n.handshake(time.Minute)
			checkEstablished(t, n, 0, SRTP_AEAD_AES_128_GCM)
		})
	}
	t.Run("no common profile", func(t *testing.T) {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		n := newDTLSNet(t, 3)
		n.pair(cc, sc, DTLSOptions{SRTPProfiles: []uint16{SRTP_AES128_CM_HMAC_SHA1_80}}, DTLSOptions{SRTPProfiles: []uint16{SRTP_AEAD_AES_128_GCM}})
		n.handshake(time.Minute)
		checkEstablished(t, n, 0, 0)
	})
	t.Run("ALPN and no client certificate", func(t *testing.T) {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		cc.Certificates = nil
		sc.ClientAuth = stdtls.RequestClientCert
		sc.VerifyPeerCertificate = nil
		cc.NextProtos, sc.NextProtos = []string{"x", "webrtc"}, []string{"webrtc"}
		n := newDTLSNet(t, 4)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		if n.errs[0] != nil || n.errs[1] != nil || !n.conns[1].HandshakeComplete() {
			t.Fatalf("client %v, server %v", n.errs[0], n.errs[1])
		}
		if p := n.conns[0].ConnectionState().NegotiatedProtocol; p != "webrtc" {
			t.Fatalf("ALPN %q", p)
		}
		if len(n.conns[1].ConnectionState().PeerCertificates) != 0 {
			t.Fatal("server has a client certificate")
		}
	})
}

// TestDTLSLossyLink runs handshakes over a path that loses, duplicates and
// reorders datagrams, with an MTU that cuts the certificates into fragments.
func TestDTLSLossyLink(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		key := "ecdsa"
		if seed%2 == 0 {
			key = "rsa"
		}
		t.Run(fmt.Sprint(seed, key), func(t *testing.T) {
			cc, sc := webrtcConfigs(t, key, key)
			n := newDTLSNet(t, seed)
			n.loss, n.dup, n.reorder = 0.25, 0.15, 0.3
			opts := DTLSOptions{MTU: minDTLSMTU, SRTPProfiles: []uint16{SRTP_AES128_CM_HMAC_SHA1_80}}
			n.pair(cc, sc, opts, opts)
			n.handshake(30 * time.Minute)
			// What the server sent last may still be lost: the client's
			// timer sends its flight again until it comes.
			for i := 0; i < 1000 && !n.conns[0].HandshakeComplete() && n.errs[0] == nil; i++ {
				if !n.step() {
					break
				}
			}
			checkEstablished(t, n, 0, SRTP_AES128_CM_HMAC_SHA1_80)
		})
	}
}

// TestDTLSFragmentation checks that every datagram of a flight fits the MTU
// and that a chain larger than one is cut into fragments.
func TestDTLSFragmentation(t *testing.T) {
	cc, sc := webrtcConfigs(t, "rsa", "rsa")
	n := newDTLSNet(t, 5)
	largest, datagrams := 0, 0
	n.drop = func(to int, b []byte) bool {
		largest = max(largest, len(b))
		datagrams++
		return false
	}
	opts := DTLSOptions{MTU: 300}
	n.pair(cc, sc, opts, opts)
	n.handshake(time.Minute)
	checkEstablished(t, n, 0, 0)
	if largest > 300 {
		t.Fatalf("a datagram of %d bytes exceeds the MTU", largest)
	}
	if datagrams < 10 {
		t.Fatalf("only %d datagrams for a handshake with RSA certificates under an MTU of 300", datagrams)
	}
}

func TestDTLSRetransmissionTimer(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 6)
	lost := 0
	n.drop = func(to int, b []byte) bool {
		if to == 1 && lost < 4 {
			lost++
			return true
		}
		return false
	}
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	start := n.now
	var fired []time.Duration
	for len(fired) < 4 {
		d, ok := n.conns[0].NextTimeout()
		if !ok {
			t.Fatal("no timer while the ClientHello is unanswered")
		}
		fired = append(fired, d.Sub(start))
		n.now = d
		n.conns[0].HandleTimeout(d.Add(-time.Millisecond))
		if d2, _ := n.conns[0].NextTimeout(); d2 != d {
			t.Fatal("the timer fired early")
		}
		n.conns[0].HandleTimeout(d)
	}
	want := []time.Duration{time.Second, 3 * time.Second, 7 * time.Second, 15 * time.Second}
	for i := range want {
		if fired[i] != want[i] {
			t.Fatalf("timer fired at %v, want %v", fired, want)
		}
	}
	n.handshake(time.Minute)
	checkEstablished(t, n, 0, 0)
	if _, ok := n.conns[0].NextTimeout(); ok {
		t.Fatal("client timer still running after the handshake")
	}
	if _, ok := n.conns[1].NextTimeout(); ok {
		t.Fatal("server timer still running after the handshake")
	}

	// The RTO is capped at a minute.
	c, _ := NewDTLSClient(cc, DTLSOptions{Output: func([]byte) {}})
	now := time.Unix(0, 0)
	c.Start(now)
	var last time.Time
	for i := 0; i < 12; i++ {
		d, _ := c.NextTimeout()
		if i > 8 && d.Sub(last) != dtlsMaxRTO {
			t.Fatalf("interval %v after %d retransmissions", d.Sub(last), i)
		}
		last = d
		c.HandleTimeout(d)
	}
}

// TestDTLSCookie checks that a server answers a ClientHello without a cookie
// with a HelloVerifyRequest and keeps nothing for it.
func TestDTLSCookie(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	var hello []byte
	client, _ := NewDTLSClient(cc, DTLSOptions{Output: func(b []byte) { hello = bytes.Clone(b) }})
	client.Start(time.Now())
	var answers [][]byte
	server, err := NewDTLSServer(sc, DTLSOptions{Output: func(b []byte) { answers = append(answers, bytes.Clone(b)) }})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := server.HandleDatagram(bytes.Clone(hello), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if server.srv != nil || server.state != dtlsIdle || len(server.partial) != 0 {
		t.Fatal("server kept state for a ClientHello without a cookie")
	}
	if len(answers) != 3 || answers[0][13] != hsHelloVerifyRequest || !bytes.Equal(answers[0], answers[2]) {
		t.Fatalf("answers %x", answers)
	}
	// The HelloVerifyRequest's record is numbered as the hello's was.
	if !bytes.Equal(answers[0][3:11], hello[3:11]) {
		t.Fatal("HelloVerifyRequest record sequence differs from the ClientHello's")
	}
	// A cookie from another context is not accepted.
	other, _ := NewDTLSServer(sc, DTLSOptions{Output: func([]byte) {}, CookieContext: []byte("elsewhere")})
	var out [][]byte
	other.opts.Output = func(b []byte) { out = append(out, bytes.Clone(b)) }
	client.opts.Output = func(b []byte) { hello = bytes.Clone(b) }
	client.HandleDatagram(answers[0], time.Now())
	if c := hello; c[13] != hsClientHello || len(client.cookie) == 0 {
		t.Fatal("client did not answer the HelloVerifyRequest")
	}
	other.HandleDatagram(bytes.Clone(hello), time.Now())
	if other.srv != nil || len(out) != 1 || out[0][13] != hsHelloVerifyRequest {
		t.Fatal("server accepted a cookie it did not make")
	}
	// The one that made it goes on.
	answers = nil
	server.HandleDatagram(bytes.Clone(hello), time.Now())
	if server.srv == nil || len(answers) == 0 || answers[0][13] != hsServerHello {
		t.Fatal("server did not accept its own cookie")
	}

	t.Run("skip", func(t *testing.T) {
		n := newDTLSNet(t, 7)
		first := -1
		n.drop = func(to int, b []byte) bool {
			if to == 0 && first < 0 {
				first = int(b[13])
			}
			return false
		}
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{InsecureSkipHelloVerify: true})
		n.handshake(time.Minute)
		checkEstablished(t, n, 0, 0)
		if first != hsServerHello {
			t.Fatalf("server answered with message %d", first)
		}
	})
}

func TestDTLSReplay(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 8)
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	n.handshake(time.Minute)
	n.drain()
	checkEstablished(t, n, 0, 0)
	var sent [][]byte
	n.conns[0].opts.Output = func(b []byte) { sent = append(sent, bytes.Clone(b)) }
	for i := range 70 {
		n.conns[0].Write([]byte{byte(i)})
	}
	n.data[1] = nil
	deliver := func(b []byte) {
		if err := n.conns[1].HandleDatagram(bytes.Clone(b), n.now); err != nil {
			t.Fatal(err)
		}
	}
	deliver(sent[5])
	deliver(sent[5])
	deliver(sent[69])
	deliver(sent[69])
	deliver(sent[6]) // within the window, not seen
	deliver(sent[4]) // too old: 65 behind the highest
	deliver(sent[68])
	got := ""
	for _, d := range n.data[1] {
		got += fmt.Sprint(d[0], " ")
	}
	if got != "5 69 6 68 " {
		t.Fatalf("delivered %q", got)
	}
	// A record that does not authenticate is dropped, and leaves its number
	// free for the real one.
	bad := bytes.Clone(sent[67])
	bad[len(bad)-1] ^= 1
	deliver(bad)
	deliver(sent[67])
	if len(n.data[1]) != 5 || n.data[1][4][0] != 67 {
		t.Fatalf("after a forged record: %v", n.data[1])
	}
}

func TestDTLSClose(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 9)
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
	n.handshake(time.Minute)
	checkEstablished(t, n, 0, 0)
	if err := n.conns[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := n.conns[0].Write([]byte("x")); err != ErrDTLSClosed {
		t.Fatalf("Write after Close: %v", err)
	}
	n.drain()
	if n.errs[1] != io.EOF {
		t.Fatalf("server: %v", n.errs[1])
	}
	if err := n.conns[1].Write([]byte("x")); err != io.EOF {
		t.Fatalf("Write after close_notify: %v", err)
	}
}

func TestDTLSFailures(t *testing.T) {
	t.Run("fingerprint", func(t *testing.T) {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		cc.VerifyPeerCertificate = fingerprintCheck("00:11")
		n := newDTLSNet(t, 10)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		n.drain()
		if n.errs[0] == nil || !strings.Contains(n.errs[0].Error(), "fingerprint") {
			t.Fatalf("client: %v", n.errs[0])
		}
		var alert stdtls.AlertError
		if !errors.As(n.errs[1], &alert) || alert != alertBadCertificate {
			t.Fatalf("server: %v", n.errs[1])
		}
	})
	t.Run("client fingerprint", func(t *testing.T) {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		sc.VerifyPeerCertificate = fingerprintCheck("00:11")
		n := newDTLSNet(t, 11)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		n.drain()
		if n.errs[1] == nil || n.errs[0] == nil || n.conns[0].HandshakeComplete() {
			t.Fatalf("client %v, server %v", n.errs[0], n.errs[1])
		}
	})
	t.Run("client certificate required", func(t *testing.T) {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		cc.Certificates = nil
		n := newDTLSNet(t, 12)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		n.drain()
		if n.errs[1] == nil || n.errs[0] == nil {
			t.Fatalf("client %v, server %v", n.errs[0], n.errs[1])
		}
	})
	t.Run("no common suite", func(t *testing.T) {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		cc.CipherSuites = []uint16{stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256}
		n := newDTLSNet(t, 13)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		n.drain()
		var alert stdtls.AlertError
		if !errors.As(n.errs[0], &alert) || alert != alertHandshakeFailure {
			t.Fatalf("client: %v", n.errs[0])
		}
	})
	t.Run("config", func(t *testing.T) {
		if _, err := NewDTLSServer(&stdtls.Config{}, DTLSOptions{Output: func([]byte) {}}); !errors.Is(err, ErrDTLSConfig) {
			t.Fatal(err)
		}
		if _, err := NewDTLSClient(&stdtls.Config{InsecureSkipVerify: true}, DTLSOptions{}); !errors.Is(err, ErrDTLSConfig) {
			t.Fatal(err)
		}
		if _, err := NewDTLSClient(&stdtls.Config{}, DTLSOptions{Output: func([]byte) {}}); !errors.Is(err, ErrDTLSConfig) {
			t.Fatal(err)
		}
		if _, err := NewDTLSClient(&stdtls.Config{InsecureSkipVerify: true}, DTLSOptions{Output: func([]byte) {}, MTU: 100}); !errors.Is(err, ErrDTLSConfig) {
			t.Fatal(err)
		}
		c, _ := NewDTLSClient(&stdtls.Config{InsecureSkipVerify: true}, DTLSOptions{Output: func([]byte) {}})
		if err := c.Write([]byte("x")); err != ErrDTLSNotEstablished {
			t.Fatal(err)
		}
		if _, err := c.ExportKeyingMaterial("x", nil, 1); err != ErrDTLSNotEstablished {
			t.Fatal(err)
		}
		if c.ConnectionState().HandshakeComplete {
			t.Fatal("handshake complete before it began")
		}
	})
}

// TestDTLSEarlyApplicationData checks that application data the server sends
// as soon as it is done, which may pass its Finished, reaches the client.
func TestDTLSEarlyApplicationData(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	n := newDTLSNet(t, 14)
	var held [][]byte
	n.drop = func(to int, b []byte) bool {
		if to == 0 && n.conns[1].txEpoch == 1 && len(held) == 0 {
			// The server's last flight: hold it back until the data is in.
			held = append(held, bytes.Clone(b))
			return true
		}
		return false
	}
	n.pair(cc, sc, DTLSOptions{}, DTLSOptions{
		OnHandshakeComplete: func() {},
	})
	for !n.conns[1].HandshakeComplete() && n.step() {
	}
	if err := n.conns[1].Write([]byte("early")); err != nil {
		t.Fatal(err)
	}
	n.drop = nil
	n.drain()
	if n.conns[0].HandshakeComplete() {
		t.Fatal("client complete without the server's Finished")
	}
	if len(n.data[0]) != 0 {
		t.Fatal("data delivered before the handshake was complete")
	}
	if err := n.conns[0].HandleDatagram(held[0], n.now); err != nil {
		t.Fatal(err)
	}
	if !n.conns[0].HandshakeComplete() || len(n.data[0]) != 1 || string(n.data[0][0]) != "early" {
		t.Fatalf("client complete %v, data %q", n.conns[0].HandshakeComplete(), n.data[0])
	}
}

func TestCertificateFingerprint(t *testing.T) {
	der := []byte("not a certificate, but it hashes all the same")
	sum := sha256.Sum256(der)
	want := strings.ToUpper(hex.EncodeToString(sum[:]))
	got := CertificateFingerprint(der)
	if strings.ReplaceAll(got, ":", "") != want || len(got) != 95 || strings.Count(got, ":") != 31 {
		t.Fatalf("fingerprint %s", got)
	}
	cert, err := GenerateWebRTCCertificate()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cert.PrivateKey.(*ecdsa.PrivateKey); !ok || cert.Leaf == nil || cert.Leaf.PublicKeyAlgorithm != x509.ECDSA {
		t.Fatalf("certificate %+v", cert)
	}
}

// TestDTLSSuitePreference checks that a server follows its client's choice
// between AES-GCM and ChaCha20-Poly1305 where the CPU has AES-GCM, as
// crypto/tls does, by the TLS 1.2 suites a DTLS client offers.
func TestDTLSSuitePreference(t *testing.T) {
	if !aesGCMHardware {
		t.Skip("no AES-GCM hardware: ChaCha20-Poly1305 is always first")
	}
	for _, first := range []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256} {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		n := newDTLSNet(t, 15)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		c := n.conns[0]
		c.cli.suites12 = append([]uint16{first}, slices.DeleteFunc(slices.Clone(c.cli.suites12), func(id uint16) bool { return id == first })...)
		c.state, c.flight = dtlsAwaitServerHello, nil
		c.txMsgSeq, n.queue = 0, nil
		c.sendClientHello(n.now)
		n.handshake(time.Minute)
		checkEstablished(t, n, first, 0)
	}
}

// TestDTLSApplicationDataAllocs checks that sealing and opening application
// data, which SCTP sends every packet of a data channel as, allocates nothing.
func TestDTLSApplicationDataAllocs(t *testing.T) {
	for _, suite := range []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256} {
		cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
		cc.CipherSuites = []uint16{suite}
		n := newDTLSNet(t, 16)
		n.pair(cc, sc, DTLSOptions{}, DTLSOptions{})
		n.handshake(time.Minute)
		n.drain()
		c, s := n.conns[0], n.conns[1]
		var wire []byte
		got := 0
		c.opts.Output = func(b []byte) { wire = append(wire[:0], b...) }
		s.opts.OnData = func(b []byte) { got += len(b) }
		payload := make([]byte, 1100)
		allocs := testing.AllocsPerRun(100, func() {
			if err := c.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := s.HandleDatagram(wire, n.now); err != nil {
				t.Fatal(err)
			}
		})
		if allocs != 0 || got != 101*len(payload) {
			t.Fatalf("%s: %v allocations a record, %d bytes delivered", stdtls.CipherSuiteName(suite), allocs, got)
		}
	}
}

// TestDTLSFragmentedClientHello checks that a server puts together a
// ClientHello cut into fragments, in records of their own and in datagrams of
// their own, as OpenSSL cuts it, before it checks its cookie.
func TestDTLSFragmentedClientHello(t *testing.T) {
	cc, sc := webrtcConfigs(t, "ecdsa", "ecdsa")
	var hello []byte
	client, _ := NewDTLSClient(cc, DTLSOptions{Output: func(b []byte) { hello = bytes.Clone(b) }})
	client.Start(time.Now())
	msg := hello[dtlsRecordHeaderLen:]
	length := len(msg) - dtlsHandshakeHeaderLen
	split := func(seq byte, off, n int) []byte {
		frag := append([]byte(nil), msg[:6]...)
		frag = append(frag, byte(off>>16), byte(off>>8), byte(off), byte(n>>16), byte(n>>8), byte(n))
		frag = append(frag, msg[dtlsHandshakeHeaderLen+off:dtlsHandshakeHeaderLen+off+n]...)
		rec := []byte{recordTypeHandshake, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, seq, byte(len(frag) >> 8), byte(len(frag))}
		return append(rec, frag...)
	}
	var answers [][]byte
	server, _ := NewDTLSServer(sc, DTLSOptions{Output: func(b []byte) { answers = append(answers, bytes.Clone(b)) }})
	server.HandleDatagram(split(2, 50, length-50), time.Now())
	if len(answers) != 0 || server.hello == nil {
		t.Fatal("server answered half a ClientHello")
	}
	server.HandleDatagram(split(1, 0, 60), time.Now())
	if len(answers) != 1 || answers[0][13] != hsHelloVerifyRequest || answers[0][10] != 1 || server.srv != nil {
		t.Fatalf("answers %x", answers)
	}
	// With the cookie, both fragments in one datagram.
	client.opts.Output = func(b []byte) { hello = bytes.Clone(b) }
	client.HandleDatagram(answers[0], time.Now())
	msg = hello[dtlsRecordHeaderLen:]
	length = len(msg) - dtlsHandshakeHeaderLen
	answers = nil
	server.HandleDatagram(append(split(3, 0, 100), split(4, 100, length-100)...), time.Now())
	if server.srv == nil || len(answers) == 0 || answers[0][13] != hsServerHello {
		t.Fatal("server did not go on with a fragmented ClientHello carrying its cookie")
	}
	// The transcript has the hello whole.
	if !bytes.Equal(server.srv.transcript[:len(msg)], appendDTLSMessage(nil, hsClientHello, 1, msg[dtlsHandshakeHeaderLen:])) {
		t.Fatal("transcript does not start with the whole ClientHello")
	}
}
