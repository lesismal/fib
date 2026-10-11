//go:build linux || darwin || windows

package tls

import (
	"bytes"
	"context"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"strconv"
	"testing"
)

// quicEnd is what crypto/tls's QUICConn and this package's both are.
type quicEnd interface {
	Start(context.Context) error
	HandleData(stdtls.QUICEncryptionLevel, []byte) error
	NextEvent() stdtls.QUICEvent
	SetTransportParameters([]byte)
	SendSessionTicket(stdtls.QUICSessionTicketOptions) error
	ConnectionState() stdtls.ConnectionState
	Close() error
}

var (
	_ quicEnd = (*QUICConn)(nil)
	_ quicEnd = (*stdtls.QUICConn)(nil)
)

// quicSide is one end of a handshake as the test sees it: what it wrote at each
// level, the secrets it set, the parameters its peer sent, and whether it is done.
type quicSide struct {
	conn   quicEnd
	out    [3][]byte
	reads  map[stdtls.QUICEncryptionLevel][]byte
	writes map[stdtls.QUICEncryptionLevel][]byte
	peer   []byte
	done   bool
	suite  uint16
}

func newQuicSide(conn quicEnd) *quicSide {
	return &quicSide{conn: conn, reads: map[stdtls.QUICEncryptionLevel][]byte{}, writes: map[stdtls.QUICEncryptionLevel][]byte{}}
}

func levelIndex(level stdtls.QUICEncryptionLevel) int {
	switch level {
	case stdtls.QUICEncryptionLevelInitial:
		return 0
	case stdtls.QUICEncryptionLevelHandshake:
		return 1
	}
	return 2
}

// events reads what the side has to report, and returns an error for an event
// the test does not know.
func (s *quicSide) events(t *testing.T) {
	t.Helper()
	for {
		e := s.conn.NextEvent()
		switch e.Kind {
		case stdtls.QUICNoEvent:
			return
		case stdtls.QUICSetReadSecret:
			s.reads[e.Level] = bytes.Clone(e.Data)
			s.suite = e.Suite
		case stdtls.QUICSetWriteSecret:
			s.writes[e.Level] = bytes.Clone(e.Data)
			s.suite = e.Suite
		case stdtls.QUICWriteData:
			s.out[levelIndex(e.Level)] = append(s.out[levelIndex(e.Level)], e.Data...)
		case stdtls.QUICTransportParameters:
			s.peer = bytes.Clone(e.Data)
		case stdtls.QUICTransportParametersRequired:
			t.Fatal("the transport parameters were asked for after they were set")
		case stdtls.QUICHandshakeDone:
			s.done = true
		}
	}
}

// quicRun runs a handshake between client and server, delivering what each
// writes to the other in pieces of at most chunk bytes, and returns the first
// error either gave. The server sends a ticket when it is done.
func quicRun(t *testing.T, client, server quicEnd, chunk int) (*quicSide, *quicSide, error) {
	t.Helper()
	cli, srv := newQuicSide(client), newQuicSide(server)
	client.SetTransportParameters([]byte("client parameters"))
	server.SetTransportParameters([]byte("server parameters"))
	if err := client.Start(context.Background()); err != nil {
		return cli, srv, err
	}
	if err := server.Start(context.Background()); err != nil {
		return cli, srv, err
	}
	ticketSent := false
	for step := 0; step < 10000; step++ {
		cli.events(t)
		srv.events(t)
		moved := false
		for _, pair := range []struct{ from, to *quicSide }{{cli, srv}, {srv, cli}} {
			// The lowest level that has anything, and no higher one until it
			// is all delivered: packets of a level cannot be read before the
			// keys that the level below gives, so a message is never partly
			// delivered when data of another level arrives.
			for level := range pair.from.out {
				data := pair.from.out[level]
				if len(data) == 0 {
					continue
				}
				n := min(chunk, len(data))
				pair.from.out[level] = data[n:]
				moved = true
				lv := [...]stdtls.QUICEncryptionLevel{stdtls.QUICEncryptionLevelInitial, stdtls.QUICEncryptionLevelHandshake, stdtls.QUICEncryptionLevelApplication}[level]
				if err := pair.to.conn.HandleData(lv, data[:n]); err != nil {
					return cli, srv, err
				}
				break
			}
		}
		if srv.done && !ticketSent {
			ticketSent = true
			if err := server.SendSessionTicket(stdtls.QUICSessionTicketOptions{}); err != nil {
				return cli, srv, err
			}
			moved = true
		}
		if !moved && cli.done && srv.done {
			return cli, srv, nil
		}
		if !moved {
			cli.events(t)
			srv.events(t)
		}
	}
	return cli, srv, errors.New("the handshake never finished")
}

// quicCheck checks that two ends that finished agree.
func quicCheck(t *testing.T, cli, srv *quicSide) {
	t.Helper()
	if !cli.done || !srv.done {
		t.Fatalf("client done %v, server done %v", cli.done, srv.done)
	}
	for _, level := range []stdtls.QUICEncryptionLevel{stdtls.QUICEncryptionLevelHandshake, stdtls.QUICEncryptionLevelApplication} {
		if len(cli.writes[level]) == 0 || !bytes.Equal(cli.writes[level], srv.reads[level]) {
			t.Errorf("level %d: what the client writes under, %x, is not what the server reads under, %x", level, cli.writes[level], srv.reads[level])
		}
		if len(srv.writes[level]) == 0 || !bytes.Equal(srv.writes[level], cli.reads[level]) {
			t.Errorf("level %d: what the server writes under, %x, is not what the client reads under, %x", level, srv.writes[level], cli.reads[level])
		}
	}
	if string(cli.peer) != "server parameters" || string(srv.peer) != "client parameters" {
		t.Errorf("transport parameters: client got %q, server got %q", cli.peer, srv.peer)
	}
}

func quicClientConfig(pool *x509.CertPool, configure func(*stdtls.Config)) *stdtls.Config {
	c := &stdtls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"h3test"}, MinVersion: stdtls.VersionTLS13}
	if configure != nil {
		configure(c)
	}
	return c
}

func quicServerConfig(cert stdtls.Certificate, configure func(*stdtls.Config)) *stdtls.Config {
	c := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, NextProtos: []string{"h3test"}, MinVersion: stdtls.VersionTLS13}
	if configure != nil {
		configure(c)
	}
	return c
}

func ourClient(t *testing.T, config *stdtls.Config) *QUICConn {
	t.Helper()
	c := NewQUICClient(config)
	if c == nil {
		t.Fatal("the client Config does not qualify")
	}
	return c
}

func ourServer(t *testing.T, config *stdtls.Config) *QUICConn {
	t.Helper()
	s := NewQUICServer(config)
	if s == nil {
		t.Fatal("the server Config does not qualify")
	}
	return s.NewConn()
}

func stdClient(config *stdtls.Config) *stdtls.QUICConn {
	return stdtls.QUICClient(&stdtls.QUICConfig{TLSConfig: config})
}

func stdServer(config *stdtls.Config) *stdtls.QUICConn {
	return stdtls.QUICServer(&stdtls.QUICConfig{TLSConfig: config})
}

func TestQUICClientAgainstCryptoTLS(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 100, 1 << 16} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				client := ourClient(t, quicClientConfig(pool, nil))
				cli, srv, err := quicRun(t, client, stdServer(quicServerConfig(cert, nil)), chunk)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				state := client.ConnectionState()
				if state.Version != stdtls.VersionTLS13 || state.NegotiatedProtocol != "h3test" || len(state.PeerCertificates) != 1 {
					t.Fatalf("state %+v", state)
				}
			})
		}
	}
}

func TestQUICServerAgainstCryptoTLS(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 100, 1 << 16} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				server := ourServer(t, quicServerConfig(cert, nil))
				cli, srv, err := quicRun(t, stdClient(quicClientConfig(pool, nil)), server, chunk)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				state := server.ConnectionState()
				if state.Version != stdtls.VersionTLS13 || state.NegotiatedProtocol != "h3test" || state.ServerName != "localhost" {
					t.Fatalf("state %+v", state)
				}
			})
		}
	}
}

func TestQUICOursAgainstOurs(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	for _, suite := range quicSuites {
		for _, chunk := range []int{1, 1 << 16} {
			t.Run(strconv.Itoa(int(suite))+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				client := ourClient(t, quicClientConfig(pool, nil))
				client.suites = []uint16{suite}
				cli, srv, err := quicRun(t, client, ourServer(t, quicServerConfig(cert, nil)), chunk)
				if err != nil {
					t.Fatal(err)
				}
				quicCheck(t, cli, srv)
				if cli.suite != suite || srv.suite != suite {
					t.Fatalf("suites %x and %x, want %x", cli.suite, srv.suite, suite)
				}
			})
		}
	}
	t.Run("ChaCha20 against crypto/tls", func(t *testing.T) {
		client := ourClient(t, quicClientConfig(pool, nil))
		client.suites = []uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256}
		cli, srv, err := quicRun(t, client, stdServer(quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if cli.suite != stdtls.TLS_CHACHA20_POLY1305_SHA256 {
			t.Fatalf("suite %x", cli.suite)
		}
	})
}

func TestQUICHandshakeOptions(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	t.Run("HelloRetryRequest, with our server", func(t *testing.T) {
		server := ourServer(t, quicServerConfig(cert, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} }))
		cli, srv, err := quicRun(t, stdClient(quicClientConfig(pool, nil)), server, 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if !server.server.retried {
			t.Fatal("no HelloRetryRequest")
		}
	})
	t.Run("HelloRetryRequest, with our client", func(t *testing.T) {
		cli, srv, err := quicRun(t, ourClient(t, quicClientConfig(pool, nil)),
			stdServer(quicServerConfig(cert, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} })), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
	})
	t.Run("no protocol in common", func(t *testing.T) {
		_, _, err := quicRun(t, stdClient(quicClientConfig(pool, func(c *stdtls.Config) { c.NextProtos = []string{"other"} })),
			ourServer(t, quicServerConfig(cert, nil)), 4096)
		if err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("untrusted certificate", func(t *testing.T) {
		_, _, err := quicRun(t, ourClient(t, quicClientConfig(x509.NewCertPool(), nil)), stdServer(quicServerConfig(cert, nil)), 4096)
		var verify *stdtls.CertificateVerificationError
		if !errors.As(err, &verify) && err == nil {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("a server that sends no ALPN protocol", func(t *testing.T) {
		_, _, err := quicRun(t, ourClient(t, quicClientConfig(pool, nil)),
			ourServer(t, quicServerConfig(cert, func(c *stdtls.Config) { c.NextProtos = nil })), 4096)
		if err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("VerifyConnection", func(t *testing.T) {
		var seen stdtls.ConnectionState
		client := ourClient(t, quicClientConfig(pool, func(c *stdtls.Config) {
			c.VerifyConnection = func(s stdtls.ConnectionState) error { seen = s; return nil }
		}))
		cli, srv, err := quicRun(t, client, ourServer(t, quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if seen.NegotiatedProtocol != "h3test" {
			t.Fatalf("seen %+v", seen)
		}
	})
}

func TestQUICResumption(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	config := quicServerConfig(cert, nil)
	server := NewQUICServer(config)
	cache := stdtls.NewLRUClientSessionCache(4)
	for i := 0; i < 3; i++ {
		conn := server.NewConn()
		client := stdClient(quicClientConfig(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache }))
		cli, srv, err := quicRun(t, client, conn, 4096)
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		quicCheck(t, cli, srv)
		if resumed := i > 0; conn.ConnectionState().DidResume != resumed || client.ConnectionState().DidResume != resumed {
			t.Fatalf("connection %d: server resumed %v, client resumed %v, want %v", i,
				conn.ConnectionState().DidResume, client.ConnectionState().DidResume, resumed)
		}
	}
}

func TestQUICServerFallsBack(t *testing.T) {
	fellBack := countFallbacks(t)
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	// A client that asks for something the server does not serve itself: a
	// session ID, which QUIC has no use for.
	h, err := newClientHSFor(quicClientConfig(pool, nil), &quicCtx{localParams: []byte("client parameters"), haveLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	h.sessionID = bytes.Repeat([]byte{7}, 32)
	hello := h.clientHello()

	server := ourServer(t, quicServerConfig(cert, nil))
	server.SetTransportParameters([]byte("server parameters"))
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleData(stdtls.QUICEncryptionLevelInitial, hello); err != nil {
		t.Fatal(err)
	}
	if n := fellBack.Load(); n != 1 {
		t.Fatalf("%d handed to crypto/tls, want 1", n)
	}
	var wrote bool
	for {
		e := server.NextEvent()
		if e.Kind == stdtls.QUICNoEvent {
			break
		}
		wrote = wrote || e.Kind == stdtls.QUICWriteData && e.Level == stdtls.QUICEncryptionLevelInitial
	}
	if !wrote {
		t.Fatal("crypto/tls did not answer the hello it was given")
	}
}

func TestQUICQualification(t *testing.T) {
	cert, _ := issueCert(t, keyOf(t, "ecdsa-p256"))
	if NewQUICClient(&stdtls.Config{ClientSessionCache: stdtls.NewLRUClientSessionCache(1)}) != nil {
		t.Error("a client with a session cache qualifies")
	}
	if NewQUICClient(&stdtls.Config{Certificates: []stdtls.Certificate{cert}}) != nil {
		t.Error("a client with certificates qualifies")
	}
	if NewQUICClient(&stdtls.Config{ServerName: "a"}) == nil {
		t.Error("an ordinary client does not qualify")
	}
	if NewQUICServer(&stdtls.Config{}) != nil {
		t.Error("a server without certificates qualifies")
	}
	if NewQUICServer(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, GetCertificate: func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) { return nil, nil }}) != nil {
		t.Error("a server with GetCertificate qualifies")
	}
	if NewQUICServer(&stdtls.Config{Certificates: []stdtls.Certificate{cert}}) == nil {
		t.Error("an ordinary server does not qualify")
	}
}
