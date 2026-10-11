//go:build linux || darwin || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

// newSrvHS makes a server handshake for cert.
func newSrvHS(config *stdtls.Config, tickets *ticketKeys) *serverHS {
	return newServerHS(config, nil, tickets)
}

// pumpServer runs a handshake of hs against a crypto/tls client, delivering
// what the client sends in pieces of at most chunk bytes, and then a round
// trip under the application keys. It returns what each side made of it.
func pumpServer(t *testing.T, hs *serverHS, client *stdtls.Config, chunk int) (state stdtls.ConnectionState, clientErr, serverErr error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	type result struct {
		err   error
		state stdtls.ConnectionState
	}
	dialed := make(chan result, 1)
	go func() {
		raw, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			dialed <- result{err: err}
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
		conn := stdtls.Client(raw, client)
		if err := conn.Handshake(); err != nil {
			dialed <- result{err: err}
			return
		}
		if _, err := conn.Write([]byte("hello")); err != nil {
			dialed <- result{err: err}
			return
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "world" {
			dialed <- result{err: errors.New("client read " + string(buf) + ": " + errString(err))}
			return
		}
		dialed <- result{state: conn.ConnectionState()}
	}()

	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	var pending []byte
	buf := make([]byte, chunk)
	finish := func(serverErr error) (stdtls.ConnectionState, error, error) {
		_ = conn.Close()
		r := <-dialed
		return r.state, r.err, serverErr
	}
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return finish(err)
		}
		pending = append(pending, buf[:n]...)
		consumed, out, done, herr := hs.process(pending)
		if len(out) > 0 {
			if _, err := conn.Write(out); err != nil {
				return finish(err)
			}
		}
		if herr != nil {
			if herr == errFallback {
				return finish(herr)
			}
			if herr.alert != 0 {
				_, _ = conn.Write(hs.alertRecord(herr.alert))
			}
			return finish(herr)
		}
		pending = append(pending[:0], pending[consumed:]...)
		if !done {
			continue
		}
		rx, tx := hs.recordKeys()
		for {
			for len(pending) >= recordHeaderLen {
				size := recordHeaderLen + int(pending[3])<<8 + int(pending[4])
				if len(pending) < size {
					break
				}
				typ, plain, alert, ok := rx.open(pending[:size])
				if !ok {
					t.Fatalf("record did not open: alert %d", alert)
				}
				pending = pending[size:]
				if typ == recordTypeApplicationData {
					if string(plain) != "hello" {
						t.Fatalf("client said %q", plain)
					}
					record, err := tx.seal(nil, recordTypeApplicationData, []byte("world"), nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := conn.Write(record); err != nil {
						return finish(err)
					}
					return finish(nil)
				}
			}
			n, err := conn.Read(buf)
			if err != nil {
				return finish(err)
			}
			pending = append(pending, buf[:n]...)
		}
	}
}

func clientFor(pool *x509.CertPool, configure func(*stdtls.Config)) *stdtls.Config {
	c := &stdtls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: stdtls.VersionTLS13}
	if configure != nil {
		configure(c)
	}
	return c
}

func TestServerHandshakeAgainstCryptoTLS(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 7, 4096} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				serverLog, clientLog := &lockedBuffer{}, &lockedBuffer{}
				config := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, KeyLogWriter: serverLog}
				hs := newSrvHS(config, nil)
				state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.KeyLogWriter = clientLog }), chunk)
				if clientErr != nil || serverErr != nil {
					t.Fatalf("client: %v, server: %v", clientErr, serverErr)
				}
				if state.Version != stdtls.VersionTLS13 || len(state.PeerCertificates) != 1 || state.ServerName != "localhost" {
					t.Fatalf("state %+v", state)
				}
				want, got := clientLog.lines(), serverLog.lines()
				for _, label := range []string{"CLIENT_HANDSHAKE_TRAFFIC_SECRET", "SERVER_HANDSHAKE_TRAFFIC_SECRET",
					"CLIENT_TRAFFIC_SECRET_0", "SERVER_TRAFFIC_SECRET_0"} {
					if want[label] == "" || want[label] != got[label] {
						t.Errorf("%s: server %q, client %q", label, got[label], want[label])
					}
				}
			})
		}
	}
}

func TestServerHandshakeOptions(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := func(configure func(*stdtls.Config)) *stdtls.Config {
		c := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
		if configure != nil {
			configure(c)
		}
		return c
	}
	t.Run("HelloRetryRequest", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, nil), 4096)
		if clientErr != nil || serverErr != nil || !hs.retried || hs.group != stdtls.CurveP256 {
			t.Fatalf("client %v, server %v, retried %v, group %v", clientErr, serverErr, hs.retried, hs.group)
		}
	})
	t.Run("ALPN", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.NextProtos = []string{"h2", "http/1.1"} }), nil)
		state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.NextProtos = []string{"http/1.1", "h2"} }), 4096)
		if clientErr != nil || serverErr != nil || state.NegotiatedProtocol != "h2" {
			t.Fatalf("client %v, server %v, protocol %q", clientErr, serverErr, state.NegotiatedProtocol)
		}
	})
	t.Run("an HTTP/1.1 client of an h2 server", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.NextProtos = []string{"h2"} }), nil)
		state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.NextProtos = []string{"http/1.1"} }), 4096)
		if clientErr != nil || serverErr != nil || state.NegotiatedProtocol != "" {
			t.Fatalf("client %v, server %v, protocol %q", clientErr, serverErr, state.NegotiatedProtocol)
		}
	})
	t.Run("no protocol in common", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.NextProtos = []string{"a"} }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.NextProtos = []string{"b"} }), 4096)
		if clientErr == nil || serverErr == nil {
			t.Fatalf("client %v, server %v", clientErr, serverErr)
		}
	})
	t.Run("server name picks the certificate", func(t *testing.T) {
		other, otherCert := issueCertFor(t, keyOf(t, "ecdsa-p256"), "other.example")
		pool.AddCert(otherCert)
		config := &stdtls.Config{Certificates: []stdtls.Certificate{cert, other}}
		hs := newSrvHS(config, nil)
		state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.ServerName = "other.example" }), 4096)
		if clientErr != nil || serverErr != nil || state.PeerCertificates[0].DNSNames[0] != "other.example" {
			t.Fatalf("client %v, server %v", clientErr, serverErr)
		}
		if hs.ch.serverName != "other.example" {
			t.Fatalf("server name %q", hs.ch.serverName)
		}
	})
	t.Run("VerifyConnection", func(t *testing.T) {
		var seen stdtls.ConnectionState
		hs := newSrvHS(server(func(c *stdtls.Config) {
			c.NextProtos = []string{"h2"}
			c.VerifyConnection = func(state stdtls.ConnectionState) error { seen = state; return nil }
		}), nil)
		_, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"} }), 4096)
		if clientErr != nil || serverErr != nil || seen.NegotiatedProtocol != "h2" || seen.ServerName != "localhost" {
			t.Fatalf("client %v, server %v, seen %+v", clientErr, serverErr, seen)
		}
	})
	t.Run("VerifyConnection refuses", func(t *testing.T) {
		boom := errors.New("boom")
		hs := newSrvHS(server(func(c *stdtls.Config) {
			c.VerifyConnection = func(stdtls.ConnectionState) error { return boom }
		}), nil)
		_, _, serverErr := pumpServer(t, hs, clientFor(pool, nil), 4096)
		if !errors.Is(serverErr, boom) {
			t.Fatalf("server %v", serverErr)
		}
	})
}

func TestServerResumption(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
	tickets := newTicketKeys(server)
	cache := stdtls.NewLRUClientSessionCache(4)
	client := func() *stdtls.Config {
		return clientFor(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache })
	}
	for i := 0; i < 3; i++ {
		serverLog, clientLog := &lockedBuffer{}, &lockedBuffer{}
		config := server.Clone()
		config.KeyLogWriter = serverLog
		hs := newSrvHS(config, tickets)
		cc := client()
		cc.KeyLogWriter = clientLog
		state, clientErr, serverErr := pumpServer(t, hs, cc, 4096)
		if clientErr != nil || serverErr != nil {
			t.Fatalf("connection %d: client %v, server %v", i, clientErr, serverErr)
		}
		if resumed := i > 0; state.DidResume != resumed || hs.resumed != resumed {
			t.Fatalf("connection %d: client resumed %v, server resumed %v, want %v", i, state.DidResume, hs.resumed, resumed)
		}
		want, got := clientLog.lines(), serverLog.lines()
		for _, label := range []string{"CLIENT_TRAFFIC_SECRET_0", "SERVER_TRAFFIC_SECRET_0"} {
			if want[label] == "" || want[label] != got[label] {
				t.Errorf("connection %d: %s: server %q, client %q", i, label, got[label], want[label])
			}
		}
	}
	t.Run("another server's tickets", func(t *testing.T) {
		hs := newSrvHS(server, newTicketKeys(server))
		state, _, _ := pumpServer(t, hs, client(), 4096)
		if state.DidResume || hs.resumed {
			t.Fatal("resumed from a ticket this server did not issue")
		}
	})
	t.Run("disabled", func(t *testing.T) {
		off := server.Clone()
		off.SessionTicketsDisabled = true
		cache := stdtls.NewLRUClientSessionCache(4)
		for i := 0; i < 2; i++ {
			hs := newSrvHS(off, newTicketKeys(off))
			cc := clientFor(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache })
			state, clientErr, serverErr := pumpServer(t, hs, cc, 4096)
			if clientErr != nil || serverErr != nil || state.DidResume {
				t.Fatalf("connection %d: client %v, server %v, resumed %v", i, clientErr, serverErr, state.DidResume)
			}
		}
	})
	t.Run("expired", func(t *testing.T) {
		cache := stdtls.NewLRUClientSessionCache(4)
		keys := newTicketKeys(server)
		for i := 0; i < 2; i++ {
			config := server.Clone()
			if i == 1 {
				config.Time = func() time.Time { return time.Now().Add(ticketLifetime + time.Hour) }
			}
			hs := newSrvHS(config, keys)
			// Only the server's clock has moved on: the client offers the
			// ticket, and the server refuses it as too old.
			cc := clientFor(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache })
			_, clientErr, serverErr := pumpServer(t, hs, cc, 4096)
			if clientErr != nil || serverErr != nil || hs.resumed {
				t.Fatalf("connection %d: client %v, server %v, resumed %v", i, clientErr, serverErr, hs.resumed)
			}
		}
	})
}

func TestTicketKeys(t *testing.T) {
	keys := newTicketKeys(&stdtls.Config{})
	now := time.Now()
	state := ticketState{suite: stdtls.TLS_AES_128_GCM_SHA256, created: now, ageAdd: 7, psk: bytes.Repeat([]byte{1}, 32), server: "a.example"}
	ticket, ok := keys.seal(state, now)
	if !ok {
		t.Fatal("not sealed")
	}
	got, ok := keys.open(ticket, now)
	if !ok || got.suite != state.suite || got.ageAdd != 7 || !bytes.Equal(got.psk, state.psk) || got.server != "a.example" ||
		got.created.Unix() != now.Unix() {
		t.Fatalf("opened %+v, %v", got, ok)
	}
	for i := range ticket {
		altered := bytes.Clone(ticket)
		altered[i] ^= 1
		if _, ok := keys.open(altered, now); ok {
			t.Fatalf("a ticket altered at byte %d opened", i)
		}
	}
	if _, ok := keys.open(ticket[:len(ticket)-1], now); ok {
		t.Fatal("a truncated ticket opened")
	}
	if _, ok := keys.open(ticket, now.Add(ticketLifetime+time.Second)); ok {
		t.Fatal("an expired ticket opened")
	}
	if _, ok := keys.open(ticket, now.Add(-time.Hour)); ok {
		t.Fatal("a ticket from the future opened")
	}
	if _, ok := newTicketKeys(&stdtls.Config{}).open(ticket, now); ok {
		t.Fatal("another key opened it")
	}
	// A key that is replaced still opens the tickets it sealed.
	later := now.Add(ticketKeyLife + time.Minute)
	newer, _ := keys.seal(state, later)
	if _, ok := keys.open(ticket, later); !ok {
		t.Fatal("the previous key no longer opens its tickets")
	}
	if _, ok := keys.open(newer, later); !ok {
		t.Fatal("the new key does not open its tickets")
	}
	// A static key is shared by servers that are given it.
	var secret [32]byte
	secret[0] = 9
	a, b := newTicketKeys(&stdtls.Config{SessionTicketKey: secret}), newTicketKeys(&stdtls.Config{SessionTicketKey: secret})
	sealed, _ := a.seal(state, now)
	if _, ok := b.open(sealed, now); !ok {
		t.Fatal("a server with the same SessionTicketKey cannot open the ticket")
	}
	if newTicketKeys(&stdtls.Config{SessionTicketsDisabled: true}) != nil {
		t.Fatal("tickets are not disabled")
	}
}

// hello makes the ClientHello record of a client handshake with config.
func helloOf(t *testing.T, pool *x509.CertPool, configure func(*stdtls.Config), tweak func(*clientHS)) []byte {
	t.Helper()
	hs := newHS12(t, pool, configure)
	if tweak != nil {
		tweak(hs)
	}
	return hs.hello()
}

func TestServerFallsBackForClientsItDoesNotServe(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	config := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
	for _, tc := range []struct {
		name  string
		hello []byte
		want  string
	}{
		{"TLS 1.3", helloOf(t, pool, nil, nil), "serve"},
		{"TLS 1.2 only", helloOf(t, pool, func(c *stdtls.Config) { c.MaxVersion = stdtls.VersionTLS12 }, nil), "fallback"},
		{"no AES-GCM suite", helloOf(t, pool, nil, func(h *clientHS) { h.suites = []uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256} }), "fallback"},
		{"no group in common", helloOf(t, pool, func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP384} }, nil), "fallback"},
		{"not a handshake", []byte("GET / HTTP/1.1\r\n\r\n"), "fallback"},
		{"an SSLv2 hello", []byte{0x80, 0x1c, 0x01, 0x03, 0x01, 0, 0, 0, 0, 0, 0, 0}, "fallback"},
		{"a partial record", helloOf(t, pool, nil, nil)[:20], "wait"},
		{"a partial header", []byte{22, 3, 1}, "wait"},
		{"nothing", nil, "wait"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hs := newSrvHS(config, nil)
			if tc.name == "no group in common" {
				hs.groups = []stdtls.CurveID{stdtls.X25519}
			}
			consumed, out, done, herr := hs.process(tc.hello)
			switch tc.want {
			case "serve":
				if herr != nil || done || consumed != len(tc.hello) || len(out) == 0 {
					t.Fatalf("consumed %d of %d, %d bytes out, error %v", consumed, len(tc.hello), len(out), herr)
				}
			case "fallback":
				if herr != errFallback || consumed != 0 || len(out) != 0 {
					t.Fatalf("consumed %d, %d bytes out, error %v", consumed, len(out), herr)
				}
			case "wait":
				if herr != nil || consumed != 0 || len(out) != 0 || done {
					t.Fatalf("consumed %d, %d bytes out, error %v", consumed, len(out), herr)
				}
			}
		})
	}
}

func TestNativeServerQualification(t *testing.T) {
	cert, _ := issueCert(t, keyOf(t, "ecdsa-p256"))
	certs := []stdtls.Certificate{cert}
	for _, tc := range []struct {
		name   string
		config *stdtls.Config
		want   bool
	}{
		{"nil", nil, false},
		{"no certificates", &stdtls.Config{}, false},
		{"a certificate", &stdtls.Config{Certificates: certs}, true},
		{"TLS 1.3 only", &stdtls.Config{Certificates: certs, MinVersion: stdtls.VersionTLS13}, true},
		{"capped at 1.2", &stdtls.Config{Certificates: certs, MaxVersion: stdtls.VersionTLS12}, false},
		{"GetCertificate", &stdtls.Config{Certificates: certs, GetCertificate: func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) { return nil, nil }}, false},
		{"GetConfigForClient", &stdtls.Config{Certificates: certs, GetConfigForClient: func(*stdtls.ClientHelloInfo) (*stdtls.Config, error) { return nil, nil }}, false},
		{"client certificates", &stdtls.Config{Certificates: certs, ClientAuth: stdtls.RequestClientCert}, false},
		{"unknown curves", &stdtls.Config{Certificates: certs, CurvePreferences: []stdtls.CurveID{stdtls.CurveP521}}, false},
		{"a key that cannot sign", &stdtls.Config{Certificates: []stdtls.Certificate{{Certificate: [][]byte{{1}}, PrivateKey: "no"}}}, false},
	} {
		if got := nativeServer(tc.config); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Our client and our server, against each other.
func TestClientAndServerHandshakesTogether(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 100, 1 << 16} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				srv := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, NextProtos: []string{"h2"}}, nil)
				cli := newHS(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"} })
				toServer, toClient := cli.hello(), []byte(nil)
				var inServer, inClient []byte
				for step := 0; step < 1000; step++ {
					for len(toServer) > 0 {
						n := min(chunk, len(toServer))
						inServer = append(inServer, toServer[:n]...)
						toServer = toServer[n:]
						consumed, out, _, herr := srv.process(inServer)
						if herr != nil {
							t.Fatalf("server: %v", herr)
						}
						inServer = inServer[consumed:]
						toClient = append(toClient, out...)
					}
					for len(toClient) > 0 {
						n := min(chunk, len(toClient))
						inClient = append(inClient, toClient[:n]...)
						toClient = toClient[n:]
						consumed, out, _, herr := cli.process(inClient)
						if herr != nil {
							t.Fatalf("client: %v", herr)
						}
						inClient = inClient[consumed:]
						toServer = append(toServer, out...)
					}
					if srv.state == srvDone && cli.state == hsDone {
						break
					}
				}
				if srv.state != srvDone || cli.state != hsDone {
					t.Fatalf("server state %d, client state %d", srv.state, cli.state)
				}
				// Each side's keys are the other's.
				record, err := cli.tx.seal(nil, recordTypeApplicationData, []byte("ping"), nil)
				if err != nil {
					t.Fatal(err)
				}
				rx, tx := srv.recordKeys()
				if typ, plain, _, ok := rx.open(record); !ok || typ != recordTypeApplicationData || string(plain) != "ping" {
					t.Fatalf("server read %q", plain)
				}
				record, _ = tx.seal(nil, recordTypeApplicationData, []byte("pong"), nil)
				if typ, plain, _, ok := cli.rx.open(record); !ok || typ != recordTypeApplicationData || string(plain) != "pong" {
					t.Fatalf("client read %q (type %d)", plain, typ)
				}
				if cli.connectionState().NegotiatedProtocol != "h2" || srv.connectionState().NegotiatedProtocol != "h2" {
					t.Fatal("protocol not negotiated")
				}
			})
		}
	}
}
