//go:build linux || darwin || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"strconv"
	"testing"
	"time"
)

func TestServerHandshakeTLS12(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 13, 4096} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				serverLog, clientLog := &lockedBuffer{}, &lockedBuffer{}
				config := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, KeyLogWriter: serverLog}
				hs := newSrvHS(config, nil)
				state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) {
					c.MinVersion, c.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12
					c.KeyLogWriter = clientLog
				}), chunk)
				if clientErr != nil || serverErr != nil {
					t.Fatalf("client: %v, server: %v", clientErr, serverErr)
				}
				if state.Version != stdtls.VersionTLS12 || len(state.PeerCertificates) != 1 || state.ServerName != "localhost" {
					t.Fatalf("state %+v", state)
				}
				if !hs.ems {
					t.Error("no extended master secret")
				}
				if !bytes.Equal(state.TLSUnique, hs.connectionState().TLSUnique) || len(state.TLSUnique) != 12 {
					t.Errorf("tls-unique: client %x, server %x", state.TLSUnique, hs.connectionState().TLSUnique)
				}
				want, got := clientLog.lines(), serverLog.lines()
				if want["CLIENT_RANDOM"] == "" || want["CLIENT_RANDOM"] != got["CLIENT_RANDOM"] {
					t.Errorf("master secret: server %q, client %q", got["CLIENT_RANDOM"], want["CLIENT_RANDOM"])
				}
			})
		}
	}
}

func TestServerHandshakeTLS12Suites(t *testing.T) {
	ecdsaCert, ecdsaPool := issueCert(t, keyOf(t, "ecdsa-p256"))
	rsaCert, rsaPool := issueCert(t, keyOf(t, "rsa"))
	for _, tc := range []struct {
		name  string
		suite uint16
		rsa   bool
	}{
		{"ECDSA AES-128-GCM", stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, false},
		{"ECDSA AES-256-GCM", stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, false},
		{"RSA AES-128-GCM", stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, true},
		{"RSA AES-256-GCM", stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, true},
		{"ECDSA AES-128-CBC-SHA", stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, false},
		{"ECDSA AES-256-CBC-SHA", stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, false},
		{"RSA AES-128-CBC-SHA", stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, true},
		{"RSA AES-256-CBC-SHA", stdtls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, true},
		{"ECDSA AES-128-CBC-SHA256", stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, false},
		{"RSA AES-128-CBC-SHA256", stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cert, pool := ecdsaCert, ecdsaPool
			if tc.rsa {
				cert, pool = rsaCert, rsaPool
			}
			hs := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{cert}}, nil)
			state, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) {
				c.MinVersion, c.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12
				c.CipherSuites = []uint16{tc.suite}
			}), 4096)
			if clientErr != nil || serverErr != nil || state.CipherSuite != tc.suite {
				t.Fatalf("client %v, server %v, suite %x", clientErr, serverErr, state.CipherSuite)
			}
		})
	}
	t.Run("the server's Config restricts them", func(t *testing.T) {
		hs := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{ecdsaCert}, MaxVersion: stdtls.VersionTLS12,
			CipherSuites: []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}}, nil)
		state, clientErr, serverErr := pumpServer(t, hs, clientFor(ecdsaPool, func(c *stdtls.Config) { c.MinVersion, c.MaxVersion = 0, stdtls.VersionTLS12 }), 4096)
		if clientErr != nil || serverErr != nil || state.CipherSuite != stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 {
			t.Fatalf("client %v, server %v, suite %x", clientErr, serverErr, state.CipherSuite)
		}
	})
}

func TestServerHandshakeTLS12Options(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := func(configure func(*stdtls.Config)) *stdtls.Config {
		c := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
		if configure != nil {
			configure(c)
		}
		return c
	}
	client := func(configure func(*stdtls.Config)) *stdtls.Config {
		return clientFor(pool, func(c *stdtls.Config) {
			c.MinVersion, c.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12
			if configure != nil {
				configure(c)
			}
		})
	}
	t.Run("ALPN", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.NextProtos = []string{"h2", "http/1.1"} }), nil)
		state, clientErr, serverErr := pumpServer(t, hs, client(func(c *stdtls.Config) { c.NextProtos = []string{"http/1.1", "h2"} }), 4096)
		if clientErr != nil || serverErr != nil || state.NegotiatedProtocol != "h2" {
			t.Fatalf("client %v, server %v, protocol %q", clientErr, serverErr, state.NegotiatedProtocol)
		}
	})
	t.Run("no protocol in common", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.NextProtos = []string{"a"} }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, client(func(c *stdtls.Config) { c.NextProtos = []string{"b"} }), 4096)
		if clientErr == nil || serverErr == nil {
			t.Fatalf("client %v, server %v", clientErr, serverErr)
		}
	})
	t.Run("server name picks the certificate", func(t *testing.T) {
		other, otherCert := issueCertFor(t, keyOf(t, "ecdsa-p256"), "other.example")
		pool.AddCert(otherCert)
		hs := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{cert, other}}, nil)
		state, clientErr, serverErr := pumpServer(t, hs, client(func(c *stdtls.Config) { c.ServerName = "other.example" }), 4096)
		if clientErr != nil || serverErr != nil || state.PeerCertificates[0].DNSNames[0] != "other.example" {
			t.Fatalf("client %v, server %v", clientErr, serverErr)
		}
	})
	t.Run("a curve the server prefers", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP384} }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, client(nil), 4096)
		if clientErr != nil || serverErr != nil || hs.group != stdtls.CurveP384 {
			t.Fatalf("client %v, server %v, group %v", clientErr, serverErr, hs.group)
		}
	})
	t.Run("a server that also speaks TLS 1.3 marks its random", func(t *testing.T) {
		hs := newSrvHS(server(nil), nil)
		_, clientErr, serverErr := pumpServer(t, hs, client(nil), 4096)
		if clientErr != nil || serverErr != nil || !bytes.Equal(hs.random[24:], downgradeTLS12) {
			t.Fatalf("client %v, server %v, random ends %x", clientErr, serverErr, hs.random[24:])
		}
	})
	t.Run("a server that is limited to TLS 1.2 does not", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.MaxVersion = stdtls.VersionTLS12 }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, client(nil), 4096)
		if clientErr != nil || serverErr != nil || bytes.Equal(hs.random[24:], downgradeTLS12) {
			t.Fatalf("client %v, server %v, random ends %x", clientErr, serverErr, hs.random[24:])
		}
	})
	t.Run("VerifyConnection", func(t *testing.T) {
		var seen stdtls.ConnectionState
		hs := newSrvHS(server(func(c *stdtls.Config) {
			c.NextProtos = []string{"h2"}
			c.VerifyConnection = func(state stdtls.ConnectionState) error { seen = state; return nil }
		}), nil)
		_, clientErr, serverErr := pumpServer(t, hs, client(func(c *stdtls.Config) { c.NextProtos = []string{"h2"} }), 4096)
		if clientErr != nil || serverErr != nil || seen.Version != stdtls.VersionTLS12 || seen.NegotiatedProtocol != "h2" {
			t.Fatalf("client %v, server %v, seen %+v", clientErr, serverErr, seen)
		}
	})
	t.Run("a TLS 1.2 only config", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.MinVersion, c.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12 }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.MinVersion = 0 }), 4096)
		if clientErr != nil || serverErr != nil || hs.version != stdtls.VersionTLS12 {
			t.Fatalf("client %v, server %v, version %x", clientErr, serverErr, hs.version)
		}
	})
	t.Run("a TLS 1.3 only config refuses a TLS 1.2 client", func(t *testing.T) {
		hs := newSrvHS(server(func(c *stdtls.Config) { c.MinVersion = stdtls.VersionTLS13 }), nil)
		_, clientErr, serverErr := pumpServer(t, hs, client(nil), 4096)
		if clientErr == nil || serverErr == nil {
			t.Fatalf("client %v, server %v", clientErr, serverErr)
		}
	})
}

func TestServerResumptionTLS12(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
	tickets := newTicketKeys(server)
	cache := stdtls.NewLRUClientSessionCache(4)
	client := func(cache stdtls.ClientSessionCache) *stdtls.Config {
		return clientFor(pool, func(c *stdtls.Config) {
			c.MinVersion, c.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12
			c.ClientSessionCache = cache
		})
	}
	for i := 0; i < 3; i++ {
		serverLog, clientLog := &lockedBuffer{}, &lockedBuffer{}
		config := server.Clone()
		config.KeyLogWriter = serverLog
		hs := newSrvHS(config, tickets)
		cc := client(cache)
		cc.KeyLogWriter = clientLog
		state, clientErr, serverErr := pumpServer(t, hs, cc, 4096)
		if clientErr != nil || serverErr != nil {
			t.Fatalf("connection %d: client %v, server %v", i, clientErr, serverErr)
		}
		if resumed := i > 0; state.DidResume != resumed || hs.resumed != resumed {
			t.Fatalf("connection %d: client resumed %v, server resumed %v, want %v", i, state.DidResume, hs.resumed, resumed)
		}
		if !bytes.Equal(state.TLSUnique, hs.connectionState().TLSUnique) {
			t.Errorf("connection %d: tls-unique: client %x, server %x", i, state.TLSUnique, hs.connectionState().TLSUnique)
		}
		// A resumption logs no secret on the client, so only the full
		// handshake's master secret can be compared.
		want, got := clientLog.lines(), serverLog.lines()
		if got["CLIENT_RANDOM"] == "" || i == 0 && want["CLIENT_RANDOM"] != got["CLIENT_RANDOM"] {
			t.Errorf("connection %d: master secret: server %q, client %q", i, got["CLIENT_RANDOM"], want["CLIENT_RANDOM"])
		}
	}
	t.Run("another server's tickets", func(t *testing.T) {
		hs := newSrvHS(server, newTicketKeys(server))
		state, _, _ := pumpServer(t, hs, client(cache), 4096)
		if state.DidResume || hs.resumed {
			t.Fatal("resumed from a ticket this server did not issue")
		}
	})
	t.Run("disabled", func(t *testing.T) {
		off := server.Clone()
		off.SessionTicketsDisabled = true
		fresh := stdtls.NewLRUClientSessionCache(4)
		for i := 0; i < 2; i++ {
			hs := newSrvHS(off, newTicketKeys(off))
			state, clientErr, serverErr := pumpServer(t, hs, client(fresh), 4096)
			if clientErr != nil || serverErr != nil || state.DidResume {
				t.Fatalf("connection %d: client %v, server %v, resumed %v", i, clientErr, serverErr, state.DidResume)
			}
		}
	})
	t.Run("expired", func(t *testing.T) {
		fresh := stdtls.NewLRUClientSessionCache(4)
		keys := newTicketKeys(server)
		for i := 0; i < 2; i++ {
			config := server.Clone()
			if i == 1 {
				config.Time = func() time.Time { return time.Now().Add(ticketLifetime + time.Hour) }
			}
			hs := newSrvHS(config, keys)
			_, clientErr, serverErr := pumpServer(t, hs, client(fresh), 4096)
			if clientErr != nil || serverErr != nil || hs.resumed {
				t.Fatalf("connection %d: client %v, server %v, resumed %v", i, clientErr, serverErr, hs.resumed)
			}
		}
	})
	t.Run("a TLS 1.3 ticket does not resume TLS 1.2", func(t *testing.T) {
		keys := newTicketKeys(server)
		cache13 := stdtls.NewLRUClientSessionCache(4)
		hs := newSrvHS(server.Clone(), keys)
		if _, clientErr, serverErr := pumpServer(t, hs, clientFor(pool, func(c *stdtls.Config) { c.ClientSessionCache = cache13 }), 4096); clientErr != nil || serverErr != nil {
			t.Fatal(clientErr, serverErr)
		}
		hs = newSrvHS(server.Clone(), keys)
		state, _, _ := pumpServer(t, hs, client(cache13), 4096)
		if state.DidResume || hs.resumed {
			t.Fatal("resumed TLS 1.2 from a TLS 1.3 ticket")
		}
	})
}

func TestTicketKeys12(t *testing.T) {
	keys := newTicketKeys(&stdtls.Config{})
	now := time.Now()
	state := ticketState12{suite: stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, created: now,
		master: bytes.Repeat([]byte{3}, 48), ems: true, server: "a.example"}
	ticket, ok := keys.seal12(state, now)
	if !ok {
		t.Fatal("not sealed")
	}
	got, ok := keys.open12(ticket, now)
	if !ok || got.suite != state.suite || !bytes.Equal(got.master, state.master) || !got.ems || got.server != "a.example" {
		t.Fatalf("opened %+v, %v", got, ok)
	}
	for i := range ticket {
		altered := bytes.Clone(ticket)
		altered[i] ^= 1
		if _, ok := keys.open12(altered, now); ok {
			t.Fatalf("a ticket altered at byte %d opened", i)
		}
	}
	if _, ok := keys.open12(ticket, now.Add(ticketLifetime+time.Second)); ok {
		t.Fatal("an expired ticket opened")
	}
	// The two kinds of ticket are not interchangeable.
	if _, ok := keys.open(ticket, now); ok {
		t.Fatal("a TLS 1.2 ticket opened as a TLS 1.3 one")
	}
	other, _ := keys.seal(ticketState{suite: stdtls.TLS_AES_128_GCM_SHA256, created: now, psk: []byte{1}}, now)
	if _, ok := keys.open12(other, now); ok {
		t.Fatal("a TLS 1.3 ticket opened as a TLS 1.2 one")
	}
}

// Our client and our server, against each other, in TLS 1.2.
func TestClientAndServerHandshakesTogetherTLS12(t *testing.T) {
	suites := []uint16{
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	}
	for _, suite := range suites {
		for _, chunk := range []int{1, 100, 1 << 16} {
			t.Run(strconv.Itoa(int(suite))+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
				srv := newSrvHS(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, NextProtos: []string{"h2"}}, nil)
				cli := newHS12(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"h2"}; c.MaxVersion = stdtls.VersionTLS12 })
				cli.suites12 = []uint16{suite}
				toServer, toClient := cli.hello(), []byte(nil)
				var inServer, inClient []byte
				for step := 0; step < 1000 && !(srv.state == srvDone && cli.state == hsDone); step++ {
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
				}
				if srv.state != srvDone || cli.state != hsDone {
					t.Fatalf("server state %d, client state %d", srv.state, cli.state)
				}
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
					t.Fatalf("client read %q", plain)
				}
				if cli.connectionState().Version != stdtls.VersionTLS12 || cli.connectionState().NegotiatedProtocol != "h2" ||
					!bytes.Equal(cli.connectionState().TLSUnique, srv.connectionState().TLSUnique) {
					t.Fatal("the two ends disagree")
				}
			})
		}
	}
}
