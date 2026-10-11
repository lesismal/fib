//go:build linux || darwin || windows

package tls

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"strconv"
	"testing"
)

// newHS12 is newHS with the Config a client has by default, which offers
// TLS 1.3 and 1.2.
func newHS12(t *testing.T, pool *x509.CertPool, configure func(*stdtls.Config)) *clientHS {
	t.Helper()
	config := &stdtls.Config{RootCAs: pool, ServerName: "localhost"}
	if configure != nil {
		configure(config)
	}
	hs, err := newClientHS(config)
	if err != nil {
		t.Fatal(err)
	}
	return hs
}

func TestClientHandshakeTLS12(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa"} {
		for _, chunk := range []int{1, 13, 4096} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				log := &lockedBuffer{}
				server := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MaxVersion: stdtls.VersionTLS12, KeyLogWriter: log}
				clientLog := &lockedBuffer{}
				hs := newHS12(t, pool, func(c *stdtls.Config) { c.KeyLogWriter = clientLog })
				serverErr, clientErr := pump(t, hs, server, chunk)
				if serverErr != nil || clientErr != nil {
					t.Fatalf("server: %v, client: %v", serverErr, clientErr)
				}
				state := hs.connectionState()
				if state.Version != stdtls.VersionTLS12 || len(state.PeerCertificates) == 0 || len(state.TLSUnique) != 12 || !hs.ems {
					t.Fatalf("state %+v, extended master secret %v", state, hs.ems)
				}
				want, got := log.lines(), clientLog.lines()
				if want["CLIENT_RANDOM"] == "" || want["CLIENT_RANDOM"] != got["CLIENT_RANDOM"] {
					t.Errorf("master secret: client %q, server %q", got["CLIENT_RANDOM"], want["CLIENT_RANDOM"])
				}
			})
		}
	}
}

func TestClientHandshakeTLS12Suites(t *testing.T) {
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
			server := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MaxVersion: stdtls.VersionTLS12,
				CipherSuites: []uint16{tc.suite}}
			hs := newHS12(t, pool, nil)
			serverErr, clientErr := pump(t, hs, server, 4096)
			if serverErr != nil || clientErr != nil || hs.connectionState().CipherSuite != tc.suite {
				t.Fatalf("server %v, client %v, suite %x", serverErr, clientErr, hs.suiteID)
			}
		})
	}
}

func TestClientHandshakeVersionChoice(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	t.Run("both offered, server takes 1.3", func(t *testing.T) {
		hs := newHS12(t, pool, nil)
		serverErr, clientErr := pump(t, hs, &stdtls.Config{Certificates: []stdtls.Certificate{cert}}, 4096)
		if serverErr != nil || clientErr != nil || hs.version != stdtls.VersionTLS13 {
			t.Fatalf("server %v, client %v, version %x", serverErr, clientErr, hs.version)
		}
	})
	t.Run("both offered, server only has 1.2", func(t *testing.T) {
		hs := newHS12(t, pool, nil)
		serverErr, clientErr := pump(t, hs, &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MaxVersion: stdtls.VersionTLS12}, 4096)
		if serverErr != nil || clientErr != nil || hs.version != stdtls.VersionTLS12 {
			t.Fatalf("server %v, client %v, version %x", serverErr, clientErr, hs.version)
		}
	})
	t.Run("only 1.2 offered", func(t *testing.T) {
		hs := newHS12(t, pool, func(c *stdtls.Config) { c.MaxVersion = stdtls.VersionTLS12 })
		serverErr, clientErr := pump(t, hs, &stdtls.Config{Certificates: []stdtls.Certificate{cert}}, 4096)
		if serverErr != nil || clientErr != nil || hs.version != stdtls.VersionTLS12 {
			t.Fatalf("server %v, client %v, version %x", serverErr, clientErr, hs.version)
		}
	})
	t.Run("only 1.2 offered, TLS 1.3 only server", func(t *testing.T) {
		hs := newHS12(t, pool, func(c *stdtls.Config) { c.MaxVersion = stdtls.VersionTLS12 })
		_, clientErr := pump(t, hs, &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MinVersion: stdtls.VersionTLS13}, 4096)
		if clientErr == nil {
			t.Fatal("no error")
		}
	})
	t.Run("CipherSuites narrows the TLS 1.2 suites", func(t *testing.T) {
		hs := newHS12(t, pool, func(c *stdtls.Config) {
			c.CipherSuites = []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384}
			c.MaxVersion = stdtls.VersionTLS12
		})
		serverErr, clientErr := pump(t, hs, &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MaxVersion: stdtls.VersionTLS12}, 4096)
		if serverErr != nil || clientErr != nil || hs.suiteID != stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 {
			t.Fatalf("server %v, client %v, suite %x", serverErr, clientErr, hs.suiteID)
		}
	})
}

func TestClientHandshakeTLS12Options(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := func(configure func(*stdtls.Config)) *stdtls.Config {
		c := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MaxVersion: stdtls.VersionTLS12}
		if configure != nil {
			configure(c)
		}
		return c
	}
	t.Run("ALPN", func(t *testing.T) {
		hs := newHS12(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"http/1.1", "h2"} })
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) { c.NextProtos = []string{"h2"} }), 4096)
		if serverErr != nil || clientErr != nil || hs.connectionState().NegotiatedProtocol != "h2" {
			t.Fatalf("server %v, client %v, protocol %q", serverErr, clientErr, hs.protocol)
		}
	})
	t.Run("P-384 only server", func(t *testing.T) {
		hs := newHS12(t, pool, nil)
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP384} }), 4096)
		if serverErr != nil || clientErr != nil || hs.skxCurve != stdtls.CurveP384 {
			t.Fatalf("server %v, client %v, curve %v", serverErr, clientErr, hs.skxCurve)
		}
	})
	t.Run("client certificate requested", func(t *testing.T) {
		hs := newHS12(t, pool, nil)
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) { c.ClientAuth = stdtls.RequestClientCert }), 4096)
		if serverErr != nil || clientErr != nil || !hs.certReq {
			t.Fatalf("server %v, client %v, requested %v", serverErr, clientErr, hs.certReq)
		}
	})
	t.Run("client certificate required", func(t *testing.T) {
		hs := newHS12(t, pool, nil)
		serverErr, _ := pump(t, hs, server(func(c *stdtls.Config) { c.ClientAuth = stdtls.RequireAnyClientCert }), 4096)
		if serverErr == nil {
			t.Fatal("a server that requires a certificate accepted none")
		}
	})
	t.Run("wrong name", func(t *testing.T) {
		hs := newHS12(t, pool, func(c *stdtls.Config) { c.ServerName = "other.example" })
		_, err := pump(t, hs, server(nil), 4096)
		var verify *stdtls.CertificateVerificationError
		if !errors.As(err, &verify) {
			t.Fatalf("got %v, want a certificate verification error", err)
		}
	})
	t.Run("VerifyConnection", func(t *testing.T) {
		var version uint16
		hs := newHS12(t, pool, func(c *stdtls.Config) {
			c.VerifyConnection = func(state stdtls.ConnectionState) error {
				version = state.Version
				return nil
			}
		})
		serverErr, clientErr := pump(t, hs, server(nil), 4096)
		if serverErr != nil || clientErr != nil || version != stdtls.VersionTLS12 {
			t.Fatalf("server %v, client %v, version %x", serverErr, clientErr, version)
		}
	})
}

func TestNativeClientQualification12(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *stdtls.Config
		want   bool
	}{
		{"default", &stdtls.Config{}, true},
		{"TLS 1.2 minimum", &stdtls.Config{MinVersion: stdtls.VersionTLS12}, true},
		{"TLS 1.2 only", &stdtls.Config{MinVersion: stdtls.VersionTLS12, MaxVersion: stdtls.VersionTLS12}, true},
		{"TLS 1.1 allowed", &stdtls.Config{MinVersion: stdtls.VersionTLS11}, false},
		{"capped at 1.1", &stdtls.Config{MaxVersion: stdtls.VersionTLS11}, false},
		{"suites beyond the record layer, TLS 1.2 only", &stdtls.Config{MaxVersion: stdtls.VersionTLS12,
			CipherSuites: []uint16{stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256}}, false},
		{"suites beyond the record layer, TLS 1.3 too", &stdtls.Config{
			CipherSuites: []uint16{stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256}}, true},
	} {
		if got := nativeClient(tc.config); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}
