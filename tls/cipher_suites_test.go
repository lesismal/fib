//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	stdtls "crypto/tls"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// defaultSuite13 is the suite a TLS 1.3 handshake settles on when nobody says
// otherwise, which is the first of the default order.
func defaultSuite13() uint16 {
	if aesGCMHardware {
		return stdtls.TLS_AES_128_GCM_SHA256
	}
	return stdtls.TLS_CHACHA20_POLY1305_SHA256
}

// A client whose Handler names its TLS 1.3 suites offers them and no other, in
// the order given, so a connection settles on ChaCha20-Poly1305 even where the
// CPU has AES-GCM and both sides would otherwise prefer it. An unset list keeps
// the default order, and the connection stays this package's either way.
func TestTLS13CipherSuiteConfiguration(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	serverLayers := make(layerOf, 3)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, serverLayers.handler()))
	dialer, _ := startEchoServer(t, fib.DefaultConfig(), nil)

	for _, tc := range []struct {
		name   string
		suites []uint16
		want   uint16
	}{
		{"unset", nil, defaultSuite13()},
		{"ChaCha20-Poly1305 alone", []uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256}, stdtls.TLS_CHACHA20_POLY1305_SHA256},
		{"AES-256 alone", []uint16{stdtls.TLS_AES_256_GCM_SHA384}, stdtls.TLS_AES_256_GCM_SHA384},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientLayers := make(chan *layer, 1)
			handler := &settledHandler{HandlerFuncs: fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
				if err := c.Send(b); err != nil {
					c.Close()
				}
			}}, layers: clientLayers}
			err := DialWithOptions(dialer, "tcp", addr, 5*time.Second, clientConfig,
				DialOptions{TLS13CipherSuites: tc.suites}, handler, nil)
			if err != nil {
				t.Fatal(err)
			}
			cl, sl := <-clientLayers, <-serverLayers
			for _, l := range []*layer{cl, sl} {
				state, ok := ConnectionState(l.c)
				if !ok || state.Version != stdtls.VersionTLS13 || state.CipherSuite != tc.want {
					t.Fatalf("settled on suite %#04x version %#04x, want %#04x %#04x",
						state.CipherSuite, state.Version, tc.want, stdtls.VersionTLS13)
				}
				if !taken(l) {
					t.Fatal("the handshake stayed on crypto/tls")
				}
			}
			cl.c.Close()
		})
	}
}

// SetCipherSuites narrows what a QUIC client's ClientHello offers, in the order
// given, and a server that follows its client's preference settles on the first
// of them: the order is the client's, AES-256 before AES-128 if it says so. An
// empty list keeps the default order.
func TestQUICCipherSuiteConfiguration(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	for _, tc := range []struct {
		name   string
		suites []uint16
		want   uint16
	}{
		{"unset", nil, defaultSuite13()},
		{"empty list", []uint16{}, defaultSuite13()},
		{"ChaCha20-Poly1305 alone", []uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256}, stdtls.TLS_CHACHA20_POLY1305_SHA256},
		{"AES-256 first", []uint16{stdtls.TLS_AES_256_GCM_SHA384, stdtls.TLS_AES_128_GCM_SHA256}, stdtls.TLS_AES_256_GCM_SHA384},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := ourClient(t, quicClientConfig(pool, nil))
			client.SetCipherSuites(tc.suites)
			cli, srv, err := quicRun(t, client, ourServer(t, quicServerConfig(cert, nil)), 4096)
			if err != nil {
				t.Fatal(err)
			}
			quicCheck(t, cli, srv)
			if cli.suite != tc.want || srv.suite != tc.want {
				t.Fatalf("suites %#04x and %#04x, want %#04x", cli.suite, srv.suite, tc.want)
			}
		})
	}
	t.Run("ChaCha20-Poly1305 against crypto/tls", func(t *testing.T) {
		// crypto/tls's server follows the client's preference, so a hello with
		// ChaCha20-Poly1305 alone settles on it there too.
		client := ourClient(t, quicClientConfig(pool, nil))
		client.SetCipherSuites([]uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256})
		cli, srv, err := quicRun(t, client, stdServer(quicServerConfig(cert, nil)), 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if cli.suite != stdtls.TLS_CHACHA20_POLY1305_SHA256 || srv.suite != stdtls.TLS_CHACHA20_POLY1305_SHA256 {
			t.Fatalf("suites %#04x and %#04x", cli.suite, srv.suite)
		}
	})
	t.Run("the server ignores it", func(t *testing.T) {
		// A TLS 1.3 server has no suites of its own to offer: it settles on
		// one of the client's, so the setter changes nothing there.
		server := ourServer(t, quicServerConfig(cert, nil))
		server.SetCipherSuites([]uint16{stdtls.TLS_CHACHA20_POLY1305_SHA256})
		client := ourClient(t, quicClientConfig(pool, nil))
		client.SetCipherSuites([]uint16{stdtls.TLS_AES_128_GCM_SHA256})
		cli, srv, err := quicRun(t, client, server, 4096)
		if err != nil {
			t.Fatal(err)
		}
		quicCheck(t, cli, srv)
		if cli.suite != stdtls.TLS_AES_128_GCM_SHA256 || srv.suite != stdtls.TLS_AES_128_GCM_SHA256 {
			t.Fatalf("suites %#04x and %#04x, want the client's %#04x",
				cli.suite, srv.suite, stdtls.TLS_AES_128_GCM_SHA256)
		}
	})
}
