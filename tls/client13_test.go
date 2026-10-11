//go:build linux || darwin || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// issueCert makes a self-signed certificate for localhost with key.
func issueCert(t *testing.T, key crypto.Signer) (stdtls.Certificate, *x509.CertPool) {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// issueCertFor is issueCert for a certificate that names host, and returns
// the certificate parsed as well.
func issueCertFor(t *testing.T, key crypto.Signer, host string) (stdtls.Certificate, *x509.Certificate) {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{host},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, cert
}

func keyOf(t *testing.T, kind string) crypto.Signer {
	t.Helper()
	switch kind {
	case "ecdsa-p256":
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		return k
	case "ecdsa-p384":
		k, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		return k
	case "rsa":
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		return k
	case "ed25519":
		_, k, _ := ed25519.GenerateKey(rand.Reader)
		return k
	}
	t.Fatal(kind)
	return nil
}

// lockedBuffer collects a key log from the goroutine that writes it.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) lines() map[string]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	lines := map[string]string{}
	for _, line := range strings.Split(l.b.String(), "\n") {
		if fields := strings.Fields(line); len(fields) == 3 {
			lines[fields[0]] = fields[1] + " " + fields[2]
		}
	}
	return lines
}

// pump runs a handshake of hs against a crypto/tls server, delivering what the
// server sends in pieces of at most chunk bytes, and returns what the
// handshake made of it. A completed handshake is followed by a round trip
// under the application keys.
func pump(t *testing.T, hs *clientHS, serverConfig *stdtls.Config, chunk int) (serverErr, clientErr error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	served := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			served <- err
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
		conn := stdtls.Server(raw, serverConfig)
		if err := conn.Handshake(); err != nil {
			served <- err
			return
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "hello" {
			served <- errors.New("server read " + string(buf) + ": " + errString(err))
			return
		}
		_, err = conn.Write([]byte("world"))
		served <- err
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(hs.hello()); err != nil {
		t.Fatal(err)
	}
	var pending []byte
	buf := make([]byte, chunk)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return <-served, err
		}
		pending = append(pending, buf[:n]...)
		consumed, out, done, herr := hs.process(pending)
		if len(out) > 0 {
			if _, err := conn.Write(out); err != nil {
				t.Fatal(err)
			}
		}
		if herr != nil {
			if herr.alert != 0 {
				_, _ = conn.Write(hs.alertRecord(herr.alert))
			}
			_ = conn.Close()
			return <-served, herr
		}
		pending = append(pending[:0], pending[consumed:]...)
		if !done {
			continue
		}
		// Application data under the keys the handshake ended with.
		record, err := hs.tx.seal(nil, recordTypeApplicationData, []byte("hello"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write(record); err != nil {
			t.Fatal(err)
		}
		for {
			for len(pending) >= recordHeaderLen {
				size := recordHeaderLen + int(pending[3])<<8 + int(pending[4])
				if len(pending) < size {
					break
				}
				typ, plain, alert, ok := hs.rx.open(pending[:size])
				if !ok {
					t.Fatalf("record did not open: alert %d", alert)
				}
				pending = pending[size:]
				if typ == recordTypeApplicationData {
					if string(plain) != "world" {
						t.Fatalf("server said %q", plain)
					}
					return <-served, nil
				}
			}
			n, err := conn.Read(buf)
			if err != nil {
				return <-served, err
			}
			pending = append(pending, buf[:n]...)
		}
	}
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// newHS makes a handshake against a server that has cert, trusting pool.
func newHS(t *testing.T, pool *x509.CertPool, configure func(*stdtls.Config)) *clientHS {
	t.Helper()
	config := &stdtls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: stdtls.VersionTLS13}
	if configure != nil {
		configure(config)
	}
	hs, err := newClientHS(config)
	if err != nil {
		t.Fatal(err)
	}
	return hs
}

func TestClientHandshakeAgainstCryptoTLS(t *testing.T) {
	for _, kind := range []string{"ecdsa-p256", "ecdsa-p384", "rsa", "ed25519"} {
		for _, chunk := range []int{1, 7, 4096} {
			t.Run(kind+"/"+strconv.Itoa(chunk), func(t *testing.T) {
				cert, pool := issueCert(t, keyOf(t, kind))
				log := &lockedBuffer{}
				server := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MinVersion: stdtls.VersionTLS13, KeyLogWriter: log}
				clientLog := &lockedBuffer{}
				hs := newHS(t, pool, func(c *stdtls.Config) { c.KeyLogWriter = clientLog })
				serverErr, clientErr := pump(t, hs, server, chunk)
				if serverErr != nil || clientErr != nil {
					t.Fatalf("server: %v, client: %v", serverErr, clientErr)
				}
				state := hs.connectionState()
				if state.Version != stdtls.VersionTLS13 || len(state.PeerCertificates) != 1 || len(state.VerifiedChains) != 1 {
					t.Fatalf("state %+v", state)
				}
				// The secrets are the ones the server derived, which is the
				// proof that the key schedule is right.
				want, got := log.lines(), clientLog.lines()
				if got["EXPORTER_SECRET"] == "" {
					t.Error("no EXPORTER_SECRET logged")
				}
				for _, label := range []string{"CLIENT_HANDSHAKE_TRAFFIC_SECRET", "SERVER_HANDSHAKE_TRAFFIC_SECRET",
					"CLIENT_TRAFFIC_SECRET_0", "SERVER_TRAFFIC_SECRET_0"} {
					if want[label] == "" || want[label] != got[label] {
						t.Errorf("%s: client %q, server %q", label, got[label], want[label])
					}
				}
			})
		}
	}
}

func TestClientHandshakeOptions(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := func(configure func(*stdtls.Config)) *stdtls.Config {
		c := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MinVersion: stdtls.VersionTLS13}
		if configure != nil {
			configure(c)
		}
		return c
	}
	t.Run("HelloRetryRequest", func(t *testing.T) {
		// The client offers X25519 first, the server wants P-256 only.
		hs := newHS(t, pool, nil)
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) { c.CurvePreferences = []stdtls.CurveID{stdtls.CurveP256} }), 4096)
		if serverErr != nil || clientErr != nil || !hs.retried || hs.group != stdtls.CurveP256 {
			t.Fatalf("server %v, client %v, retried %v, group %v", serverErr, clientErr, hs.retried, hs.group)
		}
	})
	t.Run("AES-256", func(t *testing.T) {
		hs := newHS(t, pool, nil)
		hs.suites = []uint16{stdtls.TLS_AES_256_GCM_SHA384}
		serverErr, clientErr := pump(t, hs, server(nil), 4096)
		if serverErr != nil || clientErr != nil || hs.connectionState().CipherSuite != stdtls.TLS_AES_256_GCM_SHA384 {
			t.Fatalf("server %v, client %v, suite %x", serverErr, clientErr, hs.suiteID)
		}
	})
	t.Run("ALPN", func(t *testing.T) {
		hs := newHS(t, pool, func(c *stdtls.Config) { c.NextProtos = []string{"http/1.1", "h2"} })
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) { c.NextProtos = []string{"h2"} }), 4096)
		if serverErr != nil || clientErr != nil || hs.connectionState().NegotiatedProtocol != "h2" {
			t.Fatalf("server %v, client %v, protocol %q", serverErr, clientErr, hs.protocol)
		}
	})
	t.Run("SNI", func(t *testing.T) {
		var seen string
		hs := newHS(t, pool, nil)
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) {
			c.GetConfigForClient = func(hello *stdtls.ClientHelloInfo) (*stdtls.Config, error) {
				seen = hello.ServerName
				return nil, nil
			}
		}), 4096)
		if serverErr != nil || clientErr != nil || seen != "localhost" {
			t.Fatalf("server %v, client %v, server name %q", serverErr, clientErr, seen)
		}
	})
	t.Run("client certificate requested", func(t *testing.T) {
		hs := newHS(t, pool, nil)
		serverErr, clientErr := pump(t, hs, server(func(c *stdtls.Config) { c.ClientAuth = stdtls.RequestClientCert }), 4096)
		if serverErr != nil || clientErr != nil || !hs.certReq {
			t.Fatalf("server %v, client %v, requested %v", serverErr, clientErr, hs.certReq)
		}
	})
	t.Run("client certificate required", func(t *testing.T) {
		hs := newHS(t, pool, nil)
		serverErr, _ := pump(t, hs, server(func(c *stdtls.Config) { c.ClientAuth = stdtls.RequireAnyClientCert }), 4096)
		if serverErr == nil {
			t.Fatal("a server that requires a certificate accepted none")
		}
	})
}

func TestClientHandshakeFailures(t *testing.T) {
	cert, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	server := &stdtls.Config{Certificates: []stdtls.Certificate{cert}}
	t.Run("wrong name", func(t *testing.T) {
		hs := newHS(t, pool, func(c *stdtls.Config) { c.ServerName = "other.example" })
		_, err := pump(t, hs, server, 4096)
		var verify *stdtls.CertificateVerificationError
		if !errors.As(err, &verify) {
			t.Fatalf("got %v, want a certificate verification error", err)
		}
	})
	t.Run("untrusted", func(t *testing.T) {
		hs := newHS(t, x509.NewCertPool(), nil)
		_, err := pump(t, hs, server, 4096)
		var verify *stdtls.CertificateVerificationError
		if !errors.As(err, &verify) {
			t.Fatalf("got %v, want a certificate verification error", err)
		}
	})
	t.Run("insecure skip verify", func(t *testing.T) {
		hs := newHS(t, nil, func(c *stdtls.Config) { c.InsecureSkipVerify = true; c.ServerName = "" })
		serverErr, clientErr := pump(t, hs, server, 4096)
		if serverErr != nil || clientErr != nil {
			t.Fatalf("server %v, client %v", serverErr, clientErr)
		}
	})
	t.Run("expired", func(t *testing.T) {
		hs := newHS(t, pool, func(c *stdtls.Config) { c.Time = func() time.Time { return time.Now().Add(48 * time.Hour) } })
		_, err := pump(t, hs, server, 4096)
		var verify *stdtls.CertificateVerificationError
		if !errors.As(err, &verify) {
			t.Fatalf("got %v, want a certificate verification error", err)
		}
	})
	t.Run("TLS 1.2 only server", func(t *testing.T) {
		hs := newHS(t, pool, nil)
		old := &stdtls.Config{Certificates: []stdtls.Certificate{cert}, MaxVersion: stdtls.VersionTLS12}
		serverErr, clientErr := pump(t, hs, old, 4096)
		if clientErr == nil || serverErr == nil {
			t.Fatalf("server %v, client %v: a TLS 1.2 server was talked to", serverErr, clientErr)
		}
	})
	t.Run("VerifyPeerCertificate", func(t *testing.T) {
		boom := errors.New("boom")
		var called int
		hs := newHS(t, pool, func(c *stdtls.Config) {
			c.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
				called++
				if len(raw) != 1 || len(chains) != 1 {
					t.Errorf("%d certificates, %d chains", len(raw), len(chains))
				}
				return boom
			}
		})
		_, err := pump(t, hs, server, 4096)
		if !errors.Is(err, boom) || called != 1 {
			t.Fatalf("got %v after %d calls", err, called)
		}
	})
	t.Run("VerifyConnection", func(t *testing.T) {
		var protocol string
		hs := newHS(t, pool, func(c *stdtls.Config) {
			c.NextProtos = []string{"h2"}
			c.VerifyConnection = func(state stdtls.ConnectionState) error {
				protocol = state.NegotiatedProtocol
				return nil
			}
		})
		serverErr, clientErr := pump(t, hs, &stdtls.Config{Certificates: []stdtls.Certificate{cert}, NextProtos: []string{"h2"}}, 4096)
		if serverErr != nil || clientErr != nil || protocol != "h2" {
			t.Fatalf("server %v, client %v, protocol %q", serverErr, clientErr, protocol)
		}
	})
	t.Run("no server name", func(t *testing.T) {
		if _, err := newClientHS(&stdtls.Config{MinVersion: stdtls.VersionTLS13}); err == nil {
			t.Fatal("a handshake with no name to verify was prepared")
		}
	})
}

func TestNativeClientQualification(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *stdtls.Config
		want   bool
	}{
		{"nil", nil, false},
		{"default", &stdtls.Config{}, true},
		{"TLS 1.2 allowed", &stdtls.Config{MinVersion: stdtls.VersionTLS12}, true},
		{"TLS 1.1 allowed", &stdtls.Config{MinVersion: stdtls.VersionTLS11}, false},
		{"TLS 1.3 only", &stdtls.Config{MinVersion: stdtls.VersionTLS13}, true},
		{"capped below the minimum", &stdtls.Config{MinVersion: stdtls.VersionTLS13, MaxVersion: stdtls.VersionTLS12}, false},
		{"client certificates", &stdtls.Config{MinVersion: stdtls.VersionTLS13, Certificates: []stdtls.Certificate{{}}}, false},
		{"session cache", &stdtls.Config{MinVersion: stdtls.VersionTLS13, ClientSessionCache: stdtls.NewLRUClientSessionCache(1)}, false},
		{"known curves", &stdtls.Config{MinVersion: stdtls.VersionTLS13, CurvePreferences: []stdtls.CurveID{stdtls.CurveP256}}, true},
		{"unknown curves", &stdtls.Config{MinVersion: stdtls.VersionTLS13, CurvePreferences: []stdtls.CurveID{stdtls.CurveP521}}, false},
	} {
		if got := nativeClient(tc.config); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// seal appends to a buffer that may be full, which moves it: the record's
// header, and so its additional data, must be written before that.
func TestSealIntoFullBuffer(t *testing.T) {
	s, _ := lookupSuite(stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	secret := bytes.Repeat([]byte{7}, 32)
	for n := 1; n < 700; n++ {
		a, _ := newKeys13(s.keyLen, s.hash, secret)
		b, _ := newKeys13(s.keyLen, s.hash, secret)
		plain := bytes.Repeat([]byte{1}, n)
		// A buffer with exactly the room the plaintext needs and no more.
		dst := make([]byte, 0, 5+n)
		record, err := a.seal(dst, recordTypeHandshake, plain, nil)
		if err != nil {
			t.Fatal(err)
		}
		if typ, got, alert, ok := b.open(record); !ok || typ != recordTypeHandshake || !bytes.Equal(got, plain) {
			t.Fatalf("%d bytes: ok %v alert %d", n, ok, alert)
		}
	}
}
