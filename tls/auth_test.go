//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/rand"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// testCA issues the certificates of the client authentication tests.
type testCA struct {
	cert *x509.Certificate
	key  crypto.Signer
	pool *x509.CertPool
}

var serial atomic.Int64

func newTestCA(t testing.TB, name string) *testCA {
	t.Helper()
	key := keyOf(t, "ecdsa-p256")
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial.Add(1)),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// issue makes a certificate with a key of kind for name, for a client or a
// server.
func (ca *testCA) issue(t testing.TB, kind, name string, client bool) stdtls.Certificate {
	t.Helper()
	key := keyOf(t, kind)
	usage := x509.ExtKeyUsageServerAuth
	if client {
		usage = x509.ExtKeyUsageClientAuth
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial.Add(1)),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     []string{name},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// serverResult is what a server made of a handshake.
type serverResult struct {
	state stdtls.ConnectionState
	err   error
}

// stateServer is a handler that reports each connection's handshake and
// echoes.
type stateServer struct{ results chan serverResult }

func newStateServer() *stateServer { return &stateServer{results: make(chan serverResult, 64)} }

func (s *stateServer) OnOpen(*fib.Connection)                 {}
func (s *stateServer) OnPriorityData(*fib.Connection, []byte) {}
func (s *stateServer) OnData(c *fib.Connection, b []byte)     { _ = c.Send(b) }
func (s *stateServer) OnHandshake(_ *fib.Connection, state stdtls.ConnectionState) {
	s.results <- serverResult{state: state}
}
func (s *stateServer) OnClose(_ *fib.Connection, err error) {
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		select {
		case s.results <- serverResult{err: err}:
		default:
		}
	}
}

// next is the server's outcome of the latest handshake.
func (s *stateServer) next(t *testing.T) serverResult {
	t.Helper()
	select {
	case r := <-s.results:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("the server never reported the handshake")
	}
	return serverResult{}
}

// stdStateServer is a crypto/tls server that reports each handshake and
// echoes.
func stdStateServer(t *testing.T, config *stdtls.Config) (string, *stateServer) {
	t.Helper()
	listener, err := stdtls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	s := newStateServer()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tc := conn.(*stdtls.Conn)
				_ = tc.SetDeadline(time.Now().Add(10 * time.Second))
				if err := tc.Handshake(); err != nil {
					s.results <- serverResult{err: err}
					return
				}
				s.results <- serverResult{state: tc.ConnectionState()}
				_, _ = io.Copy(tc, tc)
			}()
		}
	}()
	return listener.Addr().String(), s
}

// dialEcho handshakes with crypto/tls and has a byte echoed, which is how a
// TLS 1.3 client learns that the server turned its certificate down.
func dialEcho(addr string, config *stdtls.Config) (stdtls.ConnectionState, error) {
	conn, err := stdtls.Dial("tcp", addr, config)
	if err != nil {
		return stdtls.ConnectionState{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		return stdtls.ConnectionState{}, err
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(conn, b); err != nil {
		return stdtls.ConnectionState{}, err
	}
	return conn.ConnectionState(), nil
}

// Every mode of client authentication, with clients that send no certificate,
// one the server trusts and one it does not, ends as it does with crypto/tls,
// in TLS 1.3 and 1.2, without a worker and without handing a client to
// crypto/tls.
func TestNativeClientAuthMatchesCryptoTLS(t *testing.T) {
	started := countNative(t)
	fellBack := countFallbacks(t)
	serverCA, clientCA, otherCA := newTestCA(t, "server CA"), newTestCA(t, "client CA"), newTestCA(t, "other CA")
	serverCert := serverCA.issue(t, "ecdsa-p256", "localhost", false)
	mallory := otherCA.issue(t, "ecdsa-p256", "mallory", true)
	clients := map[string]func(*stdtls.Config){
		"none": func(*stdtls.Config) {},
		"trusted": func(c *stdtls.Config) {
			c.Certificates = []stdtls.Certificate{clientCA.issue(t, "ecdsa-p256", "alice", true)}
		},
		"rsa": func(c *stdtls.Config) {
			c.Certificates = []stdtls.Certificate{clientCA.issue(t, "rsa", "rsa-alice", true)}
		},
		"ed25519": func(c *stdtls.Config) {
			c.Certificates = []stdtls.Certificate{clientCA.issue(t, "ed25519", "ed-alice", true)}
		},
		// Not from the authorities the server names, and not sent.
		"untrusted": func(c *stdtls.Config) { c.Certificates = []stdtls.Certificate{mallory} },
		// Sent anyway.
		"untrusted, sent": func(c *stdtls.Config) {
			c.GetClientCertificate = func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) { return &mallory, nil }
		},
	}
	modes := []stdtls.ClientAuthType{stdtls.RequestClientCert, stdtls.RequireAnyClientCert,
		stdtls.VerifyClientCertIfGiven, stdtls.RequireAndVerifyClientCert}
	handshakes := 0
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		for _, mode := range modes {
			var peerCalls, connCalls atomic.Int64
			serverConfig := &stdtls.Config{
				Certificates: []stdtls.Certificate{serverCert}, ClientAuth: mode, ClientCAs: clientCA.pool,
				VerifyPeerCertificate:  func([][]byte, [][]*x509.Certificate) error { peerCalls.Add(1); return nil },
				VerifyConnection:       func(stdtls.ConnectionState) error { connCalls.Add(1); return nil },
				SessionTicketsDisabled: true,
			}
			stdAddr, stdServer := stdStateServer(t, serverConfig)
			ours := newStateServer()
			_, ourAddr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, ours))
			for name, set := range clients {
				t.Run(fmt.Sprintf("%s/%v/%s", stdtls.VersionName(version), mode, name), func(t *testing.T) {
					config := &stdtls.Config{RootCAs: serverCA.pool, ServerName: "localhost", MinVersion: version, MaxVersion: version}
					set(config)
					_, stdErr := dialEcho(stdAddr, config)
					want := stdServer.next(t)
					peerCalls.Store(0)
					connCalls.Store(0)
					_, ourErr := dialEcho(ourAddr, config)
					got := ours.next(t)
					handshakes++
					if (stdErr == nil) != (ourErr == nil) || (want.err == nil) != (got.err == nil) {
						t.Fatalf("crypto/tls: client %v, server %v; ours: client %v, server %v", stdErr, want.err, ourErr, got.err)
					}
					if got.err != nil {
						return
					}
					if len(got.state.PeerCertificates) != len(want.state.PeerCertificates) ||
						len(got.state.VerifiedChains) != len(want.state.VerifiedChains) {
						t.Fatalf("peer certificates %d, chains %d; crypto/tls %d and %d", len(got.state.PeerCertificates),
							len(got.state.VerifiedChains), len(want.state.PeerCertificates), len(want.state.VerifiedChains))
					}
					if len(want.state.PeerCertificates) > 0 && got.state.PeerCertificates[0].Subject.CommonName != want.state.PeerCertificates[0].Subject.CommonName {
						t.Fatalf("peer %q, want %q", got.state.PeerCertificates[0].Subject.CommonName, want.state.PeerCertificates[0].Subject.CommonName)
					}
					if peerCalls.Load() != 1 || connCalls.Load() != 1 {
						t.Fatalf("VerifyPeerCertificate called %d times, VerifyConnection %d", peerCalls.Load(), connCalls.Load())
					}
				})
			}
		}
	}
	if n, f := started.Load(), fellBack.Load(); n != int64(handshakes) || f != 0 {
		t.Fatalf("%d of %d handshakes began without a worker, %d fell back", n, handshakes, f)
	}
}

// A client that resumes a session is known by the certificate it sent when it
// made it, in TLS 1.3 and 1.2.
func TestNativeClientAuthResumes(t *testing.T) {
	fellBack := countFallbacks(t)
	serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{serverCA.issue(t, "ecdsa-p256", "localhost", false)},
		ClientAuth: stdtls.RequireAndVerifyClientCert, ClientCAs: clientCA.pool}
	ours := newStateServer()
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, ours))
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		config := &stdtls.Config{RootCAs: serverCA.pool, ServerName: "localhost", MinVersion: version, MaxVersion: version,
			Certificates:       []stdtls.Certificate{clientCA.issue(t, "ecdsa-p256", "alice", true)},
			ClientSessionCache: stdtls.NewLRUClientSessionCache(4)}
		for i := 0; i < 2; i++ {
			state, err := dialEcho(addr, config)
			if err != nil {
				t.Fatalf("%s, connection %d: %v", stdtls.VersionName(version), i, err)
			}
			got := ours.next(t)
			if state.DidResume != (i == 1) || got.state.DidResume != (i == 1) {
				t.Fatalf("%s, connection %d: client resumed %v, server %v", stdtls.VersionName(version), i, state.DidResume, got.state.DidResume)
			}
			if len(got.state.PeerCertificates) != 1 || got.state.PeerCertificates[0].Subject.CommonName != "alice" ||
				len(got.state.VerifiedChains) != 1 || len(got.state.VerifiedChains[0]) != 2 {
				t.Fatalf("%s, connection %d: peer %d certificates, chains %v", stdtls.VersionName(version), i,
					len(got.state.PeerCertificates), got.state.VerifiedChains)
			}
		}
	}
	if f := fellBack.Load(); f != 0 {
		t.Fatalf("%d fell back", f)
	}
}

// A client without a worker presents its certificate, from Certificates, the
// one the server's authorities allow, or from GetClientCertificate, which may
// take its time, to a crypto/tls server that requires and verifies one, and to
// a server without a worker.
func TestNativeClientCertificates(t *testing.T) {
	started := countNative(t)
	fellBack := countFallbacks(t)
	serverCA, clientCA, otherCA := newTestCA(t, "server CA"), newTestCA(t, "client CA"), newTestCA(t, "other CA")
	serverCert := serverCA.issue(t, "ecdsa-p256", "localhost", false)
	alice, bob := clientCA.issue(t, "ecdsa-p256", "alice", true), clientCA.issue(t, "rsa", "bob", true)
	mallory := otherCA.issue(t, "ecdsa-p256", "mallory", true)
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{serverCert},
		ClientAuth: stdtls.RequireAndVerifyClientCert, ClientCAs: clientCA.pool}
	stdAddr, stdServer := stdStateServer(t, serverConfig)
	ours := newStateServer()
	_, ourAddr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, ours))
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)

	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		for _, tc := range []struct {
			name string
			set  func(*stdtls.Config)
			want string
		}{
			{"Certificates", func(c *stdtls.Config) { c.Certificates = []stdtls.Certificate{alice} }, "alice"},
			{"the one the authorities allow", func(c *stdtls.Config) { c.Certificates = []stdtls.Certificate{mallory, bob} }, "bob"},
			{"GetClientCertificate", func(c *stdtls.Config) {
				c.GetClientCertificate = func(info *stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
					if info.Context() == nil || len(info.SignatureSchemes) == 0 || len(info.AcceptableCAs) != 1 || info.Version != version {
						return nil, fmt.Errorf("request info %+v", info)
					}
					time.Sleep(50 * time.Millisecond)
					return &alice, nil
				}
			}, "alice"},
		} {
			for _, server := range []struct {
				name  string
				addr  string
				state *stateServer
			}{{"crypto/tls", stdAddr, stdServer}, {"ours", ourAddr, ours}} {
				t.Run(stdtls.VersionName(version)+"/"+tc.name+"/"+server.name, func(t *testing.T) {
					startedBefore, fellBackBefore := started.Load(), fellBack.Load()
					config := &stdtls.Config{RootCAs: serverCA.pool, ServerName: "localhost", MinVersion: version, MaxVersion: version}
					tc.set(config)
					h := newCollector()
					err := Dial(client, "tcp", server.addr, 5*time.Second, config, h, func(c *fib.Connection, err error) {
						if err == nil {
							_ = c.Send([]byte("ping"))
						}
					})
					if err != nil {
						t.Fatal(err)
					}
					if got := h.waitFor(t, 4); string(got) != "ping" {
						t.Fatalf("echo %q", got)
					}
					r := server.state.next(t)
					if r.err != nil {
						t.Fatal(r.err)
					}
					if len(r.state.PeerCertificates) != 1 || r.state.PeerCertificates[0].Subject.CommonName != tc.want {
						t.Fatalf("the server saw %d certificates", len(r.state.PeerCertificates))
					}
					wantStarted := int64(1)
					if server.name == "ours" {
						wantStarted = 2
					}
					if n, f := started.Load()-startedBefore, fellBack.Load()-fellBackBefore; n != wantStarted || f != 0 {
						t.Fatalf("%d handshakes began without a worker, %d fell back", n, f)
					}
				})
			}
		}
	}

	t.Run("GetClientCertificate fails", func(t *testing.T) {
		config := &stdtls.Config{RootCAs: serverCA.pool, ServerName: "localhost",
			GetClientCertificate: func(*stdtls.CertificateRequestInfo) (*stdtls.Certificate, error) {
				return nil, errors.New("no certificate today")
			}}
		h := newCollector()
		if err := Dial(client, "tcp", stdAddr, 5*time.Second, config, h, nil); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-h.closed:
			if err == nil || !strings.Contains(err.Error(), "no certificate today") {
				t.Fatalf("closed with %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the connection never closed")
		}
		if r := stdServer.next(t); r.err == nil {
			t.Fatal("the server finished the handshake")
		}
	})
}

// GetCertificate picks a server's certificate by name, without a worker, for
// clients of TLS 1.3 and 1.2. While it takes its time for one client, the
// connections of the same loop and worker go on; an error it returns fails
// the handshake rather than handing it to crypto/tls.
func TestNativeServerGetCertificate(t *testing.T) {
	started := countNative(t)
	fellBack := countFallbacks(t)
	ca := newTestCA(t, "server CA")
	certs := map[string]*stdtls.Certificate{}
	for _, name := range []string{"a.test", "b.test", "slow.test"} {
		cert := ca.issue(t, "ecdsa-p256", name, false)
		certs[name] = &cert
	}
	release := make(chan struct{})
	var calls atomic.Int64
	serverConfig := &stdtls.Config{GetCertificate: func(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		calls.Add(1)
		if hello.Context() == nil || hello.Conn == nil || len(hello.SupportedVersions) == 0 || len(hello.Extensions) == 0 {
			return nil, fmt.Errorf("hello info %+v", hello)
		}
		if hello.ServerName == "slow.test" {
			<-release
		}
		if cert := certs[hello.ServerName]; cert != nil {
			return cert, nil
		}
		return nil, errors.New("no certificate for " + hello.ServerName)
	}}
	// One loop and one worker: a GetCertificate that blocked them would stall
	// every other handshake.
	engineConfig := fib.DefaultConfig()
	engineConfig.SplitEventLoops, engineConfig.WorkerCount, engineConfig.SharedTaskPool = false, 1, false
	ours := newStateServer()
	_, addr := startEchoServer(t, engineConfig, NewServer(serverConfig, ours))

	dial := func(name string, version uint16) (stdtls.ConnectionState, error) {
		return dialEcho(addr, &stdtls.Config{RootCAs: ca.pool, ServerName: name, MinVersion: version, MaxVersion: version})
	}
	handshakes := int64(0)
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		slow := make(chan error, 1)
		go func() {
			state, err := dial("slow.test", version)
			if err == nil && state.PeerCertificates[0].Subject.CommonName != "slow.test" {
				err = errors.New("wrong certificate")
			}
			slow <- err
		}()
		time.Sleep(50 * time.Millisecond)
		for _, name := range []string{"a.test", "b.test"} {
			state, err := dial(name, version)
			if err != nil {
				t.Fatalf("%s %s: %v", stdtls.VersionName(version), name, err)
			}
			if state.Version != version || state.PeerCertificates[0].Subject.CommonName != name {
				t.Fatalf("%s %s: version %x, certificate for %s", stdtls.VersionName(version), name, state.Version,
					state.PeerCertificates[0].Subject.CommonName)
			}
		}
		select {
		case err := <-slow:
			t.Fatalf("the slow handshake finished before GetCertificate returned: %v", err)
		default:
		}
		release <- struct{}{}
		if err := <-slow; err != nil {
			t.Fatalf("%s slow: %v", stdtls.VersionName(version), err)
		}
		if _, err := dial("unknown.test", version); err == nil {
			t.Fatalf("%s: a name GetCertificate has no certificate for was served", stdtls.VersionName(version))
		}
		handshakes += 4
	}
	if n, f := started.Load(), fellBack.Load(); n != handshakes || f != 0 {
		t.Fatalf("%d of %d handshakes began without a worker, %d fell back", n, handshakes, f)
	}
	if c := calls.Load(); c != handshakes {
		t.Fatalf("GetCertificate called %d times for %d handshakes", c, handshakes)
	}
}

// A client that names no server is given the Config's own certificate, and
// GetCertificate is not asked, as crypto/tls has it; one that names a server
// GetCertificate does not know is given the Config's too.
func TestNativeServerGetCertificateDefaults(t *testing.T) {
	fellBack := countFallbacks(t)
	ca := newTestCA(t, "server CA")
	fallback, named := ca.issue(t, "ecdsa-p256", "localhost", false), ca.issue(t, "rsa", "named.test", false)
	var calls atomic.Int64
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{fallback},
		GetCertificate: func(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
			calls.Add(1)
			if hello.ServerName == "named.test" {
				return &named, nil
			}
			return nil, nil
		}}
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
	for _, tc := range []struct{ name, want string }{{"named.test", "named.test"}, {"other.test", "localhost"}, {"", "localhost"}} {
		config := &stdtls.Config{RootCAs: ca.pool, ServerName: tc.name, InsecureSkipVerify: tc.name != "named.test"}
		state, err := dialEcho(addr, config)
		if err != nil {
			t.Fatalf("%q: %v", tc.name, err)
		}
		if got := state.PeerCertificates[0].Subject.CommonName; got != tc.want {
			t.Fatalf("%q: certificate for %s, want %s", tc.name, got, tc.want)
		}
	}
	if c, f := calls.Load(), fellBack.Load(); c != 2 || f != 0 {
		t.Fatalf("GetCertificate called %d times, %d fell back", c, f)
	}
}

// The QUIC handshake calls GetCertificate where it runs when the QUIC layer
// cannot be woken, and on a worker when it can, and authenticates clients both
// ways, against crypto/tls and against itself.
func TestQUICCertificates(t *testing.T) {
	fellBack := countFallbacks(t)
	serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
	serverCert := serverCA.issue(t, "ecdsa-p256", "localhost", false)
	alice := clientCA.issue(t, "ecdsa-p256", "alice", true)
	var calls atomic.Int64
	getCert := func(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		calls.Add(1)
		if hello.ServerName != "localhost" || hello.Context() == nil {
			return nil, errors.New("unexpected hello")
		}
		time.Sleep(10 * time.Millisecond)
		return &serverCert, nil
	}
	clientConfig := func(withCert bool) *stdtls.Config {
		return quicClientConfig(serverCA.pool, func(c *stdtls.Config) {
			if withCert {
				c.Certificates = []stdtls.Certificate{alice}
			}
		})
	}
	serverConfig := func(getCertificate bool, auth stdtls.ClientAuthType) *stdtls.Config {
		return quicServerConfig(serverCert, func(c *stdtls.Config) {
			if getCertificate {
				c.Certificates, c.GetCertificate = nil, getCert
			}
			c.ClientAuth, c.ClientCAs = auth, clientCA.pool
		})
	}
	woken := func(conn *QUICConn) (*QUICConn, chan struct{}) {
		wake := make(chan struct{}, 1)
		conn.SetWake(func() { wake <- struct{}{} })
		return conn, wake
	}
	for _, tc := range []struct {
		name           string
		client, server func() (quicEnd, chan struct{})
		peer           string
	}{
		{"GetCertificate inline", func() (quicEnd, chan struct{}) { return stdClient(clientConfig(false)), nil },
			func() (quicEnd, chan struct{}) { return ourServer(t, serverConfig(true, stdtls.NoClientCert)), nil }, ""},
		{"GetCertificate on a worker", func() (quicEnd, chan struct{}) { return stdClient(clientConfig(false)), nil },
			func() (quicEnd, chan struct{}) { return woken(ourServer(t, serverConfig(true, stdtls.NoClientCert))) }, ""},
		{"client auth, our server", func() (quicEnd, chan struct{}) { return stdClient(clientConfig(true)), nil },
			func() (quicEnd, chan struct{}) {
				return woken(ourServer(t, serverConfig(true, stdtls.RequireAndVerifyClientCert)))
			}, "alice"},
		{"client auth, our client", func() (quicEnd, chan struct{}) { return woken(ourClient(t, clientConfig(true))) },
			func() (quicEnd, chan struct{}) {
				return stdServer(serverConfig(false, stdtls.RequireAndVerifyClientCert)), nil
			}, "alice"},
		{"client auth, ours both", func() (quicEnd, chan struct{}) { return ourClient(t, clientConfig(true)), nil },
			func() (quicEnd, chan struct{}) {
				return woken(ourServer(t, serverConfig(true, stdtls.RequireAndVerifyClientCert)))
			}, "alice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, clientWake := tc.client()
			server, serverWake := tc.server()
			cli, srv, err := quicRunWith(t, client, server, 1000, quicRunOptions{wakes: []chan struct{}{clientWake, serverWake}})
			if err != nil {
				t.Fatal(err)
			}
			quicCheck(t, cli, srv)
			state := server.ConnectionState()
			if tc.peer == "" && len(state.PeerCertificates) != 0 ||
				tc.peer != "" && (len(state.PeerCertificates) != 1 || state.PeerCertificates[0].Subject.CommonName != tc.peer || len(state.VerifiedChains) != 1) {
				t.Fatalf("the server saw %d certificates, %d chains", len(state.PeerCertificates), len(state.VerifiedChains))
			}
			if got := client.ConnectionState().PeerCertificates; len(got) != 1 || !bytes.Equal(got[0].Raw, serverCert.Certificate[0]) {
				t.Fatal("the client saw another certificate")
			}
		})
	}
	if f := fellBack.Load(); f != 0 {
		t.Fatalf("%d fell back", f)
	}
}

// A client turned away for want of a certificate learns why, over QUIC too.
func TestQUICClientAuthRefused(t *testing.T) {
	serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
	serverCert := serverCA.issue(t, "ecdsa-p256", "localhost", false)
	server := ourServer(t, quicServerConfig(serverCert, func(c *stdtls.Config) {
		c.ClientAuth, c.ClientCAs = stdtls.RequireAndVerifyClientCert, clientCA.pool
	}))
	_, _, err := quicRun(t, stdClient(quicClientConfig(serverCA.pool, nil)), server, 1000)
	var alert stdtls.AlertError
	if !errors.As(err, &alert) || alert != alertCertificateRequired {
		t.Fatalf("got %v, want the certificate_required alert", err)
	}
}

// A client's checks of the server, the verification of its chain,
// VerifyPeerCertificate and VerifyConnection, run off the goroutine that drives
// the handshake: while one takes its time for one connection, the connections
// of the same loop and worker go on, in TLS 1.3 and 1.2, without a worker for
// their handshake. One that fails closes the connection with its error, and
// the server gets no Finished.
func TestNativeClientChecksDoNotBlock(t *testing.T) {
	started := countNative(t)
	fellBack := countFallbacks(t)
	ca := newTestCA(t, "server CA")
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{ca.issue(t, "ecdsa-p256", "localhost", false)}}
	addr, _ := stdStateServer(t, serverConfig)
	// One loop and one worker: a check that blocked them would stall every
	// other handshake.
	engineConfig := fib.DefaultConfig()
	engineConfig.SplitEventLoops, engineConfig.WorkerCount, engineConfig.SharedTaskPool = false, 1, false
	client, _ := startEchoServer(t, engineConfig, nil)

	dial := func(t *testing.T, config *stdtls.Config, msg string) *collector {
		t.Helper()
		h := newCollector()
		err := Dial(client, "tcp", addr, 5*time.Second, config, h, func(c *fib.Connection, err error) {
			if err == nil {
				_ = c.Send([]byte(msg))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	type check struct {
		name string
		set  func(c *stdtls.Config, run func() error)
	}
	checks := []check{
		{"VerifyPeerCertificate", func(c *stdtls.Config, run func() error) {
			c.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
				if len(raw) != 1 || len(chains) != 1 {
					return fmt.Errorf("%d certificates, %d chains", len(raw), len(chains))
				}
				return run()
			}
		}},
		{"VerifyConnection", func(c *stdtls.Config, run func() error) {
			c.VerifyConnection = func(state stdtls.ConnectionState) error {
				if len(state.PeerCertificates) != 1 || len(state.VerifiedChains) != 1 || state.ServerName != "localhost" {
					return fmt.Errorf("state %+v", state)
				}
				return run()
			}
		}},
		{"insecure VerifyPeerCertificate", func(c *stdtls.Config, run func() error) {
			c.InsecureSkipVerify, c.RootCAs = true, nil
			c.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
				if len(raw) != 1 || len(chains) != 0 {
					return fmt.Errorf("%d certificates, %d chains", len(raw), len(chains))
				}
				return run()
			}
		}},
	}
	handshakes := int64(0)
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		config := func(set func(*stdtls.Config)) *stdtls.Config {
			c := &stdtls.Config{RootCAs: ca.pool, ServerName: "localhost", MinVersion: version, MaxVersion: version}
			if set != nil {
				set(c)
			}
			return c
		}
		for _, ck := range checks {
			t.Run(stdtls.VersionName(version)+"/"+ck.name, func(t *testing.T) {
				release := make(chan struct{})
				slow := dial(t, config(func(c *stdtls.Config) {
					ck.set(c, func() error { <-release; return nil })
				}), "slow")
				time.Sleep(50 * time.Millisecond)
				// The same check that does not wait, and none at all.
				quick := dial(t, config(func(c *stdtls.Config) { ck.set(c, func() error { return nil }) }), "quick")
				if got := quick.waitFor(t, 5); string(got) != "quick" {
					t.Fatalf("echo %q", got)
				}
				plain := dial(t, config(nil), "plain")
				if got := plain.waitFor(t, 5); string(got) != "plain" {
					t.Fatalf("echo %q", got)
				}
				slow.mu.Lock()
				shaken := slow.shaken
				slow.mu.Unlock()
				if shaken {
					t.Fatal("the slow handshake finished before its check returned")
				}
				close(release)
				if got := slow.waitFor(t, 4); string(got) != "slow" {
					t.Fatalf("echo %q", got)
				}
			})
			t.Run(stdtls.VersionName(version)+"/"+ck.name+" fails", func(t *testing.T) {
				addr, server := stdStateServer(t, serverConfig)
				h := newCollector()
				c := config(func(c *stdtls.Config) {
					ck.set(c, func() error { time.Sleep(10 * time.Millisecond); return errors.New("not this server") })
				})
				if err := Dial(client, "tcp", addr, 5*time.Second, c, h, nil); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-h.closed:
					if err == nil || !strings.Contains(err.Error(), "not this server") {
						t.Fatalf("closed with %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the connection never closed")
				}
				// In TLS 1.2 the client's Finished goes first, and
				// VerifyConnection has its say after the server's.
				if r := server.next(t); r.err == nil && (version == stdtls.VersionTLS13 || ck.name != "VerifyConnection") {
					t.Fatal("the server finished the handshake")
				}
			})
			handshakes += 4
		}
	}
	if n, f := started.Load(), fellBack.Load(); n != handshakes || f != 0 {
		t.Fatalf("%d of %d handshakes began without a worker, %d fell back", n, handshakes, f)
	}
}

// A client certificate whose key cannot sign by itself is passed over, as
// crypto/tls passes it over, by a handshake that runs without a worker: a
// server that asks for a certificate without requiring one gets none, and one
// that requires one turns the client away.
func TestNativeClientCertificateThatCannotSign(t *testing.T) {
	started := countNative(t)
	serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
	unsigned := clientCA.issue(t, "ecdsa-p256", "alice", true)
	unsigned.PrivateKey = struct{ crypto.PublicKey }{unsigned.PrivateKey.(crypto.Signer).Public()}
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		for _, auth := range []stdtls.ClientAuthType{stdtls.RequestClientCert, stdtls.RequireAnyClientCert} {
			t.Run(fmt.Sprintf("%s/%v", stdtls.VersionName(version), auth), func(t *testing.T) {
				before := started.Load()
				addr, server := stdStateServer(t, &stdtls.Config{
					Certificates: []stdtls.Certificate{serverCA.issue(t, "ecdsa-p256", "localhost", false)},
					ClientAuth:   auth, ClientCAs: clientCA.pool,
				})
				config := &stdtls.Config{RootCAs: serverCA.pool, ServerName: "localhost", MinVersion: version, MaxVersion: version,
					Certificates: []stdtls.Certificate{unsigned}}
				h := newCollector()
				if err := Dial(client, "tcp", addr, 5*time.Second, config, h, func(c *fib.Connection, err error) {
					if err == nil {
						_ = c.Send([]byte("ping"))
					}
				}); err != nil {
					t.Fatal(err)
				}
				r := server.next(t)
				if auth == stdtls.RequestClientCert {
					if r.err != nil || len(r.state.PeerCertificates) != 0 {
						t.Fatalf("server: %v, %d certificates", r.err, len(r.state.PeerCertificates))
					}
					if got := h.waitFor(t, 4); string(got) != "ping" {
						t.Fatalf("echo %q", got)
					}
				} else if r.err == nil {
					t.Fatal("the server took a client without a certificate")
				}
				if n := started.Load() - before; n != 1 {
					t.Fatalf("%d handshakes began without a worker", n)
				}
			})
		}
	}
}

// Over QUIC too, a client's checks of the server run on a worker once the
// layer can be woken, and the handshake goes on from there when they return;
// one that fails fails the handshake, before the client's Finished.
func TestQUICClientChecks(t *testing.T) {
	ca := newTestCA(t, "server CA")
	serverCert := ca.issue(t, "ecdsa-p256", "localhost", false)
	for _, tc := range []struct {
		name string
		set  func(c *stdtls.Config, run func() error)
	}{
		{"VerifyPeerCertificate", func(c *stdtls.Config, run func() error) {
			c.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error { return run() }
		}},
		{"VerifyConnection", func(c *stdtls.Config, run func() error) {
			c.VerifyConnection = func(state stdtls.ConnectionState) error {
				if state.NegotiatedProtocol != "h3test" || len(state.VerifiedChains) != 1 {
					return fmt.Errorf("state %+v", state)
				}
				return run()
			}
		}},
	} {
		for _, fails := range []bool{false, true} {
			name := tc.name
			if fails {
				name += " fails"
			}
			t.Run(name, func(t *testing.T) {
				var calls atomic.Int64
				client := ourClient(t, quicClientConfig(ca.pool, func(c *stdtls.Config) {
					tc.set(c, func() error {
						calls.Add(1)
						time.Sleep(10 * time.Millisecond)
						if fails {
							return errors.New("not this server")
						}
						return nil
					})
				}))
				wake := make(chan struct{}, 1)
				client.SetWake(func() { wake <- struct{}{} })
				server := stdServer(quicServerConfig(serverCert, nil))
				cli, srv, err := quicRunWith(t, client, server, 1000, quicRunOptions{wakes: []chan struct{}{wake}, clientErrors: true})
				if fails {
					if err == nil || !strings.Contains(err.Error(), "not this server") {
						t.Fatalf("got %v", err)
					}
					if srv.done {
						t.Fatal("the server finished the handshake")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					quicCheck(t, cli, srv)
				}
				if n := calls.Load(); n != 1 {
					t.Fatalf("the check ran %d times", n)
				}
			})
		}
	}
}
