//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// A ticket offered with a binder that is not right costs the server no
// verification of the client chain the ticket carries: the binder is checked
// first. One with the right binder is verified and resumed.
func TestResumeChecksBinderBeforeChain(t *testing.T) {
	serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
	config := &stdtls.Config{Certificates: []stdtls.Certificate{serverCA.issue(t, "ecdsa-p256", "localhost", false)},
		ClientAuth: stdtls.VerifyClientCertIfGiven, ClientCAs: clientCA.pool}
	alice := clientCA.issue(t, "ecdsa-p256", "alice", true)
	keys := newTicketKeys(&stdtls.Config{})
	cache := NewQUICSessionCache(1)
	cli := newHS(t, serverCA.pool, func(c *stdtls.Config) { c.Certificates = []stdtls.Certificate{alice} })
	cli.sessions = cache
	if s13Converse(t, cli, newSrvHS(config, keys), cli.hello()) != 1 {
		t.Fatal("no ticket")
	}

	var checked int
	sessionPeerChecked = func() { checked++ }
	t.Cleanup(func() { sessionPeerChecked = nil })

	_, hello := s13Resuming(t, serverCA.pool, cache, false)
	forged := bytes.Clone(hello)
	forged[len(forged)-1] ^= 1 // the last byte of the binder
	srv := newSrvHS(config, keys)
	if _, _, _, herr := srv.process(forged); herr != nil || srv.resumed {
		t.Fatalf("a wrong binder: resumed %v, %v", srv.resumed, herr)
	}
	if checked != 0 {
		t.Fatalf("a wrong binder: the chain was checked %d times", checked)
	}

	srv = newSrvHS(config, keys)
	if _, _, _, herr := srv.process(hello); herr != nil || !srv.resumed || len(srv.peerChains) != 1 {
		t.Fatalf("the right binder: resumed %v, %d chains, %v", srv.resumed, len(srv.peerChains), herr)
	}
	if checked != 1 {
		t.Fatalf("the right binder: the chain was checked %d times", checked)
	}
}

// A server's verification of a client's chain, and its VerifyPeerCertificate,
// run off the goroutine that drives the handshake, as a client's checks of the
// server do: while one takes its time, the other handshakes of the same loop
// and worker go on, in TLS 1.3 and 1.2, and the slow one completes once it
// returns, with the client known by its chain.
func TestNativeServerClientChecksDoNotBlock(t *testing.T) {
	serverCA, clientCA := newTestCA(t, "server CA"), newTestCA(t, "client CA")
	release := make(chan struct{})
	serverConfig := &stdtls.Config{Certificates: []stdtls.Certificate{serverCA.issue(t, "ecdsa-p256", "localhost", false)},
		ClientAuth: stdtls.VerifyClientCertIfGiven, ClientCAs: clientCA.pool,
		VerifyPeerCertificate: func(raw [][]byte, chains [][]*x509.Certificate) error {
			if len(raw) > 0 && len(chains) == 1 && chains[0][0].Subject.CommonName == "slow" {
				<-release
			}
			return nil
		}}
	engineConfig := fib.DefaultConfig()
	engineConfig.IOPollers, engineConfig.WorkerCount, engineConfig.SharedTaskPool = false, 1, false
	ours := newStateServer()
	_, addr := startEchoServer(t, engineConfig, NewServer(serverConfig, ours))
	// Before the engine stops, so that a test that fails leaves no check
	// waiting.
	t.Cleanup(func() { close(release) })
	slowCert, quickCert := clientCA.issue(t, "ecdsa-p256", "slow", true), clientCA.issue(t, "ecdsa-p256", "quick", true)
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		config := func(cert *stdtls.Certificate) *stdtls.Config {
			c := &stdtls.Config{RootCAs: serverCA.pool, ServerName: "localhost", MinVersion: version, MaxVersion: version}
			if cert != nil {
				c.Certificates = []stdtls.Certificate{*cert}
			}
			return c
		}
		slow := make(chan error, 1)
		go func() { _, err := dialEcho(addr, config(&slowCert)); slow <- err }()
		time.Sleep(50 * time.Millisecond)
		for _, cert := range []*stdtls.Certificate{&quickCert, nil} {
			if _, err := dialEcho(addr, config(cert)); err != nil {
				t.Fatalf("%s: %v", stdtls.VersionName(version), err)
			}
			if got := ours.next(t); got.err != nil {
				t.Fatalf("%s: %v", stdtls.VersionName(version), got.err)
			}
		}
		select {
		case err := <-slow:
			t.Fatalf("%s: the slow handshake ended before its check returned: %v", stdtls.VersionName(version), err)
		default:
		}
		release <- struct{}{}
		if err := <-slow; err != nil {
			t.Fatalf("%s: %v", stdtls.VersionName(version), err)
		}
		if got := ours.next(t); got.err != nil || len(got.state.VerifiedChains) != 1 || got.state.PeerCertificates[0].Subject.CommonName != "slow" {
			t.Fatalf("%s: %+v", stdtls.VersionName(version), got)
		}
	}
}
