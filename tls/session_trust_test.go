package tls

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"testing"
	"time"
)

// A session is offered to a server only if the Config would trust the server
// it was made with: any session if the Config does not verify, otherwise one
// whose chain was verified, is current, is the ServerName's and has its root
// among the RootCAs the Config names, or, for a Config that trusts the
// system's roots, was verified against them: one that a Config with roots of
// its own verified is not resumed under the system's.
func TestSessionTrusted(t *testing.T) {
	ca := newTestCA(t, "session CA")
	other := newTestCA(t, "session other CA")
	leaf, err := x509.ParseCertificate(ca.issue(t, "ecdsa-p256", "localhost", false).Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	verified := [][]*x509.Certificate{{leaf, ca.cert}}
	later := func() time.Time { return time.Now().Add(48 * time.Hour) }
	for _, tc := range []struct {
		name   string
		config *stdtls.Config
		chains [][]*x509.Certificate
		system bool
		want   bool
	}{
		{"verified", &stdtls.Config{ServerName: "localhost", RootCAs: ca.pool}, verified, false, true},
		{"the system's roots, by them", &stdtls.Config{ServerName: "localhost"}, verified, true, true},
		{"the system's roots, by others", &stdtls.Config{ServerName: "localhost"}, verified, false, false},
		{"not verified, nor now", &stdtls.Config{ServerName: "localhost", InsecureSkipVerify: true}, nil, false, true},
		{"not verified", &stdtls.Config{ServerName: "localhost", RootCAs: ca.pool}, nil, false, false},
		{"another server's", &stdtls.Config{ServerName: "example.com", RootCAs: ca.pool}, verified, false, false},
		{"expired", &stdtls.Config{ServerName: "localhost", RootCAs: ca.pool, Time: later}, verified, false, false},
		{"a root no longer trusted", &stdtls.Config{ServerName: "localhost", RootCAs: other.pool}, verified, false, false},
	} {
		h, err := newClientHS(tc.config)
		if err != nil {
			t.Fatal(err)
		}
		if got := h.sessionTrusted(&QUICSession{chains: tc.chains, systemRoots: tc.system}); got != tc.want {
			t.Errorf("%s: trusted %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A session a client with roots of its own made, in a cache shared with a
// client that trusts the system's, is not offered by that one.
func TestSessionNotResumedAcrossRoots(t *testing.T) {
	config, pool := ecdsaServer(t)
	cache := s13Session(t, config, pool, newTicketKeys(&stdtls.Config{}), false, nil)
	s := cache.Get("localhost")
	if s == nil || s.systemRoots || len(s.chains) == 0 {
		t.Fatalf("session %+v", s)
	}
	cli := newHS(t, pool, nil)
	cli.sessions = cache
	if !cli.pickSession() {
		t.Fatal("not offered under the roots that verified it")
	}
	cli = newHS(t, nil, nil)
	cli.sessions = cache
	if cli.pickSession() {
		t.Fatal("offered under the system's roots")
	}
}
