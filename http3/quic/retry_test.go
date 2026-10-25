package quic

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lesismal/fib/tlstest"
)

// TestRetryHandshake checks a handshake that a Retry starts over: the
// client comes back with the token, the server takes its address as proven,
// and both sides check that the Retry was the server's.
func TestRetryHandshake(t *testing.T) {
	p := newTestPairOpts(t, 0, nil, pairOpts{retry: NewTokenKey()})
	p.client.mu.Lock()
	retried := p.client.retrySCID != nil
	p.client.mu.Unlock()
	server := p.serverConn(t)
	server.mu.Lock()
	validated, hasRetrySCID := server.addressValidated, server.local.hasRetrySCID
	server.mu.Unlock()
	if !retried || !validated || !hasRetrySCID {
		t.Fatalf("client retried %v, server validated %v, gave retry_source_connection_id %v", retried, validated, hasRetrySCID)
	}
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("after a retry"), true)
	waitFinished(t, p.clientH, s.ID())
	if got := string(p.clientH.received(s.ID())); got != "after a retry" {
		t.Fatalf("echoed %q", got)
	}
}

// retriedInitial dials a client whose first Initial is answered with a Retry
// from k, as from addr, and returns the client, its handler, the Initial it
// sends again with the token, and the end of the path the server sends on.
func retriedInitial(t *testing.T, k *TokenKey, addr net.Addr) (*Conn, *testHandler, []byte, *pipeEnd) {
	t.Helper()
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{"test"}
	clientEnd, serverEnd := newPipe()
	handler := newTestHandler()
	c, err := Dial(clientEnd, addr, Config{TLSConfig: clientTLS}, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Abort(errTestOver) })
	first := <-serverEnd.queue
	if k.Check(first, addr) != TokenMissing {
		t.Fatal("a first Initial with a token")
	}
	c.HandleDatagram(k.Retry(first, addr))
	// A first flight of more than one datagram has the rest of it queued
	// ahead of the Initial sent again.
	timeout := time.After(5 * time.Second)
	for {
		select {
		case again := <-serverEnd.queue:
			if hdr, err := parseHeader(again, cidLen); err == nil && len(hdr.token) > 0 {
				return c, handler, again, serverEnd
			}
		case <-timeout:
			t.Fatal("no Initial after the Retry")
		}
	}
}

// TestRetryTokenChecks checks what a token is taken for: good from the
// address it was made for, not good from another, and not the key's at all
// once its connection ID or the key is another.
func TestRetryTokenChecks(t *testing.T) {
	k := NewTokenKey()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	_, _, again, _ := retriedInitial(t, k, addr)
	if got := k.Check(again, addr); got != TokenValid {
		t.Fatalf("from its address: %v", got)
	}
	if got := k.Check(again, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 1}); got != TokenInvalid {
		t.Fatalf("from another address: %v", got)
	}
	if got := NewTokenKey().Check(again, addr); got != TokenMissing {
		t.Fatalf("under another key: %v", got)
	}
	h, err := parseHeader(again, cidLen)
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := k.check(h.token, addr, []byte("otherCID")); got != TokenMissing {
		t.Fatalf("to another connection ID: %v", got)
	}
}

// TestRetryInvalidTokenRefused checks that a server given an Initial whose
// Retry token is not good starts no connection, and tells the client so
// with INVALID_TOKEN rather than leaving it to time out.
func TestRetryInvalidTokenRefused(t *testing.T) {
	k := NewTokenKey()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	client, h, again, serverEnd := retriedInitial(t, k, addr)
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.NextProtos = []string{"test"}
	elsewhere := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 1}
	if c := Accept(serverEnd, elsewhere, Config{TLSConfig: serverTLS, TokenKey: k}, newTestHandler(), again); c != nil {
		c.Abort(errTestOver)
		t.Fatal("a connection from an address the token is not for")
	}
	select {
	case d := <-serverEnd.peer.queue:
		client.HandleDatagram(d)
	case <-time.After(5 * time.Second):
		t.Fatal("no answer to the token")
	}
	select {
	case err := <-h.closed:
		var te *TransportError
		if !errors.As(err, &te) || te.Code != errInvalidToken || !te.Remote {
			t.Fatalf("closed with %v, want the server's INVALID_TOKEN", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the client did not close")
	}
}
