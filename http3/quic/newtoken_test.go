package quic

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// TestNewTokenChecks checks what a token of a NEW_TOKEN frame is taken for:
// good from the address it was made for, until its own lifetime is up, not
// good from another address or once it has expired, and not the key's at
// all under another key.
func TestNewTokenChecks(t *testing.T) {
	k := NewTokenKey()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	token := k.NewToken(addr)
	if len(token) == 0 {
		t.Fatal("no token")
	}
	status, kind, odcid := k.check(token, addr, randomCID())
	if status != TokenValid || kind != TokenKindNew || odcid != nil {
		t.Fatalf("from its address: %v, kind %v, odcid %x", status, kind, odcid)
	}
	if got, gotKind, _ := k.check(token, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 1}, randomCID()); got != TokenInvalid || gotKind != TokenKindNew {
		t.Fatalf("from another address: %v, kind %v", got, gotKind)
	}
	if got, _, _ := NewTokenKey().check(token, addr, randomCID()); got != TokenMissing {
		t.Fatalf("under another key: %v", got)
	}
	// A NEW_TOKEN token is good for newTokenLifetime, and no longer.
	old := k.appendToken(nil, addr, TokenKindNew, nil, nil, time.Now().Add(-newTokenLifetime-time.Second))
	if got, kind, _ := k.check(old, addr, randomCID()); got != TokenInvalid || kind != TokenKindNew {
		t.Fatalf("expired: %v, kind %v", got, kind)
	}
	recent := k.appendToken(nil, addr, TokenKindNew, nil, nil, time.Now().Add(-newTokenLifetime/2))
	if got, _, _ := k.check(recent, addr, randomCID()); got != TokenValid {
		t.Fatalf("half its lifetime old: %v", got)
	}
	// A Retry token opens under the connection ID it was made for, and not
	// under another.
	scid := randomCID()
	retry := k.appendToken(nil, addr, TokenKindRetry, randomCID(), scid, time.Now())
	if status, kind, _ := k.check(retry, addr, scid); status != TokenValid || kind != TokenKindRetry {
		t.Fatalf("a Retry token: %v, kind %v", status, kind)
	}
	if got, kind, _ := k.check(retry, addr, randomCID()); got != TokenMissing || kind != TokenKindNone {
		t.Fatalf("a Retry token under another connection ID: %v, kind %v", got, kind)
	}
}

// TestTokenCache checks what a client keeps: a few addresses at most, each
// for a while, with the one that expires first going for a new address.
func TestTokenCache(t *testing.T) {
	k := NewTokenKey()
	cache := NewTokenCache()
	now := time.Now()
	var addrs []*net.UDPAddr
	for i := 0; i < tokenCacheSize+1; i++ {
		addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, byte(i+1)), Port: 1}
		addrs = append(addrs, addr)
		cache.remember(addr, k.appendToken(nil, addr, TokenKindNew, nil, nil, now), now)
	}
	if got := len(cache.entries); got != tokenCacheSize {
		t.Fatalf("%d addresses kept, want %d", got, tokenCacheSize)
	}
	if tok := cache.token(addrs[0], now); tok != nil {
		t.Fatal("the oldest address was kept")
	}
	for _, addr := range addrs[1:] {
		if tok := cache.token(addr, now); tok == nil {
			t.Fatalf("token for %v is gone", addr)
		}
	}
	// Keeping another token for an address replaces the one it had, and
	// does not grow the cache.
	before := len(cache.entries)
	cache.remember(addrs[1], []byte("another"), now)
	if got := len(cache.entries); got != before {
		t.Fatalf("%d addresses after replacing one, want %d", got, before)
	}
	if got := string(cache.token(addrs[1], now)); got != "another" {
		t.Fatalf("kept %q", got)
	}
	// A token is good for tokenCacheLifetime.
	later := now.Add(tokenCacheLifetime + time.Second)
	if tok := cache.token(addrs[2], later); tok != nil {
		t.Fatal("an expired token was kept")
	}
	// As a server that does not take it says, it is dropped.
	cache.forget(addrs[3])
	if tok := cache.token(addrs[3], now); tok != nil {
		t.Fatal("a forgotten token was kept")
	}
}

// testInitialWithToken returns the first datagram of a client whose first
// Initial carries token, or none when token is nil.
func testInitialWithToken(t *testing.T, token []byte) []byte {
	t.Helper()
	_, sink, _ := dialSink(t, func(c *Config) { c.Token = token })
	if len(sink.sent) == 0 {
		t.Fatal("the client sent nothing")
	}
	return sink.sent[0]
}

// TestNewTokenAccept checks that a server takes an Initial whose NEW_TOKEN
// token is good as from a proven address, and one whose token is expired or
// from elsewhere as from no token at all: a connection starts either way,
// without a Retry, and its address is validated as any new client's.
func TestNewTokenAccept(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	k := NewTokenKey()
	addr := testAddr
	elsewhere := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: 1}
	valid := testInitialWithToken(t, k.NewToken(addr))
	expired := testInitialWithToken(t, k.appendToken(nil, addr, TokenKindNew, nil, nil, time.Now().Add(-newTokenLifetime-time.Second)))
	other := testInitialWithToken(t, k.NewToken(elsewhere))

	for _, want := range []struct {
		name      string
		initial   []byte
		validated bool
	}{
		{"a good token", valid, true},
		{"an expired token", expired, false},
		{"a token of another address", other, false},
	} {
		t.Run(want.name, func(t *testing.T) {
			c := Accept(&sinkConn{}, addr, Config{TLSConfig: serverTLS, TokenKey: k}, newTestHandler(), want.initial)
			if c == nil {
				t.Fatal("no connection: an Initial whose NEW_TOKEN token is not good is as if it had none")
			}
			t.Cleanup(func() { c.Abort(errTestOver) })
			c.mu.Lock()
			validated, retrySCID, originalDCID := c.addressValidated, c.local.hasRetrySCID, bytes.Clone(c.local.originalDCID)
			c.mu.Unlock()
			if validated != want.validated {
				t.Fatalf("validated %v, want %v", validated, want.validated)
			}
			if retrySCID {
				t.Fatal("a Retry the client never had")
			}
			h, err := parseHeader(want.initial, cidLen)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(originalDCID, h.dcid) {
				t.Fatalf("original_destination_connection_id %x, want %x", originalDCID, h.dcid)
			}
			if c.closed {
				t.Fatal("the connection was closed")
			}
		})
	}
}

// TestNewTokenClientInitial checks that a client puts a token of a NEW_TOKEN
// frame in the first Initial of its next connection: the one Config.Token
// names, one the cache kept, and none when the cache has expired it or
// DisableNewTokens is set.
func TestNewTokenClientInitial(t *testing.T) {
	k := NewTokenKey()
	addr := testAddr
	cached := k.NewToken(addr)
	cache := NewTokenCache()
	cache.remember(addr, cached, time.Now())
	explicit := k.NewToken(addr)

	for _, want := range []struct {
		name   string
		config Config
		token  []byte
	}{
		{"an explicit token", Config{Token: explicit}, explicit},
		{"a token the cache kept", Config{TokenCache: cache}, cached},
		{"an expired token", Config{TokenCache: func() *TokenCache {
			c := NewTokenCache()
			c.remember(addr, cached, time.Now().Add(-tokenCacheLifetime))
			return c
		}()}, nil},
		{"tokens turned off", Config{TokenCache: cache, Token: explicit, DisableNewTokens: true}, nil},
	} {
		t.Run(want.name, func(t *testing.T) {
			_, sink, _ := dialSink(t, func(c *Config) {
				c.Token, c.TokenCache, c.DisableNewTokens = want.config.Token, want.config.TokenCache, want.config.DisableNewTokens
			})
			if len(sink.sent) == 0 {
				t.Fatal("the client sent nothing")
			}
			h, err := parseHeader(sink.sent[0], cidLen)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(h.token, want.token) {
				t.Fatalf("first Initial carried %x, want %x", h.token, want.token)
			}
		})
	}
}

// TestNewTokenTiming checks when a server arms NEW_TOKEN: only after the
// handshake, and only for a client whose address is validated, with a key to
// make it and tokens not turned off.
func TestNewTokenTiming(t *testing.T) {
	withKey := func(config Config) *Conn {
		c := newConn(&sinkConn{}, testAddr, config, nil, false)
		return c
	}
	c := withKey(Config{TokenKey: NewTokenKey()})
	c.queueNewTokenLocked()
	if c.newTokenPending {
		t.Fatal("NEW_TOKEN armed for an address that is not validated")
	}
	c.addressValidated = true
	c.queueNewTokenLocked()
	if !c.newTokenPending {
		t.Fatal("NEW_TOKEN not armed after the handshake")
	}
	c = withKey(Config{TokenKey: NewTokenKey(), DisableNewTokens: true})
	c.addressValidated = true
	c.queueNewTokenLocked()
	if c.newTokenPending {
		t.Fatal("NEW_TOKEN armed with tokens turned off")
	}
	c = withKey(Config{})
	c.addressValidated = true
	c.queueNewTokenLocked()
	if c.newTokenPending {
		t.Fatal("NEW_TOKEN armed without a key to make it")
	}
}

// recordPath is a server's path that keeps what the connection sends through
// it.
type recordPath struct {
	*pipeEnd
	mu   sync.Mutex
	sent [][]byte
}

func (p *recordPath) Send(d []byte) error {
	p.mu.Lock()
	p.sent = append(p.sent, bytes.Clone(d))
	p.mu.Unlock()
	return p.pipeEnd.Send(d)
}

func (p *recordPath) datagrams() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.sent...)
}

// frameTypes reads the frames of a 1-RTT payload, and returns how many of
// each, with the tokens a NEW_TOKEN frame carried.
func frameTypes(t *testing.T, payload []byte) (map[uint64]int, [][]byte) {
	t.Helper()
	counts := make(map[uint64]int)
	var tokens [][]byte
	r := reader{b: payload}
	for len(r.b) > 0 {
		typ := r.varint()
		if r.bad {
			t.Fatal("truncated frame type")
		}
		counts[typ]++
		switch typ {
		case framePadding:
			for len(r.b) > 0 && r.b[0] == 0 {
				r.b = r.b[1:]
			}
		case framePing, frameHandshakeDone:
		case frameAck, frameAckECN:
			r.varint()
			r.varint()
			count, _ := r.varint(), r.varint()
			for i := uint64(0); i < count; i++ {
				r.varint()
				r.varint()
			}
			if typ == frameAckECN {
				r.varint()
				r.varint()
				r.varint()
			}
		case frameCrypto:
			r.varint()
			r.bytes(r.varint())
		case frameNewToken:
			tokens = append(tokens, bytes.Clone(r.bytes(r.varint())))
		case frameMaxData, frameMaxStreamsBidi, frameMaxStreamsUni, frameRetireConnectionID,
			frameDataBlocked, frameStreamsBlockedBidi, frameStreamsBlockedUni:
			r.varint()
		case frameMaxStreamData, frameStreamDataBlocked:
			r.varint()
			r.varint()
		case frameNewConnectionID:
			r.varint()
			r.varint()
			r.bytes(uint64(r.byte()))
			r.bytes(statelessResetTokenLen)
		case framePathChallenge, framePathResponse:
			r.bytes(8)
		case frameAckFrequency:
			for i := 0; i < 4; i++ {
				r.varint()
			}
		default:
			t.Fatalf("unexpected frame 0x%x in the first packet after a handshake", typ)
		}
		if r.bad {
			t.Fatal("malformed frame")
		}
	}
	return counts, tokens
}

// appPayloads decrypts the 1-RTT packets in datagrams, which a server sent,
// with the client's keys rx.
func appPayloads(t *testing.T, datagrams [][]byte, rx *keys) [][]byte {
	t.Helper()
	var payloads [][]byte
	for _, d := range datagrams {
		for len(d) > 0 {
			h, err := parseHeader(d, cidLen)
			if err != nil {
				break
			}
			pkt := bytes.Clone(d[:h.end])
			d = d[h.end:]
			if h.typ != packet1RTT {
				continue
			}
			pn, n, ok := unprotect(pkt, h.pnOffset, rx.hp, -1)
			if !ok {
				continue
			}
			payload, err := rx.open(pkt, h.pnOffset+n, pn)
			if err != nil {
				continue
			}
			payloads = append(payloads, bytes.Clone(payload))
		}
	}
	return payloads
}

// TestServerSendsNewToken checks that a server sends exactly one NEW_TOKEN,
// once its handshake is complete: in a 1-RTT packet, with HANDSHAKE_DONE,
// never before, and the client keeps what it carries.
func TestServerSendsNewToken(t *testing.T) {
	k := NewTokenKey()
	cache := NewTokenCache()
	serverTLS, clientTLS := testTLSConfigs(t)
	clientEnd, serverEnd := newPipe()
	rec := &recordPath{pipeEnd: serverEnd}
	clientH, serverH := newTestHandler(), newTestHandler()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	client, err := Dial(clientEnd, addr, Config{TLSConfig: clientTLS, TokenCache: cache}, clientH)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Abort(errTestOver) })
	serverConfig := Config{TLSConfig: serverTLS, TokenKey: k}
	done := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	stop := func() {
		stopOnce.Do(func() { close(done) })
		wg.Wait()
	}
	t.Cleanup(stop)
	var server *Conn
	var serverMu sync.Mutex
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case d := <-serverEnd.queue:
				serverMu.Lock()
				if server == nil {
					server = Accept(rec, addr, serverConfig, serverH, d)
				} else {
					server.HandleDatagram(d)
				}
				serverMu.Unlock()
			case <-done:
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case d := <-clientEnd.queue:
				client.HandleDatagram(d)
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		serverMu.Lock()
		s := server
		serverMu.Unlock()
		if s != nil {
			s.Abort(errTestOver)
		}
	})
	select {
	case <-clientH.handshake:
	case err := <-clientH.closed:
		t.Fatalf("client closed during the handshake: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake timed out")
	}
	// HANDSHAKE_DONE and NEW_TOKEN are in the same flight; take the token
	// from the cache once it has arrived.
	var token []byte
	waitFor(t, "the token from NEW_TOKEN", func() bool {
		token = cache.token(addr, time.Now())
		return token != nil
	})
	// Nothing reads the client's keys once its goroutine is done.
	stop()
	client.mu.Lock()
	rx := client.spaces[spaceApp].rx
	client.mu.Unlock()
	if rx == nil {
		t.Fatal("no 1-RTT keys")
	}
	payloads := appPayloads(t, rec.datagrams(), rx)
	seen, withDone := 0, 0
	for _, payload := range payloads {
		counts, tokens := frameTypes(t, payload)
		if len(tokens) == 0 {
			continue
		}
		seen += len(tokens)
		if counts[frameHandshakeDone] > 0 {
			withDone++
		}
		for _, got := range tokens {
			if !bytes.Equal(got, token) {
				t.Fatalf("the frame carried %x, the cache kept %x", got, token)
			}
		}
	}
	if seen != 1 || withDone != 1 {
		t.Fatalf("%d NEW_TOKEN frames, %d with HANDSHAKE_DONE, want one of each", seen, withDone)
	}
	// One token for the connection, never more, however long it goes on.
	time.Sleep(50 * time.Millisecond)
	if got := len(cache.entries); got != 1 {
		t.Fatalf("%d tokens kept, want one", got)
	}
	client.mu.Lock()
	pending := client.newTokenPending
	client.mu.Unlock()
	if pending {
		t.Fatal("a client armed a NEW_TOKEN frame")
	}
}

// TestNewTokenSkipsRetry checks that the token a server gives after a
// handshake saves the Retry of the next connection from the same address:
// the second client has no Retry, and its server takes the token as proof of
// the address.
func TestNewTokenSkipsRetry(t *testing.T) {
	k := NewTokenKey()
	cache := NewTokenCache()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	first := newTestPairOpts(t, 0, func(client, server *Config) {
		client.TokenCache = cache
	}, pairOpts{retry: k})
	first.client.mu.Lock()
	retried := first.client.retrySCID != nil
	first.client.mu.Unlock()
	if !retried {
		t.Fatal("the first connection had no Retry")
	}
	waitFor(t, "the token from NEW_TOKEN", func() bool {
		return cache.token(addr, time.Now()) != nil
	})
	second := newTestPairOpts(t, 0, func(client, server *Config) {
		client.TokenCache = cache
	}, pairOpts{retry: k})
	second.client.mu.Lock()
	retried = second.client.retrySCID != nil
	second.client.mu.Unlock()
	if retried {
		t.Fatal("the second connection from the same address had a Retry")
	}
	server := second.serverConn(t)
	server.mu.Lock()
	validated, retrySCID := server.addressValidated, server.local.hasRetrySCID
	server.mu.Unlock()
	if !validated || retrySCID {
		t.Fatalf("server validated %v, retry_source_connection_id %v", validated, retrySCID)
	}
	// The second connection had a token from the cache: a client that did
	// not would have been answered with a Retry by this server.
	if tok := cache.token(addr, time.Now()); tok == nil {
		t.Fatal("the token was not kept")
	}
}
