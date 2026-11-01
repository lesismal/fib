package quic

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// What a TokenCache does with what it cannot key, a cache that is nil, an
// address it forgets while it still holds its token, and a full cache whose
// entries expire out of the order they were kept in.
func TestCqTokenCacheEdges(t *testing.T) {
	var none *TokenCache
	none.remember(&net.UDPAddr{Port: 1}, []byte("t"), time.Now())
	none.forget(&net.UDPAddr{Port: 1})
	if none.token(&net.UDPAddr{Port: 1}, time.Now()) != nil {
		t.Fatal("a nil cache has a token")
	}

	cache := NewTokenCache()
	now := time.Now()
	cache.remember(nil, []byte("t"), now)
	if len(cache.entries) != 0 || cache.token(nil, now) != nil {
		t.Fatal("a token kept for no address")
	}
	cache.forget(nil)

	addr := func(i int) *net.UDPAddr { return &net.UDPAddr{IP: net.IPv4(127, 0, 0, byte(i)), Port: 1} }
	// The last address kept expires first, so it is the one a new address
	// takes the room of.
	for i := 1; i <= tokenCacheSize; i++ {
		cache.remember(addr(i), []byte{byte(i)}, now.Add(time.Duration(tokenCacheSize-i)*time.Second))
	}
	cache.remember(addr(tokenCacheSize+1), []byte("new"), now)
	if cache.token(addr(tokenCacheSize), now) != nil {
		t.Fatal("the address that expires first was kept")
	}
	if !bytes.Equal(cache.token(addr(1), now), []byte{1}) {
		t.Fatal("an address that expires later was dropped")
	}
	cache.forget(addr(1))
	if cache.token(addr(1), now) != nil {
		t.Fatal("a forgotten token was kept")
	}
}

// What a TokenKey makes of what is not a client Initial, of an address that
// is not UDP, or none, of a token whose kind is not one it makes, and of a
// Retry token that names a connection ID too short to be a client's first.
func TestCqTokenKeyEdges(t *testing.T) {
	k := NewTokenKey()
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	short := append([]byte{0x40}, make([]byte, 40)...)
	if k.Retry(short, addr) != nil {
		t.Fatal("a Retry for a short header packet")
	}
	if status, kind := k.CheckToken(short, addr); status != TokenMissing || kind != TokenKindNone {
		t.Fatalf("a short header packet's token: %v, %v", status, kind)
	}

	// Tokens bound to addresses that are not UDP, and to none.
	for _, remote := range []net.Addr{&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}, nil} {
		token := k.NewToken(remote)
		if status, kind, _ := k.check(token, remote, nil); status != TokenValid || kind != TokenKindNew {
			t.Fatalf("a token for %v from it: %v, %v", remote, status, kind)
		}
	}
	if addrBytes(nil) != nil {
		t.Fatal("no address has bytes")
	}

	token := k.NewToken(addr)
	token[0] = 9
	if status, kind, _ := k.check(token, addr, nil); status != TokenMissing || kind != TokenKindNone {
		t.Fatalf("a token of an unknown kind: %v, %v", status, kind)
	}

	dcid := []byte("retrycid")
	token = k.appendToken(nil, addr, TokenKindRetry, []byte{1, 2, 3}, dcid, time.Now())
	if status, kind, odcid := k.check(token, addr, dcid); status != TokenInvalid || kind != TokenKindRetry || odcid != nil {
		t.Fatalf("a Retry token naming a short connection ID: %v, %v, %x", status, kind, odcid)
	}
}
