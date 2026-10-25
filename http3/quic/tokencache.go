package quic

import (
	"bytes"
	"net"
	"sync"
	"time"
)

// A client's memory of the tokens servers sent, as RFC 9000 section 8.1.3
// has it: the token of a NEW_TOKEN frame is bound to the address the server
// saw, and the client sends it back with the first Initial of a later
// connection from that address, which the server may then take as the
// client's own without a Retry, saving the round trip a Retry costs.
const (
	// tokenCacheSize bounds the addresses a TokenCache keeps a token for:
	// a client that reaches many servers does not grow with them, and the
	// one that expires first goes when another address needs its room.
	tokenCacheSize = 4
	// tokenCacheLifetime is how long a client keeps a token. It is shorter
	// than the newTokenLifetime a fib server gives its tokens, so that one
	// is not sent back just as it expires, and short enough that a stale
	// token, from a server that restarted or from another server behind the
	// address, does not hold its room for long.
	tokenCacheLifetime = 5 * time.Minute
)

// TokenCache keeps, for a client, the tokens that servers sent in NEW_TOKEN
// frames: the first Initial of a later connection to a server from the same
// address carries its token (see Config.TokenCache). It holds a few
// addresses at most, for a while each, so a client that reaches many servers
// does not grow with them. It is safe for concurrent use, and its zero value
// is ready to use; NewTokenCache returns one.
type TokenCache struct {
	mu      sync.Mutex
	entries []tokenCacheEntry
}

type tokenCacheEntry struct {
	addr    string
	token   []byte
	expires time.Time
}

// NewTokenCache returns an empty cache.
func NewTokenCache() *TokenCache { return &TokenCache{} }

// remember keeps token for remote until tokenCacheLifetime is up, replacing
// what the cache had for the address.
func (t *TokenCache) remember(remote net.Addr, token []byte, now time.Time) {
	if t == nil || len(token) == 0 {
		return
	}
	key := tokenCacheKey(remote)
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(now)
	for i := range t.entries {
		if t.entries[i].addr == key {
			t.entries[i].token, t.entries[i].expires = bytes.Clone(token), now.Add(tokenCacheLifetime)
			return
		}
	}
	if len(t.entries) >= tokenCacheSize {
		// The one that expires first, which is the least recently kept.
		first := 0
		for i := 1; i < len(t.entries); i++ {
			if t.entries[i].expires.Before(t.entries[first].expires) {
				first = i
			}
		}
		t.entries = append(t.entries[:first], t.entries[first+1:]...)
	}
	t.entries = append(t.entries, tokenCacheEntry{addr: key, token: bytes.Clone(token), expires: now.Add(tokenCacheLifetime)})
}

// token returns the token kept for remote, or nil if there is none, it has
// expired, or the client is done with it.
func (t *TokenCache) token(remote net.Addr, now time.Time) []byte {
	if t == nil {
		return nil
	}
	key := tokenCacheKey(remote)
	if key == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pruneLocked(now)
	for i := range t.entries {
		if t.entries[i].addr == key {
			return t.entries[i].token
		}
	}
	return nil
}

// forget drops what the cache has for remote, as a server that answered a
// Retry, or closed with INVALID_TOKEN, has said the token is not good.
func (t *TokenCache) forget(remote net.Addr) {
	if t == nil {
		return
	}
	key := tokenCacheKey(remote)
	if key == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.entries {
		if t.entries[i].addr == key {
			t.entries = append(t.entries[:i], t.entries[i+1:]...)
			return
		}
	}
}

// pruneLocked drops the tokens that have expired. Callers hold t.mu.
func (t *TokenCache) pruneLocked(now time.Time) {
	kept := t.entries[:0]
	for _, e := range t.entries {
		if now.Before(e.expires) {
			kept = append(kept, e)
		}
	}
	clear(t.entries[len(kept):])
	t.entries = kept
}

// tokenCacheKey is the address a token is kept for: the one the server saw,
// which a later connection has to come from for the token to be good.
func tokenCacheKey(remote net.Addr) string {
	if remote == nil {
		return ""
	}
	return remote.String()
}
