package dht

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	mrand "math/rand/v2"
	"net/netip"
	"time"
)

// tokens makes and checks the tokens get_peers and get hand out and
// announce_peer and put must bring back (BEP 5). A token is a MAC of the
// requester's IP address under a secret that rotates every TokenRotate;
// tokens under the current or the previous secret are valid, so a token
// lasts between one and two rotations. It is bound to the IP, not the
// port, as the BEP has it, since a node may announce from another port.
type tokens struct {
	cur, prev [20]byte
	rotated   time.Time
}

// TokenRotate is how often the token secret changes: tokens are valid for
// up to twice as long, 10 minutes, as BEP 5 suggests.
const TokenRotate = 5 * time.Minute

func newTokens(now time.Time) *tokens {
	t := &tokens{rotated: now}
	_, _ = rand.Read(t.cur[:])
	_, _ = rand.Read(t.prev[:])
	return t
}

func (t *tokens) rotate(now time.Time) {
	if now.Sub(t.rotated) < TokenRotate {
		return
	}
	t.prev = t.cur
	_, _ = rand.Read(t.cur[:])
	t.rotated = now
}

func tokenFor(secret *[20]byte, ip netip.Addr) []byte {
	mac := hmac.New(sha1.New, secret[:])
	mac.Write(ip.Unmap().AsSlice())
	return mac.Sum(nil)[:8]
}

func (t *tokens) make(ip netip.Addr) []byte { return tokenFor(&t.cur, ip) }

func (t *tokens) valid(token []byte, ip netip.Addr) bool {
	return hmac.Equal(token, tokenFor(&t.cur, ip)) || hmac.Equal(token, tokenFor(&t.prev, ip))
}

// PeerTTL is how long an announced peer is kept without announcing again.
const PeerTTL = 30 * time.Minute

// peerStore keeps the peers announced to us, bounded in torrents and in
// peers a torrent.
type peerStore struct {
	torrents    map[ID]map[netip.AddrPort]time.Time
	maxTorrents int
	maxPeers    int
}

func newPeerStore(maxTorrents, maxPeers int) *peerStore {
	return &peerStore{torrents: map[ID]map[netip.AddrPort]time.Time{}, maxTorrents: maxTorrents, maxPeers: maxPeers}
}

// add stores a peer of a torrent. At the bound of torrents the one with
// the fewest peers makes way, and at the bound of peers the stalest, so
// that a flood of announces cannot lock everyone else out.
func (s *peerStore) add(ih ID, peer netip.AddrPort, now time.Time) {
	peers := s.torrents[ih]
	if peers == nil {
		if len(s.torrents) >= s.maxTorrents {
			var victim ID
			fewest := -1
			for h, p := range s.torrents {
				if fewest < 0 || len(p) < fewest {
					victim, fewest = h, len(p)
				}
			}
			delete(s.torrents, victim)
		}
		peers = map[netip.AddrPort]time.Time{}
		s.torrents[ih] = peers
	}
	if _, ok := peers[peer]; !ok && len(peers) >= s.maxPeers {
		var oldest netip.AddrPort
		var at time.Time
		for p, t := range peers {
			if at.IsZero() || t.Before(at) {
				oldest, at = p, t
			}
		}
		delete(peers, oldest)
	}
	peers[peer] = now
}

// get returns up to n peers of a torrent of one address family, a random
// choice when there are more.
func (s *peerStore) get(ih ID, n int, v6 bool, now time.Time) []netip.AddrPort {
	peers := s.torrents[ih]
	out := make([]netip.AddrPort, 0, min(n, len(peers)))
	seen := 0
	for p, t := range peers {
		if now.Sub(t) >= PeerTTL || p.Addr().Is4() == v6 {
			continue
		}
		// Reservoir sampling: each peer is equally likely to be sent.
		if seen++; len(out) < n {
			out = append(out, p)
		} else if j := mrand.IntN(seen); j < n {
			out[j] = p
		}
	}
	return out
}

// expire drops peers that have not announced for PeerTTL.
func (s *peerStore) expire(now time.Time) {
	for h, peers := range s.torrents {
		for p, t := range peers {
			if now.Sub(t) >= PeerTTL {
				delete(peers, p)
			}
		}
		if len(peers) == 0 {
			delete(s.torrents, h)
		}
	}
}

// sample returns up to n random info hashes the store has peers for, and
// how many it has (BEP 51).
func (s *peerStore) sample(n int) ([]ID, int) {
	out := make([]ID, 0, min(n, len(s.torrents)))
	seen := 0
	for h := range s.torrents {
		if seen++; len(out) < n {
			out = append(out, h)
		} else if j := mrand.IntN(seen); j < n {
			out[j] = h
		}
	}
	return out, len(s.torrents)
}

// rateLimiter is a token bucket a source IP for the queries it sends us:
// rate a second, up to burst at once.
type rateLimiter struct {
	rate, burst float64
	ips         map[netip.Addr]*ipBucket
}

type ipBucket struct {
	tokens float64
	last   time.Time
}

// maxRateIPs bounds the IPs the limiter tracks. Past it, those whose
// buckets have filled up again are forgotten, which costs them nothing.
const maxRateIPs = 1 << 14

func (r *rateLimiter) allow(ip netip.Addr, now time.Time) bool {
	if r.rate <= 0 {
		return true
	}
	b := r.ips[ip]
	if b == nil {
		if len(r.ips) >= maxRateIPs {
			r.prune(now)
		}
		b = &ipBucket{tokens: r.burst, last: now}
		r.ips[ip] = b
	}
	b.tokens = min(r.burst, b.tokens+now.Sub(b.last).Seconds()*r.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (r *rateLimiter) prune(now time.Time) {
	for ip, b := range r.ips {
		if b.tokens+now.Sub(b.last).Seconds()*r.rate >= r.burst {
			delete(r.ips, ip)
		}
	}
	if len(r.ips) >= maxRateIPs {
		clear(r.ips)
	}
}
