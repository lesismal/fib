package tracker

import (
	"errors"
	"math/rand/v2"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Store keeps the swarms a Server tracks. MemoryStore is the one a Server
// has unless its ServerConfig gives another, one shared by several
// trackers, say. Its methods are called from many goroutines at once.
type Store interface {
	// Announce records a peer's announce and returns peers for it and the
	// swarm's state after it. An announce of EventStopped removes the peer.
	// An error refuses the announce, its message the reason the client is
	// told.
	Announce(a StoreAnnounce) (StoreResult, error)
	// Scrape returns the state of a torrent's swarm, and false for a
	// torrent it does not track.
	Scrape(hash metainfo.InfoHash) (Stats, bool)
}

// StoreAnnounce is a peer's announce, as a Server hands it to its Store.
type StoreAnnounce struct {
	InfoHash metainfo.InfoHash
	PeerID   PeerID
	// V4 and V6 are where the peer accepts connections, of each family; a
	// Server sets at least one unless the peer gave port 0, in which case
	// the peer is not to be recorded, but still wants peers.
	V4, V6 netip.AddrPort
	// Seeder says the peer has the whole torrent.
	Seeder bool
	Event  Event
	// NumWant is how many peer addresses to return at most, of the
	// families WantV4 and WantV6 say.
	NumWant        int
	WantV4, WantV6 bool
}

// StoreResult is a Store's answer to an announce.
type StoreResult struct {
	Peers []Peer
	Stats
}

// ErrStoreFull is the error of a MemoryStore asked to track a torrent past
// MemoryStoreConfig.MaxTorrents.
var ErrStoreFull = errors.New("tracker is full")

// The defaults of MemoryStoreConfig.
const (
	DefaultMaxTorrents        = 1 << 20
	DefaultMaxPeersPerTorrent = 1 << 16
	DefaultMaxPeers           = 1 << 21
	DefaultPeerTTL            = time.Hour
)

// MemoryStoreConfig bounds a MemoryStore.
type MemoryStoreConfig struct {
	// MaxTorrents bounds the torrents tracked; an announce for another is
	// refused with ErrStoreFull. DefaultMaxTorrents when zero.
	MaxTorrents int
	// MaxPeersPerTorrent and MaxPeers bound the peers kept, of a torrent
	// and in all; a peer past either still gets peers, but is not handed
	// out. DefaultMaxPeersPerTorrent and DefaultMaxPeers when zero. A peer
	// takes about 150 bytes.
	MaxPeersPerTorrent int
	MaxPeers           int
	// PeerTTL is how long a peer is kept after its last announce; it
	// should be well past the announce interval a Server gives, so that a
	// peer announcing a little late is not forgotten. DefaultPeerTTL when
	// zero, twice ServerConfig's default interval.
	PeerTTL time.Duration
}

// MemoryStore is a Store in memory. Its torrents are spread over shards,
// each with a lock of its own, so that announces of different torrents
// seldom wait for one another. Peers expire PeerTTL after their last
// announce; a torrent without peers is forgotten.
type MemoryStore struct {
	cfg      MemoryStoreConfig
	shards   [64]memShard
	torrents atomic.Int64
	peers    atomic.Int64
}

type memShard struct {
	mu       sync.Mutex
	torrents map[metainfo.InfoHash]*swarm
	// nextSweep is when the shard next looks for expired peers in swarms
	// nobody has announced to.
	nextSweep time.Time
}

// swarm is a torrent's peers: the seeders and leechers, each in a slice to
// pick random ones from, and all of them by ID.
type swarm struct {
	lists      [2][]*memPeer // leechers, seeders
	byID       map[PeerID]*memPeer
	downloaded int
	// minExpiry is no later than the earliest expiry of a peer: until then,
	// none needs looking at.
	minExpiry time.Time
}

type memPeer struct {
	id      PeerID
	v4, v6  netip.AddrPort
	expires time.Time
	seeder  bool
	// index is the peer's place in its swarm's list.
	index int
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore(cfg MemoryStoreConfig) *MemoryStore {
	if cfg.MaxTorrents <= 0 {
		cfg.MaxTorrents = DefaultMaxTorrents
	}
	if cfg.MaxPeersPerTorrent <= 0 {
		cfg.MaxPeersPerTorrent = DefaultMaxPeersPerTorrent
	}
	if cfg.MaxPeers <= 0 {
		cfg.MaxPeers = DefaultMaxPeers
	}
	if cfg.PeerTTL <= 0 {
		cfg.PeerTTL = DefaultPeerTTL
	}
	m := &MemoryStore{cfg: cfg}
	for i := range m.shards {
		m.shards[i].torrents = map[metainfo.InfoHash]*swarm{}
	}
	return m
}

// Torrents returns how many torrents the store tracks, and Peers how many
// peers it keeps, expired ones it has not looked at yet included.
func (m *MemoryStore) Torrents() int { return int(m.torrents.Load()) }

// Peers returns how many peers the store keeps.
func (m *MemoryStore) Peers() int { return int(m.peers.Load()) }

func (m *MemoryStore) shard(h metainfo.InfoHash) *memShard {
	// Info hashes are SHA-1 or SHA-256 digests: any byte of them is as
	// evenly spread as the shards need.
	return &m.shards[h[0]%byte(len(m.shards))]
}

// Announce is part of Store.
func (m *MemoryStore) Announce(a StoreAnnounce) (StoreResult, error) {
	now := time.Now()
	sh := m.shard(a.InfoHash)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	m.sweepShard(sh, now)
	sw := sh.torrents[a.InfoHash]
	if sw == nil {
		if a.Event == EventStopped {
			return StoreResult{}, nil
		}
		if m.torrents.Load() >= int64(m.cfg.MaxTorrents) {
			return StoreResult{}, ErrStoreFull
		}
		sw = &swarm{byID: map[PeerID]*memPeer{}}
		sh.torrents[a.InfoHash] = sw
		m.torrents.Add(1)
	} else {
		m.expire(sw, now)
	}
	p := sw.byID[a.PeerID]
	if a.Event == EventStopped {
		if p != nil {
			m.remove(sw, p)
		}
		res := StoreResult{Stats: sw.stats()}
		m.forgetIfEmpty(sh, a.InfoHash, sw)
		return res, nil
	}
	wasSeeder := p != nil && p.seeder
	expires := now.Add(m.cfg.PeerTTL)
	switch {
	case p != nil:
		if a.V4.IsValid() {
			p.v4 = a.V4
		}
		if a.V6.IsValid() {
			p.v6 = a.V6
		}
		p.expires = expires
		if p.seeder != a.Seeder {
			m.move(sw, p, a.Seeder)
		}
	case (a.V4.IsValid() || a.V6.IsValid()) && len(sw.byID) < m.cfg.MaxPeersPerTorrent &&
		m.peers.Load() < int64(m.cfg.MaxPeers):
		p = &memPeer{id: a.PeerID, v4: a.V4, v6: a.V6, expires: expires, seeder: a.Seeder}
		sw.byID[a.PeerID] = p
		m.insert(sw, p)
		m.peers.Add(1)
		if sw.minExpiry.IsZero() || expires.Before(sw.minExpiry) {
			sw.minExpiry = expires
		}
	}
	if a.Event == EventCompleted && a.Seeder && !wasSeeder {
		sw.downloaded++
	}
	res := StoreResult{Stats: sw.stats()}
	res.Peers = sw.pick(a, p)
	m.forgetIfEmpty(sh, a.InfoHash, sw)
	return res, nil
}

// Scrape is part of Store.
func (m *MemoryStore) Scrape(h metainfo.InfoHash) (Stats, bool) {
	now := time.Now()
	sh := m.shard(h)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sw := sh.torrents[h]
	if sw == nil {
		return Stats{}, false
	}
	m.expire(sw, now)
	if len(sw.byID) == 0 {
		m.forgetIfEmpty(sh, h, sw)
		return Stats{}, false
	}
	return sw.stats(), true
}

func (sw *swarm) stats() Stats {
	return Stats{Complete: len(sw.lists[1]), Incomplete: len(sw.lists[0]), Downloaded: sw.downloaded}
}

func listOf(seeder bool) int {
	if seeder {
		return 1
	}
	return 0
}

func (m *MemoryStore) insert(sw *swarm, p *memPeer) {
	l := &sw.lists[listOf(p.seeder)]
	p.index = len(*l)
	*l = append(*l, p)
}

// unlink takes p out of its list, moving the list's last peer to its place.
func (m *MemoryStore) unlink(sw *swarm, p *memPeer) {
	l := &sw.lists[listOf(p.seeder)]
	last := (*l)[len(*l)-1]
	(*l)[p.index] = last
	last.index = p.index
	(*l)[len(*l)-1] = nil
	*l = (*l)[:len(*l)-1]
}

func (m *MemoryStore) remove(sw *swarm, p *memPeer) {
	m.unlink(sw, p)
	delete(sw.byID, p.id)
	m.peers.Add(-1)
}

func (m *MemoryStore) move(sw *swarm, p *memPeer, seeder bool) {
	m.unlink(sw, p)
	p.seeder = seeder
	m.insert(sw, p)
}

// expire removes the peers of sw that have expired, when any may have.
func (m *MemoryStore) expire(sw *swarm, now time.Time) {
	if sw.minExpiry.IsZero() || now.Before(sw.minExpiry) {
		return
	}
	var next time.Time
	for i := range sw.lists {
		for j := 0; j < len(sw.lists[i]); {
			p := sw.lists[i][j]
			if !now.Before(p.expires) {
				m.remove(sw, p) // moves the last peer to j
				continue
			}
			if next.IsZero() || p.expires.Before(next) {
				next = p.expires
			}
			j++
		}
	}
	sw.minExpiry = next
}

func (m *MemoryStore) forgetIfEmpty(sh *memShard, h metainfo.InfoHash, sw *swarm) {
	if len(sw.byID) == 0 {
		delete(sh.torrents, h)
		m.torrents.Add(-1)
	}
}

// sweepShard expires the peers of every swarm of sh, a quarter of PeerTTL
// after it last did, so that torrents nobody announces to any more are
// forgotten in time. Its cost, a look at each torrent of the shard, is
// spread over the announces of that time.
func (m *MemoryStore) sweepShard(sh *memShard, now time.Time) {
	if now.Before(sh.nextSweep) {
		return
	}
	sh.nextSweep = now.Add(m.cfg.PeerTTL / 4)
	for h, sw := range sh.torrents {
		m.expire(sw, now)
		m.forgetIfEmpty(sh, h, sw)
	}
}

// pick returns up to a.NumWant addresses of random peers of the swarm,
// other than self, of the families a wants: leechers for a seeder, which
// has nothing to get from seeders, and either for a leecher.
//
// It walks the peers in a random order without building one: from a
// random start, by a random stride that has no factor in common with their
// number, which visits each once.
func (sw *swarm) pick(a StoreAnnounce, self *memPeer) []Peer {
	leechers, seeders := sw.lists[0], sw.lists[1]
	n := len(leechers)
	if !a.Seeder {
		n += len(seeders)
	}
	if n == 0 || a.NumWant <= 0 || !a.WantV4 && !a.WantV6 {
		return nil
	}
	start, stride := rand.IntN(n), 1
	if n > 2 {
		for range 8 {
			if s := 1 + rand.IntN(n-1); gcd(s, n) == 1 {
				stride = s
				break
			}
		}
	}
	peers := make([]Peer, 0, min(a.NumWant, n))
	for i, k := 0, start; i < n && len(peers) < a.NumWant; i, k = i+1, (k+stride)%n {
		var p *memPeer
		if k < len(leechers) {
			p = leechers[k]
		} else {
			p = seeders[k-len(leechers)]
		}
		if p == self || p.id == a.PeerID {
			continue
		}
		if a.WantV4 && p.v4.IsValid() {
			peers = append(peers, Peer{Addr: p.v4, ID: p.id})
		}
		if a.WantV6 && p.v6.IsValid() && len(peers) < a.NumWant {
			peers = append(peers, Peer{Addr: p.v6, ID: p.id})
		}
	}
	return peers
}

func gcd(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
