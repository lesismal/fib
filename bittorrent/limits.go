package bittorrent

import (
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bittorrent/peer"
)

// Limits are the client's limits that may change while it runs: NewClient
// takes them from Config, SetLimits changes them and Limits returns them.
//
// Zero keeps the meaning it has in Config: a rate, SeedRatio or SeedTime
// of zero is unlimited; a count (MaxConns, MaxConnsPerTorrent,
// MaxHalfOpen, DialsPerTick, UploadSlots) of zero is its Default. A
// negative value is the same as zero.
type Limits struct {
	// UploadRate and DownloadRate bound the payload bytes per second of
	// all torrents together; zero is unlimited.
	UploadRate, DownloadRate int
	// MaxConns bounds the client's peer connections, MaxConnsPerTorrent
	// a torrent's, MaxHalfOpen the dials in flight, and DialsPerTick the
	// new dials a torrent starts each second; their Defaults when zero.
	MaxConns, MaxConnsPerTorrent, MaxHalfOpen, DialsPerTick int
	// UploadSlots is how many peers a torrent uploads to at once, beside
	// the optimistic unchoke; DefaultUploadSlots when zero.
	UploadSlots int
	// SeedRatio pauses a torrent once it has uploaded this many times its
	// size; SeedTime once it has seeded this long. Zero is unlimited.
	SeedRatio float64
	SeedTime  time.Duration
}

// normalized is l with the Defaults in place of zero counts, and zero in
// place of negative rates and seed limits.
func (l Limits) normalized() Limits {
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&l.MaxConns, DefaultMaxConns)
	def(&l.MaxConnsPerTorrent, DefaultMaxConnsPerTorrent)
	def(&l.MaxHalfOpen, DefaultMaxHalfOpen)
	def(&l.DialsPerTick, DefaultDialsPerTick)
	def(&l.UploadSlots, DefaultUploadSlots)
	l.UploadRate = max(l.UploadRate, 0)
	l.DownloadRate = max(l.DownloadRate, 0)
	l.SeedRatio = max(l.SeedRatio, 0)
	l.SeedTime = max(l.SeedTime, 0)
	return l
}

func (cfg *Config) limits() Limits {
	return Limits{
		UploadRate: cfg.UploadRate, DownloadRate: cfg.DownloadRate,
		MaxConns: cfg.MaxConns, MaxConnsPerTorrent: cfg.MaxConnsPerTorrent,
		MaxHalfOpen: cfg.MaxHalfOpen, DialsPerTick: cfg.DialsPerTick,
		UploadSlots: cfg.UploadSlots, SeedRatio: cfg.SeedRatio, SeedTime: cfg.SeedTime,
	}.normalized()
}

// Limits returns the client's limits, with the Defaults in place of zero
// counts.
func (c *Client) Limits() Limits { return *c.lim() }

func (c *Client) lim() *Limits { return c.limits.Load() }

// SetLimits changes the client's limits while it runs, keeping its
// peers: new rates apply from the next quarter second; lowered MaxConns
// or MaxConnsPerTorrent close the peers over them at once, the least
// useful first (see TorrentLimits); a changed UploadSlots rechokes every
// torrent that takes it from the client at once; seed limits are looked
// at in the next second, and a torrent over a lowered one pauses with
// EventSeedLimit.
func (c *Client) SetLimits(l Limits) {
	l = l.normalized()
	c.limMu.Lock()
	defer c.limMu.Unlock()
	old := c.limits.Swap(&l)
	c.up.setRate(int64(l.UploadRate))
	c.down.setRate(int64(l.DownloadRate))
	if l.UploadSlots != old.UploadSlots {
		now := time.Now()
		for _, t := range c.Torrents() {
			t.mu.Lock()
			if t.lim.UploadSlots == 0 && t.running() {
				t.rechokeLocked(now)
			}
			t.mu.Unlock()
		}
	}
	if l.MaxConns < old.MaxConns || l.MaxConnsPerTorrent < old.MaxConnsPerTorrent {
		c.trimConns()
	}
}

// TorrentLimits are a torrent's own limits, beside the client's: set when
// it is added (AddOptions.Limits) or later (Torrent.SetLimits). Zero
// takes the client's limit; a negative MaxConns, UploadSlots, SeedRatio
// or SeedTime is no limit for this torrent, whatever the client's.
type TorrentLimits struct {
	// UploadRate and DownloadRate bound the torrent's payload bytes per
	// second, under the client's rates, which every byte passes too; zero
	// (or negative) is the client's rates alone.
	UploadRate, DownloadRate int
	// MaxConns bounds the torrent's peer connections in place of the
	// client's MaxConnsPerTorrent, above or below it; negative is the
	// client's MaxConns alone.
	MaxConns int
	// UploadSlots is how many peers the torrent uploads to at once, beside
	// the optimistic unchoke, in place of the client's; negative unchokes
	// every interested peer.
	UploadSlots int
	// SeedRatio and SeedTime pause the torrent as the client's do, in
	// place of them; negative seeds for ever.
	SeedRatio float64
	SeedTime  time.Duration
}

func (l TorrentLimits) normalized() TorrentLimits {
	l.UploadRate = max(l.UploadRate, 0)
	l.DownloadRate = max(l.DownloadRate, 0)
	return l
}

// Limits returns the torrent's own limits, zero where it takes the
// client's.
func (t *Torrent) Limits() TorrentLimits {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lim
}

// SetLimits changes the torrent's own limits while it runs, as
// Client.SetLimits does the client's.
func (t *Torrent) SetLimits(l TorrentLimits) {
	l = l.normalized()
	t.mu.Lock()
	oldSlots, oldConns := t.uploadSlotsLocked(), t.maxConnsLocked()
	t.setLimitsLocked(l)
	if t.uploadSlotsLocked() != oldSlots && t.running() {
		t.rechokeLocked(time.Now())
	}
	trim := t.maxConnsLocked() < oldConns && len(t.peers) > t.maxConnsLocked()
	t.mu.Unlock()
	if trim {
		t.c.trimConns()
	}
}

func (t *Torrent) setLimitsLocked(l TorrentLimits) {
	t.lim = l
	t.upLim.setRate(int64(l.UploadRate))
	t.downLim.setRate(int64(l.DownloadRate))
}

// maxConnsLocked is the bound on the torrent's connections.
func (t *Torrent) maxConnsLocked() int {
	switch n := t.lim.MaxConns; {
	case n > 0:
		return n
	case n < 0:
		return math.MaxInt
	}
	return t.c.lim().MaxConnsPerTorrent
}

// uploadSlotsLocked is how many peers the torrent unchokes, beside the
// optimistic one.
func (t *Torrent) uploadSlotsLocked() int {
	switch n := t.lim.UploadSlots; {
	case n > 0:
		return n
	case n < 0:
		return math.MaxInt
	}
	return t.c.lim().UploadSlots
}

// seedLimitsLocked are the torrent's seed ratio and time, zero for none.
func (t *Torrent) seedLimitsLocked() (float64, time.Duration) {
	cl := t.c.lim()
	ratio, d := cl.SeedRatio, cl.SeedTime
	if t.lim.SeedRatio != 0 {
		ratio = max(t.lim.SeedRatio, 0)
	}
	if t.lim.SeedTime != 0 {
		d = max(t.lim.SeedTime, 0)
	}
	return ratio, d
}

// downAllow reports that both the client's and the torrent's download
// rates allow another request; downTake spends what one asked for.
func (t *Torrent) downAllow() bool { return t.c.down.allow() && t.downLim.allow() }
func (t *Torrent) downTake(n int) {
	t.c.down.take(n)
	t.downLim.take(n)
}

// upAllow and upTake are downAllow and downTake for uploads.
func (t *Torrent) upAllow() bool { return t.c.up.allow() && t.upLim.allow() }
func (t *Torrent) upTake(n int) {
	t.c.up.take(n)
	t.upLim.take(n)
}

// rateLimited reports a download or upload rate on the client or t.
func (t *Torrent) rateLimited() bool {
	return t.c.up.limited() || t.c.down.limited() || t.upLim.limited() || t.downLim.limited()
}

// errConnLimit closes the peers a lowered connection limit leaves over.
var errConnLimit = errors.New("bittorrent: over the connection limit")

// peerRank is how useful a peer is, for choosing which to close.
type peerRank struct {
	c       *peer.Conn
	flowing bool // a side is interested and unchoked
	snubbed bool
	perSec  float64
	since   time.Time
}

func (pc *peerConn) rankLocked() peerRank {
	c := pc.c
	return peerRank{
		c:       c,
		flowing: (c.AmInterested() && !c.PeerChoking()) || (c.PeerInterested() && !c.AmChoking()),
		snubbed: pc.snubbed, perSec: pc.down.perSec + pc.up.perSec, since: pc.since,
	}
}

// lessUseful orders peers least useful first: nothing flowing either way,
// then snubbed, then slowest, then newest.
func lessUseful(a, b *peerRank) bool {
	if a.flowing != b.flowing {
		return !a.flowing
	}
	if a.snubbed != b.snubbed {
		return a.snubbed
	}
	if a.perSec != b.perSec {
		return a.perSec < b.perSec
	}
	return a.since.After(b.since)
}

// trimConns closes the peers over the torrents' bounds, then those over
// the client's MaxConns, the least useful first.
func (c *Client) trimConns() {
	var victims []*peer.Conn
	var all []peerRank
	for _, t := range c.Torrents() {
		t.mu.Lock()
		ranks := make([]peerRank, 0, len(t.peers))
		for _, pc := range t.peers {
			if !pc.closed {
				ranks = append(ranks, pc.rankLocked())
			}
		}
		maxConns := t.maxConnsLocked()
		t.mu.Unlock()
		sort.Slice(ranks, func(i, j int) bool { return lessUseful(&ranks[i], &ranks[j]) })
		if n := len(ranks) - maxConns; n > 0 {
			for _, r := range ranks[:n] {
				victims = append(victims, r.c)
			}
			ranks = ranks[n:]
		}
		all = append(all, ranks...)
	}
	if n := len(all) - c.lim().MaxConns; n > 0 {
		sort.Slice(all, func(i, j int) bool { return lessUseful(&all[i], &all[j]) })
		for _, r := range all[:n] {
			victims = append(victims, r.c)
		}
	}
	for _, pc := range victims {
		pc.CloseWithError(errConnLimit)
	}
}

// limiter is a token bucket of bytes per second; a zero rate is no
// limit. It may go into debt by one block, which the next refills pay.
// Its rate changes while it is used.
type limiter struct {
	rate   atomic.Int64
	mu     sync.Mutex
	tokens int64
}

func (l *limiter) limited() bool { return l.rate.Load() != 0 }

// setRate changes the rate: a bucket that was unlimited starts full, one
// that was limited keeps its tokens up to the new rate.
func (l *limiter) setRate(r int64) {
	l.mu.Lock()
	if l.rate.Load() == 0 || l.tokens > r {
		l.tokens = r
	}
	l.rate.Store(r)
	l.mu.Unlock()
}

func (l *limiter) allow() bool {
	if l.rate.Load() == 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokens > 0
}

func (l *limiter) take(n int) {
	if l.rate.Load() == 0 {
		return
	}
	l.mu.Lock()
	l.tokens -= int64(n)
	l.mu.Unlock()
}

func (l *limiter) refill(dt time.Duration) {
	if l.rate.Load() == 0 {
		return
	}
	l.mu.Lock()
	r := l.rate.Load()
	l.tokens = min(l.tokens+int64(float64(r)*dt.Seconds()), r)
	l.mu.Unlock()
}
