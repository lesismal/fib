//go:build linux || darwin || windows

// Package limiter bounds how many requests each client may make in a window
// of time, and answers the ones past the bound with 429 Too Many Requests.
//
// The counts are kept in the process's memory, one per key — the client's
// address by default — so every instance of a server behind a load balancer
// counts on its own. Responses say where their client stands in the
// X-RateLimit-Limit, X-RateLimit-Remaining and X-RateLimit-Reset headers,
// and a refused one says when to try again in Retry-After.
package limiter

import (
	"hash/maphash"
	"math"
	stdhttp "net/http"
	"strconv"
	"sync"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
)

const (
	// DefaultMax is how many requests a client may make in a window by
	// default.
	DefaultMax = 60
	// DefaultExpiration is how long a window lasts by default.
	DefaultExpiration = time.Minute
)

type Config struct {
	// Next passes the requests it reports true for straight on, uncounted.
	Next middleware.Skipper
	// Max is how many requests a client may make in a window; DefaultMax by
	// default.
	Max int
	// Expiration is how long a window lasts; DefaultExpiration by default.
	Expiration time.Duration
	// KeyGenerator tells clients apart; by their address by default, which
	// behind a proxy is the proxy's.
	KeyGenerator func(c *fibhttp.Context, r *stdhttp.Request) string
	// LimitReached answers a request past the bound; 429 Too Many Requests
	// by default. The Retry-After and X-RateLimit-* fields are set on its
	// response whichever way it answers.
	LimitReached fibhttp.HandlerFunc
	// SlidingWindow counts the requests of the window just past too, in
	// proportion to how much of it the window ending now still covers, so
	// that a client cannot make twice Max requests across the edge between
	// two windows. By default the windows are fixed.
	SlidingWindow bool
	// SkipFailedRequests leaves uncounted a request whose response is 400 or
	// above, and SkipSuccessfulRequests one whose response is below it.
	SkipFailedRequests     bool
	SkipSuccessfulRequests bool
	// DisableHeaders leaves the X-RateLimit-* and Retry-After fields out.
	DisableHeaders bool
}

// New returns the middleware.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Max <= 0 {
		cfg.Max = DefaultMax
	}
	if cfg.Expiration <= 0 {
		cfg.Expiration = DefaultExpiration
	}
	if cfg.KeyGenerator == nil {
		cfg.KeyGenerator = func(_ *fibhttp.Context, r *stdhttp.Request) string { return middleware.RemoteIP(r) }
	}
	if cfg.LimitReached == nil {
		cfg.LimitReached = tooManyRequests
	}
	limit := strconv.Itoa(cfg.Max)
	store := newStore(cfg.Expiration.Nanoseconds(), cfg.SlidingWindow)
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			key := cfg.KeyGenerator(c, r)
			now := time.Now().UnixNano()
			used, allowed, window, reset := store.take(key, now, cfg.Max)
			if !cfg.DisableHeaders {
				remaining := strconv.Itoa(max(cfg.Max-used, 0))
				resetIn := strconv.FormatInt((reset+int64(time.Second)-1)/int64(time.Second), 10)
				c.OnHeader(func(_ int, h stdhttp.Header) {
					h["X-Ratelimit-Limit"] = []string{limit}
					h["X-Ratelimit-Remaining"] = []string{remaining}
					h["X-Ratelimit-Reset"] = []string{resetIn}
					if !allowed {
						h["Retry-After"] = []string{resetIn}
					}
				})
			}
			if !allowed {
				cfg.LimitReached(c, r)
				return
			}
			if cfg.SkipFailedRequests || cfg.SkipSuccessfulRequests {
				c.OnFinish(func(status int, _ stdhttp.Header, _ int64) {
					if failed := status >= stdhttp.StatusBadRequest; failed && cfg.SkipFailedRequests ||
						!failed && cfg.SkipSuccessfulRequests {
						store.give(key, window)
					}
				})
			}
			next.ServeHTTP(c, r)
		})
	}
}

func tooManyRequests(c *fibhttp.Context, _ *stdhttp.Request) {
	status := stdhttp.StatusTooManyRequests
	_ = c.Respond(status, "text/plain; charset=utf-8", []byte(stdhttp.StatusText(status)+"\n"))
}

// store keeps the counts, in shards that each have a lock of their own.
type store struct {
	seed    maphash.Seed
	window  int64
	sliding bool
	shards  [64]shard
}

type shard struct {
	mu      sync.Mutex
	entries map[string]*entry
	// sweepAt is when the shard is next cleared of the keys whose windows
	// have all passed.
	sweepAt int64
}

// entry is one key's count in the window that started at start, and in the
// one before it.
type entry struct {
	start     int64
	count     int
	prevCount int
}

func newStore(window int64, sliding bool) *store {
	s := &store{seed: maphash.MakeSeed(), window: window, sliding: sliding}
	for i := range s.shards {
		s.shards[i].entries = make(map[string]*entry)
	}
	return s
}

func (s *store) shard(key string) *shard {
	return &s.shards[maphash.String(s.seed, key)%uint64(len(s.shards))]
}

// take counts a request for key at now. It returns how many requests the key
// has made, the one just counted included, whether that is within limit, the
// window it was counted in, and how long until the key's count goes down. A
// request past limit is not counted, so that a client that keeps knocking is
// not kept out for longer.
func (s *store) take(key string, now int64, limit int) (used int, allowed bool, window, reset int64) {
	start := now - now%s.window
	sh := s.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if now >= sh.sweepAt {
		sh.sweep(start, s.window)
		sh.sweepAt = now + s.window
	}
	e := sh.entries[key]
	if e == nil {
		e = &entry{start: start}
		sh.entries[key] = e
	}
	switch {
	case e.start == start:
	case e.start == start-s.window:
		e.start, e.prevCount, e.count = start, e.count, 0
	default:
		e.start, e.prevCount, e.count = start, 0, 0
	}
	weighted := float64(e.count + 1)
	if s.sliding && e.prevCount > 0 {
		// The window ending now still covers this much of the last one.
		left := float64(s.window-(now-start)) / float64(s.window)
		weighted += float64(e.prevCount) * left
	}
	allowed = weighted <= float64(limit)
	if allowed {
		e.count++
	}
	return int(math.Ceil(weighted)), allowed, start, start + s.window - now
}

// give takes back a request counted in window, as long as the key's count is
// still that window's.
func (s *store) give(key string, window int64) {
	sh := s.shard(key)
	sh.mu.Lock()
	if e := sh.entries[key]; e != nil && e.start == window && e.count > 0 {
		e.count--
	}
	sh.mu.Unlock()
}

// sweep removes the keys whose counts no longer matter in the window that
// started at start: those last counted before the window just past.
func (sh *shard) sweep(start, window int64) {
	for key, e := range sh.entries {
		if e.start < start-window {
			delete(sh.entries, key)
		}
	}
}
