package tracker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// Announcer announces to a tracker by its URL: HTTPClient, UDPClient, or
// Client, which picks one of them by the URL's scheme.
type Announcer interface {
	Announce(ctx context.Context, tracker string, r *AnnounceRequest) (*AnnounceResponse, error)
}

// DefaultTrackerTimeout is Tiers.Timeout when zero.
const DefaultTrackerTimeout = 30 * time.Second

// Tiers is a torrent's trackers, in the tiers of its announce-list (BEP
// 12), and announces to them as BEP 12 says: each tier is shuffled once,
// the trackers are tried a tier at a time, in order, until one answers,
// and the one that answers is moved to the front of its tier, to be tried
// first next time. It remembers the tracker ID each tracker gave, and
// sends it back. Its methods may be called from many goroutines at once.
type Tiers struct {
	// Timeout bounds the announce to each tracker, so that a dead one does
	// not hold up the rest; DefaultTrackerTimeout when zero.
	Timeout time.Duration

	mu    sync.Mutex
	tiers [][]string
	ids   map[string]string
}

// NewTiers returns the Tiers of an announce-list, such as
// metainfo.MetaInfo.Trackers returns; it leaves out empty tiers and URLs
// that appear twice.
func NewTiers(list [][]string) *Tiers {
	t := &Tiers{ids: map[string]string{}}
	seen := map[string]bool{}
	for _, tier := range list {
		var urls []string
		for _, u := range tier {
			if u = strings.TrimSpace(u); u != "" && !seen[u] {
				seen[u] = true
				urls = append(urls, u)
			}
		}
		if len(urls) > 0 {
			rand.Shuffle(len(urls), func(i, j int) { urls[i], urls[j] = urls[j], urls[i] })
			t.tiers = append(t.tiers, urls)
		}
	}
	return t
}

// List returns the tiers in the order they are tried.
func (t *Tiers) List() [][]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([][]string, len(t.tiers))
	for i, tier := range t.tiers {
		out[i] = append([]string(nil), tier...)
	}
	return out
}

// Announce announces r through a to the first tracker that answers, and
// returns its answer and its URL. A tracker that refuses the announce (an
// *Error) counts as not answering. When none answers, the error holds each
// tracker's.
func (t *Tiers) Announce(ctx context.Context, a Announcer, r AnnounceRequest) (*AnnounceResponse, string, error) {
	timeout := t.Timeout
	if timeout <= 0 {
		timeout = DefaultTrackerTimeout
	}
	var errs []error
	for _, tier := range t.List() {
		for _, u := range tier {
			if err := ctx.Err(); err != nil {
				return nil, "", err
			}
			req := r
			t.mu.Lock()
			if req.TrackerID == "" {
				req.TrackerID = t.ids[u]
			}
			t.mu.Unlock()
			actx, cancel := context.WithTimeout(ctx, timeout)
			resp, err := a.Announce(actx, u, &req)
			cancel()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", u, err))
				continue
			}
			t.promote(u, resp.TrackerID)
			return resp, u, nil
		}
	}
	if len(errs) == 0 {
		return nil, "", errors.New("tracker: no trackers")
	}
	return nil, "", errors.Join(errs...)
}

// promote moves u to the front of its tier, and records its tracker ID.
func (t *Tiers) promote(u, id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if id != "" {
		t.ids[u] = id
	}
	for _, tier := range t.tiers {
		for i, v := range tier {
			if v == u {
				copy(tier[1:i+1], tier[:i])
				tier[0] = u
				return
			}
		}
	}
}
