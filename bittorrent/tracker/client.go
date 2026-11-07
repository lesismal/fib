//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tracker

import (
	"context"
	"strings"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Client announces to and scrapes trackers of any scheme it has a client
// for: http:// and https:// through HTTP, udp:// through UDP. Either may be
// nil, and a URL of its scheme then fails with ErrScheme.
type Client struct {
	HTTP *HTTPClient
	UDP  *UDPClient
}

// Announce announces r to the tracker of the URL tracker.
func (c *Client) Announce(ctx context.Context, tracker string, r *AnnounceRequest) (*AnnounceResponse, error) {
	switch schemeOf(tracker) {
	case "http", "https":
		if c.HTTP != nil {
			return c.HTTP.Announce(ctx, tracker, r)
		}
	case "udp":
		if c.UDP != nil {
			return c.UDP.Announce(ctx, tracker, r)
		}
	}
	return nil, ErrScheme
}

// Scrape asks the tracker of the announce URL tracker for the state of the
// swarms of hashes.
func (c *Client) Scrape(ctx context.Context, tracker string, hashes []metainfo.InfoHash) (map[metainfo.InfoHash]Stats, error) {
	switch schemeOf(tracker) {
	case "http", "https":
		if c.HTTP != nil {
			return c.HTTP.Scrape(ctx, tracker, hashes)
		}
	case "udp":
		if c.UDP != nil {
			return c.UDP.Scrape(ctx, tracker, hashes)
		}
	}
	return nil, ErrScheme
}

func schemeOf(u string) string {
	scheme, _, ok := strings.Cut(u, "://")
	if !ok {
		return ""
	}
	return strings.ToLower(scheme)
}
