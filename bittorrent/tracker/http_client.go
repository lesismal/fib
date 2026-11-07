//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tracker

import (
	"context"
	"fmt"
	"io"
	stdhttp "net/http"

	"github.com/lesismal/fib/bittorrent/metainfo"
	fibhttp "github.com/lesismal/fib/http"
)

// DefaultUserAgent is the User-Agent of an HTTPClient's requests when it
// sets none.
const DefaultUserAgent = "fib-bittorrent/1"

// maxHTTPBody bounds the answer of an HTTP tracker the client reads: a
// tracker's answer is a few kilobytes at most, even with full peer lists.
const maxHTTPBody = 1 << 20

// HTTPClient announces to and scrapes HTTP and HTTPS trackers (BEP 3, BEP
// 23, BEP 7, BEP 48), through a fib/http Client, whose engine's workers
// read the answers. Its methods may be called from many goroutines at once.
type HTTPClient struct {
	client *fibhttp.Client
	// UserAgent is the requests' User-Agent; DefaultUserAgent when empty.
	// Some trackers allow only the clients they know.
	UserAgent string
}

// NewHTTPClient returns an HTTPClient that sends its requests through
// client. Its config's Timeout bounds each request, beyond the context
// each method is given.
func NewHTTPClient(client *fibhttp.Client) *HTTPClient {
	return &HTTPClient{client: client}
}

// Announce announces r to the HTTP tracker whose announce URL is tracker.
// A tracker's failure reason is an *Error.
func (c *HTTPClient) Announce(ctx context.Context, tracker string, r *AnnounceRequest) (*AnnounceResponse, error) {
	body, err := c.get(ctx, announceURL(tracker, r))
	if err != nil {
		return nil, err
	}
	return parseAnnounceResponse(body)
}

// Scrape asks the HTTP tracker whose announce URL is tracker for the state
// of the swarms of hashes, at the scrape URL of BEP 48 (see ScrapeURL).
// Torrents the tracker does not know are missing from the map it returns.
func (c *HTTPClient) Scrape(ctx context.Context, tracker string, hashes []metainfo.InfoHash) (map[metainfo.InfoHash]Stats, error) {
	scrape, err := ScrapeURL(tracker)
	if err != nil {
		return nil, err
	}
	body, err := c.get(ctx, scrapeURL(scrape, hashes))
	if err != nil {
		return nil, err
	}
	return parseScrapeResponse(body)
}

// get returns the body of a GET of u. A status other than 200 is an error,
// unless the body says why, as a failure reason, which some trackers send
// with a 4xx status.
func (c *HTTPClient) get(ctx context.Context, u string) ([]byte, error) {
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	ua := c.UserAgent
	if ua == "" {
		ua = DefaultUserAgent
	}
	req.Header.Set("User-Agent", ua)
	resp, err := c.client.Go(req).Wait()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxHTTPBody {
		return nil, fmt.Errorf("tracker: answer of more than %d bytes", maxHTTPBody)
	}
	if resp.StatusCode != stdhttp.StatusOK {
		if reason, ok := failureOf(body); ok {
			return nil, &Error{Reason: reason}
		}
		return nil, fmt.Errorf("tracker: HTTP status %s", resp.Status)
	}
	return body, nil
}
