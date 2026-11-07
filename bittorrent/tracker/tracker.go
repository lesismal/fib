package tracker

import (
	"encoding/hex"
	"errors"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// PeerID is the 20-byte ID a client names itself by in the handshake and to
// trackers.
type PeerID [20]byte

// String returns id in hex.
func (id PeerID) String() string { return hex.EncodeToString(id[:]) }

// Event is what an announce reports, beyond the regular one. Its values are
// those of the UDP protocol (BEP 15), and of BEP 21 for EventPaused.
type Event int32

const (
	// EventNone is a regular announce, made every interval.
	EventNone Event = 0
	// EventCompleted is the announce of a download that has just finished.
	EventCompleted Event = 1
	// EventStarted is the first announce of a download.
	EventStarted Event = 2
	// EventStopped is the last announce: the peer is leaving the swarm.
	EventStopped Event = 3
	// EventPaused says the peer is a partial seed, which wants no more of
	// the torrent than it has (BEP 21).
	EventPaused Event = 4
)

var eventNames = [...]string{"", "completed", "started", "stopped", "paused"}

// String returns the event's name in an HTTP announce, "" for EventNone.
func (e Event) String() string {
	if e >= 0 && int(e) < len(eventNames) {
		return eventNames[e]
	}
	return "unknown"
}

func parseEvent(s string) (Event, bool) {
	if s == "empty" { // what some clients send for none
		return EventNone, true
	}
	for i, n := range eventNames {
		if n == s {
			return Event(i), true
		}
	}
	return EventNone, false
}

// AnnounceRequest is what a client tells a tracker about its download of a
// torrent.
type AnnounceRequest struct {
	InfoHash metainfo.InfoHash
	PeerID   PeerID
	// Port is where the client accepts peer connections.
	Port uint16
	// Uploaded and Downloaded are the bytes sent to and received from peers
	// since EventStarted, and Left the bytes still missing; a negative Left,
	// for a torrent whose size the client does not know yet (a magnet link
	// without metadata), is sent as the largest int64, as anacrolix/torrent
	// does, since trackers reject -1.
	Uploaded, Downloaded, Left int64
	Event                      Event
	// NumWant is how many peers to ask for: zero leaves it to the tracker,
	// and a negative value asks for none, as an EventStopped announce does.
	NumWant int
	// Key identifies the client across changes of its address; it is sent
	// in hex over HTTP, as libtorrent does, and as is over UDP.
	Key uint32
	// TrackerID is the "tracker id" the tracker's last answer gave, which
	// it wants back (HTTP only).
	TrackerID string
	// IP is an address to announce in place of the one the request comes
	// from; trackers usually take it only from a local network. IPv4 and
	// IPv6 are the client's addresses of each family (BEP 7), sent as the
	// "ipv4" and "ipv6" parameters (HTTP only), for a tracker to hand out
	// both. Unset addresses are not sent.
	IP, IPv4, IPv6 netip.Addr
}

// Peer is a peer a tracker handed out. ID is zero when the tracker gave
// none, as compact peer lists never do.
type Peer struct {
	Addr netip.AddrPort
	ID   PeerID
}

// AnnounceResponse is a tracker's answer to an announce.
type AnnounceResponse struct {
	// Interval is how long to wait before the next regular announce, and
	// MinInterval, when the tracker gives one, how often it may be asked at
	// most.
	Interval, MinInterval time.Duration
	// Complete is the number of seeders, Incomplete of leechers, when the
	// tracker says.
	Complete, Incomplete int
	Peers                []Peer
	// TrackerID is to be sent back in later announces to this tracker.
	TrackerID string
	// Warning is a message the tracker wants shown; the announce worked.
	Warning string
	// ExternalIP is the client's address as the tracker sees it (BEP 24),
	// when it says.
	ExternalIP netip.Addr
}

// Stats is the state of a torrent's swarm: seeders (Complete), leechers
// (Incomplete) and completed downloads (Downloaded), as a scrape (BEP 48)
// reports it.
type Stats struct {
	Complete, Incomplete, Downloaded int
}

// Error is a tracker's refusal, its "failure reason" over HTTP or an error
// packet over UDP.
type Error struct {
	Reason string
}

func (e *Error) Error() string { return "tracker: " + e.Reason }

var (
	// ErrNoScrape is what scraping a tracker whose announce URL has no
	// scrape URL gets (BEP 48).
	ErrNoScrape = errors.New("tracker: the announce URL has no scrape URL")
	// ErrTimeout is what a UDP request gets once the tracker has not
	// answered its last retransmission.
	ErrTimeout = errors.New("tracker: no answer from the tracker")
	// ErrClosed is what requests get once their client is closed.
	ErrClosed = errors.New("tracker: client closed")
	// ErrScheme is what a tracker URL of a scheme the client has no
	// transport for gets.
	ErrScheme = errors.New("tracker: unsupported tracker URL scheme")
)

// ScrapeURL returns the scrape URL of an HTTP tracker's announce URL: the
// last path segment must begin with "announce", which becomes "scrape"
// (BEP 48). Without such a segment the tracker cannot be scraped, and
// ScrapeURL returns ErrNoScrape.
func ScrapeURL(announce string) (string, error) {
	u, err := url.Parse(announce)
	if err != nil {
		return "", err
	}
	p := u.Path
	i := strings.LastIndexByte(p, '/')
	if i < 0 || !strings.HasPrefix(p[i+1:], "announce") {
		return "", ErrNoScrape
	}
	u.Path = p[:i+1] + "scrape" + p[i+1+len("announce"):]
	u.RawPath = ""
	return u.String(), nil
}
