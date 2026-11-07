package tracker

import (
	"errors"
	"math"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

// This file is the HTTP tracker protocol's encoding, without any I/O: the
// announce and scrape query strings (BEP 3, BEP 7, BEP 48) and the bencoded
// answers to them, compact (BEP 23) or not.

const hexDigits = "0123456789ABCDEF"

// appendEscaped appends s percent-encoded, everything but the unreserved
// characters of RFC 3986 escaped. url.QueryEscape would write a space as
// '+', which old trackers take literally; binary info hashes and peer IDs
// are full of bytes that need escaping.
func appendEscaped(dst []byte, s []byte) []byte {
	for _, c := range s {
		if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			dst = append(dst, c)
		} else {
			dst = append(dst, '%', hexDigits[c>>4], hexDigits[c&15])
		}
	}
	return dst
}

func appendParam(dst []byte, name string, value []byte) []byte {
	dst = append(dst, '&')
	dst = append(dst, name...)
	dst = append(dst, '=')
	return appendEscaped(dst, value)
}

func appendIntParam(dst []byte, name string, v int64) []byte {
	dst = append(dst, '&')
	dst = append(dst, name...)
	dst = append(dst, '=')
	return strconv.AppendInt(dst, v, 10)
}

// withQuery returns the tracker URL with query appended to the query it may
// already have, which some private trackers need to stay first.
func withQuery(tracker string, query []byte) string {
	sep := "?"
	if strings.IndexByte(tracker, '?') >= 0 {
		sep = "&"
	}
	if i := strings.IndexByte(tracker, '#'); i >= 0 {
		tracker = tracker[:i]
	}
	return tracker + sep + string(query)
}

// announceURL returns the GET URL of an HTTP announce of r to tracker.
func announceURL(tracker string, r *AnnounceRequest) string {
	q := make([]byte, 0, 256)
	q = append(q, "info_hash="...)
	q = appendEscaped(q, r.InfoHash[:])
	q = appendParam(q, "peer_id", r.PeerID[:])
	q = appendIntParam(q, "port", int64(r.Port))
	q = appendIntParam(q, "uploaded", r.Uploaded)
	q = appendIntParam(q, "downloaded", r.Downloaded)
	left := r.Left
	if left < 0 {
		left = math.MaxInt64
	}
	q = appendIntParam(q, "left", left)
	q = append(q, "&compact=1"...)
	if r.Event != EventNone {
		q = append(q, "&event="...)
		q = append(q, r.Event.String()...)
	}
	switch {
	case r.NumWant > 0:
		q = appendIntParam(q, "numwant", int64(r.NumWant))
	case r.NumWant < 0:
		q = append(q, "&numwant=0"...)
	}
	if r.Key != 0 {
		q = append(q, "&key="...)
		q = append(q, hexDigits[r.Key>>28&15], hexDigits[r.Key>>24&15], hexDigits[r.Key>>20&15], hexDigits[r.Key>>16&15],
			hexDigits[r.Key>>12&15], hexDigits[r.Key>>8&15], hexDigits[r.Key>>4&15], hexDigits[r.Key&15])
	}
	if r.TrackerID != "" {
		q = appendParam(q, "trackerid", []byte(r.TrackerID))
	}
	for _, p := range [...]struct {
		name string
		a    netip.Addr
	}{{"ip", r.IP}, {"ipv4", r.IPv4}, {"ipv6", r.IPv6}} {
		if p.a.IsValid() {
			q = appendParam(q, p.name, []byte(p.a.String()))
		}
	}
	return withQuery(tracker, q)
}

// scrapeURL returns the GET URL of an HTTP scrape of hashes at the scrape
// URL scrape.
func scrapeURL(scrape string, hashes []metainfo.InfoHash) string {
	q := make([]byte, 0, len(hashes)*64)
	for i, h := range hashes {
		if i > 0 {
			q = append(q, '&')
		}
		q = append(q, "info_hash="...)
		q = appendEscaped(q, h[:])
	}
	if len(q) == 0 {
		return scrape
	}
	return withQuery(scrape, q)
}

// httpAnnounce is an announce as a tracker reads it from a query string.
type httpAnnounce struct {
	AnnounceRequest
	// numWant is the numwant parameter, -1 without one.
	numWant int
	// compact is false only for compact=0; noPeerID leaves the peer IDs out
	// of a list that is not compact.
	compact, noPeerID bool
	// ip, ipv4 and ipv6 are the address parameters with their ports: the
	// announce's Port unless BEP 7's "[address]:port" form gave another.
	ip, ipv4, ipv6 netip.AddrPort
	passkey        string
}

var (
	errInfoHash = errors.New("invalid info_hash")
	errPeerID   = errors.New("invalid peer_id")
	errPort     = errors.New("invalid port")
	errNumber   = errors.New("invalid number")
)

// parseAnnounceQuery reads an announce from the query string of its URL.
// The errors it returns are failure reasons for the client.
func parseAnnounceQuery(raw string) (*httpAnnounce, error) {
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return nil, errors.New("invalid query string")
	}
	a := &httpAnnounce{numWant: -1, compact: true}
	ih, id := vals["info_hash"], vals["peer_id"]
	if len(ih) != 1 || len(ih[0]) != 20 {
		return nil, errInfoHash
	}
	if len(id) != 1 || len(id[0]) != 20 {
		return nil, errPeerID
	}
	copy(a.InfoHash[:], ih[0])
	copy(a.PeerID[:], id[0])
	port, err := strconv.ParseUint(vals.Get("port"), 10, 16)
	if err != nil {
		return nil, errPort
	}
	a.Port = uint16(port)
	for _, f := range [...]struct {
		name string
		v    *int64
		def  int64
	}{{"uploaded", &a.Uploaded, 0}, {"downloaded", &a.Downloaded, 0}, {"left", &a.Left, -1}} {
		s := vals.Get(f.name)
		if s == "" {
			*f.v = f.def
			continue
		}
		if *f.v, err = strconv.ParseInt(s, 10, 64); err != nil {
			return nil, errNumber
		}
	}
	if s := vals.Get("numwant"); s != "" {
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errNumber
		}
		a.numWant = int(max(n, -1))
	}
	a.NumWant = numWantField(a.numWant)
	if ev, ok := parseEvent(vals.Get("event")); ok {
		a.Event = ev
	} else {
		return nil, errors.New("invalid event")
	}
	a.compact = vals.Get("compact") != "0"
	a.noPeerID = vals.Get("no_peer_id") == "1"
	a.TrackerID = vals.Get("trackerid")
	a.passkey = vals.Get("passkey")
	// libtorrent sends the key in hex, anacrolix/torrent in decimal; the
	// tracker only compares it, so either reading does.
	if s := vals.Get("key"); s != "" {
		if k, err := strconv.ParseUint(s, 16, 32); err == nil && len(s) == 8 {
			a.Key = uint32(k)
		} else if k, err := strconv.ParseInt(s, 10, 64); err == nil {
			a.Key = uint32(k)
		}
	}
	a.ip = parseAddrParam(vals.Get("ip"), a.Port)
	a.ipv4 = parseAddrParam(vals.Get("ipv4"), a.Port)
	a.ipv6 = parseAddrParam(vals.Get("ipv6"), a.Port)
	a.IP, a.IPv4, a.IPv6 = a.ip.Addr(), a.ipv4.Addr(), a.ipv6.Addr()
	return a, nil
}

// numWantField returns AnnounceRequest.NumWant of a request's numwant, -1
// when it has none.
func numWantField(n int) int {
	switch {
	case n < 0:
		return 0
	case n == 0:
		return -1
	}
	return n
}

// parseAddrParam reads an ip, ipv4 or ipv6 parameter: an address, or an
// address and a port as BEP 7 allows. A DNS name, which BEP 3 allows in ip,
// is not taken: the tracker would have to resolve it for each peer it hands
// it to. It returns the zero AddrPort for anything it does not take.
func parseAddrParam(s string, port uint16) netip.AddrPort {
	if s == "" {
		return netip.AddrPort{}
	}
	if ap, err := netip.ParseAddrPort(s); err == nil {
		if ap.Addr().Zone() != "" {
			return netip.AddrPort{}
		}
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	if a, err := netip.ParseAddr(strings.Trim(s, "[]")); err == nil && a.Zone() == "" {
		return netip.AddrPortFrom(a.Unmap(), port)
	}
	return netip.AddrPort{}
}

// parseScrapeQuery reads the info hashes of a scrape; it skips values that
// are not 20 bytes long, and stops at max of them.
func parseScrapeQuery(raw string, max int) ([]metainfo.InfoHash, error) {
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return nil, errors.New("invalid query string")
	}
	var hashes []metainfo.InfoHash
	for _, v := range vals["info_hash"] {
		if len(v) != 20 {
			continue
		}
		if len(hashes) == max {
			break
		}
		hashes = append(hashes, metainfo.InfoHash([]byte(v)))
	}
	return hashes, nil
}

// httpFailure is a tracker's refusal over HTTP.
type httpFailure struct {
	Reason string `bencode:"failure reason"`
}

func encodeFailure(reason string) []byte {
	b, _ := bencode.Marshal(httpFailure{reason})
	return b
}

// httpPeer is an entry of a peer list that is not compact.
type httpPeer struct {
	ID   []byte `bencode:"peer id,omitempty"`
	IP   string `bencode:"ip"`
	Port int64  `bencode:"port"`
}

// httpAnnounceBody is a tracker's answer to an announce, as it writes it.
type httpAnnounceBody struct {
	Complete    int                `bencode:"complete"`
	Incomplete  int                `bencode:"incomplete"`
	Interval    int64              `bencode:"interval"`
	MinInterval int64              `bencode:"min interval,omitempty"`
	Peers       bencode.RawMessage `bencode:"peers"`
	Peers6      []byte             `bencode:"peers6,omitempty"`
	TrackerID   string             `bencode:"tracker id,omitempty"`
	Warning     string             `bencode:"warning message,omitempty"`
	ExternalIP  []byte             `bencode:"external ip,omitempty"`
}

// encodeAnnounce encodes r as a tracker answers an announce: with compact
// peer lists, IPv4 peers in "peers" and IPv6 ones in "peers6" (BEP 7), or
// with one list of dictionaries holding both, with peer IDs unless noPeerID.
func encodeAnnounce(r *AnnounceResponse, compact, noPeerID bool) []byte {
	body := httpAnnounceBody{
		Complete:    r.Complete,
		Incomplete:  r.Incomplete,
		Interval:    int64(r.Interval / time.Second),
		MinInterval: int64(r.MinInterval / time.Second),
		TrackerID:   r.TrackerID,
		Warning:     r.Warning,
	}
	if r.ExternalIP.IsValid() {
		body.ExternalIP = r.ExternalIP.Unmap().AsSlice()
	}
	if compact {
		var v4 []byte
		for _, p := range r.Peers {
			if p.Addr.Addr().Unmap().Is4() {
				v4 = appendCompact(v4, p.Addr)
			} else {
				body.Peers6 = appendCompact(body.Peers6, p.Addr)
			}
		}
		body.Peers, _ = bencode.Marshal(v4)
	} else {
		list := make([]httpPeer, len(r.Peers))
		for i, p := range r.Peers {
			list[i] = httpPeer{IP: p.Addr.Addr().Unmap().String(), Port: int64(p.Addr.Port())}
			if !noPeerID {
				list[i].ID = append([]byte(nil), p.ID[:]...)
			}
		}
		body.Peers, _ = bencode.Marshal(list)
	}
	b, _ := bencode.Marshal(body)
	return b
}

// httpAnnounceWire is a tracker's answer to an announce, as a client reads
// it.
type httpAnnounceWire struct {
	Failure     string             `bencode:"failure reason"`
	Warning     string             `bencode:"warning message"`
	Interval    int64              `bencode:"interval"`
	MinInterval int64              `bencode:"min interval"`
	TrackerID   string             `bencode:"tracker id"`
	Complete    int64              `bencode:"complete"`
	Incomplete  int64              `bencode:"incomplete"`
	Peers       bencode.RawMessage `bencode:"peers"`
	Peers6      bencode.RawMessage `bencode:"peers6"`
	ExternalIP  []byte             `bencode:"external ip"`
}

// maxInterval bounds the intervals a tracker gives, which a client would
// otherwise wait out: a week.
const maxInterval = 7 * 24 * 3600

func seconds(n int64) time.Duration {
	return time.Duration(min(max(n, 0), maxInterval)) * time.Second
}

func clampInt(n int64) int {
	return int(min(max(n, 0), math.MaxInt32))
}

// failureOf returns the failure reason of a bencoded dictionary, for an
// answer that does not decode as a whole.
func failureOf(body []byte) (string, bool) {
	v, err := bencode.Lookup(body, "failure reason")
	if err != nil {
		return "", false
	}
	var s string
	if bencode.Unmarshal(v, &s) != nil {
		return "", false
	}
	return s, true
}

// parseAnnounceResponse reads a tracker's answer to an announce; a failure
// reason is an *Error.
func parseAnnounceResponse(body []byte) (*AnnounceResponse, error) {
	var w httpAnnounceWire
	if err := bencode.Unmarshal(body, &w); err != nil {
		if reason, ok := failureOf(body); ok {
			return nil, &Error{Reason: reason}
		}
		return nil, err
	}
	if w.Failure != "" {
		return nil, &Error{Reason: w.Failure}
	}
	r := &AnnounceResponse{
		Interval:    seconds(w.Interval),
		MinInterval: seconds(w.MinInterval),
		Complete:    clampInt(w.Complete),
		Incomplete:  clampInt(w.Incomplete),
		TrackerID:   w.TrackerID,
		Warning:     w.Warning,
	}
	var err error
	if r.Peers, err = parsePeerList(r.Peers, w.Peers, compactV4); err != nil {
		return nil, err
	}
	if r.Peers, err = parsePeerList(r.Peers, w.Peers6, compactV6); err != nil {
		return nil, err
	}
	if a, ok := netip.AddrFromSlice(w.ExternalIP); ok {
		r.ExternalIP = a.Unmap()
	}
	return r, nil
}

// httpPeerWire is an entry of a peer list that is not compact, as a client
// reads it.
type httpPeerWire struct {
	ID   []byte `bencode:"peer id"`
	IP   []byte `bencode:"ip"`
	Port int64  `bencode:"port"`
}

// parsePeerList appends the peers of a "peers" or "peers6" value: a compact
// string of entries of size bytes, or a list of dictionaries. Entries whose
// ip is a DNS name are skipped; so are those nobody can connect to.
func parsePeerList(dst []Peer, raw []byte, size int) ([]Peer, error) {
	if len(raw) == 0 {
		return dst, nil
	}
	if raw[0] != 'l' {
		var b []byte
		if err := bencode.Unmarshal(raw, &b); err != nil {
			return dst, err
		}
		return parseCompact(dst, b, size), nil
	}
	var list []httpPeerWire
	if err := bencode.Unmarshal(raw, &list); err != nil {
		return dst, err
	}
	for _, e := range list {
		if e.Port <= 0 || e.Port > math.MaxUint16 {
			continue
		}
		a, err := netip.ParseAddr(string(e.IP))
		if err != nil {
			// A few trackers write the address in binary.
			var ok bool
			if a, ok = netip.AddrFromSlice(e.IP); !ok {
				continue
			}
		}
		if a = a.Unmap(); a.IsUnspecified() || a.Zone() != "" {
			continue
		}
		p := Peer{Addr: netip.AddrPortFrom(a, uint16(e.Port))}
		copy(p.ID[:], e.ID)
		if len(e.ID) != len(p.ID) {
			p.ID = PeerID{}
		}
		dst = append(dst, p)
	}
	return dst, nil
}

// scrapeFileWire is a torrent's entry in a scrape's answer.
type scrapeFileWire struct {
	Complete   int64 `bencode:"complete"`
	Downloaded int64 `bencode:"downloaded"`
	Incomplete int64 `bencode:"incomplete"`
}

type scrapeWire struct {
	Failure string                    `bencode:"failure reason,omitempty"`
	Files   map[string]scrapeFileWire `bencode:"files"`
}

// encodeScrape encodes the answer to a scrape.
func encodeScrape(files map[metainfo.InfoHash]Stats) []byte {
	w := scrapeWire{Files: make(map[string]scrapeFileWire, len(files))}
	for h, s := range files {
		w.Files[string(h[:])] = scrapeFileWire{
			Complete: int64(s.Complete), Downloaded: int64(s.Downloaded), Incomplete: int64(s.Incomplete),
		}
	}
	b, _ := bencode.Marshal(w)
	return b
}

// parseScrapeResponse reads a tracker's answer to a scrape; entries whose
// key is not an info hash are skipped.
func parseScrapeResponse(body []byte) (map[metainfo.InfoHash]Stats, error) {
	var w scrapeWire
	if err := bencode.Unmarshal(body, &w); err != nil {
		if reason, ok := failureOf(body); ok {
			return nil, &Error{Reason: reason}
		}
		return nil, err
	}
	if w.Failure != "" {
		return nil, &Error{Reason: w.Failure}
	}
	files := make(map[metainfo.InfoHash]Stats, len(w.Files))
	for k, f := range w.Files {
		if len(k) != 20 {
			continue
		}
		files[metainfo.InfoHash([]byte(k))] = Stats{
			Complete: clampInt(f.Complete), Incomplete: clampInt(f.Incomplete), Downloaded: clampInt(f.Downloaded),
		}
	}
	return files, nil
}
