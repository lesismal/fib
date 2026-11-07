package metainfo

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// Magnet is a magnet link (BEP 9): a torrent named by its info hash, from
// which a client fetches the info dictionary from peers.
type Magnet struct {
	// InfoHash is the v1 hash (xt=urn:btih:), zero if absent.
	InfoHash InfoHash
	// InfoHashV2 is the v2 hash (xt=urn:btmh: with a SHA-256 multihash,
	// BEP 52), zero if absent. A hybrid torrent's link has both.
	InfoHashV2 InfoHashV2
	// Name is the display name (dn).
	Name string
	// Trackers are tracker URLs (tr).
	Trackers []string
	// WebSeeds are web seed URLs (ws, BEP 19).
	WebSeeds []string
	// Peers are peer addresses, host:port (x.pe).
	Peers []string
	// Select is the files to download (so, BEP 53), as ranges of file
	// indexes; nil for all.
	Select []IndexRange
	// Length is the torrent's length in bytes (xl), 0 if absent.
	Length int64
	// Params holds the parameters not above, such as xs, as or kt.
	Params url.Values
}

// IndexRange is the file indexes First through Last, inclusive.
type IndexRange struct {
	First, Last int
}

const (
	btihPrefix = "urn:btih:"
	btmhPrefix = "urn:btmh:"
	// sha256Multihash is the multihash header of a SHA-256 digest: code
	// 0x12, length 32.
	sha256Multihash = "1220"
)

// ParseMagnet parses a magnet link, which must name a torrent by a v1 or
// v2 info hash or both. Exact topics of other kinds are ignored.
func ParseMagnet(s string) (*Magnet, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("metainfo: magnet: %w", err)
	}
	if u.Scheme != "magnet" {
		return nil, errors.New("metainfo: not a magnet link")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("metainfo: magnet: %w", err)
	}
	m := &Magnet{}
	var found bool
	for k, vs := range q {
		// Several exact topics may be numbered: xt.1, xt.2.
		if k != "xt" && !strings.HasPrefix(k, "xt.") {
			continue
		}
		for _, v := range vs {
			lower := strings.ToLower(v)
			switch {
			case strings.HasPrefix(lower, btihPrefix):
				h, err := ParseInfoHash(v[len(btihPrefix):])
				if err != nil {
					return nil, err
				}
				m.InfoHash, found = h, true
			case strings.HasPrefix(lower, btmhPrefix):
				mh := lower[len(btmhPrefix):]
				if !strings.HasPrefix(mh, sha256Multihash) {
					return nil, errors.New("metainfo: magnet: btmh is not a SHA-256 multihash")
				}
				h, err := ParseInfoHashV2(mh[len(sha256Multihash):])
				if err != nil {
					return nil, err
				}
				m.InfoHashV2, found = h, true
			}
		}
		delete(q, k)
	}
	// A zero hash stands for none in Magnet, and names no real torrent.
	if !found || m.InfoHash.IsZero() && m.InfoHashV2.IsZero() {
		return nil, errors.New("metainfo: magnet link without a BitTorrent info hash")
	}
	m.Name = q.Get("dn")
	m.Trackers = q["tr"]
	m.WebSeeds = q["ws"]
	m.Peers = q["x.pe"]
	if so := q.Get("so"); so != "" {
		if m.Select, err = parseSelect(so); err != nil {
			return nil, err
		}
	}
	if xl := q.Get("xl"); xl != "" {
		if m.Length, err = strconv.ParseInt(xl, 10, 64); err != nil || m.Length < 0 {
			return nil, errors.New("metainfo: magnet: invalid xl")
		}
	}
	for _, k := range []string{"dn", "tr", "ws", "x.pe", "so", "xl"} {
		delete(q, k)
	}
	if len(q) > 0 {
		m.Params = q
	}
	return m, nil
}

// parseSelect parses BEP 53's "so" value: indexes and ranges of them,
// comma separated, such as "0,2,4-6".
func parseSelect(s string) ([]IndexRange, error) {
	var rs []IndexRange
	for _, part := range strings.Split(s, ",") {
		a, b, isRange := strings.Cut(part, "-")
		first, err1 := strconv.Atoi(a)
		last, err2 := first, error(nil)
		if isRange {
			last, err2 = strconv.Atoi(b)
		}
		if err1 != nil || err2 != nil || first < 0 || last < first {
			return nil, fmt.Errorf("metainfo: magnet: invalid so %q", s)
		}
		rs = append(rs, IndexRange{first, last})
	}
	return rs, nil
}

// Selected reports whether file index i is to be downloaded: always when
// Select is nil.
func (m *Magnet) Selected(i int) bool {
	if m.Select == nil {
		return true
	}
	for _, r := range m.Select {
		if i >= r.First && i <= r.Last {
			return true
		}
	}
	return false
}

// String formats the link: exact topics first, then dn, xl, tr, ws, x.pe,
// so and the other parameters sorted.
func (m *Magnet) String() string {
	var b strings.Builder
	b.WriteString("magnet:?")
	sep := ""
	add := func(k, v string) {
		b.WriteString(sep)
		sep = "&"
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v)
	}
	if !m.InfoHash.IsZero() {
		add("xt", btihPrefix+m.InfoHash.String())
	}
	if !m.InfoHashV2.IsZero() {
		add("xt", btmhPrefix+sha256Multihash+hex.EncodeToString(m.InfoHashV2[:]))
	}
	if m.Name != "" {
		add("dn", url.QueryEscape(m.Name))
	}
	if m.Length > 0 {
		add("xl", strconv.FormatInt(m.Length, 10))
	}
	for _, v := range m.Trackers {
		add("tr", url.QueryEscape(v))
	}
	for _, v := range m.WebSeeds {
		add("ws", url.QueryEscape(v))
	}
	for _, v := range m.Peers {
		add("x.pe", url.QueryEscape(v))
	}
	if len(m.Select) > 0 {
		parts := make([]string, len(m.Select))
		for i, r := range m.Select {
			parts[i] = strconv.Itoa(r.First)
			if r.Last != r.First {
				parts[i] += "-" + strconv.Itoa(r.Last)
			}
		}
		add("so", strings.Join(parts, ","))
	}
	keys := make([]string, 0, len(m.Params))
	for k := range m.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range m.Params[k] {
			add(url.QueryEscape(k), url.QueryEscape(v))
		}
	}
	return b.String()
}
