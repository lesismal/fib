package metainfo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// MaxFileSize is the largest .torrent file Load reads.
const MaxFileSize = 128 << 20

// MetaInfo is a .torrent file. Encode it with bencode.Marshal.
type MetaInfo struct {
	// Announce is the tracker of BEP 3; AnnounceList the tiers of
	// trackers of BEP 12, which replace it when present.
	Announce     string     `bencode:"announce,omitempty"`
	AnnounceList [][]string `bencode:"announce-list,omitempty"`
	Comment      string     `bencode:"comment,omitempty"`
	CreatedBy    string     `bencode:"created by,omitempty"`
	// CreationDate is in seconds since the Unix epoch.
	CreationDate int64  `bencode:"creation date,omitempty"`
	Encoding     string `bencode:"encoding,omitempty"`
	// URLList is the web seeds of BEP 19.
	URLList URLList `bencode:"url-list,omitempty"`
	// Nodes is the DHT nodes of BEP 5 for a torrent without trackers.
	Nodes []Node `bencode:"nodes,omitempty"`
	// InfoBytes is the info dictionary as encoded, over which the info
	// hashes are taken; Info is it parsed.
	InfoBytes bencode.RawMessage `bencode:"info"`
	// PieceLayers is a v2 torrent's piece layers.
	PieceLayers PieceLayers `bencode:"piece layers,omitempty"`

	Info *Info `bencode:"-"`
}

// Parse parses and checks a .torrent file. Keys outside the info
// dictionary that have values of the wrong type are ignored, as are keys
// it does not know; the info dictionary must be valid (see ParseInfo), and
// each piece layer present must match its file, though layers may be
// missing (see Info.ValidatePieceLayers).
func Parse(data []byte) (*MetaInfo, error) {
	// Some .torrent files end with a newline.
	data = bytes.TrimRight(data, " \t\r\n")
	var mi MetaInfo
	if err := bencode.Unmarshal(data, &mi); err != nil {
		var te *bencode.UnmarshalTypeError
		if !errors.As(err, &te) {
			return nil, fmt.Errorf("metainfo: %w", err)
		}
	}
	if len(mi.InfoBytes) == 0 {
		return nil, errors.New("metainfo: no info dictionary")
	}
	info, err := ParseInfo(mi.InfoBytes)
	if err != nil {
		return nil, err
	}
	mi.Info = info
	mi.InfoBytes = info.raw
	if _, err := info.ValidatePieceLayers(mi.PieceLayers); err != nil {
		return nil, err
	}
	return &mi, nil
}

// Load reads and parses a .torrent file of at most MaxFileSize bytes.
func Load(path string) (*MetaInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, MaxFileSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxFileSize {
		return nil, fmt.Errorf("metainfo: %s is larger than %d bytes", path, MaxFileSize)
	}
	return Parse(data)
}

// SetInfo encodes info and sets it as the torrent's info dictionary,
// parsed again so that its hashes and layout are those of the bytes.
func (mi *MetaInfo) SetInfo(info *Info) error {
	raw, err := info.encode()
	if err != nil {
		return err
	}
	parsed, err := ParseInfo(raw)
	if err != nil {
		return err
	}
	mi.Info, mi.InfoBytes = parsed, parsed.raw
	return nil
}

// Trackers returns the tiers of trackers: AnnounceList, or Announce alone
// when there is no list.
func (mi *MetaInfo) Trackers() [][]string {
	var tiers [][]string
	for _, tier := range mi.AnnounceList {
		var t []string
		for _, u := range tier {
			if u != "" {
				t = append(t, u)
			}
		}
		if len(t) > 0 {
			tiers = append(tiers, t)
		}
	}
	if len(tiers) == 0 && mi.Announce != "" {
		tiers = [][]string{{mi.Announce}}
	}
	return tiers
}

// Magnet returns a magnet link to the torrent: its hashes, name, trackers
// and web seeds.
func (mi *MetaInfo) Magnet() *Magnet {
	m := &Magnet{Name: mi.Info.Name, WebSeeds: append([]string(nil), mi.URLList...)}
	if mi.Info.HasV1() {
		m.InfoHash = mi.Info.InfoHashV1()
	}
	if mi.Info.HasV2() {
		m.InfoHashV2 = mi.Info.InfoHashV2()
	}
	seen := map[string]bool{}
	for _, tier := range mi.Trackers() {
		for _, u := range tier {
			if !seen[u] {
				seen[u] = true
				m.Trackers = append(m.Trackers, u)
			}
		}
	}
	return m
}

// URLList is the "url-list" of BEP 19, which is a list of URLs or a single
// URL string.
type URLList []string

// UnmarshalBencode accepts a string or a list of strings, and ignores
// anything else.
func (u *URLList) UnmarshalBencode(data []byte) error {
	var v any
	if err := bencode.Unmarshal(data, &v); err != nil {
		return nil
	}
	*u = nil
	switch v := v.(type) {
	case string:
		if v != "" {
			*u = URLList{v}
		}
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok && s != "" {
				*u = append(*u, s)
			}
		}
	}
	return nil
}

// Node is a DHT node of a torrent's "nodes" list, encoded as a list of
// its host and port.
type Node struct {
	Host string
	Port int
}

// String returns the node as host:port.
func (n Node) String() string {
	return net.JoinHostPort(n.Host, strconv.Itoa(n.Port))
}

// MarshalBencode encodes the node as [host, port].
func (n Node) MarshalBencode() ([]byte, error) {
	return bencode.Marshal([]any{n.Host, n.Port})
}

// UnmarshalBencode decodes [host, port], leaving n zero for anything
// else.
func (n *Node) UnmarshalBencode(data []byte) error {
	var v []any
	*n = Node{}
	if bencode.Unmarshal(data, &v) != nil || len(v) != 2 {
		return nil
	}
	host, ok1 := v[0].(string)
	port, ok2 := v[1].(int64)
	if ok1 && ok2 && port > 0 && port < 65536 {
		*n = Node{Host: host, Port: int(port)}
	}
	return nil
}
