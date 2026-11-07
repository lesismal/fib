package tracker

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bufferpool"
)

// The defaults of ServerConfig.
const (
	DefaultInterval   = 30 * time.Minute
	DefaultNumWant    = 50
	DefaultMaxNumWant = 200
	DefaultMaxScrape  = 64
)

// ServerConfig configures a Server.
type ServerConfig struct {
	// Store keeps the swarms; a MemoryStore with the default bounds when
	// nil.
	Store Store
	// Interval is the announce interval given to clients, DefaultInterval
	// when zero. MinInterval, when set, is given as the "min interval" of
	// HTTP answers.
	Interval, MinInterval time.Duration
	// DefaultNumWant is how many peers an announce gets when it does not
	// say, and MaxNumWant how many it gets at most: DefaultNumWant and
	// DefaultMaxNumWant when zero. A UDP answer is also kept within 1400
	// bytes, which is 230 IPv4 peers or 76 IPv6 ones.
	DefaultNumWant, MaxNumWant int
	// MaxScrape bounds the torrents of an HTTP scrape, DefaultMaxScrape
	// when zero; a UDP scrape is bounded to 74 by its packet. A scrape
	// that names none, a full scrape, gets nothing.
	MaxScrape int
	// AllowInfoHash, when set, makes the tracker private: it tracks only
	// the torrents it allows, and refuses announces of others with
	// "unregistered torrent"; scrapes of them get nothing.
	AllowInfoHash func(metainfo.InfoHash) bool
	// Authorize, when set, is asked about every announce and scrape before
	// it is served; an error refuses it, with the error's message as the
	// reason. It is where passkeys are checked (see Auth).
	Authorize func(a *Auth) error
	// TrustIPParam takes the addresses an announce gives, its ip, ipv4 and
	// ipv6 parameters (BEP 7) or the IP field of a UDP announce, for the
	// peer's addresses of their families. By default a peer is recorded at
	// the address its request came from alone, since otherwise anyone may
	// have the tracker hand out an address of anyone else's, for every
	// peer of a swarm to connect to; turn it on for a tracker serving a
	// network whose clients it trusts, or behind a proxy.
	TrustIPParam bool
	// Secret keys the connection IDs a UDP tracker hands out; trackers
	// that share an address, behind a load balancer, must share it. A
	// random one when nil.
	Secret []byte
}

// Auth is an announce or a scrape, as ServerConfig.Authorize is asked
// about it.
type Auth struct {
	// Remote is the address the request came from, and UDP says it came
	// over UDP.
	Remote netip.AddrPort
	UDP    bool
	// Scrape says it is a scrape, of InfoHashes; an announce has one.
	Scrape     bool
	InfoHashes []metainfo.InfoHash
	PeerID     PeerID
	// URL is the request's path and query: the target of an HTTP request,
	// or the URL data of a UDP one (BEP 41), which clients send from the
	// tracker URL they were given. A UDP scrape has none: BEP 41 gives
	// only announces URL data.
	URL string
	// Passkey is the URL's passkey parameter, or else the path segment
	// before its last "announce" or "scrape" one, as in
	// "/0123abcd/announce": where private trackers put their users'
	// passkeys.
	Passkey string
}

// Server is a BitTorrent tracker: over HTTP (BEP 3, BEP 23, BEP 7, BEP 48),
// as a handler of package fib/http (see ServeHTTP), and over UDP (BEP 15,
// BEP 41), as the fib.Handler of a UDP binding (see UDP), on one Store.
//
//	srv := tracker.NewServer(tracker.ServerConfig{})
//	engine, _ := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":6969", Handler: fibhttp.NewHandler(srv)},
//		fib.Binding{Network: "udp", Addr: ":6969", Handler: srv.UDP()})
//
// An HTTP request is an announce when the last segment of its path begins
// with "announce", and a scrape when it begins with "scrape", so that a
// passkey may come before it.
type Server struct {
	cfg   ServerConfig
	store Store
	macs  sync.Pool
	udp   *UDPHandler
}

// NewServer returns a Server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Store == nil {
		cfg.Store = NewMemoryStore(MemoryStoreConfig{})
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.DefaultNumWant <= 0 {
		cfg.DefaultNumWant = DefaultNumWant
	}
	if cfg.MaxNumWant <= 0 {
		cfg.MaxNumWant = DefaultMaxNumWant
	}
	if cfg.MaxScrape <= 0 {
		cfg.MaxScrape = DefaultMaxScrape
	}
	if cfg.Secret == nil {
		cfg.Secret = make([]byte, 32)
		_, _ = rand.Read(cfg.Secret)
	}
	s := &Server{cfg: cfg, store: cfg.Store}
	secret := cfg.Secret
	s.macs.New = func() any { return hmac.New(sha256.New, secret) }
	s.udp = &UDPHandler{s: s}
	return s
}

// Store returns the server's store.
func (s *Server) Store() Store { return s.store }

// UDP returns the fib.Handler that serves the tracker on UDP bindings.
func (s *Server) UDP() *UDPHandler { return s.udp }

// passkeyOf finds the passkey of a request's path and query; see
// Auth.Passkey.
func passkeyOf(path, rawQuery string) string {
	if q, err := url.ParseQuery(rawQuery); err == nil {
		if k := q.Get("passkey"); k != "" {
			return k
		}
	}
	path = strings.TrimSuffix(path, "/")
	i := strings.LastIndexByte(path, '/')
	if i < 0 {
		return ""
	}
	if last := path[i+1:]; !strings.HasPrefix(last, "announce") && !strings.HasPrefix(last, "scrape") {
		return ""
	}
	path = path[:i]
	return path[strings.LastIndexByte(path, '/')+1:]
}

// announceIn is an announce over either protocol, as the Server serves it.
type announceIn struct {
	req *AnnounceRequest
	// numWant is the request's, -1 for the default.
	numWant int
	remote  netip.AddrPort
	// ip, ipv4 and ipv6 are the addresses the request gives.
	ip, ipv4, ipv6 netip.AddrPort
	udp            bool
	path, query    string
}

func (s *Server) authorize(a *Auth, path, query string) error {
	if s.cfg.Authorize == nil {
		return nil
	}
	a.URL = path
	if query != "" {
		a.URL += "?" + query
	}
	a.Passkey = passkeyOf(path, query)
	return s.cfg.Authorize(a)
}

// announce serves an announce; an *Error refuses it.
func (s *Server) announce(in *announceIn) (*AnnounceResponse, error) {
	r := in.req
	if s.cfg.AllowInfoHash != nil && !s.cfg.AllowInfoHash(r.InfoHash) {
		return nil, &Error{Reason: "unregistered torrent"}
	}
	if err := s.authorize(&Auth{
		Remote: in.remote, UDP: in.udp, InfoHashes: []metainfo.InfoHash{r.InfoHash}, PeerID: r.PeerID,
	}, in.path, in.query); err != nil {
		return nil, &Error{Reason: err.Error()}
	}
	src := in.remote.Addr().Unmap()
	a := StoreAnnounce{
		InfoHash: r.InfoHash, PeerID: r.PeerID, Event: r.Event,
		Seeder: r.Left == 0,
		WantV4: src.Is4(), WantV6: !src.Is4(),
	}
	if r.Port != 0 {
		own := netip.AddrPortFrom(src, r.Port)
		if src.Is4() {
			a.V4 = own
		} else {
			a.V6 = own
		}
		if s.cfg.TrustIPParam {
			for _, ap := range [...]netip.AddrPort{in.ip, in.ipv4, in.ipv6} {
				switch {
				case !ap.IsValid() || ap.Port() == 0 || ap.Addr().IsUnspecified():
				case ap.Addr().Is4():
					a.V4 = ap
				default:
					a.V6 = ap
				}
			}
		}
	}
	if !in.udp {
		// Over HTTP a peer gets peers of each family it has an address
		// of; over UDP the answer's format is the request's family.
		a.WantV4 = a.WantV4 || a.V4.IsValid() || in.ipv4.IsValid()
		a.WantV6 = a.WantV6 || a.V6.IsValid() || in.ipv6.IsValid()
	}
	a.NumWant = in.numWant
	if a.NumWant < 0 {
		a.NumWant = s.cfg.DefaultNumWant
	}
	a.NumWant = min(a.NumWant, s.cfg.MaxNumWant)
	if in.udp {
		size := compactV4
		if !src.Is4() {
			size = compactV6
		}
		a.NumWant = min(a.NumWant, (maxUDPReply-udpAnnounceRespLen)/size)
	}
	res, err := s.store.Announce(a)
	if err != nil {
		return nil, &Error{Reason: err.Error()}
	}
	resp := &AnnounceResponse{
		Interval:    s.cfg.Interval,
		MinInterval: s.cfg.MinInterval,
		Complete:    res.Complete,
		Incomplete:  res.Incomplete,
		Peers:       res.Peers,
	}
	if !in.udp {
		resp.ExternalIP = src
	}
	return resp, nil
}

// scrape serves a scrape: the state of each torrent the tracker tracks and
// allows of hashes.
func (s *Server) scrape(remote netip.AddrPort, udp bool, path, query string, hashes []metainfo.InfoHash) (map[metainfo.InfoHash]Stats, error) {
	if err := s.authorize(&Auth{Remote: remote, UDP: udp, Scrape: true, InfoHashes: hashes}, path, query); err != nil {
		return nil, &Error{Reason: err.Error()}
	}
	files := make(map[metainfo.InfoHash]Stats, len(hashes))
	for _, h := range hashes {
		if s.cfg.AllowInfoHash != nil && !s.cfg.AllowInfoHash(h) {
			continue
		}
		if st, ok := s.store.Scrape(h); ok {
			files[h] = st
		}
	}
	return files, nil
}

// maxUDPReply bounds a UDP answer, to stay clear of fragmentation on the
// paths of IPv6, whose minimum MTU is 1280, and of tunnels.
const maxUDPReply = 1400

// connectionID returns the connection ID of a UDP client at addr in the
// minute epoch: an HMAC of both under the server's secret, so that the
// server keeps no state for a client to have shown it can receive at the
// address it claims, as BEP 15 suggests.
func (s *Server) connectionID(addr netip.AddrPort, epoch int64) uint64 {
	var b [8 + 16 + 2]byte
	binary.BigEndian.PutUint64(b[:], uint64(epoch))
	a16 := addr.Addr().Unmap().As16()
	copy(b[8:], a16[:])
	binary.BigEndian.PutUint16(b[24:], addr.Port())
	m := s.macs.Get().(hash.Hash)
	m.Reset()
	m.Write(b[:])
	var sum [sha256.Size]byte
	id := binary.BigEndian.Uint64(m.Sum(sum[:0]))
	s.macs.Put(m)
	return id
}

// validConnectionID reports whether id is one the server gave addr within
// the last one to two minutes: of this minute or the one before. A client
// uses an ID for one minute after it gets it.
func (s *Server) validConnectionID(addr netip.AddrPort, id uint64, now time.Time) bool {
	epoch := now.Unix() / 60
	// Comparing two words takes the same time whichever bits differ.
	return id == s.connectionID(addr, epoch) || id == s.connectionID(addr, epoch-1)
}

// serveUDP answers the UDP request b from addr, appending the answer to
// dst; it returns dst as it was for a request that gets none: one it
// cannot read, or that comes with a connection ID that is not the
// client's, which it drops rather than answer an address that may be
// forged.
func (s *Server) serveUDP(dst []byte, from netip.AddrPort, b []byte, now time.Time) []byte {
	r, err := parseUDPRequest(b)
	if err != nil {
		return dst
	}
	if r.action == actionConnect {
		if r.conn != protocolID {
			return dst
		}
		dst = appendUDPReplyHeader(dst, actionConnect, r.tx)
		return binary.BigEndian.AppendUint64(dst, s.connectionID(from, now.Unix()/60))
	}
	if !s.validConnectionID(from, r.conn, now) {
		return dst
	}
	path, query, _ := strings.Cut(r.urlData, "?")
	switch r.action {
	case actionAnnounce:
		in := &announceIn{req: &r.announce, numWant: r.numWant, remote: from, udp: true, path: path, query: query}
		if r.announce.IP.IsValid() {
			in.ip = netip.AddrPortFrom(r.announce.IP, r.announce.Port)
		}
		resp, err := s.announce(in)
		if err != nil {
			return appendUDPError(dst, r.tx, err.(*Error).Reason)
		}
		dst = appendUDPReplyHeader(dst, actionAnnounce, r.tx)
		dst = appendInt32(dst, int(resp.Interval/time.Second))
		dst = appendInt32(dst, resp.Incomplete)
		dst = appendInt32(dst, resp.Complete)
		for _, p := range resp.Peers {
			dst = appendCompact(dst, p.Addr)
		}
		return dst
	case actionScrape:
		files, err := s.scrape(from, true, path, query, r.hashes)
		if err != nil {
			return appendUDPError(dst, r.tx, err.(*Error).Reason)
		}
		dst = appendUDPReplyHeader(dst, actionScrape, r.tx)
		for _, h := range r.hashes {
			st := files[h]
			dst = appendInt32(dst, st.Complete)
			dst = appendInt32(dst, st.Downloaded)
			dst = appendInt32(dst, st.Incomplete)
		}
		return dst
	}
	return dst
}

// UDPHandler is the fib.Handler of a Server's UDP bindings (BEP 15). It
// keeps no state of its clients beyond the connection fib keeps for each
// address, which its idle timeout (fib.Config.UDPIdleTimeout) closes.
type UDPHandler struct{ s *Server }

// OnOpen is part of fib.Handler.
func (h *UDPHandler) OnOpen(*fib.Connection) {}

// OnPriorityData is part of fib.Handler.
func (h *UDPHandler) OnPriorityData(*fib.Connection, []byte) {}

// OnClose is part of fib.Handler.
func (h *UDPHandler) OnClose(*fib.Connection, error) {}

// OnData answers a datagram.
func (h *UDPHandler) OnData(c *fib.Connection, b []byte) {
	h.OnDatagrams(c, [][]byte{b})
}

// OnDatagrams answers a client's datagrams.
func (h *UDPHandler) OnDatagrams(c *fib.Connection, dgrams [][]byte) {
	from := c.RemoteAddrPort()
	now := time.Now()
	for _, d := range dgrams {
		// An address with a zone has no AddrPort, and nothing to key an ID by.
		if !from.IsValid() {
			continue
		}
		if out := h.s.serveUDP(nil, from, d, now); len(out) > 0 {
			_, _ = c.Write(out)
		}
		bufferpool.Put(d)
	}
}

// httpAnnounce answers an HTTP announce from remote, of the request target
// path?query, with the bencoded body of a 200 response.
func (s *Server) httpAnnounce(remote netip.AddrPort, path, query string) []byte {
	a, err := parseAnnounceQuery(query)
	if err != nil {
		return encodeFailure(err.Error())
	}
	resp, err := s.announce(&announceIn{
		req: &a.AnnounceRequest, numWant: a.numWant, remote: remote,
		ip: a.ip, ipv4: a.ipv4, ipv6: a.ipv6, path: path, query: query,
	})
	if err != nil {
		return encodeFailure(err.(*Error).Reason)
	}
	return encodeAnnounce(resp, a.compact, a.noPeerID)
}

// httpScrape answers an HTTP scrape, as httpAnnounce does an announce.
func (s *Server) httpScrape(remote netip.AddrPort, path, query string) []byte {
	hashes, err := parseScrapeQuery(query, s.cfg.MaxScrape)
	if err != nil {
		return encodeFailure(err.Error())
	}
	files, err := s.scrape(remote, false, path, query, hashes)
	if err != nil {
		return encodeFailure(err.(*Error).Reason)
	}
	return encodeScrape(files)
}
