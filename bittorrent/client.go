package bittorrent

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/dht"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
	"github.com/lesismal/fib/bittorrent/peer/mse"
	"github.com/lesismal/fib/bittorrent/tracker"
	"github.com/lesismal/fib/bittorrent/utp"
	fibhttp "github.com/lesismal/fib/http"
)

type dhtNode = dht.Node

// Encryption is how a client uses Message Stream Encryption (MSE/PE).
type Encryption uint8

// The encryption policies.
const (
	// EncryptionPrefer dials with MSE, selecting RC4, and dials again in
	// plaintext when the peer refuses; it accepts both. Most clients do
	// this by default.
	EncryptionPrefer Encryption = iota
	// EncryptionAllow dials in plaintext, and again with MSE when the
	// peer refuses; it accepts both, and selects plaintext where the
	// initiator offers it: the cheapest.
	EncryptionAllow
	// EncryptionRequire dials and accepts only MSE with RC4.
	EncryptionRequire
)

func (e Encryption) policy() mse.Policy {
	switch e {
	case EncryptionAllow:
		return mse.Allow
	case EncryptionRequire:
		return mse.Require
	}
	return mse.Prefer
}

// Defaults of a Config.
const (
	DefaultListenAddr         = ":6881"
	DefaultMaxConns           = 200
	DefaultMaxConnsPerTorrent = 50
	DefaultMaxHalfOpen        = 32
	DefaultDialsPerTick       = 8
	DefaultUploadSlots        = 4
	DefaultUTPDialTimeout     = 5 * time.Second
	DefaultDHTRetryInterval   = time.Minute
	DefaultLSDInterval        = 5 * time.Minute
)

// Config configures a Client. Its zero value is a working client that
// stores in the working directory, listens on DefaultListenAddr, and
// speaks TCP, uTP, MSE, the DHT, PEX, LSD and trackers.
type Config struct {
	// DataDir is where the default storage keeps the torrents' files; the
	// working directory when empty.
	DataDir string
	// Storage opens each torrent's storage, in place of files below
	// DataDir (NewFileStorage); NewMemoryStorage keeps them in memory.
	Storage StorageOpener
	// Preallocate gives the default storage's files their full size when
	// first written (see FileStorageOptions).
	Preallocate bool
	// ListenAddr is the "host:port" the client accepts peers on, the same
	// port for TCP and UDP, which uTP, the DHT and UDP trackers share; a
	// zero port picks one that is free for both. DefaultListenAddr when
	// empty.
	ListenAddr string
	// PeerIDPrefix begins the client's peer ID; peer.DefaultPeerIDPrefix
	// when empty.
	PeerIDPrefix string
	// Encryption is the MSE policy, EncryptionPrefer by default.
	Encryption Encryption
	// MaxConns bounds the client's peer connections, MaxConnsPerTorrent
	// a torrent's, MaxHalfOpen the dials in flight, and DialsPerTick the
	// new dials a torrent starts each second.
	MaxConns, MaxConnsPerTorrent, MaxHalfOpen, DialsPerTick int
	// UploadRate and DownloadRate bound the payload bytes per second of
	// all torrents together; zero is unlimited.
	UploadRate, DownloadRate int
	// UploadSlots is how many peers a torrent uploads to at once, beside
	// the optimistic unchoke; DefaultUploadSlots when zero.
	UploadSlots int
	// The Disable flags turn off what they name: TCP or uTP (one must
	// stay), the DHT (BEP 5), peer exchange (BEP 11), local service
	// discovery (BEP 14), and trackers.
	DisableTCP, DisableUTP, DisableDHT, DisablePEX, DisableLSD, DisableTrackers bool
	// SeedRatio pauses a torrent once it has uploaded this many times its
	// size; SeedTime once it has seeded this long. Zero is unlimited.
	SeedRatio float64
	SeedTime  time.Duration
	// DHT configures the DHT node: Routers nil bootstraps from the public
	// routers, an empty slice from none (with the nodes of torrents and
	// of peers' PORT messages only). Send is the client's.
	DHT dht.Config
	// DHTIPv6 runs a second DHT node for IPv6 (BEP 32) on the same port.
	DHTIPv6 bool
	// PeerSources are more sources of peers, beside trackers, the DHT and
	// LSD, for torrents that are not private.
	PeerSources []PeerSource
	// OnEvent, when set, hears the client's events in order, on a
	// goroutine of its own: it may call the client, and should be quick.
	OnEvent func(Event)
	// MinAnnounceInterval is the least time between a torrent's announces
	// to its trackers, whatever their interval; one second when zero.
	MinAnnounceInterval time.Duration
	// UTPDialTimeout bounds a uTP dial before TCP is tried; three times it
	// when TCP is disabled. DefaultUTPDialTimeout when zero.
	UTPDialTimeout time.Duration
	// DHTRetryInterval is how often a torrent without peers looks itself
	// up in the DHT; DefaultDHTRetryInterval when zero.
	DHTRetryInterval time.Duration
	// LSDInterval is how often a torrent is announced on the local
	// network; DefaultLSDInterval when zero.
	LSDInterval time.Duration
	// Engine configures the client's fib engine; fib.DefaultConfig when
	// nil.
	Engine *fib.Config
}

func (cfg *Config) setDefaults() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = DefaultListenAddr
	}
	if cfg.Storage == nil {
		dir := cfg.DataDir
		if dir == "" {
			dir = "."
		}
		cfg.Storage = NewFileStorage(dir, FileStorageOptions{Preallocate: cfg.Preallocate})
	}
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&cfg.MaxConns, DefaultMaxConns)
	def(&cfg.MaxConnsPerTorrent, DefaultMaxConnsPerTorrent)
	def(&cfg.MaxHalfOpen, DefaultMaxHalfOpen)
	def(&cfg.DialsPerTick, DefaultDialsPerTick)
	def(&cfg.UploadSlots, DefaultUploadSlots)
	if cfg.MinAnnounceInterval <= 0 {
		cfg.MinAnnounceInterval = time.Second
	}
	if cfg.UTPDialTimeout <= 0 {
		cfg.UTPDialTimeout = DefaultUTPDialTimeout
	}
	if cfg.DHTRetryInterval <= 0 {
		cfg.DHTRetryInterval = DefaultDHTRetryInterval
	}
	if cfg.LSDInterval <= 0 {
		cfg.LSDInterval = DefaultLSDInterval
	}
}

// Client is a BitTorrent client: one fib engine, one port for TCP and
// UDP, and the torrents it downloads and seeds.
type Client struct {
	cfg    Config
	peerID [20]byte
	key    uint32
	port   int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	engine     *fib.Engine
	engineDone chan error
	server     *peer.Server
	utp        *utp.Socket
	udpTracker *tracker.UDPClient
	httpClient *fibhttp.Client
	trackers   *tracker.Client
	dht4, dht6 *dht.Node
	dhtReady   chan struct{}
	lsd        *lsd
	ownAddrs   map[netip.Addr]bool

	up, down limiter

	conns    atomic.Int64
	halfOpen atomic.Int64
	closing  atomic.Bool

	mu       sync.Mutex
	torrents map[[20]byte]*Torrent

	banMu sync.Mutex
	bans  map[netip.AddrPort]time.Time

	evMu     sync.Mutex
	evCond   sync.Cond
	events   []Event
	evClosed bool
	evDone   chan struct{}
}

// ErrTorrentExists is what adding a torrent the client has already gets.
var ErrTorrentExists = errors.New("bittorrent: torrent already added")

// NewClient starts a client.
func NewClient(cfg Config) (*Client, error) {
	cfg.setDefaults()
	if cfg.DisableTCP && cfg.DisableUTP {
		return nil, errors.New("bittorrent: TCP and uTP both disabled")
	}
	c := &Client{
		cfg: cfg, peerID: peer.NewPeerID(cfg.PeerIDPrefix), key: rand.Uint32(),
		torrents: map[[20]byte]*Torrent{}, bans: map[netip.AddrPort]time.Time{},
		dhtReady: make(chan struct{}), evDone: make(chan struct{}),
		ownAddrs: map[netip.Addr]bool{},
	}
	c.evCond.L = &c.evMu
	c.up.rate = int64(cfg.UploadRate)
	c.up.tokens = c.up.rate
	c.down.rate = int64(cfg.DownloadRate)
	c.down.tokens = c.down.rate
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.server = peer.NewServer(peer.Config{
		PeerID:     c.peerID,
		DHT:        !cfg.DisableDHT,
		Encryption: cfg.Encryption.policy(),
	})
	if err := c.listen(); err != nil {
		c.shutdown()
		return nil, err
	}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			if p, err := netip.ParsePrefix(a.String()); err == nil {
				c.ownAddrs[p.Addr().Unmap()] = true
			}
		}
	}
	if !cfg.DisableDHT && c.utp != nil {
		if err := c.startDHT(); err != nil {
			c.shutdown()
			return nil, err
		}
	} else {
		close(c.dhtReady)
	}
	if !cfg.DisableLSD {
		// Without multicast (a container, a host without a route for
		// it) the client goes on without local discovery.
		if l, err := startLSD(c); err == nil {
			c.lsd = l
		}
	}
	go c.dispatch()
	c.wg.Add(1)
	go c.tickLoop()
	return c, nil
}

// listen starts the engine and binds the port: TCP for peer.Server, and
// UDP for uTP, the DHT and UDP trackers, sorted by first byte. A zero port
// takes the one the system gives TCP, and tries again if UDP has it taken.
func (c *Client) listen() error {
	ecfg := fib.DefaultConfig()
	if c.cfg.Engine != nil {
		ecfg = *c.cfg.Engine
	}
	if ecfg.Name == "" {
		ecfg.Name = "bittorrent"
	}
	ecfg.LogSettings = false
	engine, err := fib.New(ecfg)
	if err != nil {
		return err
	}
	c.engine = engine
	c.engineDone = make(chan error, 1)
	go func() { c.engineDone <- engine.Run() }()

	host, portStr, err := net.SplitHostPort(c.cfg.ListenAddr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("bittorrent: bad listen port %q", portStr)
	}
	needUDP := !c.cfg.DisableUTP || !c.cfg.DisableDHT || !c.cfg.DisableTrackers
	tcpNet, udpNet := "tcp", "udp"
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Unmap().Is4() {
			tcpNet, udpNet = "tcp4", "udp4"
		} else {
			tcpNet, udpNet = "tcp6", "udp6"
		}
	}
	for try := 0; ; try++ {
		p := port
		tcpIndex := -1
		if !c.cfg.DisableTCP {
			i, err := engine.AddBinding(fib.Binding{Network: tcpNet, Addr: net.JoinHostPort(host, strconv.Itoa(port)), Handler: c.server})
			if err != nil {
				return err
			}
			tcpIndex = i
			addrs, err := engine.BindingAddrs(i)
			if err != nil || len(addrs) == 0 {
				return fmt.Errorf("bittorrent: no TCP address: %v", err)
			}
			p = addrs[0].(*net.TCPAddr).Port
		}
		if !needUDP {
			c.port = p
			return nil
		}
		ucfg := utp.Config{}
		if c.cfg.DisableUTP {
			ucfg.AcceptBacklog = -1
		}
		s, err := utp.Listen(engine, udpNet, net.JoinHostPort(host, strconv.Itoa(p)), ucfg)
		if err != nil {
			if port == 0 && tcpIndex >= 0 && try < 8 {
				_ = engine.RemoveBinding(tcpIndex)
				continue
			}
			return err
		}
		if p == 0 {
			p = s.Addr().(*net.UDPAddr).Port
		}
		c.port = p
		c.utp = s
		break
	}
	c.utp.OnOther(c.onUDP)
	c.udpTracker = tracker.NewUDPClient(tracker.UDPClientConfig{
		Send: func(to netip.AddrPort, b []byte) error { return c.utp.WriteTo(b, to) },
	})
	hcfg := fibhttp.DefaultClientConfig()
	hcfg.Timeout = time.Minute
	c.httpClient = fibhttp.NewClient(engine, hcfg)
	c.trackers = &tracker.Client{HTTP: tracker.NewHTTPClient(c.httpClient), UDP: c.udpTracker}
	if !c.cfg.DisableUTP {
		go func() { _ = c.server.Serve(c.utp) }()
	}
	return nil
}

// onUDP takes the datagrams of the shared port that are not uTP: a
// bencoded dictionary, beginning with 'd', is the DHT's, and a UDP
// tracker's answer begins with its action, a zero byte.
func (c *Client) onUDP(from netip.AddrPort, b []byte) {
	from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
	if len(b) > 0 && b[0] == 'd' {
		n := c.dht4
		if !from.Addr().Is4() {
			n = c.dht6
		}
		if n != nil {
			n.HandleDatagram(from, b)
		}
		return
	}
	if c.udpTracker != nil && tracker.IsUDPReply(b) {
		c.udpTracker.HandleDatagram(from, b)
	}
}

func (c *Client) startDHT() error {
	send := func(to netip.AddrPort, b []byte) { _ = c.utp.WriteTo(b, to) }
	cfg := c.cfg.DHT
	cfg.Send = send
	cfg.IPv6 = false
	n, err := dht.New(cfg)
	if err != nil {
		return err
	}
	c.dht4 = n
	if c.cfg.DHTIPv6 {
		cfg6 := c.cfg.DHT
		cfg6.Send = send
		cfg6.IPv6 = true
		if n6, err := dht.New(cfg6); err == nil {
			c.dht6 = n6
		}
	}
	go func() {
		defer close(c.dhtReady)
		ctx, cancel := context.WithTimeout(c.ctx, time.Minute)
		defer cancel()
		var wg sync.WaitGroup
		for _, n := range []*dht.Node{c.dht4, c.dht6} {
			if n != nil {
				wg.Add(1)
				go func() { defer wg.Done(); _ = n.Bootstrap(ctx) }()
			}
		}
		wg.Wait()
	}()
	return nil
}

// addDHTNode adds a node a peer's PORT message names (BEP 5).
func (c *Client) addDHTNode(a netip.AddrPort) {
	if a.Port() == 0 {
		return
	}
	n := c.dht4
	if !a.Addr().Is4() {
		n = c.dht6
	}
	if n != nil {
		n.AddNodes(dht.NodeInfo{Addr: a})
	}
}

// DHT returns the client's IPv4 DHT node, nil when disabled.
func (c *Client) DHT() *dht.Node { return c.dht4 }

// Port is the port the client accepts peers on, TCP and UDP.
func (c *Client) Port() int { return c.port }

// Addr is the address the client accepts peers on, with the listening
// address's host (unspecified when it had none).
func (c *Client) Addr() netip.AddrPort {
	host, _, _ := net.SplitHostPort(c.cfg.ListenAddr)
	ip, err := netip.ParseAddr(host)
	if err != nil {
		ip = netip.IPv4Unspecified()
	}
	return netip.AddrPortFrom(ip, uint16(c.port))
}

// PeerID is the client's peer ID.
func (c *Client) PeerID() [20]byte { return c.peerID }

// Engine is the client's fib engine.
func (c *Client) Engine() *fib.Engine { return c.engine }

func (c *Client) isOwnAddr(a netip.AddrPort) bool {
	if int(a.Port()) != c.port {
		return false
	}
	ip := a.Addr()
	return ip.IsLoopback() || ip.IsUnspecified() || c.ownAddrs[ip]
}

// AddTorrent adds the torrent of a .torrent file and starts it, unless
// opts says Paused. Its data is checked first: what storage already
// holds is kept.
func (c *Client) AddTorrent(mi *metainfo.MetaInfo, opts *AddOptions) (*Torrent, error) {
	if mi.Info == nil {
		return nil, errors.New("bittorrent: metainfo without an info dictionary")
	}
	var o AddOptions
	if opts != nil {
		o = *opts
	}
	var ih metainfo.InfoHash
	var ih2 metainfo.InfoHashV2
	if mi.Info.HasV1() {
		ih = mi.Info.InfoHashV1()
	}
	if mi.Info.HasV2() {
		ih2 = mi.Info.InfoHashV2()
	}
	t := newTorrent(c, ih, ih2, o)
	t.trackers = appendTrackers(mi.Trackers(), o.Trackers)
	t.webSeeds = mi.URLList
	t.nodes = mi.Nodes
	if len(mi.PieceLayers) > 0 {
		t.layers = metainfo.PieceLayers{}
		for k, v := range mi.PieceLayers {
			t.layers[k] = v
		}
	}
	if err := c.register(t); err != nil {
		return nil, err
	}
	if c.dht4 != nil && !mi.Info.Private {
		c.dht4.AddTorrentNodes(mi.Nodes)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.setInfoLocked(mi.Info); err != nil {
		return t, err
	}
	t.begin(o)
	return t, nil
}

// AddTorrentFile adds the torrent of the .torrent file at path.
func (c *Client) AddTorrentFile(path string, opts *AddOptions) (*Torrent, error) {
	mi, err := metainfo.Load(path)
	if err != nil {
		return nil, err
	}
	return c.AddTorrent(mi, opts)
}

// AddMagnet adds the torrent of a magnet link (BEP 9): its info
// dictionary comes from peers, found through the link's trackers and
// peers, the DHT and the rest, and so= (BEP 53) selects its files.
func (c *Client) AddMagnet(uri string, opts *AddOptions) (*Torrent, error) {
	m, err := metainfo.ParseMagnet(uri)
	if err != nil {
		return nil, err
	}
	var o AddOptions
	if opts != nil {
		o = *opts
	}
	t := newTorrent(c, m.InfoHash, m.InfoHashV2, o)
	t.name = m.Name
	if len(m.Trackers) > 0 {
		t.trackers = [][]string{m.Trackers}
	}
	t.trackers = appendTrackers(t.trackers, o.Trackers)
	t.webSeeds = m.WebSeeds
	t.selection = m.Select
	if err := c.register(t); err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.begin(o)
	var peers []netip.AddrPort
	for _, p := range m.Peers {
		if ap, err := resolveAddrPort(p); err == nil {
			peers = append(peers, ap)
		}
	}
	t.addPeersLocked(SourceManual, false, peers)
	return t, nil
}

func appendTrackers(list [][]string, more []string) [][]string {
	out := append([][]string(nil), list...)
	for _, u := range more {
		out = append(out, []string{u})
	}
	return out
}

// begin starts a torrent just added, or leaves it paused.
func (t *Torrent) begin(o AddOptions) {
	var peers []netip.AddrPort
	for _, p := range o.Peers {
		if ap, err := resolveAddrPort(p); err == nil {
			peers = append(peers, ap)
		}
	}
	t.addPeersLocked(SourceManual, false, peers)
	if o.Paused {
		t.paused = true
		t.state = StatePaused
		return
	}
	t.startRunningLocked()
}

func (c *Client) register(t *Torrent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing.Load() {
		return net.ErrClosed
	}
	if _, ok := c.torrents[t.key]; ok {
		return ErrTorrentExists
	}
	if err := c.server.AddTorrent(t.pt); err != nil {
		if errors.Is(err, peer.ErrTorrentExists) {
			return ErrTorrentExists
		}
		return err
	}
	c.torrents[t.key] = t
	return nil
}

// Torrent returns the torrent of info hash ih: v1, or v2 truncated.
func (c *Client) Torrent(ih [20]byte) (*Torrent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.torrents[ih]; ok {
		return t, true
	}
	for _, t := range c.torrents {
		if [20]byte(t.ih) == ih || (!t.ih2.IsZero() && [20]byte(t.ih2.Truncate()) == ih) {
			return t, true
		}
	}
	return nil, false
}

// Torrents returns the client's torrents.
func (c *Client) Torrents() []*Torrent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Torrent, 0, len(c.torrents))
	for _, t := range c.torrents {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i].key[:]) < string(out[j].key[:]) })
	return out
}

// Close closes every torrent, telling their trackers, and the client.
func (c *Client) Close() error {
	if c.closing.Swap(true) {
		return nil
	}
	for _, t := range c.Torrents() {
		t.mu.Lock()
		t.stopRunningLocked()
		for pc := range t.peers {
			go pc.Close()
		}
		if t.storage != nil {
			_ = t.storage.Close()
			t.storage = nil
		}
		if t.state != StateRemoved {
			t.state = StateRemoved
			close(t.removed)
		}
		t.mu.Unlock()
	}
	c.cancel()
	// Wait a little for the stopped announces.
	waited := make(chan struct{})
	go func() { c.wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(6 * time.Second):
	}
	c.shutdown()
	return nil
}

func (c *Client) shutdown() {
	c.cancel()
	if c.lsd != nil {
		c.lsd.close()
	}
	for _, n := range []*dht.Node{c.dht4, c.dht6} {
		if n != nil {
			_ = n.Close()
		}
	}
	if c.udpTracker != nil {
		_ = c.udpTracker.Close()
	}
	if c.httpClient != nil {
		c.httpClient.Close()
	}
	if c.utp != nil {
		_ = c.utp.Close()
	}
	if c.engine != nil {
		c.engine.Stop()
		<-c.engineDone
		_ = c.engine.Close()
	}
	c.evMu.Lock()
	c.evClosed = true
	c.evCond.Broadcast()
	c.evMu.Unlock()
}

// ban bans a peer by its ban address (see banKey) and closes its
// connections.
func (c *Client) ban(key netip.AddrPort, why error) {
	c.banMu.Lock()
	if _, ok := c.bans[key]; ok {
		c.banMu.Unlock()
		return
	}
	c.bans[key] = time.Now()
	c.banMu.Unlock()
	c.emit(Event{Type: EventPeerBanned, Peer: key, Err: why})
	go func() {
		for _, t := range c.Torrents() {
			t.mu.Lock()
			for conn, pc := range t.peers {
				if banAddr(pc) == key {
					go conn.Close()
				}
			}
			t.mu.Unlock()
		}
	}()
}

// banned reports a peer at addr, listening at listen, banned.
func (c *Client) banned(addr, listen netip.AddrPort) bool {
	c.banMu.Lock()
	defer c.banMu.Unlock()
	if len(c.bans) == 0 {
		return false
	}
	_, a := c.bans[banKey(addr, listen)]
	_, b := c.bans[netip.AddrPortFrom(addr.Addr(), 0)]
	_, d := c.bans[addr]
	return a || b || d
}

// Ban bans the peers at an IP address (port zero), or at an address and
// port, from every torrent.
func (c *Client) Ban(a netip.AddrPort) { c.ban(a, errors.New("banned")) }

// tickLoop drives the torrents: rate limits four times a second, and once
// a second each torrent's upkeep: rates, snubbing, choking, connecting,
// metadata, hashes and peer exchange.
func (c *Client) tickLoop() {
	defer c.wg.Done()
	const step = 250 * time.Millisecond
	tk := time.NewTicker(step)
	defer tk.Stop()
	last := time.Now()
	n := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		case now := <-tk.C:
			c.up.refill(now.Sub(last))
			c.down.refill(now.Sub(last))
			last = now
			n++
			second := n%4 == 0
			if !second && c.up.rate == 0 && c.down.rate == 0 {
				continue
			}
			for _, t := range c.Torrents() {
				if second {
					t.tick(now, 4*step)
				} else {
					t.tickLimits()
				}
			}
		}
	}
}

// tickLimits restarts what the rate limits held back.
func (t *Torrent) tickLimits() {
	t.mu.Lock()
	var pump []*peerConn
	for _, pc := range t.peers {
		if t.c.down.rate > 0 {
			t.fillRequestsLocked(pc)
		}
		if len(pc.upq) > 0 {
			pump = append(pump, pc)
		}
	}
	t.mu.Unlock()
	for _, pc := range pump {
		t.pump(pc)
	}
}

// earlyClose is a dialed connection that closed before its dialer
// recorded it.
type earlyClose struct {
	err error
	at  time.Time
}

func (t *Torrent) tick(now time.Time, dt time.Duration) {
	t.mu.Lock()
	t.downRate.tick(dt)
	t.upRate.tick(dt)
	var pump []*peerConn
	for _, pc := range t.peers {
		pc.down.tick(dt)
		pc.up.tick(dt)
		if len(pc.upq) > 0 {
			pump = append(pump, pc)
		}
	}
	if !t.running() {
		t.mu.Unlock()
		return
	}
	for c, e := range t.early {
		if now.Sub(e.at) > time.Minute {
			delete(t.early, c)
		}
	}
	for _, pc := range t.peers {
		if len(pc.pending) > 0 && now.Sub(pc.lastBlock) > snubTimeout {
			// Snubbed: its requests go to others.
			pc.snubbed = true
			for r := range pc.pending {
				t.releaseRequest(r)
				_ = pc.c.Cancel(r)
			}
			clear(pc.pending)
			pc.lastBlock = now
		}
		if t.info == nil {
			t.requestMetadataLocked(pc)
		} else {
			t.updateInterestLocked(pc)
			t.fillRequestsLocked(pc)
		}
		t.sendPexLocked(pc, now)
	}
	if t.info != nil {
		t.requestHashesLocked()
	}
	t.connectPeersLocked(now)
	if now.Sub(t.lastChoke) >= 10*time.Second {
		t.rechokeLocked(now)
	}
	t.checkSeedLimitsLocked(now)
	t.mu.Unlock()
	for _, pc := range pump {
		t.pump(pc)
	}
}

// rechokeLocked is the choking algorithm, every ten seconds: the
// interested peers this side downloads from fastest (uploads to fastest,
// when seeding) get the upload slots, tit for tat, and one more peer gets
// the optimistic unchoke, which moves every thirty seconds, so that new
// peers get a start and better ones are found; new peers are three times
// as likely to get it.
func (t *Torrent) rechokeLocked(now time.Time) {
	t.lastChoke = now
	seeding := t.info != nil && t.missing == 0
	var cands []*peerConn
	for _, pc := range t.peers {
		if !pc.closed && pc.c.PeerInterested() {
			cands = append(cands, pc)
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if seeding {
			return a.up.perSec > b.up.perSec
		}
		if a.snubbed != b.snubbed {
			return !a.snubbed
		}
		if a.down.perSec != b.down.perSec {
			return a.down.perSec > b.down.perSec
		}
		return a.up.perSec > b.up.perSec
	})
	keep := map[*peerConn]bool{}
	for i := 0; i < len(cands) && i < t.c.cfg.UploadSlots; i++ {
		keep[cands[i]] = true
	}
	opt := t.optimistic
	if opt == nil || opt.closed || !opt.c.PeerInterested() || now.Sub(t.optimisticAt) >= 30*time.Second || keep[opt] {
		var pool []*peerConn
		for _, pc := range cands {
			if keep[pc] {
				continue
			}
			pool = append(pool, pc)
			if now.Sub(pc.since) < 2*time.Minute {
				pool = append(pool, pc, pc)
			}
		}
		opt = nil
		if len(pool) > 0 {
			opt = pool[t.rng.IntN(len(pool))]
		}
		t.optimistic, t.optimisticAt = opt, now
	}
	if opt != nil {
		keep[opt] = true
	}
	for _, pc := range t.peers {
		choking := pc.c.AmChoking()
		switch {
		case keep[pc] && choking:
			t.unchokeLocked(pc, now)
		case !keep[pc] && !choking:
			t.chokeLocked(pc)
		}
	}
}

// checkSeedLimitsLocked pauses a torrent that has seeded enough.
func (t *Torrent) checkSeedLimitsLocked(now time.Time) {
	if t.info == nil || t.missing > 0 || t.seedStart.IsZero() {
		return
	}
	cfg := &t.c.cfg
	ratio := cfg.SeedRatio > 0 && float64(t.uploaded.Load()) >= cfg.SeedRatio*float64(t.info.TotalLength())
	long := cfg.SeedTime > 0 && now.Sub(t.seedStart) >= cfg.SeedTime
	if ratio || long {
		t.pauseLocked(StatePaused)
		t.c.emit(Event{Type: EventSeedLimit, Torrent: t})
	}
}

// limiter is a token bucket of bytes per second; a zero rate is no
// limit. It may go into debt by one block, which the next refills pay.
type limiter struct {
	mu     sync.Mutex
	rate   int64
	tokens int64
}

func (l *limiter) allow() bool {
	if l.rate == 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tokens > 0
}

func (l *limiter) take(n int) {
	if l.rate == 0 {
		return
	}
	l.mu.Lock()
	l.tokens -= int64(n)
	l.mu.Unlock()
}

func (l *limiter) refill(dt time.Duration) {
	if l.rate == 0 {
		return
	}
	l.mu.Lock()
	l.tokens = min(l.tokens+int64(float64(l.rate)*dt.Seconds()), l.rate)
	l.mu.Unlock()
}

func resolveAddrPort(s string) (netip.AddrPort, error) {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), nil
	}
	a, err := net.ResolveTCPAddr("tcp", s)
	if err != nil {
		return netip.AddrPort{}, err
	}
	ap := a.AddrPort()
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()), nil
}
