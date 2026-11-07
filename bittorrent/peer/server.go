package peer

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer/mse"
)

// Defaults of a Config.
const (
	DefaultHandshakeTimeout  = 20 * time.Second
	DefaultIdleTimeout       = 3 * time.Minute
	DefaultKeepaliveInterval = 2 * time.Minute
	DefaultDialTimeout       = 20 * time.Second
	// DefaultReqq is the reqq a Server advertises, and assumes of a peer
	// that advertises none, as libtorrent does.
	DefaultReqq = 250
	// DefaultMaxRequests is how many requests a Conn keeps outstanding to
	// a peer at most, the peer's reqq permitting.
	DefaultMaxRequests = 64
	// DefaultMaxQueuedBytes is how much a Conn lets queue for its socket
	// before it reports itself congested.
	DefaultMaxQueuedBytes = 1 << 20
	// DefaultVersion is the client name of the extended handshake.
	DefaultVersion = "fib"
)

// DefaultExtensions are the IDs a Server announces for the extensions it
// receives when Config.Extensions is nil.
var DefaultExtensions = map[string]uint8{ExtMetadata: 1, ExtPex: 2}

// Config is how a Server's connections behave.
type Config struct {
	// PeerID is this client's peer ID; zero means NewPeerID("").
	PeerID [20]byte
	// DisableFast and DisableExtended leave the Fast extension (BEP 6) and
	// the extension protocol (BEP 10) out of the handshake, which has both
	// otherwise.
	DisableFast     bool
	DisableExtended bool
	// DHT sets the DHT bit (BEP 5): the handler then sends a port message.
	DHT bool
	// Encryption is the MSE policy: what an incoming connection may be,
	// and how a dial begins, unless DialOptions says otherwise.
	Encryption mse.Policy
	// Extensions are the extended message IDs this side receives each
	// extension with, announced in the extended handshake's m; nil means
	// DefaultExtensions.
	Extensions map[string]uint8
	// Version is the extended handshake's v; empty means DefaultVersion.
	Version string
	// Reqq is the reqq announced; zero means DefaultReqq.
	Reqq int
	// MaxRequests bounds the requests Conn.Request keeps outstanding; zero
	// means DefaultMaxRequests.
	MaxRequests int
	// HandshakeTimeout bounds a connection until the handshake is done,
	// IdleTimeout closes one that receives nothing for so long, and
	// KeepaliveInterval is how long one may send nothing before a keepalive
	// goes. Zero means the defaults, and a negative value none.
	HandshakeTimeout  time.Duration
	IdleTimeout       time.Duration
	KeepaliveInterval time.Duration
	// DialTimeout bounds a TCP connect; zero means DefaultDialTimeout.
	DialTimeout time.Duration
	// MaxMessageLength bounds a received message; zero means
	// DefaultMaxMessageLength.
	MaxMessageLength int
	// MaxQueuedBytes is how much may queue for the socket after a piece
	// message before the Conn is Congested; zero means
	// DefaultMaxQueuedBytes.
	MaxQueuedBytes int
}

func duration(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	if d < 0 {
		return 0
	}
	return d
}

func (cfg *Config) setDefaults() {
	if cfg.PeerID == ([20]byte{}) {
		cfg.PeerID = NewPeerID("")
	}
	if cfg.Extensions == nil {
		cfg.Extensions = DefaultExtensions
	}
	if cfg.Version == "" {
		cfg.Version = DefaultVersion
	}
	if cfg.Reqq <= 0 {
		cfg.Reqq = DefaultReqq
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = DefaultMaxRequests
	}
	cfg.HandshakeTimeout = duration(cfg.HandshakeTimeout, DefaultHandshakeTimeout)
	cfg.IdleTimeout = duration(cfg.IdleTimeout, DefaultIdleTimeout)
	cfg.KeepaliveInterval = duration(cfg.KeepaliveInterval, DefaultKeepaliveInterval)
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.MaxQueuedBytes <= 0 {
		cfg.MaxQueuedBytes = DefaultMaxQueuedBytes
	}
}

// Torrent is a torrent a Server serves connections of: incoming handshakes
// for its info hashes, and dials made for it, go to its Handler.
type Torrent struct {
	// InfoHash is the v1 info hash, zero for a v2-only torrent.
	InfoHash metainfo.InfoHash
	// InfoHashV2 is the v2 info hash, zero for a v1-only torrent. A
	// hybrid torrent has both, and a connection begun with either is the
	// torrent's; one begun with the v1 hash by a peer that sets the v2 bit
	// is upgraded to v2 (BEP 52).
	InfoHashV2 metainfo.InfoHashV2
	// Handler handles the torrent's connections.
	Handler Handler
}

// Errors of a Server and its connections.
var (
	ErrUnknownTorrent   = errors.New("peer: no such torrent")
	ErrTorrentExists    = errors.New("peer: torrent already added")
	ErrInfoHashMismatch = errors.New("peer: peer answered with another info hash")
	ErrHandshakeTimeout = errors.New("peer: handshake timed out")
	ErrIdleTimeout      = errors.New("peer: connection idle")
	ErrClosed           = errors.New("peer: connection closed")
	ErrNotEstablished   = errors.New("peer: handshake not done")
	ErrChoked           = errors.New("peer: choked by the peer")
	ErrRequestLimit     = errors.New("peer: too many requests outstanding")
	ErrNoExtension      = errors.New("peer: peer does not support the extension")
	ErrProtocol         = errors.New("peer: protocol violation")
)

// Server is the peer wire protocol of a set of torrents: the fib.Handler
// of a TCP binding whose connections it handshakes and hands to the
// torrent their info hash names, and the dialer of connections to peers.
// It serves net.Conns as well, such as uTP's, with ServeConn and Open.
type Server struct {
	cfg      Config
	reserved Reserved

	mu       sync.RWMutex
	torrents map[[20]byte]*Torrent
	keys     mse.KeyTable
}

var _ fib.Handler = (*Server)(nil)

// NewServer returns a Server, with the defaults in place of config's
// zeros.
func NewServer(config Config) *Server {
	config.setDefaults()
	s := &Server{cfg: config, torrents: make(map[[20]byte]*Torrent)}
	s.reserved.SetFast(!config.DisableFast)
	s.reserved.SetExtended(!config.DisableExtended)
	s.reserved.SetDHT(config.DHT)
	return s
}

// Config is the Server's configuration, defaults filled in.
func (s *Server) Config() Config { return s.cfg }

// PeerID is the Server's peer ID.
func (s *Server) PeerID() [20]byte { return s.cfg.PeerID }

// AddTorrent adds t, which must have an info hash and a Handler, under
// its v1 info hash and its truncated v2 one.
func (s *Server) AddTorrent(t *Torrent) error {
	if t.InfoHash.IsZero() && t.InfoHashV2.IsZero() {
		return errors.New("peer: torrent without an info hash")
	}
	if t.Handler == nil {
		return errors.New("peer: torrent without a handler")
	}
	keys := t.shortHashes()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		if _, ok := s.torrents[k]; ok {
			return ErrTorrentExists
		}
	}
	for _, k := range keys {
		s.torrents[k] = t
		s.keys.Add(k)
	}
	return nil
}

// RemoveTorrent removes t: new connections for it are refused, and those
// open go on until the handler closes them.
func (s *Server) RemoveTorrent(t *Torrent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range t.shortHashes() {
		if s.torrents[k] == t {
			delete(s.torrents, k)
			s.keys.Remove(k)
		}
	}
}

func (t *Torrent) shortHashes() [][20]byte {
	var keys [][20]byte
	if !t.InfoHash.IsZero() {
		keys = append(keys, t.InfoHash)
	}
	if !t.InfoHashV2.IsZero() {
		keys = append(keys, t.InfoHashV2.Truncate())
	}
	return keys
}

func (s *Server) lookup(ih [20]byte) *Torrent {
	s.mu.RLock()
	t := s.torrents[ih]
	s.mu.RUnlock()
	return t
}

// OnOpen implements fib.Handler for an accepted connection.
func (s *Server) OnOpen(fc *fib.Connection) {
	c := s.newConn(false, nil, [20]byte{}, s.cfg.Encryption)
	fc.SetAttachment(c)
	c.attach(newFibTransport(fc))
}

// OnData implements fib.Handler.
func (s *Server) OnData(fc *fib.Connection, data []byte) {
	if c, ok := fc.Attachment().(*Conn); ok {
		c.input(data)
	}
}

// OnPriorityData implements fib.Handler.
func (s *Server) OnPriorityData(fc *fib.Connection, data []byte) { s.OnData(fc, data) }

// OnClose implements fib.Handler.
func (s *Server) OnClose(fc *fib.Connection, err error) {
	if c, ok := fc.Attachment().(*Conn); ok {
		c.finish(err)
	}
}

// DialOptions describes a connection to a peer.
type DialOptions struct {
	// Network and Addr are the peer's, for Dial: Network is "tcp" when
	// empty.
	Network, Addr string
	// InfoHash is the torrent's v1 info hash, or its v2 one truncated,
	// which must be added to the Server.
	InfoHash [20]byte
	// Encryption, when set, is the policy of this connection in place of
	// Config.Encryption: Allow begins in plaintext, Prefer with MSE
	// providing both methods, Require with MSE providing RC4 alone. A
	// caller that preferred MSE and was refused may dial again in
	// plaintext.
	Encryption *mse.Policy
}

func (s *Server) dialConn(opts *DialOptions) (*Conn, error) {
	t := s.lookup(opts.InfoHash)
	if t == nil {
		return nil, ErrUnknownTorrent
	}
	policy := s.cfg.Encryption
	if opts.Encryption != nil {
		policy = *opts.Encryption
	}
	return s.newConn(true, t, opts.InfoHash, policy), nil
}

// Dial connects to a peer over TCP on engine and handshakes for the
// torrent of opts.InfoHash. It returns at once: the torrent's handler hears
// OnHandshake once the peer has answered, or OnClose if the connect or the
// handshake fails.
func (s *Server) Dial(engine *fib.Engine, opts DialOptions) (*Conn, error) {
	c, err := s.dialConn(&opts)
	if err != nil {
		return nil, err
	}
	network := opts.Network
	if network == "" {
		network = "tcp"
	}
	err = engine.DialWithHandler(network, opts.Addr, s.cfg.DialTimeout, dialHandler{c}, func(fc *fib.Connection, err error) {
		if err != nil {
			c.finish(err)
		}
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// dialHandler is the fib.Handler of a connection Dial made.
type dialHandler struct{ c *Conn }

func (h dialHandler) OnOpen(fc *fib.Connection) {
	fc.SetAttachment(h.c)
	h.c.attach(newFibTransport(fc))
}
func (h dialHandler) OnData(_ *fib.Connection, data []byte)         { h.c.input(data) }
func (h dialHandler) OnPriorityData(_ *fib.Connection, data []byte) { h.c.input(data) }
func (h dialHandler) OnClose(_ *fib.Connection, err error)          { h.c.finish(err) }

// Open handshakes over conn, already connected to a peer, for the torrent
// of opts.InfoHash (opts.Network and Addr are not used), as Dial does
// over TCP: conn is read and written by goroutines of the Conn's own.
func (s *Server) Open(conn net.Conn, opts DialOptions) (*Conn, error) {
	c, err := s.dialConn(&opts)
	if err != nil {
		return nil, err
	}
	startNetTransport(c, conn)
	return c, nil
}

// ServeConn serves conn, accepted from a peer, as the TCP binding serves
// the connections it accepts.
func (s *Server) ServeConn(conn net.Conn) *Conn {
	c := s.newConn(false, nil, [20]byte{}, s.cfg.Encryption)
	startNetTransport(c, conn)
	return c
}

// Serve serves the connections ln accepts, a uTP listener's say, until it
// fails, and returns its error.
func (s *Server) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		s.ServeConn(conn)
	}
}
