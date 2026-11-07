package kcp

import (
	"encoding/binary"
	"net"
	"net/netip"
	"sync"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// DefaultAcceptBacklog is ServerConfig.AcceptBacklog when zero.
const DefaultAcceptBacklog = 128

// ServerConfig configures a Server.
type ServerConfig struct {
	// Config is every session's.
	Config
	// Accept, when set, decides whether a peer's new conversation gets a
	// session. It runs on a fib worker and must not block.
	Accept func(remote netip.AddrPort, conv uint32) bool
	// OnSession, when set, is given each new session before it reads its
	// first datagram, on a fib worker: it must not block, nor Read or
	// Write (Send may). Without it, sessions wait for Server.Accept.
	OnSession func(s *Session)
	// MaxSessions bounds the open sessions; 0 is unlimited.
	MaxSessions int
	// AcceptBacklog bounds the sessions waiting for Accept; a new one
	// past it is dropped, for the peer to send again.
	// DefaultAcceptBacklog when zero.
	AcceptBacklog int
}

// Server serves KCP on fib UDP bindings: a fib.Handler whose peers each
// have a session, keyed by address as kcp-go's Listener keys them. A
// datagram from an address without one starts a session when its first
// segment is the first of a conversation (data with sequence number 0); a
// peer that starts another conversation that way from the same address
// replaces its session. It is a net.Listener of its sessions, too, when
// ServerConfig.OnSession is not set.
//
//	srv, _ := kcp.NewServer(kcp.ServerConfig{})
//	engine, _ := fib.New(fib.DefaultConfig(),
//		fib.Binding{Network: "udp", Addr: ":4000", Handler: srv})
//	go engine.Run()
//	http.Serve(srv, handler)
//
// Listen does that for an engine that runs already.
type Server struct {
	cfg ServerConfig

	mu       sync.Mutex
	sessions map[netip.AddrPort]*Session
	closed   bool
	addr     net.Addr

	accepts chan *Session
	die     chan struct{}

	engine  *fib.Engine
	binding int
}

// NewServer returns a Server, a fib.Handler for UDP bindings.
func NewServer(cfg ServerConfig) (*Server, error) {
	if _, err := newFramer(nil, cfg.DataShards, cfg.ParityShards); err != nil {
		return nil, err
	}
	if _, err := cfg.newKCP(0, 0, nil); err != nil {
		return nil, err
	}
	if cfg.AcceptBacklog <= 0 {
		cfg.AcceptBacklog = DefaultAcceptBacklog
	}
	return &Server{
		cfg:      cfg,
		sessions: map[netip.AddrPort]*Session{},
		accepts:  make(chan *Session, cfg.AcceptBacklog),
		die:      make(chan struct{}),
		binding:  -1,
	}, nil
}

// Listen serves KCP at addr ("host:port") on a UDP binding it adds to
// engine, which must be running; Close removes it. Listen waits for the
// engine's event loop, so it must not be called from a callback that loop
// runs.
func Listen(engine *fib.Engine, addr string, cfg ServerConfig) (*Server, error) {
	s, err := NewServer(cfg)
	if err != nil {
		return nil, err
	}
	i, err := engine.AddBinding(fib.Binding{Network: "udp", Addr: addr, Handler: s})
	if err != nil {
		return nil, err
	}
	s.engine, s.binding = engine, i
	if addrs, err := engine.BindingAddrs(i); err == nil && len(addrs) > 0 {
		s.addr = addrs[0]
	}
	return s, nil
}

// Sessions returns the open sessions.
func (s *Server) Sessions() []*Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		all = append(all, sess)
	}
	return all
}

// Accept waits for the next session, as net.Listener.
func (s *Server) Accept() (net.Conn, error) {
	select {
	case sess := <-s.accepts:
		return sess, nil
	case <-s.die:
		return nil, net.ErrClosed
	}
}

// Addr returns the address of the binding Listen added, or of the socket
// the first session came on.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.addr == nil {
		return &net.UDPAddr{}
	}
	return s.addr
}

// Close closes every session and refuses new ones; it removes the binding
// Listen added.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.die)
	s.mu.Unlock()
	for _, sess := range s.Sessions() {
		_ = sess.Close()
	}
	for {
		select {
		case sess := <-s.accepts:
			_ = sess.Close()
			continue
		default:
		}
		break
	}
	if s.engine != nil {
		return s.engine.RemoveBinding(s.binding)
	}
	return nil
}

// OnOpen is part of fib.Handler.
func (s *Server) OnOpen(*fib.Connection) {}

// OnPriorityData is part of fib.Handler.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnData reads a datagram.
func (s *Server) OnData(c *fib.Connection, b []byte) { s.OnDatagrams(c, [][]byte{b}) }

// OnDatagrams reads the datagrams of a peer, together.
func (s *Server) OnDatagrams(c *fib.Connection, dgrams [][]byte) {
	remote := c.RemoteAddrPort()
	s.mu.Lock()
	sess := s.sessions[remote]
	s.mu.Unlock()
	batch := make([][]byte, 0, len(dgrams))
	for _, d := range dgrams {
		o := open(s.cfg.Crypt, d)
		if o == nil {
			continue
		}
		conv, cmd, sn, ok := peekConv(o)
		if sess != nil && (!ok || conv == sess.Conv()) {
			batch = append(batch, o)
			continue
		}
		if !ok || cmd != cmdPush || sn != 0 {
			continue // not a conversation's start: the peer's session is gone
		}
		if sess != nil {
			sess.input(c, batch...)
			batch = batch[:0]
			sess.closeWith(ErrReplaced)
		}
		if sess = s.start(c, remote, conv); sess != nil {
			batch = append(batch, o)
		}
	}
	if sess != nil && len(batch) > 0 {
		sess.input(c, batch...)
	}
	for _, d := range dgrams {
		bufferpool.Put(d)
	}
}

// start makes the session of a new conversation.
func (s *Server) start(c *fib.Connection, remote netip.AddrPort, conv uint32) *Session {
	if s.cfg.Accept != nil && !s.cfg.Accept(remote, conv) {
		return nil
	}
	s.mu.Lock()
	if s.closed || (s.cfg.MaxSessions > 0 && len(s.sessions) >= s.cfg.MaxSessions) {
		s.mu.Unlock()
		return nil
	}
	if s.cfg.OnSession == nil && len(s.accepts) >= cap(s.accepts) {
		s.mu.Unlock()
		return nil
	}
	sess, err := newSession(s.cfg.Config, conv, remote)
	if err != nil {
		s.mu.Unlock()
		return nil
	}
	sess.fc = c
	sess.local = c.LocalAddr()
	if s.addr == nil {
		s.addr = sess.local
	}
	sess.onClosed = s.forget
	s.sessions[remote] = sess
	s.mu.Unlock()
	// The session times its peer out itself.
	c.SetUDPIdleTimeout(-1)
	if s.cfg.OnSession != nil {
		s.cfg.OnSession(sess)
	} else {
		select {
		case s.accepts <- sess:
		default:
			sess.closeWith(net.ErrClosed)
			return nil
		}
	}
	return sess
}

// forget removes a closed session, and gives its peer's connection back to
// the engine's UDP idle timeout.
func (s *Server) forget(sess *Session) {
	s.mu.Lock()
	if s.sessions[sess.remote] == sess {
		delete(s.sessions, sess.remote)
		if c := sess.fc; c != nil {
			c.SetUDPIdleTimeout(0)
		}
	}
	s.mu.Unlock()
}

// OnClose ends the session of a connection fib closed.
func (s *Server) OnClose(c *fib.Connection, err error) {
	s.mu.Lock()
	sess := s.sessions[c.RemoteAddrPort()]
	s.mu.Unlock()
	if sess != nil {
		sess.connClosed(c, err)
	}
}

// OnEngineClose sends what the session of a connection the engine is
// closing has due.
func (s *Server) OnEngineClose(c *fib.Connection) {
	s.mu.Lock()
	sess := s.sessions[c.RemoteAddrPort()]
	s.mu.Unlock()
	if sess != nil {
		_ = sess.Close()
	}
}

// connClosed closes the session when c was its connection.
func (s *Session) connClosed(c *fib.Connection, err error) {
	s.mu.Lock()
	if s.fc == c {
		s.fc = nil
		if err == nil {
			err = net.ErrClosed
		}
		s.closeLocked(err)
	}
	s.unlock()
}

// peekConv returns the conversation, command and sequence number of the
// first segment of an opened datagram, and false for one that carries
// none: a parity shard, kcp-go's out-of-band data, or one too short.
func peekConv(b []byte) (conv uint32, cmd uint8, sn uint32, ok bool) {
	switch binary.LittleEndian.Uint16(b[4:]) {
	case typeData:
		b = b[fecHeaderSizePlus2:]
	case typeParity, typeOOB:
		return 0, 0, 0, false
	}
	if len(b) < Overhead {
		return 0, 0, 0, false
	}
	return binary.LittleEndian.Uint32(b), b[4], binary.LittleEndian.Uint32(b[snOffset:]), true
}

var (
	_ fib.Handler            = (*Server)(nil)
	_ fib.EngineCloseHandler = (*Server)(nil)
	_ net.Listener           = (*Server)(nil)
)
