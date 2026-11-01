package srt

import (
	"errors"
	"net/netip"
	"sync"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// ServerConfig configures a Server.
type ServerConfig struct {
	// Hub is where publishers' streams go and players' come from, by the
	// stream ID's resource. A publisher reserves its path in the
	// handshake (hub.Hub.Reserve), so one of a path that has a publisher
	// is rejected with RejectConflict, and one the hub's OnPublish hook
	// refuses with the RejectReason it returns, or RejectForbidden. It may
	// be nil when OnSession takes every session.
	Hub *hub.Hub
	// Latency is the TSBPD latency the server asks for; the caller's wins
	// when larger. DefaultLatency when zero.
	Latency time.Duration
	// Passphrase and PBKeyLen require callers to encrypt (Config). With a
	// passphrase, callers without one are rejected, and the reverse.
	Passphrase string
	PBKeyLen   int
	// ParseStreamID parses a caller's stream ID; ParseStreamID when nil.
	ParseStreamID func(streamID string) (StreamID, error)
	// OnConnect is the authentication hook: it sees every caller's
	// handshake and parsed stream ID before anything else does, and
	// rejects the caller by returning an error, a RejectReason as it is
	// (RejectUnauthorized, RejectForbidden...), any other as
	// RejectForbidden. It runs on a fib worker and must not block.
	OnConnect func(req *Request, id StreamID) error
	// OnSession, when set, is offered every session once it is
	// connected; returning true takes it out of the hub's hands, for the
	// application to Write to and read through SetOnData.
	OnSession func(s *Session) bool
	// MaxConns bounds the sessions; 0 is unlimited. Callers over it are
	// rejected with RejectOverload.
	MaxConns int
	// PeerIdleTimeout, FlowWindow and SendBufferPackets are the Config of
	// every session.
	PeerIdleTimeout   time.Duration
	FlowWindow        int
	SendBufferPackets int
	// MaxSendBacklog is the unacknowledged payload over which a player
	// drops packets; DefaultMaxSendBacklog when zero.
	MaxSendBacklog int64
	// PlayWait lets a player subscribe to a stream not yet published,
	// waiting that long for it (hub.SubscribeOptions.Wait); without it, a
	// player of a missing stream is rejected with RejectNotFound.
	PlayWait time.Duration
	// PlayGOPCache starts players with the hub's cached GOP rather than
	// at the next keyframe. It is off by default because the GOP goes out
	// at once: a 4 Mbit/s stream's megabyte overflows a receiver's UDP
	// buffer (Linux's 208 KiB by default) faster than NAKs can bring it
	// back within the latency, all of it stamped with the time it was
	// sent, and the player loses its start. Turn it on for small streams
	// or receivers with large buffers and latencies.
	PlayGOPCache bool
	// Muxer and Demuxer configure the MPEG-TS of players and publishers;
	// their callbacks are the server's.
	Muxer   mpegts.MuxerConfig
	Demuxer mpegts.DemuxerConfig
}

// Server is an SRT listener on a fib UDP binding: every peer address is a
// fib connection whose datagrams go to the Listener until the handshake
// makes a Session of it. Publishers (m=publish) are demuxed into the hub
// and players (the default, m=request) get the hub's stream muxed into
// MPEG-TS.
//
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Network: "udp", Addr: ":9000", Handler: srv})
type Server struct {
	cfg      ServerConfig
	listener *Listener

	mu       sync.Mutex
	sessions map[netip.AddrPort]*Session
	closed   bool

	// hsMu serializes handshakes, so Accept knows which connection and
	// address the handshake it decides came on.
	hsMu   sync.Mutex
	hsConn *fib.Connection
	hsSess *Session
}

var errNoHub = errors.New("srt: ServerConfig has neither Hub nor OnSession")

// NewServer returns a Server, a fib.Handler for a UDP binding.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Hub == nil && cfg.OnSession == nil {
		return nil, errNoHub
	}
	if cfg.ParseStreamID == nil {
		cfg.ParseStreamID = ParseStreamID
	}
	s := &Server{cfg: cfg, sessions: map[netip.AddrPort]*Session{}}
	var err error
	s.listener, err = NewListener(ListenerConfig{Accept: s.accept})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Sessions returns the sessions, connected and not yet closed.
func (s *Server) Sessions() []*Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		all = append(all, sess)
	}
	return all
}

// Close shuts every session down and refuses new ones.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	for _, sess := range s.Sessions() {
		sess.Close()
	}
}

// OnOpen is part of fib.Handler.
func (s *Server) OnOpen(*fib.Connection) {}

// OnPriorityData is part of fib.Handler.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnData reads one datagram.
func (s *Server) OnData(c *fib.Connection, b []byte) {
	remote := c.RemoteAddrPort()
	s.mu.Lock()
	sess := s.sessions[remote]
	s.mu.Unlock()
	if sess != nil {
		if id, _, ok := IsHandshake(b); !ok || id == sess.conn.PeerSocketID() {
			sess.handle(c, b)
			return
		}
		// The caller started again from the same address with a new
		// socket: the old session is gone.
		sess.Close()
	}
	s.handshake(c, remote, b)
}

// handshake passes a datagram from an address without a session to the
// Listener.
func (s *Server) handshake(c *fib.Connection, remote netip.AddrPort, b []byte) {
	s.hsMu.Lock()
	s.hsConn, s.hsSess = c, nil
	conn := s.listener.Handle(b, remote, time.Now(), func(out []byte) { _ = c.Send(out) })
	// A session Accept made comes back locked: the Conn's first output,
	// its conclusion response, went into it.
	sess := s.hsSess
	s.hsConn, s.hsSess = nil, nil
	s.hsMu.Unlock()
	if conn == nil {
		if sess != nil {
			// Accepted, then rejected over the passphrase.
			if sess.sub != nil {
				sess.sub.Close()
			}
			if sess.pub != nil {
				sess.pub.Close()
			}
			sess.mu.Unlock()
		}
		return
	}
	sess.conn = conn
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		conn.Close(time.Now())
		sess.unlock()
		return
	}
	if old := s.sessions[remote]; old != nil && old != sess {
		defer old.Close()
	}
	s.sessions[remote] = sess
	s.mu.Unlock()
	sess.onClosed = s.forget
	taken := false
	if s.cfg.OnSession != nil {
		// Offered with the mutex released: the application may call the
		// session.
		sess.unlock()
		taken = s.cfg.OnSession(sess)
		sess.lock()
	}
	if taken {
		if sess.sub != nil {
			sess.sub.Close()
			sess.sub = nil
		}
		if sess.pub != nil {
			sess.pub.Close()
			sess.pub = nil
		}
		sess.demux = nil
	} else if sess.isPlayer {
		sess.startPlaying(time.Now())
	}
	sess.unlock()
}

// forget removes a closed session.
func (s *Server) forget(sess *Session) {
	s.mu.Lock()
	if s.sessions[sess.remote] == sess {
		delete(s.sessions, sess.remote)
	}
	s.mu.Unlock()
}

// accept decides on a caller's handshake (ListenerConfig.Accept), with hsMu
// held.
func (s *Server) accept(req *Request) (Config, error) {
	id, err := s.cfg.ParseStreamID(req.StreamID)
	if err != nil {
		if s.cfg.Hub != nil || req.StreamID != "" {
			return Config{}, RejectBadRequest
		}
	}
	if s.cfg.OnConnect != nil {
		if err := s.cfg.OnConnect(req, id); err != nil {
			var r RejectReason
			if !errors.As(err, &r) {
				r = RejectForbidden
			}
			return Config{}, r
		}
	}
	s.mu.Lock()
	n, closed := len(s.sessions), s.closed
	s.mu.Unlock()
	switch {
	case closed:
		return Config{}, RejectClose
	case s.cfg.MaxConns > 0 && n >= s.cfg.MaxConns:
		return Config{}, RejectOverload
	case id.Mode == ModeBidirectional && s.cfg.OnSession == nil:
		return Config{}, RejectBadMode
	}
	sess := newSession(req.Remote, s.cfg.MaxSendBacklog)
	sess.id = id
	sess.muxCfg = s.cfg.Muxer
	if s.cfg.Hub != nil {
		switch id.Mode {
		case ModePublish:
			// The path is claimed now, while the caller can still be
			// refused; it goes live once the demuxer knows the tracks.
			info := hub.PublishInfo{Protocol: "srt", Remote: req.Remote, Query: id.Query}
			pub, err := s.cfg.Hub.Reserve(id.Resource, info)
			if err != nil {
				return Config{}, publishReject(err)
			}
			sess.startPublish(s.cfg.Hub, id.Resource, info, s.cfg.Demuxer)
			sess.pub = pub
		case ModeRequest:
			sub, err := s.cfg.Hub.Subscribe(id.Resource, sess.sink(), hub.SubscribeOptions{
				Info:       hub.SubscribeInfo{Protocol: "srt", Remote: req.Remote, Query: id.Query},
				Wait:       s.cfg.PlayWait,
				NoGOPCache: !s.cfg.PlayGOPCache,
			})
			switch {
			case errors.Is(err, hub.ErrNotFound):
				return Config{}, RejectNotFound
			case errors.Is(err, hub.ErrLimit):
				return Config{}, RejectOverload
			case errors.Is(err, hub.ErrClosed):
				return Config{}, RejectClose
			case err != nil:
				return Config{}, RejectForbidden
			}
			sess.sub = sub
		}
	}
	sess.fc = s.hsConn
	s.hsSess = sess
	// Locked until the server has the Conn: a publisher may already be
	// calling the sink, and the Conn's output goes into the session.
	sess.mu.Lock()
	return sess.connConfig(Config{
		Latency:           s.cfg.Latency,
		Passphrase:        s.cfg.Passphrase,
		PBKeyLen:          s.cfg.PBKeyLen,
		PayloadSize:       MaxPayloadSize,
		FlowWindow:        s.cfg.FlowWindow,
		SendBufferPackets: s.cfg.SendBufferPackets,
		PeerIdleTimeout:   s.cfg.PeerIdleTimeout,
	}), nil
}

// OnClose ends the session of a connection fib closed, as its UDP idle
// timeout does.
func (s *Server) OnClose(c *fib.Connection, err error) {
	s.mu.Lock()
	sess := s.sessions[c.RemoteAddrPort()]
	s.mu.Unlock()
	if sess == nil {
		return
	}
	sess.lock()
	if sess.fc == c {
		sess.fc = nil
		sess.conn.closeWith(err)
	}
	sess.unlock()
}

// OnEngineClose tells the peer of a connection the engine is closing that
// the session ends.
func (s *Server) OnEngineClose(c *fib.Connection) {
	s.mu.Lock()
	sess := s.sessions[c.RemoteAddrPort()]
	s.mu.Unlock()
	if sess != nil {
		sess.Close()
	}
}

// publishReject is the rejection of a publisher the hub refused: the path
// has one (RejectConflict, SRT_REJX_CONFLICT, which the access control
// guidelines give a publish of a resource already locked for
// modification), the hub is full or closed, or
// its OnPublish hook said no, with a RejectReason of its own or as
// RejectForbidden.
func publishReject(err error) RejectReason {
	var r RejectReason
	switch {
	case errors.Is(err, hub.ErrStreamBusy):
		return RejectConflict
	case errors.Is(err, hub.ErrLimit):
		return RejectOverload
	case errors.Is(err, hub.ErrClosed):
		return RejectClose
	case errors.As(err, &r):
		return r
	}
	return RejectForbidden
}
