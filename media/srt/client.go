//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package srt

import (
	"errors"
	"net"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
)

// ClientConfig configures a client session.
type ClientConfig struct {
	// StreamID is sent to the listener: "#!::r=app/stream,m=publish" to
	// publish to a server that reads the access control syntax.
	StreamID string
	// Latency, Passphrase, PBKeyLen, HandshakeTimeout, PeerIdleTimeout,
	// FlowWindow and SendBufferPackets are the session's Config.
	Latency           time.Duration
	Passphrase        string
	PBKeyLen          int
	HandshakeTimeout  time.Duration
	PeerIdleTimeout   time.Duration
	FlowWindow        int
	SendBufferPackets int
	// MaxSendBacklog is the unacknowledged payload over which Publish
	// drops packets; DefaultMaxSendBacklog when zero.
	MaxSendBacklog int64
	// OnConnect hears that the handshake is done, OnClose that the session
	// ended (nil after Close; a RejectReason when the listener refused).
	// They are called without the session's mutex held.
	OnConnect func(s *Session)
	OnClose   func(err error)
	// OnData hears the payloads of a session Dial made; it is called with
	// the session's mutex held and must not call the session.
	OnData func(payload []byte)
	// Muxer and Demuxer configure Publish's and Play's MPEG-TS. Play
	// without a hub hands the demuxed tracks and packets to the Demuxer's
	// OnTracks and OnPacket, under the session's mutex.
	Muxer   mpegts.MuxerConfig
	Demuxer mpegts.DemuxerConfig
	// Wait is Publish's hub.SubscribeOptions.Wait.
	Wait time.Duration
}

var errNoPlayTarget = errors.New("srt: Play needs a hub or Demuxer callbacks")

// Dial connects to the SRT listener at addr as a caller, from a UDP
// binding of its own that it adds to engine and removes when the session
// ends. It returns at once; OnConnect or OnClose tells how the handshake
// ends, and Write fails with ErrNotConnected until then. Dial waits for
// the engine's event loop, so it must not be called from a callback that
// loop runs.
func Dial(engine *fib.Engine, addr string, cfg ClientConfig) (*Session, error) {
	return dial(engine, addr, cfg, nil)
}

func dial(engine *fib.Engine, addr string, cfg ClientConfig, setup func(s *Session) error) (*Session, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	raddr := ua.AddrPort()
	s := newSession(raddr, cfg.MaxSendBacklog)
	s.id, _ = ParseStreamID(cfg.StreamID)
	s.muxCfg = cfg.Muxer
	s.onData = cfg.OnData
	s.onClose = cfg.OnClose
	if setup != nil {
		if err := setup(s); err != nil {
			return nil, err
		}
	}
	network := "udp4"
	if !raddr.Addr().Unmap().Is4() {
		network = "udp6"
	}
	binding, err := engine.AddBinding(fib.Binding{Network: network, Addr: ":0", Handler: clientHandler{s}})
	if err != nil {
		s.abandon()
		return nil, err
	}
	s.onClosed = func(*Session) { go func() { _ = engine.RemoveBinding(binding) }() }
	fc, err := engine.OpenUDPPeer(binding, raddr)
	if err != nil {
		_ = engine.RemoveBinding(binding)
		s.abandon()
		return nil, err
	}
	// The session keeps its own idle timeout.
	fc.SetUDPIdleTimeout(-1)
	s.lock()
	s.fc = fc
	core := s.connConfig(Config{
		Latency:           cfg.Latency,
		Passphrase:        cfg.Passphrase,
		PBKeyLen:          cfg.PBKeyLen,
		StreamID:          cfg.StreamID,
		PayloadSize:       MaxPayloadSize,
		FlowWindow:        cfg.FlowWindow,
		SendBufferPackets: cfg.SendBufferPackets,
		HandshakeTimeout:  cfg.HandshakeTimeout,
		PeerIdleTimeout:   cfg.PeerIdleTimeout,
	})
	core.OnConnect = func() {
		if s.isPlayer {
			s.startPlaying(time.Now())
		}
		if cfg.OnConnect != nil {
			s.after = append(s.after, func() { cfg.OnConnect(s) })
		}
	}
	conn, err := NewCaller(core, time.Now())
	if err != nil {
		s.unlock()
		_ = engine.RemoveBinding(binding)
		s.abandon()
		return nil, err
	}
	s.conn = conn
	s.unlock()
	return s, nil
}

// Publish dials addr and sends it the stream at path of h, muxed into
// MPEG-TS: a relay pushing to an SRT server, or to ffmpeg listening. The
// stream must be published already unless cfg.Wait lets the subscription
// wait for it.
func Publish(engine *fib.Engine, addr string, h *hub.Hub, path string, cfg ClientConfig) (*Session, error) {
	return dial(engine, addr, cfg, func(s *Session) error {
		// Subscribed before the handshake, with the sink holding what
		// comes until the session is connected: a subscription made from
		// OnConnect, under the session's mutex, could not take it.
		sub, err := h.Subscribe(path, s.sink(), hub.SubscribeOptions{
			Info: hub.SubscribeInfo{Protocol: "srt", Remote: s.remote, Query: s.id.Query},
			Wait: cfg.Wait,
		})
		s.sub = sub
		return err
	})
}

// Play dials addr and demuxes the MPEG-TS it receives: into h at path when
// h is set (a relay pulling from an SRT server or from ffmpeg listening),
// or to cfg.Demuxer's OnTracks and OnPacket.
//
// With h, path is reserved (hub.Hub.Reserve) before anything is sent, so
// a path that has a publisher fails Play with hub.ErrStreamBusy rather
// than a session that connects and is then shut; the stream goes live
// once the demuxer knows the tracks, and is unpublished when the session
// ends.
func Play(engine *fib.Engine, addr string, h *hub.Hub, path string, cfg ClientConfig) (*Session, error) {
	if h == nil && (cfg.Demuxer.OnTracks == nil || cfg.Demuxer.OnPacket == nil) {
		return nil, errNoPlayTarget
	}
	return dial(engine, addr, cfg, func(s *Session) error {
		if h != nil {
			info := hub.PublishInfo{Protocol: "srt", Remote: s.remote, Query: s.id.Query}
			pub, err := h.Reserve(path, info)
			if err != nil {
				return err
			}
			s.startPublish(h, path, info, cfg.Demuxer)
			s.pub = pub
		} else {
			s.demux = mpegts.NewDemuxer(cfg.Demuxer)
		}
		return nil
	})
}

// clientHandler is the handler of a client session's UDP binding.
type clientHandler struct{ s *Session }

func (h clientHandler) OnOpen(*fib.Connection)                 {}
func (h clientHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h clientHandler) OnData(c *fib.Connection, b []byte) {
	if c.RemoteAddrPort() != h.s.remote && c.RemoteAddrPort().Addr().Unmap() != h.s.remote.Addr().Unmap() {
		return // not the listener
	}
	h.s.lock()
	if h.s.conn != nil {
		h.s.fc = c
		h.s.conn.Handle(b, time.Now())
	}
	h.s.unlock()
}

func (h clientHandler) OnClose(c *fib.Connection, err error) {
	h.s.lock()
	if h.s.fc == c && h.s.conn != nil {
		h.s.fc = nil
		h.s.conn.closeWith(err)
	}
	h.s.unlock()
}

// OnEngineClose tells the listener the session ends.
func (h clientHandler) OnEngineClose(*fib.Connection) { h.s.Close() }

var _ fib.EngineCloseHandler = clientHandler{}
