package rtmp

import (
	"errors"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
)

// ServerConfig is how a Server serves its clients.
type ServerConfig struct {
	// Hub is where publishers' streams go and players' come from, at path
	// "app/stream". It may be nil when OnPublish supplies the sink of each
	// publish, and players are fed through ServerConn.Player.
	Hub *hub.Hub
	// OnConnect decides a client's connect command: an error refuses it,
	// with NetConnection.Connect.Rejected and the error's text. Nil admits
	// every client. It runs on the connection's worker.
	OnConnect func(c *ServerConn) error
	// OnPublish decides a publish of path, with the query the client's
	// app and stream name carried: an error refuses it. The sink it
	// returns, when not nil, hears the stream as well as the hub does; with
	// Hub nil it is the only place the stream goes, and a nil sink refuses
	// the publish. The hub's own OnPublish hook runs after it.
	OnPublish func(c *ServerConn, path, query string) (hub.Sink, error)
	// OnPlay decides a play of path: an error refuses it. With Hub nil,
	// the player is fed by whoever holds ServerConn.Player.
	OnPlay func(c *ServerConn, path, query string) error
	// OnClose hears that a client's connection closed.
	OnClose func(c *ServerConn, err error)
	// HandshakeTimeout bounds a client from the moment it connects until
	// it publishes or plays. Zero means DefaultHandshakeTimeout, and a
	// negative value no bound.
	HandshakeTimeout time.Duration
	// IdleTimeout closes a publisher that sends nothing, and a player
	// whose output does not move, for this long. Zero means
	// DefaultIdleTimeout, and a negative value no timeout.
	IdleTimeout time.Duration
	// PlayWait lets a player wait this long for a stream that has no
	// publisher yet, or whose publisher leaves; zero fails such a play at
	// once with NetStream.Play.StreamNotFound.
	PlayWait time.Duration
	// MaxMessageSize bounds a message a client sends. Zero means
	// DefaultMaxMessageSize.
	MaxMessageSize int
	// ChunkSize is the size of the chunks the server writes. Zero means
	// DefaultChunkSize.
	ChunkSize int
	// WindowAckSize is the acknowledgement window the server asks of its
	// clients. Zero means DefaultWindowAckSize.
	WindowAckSize uint32
	// WriteQueueLimit is the backlog past which a player's packets are
	// dropped, video until the next keyframe, and MaxWriteQueue the one
	// past which the player is disconnected. Zero means
	// DefaultWriteQueueLimit and DefaultMaxWriteQueue.
	WriteQueueLimit int64
	MaxWriteQueue   int64
	// Encoder configures how streams are written to players, such as
	// H.265 with the legacy codec id.
	Encoder flv.EncoderConfig
}

// Server is a fib.Handler that serves RTMP clients: publishers into the hub
// and players out of it. Bind it, or a fib/tls server in front of it for
// RTMPS, to an engine's TCP address:
//
//	h := hub.New(hub.Config{})
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":1935", Handler: rtmp.NewServer(rtmp.ServerConfig{Hub: h})})
type Server struct {
	config ServerConfig
}

var (
	_ fib.Handler         = (*Server)(nil)
	_ fib.ShutdownHandler = (*Server)(nil)
)

// NewServer returns a Server, with the defaults in place of config's zeros.
func NewServer(config ServerConfig) *Server {
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.ChunkSize <= 0 {
		config.ChunkSize = DefaultChunkSize
	}
	if config.WindowAckSize == 0 {
		config.WindowAckSize = DefaultWindowAckSize
	}
	if config.WriteQueueLimit <= 0 {
		config.WriteQueueLimit = DefaultWriteQueueLimit
	}
	if config.MaxWriteQueue <= 0 {
		config.MaxWriteQueue = DefaultMaxWriteQueue
	}
	return &Server{config: config}
}

// Errors a Server refuses with.
var (
	ErrNoHub        = errors.New("rtmp: no hub, and OnPublish gave no sink")
	ErrNotConnected = errors.New("rtmp: command before connect")
)

// ServerConn is a client's connection to a Server.
type ServerConn struct {
	session
	server *Server

	// What the connect command said.
	app, tcURL string
	object     amf.Object
	connected  bool
	nextStream uint32

	// The publish: its message stream, decoder and where it goes.
	pubStream uint32
	pubPath   string
	ingest    *flv.Decoder
	pubSink   hub.Sink

	// The play.
	playStream uint32
	playPath   string
	sub        *hub.Subscription
	player     *egress
	playSink   hub.Sink
}

// Conn returns the client's fib connection.
func (sc *ServerConn) Conn() *fib.Connection { return sc.c }

// App returns the application the client connected to, as its connect
// command named it, query included.
func (sc *ServerConn) App() string { return sc.app }

// TcURL returns the tcUrl of the client's connect command.
func (sc *ServerConn) TcURL() string { return sc.tcURL }

// ConnectObject returns the command object of the client's connect.
func (sc *ServerConn) ConnectObject() amf.Object { return sc.object }

// Player returns the sink that sends a stream to the client once it plays,
// for a server without a hub to feed it; it is nil before the play. It is
// wrapped in the slow-consumer policy of hub.NewDropper.
func (sc *ServerConn) Player() hub.Sink { return sc.playSink }

// Close closes the client's connection.
func (sc *ServerConn) Close() error { return sc.c.Close() }

// OnOpen starts a client's handshake, and the timeout that bounds it.
func (s *Server) OnOpen(c *fib.Connection) {
	sc := &ServerConn{server: s}
	sc.init(c, ConnConfig{MaxMessageSize: s.config.MaxMessageSize}, s.config.IdleTimeout)
	c.SetAttachment(sc)
	if s.config.HandshakeTimeout > 0 {
		_ = c.SetReadDeadline(time.Now().Add(s.config.HandshakeTimeout))
	}
}

// OnData reads the client's handshake, commands and media.
func (s *Server) OnData(c *fib.Connection, data []byte) {
	sc, _ := c.Attachment().(*ServerConn)
	if sc == nil {
		return
	}
	msgs, err := sc.feed(data)
	for i := range msgs {
		if err := sc.handle(&msgs[i]); err != nil {
			_ = c.Close()
			return
		}
	}
	if err != nil {
		_ = c.Close()
		return
	}
	if sc.ingest != nil {
		sc.touchRead()
	}
}

// OnPriorityData is not used by RTMP.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose ends the client's publish and play.
func (s *Server) OnClose(c *fib.Connection, err error) {
	sc, _ := c.Attachment().(*ServerConn)
	if sc == nil {
		return
	}
	sc.mu.Lock()
	sc.conn.Close()
	sc.mu.Unlock()
	sc.stopPublish(err)
	sc.stopPlay()
	if s.config.OnClose != nil {
		s.config.OnClose(sc, err)
	}
}

// OnShutdown closes the connection: RTMP has no way to ask a client to
// leave, and a publisher reconnects.
func (s *Server) OnShutdown(c *fib.Connection) { _ = c.Close() }

func (sc *ServerConn) handle(m *Message) error {
	switch m.Type {
	case TypeCommandAMF0, TypeCommandAMF3:
		cmd, err := ParseCommand(m)
		if err != nil {
			return nil
		}
		return sc.command(cmd)
	case TypeAudio, TypeVideo, TypeDataAMF0, TypeDataAMF3:
		// One publish per connection: its media is taken on any message
		// stream, as clients are not all careful about which.
		if sc.ingest != nil {
			ingestMessage(sc.ingest, m)
		}
	}
	return nil
}

func (sc *ServerConn) result(cmd *Command, values ...any) error {
	if cmd.TransactionID == 0 {
		return nil
	}
	return sc.write(func(c *Conn) error { return c.WriteCommand(cmd.StreamID, "_result", cmd.TransactionID, values...) })
}

func (sc *ServerConn) onStatus(streamID uint32, level, code, description string) error {
	return sc.write(func(c *Conn) error {
		return c.WriteCommand(streamID, "onStatus", 0, nil, status(level, code, description))
	})
}

func (sc *ServerConn) command(cmd *Command) error {
	if cmd.Name != "connect" && !sc.connected {
		return ErrNotConnected
	}
	switch cmd.Name {
	case "connect":
		return sc.connect(cmd)
	case "createStream":
		sc.nextStream++
		return sc.result(cmd, nil, float64(sc.nextStream))
	case "releaseStream", "FCPublish", "FCUnpublish", "getStreamLength", "_checkbw", "FCSubscribe":
		return sc.result(cmd, nil, amf.Undefined{})
	case "publish":
		return sc.publish(cmd)
	case "play":
		return sc.play(cmd)
	case "deleteStream":
		var id uint32
		if len(cmd.Args) > 0 {
			n, _ := cmd.Args[0].(float64)
			id = uint32(n)
		}
		sc.closeStream(id)
		return nil
	case "closeStream":
		sc.closeStream(cmd.StreamID)
		return nil
	case "pause", "seek", "receiveAudio", "receiveVideo", "_result", "_error", "onStatus":
		return nil
	}
	if cmd.TransactionID == 0 {
		return nil
	}
	return sc.write(func(c *Conn) error {
		return c.WriteCommand(cmd.StreamID, "_error", cmd.TransactionID, nil,
			status("error", "NetConnection.Call.Failed", "unknown command "+cmd.Name))
	})
}

func (sc *ServerConn) connect(cmd *Command) error {
	if sc.connected {
		return nil
	}
	cfg := &sc.server.config
	switch o := cmd.Object.(type) {
	case amf.Object:
		sc.object = o
	case amf.ECMAArray:
		sc.object = amf.Object(o)
	}
	sc.app, _ = sc.object.String("app")
	sc.tcURL, _ = sc.object.String("tcUrl")
	encoding, _ := sc.object.Number("objectEncoding")
	if cfg.OnConnect != nil {
		if err := cfg.OnConnect(sc); err != nil {
			_ = sc.write(func(c *Conn) error {
				return c.WriteCommand(0, "_error", cmd.TransactionID, nil,
					status("error", "NetConnection.Connect.Rejected", err.Error()))
			})
			sc.c.CloseAfterSend()
			return nil
		}
	}
	sc.connected = true
	info := status("status", "NetConnection.Connect.Success", "Connection succeeded.")
	info = append(info, amf.Property{Key: "objectEncoding", Value: encoding})
	return sc.write(func(c *Conn) error {
		return errors.Join(
			c.WriteWindowAckSize(cfg.WindowAckSize),
			c.WritePeerBandwidth(cfg.WindowAckSize, 2),
			c.SetChunkSize(cfg.ChunkSize),
			c.WriteCommand(0, "_result", cmd.TransactionID,
				amf.Object{{Key: "fmsVer", Value: "FMS/3,0,1,123"}, {Key: "capabilities", Value: 31.0}}, info))
	})
}

// streamName returns the stream name a publish or play names.
func streamName(cmd *Command) string {
	if len(cmd.Args) == 0 {
		return ""
	}
	name, _ := cmd.Args[0].(string)
	return name
}

func (sc *ServerConn) publish(cmd *Command) error {
	cfg := &sc.server.config
	name := streamName(cmd)
	path, query := streamPath(sc.app, name)
	refuse := func(code string, err error) error {
		_ = sc.onStatus(cmd.StreamID, "error", code, err.Error())
		sc.c.CloseAfterSend()
		return nil
	}
	if name == "" || sc.ingest != nil {
		return refuse("NetStream.Publish.BadName", errors.New("bad stream name"))
	}
	var also hub.Sink
	if cfg.OnPublish != nil {
		var err error
		if also, err = cfg.OnPublish(sc, path, query); err != nil {
			return refuse("NetStream.Publish.Unauthorized", err)
		}
	}
	target := &publisherSink{also: also}
	if cfg.Hub != nil {
		pub, err := cfg.Hub.Publish(path, nil, hub.PublishInfo{Protocol: "rtmp", Remote: sc.c.RemoteAddrPort(), Query: query})
		if err != nil {
			if also != nil {
				also.OnClose(err)
			}
			code := "NetStream.Publish.Denied"
			if errors.Is(err, hub.ErrStreamBusy) {
				code = "NetStream.Publish.BadName"
			}
			return refuse(code, err)
		}
		target.pub = pub
	} else if also == nil {
		return refuse("NetStream.Publish.Denied", ErrNoHub)
	}
	sc.pubStream, sc.pubPath, sc.pubSink = cmd.StreamID, path, target
	sc.ingest = newIngest(target)
	// A publisher is expected to keep sending: from now on silence for
	// IdleTimeout ends it.
	sc.readAt = time.Time{}
	if sc.idle <= 0 {
		_ = sc.c.SetReadDeadline(time.Time{})
	}
	sc.touchRead()
	return sc.write(func(c *Conn) error {
		return errors.Join(
			c.WriteUserControl(EventStreamBegin, cmd.StreamID),
			c.WriteCommand(cmd.StreamID, "onStatus", 0, nil, status("status", "NetStream.Publish.Start", name+" is now published.")))
	})
}

func (sc *ServerConn) play(cmd *Command) error {
	cfg := &sc.server.config
	name := streamName(cmd)
	path, query := streamPath(sc.app, name)
	refuse := func(code string, err error) error {
		_ = sc.onStatus(cmd.StreamID, "error", code, err.Error())
		sc.c.CloseAfterSend()
		return nil
	}
	if name == "" || sc.player != nil {
		return refuse("NetStream.Play.Failed", errors.New("bad stream name"))
	}
	streamID := cmd.StreamID
	// The player exists before OnPlay, so that a server without a hub can
	// take it there; it sends nothing until the play has started.
	sc.player = &egress{s: &sc.session, streamID: streamID, enc: flv.NewEncoder(cfg.Encoder), onClose: func(error) {
		_ = sc.write(func(c *Conn) error {
			return errors.Join(
				c.WriteUserControl(EventStreamEOF, streamID),
				c.WriteCommand(streamID, "onStatus", 0, nil, status("status", "NetStream.Play.UnpublishNotify", "stream ended")))
		})
		sc.c.CloseAfterSend()
	}}
	sc.playSink = hub.NewDropper(sc.player, sc.backlog, cfg.WriteQueueLimit)
	sc.playStream, sc.playPath = streamID, path
	if cfg.OnPlay != nil {
		if err := cfg.OnPlay(sc, path, query); err != nil {
			sc.stopPlay()
			return refuse("NetStream.Play.Failed", err)
		}
	}
	if cfg.Hub != nil {
		// The subscription comes first, so that a stream that is not there
		// is refused before the play is said to start; what it delivers
		// meanwhile waits in the player until then.
		sub, err := cfg.Hub.Subscribe(path, sc.playSink, hub.SubscribeOptions{
			Info: hub.SubscribeInfo{Protocol: "rtmp", Remote: sc.c.RemoteAddrPort(), Query: query},
			Wait: cfg.PlayWait,
		})
		if err != nil {
			sc.stopPlay()
			code := "NetStream.Play.Failed"
			if errors.Is(err, hub.ErrNotFound) {
				code = "NetStream.Play.StreamNotFound"
			}
			return refuse(code, err)
		}
		sc.sub = sub
	}
	sc.c.SetWriteQueueLimit(cfg.MaxWriteQueue)
	p := amf.AppendString(nil, "|RtmpSampleAccess")
	p = amf.AppendBool(amf.AppendBool(p, true), true)
	if err := sc.write(func(c *Conn) error {
		return errors.Join(
			c.WriteUserControl(EventStreamIsRecorded, streamID),
			c.WriteUserControl(EventStreamBegin, streamID),
			c.WriteCommand(streamID, "onStatus", 0, nil, status("status", "NetStream.Play.Reset", "Playing and resetting "+name+".")),
			c.WriteCommand(streamID, "onStatus", 0, nil, status("status", "NetStream.Play.Start", "Started playing "+name+".")),
			c.WriteMessage(&Message{Type: TypeDataAMF0, StreamID: streamID, Payload: p}))
	}); err != nil {
		return err
	}
	// The player sends little from now on; its write deadline, pushed
	// forward as output moves, is what ends it when it stops reading.
	_ = sc.c.SetReadDeadline(time.Time{})
	sc.player.start(streamID)
	return nil
}

func (sc *ServerConn) closeStream(id uint32) {
	if sc.ingest != nil && id == sc.pubStream {
		sc.stopPublish(nil)
	}
	if sc.player != nil && id == sc.playStream {
		sc.stopPlay()
	}
}

func (sc *ServerConn) stopPublish(err error) {
	if sc.ingest == nil {
		return
	}
	sc.ingest.Flush()
	sc.pubSink.OnClose(err)
	sc.ingest, sc.pubSink = nil, nil
}

func (sc *ServerConn) stopPlay() {
	if sc.sub != nil {
		sc.sub.Close()
		sc.sub = nil
	}
	if sc.player != nil {
		sc.player.stop()
	}
	sc.player, sc.playSink = nil, nil
}
