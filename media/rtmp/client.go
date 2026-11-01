package rtmp

import (
	stdtls "crypto/tls"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
	fibtls "github.com/lesismal/fib/tls"
)

// DefaultDialTimeout bounds a client's TCP connect when its config leaves
// DialTimeout 0.
const DefaultDialTimeout = 10 * time.Second

// ClientConfig is how a client publishes or plays.
type ClientConfig struct {
	// TLSConfig is used for rtmps:// URLs; nil means a default config for
	// the URL's host.
	TLSConfig *stdtls.Config
	// DialTimeout bounds the TCP connect, and HandshakeTimeout what follows
	// until the publish or play has started. Zero means DefaultDialTimeout
	// and DefaultHandshakeTimeout.
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	// IdleTimeout ends a play whose server sends nothing, and a publish
	// whose output does not move, for this long. Zero means
	// DefaultIdleTimeout, and a negative value no timeout.
	IdleTimeout time.Duration
	// MaxMessageSize bounds a message the server sends. Zero means
	// DefaultMaxMessageSize.
	MaxMessageSize int
	// ChunkSize is the size of the chunks the client writes. Zero means
	// DefaultChunkSize.
	ChunkSize int
	// WriteQueueLimit and MaxWriteQueue bound what a publish queues, as
	// ServerConfig's do for a player. Zero means the defaults.
	WriteQueueLimit int64
	MaxWriteQueue   int64
	// FlashVer is the connect command's flashVer; empty means one that
	// says the client is fib.
	FlashVer string
	// SimpleHandshake sends the simple handshake instead of the Flash
	// digest one. Servers that check digests (Adobe's, and some CDNs)
	// refuse it; every server accepts the digest one.
	SimpleHandshake bool
	// Encoder configures how a published stream is written.
	Encoder flv.EncoderConfig
	// OnClose hears that the connection closed, after a publish or play
	// had started.
	OnClose func(c *ClientConn, err error)
}

// ClientConn is a client's connection to an RTMP server, publishing or
// playing one stream. A publishing ClientConn is a hub.Sink: what it hears
// it publishes, so Hub.Subscribe(path, conn, ...) pushes a hub stream to
// the server; Push does that.
type ClientConn struct {
	session
	config ClientConfig
	url    *URL

	publishing bool
	ready      func(*ClientConn, error)
	readyOnce  sync.Once
	started    bool
	nextTx     float64
	connectTx  float64
	createTx   float64
	streamID   uint32

	// play
	sink   hub.Sink
	ingest *flv.Decoder

	// publish
	out     *egress
	outSink hub.Sink

	// opened is the connection once OnOpen has set the session up, for
	// the methods called from other goroutines; closing says Close came
	// first.
	opened  atomic.Pointer[fib.Connection]
	closing atomic.Bool
}

var _ hub.Sink = (*ClientConn)(nil)

// Errors a client reports.
var (
	ErrRejected = errors.New("rtmp: server refused")
)

// StatusError is an error status a server answered with.
type StatusError struct {
	Code, Description string
}

func (e *StatusError) Error() string {
	return "rtmp: " + e.Code + ": " + e.Description
}

func (e *StatusError) Unwrap() error { return ErrRejected }

// Publish dials the server of an RTMP URL and publishes to its stream.
// ready runs once, when the server has accepted the publish (from then on
// the ClientConn takes the stream through its hub.Sink methods) or with
// the error that stopped it. It runs on a fib worker, and must not block.
func Publish(engine *fib.Engine, rawURL string, config ClientConfig, ready func(*ClientConn, error)) (*ClientConn, error) {
	return dial(engine, rawURL, config, true, nil, ready)
}

// Play dials the server of an RTMP URL and plays its stream into sink,
// which hears the tracks, the packets, and OnClose when the stream or the
// connection ends. ready runs once, as for Publish.
func Play(engine *fib.Engine, rawURL string, config ClientConfig, sink hub.Sink, ready func(*ClientConn, error)) (*ClientConn, error) {
	return dial(engine, rawURL, config, false, sink, ready)
}

func dial(engine *fib.Engine, rawURL string, config ClientConfig, publishing bool, sink hub.Sink, ready func(*ClientConn, error)) (*ClientConn, error) {
	u, err := ParseURL(rawURL)
	if err != nil {
		return nil, err
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = DefaultDialTimeout
	}
	if config.HandshakeTimeout <= 0 {
		config.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.ChunkSize <= 0 {
		config.ChunkSize = DefaultChunkSize
	}
	if config.WriteQueueLimit <= 0 {
		config.WriteQueueLimit = DefaultWriteQueueLimit
	}
	if config.MaxWriteQueue <= 0 {
		config.MaxWriteQueue = DefaultMaxWriteQueue
	}
	if config.FlashVer == "" {
		config.FlashVer = "FMLE/3.0 (compatible; fib)"
	}
	if ready == nil {
		ready = func(*ClientConn, error) {}
	}
	cc := &ClientConn{config: config, url: u, publishing: publishing, sink: sink, ready: ready}
	if publishing {
		cc.out = &egress{s: &cc.session, enc: flv.NewEncoder(config.Encoder), setDataFrame: true}
		cc.outSink = hub.NewDropper(cc.out, cc.backlog, config.WriteQueueLimit)
	}
	h := &clientHandler{cc: cc}
	done := func(c *fib.Connection, err error) {
		if err != nil {
			cc.finish(err)
		}
	}
	if u.Scheme == "rtmps" {
		err = fibtls.Dial(engine, "tcp", u.Host, config.DialTimeout, config.TLSConfig, h, done)
	} else {
		err = engine.DialWithHandler("tcp", u.Host, config.DialTimeout, h, done)
	}
	if err != nil {
		return nil, err
	}
	return cc, nil
}

// finish reports the outcome of the dial, once.
func (cc *ClientConn) finish(err error) {
	cc.readyOnce.Do(func() { cc.ready(cc, err) })
}

// URL returns the URL the client dialed, taken apart.
func (cc *ClientConn) URL() *URL { return cc.url }

// Conn returns the fib connection, or nil before it has connected.
func (cc *ClientConn) Conn() *fib.Connection { return cc.opened.Load() }

// Close closes the connection. A publish that has started is ended first,
// with FCUnpublish and deleteStream, so that the server ends the stream as
// its publisher meant rather than as a dropped connection.
func (cc *ClientConn) Close() error {
	cc.closing.Store(true)
	c := cc.opened.Load()
	if c == nil {
		// OnOpen closes it.
		return nil
	}
	if cc.out != nil {
		cc.mu.Lock()
		live := cc.out.live && !cc.out.stopped
		cc.out.stopped = true
		var err error
		if live {
			name := cc.url.StreamName()
			if err = cc.conn.WriteCommand(0, "FCUnpublish", 0, nil, name); err == nil {
				err = cc.conn.WriteCommand(0, "deleteStream", 0, nil, float64(cc.out.streamID))
			}
		}
		cc.mu.Unlock()
		if live && err == nil {
			c.CloseAfterSend()
			return nil
		}
	}
	return c.Close()
}

// OnTracks takes the tracks of the stream a ClientConn publishes.
func (cc *ClientConn) OnTracks(tracks []*av.Track) {
	if cc.outSink != nil {
		cc.outSink.OnTracks(tracks)
	}
}

// OnPacket takes a packet of the stream a ClientConn publishes; it is
// dropped while the connection's backlog is over WriteQueueLimit.
func (cc *ClientConn) OnPacket(pkt *av.Packet) {
	if cc.outSink != nil && cc.opened.Load() != nil {
		cc.outSink.OnPacket(pkt)
	}
}

// OnClose ends a publish: the stream it was sending has ended, so the
// connection closes.
func (cc *ClientConn) OnClose(error) { _ = cc.Close() }

// clientHandler is the fib.Handler of a client's connection.
type clientHandler struct {
	cc *ClientConn
}

func (h *clientHandler) OnOpen(c *fib.Connection) {
	cc := h.cc
	cc.init(c, ConnConfig{Client: true, SimpleHandshake: cc.config.SimpleHandshake, MaxMessageSize: cc.config.MaxMessageSize}, cc.config.IdleTimeout)
	if cc.publishing {
		c.SetWriteQueueLimit(cc.config.MaxWriteQueue)
	}
	_ = c.SetReadDeadline(time.Now().Add(cc.config.HandshakeTimeout))
	_ = cc.write(func(conn *Conn) error { return conn.Start() })
	cc.opened.Store(c)
	if cc.closing.Load() {
		_ = c.Close()
	}
}

func (h *clientHandler) OnData(c *fib.Connection, data []byte) {
	cc := h.cc
	msgs, err := cc.feed(data)
	if err != nil {
		cc.fail(err)
		return
	}
	if cc.connectTx == 0 && cc.conn.HandshakeDone() {
		if err := cc.connect(); err != nil {
			cc.fail(err)
			return
		}
	}
	for i := range msgs {
		if err := cc.handle(&msgs[i]); err != nil {
			cc.fail(err)
			return
		}
	}
	if cc.ingest != nil {
		cc.touchRead()
	}
}

func (h *clientHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *clientHandler) OnClose(c *fib.Connection, err error) {
	cc := h.cc
	cc.mu.Lock()
	if cc.conn != nil {
		cc.conn.Close()
	}
	cc.mu.Unlock()
	if err == nil {
		err = ErrClosed
	}
	cc.finish(err)
	if cc.ingest != nil {
		cc.ingest.Flush()
		cc.ingest = nil
		cc.sink.OnClose(err)
	}
	if cc.started && cc.config.OnClose != nil {
		cc.config.OnClose(cc, err)
	}
}

// fail ends the connection with err, which ready hears if it has not run.
func (cc *ClientConn) fail(err error) {
	cc.finish(err)
	_ = cc.c.Close()
}

func (cc *ClientConn) tx() float64 {
	cc.nextTx++
	return cc.nextTx
}

func (cc *ClientConn) connect() error {
	cc.connectTx = cc.tx()
	obj := amf.Object{
		{Key: "app", Value: cc.url.App},
		{Key: "type", Value: "nonprivate"},
		{Key: "flashVer", Value: cc.config.FlashVer},
		{Key: "tcUrl", Value: cc.url.TcURL()},
	}
	if !cc.publishing {
		obj = append(obj,
			amf.Property{Key: "fpad", Value: false},
			amf.Property{Key: "capabilities", Value: 15.0},
			amf.Property{Key: "audioCodecs", Value: 4071.0},
			amf.Property{Key: "videoCodecs", Value: 252.0},
			amf.Property{Key: "videoFunction", Value: 1.0},
		)
	}
	// Enhanced RTMP: the codecs this client takes or sends by FourCC.
	obj = append(obj, amf.Property{Key: "fourCcList", Value: []any{"av01", "hvc1", "avc1", "Opus", "mp4a"}})
	return cc.write(func(c *Conn) error {
		return errors.Join(c.SetChunkSize(cc.config.ChunkSize), c.WriteCommand(0, "connect", cc.connectTx, obj))
	})
}

func (cc *ClientConn) handle(m *Message) error {
	switch m.Type {
	case TypeCommandAMF0, TypeCommandAMF3:
		cmd, err := ParseCommand(m)
		if err != nil {
			return nil
		}
		return cc.command(cmd)
	case TypeUserControl:
		if !cc.publishing && len(m.Payload) >= 2 && m.Payload[0] == 0 && m.Payload[1] == EventStreamEOF && cc.started {
			cc.c.CloseAfterSend()
		}
	case TypeAudio, TypeVideo, TypeDataAMF0, TypeDataAMF3:
		// Media is taken on any message stream: ffmpeg serving in listen
		// mode sends it on another stream than the one it created.
		if cc.ingest != nil {
			ingestMessage(cc.ingest, m)
		}
	}
	return nil
}

func (cc *ClientConn) command(cmd *Command) error {
	switch cmd.Name {
	case "_result":
		switch cmd.TransactionID {
		case cc.connectTx:
			return cc.connected()
		case cc.createTx:
			if len(cmd.Args) == 0 {
				return ErrCommand
			}
			id, _ := cmd.Args[0].(float64)
			cc.streamID = uint32(id)
			return cc.publishOrPlay()
		}
	case "_error":
		var info any
		if len(cmd.Args) > 0 {
			info = cmd.Args[0]
		}
		_, code, desc := statusCode(info)
		return &StatusError{Code: code, Description: desc}
	case "onStatus":
		var info any
		if len(cmd.Args) > 0 {
			info = cmd.Args[0]
		}
		level, code, desc := statusCode(info)
		switch {
		case level == "error":
			return &StatusError{Code: code, Description: desc}
		case code == "NetStream.Publish.Start" && cc.publishing,
			code == "NetStream.Play.Start" && !cc.publishing:
			cc.begin()
		case code == "NetStream.Play.Stop", code == "NetStream.Play.UnpublishNotify":
			if cc.started && !cc.publishing {
				cc.c.CloseAfterSend()
			}
		}
	case "onBWDone", "onFCPublish", "onFCUnpublish", "|RtmpSampleAccess":
	default:
		// Calls the server makes that this client does not implement are
		// answered, so that a server waiting on them does not stall.
		if cmd.TransactionID != 0 {
			return cc.write(func(c *Conn) error {
				return c.WriteCommand(0, "_result", cmd.TransactionID, nil, amf.Undefined{})
			})
		}
	}
	return nil
}

// connected follows the connect's _result with the commands that create
// the stream.
func (cc *ClientConn) connected() error {
	name := cc.url.StreamName()
	cc.createTx = 0
	return cc.write(func(c *Conn) error {
		var err error
		if cc.publishing {
			err = errors.Join(
				c.WriteCommand(0, "releaseStream", cc.tx(), nil, name),
				c.WriteCommand(0, "FCPublish", cc.tx(), nil, name))
		}
		cc.createTx = cc.tx()
		return errors.Join(err, c.WriteCommand(0, "createStream", cc.createTx, nil))
	})
}

func (cc *ClientConn) publishOrPlay() error {
	name := cc.url.StreamName()
	return cc.write(func(c *Conn) error {
		if cc.publishing {
			return c.WriteCommand(cc.streamID, "publish", 0, nil, name, "live")
		}
		return errors.Join(
			c.WriteCommand(cc.streamID, "play", 0, nil, name, -2000.0),
			c.WriteUserControl(EventSetBufferLength, cc.streamID, 3000))
	})
}

// begin starts the publish or play the server has accepted.
func (cc *ClientConn) begin() {
	if cc.started {
		return
	}
	cc.started = true
	_ = cc.c.SetReadDeadline(time.Time{})
	if cc.publishing {
		cc.out.start(cc.streamID)
	} else {
		cc.ingest = newIngest(cc.sink)
		cc.readAt = time.Time{}
		cc.touchRead()
	}
	cc.finish(nil)
}

// Relay is a hub stream pushed to an RTMP server, or an RTMP stream pulled
// into a hub.
type Relay struct {
	mu     sync.Mutex
	conn   *ClientConn
	sub    *hub.Subscription
	closed bool
}

// Conn returns the relay's client connection.
func (r *Relay) Conn() *ClientConn { return r.conn }

// Close ends the relay.
func (r *Relay) Close() {
	r.mu.Lock()
	r.closed = true
	sub := r.sub
	r.mu.Unlock()
	if sub != nil {
		sub.Close()
	}
	_ = r.conn.Close()
}

// Push publishes the hub stream at path to rawURL: once the server accepts
// the publish, it subscribes to path, with wait as SubscribeOptions.Wait,
// and sends what it hears. done runs once the push has started, or with the
// error that stopped it; config.OnClose hears the end of a push that
// started.
func Push(engine *fib.Engine, h *hub.Hub, path, rawURL string, wait time.Duration, config ClientConfig, done func(error)) (*Relay, error) {
	r := &Relay{}
	cc, err := Publish(engine, rawURL, config, func(cc *ClientConn, err error) {
		if err == nil {
			r.mu.Lock()
			closed := r.closed
			r.mu.Unlock()
			var sub *hub.Subscription
			if closed {
				err = ErrClosed
			} else {
				sub, err = h.Subscribe(path, cc, hub.SubscribeOptions{
					Info: hub.SubscribeInfo{Protocol: "rtmp-push", Remote: cc.c.RemoteAddrPort()},
					Wait: wait,
				})
			}
			if err != nil {
				_ = cc.Close()
			} else {
				r.mu.Lock()
				r.sub = sub
				r.mu.Unlock()
			}
		}
		if done != nil {
			done(err)
		}
	})
	if err != nil {
		return nil, err
	}
	r.conn = cc
	return r, nil
}

// Pull plays rawURL and publishes it to the hub at path, which is how a
// server relays a stream from another. done runs once the stream is
// published, or with the error that stopped it (hub.ErrStreamBusy when the
// path has a publisher); the hub stream is unpublished when the play ends.
func Pull(engine *fib.Engine, h *hub.Hub, rawURL, path string, config ClientConfig, done func(error)) (*Relay, error) {
	r := &Relay{}
	target := &publisherSink{}
	cc, err := Play(engine, rawURL, config, target, func(cc *ClientConn, err error) {
		if err == nil {
			var pub *hub.Publisher
			pub, err = h.Publish(path, nil, hub.PublishInfo{Protocol: "rtmp-pull", Remote: cc.c.RemoteAddrPort(), Query: cc.url.Query})
			if err != nil {
				_ = cc.Close()
			} else {
				target.pub = pub
			}
		}
		if done != nil {
			done(err)
		}
	})
	if err != nil {
		return nil, err
	}
	r.conn = cc
	return r, nil
}
