// Package tls runs TLS over the engine's connections, with crypto/tls doing
// the cryptography.
//
// A Handler sits in front of an ordinary fib.Handler and hands it plaintext.
// The wrapped handler sees an ordinary connection: its Send, SendOwned,
// SendParts, SendPartsShared and CloseAfterSend encrypt, and its OnData
// receives decrypted bytes, so protocol handlers such as the http and
// websocket packages run over TLS unchanged.
package tls

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/sidepool"
)

// DefaultHandshakeTimeout bounds a handshake when Handler leaves
// HandshakeTimeout at zero.
const DefaultHandshakeTimeout = 10 * time.Second

// readBufferSize holds the largest record crypto/tls will hand back from one
// Read, so a drain never splits a record's plaintext across two calls.
const readBufferSize = 16 << 10

// errWouldBlock is what the transport tells crypto/tls when a read would
// have to wait for bytes the socket has not delivered yet. crypto/tls treats a
// temporary net.Error as retryable and keeps the partial record it has already
// buffered, which is what makes a record that arrives across several rounds
// decode correctly.
var errWouldBlock error = wouldBlockError{}

type wouldBlockError struct{}

func (wouldBlockError) Error() string   { return "fib: tls read would block" }
func (wouldBlockError) Timeout() bool   { return false }
func (wouldBlockError) Temporary() bool { return true }

// Handler runs TLS on the connections it serves and hands its Handler the
// plaintext.
//
// OnOpen reaches Handler as soon as the TCP connection is established, before
// the handshake, and it may send at once: whatever it sends is held until the
// handshake completes and then encrypted in order. A handshake that fails or
// times out closes the connection, and Handler's OnClose receives the error.
// Plaintext the peer sent before it closed reaches OnData before OnClose, even
// what arrived along with the handshake, and OnClose never runs alongside
// OnHandshake.
//
// The handshake needs a round trip or two with the peer, and crypto/tls runs
// it as a blocking call, so each connection's handshake runs on a worker of a
// pool kept for handshakes alone, which waits there for the peer until the
// handshake completes. That pool is neither the engine's nor the one its
// handlers run on, so a handshake is never queued behind the work of the
// connections whose reads feed it, and an engine worker that opens a
// connection never waits on its own queue. Records after that are decrypted
// in OnData like any other input, straight from the bytes the round read.
//
// Handshakes can also run without a worker at all. A client whose Config
// allows nothing below TLS 1.2 (the default) and asks for nothing beyond
// client13.go and client12.go (crypto/tls's session resumption, encrypted
// client hello), and a server whose Config has certificates, its own or
// GetCertificate's, and no GetConfigForClient (nativeServer), take no worker
// for their handshake; either side may authenticate the client by its
// certificate. It is a state machine that OnData drives with the bytes that
// arrive and that never waits, so a peer that is slow to answer, or that never
// answers, costs the connection's memory and a timer, and no goroutine;
// thousands of handshakes can be in flight at once. GetCertificate and
// GetClientCertificate, which may block, run on a worker of the handshake pool
// for as long as they take, and the handshake goes on from there (auth.go); so
// do a client's verification of the server's chain, VerifyPeerCertificate and
// VerifyConnection. A worker the pool cannot take on at once, with its queue
// full, is not waited for: the connection is closed. A server serves TLS 1.3
// and 1.2 that way and hands the clients it cannot serve (older versions,
// 0-RTT, suites or groups it does not implement, a hello that does not parse)
// to crypto/tls, before it has sent anything, so they see no difference; it
// issues its own session tickets, sealed under a key of the Handler's. Other
// Configs keep to crypto/tls. The TLS handshake of a QUIC connection (quic.go)
// is the same state machines, with its messages and secrets as events.
//
// Once a crypto/tls handshake settles on an AES-GCM suite of TLS 1.3 or 1.2, or
// an AES-CBC suite of TLS 1.2 or 1.1, the connection's records are protected by
// this package rather than by crypto/tls, which is then done with; the keys
// come from crypto/tls through Config.KeyLogWriter. For that the Handler
// clones Config the first time it is used and runs its handshakes with the
// clone, whose KeyLogWriter still passes every line on to Config's own;
// changes made to Config after that are not seen, as crypto/tls asks of a
// Config in use anyway.
type Handler struct {
	Config  *stdtls.Config
	Handler fib.Handler
	// Client selects the client side of the handshake. Dialed connections
	// need it; accepted ones need it left false.
	Client bool
	// HandshakeTimeout bounds the handshake. Zero means
	// DefaultHandshakeTimeout and a negative value means no bound.
	HandshakeTimeout time.Duration
	// Blocking runs every handshake through crypto/tls on a worker of the
	// handshake pool, even where it could run without one. By default a
	// handshake that can run without a worker does; this is the switch to the
	// other, for a Config that behaves differently under the state machines
	// than under crypto/tls, or to compare the two.
	Blocking bool
	// TLS13CipherSuites are the TLS 1.3 cipher suites a client handshake
	// offers, by ID, in the order to prefer them: a ClientHello's first suite
	// is the one the client would rather have, and a server that follows its
	// client's preference, as crypto/tls does, takes it. Empty is the default
	// order, AES-GCM first where the CPU computes it in hardware and
	// ChaCha20-Poly1305 first where it does not, which is crypto/tls's. A
	// server handshake ignores it: a TLS 1.3 server settles on one of the
	// suites its client offered, which its own preference among them picks.
	// So does a handshake crypto/tls runs (see Blocking, or a Config this
	// package cannot serve), since crypto/tls has no field for the suites of
	// TLS 1.3. TLS 1.2's come from Config.CipherSuites, before and after.
	TLS13CipherSuites []uint16
	// EarlyData lets a server take the early data (0-RTT) of clients that
	// resume a session (RFC 8446 section 2.3), which it can do where the
	// handshake runs without a worker and the wrapped handler is an
	// EarlyDataHandler. Its tickets then allow early data, and a client that
	// uses a ticket sends its first bytes with its first flight, which the
	// server answers before the handshake is over. Early data is not forward
	// secret and can be replayed: a ticket carries it once, but a server that
	// did not issue the ticket cannot know, and the handler must serve only
	// what is safe to repeat. Off by default. An EarlyDataHandler that
	// implements EarlyDataOptIn turns it on for itself, as the HTTP server
	// handler does with http.Config.EarlyData, so that a server configured
	// at the protocol layer needs no second switch here.
	EarlyData bool
	// MaxEarlyData is the most early data a client may send on a ticket. Zero
	// means 16 KiB.
	MaxEarlyData uint32
	// NoKernelTLS keeps the sealing of every record in this package. By
	// default, on Linux, a connection that sends a file with SendFile, over
	// an AES-GCM suite of TLS 1.3 or 1.2, hands the sealing of what it sends
	// from then on to the kernel's TLS, when the kernel has it, so that the
	// file goes from the page cache to the socket by sendfile; see
	// Connection.SendFile in package fib.
	NoKernelTLS bool
	// SessionCache, on a client, is where tickets go and sessions come from,
	// for client handshakes that run without a worker: a connection then
	// resumes the session of one before it. With EarlyData also set, what is
	// written before the handshake is over is sent as early data on a ticket
	// that allows it, with the first flight, and sent again if the server turns
	// it away. A server that takes it may be sent it twice by an attacker, so
	// only what is safe to repeat should be written then.
	SessionCache QUICSessionCache

	// fastConfig is the clone of Config handshakes run with where the
	// connection may leave crypto/tls afterwards; see fast.go.
	fastOnce   sync.Once
	fastConfig *stdtls.Config
	fastReg    *registry
	// tickets are the keys server handshakes without a worker use.
	ticketOnce sync.Once
	tickets    *ticketKeys
}

// NewServer returns a handler that serves TLS with config in front of
// handler, for a fib.Binding.
func NewServer(config *stdtls.Config, handler fib.Handler) *Handler {
	return &Handler{Config: config, Handler: handler}
}

// NewClient returns a handler that runs the client side of TLS with config in
// front of handler, for fib.Engine.DialWithHandler. config needs ServerName,
// or InsecureSkipVerify, as it does for crypto/tls.Client; Dial fills
// ServerName in.
func NewClient(config *stdtls.Config, handler fib.Handler) *Handler {
	return &Handler{Config: config, Handler: handler, Client: true}
}

// Dial is engine.DialWithHandler over TLS. When config names no server, the
// host in addr is used, as crypto/tls.Dial does, or the whole of a unix
// address. A nil handler means the engine's, which does nothing. done runs
// once the connect completes, as it does for fib.Engine.Dial; the handshake
// follows, and what done sends waits for it.
func Dial(engine *fib.Engine, network, addr string, timeout time.Duration, config *stdtls.Config,
	handler fib.Handler, done func(*fib.Connection, error)) error {
	return DialWithOptions(engine, network, addr, timeout, config, DialOptions{}, handler, done)
}

// DialOptions are the choices a dial has beyond Dial's.
type DialOptions struct {
	// Blocking runs the handshake through crypto/tls on a worker of the
	// handshake pool; see Handler.Blocking. The default is to handshake
	// without a worker where the Config allows it.
	Blocking bool
	// HandshakeTimeout bounds the handshake; see Handler.HandshakeTimeout.
	HandshakeTimeout time.Duration
	// SessionCache and EarlyData are Handler's.
	SessionCache QUICSessionCache
	EarlyData    bool
	// TLS13CipherSuites is Handler's.
	TLS13CipherSuites []uint16
	// Dial opens the connection the handshake runs over, in place of
	// engine.DialWithHandler, with the same arguments and the same contract;
	// a socks.Dialer's Dial reaches the server through a proxy. Nil dials
	// directly.
	Dial func(engine *fib.Engine, network, addr string, timeout time.Duration,
		handler fib.Handler, done func(*fib.Connection, error)) error
}

// DialWithOptions is Dial with options.
func DialWithOptions(engine *fib.Engine, network, addr string, timeout time.Duration, config *stdtls.Config,
	options DialOptions, handler fib.Handler, done func(*fib.Connection, error)) error {
	if config == nil {
		config = &stdtls.Config{}
	}
	if config.ServerName == "" {
		// A unix address is a path, which on Windows has a colon after its
		// drive letter: it is the name whole.
		host, _, err := net.SplitHostPort(addr)
		if err != nil || strings.HasPrefix(network, "unix") {
			host = addr
		}
		config = config.Clone()
		config.ServerName = host
	}
	if handler == nil {
		handler = engine.Handler()
	}
	client := NewClient(config, handler)
	client.Blocking, client.HandshakeTimeout = options.Blocking, options.HandshakeTimeout
	client.SessionCache, client.EarlyData = options.SessionCache, options.EarlyData
	client.TLS13CipherSuites = options.TLS13CipherSuites
	if options.Dial != nil {
		return options.Dial(engine, network, addr, timeout, client, done)
	}
	return engine.DialWithHandler(network, addr, timeout, client, done)
}

func (h *Handler) inner() fib.Handler {
	if h.Handler == nil {
		return fib.HandlerFuncs{}
	}
	return h.Handler
}

// takesEarlyData reports whether a server handshake should take early data:
// Handler.EarlyData says so, and so does the wrapped handler when it is an
// EarlyDataOptIn that wants it; see EarlyDataOptIn.
func (h *Handler) takesEarlyData() bool {
	if h.EarlyData {
		return true
	}
	opt, ok := h.inner().(EarlyDataOptIn)
	return ok && opt.EarlyDataEnabled()
}

// config is the Config handshakes run with, and the registry of the
// connections that may leave crypto/tls once theirs completes, or nil.
func (h *Handler) config() (*stdtls.Config, *registry) {
	h.fastOnce.Do(func() { h.fastConfig, h.fastReg = fastConfig(h.Config, h.Client) })
	if h.fastConfig != nil {
		return h.fastConfig, h.fastReg
	}
	return h.Config, nil
}

func (h *Handler) OnOpen(c *fib.Connection) {
	t := &layer{c: c, client: h.Client, handshaking: true, settling: true, owner: h}
	t.cond.L = &t.mu
	switch {
	case !h.Blocking && h.Client && nativeClient(h.Config):
		h.openNative(c, t)
	case !h.Blocking && !h.Client && nativeServer(h.Config):
		h.openNative(c, t)
	default:
		c.SetLayer(t)
		h.inner().OnOpen(c)
		h.startBlocking(t, nil)
	}
}

// startBlocking runs the handshake with crypto/tls on a worker of the
// handshake pool. initial is what the peer has sent already, which a server
// that began without a worker and then found it could not serve the client
// hands over; crypto/tls reads it as if it had been there all along.
func (h *Handler) startBlocking(t *layer, initial []byte) {
	config, reg := h.config()
	if reg != nil {
		t.capture = &capture{client: h.Client, reg: reg}
	}
	if h.Client {
		t.conn = stdtls.Client(t, config)
	} else {
		t.conn = stdtls.Server(t, config)
	}
	if len(initial) > 0 {
		t.mu.Lock()
		t.in = initial
		t.inHead = 0
		t.mu.Unlock()
	}
	timeout := h.HandshakeTimeout
	if timeout == 0 {
		timeout = DefaultHandshakeTimeout
	}
	inner := h.inner()
	if !sidepool.Handshake().TryGo(func() { t.handshake(inner, timeout) }) {
		t.mu.Lock()
		t.handshaking = false
		t.mu.Unlock()
		t.settle(inner)
		t.c.CloseWithError(errNoHandshakeWorker)
	}
}

// ticketKeys are the keys the Handler's server handshakes that run without a
// worker seal their session tickets under, made the first time they are
// needed, or nil if the Config disables tickets.
func (h *Handler) ticketKeys() *ticketKeys {
	h.ticketOnce.Do(func() { h.tickets = newTicketKeys(h.Config) })
	return h.tickets
}

// nativeBox holds a handshake that runs without a worker.
type nativeBox struct{ hs nativeHandshake }

// nativeStarted, set by tests, is told of each handshake that runs without a
// worker.
var nativeStarted func()

// nativeFellBack, set by tests, is told of each server handshake that began
// without a worker and was handed to crypto/tls.
var nativeFellBack func()

// openNative begins a handshake that runs without a worker. A client's
// ClientHello goes out now; a server waits for the peer's. OnData carries the
// handshake on from there.
func (h *Handler) openNative(c *fib.Connection, t *layer) {
	if nativeStarted != nil {
		nativeStarted()
	}
	var hs nativeHandshake
	var hello []byte
	if h.Client {
		chs, err := newClientHS(h.Config)
		if err != nil {
			c.SetLayer(t)
			h.inner().OnOpen(c)
			// No handshake will settle the layer, so it is settled here, and
			// the close goes straight to the wrapped handler.
			t.settle(h.inner())
			c.CloseWithError(err)
			return
		}
		if len(h.TLS13CipherSuites) > 0 {
			// Offered in this order, in place of the default the ClientHello
			// would have.
			chs.suites = slices.Clone(h.TLS13CipherSuites)
		}
		if h.SessionCache != nil {
			chs.sessions = h.SessionCache
			if chs.pickSession() {
				chs.offerEarly = h.EarlyData && chs.session.maxEarly > 0
			}
		}
		chs.pick.async = t.async(h.inner())
		chs.check.async = chs.pick.async
		hs, hello = chs, chs.hello()
		if chs.earlyActive {
			t.early = &earlyWrites{keys: chs.earlyTx, max: chs.session.maxEarly, active: true}
			chs.sealEarly = t.sealEarly
			chs.decided = t.earlyDecided
		}
	} else {
		shs := newServerHS(h.Config, c, h.ticketKeys())
		shs.pick.async = t.async(h.inner())
		if _, ok := h.inner().(EarlyDataHandler); ok && h.takesEarlyData() {
			shs.tcpEarly, shs.maxEarly = true, cmpOr(h.MaxEarlyData, 16<<10)
		}
		hs = shs
	}
	c.SetLayer(t)
	if t.early != nil {
		// What the handler writes in OnOpen follows the hello as early data.
		if err := c.SendRaw(hello); err != nil {
			h.inner().OnOpen(c)
			t.settle(h.inner())
			c.CloseWithError(err)
			return
		}
		hello = nil
	}
	h.inner().OnOpen(c)
	timeout := h.HandshakeTimeout
	if timeout == 0 {
		timeout = DefaultHandshakeTimeout
	}
	if timeout > 0 {
		t.hsTimer = time.AfterFunc(timeout, func() {
			if t.native.Load() != nil {
				c.CloseWithError(errHandshakeTimeout)
			}
		})
	}
	t.native.Store(&nativeBox{hs})
	if hello != nil {
		if err := c.SendRaw(hello); err != nil {
			c.CloseWithError(err)
		}
	}
}

func (h *Handler) OnData(c *fib.Connection, data []byte) {
	if t, ok := c.Layer().(*layer); ok {
		t.feed(h.inner(), data)
	}
}

// OnPriorityData passes out-of-band bytes through untouched: they travel
// beside the TLS stream, not inside it.
func (h *Handler) OnPriorityData(c *fib.Connection, data []byte) {
	h.inner().OnPriorityData(c, data)
}

// OnHangup passes word of a peer that has gone on to the wrapped handler,
// when it takes it: its handler may be waiting in OnData for the round the
// hangup is read in, which runs under this handler's own; see
// fib.HangupHandler.
func (h *Handler) OnHangup(c *fib.Connection) {
	if inner, ok := h.inner().(fib.HangupHandler); ok {
		inner.OnHangup(c)
	}
}

// OnShutdown passes word that the engine is shutting down on to the wrapped
// handler, when it takes it, so that the protocol inside the TLS stream can
// end the connection once it is idle; see fib.ShutdownHandler.
func (h *Handler) OnShutdown(c *fib.Connection) {
	if inner, ok := h.inner().(fib.ShutdownHandler); ok {
		inner.OnShutdown(c)
	}
}

// OnEngineClose passes word that Engine.Close is about to end the connection
// on to the wrapped handler, when it takes it, so that the protocol inside
// the TLS stream can write its last frame first, as an HTTP/2 server does
// with GOAWAY; see fib.EngineCloseHandler. What the handler sends here is
// encrypted by the layer as anything else it sends is.
func (h *Handler) OnEngineClose(c *fib.Connection) {
	if inner, ok := h.inner().(fib.EngineCloseHandler); ok {
		inner.OnEngineClose(c)
	}
}

func (h *Handler) OnClose(c *fib.Connection, err error) {
	if t, ok := c.Layer().(*layer); ok {
		// Once the layer has closed, which closing marks, no Send holds
		// anything more.
		defer t.dropHeld()
		err = t.reason(err)
		if t.abortNative() {
			// A handshake without a worker that did not finish: there is
			// no plaintext to deliver first, and no worker to report it.
			h.inner().OnClose(c, err)
			return
		}
		if !t.closing(err) {
			// The handshake's worker reports the close, once it has delivered
			// what arrived with the handshake.
			return
		}
	}
	h.inner().OnClose(c, err)
}

// EarlyDataHandler is implemented by a wrapped handler that takes the early
// data of Handler.EarlyData. OnEarlyData receives it as it arrives, in order,
// after OnHandshake and before any OnData, while the handshake is still going
// on: the client is not yet authenticated, and what it sent can be a replay
// by an attacker. The connection can be written to; the answers go out
// as soon as the server's side of the handshake is.
type EarlyDataHandler interface {
	OnEarlyData(*fib.Connection, []byte)
}

// EarlyDataOptIn is an EarlyDataHandler that turns early data on for itself,
// where Handler.EarlyData left it off: a protocol layer with its own switch
// for it, such as the HTTP server's Config.EarlyData, does not then need the
// server to set both. Early data is taken when either says so; a handler that
// implements both must serve only what is safe to repeat, as with
// Handler.EarlyData.
type EarlyDataOptIn interface {
	EarlyDataEnabled() bool
}

// earlyServer is what a server handshake that takes early data gives the
// layer.
type earlyServer interface {
	takeEarly() []byte
	earlyWriteKeys() *recordKeys
	takeTicket() []byte
}

// HandshakeHandler is implemented by a wrapped handler that wants to know
// when the handshake completes, such as one that speaks whichever protocol
// ALPN negotiated. OnHandshake runs once, on the handshake's worker, after
// what OnOpen sent has been encrypted and before OnData receives any
// plaintext. A handshake that fails reaches OnClose instead.
type HandshakeHandler interface {
	OnHandshake(*fib.Connection, stdtls.ConnectionState)
}

// layer sits between a connection's socket and crypto/tls. To crypto/tls it is
// the transport: Read serves the ciphertext OnData collected and Write queues
// records on the connection. To the connection it is the fib.Layer that
// encrypts what is sent.
type layer struct {
	c    *fib.Connection
	conn *stdtls.Conn
	// client is the side of the handshake the connection took.
	client bool

	// capture collects what taking the connection over from crypto/tls
	// needs, during the handshake of one that may be. Once it has been, rx
	// and tx protect its records, state is what ConnectionState reports, and
	// conn is nil; see fast.go. The read side's fields, pend, the start of a
	// record a round ended in, hsBuf, the start of a handshake message, and
	// useless, the records in a row with no application data, are readMu's.
	capture *capture
	rx, tx  *recordKeys
	state   *stdtls.ConnectionState
	pend    []byte
	hsBuf   []byte
	useless int

	// mu guards the ciphertext and the handshake and closed flags. cond wakes
	// a handshake waiting for the peer.
	//
	// in holds ciphertext that arrived while the handshake was running, or
	// that a drain left unread, from inHead on, and goes back to the pool once
	// it is empty. src is the ciphertext of the round being drained: the
	// engine's own buffer, which Read serves crypto/tls from directly, after
	// in, rather than copying it into one kept per connection; it is set only
	// for the length of a drain. rec tracks where the records in that stream
	// begin and end; see Read.
	//
	// settling holds from OnOpen until the handshake's worker has delivered
	// what arrived with the handshake. A close in the meantime leaves in to
	// that worker, and closeHeld and closeErr to report once it is done, so
	// that the wrapped handler hears of the close after the plaintext the
	// peer sent before it, and never while OnHandshake runs.
	mu          sync.Mutex
	cond        sync.Cond
	in          []byte
	inHead      int
	src         []byte
	rec         recordCursor
	handshaking bool
	settling    bool
	closed      bool
	closeHeld   bool
	closeErr    error
	// failErr is why the layer itself ended the connection, recorded before
	// it tells the peer so; see failWith.
	failErr error

	// native is the handshake of a client that runs without a worker, until it
	// completes or fails; see client13.go. nbuf holds the start of a record
	// that has not arrived whole, and hsTimer the handshake's timeout. The
	// state machine itself is only touched by OnData, under readMu.
	native  atomic.Pointer[nativeBox]
	owner   *Handler
	nbuf    []byte
	hsTimer *time.Timer
	nfailed bool
	// early is a client's early data, guarded by wmu; sink takes the tickets
	// that follow the handshake, and is readMu's.
	early *earlyWrites
	sink  *ticketSink
	// earlyOpened says a server that takes early data has begun to send
	// ahead of the end of its handshake, and notified that OnHandshake has run.
	earlyOpened bool

	// readMu makes whoever decrypts, the handshake's worker for the bytes
	// that arrived with the handshake or a worker afterwards, the only one
	// delivering plaintext, so OnData never runs twice at once.
	readMu sync.Mutex

	// wmu orders sends. Until ready, they wait in pending in the order they
	// were made, and closeAfterSend remembers a CloseAfterSend among them.
	wmu            sync.Mutex
	ready          bool
	failed         bool
	pending        [][]byte
	closeAfterSend bool
	// held is the plaintext sent while the connection is corked, sealed
	// into one record when the cork comes off; see holdLocked. It is wmu's.
	held []byte
	// ktls is where the connection stands with the kernel's TLS, which the
	// first file it sends may hand its sealing to (see kernelSealsLocked),
	// and rekeyOwed a KeyUpdate the peer asked for that the kernel has not
	// sent yet. Both are wmu's.
	ktls      uint8
	rekeyOwed bool
}

// The states of layer.ktls.
const (
	// ktlsUntried: this package seals, and the next file may change that.
	ktlsUntried uint8 = iota
	// ktlsOn: the kernel seals every record sent from here on.
	ktlsOn
	// ktlsOff: this package seals, and goes on doing so.
	ktlsOff
)

func (t *layer) handshake(handler fib.Handler, timeout time.Duration) {
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	err := t.conn.HandshakeContext(ctx)

	t.wmu.Lock()
	if err == nil {
		t.takeOver()
	}
	if t.capture != nil {
		t.capture.release()
		t.capture = nil
	}
	pending, closeAfterSend := t.pending, t.closeAfterSend
	t.pending = nil
	for i, held := range pending {
		if err == nil {
			err = t.writeLocked(held, nil)
		}
		bufferpool.Put(held)
		pending[i] = nil
	}
	if err != nil {
		t.failed = true
	} else {
		t.ready = true
		if closeAfterSend {
			t.closeNotifyLocked()
		}
	}
	t.wmu.Unlock()

	if err == nil {
		// Before any plaintext reaches OnData, so that a handler can pick its
		// protocol from what ALPN chose.
		if h, ok := handler.(HandshakeHandler); ok {
			h.OnHandshake(t.c, t.connectionState())
		}
	}
	t.mu.Lock()
	t.handshaking = false
	t.mu.Unlock()
	if err != nil {
		t.failWith(err)
		t.settle(handler)
		t.c.CloseWithError(err)
		return
	}
	// The peer may have sent application data right behind its last handshake
	// message, which feed collected while the handshake ran. Nothing raises
	// another read for it, so it is delivered here, even if the connection
	// has closed since: the peer sent it before it closed.
	t.readMu.Lock()
	t.drainLocked(handler)
	t.readMu.Unlock()
	t.settle(handler)
}

// settle ends the handshake's part: what arrived with it has been delivered,
// so a close from now on goes straight to handler, and one that came before
// is reported here.
func (t *layer) settle(handler fib.Handler) {
	t.mu.Lock()
	t.settling = false
	held, err := t.closeHeld, t.closeErr
	t.closeHeld, t.closeErr = false, nil
	if t.failErr != nil {
		err = t.failErr
	}
	if t.closed {
		t.releaseInLocked()
	}
	t.mu.Unlock()
	if held {
		handler.OnClose(t.c, err)
	}
}

// feed takes ciphertext from a read round. During the handshake it collects
// it and wakes the handshake's worker; afterwards it decrypts what it can.
// data is only borrowed for the call: crypto/tls keeps an incomplete record's
// bytes itself, so nothing of data outlives it unless the drain stops early.
func (t *layer) feed(handler fib.Handler, data []byte) {
	t.readMu.Lock()
	defer t.readMu.Unlock()
	if box := t.native.Load(); box != nil {
		t.feedNative(handler, box.hs, data)
		return
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	if t.handshaking {
		t.in = bufferpool.Append(t.in, data)
		t.cond.Signal()
		t.mu.Unlock()
		return
	}
	t.src = data
	t.mu.Unlock()
	defer t.releaseSrc()
	t.drainLocked(handler)
}

// drainLocked decrypts every complete record in what was collected before and
// in the round's bytes, and hands the plaintext to handler. The caller holds
// readMu.
//
// Read hands crypto/tls one record at a time, so once a Read has returned
// plaintext crypto/tls holds no ciphertext it has not decrypted, and the
// drain ends as soon as nothing is left to hand it, without the further
// Read that would only have reported errWouldBlock.
func (t *layer) drainLocked(handler fib.Handler) {
	if !t.hasInput() {
		return
	}
	if t.rx != nil {
		t.drainFast(handler)
		return
	}
	buf := bufferpool.Get(readBufferSize)
	defer bufferpool.Put(buf)
	for {
		n, err := t.conn.Read(buf)
		if n > 0 {
			handler.OnData(t.c, buf[:n])
		}
		if err == nil {
			if !t.hasInput() {
				return
			}
			continue
		}
		if err == errWouldBlock {
			return
		}
		// io.EOF is close_notify: the peer has finished cleanly.
		if err != io.EOF {
			t.failWith(err)
		}
		t.c.CloseWithError(err)
		return
	}
}

// hasInput reports whether ciphertext is waiting that crypto/tls has not been
// handed yet.
func (t *layer) hasInput() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.in) > t.inHead || len(t.src) > 0
}

// releaseSrc hands the round's buffer back to the engine at the end of a
// drain, keeping a copy of whatever crypto/tls did not read, which only a
// drain that stopped early leaves.
func (t *layer) releaseSrc() {
	t.mu.Lock()
	if len(t.src) > 0 && !t.closed {
		t.in = bufferpool.Append(t.in, t.src)
	}
	t.src = nil
	t.mu.Unlock()
}

// shutdown wakes a handshake still waiting on the peer, which then fails
// once it has read what already arrived.
func (t *layer) shutdown() {
	t.mu.Lock()
	t.closeLocked()
	t.mu.Unlock()
}

// closing is shutdown for Handler.OnClose. It reports whether the wrapped
// handler hears of the close now; otherwise the handshake's worker reports it
// with err once it is done.
// failWith records err as why the layer ends the connection, before the
// alert that tells the peer goes out: the peer hangs up in answer, which can
// close the connection before the layer's own close does, and OnClose then
// reports err rather than the end of input. The first reason stands.
func (t *layer) failWith(err error) {
	t.mu.Lock()
	if t.failErr == nil {
		t.failErr = err
	}
	t.mu.Unlock()
}

// reason is err, the reason a close came with, or the one failWith recorded
// before it.
func (t *layer) reason(err error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.failErr != nil {
		return t.failErr
	}
	return err
}

func (t *layer) closing(err error) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeLocked()
	if t.settling {
		t.closeHeld, t.closeErr = true, err
		return false
	}
	return true
}

func (t *layer) closeLocked() {
	t.closed = true
	if !t.settling {
		t.releaseInLocked()
	}
	t.cond.Broadcast()
}

func (t *layer) releaseInLocked() {
	bufferpool.Put(t.in)
	t.in, t.inHead = nil, 0
}

// earlyWrites is what a client has sent as early data: under which keys, how
// much the ticket allows, how much has gone, whether more may while the server
// has not answered, and a copy of it all in case it has to be sent again.
type earlyWrites struct {
	keys     *recordKeys
	max      uint32
	sent     uint32
	active   bool
	accepted bool
	copies   [][]byte
}

// sealEarly seals a handshake message under the early keys, for the handshake
// to send; the early data's own records use the same keys, so it is done under
// the lock they are.
func (t *layer) sealEarly(msg []byte) ([]byte, error) {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.early.keys.seal(nil, recordTypeHandshake, msg, nil)
}

// earlyDecided is the handshake telling that the server has taken the early
// data or turned it away: no more is sent as early data either way.
func (t *layer) earlyDecided(accepted bool) {
	t.wmu.Lock()
	t.early.active, t.early.accepted = false, accepted
	t.wmu.Unlock()
}

// Send encrypts plaintext, or holds a copy of it until the handshake is done.
func (t *layer) Send(first, second []byte) error {
	if len(first)+len(second) == 0 {
		return nil
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.failed || t.closeAfterSend || t.isClosed() {
		return syscall.EPIPE
	}
	if !t.ready {
		if e := t.early; e != nil && e.active && uint64(e.sent)+uint64(len(first)+len(second)) <= uint64(e.max) {
			e.sent += uint32(len(first) + len(second))
			e.copies = append(e.copies, bufferpool.Join(nil, first, second))
			return t.sealSendWith(e.keys, recordTypeApplicationData, first, second)
		}
		t.pending = append(t.pending, bufferpool.Join(nil, first, second))
		return nil
	}
	if t.tx != nil && t.c.Corked() {
		return t.holdLocked(first, second)
	}
	return t.writeLocked(first, second)
}

// writeLocked encrypts first followed by second and sends them. The caller
// holds wmu.
func (t *layer) writeLocked(first, second []byte) error {
	if t.tx != nil {
		return t.sealSendLocked(recordTypeApplicationData, first, second)
	}
	data := first
	if len(second) > 0 {
		// One record for both parts: a frame header sent as a record of its
		// own would cost more in record overhead than it carries. The joined
		// copy comes from the pool rather than from a buffer kept per
		// connection: crypto/tls has encrypted it by the time Write returns,
		// and at high connection counts a retained buffer apiece costs far
		// more than taking one for the length of a call.
		joined := bufferpool.Join(nil, first, second)
		defer bufferpool.Put(joined)
		data = joined
	}
	_, err := t.conn.Write(data)
	return err
}

// CloseAfterSend ends the stream with close_notify behind what was already
// sent, and then closes the connection once it has all been written.
func (t *layer) CloseAfterSend() {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.closeAfterSend {
		return
	}
	t.closeAfterSend = true
	if t.ready {
		t.closeNotifyLocked()
	} else if t.failed {
		t.c.Close()
	}
}

func (t *layer) closeNotifyLocked() {
	if t.tx != nil {
		_ = t.sealSendLocked(recordTypeAlert, []byte{alertLevelWarning, alertCloseNotify}, nil)
	} else {
		_ = t.conn.CloseWrite()
	}
	t.c.CloseAfterSendRaw()
}

// connectionState is what ConnectionState reports once the handshake has
// completed.
func (t *layer) connectionState() stdtls.ConnectionState {
	if t.state != nil {
		return *t.state
	}
	return t.conn.ConnectionState()
}

// isClosed reports whether the connection has closed.
func (t *layer) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// Read is crypto/tls reading the transport. During the handshake it waits for
// the peer; afterwards it never waits, and reports errWouldBlock instead.
//
// It never hands over more than the rest of the record under way. crypto/tls
// reads into a buffer it keeps for the life of the connection and grows to
// fit whatever it is handed; given a round's worth of pipelined records at
// once, that buffer would grow to the size of the round, and stay that size,
// where one record at a time keeps it at the size of a record.
func (t *layer) Read(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for len(t.in) == t.inHead {
		if t.closed {
			return 0, io.EOF
		}
		if len(t.src) > 0 {
			n := t.rec.take(p, t.src)
			t.src = t.src[n:]
			if t.capture != nil {
				t.capture.readBytes(p[:n])
			}
			return n, nil
		}
		if !t.handshaking {
			return 0, errWouldBlock
		}
		t.cond.Wait()
	}
	n := t.rec.take(p, t.in[t.inHead:])
	t.inHead += n
	if t.capture != nil {
		t.capture.readBytes(p[:n])
	}
	if t.inHead == len(t.in) {
		// Emptied, the buffer goes back to the pool rather than staying with
		// the connection: once the handshake is over it is only needed
		// again when a drain stops early, and at high connection counts a
		// buffer kept apiece costs far more than taking one when it is.
		t.releaseInLocked()
	}
	return n, nil
}

// recordCursor follows the TLS record framing of the ciphertext stream, so
// that Read can stop at the end of each record. A record is a five-byte
// header, whose last two bytes are the length of the body that follows.
type recordCursor struct {
	header [5]byte
	// headerLen is how much of the current record's header has been handed
	// over, and bodyLeft how much of its body has not.
	headerLen int
	bodyLeft  int
}

// take copies into p what src holds of the current record, as much as p has
// room for, and reports how many bytes it copied.
func (r *recordCursor) take(p, src []byte) int {
	if len(src) > len(p) {
		src = src[:len(p)]
	}
	n := 0
	if r.headerLen < len(r.header) {
		k := copy(r.header[r.headerLen:], src)
		r.headerLen += k
		n = k
		if r.headerLen < len(r.header) {
			return copy(p, src[:n])
		}
		r.bodyLeft = int(r.header[3])<<8 | int(r.header[4])
	}
	k := min(r.bodyLeft, len(src)-n)
	r.bodyLeft -= k
	n += k
	if r.bodyLeft == 0 {
		r.headerLen = 0
	}
	return copy(p, src[:n])
}

// Write is crypto/tls writing records. The connection copies them, since
// crypto/tls reuses its buffer for the next record.
func (t *layer) Write(p []byte) (int, error) {
	if t.capture != nil {
		t.capture.wrote(p)
	}
	if err := t.c.SendRaw(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close is crypto/tls abandoning the transport, which it does when the
// handshake context expires.
func (t *layer) Close() error {
	t.shutdown()
	t.c.CloseWithError(errHandshakeTimeout)
	return nil
}

var errHandshakeTimeout = errors.New("fib: tls handshake timed out")

// errNoHandshakeWorker closes a connection whose handshake the handshake pool
// refused to run: it has stopped, or its queue is full, which the goroutine
// that hands it work, often an event loop, does not wait out.
var errNoHandshakeWorker = errors.New("fib: tls handshake pool is full or has stopped")

func (t *layer) LocalAddr() net.Addr { return addr{} }

func (t *layer) RemoteAddr() net.Addr {
	if remote := t.c.RemoteAddr(); remote != nil {
		return remote
	}
	return addr{}
}

func (t *layer) SetDeadline(time.Time) error      { return nil }
func (t *layer) SetReadDeadline(time.Time) error  { return nil }
func (t *layer) SetWriteDeadline(time.Time) error { return nil }

// addr stands in for an address the transport cannot report.
type addr struct{}

func (addr) Network() string { return "tcp" }
func (addr) String() string  { return "fib" }

// ConnectionState reports the connection's TLS parameters, such as the
// negotiated protocol and the peer's certificates. It reports false for a
// connection without TLS and for one whose handshake has not completed.
func ConnectionState(c *fib.Connection) (stdtls.ConnectionState, bool) {
	t, ok := c.Layer().(*layer)
	if !ok {
		return stdtls.ConnectionState{}, false
	}
	t.wmu.Lock()
	ready := t.ready
	t.wmu.Unlock()
	if !ready {
		return stdtls.ConnectionState{}, false
	}
	return t.connectionState(), true
}

// Handshaking reports whether c carries TLS whose handshake has not completed
// yet. A wrapped handler's OnOpen runs before the handshake, and a handler
// that times what the connection waits for after it, such as an HTTP server
// timing its idleness, can leave the handshake to HandshakeTimeout and start
// timing in OnHandshake; see HandshakeHandler.
func Handshaking(c *fib.Connection) bool {
	t, ok := c.Layer().(*layer)
	if !ok {
		return false
	}
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return !t.ready && !t.failed
}

// feedNative carries a handshake that runs without a worker on with the bytes
// a round read. The handshake never waits: whatever does not make a whole
// record is kept until the next round, and when it completes the bytes behind
// the Finished go the way every later round's do.
func (t *layer) feedNative(handler fib.Handler, hs nativeHandshake, data []byte) {
	if t.nfailed || t.isClosed() {
		return
	}
	buf := data
	if len(t.nbuf) > 0 {
		t.nbuf = bufferpool.Append(t.nbuf, data)
		buf = t.nbuf
	}
	consumed, out, done, herr := hs.process(buf)
	if herr == errFallback {
		t.fallbackBlocking(buf)
		return
	}
	if len(out) > 0 {
		if err := t.c.SendRaw(out); err != nil {
			t.nativeFailed(hs, nil, err)
			return
		}
	}
	if herr != nil {
		t.nativeFailed(hs, herr, herr.err)
		return
	}
	if es, ok := hs.(earlyServer); ok && !t.serveEarly(handler, hs, es) {
		return
	}
	rest := buf[consumed:]
	if !done {
		if len(t.nbuf) > 0 {
			t.nbuf = t.nbuf[:copy(t.nbuf, rest)]
		} else if len(rest) > 0 {
			t.nbuf = bufferpool.Append(nil, rest)
		}
		return
	}
	t.completeNative(handler, hs, rest)
}

// async is how a handshake that runs without a worker calls a certificate
// callback of the Config, which may block (see auth.go): on a worker of the
// handshake pool, which then carries the handshake on from where it stopped,
// with whatever has arrived meanwhile, as OnData would have.
func (t *layer) async(handler fib.Handler) func(call func()) bool {
	return func(call func()) bool {
		return sidepool.Handshake().TryGo(func() {
			call()
			t.readMu.Lock()
			defer t.readMu.Unlock()
			if box := t.native.Load(); box != nil {
				t.feedNative(handler, box.hs, nil)
			}
		})
	}
}

// serveEarly sees to a server that takes early data: from the end of its flight
// it sends under its application keys, so the writes that waited go out, the
// handler learns what was negotiated, and the early data the client has sent
// is delivered. It reports false if the connection failed meanwhile.
func (t *layer) serveEarly(handler fib.Handler, hs nativeHandshake, es earlyServer) bool {
	if !t.earlyOpened {
		keys := es.earlyWriteKeys()
		if keys == nil {
			return true
		}
		t.earlyOpened = true
		state := hs.connectionState()
		t.state = &state
		t.wmu.Lock()
		t.tx = keys
		pending, closeAfterSend := t.pending, t.closeAfterSend
		t.pending = nil
		var err error
		for i, held := range pending {
			if err == nil {
				err = t.writeLocked(held, nil)
			}
			bufferpool.Put(held)
			pending[i] = nil
		}
		if err != nil {
			t.failed = true
		} else {
			t.ready = true
			if closeAfterSend {
				t.closeNotifyLocked()
			}
		}
		t.wmu.Unlock()
		if err != nil {
			t.nativeFailed(hs, nil, err)
			return false
		}
		if h, ok := handler.(HandshakeHandler); ok {
			h.OnHandshake(t.c, state)
		}
	}
	if data := es.takeEarly(); len(data) > 0 {
		handler.(EarlyDataHandler).OnEarlyData(t.c, data)
	}
	return true
}

// nativeFailed ends a handshake that failed: the peer is told why, if it is
// not the one that said so, and the connection closes with the reason, which
// reaches the wrapped handler's OnClose.
func (t *layer) nativeFailed(hs nativeHandshake, herr *hsError, err error) {
	t.nfailed = true
	bufferpool.Put(t.nbuf)
	t.nbuf = nil
	t.wmu.Lock()
	t.failed = true
	for i, held := range t.pending {
		bufferpool.Put(held)
		t.pending[i] = nil
	}
	t.pending = nil
	t.wmu.Unlock()
	t.failWith(err)
	if herr != nil && herr.alert != 0 {
		if record := hs.alertRecord(herr.alert); record != nil {
			_ = t.c.SendRaw(record)
			_ = t.c.Flush()
		}
	}
	t.c.CloseWithError(err)
}

// completeNative finishes a handshake that ran without a worker: the
// connection takes the keys it ended with, what was sent meanwhile goes out
// under them, the wrapped handler learns what was negotiated, and the bytes
// that arrived behind the Finished are delivered. The caller holds readMu.
func (t *layer) completeNative(handler fib.Handler, hs nativeHandshake, rest []byte) {
	t.native.Store(nil)
	if t.hsTimer != nil {
		t.hsTimer.Stop()
		t.hsTimer = nil
	}
	state := hs.connectionState()
	t.state = &state
	t.rx, t.tx = hs.recordKeys()

	if k, ok := hs.(interface{ sink() *ticketSink }); ok {
		t.sink = k.sink()
	}
	t.wmu.Lock()
	pending, closeAfterSend := t.pending, t.closeAfterSend
	t.pending = nil
	var err error
	if e := t.early; e != nil {
		// What went as early data goes again if the server turned it away.
		for _, copy := range e.copies {
			if !e.accepted && err == nil {
				err = t.writeLocked(copy, nil)
			}
			bufferpool.Put(copy)
		}
		t.early = nil
	}
	if es, ok := hs.(earlyServer); ok && t.earlyOpened {
		// The ticket the handshake left to us, under the keys we send with.
		if ticket := es.takeTicket(); ticket != nil {
			err = t.sealSendLocked(recordTypeHandshake, ticket, nil)
		}
	}
	for i, held := range pending {
		if err == nil {
			err = t.writeLocked(held, nil)
		}
		bufferpool.Put(held)
		pending[i] = nil
	}
	if err != nil {
		t.failed = true
	} else {
		t.ready = true
		if closeAfterSend {
			t.closeNotifyLocked()
		}
	}
	t.wmu.Unlock()

	if err == nil && !t.earlyOpened {
		// Before any plaintext reaches OnData, so that a handler can pick its
		// protocol from what ALPN chose.
		if h, ok := handler.(HandshakeHandler); ok {
			h.OnHandshake(t.c, state)
		}
	}
	t.mu.Lock()
	t.handshaking = false
	if err == nil && len(rest) > 0 {
		// rest may lie in nbuf, which goes back to the pool after this.
		t.in = bufferpool.Append(t.in, rest)
	}
	t.mu.Unlock()
	bufferpool.Put(t.nbuf)
	t.nbuf = nil
	if err != nil {
		t.failWith(err)
		t.settle(handler)
		t.c.CloseWithError(err)
		return
	}
	t.drainLocked(handler)
	t.settle(handler)
}

// abortNative gives up a handshake without a worker that has not completed,
// because the connection closed, and reports whether there was one.
func (t *layer) abortNative() bool {
	hs := t.native.Swap(nil)
	if hs == nil {
		return false
	}
	if t.hsTimer != nil {
		t.hsTimer.Stop()
	}
	t.mu.Lock()
	t.closed = true
	t.settling = false
	t.handshaking = false
	t.mu.Unlock()
	return true
}

// fallbackBlocking hands a connection whose server handshake could not be run
// without a worker to crypto/tls. Nothing has been sent, and nothing consumed,
// so crypto/tls is given everything received so far.
func (t *layer) fallbackBlocking(received []byte) {
	if nativeFellBack != nil {
		nativeFellBack()
	}
	t.native.Store(nil)
	if t.hsTimer != nil {
		t.hsTimer.Stop()
		t.hsTimer = nil
	}
	initial := bufferpool.Append(nil, received)
	bufferpool.Put(t.nbuf)
	t.nbuf = nil
	t.owner.startBlocking(t, initial)
}

func cmpOr[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}
