// Package tls runs TLS over the engine's connections, with crypto/tls doing
// the cryptography.
//
// A Handler sits in front of an ordinary fib.Handler and hands it plaintext.
// The wrapped handler sees an ordinary connection: its Send, SendOwned,
// SendParts and CloseAfterSend encrypt, and its OnData receives decrypted
// bytes, so protocol handlers such as the http and websocket packages run over
// TLS unchanged.
package tls

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
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
// A client whose Config asks for TLS 1.3 alone (MinVersion TLS13, and nothing
// else that is beyond client13.go: client certificates, session resumption,
// encrypted client hello) takes no worker for its handshake at all. It is a
// state machine that OnData drives with the bytes that arrive and that never
// waits, so a peer that is slow to answer, or that never answers, costs the
// connection's memory and a timer, and no goroutine; thousands of handshakes
// can be in flight at once. Other Configs keep to crypto/tls, as do servers.
//
// Once a handshake settles on an AES-GCM suite of TLS 1.3 or 1.2, or an
// AES-CBC suite of TLS 1.2 or 1.1, the connection's records are protected by
// this package rather than by
// crypto/tls, which is then done with; the keys come from crypto/tls through
// Config.KeyLogWriter. For that the Handler clones Config the first time it
// is used and runs its handshakes with the clone, whose KeyLogWriter still
// passes every line on to Config's own; changes made to Config after that
// are not seen, as crypto/tls asks of a Config in use anyway.
type Handler struct {
	Config  *stdtls.Config
	Handler fib.Handler
	// Client selects the client side of the handshake. Dialed connections
	// need it; accepted ones need it left false.
	Client bool
	// HandshakeTimeout bounds the handshake. Zero means
	// DefaultHandshakeTimeout and a negative value means no bound.
	HandshakeTimeout time.Duration

	// fastConfig is the clone of Config handshakes run with where the
	// connection may leave crypto/tls afterwards; see fast.go.
	fastOnce   sync.Once
	fastConfig *stdtls.Config
	fastReg    *registry
}

// NewServer returns a handler that serves TLS with config in front of
// handler, for fib.Bind.
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
// host in addr is used, as crypto/tls.Dial does. A nil handler means the
// engine's. done runs once the connect completes, as it does for
// fib.Engine.Dial; the handshake follows, and what done sends waits for it.
func Dial(engine *fib.Engine, network, addr string, timeout time.Duration, config *stdtls.Config,
	handler fib.Handler, done func(*fib.Connection, error)) error {
	if config == nil {
		config = &stdtls.Config{}
	}
	if config.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		config = config.Clone()
		config.ServerName = host
	}
	if handler == nil {
		handler = engine.Handler()
	}
	return engine.DialWithHandler(network, addr, timeout, NewClient(config, handler), done)
}

func (h *Handler) inner() fib.Handler {
	if h.Handler == nil {
		return fib.HandlerFuncs{}
	}
	return h.Handler
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
	t := &layer{c: c, client: h.Client, handshaking: true, settling: true}
	t.cond.L = &t.mu
	if h.Client && nativeClient(h.Config) {
		h.openNative(c, t)
		return
	}
	config, reg := h.config()
	if reg != nil {
		t.capture = &capture{client: h.Client, reg: reg}
	}
	if h.Client {
		t.conn = stdtls.Client(t, config)
	} else {
		t.conn = stdtls.Server(t, config)
	}
	c.SetLayer(t)
	h.inner().OnOpen(c)
	timeout := h.HandshakeTimeout
	if timeout == 0 {
		timeout = DefaultHandshakeTimeout
	}
	inner := h.inner()
	if !sidepool.Handshake().Go(func() { t.handshake(inner, timeout) }) {
		t.mu.Lock()
		t.handshaking = false
		t.mu.Unlock()
		t.settle(inner)
		c.CloseWithError(errNoHandshakeWorker)
	}
}

// nativeStarted, set by tests, is told of each handshake that runs without a
// worker.
var nativeStarted func()

// openNative begins a handshake that runs without a worker: the ClientHello
// goes out now, and OnData carries the handshake on from there.
func (h *Handler) openNative(c *fib.Connection, t *layer) {
	if nativeStarted != nil {
		nativeStarted()
	}
	hs, err := newClientHS(h.Config)
	c.SetLayer(t)
	h.inner().OnOpen(c)
	if err != nil {
		c.CloseWithError(err)
		return
	}
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
	t.native.Store(hs)
	if err := c.SendRaw(hs.hello()); err != nil {
		c.CloseWithError(err)
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

func (h *Handler) OnClose(c *fib.Connection, err error) {
	if t, ok := c.Layer().(*layer); ok {
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

	// native is the handshake of a client that runs without a worker, until it
	// completes or fails; see client13.go. nbuf holds the start of a record
	// that has not arrived whole, and hsTimer the handshake's timeout. The
	// state machine itself is only touched by OnData, under readMu.
	native  atomic.Pointer[clientHS]
	nbuf    []byte
	hsTimer *time.Timer
	nfailed bool

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
}

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
	if hs := t.native.Load(); hs != nil {
		t.feedNative(handler, hs, data)
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
		t.pending = append(t.pending, bufferpool.Join(nil, first, second))
		return nil
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
// refused to run.
var errNoHandshakeWorker = errors.New("fib: tls handshake pool has stopped")

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

// feedNative carries a handshake that runs without a worker on with the bytes
// a round read. The handshake never waits: whatever does not make a whole
// record is kept until the next round, and when it completes the bytes behind
// the Finished go the way every later round's do.
func (t *layer) feedNative(handler fib.Handler, hs *clientHS, data []byte) {
	if t.nfailed || t.isClosed() {
		return
	}
	buf := data
	if len(t.nbuf) > 0 {
		t.nbuf = bufferpool.Append(t.nbuf, data)
		buf = t.nbuf
	}
	consumed, out, done, herr := hs.process(buf)
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

// nativeFailed ends a handshake that failed: the peer is told why, if it is
// not the one that said so, and the connection closes with the reason, which
// reaches the wrapped handler's OnClose.
func (t *layer) nativeFailed(hs *clientHS, herr *hsError, err error) {
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
func (t *layer) completeNative(handler fib.Handler, hs *clientHS, rest []byte) {
	t.native.Store(nil)
	if t.hsTimer != nil {
		t.hsTimer.Stop()
		t.hsTimer = nil
	}
	state := hs.connectionState()
	t.state = &state
	t.rx, t.tx = hs.rx, hs.tx

	t.wmu.Lock()
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

	if err == nil {
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
