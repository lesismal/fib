package socks

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
)

// Defaults for the Config fields left at zero.
const (
	DefaultHandshakeTimeout = 10 * time.Second
	DefaultDialTimeout      = 10 * time.Second
	DefaultBindTimeout      = 2 * time.Minute
	DefaultMaxQueuedBytes   = 256 << 10
	DefaultMaxUDPTargets    = 256
	DefaultMaxBinds         = 64
)

// Config is how a Server serves its clients.
type Config struct {
	// Authenticate, when set, has every SOCKS5 client log in with a
	// username and password (RFC 1929), and admits it only when Authenticate
	// returns true. SOCKS4 has no password to check, and its clients are then
	// turned away. Nil admits every client without one. It runs on the
	// connection's worker, and should not block for long.
	Authenticate func(username, password string) bool
	// Allow decides each request, once its client has been admitted: a nil
	// error grants it, and an error refuses it, with the error's code when it
	// is a *ReplyError and "not allowed by ruleset" otherwise. A UDP
	// association asks it again for each destination the client sends a
	// datagram to, and drops the datagrams to one it refuses. Nil grants
	// everything, which leaves the proxy open to whoever can reach it. It
	// runs on the connection's worker, and should not block for long.
	Allow func(r *Request) error
	// AllowResolved, when set, decides each destination the proxy connects
	// to by the IP address it resolved to, right before it connects: a nil
	// error grants it, and an error refuses it as Allow's does. Allow sees
	// the destination as the client named it, a name that may resolve to an
	// address Allow would have refused, such as a loopback, private or
	// link-local one, or resolve to another by the time it is dialed (DNS
	// rebinding); a rule about addresses belongs here. The proxy resolves the
	// name itself, asks AllowResolved about each address in turn, and dials
	// the first it grants, by its address. It applies to CONNECT and to each
	// destination of a UDP association, and only when Dial is nil: a Dial of
	// the caller's own, such as a Dialer's chaining to another proxy,
	// resolves names itself, and AllowResolved is then never called. It runs
	// on the connection's worker or on the goroutine that resolved the name,
	// and should not block for long.
	AllowResolved func(r *Request, addr netip.AddrPort) error
	// Dial reaches the destinations of CONNECT requests and UDP
	// associations. Nil means DialDirect; a Dialer's Dial chains this proxy to
	// another.
	Dial DialFunc
	// HandshakeTimeout bounds a client from the moment it connects until its
	// request is decided. Zero means DefaultHandshakeTimeout, and a negative
	// value no bound.
	HandshakeTimeout time.Duration
	// DialTimeout bounds a CONNECT's dial of its destination. Zero means
	// DefaultDialTimeout, and a negative value leaves it to the operating
	// system.
	DialTimeout time.Duration
	// BindTimeout bounds how long a BIND waits for its peer to connect. Zero
	// means DefaultBindTimeout.
	BindTimeout time.Duration
	// MaxBinds bounds the BINDs waiting for their peers at once, across
	// the server, each of which holds a listening socket and a goroutine for
	// up to BindTimeout; one more is refused with a general failure. Zero
	// means DefaultMaxBinds, and a negative value no bound.
	MaxBinds int
	// MaxQueuedBytes is how much of what one side of a relayed connection
	// sends may wait for the other side's socket before the proxy stops
	// reading the first side, so that a fast sender is held back by TCP flow
	// control rather than by memory here. Zero means DefaultMaxQueuedBytes.
	MaxQueuedBytes int
	// MaxUDPTargets bounds the destinations one UDP association keeps a
	// socket open to; one more closes the one opened longest ago. Zero means
	// DefaultMaxUDPTargets.
	MaxUDPTargets int
	// IdleTimeout closes both sides of a relayed CONNECT once nothing has
	// gone either way for this long. Zero or less means no idle timeout.
	IdleTimeout time.Duration
}

// Request is a client's request, as Config.Allow sees it.
type Request struct {
	Version Version
	Command Command
	// Addr is the destination as the client named it, "host:port", where host
	// may be a name the proxy is to resolve. For a BIND it is the peer the
	// client expects to connect, and for a UDP association the destination
	// of a datagram.
	Addr string
	// Username is who the client logged in as, or a SOCKS4 client's user
	// ID.
	Username string
	// Conn is the client's connection.
	Conn *fib.Connection
}

// Server is a fib.Handler that serves SOCKS5, SOCKS4 and SOCKS4a clients;
// see the package documentation. Bound to a UDP address of the engine as
// well, it relays the datagrams of its clients' UDP associations: it tells
// each client the engine's UDP address of the same family as the one the
// client reached it at, and a server with no UDP address turns UDP
// associations away.
//
// A relayed connection carries what each side sends to the other until one
// side ends it, and then ends the other once what it was sent has gone out.
// The engine reads a peer's end of input as its close, so a client that shuts
// down its write side ends the relay as a close would. Engine.Shutdown closes
// the clients that have not been granted a request yet, and leaves the
// relayed connections to their peers, or to the shutdown's context.
type Server struct {
	config Config
	// direct says Dial is DialDirect because Config.Dial was nil, which
	// AllowResolved needs; see Server.dial.
	direct bool

	// associations are the UDP associations open, by their clients' IP.
	mu           sync.Mutex
	associations map[netip.Addr][]*association
	// binds counts the BINDs waiting for their peers.
	binds int
}

var (
	_ fib.Handler         = (*Server)(nil)
	_ fib.ShutdownHandler = (*Server)(nil)
)

// NewServer returns a Server, with the defaults in place of config's zeros.
func NewServer(config Config) *Server {
	direct := config.Dial == nil
	if direct {
		config.Dial = DialDirect
	}
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if config.DialTimeout == 0 {
		config.DialTimeout = DefaultDialTimeout
	} else if config.DialTimeout < 0 {
		config.DialTimeout = 0
	}
	if config.BindTimeout <= 0 {
		config.BindTimeout = DefaultBindTimeout
	}
	if config.MaxQueuedBytes <= 0 {
		config.MaxQueuedBytes = DefaultMaxQueuedBytes
	}
	if config.MaxUDPTargets <= 0 {
		config.MaxUDPTargets = DefaultMaxUDPTargets
	}
	if config.MaxBinds == 0 {
		config.MaxBinds = DefaultMaxBinds
	}
	return &Server{config: config, direct: direct, associations: make(map[netip.Addr][]*association)}
}

// serverConn is a client's connection, from its greeting until it is
// relayed.
type serverConn struct {
	s *Server
	c *fib.Connection

	// The negotiation belongs to the connection's rounds.
	phase    phase
	buf      []byte
	version  Version
	username string

	// The rest is shared with the callbacks of what the request opened.
	mu    sync.Mutex
	state serverState
	// early is what the client sent after its request, held until the
	// destination is there to take it, and held says reads are held back
	// meanwhile because there is too much of it.
	early []byte
	held  bool
	// relay is the client's handler once its request is relayed.
	relay fib.Handler
	// association is the client's UDP association, and binding its BIND.
	association *association
	binding     *binding
}

// phase is how far a client's negotiation has come.
type phase uint8

const (
	phaseGreeting phase = iota
	phaseLogin
	phaseRequest
)

type serverState uint8

const (
	// stateNegotiating has the client's request still to come.
	stateNegotiating serverState = iota
	// stateWaiting has a request granted, waiting for the destination: a
	// dial, or the peer of a BIND.
	stateWaiting
	// stateRelaying relays the client's connection.
	stateRelaying
	// stateAssociated keeps the connection open for a UDP association.
	stateAssociated
	// stateClosed has seen the client's connection close.
	stateClosed
)

// OnOpen starts a client's negotiation, and the timeout that bounds it. A
// UDP datagram's peer has nothing to negotiate.
func (s *Server) OnOpen(c *fib.Connection) {
	if c.IsUDP() {
		return
	}
	c.SetAttachment(&serverConn{s: s, c: c})
	if s.config.HandshakeTimeout > 0 {
		_ = c.SetReadDeadline(time.Now().Add(s.config.HandshakeTimeout))
	}
}

// OnData negotiates with the client, and then hands it to what its request
// opened. What waits for the rest of a message is bounded by the messages
// themselves, the longest a SOCKS4a request whose user ID and host name each
// take 255 bytes before their zero byte, since step refuses either one
// longer.
func (s *Server) OnData(c *fib.Connection, data []byte) {
	if c.IsUDP() {
		s.onDatagram(c, data)
		return
	}
	// OnOpen made it, and nothing else sets the attachment of a connection
	// the server serves.
	sc := c.Attachment().(*serverConn)
	sc.mu.Lock()
	switch sc.state {
	case stateNegotiating:
	case stateRelaying:
		relay := sc.relay
		sc.mu.Unlock()
		// From here on the relay takes the client's callbacks itself.
		c.SetHandler(relay)
		relay.OnData(c, data)
		return
	case stateWaiting:
		sc.holdEarlyLocked(data)
		sc.mu.Unlock()
		return
	default:
		// A UDP association's control carries nothing.
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	in := data
	if len(sc.buf) > 0 {
		sc.buf = append(sc.buf, data...)
		in = sc.buf
	}
	for len(in) > 0 {
		n, req, err := sc.step(in)
		if err != nil {
			// Once the answer that refuses the client, if there is one, has
			// gone: a round holds what it sends until it ends.
			c.CloseAfterSend()
			return
		}
		if n == 0 {
			break
		}
		in = in[n:]
		if req != nil {
			sc.buf = nil
			if len(in) > 0 {
				sc.mu.Lock()
				sc.holdEarlyLocked(in)
				sc.mu.Unlock()
			}
			s.serve(sc, req)
			return
		}
	}
	sc.buf = append(sc.buf[:0], in...)
}

// holdEarlyLocked keeps what the client sends before its destination is
// there, and stops reading from it once that is more than a relay would
// queue.
func (sc *serverConn) holdEarlyLocked(data []byte) {
	sc.early = append(sc.early, data...)
	if !sc.held && len(sc.early) >= sc.s.config.MaxQueuedBytes {
		sc.held = true
		sc.c.HoldReads(true)
	}
}

// step takes the client's next message out of in, answers it, and reports
// how many bytes it took, none while it has not all arrived, and the
// request once it is the request.
func (sc *serverConn) step(in []byte) (int, *Request, error) {
	switch sc.phase {
	case phaseGreeting:
		switch in[0] {
		case version5:
			return sc.greeting(in)
		case version4:
			return sc.request4(in)
		}
		return 0, nil, ErrMalformed
	case phaseLogin:
		return sc.login(in)
	}
	return sc.request5(in)
}

// greeting picks the method a SOCKS5 client logs in with.
func (sc *serverConn) greeting(in []byte) (int, *Request, error) {
	if len(in) < 2 || len(in) < 2+int(in[1]) {
		return 0, nil, nil
	}
	n := 2 + int(in[1])
	want := byte(methodNone)
	if sc.s.config.Authenticate != nil {
		want = methodPasswd
	}
	for _, m := range in[2:n] {
		if m == want {
			sc.phase = phaseRequest
			if want == methodPasswd {
				sc.phase = phaseLogin
			}
			sc.send([]byte{version5, want})
			return n, nil, nil
		}
	}
	sc.send([]byte{version5, methodRefuse})
	return 0, nil, ErrNoAcceptableMethod
}

// login checks a SOCKS5 client's username and password.
func (sc *serverConn) login(in []byte) (int, *Request, error) {
	if len(in) < 2 || len(in) < 3+int(in[1]) || len(in) < 3+int(in[1])+int(in[2+in[1]]) {
		return 0, nil, nil
	}
	if in[0] != authVersion {
		return 0, nil, ErrMalformed
	}
	ulen := int(in[1])
	plen := int(in[2+ulen])
	username := string(in[2 : 2+ulen])
	password := string(in[3+ulen : 3+ulen+plen])
	if !sc.s.config.Authenticate(username, password) {
		sc.send([]byte{authVersion, 1})
		return 0, nil, ErrAuthFailed
	}
	sc.username = username
	sc.phase = phaseRequest
	sc.send([]byte{authVersion, 0})
	return 3 + ulen + plen, nil, nil
}

// request5 reads a SOCKS5 request.
func (sc *serverConn) request5(in []byte) (int, *Request, error) {
	if len(in) < 4 {
		return 0, nil, nil
	}
	if in[0] != version5 {
		return 0, nil, ErrMalformed
	}
	host, port, n, err := parseAddr(in[3:])
	if err != nil {
		sc.reply(replyCode(err), nil)
		return 0, nil, err
	}
	if n == 0 {
		return 0, nil, nil
	}
	return 3 + n, sc.newRequest(Version5, Command(in[1]), host, port), nil
}

// request4 reads a SOCKS4 or SOCKS4a request: the user ID and the host name
// of 4a each end with a zero byte.
func (sc *serverConn) request4(in []byte) (int, *Request, error) {
	if len(in) < 9 {
		return 0, nil, nil
	}
	end := indexZero(in[8:])
	if end < 0 {
		return 0, nil, sc.tooLong(in[8:])
	}
	n := 8 + end + 1
	version := Version4
	host := netip.AddrFrom4([4]byte(in[4:8])).String()
	if in[4] == 0 && in[5] == 0 && in[6] == 0 && in[7] != 0 {
		// 0.0.0.x, x not zero, says a host name follows (4a).
		name := indexZero(in[n:])
		if name < 0 {
			return 0, nil, sc.tooLong(in[n:])
		}
		version = Version4A
		host = string(in[n : n+name])
		n += name + 1
		if host == "" {
			return 0, nil, ErrMalformed
		}
	}
	sc.version = version
	sc.username = string(in[8 : 8+end])
	if sc.s.config.Authenticate != nil {
		sc.reply(ReplyNotAllowed, nil)
		return 0, nil, ErrAuthFailed
	}
	return n, sc.newRequest(version, Command(in[1]), host, uint16(in[2])<<8|uint16(in[3])), nil
}

// tooLong fails a SOCKS4 field that has gone on past 255 bytes without its
// zero byte, and lets one that has not wait for more.
func (sc *serverConn) tooLong(field []byte) error {
	if len(field) > 255 {
		return ErrMalformed
	}
	return nil
}

func indexZero(b []byte) int {
	for i, x := range b {
		if x == 0 {
			return i
		}
	}
	return -1
}

func (sc *serverConn) newRequest(version Version, cmd Command, host string, port uint16) *Request {
	return &Request{Version: version, Command: cmd, Addr: net.JoinHostPort(host, strconv.Itoa(int(port))),
		Username: sc.username, Conn: sc.c}
}

// send sends b to the client. A send that fails closes the connection, and
// OnClose ends whatever the request opened.
func (sc *serverConn) send(b []byte) { _ = sc.c.Send(b) }

// reply answers the request, granting it with code ReplySucceeded and
// refusing it with any other, and names bound, the address the proxy took
// for it, which may be nil.
func (sc *serverConn) reply(code byte, bound net.Addr) {
	if sc.version != Version5 {
		b := []byte{0, reply4Granted, 0, 0, 0, 0, 0, 0}
		if code != ReplySucceeded {
			b[1] = reply4Rejected
		}
		if a, ok := bound.(*net.TCPAddr); ok {
			if ip := a.IP.To4(); ip != nil {
				b[2], b[3] = byte(a.Port>>8), byte(a.Port)
				copy(b[4:], ip)
			}
		}
		sc.send(b)
		return
	}
	sc.send(appendNetAddr([]byte{version5, code, 0}, bound))
}

// refuse answers the request with code and ends the connection once the
// answer has gone.
func (sc *serverConn) refuse(code byte) {
	sc.reply(code, nil)
	sc.c.CloseAfterSend()
}

// serve carries out a request.
func (s *Server) serve(sc *serverConn, req *Request) {
	c := sc.c
	if s.config.Allow != nil {
		if err := s.config.Allow(req); err != nil {
			code := ReplyNotAllowed
			var replyErr *ReplyError
			if errors.As(err, &replyErr) {
				code = replyErr.Code
			}
			sc.refuse(code)
			return
		}
	}
	if s.config.HandshakeTimeout > 0 {
		_ = c.SetReadDeadline(time.Time{})
	}
	sc.mu.Lock()
	sc.state = stateWaiting
	sc.mu.Unlock()
	switch {
	case req.Command == CommandConnect:
		s.connect(sc, req)
	case req.Command == CommandBind:
		s.bind(sc, req)
	case req.Command == CommandUDPAssociate && req.Version == Version5:
		s.associate(sc, req)
	default:
		sc.refuse(ReplyCommandNotSupported)
	}
}

// connect dials a CONNECT's destination. The destination's handler answers
// the client once it is open, before it can relay anything.
func (s *Server) connect(sc *serverConn, req *Request) {
	t := &tunnel{sc: sc, client: sc.c, limit: int64(s.config.MaxQueuedBytes), idle: s.config.IdleTimeout}
	err := s.dial(sc.c.Engine(), "tcp", req, (*tunnelTarget)(t),
		func(_ *fib.Connection, err error) {
			if err != nil {
				sc.dialFailed(err)
			}
		})
	if err != nil {
		sc.dialFailed(err)
	}
}

// dialFailed answers a CONNECT whose destination could not be reached.
func (sc *serverConn) dialFailed(err error) {
	sc.mu.Lock()
	closed := sc.state == stateClosed
	sc.state = stateClosed
	sc.mu.Unlock()
	if !closed {
		sc.refuse(replyCode(err))
	}
}

// opened answers a CONNECT whose destination, t, is open, passes on what the
// client sent meanwhile, and hands the client to the relay.
func (sc *serverConn) opened(tun *tunnel, t *fib.Connection) {
	sc.mu.Lock()
	if sc.state == stateClosed {
		sc.mu.Unlock()
		t.Close()
		return
	}
	sc.state = stateRelaying
	sc.relay = (*tunnelClient)(tun)
	// Each side is paced by the other's backlog; see tunnel.
	sc.c.SetWriteBackpressure(false)
	t.SetWriteBackpressure(false)
	early, held := sc.early, sc.held
	sc.early, sc.held = nil, false
	// The reply goes out before anything the destination sends can, and
	// what the client sent early before anything it sends later: a round of
	// the client's takes the lock before it relays.
	sc.reply(ReplySucceeded, t.LocalAddr())
	tun.startIdle()
	holding := len(early) > 0 && tun.forward(sc.c, t, early)
	sc.mu.Unlock()
	if held && !holding {
		sc.c.HoldReads(false)
	}
}

// OnPriorityData drops out-of-band bytes: SOCKS has nothing to carry them.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose ends what the client's request opened.
func (s *Server) OnClose(c *fib.Connection, err error) {
	if c.IsUDP() {
		if a, ok := c.Attachment().(*association); ok {
			a.peerClosed(c)
		}
		return
	}
	sc := c.Attachment().(*serverConn)
	sc.mu.Lock()
	state := sc.state
	sc.state = stateClosed
	relay, a, b := sc.relay, sc.association, sc.binding
	sc.mu.Unlock()
	switch state {
	case stateRelaying:
		// The client closed before its first round after the reply, and so
		// before the relay took its callbacks.
		relay.OnClose(c, err)
	case stateAssociated:
		a.close()
	case stateWaiting:
		if b != nil {
			b.cancel()
		}
	}
}

// OnShutdown closes a client that has not been granted a request yet. One
// that has is relayed, and its peers end it; see Server.
func (s *Server) OnShutdown(c *fib.Connection) {
	if c.IsUDP() {
		return
	}
	sc := c.Attachment().(*serverConn)
	sc.mu.Lock()
	negotiating := sc.state == stateNegotiating
	sc.mu.Unlock()
	if negotiating {
		c.Close()
	}
}
