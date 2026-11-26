package socks

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/sidepool"
)

// DefaultPort is the port a proxy URL without one names: SOCKS's own.
const DefaultPort = "1080"

// Dialer opens connections through a SOCKS proxy. Its methods are safe to
// call from any goroutine, and like fib.Engine.Dial they never wait: the
// connect to the proxy, the negotiation with it and any DNS lookup all
// happen on the engine's loop and workers, or on the pool package sidepool
// keeps for clients.
//
// A connection it opens is an ordinary one of the engine, whose
// RemoteAddr is the proxy's: what its handler sends and receives goes
// through the proxy to the destination and back.
type Dialer struct {
	// Network and Addr locate the proxy, as fib.Engine.Dial takes them: an
	// empty Network means "tcp", and Addr is "host:port", or a path when
	// Network is "unix".
	Network string
	Addr    string
	// Version is the protocol spoken to the proxy; the zero value is
	// Version5.
	Version Version
	// Username and Password are offered to a SOCKS5 proxy that asks for
	// them (RFC 1929); each may be at most 255 bytes. Username is a SOCKS4
	// request's user ID.
	Username string
	Password string
	// ResolveLocally resolves a destination's host name before asking a
	// SOCKS5 proxy for it, rather than letting the proxy resolve it, as
	// socks5:// does in a URL and socks5h:// does not. SOCKS4 always
	// resolves locally, and SOCKS4a never does.
	ResolveLocally bool
	// Forward opens the connections to the proxy: the TCP one each dial
	// makes, and the UDP one to the relay of a UDP association. Nil means
	// DialDirect; another Dialer's Dial reaches this proxy through that one.
	Forward DialFunc
}

// ParseURL returns the Dialer that a proxy URL describes, in the form curl
// and most proxy settings use:
//
//	socks5://[user[:password]@]host[:port]   SOCKS5, resolving names locally
//	socks5h://[user[:password]@]host[:port]  SOCKS5, the proxy resolves names
//	socks4://[user@]host[:port]              SOCKS4
//	socks4a://[user@]host[:port]             SOCKS4a
//
// A missing port means DefaultPort.
func ParseURL(rawURL string) (*Dialer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	d := &Dialer{}
	switch u.Scheme {
	case "socks5":
		d.ResolveLocally = true
	case "socks5h":
	case "socks4":
		d.Version = Version4
	case "socks4a":
		d.Version = Version4A
	default:
		return nil, errors.New("socks: unsupported proxy scheme " + `"` + u.Scheme + `"`)
	}
	if u.Hostname() == "" {
		return nil, errors.New("socks: proxy URL names no host")
	}
	d.Addr = u.Host
	if u.Port() == "" {
		d.Addr = net.JoinHostPort(u.Hostname(), DefaultPort)
	}
	if u.User != nil {
		d.Username = u.User.Username()
		d.Password, _ = u.User.Password()
	}
	return d, nil
}

// Dial is fib.Engine.DialWithHandler through the proxy, and a DialFunc. A
// TCP network ("tcp", "tcp4", "tcp6", or empty for "tcp") asks the proxy to
// CONNECT to addr. A UDP one ("udp", "udp4", "udp6") asks a SOCKS5 proxy
// for a UDP ASSOCIATE and connects a UDP socket to the relay it names: each
// Send is one datagram to addr, through the proxy, and each OnData one
// datagram from it, as on a UDP connection fib dials itself. SendBatch
// bypasses the proxy's framing and must not be used on such a connection.
// The association lasts as long as the connection, which closes when the
// proxy ends it.
//
// timeout bounds the whole dial: connecting to the proxy, any DNS lookup
// and the negotiation. Zero means no bound beyond the operating system's on
// the connect. On success handler's OnOpen runs and then done with the
// connection, on the worker that read the proxy's answer, which handler
// then serves as fib.Engine.Dial's handler would; any bytes the destination
// sent along with that answer reach its OnData next. On failure done alone
// runs, with a nil connection and a *net.OpError. A nil handler means the
// engine's, and done may be nil.
//
// Dial returns an error, and never calls done, only when it can tell at once
// that the dial cannot start.
func (d *Dialer) Dial(engine *fib.Engine, network, addr string, timeout time.Duration,
	handler fib.Handler, done func(*fib.Connection, error)) error {
	switch network {
	case "", "tcp", "tcp4", "tcp6":
		return d.start(engine, CommandConnect, network, addr, timeout, handler, nil, done)
	case "udp", "udp4", "udp6":
		return d.start(engine, CommandUDPAssociate, network, addr, timeout, handler, nil, done)
	}
	return &net.OpError{Op: "dial", Net: network, Addr: socksAddr{network, addr}, Err: net.UnknownNetworkError(network)}
}

// Bind asks the proxy to accept a connection from addr, the peer expected to
// connect, as the data connection of FTP's active mode needs. Once the proxy
// is listening, listening runs with the address it listens on, which the
// caller passes on to the peer; once the peer has connected, handler's
// OnOpen runs and then done with the connection, which carries what the
// peer and handler send each other. timeout bounds it all, waiting for the
// peer included, and done reports a failure as Dial's does. listening and
// done run on the worker that read the proxy's answers, and must not block.
func (d *Dialer) Bind(engine *fib.Engine, addr string, timeout time.Duration, handler fib.Handler,
	listening func(net.Addr), done func(*fib.Connection, error)) error {
	return d.start(engine, CommandBind, "tcp", addr, timeout, handler, listening, done)
}

func (d *Dialer) start(engine *fib.Engine, cmd Command, network, addr string, timeout time.Duration,
	handler fib.Handler, listening func(net.Addr), done func(*fib.Connection, error)) error {
	if network == "" {
		network = "tcp"
	}
	if done == nil {
		done = func(*fib.Connection, error) {}
	}
	if handler == nil {
		handler = engine.Handler()
	}
	h := &clientConn{dialer: d, engine: engine, cmd: cmd, network: network, addr: addr,
		handler: handler, listening: listening, done: done}
	host, port, err := splitHostPort(addr)
	if err != nil {
		return h.opError(err)
	}
	if host == "" {
		return h.opError(errors.New("socks: address names no host"))
	}
	h.host, h.port = host, port
	if len(d.Username) > 255 || len(d.Password) > 255 {
		return h.opError(errors.New("socks: username and password must be at most 255 bytes"))
	}
	if cmd == CommandUDPAssociate && d.Version != Version5 {
		return h.opError(errors.New("socks: UDP needs SOCKS5"))
	}
	if timeout > 0 {
		h.deadline = time.Now().Add(timeout)
	}
	ip, err := netip.ParseAddr(host)
	isIP := err == nil
	if !isIP && (d.Version == Version4 || d.Version == Version5 && d.ResolveLocally) {
		// The caller may be a callback running on the client pool, which must
		// not submit to it, so the engine's loop submits the lookup. A pool
		// too busy to take it fails the dial with fib.ErrPoolBusy, and a
		// lookup that waited out the dial's timeout for a worker is not made.
		resolve := func() {
			if !h.deadline.IsZero() && !time.Now().Before(h.deadline) {
				h.finish(os.ErrDeadlineExceeded)
				return
			}
			if err := h.resolve(); err != nil {
				h.finish(err)
				return
			}
			if err := h.connect(); err != nil {
				h.finish(err)
			}
		}
		if err := engine.TryHand(sidepool.Client(), resolve); err != nil {
			return h.opError(err)
		}
		return nil
	}
	if d.Version != Version5 && isIP && !ip.Unmap().Is4() {
		return h.opError(errors.New("socks: SOCKS4 reaches IPv4 addresses only"))
	}
	if !isIP && len(host) > 255 {
		return h.opError(errors.New("socks: host name must be at most 255 bytes"))
	}
	return h.connect()
}

// clientConn is a dial in progress, and the handler of its connection until
// the proxy has done what it was asked.
type clientConn struct {
	dialer    *Dialer
	engine    *fib.Engine
	cmd       Command
	network   string
	addr      string
	host      string
	port      uint16
	handler   fib.Handler
	listening func(net.Addr)
	done      func(*fib.Connection, error)
	deadline  time.Time
	// settled is set once done has run, or the connection has been handed
	// to the handler that will run it.
	settled atomic.Bool

	// The rest belongs to the connection's rounds.
	state clientState
	buf   []byte
}

type clientState uint8

const (
	// stateMethod waits for a SOCKS5 proxy's choice of method.
	stateMethod clientState = iota
	// stateAuth waits for its verdict on the username and password.
	stateAuth
	// stateReply waits for the reply to the request, and stateReply2 for
	// the second reply of a BIND.
	stateReply
	stateReply2
)

// resolve looks the destination up, for a proxy that will not.
func (h *clientConn) resolve() error {
	ctx := context.Background()
	if !h.deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, h.deadline)
		defer cancel()
	}
	family := "ip"
	if h.dialer.Version == Version4 {
		family = "ip4"
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, family, h.host)
	if err != nil {
		return err
	}
	// A lookup without an error has an address. IPv4 first, as the net package's resolver prefers.
	pick := ips[0].Unmap()
	for _, ip := range ips {
		if ip.Unmap().Is4() {
			pick = ip.Unmap()
			break
		}
	}
	h.host = pick.String()
	return nil
}

// remaining is what is left of the dial's timeout, or zero for none. A
// timeout that has run out leaves a moment, which the read deadline set in
// OnOpen then cuts short.
func (h *clientConn) remaining() time.Duration {
	if h.deadline.IsZero() {
		return 0
	}
	return max(time.Until(h.deadline), time.Millisecond)
}

// forward is the DialFunc that reaches the proxy.
func (h *clientConn) forward() DialFunc {
	if h.dialer.Forward != nil {
		return h.dialer.Forward
	}
	return DialDirect
}

// connect dials the proxy, with what is left of the timeout.
func (h *clientConn) connect() error {
	network := h.dialer.Network
	if network == "" {
		network = "tcp"
	}
	err := h.forward()(h.engine, network, h.dialer.Addr, h.remaining(), h, func(_ *fib.Connection, err error) {
		if err != nil {
			h.finish(err)
		}
	})
	if err != nil {
		return h.opError(err)
	}
	return nil
}

func (h *clientConn) opError(err error) error {
	return &net.OpError{Op: "socks " + h.cmd.String(), Net: h.network, Addr: socksAddr{h.network, h.addr}, Err: err}
}

// finish reports a dial that failed, unless it has been settled already.
func (h *clientConn) finish(err error) {
	if h.settled.CompareAndSwap(false, true) {
		h.done(nil, h.opError(err))
	}
}

// OnOpen opens the negotiation: a SOCKS5 greeting, or a SOCKS4 request,
// which has none.
func (h *clientConn) OnOpen(c *fib.Connection) {
	if !h.deadline.IsZero() {
		_ = c.SetReadDeadline(h.deadline)
	}
	if h.dialer.Version != Version5 {
		h.state = stateReply
		// A send that fails closes the connection, and OnClose fails the
		// dial.
		_ = c.Send(h.request4())
		return
	}
	greeting := []byte{version5, 1, methodNone}
	if h.dialer.Username != "" || h.dialer.Password != "" {
		greeting = []byte{version5, 2, methodNone, methodPasswd}
	}
	_ = c.Send(greeting)
}

// request4 is a SOCKS4 request, or a SOCKS4a one when the host is a name.
func (h *clientConn) request4() []byte {
	b := []byte{version4, byte(h.cmd), byte(h.port >> 8), byte(h.port)}
	ip, err := netip.ParseAddr(h.host)
	if err == nil {
		a := ip.Unmap().As4()
		b = append(b, a[:]...)
	} else {
		b = append(b, 0, 0, 0, 1)
	}
	b = append(append(b, h.dialer.Username...), 0)
	if err != nil {
		b = append(append(b, h.host...), 0)
	}
	return b
}

// request5 is a SOCKS5 request. The destination of a UDP ASSOCIATE is where
// the client will send its datagrams from, which it does not know before it
// dials the relay: it is left unspecified, which leaves the proxy to take
// the first datagram from the client's address as the client's.
func (h *clientConn) request5() []byte {
	b := []byte{version5, byte(h.cmd), 0}
	if h.cmd == CommandUDPAssociate {
		return appendIP(b, netip.IPv4Unspecified(), 0)
	}
	// start has checked the host.
	b, _ = appendAddr(b, h.host, h.port)
	return b
}

// OnData takes the proxy's answers. None is longer than 262 bytes, a reply
// naming the longest host name, so what waits for the rest of one is never
// more than that.
func (h *clientConn) OnData(c *fib.Connection, data []byte) {
	in := data
	if len(h.buf) > 0 {
		h.buf = append(h.buf, data...)
		in = h.buf
	}
	for len(in) > 0 {
		n, open, err := h.step(c, in)
		if err != nil {
			c.CloseWithError(err)
			return
		}
		if n == 0 {
			break
		}
		in = in[n:]
		if open {
			h.open(c, in)
			return
		}
		if h.settled.Load() {
			// A UDP association's control: the connection carries nothing
			// more.
			return
		}
	}
	h.buf = append(h.buf[:0], in...)
}

// step takes the next message from the proxy out of in, and reports how
// many bytes it took, none while it has not all arrived, and whether the
// connection is now the destination's.
func (h *clientConn) step(c *fib.Connection, in []byte) (n int, open bool, err error) {
	if h.dialer.Version != Version5 {
		if len(in) < 8 {
			return 0, false, nil
		}
		// The reply's version is 0, though some proxies send 4.
		if in[0] != 0 && in[0] != version4 {
			return 0, false, ErrMalformed
		}
		if in[1] != reply4Granted {
			return 0, false, &ReplyError{Version: h.dialer.Version, Code: in[1]}
		}
		port := uint16(in[2])<<8 | uint16(in[3])
		bound := &net.TCPAddr{IP: net.IP(in[4:8:8]).To16(), Port: int(port)}
		open, err := h.replied(c, bound)
		return 8, open, err
	}
	switch h.state {
	case stateMethod:
		if len(in) < 2 {
			return 0, false, nil
		}
		if in[0] != version5 {
			return 0, false, ErrMalformed
		}
		switch in[1] {
		case methodNone:
			h.state = stateReply
			_ = c.Send(h.request5())
		case methodPasswd:
			if h.dialer.Username == "" && h.dialer.Password == "" {
				return 0, false, ErrMalformed
			}
			h.state = stateAuth
			b := append([]byte{authVersion, byte(len(h.dialer.Username))}, h.dialer.Username...)
			b = append(append(b, byte(len(h.dialer.Password))), h.dialer.Password...)
			_ = c.Send(b)
		case methodRefuse:
			return 0, false, ErrNoAcceptableMethod
		default:
			return 0, false, ErrMalformed
		}
		return 2, false, nil
	case stateAuth:
		if len(in) < 2 {
			return 0, false, nil
		}
		if in[1] != 0 {
			return 0, false, ErrAuthFailed
		}
		h.state = stateReply
		_ = c.Send(h.request5())
		return 2, false, nil
	}
	if len(in) < 4 {
		return 0, false, nil
	}
	if in[0] != version5 {
		return 0, false, ErrMalformed
	}
	if in[1] != ReplySucceeded {
		return 0, false, &ReplyError{Version: Version5, Code: in[1]}
	}
	host, port, an, err := parseAddr(in[3:])
	if err != nil || an == 0 {
		return 0, false, err
	}
	var bound net.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		bound = net.TCPAddrFromAddrPort(netip.AddrPortFrom(ip, port))
	} else {
		bound = socksAddr{"tcp", net.JoinHostPort(host, strconv.Itoa(int(port)))}
	}
	open, err = h.replied(c, bound)
	return 3 + an, open, err
}

// replied takes a reply that granted the request, and reports whether the
// connection is now the destination's.
func (h *clientConn) replied(c *fib.Connection, bound net.Addr) (bool, error) {
	switch {
	case h.cmd == CommandBind && h.state != stateReply2:
		h.state = stateReply2
		if h.listening != nil {
			h.listening(bound)
		}
		return false, nil
	case h.cmd == CommandUDPAssociate:
		return false, h.associate(c, bound)
	}
	return true, nil
}

// open hands the connection to the handler the dial was for, which hears
// rest, what the destination sent along with the proxy's reply, after done.
func (h *clientConn) open(c *fib.Connection, rest []byte) {
	// Only OnClose settles a dial on the connection otherwise, and it runs
	// after every round.
	h.settled.Store(true)
	if !h.deadline.IsZero() {
		_ = c.SetReadDeadline(time.Time{})
	}
	h.buf = nil
	c.SetHandler(h.handler)
	h.handler.OnOpen(c)
	h.done(c, nil)
	if len(rest) > 0 {
		h.handler.OnData(c, rest)
	}
}

// OnPriorityData drops out-of-band bytes from the proxy, which has no use
// for them.
func (h *clientConn) OnPriorityData(*fib.Connection, []byte) {}

// OnClose fails the dial with why the connection ended before the proxy
// granted the request.
func (h *clientConn) OnClose(_ *fib.Connection, err error) {
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	h.finish(err)
}

// associate dials the relay a UDP ASSOCIATE was granted, and makes c, the
// connection the request went over, the association's control: the
// association ends when it does.
func (h *clientConn) associate(c *fib.Connection, bound net.Addr) error {
	relay, ok := bound.(*net.TCPAddr)
	if !ok {
		// A relay named by a host name would need a lookup first.
		return &ReplyError{Version: Version5, Code: ReplyAddressNotSupported}
	}
	relayAddr := relay.AddrPort()
	if relayAddr.Addr().IsUnspecified() {
		// The proxy relays on the address the client reached it at.
		relayAddr = netip.AddrPortFrom(c.RemoteAddrPort().Addr(), relayAddr.Port())
	}
	relayAddr = netip.AddrPortFrom(relayAddr.Addr().Unmap(), relayAddr.Port())
	header, _ := appendAddr([]byte{0, 0, 0}, h.host, h.port)
	a := &udpAssociation{control: c, handler: h.handler, header: header, h: h}
	h.settled.Store(true)
	if !h.deadline.IsZero() {
		_ = c.SetReadDeadline(time.Time{})
	}
	c.SetHandler(associationControl{a})
	if err := h.forward()(h.engine, "udp", relayAddr.String(), h.remaining(), a, a.dialed); err != nil {
		a.dialed(nil, err)
	}
	return nil
}

// errAssociationEnded is what a UDP connection through a proxy is closed
// with when the proxy ends the association.
var errAssociationEnded = errors.New("socks: the proxy ended the UDP association")

// udpAssociation is a UDP connection through a proxy: the handler of the UDP
// socket connected to the proxy's relay, which unwraps what arrives for the
// handler the dial was for, and its layer, which wraps what that handler
// sends in the header that tells the relay where it goes.
type udpAssociation struct {
	control *fib.Connection
	handler fib.Handler
	header  []byte
	h       *clientConn

	mu      sync.Mutex
	conn    *fib.Connection
	stopped bool
}

// dialed settles the dial of the relay, and with it the whole dial.
func (a *udpAssociation) dialed(conn *fib.Connection, err error) {
	if err != nil {
		a.control.Close()
		a.h.done(nil, a.h.opError(err))
		return
	}
	a.h.done(conn, nil)
	a.mu.Lock()
	stopped := a.stopped
	a.mu.Unlock()
	if stopped {
		conn.CloseWithError(errAssociationEnded)
	}
}

func (a *udpAssociation) OnOpen(c *fib.Connection) {
	a.mu.Lock()
	a.conn = c
	a.mu.Unlock()
	c.SetLayer(a)
	a.handler.OnOpen(c)
}

// OnData unwraps a datagram from the relay, and drops one that is not a
// whole datagram from the destination.
func (a *udpAssociation) OnData(c *fib.Connection, data []byte) {
	if len(data) < 4 || data[2] != 0 {
		return
	}
	_, _, n, err := parseAddr(data[3:])
	if err != nil || n == 0 {
		return
	}
	a.handler.OnData(c, data[3+n:])
}

func (a *udpAssociation) OnPriorityData(c *fib.Connection, data []byte) {
	a.handler.OnPriorityData(c, data)
}

func (a *udpAssociation) OnClose(c *fib.Connection, err error) {
	a.handler.OnClose(c, err)
	a.control.Close()
}

// Send wraps first and second, one datagram, in the relay's header.
func (a *udpAssociation) Send(first, second []byte) error {
	b := bufferpool.Get(len(a.header) + len(first) + len(second))
	n := copy(b, a.header)
	n += copy(b[n:], first)
	copy(b[n:], second)
	return a.conn.SendRawPooled(b)
}

func (a *udpAssociation) CloseAfterSend() { a.conn.CloseAfterSendRaw() }

// associationControl is the handler of the connection a UDP association
// was asked for over. It carries nothing more, and ends the association
// when it closes.
type associationControl struct{ a *udpAssociation }

func (associationControl) OnOpen(*fib.Connection)                 {}
func (associationControl) OnData(*fib.Connection, []byte)         {}
func (associationControl) OnPriorityData(*fib.Connection, []byte) {}

func (h associationControl) OnClose(*fib.Connection, error) {
	h.a.mu.Lock()
	h.a.stopped = true
	conn := h.a.conn
	h.a.mu.Unlock()
	if conn != nil {
		conn.CloseWithError(errAssociationEnded)
	}
}
