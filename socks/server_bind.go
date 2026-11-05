package socks

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
)

// A BIND listens on a port of its own, for one connection, and the engine
// cannot take up a listener or a socket after it has started: the listener
// and the connection it accepts are the net package's, served by two
// goroutines of their own for as long as the connection lasts. BIND is
// rare, FTP's active mode being about its only use, and the goroutines are
// its alone.

// listenTCP is net.ListenTCP, which a test replaces to make it fail.
var listenTCP = net.ListenTCP

// binding is a BIND waiting for its peer.
type binding struct {
	sc *serverConn
	ln *net.TCPListener
	// expect are the addresses the peer may connect from, those of the host
	// the client named; none means any, when it named the unspecified
	// address. name is that host when it is a name, resolved before the
	// peer is accepted.
	expect []netip.Addr
	name   string
}

// bind listens for a BIND's peer, on the address the client reached the
// server at, and tells the client where.
func (s *Server) bind(sc *serverConn, req *Request) {
	local, _ := sc.c.LocalAddr().(*net.TCPAddr)
	if local == nil {
		sc.refuse(ReplyCommandNotSupported)
		return
	}
	host, _, err := splitHostPort(req.Addr)
	if err != nil {
		sc.refuse(ReplyAddressNotSupported)
		return
	}
	if !s.startBind() {
		sc.refuse(ReplyGeneralFailure)
		return
	}
	ln, err := listenTCP("tcp", &net.TCPAddr{IP: local.IP, Zone: local.Zone})
	if err != nil {
		s.endBind()
		sc.refuse(replyCode(err))
		return
	}
	b := &binding{sc: sc, ln: ln}
	// Only the peer the client named may connect: an IP, or the addresses
	// a name resolves to. The unspecified address names none.
	if ip, err := netip.ParseAddr(host); err != nil {
		b.name = host
	} else if !ip.IsUnspecified() {
		b.expect = []netip.Addr{ip.Unmap()}
	}
	sc.mu.Lock()
	sc.binding = b
	sc.mu.Unlock()
	sc.reply(ReplySucceeded, ln.Addr())
	go func() {
		defer s.endBind()
		b.accept(s.config.BindTimeout)
	}()
}

// startBind counts a BIND that is to wait for its peer, unless MaxBinds
// already are.
func (s *Server) startBind() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.config.MaxBinds > 0 && s.binds >= s.config.MaxBinds {
		return false
	}
	s.binds++
	return true
}

// endBind forgets a BIND that no longer waits.
func (s *Server) endBind() {
	s.mu.Lock()
	s.binds--
	s.mu.Unlock()
}

// accept waits for the peer, and relays it.
func (b *binding) accept(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	_ = b.ln.SetDeadline(deadline)
	if b.name != "" {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", b.name)
		cancel()
		if err != nil {
			_ = b.ln.Close()
			b.sc.bindFailed(err)
			return
		}
		for _, ip := range ips {
			b.expect = append(b.expect, ip.Unmap())
		}
	}
	for {
		conn, err := b.ln.AcceptTCP()
		if err != nil {
			_ = b.ln.Close()
			b.sc.bindFailed(err)
			return
		}
		if len(b.expect) > 0 && !slices.Contains(b.expect, conn.RemoteAddr().(*net.TCPAddr).AddrPort().Addr().Unmap()) {
			_ = conn.Close()
			continue
		}
		_ = b.ln.Close()
		b.sc.bound(conn)
		return
	}
}

// cancel stops waiting, when the client has gone.
func (b *binding) cancel() { _ = b.ln.Close() }

// bindFailed answers a BIND whose peer never came.
func (sc *serverConn) bindFailed(err error) {
	sc.mu.Lock()
	closed := sc.state == stateClosed
	sc.state = stateClosed
	sc.mu.Unlock()
	if !closed {
		sc.refuse(replyCode(err))
	}
}

// bound answers a BIND whose peer has connected, and relays the client to
// it.
func (sc *serverConn) bound(conn *net.TCPConn) {
	p := &streamPipe{c: sc.c, conn: conn, limit: sc.s.config.MaxQueuedBytes}
	p.cond.L = &p.mu
	sc.mu.Lock()
	if sc.state == stateClosed {
		sc.mu.Unlock()
		_ = conn.Close()
		return
	}
	sc.state = stateRelaying
	sc.relay = p
	// The client is paced by the queue to the peer, not its own backlog; see
	// tunnel.
	sc.c.SetWriteBackpressure(false)
	early := sc.early
	sc.early = nil
	sc.reply(ReplySucceeded, conn.RemoteAddr())
	// Reads held for early data stay held: as much of it is in the pipe's
	// queue as was waiting here, so enqueue holds them again, and the writer
	// lets them go once it takes it.
	if len(early) > 0 {
		p.enqueue(early)
	}
	sc.mu.Unlock()
	go p.write()
	go p.read()
}

// streamPipe relays a client to a connection of the net package's: the
// client's handler queues what it receives for a goroutine that writes it,
// and another goroutine reads the connection and sends what it reads to the
// client.
type streamPipe struct {
	c     *fib.Connection
	conn  net.Conn
	limit int

	mu   sync.Mutex
	cond sync.Cond
	// queue is what waits for the writer, and held says the client's reads
	// are held because it has grown to the limit.
	queue  []byte
	held   bool
	closed bool
}

func (p *streamPipe) OnOpen(*fib.Connection)                 {}
func (p *streamPipe) OnData(_ *fib.Connection, data []byte)  { p.enqueue(data) }
func (p *streamPipe) OnPriorityData(*fib.Connection, []byte) {}

func (p *streamPipe) OnClose(*fib.Connection, error) {
	p.mu.Lock()
	p.closed = true
	p.cond.Signal()
	p.mu.Unlock()
}

// enqueue queues data for the writer, and holds the client's reads once the
// queue reaches the limit, reporting that it did.
//
// HoldReads is called under the lock, here and in write, so that the hold
// it leaves is the one held says: the last call wins, and the writer's
// release could otherwise overtake the hold it releases.
func (p *streamPipe) enqueue(data []byte) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queue = append(p.queue, data...)
	hold := !p.held && len(p.queue) >= p.limit
	if hold {
		p.held = true
		p.c.HoldReads(true)
	}
	p.cond.Signal()
	return hold
}

// write writes what the client sends to the connection, until the client
// has closed and all of it has been written, and then closes it.
func (p *streamPipe) write() {
	var out []byte
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if len(p.queue) == 0 {
			p.mu.Unlock()
			break
		}
		out, p.queue = p.queue, out[:0]
		if p.held {
			p.held = false
			p.c.HoldReads(false)
		}
		p.mu.Unlock()
		if _, err := p.conn.Write(out); err != nil {
			p.c.Close()
			break
		}
	}
	_ = p.conn.Close()
}

// read sends what the connection carries to the client, waiting while more
// than the limit of it is queued there, and ends the client once the
// connection has ended.
func (p *streamPipe) read() {
	buf := make([]byte, 32<<10)
	for {
		n, err := p.conn.Read(buf)
		if n > 0 {
			if p.c.Send(buf[:n]) != nil {
				break
			}
			if outputwait.Wait != nil && outputwait.Wait(p.c, int64(p.limit)) != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	p.c.CloseAfterSend()
}
