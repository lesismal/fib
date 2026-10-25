//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/sidepool"
)

// CONNECT over HTTP/3: an extended CONNECT (RFC 9220), whose :protocol names
// what it wants to speak, such as WebSocket, or a CONNECT to a host and port
// (RFC 9114 section 4.4), opens a stream that its handler may switch to a
// tunnel with Context.Upgrade or Context.Connect. The stream then carries the
// tunnel's bytes in DATA frames both ways until each side ends it with FIN or
// either resets it.

// serveTunnel runs the handler of an extended CONNECT, whose stream stays
// open for it: it has no body, only whatever the tunnel carries once the
// handler has accepted it, which waits in the tunnel until then.
func (rs *requestStream) serveTunnel() {
	// A tunnel outlives its handler's return; its stream is not recycled.
	rs.pooled = false
	sc := rs.sc
	tunnel := &streamTunnel{TunnelFeed: fibhttp.NewTunnelFeed(sc.conn, (*tunnelWriter)(rs), int(sc.h.config.MaxBodyBytes))}
	// Set under the lock, where a shutdown retiring the connection looks for
	// its tunnels. One that began before this is told here instead.
	sc.mu.Lock()
	rs.tunnel = tunnel
	sc.mu.Unlock()
	if sc.retiring.Load() {
		tunnel.Shutdown()
	}
	rs.block.Context(sc.conn, rs, nil)
	sc.h.streams.Queue(&sc.batch, &sc.gate, sc.conn, sc.h.handler, &rs.block)
}

// peerEndedTunnel ends the tunnel of a stream whose peer has ended its side,
// and this side too once what it has sent has gone. A handler that has not
// accepted the tunnel yet cannot any more, and answers the request as it
// would any other.
func (rs *requestStream) peerEndedTunnel() {
	rs.tunnel.End(io.EOF)
	rs.mu.Lock()
	upgraded := rs.upgraded && !rs.closed
	rs.mu.Unlock()
	if upgraded {
		_ = rs.s.Close()
		rs.sc.untrack(rs)
	}
}

// Upgrade accepts an extended CONNECT for protocol, or with protocol empty a
// CONNECT to a host and port, with a 200 that leaves the stream open, which
// carries the tunnel from here on. It makes requestStream an
// http.StreamUpgrader.
func (rs *requestStream) Upgrade(req *stdhttp.Request, protocol string, header stdhttp.Header) (*fibhttp.Tunnel, error) {
	if rs.tunnel == nil || !strings.EqualFold(req.Header.Get(":protocol"), protocol) {
		return nil, fibhttp.ErrNotUpgradable
	}
	if err := checkHeader(header); err != nil {
		return nil, err
	}
	if max, limited := rs.fieldLimit(); limited {
		size := uint64(statusSize) + headerSize(header, func(name string) bool { return name == "content-length" })
		if size > max {
			return nil, errFieldSectionTooLarge
		}
	}
	rs.mu.Lock()
	switch {
	case rs.responded:
		rs.mu.Unlock()
		return nil, fibhttp.ErrResponseWritten
	case rs.closed || rs.remoteDone:
		rs.mu.Unlock()
		return nil, errStreamClosed
	}
	rs.responded, rs.upgraded = true, true
	rs.mu.Unlock()
	block := bufferpool.Append(nil, qpack.Prefix)
	block = qpack.AppendField(block, ":status", "200", false)
	block = appendHeader(block, header, func(name string) bool { return name == "content-length" })
	err := rs.s.Write(appendHeadersFrame(nil, block), false)
	bufferpool.Put(block)
	if err != nil {
		return nil, errStreamClosed
	}
	return rs.tunnel.Tunnel(), nil
}

// abortTunnel resets both sides of the stream of a tunnel closed from this
// side.
func (rs *requestStream) abortTunnel() {
	rs.mu.Lock()
	wasClosed := rs.closed
	rs.closed = true
	rs.mu.Unlock()
	if wasClosed {
		return
	}
	rs.s.Reset(uint64(ErrCodeRequestCancelled))
	rs.s.StopSending(uint64(ErrCodeRequestCancelled))
	rs.tunnel.End(net.ErrClosed)
	rs.sc.untrack(rs)
}

// tunnelWriter is a request stream as what its tunnel sends through, each
// send in a DATA frame of its own.
type tunnelWriter requestStream

func (w *tunnelWriter) SendParts(first, second []byte) error {
	n := len(first) + len(second)
	if n == 0 {
		return nil
	}
	out := bufferpool.Get(n + 16)[:0]
	out = appendFrameHeader(out, frameData, n)
	out = append(out, first...)
	out = append(out, second...)
	// The stream takes out over, as the buffer it sends from until the peer
	// has acknowledged it.
	if err := w.s.WriteOwned(out, false); err != nil {
		return errStreamClosed
	}
	return nil
}

func (w *tunnelWriter) SendOwned(data []byte) error { return w.SendParts(data, nil) }

func (w *tunnelWriter) CloseAfterSend() { _ = w.s.Close() }

func (w *tunnelWriter) Close() error {
	(*requestStream)(w).abortTunnel()
	return nil
}

// streamTunnel is a request stream's tunnel, and how it is paced: paced is
// set once the tunnel's handler has held its reads, from when the stream
// gives the peer room for what the tunnel delivers only while held is clear,
// and owes it what arrived while held was set. It is kept apart from the
// stream so that the streams of ordinary requests stay small.
type streamTunnel struct {
	*fibhttp.TunnelFeed

	mu          sync.Mutex
	paced, held bool
	owed        int
}

// HoldReads stops or resumes giving the peer room for more of the stream as
// the tunnel takes what arrives. The first hold has the stream give room
// only as this side says, from then on.
func (w *tunnelWriter) HoldReads(hold bool) {
	t := w.tunnel
	t.mu.Lock()
	first := !t.paced
	t.paced, t.held = true, hold
	owed := 0
	if !hold {
		owed, t.owed = t.owed, 0
	}
	t.mu.Unlock()
	if first {
		// What has been delivered already counts as consumed; what arrives
		// from here on is credit's to give back.
		w.s.HoldCredit()
	}
	w.s.Credit(owed)
}

// credit gives the peer room for n more bytes of a paced tunnel's stream s,
// or owes it them while the tunnel's reads are held, and reports whether
// the tunnel is paced.
func (t *streamTunnel) credit(s *quic.Stream, n int) bool {
	t.mu.Lock()
	if !t.paced {
		t.mu.Unlock()
		return false
	}
	if t.held {
		t.owed += n
		t.mu.Unlock()
		return true
	}
	t.mu.Unlock()
	s.Credit(n)
	return true
}

// Unsent is what the stream has still to send of what the tunnel wrote.
func (w *tunnelWriter) Unsent() int { return w.s.Unsent() }

// WaitSendable waits until no more than limit bytes are still to be sent.
func (w *tunnelWriter) WaitSendable(limit int) error {
	if w.s.WaitSendable(limit) != nil {
		return errStreamClosed
	}
	return nil
}

// NotifySendable runs fn once no more than half of limit bytes are still to
// be sent, when more than limit are, or with an error once the stream's
// sending side has ended; see fibhttp.Tunnel.NotifySendable. The stream has
// room for one such callback, so a second call before the first has run
// replaces it, as Stream.NotifySendable does.
func (w *tunnelWriter) NotifySendable(limit int, fn func(error)) bool {
	wrapped := func(err error) {
		if err != nil {
			err = errStreamClosed
		}
		fn(err)
	}
	if w.s.Unsent() <= limit {
		// A stream whose sending side has ended still takes the callback,
		// which runs with its error.
		return w.s.NotifySendable(limit, wrapped)
	}
	if !w.s.NotifySendable(limit/2, wrapped) {
		// It drained between the two looks.
		sidepool.Post(func() { fn(nil) })
	}
	return true
}

var (
	_ fibhttp.StreamUpgrader = (*requestStream)(nil)
	_ fibhttp.TunnelPacer    = (*tunnelWriter)(nil)
	_ fibhttp.TunnelNotifier = (*tunnelWriter)(nil)
)
