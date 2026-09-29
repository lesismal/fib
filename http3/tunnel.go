//go:build linux || darwin || windows

package http3

import (
	"io"
	"net"
	stdhttp "net/http"
	"strings"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
)

// Extended CONNECT over HTTP/3 (RFC 9220): a request whose :protocol names
// what it wants to speak, such as WebSocket, opens a stream that its handler
// may switch to a tunnel with Context.Upgrade. The stream then carries the
// tunnel's bytes in DATA frames both ways until each side ends it with FIN or
// either resets it.

// serveTunnel runs the handler of an extended CONNECT, whose stream stays
// open for it: it has no body, only whatever the tunnel carries once the
// handler has accepted it, which waits in the tunnel until then.
func (rs *requestStream) serveTunnel() {
	sc := rs.sc
	rs.tunnel = fibhttp.NewTunnelFeed(sc.conn, (*tunnelWriter)(rs), int(sc.h.config.MaxBodyBytes))
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

// Upgrade accepts an extended CONNECT for protocol with a 200 that leaves the
// stream open, which carries the tunnel from here on. It makes requestStream
// an http.StreamUpgrader.
func (rs *requestStream) Upgrade(req *stdhttp.Request, protocol string, header stdhttp.Header) (*fibhttp.Tunnel, error) {
	if rs.tunnel == nil || !strings.EqualFold(req.Header.Get(":protocol"), protocol) {
		return nil, fibhttp.ErrNotUpgradable
	}
	if err := checkHeader(header); err != nil {
		return nil, err
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

var _ fibhttp.StreamUpgrader = (*requestStream)(nil)
