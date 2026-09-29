//go:build linux || darwin || windows

package http

import (
	"io"
	"net"
	stdhttp "net/http"
	"strings"

	"github.com/lesismal/fib/bufferpool"
)

// Extended CONNECT over HTTP/2 (RFC 8441): a request whose :protocol names
// what it wants to speak, such as WebSocket, opens a stream that its handler
// may switch to a tunnel with Context.Upgrade. The stream then carries the
// tunnel's bytes in DATA frames both ways, under the stream's flow control,
// until each side ends it with END_STREAM or either resets it.

// h2Tunnel is what a stream keeps for its tunnel, apart from the stream so
// that the streams of ordinary requests stay small. upgraded is set once the
// handler has accepted the tunnel, from when the stream carries its bytes both
// ways; ending once this side has asked to end it, which it does with
// END_STREAM once what is pending has gone; ended once the tunnel has been
// told it has ended. sendBuf is the array the stream's pending bytes are the
// tail of while the tunnel waits for the peer's window. All but feed are
// guarded by the connection's mu.
type h2Tunnel struct {
	feed     TunnelFeed
	upgraded bool
	ending   bool
	ended    bool
	sendBuf  []byte
}

// upgraded reports whether the stream carries an accepted tunnel. Callers hold
// the connection's mu.
func (st *h2ServerStream) upgraded() bool { return st.tunnel != nil && st.tunnel.upgraded }

// serveTunnel runs the handler of an extended CONNECT, whose stream stays
// open for it: it has no body, only whatever the tunnel carries once the
// handler has accepted it, which waits in the tunnel until then.
func (sc *h2ServerConn) serveTunnel(st *h2ServerStream) {
	st.tunnel = &h2Tunnel{feed: TunnelFeed{tunnel: Tunnel{conn: sc.conn, w: (*h2TunnelWriter)(st),
		limit: int(sc.handler.config.MaxBodyBytes)}}}
	st.block.bind(sc.conn, nil).stream = st
	sc.handler.streams.Serve(&sc.gate, sc.conn, sc.handler.handler, &st.block)
}

// handleTunnelData hands a DATA frame to a tunnel. The window it took is
// given back as it arrives, as a body read whole's is: the tunnel holds only
// what arrives before the handler accepts it, up to its limit, and a peer
// that sends more than that has its stream reset.
func (sc *h2ServerConn) handleTunnelData(st *h2ServerStream, f *h2Frame) error {
	length := int64(f.length)
	if length > st.recvWindow {
		return &H2StreamError{StreamID: f.streamID, Code: H2FlowControlError}
	}
	st.recvWindow -= length
	st.recvUnacked += length
	if st.recvUnacked >= h2StreamWindow/2 {
		increment := st.recvUnacked
		st.recvWindow += increment
		st.recvUnacked = 0
		sc.mu.Lock()
		sc.sendLocked(h2AppendWindowUpdate(nil, st.id, uint32(increment)))
		sc.mu.Unlock()
	}
	if err := st.tunnel.feed.Write(f.payload); err != nil {
		return &H2StreamError{StreamID: f.streamID, Code: H2EnhanceYourCalm}
	}
	if !f.has(h2FlagEndStream) {
		return nil
	}
	// The peer has ended its side, which ends the tunnel; this side ends
	// once what is pending has gone. A handler that has not accepted it yet
	// cannot any more, and answers the request as it would any other.
	sc.mu.Lock()
	st.remoteDone = true
	told := st.tunnel.ended
	st.tunnel.ended = true
	var out []byte
	if st.tunnel.upgraded && !st.localDone && !st.tunnel.ending {
		st.tunnel.ending = true
		out = sc.endTunnelStreamLocked(st)
	}
	sc.sendPooledLocked(out)
	if st.localDone {
		sc.forgetLocked(st)
	}
	sc.mu.Unlock()
	sc.closeIfDone()
	if !told {
		st.tunnel.feed.End(io.EOF)
	}
	return nil
}

// endTunnelLocked tells a stream's tunnel, if it has one, that it has ended.
// The tunnel's handler may send from the call, which needs the connection's
// lock, so the call is made on a goroutine of its own.
func (sc *h2ServerConn) endTunnelLocked(st *h2ServerStream, err error) {
	if st.tunnel == nil || st.tunnel.ended {
		return
	}
	st.tunnel.ended = true
	go st.tunnel.feed.End(err)
}

// upgrade accepts an extended CONNECT for protocol with a 200 that leaves the
// stream open, which carries the tunnel from here on.
func (st *h2ServerStream) upgrade(req *stdhttp.Request, protocol string, header stdhttp.Header) (*Tunnel, error) {
	if st.tunnel == nil || !strings.EqualFold(req.Header.Get(":protocol"), protocol) {
		return nil, ErrNotUpgradable
	}
	if err := h2CheckHeader(header); err != nil {
		return nil, err
	}
	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || st.reset || st.remoteDone || st.tunnel.ended {
		return nil, errH2StreamClosed
	}
	if st.responded {
		return nil, ErrResponseWritten
	}
	st.responded, st.tunnel.upgraded = true, true
	block := sc.enc.Begin(sc.headerBlock[:0])
	block = sc.enc.AppendField(block, ":status", "200", false)
	block = sc.appendHeaderLocked(block, header)
	out := bufferpool.Get(len(block) + h2FrameHeaderLen*(1+len(block)/sc.peerMaxFrame))[:0]
	out = h2AppendHeaderBlock(out, st.id, block, false, sc.peerMaxFrame)
	sc.keepHeaderBlockLocked(block)
	sc.sendPooledLocked(out)
	return st.tunnel.feed.Tunnel(), nil
}

// sendTunnel sends first and second through the stream's tunnel, as far as
// the windows let them go now, and keeps the rest until they let it. With end
// set it then ends this side of the stream.
func (st *h2ServerStream) sendTunnel(first, second []byte, end bool) error {
	sc := st.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || st.reset || !st.upgraded() || st.tunnel.ending || st.localDone {
		return errH2StreamClosed
	}
	n := len(first) + len(second)
	if int64(len(st.pending)+n) > sc.handler.config.MaxBodyBytes {
		return ErrTunnelBacklog
	}
	if n > 0 {
		buf := st.tunnel.sendBuf
		if len(st.pending) == 0 {
			buf = buf[:0]
		} else if len(st.pending) != len(buf) {
			// pending is the tail of buf; bring it back to the front.
			buf = buf[:copy(buf, st.pending)]
		}
		buf = bufferpool.Append(buf, first)
		buf = bufferpool.Append(buf, second)
		st.tunnel.sendBuf, st.pending = buf, buf
	}
	st.tunnel.ending = end
	sc.sendPooledLocked(sc.endTunnelStreamLocked(st))
	if len(st.pending) == 0 {
		// Nothing is waiting, so the array goes back to the pool rather than
		// staying with a tunnel that may say nothing more for a while.
		bufferpool.Put(st.tunnel.sendBuf)
		st.tunnel.sendBuf = nil
	}
	sc.finishStreamLocked(st)
	return nil
}

// endTunnelStreamLocked frames what of a tunnel's pending bytes the windows
// allow, in a buffer from the pool, and END_STREAM once nothing is pending if
// the tunnel is ending.
func (sc *h2ServerConn) endTunnelStreamLocked(st *h2ServerStream) []byte {
	size := h2FrameHeaderLen
	if len(st.pending) > 0 {
		sendable := max(min(int64(len(st.pending)), sc.sendWindow, st.sendWindow), 0)
		size += int(sendable) + h2FrameHeaderLen*(1+int(sendable)/sc.peerMaxFrame)
	}
	out := sc.flushStreamLocked(bufferpool.Get(size)[:0], st)
	if st.tunnel.ending && len(st.pending) == 0 && !st.localDone {
		out = h2AppendFrameHeader(out, h2FrameData, h2FlagEndStream, st.id, 0)
		st.localDone = true
	}
	return out
}

// abortTunnel resets the stream of a tunnel closed from this side.
func (st *h2ServerStream) abortTunnel() {
	sc := st.sc
	sc.mu.Lock()
	if sc.streams[st.id] == st {
		sc.endTunnelLocked(st, net.ErrClosed)
		sc.sendLocked(h2AppendRSTStream(nil, st.id, H2Cancel))
		sc.forgetLocked(st)
	}
	sc.mu.Unlock()
	sc.closeIfDone()
}

// h2TunnelWriter is a stream as what its tunnel sends through.
type h2TunnelWriter h2ServerStream

func (w *h2TunnelWriter) SendParts(first, second []byte) error {
	return (*h2ServerStream)(w).sendTunnel(first, second, false)
}

// SendOwned copies data like SendParts, since what the windows hold back has
// to be gathered behind what is pending anyway.
func (w *h2TunnelWriter) SendOwned(data []byte) error {
	return (*h2ServerStream)(w).sendTunnel(data, nil, false)
}

func (w *h2TunnelWriter) CloseAfterSend() { _ = (*h2ServerStream)(w).sendTunnel(nil, nil, true) }

func (w *h2TunnelWriter) Close() error {
	(*h2ServerStream)(w).abortTunnel()
	return nil
}
