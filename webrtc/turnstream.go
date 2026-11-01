package webrtc

import (
	stdtls "crypto/tls"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/sidepool"
	"github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc/turn"
)

// dialTURN connects to a TURN server reached over TCP or TLS, without
// blocking: the allocation is asked for once the connection opens, and a
// dial that fails fails the relay. It runs under the mutex.
func (pc *PeerConnection) dialTURN(r *relay, ts turnServer) {
	h := &turnStream{pc: pc, r: r}
	done := func(_ *fib.Connection, err error) {
		if err != nil {
			// done runs on the event loop, which must not run the
			// callbacks the failure queues.
			sidepool.Post(h.lost)
		}
	}
	addr := ts.addr.String()
	var err error
	if ts.transport == turnTLS {
		conf := pc.cfg.TURNTLSConfig
		if conf == nil {
			conf = &stdtls.Config{}
		}
		if conf.ServerName == "" {
			conf = conf.Clone()
			conf.ServerName = ts.host
		}
		err = tls.Dial(pc.cfg.Engine, "tcp", addr, turnDialTimeout, conf, h, done)
	} else {
		err = pc.cfg.Engine.DialWithHandler("tcp", addr, turnDialTimeout, h, done)
	}
	if err != nil {
		pc.relayFailed(r)
	}
}

// turnStream is the fib.Handler of the connection to a TURN server over
// TCP or TLS. It frames what the server sends with turn.FrameLen and hands
// each message to the relay, as handleDatagram does with a UDP server's
// datagrams; the connection closing fails the relay.
type turnStream struct {
	pc *PeerConnection
	r  *relay
}

// OnOpen asks for the allocation. It runs on the event loop: what it does
// under the mutex queues no callback.
func (h *turnStream) OnOpen(c *fib.Connection) {
	pc, r := h.pc, h.r
	pc.mu.Lock()
	if pc.closed || r.failed {
		pc.mu.Unlock()
		_ = c.Close()
		return
	}
	r.conn = c
	_ = r.r.Client.Allocate(time.Now())
	pc.unlock()
}

func (h *turnStream) OnData(c *fib.Connection, b []byte) {
	pc, r := h.pc, h.r
	pc.mu.Lock()
	if pc.closed || r.failed {
		pc.mu.Unlock()
		return
	}
	data := b
	if len(r.buf) > 0 {
		r.buf = append(r.buf, b...)
		data = r.buf
	}
	now := time.Now()
	// A message may fail the relay, or close the PeerConnection with the
	// peer's close_notify.
	for !r.failed && !pc.closed {
		n, err := turn.FrameLen(data)
		if err != nil {
			pc.relayFailed(r)
			break
		}
		if n == 0 || n > len(data) {
			break
		}
		pc.relayPacket(r, data[:n], now)
		data = data[n:]
	}
	// copy, which append uses, moves overlapping bytes correctly when
	// data is the tail of r.buf.
	r.buf = append(r.buf[:0], data...)
	pc.unlock()
}

func (h *turnStream) OnPriorityData(*fib.Connection, []byte) {}

func (h *turnStream) OnClose(*fib.Connection, error) { h.lost() }

// lost fails the relay whose connection could not be made or has closed.
func (h *turnStream) lost() {
	pc := h.pc
	pc.mu.Lock()
	if !pc.closed {
		pc.relayFailed(h.r)
	}
	pc.unlock()
}
