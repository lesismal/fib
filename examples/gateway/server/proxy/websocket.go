//go:build linux || darwin || windows

package proxy

import (
	stdhttp "net/http"
	"sync"
	"time"
	"unicode/utf8"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/websocket"
)

// closeBadGateway is the close code (RFC 6455 section 7.4.2, registered since)
// for a gateway that could not reach, or lost, the upstream.
const closeBadGateway = 1014

// serveWebSocket relays a WebSocket connection. Upgrade has to happen before
// the handler returns, so the client is upgraded first and the upstream is
// dialed after it, asynchronously: what the client sends meanwhile waits in a
// queue, and an upstream that cannot be reached closes the client's connection
// with "bad gateway". Over HTTP/1.1 the upgrade switches the connection; over
// HTTP/2 and HTTP/3 it answers the extended CONNECT and only that stream
// switches.
//
// From then on each side's callbacks write to the other, so a message goes
// from one socket to the other without a goroutine in between.
func (g *Gateway) serveWebSocket(c *fibhttp.Context, route *Route) {
	r := c.Request
	target := route.wsURL(r.URL)
	header := make(stdhttp.Header, 8)
	for _, name := range []string{"Origin", "Cookie", "Authorization", "User-Agent"} {
		if value := r.Header.Get(name); value != "" {
			header.Set(name, value)
		}
	}
	addForwarded(header, r)
	proto := r.Proto

	// The handshake picks the first subprotocol the client offers that the
	// gateway accepts; the upstream is then asked for that one.
	config := websocket.DefaultConfig()
	config.Subprotocols = g.config.Subprotocols
	s := &wsSession{}
	down, err := websocket.NewHandlerWithConfig(config, s.downstreamHandler()).Upgrade(c, nil)
	if err != nil {
		// Upgrade has answered the client already.
		g.logf("%s websocket %s: upgrade: %v", proto, target, err)
		return
	}
	s.down = down

	dialConfig := websocket.DefaultDialerConfig()
	dialConfig.HandshakeTimeout = 10 * time.Second
	dialConfig.TLSConfig = g.config.TLSConfig
	if chosen := down.Subprotocol(); chosen != "" {
		dialConfig.Subprotocols = []string{chosen}
	}
	websocket.NewDialer(g.engine, dialConfig).Dial(target, header, s.upstreamHandler(), func(up *websocket.Connection, resp *stdhttp.Response, err error) {
		if err != nil {
			g.logf("%s websocket %s: dial: %v", proto, target, err)
			_ = down.Close(closeBadGateway, "upstream unreachable")
			return
		}
		g.logf("%s websocket %s open", proto, target)
		s.attach(up)
	})
}

// wsSession joins the two halves of a relayed connection. The slower side's
// write queue is what holds the faster one back.
type wsSession struct {
	// down is set as soon as the client is upgraded, and before the upstream
	// is dialed, so the upstream's callbacks can rely on it.
	down *websocket.Connection

	mu sync.Mutex
	// up is nil until the dial is done, and what the client sends meanwhile
	// waits in queue, in order. closing is how the client closed in that time.
	up      *websocket.Connection
	queue   []queuedMessage
	closing *closeFrame
}

type queuedMessage struct {
	opcode websocket.Opcode
	data   []byte
}

type closeFrame struct {
	code   uint16
	reason string
	err    error
}

// attach connects the upstream half and delivers what the client said before
// it existed.
func (s *wsSession) attach(up *websocket.Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.up = up
	for _, m := range s.queue {
		if err := up.WriteMessage(m.opcode, m.data); err != nil {
			_ = s.down.Close(closeBadGateway, "upstream write failed")
			break
		}
	}
	s.queue = nil
	if s.closing != nil {
		relayClose(up, s.closing.code, s.closing.reason, s.closing.err)
	}
}

// downstreamHandler receives what the client sends and writes it upstream.
func (s *wsSession) downstreamHandler() websocket.Handler {
	return websocket.HandlerFuncs{
		Message: func(down *websocket.Connection, opcode websocket.Opcode, data []byte) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.up == nil {
				// data is only valid during this call.
				s.queue = append(s.queue, queuedMessage{opcode, append([]byte(nil), data...)})
				return
			}
			if err := s.up.WriteMessage(opcode, data); err != nil {
				_ = down.Close(closeBadGateway, "upstream write failed")
			}
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.up == nil {
				s.closing = &closeFrame{code, reason, err}
				return
			}
			relayClose(s.up, code, reason, err)
		},
	}
}

// upstreamHandler receives what the upstream sends and writes it to the client.
func (s *wsSession) upstreamHandler() websocket.Handler {
	return websocket.HandlerFuncs{
		Message: func(up *websocket.Connection, opcode websocket.Opcode, data []byte) {
			if err := s.down.WriteMessage(opcode, data); err != nil {
				_ = up.Close(websocket.CloseGoingAway, "downstream write failed")
			}
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			relayClose(s.down, code, reason, err)
		},
	}
}

// relayClose closes the other half the way its peer closed, with the same
// code and reason where the protocol allows them to be sent, and as "going
// away" when the peer vanished without saying anything.
func relayClose(other *websocket.Connection, code uint16, reason string, err error) {
	if !sendable(code) {
		code = websocket.CloseNormal
		if err != nil {
			code = websocket.CloseGoingAway
		}
	}
	// A close reason shares its 125 bytes with the code, and has to stay valid
	// UTF-8 when cut.
	if len(reason) > 123 {
		reason = reason[:123]
		for !utf8.ValidString(reason) {
			reason = reason[:len(reason)-1]
		}
	}
	_ = other.Close(code, reason)
}

// sendable reports whether a close code may appear in a Close frame: RFC
// 6455 reserves 1004, 1005 and 1006 for reporting, never for sending.
func sendable(code uint16) bool {
	if code >= 1000 && code <= 1014 {
		return code != 1004 && code != 1005 && code != 1006
	}
	return code >= 3000 && code <= 4999
}
