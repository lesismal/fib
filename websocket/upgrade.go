//go:build linux || darwin || windows

package websocket

import (
	"errors"
	stdhttp "net/http"
	"strings"

	epollhttp "github.com/lesismal/fib/http"
)

var errOriginRejected = errors.New("websocket: origin rejected")

// Upgrade switches a request that a server of package http or package http3
// is serving to WebSocket, and returns the connection, which the
// ServerHandler's Handler serves from then on, as it serves those that come
// to the ServerHandler itself. It is how one server answers WebSocket and
// plain HTTP requests alike, which Upgrade is called from the handler of:
//
//	ws := websocket.NewHandler(wsHandler)
//	handler := http.HandlerFunc(func(c *http.Context, r *stdhttp.Request) {
//		if r.URL.Path == "/ws" {
//			_, _ = ws.Upgrade(c, nil)
//			return
//		}
//		_ = c.Respond(200, "text/plain", []byte("hello"))
//	})
//
// Over HTTP/1.1 the request is the opening handshake of RFC 6455, and the
// whole connection switches. Over HTTP/2 and HTTP/3 it is an extended CONNECT
// whose :protocol is "websocket" (RFC 8441, RFC 9220), which those servers
// invite browsers to send, and only its stream switches, while the connection
// goes on serving other requests. Either way the Config's CheckOrigin,
// Subprotocols and EnableCompression apply, and header is sent with the
// response that accepts the handshake, which Upgrade writes.
//
// A request that is not a valid handshake is answered with 400 Bad Request,
// or 403 Forbidden when CheckOrigin rejects it, and Upgrade reports why.
// Handler.OnOpen has been called by the time Upgrade returns, with the
// request, which it must not keep: the server may reuse it once the handler
// that called Upgrade has returned.
func (h *ServerHandler) Upgrade(c *epollhttp.Context, header stdhttp.Header) (*Connection, error) {
	request := c.Request
	var result handshakeResult
	var err error
	extended := request.ProtoMajor >= 2
	if extended {
		result, err = h.validateExtendedConnect(request)
	} else {
		result, err = h.validateHandshake(request)
	}
	if err != nil {
		h.refuse(c, err)
		return nil, err
	}
	response := make(stdhttp.Header, len(header)+3)
	for key, values := range header {
		response[key] = values
	}
	if !extended {
		var accept [28]byte
		websocketAccept(accept[:], []byte(request.Header.Get("Sec-Websocket-Key")))
		response["Sec-WebSocket-Accept"] = []string{string(accept[:])}
	}
	if result.subprotocol != "" {
		response["Sec-WebSocket-Protocol"] = []string{result.subprotocol}
	}
	if result.extensions != "" {
		response["Sec-WebSocket-Extensions"] = []string{result.extensions}
	}
	compress := result.extensions != ""
	u := &upgradedConn{handler: h, request: request}
	u.ws = Connection{subprotocol: result.subprotocol, compress: compress, windowBits: result.deflate.windowBits}
	u.parser = Parser{maxMessageBytes: h.config.MaxMessageBytes, deflate: compress,
		contextTakeover: compress && result.deflate.peerContextTakeover, perFrame: h.frames != nil}
	if _, err := c.Upgrade("websocket", response, u); err != nil {
		return nil, err
	}
	return &u.ws, nil
}

// validateExtendedConnect checks an extended CONNECT that opens a WebSocket
// (RFC 8441 section 5), which has no key to answer: the stream it arrives on
// is the connection's own, so nothing else can be confused with it.
func (h *ServerHandler) validateExtendedConnect(request *stdhttp.Request) (handshakeResult, error) {
	if request.Method != stdhttp.MethodConnect ||
		!strings.EqualFold(request.Header.Get(":protocol"), "websocket") ||
		request.Header.Get("Sec-Websocket-Version") != "13" {
		return handshakeResult{}, ErrProtocol
	}
	if h.config.CheckOrigin != nil && !h.config.CheckOrigin(request) {
		return handshakeResult{}, errOriginRejected
	}
	var result handshakeResult
	var err error
	if result.subprotocol, err = h.selectSubprotocol(request); err != nil {
		return handshakeResult{}, err
	}
	h.negotiateExtensions(&result, request.Header.Values("Sec-Websocket-Extensions"))
	return result, nil
}

// refuse answers a request that is not a handshake the server accepts.
func (h *ServerHandler) refuse(c *epollhttp.Context, err error) {
	status := stdhttp.StatusBadRequest
	header := stdhttp.Header{"Content-Type": {"text/plain; charset=utf-8"}}
	if errors.Is(err, errOriginRejected) {
		status = stdhttp.StatusForbidden
	} else {
		// What a client offering another version needs to hear (RFC 6455
		// section 4.4).
		header["Sec-WebSocket-Version"] = []string{"13"}
	}
	_ = c.WriteResponse(epollhttp.Response{StatusCode: status, Header: header,
		Body: []byte(stdhttp.StatusText(status) + "\n")})
}

// upgradedConn is a WebSocket connection that a request of package http's
// switched to: the tunnel's handler, serving frames as the ServerHandler
// serves those of the connections that come to it.
type upgradedConn struct {
	handler *ServerHandler
	request *stdhttp.Request
	ws      Connection
	parser  Parser
}

func (u *upgradedConn) OnTunnelOpen(t *epollhttp.Tunnel) {
	u.ws.conn = t
	request := u.request
	u.request = nil
	u.handler.handler.OnOpen(&u.ws, request)
}

func (u *upgradedConn) OnTunnelData(_ *epollhttp.Tunnel, data []byte) {
	serveFrames(u.handler.handler, u.handler.frames, &u.ws, &u.parser, data)
}

func (u *upgradedConn) OnTunnelClose(_ *epollhttp.Tunnel, err error) {
	code, reason := u.ws.closeStatus()
	u.handler.handler.OnClose(&u.ws, code, reason, err)
}
