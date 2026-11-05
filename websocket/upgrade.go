//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bytes"
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
//	handler := http.HandlerFunc(func(c *http.Context) {
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
	u.ws = Connection{subprotocol: result.subprotocol, compress: compress, windowBits: uint8(result.deflate.windowBits)}
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
	if !h.checkOrigin(request) {
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

// checkOrigin reports whether the handshake request may open a WebSocket:
// what Config.CheckOrigin says, or, when it is nil, whether the request is
// same-origin.
func (h *ServerHandler) checkOrigin(request *stdhttp.Request) bool {
	if h.config.CheckOrigin != nil {
		return h.config.CheckOrigin(request)
	}
	return CheckSameOrigin(request)
}

// CheckSameOrigin is what a ServerHandler whose Config.CheckOrigin is nil
// accepts, as gorilla/websocket does: a request with no Origin header, which
// is not from a browser, or one whose single Origin names the host the
// request is for, compared without regard to case. It keeps a page of another
// site from opening a WebSocket with the cookies and credentials a browser
// sends along (cross-site WebSocket hijacking).
func CheckSameOrigin(request *stdhttp.Request) bool {
	origin := request.Header["Origin"]
	switch len(origin) {
	case 0:
		return true
	case 1:
		return sameOrigin([]byte(origin[0]), []byte(request.Host))
	}
	return false
}

// sameOrigin reports whether origin, an Origin header's value of the form
// scheme://host[:port], names host. "null", what a browser sends for an
// opaque origin, names none.
func sameOrigin(origin, host []byte) bool {
	i := bytes.Index(origin, []byte("://"))
	if i <= 0 {
		return false
	}
	authority := origin[i+3:]
	if end := bytes.IndexAny(authority, "/?#"); end >= 0 {
		authority = authority[:end]
	}
	if at := bytes.LastIndexByte(authority, '@'); at >= 0 {
		authority = authority[at+1:]
	}
	return len(authority) != 0 && bytes.EqualFold(authority, host)
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

var _ epollhttp.TunnelShutdownHandler = (*upgradedConn)(nil)

// OnTunnelShutdown sends a close frame with 1001 (going away) as the server
// shuts down, which ends the tunnel once it has gone; see
// http.TunnelShutdownHandler.
func (u *upgradedConn) OnTunnelShutdown(*epollhttp.Tunnel) {
	_ = u.ws.Close(CloseGoingAway, "")
}

func (u *upgradedConn) OnTunnelClose(_ *epollhttp.Tunnel, err error) {
	code, reason := u.ws.closeStatus()
	u.handler.handler.OnClose(&u.ws, code, reason, err)
}
