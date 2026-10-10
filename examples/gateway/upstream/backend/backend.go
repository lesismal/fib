//go:build linux || darwin || windows

// Package backend is the upstream the gateway example forwards to. It speaks
// HTTP/1.1 on a cleartext port, and HTTP/2 and HTTP/3 on a TLS one, so that
// the gateway can be shown reaching an upstream over each protocol. The
// upstream command runs it, and the gateway's tests do.
package backend

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	stdhttp "net/http"
	"strconv"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/websocket"
)

// DefaultHandler is the backend itself: its routes are listed in the upstream
// command. /ws is a WebSocket echo that offers the subprotocols "chat" and
// "superchat".
func DefaultHandler() fibhttp.HandlerFunc {
	config := websocket.DefaultConfig()
	config.Subprotocols = []string{"chat", "superchat"}
	return Handler(websocket.NewHandlerWithConfig(config, echoWebSocket()))
}

// NewEngines binds the TLS engine, then the HTTP/3 one, then the cleartext
// one if plainAddr is not empty. Each is unstarted.
func NewEngines(tlsConfig *tls.Config, addr, plainAddr string) ([]*fib.Engine, error) {
	return NewEnginesFor(DefaultHandler(), tlsConfig, addr, plainAddr)
}

// NewEnginesFor is NewEngines serving handler, which can be DefaultHandler
// with routes of its own in front of it.
func NewEnginesFor(handler fibhttp.Handler, tlsConfig *tls.Config, addr, plainAddr string) ([]*fib.Engine, error) {
	httpConfig := fibhttp.DefaultConfig()
	httpConfig.MaxBodyBytes = 256 << 20
	tcpConfig := fib.DefaultConfig()
	tcpConfig.Addr = addr
	tcp, err := fib.Bind(tcpConfig, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandlerWithConfig(httpConfig, handler)))
	if err != nil {
		return nil, err
	}
	udpConfig := fib.DefaultConfig()
	udpConfig.Network = "udp"
	udpConfig.Addr = addr
	h3Config := http3.DefaultConfig()
	h3Config.TLSConfig = tlsConfig
	h3Config.MaxBodyBytes = 256 << 20
	udp, err := fib.Bind(udpConfig, http3.NewHandlerWithConfig(h3Config, handler))
	if err != nil {
		return nil, err
	}
	engines := []*fib.Engine{tcp, udp}
	if plainAddr != "" {
		plainConfig := fib.DefaultConfig()
		plainConfig.Addr = plainAddr
		plain, err := fib.Bind(plainConfig, fibhttp.NewHandlerWithConfig(httpConfig, handler))
		if err != nil {
			return nil, err
		}
		engines = append(engines, plain)
	}
	return engines, nil
}

func echoWebSocket() websocket.Handler {
	return websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, opcode websocket.Opcode, data []byte) {
			if opcode == websocket.Text && string(data) == "close" {
				// Lets a client see a close code that comes from the upstream.
				_ = c.Close(4002, "upstream says bye")
				return
			}
			if err := c.WriteMessage(opcode, data); err != nil {
				_ = c.Close(websocket.CloseInternalError, "write failed")
			}
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			fmt.Printf("upstream websocket closed: code=%d reason=%q error=%v\n", code, reason, err)
		},
	}
}

// Handler is the backend: its routes are listed in the upstream command, and
// ws serves /ws.
func Handler(ws *websocket.ServerHandler) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		r := c.Request
		switch r.URL.Path {
		case "/ws":
			_, _ = ws.Upgrade(c, nil)
		case "/echo":
			echo(c)
		case "/download":
			download(c)
		case "/stream":
			stream(c)
		case "/upload":
			sum := sha256.Sum256(c.Body())
			_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
				[]byte(fmt.Sprintf("%s received %d bytes sha256=%s\n", r.Proto, len(c.Body()), hex.EncodeToString(sum[:]))))
		case "/status":
			code, _ := strconv.Atoi(c.Query("code"))
			if code < 200 || code > 599 {
				code = stdhttp.StatusBadRequest
			}
			_ = c.Respond(code, "text/plain; charset=utf-8", []byte(stdhttp.StatusText(code)+"\n"))
		default:
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("not found\n"))
		}
	}
}

// echo answers with what the upstream saw, including the headers the gateway
// adds, so that the example shows the request as it arrived here.
func echo(c *fibhttp.Context) {
	r := c.Request
	reply := fmt.Sprintf("%s %s %s?%s\nX-Forwarded-For: %s\nX-Forwarded-Proto: %s\nVia: %s\nbody: %s\n",
		r.Proto, r.Method, r.URL.Path, r.URL.RawQuery,
		r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Forwarded-Proto"), r.Header.Values("Via"), c.Body())
	_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(reply))
}

// download writes size bytes, declared up front, in pieces.
func download(c *fibhttp.Context) {
	size, err := strconv.ParseInt(c.Query("size"), 10, 64)
	if err != nil || size < 0 || size > 1<<30 {
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain; charset=utf-8", []byte("size?\n"))
		return
	}
	c.Header().Set("Content-Type", "application/octet-stream")
	c.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	piece := make([]byte, 32<<10)
	for sent := int64(0); sent < size; {
		n := min(int64(len(piece)), size-sent)
		for i := range piece[:n] {
			piece[i] = byte('a' + (sent+int64(i))%26)
		}
		if _, err := c.Write(piece[:n]); err != nil {
			return
		}
		sent += n
	}
}

// stream sends a line every delay and ends with a trailer. It never waits: it
// retains the request and the timer writes the next line.
func stream(c *fibhttp.Context) {
	n, _ := strconv.Atoi(c.Query("n"))
	if n <= 0 || n > 1000 {
		n = 5
	}
	delay, err := time.ParseDuration(c.Query("delay"))
	if err != nil || delay < 0 {
		delay = 200 * time.Millisecond
	}
	c.Header().Set("Content-Type", "text/plain; charset=utf-8")
	c.Header().Set("Trailer", "X-Lines")
	c.Retain()
	var step func(i int)
	step = func(i int) {
		if i == n {
			c.Header().Set(stdhttp.TrailerPrefix+"X-Lines", strconv.Itoa(n))
			_ = c.Finish()
			c.Release()
			return
		}
		if _, err := fmt.Fprintf(c, "line %d of %d\n", i+1, n); err != nil {
			c.Release()
			return
		}
		c.Flush()
		time.AfterFunc(delay, func() { step(i + 1) })
	}
	step(0)
}
