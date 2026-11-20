//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"

	epollhttp "github.com/lesismal/fib/http"
)

// sideReport is what a test's callbacks saw of a server's connection.
type sideReport struct {
	netConn, tunnel, tunnelConn bool
	local, remote               string
	attachment                  any
}

func reportSide(c *Connection) sideReport {
	r := sideReport{netConn: c.NetConn() != nil, tunnel: c.Tunnel() != nil, attachment: c.Attachment()}
	if t := c.Tunnel(); t != nil {
		r.tunnelConn = t.Conn() != nil
	}
	if a := c.LocalAddr(); a != nil {
		r.local = a.String()
	}
	if a := c.RemoteAddr(); a != nil {
		r.remote = a.String()
	}
	return r
}

// A connection the ServerHandler serves itself has its own fib.Connection,
// no tunnel, both addresses, and keeps what OnOpen attaches for every later
// callback, OnClose included. A dialed one has its own connection too.
func TestServedConnectionTransportAndAttachment(t *testing.T) {
	opened := make(chan sideReport, 1)
	messaged := make(chan any, 1)
	closed := make(chan any, 1)
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) {
			r := reportSide(c)
			c.SetAttachment("session")
			opened <- r
		},
		Message: func(c *Connection, _ Opcode, _ []byte) { messaged <- c.Attachment() },
		Close:   func(c *Connection, _ uint16, _ string, _ error) { closed <- c.Attachment() },
	})
	ws, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	server := waitFor(t, opened, "OnOpen")
	addr := strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws")
	if !server.netConn || server.tunnel || server.attachment != nil || server.local != addr {
		t.Fatalf("served connection: %+v, want its own connection, no tunnel or attachment, local %s", server, addr)
	}
	client := reportSide(ws)
	if !client.netConn || client.tunnel || client.local != server.remote || client.remote != addr {
		t.Fatalf("dialed connection %+v, server's %+v", client, server)
	}
	if err := ws.WriteText("hi"); err != nil {
		t.Fatal(err)
	}
	if got := waitFor(t, messaged, "OnMessage"); got != "session" {
		t.Fatalf("OnMessage saw attachment %v", got)
	}
	_ = ws.Close(CloseNormal, "")
	if got := waitFor(t, closed, "OnClose"); got != "session" {
		t.Fatalf("OnClose saw attachment %v", got)
	}
}

// UpgradeWithAttachment attaches its value before OnOpen, and an upgraded
// connection has a tunnel and no connection of its own.
func TestUpgradeWithAttachment(t *testing.T) {
	type session struct{ id int }
	want := &session{id: 7}
	opened := make(chan sideReport, 1)
	messaged := make(chan any, 1)
	ws := NewHandler(HandlerFuncs{
		Open:    func(c *Connection, _ *stdhttp.Request) { opened <- reportSide(c) },
		Message: func(c *Connection, _ Opcode, _ []byte) { messaged <- c.Attachment() },
	})
	upgraded := make(chan *Connection, 1)
	addr := serveHTTP(t, func(c *epollhttp.Context) {
		conn, err := ws.UpgradeWithAttachment(c, nil, want)
		if err != nil {
			t.Error(err)
		}
		upgraded <- conn
	})
	client, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go("ws://"+addr+"/ws", nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(CloseNormal, "")
	server := waitFor(t, opened, "OnOpen")
	if server.netConn || !server.tunnel || !server.tunnelConn || server.attachment != want || server.local != addr {
		t.Fatalf("upgraded connection: %+v, want a tunnel, attachment %p, local %s", server, want, addr)
	}
	if conn := waitFor(t, upgraded, "Upgrade"); conn == nil || conn.Attachment() != want {
		t.Fatal("UpgradeWithAttachment returned a connection without the attachment")
	}
	if err := client.WriteText("hi"); err != nil {
		t.Fatal(err)
	}
	if got := waitFor(t, messaged, "OnMessage"); got != want {
		t.Fatalf("OnMessage saw attachment %v", got)
	}
}

// SetAttachment's slot holds one value, which nil clears, and may be set and
// read from any goroutine.
func TestAttachmentSlot(t *testing.T) {
	var c Connection
	if c.Attachment() != nil {
		t.Fatal("a new connection has an attachment")
	}
	c.SetAttachment(1)
	c.SetAttachment("two")
	if got := c.Attachment(); got != "two" {
		t.Fatalf("Attachment %v, want the last set", got)
	}
	c.SetAttachment(nil)
	if got := c.Attachment(); got != nil {
		t.Fatalf("Attachment %v after SetAttachment(nil)", got)
	}
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for j := range 1000 {
				c.SetAttachment(i*1000 + j)
				_ = c.Attachment()
			}
		})
	}
	wg.Wait()
	if c.NetConn() != nil || c.Tunnel() != nil || c.LocalAddr() != nil {
		t.Fatal("a connection without a transport reports one")
	}
}
