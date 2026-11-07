package http

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/debughook"
)

// relaySide passes what arrives through one tunnel on to the other side's,
// holding its reads while the other side is behind, as a proxy of Upgrade
// tunnels does.
type relaySide struct {
	mu    sync.Mutex
	peer  *Tunnel
	other *relaySide
	// backpressure leaves each side's own backlog pacing its reads, which
	// deadlocks the relay; see Tunnel.SetWriteBackpressure.
	backpressure bool
}

func (s *relaySide) tunnel() *Tunnel {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peer
}

func (s *relaySide) set(t *Tunnel) {
	if !s.backpressure {
		t.SetWriteBackpressure(false)
	}
	s.mu.Lock()
	s.peer = t
	s.mu.Unlock()
}

func (s *relaySide) OnTunnelOpen(*Tunnel) {}
func (s *relaySide) OnTunnelData(t *Tunnel, data []byte) {
	to := s.other.tunnel()
	if to == nil || to.Send(data) != nil {
		_ = t.Close()
		return
	}
	t.HoldReads(true)
	if to.NotifySendable(256<<10, func(error) { t.HoldReads(false) }) {
		return
	}
	t.HoldReads(false)
}
func (s *relaySide) OnTunnelClose(*Tunnel, error) {
	if to := s.other.tunnel(); to != nil {
		to.CloseAfterSend()
	}
}

type shoutTunnel struct{ conns *tunnelConns }

func (h shoutTunnel) OnTunnelOpen(t *Tunnel) {
	shrinkBuffers(t.Conn())
	h.conns.add("backend", t.Conn())
}

// shrinkBuffers gives the leg between the proxy and the backend small send
// buffers, so that its sends back up into the connections' own queues, past
// the write watermark, as Windows' loopback did with its defaults. The
// receive buffers are left alone: shrinking one on a connection that has
// already advertised a larger window stalls Linux's TCP for seconds.
func shrinkBuffers(c *fib.Connection) {
	_ = c.SetWriteBuffer(4 << 10)
}
func (shoutTunnel) OnTunnelData(t *Tunnel, data []byte) {
	_ = t.Send(bytes.ToUpper(data))
}
func (shoutTunnel) OnTunnelClose(*Tunnel, error) {}

// tunnelConns names the connections of one run, for a hung run to report.
type tunnelConns struct {
	mu    sync.Mutex
	names []string
	conns []*fib.Connection
}

func (c *tunnelConns) add(name string, conn *fib.Connection) {
	c.mu.Lock()
	c.names = append(c.names, name)
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
}

func (c *tunnelConns) report() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var b strings.Builder
	for i, conn := range c.conns {
		fmt.Fprintf(&b, "== %s: %s\n", c.names[i], debughook.ConnState(conn))
		trace := debughook.ConnTrace(conn)
		if len(trace) > 12000 {
			trace = trace[len(trace)-12000:]
		}
		b.WriteString(trace)
		b.WriteString("\n")
	}
	return b.String()
}

// TestUpgradeRelayBulk relays an Upgrade tunnel through a proxy whose sides
// hold each other's reads while the other drains, and sends 2 MiB through
// it and back to a backend that answers as it reads, several times. With
// each side's own backlog pacing its reads, the proxy's leg to the backend
// stops reading the answers while its sends back up, and the backend stops
// reading while its answers do: fib-examples' http/upgrade hung that way on
// Windows. Tunnel.SetWriteBackpressure(false) on both sides lets them be
// paced by each other's backlog instead. With backpressure left on, the
// test deadlocks on macOS and Windows; Linux's larger receive buffers take
// the whole backlog.
func TestUpgradeRelayBulk(t *testing.T) { testUpgradeRelay(t, false, 20) }

func testUpgradeRelay(t *testing.T, backpressure bool, runs int) {
	debughook.SetTrace(true)
	t.Cleanup(func() { debughook.SetTrace(false) })
	var conns *tunnelConns
	var connsMu sync.Mutex
	current := func() *tunnelConns { connsMu.Lock(); defer connsMu.Unlock(); return conns }

	backendRouter := NewRouter()
	backendRouter.Get("/", func(c *Context) {
		_, _ = c.Upgrade("shout", nil, shoutTunnel{current()})
	})
	backend := serve(t, NewHandler(backendRouter))

	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	clientEngine, err := fib.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- clientEngine.Run() }()
	t.Cleanup(func() { clientEngine.Stop(); <-done; _ = clientEngine.Close() })
	client := NewClient(clientEngine, DefaultClientConfig())
	t.Cleanup(func() { client.Close() })

	proxyRouter := NewRouter()
	proxyRouter.Get("/", func(c *Context) {
		cs := current()
		cs.add("proxy-down", c.Conn)
		c.Retain()
		req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://"+backend+"/", nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "shout")
		up, down := &relaySide{backpressure: backpressure}, &relaySide{backpressure: backpressure}
		up.other, down.other = down, up
		client.StartUpgrade(req, up, func(resp *ClientResponse, err error) {
			defer c.Release()
			if err != nil || resp.Tunnel() == nil {
				_ = c.Respond(stdhttp.StatusBadGateway, "text/plain", nil)
				return
			}
			shrinkBuffers(resp.Tunnel().Conn())
			up.set(resp.Tunnel())
			cs.add("proxy-up", resp.Tunnel().Conn())
			tun, err := c.Upgrade("shout", nil, down)
			if err != nil {
				_ = resp.Tunnel().Close()
				return
			}
			down.set(tun)
		})
	})
	proxy := serve(t, NewHandler(proxyRouter))

	const size = 2 << 20
	for run := 0; run < runs; run++ {
		connsMu.Lock()
		conns = &tunnelConns{}
		connsMu.Unlock()
		conn, err := net.Dial("tcp", proxy)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: proxy\r\nConnection: Upgrade\r\nUpgrade: shout\r\n\r\nhello\n")
		r := bufio.NewReader(conn)
		resp, err := stdhttp.ReadResponse(r, nil)
		if err != nil || resp.StatusCode != stdhttp.StatusSwitchingProtocols {
			t.Fatalf("run %d: %v %v", run, resp, err)
		}
		if line, err := r.ReadString('\n'); err != nil || line != "HELLO\n" {
			t.Fatalf("run %d: %q %v", run, line, err)
		}
		go func() { _, _ = conn.Write(bytes.Repeat([]byte("abcdefgh"), size/8)) }()
		got := make([]byte, size)
		n, err := io.ReadFull(r, got)
		if err != nil {
			report := current().report()
			t.Log(report)
			fmt.Printf("\n::error title=TestUpgradeRelayBulk run %d::%d of %d bytes back: %v%%0A%s\n", run, n, size, err,
				strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(report))
			dumpForCI("TestUpgradeRelayBulk stacks")
			t.Fatalf("run %d: %d of %d bytes back: %v", run, n, size, err)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte("ABCDEFGH"), size/8)) {
			t.Fatalf("run %d: changed", run)
		}
		conn.Close()
	}
}
