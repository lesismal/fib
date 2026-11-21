//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"
)

// lateTunnelServer retains every request and upgrades it to "echo" from
// another goroutine after delay, as a proxy does once its upstream has
// accepted the switch, reporting the outcome on results.
func lateTunnelServer(t *testing.T, echo *echoTunnel, delay time.Duration, results chan error) string {
	t.Helper()
	return serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Retain()
		go func() {
			defer c.Release()
			time.Sleep(delay)
			_, err := c.Upgrade("echo", stdhttp.Header{"X-Tunnel": {"late"}}, echo)
			results <- err
			if err != nil {
				_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(err.Error()))
			}
		}()
	})))
}

// TestLateUpgradeHTTP1 switches a retained request after its handler has
// returned: what the client sent before and while it waited is the tunnel's,
// in order, and a request that did not ask is refused and answered.
func TestLateUpgradeHTTP1(t *testing.T) {
	echo := &echoTunnel{closed: make(chan error, 1)}
	results := make(chan error, 4)
	addr := lateTunnelServer(t, echo, 200*time.Millisecond, results)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	reader := bufio.NewReader(conn)

	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != stdhttp.StatusUpgradeRequired || !errors.Is(<-results, ErrNotUpgradable) {
		t.Fatalf("a request that did not ask: %d", resp.StatusCode)
	}

	// "early" comes with the request, "later" while it waits.
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\nearly"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := io.WriteString(conn, "later"); err != nil {
		t.Fatal(err)
	}
	resp, err = stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusSwitchingProtocols || resp.Header.Get("X-Tunnel") != "late" {
		t.Fatalf("status %d header %v", resp.StatusCode, resp.Header)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "after"); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("earlylaterafter"))
	if _, err := io.ReadFull(reader, got); err != nil || string(got) != "earlylaterafter" {
		t.Fatalf("read %q %v", got, err)
	}
	conn.Close()
	select {
	case <-echo.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnTunnelClose not called")
	}
}

// TestLateUpgradeHTTP2 does the same for an extended CONNECT on HTTP/2: the
// DATA the client sent before the 200 is the tunnel's first.
func TestLateUpgradeHTTP2(t *testing.T) {
	echo := &echoTunnel{closed: make(chan error, 1)}
	results := make(chan error, 2)
	c := dialH2Tunnel(t, lateTunnelServer(t, echo, 200*time.Millisecond, results))
	block := c.enc.Begin(nil)
	for _, field := range [][2]string{{":method", "CONNECT"}, {":protocol", "echo"}, {":scheme", "http"},
		{":path", "/"}, {":authority", "x"}} {
		block = c.enc.AppendField(block, field[0], field[1], false)
	}
	c.write(h2AppendHeaderBlock(nil, 1, block, false, h2DefaultMaxFrameSize))
	c.write(append(h2AppendFrameHeader(nil, h2FrameData, 0, 1, 5), "early"...))
	if f := c.next(1); f.typ != h2FrameHeaders || f.has(h2FlagEndStream) || string(f.payload) != "200" {
		t.Fatalf("frame %v flags %#x status %s, want a 200 leaving the stream open", f.typ, f.flags, f.payload)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if f := c.next(1); f.typ != h2FrameData || string(f.payload) != "early" {
		t.Fatalf("frame %v %q, want the early DATA echoed", f.typ, f.payload)
	}
	c.write(append(h2AppendFrameHeader(nil, h2FrameData, 0, 1, 4), "ping"...))
	if f := c.next(1); string(f.payload) != "ping" {
		t.Fatalf("echo %q", f.payload)
	}
}

// TestLateUpgradeAfterAnswer checks that a retained request already
// answered cannot be switched.
func TestLateUpgradeAfterAnswer(t *testing.T) {
	results := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Retain()
		_ = c.Respond(200, "text/plain", []byte("ok"))
		go func() {
			defer c.Release()
			_, err := c.Upgrade("echo", nil, &echoTunnel{closed: make(chan error, 1)})
			results <- err
		}()
	})))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("%v %v", resp, err)
	}
	if err := <-results; !errors.Is(err, ErrResponseWritten) && !errors.Is(err, ErrNotUpgradable) {
		t.Errorf("switched after the answer: %v", err)
	}
}

// upgradeClientTunnel records what arrives through a client's tunnel.
type upgradeClientTunnel struct {
	opened chan *Tunnel
	data   chan string
	closed chan error
}

func (u *upgradeClientTunnel) OnTunnelOpen(t *Tunnel)              { u.opened <- t }
func (u *upgradeClientTunnel) OnTunnelData(_ *Tunnel, data []byte) { u.data <- string(data) }
func (u *upgradeClientTunnel) OnTunnelClose(_ *Tunnel, err error)  { u.closed <- err }
func newUpgradeClientTunnel() *upgradeClientTunnel {
	return &upgradeClientTunnel{opened: make(chan *Tunnel, 1), data: make(chan string, 16), closed: make(chan error, 1)}
}

// TestClientStartUpgrade switches a client connection on a 101: the
// callback gets the tunnel, the handler what the server sends, and the
// connection leaves the client, whose one connection a host may have is
// free again; any other answer is a response like another.
func TestClientStartUpgrade(t *testing.T) {
	echo := &echoTunnel{greeting: []byte("hello"), closed: make(chan error, 1)}
	addr := tunnelServer(t, echo)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	client := newTestClient(t, config)

	upgrade := func() (*stdhttp.Request, *upgradeClientTunnel) {
		req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/", nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "echo")
		return req, newUpgradeClientTunnel()
	}
	req, handler := upgrade()
	answered := make(chan *ClientResponse, 1)
	client.StartUpgrade(req, handler, func(resp *ClientResponse, err error) {
		if err != nil {
			t.Errorf("upgrade: %v", err)
		}
		answered <- resp
	})
	resp := <-answered
	if resp == nil || resp.StatusCode != stdhttp.StatusSwitchingProtocols || resp.Tunnel() == nil || resp.Header.Get("X-Tunnel") != "1" {
		t.Fatalf("response %+v", resp)
	}
	tn := <-handler.opened
	if tn != resp.Tunnel() {
		t.Fatal("the handler opened another tunnel")
	}
	if got := <-handler.data; got != "hello" {
		t.Fatalf("first data %q", got)
	}
	if err := tn.Send([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if got := <-handler.data; got != "ping" {
		t.Fatalf("echo %q", got)
	}

	// The tunnel's connection no longer counts: an ordinary request gets the
	// one connection the host may have, and its 426 has no tunnel.
	req, other := upgrade()
	req.Header.Del("Upgrade")
	req.Header.Del("Connection")
	client.StartUpgrade(req, other, func(resp *ClientResponse, err error) {
		if err != nil {
			t.Errorf("plain: %v", err)
		}
		answered <- resp
	})
	select {
	case resp = <-answered:
	case <-time.After(5 * time.Second):
		t.Fatal("a request waited for the tunnel's connection")
	}
	if resp.StatusCode != stdhttp.StatusUpgradeRequired || resp.Tunnel() != nil {
		t.Fatalf("plain: %d, tunnel %v", resp.StatusCode, resp.Tunnel())
	}
	select {
	case <-other.opened:
		t.Fatal("a handler opened without a switch")
	default:
	}

	_ = tn.Close()
	select {
	case <-handler.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnTunnelClose not called")
	}
}

// TestClientStartUpgradeHTTP2 checks that a switch is refused on a
// multiplexed connection.
func TestClientStartUpgradeHTTP2(t *testing.T) {
	addr := tunnelServer(t, &echoTunnel{closed: make(chan error, 1)})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "echo")
	done := make(chan error, 1)
	client.StartUpgrade(req, newUpgradeClientTunnel(), func(_ *ClientResponse, err error) { done <- err })
	select {
	case err := <-done:
		if !errors.Is(err, ErrNotUpgradable) {
			t.Fatalf("%v, want ErrNotUpgradable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no answer")
	}
}

// TestUpgradeFromGoroutineAsHandlerReturns switches a retained request from
// another goroutine that begins the switch while the handler is still
// running. The handler's round and the switch both work on the connection's
// parser, so they must not overlap and must be ordered for -race, which
// reported them unordered before they were. In "switch first" the switch is
// made while the handler runs, and the handler returns after it, before the
// goroutine releases the request; in "return first" the OnHeader hook, which
// runs between the switch's start and its reaching the connection, holds it
// until the handler has returned, so the switch has to wait for the round's
// end, as one begun after the handler returned does.
func TestUpgradeFromGoroutineAsHandlerReturns(t *testing.T) {
	for _, returnFirst := range []bool{false, true} {
		name := "switch first"
		if returnFirst {
			name = "return first"
		}
		t.Run(name, func(t *testing.T) {
			testUpgradeFromGoroutine(t, returnFirst)
		})
	}
}

func testUpgradeFromGoroutine(t *testing.T, returnFirst bool) {
	echo := &echoTunnel{closed: make(chan error, 3)}
	results := make(chan error, 3)
	release := make(chan struct{})
	defer close(release)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Retain()
		hooked := make(chan struct{})
		c.OnHeader(func(int, stdhttp.Header) {
			close(hooked)
			if returnFirst {
				time.Sleep(50 * time.Millisecond)
			}
		})
		go func() {
			_, err := c.Upgrade("echo", nil, echo)
			results <- err
			// Released only once the test is over, so that nothing the
			// release does orders the switch before the round's end.
			<-release
			c.Release()
		}()
		<-hooked
		if !returnFirst {
			time.Sleep(50 * time.Millisecond)
		}
	})))
	for range 3 {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		reader := bufio.NewReader(conn)
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\nearly"); err != nil {
			t.Fatal(err)
		}
		resp, err := stdhttp.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != stdhttp.StatusSwitchingProtocols {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(conn, "after"); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len("earlyafter"))
		if _, err := io.ReadFull(reader, got); err != nil || string(got) != "earlyafter" {
			t.Fatalf("read %q %v", got, err)
		}
	}
}
