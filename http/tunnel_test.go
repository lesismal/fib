//go:build linux || darwin || windows

package http

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/lesismal/fib/hpack"
)

// echoTunnel sends back what arrives through the tunnel, after greeting, and
// reports how the tunnel ended on closed.
type echoTunnel struct {
	greeting []byte
	closed   chan error
}

func (e *echoTunnel) OnTunnelOpen(t *Tunnel) {
	if len(e.greeting) > 0 {
		_ = t.Send(e.greeting)
	}
}
func (e *echoTunnel) OnTunnelData(t *Tunnel, data []byte) { _ = t.Send(data) }
func (e *echoTunnel) OnTunnelClose(_ *Tunnel, err error)  { e.closed <- err }

// tunnelServer upgrades every request to the "echo" protocol, and answers
// one that cannot be with 426 and the error.
func tunnelServer(t *testing.T, echo *echoTunnel) string {
	t.Helper()
	return serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if _, err := c.Upgrade("echo", stdhttp.Header{"X-Tunnel": {"1"}}, echo); err != nil {
			_ = c.Respond(stdhttp.StatusUpgradeRequired, "text/plain", []byte(err.Error()))
		}
	})))
}

func TestTunnelOverHTTP1(t *testing.T) {
	echo := &echoTunnel{closed: make(chan error, 1)}
	addr := tunnelServer(t, echo)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)

	// Without asking for the protocol there is no switch.
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != stdhttp.StatusUpgradeRequired || string(body) != ErrNotUpgradable.Error() {
		t.Fatalf("status %d body %q", resp.StatusCode, body)
	}

	// The bytes behind the request are the tunnel's first.
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo/1\r\n\r\nhello"); err != nil {
		t.Fatal(err)
	}
	resp, err = stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusSwitchingProtocols || resp.Header.Get("Upgrade") != "echo" ||
		resp.Header.Get("X-Tunnel") != "1" {
		t.Fatalf("status %d header %v", resp.StatusCode, resp.Header)
	}
	got := make([]byte, 5)
	for _, want := range []string{"hello", "world"} {
		if want == "world" {
			if _, err := io.WriteString(conn, want); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := io.ReadFull(reader, got); err != nil || string(got) != want {
			t.Fatalf("read %q %v, want %q", got, err, want)
		}
	}
	conn.Close()
	select {
	case <-echo.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnTunnelClose not called")
	}
}

// h2TunnelClient is a bare cleartext HTTP/2 client.
type h2TunnelClient struct {
	t    *testing.T
	conn net.Conn
	in   []byte
	enc  *hpack.Encoder
	dec  *hpack.Decoder
}

func dialH2Tunnel(t *testing.T, addr string) *h2TunnelClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := &h2TunnelClient{t: t, conn: conn, enc: hpack.NewEncoder(), dec: hpack.NewDecoder(4096)}
	c.write(append([]byte(h2Preface), h2AppendSettings(nil)...))
	return c
}

func (c *h2TunnelClient) write(b []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

// next returns the next frame on stream, acknowledging the server's SETTINGS
// on the way.
func (c *h2TunnelClient) next(stream uint32) h2Frame {
	c.t.Helper()
	buf := make([]byte, 64<<10)
	for {
		f, n, err := h2ReadFrame(c.in, h2MaxFrameSizeLimit)
		if err != nil {
			c.t.Fatal(err)
		}
		if n == 0 {
			m, err := c.conn.Read(buf)
			if err != nil {
				c.t.Fatal(err)
			}
			c.in = append(c.in, buf[:m]...)
			continue
		}
		f.payload = append([]byte(nil), f.payload...)
		c.in = c.in[n:]
		if f.typ == h2FrameHeaders {
			// Every block is decoded, in order, to keep the table in step.
			var status string
			if err := c.dec.Decode(f.payload, func(field hpack.HeaderField) error {
				if field.Name == ":status" {
					status = field.Value
				}
				return nil
			}); err != nil {
				c.t.Fatal(err)
			}
			f.payload = []byte(status)
		}
		if f.typ == h2FrameSettings && !f.has(h2FlagAck) {
			c.write(h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0))
		}
		if f.streamID == stream {
			return f
		}
	}
}

func TestTunnelOverHTTP2FlowControlAndPeerEnd(t *testing.T) {
	greeting := bytes.Repeat([]byte("0123456789"), 20000)
	echo := &echoTunnel{greeting: greeting, closed: make(chan error, 1)}
	c := dialH2Tunnel(t, tunnelServer(t, echo))
	block := c.enc.Begin(nil)
	for _, field := range [][2]string{{":method", "CONNECT"}, {":protocol", "echo"}, {":scheme", "http"},
		{":path", "/"}, {":authority", "x"}} {
		block = c.enc.AppendField(block, field[0], field[1], false)
	}
	c.write(h2AppendHeaderBlock(nil, 1, block, false, h2DefaultMaxFrameSize))
	if f := c.next(1); f.typ != h2FrameHeaders || f.has(h2FlagEndStream) || string(f.payload) != "200" {
		t.Fatalf("frame %v flags %#x status %s, want a 200 leaving the stream open", f.typ, f.flags, f.payload)
	}

	// The greeting is larger than the default window, so it stops short
	// until the window grows.
	var got []byte
	for len(got) < h2DefaultWindow {
		f := c.next(1)
		if f.typ != h2FrameData {
			t.Fatalf("frame %v, want DATA", f.typ)
		}
		got = append(got, f.payload...)
	}
	if len(got) != h2DefaultWindow {
		t.Fatalf("%d bytes sent past a window of %d", len(got), h2DefaultWindow)
	}
	c.write(h2AppendWindowUpdate(h2AppendWindowUpdate(nil, 0, 1<<20), 1, 1<<20))
	for len(got) < len(greeting) {
		got = append(got, c.next(1).payload...)
	}
	if !bytes.Equal(got, greeting) {
		t.Fatal("greeting garbled")
	}

	// Echo, then END_STREAM from the client ends the tunnel, and the server
	// ends its side.
	out := h2AppendFrameHeader(nil, h2FrameData, 0, 1, 4)
	c.write(append(out, "ping"...))
	if f := c.next(1); string(f.payload) != "ping" {
		t.Fatalf("echo %q", f.payload)
	}
	c.write(h2AppendFrameHeader(nil, h2FrameData, h2FlagEndStream, 1, 0))
	if f := c.next(1); f.typ != h2FrameData || !f.has(h2FlagEndStream) {
		t.Fatalf("frame %v flags %#x, want END_STREAM", f.typ, f.flags)
	}
	select {
	case err := <-echo.closed:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("OnTunnelClose(%v), want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnTunnelClose not called")
	}

	// A CONNECT for a protocol the handler does not speak is answered as any
	// other request.
	block = c.enc.Begin(nil)
	for _, field := range [][2]string{{":method", "CONNECT"}, {":protocol", "other"}, {":scheme", "http"},
		{":path", "/"}, {":authority", "x"}} {
		block = c.enc.AppendField(block, field[0], field[1], false)
	}
	c.write(h2AppendHeaderBlock(nil, 3, block, false, h2DefaultMaxFrameSize))
	status := string(c.next(3).payload)
	if status != "426" {
		t.Fatalf("status %q, want 426", status)
	}
}
