//go:build linux || darwin || windows

package websocket

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/hpack"
	epollhttp "github.com/lesismal/fib/http"
)

// upgradeServer serves plain HTTP and, on /ws, WebSocket through Upgrade,
// with ws echoing what it is sent. It reports what OnClose hears on closed.
func upgradeServer(t *testing.T, config Config) (string, chan closeReport) {
	t.Helper()
	closed := make(chan closeReport, 4)
	ws := NewHandlerWithConfig(config, HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			if err := c.WriteMessage(opcode, payload); err != nil {
				t.Error(err)
			}
		},
		Close: func(_ *Connection, code uint16, reason string, _ error) { closed <- closeReport{code, reason} },
	})
	handler := epollhttp.HandlerFunc(func(c *epollhttp.Context) {
		if c.Request.URL.Path != "/ws" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("plain "+c.Request.Proto))
			return
		}
		if _, err := ws.Upgrade(c, stdhttp.Header{"X-Upgraded": {"yes"}}); err != nil {
			t.Logf("upgrade: %v", err)
		}
	})
	engineConfig := fib.DefaultConfig()
	engineConfig.Addr = "127.0.0.1:0"
	server, err := fib.Bind(engineConfig, epollhttp.NewHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return addr.String(), closed
}

func expectClose(t *testing.T, closed chan closeReport, code uint16) {
	t.Helper()
	select {
	case report := <-closed:
		if report.code != code {
			t.Fatalf("OnClose code %d, want %d", report.code, code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose not called")
	}
}

func TestUpgradeFromHTTP1Dialer(t *testing.T) {
	addr, closed := upgradeServer(t, DefaultConfig())
	recorder := newClientRecorder()
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	conn, resp, err := dialer.Go("ws://"+addr+"/ws", nil, recorder.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("X-Upgraded") != "yes" {
		t.Fatalf("response header %v", resp.Header)
	}
	for i := 0; i < 3; i++ {
		text := fmt.Sprintf("hello %d", i)
		if err := conn.WriteText(text); err != nil {
			t.Fatal(err)
		}
		if event := recorder.next(t); string(event.Payload) != text {
			t.Fatalf("echo %q, want %q", event.Payload, text)
		}
	}
	if err := conn.Close(CloseNormal, "bye"); err != nil {
		t.Fatal(err)
	}
	expectClose(t, closed, CloseNormal)
}

// A request served before the upgrade on the same connection, and a frame
// sent in the same write as the handshake, are both answered.
func TestUpgradeFromHTTP1AfterKeepAliveWithCoalescedFrame(t *testing.T) {
	addr, closed := upgradeServer(t, DefaultConfig())
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	var out bytes.Buffer
	out.WriteString("GET /plain HTTP/1.1\r\nHost: test\r\n\r\n")
	out.WriteString("GET /ws HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n")
	out.Write(clientFrame(Text, true, []byte("coalesced")))
	if _, err := conn.Write(out.Bytes()); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	plain, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(plain.Body)
	if string(body) != "plain HTTP/1.1" {
		t.Fatalf("plain body %q", body)
	}
	upgraded, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.StatusCode != stdhttp.StatusSwitchingProtocols ||
		upgraded.Header.Get("Sec-WebSocket-Accept") != acceptFor(key) ||
		upgraded.Header.Get("X-Upgraded") != "yes" {
		t.Fatalf("upgrade response %d %v", upgraded.StatusCode, upgraded.Header)
	}
	opcode, payload, err := readServerFrame(reader)
	if err != nil || opcode != Text || string(payload) != "coalesced" {
		t.Fatalf("frame %v %q %v", opcode, payload, err)
	}
	if _, err := conn.Write(clientFrame(Close, true, []byte{0x03, 0xe8})); err != nil {
		t.Fatal(err)
	}
	if opcode, _, err := readServerFrame(reader); err != nil || opcode != Close {
		t.Fatalf("close echo %v %v", opcode, err)
	}
	expectClose(t, closed, CloseNormal)
}

func TestUpgradeRefusesBadHandshake(t *testing.T) {
	config := DefaultConfig()
	config.CheckOrigin = func(r *stdhttp.Request) bool { return r.Header.Get("Origin") != "http://evil" }
	addr, _ := upgradeServer(t, config)
	for _, tc := range []struct {
		name   string
		header string
		status int
	}{
		{"no key", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n", 400},
		{"version", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 8\r\n" +
			"Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\n", 400},
		{"origin", "Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n" +
			"Sec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\nOrigin: http://evil\r\n", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: test\r\n"+tc.header+"\r\n"); err != nil {
				t.Fatal(err)
			}
			resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}
}

// h2Client is a bare cleartext HTTP/2 client, enough to open WebSockets with
// extended CONNECT.
type h2Client struct {
	t      *testing.T
	conn   net.Conn
	reader *bufio.Reader
	enc    *hpack.Encoder
	dec    *hpack.Decoder
}

type h2TestFrame struct {
	typ, flags byte
	stream     uint32
	payload    []byte
}

func dialH2(t *testing.T, addr string) (*h2Client, map[uint16]uint32) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	c := &h2Client{t: t, conn: conn, reader: bufio.NewReader(conn), enc: hpack.NewEncoder(), dec: hpack.NewDecoder(4096)}
	c.write(append([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), h2TestFrameBytes(0x4, 0, 0, nil)...))
	settings := map[uint16]uint32{}
	for {
		f := c.read()
		if f.typ == 0x4 && f.flags&0x1 == 0 {
			for p := f.payload; len(p) >= 6; p = p[6:] {
				settings[binary.BigEndian.Uint16(p)] = binary.BigEndian.Uint32(p[2:])
			}
			c.write(h2TestFrameBytes(0x4, 0x1, 0, nil))
			return c, settings
		}
	}
}

func h2TestFrameBytes(typ, flags byte, stream uint32, payload []byte) []byte {
	out := []byte{byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)), typ, flags, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[5:], stream)
	return append(out, payload...)
}

func (c *h2Client) write(b []byte) {
	c.t.Helper()
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *h2Client) read() h2TestFrame {
	c.t.Helper()
	var header [9]byte
	if _, err := io.ReadFull(c.reader, header[:]); err != nil {
		c.t.Fatal(err)
	}
	f := h2TestFrame{typ: header[3], flags: header[4], stream: binary.BigEndian.Uint32(header[5:]) & 0x7fffffff,
		payload: make([]byte, int(header[0])<<16|int(header[1])<<8|int(header[2]))}
	if _, err := io.ReadFull(c.reader, f.payload); err != nil {
		c.t.Fatal(err)
	}
	return f
}

// next reads frames until one on stream arrives, skipping the connection's.
func (c *h2Client) next(stream uint32) h2TestFrame {
	c.t.Helper()
	for {
		f := c.read()
		if f.stream == stream {
			return f
		}
		if f.stream != 0 {
			c.t.Fatalf("frame type %d on stream %d", f.typ, f.stream)
		}
	}
}

func (c *h2Client) headers(stream uint32, endStream bool, fields ...string) {
	block := c.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		block = c.enc.AppendField(block, fields[i], fields[i+1], false)
	}
	flags := byte(0x4)
	if endStream {
		flags |= 0x1
	}
	c.write(h2TestFrameBytes(0x1, flags, stream, block))
}

func (c *h2Client) decode(block []byte) map[string]string {
	fields := map[string]string{}
	if err := c.dec.Decode(block, func(f hpack.HeaderField) error {
		fields[f.Name] = f.Value
		return nil
	}); err != nil {
		c.t.Fatal(err)
	}
	return fields
}

// readWebSocket reads DATA on stream until it holds a whole server frame.
func (c *h2Client) readWebSocket(stream uint32) (Opcode, []byte, bool) {
	c.t.Helper()
	var data []byte
	for {
		f := c.next(stream)
		if f.typ != 0x0 {
			c.t.Fatalf("frame type %d, want DATA", f.typ)
		}
		data = append(data, f.payload...)
		if len(data) >= 2 && len(data) >= 2+int(data[1]&0x7f) {
			return Opcode(data[0] & 0xf), data[2 : 2+int(data[1]&0x7f)], f.flags&0x1 != 0
		}
	}
}

func TestUpgradeOverHTTP2ExtendedConnect(t *testing.T) {
	addr, closed := upgradeServer(t, DefaultConfig())
	c, settings := dialH2(t, addr)
	if settings[0x8] != 1 {
		t.Fatalf("SETTINGS_ENABLE_CONNECT_PROTOCOL not advertised: %v", settings)
	}
	c.headers(1, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
		":path", "/ws", ":authority", "test", "sec-websocket-version", "13")
	f := c.next(1)
	if f.typ != 0x1 || f.flags&0x1 != 0 {
		t.Fatalf("frame type %d flags %#x, want HEADERS leaving the stream open", f.typ, f.flags)
	}
	if fields := c.decode(f.payload); fields[":status"] != "200" || fields["x-upgraded"] != "yes" ||
		fields["sec-websocket-accept"] != "" {
		t.Fatalf("response %v", fields)
	}

	// The connection still serves ordinary requests beside the tunnel.
	c.headers(3, true, ":method", "GET", ":scheme", "http", ":path", "/plain", ":authority", "test")

	c.write(h2TestFrameBytes(0x0, 0, 1, clientFrame(Text, true, []byte("over h2"))))
	var plain []byte
	var echo []byte
	for plain == nil || echo == nil {
		f := c.read()
		switch {
		case f.stream == 3 && f.typ == 0x0:
			plain = append([]byte(nil), f.payload...)
		case f.stream == 1 && f.typ == 0x0:
			echo = append([]byte(nil), f.payload...)
		case f.stream == 3 && f.typ == 0x1, f.stream == 0:
		default:
			t.Fatalf("frame type %d on stream %d", f.typ, f.stream)
		}
	}
	if string(plain) != "plain HTTP/2.0" {
		t.Fatalf("plain body %q", plain)
	}
	if !bytes.Equal(echo, append([]byte{0x81, byte(len("over h2"))}, "over h2"...)) {
		t.Fatalf("echo %q", echo)
	}

	// The close handshake, then END_STREAM each way.
	c.write(h2TestFrameBytes(0x0, 0, 1, clientFrame(Close, true, []byte{0x03, 0xe8})))
	opcode, _, ended := c.readWebSocket(1)
	if opcode != Close {
		t.Fatalf("opcode %v, want Close", opcode)
	}
	if !ended {
		if f := c.next(1); f.typ != 0x0 || f.flags&0x1 == 0 || len(f.payload) != 0 {
			t.Fatalf("frame type %d flags %#x, want END_STREAM", f.typ, f.flags)
		}
	}
	c.write(h2TestFrameBytes(0x0, 0x1, 1, nil))
	expectClose(t, closed, CloseNormal)

	// The connection goes on.
	c.headers(5, true, ":method", "GET", ":scheme", "http", ":path", "/plain", ":authority", "test")
	if f := c.next(5); f.typ != 0x1 {
		t.Fatalf("frame type %d, want HEADERS", f.typ)
	}
}

func TestUpgradeOverHTTP2PeerReset(t *testing.T) {
	addr, closed := upgradeServer(t, DefaultConfig())
	c, _ := dialH2(t, addr)
	c.headers(1, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
		":path", "/ws", ":authority", "test", "sec-websocket-version", "13")
	if f := c.next(1); f.typ != 0x1 {
		t.Fatalf("frame type %d, want HEADERS", f.typ)
	}
	c.write(h2TestFrameBytes(0x3, 0, 1, []byte{0, 0, 0, 0x8}))
	expectClose(t, closed, 1006)
}

func TestUpgradeOverHTTP2RefusesBadConnect(t *testing.T) {
	addr, _ := upgradeServer(t, DefaultConfig())
	c, _ := dialH2(t, addr)
	// No version.
	c.headers(1, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
		":path", "/ws", ":authority", "test")
	f := c.next(1)
	if fields := c.decode(f.payload); f.typ != 0x1 || fields[":status"] != "400" {
		t.Fatalf("response %v", fields)
	}
	// An extended CONNECT without :path is malformed.
	c.headers(3, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http", ":authority", "test")
	f = c.read()
	for f.stream != 3 {
		// The rest of the 400, and the reset of the stream it answered.
		f = c.read()
	}
	if f.typ != 0x3 || binary.BigEndian.Uint32(f.payload) != 0x1 {
		t.Fatalf("frame type %d payload %x, want RST_STREAM(PROTOCOL_ERROR)", f.typ, f.payload)
	}
}
