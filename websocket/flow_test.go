//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bufio"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	epollhttp "github.com/lesismal/fib/http"
)

var errTestAbort = errors.New("test: aborted")

// flowReport is what a test's Close callback hears.
type flowReport struct {
	code uint16
	err  error
}

func waitFor[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func expectNothing[T any](t *testing.T, ch <-chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("%s arrived early: %v", what, v)
	case <-time.After(300 * time.Millisecond):
	}
}

// fillUntilAbove sends 16 KiB messages with write until it reports above,
// and reports whether the first one did, and how many bytes of payload it
// sent.
func fillUntilAbove(t *testing.T, write func([]byte) (bool, error)) (firstAbove bool, sent int) {
	payload := make([]byte, 16<<10)
	for i := 0; i < 4096; i++ {
		above, err := write(payload)
		if err != nil {
			t.Errorf("write %d: %v", i, err)
			return false, sent
		}
		sent += len(payload)
		if i == 0 {
			firstAbove = above
		}
		if above {
			return firstAbove, sent
		}
	}
	t.Error("never above the limit")
	return firstAbove, sent
}

// fillStalled sends 16 KiB messages on ws, whose peer reads nothing, until
// one reports above, and registers OnWritable. When that runs while the peer
// still reads nothing, the socket took more than it had, so it fills again,
// until an OnWritable stays pending. It reports how many payload bytes it
// sent, and where that OnWritable reports.
func fillStalled(t *testing.T, ws *Connection) (int, <-chan error) {
	t.Helper()
	sent := 0
	for round := 0; round < 50; round++ {
		first, n := fillUntilAbove(t, func(p []byte) (bool, error) { return ws.WriteMessageNoWait(Binary, p) })
		sent += n
		if round == 0 && first {
			t.Error("the first 16 KiB message was reported above a 256 KiB limit")
		}
		writable := make(chan error, 1)
		if !ws.OnWritable(func(err error) { writable <- err }) {
			continue
		}
		select {
		case err := <-writable:
			if err != nil {
				t.Fatalf("OnWritable ran with %v", err)
			}
		case <-time.After(300 * time.Millisecond):
			if p := ws.pending(); p <= ws.pendingWriteLimit()/2 {
				t.Fatalf("OnWritable pending with %d bytes waiting", p)
			}
			return sent, writable
		}
	}
	t.Fatal("the queue never stalled")
	return 0, nil
}

// flowServer serves WebSocket, its PendingWriteLimit 256 KiB, with close
// reporting to OnClose, and returns the address to dial and where each
// connection is sent once open. upgraded serves it through an HTTP server's
// Upgrade.
func flowServer(t *testing.T, upgraded bool, closed chan<- flowReport) (string, <-chan *Connection) {
	opened := make(chan *Connection, 1)
	config := DefaultConfig()
	config.PendingWriteLimit = 256 << 10
	handler := HandlerFuncs{
		Open: func(c *Connection, _ *stdhttp.Request) { opened <- c },
		Close: func(_ *Connection, code uint16, _ string, err error) {
			if closed != nil {
				closed <- flowReport{code, err}
			}
		},
	}
	if upgraded {
		ws := NewHandlerWithConfig(config, handler)
		return serveHTTP(t, func(c *epollhttp.Context) { _, _ = ws.Upgrade(c, nil) }), opened
	}
	url := startWebSocketServer(t, config, handler)
	return strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws"), opened
}

// A server's WriteMessageNoWait reports above once more than its limit waits
// for a peer that does not read, and OnWritable runs once the peer has read
// it down to half; with nothing waiting, OnWritable reports false.
func TestWriteMessageNoWaitAndOnWritable(t *testing.T) {
	t.Run("served", func(t *testing.T) { testNoWaitAndOnWritable(t, false) })
	t.Run("upgraded", func(t *testing.T) { testNoWaitAndOnWritable(t, true) })
}

func testNoWaitAndOnWritable(t *testing.T, upgraded bool) {
	addr, opened := flowServer(t, upgraded, nil)
	conn, reader := dialWebSocket(t, addr)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	ws := waitFor(t, opened, "OnOpen")
	sent, writable := fillStalled(t, ws)

	got := 0
	for got < sent {
		_, payload, err := readServerFrame(reader)
		if err != nil {
			t.Fatalf("after %d of %d bytes: %v", got, sent, err)
		}
		got += len(payload)
	}
	if err := waitFor(t, writable, "OnWritable"); err != nil {
		t.Fatalf("OnWritable ran with %v", err)
	}
	if ws.OnWritable(func(error) { t.Error("OnWritable ran with nothing waiting") }) {
		t.Fatal("OnWritable reported true with nothing waiting")
	}
	if above, err := ws.WriteMessageNoWait(Text, []byte("small")); above || err != nil {
		t.Fatalf("a small message with nothing waiting: above %v, %v", above, err)
	}
}

// Abort on a server's connection closes it at once: what waits for the peer
// is dropped, no close frame goes, OnWritable runs with an error, and
// OnClose hears 1006 and the error Abort was given.
func TestAbortServerSide(t *testing.T) {
	closed := make(chan flowReport, 1)
	addr, opened := flowServer(t, false, closed)
	conn, reader := dialWebSocket(t, addr)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	ws := waitFor(t, opened, "OnOpen")
	sent, writable := fillStalled(t, ws)
	ws.Abort(errTestAbort)
	ws.Abort(errors.New("a second abort"))
	report := waitFor(t, closed, "OnClose")
	if report.code != 1006 || !errors.Is(report.err, errTestAbort) {
		t.Fatalf("OnClose got %d, %v", report.code, report.err)
	}
	if err := waitFor(t, writable, "OnWritable"); err == nil {
		t.Fatal("OnWritable ran without an error for an aborted connection")
	}
	if err := ws.WriteMessage(Text, []byte("late")); err == nil {
		t.Fatal("a write after Abort succeeded")
	}

	got := 0
	for {
		opcode, payload, err := readServerFrame(reader)
		if err != nil {
			break
		}
		if opcode == Close {
			t.Fatal("a close frame was sent after Abort")
		}
		got += len(payload)
	}
	// On Windows a send is posted to the kernel at once (overlapped I/O), so
	// there is no queue of the connection's own for Abort to drop: the
	// kernel still delivers what it was given.
	if got >= sent && runtime.GOOS != "windows" {
		t.Fatalf("the peer read all %d bytes: nothing queued was dropped", got)
	}
}

// rawClientSink is a server that completes a client's handshake, then reads
// nothing until read is closed, and then reports what the client sent: how
// many payload bytes of complete data frames, and whether a close frame came.
func rawClientSink(t *testing.T, read <-chan struct{}) (string, <-chan [2]int) {
	result := make(chan [2]int, 1)
	url := rawServer(t, func(conn net.Conn, reader *bufio.Reader, req *stdhttp.Request) {
		if _, err := io.WriteString(conn, upgradeResponse(req)); err != nil {
			return
		}
		<-read
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
		parser := NewParser(1 << 20)
		buf := make([]byte, 64<<10)
		got, closes := 0, 0
		for {
			n, err := reader.Read(buf)
			events, perr := parser.Feed(buf[:n])
			for _, event := range events {
				if event.Opcode == Close {
					closes++
				}
				got += len(event.Payload)
			}
			if err != nil || perr != nil {
				break
			}
		}
		result <- [2]int{got, closes}
	})
	return url, result
}

// Abort on a dialed connection does the same as on a server's.
func TestAbortClientSide(t *testing.T) {
	read := make(chan struct{})
	url, result := rawClientSink(t, read)
	config := DefaultDialerConfig()
	config.PendingWriteLimit = 256 << 10
	closed := make(chan flowReport, 1)
	ws, _, err := NewDialer(startClientEngine(t), config).Go(url, nil, HandlerFuncs{
		Close: func(_ *Connection, code uint16, _ string, err error) { closed <- flowReport{code, err} },
	}).Wait()
	if err != nil {
		t.Fatal(err)
	}
	sent, writable := fillStalled(t, ws)
	ws.Abort(errTestAbort)
	report := waitFor(t, closed, "OnClose")
	if report.code != 1006 || !errors.Is(report.err, errTestAbort) {
		t.Fatalf("OnClose got %d, %v", report.code, report.err)
	}
	if err := waitFor(t, writable, "OnWritable"); err == nil {
		t.Fatal("OnWritable ran without an error for an aborted connection")
	}
	close(read)
	r := waitFor(t, result, "the server to read")
	if r[1] != 0 {
		t.Fatal("a close frame was sent after Abort")
	}
	// As in TestAbortServerSide: on Windows the kernel holds the sends.
	if r[0] >= sent && runtime.GOOS != "windows" {
		t.Fatalf("the peer read all %d bytes: nothing queued was dropped", r[0])
	}
}

// A dialed connection's OnWritable runs once the server reads.
func TestClientOnWritableRunsOnceServerReads(t *testing.T) {
	read := make(chan struct{})
	url, _ := rawClientSink(t, read)
	config := DefaultDialerConfig()
	config.PendingWriteLimit = 256 << 10
	ws, _, err := NewDialer(startClientEngine(t), config).Go(url, nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Abort(nil)
	_, writable := fillStalled(t, ws)
	close(read)
	if err := waitFor(t, writable, "OnWritable"); err != nil {
		t.Fatalf("OnWritable ran with %v", err)
	}
}

// RemoteAddr is the peer's address on both sides.
func TestRemoteAddrBothSides(t *testing.T) {
	type seen struct{ addr, request string }
	opened := make(chan seen, 1)
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{Open: func(c *Connection, r *stdhttp.Request) {
		s := seen{request: r.RemoteAddr}
		if addr := c.RemoteAddr(); addr != nil {
			s.addr = addr.String()
		}
		opened <- s
	}})
	ws, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close(CloseNormal, "")
	s := waitFor(t, opened, "OnOpen")
	if s.addr == "" || s.addr != s.request {
		t.Fatalf("server side RemoteAddr %q, request's %q", s.addr, s.request)
	}
	want := strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws")
	if addr := ws.RemoteAddr(); addr == nil || addr.String() != want {
		t.Fatalf("client side RemoteAddr %v, want %s", addr, want)
	}
}

// A connection that Upgrade switched has its RemoteAddr, and Abort ends it,
// reporting its error to OnClose.
func TestUpgradedRemoteAddrAndAbort(t *testing.T) {
	type seen struct{ addr, request string }
	opened := make(chan seen, 1)
	closed := make(chan flowReport, 1)
	ws := NewHandler(HandlerFuncs{
		Open: func(c *Connection, r *stdhttp.Request) {
			s := seen{request: r.RemoteAddr}
			if addr := c.RemoteAddr(); addr != nil {
				s.addr = addr.String()
			}
			opened <- s
		},
		Message: func(c *Connection, _ Opcode, _ []byte) { c.Abort(errTestAbort) },
		Close:   func(_ *Connection, code uint16, _ string, err error) { closed <- flowReport{code, err} },
	})
	addr := serveHTTP(t, func(c *epollhttp.Context) { _, _ = ws.Upgrade(c, nil) })
	client := newClientRecorder()
	conn, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go("ws://"+addr+"/ws", nil, client.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	s := waitFor(t, opened, "OnOpen")
	if s.addr == "" || s.addr != s.request {
		t.Fatalf("upgraded RemoteAddr %q, request's %q", s.addr, s.request)
	}
	if err := conn.WriteText("abort"); err != nil {
		t.Fatal(err)
	}
	report := waitFor(t, closed, "OnClose")
	if report.code != 1006 || !errors.Is(report.err, errTestAbort) {
		t.Fatalf("OnClose got %d, %v", report.code, report.err)
	}
	if peer := waitFor(t, client.closed, "the client's OnClose"); peer.code != 1006 {
		t.Fatalf("the client heard %d: a close frame was sent", peer.code)
	}
}

// HoldReads on a server's connection keeps what the peer sends next from
// the Handler until the hold is released.
func TestHoldReadsServerSide(t *testing.T) {
	held := make(chan *Connection, 1)
	got := make(chan string, 4)
	url := startWebSocketServer(t, DefaultConfig(), HandlerFuncs{Message: func(c *Connection, _ Opcode, p []byte) {
		if string(p) == "hold" {
			c.HoldReads(true)
			held <- c
			return
		}
		got <- string(p)
	}})
	ws, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close(CloseNormal, "")
	if err := ws.WriteText("hold"); err != nil {
		t.Fatal(err)
	}
	server := waitFor(t, held, "the hold")
	if err := ws.WriteText("after"); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, got, "a message sent while reads were held")
	server.HoldReads(false)
	if m := waitFor(t, got, "the message after the hold"); m != "after" {
		t.Fatalf("got %q", m)
	}
}

// HoldReads on a dialed connection does the same.
func TestHoldReadsClientSide(t *testing.T) {
	url := startWebSocketServer(t, DefaultConfig(), echoServerHandler())
	client := newClientRecorder()
	ws, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, client.handler()).Wait()
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close(CloseNormal, "")
	if err := ws.WriteText("first"); err != nil {
		t.Fatal(err)
	}
	if e := client.next(t); string(e.Payload) != "first" {
		t.Fatalf("got %q", e.Payload)
	}
	ws.HoldReads(true)
	if err := ws.WriteText("second"); err != nil {
		t.Fatal(err)
	}
	expectNothing(t, client.messages, "an echo while reads were held")
	ws.HoldReads(false)
	if e := client.next(t); string(e.Payload) != "second" {
		t.Fatalf("got %q", e.Payload)
	}
}

// Every connection of a server keeps a connectionState, which stays within
// the 160-byte size class.
func TestConnectionStateSize(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) == 8 && unsafe.Sizeof(connectionState{}) > 160 {
		t.Fatalf("connectionState is %d bytes", unsafe.Sizeof(connectionState{}))
	}
}
