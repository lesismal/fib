//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// serveEngine runs handler on an engine without pollers, so that the
// engine's Stats count every connection's queued output, and returns the
// engine with its host:port. The engine is stopped and closed when the test
// ends, unless stop, called first, has done it.
func serveEngine(t *testing.T, handler HandlerFunc) (*fib.Engine, string) {
	engine, addr, _ := serveEngineStoppable(t, DefaultConfig(), handler)
	return engine, addr
}

func serveEngineStoppable(t *testing.T, config Config, handler HandlerFunc) (engine *fib.Engine, addr string, stop func()) {
	t.Helper()
	setReuse(&config, false)
	engineConfig := fib.DefaultConfig()
	engineConfig.IOPollers = false
	server, err := fib.New(engineConfig, fib.Binding{Addr: "127.0.0.1:0", Handler: NewHandlerWithConfig(config, handler)})
	if err != nil {
		t.Fatal(err)
	}
	local, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			server.Stop()
			<-runDone
			_ = server.Close()
		})
	}
	t.Cleanup(stop)
	return server, local.String(), stop
}

// dialSlowReader connects to addr and asks for path, for a client that then
// reads the response at a pace of its own. Its receive buffer is left as it
// is: shrinking it once the connection is up made Linux drop what the
// window it had already offered let in, and the transfer then crept along
// at the pace of retransmission timeouts, whatever the server did.
func dialSlowReader(t *testing.T, addr, path string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := io.WriteString(conn, "GET "+path+" HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	return conn
}

// h1StreamChunk is what the streaming handlers below write at a time, as
// io.Copy does.
const h1StreamChunk = 32 << 10

// streamPattern is the body the handlers below stream, chunk by chunk, so a
// client can check every byte of what it read.
func streamPattern(i int) []byte {
	chunk := make([]byte, h1StreamChunk)
	for j := range chunk {
		chunk[j] = byte(i + j)
	}
	return chunk
}

// readStreamed reads a whole response to a request for total bytes of
// streamPattern and checks it, pausing for pause after every read of step.
func readStreamed(t *testing.T, conn net.Conn, total int, step int, pause time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Minute))
	resp, err := stdhttp.ReadResponse(bufio.NewReaderSize(conn, 4<<10), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, step)
	got := 0
	want := streamPattern(0)
	for {
		n, err := io.ReadFull(resp.Body, buf)
		for k := 0; k < n; k++ {
			i, j := (got+k)/h1StreamChunk, (got+k)%h1StreamChunk
			if buf[k] != want[j]+byte(i) {
				t.Fatalf("byte %d is %d, want %d", got+k, buf[k], want[j]+byte(i))
			}
		}
		got += n
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			t.Fatalf("after %d bytes: %v", got, err)
		}
		if pause > 0 {
			time.Sleep(pause)
		}
	}
	if got != total {
		t.Fatalf("read %d bytes, want %d", got, total)
	}
}

// h1BackpressureLimit is the most output a streaming HTTP/1 response may
// leave queued for a client that is not reading: what the handler waits at,
// one more chunk written past it, and room for the header and chunk framing.
const h1BackpressureLimit = h1MaxPendingOutput + h1StreamChunk + 4<<10

// A handler streaming a body far larger than anything the server should
// hold for one client, to a client that reads nothing for a while, waits in
// Write rather than queueing the body: the connection's queued output stays
// bounded, and the whole body still arrives once the client reads.
func TestHTTP1StreamedResponseWaitsForSlowClient(t *testing.T) {
	const total = 64 << 20
	var written atomic.Int64
	engine, addr := serveEngine(t, func(c *Context) {
		c.Header().Set("Content-Type", "application/octet-stream")
		for i := 0; i < total/h1StreamChunk; i++ {
			if _, err := c.Write(streamPattern(i)); err != nil {
				return
			}
			written.Add(h1StreamChunk)
		}
	})
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	conn := dialSlowReader(t, addr, "/")
	// The client reads nothing while the handler writes as fast as it can.
	var maxPending int64
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		maxPending = max(maxPending, engine.Stats().PendingBytes)
		time.Sleep(time.Millisecond)
	}
	var during runtime.MemStats
	runtime.ReadMemStats(&during)
	t.Logf("client not reading: handler wrote %d of %d bytes, %d queued at most, heap in use grew by %d KB",
		written.Load(), total, maxPending, (int64(during.HeapInuse)-int64(before.HeapInuse))>>10)
	if maxPending > h1BackpressureLimit {
		t.Errorf("the connection queued %d bytes for a client not reading, want at most %d", maxPending, h1BackpressureLimit)
	}
	readStreamed(t, conn, total, 1<<20, 0)
	if got := engine.Stats().PendingBytes; got != 0 {
		t.Errorf("%d bytes still queued after the whole body was read", got)
	}
}

// A streamed response written by a handler that retained its request, from a
// goroutine of its own, waits the same way: the connection's own round, idle
// by then, is not what it waits on.
func TestHTTP1RetainedStreamWaitsForSlowClient(t *testing.T) {
	const total = 16 << 20
	engine, addr := serveEngine(t, func(c *Context) {
		c.Retain()
		go func() {
			defer c.Release()
			for i := 0; i < total/h1StreamChunk; i++ {
				if _, err := c.Write(streamPattern(i)); err != nil {
					return
				}
			}
		}()
	})
	conn := dialSlowReader(t, addr, "/")
	var maxPending int64
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		maxPending = max(maxPending, engine.Stats().PendingBytes)
		time.Sleep(time.Millisecond)
	}
	if maxPending > h1BackpressureLimit {
		t.Errorf("the connection queued %d bytes for a client not reading, want at most %d", maxPending, h1BackpressureLimit)
	}
	readStreamed(t, conn, total, 64<<10, 0)
}

// Clients that keep reading, a little at a time, get their whole bodies, on
// several connections at once, through reads that leave the server waiting
// over and over: nothing the wait depends on is held up by the handler that
// is waiting.
func TestHTTP1StreamedResponsesToTricklingClients(t *testing.T) {
	const total = 4 << 20
	_, addr := serveEngine(t, func(c *Context) {
		for i := 0; i < total/h1StreamChunk; i++ {
			if _, err := c.Write(streamPattern(i)); err != nil {
				return
			}
		}
	})
	var wg sync.WaitGroup
	for range 8 {
		conn := dialSlowReader(t, addr, "/")
		wg.Add(1)
		go func() {
			defer wg.Done()
			readStreamed(t, conn, total, 16<<10, 100*time.Microsecond)
		}()
	}
	wg.Wait()
}

// A client that goes away while the handler waits for it to read lets the
// handler go: its Write reports the connection gone.
func TestHTTP1StreamWaitEndsWhenClientCloses(t *testing.T) {
	done := make(chan error, 1)
	var written atomic.Int64
	_, addr := serveEngine(t, func(c *Context) {
		for {
			if _, err := c.Write(streamPattern(0)); err != nil {
				done <- err
				return
			}
			written.Add(h1StreamChunk)
		}
	})
	conn := dialSlowReader(t, addr, "/")
	// Wait for the handler to stop writing, which it does once it waits.
	last := int64(-1)
	for n := written.Load(); n != last; n = written.Load() {
		if n > 64<<20 {
			t.Fatalf("the handler wrote %d bytes to a client reading none of them without waiting", n)
		}
		last = n
		time.Sleep(100 * time.Millisecond)
	}
	_ = conn.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Write to a closed connection reported no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler is still waiting after its client closed the connection")
	}
}

// A response short enough to be held whole, and pipelined responses, never
// wait: they all reach a client that sends every request before reading.
func TestHTTP1ShortResponsesDoNotWait(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 1000)
	_, addr := serveEngine(t, func(c *Context) { _, _ = c.Write(body) })
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const requests = 2000
	var sent bytes.Buffer
	for range requests {
		sent.WriteString("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	}
	if _, err := conn.Write(sent.Bytes()); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)
	for i := range requests {
		resp, err := stdhttp.ReadResponse(reader, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		got, err := io.ReadAll(resp.Body)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("response %d: %d bytes, %v", i, len(got), err)
		}
	}
}

// A server closed while a handler waits for its client to read lets the
// handler go, rather than wait for a write edge its stopped loop will never
// see, and the close does not wait on it.
func TestHTTP1StreamWaitEndsWhenServerCloses(t *testing.T) {
	done := make(chan error, 1)
	var written atomic.Int64
	_, addr, stop := serveEngineStoppable(t, DefaultConfig(), func(c *Context) {
		for {
			if _, err := c.Write(streamPattern(0)); err != nil {
				done <- err
				return
			}
			written.Add(h1StreamChunk)
		}
	})
	dialSlowReader(t, addr, "/")
	last := int64(-1)
	for n := written.Load(); n != last; n = written.Load() {
		if n > 64<<20 {
			t.Fatalf("the handler wrote %d bytes to a client reading none of them without waiting", n)
		}
		last = n
		time.Sleep(100 * time.Millisecond)
	}
	closed := make(chan struct{})
	go func() {
		stop()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("closing the server waits on a handler waiting for its client")
	}
	if err := <-done; err == nil {
		t.Fatal("Write on a closed server reported no error")
	}
}

// WriteTimeout ends a response whose client stops reading: the handler
// waiting for it is let go once the timeout has passed with output still
// queued, and the connection is closed.
func TestHTTP1WriteTimeoutEndsStalledStream(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 300 * time.Millisecond
	done := make(chan error, 1)
	_, addr, _ := serveEngineStoppable(t, config, func(c *Context) {
		for {
			if _, err := c.Write(streamPattern(0)); err != nil {
				done <- err
				return
			}
		}
	})
	conn := dialSlowReader(t, addr, "/")
	start := time.Now()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Write reported no error")
		}
		if waited := time.Since(start); waited < 200*time.Millisecond {
			t.Fatalf("the handler was let go after %v, before WriteTimeout", waited)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler still waits for a client that stopped reading, past WriteTimeout")
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Fatalf("the connection did not end once WriteTimeout passed: %v", err)
	}
}

// A Write made after WriteTimeout has passed fails, as net/http's does, even
// to a client that is keeping up.
func TestHTTP1WriteTimeoutRefusesLateWrites(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 100 * time.Millisecond
	done := make(chan error, 1)
	_, addr, _ := serveEngineStoppable(t, config, func(c *Context) {
		if _, err := c.Write(streamPattern(0)); err != nil {
			done <- err
			return
		}
		time.Sleep(300 * time.Millisecond)
		_, err := c.Write(streamPattern(1))
		done <- err
	})
	conn := dialSlowReader(t, addr, "/")
	go func() { _, _ = io.Copy(io.Discard, conn) }()
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Write after WriteTimeout: %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never returned")
	}
}

// WriteTimeout bounds each response, not the connection: a kept-alive client
// that comes back after the last response's timeout has passed is served.
func TestHTTP1WriteTimeoutLeavesIdleConnectionsAlone(t *testing.T) {
	config := DefaultConfig()
	// A second, not a hundred milliseconds: what the test checks is that the
	// client that comes back after the timeout has passed is served, and on a
	// loaded runner a 100ms bound can cut a 256 KiB body in half while the
	// client's next read is still waiting to be scheduled, which the Windows
	// job saw as a body ending early.
	config.WriteTimeout = time.Second
	_, addr, _ := serveEngineStoppable(t, config, func(c *Context) {
		for i := range 8 {
			if _, err := c.Write(streamPattern(i)); err != nil {
				return
			}
		}
	})
	conn := dialSlowReader(t, addr, "/")
	reader := bufio.NewReader(conn)
	for i := range 3 {
		if i > 0 {
			// Longer than WriteTimeout: the request comes after the bound on
			// the response before it has passed.
			time.Sleep(1200 * time.Millisecond)
			if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		resp, err := stdhttp.ReadResponse(reader, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil || n != 8*h1StreamChunk {
			t.Fatalf("response %d: %d bytes, %v", i, n, err)
		}
	}
}
