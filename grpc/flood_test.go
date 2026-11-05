//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

// floodService counts the handlers that ran and, with release set, holds
// each until release closes, whatever its context says: a handler that is
// slow to see that its call was cancelled.
type floodService struct {
	ran     atomic.Int64
	release chan struct{}
}

func (f *floodService) handle(_ any, stream ServerStream) error {
	f.ran.Add(1)
	if f.release != nil {
		<-f.release
		return nil
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

// rapidReset writes n streams, each opened and reset at once, from a
// goroutine of its own, since the server may close the connection before it
// has written them all.
func rapidReset(c *wireConn, n int) {
	var b bytes.Buffer
	for i := range n {
		id := uint32(2*i + 1)
		b.Write(headersFrame(id, c.request("/flood/x"), false))
		b.Write(rawFrame(frameRSTStream, 0, id, u32(uint32(errCancel))))
	}
	go func() { _, _ = c.conn.Write(b.Bytes()) }()
}

// TestWireServerRapidReset opens streams and resets them at once (Rapid
// Reset, CVE-2023-44487). The server used to free a reset stream's slot at
// once while its handler went on running, so every one of 20,000 streams
// ran a handler whatever MaxConcurrentStreams said. Now a handler counts
// against the limit until it returns, a call opened meanwhile waiting for
// it, and resets spend a budget whose end is a GOAWAY with
// ENHANCE_YOUR_CALM.
func TestWireServerRapidReset(t *testing.T) {
	t.Run("handlers slow to stop", func(t *testing.T) {
		f := &floodService{release: make(chan struct{})}
		_, addr := startServer(t, &echoImpl{}, MaxConcurrentStreams(10), UnknownServiceHandler(f.handle))
		t.Cleanup(func() { close(f.release) })
		c := dialWire(t, addr, true)
		rapidReset(c, 20000)
		if got := c.goAway(); got != errEnhanceCalm {
			t.Fatalf("GOAWAY %d, want ENHANCE_YOUR_CALM", got)
		}
		c.closed()
		if n := f.ran.Load(); n > 10 {
			t.Fatalf("%d handlers ran past MaxConcurrentStreams(10)", n)
		}
	})
	t.Run("handlers that stop", func(t *testing.T) {
		f := &floodService{}
		_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(f.handle))
		c := dialWire(t, addr, true)
		rapidReset(c, 20000)
		if got := c.goAway(); got != errEnhanceCalm {
			t.Fatalf("GOAWAY %d, want ENHANCE_YOUR_CALM", got)
		}
		c.closed()
		// The burst, and what the bucket got back while the test ran.
		if n := f.ran.Load(); n > streamResetBurst+streamResetRate*10 {
			t.Fatalf("%d handlers ran for streams reset at once", n)
		}
	})
	t.Run("a call waits for a reset one's handler", func(t *testing.T) {
		f := &floodService{release: make(chan struct{})}
		_, addr := startServer(t, &echoImpl{}, MaxConcurrentStreams(1), UnknownServiceHandler(f.handle))
		c := dialWire(t, addr, true)
		c.write(headersFrame(1, c.request("/flood/1"), false))
		c.sync()
		c.write(rawFrame(frameRSTStream, 0, 1, u32(uint32(errCancel))),
			headersFrame(3, c.request("/flood/3"), false),
			appendPing(nil, false, []byte("syncsync")))
		// Stream 3 is within the limit the client sees, so it is not
		// refused; its handler waits for stream 1's.
		for {
			fr, err := c.read()
			if err != nil {
				t.Fatal(err)
			}
			if fr.typ == frameRSTStream && fr.streamID == 3 {
				t.Fatal("stream 3 was refused")
			}
			if fr.typ == framePing && fr.has(flagAck) {
				break
			}
		}
		if n := f.ran.Load(); n != 1 {
			t.Fatalf("%d handlers ran with MaxConcurrentStreams(1)", n)
		}
		close(f.release)
		if st := c.trailers(3); st["grpc-status"] != "0" {
			t.Fatalf("the waiting call: %v", st)
		}
		if n := f.ran.Load(); n != 2 {
			t.Fatalf("%d handlers ran", n)
		}
	})
}

// TestWireServerConnectionWindow fills the windows of streams whose
// handlers read nothing. The server used to give the connection's window
// back as DATA arrived, so its streams could hold their windows each, as
// many as the client opened; now it gives it back only as the streams' data
// is read or dropped, so what they hold unread is bounded by the
// connection's window, and DATA past it ends the connection.
func TestWireServerConnectionWindow(t *testing.T) {
	f := &floodService{}
	_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(f.handle))
	c := dialWire(t, addr, true)
	c.expect(frameWindowUpdate, 0)
	chunk := make([]byte, defaultMaxFrame)
	streams := uint32(connWindow / streamWindow)
	for i := range streams {
		id := 2*i + 1
		frames := [][]byte{headersFrame(id, c.request("/flood/x"), false)}
		for range streamWindow / defaultMaxFrame {
			frames = append(frames, dataFrame(id, chunk, false))
		}
		c.write(frames...)
	}
	c.write(appendPing(nil, false, []byte("syncsync")))
	for {
		fr, err := c.read()
		if err != nil {
			t.Fatal(err)
		}
		if fr.typ == frameWindowUpdate && fr.streamID == 0 {
			t.Fatalf("the connection's window grew by %d with nothing read", binary.BigEndian.Uint32(fr.payload))
		}
		if fr.typ == framePing && fr.has(flagAck) {
			break
		}
	}
	id := 2*streams + 1
	c.write(headersFrame(id, c.request("/flood/x"), false), dataFrame(id, []byte("x"), false))
	if got := c.goAway(); got != errFlowControl {
		t.Fatalf("DATA past the connection's window: GOAWAY %d", got)
	}
}

// TestConnectionWindowReadersWait sends, at once on one connection, large
// messages that together need more than the connection's window: a reader
// waiting for the rest of its message gets back what it holds of it, so no
// call waits on the others for room they never give back. The replies are
// small: a connection whose replies back up stops reading, and both ends of
// one doing so at once is another matter.
func TestConnectionWindowReadersWait(t *testing.T) {
	sizeOf := func(_ any, stream ServerStream) error {
		var in Req
		if err := stream.RecvMsg(&in); err != nil {
			return err
		}
		return stream.SendMsg(&Rsp{N: len(in.Msg)})
	}
	_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(sizeOf))
	cc, _ := dial(t, addr)
	ctx := testCtx(t)
	big := strings.Repeat("b", 3<<20)
	const calls = 3 * connWindow / (3 << 20)
	results := make(chan error, calls)
	for range calls {
		go func() {
			var out Rsp
			err := cc.Invoke(ctx, "/big/x", &Req{Msg: big}, &out)
			if err == nil && out.N != len(big) {
				err = fmt.Errorf("the server read %d bytes", out.N)
			}
			results <- err
		}()
	}
	for range calls {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

// TestWireServerDefaultStreamLimit checks that a server left without
// MaxConcurrentStreams bounds the calls of a connection by 100, says so in
// its SETTINGS, and refuses the stream past it. It used to send no limit
// and keep none.
func TestWireServerDefaultStreamLimit(t *testing.T) {
	f := &floodService{}
	_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(f.handle))
	c := dialWire(t, addr, true)
	settings := c.expect(frameSettings, 0)
	limit := uint32(0)
	for p := settings.payload; len(p) >= 6; p = p[6:] {
		if binary.BigEndian.Uint16(p) == settingMaxConcurrentStreams {
			limit = binary.BigEndian.Uint32(p[2:])
		}
	}
	if limit != defaultMaxConcurrentStreams {
		t.Fatalf("SETTINGS_MAX_CONCURRENT_STREAMS %d, want %d", limit, defaultMaxConcurrentStreams)
	}
	var frames [][]byte
	for i := range defaultMaxConcurrentStreams + 1 {
		frames = append(frames, headersFrame(uint32(2*i+1), c.request(fmt.Sprintf("/flood/%d", i)), false))
	}
	c.write(frames...)
	if got := c.reset(2*defaultMaxConcurrentStreams + 1); got != errRefusedStream {
		t.Fatalf("the stream past the default limit: RST %d", got)
	}
}
