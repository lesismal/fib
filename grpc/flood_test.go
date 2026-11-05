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

// TestWireServerControlFloods sends a server, each on a connection of its
// own, floods of frames that ask for work and carry nothing, which it used
// to answer for as long as they came, and checks that each ends the
// connection with GOAWAY and ENHANCE_YOUR_CALM; and that PINGs following
// the server's DATA, as grpc-go sends to size its windows, are not counted.
func TestWireServerControlFloods(t *testing.T) {
	f := &floodService{}
	_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(f.handle))
	const n = controlFrameBurst + 100
	repeat := func(frame []byte, n int) [][]byte {
		frames := make([][]byte, n)
		for i := range frames {
			frames[i] = frame
		}
		return frames
	}
	// The first CONTINUATION frames carry a byte each, so that only their
	// number is past a limit.
	tooMany := repeat(rawFrame(frameContinuation, 0, 1, []byte{0x80 | 16}), maxContinuationFrames+1)
	for _, tc := range []struct {
		name   string
		frames [][]byte
	}{
		{"PING", repeat(appendPing(nil, false, []byte("floodpng")), n)},
		{"SETTINGS", repeat(settingsFrame(), n)},
		{"empty DATA", repeat(dataFrame(1, nil, false), n)},
		{"empty CONTINUATION", repeat(rawFrame(frameContinuation, 0, 1, nil), n)},
		{"CONTINUATION frames past the limit", tooMany},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := dialWire(t, addr, true)
			first := headersFrame(1, c.request("/flood/x"), false)
			if tc.name == "empty CONTINUATION" || tc.name == "CONTINUATION frames past the limit" {
				first = rawFrame(frameHeaders, 0, 1, c.request("/flood/x"))
			}
			// The server may close before it has read them all.
			b := bytes.Join(append([][]byte{first}, tc.frames...), nil)
			go func() { _, _ = c.conn.Write(b) }()
			if got := c.goAway(); got != errEnhanceCalm {
				t.Fatalf("GOAWAY %d, want ENHANCE_YOUR_CALM", got)
			}
			c.closed()
		})
	}

	// grpc-go sends a PING a round trip while it receives DATA, to size
	// its windows by.
	t.Run("PINGs after the server's DATA", func(t *testing.T) {
		ticks := make(chan struct{})
		t.Cleanup(func() { close(ticks) })
		tick := func(_ any, stream ServerStream) error {
			for range ticks {
				if err := stream.SendMsg(&Rsp{N: 1}); err != nil {
					return err
				}
			}
			return nil
		}
		_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(tick))
		c := dialWire(t, addr, true)
		c.write(headersFrame(1, c.request("/tick/x"), false))
		for range n {
			ticks <- struct{}{}
			c.expect(frameData, 1)
			c.write(appendPing(nil, false, []byte("bdpbdpbd")))
			for {
				fr := c.expect(framePing, 0)
				if fr.has(flagAck) {
					break
				}
			}
		}
	})
}

// TestWireServerInitialWindowOverflow grows a stream's send window to
// 2^31-1 and then raises SETTINGS_INITIAL_WINDOW_SIZE, which takes it past
// the limit: a FLOW_CONTROL_ERROR of the connection (RFC 9113 section
// 6.9.2), which the server used to let wrap.
func TestWireServerInitialWindowOverflow(t *testing.T) {
	f := &floodService{}
	_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(f.handle))
	c := dialWire(t, addr, true)
	c.write(headersFrame(1, c.request("/flood/x"), false),
		rawFrame(frameWindowUpdate, 0, 1, u32(maxWindow-defaultWindow)))
	c.sync()
	c.write(settingsFrame(settingInitialWindowSize, defaultWindow+1))
	if got := c.goAway(); got != errFlowControl {
		t.Fatalf("GOAWAY %d, want FLOW_CONTROL_ERROR", got)
	}
}

// TestWireServerOversizedHeaderListKeepsHPACK sends a header list over the
// limit whose block, past the limit, adds a field to the HPACK dynamic
// table, and then a call naming that field by its index. The server used to
// stop decoding at the limit, leaving its table without the field and
// misreading every block after; now it decodes the block to its end,
// dropping its fields, and resets only that stream.
func TestWireServerOversizedHeaderListKeepsHPACK(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	c := dialWire(t, addr, true)
	const acceptEncoding = 16 // "accept-encoding: gzip, deflate", 60 bytes as listed
	block := c.enc.Begin(nil)
	block = append(block, bytes.Repeat([]byte{0x80 | acceptEncoding}, maxHeaderBlock/60+1)...)
	block = c.enc.AppendField(block, "k", "late", false)
	c.write(appendHeaderBlock(nil, 1, block, false, defaultMaxFrame))
	if got := c.reset(1); got != errEnhanceCalm {
		t.Fatalf("an oversized header list: RST %d", got)
	}
	c.write(headersFrame(3, c.request("/test.Echo/Unary", "k", "late"), false),
		dataFrame(3, grpcMessage(t, Req{Msg: "meta"}), true))
	if fr := c.expect(frameData, 3); !bytes.Contains(fr.payload, []byte(`"late|`)) {
		t.Fatalf("the call after it: %q", fr.payload)
	}
	if st := c.trailers(3); st["grpc-status"] != "0" {
		t.Fatalf("the call after it: %v", st)
	}
}
