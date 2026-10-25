//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"io"
	stdhttp "net/http"
	"strconv"
	"testing"
	"time"

	"github.com/lesismal/fib/hpack"
)

// TestHTTP2ConfigLocal checks what a zero config resolves to, and what each
// field moves: the windows, the frame size, the table and the client's
// stream allowance, kept within what the protocol allows.
func TestHTTP2ConfigLocal(t *testing.T) {
	var zero HTTP2Config
	l := zero.local()
	if l.streamWindow != DefaultH2StreamWindow || l.connWindow != DefaultH2ConnWindow {
		t.Fatalf("default windows %d, %d", l.streamWindow, l.connWindow)
	}
	if l.maxFrame != h2DefaultMaxFrameSize || l.tableSize != hpack.DefaultTableSize || l.bdp != nil {
		t.Fatalf("default frame %d, table %d, bdp %v", l.maxFrame, l.tableSize, l.bdp)
	}
	if got := zero.initialMaxStreams(); got != h2DefaultInitialMaxStreams {
		t.Fatalf("default streams %d", got)
	}
	s := l.settings([2]uint32{uint32(h2SettingMaxConcurrentStreams), 7})
	if len(s) != 2 || s[1][0] != uint32(h2SettingInitialWindowSize) || s[1][1] != uint32(DefaultH2StreamWindow) {
		t.Fatalf("default settings %v", s)
	}

	c := HTTP2Config{
		StreamWindow: 1 << 21, ConnWindow: 1 << 22, MaxFrameSize: 1 << 15,
		HeaderTableSize: 1 << 13, InitialMaxStreams: 5,
	}
	l = c.local()
	if l.streamWindow != 1<<21 || l.connWindow != 1<<22 || l.maxFrame != 1<<15 || l.tableSize != 1<<13 {
		t.Fatalf("configured %d, %d, %d, %d", l.streamWindow, l.connWindow, l.maxFrame, l.tableSize)
	}
	if got := c.initialMaxStreams(); got != 5 {
		t.Fatalf("streams %d", got)
	}
	s = l.settings()
	got := map[h2SettingID]uint32{}
	for _, pair := range s {
		got[h2SettingID(pair[0])] = pair[1]
	}
	if got[h2SettingInitialWindowSize] != 1<<21 || got[h2SettingMaxFrameSize] != 1<<15 ||
		got[h2SettingHeaderTableSize] != 1<<13 {
		t.Fatalf("settings %v", got)
	}
	preface := l.preface(nil)
	// SETTINGS and a WINDOW_UPDATE raising the connection's window.
	f, n, err := h2ReadFrame(preface, h2MaxFrameSizeLimit)
	if err != nil || f.typ != h2FrameSettings || n == 0 {
		t.Fatalf("preface settings: %v, %d", err, n)
	}
	f, _, err = h2ReadFrame(preface[n:], h2MaxFrameSizeLimit)
	if err != nil || f.typ != h2FrameWindowUpdate || f.streamID != 0 {
		t.Fatalf("preface window update: %v, %v", err, f.typ)
	}
	if increment := binary.BigEndian.Uint32(f.payload); increment != uint32(1<<22-h2DefaultWindow) {
		t.Fatalf("connection window update %d", increment)
	}

	// Out of range values are kept within what a window, a frame and a table
	// may be, and a negative table size turns the dynamic table off.
	c = HTTP2Config{StreamWindow: 1, ConnWindow: 1 << 40, MaxFrameSize: 1 << 30, HeaderTableSize: -5,
		MaxAdaptiveWindow: 1}
	l = c.local()
	if l.streamWindow != h2DefaultWindow || l.connWindow != h2MaxWindow ||
		l.maxFrame != h2MaxFrameSizeLimit || l.tableSize != 0 {
		t.Fatalf("clamped %d, %d, %d, %d", l.streamWindow, l.connWindow, l.maxFrame, l.tableSize)
	}
	if s = l.settings(); len(s) != 3 {
		// The table size is announced even when it is down to zero.
		t.Fatalf("clamped settings %v", s)
	}

	// AdaptiveWindow bounds its growth at MaxAdaptiveWindow, or at the
	// stream window when that is larger, and off is no measurement at all.
	c = HTTP2Config{StreamWindow: 1 << 21, AdaptiveWindow: true}
	l = c.local()
	if l.bdp == nil || l.bdp.window != 1<<21 || l.bdp.limit != DefaultH2MaxAdaptiveWindow {
		t.Fatalf("bdp %+v", l.bdp)
	}
	c = HTTP2Config{StreamWindow: 1 << 20, AdaptiveWindow: true, MaxAdaptiveWindow: 1 << 22}
	l = c.local()
	if l.bdp == nil || l.bdp.limit != 1<<22 {
		t.Fatalf("bdp limit %+v", l.bdp)
	}
}

// TestHTTP2ConfigTableSize applies the configured table size through the
// decoder: it allows the default until the peer acknowledges the smaller
// setting, then holds the peer to it.
func TestHTTP2ConfigTableSize(t *testing.T) {
	c := HTTP2Config{HeaderTableSize: 128}
	l := c.local()
	dec := l.newDecoder()
	update := func(size byte, rest ...byte) []byte { return append([]byte{0x20 | 31, size}, rest...) }
	noop := func(hpack.HeaderField) error { return nil }
	// The peer may use the default until it has heard of the smaller table.
	if err := dec.Decode(update(0xe1, 0x1f), noop); err != nil { // 4096
		t.Fatalf("4096 before the ack: %v", err)
	}
	if l.settingsAcked != 0 {
		t.Fatalf("settings acked %d before the ack", l.settingsAcked)
	}
	l.settingsAck(dec)
	if l.settingsAcked != 1 {
		t.Fatalf("settings acked %d", l.settingsAcked)
	}
	if err := dec.Decode(update(0xe1, 0x1f), noop); err != hpack.ErrInvalidSizeUpdate {
		t.Fatalf("4096 after the ack: %v", err)
	}
	if err := dec.Decode(update(97), noop); err != nil { // 128
		t.Fatalf("128 after the ack: %v", err)
	}
	// A second ack of the same setting asks nothing more.
	l.settingsAck(dec)
	if err := dec.Decode(update(97), noop); err != nil {
		t.Fatalf("128 after the second ack: %v", err)
	}
}

// TestHTTP2ConfigServerSettings checks what a server configured with every
// parameter announces: its windows, frame size and table size.
func TestHTTP2ConfigServerSettings(t *testing.T) {
	config := DefaultConfig()
	config.HTTP2 = HTTP2Config{
		StreamWindow: 1 << 21, ConnWindow: 1 << 22, MaxFrameSize: 1 << 15, HeaderTableSize: 1 << 13,
	}
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
	})))
	tc := dialH2(t, addr)
	settings, connWindow := h2TestSettings(t, tc)
	if settings[h2SettingInitialWindowSize] != 1<<21 {
		t.Fatalf("stream window %d", settings[h2SettingInitialWindowSize])
	}
	if settings[h2SettingMaxFrameSize] != 1<<15 {
		t.Fatalf("max frame size %d", settings[h2SettingMaxFrameSize])
	}
	if settings[h2SettingHeaderTableSize] != 1<<13 {
		t.Fatalf("header table size %d", settings[h2SettingHeaderTableSize])
	}
	if want := int64(1<<22 - h2DefaultWindow); connWindow != want {
		t.Fatalf("connection window %d, want %d", connWindow, want)
	}
	tc.headers(1, true, get("/")...)
	if header, _ := tc.response(1); header[":status"] != "200" {
		t.Fatalf("status %q", header[":status"])
	}
}

// h2TestSettings reads a connection's opening SETTINGS and the window update
// that raises its connection window.
func h2TestSettings(t *testing.T, tc *h2TestConn) (map[h2SettingID]uint32, int64) {
	t.Helper()
	settings := map[h2SettingID]uint32{}
	var connWindow int64
	deadline := time.Now().Add(5 * time.Second)
	for len(settings) == 0 || connWindow == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the opening settings: %v, %d", settings, connWindow)
		}
		f := tc.read()
		switch f.typ {
		case h2FrameSettings:
			if f.has(h2FlagAck) {
				continue
			}
			if err := h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
				settings[id] = value
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		case h2FrameWindowUpdate:
			if f.streamID == 0 && len(f.payload) == 4 {
				connWindow = int64(binary.BigEndian.Uint32(f.payload))
			}
		}
	}
	return settings, connWindow
}

// TestHTTP2ConfigMaxFrameSize checks that a server reads frames as large as
// it announced, and that a default server refuses one past the protocol's.
func TestHTTP2ConfigMaxFrameSize(t *testing.T) {
	const size = 20000
	body := make([]byte, size)
	config := DefaultConfig()
	config.HTTP2.MaxFrameSize = 1 << 15
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		got, _ := io.ReadAll(c.Request.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(len(got))))
	})))
	tc := dialH2(t, addr)
	h2TestSettings(t, tc)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "test",
		":path", "/upload", "content-length", strconv.Itoa(size))
	tc.data(1, false, body[:size/2])
	tc.data(1, true, body[size/2:])
	if _, got := tc.response(1); string(got) != strconv.Itoa(size) {
		t.Fatalf("body %q", got)
	}

	// The same frame on a connection left at the protocol's frame size is a
	// connection error: the server never announced 16384 past.
	addr = serve(t, NewHandlerWithConfig(DefaultConfig(), HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
	})))
	tc = dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "test",
		":path", "/upload", "content-length", strconv.Itoa(size))
	tc.data(1, true, body)
	for {
		f := tc.read()
		if f.typ != h2FrameGoAway {
			continue
		}
		if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); code != H2FrameSizeError {
			t.Fatalf("GOAWAY code %v", code)
		}
		break
	}
}

// TestHTTP2ConfigServerTableSize checks that a server with a smaller table
// rejects a peer that goes on using the default one, once it has
// acknowledged the setting.
func TestHTTP2ConfigServerTableSize(t *testing.T) {
	config := DefaultConfig()
	config.HTTP2.HeaderTableSize = 128
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
	})))
	// A peer that acknowledged the setting, then sent a block that grows its
	// table back past it.
	tc := dialH2(t, addr)
	settings, _ := h2TestSettings(t, tc)
	if settings[h2SettingHeaderTableSize] != 128 {
		t.Fatalf("header table size %d", settings[h2SettingHeaderTableSize])
	}
	tc.write(h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0))
	// A dynamic table size update of 4096, past the 128 the server allows,
	// at the start of a header block.
	block := []byte{0x3f, 0xe1, 0x1f}
	tc.write(h2AppendHeaderBlock(nil, 1, block, true, h2DefaultMaxFrameSize))
	for {
		f := tc.read()
		if f.typ != h2FrameGoAway {
			continue
		}
		if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); code != H2CompressionError {
			t.Fatalf("GOAWAY code %v", code)
		}
		break
	}

	// A peer that shrinks its table as asked goes on being served.
	tc = dialH2(t, addr)
	h2TestSettings(t, tc)
	tc.enc.SetMaxTableSize(128)
	tc.write(h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0))
	tc.headers(1, true, get("/")...)
	if header, _ := tc.response(1); header[":status"] != "200" {
		t.Fatalf("status %q", header[":status"])
	}
}

// TestHTTP2ConfigClientSettings checks what a client configured with every
// parameter announces on its connection.
func TestHTTP2ConfigClientSettings(t *testing.T) {
	frames := make(chan map[h2SettingID]uint32, 1)
	windows := make(chan int64, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		settings := map[h2SettingID]uint32{}
		var connWindow int64
		for len(settings) == 0 || connWindow == 0 {
			f, ok := p.next()
			if !ok {
				return
			}
			switch f.typ {
			case h2FrameSettings:
				_ = h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
					settings[id] = value
					return nil
				})
			case h2FrameWindowUpdate:
				if f.streamID == 0 && len(f.payload) == 4 {
					connWindow = int64(binary.BigEndian.Uint32(f.payload))
				}
			}
		}
		frames <- settings
		windows <- connWindow
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, true, ":status", "204")
		p.drain()
	})
	client := cvH2cClient(t, func(c *ClientConfig) {
		c.HTTP2 = HTTP2Config{
			StreamWindow: 1 << 21, ConnWindow: 1 << 22, MaxFrameSize: 1 << 15,
			HeaderTableSize: 1 << 13, InitialMaxStreams: 5,
		}
	})
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
	settings := <-frames
	if settings[h2SettingInitialWindowSize] != 1<<21 {
		t.Fatalf("stream window %d", settings[h2SettingInitialWindowSize])
	}
	if settings[h2SettingMaxFrameSize] != 1<<15 {
		t.Fatalf("max frame size %d", settings[h2SettingMaxFrameSize])
	}
	if settings[h2SettingHeaderTableSize] != 1<<13 {
		t.Fatalf("header table size %d", settings[h2SettingHeaderTableSize])
	}
	if connWindow := <-windows; connWindow != int64(1<<22-h2DefaultWindow) {
		t.Fatalf("connection window %d", connWindow)
	}
}

// TestHTTP2ConfigAdaptiveWindow checks that a server measuring the
// bandwidth-delay product grows the stream window once the peer has filled
// the one it gave, and serves the request as before.
func TestHTTP2ConfigAdaptiveWindow(t *testing.T) {
	const size = 60000
	config := DefaultConfig()
	config.HTTP2 = HTTP2Config{StreamWindow: h2DefaultWindow, AdaptiveWindow: true, MaxAdaptiveWindow: 1 << 20}
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		got, _ := io.ReadAll(c.Request.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(len(got))))
	})))
	tc := dialH2(t, addr)
	h2TestSettings(t, tc)
	fields := append(get("/upload"), "content-length", strconv.Itoa(size))
	tc.headers(1, false, fields...)
	body := make([]byte, size)
	const chunk = 15000
	for sent := 0; sent < size; sent += chunk {
		tc.data(1, sent+chunk >= size, body[sent:min(sent+chunk, size)])
	}
	// The server pings once data arrives, and the measurement is over when
	// it is acknowledged. Everything was written before the ack, so the
	// sample is the whole body.
	for {
		f := tc.read()
		if f.typ == h2FramePing && !f.has(h2FlagAck) {
			tc.write(append(h2AppendFrameHeader(nil, h2FramePing, h2FlagAck, 0, 8), f.payload...))
			break
		}
	}
	grown := false
	deadline := time.Now().Add(5 * time.Second)
	for !grown {
		if time.Now().After(deadline) {
			t.Fatal("the server did not grow the window")
		}
		f := tc.read()
		switch f.typ {
		case h2FrameSettings:
			_ = h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
				if id == h2SettingInitialWindowSize && value > h2DefaultWindow {
					grown = true
				}
				return nil
			})
			if grown {
				tc.write(h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0))
			}
		case h2FrameGoAway:
			t.Fatalf("GOAWAY %v", H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])))
		}
	}
	if _, got := tc.response(1); string(got) != strconv.Itoa(size) {
		t.Fatalf("body %q", got)
	}
}

// TestH2BDP checks the bandwidth-delay product estimate: a sample too small
// to say the window is what holds the peer back grows nothing, one that
// fills the window doubles it within the limit, and the next ping waits.
func TestH2BDP(t *testing.T) {
	// A sample that does not come near the window says nothing.
	b := &h2BDP{window: 1000, limit: 8000}
	if ping := b.data(600); len(ping) == 0 || !isBDPAck(ping[h2FrameHeaderLen:]) {
		t.Fatalf("first data returned %q", ping)
	}
	if got := b.data(0); got != nil {
		t.Fatalf("a second ping while one is out: %q", got)
	}
	if got := b.ack(); got != 0 {
		t.Fatalf("grew to %d on a sample of 600 of a 1000 byte window", got)
	}

	// A sample that fills the window doubles it, up to the limit.
	for _, tc := range []struct {
		window, limit, sample, want int64
	}{
		{1000, 8000, 900, 1800},
		{7000, 8000, 7000, 8000},
	} {
		b = &h2BDP{window: tc.window, limit: tc.limit}
		b.data(tc.sample)
		if got := b.ack(); got != tc.want {
			t.Fatalf("window %d, sample %d: grew to %d, want %d", tc.window, tc.sample, got, tc.want)
		}
	}

	// The next measurement comes after the interval, and none is made once
	// the window has reached the limit.
	b = &h2BDP{window: 1000, limit: 8000}
	b.data(500)
	b.ack()
	if got := b.data(900); got != nil {
		t.Fatalf("pinged before the interval: %q", got)
	}
	b.next = time.Now().Add(-time.Millisecond)
	if got := b.data(900); len(got) == 0 {
		t.Fatal("no ping after the interval")
	}
	b = &h2BDP{window: 8000, limit: 8000}
	if got := b.data(8000); got != nil {
		t.Fatalf("pinged at the limit: %q", got)
	}

	if !isBDPAck(h2BDPPing[:]) || isBDPAck(make([]byte, 8)) || isBDPAck(h2BDPPing[:7]) {
		t.Fatal("the ping payload does not identify its acknowledgement")
	}
}

// TestH2LocalGrow checks the frames that raise a connection's windows.
func TestH2LocalGrow(t *testing.T) {
	l := &h2Local{streamWindow: 1000, connWindow: 2000}
	out, streamDelta, connDelta := l.grow(3000)
	if streamDelta != 2000 || connDelta != 1000 || l.streamWindow != 3000 || l.connWindow != 3000 {
		t.Fatalf("deltas %d, %d, windows %d, %d", streamDelta, connDelta, l.streamWindow, l.connWindow)
	}
	f, n, err := h2ReadFrame(out, h2MaxFrameSizeLimit)
	if err != nil || f.typ != h2FrameSettings {
		t.Fatalf("settings: %v", err)
	}
	_ = h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
		if id != h2SettingInitialWindowSize || value != 3000 {
			t.Fatalf("setting %v %d", id, value)
		}
		return nil
	})
	f, _, err = h2ReadFrame(out[n:], h2MaxFrameSizeLimit)
	if err != nil || f.typ != h2FrameWindowUpdate || f.streamID != 0 ||
		binary.BigEndian.Uint32(f.payload) != 1000 {
		t.Fatalf("window update: %v %+v", err, f)
	}
	// A stream window past the connection's raises that too.
	out, streamDelta, connDelta = l.grow(4000)
	if streamDelta != 1000 || connDelta != 1000 || len(out) != h2FrameHeaderLen+6+h2FrameHeaderLen+4 {
		t.Fatalf("grow again: deltas %d, %d, %d bytes", streamDelta, connDelta, len(out))
	}
}
