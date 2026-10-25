//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

// h2floodWrite writes b, ignoring an error: the server may end the
// connection before it has all of it.
func h2floodWrite(tc *h2TestConn, b []byte) {
	_, _ = tc.c.Write(b)
}

// h2floodResets is n streams opened, each waiting for its body, and reset
// at once.
func h2floodResets(tc *h2TestConn, first uint32, n int) []byte {
	var out []byte
	for i := 0; i < n; i++ {
		id := first + uint32(2*i)
		block := tc.enc.Begin(nil)
		for _, f := range [][2]string{{":method", "POST"}, {":scheme", "http"}, {":authority", "test"}, {":path", "/"}} {
			block = tc.enc.AppendField(block, f[0], f[1], false)
		}
		out = h2AppendHeaderBlock(out, id, block, false, 16384)
		out = append(h2AppendFrameHeader(out, h2FrameRSTStream, 0, id, 4), 0, 0, 0, byte(H2Cancel))
	}
	return out
}

// TestH2ServerEndsRapidReset checks that a client opening streams and
// resetting them at once, past the burst the server allows, has its
// connection ended with ENHANCE_YOUR_CALM (CVE-2023-44487).
func TestH2ServerEndsRapidReset(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), echoHandler()))
	tc := dialH2(t, addr)
	h2floodWrite(tc, h2floodResets(tc, 1, DefaultStreamResetBurst+100))
	if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
		t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
	}
}

// TestH2ServerAllowsResetsUnderTheBurst checks that resets within the burst
// leave the connection serving.
func TestH2ServerAllowsResetsUnderTheBurst(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), echoHandler()))
	tc := dialH2(t, addr)
	n := DefaultStreamResetBurst / 2
	tc.write(h2floodResets(tc, 1, n))
	id := uint32(1 + 2*n)
	tc.headers(id, true, get("/after")...)
	if header, _ := tc.response(id); header[":status"] != "200" {
		t.Fatalf("status %q after %d resets", header[":status"], n)
	}
}

// TestH2ServerEndsControlFrameFloods checks that PING, SETTINGS, empty DATA
// and empty CONTINUATION frames past the burst end the connection with
// ENHANCE_YOUR_CALM.
func TestH2ServerEndsControlFrameFloods(t *testing.T) {
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), echoHandler()))
	for _, tt := range []struct {
		name  string
		flood func(tc *h2TestConn) []byte
	}{
		{"PING", func(tc *h2TestConn) []byte {
			var out []byte
			for i := 0; i < DefaultControlFrameBurst+100; i++ {
				out = append(h2AppendFrameHeader(out, h2FramePing, 0, 0, 8), "flooding"...)
			}
			return out
		}},
		{"SETTINGS", func(tc *h2TestConn) []byte {
			var out []byte
			for i := 0; i < DefaultControlFrameBurst+100; i++ {
				out = h2AppendSettings(out)
			}
			return out
		}},
		{"empty DATA", func(tc *h2TestConn) []byte {
			block := tc.enc.Begin(nil)
			for _, f := range [][2]string{{":method", "POST"}, {":scheme", "http"}, {":authority", "test"}, {":path", "/"}} {
				block = tc.enc.AppendField(block, f[0], f[1], false)
			}
			out := h2AppendHeaderBlock(nil, 1, block, false, 16384)
			for i := 0; i < DefaultControlFrameBurst+100; i++ {
				out = h2AppendFrameHeader(out, h2FrameData, 0, 1, 0)
			}
			return out
		}},
		{"empty CONTINUATION", func(tc *h2TestConn) []byte {
			out := h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 0)
			for i := 0; i < DefaultControlFrameBurst+100; i++ {
				out = h2AppendFrameHeader(out, h2FrameContinuation, 0, 1, 0)
			}
			return out
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := dialH2(t, addr)
			h2floodWrite(tc, tt.flood(tc))
			if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
				t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
			}
		})
	}
}

// TestH2ServerBoundsContinuationFrames checks that a header block arriving
// in more CONTINUATION frames than the server allows, each carrying a byte,
// ends the connection though it stays under MaxHeaderBytes.
func TestH2ServerBoundsContinuationFrames(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 1 << 20
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	out := append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 1), 0)
	for i := 0; i < DefaultMaxContinuationFrames+10; i++ {
		out = append(h2AppendFrameHeader(out, h2FrameContinuation, 0, 1, 1), 0)
	}
	h2floodWrite(tc, out)
	if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
		t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
	}
}

// TestH2ServerBoundsASlowHeaderBlock checks that a header block whose
// CONTINUATION frames do not finish it within ReadHeaderTimeout ends the
// connection, though frames keep arriving.
func TestH2ServerBoundsASlowHeaderBlock(t *testing.T) {
	config := DefaultConfig()
	config.ReadHeaderTimeout = 200 * time.Millisecond
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	// The server's SETTINGS is read while the header block is open, and an
	// acknowledgement would interleave with the block's CONTINUATION frames,
	// which the server rightly refuses; the block's own timeout ends the
	// connection long before the acknowledgement would be due.
	tc.holdSettingsAck = true
	tc.write(append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 1), 0))
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(50 * time.Millisecond):
				h2floodWrite(tc, append(h2AppendFrameHeader(nil, h2FrameContinuation, 0, 1, 1), 0))
			}
		}
	}()
	start := time.Now()
	if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
		t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("GOAWAY after %v", elapsed)
	}
}

// h2floodPings is n PING frames, each asking for an acknowledgement.
func h2floodPings(n int) []byte {
	var out []byte
	for i := 0; i < n; i++ {
		out = append(h2AppendFrameHeader(out, h2FramePing, 0, 0, 8), "flooding"...)
	}
	return out
}

// h2floodSplitBlock is a header block for stream id sent a byte a frame: in
// a HEADERS frame and as many CONTINUATION frames as it has bytes left.
func h2floodSplitBlock(id uint32, block []byte, endStream bool) []byte {
	var flags uint8
	if endStream {
		flags = h2FlagEndStream
	}
	out := append(h2AppendFrameHeader(nil, h2FrameHeaders, flags, id, 1), block[0])
	for i := 1; i < len(block); i++ {
		flags = 0
		if i == len(block)-1 {
			flags = h2FlagEndHeaders
		}
		out = append(h2AppendFrameHeader(out, h2FrameContinuation, flags, id, 1), block[i])
	}
	return out
}

// TestH2ServerFloodLimitsFollowConfig checks that the flood limits Config
// sets replace the defaults: lowered, a client within the default ends its
// connection, and raised or lifted, one past the default keeps it serving.
func TestH2ServerFloodLimitsFollowConfig(t *testing.T) {
	const past = 2000
	for _, tt := range []struct {
		name   string
		config func(*Config)
		flood  func(tc *h2TestConn) []byte
		// next is the stream the request that follows the flood goes on, or
		// zero when the flood is to end the connection; carries says the
		// flood is that request itself.
		next    uint32
		carries bool
	}{
		{"lowered reset burst", func(c *Config) { c.StreamResetBurst = 10 },
			func(tc *h2TestConn) []byte { return h2floodResets(tc, 1, 50) }, 0, false},
		{"raised reset burst", func(c *Config) { c.StreamResetBurst = 2 * past },
			func(tc *h2TestConn) []byte { return h2floodResets(tc, 1, past) }, 1 + 2*past, false},
		{"lifted reset limit", func(c *Config) { c.StreamResetRate = -1 },
			func(tc *h2TestConn) []byte { return h2floodResets(tc, 1, past) }, 1 + 2*past, false},
		{"lowered control frame burst", func(c *Config) { c.ControlFrameBurst = 10 },
			func(*h2TestConn) []byte { return h2floodPings(50) }, 0, false},
		{"raised control frame burst", func(c *Config) { c.ControlFrameBurst = 2 * past },
			func(*h2TestConn) []byte { return h2floodPings(past) }, 1, false},
		{"lifted control frame limit", func(c *Config) { c.ControlFrameBurst = -1 },
			func(*h2TestConn) []byte { return h2floodPings(past) }, 1, false},
		{"lowered CONTINUATION bound", func(c *Config) { c.MaxContinuationFrames = 5 },
			func(tc *h2TestConn) []byte {
				return h2floodSplitBlock(1, tc.enc.AppendField(tc.enc.Begin(nil), "x-pad", "padding", false), false)
			}, 0, false},
		{"lifted CONTINUATION bound", func(c *Config) { c.MaxContinuationFrames = -1 },
			func(tc *h2TestConn) []byte {
				block := tc.enc.Begin(nil)
				fields := append(get("/after"), "x-pad", strings.Repeat("padding", 400))
				for i := 0; i < len(fields); i += 2 {
					block = tc.enc.AppendField(block, fields[i], fields[i+1], false)
				}
				if len(block) <= DefaultMaxContinuationFrames {
					t.Fatalf("block of %d bytes", len(block))
				}
				return h2floodSplitBlock(1, block, true)
			}, 1, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			tt.config(&config)
			addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
			tc := dialH2(t, addr)
			flood := tt.flood(tc)
			if tt.next == 0 {
				h2floodWrite(tc, flood)
				if code := h2covGoAway(t, tc); code != H2EnhanceYourCalm {
					t.Fatalf("GOAWAY %v, want ENHANCE_YOUR_CALM", code)
				}
				return
			}
			tc.write(flood)
			if !tt.carries {
				tc.headers(tt.next, true, get("/after")...)
			}
			if header, _ := tc.response(tt.next); header[":status"] != "200" {
				t.Fatalf("status %q after the flood", header[":status"])
			}
		})
	}
}

// TestH2ClientEndsServerFloods checks that a server sending PING, SETTINGS
// (after the first), or empty DATA and CONTINUATION frames past the burst,
// or a header block in more CONTINUATION frames than the bound, has the
// client end its connection with GOAWAY(ENHANCE_YOUR_CALM) and fail the
// request on it with that connection error.
func TestH2ClientEndsServerFloods(t *testing.T) {
	const past = DefaultControlFrameBurst + 100
	for _, tt := range []struct {
		name  string
		flood func(p *cvH2cPeer, id uint32) []byte
	}{
		{"PING", func(*cvH2cPeer, uint32) []byte { return h2floodPings(past) }},
		{"SETTINGS", func(*cvH2cPeer, uint32) []byte {
			var out []byte
			for i := 0; i < past; i++ {
				out = h2AppendSettings(out)
			}
			return out
		}},
		{"empty DATA", func(p *cvH2cPeer, id uint32) []byte {
			out := h2AppendHeaderBlock(nil, id, p.block(":status", "200"), false, h2DefaultMaxFrameSize)
			for i := 0; i < past; i++ {
				out = h2AppendFrameHeader(out, h2FrameData, 0, id, 0)
			}
			return out
		}},
		{"empty CONTINUATION", func(_ *cvH2cPeer, id uint32) []byte {
			out := append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, id, 1), 0x88)
			for i := 0; i < past; i++ {
				out = h2AppendFrameHeader(out, h2FrameContinuation, 0, id, 0)
			}
			return out
		}},
		{"CONTINUATION count", func(_ *cvH2cPeer, id uint32) []byte {
			out := append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, id, 1), 0x88)
			for i := 0; i < DefaultMaxContinuationFrames+10; i++ {
				out = append(h2AppendFrameHeader(out, h2FrameContinuation, 0, id, 1), 0)
			}
			return out
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			goAway := make(chan H2ErrorCode, 1)
			url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
				p.settings()
				id, _, ok := p.request()
				if !ok {
					return
				}
				p.write(tt.flood(p, id))
				f, _, ok := p.await(h2FrameGoAway)
				if ok && len(f.payload) >= 8 {
					goAway <- H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:]))
				}
				p.drain()
			})
			client := cvH2cClient(t, nil)
			// POST is not retried on another connection when this one fails.
			_, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
			var connErr *H2ConnError
			if !errors.As(err, &connErr) || connErr.Code != H2EnhanceYourCalm {
				t.Fatalf("err = %v, want a connection error ENHANCE_YOUR_CALM", err)
			}
			if code := <-goAway; code != H2EnhanceYourCalm {
				t.Fatalf("GOAWAY code %v, want ENHANCE_YOUR_CALM", code)
			}
		})
	}
}

// TestH2ClientAllowsControlFramesUnderTheBurst checks that a server's PING
// and SETTINGS frames within the burst leave the request to be answered.
func TestH2ClientAllowsControlFramesUnderTheBurst(t *testing.T) {
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		out := h2floodPings(DefaultControlFrameBurst / 2)
		for i := 0; i < DefaultControlFrameBurst/4; i++ {
			out = h2AppendSettings(out)
		}
		p.write(out)
		p.headers(id, true, ":status", "204")
		p.drain()
	})
	client := cvH2cClient(t, nil)
	resp, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 204 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
