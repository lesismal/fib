//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"time"

	"github.com/lesismal/fib/hpack"
)

// HTTP2Config is what one side of an HTTP/2 connection lets its peer send,
// and how: Config.HTTP2 sets it for a server's connections and
// ClientConfig.HTTP2 for a client's. Its zero value is the defaults, which
// suit most uses; each field says what moving it away from its default buys
// and costs.
type HTTP2Config struct {
	// StreamWindow is the flow-control window each stream is granted for
	// what the peer sends on it, SETTINGS_INITIAL_WINDOW_SIZE, given back
	// once half of it has been taken. A body buffered whole is bounded by
	// the body limits, and the window only paces the peer; a body that
	// streams is bounded by it, since it is given back only as the body is
	// consumed. One stream moves at most a window per round trip, so a large
	// upload or download over a long path wants a larger one, or
	// AdaptiveWindow. Zero means DefaultH2StreamWindow (1MB); the value is
	// kept between 65535, which a peer may send before it hears the
	// setting, and 2^31-1.
	StreamWindow int
	// ConnWindow is the window of the whole connection, shared by its
	// streams, given back once half of it has been taken; what a body that
	// streams takes is given back only as the body is consumed, as its
	// stream's window is, so it bounds what all the connection's streamed
	// bodies may have waiting for their handlers together. Zero means
	// DefaultH2ConnWindow (16MB); the value is kept between 65535 and
	// 2^31-1.
	ConnWindow int
	// MaxFrameSize is the largest frame payload the peer may send,
	// SETTINGS_MAX_FRAME_SIZE. A larger one lets a peer send a body in
	// fewer frames, for as much more memory per read as a frame may take.
	// Zero means 16384, the protocol's default; the value is kept between
	// 16384 and 2^24-1. What this side sends is framed to what the peer
	// allows, whatever this says.
	MaxFrameSize int
	// HeaderTableSize is the HPACK dynamic table each direction of a
	// connection may use, in bytes: the one the peer's encoder keeps a copy
	// of in this side's decoder, SETTINGS_HEADER_TABLE_SIZE, and the most
	// this side's encoder uses when the peer allows that much. A larger
	// table keeps more of the fields that repeat from one header block to
	// the next, cookies and long user agents, say, for that much more memory
	// each way per connection. Zero means 4096, the protocol's default;
	// below zero, no dynamic table, so that every field is sent in full. The
	// encoder uses at most hpack.MaxEncoderTableSize (1MB).
	HeaderTableSize int
	// AdaptiveWindow grows the stream and connection windows to the
	// connection's bandwidth-delay product, as gRPC's BDP estimation does:
	// while data arrives, a PING now and then measures how much arrives in a
	// round trip, and a window that the peer fills within one is doubled, up
	// to MaxAdaptiveWindow, with a SETTINGS frame for the streams and a
	// WINDOW_UPDATE for the connection. A transfer over a long path then
	// runs at the path's speed rather than at a window per round trip, at
	// the cost of the memory a larger window lets a streamed body take. The
	// windows never shrink. The PINGs are at least 25 milliseconds apart.
	AdaptiveWindow bool
	// MaxAdaptiveWindow bounds the stream window AdaptiveWindow grows. Zero
	// means DefaultH2MaxAdaptiveWindow (16MB).
	MaxAdaptiveWindow int
	// InitialMaxStreams is how many streams a client opens on a new
	// connection before the server's SETTINGS says how many it allows. Zero
	// means 100, the number RFC 9113 recommends a server allow at least. A
	// server ignores it.
	InitialMaxStreams int
}

const (
	// DefaultH2StreamWindow is HTTP2Config.StreamWindow's default, and
	// DefaultH2ConnWindow ConnWindow's.
	DefaultH2StreamWindow = 1 << 20
	DefaultH2ConnWindow   = 1 << 24
	// DefaultH2MaxAdaptiveWindow is HTTP2Config.MaxAdaptiveWindow's
	// default.
	DefaultH2MaxAdaptiveWindow = 1 << 24
	// h2DefaultInitialMaxStreams is HTTP2Config.InitialMaxStreams' default.
	h2DefaultInitialMaxStreams = 100
	// h2BDPInterval is how long a connection measuring its bandwidth-delay
	// product waits between one measurement and the next PING, so that a
	// path of short round trips does not draw a PING every few
	// microseconds, which the peer would count against its budget of
	// control frames (see DefaultControlFrameRate).
	h2BDPInterval = 25 * time.Millisecond
)

// h2Local is what one side of a connection lets its peer send, resolved
// from an HTTP2Config: the windows it grants, the frames it reads and its
// decoder's table. streamWindow and connWindow grow under AdaptiveWindow;
// only the connection's reader changes them, holding the connection's mu,
// so the reader reads them without it and everything else with it.
type h2Local struct {
	streamWindow int64
	connWindow   int64
	maxFrame     uint32
	tableSize    int
	// settingsSent counts the SETTINGS frames this side has sent, and
	// settingsAcked those the peer has acknowledged; the first carries the
	// table size, which the decoder holds the peer to once it is
	// acknowledged. The reader alone touches them.
	settingsSent  int
	settingsAcked int
	// bdp measures the bandwidth-delay product, with AdaptiveWindow; nil
	// otherwise.
	bdp *h2BDP
}

// local resolves c into what the connection grants its peer.
func (c *HTTP2Config) local() h2Local {
	l := h2Local{
		streamWindow: h2WindowSetting(c.StreamWindow, DefaultH2StreamWindow),
		connWindow:   h2WindowSetting(c.ConnWindow, DefaultH2ConnWindow),
		maxFrame:     h2DefaultMaxFrameSize,
		tableSize:    hpack.DefaultTableSize,
	}
	if c.MaxFrameSize > 0 {
		l.maxFrame = uint32(min(max(c.MaxFrameSize, h2DefaultMaxFrameSize), h2MaxFrameSizeLimit))
	}
	switch {
	case c.HeaderTableSize < 0:
		l.tableSize = 0
	case c.HeaderTableSize > 0:
		l.tableSize = min(c.HeaderTableSize, 1<<30)
	}
	if c.AdaptiveWindow {
		limit := h2WindowSetting(c.MaxAdaptiveWindow, DefaultH2MaxAdaptiveWindow)
		l.bdp = &h2BDP{window: l.streamWindow, limit: max(limit, l.streamWindow)}
	}
	return l
}

// initialMaxStreams is InitialMaxStreams with its default.
func (c *HTTP2Config) initialMaxStreams() int {
	if c.InitialMaxStreams > 0 {
		return c.InitialMaxStreams
	}
	return h2DefaultInitialMaxStreams
}

// h2WindowSetting is a configured window, or def for zero, kept within what
// a window may be.
func h2WindowSetting(n, def int) int64 {
	if n == 0 {
		n = def
	}
	return min(max(int64(n), h2DefaultWindow), h2MaxWindow)
}

// settings are the SETTINGS this side announces, beyond those the side adds
// of its own: the stream window always, and the frame and table sizes when
// they are not the protocol's defaults.
func (l *h2Local) settings(extra ...[2]uint32) [][2]uint32 {
	s := append(extra, [2]uint32{uint32(h2SettingInitialWindowSize), uint32(l.streamWindow)})
	if l.maxFrame != h2DefaultMaxFrameSize {
		s = append(s, [2]uint32{uint32(h2SettingMaxFrameSize), l.maxFrame})
	}
	if l.tableSize != hpack.DefaultTableSize {
		s = append(s, [2]uint32{uint32(h2SettingHeaderTableSize), uint32(l.tableSize)})
	}
	return s
}

// preface appends the SETTINGS this side opens with, and the WINDOW_UPDATE
// that raises the connection's window from the protocol's default, to out.
func (l *h2Local) preface(out []byte, extra ...[2]uint32) []byte {
	out = h2AppendSettings(out, l.settings(extra...)...)
	l.settingsSent++
	if l.connWindow > h2DefaultWindow {
		out = h2AppendWindowUpdate(out, 0, uint32(l.connWindow-h2DefaultWindow))
	}
	return out
}

// newDecoder returns the decoder for the peer's header blocks, allowed the
// larger of the configured table and the default: the peer may use the
// default until it has heard of a smaller one; see settingsAck.
func (l *h2Local) newDecoder() *hpack.Decoder {
	return hpack.NewDecoder(max(l.tableSize, hpack.DefaultTableSize))
}

// settingsAck records the peer's acknowledgement of a SETTINGS frame. The
// first is of the opening SETTINGS: from then on the peer is held to a table
// smaller than the default, if that is what they announced.
func (l *h2Local) settingsAck(dec *hpack.Decoder) {
	l.settingsAcked++
	if l.settingsAcked == 1 && l.tableSize < hpack.DefaultTableSize {
		dec.SetAllowedMaxSize(l.tableSize)
	}
}

// encoderLimit applies the configured table size to the encoder of this
// side's header blocks: the most it uses, whatever the peer allows.
func (l *h2Local) encoderLimit(enc *hpack.Encoder) {
	if l.tableSize != hpack.DefaultTableSize {
		enc.SetTableSizeLimit(l.tableSize)
	}
}

// h2BDPPing is the payload of the PINGs that measure the bandwidth-delay
// product, which tells their acknowledgements from those of other PINGs.
var h2BDPPing = [8]byte{'f', 'i', 'b', 'b', 'd', 'p', 0, 0}

// h2BDP estimates a connection's bandwidth-delay product the way gRPC does,
// from the bytes of DATA that arrive between a PING and its
// acknowledgement: that sample is what the peer sends in a round trip,
// which, once it comes close to the window, says the window is what holds
// the peer back. Only the connection's reader touches it.
type h2BDP struct {
	// window is the stream window as last grown, and limit how far it may
	// grow.
	window, limit int64
	// pinging says a PING is out, sent at sent, and sample counts the bytes
	// arrived since. next is when the next PING may go.
	pinging bool
	sent    time.Time
	next    time.Time
	sample  int64
	// rtt is the smoothed round trip, and maxRate the highest rate a sample
	// has shown, in bytes a second.
	rtt     float64
	maxRate float64
}

// data records n bytes of DATA, and returns the PING to send to start a
// measurement, or nil.
func (b *h2BDP) data(n int64) []byte {
	if b.pinging {
		b.sample += n
		return nil
	}
	if b.window >= b.limit {
		return nil
	}
	now := time.Now()
	if now.Before(b.next) {
		return nil
	}
	b.pinging, b.sent, b.sample = true, now, n
	return append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), h2BDPPing[:]...)
}

// ack ends a measurement on the acknowledgement of its PING, and returns the
// window the streams should now have, or zero to leave them as they are.
func (b *h2BDP) ack() int64 {
	if !b.pinging {
		return 0
	}
	b.pinging = false
	now := time.Now()
	b.next = now.Add(h2BDPInterval)
	rtt := now.Sub(b.sent).Seconds()
	if rtt <= 0 {
		rtt = 1e-9
	}
	if b.rtt == 0 {
		b.rtt = rtt
	} else {
		b.rtt += (rtt - b.rtt) * 0.1
	}
	rate := float64(b.sample) / b.rtt
	if rate > b.maxRate {
		b.maxRate = rate
	}
	// The peer sent close to a window in a round trip, at the highest rate
	// seen yet: the window is what holds it back.
	if float64(b.sample) < 0.66*float64(b.window) || rate < b.maxRate {
		return 0
	}
	grown := min(2*b.sample, b.limit)
	if grown <= b.window {
		return 0
	}
	b.window = grown
	return grown
}

// isBDPAck reports whether payload is the acknowledgement of a PING h2BDP
// sent.
func isBDPAck(payload []byte) bool {
	return len(payload) == 8 && binary.BigEndian.Uint64(payload) == binary.BigEndian.Uint64(h2BDPPing[:])
}

// grow is the frames that raise the windows to stream for each stream, and
// the connection's to at least that, from what they were. The caller adds
// the difference to the receive windows it keeps, under the connection's mu.
func (l *h2Local) grow(stream int64) (out []byte, streamDelta, connDelta int64) {
	streamDelta = stream - l.streamWindow
	l.streamWindow = stream
	l.settingsSent++
	out = h2AppendSettings(nil, [2]uint32{uint32(h2SettingInitialWindowSize), uint32(stream)})
	if stream > l.connWindow {
		connDelta = stream - l.connWindow
		l.connWindow = stream
		out = h2AppendWindowUpdate(out, 0, uint32(connDelta))
	}
	return out, streamDelta, connDelta
}
