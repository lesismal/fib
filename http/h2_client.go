//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	stdhttp "net/http"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/hpack"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/internal/reqbody"
	"github.com/lesismal/fib/sidepool"
)

// ErrPingTimeout is what the requests on an HTTP/2 connection get when the
// connection is taken for dead, the server having answered neither the PING
// that ClientConfig.ReadIdleTimeout sent nor anything else within
// ClientConfig.PingTimeout.
var ErrPingTimeout = errors.New("http2: connection lost: no answer to PING within PingTimeout")

// DefaultPingTimeout is ClientConfig.PingTimeout's zero.
const DefaultPingTimeout = 15 * time.Second

// errStreamGone is what sending a streamed request body gets once its stream
// or connection has ended, which whoever ended it reports.
var errStreamGone = errors.New("http2: stream closed")

// HTTP/2 on the client side: a clientConn whose TLS handshake chose "h2", or
// that was dialed for h2c, hands its traffic to an h2ClientConn, which runs
// each request as a stream of its own.

// h2InitialMaxStreams is how many streams a connection is given before the
// server's SETTINGS says how many it allows.
const h2InitialMaxStreams = 100

type h2ClientConn struct {
	cc     *clientConn
	client *Client

	// The read side is only touched by the worker running OnData.
	in          []byte
	gotSettings bool
	// settingsSeen says the server's first SETTINGS, which its preface
	// requires and so is no glitch, has been handled.
	settingsSeen  bool
	dec           *hpack.Decoder
	contStream    uint32
	contEndStream bool
	contBlock     []byte
	// contFrames counts the CONTINUATION frames of the header block still
	// arriving, which DefaultMaxContinuationFrames bounds.
	contFrames  int
	recvWindow  int64
	recvUnacked int64
	// glitches bounds the frames a server may send that ask for work and
	// carry no response, as the server's own budget does its client's; see
	// DefaultControlFrameBurst.
	glitches h2Budget

	// mu guards the write side and the streams. Frames are sent while holding
	// it, so header blocks leave in the order the encoder produced them.
	mu           sync.Mutex
	streams      map[uint32]*h2ClientStream
	enc          *hpack.Encoder
	nextID       uint32
	peerMaxFrame int
	peerWindow   int64
	sendWindow   int64
	// peerMaxHeaderList is the largest header list the server said it would
	// take, with SETTINGS_MAX_HEADER_LIST_SIZE, or math.MaxInt64 for no
	// limit, which is what it is until the server says otherwise.
	peerMaxHeaderList int64
	goAway            bool
	closed            bool
	// room wakes the senders of streamed request bodies waiting for their
	// stream's body to leave, or for the stream to end. Its lock is mu.
	room sync.Cond
	// failed is the error this side ended the connection with, which the
	// requests still on it then report.
	failed error

	// The health check, with ClientConfig.ReadIdleTimeout set, which
	// checking says: lastRead is when the server was last heard from, in
	// Unix nanoseconds, health the timer that looks, and pingSent when the
	// PING it sent went, zero while none is out. health and pingSent are
	// guarded by mu.
	checking bool
	lastRead atomic.Int64
	health   *time.Timer
	pingSent int64
}

// healthPing is the payload of the health check's PING.
var healthPing = [8]byte{'f', 'i', 'b', 'h', 'l', 't', 'h', 0}

type h2ClientStream struct {
	id uint32
	r  *clientRequest
	// The reader alone touches the response as it arrives.
	resp        *stdhttp.Response
	body        []byte
	declared    int64
	recvWindow  int64
	recvUnacked int64
	// stream is set when the response body streams to its callback, and
	// passed counts what has gone to it. A streamed stream's recvWindow and
	// recvUnacked are guarded by the connection's mu instead, since the
	// window is given back as the body is consumed, by whoever consumes it.
	stream *ResponseStream
	passed int64
	// The rest is guarded by the connection's mu.
	received   bool
	sendWindow int64
	pending    []byte
	localDone  bool
	// streaming says more of the request body is to come than pending
	// holds: it streams, and has not ended.
	streaming bool
	// pushRoom is a pushed body waiting for the windows to let pending go,
	// to be kicked once they have.
	pushRoom *reqbody.Pusher
	// trailer holds the request's trailer fields, as name and value pairs,
	// which go out in a HEADERS frame once the body has.
	trailer []string
	// gate holds back the body of a request that expects 100-continue.
	gate *reqbody.Gate
}

// startH2 turns the connection over to HTTP/2 before it carries anything,
// and sends the client's connection preface.
func (cc *clientConn) startH2() {
	config := cc.client.config
	dec := hpack.NewDecoder(hpack.DefaultTableSize)
	dec.MaxStringLength = config.MaxResponseHeaderBytes
	hc := &h2ClientConn{
		cc:                cc,
		client:            cc.client,
		dec:               dec,
		recvWindow:        h2ConnWindow,
		streams:           make(map[uint32]*h2ClientStream),
		enc:               hpack.NewEncoder(),
		nextID:            1,
		peerMaxFrame:      h2DefaultMaxFrameSize,
		peerWindow:        h2DefaultWindow,
		sendWindow:        h2DefaultWindow,
		peerMaxHeaderList: math.MaxInt64,
		glitches:          h2NewBudget(0, 0, DefaultControlFrameBurst, DefaultControlFrameRate),
	}
	hc.room.L = &hc.mu
	if config.ReadIdleTimeout > 0 {
		hc.checking = true
		hc.lastRead.Store(time.Now().UnixNano())
		hc.mu.Lock()
		hc.health = time.AfterFunc(config.ReadIdleTimeout, hc.checkHealth)
		hc.mu.Unlock()
	}
	cc.maxStreams = h2InitialMaxStreams
	cc.h2 = hc
	out := append([]byte(nil), h2Preface...)
	out = h2AppendSettings(out,
		[2]uint32{uint32(h2SettingEnablePush), 0},
		[2]uint32{uint32(h2SettingInitialWindowSize), h2StreamWindow},
		[2]uint32{uint32(h2SettingMaxHeaderListSize), uint32(config.MaxResponseHeaderBytes)},
	)
	out = h2AppendWindowUpdate(out, 0, h2ConnWindow-h2DefaultWindow)
	_ = cc.conn.SendOwned(out)
}

// checkHealth runs when the health check's timer fires. A server not heard
// from for ReadIdleTimeout is sent a PING; one that then stays silent for
// PingTimeout, its acknowledgement and every other frame included, is taken
// for dead: the connection is closed, and its requests fail with
// ErrPingTimeout, or go again on another when the server answered nothing
// of them and they are safe to repeat (see shutdown). Anything heard since
// the PING settles it, since a server that is sending is alive, if busy.
// Otherwise the timer is set for when there is next something to look at.
func (hc *h2ClientConn) checkHealth() {
	config := &hc.client.config
	readIdle, pingTimeout := int64(config.ReadIdleTimeout), int64(config.PingTimeout)
	now := time.Now().UnixNano()
	last := hc.lastRead.Load()
	hc.mu.Lock()
	if hc.closed {
		hc.mu.Unlock()
		return
	}
	if hc.pingSent != 0 {
		switch {
		case last >= hc.pingSent:
			hc.pingSent = 0
		case now-hc.pingSent >= pingTimeout:
			if hc.failed == nil {
				hc.failed = ErrPingTimeout
			}
			hc.goAway = true
			hc.mu.Unlock()
			hc.client.retire(hc.cc)
			// Not CloseAfterSend: the socket of a dead connection may
			// never drain what is queued for it.
			hc.cc.conn.Close()
			return
		default:
			hc.health.Reset(time.Duration(hc.pingSent + pingTimeout - now))
			hc.mu.Unlock()
			return
		}
	}
	if idle := now - last; idle < readIdle {
		hc.health.Reset(time.Duration(readIdle - idle))
		hc.mu.Unlock()
		return
	}
	hc.pingSent = now
	out := h2AppendFrameHeader(nil, h2FramePing, 0, 0, len(healthPing))
	hc.sendLocked(append(out, healthPing[:]...))
	hc.health.Reset(time.Duration(pingTimeout))
	hc.mu.Unlock()
}

func (hc *h2ClientConn) sendLocked(out []byte) {
	if len(out) > 0 && !hc.closed {
		_ = hc.cc.conn.SendOwned(out)
	}
}

// send opens a stream for r, which the client has already counted against
// the connection.
func (hc *h2ClientConn) send(r *clientRequest) {
	if !r.attach(hc.cc) {
		hc.client.streamDone(hc.cc)
		return
	}
	hc.mu.Lock()
	if hc.closed || hc.goAway {
		hc.mu.Unlock()
		r.detach()
		hc.client.streamDone(hc.cc)
		hc.client.enqueue(hc.cc.host.target, r, true)
		return
	}
	fields, trailer, err := h2RequestFields(r, hc.cc.host.target.secure)
	if err == nil && r.tunnel != nil {
		// A switch of protocols takes a connection of its own, which a
		// multiplexed one is not.
		err = ErrNotUpgradable
	}
	if err == nil && hc.peerMaxHeaderList < math.MaxInt64 &&
		(h2PairsSize(fields) > hc.peerMaxHeaderList || h2PairsSize(trailer) > hc.peerMaxHeaderList) {
		// The server said it would not take a header, or a trailer, this
		// large: the request fails here rather than costing a stream, the
		// bytes and room in the HPACK table for a certain refusal.
		err = errH2HeaderListTooLarge
	}
	if err != nil {
		hc.mu.Unlock()
		r.detach()
		hc.client.streamDone(hc.cc)
		r.finish(nil, err)
		return
	}
	id := hc.nextID
	hc.nextID += 2
	r.streamID = id
	st := &h2ClientStream{id: id, r: r, declared: -1, recvWindow: h2StreamWindow, sendWindow: hc.peerWindow}
	hc.streams[id] = st
	block := hc.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		name := fields[i]
		block = hc.enc.AppendField(block, name, fields[i+1], name == "authorization" || name == "cookie")
	}
	st.trailer = trailer
	st.streaming = r.upload != nil
	// The stream ends with the body, or with the trailers when there are any.
	out := h2AppendHeaderBlock(nil, id, block, len(r.body) == 0 && len(trailer) == 0 && r.upload == nil, hc.peerMaxFrame)
	if r.expect {
		// The body waits for the server's 100, or for the wait to run out.
		st.gate = reqbody.NewGate(func() { hc.sendHeld(st) })
	} else {
		out = hc.startBodyLocked(out, st)
	}
	hc.sendLocked(out)
	hc.mu.Unlock()
	if st.gate != nil {
		st.gate.Start(hc.client.config.ExpectContinueTimeout)
		return
	}
	hc.startUpload(st)
}

// startBodyLocked appends to out what follows the request's header block
// straight away: its body as far as the windows allow, or its trailers, or
// nothing, for a request that has ended or whose body streams.
func (hc *h2ClientConn) startBodyLocked(out []byte, st *h2ClientStream) []byte {
	r := st.r
	switch {
	case r.upload != nil:
	case len(r.body) > 0:
		st.pending = r.body
		out = hc.flushStreamLocked(out, st)
	case len(st.trailer) > 0:
		out = hc.appendTrailerLocked(out, st)
	default:
		st.endLocalLocked()
	}
	return out
}

// startUpload starts sending the body of st's request, if it streams.
func (hc *h2ClientConn) startUpload(st *h2ClientStream) {
	r := st.r
	if r.upload == nil {
		return
	}
	if push := r.upload.Pusher(); push != nil {
		attachPush(r, push, &h2Sink{hc: hc, st: st, push: push})
		return
	}
	// See clientConn.send for why a goroutine of its own.
	go hc.sendBody(st)
}

// sendHeld sends the body that st's gate held back, once the server has
// answered 100 (Continue) or the wait has run out, unless the stream has
// ended meanwhile.
func (hc *h2ClientConn) sendHeld(st *h2ClientStream) {
	hc.mu.Lock()
	if !hc.liveLocked(st) {
		hc.mu.Unlock()
		return
	}
	hc.sendLocked(hc.startBodyLocked(nil, st))
	hc.mu.Unlock()
	hc.startUpload(st)
}

// endLocalLocked records that st's request has all been handed to the
// connection, which starts the wait for its response's header.
func (st *h2ClientStream) endLocalLocked() {
	st.localDone = true
	st.r.requestSent()
}

// liveLocked reports whether st is still open on the connection.
func (hc *h2ClientConn) liveLocked(st *h2ClientStream) bool {
	return !hc.closed && hc.streams[st.id] == st
}

// sendBody sends the body of st's request, which streams, a piece at a time:
// each waits until the windows have let it all go, and then for the
// connection's queue to drain to uploadQueued, so that neither a stream
// the server reads slowly nor a slow socket has more of it in memory. It ends
// early when the stream does, which a response that arrives before the body
// has gone does (see complete).
func (hc *h2ClientConn) sendBody(st *h2ClientStream) {
	r := st.r
	rc, err := r.upload.Take()
	if err == nil {
		err = reqbody.Pump(rc, r.declared, func(p []byte) error {
			hc.mu.Lock()
			if !hc.liveLocked(st) {
				hc.mu.Unlock()
				return errStreamGone
			}
			st.pending = p
			hc.sendLocked(hc.flushStreamLocked(nil, st))
			for len(st.pending) > 0 && hc.liveLocked(st) {
				hc.room.Wait()
			}
			live := hc.liveLocked(st)
			// p is the caller's again: nothing may send from it now.
			st.pending = nil
			hc.mu.Unlock()
			if !live {
				return errStreamGone
			}
			return outputwait.Wait(hc.cc.conn, uploadQueued)
		})
	}
	if err == nil {
		hc.mu.Lock()
		if hc.liveLocked(st) {
			st.streaming = false
			// The trailer's values are those of when the body has been read.
			st.trailer = h2TrailerFields(r.req.Trailer)
			if h2PairsSize(st.trailer) > hc.peerMaxHeaderList {
				// The body has gone; a request without its trailer would
				// read as whole, which it is not.
				st.trailer = nil
				hc.mu.Unlock()
				r.abort(errH2HeaderListTooLarge)
				return
			}
			var out []byte
			if len(st.trailer) > 0 {
				out = hc.appendTrailerLocked(nil, st)
			} else {
				out = h2AppendFrameHeader(nil, h2FrameData, h2FlagEndStream, st.id, 0)
				st.endLocalLocked()
			}
			hc.sendLocked(out)
		}
		hc.mu.Unlock()
		return
	}
	var bodyErr *reqbody.BodyError
	if errors.As(err, &bodyErr) {
		err = bodyErr.Err
	} else if !errors.Is(err, reqbody.ErrNotReplayable) {
		// The stream or connection ended, which is reported where it did.
		return
	}
	r.abort(err)
}

// h2TrailerFields lists trailer fields, a request's Trailer or what
// CloseWithTrailer gave its body, as name and value pairs, leaving out any
// that h2RequestFields would refuse to announce.
func h2TrailerFields(fields stdhttp.Header) []string {
	var trailer []string
	for key, values := range fields {
		name := strings.ToLower(key)
		if !h2ValidHeaderName(name) || strings.HasPrefix(name, ":") || h2ConnectionHeaders[name] {
			continue
		}
		for _, value := range values {
			if validHeaderValue(value) {
				trailer = append(trailer, name, value)
			}
		}
	}
	return trailer
}

// h2RequestFields lists a request's header fields, pseudo-headers first, and
// its trailer fields, both as name and value pairs. Everything is checked
// before anything is encoded, since encoding changes the table the server
// tracks.
func h2RequestFields(r *clientRequest, secure bool) (fields, trailer []string, err error) {
	req := r.req
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	method := req.Method
	if method == "" {
		method = stdhttp.MethodGet
	}
	fields = []string{":method", method}
	if method != stdhttp.MethodConnect {
		scheme := "http"
		if secure {
			scheme = "https"
		}
		fields = append(fields, ":scheme", scheme, ":authority", host, ":path", req.URL.RequestURI())
	} else {
		fields = append(fields, ":authority", host)
	}
	userAgent, te := false, false
	for key, values := range req.Header {
		name := strings.ToLower(key)
		if name == "te" {
			// Only "trailers" may go (RFC 9113 section 8.2.2), which gRPC
			// servers ask of their clients.
			te = te || slices.ContainsFunc(values, hasTrailersToken)
			continue
		}
		if h2ConnectionHeaders[name] || name == "host" || name == "content-length" || name == "trailer" {
			continue
		}
		if !h2ValidHeaderName(name) {
			return nil, nil, errors.New("http: invalid request header name " + strconv.Quote(key))
		}
		userAgent = userAgent || name == "user-agent"
		for _, value := range values {
			if !validHeaderValue(value) {
				return nil, nil, errors.New("http: invalid request header value for " + key)
			}
			fields = append(fields, name, value)
		}
	}
	if !userAgent {
		fields = append(fields, "user-agent", "Go-http-client/2.0")
	}
	if te {
		fields = append(fields, "te", "trailers")
	}
	switch {
	case len(r.body) > 0:
		fields = append(fields, "content-length", strconv.Itoa(len(r.body)))
	case r.upload != nil:
		if r.declared >= 0 {
			fields = append(fields, "content-length", strconv.FormatInt(r.declared, 10))
		}
	case method == stdhttp.MethodPost || method == stdhttp.MethodPut || method == stdhttp.MethodPatch:
		fields = append(fields, "content-length", "0")
	}
	var declared []string
	for key, values := range req.Trailer {
		name := strings.ToLower(key)
		if !h2ValidHeaderName(name) || strings.HasPrefix(name, ":") || h2ConnectionHeaders[name] {
			return nil, nil, errors.New("http: invalid request trailer name " + strconv.Quote(key))
		}
		declared = append(declared, name)
		for _, value := range values {
			if !validHeaderValue(value) {
				return nil, nil, errors.New("http: invalid request trailer value for " + key)
			}
			trailer = append(trailer, name, value)
		}
	}
	if len(declared) > 0 {
		// Servers keep only the trailers a request announced, so the names
		// go out ahead of the body in a Trailer field of this side's making.
		slices.Sort(declared)
		fields = append(fields, "trailer", strings.Join(declared, ","))
	}
	return fields, trailer, nil
}

// hasTrailersToken reports whether a TE field's value lists "trailers".
func hasTrailersToken(value string) bool {
	for item := range strings.SplitSeq(value, ",") {
		if name, _, _ := strings.Cut(item, ";"); strings.EqualFold(strings.TrimSpace(name), "trailers") {
			return true
		}
	}
	return false
}

// cancel abandons r's stream, which the request's timeout or context has
// already failed.
func (hc *h2ClientConn) cancel(r *clientRequest) {
	hc.mu.Lock()
	st := hc.streams[r.streamID]
	if st == nil || st.r != r {
		hc.mu.Unlock()
		return
	}
	delete(hc.streams, st.id)
	hc.sendLocked(h2AppendRSTStream(nil, st.id, H2Cancel))
	hc.closeIfDoneLocked()
	hc.room.Broadcast()
	hc.mu.Unlock()
	st.gate.Shut()
	hc.client.streamDone(hc.cc)
}

// shutdown fails or retries whatever the connection still carried when it
// closed.
func (hc *h2ClientConn) shutdown(err error) {
	hc.mu.Lock()
	hc.closed = true
	if hc.health != nil {
		hc.health.Stop()
	}
	if hc.failed != nil {
		err = hc.failed
	}
	streams := make([]*h2ClientStream, 0, len(hc.streams))
	for _, st := range hc.streams {
		streams = append(streams, st)
	}
	clear(hc.streams)
	hc.room.Broadcast()
	hc.mu.Unlock()
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	for _, st := range streams {
		st.gate.Shut()
		r := st.r
		r.detach()
		if !st.received && r.retryable() {
			// Nothing came back, so a request that is safe to repeat goes out
			// again on another connection.
			r.retried = true
			hc.client.enqueue(hc.cc.host.target, r, true)
			continue
		}
		// OnClose runs in the connection's last round, which may be on the
		// event loop, and the callback must not hold that up, so it runs on
		// the client pool.
		sidepool.Go(func() { r.finish(nil, err) })
	}
}

// fail ends the connection with GOAWAY; OnClose then settles its streams.
func (hc *h2ClientConn) fail(err *H2ConnError) {
	hc.mu.Lock()
	if !hc.closed {
		hc.sendLocked(h2AppendGoAway(nil, 0, err.Code, err.Reason))
	}
	hc.goAway = true
	if hc.failed == nil {
		hc.failed = err
	}
	hc.mu.Unlock()
	hc.client.retire(hc.cc)
	hc.cc.conn.CloseAfterSend()
}

// closeIfDoneLocked closes a connection the server is retiring once its last
// stream has finished.
func (hc *h2ClientConn) closeIfDoneLocked() {
	if hc.goAway && !hc.closed && len(hc.streams) == 0 {
		hc.cc.conn.CloseAfterSend()
	}
}

func (hc *h2ClientConn) feed(data []byte) {
	if hc.checking {
		hc.lastRead.Store(time.Now().UnixNano())
	}
	hc.in = bufferpool.Append(hc.in, data)
	offset := 0
	for {
		f, n, err := h2ReadFrame(hc.in[offset:], h2DefaultMaxFrameSize)
		if err == nil && n > 0 {
			offset += n
			err = hc.handleFrame(&f)
		}
		if err != nil {
			var streamErr *H2StreamError
			if errors.As(err, &streamErr) {
				hc.resetStream(streamErr.StreamID, streamErr.Code, streamErr)
				continue
			}
			var connErr *H2ConnError
			if !errors.As(err, &connErr) {
				connErr = h2ConnErr(H2InternalError, "%v", err)
			}
			hc.fail(connErr)
			bufferpool.Put(hc.in)
			hc.in = nil
			return
		}
		if n == 0 {
			break
		}
	}
	if offset == len(hc.in) {
		if cap(hc.in) > maxRetainedBuffer {
			bufferpool.Put(hc.in)
			hc.in = nil
		} else {
			hc.in = hc.in[:0]
		}
	} else if offset > 0 {
		hc.in = append(hc.in[:0], hc.in[offset:]...)
	}
}

func (hc *h2ClientConn) handleFrame(f *h2Frame) error {
	if hc.contStream != 0 && (f.typ != h2FrameContinuation || f.streamID != hc.contStream) {
		return h2ConnErr(H2ProtocolError, "expected CONTINUATION for stream %d", hc.contStream)
	}
	if !hc.gotSettings {
		if f.typ != h2FrameSettings || f.has(h2FlagAck) {
			return h2ConnErr(H2ProtocolError, "server preface must be SETTINGS")
		}
		hc.gotSettings = true
	}
	switch f.typ {
	case h2FrameData:
		return hc.handleData(f)
	case h2FrameHeaders:
		if f.streamID == 0 {
			return h2ConnErr(H2ProtocolError, "HEADERS on stream 0")
		}
		block := f.payload
		if f.has(h2FlagPriority) {
			if len(block) < 5 {
				return h2ConnErr(H2FrameSizeError, "HEADERS priority truncated")
			}
			block = block[5:]
		}
		if !f.has(h2FlagEndHeaders) {
			if len(block) > hc.client.config.MaxResponseHeaderBytes {
				return h2ConnErr(H2EnhanceYourCalm, "header block too large")
			}
			if len(block) == 0 {
				if err := hc.glitch(); err != nil {
					return err
				}
			}
			hc.contStream, hc.contEndStream = f.streamID, f.has(h2FlagEndStream)
			hc.contBlock = bufferpool.Append(hc.contBlock[:0], block)
			hc.contFrames = 0
			return nil
		}
		return hc.handleHeaderBlock(f.streamID, block, f.has(h2FlagEndStream))
	case h2FrameContinuation:
		if hc.contStream == 0 {
			return h2ConnErr(H2ProtocolError, "unexpected CONTINUATION")
		}
		hc.contBlock = bufferpool.Append(hc.contBlock, f.payload)
		if len(hc.contBlock) > hc.client.config.MaxResponseHeaderBytes {
			return h2ConnErr(H2EnhanceYourCalm, "header block too large")
		}
		hc.contFrames++
		if hc.contFrames > DefaultMaxContinuationFrames {
			return h2ConnErr(H2EnhanceYourCalm, "header block in too many frames")
		}
		if !f.has(h2FlagEndHeaders) {
			if len(f.payload) == 0 {
				return hc.glitch()
			}
			return nil
		}
		id := hc.contStream
		hc.contStream = 0
		err := hc.handleHeaderBlock(id, hc.contBlock, hc.contEndStream)
		if cap(hc.contBlock) > maxRetainedBuffer {
			bufferpool.Put(hc.contBlock)
			hc.contBlock = nil
		}
		return err
	case h2FramePriority:
		return nil
	case h2FrameRSTStream:
		if f.streamID == 0 || len(f.payload) != 4 {
			return h2ConnErr(H2ProtocolError, "malformed RST_STREAM")
		}
		code := H2ErrorCode(binary.BigEndian.Uint32(f.payload))
		hc.endStream(f.streamID, code == H2RefusedStream, &H2StreamError{StreamID: f.streamID, Code: code})
		return nil
	case h2FrameSettings:
		return hc.handleSettings(f)
	case h2FramePushPromise:
		// SETTINGS_ENABLE_PUSH is 0, so a server may not push.
		return h2ConnErr(H2ProtocolError, "server sent PUSH_PROMISE")
	case h2FramePing:
		if f.streamID != 0 || len(f.payload) != 8 {
			return h2ConnErr(H2ProtocolError, "malformed PING")
		}
		if !f.has(h2FlagAck) {
			if err := hc.glitch(); err != nil {
				return err
			}
			out := h2AppendFrameHeader(nil, h2FramePing, h2FlagAck, 0, 8)
			hc.mu.Lock()
			hc.sendLocked(append(out, f.payload...))
			hc.mu.Unlock()
		}
		return nil
	case h2FrameGoAway:
		if f.streamID != 0 || len(f.payload) < 8 {
			return h2ConnErr(H2ProtocolError, "malformed GOAWAY")
		}
		hc.handleGoAway(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
		return nil
	case h2FrameWindowUpdate:
		return hc.handleWindowUpdate(f)
	}
	return nil
}

func (hc *h2ClientConn) handleSettings(f *h2Frame) error {
	if f.streamID != 0 {
		return h2ConnErr(H2ProtocolError, "SETTINGS on stream %d", f.streamID)
	}
	if f.has(h2FlagAck) {
		if len(f.payload) != 0 {
			return h2ConnErr(H2FrameSizeError, "SETTINGS ack with payload")
		}
		return nil
	}
	if hc.settingsSeen {
		if err := hc.glitch(); err != nil {
			return err
		}
	}
	hc.settingsSeen = true
	maxStreams := -1
	hc.mu.Lock()
	err := h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
		switch id {
		case h2SettingHeaderTableSize:
			hc.enc.SetMaxTableSize(int(min(value, hpack.DefaultTableSize)))
		case h2SettingEnablePush:
			if value > 1 {
				return h2ConnErr(H2ProtocolError, "ENABLE_PUSH %d", value)
			}
		case h2SettingMaxConcurrentStreams:
			maxStreams = int(min(value, 1<<20))
		case h2SettingInitialWindowSize:
			if value > h2MaxWindow {
				return h2ConnErr(H2FlowControlError, "INITIAL_WINDOW_SIZE %d", value)
			}
			delta := int64(value) - hc.peerWindow
			hc.peerWindow = int64(value)
			for _, st := range hc.streams {
				st.sendWindow += delta
				if st.sendWindow > h2MaxWindow {
					return h2ConnErr(H2FlowControlError, "stream window overflow")
				}
			}
		case h2SettingMaxFrameSize:
			if value < h2DefaultMaxFrameSize || value > h2MaxFrameSizeLimit {
				return h2ConnErr(H2ProtocolError, "MAX_FRAME_SIZE %d", value)
			}
			hc.peerMaxFrame = int(value)
		case h2SettingMaxHeaderListSize:
			hc.peerMaxHeaderList = int64(value)
		}
		return nil
	})
	if err == nil {
		out := h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0)
		hc.sendLocked(hc.flushLocked(out))
	}
	hc.mu.Unlock()
	if err != nil {
		return err
	}
	if maxStreams >= 0 {
		c, h := hc.client, hc.cc.host
		c.mu.Lock()
		hc.cc.maxStreams = maxStreams
		work, dials := c.dispatchLocked(h)
		c.mu.Unlock()
		c.carryOut(h, work, dials)
	}
	return nil
}

func (hc *h2ClientConn) handleWindowUpdate(f *h2Frame) error {
	if len(f.payload) != 4 {
		return h2ConnErr(H2FrameSizeError, "WINDOW_UPDATE length %d", len(f.payload))
	}
	increment := int64(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if f.streamID == 0 {
		if increment == 0 {
			return h2ConnErr(H2ProtocolError, "WINDOW_UPDATE of 0")
		}
		hc.sendWindow += increment
		if hc.sendWindow > h2MaxWindow {
			return h2ConnErr(H2FlowControlError, "connection window overflow")
		}
	} else if st := hc.streams[f.streamID]; st != nil {
		if increment == 0 {
			return &H2StreamError{StreamID: f.streamID, Code: H2ProtocolError}
		}
		st.sendWindow += increment
		if st.sendWindow > h2MaxWindow {
			return &H2StreamError{StreamID: f.streamID, Code: H2FlowControlError}
		}
	}
	hc.sendLocked(hc.flushLocked(nil))
	return nil
}

// handleGoAway retires the connection. Streams past lastID were never
// processed, so they go out again on another connection.
func (hc *h2ClientConn) handleGoAway(lastID uint32) {
	hc.mu.Lock()
	hc.goAway = true
	var unprocessed []*h2ClientStream
	for id, st := range hc.streams {
		if id > lastID {
			unprocessed = append(unprocessed, st)
			delete(hc.streams, id)
		}
	}
	hc.closeIfDoneLocked()
	hc.room.Broadcast()
	hc.mu.Unlock()
	hc.client.retire(hc.cc)
	for _, st := range unprocessed {
		st.gate.Shut()
		st.r.detach()
		hc.client.streamDone(hc.cc)
		hc.client.enqueue(hc.cc.host.target, st.r, true)
	}
}

func (hc *h2ClientConn) handleData(f *h2Frame) error {
	if f.streamID == 0 {
		return h2ConnErr(H2ProtocolError, "DATA on stream 0")
	}
	if len(f.payload) == 0 && !f.has(h2FlagEndStream) {
		if err := hc.glitch(); err != nil {
			return err
		}
	}
	length := int64(f.length)
	if length > hc.recvWindow {
		return h2ConnErr(H2FlowControlError, "DATA exceeds connection window")
	}
	hc.recvWindow -= length
	hc.recvUnacked += length
	var out []byte
	if hc.recvUnacked >= h2ConnWindow/2 {
		out = h2AppendWindowUpdate(out, 0, uint32(hc.recvUnacked))
		hc.recvWindow += hc.recvUnacked
		hc.recvUnacked = 0
	}
	hc.mu.Lock()
	st := hc.streams[f.streamID]
	if st != nil {
		st.received = true
	}
	hc.sendLocked(out)
	hc.mu.Unlock()
	if st == nil {
		// A stream abandoned in the meantime.
		return nil
	}
	if st.resp == nil {
		return &H2StreamError{StreamID: st.id, Code: H2ProtocolError}
	}
	if st.stream != nil {
		return hc.handleStreamedData(st, f)
	}
	if length > st.recvWindow {
		return &H2StreamError{StreamID: st.id, Code: H2FlowControlError}
	}
	st.recvWindow -= length
	st.recvUnacked += length
	if int64(len(st.body))+int64(len(f.payload)) > hc.client.config.MaxResponseBodyBytes {
		hc.resetStream(st.id, H2Cancel, ErrResponseBodyTooLarge)
		return nil
	}
	st.body = append(st.body, f.payload...)
	if f.has(h2FlagEndStream) {
		hc.complete(st)
		return nil
	}
	if st.recvUnacked >= h2StreamWindow/2 {
		increment := st.recvUnacked
		st.recvWindow += increment
		st.recvUnacked = 0
		hc.mu.Lock()
		hc.sendLocked(h2AppendWindowUpdate(nil, st.id, uint32(increment)))
		hc.mu.Unlock()
	}
	return nil
}

// handleStreamedData passes the DATA of a streaming response to its stream.
// The stream's window is not given back as the data arrives but as it is
// consumed (see credit), so a consumer that falls behind slows the server
// down to the window; padding, which no one consumes, is given back at once.
func (hc *h2ClientConn) handleStreamedData(st *h2ClientStream, f *h2Frame) error {
	length := int64(f.length)
	hc.mu.Lock()
	if length > st.recvWindow {
		hc.mu.Unlock()
		return &H2StreamError{StreamID: st.id, Code: H2FlowControlError}
	}
	st.recvWindow -= length
	hc.mu.Unlock()
	hc.credit(st, length-int64(len(f.payload)))
	st.passed += int64(len(f.payload))
	if limit := hc.client.config.MaxStreamedBodyBytes; limit > 0 && st.passed > limit {
		hc.resetStream(st.id, H2Cancel, ErrResponseBodyTooLarge)
		return nil
	}
	if len(f.payload) > 0 {
		st.stream.Data(f.payload)
	}
	if f.has(h2FlagEndStream) {
		hc.complete(st)
	}
	return nil
}

// credit gives a streamed response's sender window for n bytes of its body
// that have been consumed, in updates of at least half a window.
func (hc *h2ClientConn) credit(st *h2ClientStream, n int64) {
	if n <= 0 {
		return
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if hc.streams[st.id] != st || hc.closed {
		return
	}
	st.recvUnacked += n
	if st.recvUnacked >= h2StreamWindow/2 {
		increment := st.recvUnacked
		st.recvWindow += increment
		st.recvUnacked = 0
		hc.sendLocked(h2AppendWindowUpdate(nil, st.id, uint32(increment)))
	}
}

func (hc *h2ClientConn) handleHeaderBlock(id uint32, block []byte, endStream bool) error {
	var fields []hpack.HeaderField
	listSize := 0
	err := hc.dec.Decode(block, func(f hpack.HeaderField) error {
		listSize += len(f.Name) + len(f.Value) + 32
		fields = append(fields, f)
		return nil
	})
	if err != nil {
		return h2ConnErr(H2CompressionError, "%v", err)
	}
	hc.mu.Lock()
	st := hc.streams[id]
	if st != nil {
		st.received = true
	}
	hc.mu.Unlock()
	if st == nil {
		return nil
	}
	if listSize > hc.client.config.MaxResponseHeaderBytes {
		hc.resetStream(id, H2Cancel, ErrResponseHeaderTooLarge)
		return nil
	}
	if st.resp != nil {
		// Trailers.
		if !endStream {
			return &H2StreamError{StreamID: id, Code: H2ProtocolError}
		}
		trailer := make(stdhttp.Header)
		for _, f := range fields {
			if strings.HasPrefix(f.Name, ":") {
				return &H2StreamError{StreamID: id, Code: H2ProtocolError}
			}
			key := textproto.CanonicalMIMEHeaderKey(f.Name)
			trailer[key] = append(trailer[key], f.Value)
		}
		st.resp.Trailer = trailer
		hc.complete(st)
		return nil
	}
	resp, err := h2NewResponse(fields, st.r.req)
	if err != nil {
		return &H2StreamError{StreamID: id, Code: H2ProtocolError}
	}
	if resp == nil {
		// An interim 1xx response; the real one follows.
		if endStream {
			return &H2StreamError{StreamID: id, Code: H2ProtocolError}
		}
		if h2IsContinue(fields) {
			st.gate.Open()
		}
		return nil
	}
	// The answer has come, and a body still held back is not wanted: the
	// stream is reset once the response is complete (see complete).
	st.gate.Shut()
	st.r.headerArrived()
	config := &hc.client.config
	streams := config.StreamResponseBody && !st.r.whole && !endStream && bodyAllowed(st.r.req, resp.StatusCode) &&
		!(resp.ContentLength >= 0 && resp.ContentLength <= max(config.StreamResponseBodyThreshold, 0))
	limit := config.MaxResponseBodyBytes
	if streams {
		limit = config.MaxStreamedBodyBytes
	}
	if limit > 0 && resp.ContentLength > limit && bodyAllowed(st.r.req, resp.StatusCode) {
		hc.resetStream(id, H2Cancel, ErrResponseBodyTooLarge)
		return nil
	}
	st.resp = resp
	st.declared = resp.ContentLength
	if endStream {
		hc.complete(st)
		return nil
	}
	if streams {
		st.stream = newClientStream(hc.cc, st.r, resp, func(n int64) { hc.credit(st, n) })
		st.stream.Start()
	}
	return nil
}

// h2IsContinue reports whether an interim response's fields are a
// 100 (Continue)'s.
func h2IsContinue(fields []hpack.HeaderField) bool {
	for _, f := range fields {
		if f.Name == ":status" {
			return f.Value == "100"
		}
	}
	return false
}

// h2NewResponse builds a response from its header fields. It returns nil for
// an interim response.
func h2NewResponse(fields []hpack.HeaderField, req *stdhttp.Request) (*stdhttp.Response, error) {
	status := ""
	header := make(stdhttp.Header, len(fields))
	for _, f := range fields {
		if strings.HasPrefix(f.Name, ":") {
			if f.Name != ":status" || status != "" || len(header) > 0 {
				return nil, errors.New("malformed pseudo-header")
			}
			status = f.Value
			continue
		}
		if h2ConnectionHeaders[f.Name] {
			return nil, errors.New("connection-specific header")
		}
		key := textproto.CanonicalMIMEHeaderKey(f.Name)
		header[key] = append(header[key], f.Value)
	}
	code, err := strconv.Atoi(status)
	if err != nil || len(status) != 3 || code < 100 {
		return nil, errors.New("malformed :status")
	}
	if code < 200 {
		if code == stdhttp.StatusSwitchingProtocols {
			return nil, errors.New("101 over HTTP/2")
		}
		return nil, nil
	}
	resp := &stdhttp.Response{
		Status:        status + " " + stdhttp.StatusText(code),
		StatusCode:    code,
		Proto:         "HTTP/2.0",
		ProtoMajor:    2,
		Header:        header,
		ContentLength: -1,
		Request:       req,
		Body:          stdhttp.NoBody,
	}
	if cl := header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err != nil || n < 0 {
			return nil, errors.New("malformed content-length")
		}
		resp.ContentLength = n
	}
	return resp, nil
}

// complete hands a finished response to its request.
func (hc *h2ClientConn) complete(st *h2ClientStream) {
	resp := st.resp
	total := int64(len(st.body))
	if st.stream != nil {
		total = st.passed
	}
	if bodyAllowed(st.r.req, resp.StatusCode) && st.declared >= 0 && st.declared != total {
		hc.resetStream(st.id, H2ProtocolError, errors.New("http2: response body length does not match Content-Length"))
		return
	}
	hc.mu.Lock()
	if hc.streams[st.id] != st {
		hc.mu.Unlock()
		return
	}
	delete(hc.streams, st.id)
	if !st.localDone {
		// The server answered before taking the whole body; stop sending it.
		st.pending = nil
		hc.sendLocked(h2AppendRSTStream(nil, st.id, H2Cancel))
	}
	hc.closeIfDoneLocked()
	hc.room.Broadcast()
	hc.mu.Unlock()
	if st.stream != nil {
		st.r.detach()
		hc.client.streamDone(hc.cc)
		st.stream.End()
		return
	}
	if len(st.body) > 0 {
		resp.Body = &bufferedBody{data: st.body}
	}
	if resp.ContentLength < 0 && st.r.req.Method != stdhttp.MethodHead {
		resp.ContentLength = int64(len(st.body))
	}
	st.body = nil
	st.r.detach()
	hc.client.streamDone(hc.cc)
	st.r.finish(resp, nil)
}

// resetStream ends a stream this side found at fault with RST_STREAM, and
// fails its request with err.
func (hc *h2ClientConn) resetStream(id uint32, code H2ErrorCode, err error) {
	hc.mu.Lock()
	hc.sendLocked(h2AppendRSTStream(nil, id, code))
	hc.mu.Unlock()
	hc.endStream(id, false, err)
}

// endStream forgets a stream and fails its request, or retries it when the
// server refused it before doing anything.
func (hc *h2ClientConn) endStream(id uint32, retry bool, err error) {
	hc.mu.Lock()
	st := hc.streams[id]
	if st != nil {
		delete(hc.streams, id)
		st.pending = nil
	}
	hc.closeIfDoneLocked()
	hc.room.Broadcast()
	hc.mu.Unlock()
	if st == nil {
		return
	}
	st.gate.Shut()
	st.r.detach()
	hc.client.streamDone(hc.cc)
	if retry {
		hc.client.enqueue(hc.cc.host.target, st.r, true)
		return
	}
	st.r.finish(nil, err)
}

// flushLocked sends what request bodies the windows now allow.
func (hc *h2ClientConn) flushLocked(out []byte) []byte {
	for _, st := range hc.streams {
		if hc.sendWindow <= 0 {
			break
		}
		if len(st.pending) > 0 {
			out = hc.flushStreamLocked(out, st)
		}
	}
	for _, st := range hc.streams {
		if st.pushRoom != nil && len(st.pending) == 0 {
			goKick(st.pushRoom)
			st.pushRoom = nil
		}
	}
	// A streamed body waiting for its piece to leave looks again.
	hc.room.Broadcast()
	return out
}

func (hc *h2ClientConn) flushStreamLocked(out []byte, st *h2ClientStream) []byte {
	for len(st.pending) > 0 {
		n := min(int64(len(st.pending)), hc.sendWindow, st.sendWindow, int64(hc.peerMaxFrame))
		if n <= 0 {
			return out
		}
		chunk := st.pending[:n]
		st.pending = st.pending[n:]
		var flags uint8
		last := len(st.pending) == 0
		if last {
			st.pending = nil
		}
		// A body that streams ends once it has been read, not when what of
		// it has been read so far has gone.
		last = last && !st.streaming
		if last {
			if len(st.trailer) == 0 {
				flags = h2FlagEndStream
				st.endLocalLocked()
			}
		}
		out = h2AppendFrameHeader(out, h2FrameData, flags, st.id, len(chunk))
		out = append(out, chunk...)
		hc.sendWindow -= n
		st.sendWindow -= n
		if last && len(st.trailer) > 0 {
			return hc.appendTrailerLocked(out, st)
		}
	}
	return out
}

// appendTrailerLocked ends the request with its trailers.
func (hc *h2ClientConn) appendTrailerLocked(out []byte, st *h2ClientStream) []byte {
	block := hc.enc.Begin(nil)
	for i := 0; i < len(st.trailer); i += 2 {
		block = hc.enc.AppendField(block, st.trailer[i], st.trailer[i+1], false)
	}
	st.trailer = nil
	st.endLocalLocked()
	return h2AppendHeaderBlock(out, st.id, block, true, hc.peerMaxFrame)
}
