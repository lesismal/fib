//go:build linux || darwin || windows

package grpc

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
	"github.com/lesismal/fib/hpack"
	"github.com/lesismal/fib/taskpool"
)

// transport is one HTTP/2 connection carrying gRPC streams, on either side.
// What it reads, it reads in the connection's rounds, one at a time; what it
// sends, it sends from any goroutine under mu, which also keeps the HPACK
// encoder's blocks in the order they go out.
type transport struct {
	conn   *fib.Connection
	client bool
	server *Server
	cc     *ClientConn
	// scheme is what a client's requests carry as :scheme.
	scheme string

	// The reading side, touched only by the connection's rounds.
	buf          []byte
	awaitPreface bool
	sawSettings  bool
	dec          *hpack.Decoder
	block        []byte
	blockID      uint32
	blockEnd     bool
	connUnacked  int64

	mu      sync.Mutex
	enc     *hpack.Encoder
	scratch []byte
	streams map[uint32]*stream
	// nextID is the next stream a client opens; lastPeerID the last stream
	// a server's peer opened.
	nextID     uint32
	lastPeerID uint32
	// sendWindow is the connection's send window; peerInitialWindow and
	// peerMaxFrame, peerMaxStreams are what the peer's SETTINGS say.
	sendWindow        int64
	peerInitialWindow int64
	peerMaxFrame      int
	peerMaxStreams    uint32
	closed            bool
	closeErr          error
	// goingAway is set once a GOAWAY has gone either way: no stream is
	// opened on the connection after it.
	goingAway bool
	// changed is closed, and replaced, whenever a window grows, a stream
	// ends or the connection closes: what a sender waiting for room waits
	// on.
	changed chan struct{}
	// out is what has been framed and not yet handed to the connection,
	// writing whether a goroutine is handing it over; see flush.
	out, spare []byte
	writing    bool

	pool *taskpool.TaskPool
}

func newTransport(client bool) *transport {
	t := &transport{
		client:            client,
		dec:               hpack.NewDecoder(defaultHeaderTable),
		enc:               hpack.NewEncoder(),
		streams:           map[uint32]*stream{},
		sendWindow:        defaultWindow,
		peerInitialWindow: defaultWindow,
		peerMaxFrame:      defaultMaxFrame,
		peerMaxStreams:    ^uint32(0),
		changed:           make(chan struct{}),
		awaitPreface:      !client,
		nextID:            1,
	}
	t.dec.MaxStringLength = maxHeaderBlock
	return t
}

// start sends what opens the connection: a client's preface, and either
// side's SETTINGS and the connection window beyond the default.
func (t *transport) start(maxStreams uint32) {
	t.mu.Lock()
	if t.client {
		t.out = append(t.out, preface...)
		t.out = appendSettings(t.out, settingEnablePush, 0, settingInitialWindowSize, streamWindow)
	} else if maxStreams > 0 {
		t.out = appendSettings(t.out, settingInitialWindowSize, streamWindow, settingMaxConcurrentStreams, maxStreams)
	} else {
		t.out = appendSettings(t.out, settingInitialWindowSize, streamWindow)
	}
	t.out = appendWindowUpdate(t.out, 0, connWindow-defaultWindow)
	t.mu.Unlock()
	t.flush()
}

// broadcastLocked wakes whatever waits on changed. Callers hold mu.
func (t *transport) broadcastLocked() {
	close(t.changed)
	t.changed = make(chan struct{})
}

// maxSpareOut is the largest buffer flush keeps for the next batch.
const maxSpareOut = 256 << 10

// flush hands what has been framed to the connection. While one goroutine
// is handing it over, others only add to it, and that goroutine takes what
// they added too before it stops: frames that streams finish together reach
// the socket in one write.
func (t *transport) flush() {
	t.mu.Lock()
	if t.writing || len(t.out) == 0 {
		t.mu.Unlock()
		return
	}
	t.writing = true
	for len(t.out) > 0 {
		batch := t.out
		t.out = t.spare[:0]
		t.spare = nil
		conn := t.conn
		t.mu.Unlock()
		if conn != nil {
			_ = conn.Send(batch)
		}
		if cap(batch) > maxSpareOut {
			batch = nil
		}
		t.mu.Lock()
		if t.spare == nil {
			t.spare = batch
		}
	}
	t.writing = false
	t.mu.Unlock()
}

// fail ends the connection for err, with a GOAWAY saying why if it is the
// protocol's.
func (t *transport) fail(err error) {
	t.mu.Lock()
	if !t.closed {
		code := errInternal
		var ce *connError
		if errors.As(err, &ce) {
			code = ce.code
		}
		t.out = appendGoAway(t.out, t.lastPeerID, code, "")
	}
	t.mu.Unlock()
	t.flush()
	if t.conn != nil {
		t.conn.CloseAfterSend()
	}
}

// onClose ends every stream of a connection that has closed.
func (t *transport) onClose(err error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	if err == nil {
		err = io.EOF
	}
	t.closeErr = err
	streams := t.streams
	t.streams = map[uint32]*stream{}
	t.broadcastLocked()
	t.mu.Unlock()
	st := status.New(codes.Unavailable, "grpc: connection closed: "+err.Error())
	for _, s := range streams {
		s.abort(st.Err())
	}
}

// feed reads frames from what the connection read.
func (t *transport) feed(data []byte) {
	if err := t.read(data); err != nil {
		t.fail(err)
	}
}

func (t *transport) read(data []byte) error {
	if len(t.buf) > 0 {
		t.buf = append(t.buf, data...)
		data = t.buf
	}
	if t.awaitPreface {
		if len(data) < len(preface) {
			if string(data) != preface[:len(data)] {
				return protocolError("bad client preface")
			}
			t.buf = append(t.buf[:0], data...)
			return nil
		}
		if string(data[:len(preface)]) != preface {
			return protocolError("bad client preface")
		}
		data = data[len(preface):]
		t.awaitPreface = false
	}
	for len(data) >= frameHeaderLen {
		f := parseFrameHeader(data)
		if f.length > defaultMaxFrame {
			return &connError{code: errFrameSize, msg: "frame too large"}
		}
		end := frameHeaderLen + int(f.length)
		if len(data) < end {
			break
		}
		f.payload = data[frameHeaderLen:end]
		data = data[end:]
		if err := t.handleFrame(&f); err != nil {
			var se *streamError
			if !errors.As(err, &se) {
				return err
			}
			t.resetStream(se.id, se.code, status.New(codes.Internal, se.Error()).Err())
		}
	}
	// Keep what is left of an incomplete frame. When data is the tail of
	// buf, the copy moves it to the front.
	t.buf = append(t.buf[:0], data...)
	return nil
}

func (t *transport) handleFrame(f *frame) error {
	if !t.sawSettings {
		if f.typ != frameSettings || f.has(flagAck) {
			return protocolError("first frame is not SETTINGS")
		}
		t.sawSettings = true
	}
	if t.blockID != 0 && (f.typ != frameContinuation || f.streamID != t.blockID) {
		return protocolError("expected CONTINUATION")
	}
	switch f.typ {
	case frameData:
		return t.handleData(f)
	case frameHeaders:
		if f.streamID == 0 {
			return protocolError("HEADERS on stream 0")
		}
		if err := stripPadding(f); err != nil {
			return err
		}
		if f.has(flagPriority) {
			if len(f.payload) < 5 {
				return protocolError("HEADERS priority too short")
			}
			f.payload = f.payload[5:]
		}
		if f.has(flagEndHeaders) {
			return t.handleHeaderBlock(f.streamID, f.payload, f.has(flagEndStream))
		}
		t.block = append(t.block[:0], f.payload...)
		t.blockID, t.blockEnd = f.streamID, f.has(flagEndStream)
		return nil
	case frameContinuation:
		if t.blockID == 0 {
			return protocolError("unexpected CONTINUATION")
		}
		t.block = append(t.block, f.payload...)
		if len(t.block) > maxHeaderBlock {
			return &connError{code: errEnhanceCalm, msg: "header block too large"}
		}
		if !f.has(flagEndHeaders) {
			return nil
		}
		id := t.blockID
		t.blockID = 0
		return t.handleHeaderBlock(id, t.block, t.blockEnd)
	case framePriority:
		if len(f.payload) != 5 {
			return &connError{code: errFrameSize, msg: "PRIORITY size"}
		}
		return nil
	case frameRSTStream:
		if len(f.payload) != 4 || f.streamID == 0 {
			return protocolError("bad RST_STREAM")
		}
		t.onReset(f.streamID, errCode(binary.BigEndian.Uint32(f.payload)))
		return nil
	case frameSettings:
		return t.handleSettings(f)
	case framePushPromise:
		return protocolError("PUSH_PROMISE")
	case framePing:
		if len(f.payload) != 8 || f.streamID != 0 {
			return protocolError("bad PING")
		}
		if !f.has(flagAck) {
			t.mu.Lock()
			t.out = appendPing(t.out, true, f.payload)
			t.mu.Unlock()
			t.flush()
		}
		return nil
	case frameGoAway:
		if len(f.payload) < 8 || f.streamID != 0 {
			return protocolError("bad GOAWAY")
		}
		t.onGoAway(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
		return nil
	case frameWindowUpdate:
		return t.handleWindowUpdate(f)
	}
	return nil
}

func (t *transport) handleSettings(f *frame) error {
	if f.streamID != 0 {
		return protocolError("SETTINGS on a stream")
	}
	if f.has(flagAck) {
		if len(f.payload) != 0 {
			return &connError{code: errFrameSize, msg: "SETTINGS ack with a payload"}
		}
		return nil
	}
	if len(f.payload)%6 != 0 {
		return &connError{code: errFrameSize, msg: "SETTINGS size"}
	}
	t.mu.Lock()
	defer func() {
		t.mu.Unlock()
		t.flush()
	}()
	for p := f.payload; len(p) > 0; p = p[6:] {
		id, v := binary.BigEndian.Uint16(p), binary.BigEndian.Uint32(p[2:])
		switch id {
		case settingHeaderTableSize:
			t.enc.SetMaxTableSize(int(min(v, defaultHeaderTable)))
		case settingInitialWindowSize:
			if v > maxWindow {
				return &connError{code: errFlowControl, msg: "initial window too large"}
			}
			delta := int64(v) - t.peerInitialWindow
			t.peerInitialWindow = int64(v)
			for _, s := range t.streams {
				s.sendWindow += delta
			}
		case settingMaxFrameSize:
			if v < defaultMaxFrame || v > maxFrameSizeLimit {
				return protocolError("bad max frame size")
			}
			t.peerMaxFrame = int(v)
		case settingMaxConcurrentStreams:
			t.peerMaxStreams = v
		}
	}
	t.out = appendFrameHeader(t.out, frameSettings, flagAck, 0, 0)
	t.broadcastLocked()
	return nil
}

func (t *transport) handleWindowUpdate(f *frame) error {
	if len(f.payload) != 4 {
		return &connError{code: errFrameSize, msg: "WINDOW_UPDATE size"}
	}
	inc := int64(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
	if inc == 0 {
		if f.streamID == 0 {
			return protocolError("zero window increment")
		}
		return &streamError{id: f.streamID, code: errProtocol}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if f.streamID == 0 {
		t.sendWindow += inc
		if t.sendWindow > maxWindow {
			return &connError{code: errFlowControl, msg: "window overflow"}
		}
	} else if s := t.streams[f.streamID]; s != nil {
		s.sendWindow += inc
		if s.sendWindow > maxWindow {
			return &streamError{id: f.streamID, code: errFlowControl}
		}
	}
	t.broadcastLocked()
	return nil
}

func (t *transport) handleData(f *frame) error {
	if f.streamID == 0 {
		return protocolError("DATA on stream 0")
	}
	length := int64(len(f.payload))
	t.connUnacked += length
	if t.connUnacked >= connWindow/2 {
		t.mu.Lock()
		t.out = appendWindowUpdate(t.out, 0, uint32(t.connUnacked))
		t.mu.Unlock()
		t.connUnacked = 0
		t.flush()
	}
	if err := stripPadding(f); err != nil {
		return err
	}
	t.mu.Lock()
	s := t.streams[f.streamID]
	idle := t.idleLocked(f.streamID)
	t.mu.Unlock()
	if s == nil {
		if idle {
			return protocolError("DATA on an idle stream")
		}
		// A stream that has ended here; what was in flight is dropped.
		return nil
	}
	return s.onData(f.payload, length, f.has(flagEndStream))
}

// idleLocked reports whether id names a stream not yet opened.
func (t *transport) idleLocked(id uint32) bool {
	if t.client {
		return id >= t.nextID
	}
	return id > t.lastPeerID
}

func (t *transport) handleHeaderBlock(id uint32, block []byte, endStream bool) error {
	var fields []hpack.HeaderField
	size := 0
	err := t.dec.Decode(block, func(hf hpack.HeaderField) error {
		size += len(hf.Name) + len(hf.Value) + 32
		if size > maxHeaderBlock {
			return errHeaderListTooLarge
		}
		fields = append(fields, hf)
		return nil
	})
	if err != nil {
		if errors.Is(err, errHeaderListTooLarge) {
			return &streamError{id: id, code: errEnhanceCalm}
		}
		return &connError{code: errCompression, msg: err.Error()}
	}
	if t.client {
		t.mu.Lock()
		s := t.streams[id]
		t.mu.Unlock()
		if s == nil {
			return nil
		}
		return s.cs.onHeaders(fields, endStream)
	}
	t.mu.Lock()
	s := t.streams[id]
	if s == nil {
		if id%2 == 0 || id <= t.lastPeerID {
			t.mu.Unlock()
			return protocolError("bad stream id %d", id)
		}
		t.lastPeerID = id
	}
	t.mu.Unlock()
	if s != nil {
		// Trailers of the client, which end its side.
		if !endStream {
			return &streamError{id: id, code: errProtocol}
		}
		return s.onData(nil, 0, true)
	}
	t.server.openStream(t, id, fields, endStream)
	return nil
}

var errHeaderListTooLarge = errors.New("grpc: header list too large")

// onReset ends a stream the peer reset.
func (t *transport) onReset(id uint32, code errCode) {
	t.mu.Lock()
	s := t.streams[id]
	if s != nil {
		t.removeLocked(s)
	}
	t.mu.Unlock()
	if s == nil {
		return
	}
	c := codes.Internal
	switch code {
	case errCancel:
		c = codes.Canceled
	case errRefusedStream:
		c = codes.Unavailable
	case errEnhanceCalm:
		c = codes.ResourceExhausted
	case errNo:
		if t.client {
			// The server ended the stream early, after its status.
			c = codes.Internal
		} else {
			c = codes.Canceled
		}
	}
	s.abort(status.Newf(c, "grpc: stream reset by the peer with code %d", code).Err())
}

// resetStream resets a stream from this side.
func (t *transport) resetStream(id uint32, code errCode, cause error) {
	t.mu.Lock()
	s := t.streams[id]
	if s != nil {
		t.removeLocked(s)
	}
	if !t.closed {
		t.out = appendRSTStream(t.out, id, code)
	}
	t.mu.Unlock()
	t.flush()
	if s != nil {
		s.abort(cause)
	}
}

// onGoAway stops new streams on the connection, and fails those the peer
// will not serve.
func (t *transport) onGoAway(lastID uint32) {
	t.mu.Lock()
	t.goingAway = true
	var refused []*stream
	if t.client {
		for id, s := range t.streams {
			if id > lastID {
				refused = append(refused, s)
				t.removeLocked(s)
			}
		}
	}
	idle := len(t.streams) == 0
	t.broadcastLocked()
	t.mu.Unlock()
	for _, s := range refused {
		s.abort(status.New(codes.Unavailable, "grpc: the connection is draining").Err())
	}
	if t.client {
		t.cc.transportDraining(t)
		if idle {
			t.conn.Close()
		}
	}
}

// removeLocked forgets a stream, and closes a draining connection once it
// has no stream left. Callers hold mu.
func (t *transport) removeLocked(s *stream) {
	if t.streams[s.id] != s {
		return
	}
	delete(t.streams, s.id)
	t.broadcastLocked()
	if t.goingAway && len(t.streams) == 0 && t.conn != nil {
		conn := t.conn
		// Closed after what was framed has gone, which flush hands over.
		go func() {
			t.flush()
			conn.CloseAfterSend()
		}()
	}
}

// taskPool is the pool a server's handlers run on.
func (t *transport) taskPool() fib.TaskPool {
	if pool := t.server.opts.pool; pool != nil {
		return pool
	}
	if t.pool != nil {
		return t.pool
	}
	return nil
}

// run runs task on the handler pool, or on a goroutine of its own when there
// is none or it turns the task away. Never where the connection is read: a
// streaming handler waits for messages that only that goroutine delivers.
func (t *transport) run(task taskpool.Task) {
	if pool := t.taskPool(); pool != nil && pool.GoTask(task) {
		return
	}
	go task.RunTask()
}

// fib.Handler of a client's connection.

func (t *transport) OnOpen(conn *fib.Connection) {
	t.conn = conn
	conn.SetAttachment(t)
	t.start(0)
}

func (t *transport) OnData(conn *fib.Connection, data []byte) { t.feed(data) }

func (t *transport) OnPriorityData(*fib.Connection, []byte) {}

func (t *transport) OnClose(conn *fib.Connection, err error) {
	t.onClose(err)
	t.cc.transportClosed(t)
}
