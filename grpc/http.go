//go:build linux || darwin || windows

package grpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/internal/ctxkeys"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/grpc/peer"
	"github.com/lesismal/fib/grpc/status"
	fibhttp "github.com/lesismal/fib/http"
)

// ServeHTTP serves a gRPC call that arrived through package http, as
// grpc-go's Server.ServeHTTP does one that arrived through net/http, so that
// one port, and one TLS listener offering h2, serves gRPC beside the rest of
// a program's HTTP: route the service's paths to the Server.
//
//	r := fibhttp.NewRouter()
//	r.Handle("/helloworld.Greeter/*", server)
//	r.Get("/", index)
//	engine, err := fib.Bind(config, fibhttp.NewHandler(r))
//
// The calls are served by the Server's methods, codecs, compressors and
// interceptors, as on its own connections; its MaxConcurrentStreams and
// TaskPool options apply only there, package http's Config bounding the
// streams here, whose handlers run on the engine's handler pool.
//
// Each message a call sends is flushed, so it reaches the client as it is
// sent, under the stream's flow control: a call that sends faster than its
// client reads waits for it, as on the Server's own connections. With package
// http's defaults a handler runs once its request has arrived whole, which
// serves unary and server-streaming calls; a call whose client streams, and
// above all a bidirectional one whose client waits for replies before it has
// sent everything, takes Config.StreamRequestBody, with which the call reads
// its messages as they arrive.
//
// A request that is not a gRPC call is answered as grpc-go answers it: one
// over HTTP/1 400, one not a POST 405 and one whose Content-Type is not
// gRPC's 415.
func (s *Server) ServeHTTP(c *fibhttp.Context, r *stdhttp.Request) {
	switch {
	case r.ProtoMajor != 2:
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte("gRPC requires HTTP/2"))
		return
	case r.Method != stdhttp.MethodPost:
		_ = c.Respond(stdhttp.StatusMethodNotAllowed, "text/plain", []byte("invalid gRPC request method"))
		return
	}
	subtype, ok := contentSubtype(r.Header.Get("Content-Type"))
	if !ok {
		_ = c.Respond(stdhttp.StatusUnsupportedMediaType, "text/plain", []byte("invalid gRPC request content-type"))
		return
	}
	hs := &httpStream{server: s, c: c, r: r, conn: c.Conn, method: r.URL.Path, codec: s.opts.codec,
		whole: c.RequestBody() == nil, body: c.Body()}
	if hs.codec == nil {
		hs.codec = GetCodec(subtype)
	}
	var fail *status.Status
	for key := range r.Header {
		if len(key) > 4 && strings.EqualFold(key[len(key)-4:], "-bin") {
			// Binary metadata is decoded now, so that a malformed value
			// fails the call rather than vanishing from its metadata.
			if _, err := hs.incoming(); err != nil {
				fail = status.Newf(codes.Internal, "grpc: malformed binary metadata: %v", err)
			}
			break
		}
	}
	// The call's context is hs itself, which answers for its metadata, its
	// peer and the call, and is cancelled with the request; a deadline the
	// client set goes on top of it.
	hs.ctx = hs
	cancel := context.CancelFunc(func() {})
	if timeout, ok := decodeTimeout(r.Header.Get("Grpc-Timeout")); ok {
		hs.ctx, cancel = context.WithTimeout(hs, timeout)
	}
	if !hs.whole {
		hs.in = newBodyPipe()
	}
	// A client that resets the stream, or a connection that goes, ends the
	// call's context, and a read waiting on the rest of the request.
	c.OnCancel(hs.cancel)
	if hs.whole {
		hs.serve(fail, subtype)
		cancel()
		return
	}
	// The rest of the request arrives through OnBody once this returns, so
	// the call is served on a goroutine of its own, holding the request
	// until it has answered.
	c.OnBody(hs.in.write)
	c.Retain()
	go func() {
		defer c.Release()
		defer cancel()
		hs.serve(fail, subtype)
	}()
}

// serve serves the call, unless fail already says how it ends.
func (hs *httpStream) serve(fail *status.Status, subtype string) {
	s, r := hs.server, hs.r
	var svc *service
	var unary *MethodDesc
	var sd *StreamDesc
	switch encoding := r.Header.Get("Grpc-Encoding"); {
	case fail != nil:
	case hs.codec == nil:
		fail = status.Newf(codes.Internal, "grpc: no codec registered for content-subtype %s", subtype)
	case encoding != "" && encoding != "identity":
		if hs.recvComp = GetCompressor(encoding); hs.recvComp == nil {
			fail = status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", encoding)
		}
		// Responses are compressed the way the request was.
		hs.sendComp = hs.recvComp
	}
	if fail == nil {
		svc, unary, sd, fail = s.lookup(hs.method)
	}
	if fail == nil {
		hs.unary = unary != nil
		fail = status.Convert(errOrContext(s.invoke(hs, hs.method, svc, unary, sd), hs.ctx))
	}
	hs.finish(fail)
}

// httpStream is the server's side of a call that arrived through package
// http, and the call's context. Like grpc-go's, its stream methods are for
// the handler's goroutine, one at a time; its context methods are for any.
type httpStream struct {
	server *Server
	c      *fibhttp.Context
	r      *stdhttp.Request
	// ctx is the context the handler is given: the stream itself, or a
	// context with the client's deadline over it.
	ctx    context.Context
	method string
	codec  Codec
	// recvComp and sendComp are the compressors of what arrives and of what
	// is sent.
	recvComp Compressor
	sendComp Compressor
	// unary records that the call is unary, whose one reply is held until
	// its status, so that the response goes out at once.
	unary bool
	// whole records that the request arrived whole before the handler ran,
	// and body is what of it RecvMsg has yet to slice messages from. A body
	// still arriving RecvMsg reads from in instead, which OnBody fills.
	whole bool
	body  []byte
	in    *bodyPipe
	// reply is a unary call's reply, framed.
	reply   []byte
	replied bool

	header     metadata.MD
	trailer    metadata.MD
	headerSent bool
	done       bool

	// The context's state, guarded by mu: why it was cancelled, the channel
	// Done made, if one asked; and the call's metadata and peer, made when
	// first asked for from the request and the connection, which conn and
	// ended let go of once the call has ended, after which neither is the
	// call's any more.
	mu       sync.Mutex
	err      error
	doneCh   chan struct{}
	conn     *fib.Connection
	md       metadata.MD
	mdErr    error
	mdMade   bool
	peerAddr *peer.Peer
	ended    bool
}

func (hs *httpStream) Context() context.Context { return hs.ctx }

// Deadline is none: a deadline the client set is on the context the handler
// is given, over this one.
func (hs *httpStream) Deadline() (time.Time, bool) { return time.Time{}, false }

func (hs *httpStream) Done() <-chan struct{} {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.doneCh == nil {
		hs.doneCh = make(chan struct{})
		if hs.err != nil {
			close(hs.doneCh)
		}
	}
	return hs.doneCh
}

func (hs *httpStream) Err() error {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return hs.err
}

func (hs *httpStream) Value(key any) any {
	switch key.(type) {
	case streamKey:
		return hs
	case ctxkeys.Incoming:
		if md, err := hs.incoming(); err == nil {
			return md
		}
		return metadata.MD{}
	case ctxkeys.Peer:
		return hs.peer()
	}
	return nil
}

// cancel is OnCancel's callback: the request ended before the call did.
func (hs *httpStream) cancel(err error) {
	hs.mu.Lock()
	if hs.err == nil {
		hs.err = context.Canceled
		if hs.doneCh != nil {
			close(hs.doneCh)
		}
	}
	hs.mu.Unlock()
	if hs.in != nil {
		hs.in.close(err)
	}
}

// incoming is the metadata that came with the call, made from the request's
// header the first time it is asked for while the call lasts.
func (hs *httpStream) incoming() (metadata.MD, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if !hs.mdMade && !hs.ended {
		hs.mdMade = true
		hs.md = metadata.MD{}
		for key, values := range hs.r.Header {
			name := lowerKey(key)
			if reservedHeader(name) {
				continue
			}
			for _, value := range values {
				if err := addMetadata(hs.md, name, value); err != nil && hs.mdErr == nil {
					hs.mdErr = fmt.Errorf("%q: %w", name, err)
				}
			}
		}
	}
	if hs.md == nil {
		return metadata.MD{}, hs.mdErr
	}
	return hs.md, hs.mdErr
}

// peer is the other side of the call, found the first time it is asked for
// while the call lasts.
func (hs *httpStream) peer() *peer.Peer {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.peerAddr == nil && hs.conn != nil {
		hs.peerAddr = &peer.Peer{Addr: hs.conn.RemoteAddr(), LocalAddr: hs.conn.LocalAddr()}
	}
	return hs.peerAddr
}

// end lets go of what the call's metadata and peer are made from, which are
// recycled once the response is finished; what was made of them stays.
func (hs *httpStream) end() {
	hs.mu.Lock()
	hs.ended = true
	hs.conn = nil
	hs.mu.Unlock()
}

// lowerKey is a canonical header key in lower case, as metadata keys are,
// without an allocation for the names calls nearly always carry.
func lowerKey(key string) string {
	switch key {
	case "User-Agent":
		return "user-agent"
	case "Te":
		return "te"
	case "Content-Type":
		return "content-type"
	case "Grpc-Timeout":
		return "grpc-timeout"
	case "Grpc-Encoding":
		return "grpc-encoding"
	case "Grpc-Accept-Encoding":
		return "grpc-accept-encoding"
	}
	return strings.ToLower(key)
}

func (hs *httpStream) fullMethod() string { return hs.method }

func (hs *httpStream) SetHeader(md metadata.MD) error {
	if hs.headerSent || hs.done {
		return status.Error(codes.Internal, "grpc: SetHeader called after the header was sent")
	}
	if hs.header == nil {
		hs.header = metadata.MD{}
	}
	for k, v := range md {
		hs.header[strings.ToLower(k)] = append(hs.header[strings.ToLower(k)], v...)
	}
	return nil
}

func (hs *httpStream) SendHeader(md metadata.MD) error {
	if err := hs.SetHeader(md); err != nil {
		return err
	}
	hs.sendHeader()
	return hs.flush()
}

func (hs *httpStream) SetTrailer(md metadata.MD) {
	if hs.trailer == nil {
		hs.trailer = metadata.MD{}
	}
	for k, v := range md {
		hs.trailer[strings.ToLower(k)] = append(hs.trailer[strings.ToLower(k)], v...)
	}
}

// responseHeader sets the fields of the response's header on h.
func (hs *httpStream) responseHeader(h stdhttp.Header) {
	h["Content-Type"] = []string{contentType(hs.codec.Name())}
	if hs.sendComp != nil {
		h["Grpc-Encoding"] = []string{hs.sendComp.Name()}
	}
	setMetadata(h, "", hs.header)
}

// sendHeader begins the response, which then goes out as the call sends it.
func (hs *httpStream) sendHeader() {
	if hs.headerSent || hs.done {
		return
	}
	hs.headerSent = true
	hs.responseHeader(hs.c.Header())
	hs.c.WriteHeader(stdhttp.StatusOK)
	if hs.reply != nil {
		// A unary call that sent its header after its reply: the reply goes
		// out behind it.
		_, _ = hs.c.Write(hs.reply)
		hs.reply = nil
	}
}

func (hs *httpStream) flush() error {
	if err := hs.c.FlushError(); err != nil {
		return status.Errorf(codes.Unavailable, "grpc: %v", err)
	}
	return nil
}

func (hs *httpStream) SendMsg(m any) error {
	if hs.done {
		return status.Error(codes.Internal, "grpc: the stream is done")
	}
	data, err := hs.codec.Marshal(m)
	if err != nil {
		return status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
	}
	compressed := false
	if hs.sendComp != nil {
		if data, err = compress(hs.sendComp, data); err != nil {
			return status.Errorf(codes.Internal, "grpc: error while compressing: %v", err)
		}
		compressed = true
	}
	if len(data) > hs.server.opts.maxSendMsgSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(data), hs.server.opts.maxSendMsgSize)
	}
	frame := make([]byte, 5, 5+len(data))
	if compressed {
		frame[0] = 1
	}
	binary.BigEndian.PutUint32(frame[1:], uint32(len(data)))
	frame = append(frame, data...)
	if hs.unary && !hs.headerSent && !hs.replied {
		hs.reply, hs.replied = frame, true
		return nil
	}
	hs.sendHeader()
	if _, err := hs.c.Write(frame); err != nil {
		return status.Errorf(codes.Unavailable, "grpc: %v", err)
	}
	return hs.flush()
}

func (hs *httpStream) RecvMsg(m any) error {
	compressed, data, err := hs.readMsg()
	if err != nil {
		return err
	}
	if compressed {
		if hs.recvComp == nil {
			return status.Error(codes.Internal, "grpc: compressed message without grpc-encoding")
		}
		if data, err = decompress(hs.recvComp, data, hs.server.opts.maxRecvMsgSize); err != nil {
			if err == errMessageTooLarge {
				return status.Errorf(codes.ResourceExhausted, "grpc: received message after decompression larger than max %d", hs.server.opts.maxRecvMsgSize)
			}
			return status.Errorf(codes.Internal, "grpc: failed to decompress the received message: %v", err)
		}
	}
	if err := hs.codec.Unmarshal(data, m); err != nil {
		return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err)
	}
	return nil
}

// readMsg returns the next message of the request, or io.EOF once the
// client has sent all of them.
func (hs *httpStream) readMsg() (bool, []byte, error) {
	limit := hs.server.opts.maxRecvMsgSize
	if hs.whole {
		if len(hs.body) == 0 {
			return false, nil, io.EOF
		}
		if len(hs.body) < 5 {
			return false, nil, status.Error(codes.Internal, "grpc: truncated message header")
		}
		n := binary.BigEndian.Uint32(hs.body[1:5])
		if int64(n) > int64(limit) {
			return false, nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", n, limit)
		}
		if int64(len(hs.body)-5) < int64(n) {
			return false, nil, status.Error(codes.Internal, "grpc: truncated message")
		}
		compressed, data := hs.body[0] == 1, hs.body[5:5+n]
		hs.body = hs.body[5+n:]
		return compressed, data, nil
	}
	var head [5]byte
	if _, err := io.ReadFull(hs.in, head[:]); err != nil {
		if err == io.EOF {
			return false, nil, io.EOF
		}
		return false, nil, hs.readError(err)
	}
	n := binary.BigEndian.Uint32(head[1:])
	if int64(n) > int64(limit) {
		return false, nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", n, limit)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(hs.in, data); err != nil {
		return false, nil, hs.readError(err)
	}
	return head[0] == 1, data, nil
}

func (hs *httpStream) readError(err error) error {
	if err == io.ErrUnexpectedEOF {
		return status.Error(codes.Internal, "grpc: truncated message")
	}
	if hs.ctx.Err() != nil {
		return status.FromContextError(hs.ctx.Err()).Err()
	}
	return status.Errorf(codes.Unavailable, "grpc: %v", err)
}

// finish ends the call with st: its status and trailer metadata go out as
// the response's trailer, or, as the whole response, with what is held of a
// unary call's, or in its header when nothing else has been sent.
func (hs *httpStream) finish(st *status.Status) {
	if hs.done {
		return
	}
	hs.done = true
	hs.end()
	if hs.in != nil {
		// What more the client sends is not wanted.
		hs.in.close(io.ErrClosedPipe)
	}
	if hs.headerSent {
		prefixed := hs.c.Header()
		setStatus(prefixed, stdhttp.TrailerPrefix, st)
		setMetadata(prefixed, stdhttp.TrailerPrefix, hs.trailer)
		_ = hs.c.Finish()
		return
	}
	var header stdhttp.Header
	if hs.reply == nil || st.Code() != codes.OK || hs.sendComp != nil || len(hs.header) > 0 {
		header = stdhttp.Header{}
		hs.responseHeader(header)
	} else {
		header = codecHeader(hs.codec.Name())
	}
	if hs.reply == nil || st.Code() != codes.OK {
		// Trailers-only: one HEADERS is the whole response.
		setStatus(header, "", st)
		setMetadata(header, "", hs.trailer)
		_ = hs.c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header})
		return
	}
	trailer := okTrailer
	if len(hs.trailer) > 0 {
		trailer = stdhttp.Header{}
		setStatus(trailer, "", st)
		setMetadata(trailer, "", hs.trailer)
	}
	_ = hs.c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: header, Body: hs.reply, Trailer: trailer})
}

// okTrailer is the trailer of nearly every unary call's response, and
// codecHeaders hold the header of one for each codec, which package http only
// reads, so that one of each serves them all.
var (
	okTrailer    = stdhttp.Header{"Grpc-Status": {"0"}}
	codecHeaders sync.Map
)

// codecHeader is the header of a response whose messages codec name encodes,
// carrying nothing else.
func codecHeader(name string) stdhttp.Header {
	if h, ok := codecHeaders.Load(name); ok {
		return h.(stdhttp.Header)
	}
	h, _ := codecHeaders.LoadOrStore(name, stdhttp.Header{"Content-Type": {contentType(name)}})
	return h.(stdhttp.Header)
}

// setStatus sets st's fields on h, each name behind prefix.
func setStatus(h stdhttp.Header, prefix string, st *status.Status) {
	h[prefix+"Grpc-Status"] = []string{strconv.Itoa(int(st.Code()))}
	if msg := st.Message(); msg != "" {
		h[prefix+"Grpc-Message"] = []string{encodeGRPCMessage(msg)}
	}
	if details := st.Details(); len(details) > 0 {
		h[prefix+"Grpc-Status-Details-Bin"] = []string{encodeBin(details)}
	}
}

// setMetadata sets md on h, each name behind prefix and binary values in
// base64, leaving out what is reserved.
func setMetadata(h stdhttp.Header, prefix string, md metadata.MD) {
	for k, vs := range md {
		k = strings.ToLower(k)
		if reservedHeader(k) || !validHeaderName(k) {
			continue
		}
		key := prefix + stdhttp.CanonicalHeaderKey(k)
		bin := strings.HasSuffix(k, "-bin")
		for _, v := range vs {
			if bin {
				v = encodeBin([]byte(v))
			}
			h[key] = append(h[key], v)
		}
	}
}

// bodyPipe carries a request's body from OnBody's callback to RecvMsg on the
// call's goroutine. The callback waits while bodyPipeLimit bytes are unread,
// which holds the stream's flow-control window, and so the client, back.
type bodyPipe struct {
	mu   sync.Mutex
	cond sync.Cond
	// buf holds what has arrived, read what of it the reader has taken.
	buf  []byte
	read int
	// err is what a read meets once buf is drained: io.EOF after the whole
	// body, or why the rest will not come. closed is set once the reader is
	// gone, after which what arrives is dropped.
	err    error
	closed bool
}

const bodyPipeLimit = 1 << 20

func newBodyPipe() *bodyPipe {
	p := &bodyPipe{}
	p.cond.L = &p.mu
	return p
}

// write is OnBody's callback.
func (p *bodyPipe) write(data []byte, fin bool, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if p.read > 0 && p.read >= len(p.buf)/2 {
		p.buf = p.buf[:copy(p.buf, p.buf[p.read:])]
		p.read = 0
	}
	p.buf = append(p.buf, data...)
	switch {
	case err != nil:
		p.err = err
	case fin:
		p.err = io.EOF
	}
	p.cond.Broadcast()
	for len(p.buf)-p.read >= bodyPipeLimit && !p.closed {
		p.cond.Wait()
	}
}

func (p *bodyPipe) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.read == len(p.buf) && p.err == nil && !p.closed {
		p.cond.Wait()
	}
	if p.read == len(p.buf) {
		if p.err == nil {
			return 0, io.ErrClosedPipe
		}
		return 0, p.err
	}
	n := copy(b, p.buf[p.read:])
	p.read += n
	if p.read == len(p.buf) {
		p.buf, p.read = p.buf[:0], 0
	}
	p.cond.Broadcast()
	return n, nil
}

// close ends the pipe for the reader, with err for a read that finds it
// drained, and lets a waiting callback go.
func (p *bodyPipe) close(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
}
