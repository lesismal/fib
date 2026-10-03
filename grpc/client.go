//go:build linux || darwin || windows

package grpc

import (
	"context"
	stdtls "crypto/tls"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/grpc/peer"
	"github.com/lesismal/fib/grpc/status"
	"github.com/lesismal/fib/hpack"
	fibtls "github.com/lesismal/fib/tls"
)

type dialOptions struct {
	engine         *fib.Engine
	tlsConfig      *stdtls.Config
	authority      string
	userAgent      string
	connectTimeout time.Duration
	unaryInts      []UnaryClientInterceptor
	streamInts     []StreamClientInterceptor
	unaryInt       UnaryClientInterceptor
	streamInt      StreamClientInterceptor
	callOpts       []CallOption
}

// DialOption configures a ClientConn.
type DialOption func(*dialOptions)

// WithEngine sets the engine the ClientConn's connections run on. Without
// it, they run on an engine of the package's own, made the first time one is
// needed and running for as long as the program does.
func WithEngine(engine *fib.Engine) DialOption { return func(o *dialOptions) { o.engine = engine } }

// WithTLSConfig makes the ClientConn speak TLS, with ALPN offering h2.
// Without it, the ClientConn speaks HTTP/2 in the clear (h2c).
func WithTLSConfig(config *stdtls.Config) DialOption {
	return func(o *dialOptions) { o.tlsConfig = config }
}

// WithAuthority sets :authority, the target by default.
func WithAuthority(authority string) DialOption {
	return func(o *dialOptions) { o.authority = authority }
}

// WithUserAgent puts ua in front of the user-agent the calls send.
func WithUserAgent(ua string) DialOption { return func(o *dialOptions) { o.userAgent = ua } }

// WithConnectTimeout bounds each connect, 20 seconds by default.
func WithConnectTimeout(d time.Duration) DialOption {
	return func(o *dialOptions) { o.connectTimeout = d }
}

// WithUnaryInterceptor sets the interceptor of unary calls; it comes before
// those WithChainUnaryInterceptor adds.
func WithUnaryInterceptor(i UnaryClientInterceptor) DialOption {
	return func(o *dialOptions) { o.unaryInts = append([]UnaryClientInterceptor{i}, o.unaryInts...) }
}

// WithChainUnaryInterceptor adds interceptors of unary calls, the first
// outermost.
func WithChainUnaryInterceptor(interceptors ...UnaryClientInterceptor) DialOption {
	return func(o *dialOptions) { o.unaryInts = append(o.unaryInts, interceptors...) }
}

// WithStreamInterceptor sets the interceptor of streaming calls; it comes
// before those WithChainStreamInterceptor adds.
func WithStreamInterceptor(i StreamClientInterceptor) DialOption {
	return func(o *dialOptions) { o.streamInts = append([]StreamClientInterceptor{i}, o.streamInts...) }
}

// WithChainStreamInterceptor adds interceptors of streaming calls, the first
// outermost.
func WithChainStreamInterceptor(interceptors ...StreamClientInterceptor) DialOption {
	return func(o *dialOptions) { o.streamInts = append(o.streamInts, interceptors...) }
}

// WithDefaultCallOptions sets CallOptions every call takes, before its own.
func WithDefaultCallOptions(opts ...CallOption) DialOption {
	return func(o *dialOptions) { o.callOpts = append(o.callOpts, opts...) }
}

var (
	defaultEngineOnce sync.Once
	defaultEngine     *fib.Engine
	defaultEngineErr  error
)

func sharedEngine() (*fib.Engine, error) {
	defaultEngineOnce.Do(func() {
		config := fib.DefaultConfig()
		config.Name = "fib-grpc-client"
		defaultEngine, defaultEngineErr = fib.NewEngine(config, nil)
		if defaultEngineErr == nil {
			go func() { _ = defaultEngine.Run() }()
		}
	})
	return defaultEngine, defaultEngineErr
}

// ClientConn is a client of one server: a connection made when a call first
// needs one, and made again after it is lost. Its calls share the connection,
// each a stream of its own. It is safe for concurrent use.
type ClientConn struct {
	target string
	addr   string
	opts   dialOptions

	mu      sync.Mutex
	t       *transport
	dialing chan struct{}
	dialErr error
	closed  bool
}

// NewClient returns a ClientConn for target, "host:port", optionally after
// "dns:///" or "passthrough:///". It connects when the first call needs it.
func NewClient(target string, opts ...DialOption) (*ClientConn, error) {
	o := dialOptions{connectTimeout: 20 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	o.unaryInt = chainUnaryClient(o.unaryInts)
	o.streamInt = chainStreamClient(o.streamInts)
	addr := target
	for _, scheme := range []string{"dns:///", "passthrough:///", "dns:", "passthrough:"} {
		addr = strings.TrimPrefix(addr, scheme)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, err
	}
	if o.engine == nil {
		engine, err := sharedEngine()
		if err != nil {
			return nil, err
		}
		o.engine = engine
	}
	if o.authority == "" {
		o.authority = addr
	}
	ua := "grpc-go-fib/1.0"
	if o.userAgent != "" {
		ua = o.userAgent + " " + ua
	}
	o.userAgent = ua
	return &ClientConn{target: target, addr: addr, opts: o}, nil
}

// Target returns the target the ClientConn was made for.
func (cc *ClientConn) Target() string { return cc.target }

// Connect connects now, rather than when the first call needs it, and waits
// until it has or ctx ends.
func (cc *ClientConn) Connect(ctx context.Context) error {
	_, err := cc.transport(ctx, false)
	return err
}

// Close closes the connection, failing the calls in progress with
// codes.Canceled; the ClientConn makes no more.
func (cc *ClientConn) Close() error {
	cc.mu.Lock()
	if cc.closed {
		cc.mu.Unlock()
		return nil
	}
	cc.closed = true
	t := cc.t
	cc.t = nil
	cc.mu.Unlock()
	if t != nil {
		t.mu.Lock()
		streams := make([]*stream, 0, len(t.streams))
		for _, s := range t.streams {
			streams = append(streams, s)
		}
		t.mu.Unlock()
		for _, s := range streams {
			s.abort(errConnClosing)
		}
		t.conn.Close()
	}
	return nil
}

var errConnClosing = status.Error(codes.Canceled, "grpc: the client connection is closing")

// transport returns a connection to make a call on, connecting if there is
// none. With wait, a failed connect is tried again until ctx ends.
func (cc *ClientConn) transport(ctx context.Context, wait bool) (*transport, error) {
	backoff := 100 * time.Millisecond
	for {
		cc.mu.Lock()
		if cc.closed {
			cc.mu.Unlock()
			return nil, errConnClosing
		}
		if t := cc.t; t != nil {
			t.mu.Lock()
			usable := !t.closed && !t.goingAway
			t.mu.Unlock()
			if usable {
				cc.mu.Unlock()
				return t, nil
			}
			cc.t = nil
		}
		if cc.dialing == nil {
			cc.dialing = make(chan struct{})
			cc.dial()
		}
		dialing := cc.dialing
		cc.mu.Unlock()
		select {
		case <-dialing:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		cc.mu.Lock()
		t, err := cc.t, cc.dialErr
		cc.mu.Unlock()
		if t != nil {
			continue
		}
		if !wait {
			return nil, status.Errorf(codes.Unavailable, "grpc: connection error: %v", err)
		}
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, 5*time.Second)
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

// dial starts a connect. Callers hold mu and have set dialing.
func (cc *ClientConn) dial() {
	t := newTransport(true)
	t.cc = cc
	t.scheme = "http"
	done := func(_ *fib.Connection, err error) {
		cc.mu.Lock()
		cc.dialErr = err
		if err == nil {
			if cc.closed {
				t.conn.Close()
			} else {
				cc.t = t
			}
		}
		close(cc.dialing)
		cc.dialing = nil
		cc.mu.Unlock()
	}
	var err error
	if cc.opts.tlsConfig != nil {
		t.scheme = "https"
		config := cc.opts.tlsConfig.Clone()
		config.NextProtos = []string{"h2"}
		err = fibtls.Dial(cc.opts.engine, "tcp", cc.addr, cc.opts.connectTimeout, config, t, done)
	} else {
		err = cc.opts.engine.DialWithHandler("tcp", cc.addr, cc.opts.connectTimeout, t, done)
	}
	if err != nil {
		cc.dialErr = err
		close(cc.dialing)
		cc.dialing = nil
	}
}

// transportClosed forgets a connection that has closed.
func (cc *ClientConn) transportClosed(t *transport) {
	cc.mu.Lock()
	if cc.t == t {
		cc.t = nil
	}
	cc.mu.Unlock()
}

// transportDraining forgets a connection the server sent GOAWAY on, which the
// calls already on it go on using.
func (cc *ClientConn) transportDraining(t *transport) { cc.transportClosed(t) }

// CallOption configures one call.
type CallOption interface {
	apply(*callInfo)
}

type callOption func(*callInfo)

func (f callOption) apply(ci *callInfo) { f(ci) }

type callInfo struct {
	header       *metadata.MD
	trailer      *metadata.MD
	peer         *peer.Peer
	maxRecv      int
	maxSend      int
	compressor   string
	subtype      string
	codec        Codec
	waitForReady bool
}

// Header stores the server's header metadata in md once the call has it.
func Header(md *metadata.MD) CallOption { return callOption(func(ci *callInfo) { ci.header = md }) }

// Trailer stores the server's trailer metadata in md once the call ends.
func Trailer(md *metadata.MD) CallOption { return callOption(func(ci *callInfo) { ci.trailer = md }) }

// Peer stores the server's address in p.
func Peer(p *peer.Peer) CallOption { return callOption(func(ci *callInfo) { ci.peer = p }) }

// MaxCallRecvMsgSize bounds a message the call receives, 4MB by default.
func MaxCallRecvMsgSize(n int) CallOption { return callOption(func(ci *callInfo) { ci.maxRecv = n }) }

// MaxCallSendMsgSize bounds a message the call sends.
func MaxCallSendMsgSize(n int) CallOption { return callOption(func(ci *callInfo) { ci.maxSend = n }) }

// UseCompressor compresses the call's messages with the compressor
// registered as name.
func UseCompressor(name string) CallOption {
	return callOption(func(ci *callInfo) { ci.compressor = name })
}

// CallContentSubtype names the codec of the call, which travels as the
// content subtype: "json" makes it application/grpc+json.
func CallContentSubtype(subtype string) CallOption {
	return callOption(func(ci *callInfo) { ci.subtype = strings.ToLower(subtype) })
}

// ForceCodec encodes the call's messages with codec, sending its Name as the
// content subtype.
func ForceCodec(codec Codec) CallOption { return callOption(func(ci *callInfo) { ci.codec = codec }) }

// WaitForReady makes a call wait for a connection, trying again after a
// failed connect, for as long as its context lets it, rather than failing
// with codes.Unavailable.
func WaitForReady(wait bool) CallOption {
	return callOption(func(ci *callInfo) { ci.waitForReady = wait })
}

// StaticMethod marks a call of a method generated code knows of; it changes
// nothing here.
func StaticMethod() CallOption { return callOption(func(*callInfo) {}) }

var unaryStreamDesc = &StreamDesc{}

// Invoke makes a unary call of method, sending args and receiving reply.
func (cc *ClientConn) Invoke(ctx context.Context, method string, args, reply any, opts ...CallOption) error {
	if cc.opts.unaryInt != nil {
		return cc.opts.unaryInt(ctx, method, args, reply, cc, invoke, opts...)
	}
	return invoke(ctx, method, args, reply, cc, opts...)
}

func invoke(ctx context.Context, method string, args, reply any, cc *ClientConn, opts ...CallOption) error {
	cs, err := cc.newStream(ctx, unaryStreamDesc, method, opts...)
	if err != nil {
		return err
	}
	if err := cs.SendMsg(args); err != nil && err != io.EOF {
		return err
	}
	if err := cs.CloseSend(); err != nil {
		return err
	}
	return cs.RecvMsg(reply)
}

// NewStream opens a streaming call of method.
func (cc *ClientConn) NewStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (ClientStream, error) {
	if cc.opts.streamInt != nil {
		return cc.opts.streamInt(ctx, desc, cc, method, newClientStream, opts...)
	}
	return newClientStream(ctx, desc, cc, method, opts...)
}

func newClientStream(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, opts ...CallOption) (ClientStream, error) {
	return cc.newStream(ctx, desc, method, opts...)
}

func (cc *ClientConn) newStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (*clientStream, error) {
	ci := callInfo{maxRecv: defaultMaxRecvMsgSize, maxSend: defaultMaxSendMsgSize}
	for _, opt := range cc.opts.callOpts {
		opt.apply(&ci)
	}
	for _, opt := range opts {
		opt.apply(&ci)
	}
	codec := ci.codec
	if codec == nil {
		name := ci.subtype
		if name == "" {
			name = "proto"
		}
		if codec = GetCodec(name); codec == nil {
			return nil, status.Errorf(codes.Internal, "grpc: no codec registered for content-subtype %s", name)
		}
	}
	cs := &clientStream{cc: cc, desc: desc, ctx: ctx, ci: ci, codec: codec, headerDone: make(chan struct{})}
	if ci.compressor != "" && ci.compressor != "identity" {
		if cs.sendComp = GetCompressor(ci.compressor); cs.sendComp == nil {
			return nil, status.Errorf(codes.Internal, "grpc: Compressor is not installed for requested grpc-encoding %q", ci.compressor)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	for {
		t, err := cc.transport(ctx, ci.waitForReady)
		if err != nil {
			return nil, err
		}
		retry, err := cs.open(t, method, md)
		if err == nil {
			break
		}
		if !retry {
			return nil, err
		}
	}
	if ci.peer != nil {
		ci.peer.Addr = cs.s.t.conn.RemoteAddr()
		ci.peer.LocalAddr = cs.s.t.conn.LocalAddr()
	}
	cs.stopCancel = context.AfterFunc(ctx, func() {
		cs.cancel(status.FromContextError(ctx.Err()).Err())
	})
	return cs, nil
}

// clientStream is the client's side of a call.
type clientStream struct {
	cc         *ClientConn
	desc       *StreamDesc
	ctx        context.Context
	ci         callInfo
	codec      Codec
	sendComp   Compressor
	recvComp   Compressor
	s          *stream
	stopCancel func() bool

	// headerDone is closed once the header has arrived or the call has
	// ended; done once the call has ended, with st its status.
	headerDone chan struct{}
	headerOnce sync.Once
	header     metadata.MD
	gotHeader  bool
	trailer    metadata.MD
	st         *status.Status
	endOnce    sync.Once
	// received counts the messages of a call whose server does not stream.
	received int
}

// open opens the call's stream on t. retry reports a connection that turned
// out to be closing, on which another try may succeed.
func (cs *clientStream) open(t *transport, method string, md metadata.MD) (retry bool, err error) {
	s := &stream{t: t, ctx: cs.ctx, cs: cs, recvWindow: streamWindow, ready: make(chan struct{}, 1)}
	cs.s = s
	o := &cs.cc.opts
	t.mu.Lock()
	for !t.closed && !t.goingAway && uint32(len(t.streams)) >= t.peerMaxStreams {
		changed := t.changed
		t.mu.Unlock()
		select {
		case <-changed:
		case <-cs.ctx.Done():
			return false, status.FromContextError(cs.ctx.Err()).Err()
		}
		t.mu.Lock()
	}
	if t.closed || t.goingAway || t.nextID > maxWindow {
		t.goingAway = true
		t.mu.Unlock()
		cs.cc.transportDraining(t)
		return true, status.Error(codes.Unavailable, "grpc: the connection is closing")
	}
	s.id = t.nextID
	t.nextID += 2
	s.sendWindow = t.peerInitialWindow
	t.streams[s.id] = s
	block := t.enc.Begin(t.scratch[:0])
	block = t.enc.AppendField(block, ":method", "POST", false)
	block = t.enc.AppendField(block, ":scheme", t.scheme, false)
	block = t.enc.AppendField(block, ":path", method, false)
	block = t.enc.AppendField(block, ":authority", o.authority, false)
	block = t.enc.AppendField(block, "content-type", contentType(cs.codec.Name()), false)
	block = t.enc.AppendField(block, "te", "trailers", false)
	block = t.enc.AppendField(block, "user-agent", o.userAgent, false)
	if deadline, ok := cs.ctx.Deadline(); ok {
		block = t.enc.AppendField(block, "grpc-timeout", encodeTimeout(time.Until(deadline)), false)
	}
	if cs.sendComp != nil {
		block = t.enc.AppendField(block, "grpc-encoding", cs.sendComp.Name(), false)
	}
	block = t.enc.AppendField(block, "grpc-accept-encoding", acceptEncoding(), false)
	block = appendMetadata(t.enc, block, md)
	t.out = appendHeaderBlock(t.out, s.id, block, false, t.peerMaxFrame)
	t.scratch = block
	t.mu.Unlock()
	t.flush()
	return false, nil
}

// onHeaders takes a header block of the server: the header, or the trailers
// that end the call, or both at once.
func (cs *clientStream) onHeaders(fields []hpack.HeaderField, end bool) error {
	md := metadata.MD{}
	httpStatus, grpcStatus := 0, -1
	var msg, encoding, ctype string
	var details []byte
	for _, f := range fields {
		switch f.Name {
		case ":status":
			httpStatus, _ = strconv.Atoi(f.Value)
		case "grpc-status":
			if v, err := strconv.Atoi(f.Value); err == nil {
				grpcStatus = v
			}
		case "grpc-message":
			msg = decodeGRPCMessage(f.Value)
		case "grpc-encoding":
			encoding = f.Value
		case "content-type":
			ctype = f.Value
		case "grpc-status-details-bin":
			details, _ = decodeBinHeader(f.Value)
		default:
			if !reservedHeader(f.Name) {
				_ = addMetadata(md, f.Name, f.Value)
			}
		}
	}
	if !cs.gotHeader {
		cs.gotHeader = true
		if httpStatus != 200 && grpcStatus < 0 {
			st := status.Newf(httpStatusCode(httpStatus), "grpc: unexpected HTTP status code received from server: %d (%s)", httpStatus, ctype)
			cs.s.t.resetStream(cs.s.id, errCancel, st.Err())
			return nil
		}
		if _, ok := contentSubtype(ctype); !ok && grpcStatus < 0 {
			st := status.Newf(codes.Internal, "grpc: unexpected content-type %q", ctype)
			cs.s.t.resetStream(cs.s.id, errCancel, st.Err())
			return nil
		}
		if encoding != "" && encoding != "identity" {
			if cs.recvComp = GetCompressor(encoding); cs.recvComp == nil {
				st := status.Newf(codes.Internal, "grpc: Decompressor is not installed for grpc-encoding %q", encoding)
				cs.s.t.resetStream(cs.s.id, errCancel, st.Err())
				return nil
			}
		}
		if !end {
			cs.header = md
			if cs.ci.header != nil {
				*cs.ci.header = md
			}
			cs.headerOnce.Do(func() { close(cs.headerDone) })
			return nil
		}
	} else if !end {
		return &streamError{id: cs.s.id, code: errProtocol}
	}
	// The trailers.
	cs.trailer = md
	if cs.ci.trailer != nil {
		*cs.ci.trailer = md
	}
	if grpcStatus < 0 {
		cs.st = status.New(codes.Internal, "grpc: server closed the stream without sending trailers")
	} else {
		cs.st = status.WithDetails(codes.Code(grpcStatus), msg, details)
	}
	t, s := cs.s.t, cs.s
	t.mu.Lock()
	if !s.localEnd && t.streams[s.id] == s {
		// The server needs nothing more of the call.
		t.out = appendRSTStream(t.out, s.id, errNo)
		s.localEnd = true
	}
	t.removeLocked(s)
	t.mu.Unlock()
	t.flush()
	if err := s.onData(nil, 0, true); err != nil {
		return err
	}
	cs.ended()
	return nil
}

// ended runs once the call has ended, however it did.
func (cs *clientStream) ended() {
	cs.endOnce.Do(func() {
		cs.headerOnce.Do(func() { close(cs.headerDone) })
		if cs.stopCancel != nil {
			cs.stopCancel()
		}
	})
}

// cancel ends the call from this side, with err as its outcome.
func (cs *clientStream) cancel(err error) {
	s := cs.s
	t := s.t
	t.mu.Lock()
	live := t.streams[s.id] == s
	t.mu.Unlock()
	if live {
		t.resetStream(s.id, errCancel, err)
	}
}

func (cs *clientStream) Context() context.Context { return cs.ctx }

func (cs *clientStream) Header() (metadata.MD, error) {
	select {
	case <-cs.headerDone:
	case <-cs.ctx.Done():
		return nil, status.FromContextError(cs.ctx.Err()).Err()
	}
	if cs.header != nil {
		return cs.header, nil
	}
	if err := cs.outcome(); err != nil && err != io.EOF {
		return nil, err
	}
	return metadata.MD{}, nil
}

func (cs *clientStream) Trailer() metadata.MD { return cs.trailer }

// outcome is how the call ended: io.EOF for codes.OK.
func (cs *clientStream) outcome() error {
	s := cs.s
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if cs.st == nil {
		return nil
	}
	if err := cs.st.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (cs *clientStream) CloseSend() error {
	s := cs.s
	t := s.t
	t.mu.Lock()
	if !s.localEnd && t.streams[s.id] == s && !t.closed {
		s.localEnd = true
		t.out = appendFrameHeader(t.out, frameData, flagEndStream, s.id, 0)
	}
	t.mu.Unlock()
	t.flush()
	return nil
}

func (cs *clientStream) SendMsg(m any) error {
	data, err := cs.codec.Marshal(m)
	if err != nil {
		return status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
	}
	compressed := false
	if cs.sendComp != nil {
		if data, err = compress(cs.sendComp, data); err != nil {
			return status.Errorf(codes.Internal, "grpc: error while compressing: %v", err)
		}
		compressed = true
	}
	if len(data) > cs.ci.maxSend {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(data), cs.ci.maxSend)
	}
	if err := cs.s.writeMsg(compressed, data); err != nil {
		if err == errStreamDone && cs.desc.ClientStreams {
			return status.Error(codes.Internal, "grpc: SendMsg called after CloseSend")
		}
		// The call has ended; RecvMsg says how.
		return io.EOF
	}
	return nil
}

func (cs *clientStream) RecvMsg(m any) error {
	err := cs.recvMsg(m)
	if err != nil || cs.desc.ServerStreams {
		return err
	}
	// A server that does not stream sends one message, and then its status.
	if err := cs.recvMsg(nil); err != io.EOF {
		if err == nil {
			return status.Error(codes.Internal, "grpc: cardinality violation: expected <EOF> for non server-streaming RPCs, but received another message")
		}
		return err
	}
	return nil
}

func (cs *clientStream) recvMsg(m any) error {
	compressed, data, err := cs.s.readMsg(cs.ci.maxRecv)
	if err == io.EOF {
		err = cs.outcome()
		if err == io.EOF && !cs.desc.ServerStreams && cs.received == 0 && m != nil {
			return status.Error(codes.Internal, "grpc: cardinality violation: received no response message from non server-streaming RPC")
		}
		return err
	}
	if err != nil {
		return err
	}
	cs.received++
	if m == nil {
		return nil
	}
	if compressed {
		if cs.recvComp == nil {
			return status.Error(codes.Internal, "grpc: compressed message without grpc-encoding")
		}
		if data, err = decompress(cs.recvComp, data, cs.ci.maxRecv); err != nil {
			if err == errMessageTooLarge {
				return status.Errorf(codes.ResourceExhausted, "grpc: received message after decompression larger than max %d", cs.ci.maxRecv)
			}
			return status.Errorf(codes.Internal, "grpc: failed to decompress the received message: %v", err)
		}
	}
	if err := cs.codec.Unmarshal(data, m); err != nil {
		return status.Errorf(codes.Internal, "grpc: failed to unmarshal the received message: %v", err)
	}
	return nil
}
