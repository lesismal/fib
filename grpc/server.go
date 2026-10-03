//go:build linux || darwin || windows

package grpc

import (
	"context"
	stdtls "crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"net"
	"reflect"
	"runtime/debug"
	"slices"
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

const (
	defaultMaxRecvMsgSize = 4 << 20
	defaultMaxSendMsgSize = math.MaxInt32
)

type serverOptions struct {
	unaryInts      []UnaryServerInterceptor
	streamInts     []StreamServerInterceptor
	unaryInt       UnaryServerInterceptor
	streamInt      StreamServerInterceptor
	maxRecvMsgSize int
	maxSendMsgSize int
	maxStreams     uint32
	pool           fib.TaskPool
	codec          Codec
	unknown        StreamHandler
}

// ServerOption configures a Server.
type ServerOption func(*serverOptions)

// UnaryInterceptor sets the interceptor of unary calls; it comes before those
// ChainUnaryInterceptor adds.
func UnaryInterceptor(i UnaryServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.unaryInts = append([]UnaryServerInterceptor{i}, o.unaryInts...) }
}

// ChainUnaryInterceptor adds interceptors of unary calls, the first
// outermost.
func ChainUnaryInterceptor(interceptors ...UnaryServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.unaryInts = append(o.unaryInts, interceptors...) }
}

// StreamInterceptor sets the interceptor of streaming calls; it comes before
// those ChainStreamInterceptor adds.
func StreamInterceptor(i StreamServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.streamInts = append([]StreamServerInterceptor{i}, o.streamInts...) }
}

// ChainStreamInterceptor adds interceptors of streaming calls, the first
// outermost.
func ChainStreamInterceptor(interceptors ...StreamServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.streamInts = append(o.streamInts, interceptors...) }
}

// MaxRecvMsgSize bounds a message the server receives, 4MB by default.
func MaxRecvMsgSize(n int) ServerOption { return func(o *serverOptions) { o.maxRecvMsgSize = n } }

// MaxSendMsgSize bounds a message the server sends, math.MaxInt32 by
// default.
func MaxSendMsgSize(n int) ServerOption { return func(o *serverOptions) { o.maxSendMsgSize = n } }

// MaxConcurrentStreams bounds the calls one connection may have open at
// once; zero, the default, leaves them unbounded.
func MaxConcurrentStreams(n uint32) ServerOption { return func(o *serverOptions) { o.maxStreams = n } }

// TaskPool sets the pool every call's handler runs on. Without it, a call
// runs on fib.Engine.HandlerPool of its connection's engine, the pool fib's
// HTTP/2 and HTTP/3 handlers run on. A call the pool turns away runs on a
// goroutine of its own.
func TaskPool(pool fib.TaskPool) ServerOption { return func(o *serverOptions) { o.pool = pool } }

// ForceServerCodec makes the server encode and decode every call with codec,
// whatever content subtype the call names.
func ForceServerCodec(codec Codec) ServerOption { return func(o *serverOptions) { o.codec = codec } }

// UnknownServiceHandler serves the calls of methods no service registers,
// as a bidirectional streaming call, instead of answering Unimplemented.
func UnknownServiceHandler(h StreamHandler) ServerOption {
	return func(o *serverOptions) { o.unknown = h }
}

type service struct {
	impl    any
	methods map[string]*MethodDesc
	streams map[string]*StreamDesc
	mdata   any
}

// Server is a gRPC server. It is a fib.Handler, serving HTTP/2 with prior
// knowledge (h2c) on the connections of the engine it is bound to:
//
//	engine, err := fib.Bind(config, server)
//
// and gRPC over TLS wrapped in package tls, with ALPN offering h2:
//
//	engine, err := fib.Bind(config, fibtls.NewServer(grpc.ConfigureTLS(tlsConfig), server))
//
// Serve and ServeTLS make the engine themselves.
type Server struct {
	opts serverOptions

	mu         sync.Mutex
	services   map[string]*service
	transports map[*transport]struct{}
	draining   bool
	// drained is closed once the last transport has gone while draining.
	drained chan struct{}
	engine  *fib.Engine
	served  chan struct{}
}

// NewServer returns a Server with opts.
func NewServer(opts ...ServerOption) *Server {
	o := serverOptions{maxRecvMsgSize: defaultMaxRecvMsgSize, maxSendMsgSize: defaultMaxSendMsgSize}
	for _, opt := range opts {
		opt(&o)
	}
	o.unaryInt = chainUnaryServer(o.unaryInts)
	o.streamInt = chainStreamServer(o.streamInts)
	return &Server{opts: o, services: map[string]*service{}, transports: map[*transport]struct{}{}}
}

// ConfigureTLS returns a copy of config whose ALPN offers h2, which gRPC
// clients require.
func ConfigureTLS(config *stdtls.Config) *stdtls.Config {
	if config == nil {
		config = &stdtls.Config{}
	} else {
		config = config.Clone()
	}
	if !slices.Contains(config.NextProtos, "h2") {
		config.NextProtos = append([]string{"h2"}, config.NextProtos...)
	}
	return config
}

// RegisterService registers impl as the implementation of desc. It panics if
// impl does not implement desc's HandlerType, or the service is registered
// already. Register services before the server serves.
func (s *Server) RegisterService(desc *ServiceDesc, impl any) {
	if impl != nil && desc.HandlerType != nil {
		ht := reflect.TypeOf(desc.HandlerType).Elem()
		if !reflect.TypeOf(impl).Implements(ht) {
			panic(fmt.Sprintf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", reflect.TypeOf(impl), ht))
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[desc.ServiceName]; ok {
		panic("grpc: Server.RegisterService found duplicate service registration for " + desc.ServiceName)
	}
	svc := &service{impl: impl, methods: map[string]*MethodDesc{}, streams: map[string]*StreamDesc{}, mdata: desc.Metadata}
	for i := range desc.Methods {
		svc.methods[desc.Methods[i].MethodName] = &desc.Methods[i]
	}
	for i := range desc.Streams {
		svc.streams[desc.Streams[i].StreamName] = &desc.Streams[i]
	}
	s.services[desc.ServiceName] = svc
}

// GetServiceInfo describes the registered services, by name.
func (s *Server) GetServiceInfo() map[string]ServiceInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := make(map[string]ServiceInfo, len(s.services))
	for name, svc := range s.services {
		var methods []MethodInfo
		for m := range svc.methods {
			methods = append(methods, MethodInfo{Name: m})
		}
		for m, d := range svc.streams {
			methods = append(methods, MethodInfo{Name: m, IsClientStream: d.ClientStreams, IsServerStream: d.ServerStreams})
		}
		info[name] = ServiceInfo{Methods: methods, Metadata: svc.mdata}
	}
	return info
}

// OnOpen serves a connection the engine accepted.
func (s *Server) OnOpen(conn *fib.Connection) {
	t := newTransport(false)
	t.server = s
	t.conn = conn
	t.pool = conn.Engine().HandlerPool()
	conn.SetAttachment(t)
	s.mu.Lock()
	draining := s.draining
	if !draining {
		s.transports[t] = struct{}{}
	}
	s.mu.Unlock()
	if draining {
		conn.Close()
		return
	}
	t.start(s.opts.maxStreams)
}

// OnData reads the connection's frames.
func (s *Server) OnData(conn *fib.Connection, data []byte) {
	if t, ok := conn.Attachment().(*transport); ok {
		t.feed(data)
	}
}

// OnPriorityData ignores out-of-band data.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose ends the connection's calls.
func (s *Server) OnClose(conn *fib.Connection, err error) {
	if t, ok := conn.Attachment().(*transport); ok {
		t.onClose(err)
		s.removeTransport(t)
	}
}

func (s *Server) removeTransport(t *transport) {
	s.mu.Lock()
	delete(s.transports, t)
	if s.draining && len(s.transports) == 0 && s.drained != nil {
		close(s.drained)
		s.drained = nil
	}
	s.mu.Unlock()
}

// Serve binds an engine of config, serving gRPC over h2c, and runs it until
// Stop or GracefulStop, after which it closes the engine and returns.
func (s *Server) Serve(config fib.Config) error { return s.serve(config, s) }

// ServeTLS is Serve over TLS, with ALPN offering h2.
func (s *Server) ServeTLS(config fib.Config, tlsConfig *stdtls.Config) error {
	return s.serve(config, fibtls.NewServer(ConfigureTLS(tlsConfig), s))
}

func (s *Server) serve(config fib.Config, handler fib.Handler) error {
	engine, err := fib.Bind(config, handler)
	if err != nil {
		return err
	}
	served := make(chan struct{})
	defer close(served)
	s.mu.Lock()
	s.engine, s.served = engine, served
	s.mu.Unlock()
	err = engine.Run()
	if closeErr := engine.Close(); err == nil {
		err = closeErr
	}
	// Closing the engine ended the connections without OnClose.
	s.mu.Lock()
	transports := make([]*transport, 0, len(s.transports))
	for t := range s.transports {
		transports = append(transports, t)
	}
	s.mu.Unlock()
	for _, t := range transports {
		t.onClose(net.ErrClosed)
		s.removeTransport(t)
	}
	return err
}

// Engine returns the engine Serve or ServeTLS made, or nil.
func (s *Server) Engine() *fib.Engine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.engine
}

// Stop closes every connection at once, failing the calls in progress, and
// stops the engine Serve made.
func (s *Server) Stop() {
	s.mu.Lock()
	s.draining = true
	engine := s.engine
	transports := make([]*transport, 0, len(s.transports))
	for t := range s.transports {
		transports = append(transports, t)
	}
	s.mu.Unlock()
	for _, t := range transports {
		t.conn.Close()
	}
	if engine != nil {
		engine.Stop()
	}
}

// GracefulStop tells every connection's client to open no more calls on it,
// with a GOAWAY, waits for the calls in progress to finish, and then stops as
// Stop does.
func (s *Server) GracefulStop() {
	s.mu.Lock()
	s.draining = true
	var drained chan struct{}
	if len(s.transports) > 0 {
		if s.drained == nil {
			s.drained = make(chan struct{})
		}
		drained = s.drained
	}
	transports := make([]*transport, 0, len(s.transports))
	for t := range s.transports {
		transports = append(transports, t)
	}
	s.mu.Unlock()
	for _, t := range transports {
		t.mu.Lock()
		t.goingAway = true
		t.out = appendGoAway(t.out, t.lastPeerID, errNo, "")
		idle := len(t.streams) == 0
		t.mu.Unlock()
		t.flush()
		if idle {
			t.conn.CloseAfterSend()
		}
	}
	if drained != nil {
		<-drained
	}
	s.Stop()
}

// openStream starts a call the client opened with a header block.
func (s *Server) openStream(t *transport, id uint32, fields []hpack.HeaderField, endStream bool) {
	ss := &serverStream{server: s, header: metadata.MD{}}
	st := &stream{t: t, id: id, ss: ss, recvWindow: streamWindow, ready: make(chan struct{}, 1)}
	ss.s = st
	md := metadata.MD{}
	var method, path, ctype, encoding string
	var timeout time.Duration
	hasTimeout := false
	for _, f := range fields {
		switch f.Name {
		case ":method":
			method = f.Value
		case ":path":
			path = f.Value
		case "content-type":
			ctype = f.Value
		case "grpc-encoding":
			encoding = f.Value
		case "grpc-timeout":
			timeout, hasTimeout = decodeTimeout(f.Value)
		default:
			if !reservedHeader(f.Name) {
				if err := addMetadata(md, f.Name, f.Value); err != nil {
					t.resetStream(id, errProtocol, err)
					return
				}
			}
		}
	}
	subtype, ok := contentSubtype(ctype)
	if !ok || method != "POST" {
		ss.rejectHTTP(t, id, ok, endStream)
		return
	}
	ss.method = path
	ss.codec = s.opts.codec
	if ss.codec == nil {
		ss.codec = GetCodec(subtype)
	}
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx = peer.NewContext(ctx, &peer.Peer{Addr: t.conn.RemoteAddr(), LocalAddr: t.conn.LocalAddr()})
	ctx = context.WithValue(ctx, streamKey{}, ss)
	if hasTimeout {
		ctx, ss.cancelCtx = context.WithTimeout(ctx, timeout)
	} else {
		ctx, ss.cancelCtx = context.WithCancel(ctx)
	}
	st.ctx, ss.ctx = ctx, ctx

	var fail *status.Status
	switch {
	case ss.codec == nil:
		fail = status.Newf(codes.Internal, "grpc: no codec registered for content-subtype %s", subtype)
	case encoding != "" && encoding != "identity":
		if ss.recvComp = GetCompressor(encoding); ss.recvComp == nil {
			fail = status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", encoding)
		}
		// Responses are compressed the way the request was.
		ss.sendComp = ss.recvComp
	}
	if fail == nil {
		fail = ss.resolve(path)
	}

	t.mu.Lock()
	switch {
	case t.closed:
		t.mu.Unlock()
		ss.cancelCtx()
		return
	case t.goingAway:
		t.out = appendRSTStream(t.out, id, errRefusedStream)
		t.mu.Unlock()
		t.flush()
		ss.cancelCtx()
		return
	case s.opts.maxStreams > 0 && uint32(len(t.streams)) >= s.opts.maxStreams:
		t.out = appendRSTStream(t.out, id, errRefusedStream)
		t.mu.Unlock()
		t.flush()
		ss.cancelCtx()
		return
	}
	st.sendWindow = t.peerInitialWindow
	st.remoteEnd = endStream
	t.streams[id] = st
	t.mu.Unlock()
	if fail != nil {
		ss.finish(fail)
		return
	}
	if ss.unary != nil && !endStream {
		// A unary call is served once its request has arrived whole, so that
		// it never holds a worker of the pool waiting for it.
		st.mu.Lock()
		st.onEnd = func() { t.run(ss) }
		st.mu.Unlock()
		return
	}
	t.run(ss)
}

type streamKey struct{}

// serverStream is the server's side of a call.
type serverStream struct {
	server *Server
	s      *stream
	ctx    context.Context
	// cancelCtx ends ctx, once the call is done or the client gone.
	cancelCtx context.CancelFunc
	method    string
	codec     Codec
	recvComp  Compressor
	sendComp  Compressor
	// What resolve found the call to be.
	svc    *service
	unary  *MethodDesc
	stream *StreamDesc

	// Guarded by the transport's mu.
	header     metadata.MD
	trailer    metadata.MD
	headerSent bool
	done       bool
}

func (ss *serverStream) cancel() {
	if ss.cancelCtx != nil {
		ss.cancelCtx()
	}
}

// resolve finds the method of path, "/service/method".
func (ss *serverStream) resolve(path string) *status.Status {
	s := ss.server
	name := strings.TrimPrefix(path, "/")
	i := strings.LastIndexByte(name, '/')
	if i <= 0 {
		if s.opts.unknown != nil {
			ss.stream = &StreamDesc{StreamName: name, Handler: s.opts.unknown, ServerStreams: true, ClientStreams: true}
			return nil
		}
		return status.Newf(codes.Unimplemented, "malformed method name: %q", path)
	}
	svcName, methodName := name[:i], name[i+1:]
	s.mu.Lock()
	svc := s.services[svcName]
	s.mu.Unlock()
	if svc != nil {
		if md := svc.methods[methodName]; md != nil {
			ss.svc, ss.unary = svc, md
			return nil
		}
		if sd := svc.streams[methodName]; sd != nil {
			ss.svc, ss.stream = svc, sd
			return nil
		}
	}
	if s.opts.unknown != nil {
		ss.stream = &StreamDesc{StreamName: methodName, Handler: s.opts.unknown, ServerStreams: true, ClientStreams: true}
		return nil
	}
	if svc == nil {
		return status.Newf(codes.Unimplemented, "unknown service %v", svcName)
	}
	return status.Newf(codes.Unimplemented, "unknown method %v for service %v", methodName, svcName)
}

// rejectHTTP answers a request that is not a gRPC call with an HTTP status.
func (ss *serverStream) rejectHTTP(t *transport, id uint32, grpcType, endStream bool) {
	code := "415"
	if grpcType {
		code = "405"
	}
	t.mu.Lock()
	block := t.enc.Begin(t.scratch[:0])
	block = t.enc.AppendField(block, ":status", code, false)
	t.out = appendHeaderBlock(t.out, id, block, true, t.peerMaxFrame)
	t.scratch = block
	if !endStream {
		t.out = appendRSTStream(t.out, id, errNo)
	}
	t.mu.Unlock()
	t.flush()
}

// RunTask serves the call on the pool.
func (ss *serverStream) RunTask() {
	var err error
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("grpc: handler panicked", "method", ss.method, "panic", recovered, "stack", string(debug.Stack()))
			err = status.Errorf(codes.Internal, "grpc: handler panicked: %v", recovered)
		}
		ss.finish(status.Convert(errOrContext(err, ss.ctx)))
	}()
	s := ss.server
	if ss.unary != nil {
		var reply any
		reply, err = ss.unary.Handler(ss.svc.impl, ss.ctx, ss.RecvMsg, s.opts.unaryInt)
		if err == nil {
			err = ss.SendMsg(reply)
		}
		return
	}
	var impl any
	if ss.svc != nil {
		impl = ss.svc.impl
	}
	if s.opts.streamInt != nil {
		info := &StreamServerInfo{FullMethod: ss.method, IsClientStream: ss.stream.ClientStreams, IsServerStream: ss.stream.ServerStreams}
		err = s.opts.streamInt(impl, ss, info, ss.stream.Handler)
		return
	}
	err = ss.stream.Handler(impl, ss)
}

// errOrContext is err, or, for an error the call's context caused, the
// status the context's end means.
func errOrContext(err error, ctx context.Context) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); !ok && ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	return err
}

func (ss *serverStream) Context() context.Context { return ss.ctx }

func (ss *serverStream) SetHeader(md metadata.MD) error {
	t := ss.s.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if ss.headerSent || ss.done {
		return status.Error(codes.Internal, "grpc: SetHeader called after the header was sent")
	}
	for k, v := range md {
		ss.header[strings.ToLower(k)] = append(ss.header[strings.ToLower(k)], v...)
	}
	return nil
}

func (ss *serverStream) SendHeader(md metadata.MD) error {
	if err := ss.SetHeader(md); err != nil {
		return err
	}
	t := ss.s.t
	t.mu.Lock()
	ss.sendHeaderLocked()
	t.mu.Unlock()
	t.flush()
	return nil
}

func (ss *serverStream) SetTrailer(md metadata.MD) {
	t := ss.s.t
	t.mu.Lock()
	defer t.mu.Unlock()
	if ss.trailer == nil {
		ss.trailer = metadata.MD{}
	}
	for k, v := range md {
		ss.trailer[strings.ToLower(k)] = append(ss.trailer[strings.ToLower(k)], v...)
	}
}

// sendHeaderLocked frames the response header. Callers hold the transport's
// mu.
func (ss *serverStream) sendHeaderLocked() {
	if ss.headerSent || ss.done {
		return
	}
	t := ss.s.t
	if t.streams[ss.s.id] != ss.s || t.closed {
		return
	}
	ss.headerSent = true
	block := t.enc.Begin(t.scratch[:0])
	block = t.enc.AppendField(block, ":status", "200", false)
	block = t.enc.AppendField(block, "content-type", contentType(ss.codec.Name()), false)
	if ss.sendComp != nil {
		block = t.enc.AppendField(block, "grpc-encoding", ss.sendComp.Name(), false)
	}
	block = appendMetadata(t.enc, block, ss.header)
	t.out = appendHeaderBlock(t.out, ss.s.id, block, false, t.peerMaxFrame)
	t.scratch = block
}

func (ss *serverStream) SendMsg(m any) error {
	data, err := ss.codec.Marshal(m)
	if err != nil {
		return status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err)
	}
	compressed := false
	if ss.sendComp != nil {
		if data, err = compress(ss.sendComp, data); err != nil {
			return status.Errorf(codes.Internal, "grpc: error while compressing: %v", err)
		}
		compressed = true
	}
	if len(data) > ss.server.opts.maxSendMsgSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(data), ss.server.opts.maxSendMsgSize)
	}
	t := ss.s.t
	t.mu.Lock()
	ss.sendHeaderLocked()
	t.mu.Unlock()
	if err := ss.s.writeMsg(compressed, data); err != nil {
		return toRPCErr(err)
	}
	return nil
}

func (ss *serverStream) RecvMsg(m any) error {
	compressed, data, err := ss.s.readMsg(ss.server.opts.maxRecvMsgSize)
	if err != nil {
		return toRPCErr(err)
	}
	if compressed {
		if ss.recvComp == nil {
			return status.Error(codes.Internal, "grpc: compressed message without grpc-encoding")
		}
		if data, err = decompress(ss.recvComp, data, ss.server.opts.maxRecvMsgSize); err != nil {
			if err == errMessageTooLarge {
				return status.Errorf(codes.ResourceExhausted, "grpc: received message after decompression larger than max %d", ss.server.opts.maxRecvMsgSize)
			}
			return status.Errorf(codes.Internal, "grpc: failed to decompress the received message: %v", err)
		}
	}
	if err := ss.codec.Unmarshal(data, m); err != nil {
		return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err)
	}
	return nil
}

// toRPCErr is err as a call sees it.
func toRPCErr(err error) error {
	if err == errStreamDone {
		return status.Error(codes.Internal, "grpc: the stream is done")
	}
	return err
}

// finish ends the call with st: its status and trailers go out, as the
// whole response if no header has, and the stream is forgotten.
func (ss *serverStream) finish(st *status.Status) {
	defer ss.cancel()
	t := ss.s.t
	t.mu.Lock()
	if ss.done || t.closed || t.streams[ss.s.id] != ss.s {
		ss.done = true
		t.mu.Unlock()
		return
	}
	block := t.enc.Begin(t.scratch[:0])
	if !ss.headerSent {
		// Trailers-only: one HEADERS is the whole response.
		block = t.enc.AppendField(block, ":status", "200", false)
		codecName := "proto"
		if ss.codec != nil {
			codecName = ss.codec.Name()
		}
		block = t.enc.AppendField(block, "content-type", contentType(codecName), false)
		block = appendMetadata(t.enc, block, ss.header)
	}
	ss.done = true
	block = t.enc.AppendField(block, "grpc-status", strconv.Itoa(int(st.Code())), false)
	if msg := st.Message(); msg != "" {
		block = t.enc.AppendField(block, "grpc-message", encodeGRPCMessage(msg), false)
	}
	if details := st.Details(); len(details) > 0 {
		block = t.enc.AppendField(block, "grpc-status-details-bin", encodeBin(details), false)
	}
	block = appendMetadata(t.enc, block, ss.trailer)
	t.out = appendHeaderBlock(t.out, ss.s.id, block, true, t.peerMaxFrame)
	t.scratch = block
	ss.s.localEnd = true
	ss.s.mu.Lock()
	remoteEnd := ss.s.remoteEnd
	ss.s.mu.Unlock()
	if !remoteEnd {
		// The client need send nothing more.
		t.out = appendRSTStream(t.out, ss.s.id, errNo)
	}
	t.removeLocked(ss.s)
	t.mu.Unlock()
	t.flush()
}

// SetHeader sets header metadata of the call of ctx, a server handler's.
func SetHeader(ctx context.Context, md metadata.MD) error {
	ss, ok := ctx.Value(streamKey{}).(*serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return ss.SetHeader(md)
}

// SendHeader sends the header metadata of the call of ctx.
func SendHeader(ctx context.Context, md metadata.MD) error {
	ss, ok := ctx.Value(streamKey{}).(*serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return ss.SendHeader(md)
}

// SetTrailer sets trailer metadata of the call of ctx.
func SetTrailer(ctx context.Context, md metadata.MD) error {
	ss, ok := ctx.Value(streamKey{}).(*serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	ss.SetTrailer(md)
	return nil
}

// Method returns the full method name of the call of ctx.
func Method(ctx context.Context) (string, bool) {
	ss, ok := ctx.Value(streamKey{}).(*serverStream)
	if !ok {
		return "", false
	}
	return ss.method, true
}
