//go:build linux || darwin || windows

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/metadata"
	"github.com/lesismal/fib/grpc/peer"
	"github.com/lesismal/fib/grpc/status"
	"github.com/lesismal/fib/hpack"
	"github.com/lesismal/fib/taskpool"
	"github.com/lesismal/fib/tlstest"
)

// The test service, as protoc-gen-go-grpc would generate it, with JSON
// messages.

type Req struct {
	Msg string
	N   int
}

type Rsp struct {
	Msg string
	N   int
}

type echoServer interface {
	Unary(context.Context, *Req) (*Rsp, error)
	Count(*Req, ServerStreamingServer[Rsp]) error
	Sum(ClientStreamingServer[Req, Rsp]) error
	Chat(BidiStreamingServer[Req, Rsp]) error
}

var echoDesc = ServiceDesc{
	ServiceName: "test.Echo",
	HandlerType: (*echoServer)(nil),
	Methods: []MethodDesc{{
		MethodName: "Unary",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor UnaryServerInterceptor) (any, error) {
			in := new(Req)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(echoServer).Unary(ctx, in)
			}
			info := &UnaryServerInfo{Server: srv, FullMethod: "/test.Echo/Unary"}
			return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
				return srv.(echoServer).Unary(ctx, req.(*Req))
			})
		},
	}},
	Streams: []StreamDesc{
		{StreamName: "Count", ServerStreams: true, Handler: func(srv any, stream ServerStream) error {
			m := new(Req)
			if err := stream.RecvMsg(m); err != nil {
				return err
			}
			return srv.(echoServer).Count(m, &GenericServerStream[Req, Rsp]{ServerStream: stream})
		}},
		{StreamName: "Sum", ClientStreams: true, Handler: func(srv any, stream ServerStream) error {
			return srv.(echoServer).Sum(&GenericServerStream[Req, Rsp]{ServerStream: stream})
		}},
		{StreamName: "Chat", ServerStreams: true, ClientStreams: true, Handler: func(srv any, stream ServerStream) error {
			return srv.(echoServer).Chat(&GenericServerStream[Req, Rsp]{ServerStream: stream})
		}},
	},
}

type echoImpl struct {
	// block, when set, is waited on by "block" calls, which report entering
	// on entered.
	block   chan struct{}
	entered chan struct{}
	// ctxDone receives the error of a "watch" call's context once it ends.
	ctxDone chan error
}

func (e *echoImpl) Unary(ctx context.Context, in *Req) (*Rsp, error) {
	switch in.Msg {
	case "fail":
		return nil, status.Error(codes.NotFound, "no such thing")
	case "plain":
		return nil, errors.New("plain error")
	case "panic":
		panic("handler bug")
	case "sleep":
		<-ctx.Done()
		return nil, ctx.Err()
	case "watch":
		<-ctx.Done()
		e.ctxDone <- ctx.Err()
		return nil, ctx.Err()
	case "block":
		e.entered <- struct{}{}
		<-e.block
	case "meta":
		md, _ := metadata.FromIncomingContext(ctx)
		SetHeader(ctx, metadata.Pairs("h", "header"))
		SetTrailer(ctx, metadata.Pairs("t", "trailer", "t-bin", "\x00\xff"))
		p, _ := peer.FromContext(ctx)
		method, _ := Method(ctx)
		return &Rsp{Msg: strings.Join(md.Get("k"), ",") + "|" + fmt.Sprintf("%x", md.Get("k-bin")) + "|" + method + "|" + fmt.Sprint(p.Addr != nil)}, nil
	}
	return &Rsp{Msg: in.Msg, N: in.N}, nil
}

func (e *echoImpl) Count(in *Req, s ServerStreamingServer[Rsp]) error {
	for i := 0; i < in.N; i++ {
		if in.Msg == "block" && i == 1 {
			// The first message is out; the rest wait to be let go.
			<-e.block
		}
		if err := s.Send(&Rsp{Msg: in.Msg, N: i}); err != nil {
			return err
		}
	}
	return nil
}

func (e *echoImpl) Sum(s ClientStreamingServer[Req, Rsp]) error {
	total, text := 0, ""
	for {
		m, err := s.Recv()
		if err == io.EOF {
			return s.SendAndClose(&Rsp{Msg: text, N: total})
		}
		if err != nil {
			return err
		}
		total += m.N
		text += m.Msg
	}
}

func (e *echoImpl) Chat(s BidiStreamingServer[Req, Rsp]) error {
	for {
		m, err := s.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if m.Msg == "slow" {
			// A reader that stops lets the windows fill.
			time.Sleep(300 * time.Millisecond)
		}
		if err := s.Send(&Rsp{Msg: strings.ToUpper(m.Msg), N: m.N}); err != nil {
			return err
		}
	}
}

// echoClient is the generated client.
type echoClient struct{ cc ClientConnInterface }

func (c echoClient) Unary(ctx context.Context, in *Req, opts ...CallOption) (*Rsp, error) {
	out := new(Rsp)
	if err := c.cc.Invoke(ctx, "/test.Echo/Unary", in, out, append([]CallOption{StaticMethod()}, opts...)...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c echoClient) Count(ctx context.Context, in *Req, opts ...CallOption) (ServerStreamingClient[Rsp], error) {
	stream, err := c.cc.NewStream(ctx, &echoDesc.Streams[0], "/test.Echo/Count", opts...)
	if err != nil {
		return nil, err
	}
	x := &GenericClientStream[Req, Rsp]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

func (c echoClient) Sum(ctx context.Context, opts ...CallOption) (ClientStreamingClient[Req, Rsp], error) {
	stream, err := c.cc.NewStream(ctx, &echoDesc.Streams[1], "/test.Echo/Sum", opts...)
	if err != nil {
		return nil, err
	}
	return &GenericClientStream[Req, Rsp]{ClientStream: stream}, nil
}

func (c echoClient) Chat(ctx context.Context, opts ...CallOption) (BidiStreamingClient[Req, Rsp], error) {
	stream, err := c.cc.NewStream(ctx, &echoDesc.Streams[2], "/test.Echo/Chat", opts...)
	if err != nil {
		return nil, err
	}
	return &GenericClientStream[Req, Rsp]{ClientStream: stream}, nil
}

// startServer serves s, with the echo service on impl, on a loopback port.
func startServer(t *testing.T, impl *echoImpl, opts ...ServerOption) (*Server, string) {
	t.Helper()
	s := NewServer(opts...)
	s.RegisterService(&echoDesc, impl)
	return s, startHandler(t, s)
}

func startHandler(t *testing.T, handler fib.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	return addr.String()
}

func startClientEngine(t *testing.T) *fib.Engine {
	t.Helper()
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	return engine
}

func dial(t *testing.T, addr string, opts ...DialOption) (*ClientConn, echoClient) {
	t.Helper()
	opts = append([]DialOption{WithEngine(startClientEngine(t)), WithDefaultCallOptions(CallContentSubtype("json"))}, opts...)
	cc, err := NewClient(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc, echoClient{cc}
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestUnary(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	_, c := dial(t, addr)
	ctx := testCtx(t)
	r, err := c.Unary(ctx, &Req{Msg: "hello", N: 3})
	if err != nil || r.Msg != "hello" || r.N != 3 {
		t.Fatalf("Unary = %+v, %v", r, err)
	}
	_, err = c.Unary(ctx, &Req{Msg: "fail"})
	if s := status.Convert(err); s.Code() != codes.NotFound || s.Message() != "no such thing" {
		t.Fatalf("fail = %v", err)
	}
	_, err = c.Unary(ctx, &Req{Msg: "plain"})
	if s := status.Convert(err); s.Code() != codes.Unknown || s.Message() != "plain error" {
		t.Fatalf("plain error = %v", err)
	}
	_, err = c.Unary(ctx, &Req{Msg: "panic"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic = %v", err)
	}
	if r, err := c.Unary(ctx, &Req{Msg: "after panic"}); err != nil || r.Msg != "after panic" {
		t.Fatalf("after a panic = %+v, %v", r, err)
	}
	big := strings.Repeat("b", 3<<20)
	if r, err := c.Unary(ctx, &Req{Msg: big}); err != nil || r.Msg != big {
		t.Fatalf("a message over the window: %d bytes, %v", len(r.GetMsg()), err)
	}
	if r, err := c.Unary(ctx, &Req{Msg: "zipped " + strings.Repeat("z", 1000)}, UseCompressor("gzip")); err != nil || !strings.HasPrefix(r.Msg, "zipped") {
		t.Fatalf("gzip = %v", err)
	}
}

func (r *Rsp) GetMsg() string {
	if r == nil {
		return ""
	}
	return r.Msg
}

func TestStreams(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	_, c := dial(t, addr)
	ctx := testCtx(t)

	count, err := c.Count(ctx, &Req{Msg: "c", N: 500})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		m, err := count.Recv()
		if err == io.EOF {
			if i != 500 {
				t.Fatalf("Count ended after %d", i)
			}
			break
		}
		if err != nil || m.N != i {
			t.Fatalf("Count %d = %+v, %v", i, m, err)
		}
	}

	sum, err := c.Sum(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		if err := sum.Send(&Req{Msg: "x", N: i}); err != nil {
			t.Fatal(err)
		}
	}
	r, err := sum.CloseAndRecv()
	if err != nil || r.N != 5050 || len(r.Msg) != 100 {
		t.Fatalf("Sum = %+v, %v", r, err)
	}

	chat, err := c.Chat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i, w := range []string{"a", "b", "c"} {
		if err := chat.Send(&Req{Msg: w, N: i}); err != nil {
			t.Fatal(err)
		}
		m, err := chat.Recv()
		if err != nil || m.Msg != strings.ToUpper(w) {
			t.Fatalf("Chat %s = %+v, %v", w, m, err)
		}
	}
	chat.CloseSend()
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatalf("Chat end = %v", err)
	}
}

// TestBackpressure streams far more both ways than the windows hold through a
// server that pauses, so that both sides wait for room.
func TestBackpressure(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	_, c := dial(t, addr)
	ctx := testCtx(t)
	chat, err := c.Chat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("p", 256<<10)
	const n = 100 // 25MB each way, over the 16MB connection window
	errs := make(chan error, 1)
	go func() {
		for i := range n {
			msg := payload
			if i == 0 {
				msg = "slow"
			}
			if err := chat.Send(&Req{Msg: msg, N: i}); err != nil {
				errs <- err
				return
			}
		}
		errs <- chat.CloseSend()
	}()
	for i := range n {
		m, err := chat.Recv()
		if err != nil || m.N != i {
			t.Fatalf("Recv %d = %v", i, err)
		}
	}
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestMetadata(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	_, c := dial(t, addr)
	ctx := metadata.AppendToOutgoingContext(testCtx(t), "k", "v1", "K", "v2", "k-bin", "\x01\x02")
	var header, trailer metadata.MD
	r, err := c.Unary(ctx, &Req{Msg: "meta"}, Header(&header), Trailer(&trailer))
	if err != nil {
		t.Fatal(err)
	}
	if r.Msg != "v1,v2|[0102]|/test.Echo/Unary|true" {
		t.Fatalf("the server saw %q", r.Msg)
	}
	if fmt.Sprint(header.Get("h")) != "[header]" || fmt.Sprint(trailer.Get("t")) != "[trailer]" || fmt.Sprint(trailer.Get("t-bin")) != "[\x00\xff]" {
		t.Fatalf("header %v trailer %v", header, trailer)
	}
}

func TestDeadlineAndCancel(t *testing.T) {
	impl := &echoImpl{ctxDone: make(chan error, 1)}
	_, addr := startServer(t, impl)
	_, c := dial(t, addr)
	ctx, cancel := context.WithTimeout(testCtx(t), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Unary(ctx, &Req{Msg: "sleep"}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("deadline = %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the deadline took %v", d)
	}

	// Canceling a call cancels the handler's context.
	ctx, cancel = context.WithCancel(testCtx(t))
	done := make(chan error, 1)
	go func() {
		_, err := c.Unary(ctx, &Req{Msg: "watch"})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatalf("canceled call = %v", err)
	}
	select {
	case err := <-impl.ctxDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the handler's context ended with %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the handler's context outlived the call")
	}
}

func TestUnimplemented(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	cc, _ := dial(t, addr)
	ctx := testCtx(t)
	err := cc.Invoke(ctx, "/test.Echo/Nope", &Req{}, &Rsp{})
	if s := status.Convert(err); s.Code() != codes.Unimplemented || !strings.Contains(s.Message(), "unknown method Nope") {
		t.Fatalf("unknown method = %v", err)
	}
	err = cc.Invoke(ctx, "/test.Nope/X", &Req{}, &Rsp{})
	if s := status.Convert(err); s.Code() != codes.Unimplemented || !strings.Contains(s.Message(), "unknown service test.Nope") {
		t.Fatalf("unknown service = %v", err)
	}

	s := NewServer(UnknownServiceHandler(func(srv any, stream ServerStream) error {
		method, _ := Method(stream.Context())
		return stream.SendMsg(&Rsp{Msg: method})
	}))
	cc2, _ := dial(t, startHandler(t, s))
	var r Rsp
	if err := cc2.Invoke(ctx, "/any.Thing/Do", &Req{}, &r); err != nil || r.Msg != "/any.Thing/Do" {
		t.Fatalf("unknown service handler = %+v, %v", r, err)
	}
}

func TestInterceptors(t *testing.T) {
	var trace []string
	var mu sync.Mutex
	record := func(s string) {
		mu.Lock()
		trace = append(trace, s)
		mu.Unlock()
	}
	unary := func(name string) UnaryServerInterceptor {
		return func(ctx context.Context, req any, info *UnaryServerInfo, handler UnaryHandler) (any, error) {
			record(name + ">" + info.FullMethod)
			resp, err := handler(ctx, req)
			record("<" + name)
			return resp, err
		}
	}
	streamInt := func(srv any, ss ServerStream, info *StreamServerInfo, handler StreamHandler) error {
		record("stream>" + info.FullMethod)
		return handler(srv, ss)
	}
	_, addr := startServer(t, &echoImpl{}, UnaryInterceptor(unary("a")), ChainUnaryInterceptor(unary("b")), StreamInterceptor(streamInt))
	clientUnary := func(ctx context.Context, method string, req, reply any, cc *ClientConn, invoker UnaryInvoker, opts ...CallOption) error {
		record("client>" + method)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	clientStream := func(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, streamer Streamer, opts ...CallOption) (ClientStream, error) {
		record("cstream>" + method)
		return streamer(ctx, desc, cc, method, opts...)
	}
	_, c := dial(t, addr, WithUnaryInterceptor(clientUnary), WithStreamInterceptor(clientStream))
	ctx := testCtx(t)
	if _, err := c.Unary(ctx, &Req{Msg: "x"}); err != nil {
		t.Fatal(err)
	}
	count, err := c.Count(ctx, &Req{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := count.Recv(); err != nil {
			break
		}
	}
	mu.Lock()
	got := strings.Join(trace, " ")
	mu.Unlock()
	want := "client>/test.Echo/Unary a>/test.Echo/Unary b>/test.Echo/Unary <b <a cstream>/test.Echo/Count stream>/test.Echo/Count"
	if got != want {
		t.Fatalf("trace\n got %s\nwant %s", got, want)
	}
}

func TestMessageLimits(t *testing.T) {
	_, addr := startServer(t, &echoImpl{}, MaxRecvMsgSize(1024))
	_, c := dial(t, addr)
	ctx := testCtx(t)
	if _, err := c.Unary(ctx, &Req{Msg: strings.Repeat("x", 2048)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("over the server's limit = %v", err)
	}
	if _, err := c.Unary(ctx, &Req{Msg: "fits"}, MaxCallRecvMsgSize(4)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("over the client's limit = %v", err)
	}
	if _, err := c.Unary(ctx, &Req{Msg: "fits"}, MaxCallSendMsgSize(4)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("over the client's send limit = %v", err)
	}
	if r, err := c.Unary(ctx, &Req{Msg: "fits"}); err != nil || r.Msg != "fits" {
		t.Fatalf("within the limits = %v", err)
	}
}

// TestTaskPool checks where calls run: on the engine's stream pool by
// default, where one that blocks holds up nothing else of its connection, and
// on the pool TaskPool names.
func TestTaskPool(t *testing.T) {
	impl := &echoImpl{block: make(chan struct{}), entered: make(chan struct{}, 1)}
	s, addr := startServer(t, impl)
	_, c := dial(t, addr)
	ctx := testCtx(t)
	blocked := make(chan error, 1)
	go func() {
		_, err := c.Unary(ctx, &Req{Msg: "block"})
		blocked <- err
	}()
	<-impl.entered
	if r, err := c.Unary(ctx, &Req{Msg: "through"}); err != nil || r.Msg != "through" {
		close(impl.block)
		t.Fatalf("a blocked call held up its connection: %v", err)
	}
	s.mu.Lock()
	var pool *taskpool.TaskPool
	for tr := range s.transports {
		pool, _ = tr.taskPool().(*taskpool.TaskPool)
	}
	s.mu.Unlock()
	close(impl.block)
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
	if pool == nil || pool.Name() != "fib-streams" {
		t.Fatalf("calls ran on %v, want fib-streams", pool)
	}

	custom := &countingPool{TaskPool: taskpool.New("grpc-test", 8, 64)}
	t.Cleanup(custom.Stop)
	_, addr2 := startServer(t, &echoImpl{}, TaskPool(custom))
	_, c2 := dial(t, addr2)
	for range 3 {
		if _, err := c2.Unary(ctx, &Req{Msg: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	chat, err := c2.Chat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	chat.CloseSend()
	if _, err := chat.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	if n := custom.n.Load(); n != 4 {
		t.Fatalf("the custom pool ran %d calls, want 4", n)
	}
}

type countingPool struct {
	*taskpool.TaskPool
	n atomic.Int32
}

func (p *countingPool) GoTask(task taskpool.Task) bool {
	p.n.Add(1)
	return p.TaskPool.GoTask(task)
}

func TestMaxConcurrentStreams(t *testing.T) {
	impl := &echoImpl{block: make(chan struct{}), entered: make(chan struct{}, 4)}
	_, addr := startServer(t, impl, MaxConcurrentStreams(2))
	_, c := dial(t, addr)
	ctx := testCtx(t)
	// Settle the server's SETTINGS first.
	if _, err := c.Unary(ctx, &Req{Msg: "x"}); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 3)
	for range 3 {
		go func() {
			_, err := c.Unary(ctx, &Req{Msg: "block"})
			results <- err
		}()
	}
	<-impl.entered
	<-impl.entered
	select {
	case <-impl.entered:
		t.Fatal("a third call ran past MaxConcurrentStreams(2)")
	case <-time.After(200 * time.Millisecond):
	}
	close(impl.block)
	for range 3 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestTLS(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer()
	s.RegisterService(&echoDesc, &echoImpl{})
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	ln, err := net.Listen("tcp", config.Addr)
	if err != nil {
		t.Fatal(err)
	}
	config.Addr = ln.Addr().String()
	ln.Close()
	served := make(chan error, 1)
	go func() { served <- s.ServeTLS(config, serverTLS) }()
	t.Cleanup(func() {
		s.Stop()
		<-served
	})
	_, c := dial(t, config.Addr, WithTLSConfig(clientTLS))
	ctx := testCtx(t)
	var r *Rsp
	for {
		r, err = c.Unary(ctx, &Req{Msg: "secure"}, WaitForReady(true))
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil || r.Msg != "secure" {
		t.Fatalf("over TLS = %+v, %v", r, err)
	}
}

func TestGracefulStop(t *testing.T) {
	impl := &echoImpl{block: make(chan struct{}), entered: make(chan struct{}, 1)}
	s := NewServer()
	s.RegisterService(&echoDesc, impl)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	config := fib.DefaultConfig()
	config.Addr = addr
	served := make(chan error, 1)
	go func() { served <- s.Serve(config) }()
	_, c := dial(t, addr)
	ctx := testCtx(t)
	inFlight := make(chan error, 1)
	go func() {
		r, err := c.Unary(ctx, &Req{Msg: "block"}, WaitForReady(true))
		if err == nil && r.Msg != "block" {
			err = fmt.Errorf("got %q", r.Msg)
		}
		inFlight <- err
	}()
	<-impl.entered
	stopped := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(stopped)
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("GracefulStop returned with a call in progress")
	default:
	}
	close(impl.block)
	if err := <-inFlight; err != nil {
		t.Fatalf("the call in progress = %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("GracefulStop never returned")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Unary(ctx, &Req{Msg: "after"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a call after the server stopped = %v", err)
	}
}

// TestReconnect checks that a ClientConn connects again once its connection
// is lost.
func TestReconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	serve := func() (*Server, chan error) {
		s := NewServer()
		s.RegisterService(&echoDesc, &echoImpl{})
		config := fib.DefaultConfig()
		config.Addr = addr
		served := make(chan error, 1)
		go func() { served <- s.Serve(config) }()
		return s, served
	}
	s, served := serve()
	cc, c := dial(t, addr)
	ctx := testCtx(t)
	if _, err := c.Unary(ctx, &Req{Msg: "one"}, WaitForReady(true)); err != nil {
		t.Fatal(err)
	}
	s.Stop()
	<-served
	// Until the client has read the close, a call still goes out on the old
	// connection and fails with it: wait for it to let the connection go.
	for deadline := time.Now().Add(5 * time.Second); ; {
		cc.mu.Lock()
		lost := cc.t == nil
		cc.mu.Unlock()
		if lost {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the client never noticed the connection was lost")
		}
		time.Sleep(time.Millisecond)
	}
	s, served = serve()
	defer func() {
		s.Stop()
		<-served
	}()
	if r, err := c.Unary(ctx, &Req{Msg: "two"}, WaitForReady(true)); err != nil || r.Msg != "two" {
		t.Fatalf("after the server came back = %v", err)
	}
}

// TestServeAfterStop checks that a Server stopped before Serve made its
// engine does not then serve forever.
func TestServeAfterStop(t *testing.T) {
	s := NewServer()
	s.Stop()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	served := make(chan error, 1)
	go func() { served <- s.Serve(config) }()
	select {
	case err := <-served:
		if err != ErrServerStopped {
			t.Fatalf("Serve after Stop = %v, want ErrServerStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve after Stop never returned")
	}
}

func TestConcurrentCalls(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	_, c := dial(t, addr)
	ctx := testCtx(t)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				want := fmt.Sprintf("%d-%d-%s", g, i, strings.Repeat("y", i*50))
				r, err := c.Unary(ctx, &Req{Msg: want, N: i})
				if err != nil || r.Msg != want {
					errs <- fmt.Errorf("call %d/%d: %v", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestHeaderEncodings(t *testing.T) {
	for _, d := range []time.Duration{time.Nanosecond, 1500 * time.Microsecond, 3 * time.Second, 100 * time.Hour, 1 << 62} {
		got, ok := decodeTimeout(encodeTimeout(d))
		if !ok || got < d || got > d+d/100+time.Millisecond && d < time.Hour {
			t.Fatalf("timeout %v came back as %v", d, got)
		}
	}
	for _, s := range []string{"plain", "100%", "line\nbreak", "ünïcode"} {
		if got := decodeGRPCMessage(encodeGRPCMessage(s)); got != s {
			t.Fatalf("grpc-message %q came back as %q", s, got)
		}
	}
	if sub, ok := contentSubtype("application/grpc+json; charset=utf-8"); !ok || sub != "json" {
		t.Fatalf("subtype %q %v", sub, ok)
	}
	if _, ok := contentSubtype("application/json"); ok {
		t.Fatal("application/json taken for gRPC")
	}
}

// TestRawHTTP2 speaks HTTP/2 to the server by hand: a PING is answered, a
// request that is not gRPC gets an HTTP status, and a bad preface ends the
// connection.
func TestRawHTTP2(t *testing.T) {
	_, addr := startServer(t, &echoImpl{})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	enc := hpack.NewEncoder()
	block := enc.Begin(nil)
	for _, f := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":path", "/"}, {":authority", "x"}, {"content-type", "text/plain"}} {
		block = enc.AppendField(block, f[0], f[1], false)
	}
	out := append([]byte(preface), appendSettings(nil)...)
	out = appendPing(out, false, []byte("12345678"))
	out = appendHeaderBlock(out, 1, block, true, defaultMaxFrame)
	if _, err := conn.Write(out); err != nil {
		t.Fatal(err)
	}
	dec := hpack.NewDecoder(defaultHeaderTable)
	sawPong, sawStatus := false, ""
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for !sawPong || sawStatus == "" {
		head := make([]byte, frameHeaderLen)
		if _, err := io.ReadFull(conn, head); err != nil {
			t.Fatalf("pong %v status %q: %v", sawPong, sawStatus, err)
		}
		f := parseFrameHeader(head)
		payload := make([]byte, f.length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Fatal(err)
		}
		switch f.typ {
		case framePing:
			sawPong = f.has(flagAck) && string(payload) == "12345678"
		case frameHeaders:
			dec.Decode(payload, func(hf hpack.HeaderField) error {
				if hf.Name == ":status" {
					sawStatus = hf.Value
				}
				return nil
			})
		}
	}
	if sawStatus != "415" {
		t.Fatalf("a request that is not gRPC got %s", sawStatus)
	}

	bad, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	bad.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
	bad.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadAll(bad); err != nil {
		t.Fatalf("a bad preface was not answered by a close: %v", err)
	}
}
