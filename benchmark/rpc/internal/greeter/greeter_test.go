//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package greeter

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/benchmark/rpc/internal/example"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

type server struct{}

func (server) SayHello(_ context.Context, in *HelloRequest) (*HelloReply, error) {
	return &HelloReply{Message: "hello, " + in.Name}, nil
}

func (server) SayHellos(in *HelloRequest, stream grpc.ServerStreamingServer[HelloReply]) error {
	return stream.Send(&HelloReply{Message: "hello, " + in.Name})
}

// garbled is a codec that calls itself json and sends what JSON cannot
// decode, or, with fail set, cannot encode at all.
type garbled struct{ fail bool }

func (g garbled) Marshal(any) ([]byte, error) {
	if g.fail {
		return nil, errors.New("cannot encode")
	}
	return []byte("{not json"), nil
}
func (garbled) Unmarshal([]byte, any) error { return errors.New("cannot decode") }
func (garbled) Name() string                { return "json" }

// connect serves the greeter with opts and returns a client of it.
func connect(t *testing.T, opts ...grpc.ServerOption) (*GreeterClient, *grpc.ClientConn) {
	t.Helper()
	s := grpc.NewServer(opts...)
	RegisterGreeterServer(s, server{})
	config := fib.DefaultConfig()
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: s})
	if err != nil {
		t.Fatal(err)
	}
	stop := example.Start(engine)
	t.Cleanup(func() { _ = stop() })
	clientEngine, stopClient := example.ClientEngine()
	t.Cleanup(stopClient)
	conn, err := grpc.NewClient(example.Addr(engine), grpc.WithEngine(clientEngine))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewGreeterClient(conn), conn
}

func callContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// A unary call goes through the server's interceptor, which the generated
// handler hands the request and the method's name.
func TestSayHelloThroughAnInterceptor(t *testing.T) {
	var method string
	client, _ := connect(t, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		method = info.FullMethod
		return handler(ctx, req)
	}))
	reply, err := client.SayHello(callContext(t), &HelloRequest{Name: "intercepted"})
	if err != nil || reply.Message != "hello, intercepted" {
		t.Fatalf("SayHello: %v, %v", reply, err)
	}
	if method != "/greeter.Greeter/SayHello" {
		t.Fatalf("interceptor saw %q", method)
	}
}

// Without an interceptor, a unary call reaches the server directly, and
// the streaming call streams its replies back.
func TestSayHelloAndSayHellos(t *testing.T) {
	client, _ := connect(t)
	ctx := callContext(t)
	reply, err := client.SayHello(ctx, &HelloRequest{Name: "direct"})
	if err != nil || reply.Message != "hello, direct" {
		t.Fatalf("SayHello: %v, %v", reply, err)
	}
	stream, err := client.SayHellos(ctx, &HelloRequest{Name: "streamed"})
	if err != nil {
		t.Fatal(err)
	}
	if reply, err := stream.Recv(); err != nil || reply.Message != "hello, streamed" {
		t.Fatalf("SayHellos: %v, %v", reply, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("after the reply: %v", err)
	}
}

// A request the server cannot decode fails the call, unary and streaming.
func TestUndecodableRequests(t *testing.T) {
	client, _ := connect(t)
	ctx := callContext(t)
	if _, err := client.SayHello(ctx, &HelloRequest{}, grpc.ForceCodec(garbled{})); err == nil {
		t.Error("SayHello took a request it cannot decode")
	}
	stream, err := client.SayHellos(ctx, &HelloRequest{}, grpc.ForceCodec(garbled{}))
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) == codes.OK {
		t.Errorf("SayHellos took a request it cannot decode: %v", err)
	}
}

// SayHellos reports a request it cannot send, and a connection that is
// closed.
func TestSayHellosClientErrors(t *testing.T) {
	client, conn := connect(t)
	ctx := callContext(t)
	if _, err := client.SayHellos(ctx, &HelloRequest{}, grpc.ForceCodec(garbled{fail: true})); err == nil {
		t.Error("sent a request that cannot be encoded")
	}
	_ = conn.Close()
	if _, err := client.SayHellos(ctx, &HelloRequest{}); err == nil {
		t.Error("opened a stream on a closed connection")
	}
}
