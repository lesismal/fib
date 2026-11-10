# gRPC

[English](11-grpc.md) | [简体中文](11-grpc.zh-CN.md)

`fib/grpc` serves and calls gRPC over fib's own HTTP/2: a `grpc.Server` is a plain `fib.Handler` that
speaks h2c, or gRPC over TLS once wrapped in `fibtls.NewServer(grpc.ConfigureTLS(config), server)`,
and `grpc.NewClient` dials on the engine `grpc.WithEngine` names, or on one the package makes for
itself. Generated `protoc-gen-go-grpc` code works once
its imports name this package, but fib depends only on the standard library, so this chapter also
shows how to write a service descriptor by hand and use the built-in `json` codec — every complete
program here builds with fib alone. It assumes the bindings of [02-engine.md](02-engine.md) and the HTTP/2
transport of [07-http2.md](07-http2.md).

## The model

- **`Server` is a `fib.Handler`.** Bind it directly and it serves HTTP/2 with prior knowledge (h2c)
  on every connection; wrap it for TLS. Every call is an HTTP/2 stream with the gRPC framing
  (`application/grpc`, a five-byte message header, trailers carrying `grpc-status`).
- **Handlers run on a pool, not on the reader.** A call's handler runs on the engine's
  `HandlerPool` — the pool fib's HTTP/2 and HTTP/3 handlers use — or on the pool named by
  `grpc.TaskPool(pool)`. A unary call is scheduled once its request has arrived whole; a streaming
  call as soon as it opens, so a handler may block without holding up the connection's reader.
- **A client is one connection, many streams.** A `ClientConn` connects when its first call needs a
  connection (or at once with `Connect(ctx)`), reconnects after losing one, and is safe for
  concurrent use. Calls are streams; unary calls are a stream with one request and one reply. A
  call made while no connection can be had fails with `codes.Unavailable`, unless
  `grpc.WaitForReady(true)` makes it wait and retry for as long as its context lets it.
- **The wire is gRPC's.** It talks to `google.golang.org/grpc` clients and servers in both
  directions; the import rewrite below only changes whose code implements the protocol.

## Generated code and codecs

Generated code needs one rewrite: its `_grpc.pb.go` imports, and only those, must name fib's
packages. The `_pb.go` with the message types keeps its `google.golang.org/protobuf` imports.

```sh
sed -i 's#google.golang.org/grpc#github.com/lesismal/fib/grpc#' foo_grpc.pb.go
```

That substitution covers `google.golang.org/grpc/codes` and `.../status` too, since they are
prefixes of it. The package exports `SupportPackageIsVersion7`, `8` and `9` and the names generated
code uses (`ServiceDesc`, `MethodDesc`, `StreamDesc`, `RegisterService`, the typed stream wrappers),
so it compiles unchanged otherwise.

Messages are the codec's job. The default `proto` codec encodes messages that encode themselves:
`MarshalVT`/`UnmarshalVT` (vtprotobuf) or `Marshal`/`Unmarshal` (gogo/protobuf). Messages of
`google.golang.org/protobuf` cannot be encoded by a standard-library-only module, so a program that
uses them registers a codec of its own:

```go
type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }

func (protoCodec) Unmarshal(data []byte, v any) error { return proto.Unmarshal(data, v.(proto.Message)) }

func init() { grpc.RegisterCodec(protoCodec{}) }
```

This fragment needs `google.golang.org/protobuf` and its `proto` package; it replaces the built-in
`proto` codec under the same name. The built-in `json` codec needs nothing: a call whose content
subtype is `json` (`application/grpc+json`) sends and receives plain Go values through
`encoding/json`, which is what the examples below use.

## A server and a client without generated code

A generated `RegisterGreeterServer` is a `ServiceDesc` plus a `RegisterService` call; both can be
written by hand. The `Handler` of each method has the shape generated code emits: decode, then call
the method, through the interceptor when one is configured.

```go
package main

import (
	"context"
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc"
	"github.com/lesismal/fib/grpc/codes"
	"github.com/lesismal/fib/grpc/status"
)

type HelloRequest struct {
	Name string `json:"name"`
}

type HelloReply struct {
	Message string `json:"message"`
}

// GreeterServer is the service interface generated code would declare.
type GreeterServer interface {
	SayHello(context.Context, *HelloRequest) (*HelloReply, error)
}

type greeter struct{}

func (greeter) SayHello(ctx context.Context, in *HelloRequest) (*HelloReply, error) {
	if in.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is empty")
	}
	return &HelloReply{Message: "Hello, " + in.Name}, nil
}

// desc is the hand-written equivalent of a generated _grpc.pb.go's
// ServiceDesc: same fields, same handler shape.
var desc = grpc.ServiceDesc{
	ServiceName: "helloworld.Greeter",
	HandlerType: (*GreeterServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "SayHello",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(HelloRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(GreeterServer).SayHello(ctx, in)
			}
			info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/helloworld.Greeter/SayHello"}
			return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
				return srv.(GreeterServer).SayHello(ctx, req.(*HelloRequest))
			})
		},
	}},
}

func main() {
	server := grpc.NewServer()
	server.RegisterService(&desc, greeter{})

	serverEngine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:50051",
		Handler: server,
	})
	if err != nil {
		panic(err)
	}
	defer serverEngine.Close()
	go func() { _ = serverEngine.Run() }()

	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	conn, err := grpc.NewClient("127.0.0.1:50051", grpc.WithEngine(clientEngine))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var reply HelloReply
	err = conn.Invoke(ctx, "/helloworld.Greeter/SayHello",
		&HelloRequest{Name: "fib"}, &reply, grpc.CallContentSubtype("json"))
	if err != nil {
		log.Fatalf("call: %v (code %v)", err, status.Code(err))
	}
	log.Println(reply.Message)
}
```

`RegisterService` panics if the implementation does not satisfy `HandlerType`, or if the service is
registered twice, so services are registered before the server starts. The client names the method
with its full path (`/package.Service/Method`) and selects the `json` codec with
`CallContentSubtype`; generated clients name the method for you. The server-side service name and
method names are what the method path must match. The server picks the codec from each call's
content type, so only the client chooses `json`; `grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json"))`
on `NewClient` chooses it for every call of the `ClientConn`, and `grpc.ForceServerCodec` makes a
server use one codec whatever the call names. `NewClient` without `WithEngine` works too: the
package then makes an engine of its own the first time one is needed.

A server that should own its engine skips the binding: `Serve` makes the engine, listens and runs
until `Stop` or `GracefulStop`, then closes the engine and returns nil:

```go
server := grpc.NewServer()
server.RegisterService(&desc, greeter{})
go func() {
	if err := server.Serve(fib.DefaultConfig(), "127.0.0.1:50051"); err != nil {
		log.Print(err)
	}
}()
// ...
server.GracefulStop() // GOAWAY to every client, wait for the calls in progress, then stop
```

`Stop` closes every connection at once and fails the calls in progress; `GracefulStop` also works
on a `Server` bound to an engine of the caller's own, which the caller then closes. `ServeTLS` is
`Serve` over TLS.

## Streaming calls

A streaming method is a `StreamDesc` in the same `ServiceDesc`. `ServerStreams` and `ClientStreams`
say which directions stream: `ServerStreams` alone is a server-streaming call, `ClientStreams`
alone a client-streaming one, and both a bidirectional one. The handler receives the call's
`ServerStream`:

```go
Streams: []grpc.StreamDesc{{
	StreamName:    "SayHelloStream",
	ServerStreams: true,
	Handler: func(srv any, stream grpc.ServerStream) error {
		in := new(HelloRequest)
		if err := stream.RecvMsg(in); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			msg := &HelloReply{Message: fmt.Sprintf("%s %d", in.Name, i)}
			if err := stream.SendMsg(msg); err != nil {
				return err
			}
		}
		return nil
	},
}},
```

`RecvMsg` returns `io.EOF` once the client has sent all its messages, so a client-streaming handler
loops until then. The client opens the same method through `NewStream` and uses `SendMsg`, `RecvMsg`
and `CloseSend` directly:

```go
func callStream(ctx context.Context, conn *grpc.ClientConn) error {
	stream, err := conn.NewStream(ctx, &desc.Streams[0], "/helloworld.Greeter/SayHelloStream",
		grpc.CallContentSubtype("json"))
	if err != nil {
		return err
	}
	if err := stream.SendMsg(&HelloRequest{Name: "fib"}); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	for {
		var reply HelloReply
		err := stream.RecvMsg(&reply)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		log.Println(reply.Message)
	}
}
```

A bidirectional method sets both flags, and one loop receives and answers:

```go
Streams: []grpc.StreamDesc{{
	StreamName:    "Chat",
	ServerStreams: true,
	ClientStreams: true,
	Handler: func(srv any, stream grpc.ServerStream) error {
		for {
			in := new(HelloRequest)
			if err := stream.RecvMsg(in); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			if err := stream.SendMsg(&HelloReply{Message: "echo " + in.Name}); err != nil {
				return err
			}
		}
	},
}},
```

Every message is flushed as it is sent, under the stream's flow control, so a server stream reaches
the client message by message rather than all at once when the call ends. `GenericClientStream`
and `GenericServerStream`, the typed wrappers generated code uses to turn these methods into the
`Recv`/`Send` methods of the streaming interfaces, are exported for hand-written descriptors too.

## Metadata, deadlines, cancellation and status

Metadata travels as HTTP/2 headers and trailers. The client adds it to its context and reads the
server's header and trailer back through call options:

```go
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()
ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer token")

var header, trailer metadata.MD
var reply HelloReply
err := conn.Invoke(ctx, "/helloworld.Greeter/SayHello",
	&HelloRequest{Name: "fib"}, &reply,
	grpc.CallContentSubtype("json"),
	grpc.Header(&header), grpc.Trailer(&trailer))
switch status.Code(err) {
case codes.OK:
	log.Printf("reply %q, header %v, trailer %v", reply.Message, header, trailer)
case codes.DeadlineExceeded:
	log.Println("the server took too long")
default:
	log.Printf("call failed: %v", err)
}
```

The handler reads the incoming metadata, sets header and trailer, and honors the deadline. The
context's deadline becomes `grpc-timeout` on the wire; when it passes, the server's context is done
and the client gets `codes.DeadlineExceeded`. A client that cancels its context gets `codes.Canceled`
and the server sees the same in `ctx.Done()`:

```go
md, _ := metadata.FromIncomingContext(ctx)
if len(md.Get("authorization")) == 0 {
	return nil, status.Error(codes.Unauthenticated, "missing credentials")
}
if err := grpc.SetHeader(ctx, metadata.Pairs("x-served-by", "fib")); err != nil {
	return nil, err
}
grpc.SetTrailer(ctx, metadata.Pairs("x-cache", "miss"))
select {
case <-ctx.Done():
	return nil, status.FromContextError(ctx.Err()).Err()
default:
}
```

A call that fails before it has sent a message answers with one HEADERS frame, gRPC's
"trailers-only" response, so what `SetHeader` set arrives in the client's `Trailer`, and its
`Header` comes back empty. `status.Error`, `status.Errorf` and `status.FromContextError` build the
errors; `status.Code`, `status.Convert` and `status.FromError` inspect them. An ordinary Go error
becomes `codes.Unknown`, a panic in a handler becomes `codes.Internal` (and is logged with its
stack). `status.WithDetails` carries encoded
details as `grpc-status-details-bin`, which this package passes along as bytes without decoding.

## Interceptors, compression and task pools

Server interceptors wrap every unary or streaming call, outermost first; client interceptors do the
same on the call site:

```go
loggingUnary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Printf("%s took %v: %v", info.FullMethod, time.Since(start), err)
	return resp, err
}
pool := taskpool.New("grpc-calls", 64, 1024)
server := grpc.NewServer(
	grpc.UnaryInterceptor(loggingUnary),
	grpc.MaxRecvMsgSize(1<<20),
	grpc.MaxSendMsgSize(1<<20),
	grpc.MaxConcurrentStreams(200),
	grpc.TaskPool(pool),
)
```

`TaskPool` takes any `fib.TaskPool`; without it a call runs on the engine's `HandlerPool`, and a
call the pool refuses runs on a goroutine of its own. On the client, `WithUnaryInterceptor` and
`WithChainUnaryInterceptor` (and the streaming pair) wrap calls, `UseCompressor("gzip")` compresses a
call's messages — gzip is registered by default, so fib and grpc-go messages inflate on either side
— and `MaxCallRecvMsgSize`/`MaxCallSendMsgSize` bound messages. Server defaults: 4 MiB received,
effectively unbounded sent, 100 concurrent streams per connection.

## gRPC and HTTP on one port

`Server.ServeHTTP` serves a gRPC call that arrived through package `http`'s HTTP/2, so a router can
route the service's paths to the `Server` and everything else to the web application:

```go
package main

import (
	"context"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/grpc"
	fibhttp "github.com/lesismal/fib/http"
)

type HelloRequest struct {
	Name string `json:"name"`
}

type HelloReply struct {
	Message string `json:"message"`
}

type GreeterServer interface {
	SayHello(context.Context, *HelloRequest) (*HelloReply, error)
}

type greeter struct{}

func (greeter) SayHello(ctx context.Context, in *HelloRequest) (*HelloReply, error) {
	return &HelloReply{Message: "Hello, " + in.Name}, nil
}

var desc = grpc.ServiceDesc{
	ServiceName: "helloworld.Greeter",
	HandlerType: (*GreeterServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "SayHello",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(HelloRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(GreeterServer).SayHello(ctx, in)
		},
	}},
}

func main() {
	server := grpc.NewServer()
	server.RegisterService(&desc, greeter{})

	router := fibhttp.NewRouter()
	router.Handle("/helloworld.Greeter/*", server)
	router.Get("/", func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello over HTTP\n"))
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(router),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

The calls keep the Server's codecs, compressors and interceptors, but `MaxConcurrentStreams` and
`TaskPool` apply only to the Server's own h2c/TLS connections: here package `http`'s `Config` bounds
the streams and the handlers run on the engine's `HandlerPool`. A request that is not a gRPC call is
answered as grpc-go answers it (400 over HTTP/1, 405 for a non-POST, 415 for the wrong
`Content-Type`). With package `http`'s defaults a handler runs once the request has arrived whole,
which is fine for unary and server-streaming calls; a client-streaming call, above all a
bidirectional one, needs `Config.StreamRequestBody` so that it starts as its request starts arriving.

## TLS

For gRPC over TLS, use `fibtls.NewServer(grpc.ConfigureTLS(grpcTLS), server)` as the binding's
handler and give the client `grpc.WithTLSConfig(clientTLS)`: `grpc.ConfigureTLS` clones the config
and puts `h2` first in `NextProtos`, and `WithTLSConfig` makes the client offer h2 over TLS instead
of speaking h2c. In the first program, these are the only lines that change:

```go
serverTLS, clientTLS, err := tlstest.Configs() // a throwaway certificate, for trying it out
if err != nil {
	panic(err)
}
serverEngine, err := fib.New(fib.DefaultConfig(), fib.Binding{
	Addr:    "127.0.0.1:50443",
	Handler: fibtls.NewServer(grpc.ConfigureTLS(serverTLS), server),
})
// ...
conn, err := grpc.NewClient("127.0.0.1:50443",
	grpc.WithEngine(clientEngine), grpc.WithTLSConfig(clientTLS))
```

Certificates and verification are `crypto/tls`'s, as [chapter 10](10-tls.md) describes.

## Not implemented

The package documents what it does not do: name resolution and load balancing beyond a single
`host:port` target (`dns:///` and `passthrough:///` are accepted, but addresses are not
round-robined), retries and service config, keepalive pings, channelz, the health, reflection and
binary-log services, and decoding `grpc-status-details-bin`, which travels as bytes. Calls are
HTTP/2 only: a plain HTTP/1 request to a gRPC binding gets no HTTP response (the server answers
with an HTTP/2 GOAWAY and closes the connection), and `ServeHTTP` answers 400.

## Pitfalls

- **Close the `ClientConn` before the engine.** Closing the engine ends its connections without a
  word to the calls on them; `ClientConn.Close` fails in-flight calls with `codes.Canceled`.
- **Messages need a codec that knows them.** A `google.golang.org/protobuf` message fails with
  `codes.Internal` and `the proto codec cannot marshal ...` until the program registers a codec
  under `proto`; sending plain structs takes the `json` content subtype on the client, for every
  call (`CallContentSubtype` or `WithDefaultCallOptions`).
- **The method path is exact.** `/package.Service/Method` must match `ServiceName` and `MethodName`,
  case included; a mismatch is an `Unimplemented` from the server.
- **No reflection means no grpcurl by default.** Point tools at the `.proto` files instead, or add a
  reflection service of your own; this package does not ship one.
- **The default limits are real.** 4 MiB per received message and 100 concurrent streams per
  connection; raise them deliberately with `MaxRecvMsgSize` and `MaxConcurrentStreams` (and
  `MaxCallRecvMsgSize` on the client).

## See also

- [HTTP/2](07-http2.md) — the transport, flow control and extended CONNECT underneath gRPC
- [TLS](10-tls.md) — certificates, ALPN and the client options `WithTLSConfig` uses
- [arpc](12-arpc.md) — fib's own RPC protocol, simpler when you control both ends
- [Multiprotocol ports](15-multiprotocol.md) — gRPC beside HTTP, SOCKS and raw TCP on one port
- [The README](../../README.md) — the gRPC feature summary and wire-compatibility notes
- [docs/http2.md](../http2.md) — the reference for the HTTP/2 implementation gRPC rides on
