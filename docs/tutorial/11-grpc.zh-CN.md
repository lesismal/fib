# gRPC

[English](11-grpc.md) | [简体中文](11-grpc.zh-CN.md)

`fib/grpc` 在 fib 自己的 HTTP/2 之上提供并调用 gRPC：`grpc.Server` 就是一个普通的 `fib.Handler`，使用 h2c 协议，或用 `fibtls.NewServer(grpc.ConfigureTLS(config), server)` 包裹后在 TLS 上运行 gRPC；`grpc.NewClient` 则在 `grpc.WithEngine` 指定的引擎上拨号，不指定时用本包自己创建的引擎。`protoc-gen-go-grpc` 生成代码只要把 import 指向本包就能使用，但 fib 只依赖标准库，因此本章还会展示如何手写服务描述符并使用内置的 `json` 编解码器——这里的每个完整程序都只依赖 fib 本身即可构建。读者应已了解 [02-engine.md](02-engine.zh-CN.md) 的绑定与 [07-http2.md](07-http2.zh-CN.md) 的 HTTP/2 传输。

## 模型

- **`Server` 是 `fib.Handler`。** 直接绑定它，就在每条连接上提供 HTTP/2 先验知识（h2c）服务；需要 TLS 时把它包起来。每个调用都是一个带 gRPC 帧格式的 HTTP/2 stream（`application/grpc`、五字节消息头、携带 `grpc-status` 的 trailer）。
- **handler 运行在池上，而不是读连接的 worker 上。** 调用的 handler 运行在引擎的 `HandlerPool` 上——也就是 fib 的 HTTP/2 与 HTTP/3 handler 所用的池——或运行在 `grpc.TaskPool(pool)` 指定的池上。一元调用在请求完整到达后被调度，流式调用在 stream 打开时就被调度，因此 handler 阻塞不会拖住连接的读取者。
- **客户端是一条连接、多条流。** `ClientConn` 在第一个调用需要连接时建立连接（或用 `Connect(ctx)` 立即建立），连接丢失后自动重连，并且可以安全地并发使用。每个调用都是一条 stream；一元调用就是一条只有一请求一响应的 stream。连不上时发起的调用会以 `codes.Unavailable` 失败，除非 `grpc.WaitForReady(true)` 让它在 context 允许的时间内等待并重试。
- **线上格式是 gRPC 的。** 它可以在两个方向上与 `google.golang.org/grpc` 的客户端和服务端互通；下面的 import 替换只是改变由谁的代码实现这个协议。

## 生成代码与编解码器

生成代码只需要一处替换：`_grpc.pb.go` 的 import（也只有这些 import）必须指向 fib 的包。存放消息类型的 `_pb.go` 仍然保留它对 `google.golang.org/protobuf` 的 import。

```sh
sed -i 's#google.golang.org/grpc#github.com/lesismal/fib/grpc#' foo_grpc.pb.go
```

这一次替换同时覆盖 `google.golang.org/grpc`、`google.golang.org/grpc/codes` 与 `google.golang.org/grpc/status`，因为后两者是第一个的前缀。本包导出了 `SupportPackageIsVersion7`、`8`、`9` 以及生成代码用到的名字（`ServiceDesc`、`MethodDesc`、`StreamDesc`、`RegisterService`、带类型的 stream 包装器），所以除此之外无需改动即可编译。

消息由编解码器负责。默认的 `proto` 编解码器处理能自我编码的消息：`MarshalVT`/`UnmarshalVT`（vtprotobuf）或 `Marshal`/`Unmarshal`（gogo/protobuf）。只依赖标准库的模块无法编码 `google.golang.org/protobuf` 的消息，因此使用它们的程序要注册自己的编解码器：

```go
type protoCodec struct{}

func (protoCodec) Name() string { return "proto" }

func (protoCodec) Marshal(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }

func (protoCodec) Unmarshal(data []byte, v any) error { return proto.Unmarshal(data, v.(proto.Message)) }

func init() { grpc.RegisterCodec(protoCodec{}) }
```

这个片段需要 `google.golang.org/protobuf` 及其 `proto` 包；它用同一个名字替换内置的 `proto` 编解码器。内置的 `json` 编解码器则不需要任何外部依赖：内容子类型为 `json`（`application/grpc+json`）的调用通过 `encoding/json` 收发普通 Go 值，下面的示例正是这样做的。

## 不依赖生成代码的服务端与客户端

生成代码里的 `RegisterGreeterServer` 就是一个 `ServiceDesc` 加一次 `RegisterService` 调用；两者都可以手写。每个方法的 `Handler` 与生成代码产出的形状相同：先解码，再调用方法，配置了拦截器时经由拦截器调用。

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

如果实现不满足 `HandlerType`，或者同名服务被注册两次，`RegisterService` 会 panic，因此服务要在服务端开始工作之前注册。客户端用完整路径（`/package.Service/Method`）指明方法，并用 `CallContentSubtype` 选择 `json` 编解码器；生成的客户端会替你填好方法路径。服务端的方法路径必须与服务名、方法名一致。服务端按每个调用的 content type 选择编解码器，所以只需客户端选择 `json`；在 `NewClient` 上加 `grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json"))` 会让这个 `ClientConn` 的每个调用都使用它，而 `grpc.ForceServerCodec` 让服务端不管调用指明什么都使用同一个编解码器。不带 `WithEngine` 的 `NewClient` 也能用：本包会在第一次需要时创建自己的引擎。

想让服务端自己拥有引擎时可以省掉 binding：`Serve` 创建引擎、监听并运行，直到 `Stop` 或 `GracefulStop`，然后关闭引擎并返回 nil：

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

`Stop` 立即关闭所有连接，让进行中的调用失败；`GracefulStop` 对绑定在调用方自己引擎上的 `Server` 同样有效，之后由调用方关闭引擎。`ServeTLS` 就是 TLS 上的 `Serve`。

## 流式调用

流式方法就是同一个 `ServiceDesc` 中的 `StreamDesc`。`ServerStreams` 与 `ClientStreams` 指明哪个方向是流：只设 `ServerStreams` 是服务端流，只设 `ClientStreams` 是客户端流，两个都设是双向流。handler 拿到调用的 `ServerStream`：

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

客户端把所有消息发完后，`RecvMsg` 返回 `io.EOF`，因此客户端流的 handler 循环到它为止。客户端通过 `NewStream` 打开同一个方法，直接使用 `SendMsg`、`RecvMsg` 与 `CloseSend`：

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

双向方法把两个标志都设为 true，用一个循环收发：

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

每条消息在发送时就会在 stream 的流控约束下被刷出，因此服务端流会逐条到达客户端，而不是等调用结束才一次性给出。带类型的 `GenericClientStream` 与 `GenericServerStream` 正是生成代码用来把这些底层方法变成四种接口的 `Recv`/`Send` 方法的包装器；它们同样导出了，手写描述符也可以使用。

## 元数据、截止时间、取消与状态码

元数据以 HTTP/2 header 与 trailer 的形式传输。客户端把它加到 context 上，并通过调用选项取回服务端的 header 与 trailer：

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

handler 读取入站元数据，设置 header 与 trailer，并遵守截止时间。context 的截止时间会变成线上的 `grpc-timeout`；一旦超时，服务端的 context 结束，客户端收到 `codes.DeadlineExceeded`。客户端取消 context 会得到 `codes.Canceled`，服务端也会在 `ctx.Done()` 中看到同样的结果：

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

在发出任何消息之前就失败的调用只用一个 HEADERS 帧应答，即 gRPC 的 “trailers-only” 响应，因此 `SetHeader` 设置的内容会出现在客户端的 `Trailer` 里，`Header` 则为空。`status.Error`、`status.Errorf` 与 `status.FromContextError` 用来构造错误；`status.Code`、`status.Convert` 与 `status.FromError` 用来检查错误。普通 Go 错误会变成 `codes.Unknown`，handler 中的 panic 会变成 `codes.Internal`（并连同调用栈记入日志）。`status.WithDetails` 携带的编码详情通过 `grpc-status-details-bin` 传输，本包把它当字节原样转发，不做解码。

## 拦截器、压缩与协程池

服务端拦截器包住每个一元或流式调用，最外层最先执行；客户端拦截器在调用侧做同样的事：

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

`TaskPool` 接受任意 `fib.TaskPool`；不设置时调用运行在引擎的 `HandlerPool` 上。池拒绝接收的调用会改在独立协程上运行。客户端侧，`WithUnaryInterceptor` 与 `WithChainUnaryInterceptor`（以及对应的流式版本）包住调用，`UseCompressor("gzip")` 压缩调用的消息——gzip 默认已注册，因此 fib 与 grpc-go 编码的消息在两侧都能解压——`MaxCallRecvMsgSize`/`MaxCallSendMsgSize` 则限制消息大小。服务端的默认值是接收 4 MiB、发送基本不设限、每条连接 100 条并发 stream。

## 同一端口上的 gRPC 与 HTTP

`Server.ServeHTTP` 可以服务经由 `http` 包的 HTTP/2 到达的 gRPC 调用，因此路由可以把服务路径交给 `Server`，其余请求交给 Web 应用：

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

这些调用仍然使用 Server 的编解码器、压缩器与拦截器，但 `MaxConcurrentStreams` 与 `TaskPool` 只对 Server 自己的 h2c/TLS 连接生效：这里由 `http` 包的 `Config` 限制 stream 数量，handler 运行在引擎的 `HandlerPool` 上。不是 gRPC 调用的请求会按 grpc-go 的方式应答（HTTP/1 上 400，非 POST 405，`Content-Type` 不对 415）。使用 `http` 包的默认配置时，handler 在请求完整到达后才运行，这适合一元调用与服务端流调用；客户端流调用（尤其是双向流）需要 `Config.StreamRequestBody`，让调用在请求开始到达时就开始。

## TLS

要在 TLS 上运行 gRPC，把 `fibtls.NewServer(grpc.ConfigureTLS(grpcTLS), server)` 用作绑定的 handler，并给客户端 `grpc.WithTLSConfig(clientTLS)`：`grpc.ConfigureTLS` 克隆配置并把 `h2` 放到 `NextProtos` 最前面，`WithTLSConfig` 让客户端在 TLS 上提议 h2，而不是使用 h2c。在第一个程序里，只有这几行需要改：

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

证书与校验属于 `crypto/tls`，见 [第 10 章](10-tls.zh-CN.md)。

## 未实现的部分

本包在文档里写明了它不做什么：除单个 `host:port` 目标之外的名字解析与负载均衡（接受 `dns:///` 与 `passthrough:///` 前缀，但不会在解析出的多个地址间轮询）、重试与服务配置、keepalive ping、channelz，以及 health、reflection 与 binary-log 服务；此外 `grpc-status-details-bin` 只作为字节传递，不做解码。调用只支持 HTTP/2：发往 gRPC 绑定的普通 HTTP/1 请求得不到 HTTP 响应（服务端回一个 HTTP/2 GOAWAY 后关闭连接），`ServeHTTP` 则回以 400。

## 坑与限制

- **先关闭 `ClientConn`，再关闭引擎。** 关闭引擎会在不通知调用的情况下结束它的连接；`ClientConn.Close` 则会让进行中的调用以 `codes.Canceled` 失败。
- **消息需要认识它的编解码器。** `google.golang.org/protobuf` 的消息会以 `codes.Internal` 和 `the proto codec cannot marshal ...` 失败，直到程序以 `proto` 为名注册编解码器；发送普通结构体则要求客户端的每个调用都使用 `json` 内容子类型（`CallContentSubtype` 或 `WithDefaultCallOptions`）。
- **方法路径必须精确。** `/package.Service/Method` 必须与 `ServiceName` 和 `MethodName` 完全一致（包括大小写）；不一致时服务端返回 `Unimplemented`。
- **没有 reflection，grpcurl 默认用不了。** 请让工具读取 `.proto` 文件，或自行添加 reflection 服务；本包不附带。
- **默认限制是真实存在的。** 每条接收消息 4 MiB、每条连接 100 条并发 stream；要放宽请有意识地使用 `MaxRecvMsgSize` 与 `MaxConcurrentStreams`（客户端还有 `MaxCallRecvMsgSize`）。

## 相关文档

- [HTTP/2](07-http2.zh-CN.md) —— gRPC 底下的传输、流控与扩展 CONNECT
- [TLS](10-tls.zh-CN.md) —— `WithTLSConfig` 用到的证书、ALPN 与客户端选项
- [arpc](12-arpc.zh-CN.md) —— fib 自己的 RPC 协议，两端都由你控制时更简单
- [单端口多协议](15-multiprotocol.zh-CN.md) —— 在同一个端口上同时跑 gRPC、HTTP、SOCKS 与原生 TCP
- [README](../../README.zh-CN.md) —— gRPC 特性概览与线上格式兼容性说明
- [docs/http2.md](../http2.zh-CN.md) —— gRPC 所依赖的 HTTP/2 实现参考
