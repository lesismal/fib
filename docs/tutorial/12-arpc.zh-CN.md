# arpc

[English](12-arpc.md) | [简体中文](12-arpc.zh-CN.md)

arpc 是 [lesismal/arpc](https://github.com/lesismal/arpc) 那套紧凑的双向 RPC 协议，由 fib 的
Engine 来跑：两个对端共享一条连接，任一方都可以调用对方的方法、发单向通知、开流，并向所有连接
广播。它的帧很精简——16 字节头部、方法名、负载——线格式与原包一致，所以 fib 端点
能和 `lesismal/arpc` 端点互通。本章讲服务端、客户端、handler 在哪里执行、Context、流、中间件、
编解码器、TLS 与重连。本章沿用第 03 章的 handler 模型：一条连接一次只在一个 worker 上处理一轮，
因此 handler 不能阻塞。

## 协议模型

- **服务端就是一个 `fib.Handler`。** `arpc.NewServer()` 返回的值放进 `fib.Binding` 即可；需要
  TLS 时先包一层 `fibtls.NewServer`。`Server.Handler` 持有方法表、中间件、编解码器和回调，所有
  被接受的连接共享同一份。
- **客户端就是一条连接，两端都一样。** `arpc.Dial` / `arpc.NewClient` 打开“拨号客户端”，连接
  断开后它会自行重连；`Server` 为每一条被接受的连接创建一个 `*arpc.Client`。两端都能调用对方、
  也都能服务对方。
- **默认情况下 handler 不在读路径上执行。** 在 `Handler.SetAsyncResponse(true)`（默认值）之后
  注册的方法跑在 Engine 的协程池上——就是 HTTP/2、HTTP/3 handler 用的那个池——所以慢 handler
  不会卡住自己的连接。`SetAsyncResponse(false)`、或 `Handle` / `HandleStream` 的可选 `bool`
  参数，让 handler 在连接被读取的位置执行，一次处理一条消息；这是唯一有确定顺序的模式，而在这种
  模式下调用同一个对端并等待应答会死锁。
- **线格式是固定的。** 16 字节小端头部，然后是方法名，最后是负载：

```text
[0, 4)  body length: len(method) + len(payload)
[4]     reserved for the application's flag bits (Message.SetFlagBit)
[5]     command in the low six bits; a stream's EOF and local bits above
[6]     flags: error response 0x01, asynchronous call 0x02
[7]     method length
[8, 16) sequence number
```

`Codec` 把 Go 值变成负载；除非 Server 或 Client 另行指定，它就是 `JSONCodec`，而 `[]byte`、
`string`、`error` 值会原样通过，不经过它。

## 一个服务端与客户端

先在一个 fib Binding 上跑 echo 服务端，再用一个自己的 Engine 建客户端：

```go
package main

import (
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
)

func main() {
	server := arpc.NewServer()
	server.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var msg string
		if err := ctx.Bind(&msg); err != nil {
			_ = ctx.Error(err)
			return
		}
		_ = ctx.Write(msg)
	})

	srv, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:8888", Handler: server})
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() { _ = srv.Run() }()

	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	client, err := arpc.Dial(clientEngine, "tcp", "127.0.0.1:8888", 3*time.Second, nil)
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	var rsp string
	if err := client.Call("/echo", "hello arpc", &rsp, time.Second); err != nil {
		panic(err)
	}
	log.Println(rsp)
}
```

`Call` 用客户端的 codec 编码请求、发送、然后等应答并解码进 `rsp`；错误应答会作为非 nil 的
`error` 返回。`Dial` 的最后一个参数是 `*arpc.Handler`——nil 表示用 `arpc.DefaultHandler` 的
克隆——并且要等连接建立后才返回。如果希望 Server 自己创建并运行 Engine，可以用
`Server.Run(addr)` 和 `Server.Serve(config, addr)` 两个捷径。

## Handler 与 Context

每个请求或通知都以 `*arpc.Context` 交给 `HandlerFunc`。Context 持有消息、消息来自的客户端以及
解码器：

```go
type addReq struct{ A, B int }
type addRsp struct{ Sum int }

server.Handler.Handle("/add", func(ctx *arpc.Context) {
	var req addReq
	if err := ctx.Bind(&req); err != nil {
		_ = ctx.Error(err) // arrives at the caller as the Call error
		return
	}
	_ = ctx.Write(addRsp{Sum: req.A + req.B})
	ctx.Set("served-by", "add") // values stay on this side; they are never sent
})

server.Handler.Handle("/bytes", func(ctx *arpc.Context) {
	_ = ctx.Write(ctx.Body()) // a []byte payload goes back exactly as it came
})
```

客户端方向也遵循同样的解码规则：

```go
var sum addRsp
if err := client.Call("/add", addReq{A: 2, B: 3}, &sum, time.Second); err != nil {
	panic(err)
}
var raw []byte
if err := client.Call("/bytes", []byte{0x00, 0x01, 0x02}, &raw, time.Second); err != nil {
	panic(err)
}
```

`Bind` 对 `*[]byte` 给出负载本身，对 `*string` 给出副本，其余类型交给 codec 解码。`Write` 收到
`error` 时会发错误应答，`Error` 也一样。还可以用 `Handler.Register` 注册整个服务：导出的方法
`func(ctx context.Context, req *Req, rsp *Rsp)` 会被路由为 `name.Method`，请求绑定和应答写回都由
反射完成；而 `func(*arpc.Context)` 形式的方法会按原样注册：

```go
type Greeter struct{}

func (Greeter) Hello(ctx context.Context, req *addReq, rsp *addRsp) {
	rsp.Sum = req.A + req.B
}

if err := server.Handler.Register("math", Greeter{}); err != nil {
	panic(err) // the "math.Hello" route now answers calls
}
```

## 异步调用与通知

`Call` 会阻塞调用方。`CallAsync` 发出请求后立刻返回；它的回调只执行一次：有应答时在连接被读取的
位置执行，超时、重连或停止时带着错误在协程池上执行。`Notify` 发出的是完全不期待应答的消息：

```go
package main

import (
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
)

func main() {
	server := arpc.NewServer()
	server.Handler.Handle("/slow", func(ctx *arpc.Context) {
		time.Sleep(100 * time.Millisecond) // stand-in for real work
		_ = ctx.Write(ctx.Body())
	})
	server.Handler.Handle("/event", func(ctx *arpc.Context) {
		log.Printf("event: %s", ctx.Body())
	})

	srv, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:8889", Handler: server})
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() { _ = srv.Run() }()

	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	client, err := arpc.Dial(clientEngine, "tcp", "127.0.0.1:8889", 3*time.Second, nil)
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	done := make(chan struct{})
	if err := client.CallAsync("/slow", "async request", func(ctx *arpc.Context, err error) {
		defer close(done)
		if err != nil {
			log.Printf("async call failed: %v", err) // ErrTimeout, ErrClientReconnecting, ...
			return
		}
		var rsp string
		if err := ctx.Bind(&rsp); err != nil {
			log.Printf("bind: %v", err)
			return
		}
		log.Printf("async response: %s", rsp)
	}, 2*time.Second); err != nil {
		panic(err)
	}
	if err := client.Notify("/event", "fired once", time.Second); err != nil {
		panic(err)
	}
	<-done
}
```

通知的 handler 如果调 `ctx.Write` 会拿到 `ErrContextResponseToNotify`：对端根本不等待任何应答。

## 受 context 约束的调用、未知方法、客户端池与 singleflight

`CallContext` 就是用 `context.Context` 而不是超时来约束的 `Call`。请求一个没有注册 handler 的
方法会得到错误应答 `method not found`（`ErrMethodNotFound`），除非用 `HandleNotFound` 为所有这类
方法注册一个 handler。`ClientPool` 是一组固定数量的拨号客户端；`Handler.Singleflight` 让一个
客户端对同一方法、同一请求的并发调用共用一次往返：

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
if err := client.CallContext(ctx, "/echo", "bounded by ctx", &rsp); err != nil {
	log.Print(err)
}

server.Handler.HandleNotFound(func(ctx *arpc.Context) {
	_ = ctx.Error("no such method: " + ctx.Message.Method())
})

handler := arpc.NewHandler()
handler.Singleflight("/profile") // calls share by fmt.Sprint(req) unless a key function is given
pool, err := arpc.NewClientPool(
	arpc.TCPDialer(clientEngine, "tcp", "127.0.0.1:8888", 3*time.Second), 4, handler)
if err != nil {
	panic(err)
}
defer pool.Stop()
err = pool.Next().Call("/profile", "alice", &rsp, time.Second) // round-robin over the 4 clients
```

池里的客户端共享同一个 `Handler`，所以 singleflight 方法是按客户端合并调用的，而不是跨整个池。

## 流

流（Stream）是在一条 Client 之上、绑定到某个方法上的双向消息序列。任一端都可以用 `NewStream`
为对端用 `HandleStream` 注册过的方法打开一条流，对端的 handler 在流的第一条消息到达时运行；下面
由拨号客户端打开、服务端来服务。两端各自独立关闭发送半部和接收半部，对端关闭发送半部后 `Recv`
返回 `io.EOF`：

```go
server.Handler.HandleStream("/upper", func(st *arpc.Stream) {
	defer st.CloseSend()
	for {
		var msg string
		if err := st.Recv(&msg); err != nil {
			return // io.EOF when the client closed its sending half
		}
		if err := st.Send(strings.ToUpper(msg)); err != nil {
			return
		}
	}
})

st := client.NewStream("/upper")
defer st.CloseSend()
if err := st.Send("hello"); err != nil {
	panic(err)
}
var upper string
if err := st.Recv(&upper); err != nil {
	panic(err)
}
```

流消息在线格式上带的是流命令，两端都不会把它和普通调用混淆。`Handler.SetStreamQueueSize` 限制
一条流为它的 handler 缓存多少条消息，默认 4 条；流满了的连接会暂停读取，直到 handler 取走一条。
`RecvContext` 用 context 约束一次等待，`SendAndClose` 一次调用里发出最后一条消息并关闭发送半部。
中间件不作用于流：流方法没有调用链。

## 广播与生命周期回调

每个被接受的连接都是服务端持有的 `*arpc.Client`。`Broadcast` 向所有连接发通知，
`BroadcastWithFilter` 只发给回调挑中的连接，`ForEach` 用来遍历它们：

```go
server.Handler.HandleConnected(func(c *arpc.Client) {
	log.Printf("joined: %v", c.Conn().RemoteAddr())
})
server.Handler.HandleDisconnected(func(c *arpc.Client) {
	log.Print("left")
})

server.Broadcast("/tick", time.Now().UnixMilli())
server.BroadcastWithFilter("/tick", "vip only", func(c *arpc.Client) bool {
	plan, _ := c.Get("plan") // values a handler or callback stored on the client
	return plan == "vip"
})
```

这些回调跑在协程池上：`HandleConnected` 在服务端接受连接时执行，也在拨号客户端第一次连上以及每次重连成功时执行，因此它
返回之前连接的首批消息可能已经被处理；`HandleDisconnected` 在客户端停止时执行。`Server.MaxLoad` 限制
服务端同时服务的连接数，超出的连接一被接受就关闭；`Server.Accepted` 和 `Server.CurrLoad` 给出
计数。

## 中间件与 Handler 的执行位置

`Handler.Use` 为每个方法添加中间件，包括已经注册的和之后注册的方法，按链的执行顺序生效。链是
按方法组装的：在方法之前注册的中间件在 handler 之前执行，之后注册的在 handler 之后执行。中间件
返回后链会自动继续，所以 `Next` 只用于在链的剩余部分之后再做事。中间件可以调用 `ctx.Next()` 执行链的
剩余部分然后再做自己的事，或者用 `ctx.Abort()` 停止整条链：

```go
server.Handler.Use(func(ctx *arpc.Context) {
	start := time.Now()
	ctx.Next() // the rest of the chain: the method handler and later middlewares
	log.Printf("%s took %v", ctx.Message.Method(), time.Since(start))
})
server.Handler.Use(func(ctx *arpc.Context) {
	if string(ctx.Body()) == "deny" {
		_ = ctx.Error("denied by middleware")
		ctx.Abort()
	}
})
server.Handler.Handle("/echo", func(ctx *arpc.Context) { _ = ctx.Write(ctx.Body()) })

server.Handler.SetAsyncResponse(false) // from here on: run where the connection is read
server.Handler.Handle("/count", func(ctx *arpc.Context) { /* must not block */ }, true)
```

默认是异步：handler 在 `Handler.TaskPool()` 上执行，也就是用 `Handler.SetTaskPool` 安装的池，
否则就是 Engine 的 `HandlerPool`。`SetAsyncResponse(false)`、单个方法传 `false`、或
`HandleStream(..., false)` 会把 handler 留在连接的读路径上。同一条连接的异步 handler 可能并发、
乱序执行；要求按到达顺序处理的方法要选同步模式。

## 编码器与 TLS

`MessageCoder` 在线上变换消息——压缩、加密、签名都用它。编码器对发出的消息按添加顺序执行，对
收到的消息按相反顺序执行，所以对称的编码器要在两端都安装：

```go
type xorCoder struct{ key byte }

func (c xorCoder) apply(m *arpc.Message) *arpc.Message {
	for i := arpc.HeadLen + m.MethodLen(); i < len(m.Buffer); i++ { // payload only
		m.Buffer[i] ^= c.key
	}
	return m
}
func (c xorCoder) Encode(_ *arpc.Client, m *arpc.Message) *arpc.Message { return c.apply(m) }
func (c xorCoder) Decode(_ *arpc.Client, m *arpc.Message) *arpc.Message { return c.apply(m) }

server.Handler.UseCoder(xorCoder{0x5a})

handler := arpc.NewHandler() // the client's Handler, configured before it dials
handler.UseCoder(xorCoder{0x5a})
client, err := arpc.Dial(clientEngine, "tcp", "127.0.0.1:8888", 3*time.Second, handler)
```

TLS 就是在同一个 handler 前套一层 `tls` 包，客户端用 `TLSDialer`：

```go
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Addr: "127.0.0.1:8443", Handler: fibtls.NewServer(serverTLS, server)})
if err != nil {
	panic(err)
}
client, err := arpc.NewClient(
	arpc.TLSDialer(engine, "tcp", "127.0.0.1:8443", 3*time.Second, clientTLS), nil)
```

TLS 内部跑的线格式不变，所以任何信任该证书的 TLS 拨号方都能连上一个 arpc 端点。

## 重连

拨号客户端在连接断开后会自行重连，每次间隔 `ReconnectInterval`，直到 `MaxReconnectTimes` 允许的
次数用完为止（零或负数，即默认，表示一直重试）。没有连接期间，调用返回
`ErrClientReconnecting`，`CheckState` 也报告这个状态。回调能描述整个过程：

```go
handler := arpc.NewHandler()
handler.SetReconnectInterval(time.Second)
handler.SetMaxReconnectTimes(0) // 0: never give up
handler.HandleReconnect(func(c *arpc.Client, info *arpc.ReconnectInfo) {
	log.Printf("attempt %d/%d to %s: success=%v err=%v",
		info.Times, info.MaxTimes, info.Addr, info.Success, info.Err)
})
client, err := arpc.NewClient(
	arpc.TCPDialer(engine, "tcp", "127.0.0.1:8888", 3*time.Second), handler)
if err != nil {
	panic(err)
}
defer client.Stop()
client.Keepalive(30 * time.Second) // ping the peer so a dead path is noticed
```

重连过的客户端并没有“停止”：它不会触发 `HandleDisconnected`。`Stop` 关闭连接并终止重连；已停止
的客户端返回 `ErrClientStopped`。关闭 Engine 之前要先停止所有客户端，因为关闭 Engine 会直接丢弃
连接、不会通知它们。

## 与 lesismal/arpc 的线格式兼容性

头部、命令字、方法与负载布局、流标志位以及 error/async 标志都是原包的，所以 `lesismal/arpc`
的客户端能连 fib 的 `arpc.Server`，fib 客户端也能连 `lesismal/arpc` 服务端。差异在发送路径和
已经不存在的选项上：

- fib 通过 `fib.Connection` 的非阻塞 `Send` 发送，其队列和水位线限制等待 socket 的数据量。没有
  发送队列、没有发送 goroutine，也没有可调项：batch、writev、发送队列和 buffer 大小选项都不再
  存在，读取方向的选项也一样。
- 消息立刻进入连接，所以 `Notify`、`PushMsg` 和 `Context.WriteWithTimeout` 的 timeout 限不住
  任何东西。
- `Client` 没有 `Restart`：拨号客户端自行重连。
- `Handler` 保留了其余 API：方法注册、中间件、编码器、服务注册、singleflight
  （`Handler.Singleflight`）、连接回调和协程池。

## 坑与限制

- **不要在同步 handler 里调用同一个对端并等待。** 跑在读路径上的 handler 会阻塞这条连接的读取，
  对端的应答永远读不到，调用最终超时。这类方法保持默认的异步模式，或者从另一条连接发起调用。
- **handler 返回后不要继续持有 `*arpc.Context`。** 开启 `Handler.EnablePool` 后它会被放回池中，
  而 `Bind` 给出的 `*[]byte` 只在对应 Message 存活期间有效。需要的就复制出来，或对
  `Message.Retain`。
- **异步 handler 没有顺序。** 两个异步处理的调用可能以任意顺序完成；要求按到达顺序处理的方法要
  用 `false` 注册成同步。
- **通知不是调用。** 通知的 handler 没有应答可写（`ctx.Write` 返回
  `ErrContextResponseToNotify`），而写不出任何应答的请求 handler 会让调用方一直等到调用超时。
- **了解这些上限。** 收到的 body 超过 `Handler.MaxBodyLen`（`DefaultMaxBodyLen`，64 MiB − 16）
  会关闭连接；在两端都用 `SetMaxBodyLen` 放宽。方法名长 1 到 `MaxMethodLen`（127）字节：`Handle`
  遇到更长的会 panic，发送会失败，收到方法长度非法的消息则直接丢弃。
- **在客户端使用之前把 Handler 配置好。** 有客户端在服务时修改 `Handler` 的方法和中间件是不安全
  的，而一个 `Server` 的所有连接共享同一个 Handler。
- **调用超时不会取消服务端的 handler。** 迟到的应答会交给 `HandleSessionMiss`，活儿已经干完了。
- **Engine 关闭前先停止客户端。** 关闭 Engine 会直接丢弃连接、不通知它们的 handler，异步调用就
  收不到回调。

## 相关文档

- [`arpc` 包](../../arpc) 与它的包文档，完整 API 参考。
- 根 README 的 [arpc 一节](../../README.zh-CN.md#arpc)。
- [第 11 章：gRPC](11-grpc.zh-CN.md) 讲另一种 RPC 协议，[第 13 章：SOCKS 与 HTTP
  CONNECT](13-socks-connect.zh-CN.md) 讲代理与隧道。
- [第 15 章：单端口多协议](15-multiprotocol.zh-CN.md) 讲在共享端口上跑 arpc，
  [第 20 章：客户端](20-clients.zh-CN.md) 讲客户端生命周期。
- [docs/hacks.zh-CN.md](../hacks.zh-CN.md) 里同样适用于 arpc handler 的异步响应、cork 与池技巧。
