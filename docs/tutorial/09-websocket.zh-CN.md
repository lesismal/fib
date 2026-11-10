# WebSocket

[English](09-websocket.md) | [简体中文](09-websocket.zh-CN.md)

`fib/websocket` 在 fib 的连接之上实现了 RFC 6455：`websocket.ServerHandler` 负责完成握手、在字节到达时解析帧并以整条消息回调你的 `Handler`，而它交给你的 `Connection` 可以从任意协程发送消息、ping 和关闭帧。同一个 handler 既可以绑定在独立端口上提供 WebSocket，也可以把 HTTP/1.1、HTTP/2 或 HTTP/3 服务中的一个请求升级为 WebSocket；客户端则可以在不阻塞调用方的情况下拨号 `ws://` 与 `wss://`。本章假定你已了解 [03-handlers.md](03-handlers.zh-CN.md) 的 handler 模型，以及 [06-http1.md](06-http1.zh-CN.md) 与 [07-http2.md](07-http2.zh-CN.md) 中的 HTTP 服务。

## 模型

- **一个 handler，两个入口。** `websocket.NewHandler(handler)` 返回一个 `fib.Handler`，在它绑定的连接上使用 WebSocket 协议。另一个入口是 `Upgrade`：它把 `http` 包正在处理的某个请求变成 WebSocket，从而让 WebSocket 与普通 HTTP 共用一个端口。
- **回调是串行的。** 对同一条连接，`Open`、随后的每条消息、每个帧、ping 与 pong、最后的 `Close` 依次执行，绝不会同时运行两个。它们运行在引擎的 worker 上；经 `Upgrade` 升级时，`Open` 已经在 `Upgrade` 内部、也就是 HTTP handler 的协程上运行过了。不同连接的回调可能在不同 worker 上同时运行，因此连接之间共享的状态需要加锁。
- **payload 是借用的。** `OnMessage` 与 `OnFrame` 收到的切片只在回调执行期间有效；要保留内容，必须先复制。交给 `Open` 的 `*http.Request` 是握手请求，`Open` 返回后 HTTP 服务可能复用它：复制需要的字段（某个 cookie、用户 ID），不要保留请求本身。
- **写操作安全且从不阻塞。** `WriteMessage`、`WriteText`、`WriteBinary`、`WritePreparedMessage`、`Ping`、`Pong` 与 `Close` 都可以在应用协程中调用。发送只是把帧放进队列就返回；不读数据的对端会让队列持续增长，这种对端应由你主动关闭。
- **限制。** `Config.MaxMessageBytes`（`websocket.DefaultConfig()` 中为 16 MiB）限制一条消息重组后的大小；控制帧不计入，非法的 Text payload 会以 1007 关闭连接。收到 ping 会自动回复携带相同 payload 的 pong，但自定义 `HandlerFuncs.Ping` 会取代这个默认回复，因此它必须自己调用 `Connection.Pong`。

## 独立绑定的回显服务

把 WebSocket handler 直接绑定到一个端口，就是一个只讲 WebSocket 的服务：

```go
package main

import (
	"log"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

func main() {
	handler := websocket.NewHandler(websocket.HandlerFuncs{
		Open: func(c *websocket.Connection, r *stdhttp.Request) {
			log.Printf("open from %s", r.RemoteAddr)
		},
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data)
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			log.Printf("close %d %q: %v", code, reason, err)
		},
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: handler,
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

`HandlerFuncs` 这个结构体省去了手写六个方法的麻烦：`Open`、`Message`、`Close`、`Frame`、`Ping` 与 `Pong`。`Ping` 为 nil 时保留默认的 pong 回复；`Pong` 为 nil 时丢弃收到的 pong。这里 `Connection` 写出的内容就是客户端读到的：`WriteMessage` 接受任意数据帧 opcode，`WriteText` 与 `WriteBinary` 是两种简写，`Close(code, reason)` 发送关闭帧。可以用任意客户端测试，例如 `websocat ws://127.0.0.1:8080/`，每一行都会原样回显；[下文](#客户端)的客户端程序也可以。对这个端口发普通 HTTP 请求会得到 `400 Bad Request`。

## 从 HTTP 服务升级

WebSocket 通常和打开它的网站共用一个端口。在 HTTP handler 内部把对应路径交给 `ServerHandler.Upgrade`：它会写出握手响应并返回 `*websocket.Connection`，之后同一条连接就由同一个 `Handler` 服务。

```go
ws := websocket.NewHandler(websocket.HandlerFuncs{
	Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
		_ = c.WriteMessage(op, data)
	},
})
app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	if c.Request.URL.Path == "/ws" {
		_, _ = ws.Upgrade(c, nil) // the header argument adds response headers; nil sends none
		return
	}
	_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
})
engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
	Addr:    "127.0.0.1:8080",
	Handler: fibhttp.NewHandler(app),
})
```

`Upgrade` 的行为取决于请求本身的协议。在 HTTP/1.1 上它就是 RFC 6455 的握手，整条连接切换为 WebSocket。在 HTTP/2 与 HTTP/3 上它是 `:protocol` 为 `websocket` 的扩展 CONNECT（RFC 8441、RFC 9220）：只有该 stream 切换，连接上的其他请求照常服务，`http` 包会发送 SETTINGS 引导浏览器尝试。`Config` 中的来源校验、子协议与压缩在两种情况下都生效。不是合法握手的请求由 `Upgrade` 自己应答——400 Bad Request，来源校验拒绝时为 403 Forbidden——并通过返回的 error 说明原因，handler 只需记录日志。`Upgrade` 返回后应立刻从 HTTP handler 返回，连接从此归 WebSocket handler 所有。

## 用 PreparedMessage 广播

一条广播消息应当只校验、只组帧一次，而不是每个连接各做一次。`NewPreparedMessage` 正是这样做的，而且它不可变、可安全并发使用：

```go
package main

import (
	"log"
	stdhttp "net/http"
	"sync"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/websocket"
)

func main() {
	var (
		mu    sync.Mutex
		conns = map[*websocket.Connection]struct{}{}
	)
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Open: func(c *websocket.Connection, r *stdhttp.Request) {
			mu.Lock()
			conns[c] = struct{}{}
			mu.Unlock()
			_ = c.WriteText("welcome\n")
		},
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			m, err := websocket.NewPreparedMessage(op, data)
			if err != nil {
				return
			}
			mu.Lock()
			targets := make([]*websocket.Connection, 0, len(conns))
			for conn := range conns {
				targets = append(targets, conn)
			}
			mu.Unlock()
			for _, conn := range targets {
				_ = conn.WritePreparedMessage(m)
			}
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			mu.Lock()
			delete(conns, c)
			mu.Unlock()
		},
	})
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/ws" {
			_, _ = ws.Upgrade(c, nil)
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		log.Printf("run: %v", err)
	}
}
```

三个细节让这个循环开销很小。PreparedMessage 只校验、只组帧一次，并且对每个接收方商定的 permessage-deflate 窗口最多只压缩一次；于是每个服务端连接排队的都是**同一份**帧，而不是各自一份拷贝，慢对端只付出其队列的代价，不会多出帧拷贝。发送从不阻塞，所以一个协程遍历连接就够了——只有在连接极多或启用 TLS（每帧都要按连接加密）时，才需要把广播分摊到几个协程。最后，这个 map 会被运行在不同 worker 上的回调共享，因此必须加锁。

## 逐帧接收

`OnMessage` 会先重组出整条消息再回调，这意味着一个大上传要先完整驻留内存。改为设置 `HandlerFuncs.Frame`，每个数据帧一到达就交给回调，帧之间不保留任何状态：

```go
var frameBuf bytes.Buffer // or a file, a hash, a decoder: any writer
handler := websocket.NewHandler(websocket.HandlerFuncs{
	Frame: func(c *websocket.Connection, op websocket.Opcode, fin bool, payload []byte) {
		_, _ = frameBuf.Write(payload) // payload is valid only during this call
		if fin {
			log.Printf("message of %d bytes", frameBuf.Len())
			frameBuf.Reset()
		}
	},
})
```

设置 `Frame` 后，Text 与 Binary 消息不会再调用 `Message`。每一帧携带的都是消息本身的 opcode，而绝不是 `Continuation`，最后一帧的 `fin` 为 true；未分片的消息就是一次 `fin` 为 true 的调用。由于帧之间不累积数据，对端可以流式发送远超 `MaxMessageBytes` 的消息，此时该限制只约束单个帧。例外是压缩消息：它的各个帧属于同一条 DEFLATE 流，因此仍会被重组并以一次 `fin` 调用交付。同样的开关与同样的理由也出现在进阶技巧文档的逐帧接收一节（[../hacks.zh-CN.md](../hacks.zh-CN.md)）。

## 压缩与握手选项

握手通过 `websocket.Config` 配置，交给 `NewHandlerWithConfig`；请从 `websocket.DefaultConfig()` 开始，它的 `HTTP` 字段带有独立绑定读取握手时使用的 `http` 包限制与超时：

```go
config := websocket.DefaultConfig()
config.MaxMessageBytes = 4 << 20
config.EnableCompression = true
config.CheckOrigin = func(r *stdhttp.Request) bool {
	return r.Header.Get("Origin") == "https://example.com"
}
config.Subprotocols = []string{"chat.v2", "chat.v1"}
handler := websocket.NewHandlerWithConfig(config, websocket.HandlerFuncs{
	Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
		_ = c.WriteMessage(op, data)
	},
})
```

`EnableCompression` 接受客户端的 permessage-deflate 提议（RFC 7692）；此后消息以压缩形式发送，也可能以压缩形式到达，`MaxMessageBytes` 限制的是解压后的消息大小。发送端不保留 deflate 上下文：每条消息从 `sync.Pool` 借一个压缩器，因此连接再多内存也有界。`CheckOrigin` 是跨站 WebSocket 劫持的防线：为 nil 时等价于 `CheckSameOrigin`，即接受没有 `Origin` 头的请求，或 `Origin` 与请求 Host 一致的请求，而对来自其他站点的浏览器页面返回 403。要让所有来源都通过，可以明确地设为 `func(*http.Request) bool { return true }`。服务端选取客户端 `Sec-WebSocket-Protocol` 提议中第一个出现在 `Subprotocols` 里的子协议（顺序以客户端为准，而不是这个列表），结果通过 `Connection.Subprotocol()` 获取；没有匹配时握手照样成功，只是没有子协议，是否 `Close` 由 handler 决定。

## 客户端

`websocket.Dialer` 通过引擎拨号，用回调报告结果，也可以用 `DialFuture` 等待。引擎必须处于运行状态；它可以是一个没有任何绑定、只做客户端用的引擎：

```go
package main

import (
	"log"
	stdhttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	done := make(chan struct{})
	handler := websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			log.Printf("echo: %s", data)
			_ = c.Close(websocket.CloseNormal, "done")
			close(done)
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			log.Printf("closed %d %q", code, reason)
		},
	}
	dialer := websocket.NewDialer(engine, websocket.DefaultDialerConfig())
	dialer.Dial("ws://127.0.0.1:8080/ws", nil, handler,
		func(c *websocket.Connection, r *stdhttp.Response, err error) {
			if err != nil {
				log.Printf("dial: %v", err)
				return
			}
			_ = c.WriteText("hello from fib")
		})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("timed out")
	}
}
```

握手完成后，客户端连接的服务方式与服务端完全相同：同一套 `Handler` 回调在引擎 worker 上运行，同样的 `Connection` 方法会按 RFC 6455 的要求为客户端发出的帧加掩码。`DialerConfig` 可以限制握手时间（`HandshakeTimeout`；为 0 表示不限时）、限制入站消息（`MaxMessageBytes`）、提供子协议、开启压缩、设置 `wss://` 使用的 `crypto/tls` 配置，或者用 `Dial` 钩子替换拨号过程（例如走 SOCKS 代理）。`Go` 不接收回调，而是返回一个 `DialFuture`：先 `future := dialer.Go(url, nil, handler)`，
再 `conn, resp, err := future.Wait()`。

## 基于 TLS 的 wss://

安全 WebSocket 就是 TLS（[10-tls.md](10-tls.zh-CN.md)）后面的 HTTP 栈，再在其中调用 `Upgrade`；客户端则用一个 `DialerConfig.TLSConfig` 信任服务端证书的 dialer。使用 `tlstest` 证书时两端必须在同一个进程里，所以这个程序在同一个引擎上既服务又拨号：

```go
package main

import (
	"log"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func main() {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data)
		},
	})
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/ws" {
			_, _ = ws.Upgrade(c, nil)
			return
		}
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8443",
		Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(app)),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	config := websocket.DefaultDialerConfig()
	config.TLSConfig = clientTLS // nil means the system roots, for a real certificate
	dialer := websocket.NewDialer(engine, config)
	got := make(chan string, 1)
	conn, _, err := dialer.Go("wss://127.0.0.1:8443/ws", nil, websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			got <- string(data) // a string is a copy; data itself is borrowed
		},
	}).Wait()
	if err != nil {
		panic(err)
	}
	_ = conn.WriteText("hello over wss")
	log.Printf("echo: %s", <-got)
	_ = conn.Close(websocket.CloseNormal, "")
}
```

服务端就是第 [07](07-http2.zh-CN.md) 章的 HTTPS 服务：协商到 h2 的浏览器在一条流上以扩展 CONNECT 打开 WebSocket，而 fib 的 dialer 讲 HTTP/1.1，会占用整条连接；同一个 `Upgrade` 调用两者都能服务。

## 坑与限制

- **只绑 WebSocket 的端口无法应答普通 HTTP。** 同一个端口要同时服务两者时，请在 HTTP handler 中调用 `Upgrade`，并在调用后立即返回。
- **在 HTTP/2 与 HTTP/3 上只切换 stream。** 连接上的其他请求会继续，这是 RFC 8441/9220 的用意，但也意味着请求的 handler 在 `Upgrade` 之后不能再碰这条连接。
- **消息 payload 是借用的。** 在写入环形缓冲、广播缓冲或 channel 之前先复制；切片随回调结束而失效。
- **发送从不阻塞。** 停止读取的对端会不断堆积帧。慢对端要自己关闭；在引擎上设置 `MaxWriteQueueBytes` 或 `MaxPendingBytes`（见 [02-engine.md](02-engine.zh-CN.md)）是更粗放的做法。
- **自定义 `Ping` 会关闭默认 pong。** 仍需 RFC 6455 的回复时，自己调用 `c.Pong(payload)`。
- **`CheckOrigin` 为 nil 不等于“全部放行”。** 它是 `CheckSameOrigin`；来自其他站点的浏览器页面会被 403 拒绝。
- **不要保留握手请求。** 从 `Open` 的 `*http.Request` 中复制需要的内容；调用 `Upgrade` 的 HTTP handler 返回后，这个请求对象会被复用。

## 相关文档

- [Handler：同步与异步](03-handlers.zh-CN.md) —— 连接回调，以及它们为什么不能阻塞
- [HTTP/2](07-http2.zh-CN.md) —— 扩展 CONNECT，WebSocket over h2 的底层传输
- [TLS](10-tls.zh-CN.md) —— `wss://`、证书以及 HTTP 服务前面的 ALPN 栈
- [客户端](20-clients.zh-CN.md) —— 与 HTTP 客户端并列的 WebSocket 客户端
- [进阶技巧](../hacks.zh-CN.md) —— 逐帧接收、PreparedMessage 与其他技巧
- [README](../../README.zh-CN.md) —— WebSocket 特性概览与 Autobahn CI
