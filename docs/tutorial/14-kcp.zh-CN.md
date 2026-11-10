# KCP

[English](14-kcp.md) | [简体中文](14-kcp.zh-CN.md)

KCP 是用带宽换延迟的可靠 ARQ 协议：它比 TCP 更早重传，依据选择确认和累积确认，并且可以关掉拥塞
控制。fib 在 UDP binding 之上实现了它，线格式与
[skywind3000/kcp](https://github.com/skywind3000/kcp)、
[xtaci/kcp-go](https://github.com/xtaci/kcp-go) 兼容，因此也与 kcptun 兼容，包括 kcp-go 的
crypt 与 FEC 分帧。本章讲把会话当作 `net.Conn`、在 KCP 上跑 `net/http`、消息式会话、调优旋钮、
FEC 与 crypt，以及给自定义传输用的 sans-IO `KCP`。

## 分层模型

fib 里的 KCP 是叠在 UDP 连接上的三层：

- **`KCP`** 是去掉 I/O 的协议本体：`Input` 吃对端的包，`Send` 排入消息，`Recv` 取出对端的消息，
  `Flush` 在 `NextFlush` 说到点时通过调用方给的回调发出。它采用参考实现的分段格式，可以完全脱离
  fib 的 socket 使用。
- **`Crypt` 与 FEC 分片** 按 kcp-go 的方式给这些包分帧：它的 CFB 分组密码、XOR 表、AEAD 或
  “none”，以及它的 Reed-Solomon 数据片与校验片。`Config.Crypt`、`Config.DataShards`、
  `Config.ParityShards` 用来打开它们。
- **`Session`、`Server`、`Dial`** 把一次会话放到 fib 的 UDP 连接上。`Server` 是 UDP binding 的
  `fib.Handler`，同时也是各会话的 `net.Listener`；`Dial` 从自己的 binding 启动一个客户端会话。

`Server` 按对端地址给会话建索引，和 kcp-go 的 `Listener` 一样。来自某个没有会话的地址的数据报，
只有当第一个分段是会话的开头——序号 0 的数据——时才会新建会话；同一地址的对端再用这种方式开启新
会话时，会替换掉原来的会话。KCP 没有握手也没有关闭：一次会话是一个双方都知道的 32 位 ID，任一方
不再应答它就结束了——`Config.IdleTimeout` 到期（`ErrIdle`，默认一分钟），或某个分段发送
`Config.DeadLink` 次仍未收到确认（`ErrDeadLink`）。

会话是一个 `net.Conn`——`Read`、`Write` 搬运字节流，和 kcp-go 的 `UDPSession` 一样——或者，在
`Config.OnMessage` 下，消息完整地到达回调，用 `Send` 发送消息。它的 KCP 由 fib 读到的数据报、它
自己的定时器和应用的调用共同驱动，用一把互斥锁串行化；每一方要发的东西在释放锁后一次性批量发出。
和参考实现每个 interval 都把所有会话 flush 一遍（不管有没有要发的）不同，这里的会话只在有事到期
时才 flush：每批数据报之后、`Write` 和 `Send` 之后、以及定时器报告最早的重传超时或窗口探测时。空闲
会话除了空闲超时之外没有定时器。

## 一个 KCP echo 服务端与客户端

完整程序：一个 Engine、它上面的一个 KCP 监听、会话 echo 循环，以及通过同一个 Engine 拨号的客户端。
两端都使用 kcptun 的 “fast3” 设置：

```go
package main

import (
	"io"
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/kcp"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	// kcptun's "fast3": lower RTO, faster resends, no congestion control.
	// Both sides should agree.
	fast3 := kcp.Config{NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true}

	srv, err := kcp.Listen(engine, "127.0.0.1:4000", kcp.ServerConfig{Config: fast3})
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() {
		for {
			conn, err := srv.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	conn, err := kcp.Dial(engine, "127.0.0.1:4000", fast3)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello kcp")); err != nil {
		panic(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		panic(err)
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	log.Printf("echo: %s", buf[:n])
}
```

`kcp.Listen` 给正在运行的 Engine 添加一个 UDP binding，返回 `*kcp.Server`，它就是一个
`net.Listener`：`Accept` 交出会话，`Close` 关闭会话并移除 binding。`kcp.Dial` 添加自己的客户端
binding，启动一个随机 ID 的会话并立刻返回——没有握手指令可等，服务端会在收到第一个分段时知道这个
会话。两者都不能在 Engine 事件循环所执行的某个回调里调用。

## 在 KCP 上跑 net/http

因为会话是 `net.Conn`、服务端是 `net.Listener`，标准 `net/http` 服务端可以原样跑在 KCP 上；客户端
只需要一个返回会话的 `DialContext`：

```go
package main

import (
	"context"
	"io"
	"log"
	"net"
	nethttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/kcp"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	srv, err := kcp.Listen(engine, "127.0.0.1:4001", kcp.ServerConfig{})
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	mux := nethttp.NewServeMux()
	mux.HandleFunc("/", func(w nethttp.ResponseWriter, r *nethttp.Request) {
		_, _ = io.WriteString(w, "hello from net/http over KCP\n")
	})
	go func() { _ = nethttp.Serve(srv, mux) }()

	client := &nethttp.Client{Transport: &nethttp.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return kcp.Dial(engine, addr, kcp.Config{})
		},
	}}
	rsp, err := client.Get("http://127.0.0.1:4001/")
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: %s", rsp.Status, body)
}
```

KCP 替换的只是监听器和拨号器；上面的 HTTP、TLS 或任何其他流式协议都不知道传输变了。没有 crypt
也没有 FEC 时，会话发出的 KCP 包不带额外分帧，和没有 `BlockCrypt` 的 kcp-go 一样，所以可以直接
落在 UDP 路径上。

## 服务端配置

当 binding 要和 Engine 一起创建、而不是事后添加时，用 `kcp.NewServer` 构造 handler；`ServerConfig`
决定会话如何被接纳以及它们的默认配置：

```go
server, err := kcp.NewServer(kcp.ServerConfig{
	Config: kcp.Config{}, // every session's configuration
	Accept: func(remote netip.AddrPort, conv uint32) bool {
		return remote.Addr().IsLoopback() // a cheap filter before a session exists
	},
	OnSession: func(s *kcp.Session) {
		// Runs before the session's first datagram is read, on a fib worker:
		// do not block, do not Read or Write; Send may.
		_ = s.Send([]byte("welcome"))
	},
	MaxSessions:   1024,
	AcceptBacklog: 128,
})
if err != nil {
	panic(err)
}
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Network: "udp", Addr: "127.0.0.1:4000", Handler: server})
if err != nil {
	panic(err)
}
defer engine.Close()
go func() { _ = engine.Run() }()
```

不设置 `OnSession` 时，会话等待 `Server.Accept`，也就是上面两个程序走的 `net.Listener` 路径。
`AcceptBacklog` 限制等待的会话数量，超出的新会话会被丢弃、由对端重发；`MaxSessions` 限制打开的会话
总数。调用 `Listen` 或 `Dial` 时，binding 所在的 Engine 必须已经在运行。

## 消息式会话

消息式会话用 `Config.OnMessage` 和 `Session.Send` 取代 `Read`、`Write`：每条消息完整到达，在 fib
worker 上执行，每条会话一次一条，而 `Send` 不阻塞地把一条消息排入队列：

```go
server, err := kcp.NewServer(kcp.ServerConfig{Config: kcp.Config{
	OnMessage: func(s *kcp.Session, msg []byte) {
		_ = s.Send(append([]byte("echo: "), msg...)) // never blocks
	},
	OnClose: func(s *kcp.Session, err error) {
		log.Printf("session %d closed: %v", s.Conv(), err)
	},
}})
if err != nil {
	panic(err)
}
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Network: "udp", Addr: "127.0.0.1:4002", Handler: server})
// ...
conn, err := kcp.Dial(engine, "127.0.0.1:4002", kcp.Config{
	OnMessage: func(s *kcp.Session, msg []byte) {
		log.Printf("message: %s", msg)
	},
})
if err := conn.Send([]byte("hello")); err != nil {
	panic(err)
}
```

回调里不能调用 `Write`：它可能等待确认，而确认只有正在读这条会话的 worker 才能送出；应该用
`Send`，`WaitSend` 报告它已排入多少。开启 `OnMessage` 后，会话的流控属于应用：消息一完整就从 KCP
取出，而不是等应用来读。`OnClose` 报告会话结束的原因：`Close` 为 nil，另外可能是 `ErrIdle`、
`ErrDeadLink`、`ErrReplaced`。

## 调优

旋钮来自参考实现，命名与 kcp-go 一致：

```go
cfg := kcp.Config{
	// kcptun's fast3; less aggressive settings are the reference defaults.
	NoDelay: 1, Interval: 10 * time.Millisecond, Resend: 2, NoCongestion: true,
	// Windows in segments, both sides; the default is 128/128 and kcp-go's is 32/32.
	SendWindow: 256, RecvWindow: 256,
	// The largest datagram, framing (crypt, FEC) included.
	MTU: 1400,
	// A quiet session ends after IdleTimeout; a negative value never ends it.
	IdleTimeout: time.Minute,
	// Transmissions of one segment without an acknowledgement before the link is dead.
	DeadLink: 20,
	// Segments queued or in flight before Write blocks; twice the send window when zero.
	WriteQueue: 512,
}
srv, err := kcp.Listen(engine, "127.0.0.1:4000", kcp.ServerConfig{Config: cfg})
if err != nil {
	panic(err)
}
```

`NoDelay`、`Interval`、`Resend`、`NoCongestion` 就是 `KCP.NoDelay` 的旋钮：interval 参与 RTO 的
计算，`Resend` 在这么多后续分段被确认后提前重传，`NoCongestion` 只留下窗口限制。两端应当就这些
取值达成一致。窗口和 `MTU` 必须与对端对路径承载能力的预期一致；如果 crypt 或 FEC 分帧让数据报超过
路径 MTU，会话就会不停重传。

## FEC 与 crypt

`Config.Crypt` 就是 kcp-go 的 `BlockCrypt`，`Config.DataShards`/`ParityShards` 就是它的前向纠错。
两者都必须与对端一致；对端发送的 FEC 分片即使本会话没开 FEC 也能被读取，但它的校验片不会被使用：

```go
key := bytes.Repeat([]byte{0x2a}, 32) // already derived; kcptun derives with PBKDF2-SHA1
crypt, err := kcp.NewCrypt("aes", key) // "none", "xor", "aes", "aes-128", "aes-192", "3des", "aes-128-gcm"
if err != nil {
	panic(err)
}
cfg := kcp.Config{
	Crypt:        crypt,
	DataShards:   10, // kcptun's default FEC
	ParityShards: 3,
}
srv, err := kcp.Listen(engine, "127.0.0.1:4000", kcp.ServerConfig{Config: cfg})
if err != nil {
	panic(err)
}
```

`NewCrypt` 接受 kcptun 的加密名和一把“已经派生好”的密钥：kcptun 用
`pbkdf2.Key(sha1.New, passphrase, []byte("kcp-go"), 4096, 32)` 派生，所以 fib 服务端要服务 kcptun
客户端，前面必须接同样的派生。标准库没有的分组密码——blowfish、cast5、twofish、tea、xtea、sm4——
用 `NewBlockCrypt` 包装任意 8 或 16 字节分组、来自任何实现的 `cipher.Block`；其余的是
`NewAESCrypt`、`NewAESGCMCrypt`、`NewTripleDESCrypt`、`NewXORCrypt`、`NewNoneCrypt`。Salsa20
不支持：它不是分组密码。

## Sans-IO 的 KCP

`kcp.New` 是纯协议，给应用自己的传输层用。调用方提供时间、搬运数据包：

```go
kcpConn := kcp.New(7, func(packet []byte) {
	_ = udpConn.Write(packet) // packet is valid only for the call
})
now := uint32(time.Now().UnixMilli())
if err := kcpConn.Input(packet, now); err != nil {
	log.Print(err)
}
_ = kcpConn.Send([]byte("hello"))
if at, due := kcpConn.NextFlush(now); due && int32(at-now) <= 0 {
	kcpConn.Flush(now)
}
if size := kcpConn.PeekSize(); size > 0 {
	buf := make([]byte, size)
	if _, err := kcpConn.Recv(buf); err != nil {
		log.Print(err)
	}
}
```

`Input` 会复制它保留的分段数据；`NextFlush` 指出 `Flush` 下一次有事情要做的时刻；`Send` 会保留
传入的消息直到发出（最多 255 个分段），除非开启 stream 模式。一个 `KCP` 不是并发安全的，时间单位是
任意时钟上的毫秒，在 2^32 处回绕。当应用自己的中继无法把 UDP socket 交给 fib 时，就用这一层。

## 与 kcp-go、kcptun 的兼容性

`kcp.Dial` 就是 kcp-go 的 `DialWithOptions`，`kcp.Listen` 是 `ListenWithOptions`，
`ServerConfig.Config` 是 `DialWithOptions` 的选项；那些旋钮就是换个名字的 `SetNoDelay`、
`SetWindowSize`、`SetMtu` 和 `SetStreamMode`。fib 服务端能接 kcp-go 与 kcptun 客户端，fib 客户端
也能连它们的服务端，覆盖两者共有的每种 crypt，并支持 FEC。`kcp/interop` 是一个独立模块（这样
kcp-go 永远不会成为 fib 的依赖），它通过一个每 17 个数据报丢 1 个的中继，在两种角色下都做了验证：

```sh
cd kcp/interop && go test -race ./...
```

差异在于：kcp-go 的带外数据包（类型 0xf3）会被丢弃；没有 FEC 自动调参，分片数必须匹配；默认值
不同——窗口是 128/128 而不是 32/32，有 kcp-go 没有的空闲超时，并且 dead link 会关闭会话（kcp-go
则保留）。完整映射见 [docs/kcp.zh-CN.md](../kcp.zh-CN.md#与-kcp-go-的对应)。

## 坑与限制

- **KCP 没有握手，也没有关闭。** 会话一建立就可用，`Close` 把到期的数据发出后立即停止，不等待未
  确认的数据。对端只能通过自己的空闲超时、dead link，或应用层协议得知会话结束。
- **应用可能长时间静默时，要自己发 keepalive。** `Config.IdleTimeout` 默认一分钟；kcptun 靠
  smux 让安静链路保持存活。可以调大超时，或在应用层发送心跳。
- **超时死掉的会话无法复活。** 新会话只从序号 0 的分段开始，服务端已超时的客户端用会话中段的分段
  得不到任何应答，最终 dead link；它必须重新拨号。
- **两端必须在调优、crypt 和 FEC 上一致。** FEC 分片必须匹配；密码算法或密钥不同的 crypt 会让
  每个数据报都变成乱码；`NoDelay`、`Interval`、`Resend`、`NoCongestion` 文档上写明是双方的约定，
  不是单方面的提示。
- **`OnMessage` 里不能 `Write`。** `Write` 可能等待确认，而确认只有正在读这条会话的 worker 才能
  送出，会死锁；用 `Send` 并关注 `WaitSend`。
- **`OnSession` 不能阻塞、不能读。** 它在第一个数据报被读取之前运行于 fib worker；可以 `Send`，
  不可以 `Read`/`Write`。
- **`Write` 会阻塞，`Send` 不会。** `Write` 在 `Config.WriteQueue` 个分段排队或在途时等待（默认
  是发送窗口的两倍），发送速度超过对端的会话应该用 `Send` 配合 `WaitSend`。
- **一个对端地址一个会话。** 地址变化（NAT 重绑定、移动网络切换）的对端看起来就是新对端：旧会话
  空闲超时退场，新会话只能从序号 0 开始。
- **会话只在忙碌时才有开销。** 空闲会话没有定时器，但 UDP binding 和会话记录会保留到
  `IdleTimeout`；用 `MaxSessions` 和合理的空闲超时加以限制。
- **不要在 Engine 的回调里调用 `Listen` 或 `Dial`。** 两者都要等 Engine 的事件循环，而回调正占着
  它。

## 相关文档

- [docs/kcp.zh-CN.md](../kcp.zh-CN.md)，包含分层与兼容性表格的参考页。
- [`kcp` 包](../../kcp) 与互操作模块 [`kcp/interop`](../../kcp/interop)。
- 根 README 的 [KCP 一节](../../README.zh-CN.md#kcp)。
- [第 19 章：原生 TCP、UDP 与 Unix socket](19-raw-io.zh-CN.md) 讲 KCP 所依赖的 UDP 层。
- [第 22 章：性能与运维](22-performance.zh-CN.md) 讲服务端调优与 profiling。
- 兼容的上游：[xtaci/kcp-go](https://github.com/xtaci/kcp-go) 与
  [xtaci/kcptun](https://github.com/xtaci/kcptun)。
