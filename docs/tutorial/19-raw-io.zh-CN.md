# 原生 TCP、UDP 与 Unix socket

[English](19-raw-io.md) | [简体中文](19-raw-io.zh-CN.md)

本章讲 fib 最底层的东西：一个带 TCP、UDP 或 Unix binding 的 `Engine`，加上一个直接面对字节的
`fib.Handler`，中间没有 HTTP、WebSocket 或 RPC。内容包括 handler 模型、`Connection` API（写、cork、
读、deadline、半关闭、sendfile 与 `SpliceTo`）、每个 peer 一个连接的 UDP、Unix socket，以及同一个
engine 的拨号侧。本章这一层是本教程里所有其他协议的底座；当协议是你自己的、或者分帧与背压必须由你
精确定夺时，它也是正确的选择。

## 运行模型

一个 binding 负责监听；每个被接受的连接（UDP 上是每个 peer 地址）就是一个 `*fib.Connection`。连接的
回调在 engine 池的一个 worker 上按轮（round）一次一轮地执行：一轮会一直读到 socket 为空，每读一次调用一次
`OnData`（每次最多 `ReadBufferSize`，默认 16KiB），`OnOpen`
在任何一轮之前于事件循环的 goroutine 上运行，`OnClose` 在连接的最后一轮、最后一次 `OnData` 之后运行。
由此有两条决定每个 handler 写法的结论。第一，在 TCP 与 Unix 连接上，传给 `OnData` 的数据只在本次调用期间
有效——要留下就拷贝（UDP 数据报是归 handler 所有的副本，见下文）。第二，handler 不能阻塞：它运行时，它占着的
worker 不能服务其他连接，同一个连接的下一轮也要等它。一轮里做的写会被 cork 住，在轮结束时一次写入 socket
（`Config.CoalesceWrites`，`DefaultConfig` 默认打开），这就是 fib 的 pipelining。连接上的 `Read` 不会阻塞：
socket 当前没有更多数据时返回 `fib.ErrWouldBlock`。

## 一个 TCP echo 服务端和它的客户端

同一个 engine 可以既提供 binding 又拨号访问它；`DialWithHandler` 给拨出的连接一个自己的 handler，所以
一个程序不用第二个 engine 就能同时是服务端和客户端：

```go
package main

import (
	"fmt"
	"log"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:9000",
		Handler: fib.HandlerFuncs{
			Open: func(c *fib.Connection) { log.Printf("open %s", c.RemoteAddr()) },
			Data: func(c *fib.Connection, b []byte) {
				// b is valid only during this call; Send copies what it needs.
				if err := c.Send(b); err != nil {
					c.Close()
				}
			},
			Close: func(c *fib.Connection, err error) { log.Printf("close: %v", err) },
		},
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() {
		if err := engine.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	got := make(chan string, 1)
	err = engine.DialWithHandler("tcp", "127.0.0.1:9000", 5*time.Second, fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("hello\n")) },
		Data: func(c *fib.Connection, b []byte) {
			got <- string(b)
			c.Close()
		},
	}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("echo: %s", <-got)
}
```

`fib.HandlerFuncs` 只填 handler 需要的回调，其余的是空实现。有状态的 handler 就是一个实现了
`OnOpen`、`OnData`、`OnPriorityData`、`OnClose` 的 struct。拨号的 done 回调恰好执行一次，在事件循环上，
失败时给 `*net.OpError`，所以它也不能阻塞。

## Connection API

- `Send(data)` 返回前拷贝 `data`：先尝试直接写，剩下的进发送队列，从不等待；`SendParts(header, body)` 不拼接
  两段而直接发；`SendOwned`/`SendShared` 在所有权明确时省掉拷贝：`SendOwned` 之后调用方不得再碰这个切片，
  `SendShared` 用于把一份只读 buffer 发给多个连接。
- `Cork()` 与 `Flush()` 手工控制批量写，适合在连接这一轮之外生成多个回复时使用。在 `OnData` 里写的
  内容会自动 cork，直到这一轮结束才一次写出。
- `Read` 取 socket 里已有的数据；`SendFile(f, offset, count)` 用 `sendfile(2)`/`TransmitFile` 发一段文件，
  随 peer 消费而读，内存不随文件大小增长。
- `SetReadDeadline`、`SetWriteDeadline`、`SetDeadline` 到点关闭连接，而不是让某次调用失败；`OnClose`
  收到 `os.ErrDeadlineExceeded`。
- `HoldReads(true)` 让应用程序跟不上的连接停止读取，数据留在内核、由 TCP 流控拖慢 peer；`HoldReads(false)`
  会重新投递已经到达的数据。
- `Close` 丢掉还没发出的输出，`CloseAfterSend` 先发完；`CloseWrite` 只关闭写方向（peer 读完数据后收到
  EOF，`OnData` 继续运行），`CloseRead` 只关闭读方向。
- `SetHandler` 把连接交给另一个 handler，`mux` 就是嗅探到首字节后这样切换协议的；`ShuttingDown` 报告
  engine 或 binding 正在排空。
- `SetAttachment`/`Attachment` 把每连接状态挂在连接上；`SendBatch` 与 `SetUDPIdleTimeout` 是 UDP 的成员
  （在流式连接上 `SendBatch` 只是逐个 `Send`）。

`Cork` 是最值得记牢的一个调用，因为最常见的错误就是在 goroutine 里发多个回复，结果变成一次回复一次写：

```go
func replyAll(c *fib.Connection, replies [][]byte) error {
	c.Cork()
	for _, r := range replies {
		if err := c.Send(r); err != nil {
			_ = c.Flush()
			return err
		}
	}
	return c.Flush() // nothing else writes what a Cork holds
}
```

`SpliceTo` 更进一步：它在两个连接之间通过管道搬字节，数据完全不经过用户态。它只在 Linux 上、且只对
没有加层的 TCP 或 Unix 连接可用：

```go
// proxyNext moves the next n bytes of src to dst through splice(2).
// Elsewhere, or between connections it cannot splice, it returns fib.ErrSpliceUnsupported
// and calls none of the callbacks.
func proxyNext(src, dst *fib.Connection, n int64) error {
	return src.SpliceTo(dst, fib.Splice{
		Begin: func() int64 { return n }, // a negative count gives the splice up
		Read: func(moved int64, err error) {
			if err != nil {
				src.Close() // a failed splice leaves src's reads held
			}
		},
		Written: func(err error) {
			// fib has already closed dst if err != nil: its stream was cut mid-way.
			log.Printf("splice done: %v", err)
		},
	})
}
```

splice 正常完成后，`src` 的读取回到 `OnData`，从搬完的最后一个字节之后开始；这些回调在 `src` 的轮里运行，
所以同样不能阻塞。

## 读、deadline 与背压

`OnData` 看到的是本轮读循环拿到的数据；如果 handler 必须把 socket 里剩下的也消费掉，就自己读，而滚动
的空闲超时就是每一轮重置一次的 deadline：

```go
func echoWithIdleTimeout() fib.Handler {
	return fib.HandlerFuncs{
		Data: func(c *fib.Connection, b []byte) {
			_ = c.Send(b) // what this read delivered
			// Then take what is left in the socket now, e.g. the rest of a message.
			buf := make([]byte, 4096)
			for {
				n, err := c.Read(buf)
				if errors.Is(err, fib.ErrWouldBlock) {
					break // nothing more now; the next OnData delivers the rest
				}
				if err != nil {
					return // io.EOF, or the reason the connection closed
				}
				_ = c.Send(buf[:n])
			}
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
		},
	}
}
```

因为 `Read` 从不等待，不能把它交给 `bufio.Reader` 或任何按阻塞 socket 写的代码。handler 要缓存输入时，
惯用的背压手段是 `HoldReads`：缓冲超过高水位就 hold，由排空它的 goroutine 释放，于是被放慢的是 TCP
窗口而不是内存增长（见[进阶技巧](../hacks.zh-CN.md)第 8 节）。

## UDP 数据报

UDP binding 上每个 peer 地址变成一个自己的连接：第一个数据报打开它（`OnOpen`），之后的数据报都到这个
连接，静默达到 `Config.UDPIdleTimeout`（默认 60 秒）后关闭，`OnClose` 收到 `ErrUDPIdleTimeout`。实现了
`fib.DatagramsHandler` 的 handler 会把该 peer 已排队的数据报作为一批一次性收到，并用一个 `SendBatch`
回复整批——这正是 QUIC 与 HTTP/3 用的 ACK 合并。和 TCP 的 `OnData` 切片不同，每个数据报都是归 handler 所有
的副本，可以留着（或在没有任何引用后用 `bufferpool.Put` 还回去）；只有装着这一批的 `[][]byte` 不能留：

```go
package main

import (
	"fmt"
	"log"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "udp",
		Addr:    "127.0.0.1:9001",
		Handler: burstEcho{},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	reply := make(chan string, 1)
	client := fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("ping")) },
		Data: func(c *fib.Connection, b []byte) {
			reply <- string(b)
			c.Close()
		},
	}
	if err := engine.DialWithHandler("udp", "127.0.0.1:9001", 5*time.Second, client, nil); err != nil {
		log.Fatal(err)
	}
	fmt.Println(<-reply)
}

// burstEcho answers each burst of datagrams with one batch.
type burstEcho struct{}

func (burstEcho) OnOpen(c *fib.Connection)                   {}
func (burstEcho) OnClose(c *fib.Connection, err error)       {}
func (burstEcho) OnPriorityData(c *fib.Connection, b []byte) {}
func (burstEcho) OnData(c *fib.Connection, b []byte)         { answer(c, [][]byte{b}) }
func (burstEcho) OnDatagrams(c *fib.Connection, d [][]byte)  { answer(c, d) }

func answer(c *fib.Connection, dgrams [][]byte) {
	if err := c.SendBatch(dgrams); err != nil { // one sendmmsg (Linux) for the whole burst
		log.Print(err)
	}
}
```

有两点把 UDP 和 TCP 区分开：socket 没有空间时数据报会被丢弃，错误返回、连接保持打开（UDP 发送从不入队；
`SendDatagrams` 报告一批里发出了几个，`NotifyWritable` 在 socket 重新有空间时通知你）；
`OpenUDPPeer(binding, addr)` 让 binding 先向一个还没发过东西的 peer 发言。它要等 socket 的事件循环，
所以不能在该循环跑的回调里调用（例如 UDP peer 的 `OnOpen`）。`ReusePort` 打开时（默认如此），
`OpenUDPPeer` 打开的 peer 位于第一个 poller 的 socket 上，回复却可能落到另一个 socket，后者会为同一地址
再打开一个连接：对这类 peer 的状态要按地址而不是按连接来记。拨出的 UDP 连接不会因空闲超时被关，只有
监听端的 peer 会。

## Unix socket

Unix binding 和 TCP 一样，只是 `Network: "unix"`、`Addr` 是 socket 路径。engine 或 binding 关闭时 fib
会删掉这个路径，而启动时路径不能已存在，所以被异常杀掉的进程会留下一个文件，下一个进程必须先删掉（否则 `fib.New` 报
"address already in use"）。连接行为与 TCP 完全一致；拨号的客户端没有名字，所以 `RemoteAddr` 是名字为空的
`*net.UnixAddr`，`RemoteAddrPort` 无效：

```go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	const sock = "/tmp/fib-admin.sock"
	_ = os.Remove(sock) // a crashed process may have left the path behind

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "unix",
		Addr:    sock,
		Handler: fib.HandlerFuncs{
			Data: func(c *fib.Connection, b []byte) {
				_ = c.SendParts([]byte("admin: "), b)
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	replies := make(chan string, 1)
	err = engine.DialWithHandler("unix", sock, 5*time.Second, fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("status")) },
		Data: func(c *fib.Connection, b []byte) {
			replies <- string(b)
			c.Close()
		},
	}, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(<-replies)
}
```

## 向外拨号

`Engine.Dial(network, addr, timeout, done)` 是简短形式。`DialWithHandler` 为拨出的连接指定 handler，一个
engine 因此能同时承载服务端和它自己的客户端。`DialWith` 接受一个 `DialOptions`：本地地址，以及必须从
已知端口段发出的协议所需的端口范围。它们都不阻塞调用方——域名解析在客户端池上做，结果通过 done 回调
送达：

```go
func dialFromRange(engine *fib.Engine, addr string, handler fib.Handler) error {
	return engine.DialWith(fib.DialOptions{
		Network:   "tcp",
		Addr:      addr,
		Timeout:   5 * time.Second,
		Handler:   handler,
		LocalAddr: netip.MustParseAddrPort("127.0.0.1:0"), // the port comes from the range
		PortMin:   20000,
		PortMax:   20099,
	}, func(c *fib.Connection, err error) {
		if err != nil {
			log.Printf("dial failed: %v", err) // *net.OpError
			return
		}
		_ = c.Send([]byte("hello"))
	})
}
```

同一组选项也能拨 `udp`（socket 已连接，所以一次 `Send` 是一个数据报，`OnData` 是收到的一个数据报）和
`unix`。

## 何时该用原生层

当协议是你自己的——自定义二进制分帧、游戏协议、数据库 wire 协议、有特殊要求的内部 RPC——或者当你需要
上层藏起来的能力时，用原生 TCP、UDP 或 Unix：`Cork` 与 `SendParts` 精确控制写批次，`HoldReads` 做
应用驱动的背压，`SendFile`/`SpliceTo` 做零拷贝转发，`CloseWrite` 支持带半关闭的协议，`SetHandler` 做
嗅探。其余有标准形态的东西，就用已经负责解析、分帧和加密的那一层：[03-handlers.md](03-handlers.zh-CN.md)
与 [06-http1.md](06-http1.zh-CN.md) 的 HTTP、[09-websocket.md](09-websocket.zh-CN.md)、
[11-grpc.md](11-grpc.zh-CN.md)、[12-arpc.md](12-arpc.zh-CN.md)、
[15-multiprotocol.md](15-multiprotocol.zh-CN.md)。它们全都是同一层之上的 `fib.Handler`，所以原生 handler
可以和它们并排放在一个 engine 上，甚至可以共用一个端口。

## 坑与限制

- **TCP 与 Unix 上 `OnData` 的切片是借来的。** 它只在本次调用期间有效；要留下就拷贝，或者交给会拷贝的地方
  （`Send` 会）。UDP 数据报归 handler 所有，但 `OnDatagrams` 收到的 `[][]byte` 不是。
- **`OnOpen` 在事件循环上运行。** 在那里阻塞会卡住该循环上的所有连接；把握手和 I/O 放到 `OnData` 里，
  或者把工作交给池。
- **绝不要在 handler 里阻塞。** 一个连接一轮一轮地跑，阻塞占掉的是其他连接共享的 worker。长任务放到
  goroutine 或 task pool 上，期间连接可以保活（见 [03-handlers.md](03-handlers.zh-CN.md)）。
- **`Read` 不是 `net.Conn.Read`。** 没有数据时它返回 `fib.ErrWouldBlock` 而不是等待；deadline 到点关闭
  连接，而不是让调用失败。
- **UDP 没有 `Read`。** 事件循环读出数据报并整块投递；socket 收不下的数据报被丢弃，返回错误、连接仍开着。
- **静默的 UDP peer 会消失。** 什么时候关由 `UDPIdleTimeout`（默认 60 秒）或 `SetUDPIdleTimeout` 决定；
  要活得更久的 peer 需要持续有流量，或者用负的超时值。
- **Unix socket 路径会残留。** 进程被杀时 socket 文件还在；监听前要先删除，正常关闭时 fib 会删。
- **`SpliceTo` 只在 Linux 上可用**，而且两端都必须是没加层的 TCP 或 Unix 连接；要检查它返回的 `error`，
  其他平台退回 `Read`/`Send` 循环。

## 相关文档

- [进阶技巧](../hacks.zh-CN.md) — 第 8 节 `HoldReads`、第 9 节 cork 与 sendfile、第 10 节 `Read` 与 deadline、第 15 节 UDP
- [Handler：同步与异步](03-handlers.zh-CN.md) — 完整的执行模型
- [引擎与配置](02-engine.zh-CN.md) — `UDPIdleTimeout`、缓冲区、`ReusePort`、`IOPollers`
- [README](../../README.zh-CN.md#裸-tcp) — 四行代码的原生 TCP，以及每种协议在哪里运行
- [BitTorrent](18-bittorrent.zh-CN.md) — 建在这一层之上的完整协议栈
- [客户端](20-clients.zh-CN.md) — 用 engine 拨号、回调与 future
