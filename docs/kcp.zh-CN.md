# KCP

[English](kcp.md)

[`kcp`](../kcp) 包在 fib 的 UDP binding 上实现了 [skywind3000/kcp](https://github.com/skywind3000/kcp)
的 ARQ 协议。它的 segment 与参考实现一致，数据报与 [xtaci/kcp-go](https://github.com/xtaci/kcp-go)
一致（包括加密与 FEC），因此 fib 服务端可接入 kcp-go、kcptun 客户端，fib 客户端也能连它们的服务端。
它没有外部依赖：密码算法来自标准库，Reed-Solomon 编码自行实现，产生的校验分片与
`klauspost/reedsolomon` 相同。

## 分层

| 层 | 内容 |
| --- | --- |
| `KCP` | 不带 I/O 的协议：`Input` 收对端的包，`Send` 排队消息，`Recv` 取对端的消息，`NextFlush` 指示时调用 `Flush`，经调用方的函数发出。 |
| `Crypt`、FEC | kcp-go 的封装：nonce 加 CRC32 头配 CFB 分组密码或 XOR 表，或 AEAD 的 nonce 与 tag；Reed-Solomon 的数据分片与校验分片。 |
| `Session`、`Server`、`Dial` | fib UDP 连接上的一个会话，可当 `net.Conn` 用或按消息回调；为每个对端建会话的 `fib.Handler`（也是 `net.Listener`）；从自有 binding 拨出的客户端。 |

## 有事才 flush

参考实现和 kcp-go 每隔 `interval`（10 到 100 ms）对每个会话 flush 一次，不管有没有东西要发。这里会话在以下时机 flush：

- fib 为它读到的每一批数据报处理完后，ACK 立即一起发出，Linux 上是一次 `sendmmsg`；
- `Write`、`Send` 之后，以及 `Read` 让关闭的接收窗口重新打开之后；
- 定时器到点时：最早的重传超时或窗口探测时刻。

没有在途数据时 `KCP.NextFlush` 不返回任何时间，空闲会话除空闲超时外不占定时器。`Config.Interval` 仍和参考实现一样参与 RTO 计算。

## 与 kcp-go 的对应

| kcp-go | fib |
| --- | --- |
| `DialWithOptions(addr, block, ds, ps)` | `kcp.Dial(engine, addr, kcp.Config{Crypt: c, DataShards: ds, ParityShards: ps})` |
| `ListenWithOptions(addr, block, ds, ps)` | `kcp.Listen(engine, addr, kcp.ServerConfig{Config: ...})`，或把 `kcp.Server` 放进 `fib.Binding` |
| `SetNoDelay(nodelay, interval, resend, nc)` | `Config.NoDelay`、`Interval`、`Resend`、`NoCongestion` |
| `SetWindowSize(snd, rcv)` | `Config.SendWindow`、`RecvWindow`（为 0 时 128 与 128） |
| `SetMtu(mtu)` | `Config.MTU`，整个数据报（为 0 时 1400） |
| `SetStreamMode(true)` | `Config.StreamMode` |
| `block == nil` | `Config.Crypt == nil` |
| `NewNoneBlockCrypt`、`NewSimpleXORBlockCrypt`、`NewAESBlockCrypt`、`NewTripleDESBlockCrypt`、`NewAESGCMCrypt` | `NewNoneCrypt`、`NewXORCrypt`、`NewAESCrypt`、`NewTripleDESCrypt`、`NewAESGCMCrypt`；`NewCrypt` 接受 kcptun 的名字 |
| Blowfish、CAST5、Twofish、TEA、XTEA、SM4 | `NewBlockCrypt(block)`，传入任意 8 或 16 字节分组的 `cipher.Block`，如来自 `golang.org/x/crypto` |
| Salsa20 | 不支持：它不是分组密码 |

`kcp/interop` 是独立模块（kcp-go 永远不会成为 fib 的依赖），让 fib 与 kcp-go 双向互为服务端和客户端，
中间经过一个每 17 个数据报丢 1 个的转发器，覆盖上表每种加密，带与不带 10/3 FEC，并检查双方都用校验分片恢复了对方丢掉的包。
CI 中对应 `kcp-interop` 任务。

```sh
cd kcp/interop && go test -race ./...
```

## 服务端

`Server` 和 kcp-go 的 `Listener` 一样按对端地址区分会话。没有会话的地址发来的数据报，只有第一个 segment
是会话的开头（序号为 0 的数据）才会建会话。同一地址以这种方式开始另一个会话，会替换原会话（原会话以
`ErrReplaced` 关闭）。`ServerConfig.Accept` 用来过滤，`MaxSessions` 限制数量，`OnSession` 在 fib worker
上接收每个新会话，取代 `Accept`。

KCP 没有关闭报文：会话在 `Close` 时结束，或对端静默超过 `Config.IdleTimeout`（`ErrIdle`，默认一分钟），
或某个 segment 发送 `Config.DeadLink` 次仍未被确认（`ErrDeadLink`）。可能静默更久的应用需自己发心跳，
kcptun 就是通过 smux 发的。

## Read/Write 或消息回调

不设 `Config.OnMessage` 时，会话是带 deadline 的 `net.Conn`：`Read` 只在应用读取时才从 KCP 取消息，
读得慢会让接收窗口关闭、对端等待；排队与在途的 segment 达到 `Config.WriteQueue`（发送窗口的两倍）时
`Write` 阻塞。与 kcp-go 一样，`Write` 的字节各自成 segment 发送，对端读到的是字节流。

设了 `Config.OnMessage` 时，每条消息完整后在 fib worker 上交给回调，同一会话一次一条，回调时会话锁已释放。
回调里可以 `Send`（一条消息，从不阻塞），但不能 `Write`：它可能要等只有这个 worker 才会读到的 ACK。

## 待审的决定

- **按需 flush** 而非固定间隔：ACK 不再最多延迟 `interval`，空闲会话不占定时器；小批量时 ACK 数据报更多。
- **只在序号 0 时建会话**（kcp-go 任意 segment 都建）：服务端已超时关闭的会话，客户端不能用会话中途的
  segment 复活它（那会永远等之前的 segment）；它得不到应答，最终 dead link。
- **每个地址一个会话**，沿用 kcp-go 的替换规则，而不是像部分参考实现用户那样按（地址, conv）复用。
- **默认值**：窗口 128/128（kcp-go 为 32/32），空闲超时一分钟（kcp-go 无），dead link 关闭会话（kcp-go 保留）。
- **不自动调整 FEC**：分片数须与对端一致。会话自身无 FEC 时，对端发来的 FEC 数据分片照常读取，校验分片忽略。
  kcp-go 的带外包（类型 0xf3）丢弃。
- **Close 发出到期的数据即结束**：和 kcp-go 一样，不等待未确认的数据。
