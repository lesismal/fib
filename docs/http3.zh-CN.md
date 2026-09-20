# HTTP/3：限制、有意未实现的功能与待优化项

[English](http3.md) | [简体中文](http3.zh-CN.md)

本文记录 Go 版 `http3` package 的边界：当前行为上的限制、出于设计考虑有意没有实现的
功能，以及后续可以优化的方向。功能用法见
[Go README 的 HTTP/3 章节](../go/README.zh-CN.md#http3-子-package)。

实现位于 [`go/http3`](../go/http3)，QUIC 传输层在
[`go/http3/internal/quic`](../go/http3/internal/quic)，QPACK 在
[`go/http3/internal/qpack`](../go/http3/internal/qpack)。三者全部自行实现，TLS 1.3 握手
使用标准库的 `crypto/tls.QUICConn`，不依赖 quic-go 或 `golang.org/x/net`。

## 已支持的范围（概览）

| 方向 | 内容 |
| --- | --- |
| QUIC | version 1；Initial、Handshake、1-RTT 三个包号空间；AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305 包保护与头部保护；双向 key update（本端按 AEAD 使用上限主动发起，也响应对端发起）与解密失败次数上限；客户端处理 Retry；Version Negotiation；stateless reset；RFC 9002 丢包检测、PTO 与 NewReno 拥塞控制；服务端 3 倍放大限制；连接级与 stream 级双向流控；MAX_STREAMS；空闲超时与可选 keep-alive |
| 服务端 | 与 HTTP/1、HTTP/2 共用同一个 `Handler` 和 `Context`；1xx 中间响应、自动 100 Continue、请求与响应 trailer、`Response.Close` 通过 GOAWAY 优雅关闭、413/431/417、非法请求（含 `:authority` 与 `Host` 不一致）以 H3_MESSAGE_ERROR 重置、协议错误按 RFC 9114 / RFC 9204 错误码关闭连接、`Request.TLS`、`AltSvc` 辅助函数 |
| 客户端 | 异步 `Do`/`Go`；每个 host:port 一条连接、多路复用；遵守 MAX_STREAMS 并排队；取消只重置单个 stream；GOAWAY 与 H3_REQUEST_REJECTED 的请求自动在新连接上重发；请求与响应 trailer；对端违反协议时按错误码关闭连接 |
| 一致性 | 与 quic-go 客户端、服务端双向互通，以及用手写帧构造的协议错误用例，都在 CI 中运行 |
| 互通验证 | quic-go 客户端与服务端（双向，含 5% 丢包）；客户端访问 Cloudflare、Google、nginx、Facebook（mvfst）、Varnish、quiche 的线上服务 |

以上内容都有测试覆盖并在 CI 中运行，见下面的[一致性测试](#一致性测试)。

## 当前限制

使用时需要了解的行为约束。

### 连接按对端地址区分，不支持迁移

- Engine 的 UDP socket 按“对端 IP:端口”把数据报分给各自的 `fib.Connection`，每个地址
  对应一个 QUIC 连接，**不按 Connection ID 路由**。客户端换网络或 NAT 重绑定后，旧连接
  收不到后续数据，只能等空闲超时或由客户端重新连接。服务端在传输参数中声明了
  `disable_active_migration`。
- 同样的原因，无法在多个进程或多个 `SO_REUSEPORT` socket 之间按连接分流。
- 本端只使用握手时的一个 Connection ID，从不发送 NEW_CONNECTION_ID。对端提供的
  CID 会被记录，也会遵守 retire_prior_to，但本端不主动轮换 CID。PATH_CHALLENGE 只回复、
  从不主动发起，待发送的回复最多保留最近 4 个（RFC 9000 §8.2.2 只要求回复最新的挑战）；
  preferred_address 被忽略。

### 与 Engine 的配合

- QUIC 的 `MaxIdleTimeout`（默认 30 秒）必须小于 Engine 的 `Config.UDPIdleTimeout`
  （默认 60 秒），否则 Engine 会先把静默的对端关掉。
- Engine 为每个 UDP 连接最多排队 1024 个待处理的数据报，超出的会被丢弃，由 QUIC 按丢包
  处理。
- QUIC 层依赖 Engine 把每个 UDP 数据报复制一份交给 `OnData` 这一行为：乱序重组和等待
  密钥时，QUIC 层会直接持有这些切片。
- `Engine.Stop`/`Close` 不会给 HTTP/3 连接发送 CONNECTION_CLOSE 或 GOAWAY，连接被直接
  丢弃，客户端要等空闲超时才会发现。
- 与 `http` package 相同，`http3` 只在 Linux、macOS、Windows 三个原生后端上编译，兼容
  后端（如 FreeBSD）上这个 package 为空。

### 消息体整体缓存，handler 同步执行

- 请求 body 在 handler 运行前完整读入内存，响应通过 `Response.Body []byte` 一次性给出；
  客户端同样把响应 body 完整缓存后再回调，因此不支持 SSE、流式上传下载等场景。
  `Context` 的 `http.ResponseWriter` 方法可以使用（含 trailer），但与 HTTP/2 一样，
  写出的响应会先缓存，handler 返回后整体发送；只有 HTTP/1 是流式的。
- 内存上限：服务端单连接最坏约为 `MaxConcurrentStreams × MaxBodyBytes`（默认
  100 × 16MB），客户端单个响应受 `MaxResponseBodyBytes` 限制。
- 同一连接上的 handler 调用是串行的，同步阻塞的 handler 会推迟同一连接上其他请求的处理。
  耗时操作应在 goroutine 中异步回复。
- 不要调用 `Context.Conn.Send` 直接写数据：`Conn` 是 UDP 连接，所有输出都必须经过
  `Context`。

### 固定参数

以下参数目前是常量，或者只在内部的 `quic.Config` 中存在，没有通过 `http3.Config` 或
`http3.ClientConfig` 暴露：

| 参数 | 值 |
| --- | --- |
| 数据报大小 | 固定 1200 字节，不做 PMTU 探测 |
| 每个 stream 的接收窗口 | 1MB，消费一半后补充 |
| 连接的接收窗口 | 16MB，消费一半后补充 |
| 对端可以打开的单向 stream 数 | 16 |
| 客户端声明的双向 stream 上限 | 1（服务端不能打开请求 stream） |
| QPACK 动态表容量 | 0 |
| ACK 策略 | 每 2 个需要确认的包回一次 ACK，或最多延迟 25ms |
| 可记住的已接收包号区间 | 32 个，更早的包被当作重复包丢弃 |
| keep-alive | 客户端默认不发，空闲 30 秒后连接关闭，下一个请求重新握手 |

### 协议细节

- **关闭**：发出 CONNECTION_CLOSE 后立即释放连接，没有 RFC 9000 §10.2 的 3×PTO
  closing 期，也不会对之后到达的包重发 CONNECTION_CLOSE；收到对端的 CONNECTION_CLOSE
  时同样立即释放，没有 draining 期。如果关闭包丢失，对端要等 stateless reset 或空闲
  超时才会结束。
- **密钥更新**：本端在一把密钥保护的包数达到 RFC 9001 §6.6 的上限（AES-GCM 约 2^23 个
  包）时主动发起 key update，也响应对端发起的更新；解密失败次数超过完整性上限时以
  AEAD_LIMIT_REACHED 关闭连接。不做的是按时间或按需要更换密钥。
- **Stateless reset**：reset key 由每个 `ServerHandler` 随机生成，不能配置。服务端重启后
  key 变了，客户端认不出服务端对重启前的连接发出的 reset。只有客户端会识别 stateless reset。
- **BLOCKED 类帧**：受流控限制时不发送 DATA_BLOCKED、STREAM_DATA_BLOCKED、
  STREAMS_BLOCKED。
- **拥塞控制**：只有 NewReno，没有 pacing，没有判断应用受限（app-limited），也没有
  持续拥塞（persistent congestion）判定。发送缓冲区满（`EAGAIN`）时数据报直接丢弃，
  按丢包处理。
- **对端的 `SETTINGS_MAX_FIELD_SECTION_SIZE`**：能解析，但发送时不检查。
- **优先级**：`priority` 头和 PRIORITY_UPDATE 帧都被忽略，待发送的 stream 轮流发送。
- **ECN**：不设置也不上报 ECN 标记。ACK_ECN 帧能解析，但其中的计数被忽略。

### 客户端

- 只支持 `https://`。不会根据 `Alt-Svc` 从 HTTP/1.1 或 HTTP/2 自动升级到 HTTP/3，也不会
  在 UDP 被屏蔽时回退到 TCP；需要时在应用层组合使用 `http.Client` 和 `http3.Client`。
- 已收到部分响应后连接断开的请求直接失败，不会重发；只有被 GOAWAY 或
  H3_REQUEST_REJECTED 明确标为未处理的请求、以及还在排队没有发出的请求才会重发。
- 带 `Expect: 100-continue` 的请求不会等待 100，body 和请求头一起发送。
- 设置了 `TLSConfig.ClientSessionCache` 时会话恢复（1-RTT）理论上可用，但没有测试过。

## 有意没有实现的功能

| 功能 | 原因 |
| --- | --- |
| 0-RTT | 0-RTT 请求可以被重放，需要 handler 识别并拒绝不幂等的请求，收益和复杂度不成比例。客户端不发送 early data，服务端丢弃 0-RTT 包，客户端随后按 1-RTT 重发。 |
| 连接迁移 / 按 CID 路由 | 需要在 Engine 的 UDP 层加按 Connection ID 分发的逻辑，改变 Engine 按地址区分 UDP 连接的模型。 |
| QPACK 动态表 | 两端都声明容量为 0：不会出现 blocked stream 造成的队头阻塞，编码器流和解码器流都没有内容，也没有额外的状态和内存。代价是重复出现的 header 每次都要完整发送。 |
| Server push | 主流浏览器都不启用 HTTP/3 push。客户端不发 MAX_PUSH_ID，`Context.Push` 返回 `http.ErrNotSupported`。预加载推荐用 103 Early Hints（`WriteInterim`）。 |
| 服务端发送 Retry / NEW_TOKEN | 地址验证只靠握手本身和 3 倍放大限制，省去令牌的签发与校验。客户端能处理服务端发来的 Retry，但会忽略 NEW_TOKEN。 |
| QUIC v2（RFC 9369）等其他版本 | 部署上 version 1 已足够。客户端收到 Version Negotiation 时直接失败。 |
| DATAGRAM（RFC 9221）、Extended CONNECT（RFC 9220）、WebTransport | 依赖流式 stream 或不可靠数据报的 API，当前 handler 模型不支持。对端发来的 DATAGRAM 帧会被当作未知帧，连接以 FRAME_ENCODING_ERROR 关闭。 |
| RFC 9218 可扩展优先级 | 在“body 整体缓存、handler 同步执行”的模型下，调度收益有限。 |
| ECN | 需要在 Engine 的 UDP 层读写 IP 头的 ECN 位，收益主要体现在拥塞控制上。 |

## 待优化项

按优先级排列。

### 1. 正确性与安全加固（高）

- **控制帧与 reset 洪泛**：没有对 PING、MAX_* 等帧，以及 stream 的反复打开和重置做速率
  限制（类似 HTTP/2 的 Rapid Reset）。
- **握手资源消耗**：服务端不发 Retry，也不限制同时进行的握手数量，伪造源地址的 Initial
  洪泛可以消耗 CPU（每个都要做 TLS 运算）。可以考虑在负载高时启用 Retry。
- **closing / draining 状态**：实现 3×PTO 的 closing 期和 CONNECTION_CLOSE 重发；允许
  配置固定的 stateless reset key；`Engine` 停止时给连接发送 GOAWAY 和 CONNECTION_CLOSE。

### 2. 性能（中）

- **pacing**：按拥塞窗口和 RTT 均匀发送，并在 `EAGAIN` 时停止本轮发送，而不是让数据报
  被丢弃。实测 5% 随机丢包下 20 MiB 双向传输约 7 秒，本机回环无丢包约 0.3 秒，丢包场景下
  吞吐主要受这一项和 NewReno 限制。
- **PMTU 探测（DPLPMTUD，RFC 8899）**：以太网上通常可以用到约 1450 字节的数据报，每个
  包的头部和 AEAD 标签开销会降低。
- **拥塞控制**：增加 CUBIC 或 BBR，判断应用受限，实现持续拥塞判定。
- **批量收发**：使用 GSO/GRO、`sendmmsg`/`recvmmsg`，需要 Engine 的 UDP 层配合。
- **减少复制与分配**：发送时 `Write` 复制一次、组包时再复制一次；接收时 Engine、HTTP/3
  帧解析器、body 拼接各复制一次。每个数据报和每个包都有若干次小分配，可以用
  `sync.Pool` 复用缓冲区。
- **数据结构**：已发送的包存在切片里，ACK 处理和丢包检测都是线性扫描；每次 flush 都会
  重置一次 `time.Timer`；发送数据报的系统调用也在连接锁内进行。
- **ACK 频率**：大流量时可以实现 ACK_FREQUENCY 扩展，减少 ACK 数量。
- **ChaCha20-Poly1305**：目前是纯 Go 实现，比汇编实现慢。在有 AES 硬件加速的机器上会
  优先协商 AES-GCM，通常不是瓶颈。
- **基准测试**：补充 QUIC 与 HTTP/3 的 benchmark（单连接吞吐、并发请求、丢包下的吞吐），
  用 pprof 指导优化。

### 3. 流式 body 与 handler 模型（中）

- 与 HTTP/1、HTTP/2 相同：提供流式的请求 body 读取和响应写出，同时让接收窗口的补充
  与实际消费挂钩，形成端到端背压。这也是实现 Extended CONNECT、WebTransport 的前提。

### 4. 可配置性（低）

- 在 `http3.Config` 和 `http3.ClientConfig` 中暴露 keep-alive、接收窗口、单向 stream
  上限、握手超时（服务端）等参数。
- 发送时遵守对端的 `SETTINGS_MAX_FIELD_SECTION_SIZE`。
- 支持发送请求 trailer。
- 客户端：根据 `Alt-Svc` 自动选择 HTTP/3，并在 UDP 不通时回退到 TCP。

### 5. 测试覆盖（低）

一致性测试已完成，见[一致性测试](#一致性测试)。还没有端到端验证的是：

- ChaCha20-Poly1305 在真实握手中被协商的情况：测试机和 CI 机器都有 AES 硬件，握手总是
  选 AES-GCM，这条路径只有 RFC 9001 的测试向量覆盖。
- 会话恢复（`TLSConfig.ClientSessionCache`）。
- 压测才能暴露的并发问题。帧解析、请求构造、QPACK 解码、QUIC 包头、传输参数和帧处理
  都有 fuzz 目标，CI 的 `Fuzz the parsers` job 每个目标跑 20 秒。
- 可以考虑接入 [QUIC Interop Runner](https://github.com/quic-interop/quic-interop-runner)，
  与更多实现（quiche、ngtcp2、mvfst 等）持续做互通测试。

## 一致性测试

测试集在 `go/http3/http3_conformance_test.go`（测试名以 `TestHTTP3Conformance` 开头）
和 `go/http3/protocol_test.go`（手写帧构造的协议错误），CI 的 `HTTP/3 conformance`
job 在 Linux、macOS、Windows 上运行。

标准库没有 HTTP/3 实现，所以正向互通的对端用 quic-go，但它放在 `go/http3/interop`
这个**独立的 module** 里：fib 的 `go.mod` 不引入任何依赖，`go build ./...` 也看不到它。
测试把它编译成一个程序并以子进程方式驱动，两边用 JSON 逐行通信。构建不了（例如没有
网络）时这些用例会跳过，CI 里设了 `FIB_REQUIRE_H3_INTEROP=1`，跳过即失败。

| 对端 | 验证内容 |
| --- | --- |
| quic-go 的 HTTP/3 客户端（对 fib 服务端） | GET/POST/PUT/HEAD、请求与响应头、多个 Cookie 与 Set-Cookie、4 MiB 上传与下载（按哈希校验）、单连接 50 个并发请求、204、响应 trailer、请求 trailer、103 Early Hints、100-continue、417、413、431、各种状态码 |
| quic-go 的 HTTP/3 服务端（对 fib 客户端） | 同样的矩阵反过来验证，另加：超时后连接仍可用、服务端优雅关闭（GOAWAY）时在途请求正常完成、服务端用 Retry 验证源地址、证书不受信任时拒绝连接 |
| 测试内置的手写 HTTP/3 客户端（零依赖，对 fib 服务端） | 正常实现不会发的东西：HEADERS 之前的 DATA、不以 SETTINGS 开头的控制流、第二条控制流、缺少伪头部的请求、引用 QPACK 动态表、CANCEL_PUSH、调低的 MAX_PUSH_ID、抬高的 GOAWAY、QPACK 插入指令、`:authority` 与 `Host` 不一致、两者都缺失 |
| 测试内置的手写 HTTP/3 服务端（零依赖，对 fib 客户端） | 客户端的协议纠错：服务端发来的 MAX_PUSH_ID、CANCEL_PUSH、GOAWAY 指向非请求流、抬高的 GOAWAY、QPACK 插入指令、引用动态表的响应 |
| 内存管道上的 QUIC 两端 | 握手、5% 与 20% 丢包下的传输、stream 数限制、reset、应用关闭、空闲超时、keep-alive、stateless reset、密钥更新（调低 AEAD 上限触发）、解密失败上限 |
| fuzz（`go test -fuzz`） | 面向不可信输入的解析：HTTP/3 帧解析器、从字段构造请求、QPACK 解码与编解码往返、QUIC 包头、传输参数、1-RTT 帧处理 |
