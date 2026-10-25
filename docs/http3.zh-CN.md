# HTTP/3：限制、有意未实现的功能与待优化项

[English](http3.md) | [简体中文](http3.zh-CN.md)

本文记录 Go 版 `http3` package 的边界：当前行为上的限制、出于设计考虑有意没有实现的
功能，以及后续可以优化的方向。功能用法见
[Go 使用指南的 HTTP/3 章节](guide.zh-CN.md#http3-子-package)。

实现位于 [`http3`](../http3)，QUIC 传输层在
[`http3/quic`](../http3/quic)，QPACK 在
[`http3/qpack`](../http3/qpack)。三者全部自行实现，TLS 1.3 握手
使用标准库的 `crypto/tls.QUICConn`，不依赖 quic-go 或 `golang.org/x/net`。

## 已支持的范围（概览）

| 方向 | 内容 |
| --- | --- |
| QUIC | version 1；Initial、Handshake、1-RTT 三个包号空间；AES-128-GCM、AES-256-GCM、ChaCha20-Poly1305 包保护与头部保护；双向 key update（本端按 AEAD 使用上限或可选的时间间隔主动发起，也响应对端发起）与解密失败次数上限；服务端负载高时发送 Retry、客户端处理 Retry，以及 NEW_TOKEN：同一地址上后一个连接可以借此省掉 Retry；Version Negotiation；stateless reset；RFC 9002 丢包检测、PTO 与持续拥塞判定；CUBIC（RFC 9438，默认）或 NewReno 拥塞控制，带 pacing 和应用受限判断；HyStart++（RFC 9406），初始慢启动在 RTT 上升时提前退出；ACK_FREQUENCY（draft-ietf-quic-ack-frequency），大流量传输时协商使用；服务端 3 倍放大限制；连接级与 stream 级双向流控及 BLOCKED 帧；MAX_STREAMS；用 NEW_CONNECTION_ID 签发、可退役的 Connection ID；draining 期；空闲超时与可选 keep-alive |
| 服务端 | 与 HTTP/1、HTTP/2 共用同一个 `Handler` 和 `Context`；1xx 中间响应、自动 100 Continue、请求与响应 trailer、`Response.Close` 通过 GOAWAY 优雅关闭、413/431/417、非法请求（含 `:authority` 与 `Host` 不一致）以 H3_MESSAGE_ERROR 重置、协议错误按 RFC 9114 / RFC 9204 错误码关闭连接、`Request.TLS`、`AltSvc` 辅助函数、Extended CONNECT（RFC 9220）经 `Context.Upgrade` 切换为隧道，`websocket.ServerHandler.Upgrade` 即借此提供 WebSocket over HTTP/3 |
| 客户端 | 异步 `Do`/`Go`；每个 host:port 一条连接、多路复用；遵守 MAX_STREAMS 并排队；取消只重置单个 stream；GOAWAY 与 H3_REQUEST_REJECTED 的请求自动在新连接上重发；`Expect: 100-continue` 等待 100；请求与响应 trailer；对端违反协议时按错误码关闭连接；`AltSvcClient` 把它和 `http.Client` 组合起来按 `Alt-Svc`（RFC 7838）切换，并在失败时回退 TCP |
| 一致性 | 与 quic-go 客户端、服务端双向互通，以及用手写帧构造的协议错误用例，都在 CI 中运行 |
| 互通验证 | quic-go 客户端与服务端（双向，含 5% 丢包）；容器里的 quiche 客户端与服务端（双向，`http3/quicheinterop`）；客户端访问 Cloudflare、Google、nginx、Facebook（mvfst）、Varnish、quiche 的线上服务 |

以上内容都有测试覆盖并在 CI 中运行，见下面的[一致性测试](#一致性测试)。

## 当前限制

使用时需要了解的行为约束。

### 跟随 NAT 重绑定，不支持主动迁移

- Engine 的 UDP socket 按“对端 IP:端口”把数据报分给各自的 `fib.Connection`，QUIC 连接
  挂在客户端最初那个地址的连接上。从没有连接的地址来的 1-RTT 包，按 Connection ID 路由到
  它所属的连接，这样 NAT 重绑定后客户端的连接不会断：新地址来的包通过认证、而且证明是客户端
  最新的包（包号大于之前所有的包，并且带有探测帧以外的帧）后，服务端改向新地址发送，发送量
  不超过从新地址收到的 3 倍，同时发 PATH_CHALLENGE 验证新地址，客户端回应后关闭旧路径
  （RFC 9000 §9.3）。客户端没有按时回应就退回旧路径；旧路径上再来更新的包也会把连接拉回去。
  除非只是端口变了，拥塞控制和 RTT 从头开始；新路径验证完成前数据报降到 1200 字节，验证后重新做路径 MTU 探测。迁移之后
  的请求，`Request.RemoteAddr` 是新地址。
- Linux 上开启 `Config.IOPollers` 和 `ReusePort` 时，新地址可能落到另一个 poller 的
  socket 上；QUIC 连接可以从任意 goroutine 接收数据报，所以连接会跟过去。
- 握手确认后，两端用 NEW_CONNECTION_ID 签发握手之外的 Connection ID，数量取对端
  `active_connection_id_limit` 与 `TransportConfig.ActiveConnectionIDs`（默认 4）中的较小者；
  对端用 RETIRE_CONNECTION_ID 退役一个，就补签一个（RFC 9000 §5.1）。服务端在把新 CID
  发给客户端之前就为它建好路由，所以客户端换地址时顺带换成新 CID（这样旁观者无法把新旧
  路径关联起来），也会像 NAT 重绑定一样被跟随。连接结束时这些路由一并删除。
- 服务端仍然声明 `disable_active_migration`：PATH_CHALLENGE 在连接当前发送的路径上回复，
  待发送的回复最多保留最近 4 个（RFC 9000 §8.2.2 只要求回复最新的挑战），客户端主动探测
  自选的新路径时收不到回复。它的 NAT 可以换地址。对端提供的 CID 会被记录，也会遵守
  retire_prior_to，但本端一直向手上那一个发送；preferred_address 被忽略。

### 与 Engine 的配合

- QUIC 的 `MaxIdleTimeout`（默认 30 秒）必须小于 Engine 的 `Config.UDPIdleTimeout`
  （默认 60 秒），否则 Engine 会先把静默的对端关掉。

### 服务端超时

`http3.Config` 约束客户端能拖长的每一种等待，`DefaultConfig` 为每一项都设了值：

| 约束 | 配置 | `DefaultConfig` | 到期后 |
| --- | --- | --- | --- |
| QUIC 握手 | `HandshakeTimeout` | QUIC 默认 10 秒 | 关闭连接 |
| 收不到任何包的连接 | `MaxIdleTimeout`（QUIC 的） | 30 秒 | 关闭连接 |
| 没有任何请求的连接 | `IdleTimeout` | 2 分钟 | 发 GOAWAY 后关闭连接 |
| 请求 header | `ReadHeaderTimeout` | 10 秒 | 以 `H3_REQUEST_CANCELLED` reset 该流 |
| 请求 body | `ReadTimeout`、`ReadBodyTimeout` | 5 分钟（body） | 流式交给 handler 的 body 以 `os.ErrDeadlineExceeded` 失败；还在收集的 body，其流被 reset |
| 被暂停的 body | `MaxBodyHold` | 1 分钟 | body 以 `http.ErrBodyHeldTooLong` 失败 |
| 响应 | `WriteTimeout` | 不限 | 以 `H3_INTERNAL_ERROR` reset 该流 |

`IdleTimeout` 和 `ReadHeaderTimeout` 未设时用 `ReadTimeout`，与 HTTP/1、HTTP/2 一致；负数表示不限。
只靠 `MaxIdleTimeout` 约束不了空闲连接：发 PING（QUIC keep-alive）的客户端不发请求也能一直保持
连接，`IdleTimeout` 就是为此而设。header 和 body 的计时器只在某次投递没能收全请求时才会创建，
所以一个包就收全的请求（大多数如此）没有这部分开销。
- Engine 为每个 UDP 连接最多排队 1024 个待处理的数据报，超出的会被丢弃，由 QUIC 按丢包
  处理。
- Engine 把每个 UDP 数据报放进一个取自 `bufferpool` 的独立缓冲区交给 QUIC 层，QUIC 层在
  由它引起的 handler 回调都执行完后把缓冲区归还。需要比数据报活得更久的内容会被复制：
  乱序到达、前面还有空缺的数据，等待密钥的包，以及请求 body——body 汇集到一个单独的池化
  缓冲区里，响应结束后归还，与 HTTP/1 相同：handler 既没有 Retain 请求、也没有在响应前
  读完 body，之后再读会得到 `http.ErrBodyReleased`。
- 服务端以 `fib.DatagramsHandler` 的方式成批接收一条连接的数据报：连接的 worker 运行时
  已经排队的数据报一起处理，ACK 和这期间写出的响应尽量装进更少的包里一起发出。
- 交给 handler 池的请求会告诉它的连接即将有响应写出（`quic.Conn.ExpectWrite`）：池里各个
  goroutine 写出的响应等这一批其余的响应（最多 1 毫秒），再和这批请求的 ACK 一起尽量装进
  更少的包里发出，与 handler 在连接上直接应答时一样。同一批数据报里完整到达的请求一次性
  成批交给协程池，落在池的同一个分片上、先后紧挨着得到应答，而不是像逐个提交那样按各分片
  队列的深浅拉开距离。这 1 毫秒从有东西要发时才开始计算：还没到期的 ACK 只按它自己的
  截止时间发送。
- 请求的状态（`requestStream`、`http.Request`、它的 header 和 Context）在响应写出、
  handler 返回后就释放，不再保留到客户端确认响应为止；服务端在握手结束后也会释放 TLS
  连接，只保留它报告的 `tls.ConnectionState`。
- ACK 不再重复确认对端已经看到被确认过的范围（RFC 9000 第 13.2.4 节），所以对跳过包号的
  对端（quiche 就会这样做），ACK 帧只带少数几个范围，而不是保留的全部范围。
- `Engine.Shutdown` 会发送 GOAWAY，让在途请求完成，再以 H3_NO_ERROR 关闭连接；但单独
  调用 `Engine.Stop`/`Close` 时不会给 HTTP/3 连接发送 CONNECTION_CLOSE 或 GOAWAY，
  连接被直接丢弃，客户端要等空闲超时才会发现。
- 与 `http` package 相同，`http3` 只在 Linux、macOS、Windows 三个原生后端上编译，兼容
  后端（如 FreeBSD）上这个 package 为空。

### 消息体整体缓存

- 请求 body 默认在 handler 运行前完整读入内存。设置 `Config.StreamRequestBody` 后，body
  一开始到达就调用 handler——带 `content-length` 且超过 `StreamRequestBodyThreshold` 的在
  HEADERS 到达时，不带 `content-length` 的在收到超过阈值的数据时——handler 从
  `Request.Body`（不阻塞的 `*http.BodyStream`）读，或用 `Context.OnBody` 接收，和 HTTP/1、
  HTTP/2 一样。这时 QUIC 只在 handler 消费了 body 之后才补充 stream 的接收窗口，快速上传
  的客户端最多领先一个窗口（1MB）。流式 body 的上限是 `MaxStreamedBodyBytes` 而不是
  `MaxBodyBytes`；`Expect: 100-continue` 要等 handler 第一次要 body 时才回复 100。
- 交给 `WriteResponse` 或 `Respond` 的响应通过 `Response.Body []byte` 一次性给出；客户端
  默认把响应 body 完整缓存后再回调，设置 `ClientConfig.StreamResponseBody` 后大 body 边到边
  交给回调。`Context` 的 `http.ResponseWriter` 方法可以使用（含
  trailer），并与 HTTP/1、HTTP/2 一样流式输出：不超过 4KB（或设置了不超过 64KB 的
  `Content-Length`）且没有 `Flush` 的响应在 handler 返回后整体发送，更长的或 `Flush` 过的
  响应边写边发——header 单独一个 HEADERS 帧，body 用 DATA 帧，trailer 跟在后面——因此
  SSE、流式下载都可以实现。handler 写得比客户端读得快时，一旦有 64KB body 在 QUIC stream
  里等待流控或拥塞控制，`Write` 就会等待，直到部分发出或 stream 结束；连接在接收数据报的
  协程上运行的 handler 从不等待，它写的内容留在 stream 里等待发送。
- 内存上限：服务端单连接最坏约为 `MaxConcurrentStreams × MaxBodyBytes`（默认
  100 × 16MB），客户端单个响应受 `MaxResponseBodyBytes` 限制。整体响应会拷进一块
  QUIC 帧缓冲并保留到对端确认，因此一个卡住的 stream 要占 body 大小按 2 的幂向上取整
  （8MiB 变 16MiB），上界约为 `MaxConcurrentStreams` 乘以这个值。用 `Write` 和
  `Flush` 流式发送只需 64KiB；见[压测](loadtest.zh-CN.md)。
- 请求完整后按 `Config.StreamPool` 的描述交给 engine 的 handler 协程池（与 HTTP/2 server
  相同：同名 engine 的 `<Name>-streams`，不与 engine 的协程池共用），因此同一连接上客户端并发发起的多个请求是并发处理的，阻塞的 handler 只拖累它
  自己。`StreamPool.MaxConcurrentHandlers` 限制单个连接同时处理的请求数 N，其中第 N 个在
  读取该连接的协程上执行，在它返回前该连接不再读取新数据；N 为 1 时（与
  `StreamPool.Disable` 相同）又回到引入协程池之前的串行处理。
- 不要调用 `Context.Conn.Send` 直接写数据：`Conn` 是 UDP 连接，所有输出都必须经过
  `Context`。
- 请求结束后，请求的 stream 连同它持有的 `*http.Request`、`Header`、`URL`、`*Context` 会被
  复用（设置 `Config.DisableReuse` 则不复用）：handler 不能在响应完成、最后一次 `Release` 之后
  继续持有它们，规则同 `http.Config.ReuseRequests`。body 流式交付的请求和 extended CONNECT
  不复用。服务端限制在 8 个 CPU 时，复用后每秒 156 万请求，不复用 119 万。

### 固定参数

`Config.Transport` 和 `ClientConfig.Transport`（类型为 `http3.TransportConfig`）设置服务端或
客户端下 QUIC 连接的行为：

| 参数 | 默认值 | 字段 |
| --- | --- | --- |
| 每个 stream 的接收窗口 | 1MB，消费一半后补充 | `StreamReceiveWindow` |
| 连接的接收窗口 | 16MB，消费一半后补充 | `ConnReceiveWindow` |
| 对端可以打开的单向 stream 数 | 16 | `MaxIncomingUniStreams` |
| ACK 最多延迟多久（`max_ack_delay`） | 25ms；此外每 2 个需要确认的包立即回一次 ACK | `MaxAckDelay` |
| keep-alive | 不发：什么都不传的连接空闲 `MaxIdleTimeout`（30 秒）后关闭，下一个请求重新握手 | `KeepAlivePeriod` |
| 洪泛额度 | 突发 1000，每秒恢复 100（见[协议细节](#协议细节)） | `StreamResetBurst`、`StreamResetRate`、`ControlFrameBurst`、`ControlFrameRate` |
| 按时间更新密钥 | 不做：密钥在 AEAD 的包数上限时更新 | `KeyUpdateInterval` |
| 签发给对端的 Connection ID 数 | 4，以对端允许的为限 | `ActiveConnectionIDs` |

以下参数仍是常量：

| 参数 | 值 |
| --- | --- |
| 数据报大小 | 1200 字节，直到路径 MTU 探测找到更大的（见[协议细节](#协议细节)），最多 1452；已知网络能承载更大数据报时，服务端可以用 `Config.MaxDatagramSize` 从更大的值开始 |
| 客户端声明的双向 stream 上限 | 1（服务端不能打开请求 stream） |
| QPACK 动态表容量 | 0 |
| 可记住的已接收包号区间 | 32 个，更早的包被当作重复包丢弃 |

### 协议细节

- **关闭**：发出 CONNECTION_CLOSE 的连接保留一段 closing 期（RFC 9000 §10.2.1），长度为
  3 个 PTO，至少 1 秒、至多空闲超时；其间对之后到达的包重发 CONNECTION_CLOSE，在第 1、2、
  4、8……个包时各发一次。收到对端 CONNECTION_CLOSE 的连接进入同样长的 draining 期（RFC 9000
  §10.2.2）：不再发送任何东西，但保留路径，对端仍在途中的包被静默吸收，而不是回一个
  stateless reset 或被当成新连接。
  `Engine.Close` 在结束连接之前给每个连接（服务端的和客户端的）发送 CONNECTION_CLOSE
  （H3_NO_ERROR）；`Engine.Stop` 让连接保持打开，直到 `Close`。
- **密钥更新**：本端在一把密钥保护的包数达到 RFC 9001 §6.6 的上限（AES-GCM 约 2^23 个
  包）时主动发起 key update，也响应对端发起的更新；解密失败次数超过完整性上限时以
  AEAD_LIMIT_REACHED 关闭连接。`TransportConfig.KeyUpdateInterval` 还可以让发送密钥用满
  这段时间后更新；更新仍要等对端确认过当前密钥的包，所以空闲的连接不会更新。不支持按需更新。
- **Stateless reset**：reset key 由每个 `ServerHandler` 随机生成，`Config.StatelessResetKey`
  可以指定固定的 key；用随机 key 时，服务端重启后客户端认不出它对重启前的连接发出的 reset。
  只有客户端会识别 stateless reset。
- **洪泛**：对端不请求任何东西就能让连接做的事有上限，默认突发 1000 次、每秒恢复 100 次
  （`TransportConfig.StreamResetBurst`/`StreamResetRate` 与 `ControlFrameBurst`/`ControlFrameRate`，
  小于 0 不限制），超过后连接以 H3_EXCESSIVE_LOAD 关闭，见[待优化项](#1-正确性与安全加固)。
- **BLOCKED 类帧**：有数据要发却被对端流控挡住时，每碰到一个新上限报告一次：连接级用
  DATA_BLOCKED，stream 级用 STREAM_DATA_BLOCKED，想开新 stream 而对端还不允许时用
  STREAMS_BLOCKED（RFC 9000 §4.1）；丢失的只在该上限仍然有效时重发，还没发出的在上限被
  提高后丢弃。
- **拥塞控制**：默认 CUBIC（RFC 9438），可用 `quic.Config.CongestionControl` 改为
  NewReno，两者都在初始慢启动里用 HyStart++ 判断 RTT 上升而提前退出；没有 BBR 和 ECN。
  发送按每个平滑 RTT 1.25 个拥塞窗口的速率 pacing。
  发送缓冲区满（`EAGAIN`）时，这一轮剩下的数据报保留下来，Engine 在 socket 又可写时
  （`fib.Connection.NotifyWritable`）通知发送方，于是它先于新数据立刻续发；无法通知时
  ——Windows 的完成端口、portable 后端，以及 macOS 对写满的 socket 报的 `ENOBUFS` 和
  Linux 对设备队列满报的 `ENOBUFS`——仍由 1 毫秒的定时器重试。
- **路径 MTU 探测**（DPLPMTUD，RFC 8899；RFC 9000 §14.3），两端默认开启：握手确认后连接
  发一个探测包——用 PADDING 填到 1452 字节（`MaxProbeDatagramSize`，且不超过对端的
  `max_udp_payload_size`）的 PING——被确认后就按这个大小发送数据报。同一大小的探测包丢 3 次，
  就在下面的区间二分查找，精确到 20 字节以内，10 分钟后再从上限试一次。探测包丢失不算拥塞。
  只在 Engine 的 UDP socket 能设成不分片时进行（`fib.Connection.SetDontFragment`：Linux 用
  `IP_PMTUDISC_PROBE`，macOS、FreeBSD、Windows 都支持，其他 BSD 只有 IPv6），因为被分片的
  探测包证明不了什么；`Config` 和 `ClientConfig` 的 `DisablePathMTUDiscovery` 关闭它。黑洞检测：
  发送大于 1200 字节数据报的连接，在 3 个更大的包丢失、而它们之后发出的大包一个都没被确认时，或者
  连续 3 次 PTO 时，退回 1200 字节，1 分钟后重新探测；用 `MaxDatagramSize` 设的大小也是如此。
  拥塞窗口仍按 1200 字节的数据报计算。
- **对端的 `SETTINGS_MAX_FIELD_SECTION_SIZE`**：对端 SETTINGS 到达后发送时遵守，按
  RFC 9114 §4.2.2 计算每个字段。服务端的响应头、1xx 或 trailer 超过时写入失败，handler 可以
  改用别的回应；body 已发出后才超限的 trailer 改为重置 stream，免得客户端把 body 当成完整的。
  客户端请求的头或 trailer 超过时请求直接失败，不发送。
- **优先级**：`priority` 头和 PRIORITY_UPDATE 帧都被忽略，待发送的 stream 轮流发送。
- **ECN**：不设置也不上报 ECN 标记。ACK_ECN 帧能解析，但其中的计数被忽略。

### 客户端

- 只支持 `https://`。`Client` 自身不会根据 `Alt-Svc` 从 HTTP/1.1 或 HTTP/2 自动升级到 HTTP/3，
  也不会在 UDP 被屏蔽时回退到 TCP；做这件事的是 `http3.AltSvcClient`：它让一个 origin 的
  第一个请求走 `http.Client`，之后按响应里的 `Alt-Svc: h3="host:port"`（RFC 7838）走 HTTP/3，
  请求的 `:authority` 和证书校验用的名字仍是 origin 的；QUIC 连不上时，可以安全重发的请求
  回退到 TCP，并把这个 origin 在 TCP 上留一段时间。
- 已收到部分响应后连接断开的请求直接失败，不论方法都不重发，与 `net/http` 一致：服务端
  已经开始回应，说明已经处理了请求，流式响应的回调也可能已经把部分 body 交出去了。只有被
  GOAWAY 或 H3_REQUEST_REJECTED 明确标为未处理的请求、以及还在排队没有发出的请求才会重发；
  调用方确认请求在部分响应后重发也安全时，可以自己再发一次。理由与 HTTP/2 客户端相同，见
  [HTTP/2](http2.zh-CN.md#客户端)。
- 带 body 和 `Expect: 100-continue` 的请求先单独发送 HEADERS，等服务端回 100、或
  `ClientConfig.ExpectContinueTimeout`（`DefaultClientConfig` 中为 1 秒；为零时立即发送
  body）到期后再发送 body。如果先收到最终响应，它就是答复：body 不会发送，响应完整后重置
  stream 的发送方向。
- 会话从 `ClientConfig.SessionCache` 恢复（本库的握手），握手走 `crypto/tls`（`BlockingTLSHandshake`，或 `TLSConfig` 只有
  `crypto/tls` 能服务）时从 `TLSConfig.ClientSessionCache` 恢复。只有重发也无妨的请求会作为 0-RTT 数据发出，见
  [0-RTT](#0-rtt)。
- `ClientConfig.TLS13CipherSuites` 决定 ClientHello 提供哪些 TLS 1.3 套件、按什么顺序，按套件 ID 列出；为空时是默认顺序，
  CPU 有 AES-GCM 硬件加速时 AES-GCM 在前，否则 ChaCha20-Poly1305 在前，和 `crypto/tls` 一致。TLS 1.3 服务端从客户端提供的
  套件里选，所以只列一个就是限定这条连接用哪个：runner 的 `chacha20` 用例就是
  `[]uint16{tls.TLS_CHACHA20_POLY1305_SHA256}`。服务端没有对应的开关，因为 TLS 1.3 服务端不自己提出套件；走 `crypto/tls`
  的握手自己选套件，`crypto/tls` 没有 TLS 1.3 套件的字段。

### 0-RTT

恢复会话的客户端可以把头几个请求随第一个飞行一起发出（RFC 9001 第 4.6 节），比握手早一个往返。这些数据用只来自会话的密钥加密，
没有前向保密，抓到包的攻击者可以重放。本库的处理：

- **两端都默认关闭**：服务端 `Config.EarlyData`（会话票据随之允许 early data），客户端 `ClientConfig.EarlyData`。
- **一张票据只带一次 early data。** 服务端记住带过 early data 的票据（到票据过期为止），再次使用时拒绝 early data（连接仍然恢复）。
  客户端报告的票据年龄和服务端自己算的相差超过 10 秒、握手经过 HelloRetryRequest、签发票据时给出的传输限制高于现在，也拒绝。
  这个登记表是每个 `Handler` 一份；不是签发票据的那台服务端无从得知，所以放在负载均衡后面的服务端要自己防重放。
- **只服务安全方法。** 服务端给作为 early data 到达的请求加 `Early-Data: 1`（RFC 8470），只服务 GET、HEAD、OPTIONS、TRACE，
  其他方法回 425 Too Early（`Config.EarlyDataUnsafe` 全部服务，由 handler 检查这个头）；QUIC 的 `Stream.EarlyData` 对 stream
  说同样的事。客户端只把这些方法且没有 body 的请求作为 early data 发出（`ClientConfig.EarlyData`），收到 425 的请求在握手结束后重发。
  `Client.Stats` 统计作为 early data 发出的、被接受的、被拒绝的（服务端丢弃了 early data，QUIC 层随后用 1-RTT 密钥重发）和
  回 425 的请求数。
- **握手结束前就可以回应。** 接受 early data 的服务端在发出自己的 Finished 之后、客户端的 Finished 到达之前就回应（0.5-RTT）。
  `quic.EarlyDataHandler` 让 handler 在 early data 到达时就收到；不是它的 handler 在握手结束后才收到。

客户端的会话由本库的 TLS 保存（`tls.NewQUICSessionCache`，或 `ClientConfig.SessionCache`）；另一种握手用的 `crypto/tls` 的
`QUICConn` 通过同一套 QUIC 代码恢复会话、发送 early data。

## 有意没有实现的功能

| 功能 | 原因 |
| --- | --- |
| 主动连接迁移 | 服务端已为每个连接签发并路由多个 Connection ID，但 PATH_CHALLENGE 在当前发送的路径上回复，而不是挑战到达的路径，客户端迁移前探测新路径无法完成验证；新路径上也仍向客户端的第一个 Connection ID 发送。NAT 重绑定（无论客户端是否换了 CID）会被跟随（见上）。 |
| QPACK 动态表 | 两端都声明容量为 0：不会出现 blocked stream 造成的队头阻塞，编码器流和解码器流都没有内容，也没有额外的状态和内存。代价是重复出现的 header 每次都要完整发送。 |
| Server push | 主流浏览器都不启用 HTTP/3 push。客户端不发 MAX_PUSH_ID，`Context.Push` 返回 `http.ErrNotSupported`。预加载推荐用 103 Early Hints（`WriteInterim`）。 |
| QUIC v2（RFC 9369）等其他版本 | 部署上 version 1 已足够。客户端收到 Version Negotiation 时直接失败。 |
| DATAGRAM（RFC 9221）、WebTransport | 依赖流式 stream 或不可靠数据报的 API，当前 handler 模型不支持。对端发来的 DATAGRAM 帧会被当作未知帧，连接以 FRAME_ENCODING_ERROR 关闭。 |
| RFC 9218 可扩展优先级 | 在“body 整体缓存”的模型下，调度收益有限。 |
| ECN | 需要在 Engine 的 UDP 层读写 IP 头的 ECN 位，收益主要体现在拥塞控制上。 |

## 待优化项

按优先级排列。

### 1. 正确性与安全加固

已完成：

- **控制帧与 reset 洪泛**：对端有下列行为、超过突发 1000 次（每秒恢复 100 次，与 HTTP/2
  相同）时，QUIC 连接关闭，HTTP/3 下错误码为 H3_EXCESSIVE_LOAD（`quic.Config.ExcessiveLoadCode`）：
  - 在 stream 仍打开时用 RESET_STREAM 或 STOP_SENDING 放弃它，或发送畸形请求让服务端
    重置它，每个 stream 只计一次（HTTP/3 版的 Rapid Reset，CVE-2023-44487）；只有服务端计数，
    回应服务端自己 STOP_SENDING 的 RESET_STREAM 也不计；
  - 发送要求本端做事却不带任何内容的帧：只有 PING 的包、PATH_CHALLENGE、没有提高上限的
    MAX_DATA、MAX_STREAM_DATA 和 MAX_STREAMS、没有报告新上限的 DATA_BLOCKED、
    STREAM_DATA_BLOCKED 和 STREAMS_BLOCKED（发送方每到一个新上限只报告一次）、没有数据也不结束 stream 的 STREAM 帧，以及针对已退役 connection ID
    的 NEW_CONNECTION_ID。

  和确认了新包的 ACK 一起发来的 PING 不计数：只发 ACK 的一端（比如在下载的客户端）会不时
  捎带一个 PING，让对端确认它的 ACK；对端可能在 stream 结束后仍在途中的帧也不计数。
  HTTP/3 自己的帧只花解析字节的开销（流控在 QUIC 层），不计数。这些预算在 `TransportConfig` 中设置。
- **握手资源消耗**：进行中的握手超过 `Config.RetryHandshakes`（默认 1024；小于 0 表示
  总是）时，新客户端先收到 Retry，它带回的令牌证明了源地址之后，服务端才做 TLS 运算、
  保存状态；超过 `Config.MaxHandshakes`（默认 4096）时新客户端的 Initial 被丢弃。
  不合格的 Retry 令牌（来自别的地址，或已超过 10 秒）以 INVALID_TOKEN 回应。令牌的 key
  由每个 handler 随机生成，所以同一地址后面的另一台服务端不认这台发出的 Retry。
  握手完成之后，服务端再给客户端一个 NEW_TOKEN 令牌（RFC 9000 8.1.3 节），绑定客户端
  的地址、有效期 10 分钟：同一地址上的后一个连接把它放在第一个 Initial 里，服务端就
  直接把地址视为已验证，既省掉 Retry 的一个往返，也不必先为它做 TLS 运算。来自别的
  地址、已经过期、或由服务端已不持有的 key 签出的 NEW_TOKEN 等同于没有令牌：连接按
  新客户端的方式开始，服务端可以再发 Retry，而不是关闭连接。客户端用一个每个
  `Client` 一份的缓存保存少数几个地址的令牌，每个 5 分钟；
  `Transport.DisableNewTokens` 可以在任一端关掉这套机制。
- **closing / draining 状态**：3×PTO 的 closing 期，其间对到达的包重发
  CONNECTION_CLOSE；收到对端 CONNECTION_CLOSE 后同样长的 draining 期，期间不发送；
  可配置固定的 stateless reset key（`Config.StatelessResetKey`）；`Engine.Close` 时给连接
  发送 CONNECTION_CLOSE，`Engine.Stop` 让连接保持打开到那时（见[协议细节](#协议细节)）。

### 2. 性能（中）

- **pacing、拥塞控制、丢包恢复的数据结构**：已完成。发送按每个平滑 RTT 1.25 个拥塞窗口
  的速率 pacing，突发上限为初始窗口与 2 毫秒发送量中的较大者（RFC 9002 7.7 节），pacing
  压住的部分由定时器接着发；socket 以 `EAGAIN` 拒收的一轮数据报保留下来，等 Engine 通知
  socket 可写时立刻重发，而不是丢掉（`fib.Connection.SendDatagrams` 报告一批里发出了
  几个）。默认 CUBIC（RFC 9438），可在 `quic.Config.CongestionControl` 选 NewReno；
  窗口只在被用满时增长（RFC 9002 7.8 节），持续拥塞（7.6 节）时降到最小窗口。已发送的包
  按包号存进环形队列，一个 ACK 帧的开销只与它新确认的包相关，丢包检测只看到最大已确认包为止；定时器只在截止时间提前时才重设。
  在基准机上实测（`TestLossyTransfer`，经内存路径双向各 20 MiB，三轮）：5% 随机丢包下
  改动前 1.2-1.7 秒，改动后 0.40、0.43、1.06 秒；无丢包时改动前 235-275 毫秒，改动后
  82-92 毫秒。fit 的 fixed-h3 从每秒 165.8 万请求提高到 177.3 万（+7%），static-h3 不变
  （117.5 万、116.5 万）。
- **PMTU 探测（DPLPMTUD，RFC 8899）**：已完成，带黑洞检测（见[协议细节](#协议细节)）。
  还没做的是探测 1452 字节以上（巨型帧，或 IPv4 上的 1472），这需要更大的发送缓冲区；
  以及按探测到的大小计算拥塞窗口。
- **HyStart++**（RFC 9406）：已完成，用于初始慢启动。每轮记下本轮与上一轮的最小 RTT，
  本轮比上一轮高出八分之一（下限 4 毫秒、上限 16 毫秒）时离开慢启动，进入保守阶段，
  窗口按慢启动的四分之一增长；RTT 回落说明那只是抖动，回到慢启动；保守阶段最多 5 轮，
  之后把当前窗口作为阈值进入拥塞避免。出现丢包后不再使用它：此后由丢包恢复得到的阈值
  决定退出点。RTT 平稳的路径上（内存管道、loopback）4 毫秒的下限让它根本不会触发，
  所以它的行为由单元测试覆盖，不体现在下面的数字里。
- **BBR** 经过评估后有意未实现：它换的是对路径的模型而不只是窗口，靠每轮投递量估计带宽、
  算出 pacing 速率，还有自己的 startup、drain、probe-bw 阶段；要做得可靠，得把已发送包
  的记录扩展到每轮的投递率采样，并与 RFC 9002 的丢包恢复、持续拥塞判定协调起来，改动
  很大，而且除了内存管道这里没有可靠的验证手段。半成品会比没有更糟。
- **ECN** 尚未实现。
- **批量收发**：Engine 在 Linux 上用 `recvmmsg`、macOS 上用 `recvmsg_x` 批量读取；一轮
  发送多于一个数据报时用 `sendmmsg` 或 `sendmsg_x` 一次发出（`fib.Connection.SendBatch`），服务端
  和客户端的连接都是如此。
  Linux 上一批里大小相同的连续数据报用 GSO（`UDP_SEGMENT`）作为一条消息交给内核分段，8 个 CPU
  的服务端上 static-h3 从每秒 4 万请求提高到 7.7 万。接收方向在 Linux 上打开 `UDP_GRO`：
  内核合并的一串数据报作为一次读取返回，Engine 按控制消息给出的大小再拆回单个数据报，
  一串数据报因此只花一次接收而不是每个一次系统调用。内核不支持 `UDP_GRO` 时以及其他系统上
  仍然一次读一个数据报。
- **减少复制与分配**：发送时 `Write` 复制一次（复制进缓冲池里的缓冲区）、组包时再复制
  一次；接收时 Engine 复制一次（复制进缓冲池里的缓冲区），HTTP/3 帧解析器只复制跨片段的帧
  和请求 body。HTTP/3 的响应本来就用 `WriteOwned` 送入，所以只付第二次复制；第一次需要
  Engine 用多个缓冲区拼一个数据报（iovec 的改动，属于别的工作流）。包保护已经不再
  分配：ChaCha20-Poly1305 的认证器原来每个包一次堆分配，现在是值类型。QUIC stream 有意
  不做池化：`*Stream` 会交给应用，生命周期由应用决定，重用会把「stream 结束后继续使用」
  变成破坏另一条 stream。开、关一条 stream 8 次分配，内存管道上一个请求 11-12 次；请求
  stream 和 header 会被复用（见上文）。
- **ACK 频率**：ACK_FREQUENCY 扩展（draft-ietf-quic-ack-frequency）已完成。两端都在传输
  参数里声明 `min_ack_delay`；对端声明了的一端在已发送 32 KiB 之后请求对端每 10 个
  ack-eliciting 包确认一次（容忍 2 个包的乱序），且最迟不超过平滑 RTT 的四分之一，这样
  窗口小于阈值时一个 RTT 内也能拿到几次反馈。发生丢包后本端会再请求一次（序号更大的
  第二个帧，每个 RTT 至多一次），改回默认的每两个包确认一次，直到连接结束：丢包后窗口
  往往小到填不满阈值，而丢包恢复依赖确认。对端没声明这个扩展就不发这个帧；对端忽略它
  也只是照旧确认，因为请求只是建议，本端没有任何行为依赖它。接收端采用序号最大的请求，
  收到 IMMEDIATE_ACK 立即确认。在共享的基准机上取六次样本（两轮交替）的中位数：
  64 KiB 传输在噪声范围内（中位数从 567 到 533 MiB/s，较安静的第一轮从 582 到
  692 MiB/s），1 MiB 从 714 到 877 MiB/s，4 MiB 从 882 到 1350 MiB/s，HTTP/3 上传从
  369 到 589 MiB/s，小请求从 6018 到 5780 纳秒，并发请求从 4876 到 3672 纳秒；1% 丢包
  下从 19.0 升到 20.0 MiB/s，5% 丢包下从 4.4 升到 7.2 MiB/s。确认变少会让丢包恢复
  略慢一点（丢包后的更新让它不至于更差），而确认自己的包也少了一批，同样少被丢掉。
- **ChaCha20-Poly1305**：标准库没有导出 ChaCha20-Poly1305 AEAD（`crypto/hpke` 里有一个，
  但隔着 internal 包和标准库内部的 `golang.org/x/crypto` 副本，标准库之外的模块 import
  不到；`crypto/cipher` 也没有），所以是本库自己的实现。现在在纯 Go 范围内写得更快：
  块函数的 16 个字放在变量里而不是数组里，密钥流按 8 字节一次异或，认证器就地初始化、
  不再每个包分配一次。实现放在 `internal/chacha20poly1305`，TLS 记录层保护
  TLS_CHACHA20_POLY1305_SHA256 和 RFC 7905 套件也用它。实测 1200 字节的包 seal 从 355
  到 548 MiB/s、open 从 358 到 551 MiB/s，16 KiB 从 398 到 682 MiB/s，一次头部保护的
  块函数从 141 到 78 纳秒。
  汇编（AVX2/NEON）还能更快，但那是分架构、常量时间容易写错的一大块代码，而只有没有
  AES 硬件的机器才会优先协商这个套件，所以没有写。这条路径现在有端到端测试：
  `TestChaCha20Poly1305Handshake` 在带 `GODEBUG=cpu.aes=off` 的子进程里跑握手
  （crypto/tls 于是优先 ChaCha20），确认两端都用它保护包和头部、数据双向都能通过。
- **基准测试**：已完成，都是普通的 `go test -bench` 目标。`http3/quic` 里有内存管道上
  64 KiB、1 MiB、4 MiB 的单连接传输，单个与按 CPU 数并发的请求，1% 与 5% 丢包下的传输，
  开、关一条 stream，以及 ChaCha20-Poly1305 的 seal、open 和头部保护；`http3` 里有
  64 KiB 与 1 MiB 的响应 body、上传、小请求和并发请求，走 loopback 的 UDP socket，
  包含 Engine 和 HTTP/3 组包。上面的数字就是它们在基准机上的中位数：本轮改动前后各取
  六个样本，两轮交替测量。

### 3. 流式 body 与 handler 模型（中）

- 已完成：流式请求 body（接收窗口随 handler 的消费补充）和流式响应写出（handler 写多少
  就发多少）。
- 双向流式是 WebTransport 的前提；Extended CONNECT 已通过 `Context.Upgrade` 实现，但
  handler 模型还没有同时操作两个方向的 API。

### 4. 可配置性（低）

- 已完成：keep-alive、接收窗口、单向 stream 上限、ACK 延迟、洪泛额度、按时间更新密钥和签发的
  Connection ID 数可在 `Config.Transport` 和 `ClientConfig.Transport` 中设置；发送时遵守对端的
  `SETTINGS_MAX_FIELD_SECTION_SIZE`；客户端可以发送请求 trailer（body 整体发送或流式发送都可以）；
  `AltSvcClient` 按 `Alt-Svc` 选择 HTTP/3，并在 UDP 不通时回退到 TCP。

### 5. 测试覆盖（低）

一致性测试已完成，见[一致性测试](#一致性测试)。还没有端到端验证的是：

- 0-RTT 与 quiche、quic-go 之外更多服务端的互通，以及共享票据密钥的多台服务端之间的重放（票据登记表是每个进程一份）。
- 压测才能暴露的并发问题：已由[压测](loadtest.zh-CN.md)覆盖，fib 两端对每种配置
  压 30 秒，没有请求失败、reset 或 GOAWAY。它暴露了上面整体响应的内存代价；带丢包的
  路径和长时间 soak 仍未覆盖。帧解析、请求构造、QPACK 解码、QUIC 包头、传输参数和帧
  处理都有 fuzz 目标，CI 的 `Fuzz the parsers` job 每个目标跑 20 秒。
- QPACK 动态表、服务端推送、DATAGRAM 和 ECN 都未实现，无法测试；
  [QUIC Interop Runner](#quic-interop-runner) 端点会把 ECN 用例如实报成“不支持”，
  而不是让它悄悄通过。

与其他实现的互通现在有了容器化端点和 CI job，见下面的
[QUIC Interop Runner](#quic-interop-runner)。

## 一致性测试

测试集在 `http3/http3_conformance_test.go`（测试名以 `TestHTTP3Conformance` 开头）
和 `http3/protocol_test.go`（手写帧构造的协议错误），CI 的 `HTTP/3 conformance`
job 在 Linux、macOS、Windows 上运行。

标准库没有 HTTP/3 实现，所以正向互通的对端用 quic-go，但它放在 `http3/interop`
这个**独立的 module** 里：fib 的 `go.mod` 不引入任何依赖，`go build ./...` 也看不到它。
测试把它编译成一个程序并以子进程方式驱动，两边用 JSON 逐行通信。构建不了（例如没有
网络）时这些用例会跳过，CI 里设了 `FIB_REQUIRE_H3_INTEROP=1`，跳过即失败。

### QUIC Interop Runner

`http3/interoprunner` 是 [QUIC Interop
Runner](https://github.com/quic-interop/quic-interop-runner) 的端点，和
`http3/interop` 一样是独立 module。同一个镜像两种角色：`ROLE=server` 时在 UDP 443
上按 `TESTCASE` 用 `/certs` 里的证书提供 `/www`；`ROLE=client` 时把 `REQUESTS`
下载到 `DOWNLOADS`，并把 TLS 密钥写进 `SSLKEYLOGFILE`，让 runner 能解密抓包。
runner 除 `http3` 外都走 HTTP/0.9（QUIC stream 上一行请求加原始文件），所以这些用例
直接跑 fib 的 QUIC 传输；`http3` 则跑 fib 完整的 HTTP/3 服务端和客户端。

```sh
docker build -f http3/interoprunner/Dockerfile -t fib-interop .
http3/interoprunner/local-test.sh       # 镜像自己对自己，不需要 runner
http3/interoprunner/run-runner.sh       # 真正的 runner，fib 对 quic-go
```

`local-test.sh` 只需要 Docker 和 openssl，用同一个镜像做两端跑七个用例。
`run-runner.sh` 需要带 compose 的 Docker、tshark 和 runner 的 Python 依赖，
会克隆 runner、登记镜像，然后与 quic-go 双向跑；`TESTCASES`、`PEER`、`DIRECTION`
可以选择子集。逐个 testcase 的支持表、以 127 退出的原因和实测结果都在
`http3/interoprunner/README.md`。

CI 里有两个 job：`HTTP/3 interop endpoint image`（阻塞：构建镜像并让端点自己对自己
跑支持的用例）和 `HTTP/3 QUIC Interop Runner`（只在 `workflow_dispatch` 触发、
非阻塞：真正的 runner 对 quic-go；它要拉模拟器和对端镜像，还要 tshark，较慢也更容易
受环境影响）。

| 对端 | 验证内容 |
| --- | --- |
| QUIC Interop Runner 的 fib 端点（`http3/interoprunner`，对 quic-go 端点） | 双向、两种角色：HTTP/3，以及 HTTP/0.9-over-QUIC 的握手、传输、Retry、会话恢复、0-RTT、密钥更新、多路复用、丢包、IPv6、NAT 重绑定和长 RTT。fib 没有 ECN、QUIC v2 和 `preferred_address`，这些用例以 127 退出，runner 会如实记为“不支持”；没有通过的两个（服务端在地址重绑定下、客户端在长 RTT 下）在那个 README 里单独列出并说明原因。`chacha20` 双向都通过：客户端用 `http3.ClientConfig.TLS13CipherSuites` 只提供 ChaCha20-Poly1305，服务端接受它。 |
| quic-go 的 HTTP/3 客户端（对 fib 服务端） | GET/POST/PUT/HEAD、请求与响应头、多个 Cookie 与 Set-Cookie、4 MiB 上传与下载（按哈希校验）、单连接 50 个并发请求、204、响应 trailer、请求 trailer、103 Early Hints、100-continue、417、413、431、各种状态码 |
| quic-go 的 HTTP/3 服务端（对 fib 客户端） | 同样的矩阵反过来验证，另加：超时后连接仍可用、服务端优雅关闭（GOAWAY）时在途请求正常完成、服务端用 Retry 验证源地址、证书不受信任时拒绝连接 |
| 测试内置的手写 HTTP/3 客户端（零依赖，对 fib 服务端） | 正常实现不会发的东西：HEADERS 之前的 DATA、不以 SETTINGS 开头的控制流、第二条控制流、缺少伪头部的请求、引用 QPACK 动态表、CANCEL_PUSH、调低的 MAX_PUSH_ID、抬高的 GOAWAY、QPACK 插入指令、`:authority` 与 `Host` 不一致、两者都缺失 |
| body 矩阵的手写 QUIC 客户端（`http3/body_matrix_test.go`，对 fib 服务端） | 请求 body 的各种到达方式：无 body、带 content-length、不带长度的 DATA、带 trailer、`Expect: 100-continue`，小和大，stream 一次写完或分片间隔写，handler 直接读、用 OnBody、按 `BodyComplete` 二选一、或在别的 goroutine 里接收，服务端整体缓存、全部流式、超过阈值才流式；另有 body 发到一半 reset、body 上限、stream 窗口对慢 handler 的限速，以及 OnBody 在 handler 返回前不交付 body |
| 测试内置的手写 HTTP/3 服务端（零依赖，对 fib 客户端） | 客户端的协议纠错：服务端发来的 MAX_PUSH_ID、CANCEL_PUSH、GOAWAY 指向非请求流、抬高的 GOAWAY、QPACK 插入指令、引用动态表的响应 |
| 内存管道上的 QUIC 两端 | 握手、5% 与 20% 丢包下的传输、stream 数限制、reset、应用关闭、空闲超时、keep-alive、stateless reset、密钥更新（调低 AEAD 上限触发）、解密失败上限、Retry 及其令牌、洪泛限制及其配置、BLOCKED 帧、draining、Connection ID 的签发、路由与退役 |
| fuzz（`go test -fuzz`） | 面向不可信输入的解析：HTTP/3 帧解析器、从字段构造请求、QPACK 解码与编解码往返、QUIC 包头、传输参数、1-RTT 帧处理 |

## 压测

`benchmark/loadtest` 用 fib 自己的客户端压 fib 的 HTTP/3 服务端，每种配置 30 秒：
4 条连接各 128 个 stream 时 897,233 req/s，32 x 16 时 689,402，单连接 256 个 stream
时 373,362；没有请求失败、reset 或 GOAWAY；p99 分别为 1.56ms、2.90ms、1.34ms。同一
次压测里 h2c 在 64 x 8 时达到 2,836,822，HTTP/3 的峰值低于它，而且更多 QUIC 连接
得不偿失。唯一的内存限制来自不读的客户端：每个整体响应会按 2 的幂分档占一块缓冲，直到
对端确认。方法、与 HTTP/1、HTTP/2 的对比和内存测量见[压测](loadtest.zh-CN.md)。
