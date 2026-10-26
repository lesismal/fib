# fib 路线图：计划中但未做，以及有意不做

各协议的文档（`http1*.md`、`http2*.md`、`http3*.md`）各有一节"有意没有实现的功能"和"待优化"。这份文档把它们、以及散在 `guide.zh-CN.md` 里的取舍，汇总成一页，每项给出**原因**、**成本量级**和**什么情况下该重新评估**。

- 表里的"成本"是量级估计：S（一两天内）／M（几天）／L（一两周以上，需要新的测试手段）
- 标"有意不做"的，是权衡后的结论，不是遗漏；需求变了就按第 6 节重新评估
- 各项的权威细节仍在原协议文档里，本页只做索引与理由，行尾给出出处

## 1. QUIC 与 HTTP/3

| 项 | 状态 | 原因 | 成本 | 何时重估 |
| --- | --- | --- | --- | --- |
| BBR 拥塞控制 | 有意不做 | 它换的是**对路径的模型**而不只是窗口：按每轮投递量估带宽、算 pacing 速率，还有自己的 startup／drain／probe-bw 阶段；要可靠就得把已发送包的记录扩展成每轮的投递率采样，并与 RFC 9002 的丢包恢复、持续拥塞判定协调起来。改动很大，而且除了内存管道这里没有可靠的验证手段；半成品比没有更糟 | L | 有自己的网络与真实流量，能量到 CUBIC 在丢包／长肥管道上确实是瓶颈 |
| ECN | 有意不做 | 需要在 Engine 的 UDP 层读写 IP 头的 ECN 位，收益主要体现在拥塞控制上；没有配套的拥塞控制时收益有限 | M | 数据中心内部署、或换上有 ECN 的拥塞控制时 |
| PMTU 探到 1452 以上（巨型帧、IPv4 上的 1472） | 计划中未做 | 需要更大的发送缓冲区；收益是 1–2% 的包数减少。比它更值得做的"拥塞窗口按字节算"已完成 | S–M | 已知路径支持更大数据报、或包数开销成为瓶颈 |
| ChaCha20-Poly1305 汇编（AVX2／NEON） | 有意不做 | 分架构、常量时间容易写错的一大块代码；而只有**没有** AES 硬件的机器才会优先协商这个套件。纯 Go 版本已经比原实现快 54–81% | L | 目标机器普遍没有 AES 硬件 |
| QUIC `*Stream` 池化 | 有意不做 | `*Stream` 会交给应用、生命周期由应用决定；重用会把"stream 结束后继续使用"变成破坏另一条 stream。现在开／关一条 stream 8 次分配，请求 stream 与 header 已复用 | M | 不重估：这是 API 所有权边界 |
| 主动连接迁移、`preferred_address` | 有意不做 | 服务端已为每个连接签发并路由多个 Connection ID，但 `PATH_CHALLENGE` 在当前发送路径上回复而不是挑战到达的路径，客户端迁移前对新路径的验证无法完成。NAT 重绑定（含换 CID）会被跟随，实际场景已覆盖 | L | 移动端长连接、Wi-Fi／蜂窝切换频繁的产品 |
| QPACK 动态表 | 有意不做 | 两端都声明容量 0：不会出现 blocked stream 造成的队头阻塞，编解码器流都没有内容，也没有额外状态与内存；代价是重复 header 每次完整发送 | M | header 很大且高度重复（如大 cookie、内部 RPC）成为带宽瓶颈 |
| Server push | 有意不做 | 主流浏览器都不启用 HTTP/3 push；客户端不发 `MAX_PUSH_ID`，`Context.Push` 返回 `http.ErrNotSupported`。预加载用 103 Early Hints（`WriteInterim`） | L | 不重估：浏览器侧已凉 |
| DATAGRAM（RFC 9221）、WebTransport | 有意不做 | 依赖流式 stream 或不可靠数据报的 API，当前 handler 模型不支持；对端发来的 DATAGRAM 帧被当未知帧，连接以 `FRAME_ENCODING_ERROR` 关闭 | L | 要做实时媒体／游戏类服务时，且需要同时扩展 handler 模型 |
| QUIC v2（RFC 9369）等其它版本 | 有意不做 | 部署上 version 1 已足够；客户端收到 Version Negotiation 直接失败 | S–M | 出现要求 v2 的部署环境 |
| 双向流式的 handler API | 计划中未做 | Extended CONNECT 已通过 `Context.Upgrade` 实现（WebTransport 的前提之一），但 handler 模型还没有同时操作两个方向的 API | M | 同 WebTransport 一项 |
| 接收方向再省一次拷贝 | 计划中未做 | 接收时 Engine 复制一次（复制进缓冲池），帧解析器只复制跨片段的帧与 body；要再省需要零拷贝投递，代价是生命周期管理 | M | 接收吞吐成为瓶颈时 |
| Interop Runner 的 `ecn`、`v2`、`connectionmigration` | 有意不做 | 直接来自上面三项；端点以 127 退出，runner 记为"不支持"而不是通过 | — | 同上 |

## 2. HTTP/2

| 项 | 状态 | 原因 | 成本 | 何时重估 |
| --- | --- | --- | --- | --- |
| 客户端接收 server push | 有意不做 | Chrome、Firefox 已移除 push，Go 的 `net/http` 客户端从未支持；客户端在 SETTINGS 里声明 `ENABLE_PUSH=0`。预加载用 103 Early Hints | M | 不重估 |
| RFC 9218 的 `i` 增量提示、PRIORITY_UPDATE 帧 | 有意不做 | 紧急度 `priority` 头**已**用于发送调度；响应 body 整体发送时 `i` 没有可重排的内容。RFC 7540 的优先级树已被 RFC 9113 废弃 | M | 做真正的流式／分块重排调度时 |
| 客户端发起 `Upgrade: h2c` | 有意不做 | RFC 9113 已废弃这种升级；明文 HTTP/2 用 prior knowledge（`UnencryptedHTTP2`）。服务端仍接受升级以兼容 curl | S | 不重估 |
| TLS 连接上的 `Upgrade: h2c` | 有意不做 | RFC 规定 TLS 上只能通过 ALPN 切协议 | — | 不重估 |
| HTTP/1.0 请求的 1xx 中间响应 | 有意不做 | HTTP/1.0 客户端不认识 1xx，`WriteInterim` 返回 `http.ErrNotSupported` | — | 不重估 |
| 在被推送的请求上再次 push | 有意不做 | 协议规定 PUSH_PROMISE 只能在客户端发起的 stream 上发送 | — | 不重估 |
| body 流式交付的 stream 复用 | 计划中未做 | 它的 `feed` 回调可能在响应结束之后发生，安全复用需要额外一层 feed 生命周期引用，风险大于收益（一次 stream 分配） | M–H | 流式请求成为主流、分配成为瓶颈 |
| 多帧 body 的分散写（vectored send） | 计划中未做 | 需要协议层把多个缓冲区直接交给 `Layer`。h2 侧 pprof 显示发送路径系统调用打满（`Syscall6` 44.7%）、memmove 只占 2.5%，上限只有几个百分点；单帧 body 的第二次拷贝已在 h2c 上去掉 | M | 发送侧拷贝出现在 pprof 前排时 |
| 客户端 0-RTT | 计划中未做 | HTTP/2 的帧格式要等 ALPN，握手完成前不知道用哪个协议。服务端方向的 0-RTT 已实现（`Config.EarlyData`、`Early-Data: 1`、425） | M | TLS 层能提前确定 ALPN（例如只提供 h2）时 |

## 3. HTTP/1.x

| 项 | 状态 | 原因 | 成本 | 何时重估 |
| --- | --- | --- | --- | --- |
| `compress`（LZW）transfer coding | 有意不做 | Go 的 `compress/lzw` code 最大 12 位，而 `compress(1)` 会增长到 16 位，解不了流开头几 KB 之后的部分；要支持得自己写 9→16 位码增长的 LZW。RFC 9110 已弃用这个 coding | M | 遇到只发 `compress` 的老客户端／代理（预计为零） |
| `br`（Brotli） | 有意不做 | 没有标准库实现，引入第三方依赖违反"零依赖" | L | 允许引入依赖时 |
| 客户端 pipelining | 有意不做 | 一个连接同时只有一个请求，语义简单、不会被队头阻塞放大；HTTP/2 才是并发路径 | M | 不重估 |

## 4. fib 引擎与其它包

| 项 | 状态 | 原因 | 成本 | 何时重估 |
| --- | --- | --- | --- | --- |
| Windows 上 UDP 的可写事件 | 有意不做 | 完成端口没有"可写"语义（可读靠零字节 `WSARecv`、可写靠 `WSASend` 完成模拟），只能退化成轮询；现在的 1 毫秒重试定时器已经近似，而 Windows 上跑 QUIC 服务端本来就少 | M | Windows 成为 QUIC 的主要部署平台 |
| macOS 上 `ENOBUFS`（socket 写满）的可写通知 | 有意不做 | 同上：`sendmsg_x` 不报"又有空间了"，仍用 1 毫秒重试；`EAGAIN` 已经走可写事件 | S–M | macOS 上跑高吞吐 QUIC 服务端 |
| kqueue 可写事件只在 darwin/arm64 验证过 | 已知边界 | `EVFILT_WRITE` 路径在 macOS 上跑过，FreeBSD／NetBSD／OpenBSD／DragonFly 只有交叉编译验证 | S | 有 BSD 生产环境时补一次真机测试 |
| `MaxWriteQueueBytes` 允许单条消息越界 | 已知边界 | 上限在入队后检查，一条超大消息可以超出上限一条消息才关闭连接；这是为了让"一次发送要么成功要么关闭"语义简单 | S | 需要严格上限时改成入队前预判 |
| WebSocket 广播的发送队列 | 已缓解 | 默认无上限（只有 `MaxWriteQueueBytes`／`SetWriteQueueLimit` 可开）；对端长期不读要靠应用层心跳断开 | — | 见 `guide.zh-CN.md` 的广播一节 |

## 5. 测试与基础设施

| 项 | 状态 | 原因 | 成本 | 何时重估 |
| --- | --- | --- | --- | --- |
| CI 里"干净 checkout 的 `go build ./...` 必过" | 计划中未做 | 多次外部提交把别人的文件带回旧状态，导致 main 不可编译，只能人工发现回补 | S | 建议尽快做 |
| `http3-interop-image` job 依赖 Docker Hub | 已知风险 | 需要拉 `golang:1.27.3` 基础镜像，极端情况下 rate limit 会让 PR 失败 | S | 出现失败时改成非阻塞或换镜像源 |
| Interop Runner 的 `handshakecorruption`／`transfercorruption` | 计划中未做 | 与 `handshakeloss`／`transferloss` 走同一端点路径，尚未单独跑过 | S | 想补齐矩阵时 |
| 与更多 QUIC 实现的互通 | 部分完成 | 现在有容器化端点（`http3/interoprunner/`）和 CI job，但只与 quic-go 互为对端；quiche、ngtcp2、mvfst 未接 | M | 需要更广的兼容性证据时 |
| 0-RTT 与更多实现互通、多机共享票据密钥的防重放 | 计划中未做 | 票据登记表是每个进程一份；放在负载均衡后面的服务端要自己防重放 | M | 真的上线 0-RTT 时 |
| 压测（并发问题） | 已完成 | `benchmark/loadtest/` 的完整战役 + `stallmem.sh` 内存用例；结论见 `docs/loadtest*.md` | — | 见该文档的"如何做一次干净的 A/B" |
| `arpc` 重连用例的临时端口竞态 | 已知边界 | `TestReconnectGivesUp` 依赖服务端端口在重连窗口内保持关闭；理论上别的进程抢占该临时端口会让它误判，6600+ 次运行未出现 | S | 出现误报时改成用固定端口或连接拒绝注入 |
| HTTP/3 带 trailer 的整段响应多一次列表分配 | 已知成本 | send buffer 内联只放两个 piece；给每条 stream 加槽位要多 ~56 字节，判断不值得 | S | trailer 变成常态时 |

## 6. 如何重新评估一项

1. 先量：有没有证据说明它现在是瓶颈（pprof、压测、真实流量）——`docs/loadtest*.md` 里的方法可以复用
2. 再看成本量级：S／M 的项可以直接排；L 的项（BBR、WebTransport、主动迁移、汇编）应当有明确的验证手段再动手
3. 改完删掉这里的对应行，并把理由写进提交信息与原协议文档；本页不保留历史

## 7. 各项在协议文档里的出处

| 内容 | 中文 | 英文 |
| --- | --- | --- |
| HTTP/1 限制与待优化 | `docs/http1.zh-CN.md` "当前限制"、"待优化" | `docs/http1.md` "Current limitations"、"Planned improvements" |
| HTTP/2 限制／有意不做／待优化 | `docs/http2.zh-CN.md` "当前限制"、"有意没有实现的功能"、"待优化项" | `docs/http2.md` "Current limitations"、"Intentionally not implemented"、"Planned improvements" |
| HTTP/3 限制／有意不做／待优化 | `docs/http3.zh-CN.md` "当前限制"、"有意没有实现的功能"、"待优化项" | `docs/http3.md` "Current limitations"、"Intentionally not implemented"、"Planned improvements" |
| 引擎、TLS、WebSocket 的取舍 | `docs/guide.zh-CN.md` 对应小节 | 同左（guide 只有中文） |
| 压测方法与结论 | `docs/loadtest.zh-CN.md` | `docs/loadtest.md` |
| Interop 逐 case 结果 | `http3/interoprunner/README.md` | 同左 |
