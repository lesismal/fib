# 压测

一致性测试一次只驱动一个连接、一个请求或一个手写帧。`benchmark/loadtest`
按压测工具的方式驱动服务端：少量连接上开几百个 stream，或者几百个连接上各开少量
stream，持续几十秒，并让一部分客户端故意不读响应。本文记录这套工具、这次压测以及
它发现的问题。

## 运行

```sh
./benchmark/loadtest/run.sh --mode full --rounds 3
```

需要 Go、nghttp2-client 里的 `h2load`、`python3`、`curl`，Linux 上还需要
`taskset` 和 `flock`；Debian/Ubuntu 上 `apt-get install -y nghttp2-client` 即可。
脚本会构建下面三个程序，每个用例起一个全新的服务端进程，预热 3 秒后测量 30 秒，
把输出写到 `/tmp/fib-loadtest/<time>/`：

| 文件 | 内容 |
| --- | --- |
| `results.tsv` | 每个用例一行：请求数、req/s、错误、延迟、RSS、goroutine、描述符、打开的连接、待发字节 |
| `growth.tsv` | 同一服务端上反复短跑的记录：RSS、goroutine、描述符 |
| `cases/<name>/` | 压测工具的输出、服务端日志，以及 `/stats` 在 `before`、`after`、`settle` 三个时刻的快照 |
| `cases/<name>/server.log` | 服务端的 stderr；本次压测的所有用例都没有输出 |

`--mode smoke` 每个协议跑一个短用例，再加一个慢读用例，一分钟左右，适合发版前或改动
发送路径后跑。`--cases LIST` 只跑指定用例；`report.py` 把 `results.tsv` 转成 Markdown
表格：

```sh
python3 benchmark/loadtest/report.py /tmp/fib-loadtest/<time>
```

这套压测刻意没有放进 CI：它需要 `h2load` 和一台安静的机器，数字也不能跨机器比较；
仓库的阻塞 job 保持确定性。需要时在测试机上跑 `--mode smoke`。

## 驱动了什么

| 程序 | 作用 |
| --- | --- |
| `benchmark/loadtest/server` | 一个进程同时提供 HTTP/1 与 h2c（TCP）、HTTP/1 与 HTTP/2 over TLS（另一个 TCP 端口）、HTTP/3（UDP），共用一个 handler。`/plain` 返回 `ok\n`；`/big?n=N` 返回 N 字节的缓存 body；`/upload` 读 POST body；`/stream?n=N&chunk=C&delayus=D` 分片写 body；`/stats` 返回进程的 goroutine、描述符、RSS、堆和引擎计数，`?gc=1` 先做一次 GC，`?reset=1` 重置计数 |
| `benchmark/loadtest/h3load` | 用 fib 自己的客户端持续压 HTTP/3：每个 client 一条 QUIC 连接，全局固定并发请求数，每个请求有超时；结束时输出吞吐、p50/p90/p99、每秒完成数和错误样本 |
| `benchmark/loadtest/stall` | 请求 `/big?n=8388608`，读完响应头后要么慢速读（`-delay`），要么完全不读（`-stall`），每个 stream 一条连接。它和快速负载一起跑，用来压背压、流控和清理路径 |
| `benchmark/loadtest/stallmem.sh` | 单独测卡住的读者：起一个全新服务端，保持 N 个 `--body` 字节的响应不读，在它们还开着时从 `/stats?gc=1` 读活堆和 RSS。`--bin DIR` 接收已经构建好的 `server` 和 `stall`，这样改动前后可以用同一批客户端交替测量 |
| `benchmark/loadtest/run.sh` | 压测主脚本：构建上面三个程序，把服务端绑在前一半 CPU、压测端绑在后一半，每个用例起新服务端，最后汇总 |

HTTP/1 与 HTTP/2 由 `h2load` 1.59.0 驱动：HTTP/1 用 `--h1`，h2c 用 prior
knowledge，HTTP/2 over TLS 用 ALPN。HTTP/3 用 `h3load`，因为发行版里的 `h2load`
没有编入 QUIC。

## 方法

机器是包里 benchmark 一直用的共享 16 线程 Ryzen 7 7800X3D，Go 1.27.3，Linux
socket buffer 16MiB；服务端绑 CPU 0-7，压测端绑 8-15。每个用例起一个新服务端进程，
预热 3 秒后测 30 秒；六个 A/B 配置交替跑三轮，表中取中位数。机器上还有别的工作流在
跑，压测开始前看过 load average；另一个 workstream 的一个三秒 `http3.test` 与
HTTP/3 的第一轮重叠，所以那一轮的 req/s 是逐轮表里偏低的一次。卡住读者在 HTTP/3
改动前后的数据是用 `stallmem.sh` 在同一台机器上逐轮交替测的，机器负载的变化会同时
作用在两边；用于对比快速用例的两次完整压测相隔约一小时，那时机器的 load average
是 10-17，比上面那次压测更高，所以两次的 req/s 整体都偏低。

HTTP/1 与 HTTP/2 的请求和响应都很小（`/plain`，3 字节），因此这些数字是请求速率，
不是带宽。`h2load` 只报告单请求耗时（request time）的最小、平均、最大和标准差，不报
分位数；下表的 p50/p99 只对 HTTP/3 有值，那是 `h3load` 自己统计的。HTTP/3 的压测端
是跑在负载 CPU 上的 fib Go 客户端，HTTP/1 与 HTTP/2 的压测端是 C，协议之间是在这个
前提下互相对比，而不是和一个外部基线对比。

## 结果

三轮交替 30 秒压测的中位数。“4 x 128”是 4 条连接、每条 128 个并发请求，共 512 个
stream；`h3load -conns 4 -streams 512` 是同一种形状。

| 用例 | 连接 x stream | req/s | 平均 | p99 | 最大 | 错误 |
| --- | --- | --- | --- | --- | --- | --- |
| h2c-4x128 | 4 x 128 | 1,199,024 | 0.41ms | - | 3.99ms | 0 |
| h2-4x128 | 4 x 128 | 1,092,137 | 0.43ms | - | 4.33ms | 0 |
| h3-4x128 | 4 x 128 | 897,233 | 0.56ms | 1.56ms | 8.60ms | 0 |
| h2c-64x8 | 64 x 8 | 2,836,822 | 0.15ms | - | 16.49ms | 0 |
| h2-64x8 | 64 x 8 | 2,404,461 | 0.16ms | - | 19.99ms | 0 |
| h3-32x16 | 32 x 16 | 689,402 | 0.71ms | 2.90ms | 33.52ms | 0 |

各协议不同连接/stream 形状的补充用例，每个跑一次：

| 用例 | 负载 | req/s | 平均 | p99 | 最大 | 错误 |
| --- | --- | --- | --- | --- | --- | --- |
| h1-16c | 16 条连接，各 1 个请求 | 414,508 | 0.04ms | - | 3.40ms | 0 |
| h1-256c | 256 条连接，各 1 个请求 | 799,966 | 0.33ms | - | 19.03ms | 0 |
| h1-pipeline-4x16 | 4 条连接，各 16 个 pipeline 请求 | 1,760,884 | 0.04ms | - | 3.48ms | 0 |
| h2c-512x1 | 512 条连接，各 1 个 stream | 762,134 | 0.39ms | - | 20.75ms | 0 |
| h2-512x1 | 512 条连接，各 1 个 stream | 575,979 | 0.49ms | - | 25.79ms | 0 |
| h3-1x256 | 1 条连接，256 个 stream | 373,362 | 0.68ms | 1.34ms | 4.40ms | 0 |

三轮加补充用例一共略多于十亿个请求。没有请求失败、出错或超时，也没有服务端日志警告；
GOAWAY 或 reset 风暴会体现为失败或出错请求，因此也不在这个计数里。512 连接的用例会在开始时同时发起 512 个
TLS 握手，全部完成，没有失败，描述符峰值 533，RSS 45-52MiB，这个规模下没有出现
握手堆积。

数字说明：

- **HTTP/2 随连接数扩展。** 4 条连接、512 个 stream 时 h2c 是 1.20M req/s；64 条
  连接、每条 8 个 stream 时是 2.84M。4 条连接不是 stream 并发不够，而是 reader 和
  worker 的并行度不够：每条连接只由一个 goroutine 读。
- **TLS 的代价是 9-15%。** 4 x 128 时 h2c 1.20M、TLS 1.09M；64 x 8 时 2.84M 对
  2.40M。TLS 的代价在不同连接数下基本稳定，说明这个请求大小、loopback 路径上，
  握手和加密 record 不是瓶颈。
- **HTTP/3 的峰值在少得多的连接数上。** 4 x 128 时 897k req/s，32 x 16 时 689k，
  单连接 256 stream 时 373k。更多的 QUIC 连接在这里得不偿失：每条连接有自己的加密
  和拥塞状态，而且 fib 的客户端与服务端共用同 8 个负载 CPU，C 写的 `h2load` 不会。
  同样 4 x 128 的形状下，h3 达到 h2c 的 75%、h2 的 82%，p99 为 1.56ms；各自最好的
  形状下，h3 是 h2c 的 32%。
- **HTTP/1 pipeline 可用，而且很快。** 4 条连接各 16 个 pipeline 请求达到 1.76M
  req/s，高于 256 条非 pipeline 连接的 800k，没有失败。浏览器不用这种形状，但它把
  解析器“一次读里多个请求”的路径压到了。
- **吞吐上去，延迟也上去。** h2c 在 64 x 8 时平均 0.15ms，4 x 128 时 0.41ms；
  HTTP/3 在连接数从 4 涨到 32 时 p99 从 1.56ms 涨到 2.90ms，最大从 8.60ms 涨到
  33.52ms。

## 慢客户端与内存

`stall` 与快速负载同时跑，分两组各 8 条连接：一组读完响应头就不再读，另一组每 50ms
读 16KiB。三个协议下快速负载都继续服务，快速请求没有失败：

| 慢客户端用例 | 快速负载 | 快速 req/s | after 快照的活堆 | RSS 峰值 | 客户端结束后 2 秒 RSS |
| --- | --- | --- | --- | --- | --- |
| slow-h1 | 16 条连接 | 388,639 | 156MiB | 257.6MiB | 232.5MiB |
| slow-h2c | 4 x 128 | 866,056 | 25MiB | 73.4MiB | 39.0MiB |
| slow-h3 | 4 x 128 | 273,167 | 37MiB（空闲超时之后） | 97.4MiB | 78.4MiB |

HTTP/3 这一行是 30 秒空闲超时关掉卡住的连接之后的数据。stream 还开着时，对一个新
服务端（压测程序的 `/big` body 已缓存）测，代价是：

| 协议 | 卡住的 stream | 活堆 | RSS |
| --- | --- | --- | --- |
| h1 | 16 | 268MiB | 206MiB |
| h2c | 16 | 45MiB | 158MiB |
| h3 | 1 | 32MiB | 31MiB |
| h3 | 4 | 81MiB | 79MiB |
| h3 | 16 | 275MiB | 260MiB |

HTTP/1 这一行是按下面那次改动用 `stallmem.sh` 重测的，其余几行是当初压测的数据。
HTTP/3 这几行是它发送路径改动之前的数据；改动之后用 `stallmem.sh` 测（改动前后各三轮、
交替进行、取中位数，服务端和卡住的客户端钉在机器的不同半边），低得多。每一行里
body 都是压测服务端缓存的那一份，所以不管多少 stream 持有，它只有一份 8MiB：

| 卡住的 stream | 改动前：活堆 | 改动前：RSS | 现在：活堆 | 现在：RSS |
| --- | --- | --- | --- | --- |
| 1 | 32MiB | 29MiB | 16MiB | 21MiB |
| 4 | 83MiB | 78MiB | 19MiB | 46MiB |
| 16 | 275MiB | 209MiB | 19MiB | 113MiB |

HTTP/1 的发送路径后来也按同样思路改了，用同一个脚本、改动前后各三轮交替、取中位数，
body 还是缓存的那一份 8MiB：

| 卡住的连接 | 改动前：活堆 | 改动前：RSS | 现在：活堆 | 现在：RSS |
| --- | --- | --- | --- | --- |
| 1 | 27MiB | 28MiB | 11MiB | 20MiB |
| 4 | 75MiB | 76MiB | 11MiB | 44MiB |
| 16 | 268MiB | 206MiB | 12MiB | 85MiB |

改动前每条连接都持有一份 body 拷贝，按 16MiB 的池分档，16 条就是 16 x 16MiB 加上
缓存的那一份；现在队列里排的是 handler 的 slice，16 条连接共用服务端缓存的那一份
8MiB，所以活堆停在 12MiB，不管卡住多少条。RSS 仍随连接数上涨，因为每条连接都有
内核保留的 socket 缓冲，16 条时的 85MiB 就是它。

差别在三条发送路径如何持有整体响应：

- **HTTP/2** 只把当前 stream 窗口和连接窗口允许的部分装帧，其余保留为对 handler
  的 `Response.Body` 的引用。卡住的 stream 只占大约一个 stream 窗口（1MiB）的帧
  缓冲，而不是整个 body，所以 16 个卡住的 stream 只有 45MiB 活堆。158MiB 的 RSS 是
  帧缓冲带来的分配器高水位，stream 结束后会回落到 39MiB。
- **HTTP/1** 过去把响应头和 body 交给 `Conn.SendParts`，socket 只吃下部分 body 时，
  `queueLocked` 把剩下的拷进池里的一块缓冲：卡住的连接因此持有一份 body 拷贝，
  并按 2 的幂大小分档，16 条连接就是 16 份。现在超过 4KiB 的 body 按 handler 给的
  原样保留（`Conn.SendPartsShared`），排在响应头旁边，按长度计数，所以 16 条卡住
  的连接持有的是缓存的那一份 8MiB。4KiB 及以下的 body 仍然拷进本轮发送缓冲，
  这样小响应还是一次带缓冲的写。见 [HTTP/1](http1.zh-CN.md) 和
  `fibhttp.Response.Body`。
- **HTTP/3** 过去把响应的 HEADERS 和整个 body 构造成一块帧缓冲，用 `WriteFinal`
  交给 QUIC stream，后者一直保留到对端确认。`bufferpool` 按 2 的幂分档，8MiB 的
  body 要占 16MiB 缓冲：16 个卡住的 stream 就是 16 x 16MiB，再加一份缓存 body
  8MiB，即上表的 275MiB 活堆。这条路径过去没有单响应的队列上限。
- **HTTP/3 现在**和 HTTP/2 一样，把 body 留在 handler 给它的地方，按对端窗口、再按
  对端确认逐步装帧：stream 以若干 run 持有响应（`quic.Stream.WriteFinalParts`），
  卡住的 stream 因此只占流控窗口允许的那些帧，每个约五分之一 MiB，而不是一个比
  body 还大的池缓冲。body 只剩装进 packet 的那一次拷贝，这是 AEAD 需要的，与 body
  大小无关：小响应的帧仍然共用一块池缓冲，所以小的整体响应并不比大的贵。这对
  handler 的要求见 [HTTP/3](http3.zh-CN.md) 和 `fibhttp.Response.Body`：
  body 在对端确认之前不能修改。

这是部署上的限制，不是错误结果：对端没有被 reset，快速负载照常跑。规划时值得注意：

- 大响应用 `Context.Write` 和 `Flush` 流式写，不要整体交给 `Respond`。HTTP/2 与
  HTTP/3 的流式写在未发送数据到 64KiB 时等待，HTTP/1 在
  `Config.WriteBufferHighWatermark` 处暂停读，因此内存由 stream 窗口或 watermark
  限制，而不是由 body 大小决定。
- 用 `Config.MaxWriteQueueBytes` 关掉不读的 HTTP/1 连接，用
  `Config.MaxPendingBytes` 限制整个服务端的待发输出。两者默认都不限。
- 整体响应不再让 HTTP/3 服务端的内存由 body 大小决定：卡住的 stream 只持有对端
  窗口允许的部分，因此上界大致是 `MaxConcurrentStreams` 乘以 stream 窗口，再加
  handler 自己保留的那些 body。HTTP/1 现在也保留 handler 的 body，多少条连接卡住
  都只有一份，所以卡住的 HTTP/1 读者占的就是 body 本身；`MaxWriteQueueBytes` 仍然
  限制等待的字节数，而流式写是不让它一次占住整个 body 的办法。
- QUIC 的 30 秒空闲超时会关掉没有包流动的连接。本次压测里卡住的 HTTP/3 客户端报的
  就是 `quic: idle timeout`，不是 reset 或 GOAWAY；持续消费的慢客户端会发送窗口
  更新，连接不会因空闲被关。

## 反复运行

在同一个服务端进程上，先 h2c 再 HTTP/3，各跑 10 次 5 秒的 4 x 128 快速负载。
描述符始终是 22。每轮结束时的 goroutine 数停在 handler pool 的负载规模（h2c 约
1,150-1,280，HTTP/3 约 735-911），没有逐轮上涨；负载结束两秒后的用例快照里，h2c
只剩 296 个空闲 worker，HTTP/3 是 58-162，说明池会收缩。h2c 的 RSS 稳定在
33-38MiB；HTTP/3 从 39MiB 涨到 48-52MiB 的区间。没有发现逐轮泄漏。HTTP/3 的
RSS 漂移更像分配器高水位而不是确定的增长；在下结论之前还值得跑更长的 soak。

本次压测没有改生产代码。没有发现可以用一个小的、独立的修复去掉的并发 bug：没有请求
失败、reset 风暴、GOAWAY 风暴、卡死的连接，也没有描述符或 goroutine 泄漏。唯一暴露
出来的是慢客户端下整体响应的内存代价，它是整体响应发送路径和 buffer 分档的性质，
不是一行代码的缺陷；上面已经连同缓解办法一起记录。

这个限制后来只对 HTTP/3 做了修复：QUIC stream 现在把 handler 的 body 原样留着，
发送路径也把每个 packet 直接建在它要发出去的位置，因此响应和 datagram 都不再被
拼成一整块拷贝。HTTP/1 与 HTTP/2 在那个时候保持原样（HTTP/1 的拷贝后来也去掉了，
见上面 HTTP/1 的表）。改动之后又跑了一遍整轮压测，用例、三轮
交替和读数方式都相同：两次的 load average 都是 10-17，第二次机器更闲，所以三个协议
的快速用例都偏高，包括代码没动的 HTTP/1 和 HTTP/2。因此快速用例不是两个版本的对比；
能对比的是上面那些交替跑出来的卡住读者数据。这次压测能说明的是两次都没有失败，慢客户端
用例与之前相当：

| 用例 | 改动前 req/s | 现在 req/s | 改动前 p99 | 现在 p99 | 改动前 RSS after | 现在 RSS after |
| --- | --- | --- | --- | --- | --- | --- |
| h1-16c | 300,350 | 418,320 | - | - | 23MiB | 22MiB |
| h1-256c | 499,671 | 828,433 | - | - | 27MiB | 23MiB |
| h2c-4x128 | 1,102,394 | 1,207,983 | - | - | 39MiB | 37MiB |
| h2-4x128 | 992,534 | 1,131,507 | - | - | 48MiB | 45MiB |
| h3-4x128 | 539,183 | 947,026 | 3.16ms | 1.37ms | 41MiB | 40MiB |
| h3-32x16 | 659,866 | 705,770 | 3.36ms | 2.50ms | 43MiB | 44MiB |
| h3-1x256 | 333,445 | 337,569 | 1.72ms | 1.29ms | 38MiB | 34MiB |
| slow-h1 | 418,834 | 417,694 | - | - | 43MiB | 43MiB |
| slow-h2c | 1,217,853 | 1,209,984 | - | - | 73MiB | 53MiB |
| slow-h3 | 1,005,391 | 980,328 | 1.28ms | 1.32ms | 94MiB | 63MiB |

两个没有改动的慢客户端用例 req/s 基本持平，而它们的 RSS 在两次之间的变化和 HTTP/3
的差不多：单个 30 秒用例里卡住读者的代价噪声很大，所以内存要看上面那组交替测量。
HTTP/3 的慢客户端用例两次都持有同样的 67MB 响应，快速负载保住了 98% 的速率，p99 不变，
以错误结束的卡住客户端从 8 个降到 1 个：它们在不发任何包之后被 QUIC 空闲超时关掉，
时间点落在用例快结束时。

## 这次压测的局限

- 一台机器、一个内核、loopback：没有丢包、NAT 和真实网络往返。QUIC 的拥塞控制、
  丢包恢复和 PMTU 发现因此几乎没有被压到。
- 压测端和服务端在同一台机器上，HTTP/3 的压测端还是跑在同样 CPU 上的 Go 程序；
  协议之间只适合互相对比，不适合和外部基线对比。
- `h2load` 不报分位数，HTTP/1 与 HTTP/2 只有平均和最大延迟；HTTP/3 的 p50/p90/p99
  来自 `h3load`。
- HTTP/3 的慢客户端用例会被 30 秒空闲超时截断；要压更久，需要打开
  `KeepAlivePeriod`，或者让客户端先消费一段再停。
- 10 次 5 秒只是泄漏的冒烟测试，不是 soak。带连接 churn、TLS 会话恢复和不同 body
  大小的数小时压测仍然没有做。
- 压测跑在并入 main 之前的提交上；HTTP/3 第一轮还叠加了另一个 workstream 的三秒
  测试。并入 main 后重跑了六个 A/B 用例两轮，前四个在 0-5% 内一致；h2c-64x8 为
  2.50-2.57M，原为 2.71-2.92M，h3-32x16 为 625-656k，原为 661-701k，都在共享
  机器上这两个用例自身的波动范围内。
