# 流媒体：设计、用法与边界

[English](media.md) | [简体中文](media.zh-CN.md)

本文介绍 fib 的直播流媒体实现：各部分如何组织，服务端和转推/拉流怎么用，每种协议能承载
哪些编码，有意不做哪些，以及如何与 ffmpeg 和 Chromium 互通验证。一个同时接收和分发所有
协议的完整服务端程序见[一个服务跑全部协议](#一个服务跑全部协议)。

全部代码位于 [`media`](../media) 下。各协议是 [`media/rtmp`](../media/rtmp)（其下有
[`media/amf`](../media/amf) 和 [`media/flv`](../media/flv)）、
[`media/httpflv`](../media/httpflv)、[`media/rtsp`](../media/rtsp)（其下有
[`media/rtpcodec`](../media/rtpcodec)）、[`media/srt`](../media/srt)（其下有
[`media/mpegts`](../media/mpegts)）、[`media/hls`](../media/hls)（其下有
[`media/fmp4`](../media/fmp4)），以及 [`media/rtc`](../media/rtc)，即基于 fib 自己的
[WebRTC](webrtc.zh-CN.md) 实现的 WHIP 与 WHEP。它们在 [`media/hub`](../media/hub) 中汇合，
共用 [`media/av`](../media/av) 的类型，以及 [`media/codec/h264`](../media/codec/h264)、
[`media/codec/h265`](../media/codec/h265)、[`media/codec/aac`](../media/codec/aac)、
[`media/codec/opus`](../media/codec/opus) 的码流工具函数。全部基于标准库和 fib 实现，不依赖
任何其它模块，也没有任何代码对音视频做解码或编码。

## 已支持的范围（概览）

| 协议 | package | 服务端 | 客户端 | 内容 |
| --- | --- | --- | --- | --- |
| RTMP、RTMPS | `media/rtmp` | 推流与播放 | 推流、播放、转推与拉流转发 | simple 与 Flash digest 两种握手，支持全部四种头部格式和扩展时间戳的 chunk stream，aggregate 消息，确认窗口；Enhanced RTMP（以 FourCC 表示的 H.265、AV1、Opus），以及旧式 codec id 12 的 H.265；经 fib/tls 提供 RTMPS |
| HTTP-FLV、WebSocket-FLV | `media/httpflv` | 播放 | 拉流转发 | 一个永不结束的 FLV 文件，走 HTTP/1.1、HTTP/2 或 HTTP/3，或者把同样的字节放进二进制 WebSocket 消息（也支持 RFC 8441 与 RFC 9220 的 extended CONNECT）；供 flv.js、mpegts.js 和 ffmpeg 播放。拉流转发经 HTTP/1.1 或 HTTP/2（http 与 https）播放 HTTP-FLV 源站，或播放 WebSocket-FLV 源站（ws 与 wss），发布进 hub |
| RTSP 1.0 | `media/rtsp` | 录制（ANNOUNCE/RECORD）与播放 | 播放、推流、转推与拉流转发 | 基于 TCP interleaved 和 UDP 单播的 RFC 2326，所有会话共用一对 RTP/RTCP 端口，RTCP sender report，Digest 与 Basic 认证，GET_PARAMETER 保活，会话超时 |
| SRT | `media/srt` | 推流与播放 | 推流、播放、原始会话 | live 模式：带 SYN cookie 的 v5 握手，stream ID 及其访问控制语法，ACK/ACKACK/NAK，重传，TSBPD 与过晚丢弃，128、192、256 位密钥的 AES-CTR 加密以及带内的密钥更新；MPEG-TS 与 hub 互转 |
| HLS、LL-HLS | `media/hls` | 播放 | — | RFC 8216，分片放在内存中，格式为 fMP4 或 MPEG-TS，按请求选择（`?format=ts`）；以及低延迟扩展：partial segment、preload hint、blocking playlist reload |
| WHIP、WHEP | `media/rtc` | WHIP 接入、WHEP 分发 | WHIP 推流、WHEP 拉流，以及转发 | RFC 9725 与 draft-ietf-wish-whep：201 加 Location，PATCH 做 trickle，DELETE，CORS，用 Link 头下发 ICE 服务器，bearer token；所有 PeerConnection 共用一个 UDP 端口 |
| Hub | `media/hub` | — | — | 按路径登记的流，每路一个发布者、任意多个订阅者；GOP 缓存；为较晚才知道 track 的接入预占路径；订阅者可以等待发布者，并在重新推流后继续；推流与播放的认证集中在一处 |
| 容器与载荷 | `media/flv`、`media/mpegts`、`media/fmp4`、`media/rtpcodec`、`media/amf` | — | — | 带 Enhanced RTMP tag 的 FLV；MPEG-TS 复用与解复用；fragmented MP4 的写入与读取；H.264、H.265、AAC、Opus、G.711 的 RTP 载荷格式及 SDP；AMF0 双向，AMF3 解码 |
| 编码工具 | `media/codec/...` | — | — | H.264 与 H.265 的分帧、SPS、配置记录与关键帧判断；AAC 的 AudioSpecificConfig 与 ADTS；OpusHead 与包时长 |

任何地方都不做转码。流以发布者选定的编码发布，每个出口承载其协议能承载的 track，其余的
不发；具体见[编码矩阵](#各协议支持的编码)。

## 架构

### sans-IO 核心与 fib 胶水层

和 fib 的 WebRTC 实现一样，每个协议都是一个状态机，自己不持有 socket、不起 goroutine、也不
读时钟。调用方把收到的数据（以及在需要时间的地方，当前时间）交给它，问它下次什么时候需要
时间，再给它一个回调用来发送。一层很薄的胶水代码从 `fib.Handler`、UDP binding、
`Engine.OpenUDPPeer` 或 fib/http handler 驱动这些核心。正因为如此，核心可以脱离网络做测试和
fuzz，一个会话也可以在一把锁下驱动它的核心。

| 核心 | package | 输入入口 | 发送出口 | 时间 |
| --- | --- | --- | --- | --- |
| RTMP 连接 | `rtmp.Conn` | `Feed(b)` | `ConnConfig.Output` | 无（超时由胶水层负责） |
| SRT 连接 | `srt.Conn`、`srt.Listener` | `Handle(b, now)` | `Config.Output` | `NextTimeout` / `HandleTimeout` |
| RTSP 消息 | `rtsp.Parser` | `Feed(b, fn)` | `Request`、`Response`、`AppendInterleaved` 写入 buffer | 无 |
| RTP 载荷 | `rtpcodec.Packetizer`、`Depacketizer`、`Reorder` | `Depacketize(p, emit)`、`Push(now, p, emit)` | `Packetize(pkt, emit)` | `Reorder.NextTimeout` / `Expire` |
| FLV | `flv.Demuxer`、`Decoder`、`Encoder`、`Muxer` | `Feed`、`WriteTag` | 追加到调用方的 buffer | 无 |
| MPEG-TS | `mpegts.Demuxer`、`Muxer` | `Write(chunk)` | `WritePacket(dst, pkt)` 追加 | 无 |
| fMP4 | `fmp4.Writer` | `WritePacket(pkt)` | `Init()`、`Flush(dst, next)` 追加 | 无 |
| HLS 切片器 | `hls.Muxer`（一个 `hub.Sink`） | `OnTracks`、`OnPacket` | `MediaPlaylist`、`File` | 无 |

| 胶水层 | 运行在 |
| --- | --- |
| `rtmp.Server`、`rtsp.Server` | TCP binding 上的 `fib.Handler`；RTSP 的 UDP 端口对由 `ListenUDP` 用 `Engine.AddBinding` 添加 |
| `srt.Server` | UDP binding 上的 `fib.Handler`，每个对端地址是一个 fib 连接；每个会话一把锁、一个定时器，每次调用产生的数据报用一次 `SendDatagrams` 批量发出 |
| `httpflv.Handler`、`hls.Handler`、`rtc.Server` | fib/http handler，HTTP/1.1、HTTP/2、HTTP/3 均可 |
| `rtmp.Publish`/`Play`、`rtsp.Client` | 经 engine 拨号（`DialWithHandler`），回调驱动 |
| `srt.Dial`/`Publish`/`Play` | 在自己的 binding 上用 `Engine.OpenUDPPeer` |
| `rtc.Pull`/`Push` | 自带端口的 PeerConnection，信令走 fib/http 客户端 |
| `httpflv.Pull` | 响应 body 流式交付（`OnBody`）的 fib/http 客户端，或 fib/websocket 的 Dialer，在读连接的 worker 上直接送进 `flv.Demuxer` |

### hub

`hub.Hub` 是按路径登记的流表。每路流最多一个发布者、任意多个订阅者，协议之间互不知晓：
RTMP 推上来的流能用 RTSP、SRT、HTTP-FLV、HLS 和 WHEP 播放，是因为这些协议都订阅 hub，
而不是订阅 RTMP。

- **Publish。** 一开始就知道自己 track 的接入（RTSP 的 ANNOUNCE、WHIP 的 offer）调用
  `Hub.Publish(path, tracks, info)`，然后每个 access unit 调一次 `Publisher.WritePacket`，
  编码配置变化时调 `Publisher.SetTracks`，离开时调 `Publisher.Close`。已有发布者的路径，
  第二个发布者会得到 `ErrStreamBusy`。
- **Reserve。** 只能从收到的数据里得知 track 的接入（SRT 的 MPEG-TS，以及在 metadata 和
  sequence header 到达之前的 RTMP 发布者）在还能拒绝对端的时候调用 `Hub.Reserve(path, info)`。
  路径立即被占用，`OnPublish` 也立即被调用，所以第二个发布者在握手阶段就被拒绝（RTMP 的
  `NetStream.Publish.BadName`、SRT 的 `RejectConflict`），而不是先被接受、之后再断开。
  被预占的 Publisher 第一次 `SetTracks` 时流才开始直播；在此之前，不等待的订阅者找不到它。
- **Subscribe。** 出口用一个 `hub.Sink` 调用 `Hub.Subscribe(path, sink, opts)`，sink 先收到
  `OnTracks`，然后每个包一次 `OnPacket`，结束时收到 `OnClose`（`ErrUnpublished`、等待落空的
  `ErrNotFound`、`ErrClosed`）。设置 `SubscribeOptions.Wait` 后，订阅可以早于发布者存在，
  发布者离开后最多再保留 `Wait`，并从下一个发布者那里再次收到 `OnTracks`：先连上来的播放器、
  编码器重连期间的 HLS 播放列表、等待源流的转推，都是这样保住位置的。`SubscribeOptions.StartWait`
  把等待第一个发布者的时长与 `Wait` 分开限制，供等待流开始与等待流回来时长不同的出口使用（HLS 与
  WHEP：先 `PlayWait`，之后 `RepublishWait`）。包的时间是发布者自己的
  时间，重新推流可能让它从头开始；各出口会让播放者看到的时间在重新推流前后保持递增（见
  [重新推流](#播放起点与重新推流)）。

**GOP 缓存。** 每路流保留新订阅者从一幅完整画面开始所需的内容：从最近一个视频关键帧起的
所有包（含音频），纯音频流则保留最近一秒。`Subscribe` 在流的锁下把 `OnTracks` 和缓存交给
订阅者，所以之后的包正好接上，不丢也不乱序。缓存受 `Config.GOPCacheBytes`（16 MiB）限制：
超出的 GOP 被丢弃，从下一个关键帧重新开始缓存。没有缓存可用的订阅者（设置了 `NoGOPCache`，
或缓存刚被丢弃）从下一个关键帧开始收视频，音频则立即开始。

**慢消费者。** `hub.NewDropper(next, backlog, limit)` 是每个出口包在 sink 外面的策略。
`backlog` 报告消费者已排队但尚未发出的量（fib 连接排队的字节、HTTP/2 或 HTTP/3 流控压住的
数据、SRT 未确认的载荷）；超过 `limit` 时包被丢弃而不是排队，一旦丢了一个视频包，视频就一直
压到下一个关键帧，因为依赖被丢帧的那些帧解码器用不了。积压回到限值以下，音频立即恢复。这样
一个 sink 的内存不超过限值加一个包，发布者也永远不用等待。有连接可断的出口还会检查第二个、
更高的限值，超过它，或输出在空闲超时内一直不动，就断开这个消费者。

| 出口 | 积压 | 超过多少丢包 | 何时断开 |
| --- | --- | --- | --- |
| RTMP | 连接排队的字节 | `WriteQueueLimit`（2 MiB） | 超过 `MaxWriteQueue`（16 MiB），或输出在 `IdleTimeout`（30 s）内一直不动 |
| HTTP-FLV、WebSocket-FLV | 排队的字节；HTTP/2 与 HTTP/3 上为流控压住超过 64 KiB 的数据 | `WriteQueueLimit`（2 MiB） | HTTP/1 超过 `MaxWriteQueue`（16 MiB）；任何输出在 `IdleTimeout`（30 s）内一直不动的播放者 |
| 走 TCP 的 RTSP 播放者 | 连接排队的字节 | `MaxQueuedBytes`（4 MiB） | 超过 `MaxQueuedBytes` 的两倍 |
| 走 UDP 的 RTSP 播放者 | —（数据报来一个就发一个给 socket）；其接收端报告的丢包率 | socket 拒收一个数据报：视频等到下一个关键帧 | 接收端报告的丢包率在 `PlayerLossWindow`（10 s）内一直高于 `MaxPlayerLoss`（50%）；`SessionTimeout`（60 s）内没有请求或 RTCP |
| SRT 播放者 | 未确认的载荷 | `MaxSendBacklog`（4 MiB） | 对端在 `PeerIdleTimeout`（5 s）内不发任何数据 |
| WHEP 观众 | 其接收端报告的丢包率 | `MaxLoss`（25%） | ICE consent 丢失 |
| HLS | —（每路流每种格式一个 Muxer，而不是每个播放者一个） | Muxer 的 `MaxBytes`（64 MiB）淘汰旧分片 | — |

等待流的第一个发布者的播放者不持有媒体，只占一个位置：RTMP、HTTP-FLV 和 SRT 播放者各占一个
hub 订阅；HLS 播放列表请求占一个被 retain 的请求（整个 handler 最多 `MaxPlayWaiters` 个，默认
1024，单路流上还受 `Muxer.MaxWaiters` 限制）；WHEP offer 占一个被 retain 的请求和一个尚未创建
PeerConnection 的会话（最多 `MaxPlayWaiters` 个，默认 1024）。

转推也以同样的方式丢包：`rtmp.Push` 超过 `ClientConfig.WriteQueueLimit`，
`rtsp.PublishFromHub` 超过 `ClientConfig.MaxQueuedBytes`，`srt.Publish` 超过
`ClientConfig.MaxSendBacklog`，`rtc.PushFromHub` 在服务端报告丢包超过 25% 期间。

**上限。** 流数由 `MaxStreams` 限制，订阅数由 `MaxSubscribersPerStream` 限制（超过任一个返回
`ErrLimit`），缓存由 `GOPCacheBytes` 限制，每个 sink 的队列由 sink 自己限制。既没有发布者也
没有订阅的流会从流表中移除。`Hub.Streams` 返回按路径排序的每路流快照：路径、track、已经在直播
还是仅被预占、发布者的 `PublishInfo` 及开始时间、订阅者数，以及流入的字节数与包数；
`Hub.Stream(path)` 给出单个路径的同样快照，不必遍历其他流。
`Hub.Close` 以 `ErrClosed` 结束所有订阅，丢弃发布者之后写入的内容，并让之后的 `Publish`、
`Reserve` 和 `Subscribe` 失败。

### 线程规则

分发是同步的。`WritePacket` 在流的锁下、在发布者的 goroutine 上调用每个 sink 的 `OnPacket`，
这个 goroutine 通常就是读到这个包的 fib worker。发布者和订阅者之间没有 hub 自己的队列。由此
有三条规则：

- **sink 不能阻塞。** 它复用这个包，把字节不等待地排进自己连接的队列（fib 的 `Send`，或对被
  retain 的 HTTP 响应用 `WriteNoWait`），队列太长就丢包，`NewDropper` 就是这么做的。一个会等待
  的 sink 会卡住发布者以及这路流的所有其他订阅者。
- **sink 不能在回调里调用 hub**，只有一个例外：`Subscription.Close` 从不阻塞，在任何地方都能
  调用，包括 sink 自己的回调；流的锁被占用时它只做标记，由持锁者把订阅摘掉。
  （`Subscription.Close` 不会调用 `OnClose`。）
- **包是共享的。** `Packet.Data` 交给 `WritePacket` 之后就不可修改；所有订阅者读的是同一份
  字节，谁都不能改。hub 的钩子被调用时不持有它的任何锁。

各协议的服务端自己也遵守这些规则。RTMP 在会话锁下收集一轮读到的消息，释放锁之后再处理，
因为播放者的 sink 是在其发布者的锁下被调用的。HLS handler 对需要等待的请求（第一个分片、
blocking reload、preload hint 指向的 part）的处理方式是 retain 请求，由完成所等内容的那个
goroutine 去应答，既不在发布者的 goroutine 上、也不在锁下，所以等待中的播放器不会占住 fib
worker。

### 认证

`hub.Config.OnPublish` 和 `OnSubscribe` 能看到每一次推流和播放，不论协议，参数是
`PublishInfo`/`SubscribeInfo`：协议名（`rtmp`、`rtsp`、`srt`、`whip`、`whep`、`http-flv`、
`ws-flv`、`hls`；RTMP 的转发是 `rtmp-pull` 和 `rtmp-push`，HTTP-FLV 的是 `http-flv-pull` 和
`ws-flv-pull`，其它协议的转发用协议名本身）、
远端地址，以及 URL 的 query，token 通常放在这里。返回错误即拒绝这次推流或播放。转发同样会
填写这些信息：`rtmp.Pull`、`rtsp.PlayToHub` 与 `httpflv.Pull` 填远端服务器的地址和其 URL 的 query，
`rtc.PullToHub` 填其 URL 的 query，`srt.Play` 与 `srt.Publish` 填服务器地址和普通 stream ID
中的 query（`live/s?token=...`）。URL 带 query 的 RTSP 播放者会像 ffmpeg 那样以该
URL 为基准 SETUP 各 track（`rtsp://host/live/s?token=x/trackID=0`），服务端会把 `trackID`
从 query 中取回。每个协议还有自己的钩子，处理只有它才知道的信息：

```go
h := hub.New(hub.Config{
	// RTMP (rtmp://host/live/cam?token=...), RTSP, WHIP and WHEP, HTTP-FLV
	// and HLS carry the URL's query; a plain SRT stream ID carries what
	// follows its '?'.
	OnPublish: func(path string, info hub.PublishInfo) error {
		q, _ := url.ParseQuery(info.Query)
		if !valid(path, q.Get("token")) {
			return errors.New("forbidden")
		}
		return nil
	},
	OnSubscribe: func(path string, info hub.SubscribeInfo) error {
		return nil // players: anyone
	},
})

// SRT publishers use the access control syntax, which has no query:
// check one of its keys in OnConnect instead.
srtServer, _ := srt.NewServer(srt.ServerConfig{Hub: h,
	OnConnect: func(req *srt.Request, id srt.StreamID) error {
		if id.Mode == srt.ModePublish && !valid(id.Resource, id.Params["t"]) {
			return srt.RejectUnauthorized
		}
		return nil
	},
})
// RTSP clients answer Digest challenges.
rtspServer := rtsp.NewServer(rtsp.ServerConfig{Hub: h,
	Password: func(path, user string) (string, bool) {
		return "secret", user == "cam"
	},
})
// WHIP and WHEP take a bearer token.
rtcServer, _ := rtc.NewServer(rtc.Config{Hub: h,
	Authorize: func(r *http.Request, kind rtc.Kind, path, token string) error {
		if kind == rtc.KindWHIP && !valid(path, token) {
			return rtc.ErrUnauthorized
		}
		return nil
	},
})
```

`rtmp.ServerConfig` 有 `OnConnect`、`OnPublish` 和 `OnPlay`（其 `OnPublish` 先于 hub 的钩子
执行），`httpflv.Config` 有 `OnPlay`，在 hub 的 `OnSubscribe` 之后执行。RTSP 在 DESCRIBE
和 PLAY 时各询问一次 `OnSubscribe`。WHIP 与 WHEP 先询问 `rtc.Config.Authorize`，再询问 hub
的钩子：`Authorize` 拒绝时，没有 token 回 401，有 token 回 403；钩子拒绝回 403，路径已被
占用回 409，流不存在回 404（设置了 `PlayWait` 时，在它耗尽之后）。HLS 对每路流的每种
格式只订阅一次，发生在第一个播放列表请求时，所以 `OnSubscribe` 只能看到那一个请求；要检查
每个 HLS 请求，应在 handler 前面加一层 HTTP 中间件。

## 统一的包格式

每个接入都把收到的内容转换成同一种形式，每个出口都从这种形式复用输出，所以任意两种协议之间
都不需要专门的代码。这种形式就是 ISO BMFF（MP4）使用的形式（[`media/av`](../media/av)）：

- `av.Track.Config` 是编码的带外配置：H.264 的 AVCDecoderConfigurationRecord，H.265 的
  HEVCDecoderConfigurationRecord，AV1 的 AV1CodecConfigurationRecord，AAC 的
  AudioSpecificConfig，Opus 的 OpusHead（可选），G.711 为 nil。发布后不可修改；配置变化就是
  一组新的 `[]*av.Track`（`Publisher.SetTracks`）。`Width`、`Height`、`SampleRate`、
  `Channels` 在已知时填写。
- `av.Packet.Data` 是一个 access unit：H.264 与 H.265 为 AVCC（4 字节长度前缀，不含 access
  unit delimiter），参数集在 `Track.Config` 中，但也可能出现在带内；AV1 为一个 temporal unit
  的 OBU，不含 temporal delimiter；AAC 为一个不带 ADTS 的原始 access unit；Opus 为一个 Opus
  包；G.711 为采样数据。
- `DTS` 和 `PTS` 是从流开始起算的 `time.Duration`，所以不同时钟频率的 track 可以直接比较；
  `av.ToTicks` 与 `av.FromTicks` 在它和编码时钟之间做精确换算。`Keyframe` 标记 IDR/IRAP 的
  access unit，所有音频包也都标记为关键帧。

| 协议 | 转入 `av` | 从 `av` 转出 |
| --- | --- | --- |
| RTMP、HTTP-FLV | FLV tag body 本身就是 AVCC 和配置记录；sequence header 成为 `Track.Config`，composition time offset 给出 `PTS`，32 位毫秒时间戳被展开；在 `onMetaData` 声明的每个 track 都收到 sequence header 之前，包会被暂存（最多 512 个包或 1 秒） | 先发 `onMetaData` 和每个 track 的 sequence header，再每个包一个 tag body；H.265 用 enhanced 形式（hvc1），除非设置了 `EncoderConfig.LegacyHEVC`，AV1 与 Opus 总是 enhanced |
| RTSP、WHIP（RTP） | depacketizer 把 access unit 重组为 AVCC，从 SDP 的 `sprop-*`/`config=` 取配置，并跟随带内发送的参数集，参数集改变配置时替换 track；RTP 只有显示时间，解码时间由 SPS 给出的重排深度恢复；每个 track 从第一个包到达的时间开始，之后按 RTP 时间戳推进，再由发送端的 RTCP sender report 对齐各 track（RTSP 在每个 track 开始时对齐；WHIP 的浏览器在发出头几个包之后才发 report，所以每收到一个 report 就调整一次，调整量逐步摊入，时间戳既不跳变也不回退） | packetizer 按 MTU 切分 access unit（STAP-A/FU-A、AP/FU），在每个关键帧前发送参数集，序号与时间戳从随机值开始；RTSP 和 WHEP 另外为所有 track 在同一时钟上发送 RTCP sender report |
| SRT、HLS-TS（MPEG-TS） | Annex B 转为 AVCC，配置记录由带内参数集构造；去掉 ADTS，由其头部得出 AudioSpecificConfig；Opus 由描述符构造 OpusHead；33 位 PTS 与 DTS 被展开；节目中每路流都有了配置之后才宣布 track | Annex B，带 access unit delimiter，缺参数集的关键帧前补上参数集；AAC 用 ADTS；Opus 作为私有流，带 ffmpeg 的描述符和控制头；每个关键帧前以及每 100 ms 发送 PAT 和 PMT；每 40 ms 一个 PCR |
| HLS fMP4 | —（`fmp4` 的读取器用于测试和将来的接入） | 配置原样放入 avcC、hvcC、av1C、esds 和 dOps，AVCC 采样原样放入 mdat |

## 各协议支持的编码

每种协议接入（ingest）和分发（egress）的编码。某个出口承载不了的 track 只是在这个出口被
去掉，其它出口照常收到。FLV，因而 RTMP 和 HTTP-FLV，只承载一个视频 track 和一个音频 track。

| 编码 | RTMP、HTTP-FLV | RTSP（RTP） | SRT、HLS-TS（MPEG-TS） | HLS fMP4 | WHIP / WHEP |
| --- | --- | --- | --- | --- | --- |
| H.264 | ✓ | ✓ RFC 6184（发送用 mode 1；STAP-B、MTAP、FU-B 也能读） | ✓ stream type 0x1B | ✓ avc1 | ✓ 接入：mode 0 与 1；分发：mode 1，profile-level-id 取浏览器能解码的 |
| H.265 | ✓ Enhanced RTMP（hvc1）收发；旧式 codec id 12 可接入，设置 `LegacyHEVC` 时也可发出 | ✓ RFC 7798，不含 DONL | ✓ 0x24 | ✓ hvc1 | ✓ 接入；浏览器提供时分发 |
| AV1 | ✓ Enhanced RTMP（av01） | — 无载荷格式 | — | ✓ av01 | — |
| AAC | ✓ | ✓ mpeg4-generic（发送 AAC-hbr；AAC-lbr 与 MP4A-LATM 可读） | ✓ ADTS，0x0F | ✓ mp4a | — 去掉：WebRTC 没有 AAC |
| Opus | ✓ Enhanced RTMP | ✓ RFC 7587 | ✓ 带 Opus registration 的私有流 | ✓ Opus（dOps） | ✓ |
| G.711（PCMU、PCMA） | ✓ sound format 8 与 7 | ✓ | — | — | ✓ |
| MP3 | ✓ sound format 2 | — | —（demuxer 跳过） | — | — |

对常见编码器而言，这意味着：

- **H.264 + AAC**（OBS、ffmpeg、大多数摄像头）除 WHEP 外处处可播，WHEP 只发视频、不发音频。
  浏览器观众如果需要声音，流应以 Opus 发布（走 RTSP、SRT、WHIP 或 Enhanced RTMP），或者由
  fib 之外的转码器转码后重新发布。
- **H.264 + Opus**（浏览器经 WHIP 推流）处处可播，只有早于 Enhanced RTMP Opus 的 RTMP 和
  HTTP-FLV 播放器例外，而这样的播放器占大多数：ffmpeg 5.1 和 7.1 也读不了 FLV 中的 Opus。
- **H.265** 在 RTMP 和 HTTP-FLV 上需要 Enhanced RTMP 或旧式 codec id 12（ffmpeg 7.1 能读
  enhanced 形式，5.1 两种都不能），在 WHEP 上需要浏览器提供 H.265。
- **G.711**（IP 摄像头）由 RTMP、HTTP-FLV、RTSP 和 WebRTC 承载，在 SRT 和 HLS 上被去掉，
  这两者只播视频。
- 协议接入不了的 track 在接入时就被丢弃：FLV 解码器丢弃它不认识的编码的 tag（VP6、Speex、
  Nellymoser……），MPEG-TS demuxer 跳过 MP3、MPEG-2 视频和 AC-3。分发方向上，WHEP 端点在
  第一次去掉 AAC、MP3、AV1 时各记一行日志。

## 一个服务跑全部协议

一个 engine、一个 hub 服务所有协议：TCP 1935 上的 RTMP，TCP 8554 上的 RTSP 以及 UDP 8000 和
8001 上的 RTP，UDP 9000 上的 SRT，8080 上的 HTTP-FLV、WebSocket-FLV、HLS（fMP4，或加
`?format=ts` 的 MPEG-TS）、`/ll/` 下的 LL-HLS，以及 WHIP 和 WHEP，它们的 PeerConnection 共用
UDP 8080 端口。任何协议都可以向 `live/` 下的路径推流，其它所有协议都能播放。

```go
package main

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/hls"
	"github.com/lesismal/fib/media/httpflv"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/rtc"
	"github.com/lesismal/fib/media/rtmp"
	"github.com/lesismal/fib/media/rtsp"
	"github.com/lesismal/fib/media/srt"
	"github.com/lesismal/fib/webrtc"
)

// router sends each HTTP request to the handler of its protocol.
type router struct {
	flv, hls, llhls fibhttp.Handler
	rtc             *rtc.Server // set before the engine runs
}

func (r *router) ServeHTTP(c *fibhttp.Context) {
	p := c.Request.URL.Path
	switch {
	case strings.HasPrefix(p, "/whip/"), strings.HasPrefix(p, "/whep/"), strings.HasPrefix(p, "/rtc/session/"):
		r.rtc.ServeHTTP(c) // WHIP, WHEP and their sessions
	case strings.HasSuffix(p, ".flv"):
		r.flv.ServeHTTP(c) // HTTP-FLV, or WebSocket-FLV on an upgrade
	case strings.HasPrefix(p, "/ll/"):
		r.llhls.ServeHTTP(c) // LL-HLS
	default:
		r.hls.ServeHTTP(c) // HLS: fMP4, or MPEG-TS with ?format=ts
	}
}

func main() {
	h := hub.New(hub.Config{
		// One place decides who publishes and plays, whatever the protocol:
		// info.Protocol, info.Remote and info.Query (where tokens travel).
		OnPublish: func(path string, info hub.PublishInfo) error {
			if !strings.HasPrefix(path, "live/") {
				return errors.New("publish under live/ only")
			}
			log.Printf("publish %s over %s from %v", path, info.Protocol, info.Remote)
			return nil
		},
		OnUnpublish: func(path string) { log.Printf("unpublish %s", path) },
	})

	wait := 10 * time.Second // players may come first, and survive a republish
	r := &router{
		flv: httpflv.NewHandler(httpflv.Config{Hub: h, Wait: wait, CORS: "*"}),
		hls: hls.NewHandler(hls.HandlerConfig{
			Hub:         h,
			Formats:     []hls.Format{hls.MPEGTS}, // index.m3u8?format=ts
			AllowOrigin: "*",
		}),
		llhls: hls.NewHandler(hls.HandlerConfig{
			Hub:         h,
			Prefix:      "/ll/",
			Muxer:       hls.Config{PartDuration: 500 * time.Millisecond},
			Formats:     []hls.Format{hls.MPEGTS},
			AllowOrigin: "*",
		}),
	}
	rtspServer := rtsp.NewServer(rtsp.ServerConfig{Hub: h}) // RTP over UDP on :8000/:8001
	srtServer, err := srt.NewServer(srt.ServerConfig{Hub: h, PlayWait: wait})
	if err != nil {
		log.Fatal(err)
	}

	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: ":1935", Handler: rtmp.NewServer(rtmp.ServerConfig{Hub: h, PlayWait: wait})},
		fib.Binding{Addr: ":8554", Handler: rtspServer},
		fib.Binding{Network: "udp", Addr: ":9000", Handler: srtServer},
		fib.Binding{Addr: ":8080", Handler: fibhttp.NewHandler(r)},
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := rtspServer.ListenUDP(engine); err != nil {
		log.Fatal(err)
	}
	// Every WHIP and WHEP PeerConnection shares UDP port 8080.
	mux, err := webrtc.NewMux(engine, webrtc.MuxConfig{Addr: ":8080"})
	if err != nil {
		log.Fatal(err)
	}
	r.rtc, err = rtc.NewServer(rtc.Config{
		Hub:         h,
		WebRTC:      webrtc.Configuration{Mux: mux, ICELite: true},
		AllowOrigin: "*",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(engine.Run())
}
```

用下面任意一种方式推流：

```sh
# RTMP（OBS：服务器 rtmp://host/live，串流密钥 s）
ffmpeg -re -i in.mp4 -c copy -f flv rtmp://127.0.0.1/live/s
# RTSP，TCP 或 UDP
ffmpeg -re -i in.mp4 -c copy -f rtsp -rtsp_transport tcp rtsp://127.0.0.1:8554/live/cam
# SRT：访问控制语法，m=publish
ffmpeg -re -i in.mp4 -c copy -f mpegts 'srt://127.0.0.1:9000?streamid=#!::r=live/srt,m=publish'
# WHIP：浏览器或支持 WHIP 的编码器把 offer POST 到 http://127.0.0.1:8080/whip/live/web
```

再用下面任意一种方式播放 `live/s`：

| 协议 | URL |
| --- | --- |
| RTMP | `rtmp://127.0.0.1/live/s` |
| RTSP | `rtsp://127.0.0.1:8554/live/s`（TCP 或 UDP） |
| SRT | `srt://127.0.0.1:9000?streamid=live/s` |
| HTTP-FLV | `http://127.0.0.1:8080/live/s.flv` |
| WebSocket-FLV | `ws://127.0.0.1:8080/live/s.flv` |
| HLS，fMP4 分片 | `http://127.0.0.1:8080/live/s/index.m3u8` |
| HLS，MPEG-TS 分片 | `http://127.0.0.1:8080/live/s/index.m3u8?format=ts` |
| LL-HLS | `http://127.0.0.1:8080/ll/live/s/index.m3u8` |
| WHEP | 把 SDP offer POST 到 `http://127.0.0.1:8080/whep/live/s` |

路径在各处的映射方式相同：`rtmp://host/app/stream` 对应 `app/stream`，
`rtsp://host:8554/app/stream`、`/app/stream.flv`、`/app/stream/index.m3u8`、
`/whip/app/stream`、`/whep/app/stream` 以及 SRT stream ID `app/stream` 或
`#!::r=app/stream,m=publish` 也都对应它。

正式的服务还要考虑：

- **地址。** 绑定在通配地址上的 Mux 会通告所有处于 up 状态的网卡地址；在一对一 NAT 之后，
  设置 `webrtc.MuxConfig.PublicAddrs`。走 UDP 的 RTSP 还需要开放 8000 和 8001 端口
  （`ServerConfig.UDPRTPAddr`、`UDPRTCPAddr`）。见
  [WebRTC](webrtc.zh-CN.md#应答浏览器-offer-的服务端)。
- **上限。** `hub.Config.MaxStreams` 与 `MaxSubscribersPerStream`、
  `hls.HandlerConfig.MaxStreams`、`rtc.Config.MaxSessions`、`srt.ServerConfig.MaxConns`，
  以及 fib 自身的连接数上限。
- **TLS。** RTMPS 就是把 `tls.NewServer(config, rtmpServer)` 作为 binding 的 handler；
  HTTPS、HTTP/2 和 HTTP/3 提供 HTTP-FLV、HLS 和 WHIP/WHEP 的方式，与提供任何 fib/http
  handler 相同。
- **统计。** `Hub.Streams()` 给出正在发布和观看的内容，`srt.Session.Stats` 给出丢包和往返
  时间。

## 客户端与转发

所有客户端都经 fib engine 拨号，通过回调报告结果，都不会阻塞 fib worker。每种协议都有拉流
转发（把远端的流播放进 hub 的某个路径）和转推（订阅 hub 的某个路径，发布到远端服务器）：

```go
// RTMP: pull another server's stream into "live/cam", and push "live/cam" on,
// waiting up to 30 s for a publisher (hub.SubscribeOptions.Wait).
rtmp.Pull(engine, h, "rtmp://origin.example.com/live/cam", "live/cam", rtmp.ClientConfig{},
	func(err error) { log.Println("pull started:", err) })
rtmp.Push(engine, h, "live/cam", "rtmp://edge.example.com/live/cam?token=t", 30*time.Second,
	rtmp.ClientConfig{}, func(err error) { log.Println("push started:", err) })

// RTSP: a camera pulled over TCP interleaved, and a push to another server.
rtsp.PlayToHub(rtsp.ClientConfig{Engine: engine, Transport: rtsp.TransportTCP},
	"rtsp://admin:pass@192.168.1.64:554/Streaming/Channels/101", h, "live/door")
rtsp.PublishFromHub(rtsp.ClientConfig{Engine: engine}, "rtsp://edge.example.com:8554/live/door", h,
	"live/door", hub.SubscribeOptions{Wait: 30 * time.Second})

// SRT: push to a listener, and pull from one.
srt.Publish(engine, "edge.example.com:9000", h, "live/cam",
	srt.ClientConfig{StreamID: "#!::r=live/cam,m=publish", Latency: 200 * time.Millisecond, Wait: 30 * time.Second})
srt.Play(engine, "origin.example.com:9000", h, "live/remote",
	srt.ClientConfig{StreamID: "#!::r=live/cam,m=request", Passphrase: "a passphrase of 10+ bytes"})

// WHIP and WHEP: push to and pull from another WebRTC server.
cc := rtc.ClientConfig{Engine: engine, Token: "bearer-token"}
rtc.PushFromHub(cc, "https://edge.example.com/whip/live/cam", h, "live/cam", hub.SubscribeOptions{Wait: 30 * time.Second})
rtc.PullToHub(cc, "https://origin.example.com/whep/live/webrtc", h, "live/webrtc")

// HTTP-FLV: pull from an origin or a CDN (ws:// and wss:// pull WebSocket-FLV).
httpflv.Pull(httpflv.PullConfig{Engine: engine, OnClose: func(err error) { log.Println("pull ended:", err) }},
	"https://cdn.example.com/live/cam.flv?token=t", h, "live/cdn")
```

- 拉流转发在得知远端的 track 后发布路径（`rtmp.Pull`、`srt.Play` 与 `httpflv.Pull` 会先预占
  路径，所以路径已被占用时立即以 `hub.ErrStreamBusy` 失败；`httpflv.Pull` 在源站回 200 OK 时
  预占），把 track 的变化以 `SetTracks` 传下去，远端流结束时取消发布。
  发布被拒绝会关闭客户端。
- 转推从流的第一组 track 开始，远端跟不上的包按 `hub.NewDropper` 的方式丢弃，流结束时转推
  也结束。track 变化时，RTMP 转推发送新的 sequence header，SRT 转推发送新的 PMT；WHIP 转推
  沿用连接时的编码继续，新出现的编码不发，因为那需要新的 offer；RTSP 在一个会话内无法重新
  announce track，所以 track 变化会关闭 RTSP 转推。WHIP 转推在 PeerConnection 连上之前暂存
  最近一个 GOP。
- 重连由应用负责：`done`/`OnClose` 告知转发已结束，重新启动只需一次调用。转推的 `Wait` 让它
  在源流重新推流期间保住位置。
- 不用 hub 时，这些客户端就是普通的播放器和推流端：`rtmp.Play(engine, url, config, sink,
  ready)` 播放到任意 `hub.Sink`，`rtmp.Publish` 返回的 `ClientConn` 本身就是一个
  `hub.Sink`，`rtsp.Play`/`rtsp.Publish` 和 `rtc.Pull`/`rtc.Push` 接受 `OnTracks` 与
  `OnPacket` 回调并提供 `WritePacket`，`srt.Dial` 给出一个原始会话，传输应用写入的任意字节。
- 走 UDP 的 RTSP 客户端为每路流在自己的 engine 上添加一对 UDP binding；SRT 客户端用
  `Engine.OpenUDPPeer` 打开自己的 binding；WHIP/WHEP 客户端打开一个自带端口的
  PeerConnection，除非 `ClientConfig.WebRTC` 指定了 Mux 或 engine。

## 播放起点与重新推流

**播放从哪里开始。** 播放者通常从 hub 缓存的 GOP 开始，这样立刻就有画面。SRT 播放者，以及
有流走 UDP 的 RTSP 播放者，则从下一个关键帧开始。缓存的 GOP 是一次性突发发出的，一路
4 Mbit/s 的 720p 流的 GOP 约有 1 MB，超过接收端 socket 缓冲区的容量（Linux 默认 208 KiB）。
RTSP 的 UDP 没有任何机制重发缓冲区丢掉的数据；SRT 中这次突发的每个包都打上发送时刻的时间戳，
NAK 只有一个延迟（120 ms）的时间去找回几百个包，结果找不回来。加入这样一路流的每个 ffmpeg
播放器开头都有 H.264 解码错误（SRT 还报告 RCV-DROPPED），一个播放器和五十个都一样。从下一个
关键帧开始，代价是最多等待一个关键帧间隔，之后平稳的码流 TSBPD 和 NAK 都能应付。
`srt.ServerConfig.PlayGOPCache` 可以重新打开从缓存开始的方式，适合小码流，或缓冲区与延迟都
较大的接收端。RTMP、HTTP-FLV、走 TCP 的 RTSP、HLS 和 WHEP（除非设置 `rtc.Config.NoGOPCache`）
都从缓存开始。

**第一个发布者到来之前。** 播放者可能在任何人发布之前就请求一路流，比如比编码器先启动的监控墙
或测试台。RTMP 和 SRT 的 `PlayWait`、HTTP-FLV 的 `Wait` 让这样的播放者等待
（`hub.SubscribeOptions.Wait`）；RTSP 做不到，因为 DESCRIBE 需要 track。HLS 和 WHEP 的
`PlayWait`（`hub.SubscribeOptions.StartWait`）让播放列表请求或 WHEP offer 等待，请求被 retain，
不占用 fib worker：播放列表在第一个分片切出后回复（最多 `PlayWait` 加 `WaitTimeout`），offer 在
发布者的 track 确定后回复；`PlayWait` 内没有发布者到来时两者都回 404。超过 `MaxPlayWaiters`
的播放者回 503，等待中挂断的播放者会被放掉。这些字段都为 0（默认）时，播放缺席的流会被立即拒绝。

**重新推流前后。** 在重新推流期间等待的播放者（RTMP 和 SRT 的 `PlayWait`、HTTP-FLV 的
`Wait`、RTSP 的 `PlayWait`、HLS 和 WHEP 的 `RepublishWait`）会从下一个发布者那里收到
`OnTracks`，而它的时钟可能从任何地方开始：RTMP 或 ffmpeg 发布者会重新从 0 开始。播放者需要
持续递增的时间（ffmpeg 对时间回退之后的每个包都报错，flv.js 和 MSE 播放器会卡住），所以每个
出口都让自己的时间线接着往下走：

- **`flv.Timeline`** 为 RTMP 和 HTTP-FLV 出口（以及 `flv.Muxer`）的 RTMP 消息和 FLV tag
  打时间戳。`Restart` 之后，落在最后一个时间戳之前、或超出它十秒以上的包，会在最后一个时间戳
  之后开始一个新时钟，间隔等于流中断的时长（20 ms 到十秒），这样时间戳与播放者等待所依据的
  墙上时钟保持同步，两个时钟也不会共用同一个时间戳；落在本 track 最后一个时间戳之前的包取那个
  时间戳。自己写 FLV 的应用也可以使用它。
- **RTSP** 播放者（`rtsp.ServerConfig.PlayWait`）用同样的方式推进 RTP 时间，SSRC 不变，并
  保持 RTCP sender report 的映射不变——ffmpeg 正是按它给 RTSP 包定时的。下一个发布者编码不同
  时 RTSP 播放者结束，因为它的 SDP 无法改变；DESCRIBE 仍然需要有发布者。
- **SRT** 播放者用同样的方式推进 MPEG-TS 时间。
- **HLS** 在 `EXT-X-DISCONTINUITY` 之后启动新的分片写入器，两种格式都让它从上一个写入器结束
  的地方开始（`fmp4.Writer.StartAt` 与 `End`，以及 TS 写入器自己的同名方法）。RFC 8216 允许
  时间在 discontinuity 处跳变，但 ffmpeg 在 init 分片相同时会跨 discontinuity 沿用同一个 MP4
  解复用器，曾因此丢弃之后的第一个 fMP4 分片；它对 TS 时间回退的修正也曾让一个解码时间重复。
  TS 写入器在 discontinuity 之后暂存最初的包（直到每个 track 都有一个，或 256 个包、一秒），
  使比关键帧稍早一点的音频不会落到上一个写入器结束的位置之前。

## 配置与限制

除了 `Hub`（以及 `rtc.Config` 中带 Mux 或 engine 的 `WebRTC`），所有字段都是可选的；零值取
下面的默认值。RTMP 服务端在 `OnPublish` 为每次推流返回 sink 时可以不用 hub，SRT 服务端在
`OnSession` 接管所有会话时也可以。处处都有超时，对端能填满的每个缓冲区都有上限。

**hub.Config**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `GOPCacheBytes` | 16 MiB | 每路流的 GOP 缓存；负数表示不缓存 |
| `MaxStreams`、`MaxSubscribersPerStream` | 不限 | 超过时返回 `ErrLimit` |
| `OnPublish`、`OnUnpublish`、`OnSubscribe` | 无 | 认证与观察，调用时不持有 hub 的锁 |

`hub.SubscribeOptions` 有 `Info`（`OnSubscribe` 看到的内容）、`Wait`（0 表示不等待：没有
发布者时返回 `ErrNotFound`，发布者离开时订阅结束）、`StartWait`（设置时代替 `Wait` 作为等待第一个
发布者的时长）和 `NoGOPCache`（从下一个关键帧开始）。

**rtmp.ServerConfig**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `HandshakeTimeout` | 10 s | 从连接到开始推流或播放 |
| `IdleTimeout` | 30 s | 不发任何数据的发布者，或输出不动的播放者 |
| `PlayWait` | 0 | 播放等待缺席发布者、或自己的发布者离开后等待下一个发布者的时长；0 表示立即回 `StreamNotFound` |
| `MaxMessageSize` | 8 MiB | 对端可发送的最大消息 |
| `ChunkSize`、`WindowAckSize` | 4096、2,500,000 | 服务端写出的 chunk 大小，以及要求客户端使用的确认窗口 |
| `WriteQueueLimit`、`MaxWriteQueue` | 2 MiB、16 MiB | 超过前者丢包（视频丢到下一个关键帧），超过后者断开 |
| `Encoder.LegacyHEVC` | false | 用 codec id 12 而不是 Enhanced RTMP 写 H.265 |
| `OnConnect`、`OnPublish`、`OnPlay`、`OnClose` | 无 | 钩子；`OnPublish` 可以返回一个 sink，从而让服务端不用 hub 也能运行 |

`rtmp.ClientConfig` 同样有 `HandshakeTimeout`、`IdleTimeout`、`MaxMessageSize`、
`ChunkSize`、`WriteQueueLimit`、`MaxWriteQueue` 和 `Encoder`，另有 `DialTimeout`（10 s）、
用于 `rtmps://` URL 的 `TLSConfig`、`FlashVer`、`SimpleHandshake`（不设置时发送 digest
握手；检查 digest 的服务端会拒绝 simple 握手）以及 `OnClose`。

**httpflv.Config**：`Path`（`/app/stream.flv` 播放 `app/stream`）、`OnPlay`、`CORS`、
`Wait`（0 表示缺席的流回 404）、`WriteQueueLimit` 与 `MaxWriteQueue`（2 MiB 与 16 MiB；在
HTTP/2 和 HTTP/3 上，积压指流控压住超过 64 KiB 的数据，且只有 HTTP/1 的播放者会被断开）、
`IdleTimeout`（30 s）、`Encoder`，以及 `WebSocket`（零值接受任何 origin）。

**httpflv.PullConfig**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `Engine` | 必填 | 拨号源站并读取；必须已在运行 |
| `TLSConfig`、`Header`、`Dial` | 无 | 用于 https:// 和 wss://（源站选择时走 HTTP/2）；随请求发送的头；代替 engine 拨号的函数，例如 SOCKS 代理的 |
| `DialTimeout`、`HeaderTimeout` | 10 s、10 s | 建立连接，以及等待响应头（WebSocket-FLV 时两者之和限定握手） |
| `IdleTimeout` | 30 s | 从响应头起，源站这么久什么都没发就结束拉流；负值表示不限 |
| `MaxRedirects` | 5 | 跟随的 301、302、303、307 和 308 次数；负值表示不跟随 |
| `BufferSize` | 256 KiB | 等待解封装的 body 达到这么多时，连接暂停读取 |
| `MaxTagSize` | 8 MiB | 最大的 FLV tag |
| `OnTracks`、`OnPacket`、`OnClose` | 无 | 在 hub 拿到 track 或包之后调用；`OnClose` 只调用一次：源站结束流时为 nil，`Close` 之后为 `ErrClosed`，源站不回 200 OK 时为 `StatusError`，或 hub 拒绝的错误 |

**rtsp.ServerConfig**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `UDPRTPAddr`、`UDPRTCPAddr` | `:8000`、`:8001` | `ListenUDP` 绑定的 RTP/RTCP 端口对，所有会话共用 |
| `Password`、`Realm`、`AuthMethods` | 无、`fib`、Digest | 认证；Basic 以明文发送密码。Digest 的 nonce 无状态，有效期 5 分钟 |
| `SessionTimeout`、`IdleTimeout` | 60 s、60 s | 会话没有请求、RTCP 或（录制时）RTP 的时长；连接没有会话的时长 |
| `MaxHeaderBytes`、`MaxBodyBytes` | 16 KiB、256 KiB | 请求的上限 |
| `PlayWait` | 0 | 播放者在发布者离开后继续存在、等待编码相同的下一个发布者的时长；0 表示播放者随发布者一起结束 |
| `MaxQueuedBytes` | 4 MiB | 走 TCP 的播放者积压超过它就丢包；超过两倍就关闭。走 UDP 的播放者，RTP 来一个就发一个给 socket；socket 拒收一个视频数据报后，视频等到下一个关键帧再发 |
| `MaxPlayerLoss`、`PlayerLossWindow` | 0.5、10 s | 走 UDP 的播放者，RTCP 接收端报告在整个窗口内每次都说某路流丢包超过这个比例，就关闭它；有一次低于它，窗口重新计算。`MaxPlayerLoss` 为负数或不小于 1 时不因丢包关闭任何人 |
| `MTU`、`SenderReportInterval` | 1400、5 s | RTP 包大小，RTCP sender report 间隔 |
| `ReorderSize`、`ReorderDelay` | 128 个包、100 ms | 每路经 UDP 录制的流的重排缓冲区 |
| `OnRequest` | 无 | 查看每个已认证的请求，也可以代为应答 |

`rtsp.ClientConfig` 有 `Engine`（必填，且已在运行）、`Transport`（TCP 或 UDP）与
`UDPHost`（UDP binding 的本地地址）、`Username` 与 `Password`（不设置时取 URL 中的用户
信息）、`DialTimeout`、`RequestTimeout` 与 `ReadTimeout`（各 10 s）、`KeepAlive`（服务端
给出的会话超时的一半，最多 30 s）、`MTU`、`MaxQueuedBytes`（4 MiB）、重排缓冲区的大小和
延迟、`UserAgent`，以及回调 `OnTracks`、`OnPacket`、`OnReady` 和 `OnClose`。

**srt.ServerConfig**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `Latency` | 120 ms | 请求的 TSBPD 延迟；取两端中较大的 |
| `Passphrase`、`PBKeyLen` | 无、16 | 加密：10 到 79 字节；密钥长度 16、24 或 32（listener 采用 caller 的）。设置了口令时，拒绝没有口令的 caller，反之亦然 |
| `PeerIdleTimeout` | 5 s | 对端不发任何数据的超时 |
| `FlowWindow`、`SendBufferPackets` | 8192、8192 个包 | 接收缓冲区与对端在途包数；未确认加待发的包数 |
| `MaxSendBacklog` | 4 MiB | 播放者未确认的载荷超过它就丢包 |
| `PlayWait` | 0 | 播放者等待缺席流、或自己的发布者离开后等待下一个发布者的时长；0 表示以 `RejectNotFound` 拒绝缺席流的播放者 |
| `PlayGOPCache` | false | 播放者从缓存的 GOP 而不是下一个关键帧开始（见[播放从哪里开始](#播放起点与重新推流)） |
| `MaxConns` | 不限 | 会话数；超过时 caller 收到 `RejectOverload` |
| `OnConnect`、`OnSession`、`ParseStreamID` | 无 | 认证、把会话从 hub 手里接管过来、自定义 stream ID 语法 |
| `Muxer`、`Demuxer` | MPEG-TS 默认值 | 每 100 ms 发送一次 PAT/PMT、每 40 ms 一个 PCR，PES 上限 4 MiB，得知 track 期间最多暂存 256 个包 |

载荷最多 1456 字节；播放者每个 SRT 包收到 7 个 TS 包（1316 字节）。

`srt.ClientConfig` 有 `StreamID`，同样的 `Latency`、`Passphrase`、`PBKeyLen`、
`PeerIdleTimeout`、`FlowWindow`、`SendBufferPackets`、`MaxSendBacklog`、`Muxer` 和
`Demuxer`，另有 `HandshakeTimeout`（3 s）、`Wait`（`Publish` 的
`hub.SubscribeOptions.Wait`），以及回调 `OnConnect`、`OnClose`（listener 拒绝时为一个
`RejectReason`）和 `OnData`（`Dial` 会话的载荷）。不带 hub 的 `Play` 把流交给 `Demuxer` 的
`OnTracks` 与 `OnPacket`。

**hls.HandlerConfig** 及其 `Muxer hls.Config`

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `Muxer.Format`、`Formats` | FMP4、无 | 默认分片格式，以及播放器可用 `?format=` 选择的其它格式（每种被播放的格式各自运行一个 Muxer） |
| `Muxer.TargetDuration` | 2 s | 在距分片起点至少这么久的第一个关键帧处切分；应设为关键帧间隔 |
| `Muxer.PlaylistSize`、`KeepSegments` | 6、3 | 列出的分片数，以及离开列表后仍保留的分片数 |
| `Muxer.MaxBytes` | 64 MiB | 每个 Muxer 持有的媒体数据；超过其四分之一的分片不等关键帧直接切分 |
| `Muxer.PartDuration` | 0（关闭） | LL-HLS 的 part 时长；通常 200 ms 到 1 s |
| `Muxer.MaxWaiters` | 4096 | 阻塞在一个 Muxer 上的请求数 |
| `Prefix` | `/` | 流路径之前的 URL 路径 |
| `IdleTimeout`、`WaitTimeout` | 30 s、10 s | 没有请求的 Muxer 被停止；请求最多等待这么久 |
| `RepublishWait` | 10 s | 发布者在此时间内回来，播放列表在 discontinuity 之后继续 |
| `PlayWait` | 0 | 播放列表请求等待尚无发布者的流的时长；0 表示立即回 404 |
| `MaxPlayWaiters` | 1024 | 同时等待发布者的请求数；超过时回 503 |
| `AllowOrigin`、`MaxStreams` | 无、不限 | CORS，以及同时运行的 Muxer 数 |

**rtc.Config**（WHIP/WHEP 服务端）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `WebRTC` | — | PeerConnection 的配置：服务端用 `Mux` 加 `ICELite` |
| `WHIPPrefix`、`WHEPPrefix`、`ResourcePrefix` | `/whip/`、`/whep/`、`/rtc/session/` | 端点与会话资源的路径 |
| `MaxSessions` | 不限 | WHIP 与 WHEP 合计；超过时回 503 |
| `Authorize`、`AllowOrigin`、`LinkICEServers` | 无 | bearer token、CORS、以 Link 头下发的 ICE 服务器 |
| `GatherTimeout`、`ConnectTimeout` | 5 s、15 s | 应答前的 candidate 收集，应答后的连接 |
| `RepublishWait` | 10 s | 观众等待下一个发布者的时长 |
| `PlayWait` | 0 | offer 等待尚无发布者的流的时长；0 表示立即回 404 |
| `MaxPlayWaiters` | 1024 | 同时等待发布者的 offer 数；超过时回 503 |
| `NoGOPCache`、`MaxHeldBytes` | false、8 MiB | 观众从下一个关键帧而不是缓存的 GOP 开始；观众连接期间暂存的数据上限 |
| `MaxLoss` | 0.25 | 观众报告的丢包率超过它就丢包（视频丢到下一个关键帧） |
| `MTU`、`ReorderSize`、`ReorderDelay` | 1200、128、100 ms | RTP 包大小，以及 WHIP track 的重排缓冲区 |
| `PLIInterval`、`KeyFrameInterval` | 500 ms、0 | 两次关键帧请求的最小间隔；定期请求 |
| `MaxOfferBytes` | 64 KiB | offer 或 PATCH body 的上限 |
| `OnKeyFrameRequest` | 无 | 观众对一路不由本服务端 WHIP 会话发布的流请求关键帧 |

`rtc.Config.WebRTC.Codecs` 被忽略：每个会话使用其流或 offer 的编码。`Certificates` 为空时，
所有 PeerConnection 共用一张生成的证书。

`rtc.ClientConfig` 有 `Engine`（已在运行；除非设置了 `HTTPClient`，由它发送请求，除非
`WebRTC` 指定了 Mux 或 engine，由它打开 PeerConnection 的端口）、`HTTPClient`、`WebRTC`
（播放的客户端提供其中的 `Codecs`，为 nil 时用 `rtc.DefaultReceiveCodecs()`；推流的客户端
提供其 track 的编码）、`Timeout`（15 s）、`Token`、`Header`、`MTU`（1200）、
`MaxHeldBytes`（8 MiB）、重排缓冲区、`PLIInterval`（500 ms）和 `KeyFrameInterval`，以及回调
`OnTracks`、`OnPacket`、`OnReady`、`OnClose` 和 `OnKeyFrameRequest`（服务端向推流的客户端
请求关键帧）。

核心内部的固定上限：

| 对象 | 上限 |
| --- | --- |
| 从 RTP 重组的一个 access unit | 8 MiB（`DepacketizerConfig.MaxAccessUnit`） |
| 等待所有已声明 track 收到 sequence header 时暂存的 FLV 包 | 512 个包或 1 秒 |
| AMF 嵌套 | `amf.MaxDepth`；计数受剩余字节数约束 |
| MPEG-TS PES | 4 MiB，更大的丢弃 |
| SRT 收发缓冲区 | `FlowWindow` 与 `SendBufferPackets` |
| 每个 HLS Muxer 的媒体数据 | `MaxBytes`，先淘汰旧分片 |
| discontinuity 之后为确定新时钟而暂存的 HLS-TS 包 | 256 个包或 1 秒 |

## 有意没有实现的功能

汇总自各 package 的文档。不做的原因是编码器和播放器所用的直播并不需要，或者需要 fib 没有的
编解码器，或者应由应用负责。

- **全局**：任何形式的转码（音频、视频、分辨率、码率），也因此没有多码率；录制、DVR 和点播
  （所有流都是直播）。
- **Hub**：两个发布者争抢同一个空闲路径时，仍可能留下一次没有配对 `OnUnpublish` 的
  `OnPublish`，因为钩子在锁外执行；路径已被占用时，会在 `OnPublish` 执行前就拒绝。
- **RTMP**：RTMPE 与 RTMPT；shared object（其消息被忽略）；AMF3 回复（objectEncoding 为 3 的
  客户端收到 AMF0 回复）；录播流（play 的 start 和 duration 被忽略）；Enhanced RTMP v2 的
  multitrack 消息（multitrack tag 被跳过）。
- **FLV 与 AMF**：VP8、VP9，以及原规范中除上面矩阵所列之外的编码（Sorenson H.263、VP6、
  Screen Video、Nellymoser、Speex、ADPCM：能解析，随后丢弃）；FLV 加密头；AMF3 编码、其
  externalizable 对象，以及 AMF0 的保留标记。
- **HTTP-FLV**：提供录制文件；自动重连的拉流（`OnClose` 告知已结束，再拉一次只需一次调用）。
- **RTSP**：RTSP 2.0；基于 TLS（rtsps）和 HTTP 隧道的 RTSP；组播；用于跳转的 PLAY Range、
  Scale 与 Speed；比连接活得更久的会话；向录制端发送 RTCP receiver report。
- **RTP 载荷**：H.264 的 interleaved 模式（能读，但不按解码顺序号重排）；H.265 的 DONL；MP3 和
  AV1 的载荷格式；AAC 以外的 mpeg4-generic 模式。
- **SRT**：rendezvous 模式；file（message 与 stream）模式及其拥塞控制；AES-GCM；包过滤器
  （FEC）；socket group（bonding）；SRT 1.3 之前的 HSv4 握手；主动发起密钥更新（Conn 起始的
  密钥可用约 2^31 个包，按 20 Mbit/s 算超过一天；对端发起的密钥更新会被跟随）。
- **MPEG-TS**：MP3、MPEG-2 视频、AC-3 等流类型；多节目；PAT、PMT 之外的 PSI；加扰包；192
  与 204 字节的包；T-STD 缓冲模型（PCR 以固定时间领先 DTS）。
- **fMP4**：edit list（带 B 帧的流会在 0 之后稍晚才开始显示）；加密（cenc）；sample group、
  subsample 信息与 mfra；G.711 与 MP3 track。
- **HLS**：增量播放列表（`_HLS_skip`）与 rendition report；byte-range part；
  `EXT-X-PROGRAM-DATE-TIME`；多 variant；I 帧播放列表；加密。分片比目标时长更长时（关键帧
  间隔大于 `TargetDuration`），`EXT-X-TARGETDURATION` 会被调高，严格来说规范不允许这样做。
- **WHIP/WHEP**：ICE restart（回 422）；simulcast（每个 media section 只接收第一层）；每种
  媒体超过 offer 中 section 数量的流；RTX 与 FEC；流的编码变化时重新协商（旧编码的 track
  停止）。WHIP 按发布者的 sender report 对齐各 track：在第一批 report 到达之前（浏览器要
  一秒多之后才发），以及对从不发 report 的发布者，各 track 之间的偏差只能保证在网络抖动和
  各编码器延迟之差以内。
- **编码工具**：H.264 的 PPS、slice header、SVC/MVC 与 subset SPS；H.265 SPS 中位深和重排之后
  的部分（因此没有帧率和宽高比），以及 SHVC/MV-HEVC；AAC 的 program config element、codec
  package 中的 LATM、含多个 raw data block 的 ADTS 帧、CRC 校验；OpusTags 与 multistream 的
  self-delimited 分帧。

## 验证方式

每个 package 都有自己的测试并在 CI 中运行，不需要 ffmpeg：sans-IO 核心彼此对接、或对接抓取的
字节（`testdata/` 中由 ffmpeg 5.1 和 7.1 为每种编码写出的样本，其中包括 H.264 的 4:4:4、
4:2:2 10-bit、隔行和带裁剪的流，以及 H.265 Main 10），每个接收不可信字节的解析器都有 fuzz
目标，服务端和客户端在 fib engine 上经环回地址对接。每个 media package 都在下面的 Linux
容器中和 macOS 上通过 `go test -race`，加入它们之后，CI 对整个模块的覆盖率下限（96%）依然
满足。

互通测试用的是真实实现，在 Docker 中经环回地址进行：ffmpeg 5.1（Debian bookworm，libx264、
libx265、libopus、libsrt）、ffmpeg 7.1（Debian trixie，能读 Enhanced RTMP 的 H.265 和 AV1），
以及 headless Chromium。大部分测试程序当时是仓库之外的临时模块。其核心部分现在放在
[`media/interop`](../media/interop)，它是一个独立的模块，因此 fib 的 `go.mod` 仍然不引入
任何依赖，根模块的测试也仍然不需要 ffmpeg 就能通过。

CI 在每次 push 和 pull request 时运行 `media/interop`，即 `media-interop` 任务（目前不阻塞
合并）。它原样运行上面的示例服务端。ffmpeg 7.1 经 RTMP、TCP 与 UDP 的 RTSP、SRT 推
H.264+AAC、H.265+AAC 和 H.264+Opus，再经下面[矩阵](#接入与出口矩阵)中的八种出口逐一播放回来，
检查条件与矩阵相同。fib 的 WHEP 客户端也播放每一路，必须收到视频，音频为 Opus 时还必须收到
音频。ffmpeg 自身无法推或读的组合（FLV 中的 Opus）会被跳过，并在日志中列出。在本地用
Docker 运行：

```sh
./media/interop/run.sh                                  # 整个矩阵，镜像拉取之后不到两分钟
./media/interop/run.sh -run 'TestMatrix/h264-aac/rtmp'  # 一种编码组合的一种接入
```

`run.sh` 在 `golang:1.27` 中安装 Debian trixie 的 ffmpeg；`IMAGE=` 可以指定一个已经装好 Go 和
ffmpeg 的镜像，`CPUS=` 限制容器可用的 CPU。PATH 中有 ffmpeg 时，在 `media/interop` 中执行
`go test ./...` 会直接运行这些检查；没有 ffmpeg 时则跳过。下表中的其余各行，以及 ffmpeg 5.1、
G.711、音画同步、重新推流、负载和卡住播放者的测试，都是手工完成的。

| 方面 | 对端 | 验证内容 |
| --- | --- | --- |
| RTMP、HTTP-FLV | ffmpeg 5.1、7.1 | ffmpeg 推 H.264+AAC（5.1），以及经 Enhanced RTMP 推 H.265+AAC 和 AV1+AAC（7.1）；ffprobe 与 ffmpeg 经 RTMP 和 HTTP-FLV 播放回来，四个播放器同时进行，DTS 单调；fib 的客户端播放并写成 FLV 文件，ffmpeg 能解码；fib 的客户端向 ffmpeg 的 `-listen 1` 推流、并从中播放（5.1 上用 H.264，7.1 上用 H.265）；RTMPS 推流与拉流 |
| RTMP、HTTP-FLV 慢播放者 | ffmpeg 5.1 | 一个 RTMP 和一个 HTTP-FLV 播放者在 6 Mbit/s 的 720p 流上停止读取，被断开，旁边正常的播放者照常解码；服务端堆内存全程在 11 到 20 MB 之间 |
| RTSP | ffmpeg 5.1、7.1 | ffmpeg 经 TCP 和 UDP 录制；ffprobe 与 ffmpeg 经 TCP 和 UDP 播放：H.264+AAC、H.265+Opus、H.264+PCMA；经 fib 客户端转发（TCP 与 UDP，H.264 与 H.265）；fib 客户端向 ffmpeg 的 listen 模式推流（TCP 与 UDP）；Digest 与 Basic，没有凭据和凭据错误都被拒绝；URL 带 query（`?token=...`）的播放者经 TCP 和 UDP 播放；一个播放者卡住而另一个正常解码，堆内存平稳 |
| SRT | ffmpeg 5.1、7.1（libsrt） | ffmpeg 推流并播放回 H.264+AAC 与 H.265+Opus，不加密以及 pbkeylen 为 16、24、32，也经过一个双向丢弃 2–10% 数据报的中继，解码干净；fib 的客户端向 listener 模式的 ffmpeg 推流、并从中拉流 |
| MPEG-TS | ffmpeg 5.1、7.1 | ffmpeg 生成的 H.264+AAC、H.265+Opus 和纯 AAC 流能解复用，重新复用后解码没有警告 |
| fMP4 | ffmpeg 5.1、7.1 | H.264+AAC、H.265+AAC、H.264+Opus 的 init 分片和 fragment，来源包括原始码流和 ffmpeg 自己带 B 帧的 fMP4，解码没有错误，重新复用后每个包的时间戳和关键帧标志保持不变 |
| HLS | ffmpeg 5.1、7.1 | fMP4：H.264+AAC、H.265+AAC、H.264+Opus、纯 AAC，以及 500 ms part 的 LL-HLS，播放 10 秒没有错误；同编码重新推流后跨 discontinuity 继续播放；播放列表检查程序确认 target duration、序号、discontinuity、part、blocking reload 和 preload hint 均符合规范。MPEG-TS：RTMP 推上来的 H.264+AAC（5.1）和 H.265+AAC（7.1）从 TS 变体播放，普通与 LL-HLS 均可 |
| 所有接入 × 所有出口 | ffmpeg 5.1、7.1 | 见下面的[矩阵](#接入与出口矩阵) |
| WHIP、WHEP | ffmpeg 5.1、fib 的客户端 | ffmpeg 经 RTSP 推 H.264+Opus；fib 的 WHEP 客户端从 WHEP 端点把它拉进一个路径，fib 的 WHIP 客户端把它推到 WHIP 端点成为另一个路径；ffmpeg 经 RTSP 播放两者，解码没有错误 |
| WHIP、WHEP | headless Chromium | WHEP 播放 ffmpeg 的 H.264+Opus：连接成功，640×360，解码 244 帧，没有丢包；Chromium 用虚拟摄像头和麦克风经 WHIP 推流（OpenH264 Constrained Baseline 与 Opus），ffmpeg 随后经 RTSP 播放并解码，没有错误 |
| 上面的示例 | ffmpeg 5.1、7.1，fib 的 WHEP 客户端 | 原样编译；ffmpeg 同时经 RTMP 和 SRT 推 H.264+AAC、经 RTSP 推 H.264+Opus。RTMP 的流经 RTMP、TCP 与 UDP 的 RTSP、SRT、HTTP-FLV、HLS fMP4 与 TS、LL-HLS 播放；SRT 的流经 HLS 和 RTMP 播放；RTSP 的流经 SRT 和 HLS 播放；ffmpeg 对每一路解码 4 秒没有错误，fib 的 WHEP 客户端收到 RTSP 流的 H.264 与 Opus（5 秒约 400 个包）。WebSocket-FLV 的升级请求得到 101，向 `live/` 之外推流被钩子拒绝 |

### 接入与出口矩阵

所有服务端共用一个 hub，与上面的示例相同。每个格子里，ffmpeg 经一种接入推 15 秒 640×360、
25 fps 的测试图案加单音（x264 或 x265，两个 B 帧，两秒一个 GOP；AAC、Opus 或 G.711 A-law），
三秒后八种出口同时各启动一个播放器，各自复制 6 秒到文件并解码。通过的条件是：ffmpeg 正常退出、
没有错误行，ffprobe 看到的编码、画面尺寸和采样率与推流一致，视频有 120 到 180 帧（SRT 和
走 UDP 的 RTSP 播放者从下一个关键帧开始，为 90 到 180 帧），每路视频流的 DTS 严格递增、每路
音频流的 DTS 不回退。

| 接入 | 编码 | 出口：RTMP、RTSP/TCP、RTSP/UDP、SRT、HTTP-FLV、HLS fMP4、HLS TS、LL-HLS |
| --- | --- | --- |
| RTMP、RTSP/TCP、RTSP/UDP、SRT | H.264 + AAC | 7.1 与 5.1 全部通过 |
| RTMP | H.265 + AAC | 7.1 全部通过；5.1 为 N/A（没有 Enhanced RTMP 可用于推流） |
| RTSP/TCP、RTSP/UDP、SRT | H.265 + AAC | 7.1 全部通过；5.1 除 RTMP 和 HTTP-FLV 为 N/A 外全部通过（5.1 读不了 FLV 中的 H.265） |
| RTMP | H.264 + Opus | N/A（ffmpeg 无法把 Opus 写进 FLV） |
| RTSP/TCP、RTSP/UDP、SRT | H.264 + Opus | RTSP、SRT 与各种 HLS 通过；RTMP 与 HTTP-FLV 为 N/A（ffmpeg 读不了 FLV 中的 Opus） |
| RTMP、RTSP/TCP、RTSP/UDP | H.264 + PCMA | RTMP、RTSP 与 HTTP-FLV 通过；SRT 与各种 HLS 无法承载 G.711，按设计只有视频并通过 |
| SRT | H.264 + PCMA | N/A（MPEG-TS 没有 G.711） |

合计：ffmpeg 7.1 通过 94 个、只含视频通过 12 个、N/A 22 个；ffmpeg 5.1 通过 80 个、只含视频
通过 12 个、N/A 36 个；没有失败。N/A 一律表示 ffmpeg 本身无法推或播这种组合，而不是 fib 拒绝了
它。

在同一个服务端上、用两个 ffmpeg 版本做的其它测试：

- **音视频同步。** 每两秒闪一次白屏并响一声的源，经 RTMP、RTSP/TCP 和 SRT 推流，经每种出口
  播放：每一对的响声减闪屏中位数都是 0 ms，与直接解码本地文件中的同一份编码相同。7.1 上每一对
  都在 −21 到 +26 ms 之内，不到一个 40 ms 的帧；5.1 有个别单次离群值，出现在录制开始或结束于
  响声中间的时候。在这项测试发现的问题修复之前（用 RTCP sender report 对齐录制的 RTSP 流，
  fMP4 中早于第一个包开始的 track 保住自己的位置），RTSP 接入在每种出口上音频都晚 260 ms，
  HLS-fMP4 和 LL-HLS 晚 128 ms（SRT 接入）到 341 ms（RTSP 接入）。
- **重新推流。** 发布者推 8 秒、离开 2 秒、再推 8 秒，每种出口一个在第一次推流期间启动的播放者
  运行 20 秒。7.1 上每种接入（RTMP、RTSP/TCP、SRT）× 每种出口都播放到结束（24–25 秒），没有
  错误行：350 帧，SRT 和走 UDP 的 RTSP 为 300 帧，因为它们从关键帧重新开始。服务端在
  `PlayWait` 之后结束会话时，SRT 播放者会打印一行 "Input/output error"，ffmpeg 的 libsrt 对
  任何对端关闭都这样报告。5.1 上相同，只是从 SRT 接入到 HLS-TS 在 discontinuity 处有一行重复的
  音频 DTS，TS 写入器延续时间线（见上文）之后已经消除：在两个版本上重新运行，任何接入的 HLS
  播放者都没有错误行。这些修复之前，RTMP 和 HTTP-FLV 播放者
  报告 500 多行非单调 DTS，RTSP 播放者 4.6 秒后随发布者一起结束，HLS-fMP4 和 LL-HLS 丢掉重新
  推流后的第一个分片，SRT 和 RTSP 播放者的时间回退，SRT 发布者最后 1.6 秒（x264 的 lookahead，
  在关闭时一次性冲出）在服务端被丢弃。
- **负载。** 50 个 ffmpeg 播放者播放一路 4 Mbit/s、720p25 的 RTMP 流 30 秒（RTMP 7 个、
  RTSP/TCP 7 个，RTSP/UDP、SRT、HTTP-FLV、HLS fMP4、HLS TS、LL-HLS 各 6 个），全部解码：每个
  播放者都没有错误行，各 748 帧（RTSP/UDP 和 SRT 从下一个关键帧开始，为 712–728 帧）。服务端在
  34 秒内用了约 1.8 秒 CPU，约为单核的 5%，涵盖接入、50 个出口和 3 个 HLS muxer；堆在 10 秒时
  为 25 MB，峰值 35–37 MB，播放者离开后为 34 MB，因为 HLS muxer 在 `IdleTimeout` 之前保留各自
  的分片窗口。
- **卡住的播放者。** 在一路 4 Mbit/s 的 720p 流上，每种出口各有一个播放者被 SIGSTOP 暂停
  30 秒：RTSP（TCP 与 UDP；UDP 的第一个对端一次性增加 4 MB，四轮暂停再杀掉也没有再增长）、SRT
  和 HTTP-FLV 的堆保持平稳；RTMP 播放者先被丢包、随后被断开（`MaxWriteQueue`、
  `IdleTimeout`）；HLS 每种格式增长一个分片窗口（4 Mbit/s 时约 9 MB）后保持不变，这是窗口本身，
  而不是卡住造成的。

没有与其它实现验证过的：codec id 12 的旧式 FLV H.265（上游 ffmpeg 7.1 把它识别为 unknown，
所以只有 fib 自己的测试覆盖）、RTMP 或 HTTP-FLV 上的 Opus（ffmpeg 5.1 和 7.1 都读不了 FLV 中
的 Opus）、WebSocket-FLV 升级之后的部分，以及 SRT 客户端对 ffmpeg listener 以外的 libsrt
服务端。
