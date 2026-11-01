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
| HTTP-FLV、WebSocket-FLV | `media/httpflv` | 播放 | — | 一个永不结束的 FLV 文件，走 HTTP/1.1、HTTP/2 或 HTTP/3，或者把同样的字节放进二进制 WebSocket 消息（也支持 RFC 8441 与 RFC 9220 的 extended CONNECT）；供 flv.js、mpegts.js 和 ffmpeg 播放 |
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
  编码器重连期间的 HLS 播放列表、等待源流的转推，都是这样保住位置的。包的时间是发布者自己的
  时间，重新推流可能让它从头开始。

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

**上限。** 流数由 `MaxStreams` 限制，订阅数由 `MaxSubscribersPerStream` 限制（超过任一个返回
`ErrLimit`），缓存由 `GOPCacheBytes` 限制，每个 sink 的队列由 sink 自己限制。既没有发布者也
没有订阅的流会从流表中移除。`Hub.Streams` 返回每路流的快照：路径、track、发布者、订阅者数，
以及流入的字节数与包数。

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
`ws-flv`、`hls`，RTMP 的转发是 `rtmp-pull`/`rtmp-push`）、远端地址，以及 URL 的 query，
token 通常放在这里。返回错误即拒绝这次推流或播放。每个协议还有自己的钩子，处理只有它才知道
的信息：

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
执行），`httpflv.Config` 有 `OnPlay`，在 hub 的 `OnSubscribe` 之后执行。HLS 对每路流的每种
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
| RTSP、WHIP（RTP） | depacketizer 把 access unit 重组为 AVCC，从 SDP 的 `sprop-*`/`config=` 取配置，并跟随带内发送的参数集，参数集改变配置时替换 track；RTP 只有显示时间，解码时间由 SPS 给出的重排深度恢复；WHIP 按包到达的时间打时间戳 | packetizer 按 MTU 切分 access unit（STAP-A/FU-A、AP/FU），在每个关键帧前发送参数集，序号与时间戳从随机值开始；RTSP 另外为所有 track 在同一时钟上发送 RTCP sender report |
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
```

- 拉流转发在得知远端的 track 后发布路径（`rtmp.Pull` 会先预占路径，所以路径已被占用时立即以
  `hub.ErrStreamBusy` 失败），把 track 的变化以 `SetTracks` 传下去，远端流结束时取消发布。
  发布被拒绝会关闭客户端。
- 转推从流的第一组 track 开始，远端跟不上的包按 `hub.NewDropper` 的方式丢弃，流结束时转推
  也结束。RTSP 在一个会话内无法重新 announce track，所以 track 变化会关闭 RTSP 转推；WHIP
  转推在 PeerConnection 连上之前暂存最近一个 GOP。
- 重连由应用负责：`done`/`OnClose` 告知转发已结束，重新启动只需一次调用。转推的 `Wait` 让它
  在源流重新推流期间保住位置。
- 不用 hub 时，这些客户端就是普通的播放器和推流端：`rtmp.Play(engine, url, config, sink,
  ready)` 播放到任意 `hub.Sink`，`rtmp.Publish` 返回的 `ClientConn` 本身就是一个
  `hub.Sink`，`rtsp.Play`/`rtsp.Publish` 和 `rtc.Pull`/`rtc.Push` 接受 `OnTracks` 与
  `OnPacket` 回调并提供 `WritePacket`，`srt.Dial` 给出一个原始会话，传输应用写入的任意字节。
- 走 UDP 的 RTSP 客户端为每路流在自己的 engine 上添加一对 UDP binding；SRT 客户端用
  `Engine.OpenUDPPeer` 打开自己的 binding；WHIP/WHEP 客户端打开一个自带端口的
  PeerConnection，除非 `ClientConfig.WebRTC` 指定了 Mux 或 engine。

## 配置与限制

除了 `Hub`（以及 `rtc.Config` 中带 Mux 或 engine 的 `WebRTC`），所有字段都是可选的；零值取
下面的默认值。处处都有超时，对端能填满的每个缓冲区都有上限。

**hub.Config**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `GOPCacheBytes` | 16 MiB | 每路流的 GOP 缓存；负数表示不缓存 |
| `MaxStreams`、`MaxSubscribersPerStream` | 不限 | 超过时返回 `ErrLimit` |
| `OnPublish`、`OnUnpublish`、`OnSubscribe` | 无 | 认证与观察，调用时不持有 hub 的锁 |

**rtmp.ServerConfig**（以及字段大多相同的 `ClientConfig`）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `HandshakeTimeout` | 10 s | 从连接到开始推流或播放（客户端另有 `DialTimeout`，10 s） |
| `IdleTimeout` | 30 s | 不发任何数据的发布者，或输出不动的播放者 |
| `PlayWait` | 0 | 播放等待缺席发布者的时长；0 表示立即回 `StreamNotFound` |
| `MaxMessageSize` | 8 MiB | 对端可发送的最大消息 |
| `ChunkSize`、`WindowAckSize` | 4096、2,500,000 | 服务端写出的 chunk 大小，以及要求客户端使用的确认窗口 |
| `WriteQueueLimit`、`MaxWriteQueue` | 2 MiB、16 MiB | 超过前者丢包（视频丢到下一个关键帧），超过后者断开 |
| `Encoder.LegacyHEVC` | false | 用 codec id 12 而不是 Enhanced RTMP 写 H.265 |
| `OnConnect`、`OnPublish`、`OnPlay`、`OnClose` | 无 | 钩子；`OnPublish` 可以返回一个 sink，从而让服务端不用 hub 也能运行 |

**httpflv.Config**：`Path`（`/app/stream.flv` 播放 `app/stream`）、`OnPlay`、`CORS`、
`Wait`（0 表示缺席的流回 404）、`WriteQueueLimit` 与 `MaxWriteQueue`（2 MiB 与 16 MiB；在
HTTP/2 和 HTTP/3 上，积压指流控压住超过 64 KiB 的数据，且只有 HTTP/1 的播放者会被断开）、
`IdleTimeout`（30 s）、`Encoder`，以及 `WebSocket`（零值接受任何 origin）。

**rtsp.ServerConfig**

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `UDPRTPAddr`、`UDPRTCPAddr` | `:8000`、`:8001` | `ListenUDP` 绑定的 RTP/RTCP 端口对，所有会话共用 |
| `Password`、`Realm`、`AuthMethods` | 无、`fib`、Digest | 认证；Basic 以明文发送密码。Digest 的 nonce 无状态，有效期 5 分钟 |
| `SessionTimeout`、`IdleTimeout` | 60 s、60 s | 会话没有请求、RTCP 或（录制时）RTP 的时长；连接没有会话的时长 |
| `MaxHeaderBytes`、`MaxBodyBytes` | 16 KiB、256 KiB | 请求的上限 |
| `MaxQueuedBytes` | 4 MiB | 播放者积压超过它就丢包；超过两倍就关闭 |
| `MTU`、`SenderReportInterval` | 1400、5 s | RTP 包大小，RTCP sender report 间隔 |
| `ReorderSize`、`ReorderDelay` | 128 个包、100 ms | 每路经 UDP 录制的流的重排缓冲区 |
| `OnRequest` | 无 | 查看每个已认证的请求，也可以代为应答 |

`rtsp.ClientConfig` 有 `Transport`（TCP 或 UDP）、`DialTimeout`、`RequestTimeout` 与
`ReadTimeout`（各 10 s）、`KeepAlive`（服务端给出的会话超时的一半，最多 30 s）、`MTU`、
`MaxQueuedBytes`，以及重排缓冲区的大小和延迟。

**srt.ServerConfig**（以及 `ClientConfig`）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `Latency` | 120 ms | 请求的 TSBPD 延迟；取两端中较大的 |
| `Passphrase`、`PBKeyLen` | 无、16 | 加密：10 到 79 字节；密钥长度 16、24 或 32。设置了口令时，拒绝没有口令的 caller，反之亦然 |
| `HandshakeTimeout`、`PeerIdleTimeout` | 3 s、5 s | 握手超时，以及对端不发任何数据的超时 |
| `FlowWindow`、`SendBufferPackets` | 8192、8192 个包 | 接收缓冲区与对端在途包数；未确认加待发的包数 |
| `MaxSendBacklog` | 4 MiB | 播放者未确认的载荷超过它就丢包 |
| `PlayWait` | 0 | 播放者等待缺席流的时长；0 表示以 `RejectNotFound` 拒绝 |
| `MaxConns` | 不限 | 会话数；超过时 caller 收到 `RejectOverload` |
| `OnConnect`、`OnSession`、`ParseStreamID` | 无 | 认证、把会话从 hub 手里接管过来、自定义 stream ID 语法 |
| `Muxer`、`Demuxer` | MPEG-TS 默认值 | 每 100 ms 发送一次 PAT/PMT、每 40 ms 一个 PCR，PES 上限 4 MiB，得知 track 期间最多暂存 256 个包 |

载荷最多 1456 字节；播放者每个 SRT 包收到 7 个 TS 包（1316 字节）。

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
| `NoGOPCache`、`MaxHeldBytes` | false、8 MiB | 观众从缓存的 GOP 开始，连接期间暂存它 |
| `MaxLoss` | 0.25 | 观众报告的丢包率超过它就丢包（视频丢到下一个关键帧） |
| `MTU`、`ReorderSize`、`ReorderDelay` | 1200、128、100 ms | RTP 包大小，以及 WHIP track 的重排缓冲区 |
| `PLIInterval`、`KeyFrameInterval` | 500 ms、0 | 两次关键帧请求的最小间隔；定期请求 |
| `MaxOfferBytes` | 64 KiB | offer 或 PATCH body 的上限 |
| `OnKeyFrameRequest` | 无 | 观众对一路不由本服务端 WHIP 会话发布的流请求关键帧 |

`rtc.ClientConfig` 有 `Timeout`（15 s）、`Token`、`Header`、`MTU`、`MaxHeldBytes`、重排
缓冲区、`PLIInterval` 和 `KeyFrameInterval`。

核心内部的固定上限：

| 对象 | 上限 |
| --- | --- |
| 从 RTP 重组的一个 access unit | 8 MiB（`DepacketizerConfig.MaxAccessUnit`） |
| 等待所有已声明 track 收到 sequence header 时暂存的 FLV 包 | 512 个包或 1 秒 |
| AMF 嵌套 | `amf.MaxDepth`；计数受剩余字节数约束 |
| MPEG-TS PES | 4 MiB，更大的丢弃 |
| SRT 收发缓冲区 | `FlowWindow` 与 `SendBufferPackets` |
| 每个 HLS Muxer 的媒体数据 | `MaxBytes`，先淘汰旧分片 |

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
- **HTTP-FLV**：拉流客户端（`rtmp.Pull` 从 RTMP 转发；拉 HTTP-FLV 需要一个能把响应 body 流式
  送进 `flv.Demuxer` 的 fib/http 客户端，留待以后）；提供录制文件。
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
  停止）；基于 RTCP sender report 的音视频同步（WHIP 按到达时间打时间戳，各 track 之间的
  偏差不超过网络抖动）。
- **编码工具**：H.264 的 PPS、slice header、SVC/MVC 与 subset SPS；H.265 SPS 中位深和重排之后
  的部分（因此没有帧率和宽高比），以及 SHVC/MV-HEVC；AAC 的 program config element、codec
  package 中的 LATM、含多个 raw data block 的 ADTS 帧、CRC 校验；OpusTags 与 multistream 的
  self-delimited 分帧。

## 验证方式

每个 package 都有自己的测试并在 CI 中运行，不需要 ffmpeg：sans-IO 核心彼此对接、或对接抓取的
字节（`testdata/` 中由 ffmpeg 5.1 和 7.1 为每种编码写出的样本，其中包括 H.264 的 4:4:4、
4:2:2 10-bit、隔行和带裁剪的流，以及 H.265 Main 10），每个接收不可信字节的解析器都有 fuzz
目标，服务端和客户端在 fib engine 上经环回地址对接。整个 `media` 目录在下面的 Linux 容器中
通过 `go test -race`。

互通测试用的是真实实现，在 Docker 中经环回地址进行：ffmpeg 5.1（Debian bookworm，libx264、
libx265、libopus、libsrt）、ffmpeg 7.1（Debian trixie，能读 Enhanced RTMP 的 H.265 和 AV1），
以及 headless Chromium。测试程序是仓库之外的临时模块，因为 fib 的 `go.mod` 不引入任何依赖，
仓库内的测试也必须在没有 ffmpeg 的情况下通过。

| 方面 | 对端 | 验证内容 |
| --- | --- | --- |
| RTMP、HTTP-FLV | ffmpeg 5.1、7.1 | ffmpeg 推 H.264+AAC（5.1），以及经 Enhanced RTMP 推 H.265+AAC 和 AV1+AAC（7.1）；ffprobe 与 ffmpeg 经 RTMP 和 HTTP-FLV 播放回来，四个播放器同时进行，DTS 单调；fib 的客户端播放并写成 FLV 文件，ffmpeg 能解码；fib 的客户端向 ffmpeg 的 `-listen 1` 推流、并从中播放（5.1 上用 H.264，7.1 上用 H.265）；RTMPS 推流与拉流 |
| RTMP、HTTP-FLV 慢播放者 | ffmpeg 5.1 | 一个 RTMP 和一个 HTTP-FLV 播放者在 6 Mbit/s 的 720p 流上停止读取，被断开，旁边正常的播放者照常解码；服务端堆内存全程在 11 到 20 MB 之间 |
| RTSP | ffmpeg 5.1、7.1 | ffmpeg 经 TCP 和 UDP 录制；ffprobe 与 ffmpeg 经 TCP 和 UDP 播放：H.264+AAC、H.265+Opus、H.264+PCMA；经 fib 客户端转发（TCP 与 UDP，H.264 与 H.265）；fib 客户端向 ffmpeg 的 listen 模式推流（TCP 与 UDP）；Digest 与 Basic，没有凭据和凭据错误都被拒绝；一个播放者卡住而另一个正常解码，堆内存平稳 |
| SRT | ffmpeg 5.1、7.1（libsrt） | ffmpeg 推流并播放回 H.264+AAC 与 H.265+Opus，不加密以及 pbkeylen 为 16、24、32，也经过一个双向丢弃 2–10% 数据报的中继，解码干净；fib 的客户端向 listener 模式的 ffmpeg 推流、并从中拉流 |
| MPEG-TS | ffmpeg 5.1、7.1 | ffmpeg 生成的 H.264+AAC、H.265+Opus 和纯 AAC 流能解复用，重新复用后解码没有警告 |
| fMP4 | ffmpeg 5.1、7.1 | H.264+AAC、H.265+AAC、H.264+Opus 的 init 分片和 fragment，来源包括原始码流和 ffmpeg 自己带 B 帧的 fMP4，解码没有错误，重新复用后每个包的时间戳和关键帧标志保持不变 |
| HLS | ffmpeg 5.1、7.1 | fMP4：H.264+AAC、H.265+AAC、H.264+Opus、纯 AAC，以及 500 ms part 的 LL-HLS，播放 10 秒没有错误；同编码重新推流后跨 discontinuity 继续播放；播放列表检查程序确认 target duration、序号、discontinuity、part、blocking reload 和 preload hint 均符合规范。MPEG-TS：RTMP 推上来的 H.264+AAC（5.1）和 H.265+AAC（7.1）从 TS 变体播放，普通与 LL-HLS 均可 |
| 所有接入 × 所有出口 | ffmpeg 5.1、7.1 | ffmpeg 经 RTMP、RTSP/TCP、RTSP/UDP 和 SRT 推流，编码为 H.264+AAC、H.265+AAC、H.264+Opus、H.264+PCMA，每路流再经 RTMP、RTSP/TCP、RTSP/UDP、SRT、HTTP-FLV、HLS fMP4、HLS TS 和 LL-HLS 播放：复制并解码 6 秒没有错误，编码与画面尺寸正确，120–180 个视频帧，DTS 单调，SRT 和 HLS 上去掉了 G.711（只有视频）。两个版本上，ffmpeg 能推、能读的组合全部通过；其余是 ffmpeg 本身做不到的（FLV 中的 Opus 无论收发、5.1 上的 Enhanced RTMP、MPEG-TS 中的 G.711） |
| WHIP、WHEP | ffmpeg 5.1、fib 的客户端 | ffmpeg 经 RTSP 推 H.264+Opus；fib 的 WHEP 客户端从 WHEP 端点把它拉进一个路径，fib 的 WHIP 客户端把它推到 WHIP 端点成为另一个路径；ffmpeg 经 RTSP 播放两者，解码没有错误 |
| WHIP、WHEP | headless Chromium | WHEP 播放 ffmpeg 的 H.264+Opus：连接成功，640×360，解码 244 帧，没有丢包；Chromium 用虚拟摄像头和麦克风经 WHIP 推流（OpenH264 Constrained Baseline 与 Opus），ffmpeg 随后经 RTSP 播放并解码，没有错误 |
| 上面的示例 | ffmpeg 5.1、7.1，fib 的 WHEP 客户端 | 原样编译；ffmpeg 同时经 RTMP 和 SRT 推 H.264+AAC、经 RTSP 推 H.264+Opus。RTMP 的流经 RTMP、TCP 与 UDP 的 RTSP、SRT、HTTP-FLV、HLS fMP4 与 TS、LL-HLS 播放；SRT 的流经 HLS 和 RTMP 播放；RTSP 的流经 SRT 和 HLS 播放；ffmpeg 对每一路解码 4 秒没有错误，fib 的 WHEP 客户端收到 RTSP 流的 H.264 与 Opus（5 秒约 400 个包）。WebSocket-FLV 的升级请求得到 101，向 `live/` 之外推流被钩子拒绝 |

没有与其它实现验证过的：codec id 12 的旧式 FLV H.265（上游 ffmpeg 7.1 把它识别为 unknown，
所以只有 fib 自己的测试覆盖）、RTMP 或 HTTP-FLV 上的 Opus（ffmpeg 5.1 和 7.1 都读不了 FLV 中
的 Opus）、WebSocket-FLV 升级之后的部分，以及 SRT 客户端对 ffmpeg listener 以外的 libsrt
服务端。
