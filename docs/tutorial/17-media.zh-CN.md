# 流媒体

[English](17-media.md) | [简体中文](17-media.zh-CN.md)

`media` 包是 fib engine 上的直播流媒体栈：RTMP 与 RTMPS、TCP 与 UDP 上的 RTSP、SRT、
HTTP-FLV 与 WebSocket-FLV、使用 fMP4 或 MPEG-TS 分片的 HLS 与 LL-HLS，以及面向 WebRTC 的
WHIP 和 WHEP。它们都在一个按路径保存流的 hub 中汇合，因此一种协议推上来的流可以用其他任何
协议播放——全程不做转码。本章介绍 hub 模型、一个同时服务多个协议族的 engine、各协议族各自的
服务端、转发客户端，以及“只转封装不转码”带来的限制。完整设计与配置见
[流媒体参考文档](../media.zh-CN.md)。

## hub

`hub.Hub` 是按路径组织的流注册表。每条流最多一个发布者、任意多个订阅者，协议之间互不
知情：RTMP 发布者推的流能通过 RTSP、SRT、HTTP-FLV、HLS 与 WHEP 播放，是因为每个出口订阅
的是 hub，而不是 RTMP。

- 一开始就知道有哪些 track 的接入端调用 `Hub.Publish(path, tracks, info)`，然后为每个访问
  单元调用 `Publisher.WritePacket`，在编码配置变化时调用 `Publisher.SetTracks`，离开时
  调用 `Publisher.Close`。
- 只能从收到的数据中得知 track 的接入端（SRT 的 MPEG-TS、拿到 metadata 之前的 RTMP）先调用
  `Hub.Reserve(path, info)`，这样第二个发布者会在握手阶段被拒绝，而不是先接受再丢弃。
- 出口用 `hub.Sink` 调用 `Hub.Subscribe(path, sink, opts)`：先 `OnTracks`，之后每个包一次
  `OnPacket`，结束时 `OnClose`。`SubscribeOptions.Wait` 让订阅可以早于发布者存在，并在重新
  推流后继续存活。

扇出是同步的：每条流一把互斥锁，`WritePacket` 在持锁状态下、在发布者的 goroutine（通常是
fib worker）里调用每个 sink 的 `OnPacket`。因此 sink 不能阻塞；它把数据排到自己的连接上，
消费者落后太多时丢弃，`hub.NewDropper` 就是这套策略的实现。`Config` 中的钩子是所有协议
共用的推流/播放鉴权入口：

```go
h := hub.New(hub.Config{
	OnPublish: func(path string, info hub.PublishInfo) error {
		if !strings.HasPrefix(path, "live/") {
			return errors.New("publish under live/ only")
		}
		return checkToken(info.Query) // info.Protocol, info.Remote, info.Query
	},
	OnSubscribe: func(path string, info hub.SubscribeInfo) error {
		return checkToken(info.Query) // info.Protocol, info.Remote, info.Query
	},
	MaxStreams:              1000,
	MaxSubscribersPerStream: 10000,
})
```

所有包都采用同一种统一格式，即 ISO BMFF（MP4）所用的形式（`media/av`）：
`av.Track.Config` 保存编码器的带外配置（AVC/HEVC decoder configuration record、AV1
配置、AudioSpecificConfig、OpusHead），`av.Packet.Data` 保存一个访问单元——H.264/H.265
为 AVCC，AV1 为一个不带 temporal delimiter 的 OBU 时间单元，AAC 为一个裸访问单元，Opus
为一个包。`DTS` 与 `PTS` 是 `time.Duration`，`Keyframe` 标记 IDR/IRAP 访问单元以及每个
音频包。每种协议都只负责在这套格式与自己的格式之间转换，因此协议两两之间不需要任何专用代码。

## 一个 RTMP、HTTP-FLV 与 HLS 服务端

一个 engine 和一个 hub 在 TCP 1935 上服务 RTMP，在 8080 上服务 HTTP-FLV、WebSocket-FLV、
HLS 与 LL-HLS。其中任一协议都可以发布 `live/` 下的路径，其他协议都能播放：

```go
package main

import (
	"errors"
	"log"
	"strings"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/hls"
	"github.com/lesismal/fib/media/httpflv"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/rtmp"
)

func main() {
	h := hub.New(hub.Config{
		OnPublish: func(path string, info hub.PublishInfo) error {
			if !strings.HasPrefix(path, "live/") {
				return errors.New("publish under live/ only")
			}
			log.Printf("publish %s over %s from %v", path, info.Protocol, info.Remote)
			return nil
		},
	})

	flv := httpflv.NewHandler(httpflv.Config{Hub: h, CORS: "*", Wait: 10 * time.Second})
	live := hls.NewHandler(hls.HandlerConfig{
		Hub:         h,
		Formats:     []hls.Format{hls.MPEGTS}, // ?format=ts as well as fMP4
		AllowOrigin: "*",
	})
	ll := hls.NewHandler(hls.HandlerConfig{
		Hub:         h,
		Prefix:      "/ll/",
		Muxer:       hls.Config{PartDuration: 500 * time.Millisecond}, // LL-HLS parts, fMP4
		AllowOrigin: "*",
	})

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		switch p := c.Request.URL.Path; {
		case strings.HasSuffix(p, ".flv"):
			flv.ServeHTTP(c) // HTTP-FLV, or WebSocket-FLV on an upgrade
		case strings.HasPrefix(p, "/ll/"):
			ll.ServeHTTP(c) // LL-HLS
		default:
			live.ServeHTTP(c) // HLS: fMP4, or MPEG-TS with ?format=ts
		}
	})

	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: "127.0.0.1:1935", Handler: rtmp.NewServer(rtmp.ServerConfig{Hub: h, PlayWait: 10 * time.Second})},
		fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(app)},
	)
	if err != nil {
		panic(err)
	}
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

用 ffmpeg 推流，或者用 OBS（服务器 `rtmp://127.0.0.1/live`，串流密钥 `s`）：

```sh
ffmpeg -re -i in.mp4 -c copy -f flv rtmp://127.0.0.1/live/s
```

然后用以下任一地址播放 `live/s`：

| 协议 | URL |
| --- | --- |
| RTMP | `rtmp://127.0.0.1/live/s` |
| HTTP-FLV | `http://127.0.0.1:8080/live/s.flv` |
| WebSocket-FLV | `ws://127.0.0.1:8080/live/s.flv` |
| HLS，fMP4 | `http://127.0.0.1:8080/live/s/index.m3u8` |
| HLS，MPEG-TS | `http://127.0.0.1:8080/live/s/index.m3u8?format=ts` |
| LL-HLS | `http://127.0.0.1:8080/ll/live/s/index.m3u8` |

`index.m3u8` 是一个指向 `media.m3u8` 的多变体（multivariant）playlist；handler 的
`Formats` 里没有的格式会得到 404，所以这里 `?format=ts` 在 `live` 上可用，在 `ll` 上不可用。
`HandlerConfig` 不设 `PlayWait` 时，发布者到来之前的 playlist 请求同样是 404。

路径在各协议中映射方式相同：`rtmp://host/app/stream` 是 `app/stream`，
`/app/stream.flv`、`/app/stream/index.m3u8` 以及 SRT 的 stream ID `app/stream` 也一样。
HTTP-FLV 播放器是 flv.js 和 mpegts.js；HLS 播放器是 hls.js、Safari 和 ffmpeg。这些 HTTP
handler 就是普通的 fib/http handler，因此 HTTP/2、HTTP/3 和 TLS 能像服务其他 handler 一样
服务它们。

## TCP 与 UDP 上的 RTSP

从这里开始的片段都是给上面的程序添加协议：它们使用其中的 `engine` 和 `h`，放在
`engine.Run()` 之前——`AddBinding` 对尚未运行的 engine 和已在运行的 engine 都有效。

RTSP 需要一个独立的 binding，再加一对所有会话共用的 UDP 端口。`ListenUDP` 在同一个 engine
上绑定 RTP/RTCP 端口，因此 `rtsp://…/live/s` 默认用 TCP 交织的 RTP 播放，客户端要求时改用
UDP：

```go
rtspServer := rtsp.NewServer(rtsp.ServerConfig{Hub: h, PlayWait: 10 * time.Second})
if _, err := engine.AddBinding(fib.Binding{Addr: "127.0.0.1:8554", Handler: rtspServer}); err != nil {
	panic(err)
}
// RTP/RTCP over UDP on :8000 and :8001 (ServerConfig.UDPRTPAddr, UDPRTCPAddr).
if err := rtspServer.ListenUDP(engine); err != nil {
	panic(err)
}
```

用 `ffmpeg -re -i in.mp4 -c copy -f rtsp -rtsp_transport tcp
rtsp://127.0.0.1:8554/live/s` 推流，用 `ffplay rtsp://127.0.0.1:8554/live/s` 播放（UDP 则加上
`-rtsp_transport udp`）。`ServerConfig.Password` 配合 `AuthMethods` 开启
Digest（或 Basic）鉴权；`PlayWait` 让播放器在发布者离开后继续等待；对 UDP 播放器，
`MaxPlayerLoss` 会关闭 RTCP 报告显示丢包过多的客户端。

## SRT

SRT 是一个 UDP binding，handler 就是服务端；每个对端地址是一条 fib 连接，服务端读取 SRT 的
访问控制 stream ID 来决定推流还是播放。双方都设置 passphrase 时用 AES-CTR 加密：

```go
srtServer, err := srt.NewServer(srt.ServerConfig{
	Hub:        h,
	PlayWait:   10 * time.Second,
	Passphrase: "correct-horse-battery", // optional, 10 to 79 bytes; callers must send the same
	MaxConns:   100,
})
if err != nil {
	panic(err)
}
if _, err := engine.AddBinding(fib.Binding{Network: "udp", Addr: "127.0.0.1:9000", Handler: srtServer}); err != nil {
	panic(err)
}
```

服务端设置了 passphrase 时，每个 caller 都必须带上同样的值，不带的会在握手阶段被拒绝。
ffmpeg 需要带 libsrt 编译（`ffmpeg -protocols | grep srt`）：

```sh
ffmpeg -re -i in.mp4 -c copy -f mpegts \
  'srt://127.0.0.1:9000?streamid=#!::r=live/s,m=publish&passphrase=correct-horse-battery'
ffplay 'srt://127.0.0.1:9000?streamid=live/s&passphrase=correct-horse-battery'
```

发布者在握手期间就预留路径，因此忙碌的路径会
在传输任何媒体之前以 `RejectConflict` 拒绝。SRT 在这里只承载 MPEG-TS，AV1 与 G.711 不在
其中。

## RTMPS 与 TLS

RTMPS 就是用 TLS handler 包一层 RTMP 服务端；HTTP handler 的 HTTPS 和 HTTP/3 就是普通的
fib binding。hub 侧没有任何变化：

```go
serverTLS, _, err := tlstest.Configs() // real certificates in production
if err != nil {
	panic(err)
}
if _, err := engine.AddBinding(fib.Binding{
	Addr:    "127.0.0.1:1936",
	Handler: fibtls.NewServer(serverTLS, rtmp.NewServer(rtmp.ServerConfig{Hub: h})),
}); err != nil {
	panic(err)
}
```

## WHIP 与 WHEP

WHIP 接入与 WHEP 出口是 fib/http handler，背后是共享一个 UDP `Mux` 的 WebRTC
PeerConnection。`rtc.Config.WebRTC` 是每个会话所用的 `webrtc.Configuration`，因此 Mux、
证书和 `ICELite` 都放在这里：

```go
mux, err := webrtc.NewMux(engine, webrtc.MuxConfig{Addr: "127.0.0.1:8080"})
if err != nil {
	panic(err)
}
rtcServer, err := rtc.NewServer(rtc.Config{
	Hub:         h,
	WebRTC:      webrtc.Configuration{Mux: mux, ICELite: true},
	AllowOrigin: "*", // a browser page on another origin
	MaxSessions: 10000,
})
if err != nil {
	panic(err)
}

router := fibhttp.NewRouter()              // served by the HTTP binding
router.Handle("/whip/*", rtcServer)        // POST publishes
router.Handle("/whep/*", rtcServer)        // POST plays
router.Handle("/rtc/session/*", rtcServer) // PATCH (trickle) and DELETE
```

`rtcServer.ServeHTTP` 处理上述全部请求；这些前缀分别由 `rtc.Config.WHIPPrefix`、
`WHEPPrefix` 和 `ResourcePrefix` 决定。WHEP 承载视频和 Opus，并丢弃 AAC：以 H.264+AAC 发布的流播放到浏览器时没有声音，
除非用 Opus 重新发布。底层 WebRTC 见 [16-webrtc.zh-CN.md](16-webrtc.zh-CN.md)。

## 拉流与推流转发

每个客户端都通过 fib engine（必须已在运行）拨号并用回调上报，没有任何一个会阻塞 worker。
拉流转发把远端流播放进 hub 的某个路径，推流转发把 hub 的路径发布到远端服务器。重连由应用
负责：`done` 回调告知转发已经开始（nil）或无法开始的原因，`ClientConfig.OnClose`（以及
`PullConfig.OnClose`）告知运行中的转发已结束，重新启动只是一次调用。每个调用也会返回对应
的转发对象，调用它的 `Close` 即可停止。

```go
// RTMP: pull another server's stream into "live/cam", and push "live/cam" on,
// waiting up to 30 s for a publisher (hub.SubscribeOptions.Wait).
rtmp.Pull(engine, h, "rtmp://origin.example.com/live/cam", "live/cam", rtmp.ClientConfig{},
	func(err error) { log.Println("pull started:", err) })
rtmp.Push(engine, h, "live/cam", "rtmp://edge.example.com/live/cam?token=t", 30*time.Second,
	rtmp.ClientConfig{}, func(err error) { log.Println("push started:", err) })

// HTTP-FLV or WebSocket-FLV from a CDN (https:// or wss://) into "live/cdn".
httpflv.Pull(httpflv.PullConfig{Engine: engine, OnClose: func(err error) { log.Println("pull ended:", err) }},
	"https://cdn.example.com/live/cam.flv?token=t", h, "live/cdn")
```

其他协议的模式相同：`srt.Publish`/`srt.Play`、`rtsp.PublishFromHub`/`rtsp.PlayToHub`，以及
`rtc.PushFromHub`/`rtc.PullToHub`。不接 hub 时同一批客户端就是普通播放器与发布器：
`rtmp.Play` 播放进任意 `hub.Sink`，`rtmp.Publish` 返回一个本身就是 sink 的 `ClientConn`，
`rtc.Pull`/`rtc.Push` 用 `OnTracks`/`OnPacket` 回调。

## 坑与限制

- **没有转码。** 出口无法承载的 track 只会在该出口缺失：WHEP 丢弃 AAC 和 MP3，SRT 与
  HLS-TS 丢弃 AV1 和 G.711，HLS fMP4 丢弃 G.711，只有 RTMP/HTTP-FLV 承载 MP3，早于 Enhanced RTMP 的 RTMP/HTTP-FLV 播放器读不了 Opus。推流时
  要用播放器需要的编码——通常 H.264 同时配 AAC（给 HLS/RTMP）和 Opus（给 WebRTC）。见
  [流媒体参考文档](../media.zh-CN.md#各协议支持的编码)中的编码矩阵。
- **每条路径只有一个发布者。** 第二个发布会得到 `hub.ErrStreamBusy`，RTMP 与 SRT 则在握手
  阶段就被拒绝。发布者身份、token 和路径都由 `hub.Config.OnPublish` 统一看到，这里也是
  统一做限制的地方。
- **sink 不能阻塞。** 扇出在发布者的 worker 上、持流互斥锁运行；慢 sink 会拖慢所有其他
  订阅者。把数据排到连接上并在超过上限时丢弃（`hub.NewDropper` 的做法），否则发布者被拖住，
  所有播放器一起遭殃。
- **先连上的播放器需要等待。** `Wait`/`PlayWait` 为 0 时，播放一条还没有发布者的流会得到
  404 或 `StreamNotFound`。设置该值，观众、HLS playlist 或推流转发才能在编码器重连时保住
  位置。
- **新订阅者从最后一个关键帧开始**，来自 GOP 缓存，而不是直播边缘。这是合理默认；
  `SubscribeOptions.NoGOPCache` 用启动等待换低延迟，LL-HLS（`PartDuration`）则是 HLS 的
  低延迟模式。
- **重推可能重置包时间戳。** 出口会让播放器的时间戳继续前进，并在 HLS 中标记 discontinuity；
  不要假设发布者的时钟在重连前后单调。
- **UDP 上的 RTSP 需要端口可达。** RTP/RTCP 端口对（默认 8000/8001）独立于 RTSP 端口；
  不可达时客户端会回退到 TCP 交织或直接失败。
- **加密参数必须一致。** SRT passphrase 为 10 到 79 字节，双方必须相同；RTMPS、HTTPS 和
  TLS 后的 WHEP 需要真实证书，而不是 `tlstest`。
- **内存有界，不落盘。** HLS 分片与 GOP 缓存都在内存中，分别由 `hls.Config.MaxBytes`
  （每个 muxer 64 MiB）与 `hub.Config.GOPCacheBytes`（每条流 16 MiB）约束；长时间直播和
  大量观众需要调整这些上限，或者换更大的机器。
- **每条流都是直播。** 没有录制、DVR 或视频点播：hub 只保存当前流和它的 GOP 缓存，不落盘。
  `media/mp4` 包可以读取文件的 track 与 sample table（`media/fmp4` 则生成 fragmented MP4
  分片），因此应用可以把文件当作直播样本喂进 hub，或自己切 VOD 分片——VOD 服务端本身需要
  你自己实现。
- **ffmpeg 版本有影响。** Enhanced RTMP（H.265、AV1、Opus）、AV1 RTP 和 LL-HLS 的支持
  随版本不同；`-c copy` 只有在输入的编码能被目标协议族承载时才有效。

## 相关文档

- [流媒体：设计、用法与边界](../media.zh-CN.md) —— hub、各协议配置、编码矩阵以及
  ffmpeg/Chromium 兼容性矩阵
- [WebRTC](16-webrtc.zh-CN.md) —— WHIP 与 WHEP 之下的 PeerConnection 与 Mux
- [WebSocket](09-websocket.zh-CN.md) —— WebSocket-FLV 使用的 RFC 6455 层
- [HTTP/1.x](06-http1.zh-CN.md) —— HTTP-FLV 与 HLS 依赖的流式响应
- [引擎与配置](02-engine.zh-CN.md) —— binding、`AddBinding` 与 UDP binding
- [fib-examples](https://github.com/lesismal/fib-examples) —— 每种协议的可运行服务端以及
  播放它们的浏览器页面
