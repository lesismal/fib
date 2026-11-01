# WebRTC：设计、用法与边界

[English](webrtc.md) | [简体中文](webrtc.zh-CN.md)

本文介绍 fib 的 WebRTC 实现：各部分如何组织，服务端和客户端怎么用，支持哪些功能、有意不做
哪些，以及如何与其它实现互通验证。可运行的程序在
[fib-examples](https://github.com/lesismal/fib-examples) 中：
[`webrtc`](https://github.com/lesismal/fib-examples/tree/main/webrtc) 是一个网页，服务端把它
data channel 上的消息和摄像头画面原样送回；
[`webrtc/sfu`](https://github.com/lesismal/fib-examples/tree/main/webrtc/sfu) 把一个发布者的
音视频转发给多个观看者，两者都经共用的
[`webrtc/whip`](https://github.com/lesismal/fib-examples/tree/main/webrtc/whip) package 应答
WHIP 与 WHEP 的 offer。不用浏览器的有：
[`webrtc/datachannel`](https://github.com/lesismal/fib-examples/tree/main/webrtc/datachannel) 和
[`webrtc/media`](https://github.com/lesismal/fib-examples/tree/main/webrtc/media) 连接两个 Go
PeerConnection，[`webrtc/turnserver`](https://github.com/lesismal/fib-examples/tree/main/webrtc/turnserver)
是独立的 TURN 服务器，支持 UDP、TCP 与 TLS，[`webrtc/stun`](https://github.com/lesismal/fib-examples/tree/main/webrtc/stun)
是 STUN 客户端与服务端，[`tls/dtls`](https://github.com/lesismal/fib-examples/tree/main/tls/dtls) 和
[`udp/openpeer`](https://github.com/lesismal/fib-examples/tree/main/udp/openpeer) 分别单独演示 DTLS
与 `Engine.OpenUDPPeer`。

对端实现在 [`webrtc`](../webrtc) 中，下面各层协议各自独立成 package：
[`webrtc/stun`](../webrtc/stun)、[`webrtc/ice`](../webrtc/ice)、
[`webrtc/turn`](../webrtc/turn) 及 [`webrtc/turn/turnserver`](../webrtc/turn/turnserver)、
[`webrtc/sdp`](../webrtc/sdp)、[`webrtc/sctp`](../webrtc/sctp)、
[`webrtc/srtp`](../webrtc/srtp)、[`webrtc/rtp`](../webrtc/rtp)，DTLS 1.2 在
[`tls`](../tls)（`dtls*.go`）中。全部基于标准库自行实现，不依赖 pion 或其它任何模块。

## 已支持的范围（概览）

| 方向 | 内容 |
| --- | --- |
| PeerConnection | 沿用 RTCPeerConnection 的模型：`CreateOffer`（支持 ICE restart）、`CreateAnswer`、`SetLocalDescription`、`SetRemoteDescription`、`AddICECandidate`/`OnICECandidate`，connection、ICE、gathering、signaling 四种状态及其回调，任一端发起的重新协商，`GatheringComplete`，`SelectedCandidatePair`，以及像 WHIP、WHEP 那样一次完成整个交换的 `Offer`/`Answer` 辅助函数；max-bundle 与 rtcp-mux，所有内容走同一个传输 |
| Mux | fib engine 的一个 UDP 端口，任意多个 PeerConnection 共用，这是服务端需要的形态；没有 Mux 的 PeerConnection 自己开一个端口，就像客户端那样 |
| ICE | RFC 8445 的 full 与 lite agent、两种角色、角色冲突、frozen/waiting 状态的 pair、按节奏发送的普通检查与 triggered check、peer-reflexive candidate、regular nomination、restart；trickle（RFC 8838）；consent freshness（RFC 7675）；向 STUN 服务器取 server-reflexive candidate；向 TURN 服务器取 relay candidate |
| STUN | RFC 8489 消息原地解码，STUN、ICE、TURN 的属性，短期与长期凭据的 MESSAGE-INTEGRITY、MESSAGE-INTEGRITY-SHA256、FINGERPRINT，以及按 RFC 8489 重传的客户端事务 |
| TURN | 客户端（基于 UDP、TCP 或 TLS 的 RFC 8656：allocation、刷新、permission、channel，处理 300/401/438），以及可并发调用的服务端核心：长期凭据、无状态 nonce、按服务器、用户和 allocation 的配额、拒绝内网地址的转发策略、事件与统计、原地修改限制与凭据、drain；`turnserver` 把服务端跑在 fib engine 上，支持 UDP、TCP 与 TLS。PeerConnection 可经 UDP、TCP 与 TLS 访问 TURN 服务器 |
| DTLS | DTLS 1.2（RFC 6347）与 use_srtp（RFC 5764），sans-IO，与 fib 的 TLS 1.2 共用握手代码：ECDHE 搭配 AES-GCM 和 ChaCha20-Poly1305，ECDSA、Ed25519、RSA 密钥，X25519、P-256、P-384，按 MTU 切分并重传的 flight，64 条记录的防重放窗口，无状态 cookie，按 SDP fingerprint 校验证书 |
| SRTP | 由 DTLS-SRTP 派生密钥的 SRTP 与 SRTCP（RFC 3711）：AES_CM_128_HMAC_SHA1_80 与 _32、AEAD_AES_128_GCM 与 AEAD_AES_256_GCM（RFC 7714），每个 SSRC 各自的 rollover counter 与防重放窗口；某个 SSRC 一旦见过，处理每个包都不再分配内存 |
| RTP 与 RTCP | RTP 原地解析，RFC 8285 两种格式的头部扩展，MID、RID、abs-send-time、transport-wide 序号与 audio level 的辅助函数；RTCP 的 SR、RR、SDES、BYE、APP、NACK、PLI、FIR、REMB 与 transport-cc 反馈，遍历复合包不分配内存 |
| Data channel | 跑在 DTLS 上的 SCTP（RFC 8261）与 DCEP（RFC 8832）：有序与无序，可靠，或按重传次数、存活时间受限（RFC 3758），negotiated channel，用 stream reset 关闭（RFC 6525），消息默认最大 256 KiB，带低水位回调的 buffered amount |
| 媒体 | RTP 层面的 unified plan transceiver：应用写入的 `TrackLocal` 可由任意多个 PeerConnection 发送，各用自己的 SSRC、payload type 和扩展 id；`TrackRemote` 按 MID、SSRC 或 payload type 识别，simulcast 每个 RID 一个；发送 sender report 与 receiver report，转交 PLI 和 FIR；可协商 Opus、VP8、VP9、H.264、AV1 |
| SDP | 逐行保存 description，序列化后与原文逐字节一致；JSEP 每个属性都有类型化的读取与构造函数；测试中包含 Chrome、Firefox、Safari 的 offer 与 answer |
| 互通验证 | pion（每一层，以及整个 PeerConnection）、DTLS 对 OpenSSL、通过 fib-examples 网页对 Chromium；见[验证方式](#验证方式) |

## 架构

### sans-IO 分层

每层协议都是一个状态机，自己不碰 socket、不起 goroutine、也不读时钟。调用方把收到的数据和
当前时间交给它，问它下次什么时候需要时间，再给它一个回调用来发送。正因为如此，一个
PeerConnection 才能用一把锁、一个定时器驱动所有层；测试也才能让两个对端在一个会丢包、
乱序、重复的内存网络里、按假时钟运行。

| 层 | package | 收包入口 | 发送出口 | 时间 |
| --- | --- | --- | --- | --- |
| ICE agent | `webrtc/ice` `Agent` | `HandlePacket(local, from, b, now)` | `Config.Send(local, to, b)` | `NextTimeout` / `HandleTimeout` |
| TURN 客户端 | `webrtc/turn` `Client`、`ICERelay` | `HandlePacket(from, b, now)` | `ClientConfig.Send` | `NextTimeout` / `HandleTimeout` |
| TURN 服务端 | `webrtc/turn` `Server` | `HandlePacket(t, b, now)`、`HandleRelayPacket`、`ClientClosed` | `ServerConfig.Send`、`Relay.SendTo`；中继地址通过 `ServerConfig.AllocateRelay` 打开 | `NextTimeout` / `HandleTimeout` |
| DTLS | `tls` `DTLSConn` | `HandleDatagram(b, now)` | `DTLSOptions.Output` | `NextTimeout` / `HandleTimeout` |
| SRTP | `webrtc/srtp` `Context` | `UnprotectRTP` / `UnprotectRTCP` | 返回包本身（`ProtectRTP` / `ProtectRTCP`） | 无 |
| SCTP | `webrtc/sctp` `Association` | `HandlePacket(b, now)` | `Config.Output` | `NextTimeout` / `HandleTimeout` |
| SDP、RTP、RTCP、STUN | `webrtc/sdp`、`webrtc/rtp`、`webrtc/stun` | 原地解析 | 写入调用方给的 buffer | 无 |

这些状态机同一时刻只能由一个 goroutine 调用，TURN 服务端核心除外：它可以被任意多个
goroutine 同时调用，不同 allocation 的转发并行进行。

ICE agent 只把它的传输看作不透明的 `LocalID`：Mux 的 socket 是一个，每个 TURN allocation
又是一个。candidate pair 以 `LocalID` 加远端地址为键，从不以连接对象为键，原因见下一节。

### engine 上的 Mux 与 PeerConnection

`Mux` 是 fib engine 上用 `Engine.AddBinding` 加的一个 UDP binding，任意多个 PeerConnection
共用它：每个对端都在同一个地址上找到各自的 PeerConnection。收到的数据分两步分发。

第一步按发送方。来自某个 PeerConnection 已经在通信的地址的数据报，不论内容，都交给那个
PeerConnection。来自一个谁都不认识的地址的 STUN Binding request，正是浏览器第一次找到
服务端的方式：这个地址服务端可能从未被告知过（浏览器的 host candidate 是 mDNS 名字，其余
的又被 NAT 改写过）。请求的 USERNAME 以目标 PeerConnection 的 ICE ufrag 开头，Mux 按这个
ufrag 查找；那边的 ICE agent 先校验请求的 MESSAGE-INTEGRITY，通过之后才会信任这个地址做
别的事。其它来自未知地址的数据一律丢弃。

第二步在 PeerConnection 内部，按首字节分发，规则来自 RFC 7983：

| 首字节 | 协议 | 交给 |
| --- | --- | --- |
| 0–3 | STUN | ICE agent |
| 20–63 | DTLS | DTLS 连接，其应用数据是 SCTP |
| 64–79 | TURN ChannelData | TURN 客户端，前提是来自本 PeerConnection 申请过 allocation 的 TURN 服务器 |
| 128–191 | RTP 与 RTCP（按 payload type 区分） | 先 SRTP 解密，再交给 track 或 RTCP 处理 |

一切都以远端地址为键，从不以 `fib.Connection` 为键。开启 `Config.ReusePort` 时，一个
binding 在每个 poller 上各有一个 socket，对端的数据报落到哪一个由内核决定，于是同一个
地址可能在几个 socket 上各有一个连接。Mux 向某个地址发送时，用最近一次见到的那个连接；
PeerConnection 先开口时（full agent 的检查、STUN 请求），就用 `Engine.OpenUDPPeer` 打开
一个。`OpenUDPPeer` 要等 socket 所在的事件循环，所以 Mux 在单独的 goroutine 里调用它，
期间为该地址最多缓存 8 个数据报；丢了的由 ICE 和 DTLS 自己重发。每个连接的空闲超时是 45 秒
（`DefaultMuxIdleTimeout`），比 ICE consent 允许无应答的 30 秒更长，所以对端还在 consent
时，活着的路径不会被关掉；consent 检查每 5 秒一次，媒体停多久都能把连接保持住。

没有 Mux 的 PeerConnection 在 `Configuration.Engine` 上、`ListenAddr`（默认 `:0`）处开一个
自己的 Mux，并随自己一起关闭：每个对端一个端口，和浏览器一样。这是客户端的用法，也是使用
TURN 的唯一方式。

### 线程与回调模型

数据包从 fib 的 worker 上来，同一个 PeerConnection 可能同时来好几个；应用从自己的
goroutine 调进来；每个 PeerConnection 还有一个定时器，总是重设到 ICE、DTLS、SCTP、TURN 与
RTCP report 中最早的那个截止时间，在另一个 goroutine 上触发。每个 PeerConnection 一把锁，
把这些全部串起来。

应用回调从不在这把锁下执行。持锁期间产生的回调先排队，释放锁之后再按顺序、一次一个地执行，
由碰巧发现队列里有回调的那个 goroutine 来跑：可能是 worker、定时器，也可能是应用自己的那次
调用。因此回调里可以再调用 PeerConnection（比如在 `OnMessage` 里用 `Send` 回复，在
`OnTrack` 里调用 `RequestKeyFrame`）；但阻塞的回调会拖住同一个 PeerConnection 后面所有的
回调，以及它所在 worker 上的其它连接。耗时的工作请交给 goroutine 或任务池。

buffer 遵循 fib 一贯的约定：`TrackRemote.OnRTP` 和 `PeerConnection.OnRTCP` 的 `pkt` 只在回调期间有效；
`DataChannelMessage` 的 `Data` 归回调所有，可以留着用。`TrackLocal.WriteRTP` 可以在任何
goroutine 上调用，不会保留传入的包。

## 快速上手

### 应答浏览器 offer 的服务端

服务端有公网地址，所以作为 ICE-lite agent：自己从不发检查，只应答浏览器的检查。它的
PeerConnection 共用一个 Mux，而 Mux 一开始就知道自己的 host candidate，所以 answer 一经
设置就可以返回。这让一次往返的 HTTP 信令变得很简单：浏览器把带 candidate 的 offer POST
过来，响应里拿回 answer，这就是 WHIP（RFC 9725）和 WHEP。

```go
package main

import (
	"context"
	"io"
	stdhttp "net/http"
	"time"

	"github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/webrtc"
)

func main() {
	var mux *webrtc.Mux // set before the engine runs

	router := fibhttp.NewRouter()
	router.Post("/whip", func(c *fibhttp.Context) {
		offer, _ := io.ReadAll(c.Request.Body)
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Mux: mux, ICELite: true})
		if err != nil {
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", nil)
			return
		}
		pc.OnDataChannel(func(d *webrtc.DataChannel) {
			d.OnMessage(func(m webrtc.DataChannelMessage) { _ = d.Send(m.Data) }) // echo
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		answer, err := webrtc.Answer(ctx, pc, webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offer)})
		if err != nil {
			_ = pc.Close()
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
			return
		}
		// Keep pc to close it later: on DELETE of the Location, or when
		// OnConnectionStateChange reports failed.
		_ = c.WriteResponse(fibhttp.Response{
			StatusCode: stdhttp.StatusCreated,
			Header:     stdhttp.Header{"Content-Type": {"application/sdp"}, "Location": {"/whip/1"}},
			Body:       []byte(answer.SDP),
		})
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: ":8080", Handler: fibhttp.NewHandler(router)})
	if err != nil {
		panic(err)
	}
	// Every PeerConnection shares UDP port 8080.
	mux, err = webrtc.NewMux(engine, webrtc.MuxConfig{Addr: ":8080"})
	if err != nil {
		panic(err)
	}
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

网页这边只要几行 JavaScript：`createDataChannel`、`createOffer`、`setLocalDescription`，
等 `iceGatheringState` 变成 `complete`，把 `localDescription.sdp` 以
`Content-Type: application/sdp` POST 出去，再用响应调用
`setRemoteDescription({type: 'answer', sdp})`。

真正的服务端还要补上几件事，fib-examples 的 `webrtc` 程序里都有：

- **地址。** 监听通配地址的 Mux 会把所有 up 状态网卡的地址（`ice.HostAddrs`，不含环回与
  link-local）作为 host candidate 公布；监听具体地址的只公布那一个。位于一对一 NAT 之后的
  服务端（比如云主机），把外界访问它用的地址设到 `MuxConfig.PublicAddrs`，端口沿用 Mux 的。
- **共用证书。** `Configuration.Certificates` 是 DTLS 证书；不设的话每个 PeerConnection
  各自生成一个 ECDSA P-256 密钥。服务端用 `tls.GenerateWebRTCCertificate`（有效期 30 天）
  生成一个，大家共用。
- **会话管理。** 按 `Location` 里的 id 保存每个 PeerConnection，收到 DELETE 时关闭；
  `OnConnectionStateChange` 报告 failed 时关闭并忘掉它：没发 DELETE 就离开的浏览器会停止
  应答，lite PeerConnection 在对端沉默 30 秒后进入 failed。另外要给会话数设上限。

### 客户端 PeerConnection

客户端，或主动连出去的服务端，是一个 full ICE agent，用自己的端口。`Offer` 创建并设置
offer，等所有 candidate 收集完再返回，正好用于 WHIP 的 POST：

```go
package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig()) // no bindings: the PeerConnection adds its own
	if err != nil {
		panic(err)
	}
	go engine.Run()
	defer engine.Close()

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		Engine:     engine,
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	})
	if err != nil {
		panic(err)
	}
	defer pc.Close()
	d, _ := pc.CreateDataChannel("chat", nil)
	d.OnOpen(func() { _ = d.SendText("hello") })
	d.OnMessage(func(m webrtc.DataChannelMessage) { println(string(m.Data)) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offer, err := webrtc.Offer(ctx, pc, nil) // waits for every candidate
	if err != nil {
		panic(err)
	}
	resp, err := http.Post("http://server:8080/whip", "application/sdp", strings.NewReader(offer.SDP))
	if err != nil {
		panic(err)
	}
	answer, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(answer)}); err != nil {
		panic(err)
	}
	select {}
}
```

如果信令通道能随时传 candidate（比如 WebSocket），就不必等待，边收集边 trickle。
`SessionDescription` 和 `ICECandidateInit` 序列化出的 JSON 与浏览器的
`RTCSessionDescriptionInit`、`RTCIceCandidateInit` 相同，可以原样和网页互传：

```go
pc.OnICECandidate(func(c *webrtc.ICECandidateInit) {
	if c != nil { // nil: gathering is complete
		send("candidate", c) // marshals as RTCIceCandidateInit does
	}
})
offer, _ := pc.CreateOffer(nil)
_ = pc.SetLocalDescription(offer) // starts gathering
send("offer", offer)

// and for what the peer signals back:
onSignal := func(kind string, msg []byte) {
	switch kind {
	case "answer":
		var d webrtc.SessionDescription
		_ = json.Unmarshal(msg, &d)
		_ = pc.SetRemoteDescription(d)
	case "candidate":
		var c webrtc.ICECandidateInit
		_ = json.Unmarshal(msg, &c)
		_ = pc.AddICECandidate(c) // held until the answer is set
	}
}
```

`CreateOffer(&webrtc.OfferOptions{ICERestart: true})` 用新的凭据重启 ICE；在新的检查选出
pair 之前，数据和媒体继续走旧的 pair。添加 track 或创建第一个 data channel 会触发
`OnNegotiationNeeded`，任一端都可以重新发起 offer。

## Data channel

```go
// Unordered, given up after 3 retransmissions: for state updates
// where only the latest matters.
retransmits := uint16(3)
d, err := pc.CreateDataChannel("state", &webrtc.DataChannelInit{Unordered: true, MaxRetransmits: &retransmits})
if err != nil {
	log.Fatal(err)
}
d.OnOpen(func() { log.Println(d.Label(), "is open") })
d.OnMessage(func(m webrtc.DataChannelMessage) {
	if m.IsString {
		log.Println("text:", string(m.Data))
	}
})
// Back-pressure: write while BufferedAmount() is below a limit of
// your own, and again once it falls to the threshold.
d.SetBufferedAmountLowThreshold(256 << 10)
d.OnBufferedAmountLow(func() { /* write more */ })

// The channels the peer opens.
pc.OnDataChannel(func(d *webrtc.DataChannel) {
	log.Println("peer opened", d.Label(), d.Protocol())
	d.OnMessage(func(m webrtc.DataChannelMessage) { _ = d.Send(m.Data) })
})
```

- channel 可以在 PeerConnection 连上之前创建，SCTP association 建好后才打开。第一个
  channel 会在下一个 offer 里加上 data section。
- `DataChannelInit` 对应 `RTCDataChannelInit`：`Protocol`、`Unordered`、
  `MaxRetransmits` 或 `MaxPacketLifeTime`（最多设一个），以及 `Negotiated` 加 `ID`，用于
  两端各自创建、不走 DCEP 的 channel。stream id 在 DTLS 客户端一侧是偶数，服务端一侧是奇数。
- `Send` 发二进制，`SendText` 发文本；空消息按 RFC 8831 规定的 PPID 发送。`Close` 在排队
  的数据发完后 reset stream，对端的 channel 也随之关闭。
- 能接收的最大消息是 `Configuration.MaxMessageSize`，默认 256 KiB，通过 SDP 的
  max-message-size 公布；发送时遵守对端的限制。接收窗口是 `SCTPReceiveWindow`，1 MiB。
- 分片按序到达的消息直接重组进同一个 buffer，DATA 路径预热后不分配内存。association 保留的
  空闲 buffer 由 `SCTPPoolBytes` 限制，默认每个尺寸档 64 KiB，适合同时持有成千上万个大多
  空闲的 channel 的服务端；单个繁忙的 association 可以调大，设为负数则一个都不留。

## 媒体

PeerConnection 只搬运 RTP，从不查看 payload：没有编解码器、打包器，也没有 jitter buffer。
它做的正是选择性转发单元（SFU）需要的事：协商编解码器和头部扩展，按对端改写 SSRC、payload
type 和扩展 id，用 SRTP 加解密，发送 sender report 与 receiver report，并转交关键帧请求。

### 用 TrackLocal 与 TrackRemote 转发

`TrackLocal` 是应用往里写 RTP 包的流，包可以来自编码器，也可以来自某个 `TrackRemote`。
任意多个 PeerConnection 都可以发送它，各用自己的 SSRC、payload type 和头部扩展 id；暂时
发不了的（还没连上，或者没协商到这个编解码器）就丢弃。`TrackRemote` 是对端发来的流：
`OnTrack` 在它的第一个包到达时报告它，在 `OnTrack` 里设置的 `OnRTP` 从此收到它解密后的
每一个包。

```go
// One track per kind, which every viewer's PeerConnection sends.
var vp8 webrtc.Codec
for _, c := range webrtc.DefaultCodecs() {
	if c.Name() == "VP8" {
		vp8 = c
	}
}
video := webrtc.NewTrackLocal(vp8, "video", "sfu")
var source atomic.Pointer[webrtc.TrackRemote] // the publisher's video

// The publisher: what it sends is written to the track.
publisher.OnTrack(func(r *webrtc.TrackRemote) {
	if r.Kind() != webrtc.MediaKindVideo {
		return
	}
	video.SetHeaderExtensions(r.HeaderExtensions())
	source.Store(r)
	r.OnRTP(func(pkt []byte) { _ = video.WriteRTP(pkt) })
})

// Each viewer, before its offer is answered: the track goes out on the
// offer's video section, and its key frame requests go to the publisher.
sender, _ := viewer.AddTrack(video)
sender.OnKeyFrameRequest(func() {
	if r := source.Load(); r != nil {
		_ = r.RequestKeyFrame()
	}
})
```

- **编解码器。** `Configuration.Codecs`（为 nil 时用 `DefaultCodecs()`）既用于 offer，也
  用于与对端取交集：Opus、VP8、VP9 profile 0、浏览器会提供的四种 profile 的 H.264
  （packetization-mode=1），以及 AV1，payload type 与 Chrome 相同。不做转码的转发服务必须
  让所有对端在转发的编解码器上达成一致，最简单的办法就是只列这几个，比如每个浏览器都支持的
  VP8 和 Opus。不提供 RTX 和 FEC：没有保留可供修复的历史包。
- **头部扩展。** 协商 MID、simulcast 层的 RID、abs-send-time 和 audio level
  （`DefaultHeaderExtensions`）。MID 和 abs-send-time 由 PeerConnection 自己写；其余按 URI
  透传，所以转发用的 track 要用 `SetHeaderExtensions` 接收远端的 id 映射。没被告知 id 的
  扩展会被丢掉。
- **track 发到哪里。** 在 `SetRemoteDescription`（或 `Answer`）之前 `AddTrack`，track 会
  落在对端 offer 里同类型的 media section 上，answer 就在那里发送它；之后再加，则在下一次
  协商时生效。`AddTransceiver(kind, webrtc.DirectionRecvOnly)` 只接收。
  `RTPSender.ReplaceTrack` 无需协商即可换 track，`RemoveTrack` 停止发送。
- **接收。** 包按 MID、信令里给出的 SSRC 或 payload type 找到所属的 track；simulcast 发送方
  的每一层按 RID 各成一个 `TrackRemote`。`Configuration.MaxRemoteSSRCs`（默认 64）限制接收
  的流数，超出的 SSRC 的包在解密之前就被丢弃。
- **序号和时间戳**按写入的原样发出。转发服务在同一个 track 下切换来源时，接收方会看到跳变，
  浏览器能扛过去，但新来源需要一个关键帧。

### RTCP

媒体流动期间，PeerConnection 每隔 `RTCPReportInterval`（1 秒；设为负数则不发）为发送的流
发 sender report、为接收的流发 receiver report。sender report 按 track 的
`SetReportClock(wall, ts)` 把墙上时间映射为 RTP 时间，它表示时间戳 `ts` 采样于 `wall`；发送
多个 track 的一方给同一媒体时间设同一个墙上时间，接收方才能对齐它们。没有设置时，report 把
最近一个包的时间戳当作在该包发出时采样。对端针对本端发送的流发来的 PLI 或 FIR，会
调用对应 `RTPSender` 的 `OnKeyFrameRequest`；`TrackRemote.RequestKeyFrame` 发 PLI，
`RequestFIR` 发 FIR。对端发来的每个复合 RTCP 包解密后也会交给 `OnRTCP`，`WriteRTCP` 则可以
发送 `rtp` package 构造的任何 RTCP：

```go
pc.OnRTCP(func(pkt []byte) {
	var rr rtp.ReceiverReport
	for len(pkt) > 0 {
		h, p, rest, err := rtp.NextRTCP(pkt)
		if err != nil {
			return
		}
		if h.Type == rtp.TypeRR && rr.Unmarshal(p) == nil {
			for _, r := range rr.Reports {
				log.Printf("SSRC %d: %d/256 lost, jitter %d", r.SSRC, r.FractionLost, r.Jitter)
			}
		}
		pkt = rest
	}
})

// Ask the browser to send a stream at 500 kbit/s at most (goog-remb).
remb, _ := rtp.AppendRTCP(nil, &rtp.REMB{Bitrate: 500_000, SSRCs: []uint32{remote.SSRC()}})
_ = pc.WriteRTCP(remb)
```

NACK 也会到达 `OnRTCP`，除非应用自己处理，否则没人应答：PeerConnection 不保存可供重发的包。

## TURN

### 客户端

`Configuration.ICEServers` 接受 STUN 服务器（用来取 server-reflexive candidate）和 TURN
服务器：PeerConnection 在每个 TURN 服务器上申请一个 allocation，作为 ICE agent 的 relay
candidate（两者由 `turn.ICERelay` 连接）。`ICETransportPolicyRelay` 只收集 relay
candidate，于是所有流量都经 TURN 服务器中转，对端永远不知道本端的地址。

```go
pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
	Engine: engine, // a port of its own: TURN cannot share a Mux
	ICEServers: []webrtc.ICEServer{
		{URLs: []string{"stun:stun.example.com"}},
		{URLs: []string{
			"turn:turn.example.com:3478",
			"turn:turn.example.com:3478?transport=tcp",
			"turns:turn.example.com:443",
		}, Username: "1793480000:alice", Credential: "base64-hmac"},
	},
	// Relay candidates only: the peer never learns this side's addresses.
	ICETransportPolicy: webrtc.ICETransportPolicyRelay,
})
```

- TURN 要求 PeerConnection 有自己的端口：TURN 服务器靠地址区分客户端，而同一个 Mux 上的
  PeerConnection 共用一个地址。`NewPeerConnection` 遇到"有 Mux 又配了 TURN"，或"relay 策略
  却没有 TURN 服务器"，都返回 `ErrConfiguration`。
- lite PeerConnection 只收集 host candidate。它不向 STUN 服务器请求，所以 `stun:` URL
  会被接受但不使用；而任何 TURN URL（`turn:` 或 `turns:`）以及 relay 策略都会让它返回
  `ErrConfiguration`，无论用 Mux 还是自己的端口：它永远收集不到这些配置所要的 relay
  candidate。
- URL 形如 `stun:host[:port]` 和 `turn:host[:port]`，端口默认 3478；经 TCP 的 TURN 写作
  `turn:host[:port]?transport=tcp`，经 TLS 的写作 `turns:host[:port]`，端口默认 5349。
  URL 在创建 PeerConnection 时解析。`stuns:` 与 `turns:...?transport=udp`（DTLS）的 URL
  会被跳过。
- 经 TCP 或 TLS 时，只有到服务器的这一段是流：allocation 向对端中继的仍是 UDP（RFC 8656），
  对端看到的没有任何不同。连接在开始收集 candidate 时于 PeerConnection 的 engine 上非阻塞
  地拨出，TLS 握手由 fib 的 `tls` 包完成。`Configuration.TURNTLSConfig` 是 TLS 配置，用于
  私有根证书或客户端证书；为 nil 时使用系统根证书，未设置 `ServerName` 时以 URL 的主机名
  校验服务器。
- 无论经哪种传输，申请不到 allocation 都只是让该服务器的收集结束、不产生 candidate：连接
  被拒或超时（10s）、TLS 握手失败、流无法按 TURN 切分，或者错误响应。之后才失去的
  allocation（连接断开或刷新失败）会把它的 relay candidate 从 ICE agent 中撤回：相关的 pair
  失败；若选中的 pair 在其上，就放弃它，controlling 一方随后提名另一个可用的 pair，若一个都
  没有，ICE 在 `ICEFailedTimeout` 后失败。其他服务器的 candidate 照常工作。
- 客户端处理 401 质询、带新 nonce 的 438、转向备用服务器的 300（仅 UDP；在流上所有消息都走
  同一条连接），刷新 allocation、每个对端地址的 permission（每 4 分钟）以及它绑定的
  channel，没得到应答的请求会重发，`Close` 时释放 allocation；经 TCP 或 TLS 时，释放请求
  发出后连接随之关闭。

`turn.Client` 也可以单独使用：`ClientConfig.Stream` 让它把 ChannelData 消息补齐到 4 字节、
每个请求只发一次；宿主用 `turn.FrameLen` 从流中切出每条消息，再交给 `HandlePacket`，
PeerConnection 正是这样做的。

不过 TURN 客户端通常是浏览器：共用 Mux 的 fib 服务端经由浏览器的 relay 被访问，自己并不需要
TURN。

### 运行 turnserver

`turnserver.Server` 把 `turn.Server` 核心跑在 fib engine 上，所以 WebRTC 服务可以把 TURN
服务器和 HTTP 信令、Mux 放在同一个 engine 上。监听地址是一个以 Server 为 handler 的 UDP
binding；每个 allocation 的中继地址是它自己的一个 UDP binding，用 `AddBinding` 打开，端口
由内核挑选或取自 `PortMin`–`PortMax`，allocation 结束时移除。

```go
engine, err := fib.New(fib.DefaultConfig())
if err != nil {
	panic(err)
}
secret := []byte("shared with the web service that hands out credentials")

cfg := turnserver.Config{
	RelayIP:  netip.MustParseAddr("10.0.0.5"),    // the interface relayed ports bind to
	PublicIP: netip.MustParseAddr("203.0.113.7"), // what clients are told, behind a 1:1 NAT
	PortMin:  49152, PortMax: 65535,
}
cfg.Realm = "example.com"
cfg.Auth = turn.EphemeralAuth(secret, time.Now)
// or fixed users: cfg.Auth = turn.StaticAuth(cfg.Realm, map[string]string{"alice": "secret"})
cfg.MaxAllocations, cfg.MaxAllocationsPerUser = 10000, 10
cfg.MaxPermissions, cfg.MaxChannels = 64, 64
// The WebRTC servers on the private network that clients may relay to;
// everything else private stays refused.
cfg.AllowedPeers = []netip.Prefix{netip.MustParsePrefix("10.0.1.0/24")}

server, err := turnserver.Listen(engine, ":3478", cfg)
if err != nil {
	panic(err)
}
defer server.Close()
go engine.Run()

// The web service, for each browser:
user, pass := turn.EphemeralCredentials(secret, "alice", time.Now().Add(time.Hour))
// RTCIceServer: {urls: "turn:203.0.113.7:3478", username: user, credential: pass}
```

**从不让 worker 等待。** 核心可以并发调用：每个 allocation 有自己的锁，查找 allocation 的
表分了片，所以不同 allocation 的数据报在 fib 的各个 worker 上并行中继，回调宿主时也不持有
任何锁。必须等事件循环的操作，即为中继地址调用 `AddBinding`、为尚未先发包的对端调用
`OpenUDPPeer`，交给 `SetupWorkers` 个 goroutine（默认 4）去做，它们从容量为 `SetupQueue`
（默认 1024）的队列取任务。Allocate 在中继地址绑定好之后才应答；队列满时 allocation 以 508
拒绝，或数据报被丢弃，丢弃数由 `Server.Unsent` 统计。

**认证。** `ServerConfig.Auth` 返回用户的长期密钥 MD5(username:realm:password)，因此服务端
可以只存密钥、不存密码。`StaticAuth` 适用于固定的一组用户。`EphemeralAuth` 接受 TURN REST
API 方案的限时凭据，coturn 的 use-auth-secret 和大多数 WebRTC 服务用的都是这个方案：
`EphemeralCredentials` 生成的用户名是 `<过期时间的 Unix 秒数>:<user>`，密码是
HMAC-SHA1(secret, username) 的 base64。Web 服务与 TURN 服务器之间只共享这个 secret，TURN
服务器不需要用户列表。nonce 不落存储：每个 nonce 携带自己的过期时间，以及用 `NonceSecret`
对过期时间和客户端地址算出的 HMAC（不设则随机生成；同一个负载均衡后面的多台服务器应共用
一个）。超过 `NonceLifetime`（10 分钟）的过期 nonce 以 438 加一个新 nonce 应答。

**允许中继到哪里。** 暴露在公网上的 TURN 服务器绝不能变成通往它所在内网的代理，否则任何
拿到凭据的人（包括从公开网页拿到限时凭据的人）都能借道进去。默认情况下，对
`turn.DeniedByDefault` 所列的每一类地址，permission 和 channel 都以 403 拒绝：未指定地址、
环回、私有地址（RFC 1918 与 unique local）、link-local、组播与广播、共享地址空间
（RFC 6598）、保留与特殊用途的 IPv4 段，以及经转换器通往 IPv4 的 IPv6 前缀（NAT64、
6to4）、site-local 与 discard-only 地址。`AllowedPeers` 中的前缀在这项检查之前放行，
`DeniedPeers` 额外拒绝更多，设置了 `PeerAllowed` 则完全由它决定。WebRTC 服务器位于私有网络
的部署，应在 `AllowedPeers` 中恰好列出这些服务器的前缀；fib-examples 的 `webrtc` 程序只放行
它自己 Mux 的地址和它的中继地址。

`PeerAllowed` 接受 `netip.AddrPort`。permission 只针对地址，所以 CreatePermission 以端口 0
询问；ChannelBind 以它的对端询问，双向中继的每个数据报还会带上端口再问一次。借助
`HoldsRelay`，服务器可以让自己的两个客户端经各自的中继端口互相中继，而本机地址的其它端口
依然对中继关闭：

```go
var server *turnserver.Server // set from Listen before any request comes
publicIP := netip.MustParseAddr("203.0.113.7")
cfg.PeerAllowed = func(p netip.AddrPort) bool {
	if p.Addr() == publicIP {
		return p.Port() == 0 || server.Core().HoldsRelay(p)
	}
	return !turn.DeniedByDefault(p.Addr())
}
```

**其它限制。** allocation 的存活时间是 `DefaultLifetime`（600 秒），客户端可以要求更长，
上限 `MaxLifetime`（3600 秒）；permission 存活 300 秒，channel 绑定 600 秒，一个 channel 号
或对端在重新绑定前要隔离 5 分钟。`MaxAllocations`（超出以 508 拒绝）和
`MaxAllocationsPerUser`（486）限制 allocation 数量，`MaxPermissions` 和 `MaxChannels`
限制单个 allocation 持有的数量（超出任一项以 508 拒绝，channel 顺带装上的 permission 也算
在内）；0 表示不限。这些字段属于 `turn.Limits`，内嵌在 `ServerConfig` 中。`OnRelay` 在每个
数据报中继之前看到它的大小和所属用户，返回 false 即丢弃：带宽限制和流量统计就挂在这里。
Binding request 无需凭据即可应答，供 ICE 获取 server-reflexive candidate。

### 经 TCP 与 TLS 的 TURN

有些网络只放行 TCP，或者只放行 443 端口上的 TLS，而 WebRTC 服务之所以要跑 TURN，正是为了
这些客户端。`ListenTCP` 和 `ListenTLS` 为同一个 Server 加一个 TCP binding，须在 `Listen`
或 `Start` 之后调用；浏览器以 `turns:host:443` 访问 TLS 的那个，以
`turn:host:3478?transport=tcp` 访问 TCP 的那个。每个连接就是一个客户端，消息由
`turn.FrameLen` 切分；它的 allocation 到对端仍走 UDP，连接关闭时随之结束。调用方自己加
binding 时，用 `StreamHandler` 作为 handler。

```go
if _, err := server.ListenTCP(":3478"); err != nil {
	panic(err)
}
if _, err := server.ListenTLS(":443", &stdtls.Config{Certificates: []stdtls.Certificate{cert}}); err != nil {
	panic(err)
}
```

`MaxStreamMessage`（8 KiB）限制单条消息的大小，也就限制了一个连接缓存的数据，下一条消息
声称更大时关闭连接；`StreamIdleTimeout`（30 秒）关闭在这么长时间里没有 allocation 的连接，
无论是第一个 allocation 之前还是最后一个结束之后。

### 事件与统计

`ServerConfig.OnEvent` 会收到这些事件：allocation 的创建、刷新与释放（附带原因：deleted、
expired 或 closed，以及双向的中继流量），凭据通过之后仍被拒绝的请求（附带错误码），以及
凭据校验失败。它在处理该数据包的 goroutine 上运行，不能阻塞。`Core().Stats()` 返回当前持有
的 allocation、待完成的 allocation、permission 和 channel 数，已中继的流量，以及丢弃的数据报、
被拒绝的请求和认证失败的次数；它会遍历所有 allocation，适合在采集指标时调用。

```go
cfg.OnEvent = func(e turn.Event) {
	switch e.Kind {
	case turn.EventFreed:
		log.Printf("%s freed %v (%v): %d bytes out, %d back", e.User, e.Relay, e.Reason, e.ToPeer.Bytes, e.ToClient.Bytes)
	case turn.EventAuthFailed:
		log.Printf("auth failed for %q from %v", e.User, e.Tuple.Client)
	}
}
```

### 重载与关闭

限制和凭据可以原地修改，从下一个请求开始生效，无需重启，也不会丢掉已持有的状态。降低配额
后，超出的 allocation、permission 或 channel 保留到它自然结束；新的 `AuthFunc` 拒绝的用户
不能再刷新。`Shutdown` 用于替换服务器时排空：新的 allocation 以 508 拒绝，已有的继续中继和
刷新，等到一个都不剩或 context 结束时关闭。

```go
core := server.Core()
l := core.Limits()
l.MaxAllocationsPerUser = 5
core.SetLimits(l)
core.SetAuth(turn.EphemeralAuth(newSecret, time.Now))

// Replacing the server: its clients finish, or move on as refreshes fail.
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
defer cancel()
_ = server.Shutdown(ctx)
```

`Server.Close` 立即释放所有 allocation 并移除它们的 binding，同时关闭 TCP 与 TLS 客户端的
连接；监听用的 binding 由调用方自己移除。不用 `Listen` 新建 binding，也可以用
`turnserver.New` 加 `Server.Start` 接管一个已有的 UDP binding，这样调用方就能用
`Engine.BindingAddrs` 得知监听在 `:0` 上的实际端口。

`turnserver.Config` 内嵌 `turn.ServerConfig`，其中 `Realm` 和 `Auth` 必填（`Send` 与
`AllocateRelay` 由 Server 自己设置），另外还有：

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `RelayIP` | 必填 | 中继端口绑定的具体地址；它的地址族就是所有 allocation 的地址族 |
| `PublicIP` | `RelayIP` | 告诉客户端的地址，用于一对一 NAT 之后 |
| `PortMin`、`PortMax` | 由内核挑选 | 中继端口的取值范围 |
| `SetupWorkers`、`SetupQueue` | 4、1024 | 绑定中继地址、打开对端连接的 goroutine 数，以及为它们排队的任务数 |
| `MaxStreamMessage` | 8 KiB | TCP 或 TLS 客户端单条消息的上限 |
| `StreamIdleTimeout` | 30 秒 | TCP 或 TLS 客户端没有 allocation 时连接可保持的时长 |

## 配置与限制

`webrtc.Configuration`，除了 `Mux` 或 `Engine` 二选一之外都可不填：

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `Mux` / `Engine`、`ListenAddr` | — / `:0` | 共用一个 Mux，或在 engine 上开自己的端口 |
| `Certificates` | 新生成的 ECDSA P-256 证书 | DTLS 证书；服务端的各个 PeerConnection 可共用一个 |
| `ICEServers`、`ICETransportPolicy` | 无、all | STUN 与 TURN 服务器（TURN 可经 UDP、TCP 或 TLS）；只收集 relay candidate，需要有 TURN 服务器 |
| `TURNTLSConfig` | nil：系统根证书、URL 的主机名 | `turns:` 服务器的 TLS 配置 |
| `ICELite` | false | 只应答检查，始终 controlled，只有 host candidate；不使用 STUN 服务器，拒绝 TURN 服务器与 relay 策略 |
| `Codecs`、`HeaderExtensions` | `DefaultCodecs()`、`DefaultHeaderExtensions()` | 提供与接受的内容 |
| `MaxMessageSize` | 256 KiB | 能接收的最大 data channel 消息 |
| `SCTPReceiveWindow` | 1 MiB | SCTP 接收窗口 |
| `SCTPPoolBytes` | 每个尺寸档 64 KiB | 保留的 SCTP 空闲 buffer；负数表示不保留 |
| `ICEDisconnectedTimeout`、`ICEFailedTimeout`、`ICEKeepaliveInterval` | 8 秒、30 秒、5 秒 | 选中的 pair 多久无应答判为 disconnected，多久判 ICE failed，多久检查一次 consent；lite PeerConnection 把前两项用于对端沉默的时长 |
| `RTCPReportInterval` | 1 秒 | sender/receiver report 的间隔；负数表示不发 |
| `MaxRemoteSSRCs` | 64 | 接收的媒体流数，每条占用 SRTP 状态和一个 TrackRemote |
| `MaxRemoteCandidates` | 64 | 远端 description 到达前暂存的 trickle candidate 数 |

`webrtc.MuxConfig`：`Network`（`udp`、`udp4` 或 `udp6`）、`Addr`（`:0`）、`PublicAddrs`
以及 `IdleTimeout`（45 秒，`DefaultMuxIdleTimeout`）。Mux 自己给它的每个连接设置空闲超时，
所以 engine 的 `Config.UDPIdleTimeout` 不会提前切断安静的对端。

固定的上限。这些值的选取原则是：有大量对端的服务端为每个对端保留的东西很少，而且对端无法
让它保留更多：

| 内容 | 上限 |
| --- | --- |
| 一个 PeerConnection 在其 Mux 上路由给自己的远端地址 | 256 |
| 远端 description 指明谁是 DTLS 客户端之前暂存的 DTLS 数据报 | 16 |
| 某地址的连接打开期间为它排队的数据报 | 8 |
| 分片的 DTLS ClientHello 在解析前重组的大小 | 8 KiB |
| DTLS 记录、SRTP 与 SRTCP 包的防重放窗口 | 64 |
| DTLS flight 的 MTU 与重传定时器 | 1200 字节，1 秒起翻倍至 60 秒 |
| PeerConnection 提供的 SRTP profile | AEAD_AES_128_GCM、AES128_CM_HMAC_SHA1_80 |

每个 PeerConnection 持有一个 ICE agent、一个 DTLS 连接、两个 SRTP context、一个 SCTP
association 和一个定时器；在 Mux 上，空闲时它不占 socket，也不占 goroutine。测试里让 200 个
ICE-lite PeerConnection 共用一个 Mux，每个都和自己的客户端在 data channel 上互相回显。

## 有意没有实现的功能

以下汇总自各 package 的文档。之所以不做，要么是浏览器所说的 WebRTC 用不到，要么是那属于
应用自己的事。

- **PeerConnection**：provisional answer（`pranswer`）与 rollback；max-bundle 以外的
  BundlePolicy；基于 TCP 的 ICE；解析浏览器 host candidate 的 mDNS 名字（对端的检查本来就
  会以 peer-reflexive candidate 的形式暴露它的地址）；RTX 与 FEC；transport-wide 拥塞控制
  反馈与带宽估计；发送 simulcast（按 RID 接收是支持的）；编解码器的 RTP 打包；DTLS 1.3；
  SDES 密钥交换；RTCP report 以外的统计；在共用的 Mux 上使用 TURN，以及经 DTLS 的
  TURN（`turns:...?transport=udp`）。
- **ICE**：基于 TCP 的 ICE（RFC 6544）；RFC 8445 已废弃的 aggressive nomination；bundle 后
  那一条数据流以外的 component。
- **DTLS**：DTLS 1.3，以及 ECDHE 加 AEAD 以外的密码套件。
- **SRTP**：MKI、非零的密钥派生率、NULL 加密，以及加密的头部扩展（RFC 6904）。
- **SCTP**：多宿主、association restart、I-DATA 与消息交错（RFC 8260）、SCTP-AUTH、零校验和
  （RFC 9653）、ECN、路径 MTU 发现，以及重置出向 stream 以外的 RE-CONFIG 请求；对已被 gap
  确认的数据反悔（renege）的对端。
- **RTP**：RFC 8285 以外的头部扩展 profile（保留但不解读）、RTCP 扩展报告（RFC 3611）、
  编解码器的 payload 格式。
- **SDP**：k=、z=、r= 行只保留不处理；组播与端口数只解析不处理。
- **STUN**：密码的 OpaqueString 与 SASLprep profile（非 ASCII 凭据由调用方自行预处理）、
  USERHASH、STUN URI 与 DNS 发现。
- **TURN**：向对端中继 TCP（RFC 6062）、到服务器的 DTLS、双栈 allocation、
  EVEN-PORT 与 RESERVATION-TOKEN、DONT-FRAGMENT（以 420 应答）、SHA-256 密码算法、RFC 8489
  的 USERHASH 与 nonce cookie、ICMP 属性，以及移动性扩展（RFC 8016）。

## 验证方式

每一层都有自己的测试并在 CI 中运行，其中大部分跑在带丢包、乱序、重复和小 MTU 的内存网络上，
使用假时钟；每个接收不可信字节的解析器都有 fuzz 目标。与其它实现的互通测试是在仓库之外用
临时模块、经环回地址完成的，因为 fib 的 `go.mod` 不引入任何依赖；所用版本即提交说明中记录
的版本。

| 对端 | 验证内容 |
| --- | --- |
| pion/webrtc v4.2.26 | pion 发 offer、fib 应答，以及反过来，trickle 与非 trickle 两种方式：data channel 上最大 60 KiB 的消息回显，伪造的 VP8 和 Opus RTP 双向回环，SSRC 和 payload type 与各自 answer 中给出的一致；两端都只收集 relay candidate、经 `turnserver` 中转 |
| pion/ice v4.4.8 | 两种角色下都能连通并双向传数据，任一端为 lite 时亦然，角色冲突时也能连通 |
| pion/dtls v3.1.15、OpenSSL 3.6 `s_server`/`s_client` | 双向，ECDSA 与 RSA，use_srtp，导出的密钥材料一致，数据回显 |
| pion/srtp v3.1.3 | 每个 profile 下双方都能解开对方加密的包：每个方向 142,744 个 RTP 包，跨越 4 次序号回绕并带乱序，以及 3,000 个 SRTCP 包 |
| pion/rtp v1.10.6、pion/rtcp v1.2.20 | 40,000 个随机 RTP 包和 10,000 个包含所有类型的复合 RTCP 包，一方序列化、另一方解析 |
| pion/sctp v1.12.0、pion/datachannel v1.6.3 | fib 作为客户端、服务端以及双方同时发 INIT，0% 与 2% 丢包：大的有序消息、无序与部分可靠的消息、negotiated channel、任一端关闭，10% 丢包下双向的 FORWARD TSN |
| pion/sdp v3.0.20 | 对 Chrome、Firefox、Safari 的 description 以及生成的 description，双方读出的值相同，各自都能原样解析对方的输出 |
| pion/turn v4 | fib 的客户端经 pion 的服务器，pion 的客户端经 `turnserver`：allocate、refresh、permission、channel 绑定与中继 |
| Chromium | fib-examples 的网页对接各自的服务端：data channel 回显；canvas 生成的视频 track 以 VP8 发出，从服务端回来后被成功解码；经同一个 engine 上的 `turnserver` 只走 relay 的 ICE；SFU 把视频和音频转发给观看者，观看者的 PLI 被转交给发布者 |
| RFC 测试向量 | STUN（RFC 5769）、SRTP 密钥派生与密钥流（RFC 3711、RFC 6188）、AES-GCM 的 SRTP 与 SRTCP（RFC 7714） |
| fib 对 fib | 两个 PeerConnection 在同一个 engine 上或两个 engine 上：双向最大 200 KiB 的 data channel 消息，带 SRTP 的 VP8 与 Opus，PLI 与 RTCP report，任一端发起的重新协商，ICE restart 期间数据不断，对端能看到关闭；200 个 ICE-lite PeerConnection 共用一个 Mux；一对只走 relay、经 `turnserver` 中转的 PeerConnection；`turn.Client` 经 TCP 与 TLS 通过 `turnserver` 中继，带或不带 channel |

fib 的 SCTP 在环回地址上 fib 对 fib 传 64 KiB 消息达到 240 MB/s，同样条件下 pion 对 pion 是
163 MB/s；在内存中，两个 association 传这样的消息约 1.9 GB/s，每条消息不分配内存。在 M4 Pro
上，保护一个 1200 字节的 SRTP 包，AES-128-GCM 需要 207ns，AES-CM 加 HMAC-SHA1 需要 890ns。
