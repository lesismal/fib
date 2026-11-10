# WebRTC

[English](16-webrtc.md) | [简体中文](16-webrtc.zh-CN.md)

`webrtc` 包在 fib 的 engine 上运行 WebRTC 对端：一个 `PeerConnection` 用 data channel 和
RTP 媒体，与浏览器以及其他 WebRTC 实现通信。它下面的每一层都只用标准库从零写成——ICE、
DTLS 1.2 与 DTLS-SRTP、SRTP、SCTP data channel、offer/answer，以及 TURN 客户端与服务端——
因此本章示例不需要 pion 或任何外部模块就能构建。本章涵盖模型、共享 UDP 端口上的信令服务端、
offer/answer 与 trickle ICE、data channel、SFU 的 RTP 转发，以及 TURN；完整设计与边界见
[WebRTC 参考文档](../webrtc.zh-CN.md)。

## 模型

`PeerConnection` 就是 `RTCPeerConnection`：一个 ICE agent、一条 DTLS 1.2 连接、两个 SRTP
上下文和一个 SCTP 关联，全部复用同一条传输，由 offer/answer 协商（JSEP，带 BUNDLE 与
rtcp-mux）。它自己不持有 socket。`Mux` 是 engine 上的一个 UDP binding，被任意多个
`PeerConnection` 共享——这正是有成千上万个对端的服务端需要的：所有对端都通过同一个地址找到
各自的 `PeerConnection`。没有 `Mux` 的 `PeerConnection` 会在
`Configuration.ListenAddr`（默认 `:0`）上自己开一个端口，这是客户端的方式，也是使用 TURN
的唯一方式。服务端侧的 `PeerConnection` 通常是 ICE-lite：它只应答浏览器的连通性检查并收集
host candidate，因为它的地址是公网可及的。

`Mux` 分两步分拣收到的数据报。第一步按发送方：来自某个 `PeerConnection` 已在通信的地址的
数据报直接交给它。来自未知地址的 STUN Binding 请求——浏览器首次到达服务端的方式，而且其
地址可能是服务端从未被告知的——按请求 `USERNAME` 里的 ICE ufrag 路由，前提是该
`PeerConnection` 的 ICE agent 校验过请求的 `MESSAGE-INTEGRITY`。第二步在
`PeerConnection` 内部按首字节，如 RFC 7983 规定：0–3 为 STUN，20–63 为 DTLS（其应用数据
是 SCTP），64–79 为 TURN ChannelData，128–191 为 RTP 和 RTCP。一切都以远端地址为键，
而不是 `fib.Connection`，因为 `Config.ReusePort` 可能让同一对端的数据报落在多条连接上。

线程模型遵循 fib 的规则，但有一点特殊：数据报到达 worker，每个 `PeerConnection` 有一个
自己的定时器在自己的 goroutine 上触发，应用也可以从任意 goroutine 调用进来。每个
`PeerConnection` 一把互斥锁把它们串行化。应用回调在持锁期间入队，释放锁之后在锁外按顺序
逐个执行，由发现队列有内容的那个 goroutine 来跑。因此回调里可以再调用该连接（在
`OnMessage` 里 `Send`，在 `OnTrack` 里 `RequestKeyFrame`），但会阻塞的回调会拖慢该连接之后
的所有回调，以及它所在 worker 上的其他连接。传给 `TrackRemote.OnRTP` 或 `OnRTCP` 的包只在
本次调用期间有效；`DataChannelMessage.Data` 则归回调所有，可以保留。

## 共享 `Mux` 上的 data channel 服务端

服务端有公网地址，所以是 ICE-lite agent，它的 `PeerConnection` 共享同一个 `Mux`，而
`webrtc.Answer` 会完成整个交换——输入 offer，输出带全部 candidate 的 answer——因此普通
HTTP 信令就够了。这就是 WHIP，媒体场景下则是 WHEP：

```go
package main

import (
	"context"
	"io"
	stdhttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/webrtc"
)

func main() {
	var mux *webrtc.Mux // set before the engine runs, read by the handler

	router := fibhttp.NewRouter()
	router.Post("/whip", func(c *fibhttp.Context) {
		offer, err := io.ReadAll(c.Request.Body)
		if err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", nil)
			return
		}
		pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Mux: mux, ICELite: true})
		if err != nil {
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", nil)
			return
		}
		pc.OnDataChannel(func(d *webrtc.DataChannel) {
			d.OnMessage(func(m webrtc.DataChannelMessage) {
				_ = d.Send(m.Data) // echo
			})
		})
		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			if s == webrtc.PeerConnectionStateFailed {
				_ = pc.Close()
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		answer, err := webrtc.Answer(ctx, pc, webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offer)})
		if err != nil {
			_ = pc.Close()
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
			return
		}
		_ = c.WriteResponse(fibhttp.Response{
			StatusCode: stdhttp.StatusCreated,
			Header:     stdhttp.Header{"Content-Type": {"application/sdp"}, "Location": {"/whip/1"}},
			Body:       []byte(answer.SDP),
		})
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(router),
	})
	if err != nil {
		panic(err)
	}
	// Every PeerConnection of this server shares UDP port 8080.
	mux, err = webrtc.NewMux(engine, webrtc.MuxConfig{Addr: "127.0.0.1:8080"})
	if err != nil {
		panic(err)
	}
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

页面侧只有几步：`createDataChannel`、`createOffer`、`setLocalDescription`，等
`iceGatheringState === "complete"`，把 `localDescription.sdp` 以 `application/sdp` POST
出去，再用响应 `setRemoteDescription({type: "answer", sdp})`。浏览器的 host candidate 是
服务端不会去解析的 mDNS 名字；它的连通性检查来自真实地址，会作为 peer-reflexive
candidate 被接受，所以服务端仍然能连通。

真实服务端还要补三件事，这个程序省略了：用 `tls.GenerateWebRTCCertificate` 生成一张所有
连接共享的 DTLS 证书；会话管理（按 `Location` 里的 id 保存每个 `PeerConnection`，收到
`DELETE` 或状态变为 `failed` 时关闭它）；以及 NAT 下的地址配置——主机在一对一 NAT 后面时
设置 `MuxConfig.PublicAddrs`。

## offer/answer 与 trickle ICE

`webrtc.Offer(ctx, pc, opts)` 创建并设置 offer，等待 candidate 收集完成，然后返回一个带着
全部 candidate 的 offer；`webrtc.Answer` 与之对称。对 HTTP 信令来说这是正确形态，但
WebSocket 信令通常会在 candidate 产生时逐个 trickle：

```go
pc.OnICECandidate(func(c *webrtc.ICECandidateInit) {
	if c == nil {
		return // gathering is complete
	}
	msg, _ := json.Marshal(c) // marshals as RTCIceCandidateInit does
	send("candidate", msg)
})

offer, _ := pc.CreateOffer(nil)
_ = pc.SetLocalDescription(offer) // starts gathering
msg, _ := json.Marshal(offer)     // marshals as RTCSessionDescriptionInit does
send("offer", msg)

// What the peer signals back, in the signalling handler:
var desc webrtc.SessionDescription
_ = json.Unmarshal(answerMsg, &desc)
_ = pc.SetRemoteDescription(desc)
var cand webrtc.ICECandidateInit
_ = json.Unmarshal(candidateMsg, &cand)
_ = pc.AddICECandidate(cand) // held until the remote description is set
```

在远端描述设置之前到达的 candidate 会被暂存，上限为
`Configuration.MaxRemoteCandidates`（64）；超过后 `AddICECandidate` 返回
`ErrTooManyCandidates`。`Candidate` 为空的 `ICECandidateInit` 表示 candidate 结束。客户端侧 `Offer` 同样是完整交换，因此一个 Go
客户端对接 HTTP WHIP 端点也很短：

```go
engine, err := fib.New(fib.DefaultConfig()) // no bindings: the PeerConnection adds its own
if err != nil {
	panic(err)
}
go engine.Run()
defer engine.Close()

pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
	Engine:     engine,
	ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.example.com:3478"}}},
})
if err != nil {
	panic(err)
}
defer pc.Close()

dc, err := pc.CreateDataChannel("chat", nil)
if err != nil {
	panic(err)
}
dc.OnOpen(func() { _ = dc.SendText("hello") })
dc.OnMessage(func(m webrtc.DataChannelMessage) { log.Printf("peer: %s", m.Data) })

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
offer, err := webrtc.Offer(ctx, pc, nil) // waits for every candidate
if err != nil {
	panic(err)
}
resp, err := stdhttp.Post("http://127.0.0.1:8080/whip", "application/sdp", strings.NewReader(offer.SDP))
if err != nil {
	panic(err)
}
body, _ := io.ReadAll(resp.Body)
_ = resp.Body.Close()
err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)})
if err != nil {
	panic(err)
}
```

任何一方都可以发起重新协商：新增 track 或第一个 data channel 时会触发
`OnNegotiationNeeded`；`CreateOffer(&webrtc.OfferOptions{ICERestart: true})` 用新凭据重启
ICE，而数据和媒体会在旧候选对上继续流动。

## 媒体与 SFU 转发

`PeerConnection` 只承载 RTP，从不查看 payload：没有编解码器、打包器或抖动缓冲。它做的正是
选择性转发单元（SFU）需要的事——协商编码与头扩展，按对端重写 SSRC、payload type 和扩展
id，用 SRTP 加解密，发送 RTCP 报告，并转发关键帧请求。应用把 RTP 包写入 `TrackLocal`，
从 `TrackRemote` 接收：

```go
var video *webrtc.TrackLocal // one track, sent by every viewer
for _, c := range webrtc.DefaultCodecs() {
	if c.Name() == "VP8" {
		video = webrtc.NewTrackLocal(c, "video", "sfu")
	}
}

var source atomic.Pointer[webrtc.TrackRemote] // the publisher's video
publisher.OnTrack(func(r *webrtc.TrackRemote) {
	if r.Kind() != webrtc.MediaKindVideo {
		return
	}
	video.SetHeaderExtensions(r.HeaderExtensions()) // keep the publisher's ids
	source.Store(r)
	r.OnRTP(func(pkt []byte) { _ = video.WriteRTP(pkt) }) // forward, do not decode
})

// Each viewer, before its offer is answered: the track goes out on the
// offer's video section.
sender, err := viewer.AddTrack(video)
if err != nil {
	panic(err)
}
sender.OnKeyFrameRequest(func() {
	if r := source.Load(); r != nil {
		_ = r.RequestKeyFrame()
	}
})
```

要让 SFU 正常工作，有三条规则。**编码要一致**：转发器无法转码，所以只列出每个对端都有的
编码（`Configuration.Codecs`，例如 VP8 和 Opus），其余交给协商取交集。**保留头扩展 id**：
转发用的 track 通过 `SetHeaderExtensions` 取用远端的 id；没有告知它的扩展会被丢弃。
**请求关键帧**：新观众从关键帧开始，所以要把它的 `OnKeyFrameRequest` 转到发布端的
`TrackRemote.RequestKeyFrame`（发送端忽略 PLI 时用 `RequestFIR`）。序列号与时间戳按写入值
发出，因此在同一个 track 下切换源会让接收端看到跳变，也需要新的关键帧。

丢包修复默认关闭。设置了 `Packets` 的 `Configuration.Retransmit` 会为每个 `TrackLocal` 保留
最近那么多个包，并用它们应答观众的 NACK；观众协商了 RTX 时以 RTX 重传。这份历史属于 track，
由它发往的所有观众共享：

```go
cfg := webrtc.Configuration{
	Mux:        mux,
	ICELite:    true,
	Retransmit: webrtc.RetransmitConfig{Packets: 512}, // about 0.75 MiB per track, once written
}
```

## TURN

客户端通过 `Configuration.ICEServers` 使用 TURN 服务器，分配到的中继地址会成为 ICE agent
的 relay candidate。TURN 需要 `PeerConnection` 有自己的端口：TURN 服务器按地址区分客户端，
而同一个 `Mux` 上的所有 `PeerConnection` 共享地址，因此带 `Mux` 又有 TURN URL 时
`NewPeerConnection` 会报错：

```go
pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
	Engine: engine, // a port of its own: TURN cannot share a Mux
	ICEServers: []webrtc.ICEServer{
		{URLs: []string{"stun:stun.example.com:3478"}},
		{URLs: []string{
			"turn:turn.example.com:3478",
			"turn:turn.example.com:3478?transport=tcp",
			"turns:turn.example.com:443",
		}, Username: "1793480000:alice", Credential: "base64-hmac"},
	},
	ICETransportPolicy: webrtc.ICETransportPolicyRelay, // peer learns no local address
})
if err != nil {
	panic(err)
}
defer pc.Close()
```

服务端侧是 `webrtc/turn/turnserver` 包，它在同一个 engine 上运行 `webrtc/turn` 核心。它需要一个具体地址的
`RelayIP`（在一对一 NAT 后面还要 `PublicIP`）、一个 realm，以及一个 `AuthFunc`——固定用户
用 `turn.StaticAuth`，Web 服务发放的限时凭据用 `turn.EphemeralAuth`：

```go
secret := []byte("shared with the web service that hands out credentials")
cfg := turnserver.Config{
	RelayIP:  netip.MustParseAddr("10.0.0.5"),    // the interface relayed ports bind to
	PublicIP: netip.MustParseAddr("203.0.113.7"), // what clients are told
	PortMin:  49152, PortMax: 65535,
}
cfg.Realm = "example.com"
cfg.Auth = turn.EphemeralAuth(secret, time.Now) // or turn.StaticAuth(cfg.Realm, users)
cfg.MaxAllocations, cfg.MaxAllocationsPerUser = 10000, 10
cfg.AllowedPeers = []netip.Prefix{netip.MustParsePrefix("10.0.1.0/24")} // private peers to relay to

server, err := turnserver.Listen(engine, "10.0.0.5:3478", cfg) // UDP; ListenTCP, ListenTLS add more
if err != nil {
	panic(err)
}
defer server.Close()
go engine.Run()

// For each browser, the web service makes the credentials it puts in RTCIceServer:
user, pass := turn.EphemeralCredentials(secret, "alice", time.Now().Add(time.Hour))
log.Printf("RTCIceServer{urls: turn:203.0.113.7:3478, username: %q, credential: %q}", user, pass)
```

默认情况下服务端拒绝向 loopback、私网和 link-local 地址中继，因此它不会变成进入其所在网络的
代理；`AllowedPeers` 用来指定例外。`server.ListenTCP(addr)` 和
`server.ListenTLS(addr, tlsConfig)` 补上只放行 443 出站的防火墙后面的客户端需要的传输方式。
即使走 TCP 或 TLS，TURN 仍然用 UDP 向对端中继：只有到服务端这一段是流。`Close` 释放所有
allocation，但监听用的 binding 留给调用方移除；`Shutdown(ctx)` 会先让 allocation 自然结束。

## 兼容性与限制

这套实现不只和自己联调：[`webrtc/interop`](../../webrtc/interop) 是一个可依赖 pion 的小模块，每次 push 都会让
pion 的 PeerConnection、DTLS 和 TURN 与 fib 的实现对跑；Chromium 则通过 fib-examples 的
页面验证——data channel 回声、canvas 发送 VP8、经 `turnserver` 的 relay-only ICE，以及一个
把 PLI 传回发布端的 SFU 转发。浏览器页面在使用 `getUserMedia` 时必须由 HTTPS（或
`localhost`）提供，但信令端点可以是普通 HTTP。

有意没有实现的部分：临时应答（pranswer）与回滚，max-bundle 之外的 bundle 策略，ICE over
TCP，解析浏览器的 mDNS host candidate，FEC 与发送 NACK，transport-wide 拥塞控制与带宽
估计，simulcast 的**发送**（按 RID 接收是支持的），编解码器的 RTP 打包，DTLS 1.3，SDES
密钥，RTCP 报告之外的统计，以及共享 `Mux` 上的 TURN。单条消息上限由
`Configuration.MaxMessageSize`（默认 256 KiB）决定，并通过 SDP 通告。

## 坑与限制

- **回调里不能阻塞。** 在 `OnMessage`/`OnTrack` 里直接做耗时工作会拖慢该连接之后的所有回调
  以及所在 worker；慢任务交给 goroutine。
- **包缓冲区不能跨回调保留。** `OnRTP` 和 `OnRTCP` 的包只在调用期间有效，要保留就复制；
  `DataChannelMessage.Data` 可以保留。
- **ICE-lite 需要对面是 full agent。** 浏览器都是 full agent；标记 `ICELite` 的
  `PeerConnection` 会拒绝 TURN 服务器和 relay-only 策略，lite 与 lite 之间永远连不上。
- **ICE 服务器的主机名会立即解析。** `NewPeerConnection` 在创建时就查询每个 `stun:`/`turn:`
  URL 的主机名，这是会阻塞调用方的 DNS 查询，解析不了的名字会让它返回 `ErrConfiguration`。
  这类 PeerConnection 应在 fib worker 之外创建，或者直接用 IP 地址给出服务器。
- **TURN 不能共享 `Mux`。** 给 `PeerConnection` 一个 `Engine`，让它自己开端口，否则
  `NewPeerConnection` 返回 `ErrConfiguration`。
- **服务端必须保存 `PeerConnection`。** `Answer` 不会把它返回；按会话 id 存好，在 `DELETE`
  或 `failed` 时关闭，否则消失的浏览器会永久泄漏一条连接。
- **不要晚设 channel 回调。** `OnDataChannel` 应在返回前设置好 `OnMessage`/`OnOpen`：早期
  到达的消息会落到没有回调的 channel 上并被丢弃。
- **offer/answer 必须串行化。** 两个 goroutine 同时在一个 `PeerConnection` 上调用
  `CreateOffer` 或 `SetRemoteDescription` 会让信令状态交错；每个连接只用一个信令协程。
- **重新协商不是自动的。** 在 answer 之前 `AddTrack` 会把 track 放进 offer 的媒体段，
  answer 于是在那里发送它；之后再加则要等下一次协商——SFU 在 answer 之后为观众加 track，
  必须重新发起 offer。

## 相关文档

- [WebRTC：设计、用法与边界](../webrtc.zh-CN.md) —— 分层、线程、配置与兼容性矩阵
- [TLS](10-tls.zh-CN.md) —— DTLS 1.2、证书以及媒体栈使用的 `tls` 包
- [流媒体](17-media.zh-CN.md) —— 媒体 hub 上的 WHIP 与 WHEP 端点
- [引擎与配置](02-engine.zh-CN.md) —— UDP binding、`AddBinding` 与 `OpenUDPPeer`
- [README](../../README.zh-CN.md) —— 特性表与可运行示例
- [fib-examples](https://github.com/lesismal/fib-examples) —— 浏览器页面、SFU、datachannel
  与 TURN 程序
