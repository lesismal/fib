# Streaming media

[English](17-media.md) | [简体中文](17-media.zh-CN.md)

Package `media` is a live-streaming stack on fib's engine: RTMP and RTMPS, RTSP over TCP
and UDP, SRT, HTTP-FLV and WebSocket-FLV, HLS with fMP4 or MPEG-TS segments and LL-HLS, and
WHIP and WHEP for WebRTC. They all meet in a hub that holds streams by path, so a stream
published by one protocol plays over every other one — with no transcoding anywhere.
This chapter covers the hub model, one engine that serves several protocol families, each
family on its own, the relay clients, and the limits that come from remuxing instead of
transcoding. [The media reference](../media.md) has the full design and configuration.

## The hub

A `hub.Hub` is a registry of streams by path. Each stream has at most one publisher and any
number of subscribers, and no protocol knows another: an RTMP publisher's stream plays over
RTSP, SRT, HTTP-FLV, HLS and WHEP because each egress subscribes to the hub, not to RTMP.

- An ingest that knows its tracks up front calls `Hub.Publish(path, tracks, info)`, then
  `Publisher.WritePacket` for every access unit, `Publisher.SetTracks` when the codec
  configuration changes, and `Publisher.Close` when it leaves.
- An ingest that learns its tracks from what it receives (SRT's MPEG-TS, RTMP before its
  metadata) calls `Hub.Reserve(path, info)` first, so a second publisher is refused in the
  handshake rather than accepted and dropped.
- An egress calls `Hub.Subscribe(path, sink, opts)` with a `hub.Sink`: `OnTracks`, then
  `OnPacket` for every packet, and `OnClose` at the end. `SubscribeOptions.Wait` lets a
  subscription exist before its publisher and survive a republish.

Fan-out is synchronous: each stream has one mutex, and `WritePacket` calls every sink's
`OnPacket` under it, from the publisher's goroutine, usually a fib worker. A sink must
therefore not block; it queues onto its connection and drops when the consumer falls too far
behind, which `hub.NewDropper` implements. The `Config` hooks are one place for publish and
play authentication across every protocol:

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

All packets are in one canonical form, the one ISO BMFF uses (`media/av`): an
`av.Track.Config` holding the codec's out-of-band configuration (AVC/HEVC decoder
configuration record, AV1 config, AudioSpecificConfig, OpusHead), and `av.Packet.Data`
holding one access unit — AVCC for H.264/H.265, a temporal unit of OBUs without the temporal
delimiter for AV1, a raw AAC access unit, one Opus packet. `DTS` and `PTS` are `time.Duration`, `Keyframe` marks IDR/IRAP
access units and every audio packet. Every protocol converts into and out of this form; that
is why no pair of protocols needs code of its own.

## An RTMP, HTTP-FLV and HLS server

One engine and one hub serve RTMP on TCP 1935 and HTTP-FLV, WebSocket-FLV, HLS and LL-HLS on
8080. Any of them publishes a path under `live/`, and every other one plays it:

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

Publish with ffmpeg, or with OBS (`server rtmp://127.0.0.1/live`, stream key `s`):

```sh
ffmpeg -re -i in.mp4 -c copy -f flv rtmp://127.0.0.1/live/s
```

and play `live/s` with any of:

| Protocol | URL |
| --- | --- |
| RTMP | `rtmp://127.0.0.1/live/s` |
| HTTP-FLV | `http://127.0.0.1:8080/live/s.flv` |
| WebSocket-FLV | `ws://127.0.0.1:8080/live/s.flv` |
| HLS, fMP4 | `http://127.0.0.1:8080/live/s/index.m3u8` |
| HLS, MPEG-TS | `http://127.0.0.1:8080/live/s/index.m3u8?format=ts` |
| LL-HLS | `http://127.0.0.1:8080/ll/live/s/index.m3u8` |

`index.m3u8` is a multivariant playlist naming `media.m3u8`; a format a handler was not given
in `Formats` is answered 404, so `?format=ts` works on `live` here and not on `ll`. Without
`PlayWait` in `HandlerConfig`, a playlist request before the publisher arrives is 404 too.

Paths map the same way everywhere: `rtmp://host/app/stream` is `app/stream`, as are
`/app/stream.flv`, `/app/stream/index.m3u8` and the SRT stream ID `app/stream`. HTTP-FLV
players are flv.js and mpegts.js; HLS players are hls.js, Safari and ffmpeg. The HTTP
handlers are ordinary fib/http handlers, so HTTP/2, HTTP/3 and TLS serve them as they serve
any other handler.

## RTSP over TCP and UDP

The fragments from here on add protocols to the program above: they use its `engine` and
`h`, and go before `engine.Run()`, since `AddBinding` works on an engine that is not running
yet as well as on one that is.

RTSP is a separate binding plus a UDP pair that every session shares. `ListenUDP` binds the
RTP/RTCP ports on the same engine, so `rtsp://…/live/s` plays over TCP-interleaved RTP by
default and over UDP when the client asks:

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

Publish with `ffmpeg -re -i in.mp4 -c copy -f rtsp -rtsp_transport tcp
rtsp://127.0.0.1:8554/live/s` and play with `ffplay rtsp://127.0.0.1:8554/live/s` (add
`-rtsp_transport udp` for UDP). `ServerConfig.Password` with `AuthMethods` turns on Digest
(or Basic) authentication, `PlayWait` lets a player outlive its publisher, and for UDP
players `MaxPlayerLoss` closes a client whose RTCP reports say it loses too much.

## SRT

SRT is a UDP binding whose handler is the server; each peer address is a fib connection, and
the server reads the SRT access-control stream ID to decide publish or play. A passphrase on
both sides encrypts with AES-CTR:

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

With a passphrase on the server, every caller must send it, and one without is rejected in
the handshake. ffmpeg needs to be built with libsrt (`ffmpeg -protocols | grep srt`):

```sh
ffmpeg -re -i in.mp4 -c copy -f mpegts \
  'srt://127.0.0.1:9000?streamid=#!::r=live/s,m=publish&passphrase=correct-horse-battery'
ffplay 'srt://127.0.0.1:9000?streamid=live/s&passphrase=correct-horse-battery'
```

A publisher reserves its path during the handshake,
so a busy path is rejected with `RejectConflict` before any media flows. MPEG-TS is the only
container SRT carries here; AV1 and G.711 are not in it.

## RTMPS and TLS

RTMPS is the RTMP server wrapped in a TLS handler; HTTPS and HTTP/3 for the HTTP handlers are
the usual fib bindings. Nothing about the hub changes:

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

## WHIP and WHEP

WHIP ingest and WHEP egress are fib/http handlers backed by WebRTC PeerConnections that
share a UDP `Mux`. `rtc.Config.WebRTC` is the `webrtc.Configuration` every session is made
from, so it carries the Mux, the certificate and `ICELite`:

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

`rtcServer.ServeHTTP` handles every one of those; the prefixes themselves are
`rtc.Config.WHIPPrefix`, `WHEPPrefix` and `ResourcePrefix`. WHEP carries video and Opus and
drops AAC: a stream published as H.264+AAC plays to browsers without sound unless it is
republished with Opus. Chapter [16-webrtc.md](16-webrtc.md) covers the WebRTC layer
underneath.

## Pull and push relays

Every client dials through a fib engine, which must be running, and reports through
callbacks; none blocks a worker. A pull relay plays a remote stream into a hub path, a push
relay publishes a hub path to a remote server. Reconnecting is the application's: the `done`
callback says the relay started (nil) or why it could not, `ClientConfig.OnClose` (and
`PullConfig.OnClose`) that a running one ended, and starting it again is one call. Each call
returns its relay as well, whose `Close` stops it.

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

The other protocols follow the same pattern: `srt.Publish`/`srt.Play`,
`rtsp.PublishFromHub`/`rtsp.PlayToHub`, and `rtc.PushFromHub`/`rtc.PullToHub`. Without the
hub the same clients are plain players and publishers: `rtmp.Play` into any `hub.Sink`,
`rtmp.Publish` returning a `ClientConn` that is itself a sink, and `rtc.Pull`/`rtc.Push` with
`OnTracks`/`OnPacket` callbacks.

## Pitfalls

- **There is no transcoding.** A track an egress cannot carry is left out of that egress only:
  WHEP drops AAC and MP3, SRT and HLS-TS drop AV1 and G.711, HLS fMP4 drops G.711, only
  RTMP/HTTP-FLV carry MP3, and RTMP/HTTP-FLV players older
  than Enhanced RTMP cannot read Opus. Publish the codecs your players need — usually H.264
  with both AAC for HLS/RTMP and Opus for WebRTC. See the codec matrix in
  [the media reference](../media.md#codecs-by-protocol).
- **One publisher per path.** A second publish fails with `hub.ErrStreamBusy`, or for RTMP
  and SRT is refused in the handshake. Publisher identity, tokens and paths are all seen by
  `hub.Config.OnPublish`, which is the one place to enforce them.
- **A sink must not block.** Fan-out runs on the publisher's worker under the stream's mutex;
  a slow sink delays every other subscriber. Queue on the connection and drop past a limit,
  as `hub.NewDropper` does, or the publisher stalls and every player suffers.
- **Players that connect first need a wait.** With `Wait`/`PlayWait` zero, a player of a
  stream with no publisher gets 404 or `StreamNotFound`. Set it so a viewer, an HLS playlist
  or a push relay survives the encoder reconnecting.
- **A new subscriber starts at the last keyframe**, from the GOP cache, not at the live
  edge. That is the right default; `SubscribeOptions.NoGOPCache` trades the start-up wait for
  latency, and LL-HLS (`PartDuration`) is the low-latency HLS mode.
- **A republish may restart packet times.** Egresses keep their players' timestamps moving
  forward and mark an HLS discontinuity; do not assume the publisher's clock is monotonic
  across reconnects.
- **RTSP over UDP needs its ports open.** The RTP/RTCP pair (default 8000/8001) is separate
  from the RTSP port; if it is unreachable, clients fall back to TCP interleaving or fail.
- **Encryption must match.** An SRT passphrase is 10 to 79 bytes and must be the same on both
  sides; RTMPS, HTTPS and WHEP behind TLS need real certificates, not `tlstest`.
- **Bounded in memory, not on disk.** HLS segments and the GOP cache live in memory, bounded
  by `hls.Config.MaxBytes` (64 MiB per muxer) and by `hub.Config.GOPCacheBytes` (16 MiB per
  stream); long streams and many viewers need those limits tuned, or a bigger box.
- **Every stream is live.** There is no recording, DVR or video on demand: the hub holds
  the current stream and its GOP cache, nothing on disk. Package `media/mp4` reads a file's
  tracks and sample tables (and `media/fmp4` builds fragmented MP4 segments), so an
  application can feed a file into the hub as live samples or cut its own VOD segments — the
  VOD server itself is yours to write.
- **ffmpeg version matters.** Enhanced RTMP (H.265, AV1, Opus), AV1 RTP and LL-HLS support
  vary by version; `-c copy` only works when the input already has a codec the target family
  carries.

## See also

- [Streaming media: design, usage and limits](../media.md) — the hub, every protocol's
  configuration, the codec matrix and the ffmpeg/Chromium interop matrix
- [WebRTC](16-webrtc.md) — the PeerConnection and Mux under WHIP and WHEP
- [WebSocket](09-websocket.md) — the RFC 6455 layer WebSocket-FLV uses
- [HTTP/1.x](06-http1.md) — the streaming responses HTTP-FLV and HLS rely on
- [Engine and configuration](02-engine.md) — bindings, `AddBinding` and UDP bindings
- [fib-examples](https://github.com/lesismal/fib-examples) — a runnable server for every
  protocol and the browser pages that play them
