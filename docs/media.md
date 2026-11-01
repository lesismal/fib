# Streaming Media: Design, Usage and Limits

[English](media.md) | [简体中文](media.zh-CN.md)

This document describes fib's live streaming stack: how it is put together,
how a server and its relays use it, which codecs each protocol carries, what
it leaves out, and how it was checked against ffmpeg and Chromium. A complete
server that takes and serves every protocol at once is in
[One server, every protocol](#one-server-every-protocol).

Everything lives under [`media`](../media). The protocols are
[`media/rtmp`](../media/rtmp) (with [`media/amf`](../media/amf) and
[`media/flv`](../media/flv) under it), [`media/httpflv`](../media/httpflv),
[`media/rtsp`](../media/rtsp) (with [`media/rtpcodec`](../media/rtpcodec)),
[`media/srt`](../media/srt) (with [`media/mpegts`](../media/mpegts)),
[`media/hls`](../media/hls) (with [`media/fmp4`](../media/fmp4)) and
[`media/rtc`](../media/rtc), WHIP and WHEP on fib's own [WebRTC](webrtc.md).
They meet in [`media/hub`](../media/hub) and share the types of
[`media/av`](../media/av) and the bitstream helpers of
[`media/codec/h264`](../media/codec/h264),
[`media/codec/h265`](../media/codec/h265),
[`media/codec/aac`](../media/codec/aac) and
[`media/codec/opus`](../media/codec/opus). All of it is written on the
standard library and fib: there is no dependency on any other module, and
nothing decodes or encodes audio or video.

## What is supported (overview)

| Protocol | Package | Server | Client | Content |
| --- | --- | --- | --- | --- |
| RTMP, RTMPS | `media/rtmp` | publish and play | publish, play, push and pull relays | Simple and Flash digest handshakes, the chunk stream with all four header formats and extended timestamps, aggregate messages, acknowledgement windows; Enhanced RTMP (H.265, AV1 and Opus by FourCC) and H.265 with the legacy codec id 12; RTMPS through fib/tls |
| HTTP-FLV, WebSocket-FLV | `media/httpflv` | play | — | An FLV file that never ends, over HTTP/1.1, HTTP/2 and HTTP/3, or the same bytes in binary WebSocket messages (also over RFC 8441 and RFC 9220 extended CONNECT); for flv.js, mpegts.js and ffmpeg |
| RTSP 1.0 | `media/rtsp` | record (ANNOUNCE/RECORD) and play | play, publish, push and pull relays | RFC 2326 over TCP interleaved and UDP unicast, one RTP/RTCP port pair shared by every session, RTCP sender reports, Digest and Basic authentication, GET_PARAMETER keep-alives, session timeouts |
| SRT | `media/srt` | publish and play | publish, play, raw sessions | Live mode: the version 5 handshake with SYN cookies, the stream ID and its access control syntax, ACK/ACKACK/NAK, retransmission, TSBPD and too-late drop, AES-CTR encryption with 128-, 192- and 256-bit keys and in-band key refreshes; MPEG-TS bridged to the hub |
| HLS, LL-HLS | `media/hls` | play | — | RFC 8216 with fMP4 or MPEG-TS segments held in memory, chosen per request (`?format=ts`), and the Low-Latency extensions: partial segments, preload hints, blocking playlist reloads |
| WHIP, WHEP | `media/rtc` | WHIP ingest, WHEP egress | WHIP push, WHEP pull, and relays | RFC 9725 and draft-ietf-wish-whep: 201 with a Location, trickle by PATCH, DELETE, CORS, Link headers for ICE servers, bearer tokens; every PeerConnection on one UDP port |
| Hub | `media/hub` | — | — | Streams by path, one publisher and any number of subscribers each; GOP cache; reservations for ingests that learn their tracks late; subscribers that wait for a publisher and survive a republish; one place for publish and play authentication |
| Containers and payloads | `media/flv`, `media/mpegts`, `media/fmp4`, `media/rtpcodec`, `media/amf` | — | — | FLV with Enhanced RTMP tags; MPEG-TS mux and demux; fragmented MP4 writer and reader; RTP payload formats for H.264, H.265, AAC, Opus and G.711 with SDP; AMF0 both ways and AMF3 decoding |
| Codecs | `media/codec/...` | — | — | H.264 and H.265 framing, SPS, configuration records and keyframes; AAC AudioSpecificConfig and ADTS; OpusHead and packet durations |

There is no transcoding anywhere. A stream is published in the codecs its
publisher chose, and each egress carries the tracks its protocol can and
leaves out the rest; the [codec matrix](#codecs-by-protocol) says which.

## Architecture

### Sans-IO cores and the fib glue

As in fib's WebRTC stack, each protocol is a state machine that never owns a
socket, a goroutine or a clock. The caller hands it what arrives (and, where
time matters, the current time), asks it when it next needs the time, and
gives it a callback for what it sends. A thin layer of glue drives the core
from a `fib.Handler`, a UDP binding, `Engine.OpenUDPPeer` or a fib/http
handler. That is what lets the cores be tested and fuzzed without a network,
and what lets one session run its core under one mutex.

| Core | Package | Takes input with | Sends through | Time |
| --- | --- | --- | --- | --- |
| RTMP connection | `rtmp.Conn` | `Feed(b)` | `ConnConfig.Output` | none (timeouts are the glue's) |
| SRT connection | `srt.Conn`, `srt.Listener` | `Handle(b, now)` | `Config.Output` | `NextTimeout` / `HandleTimeout` |
| RTSP messages | `rtsp.Parser` | `Feed(b, fn)` | `Request`, `Response`, `AppendInterleaved` build into a buffer | none |
| RTP payloads | `rtpcodec.Packetizer`, `Depacketizer`, `Reorder` | `Depacketize(p, emit)`, `Push(now, p, emit)` | `Packetize(pkt, emit)` | `Reorder.NextTimeout` / `Expire` |
| FLV | `flv.Demuxer`, `Decoder`, `Encoder`, `Muxer` | `Feed`, `WriteTag` | appended to the caller's buffer | none |
| MPEG-TS | `mpegts.Demuxer`, `Muxer` | `Write(chunk)` | `WritePacket(dst, pkt)` appends | none |
| fMP4 | `fmp4.Writer` | `WritePacket(pkt)` | `Init()`, `Flush(dst, next)` append | none |
| HLS segmenter | `hls.Muxer` (a `hub.Sink`) | `OnTracks`, `OnPacket` | `MediaPlaylist`, `File` | none |

| Glue | Runs on |
| --- | --- |
| `rtmp.Server`, `rtsp.Server` | a `fib.Handler` on a TCP binding; RTSP's UDP pair added with `Engine.AddBinding` by `ListenUDP` |
| `srt.Server` | a `fib.Handler` on a UDP binding, where each peer address is a fib connection; one mutex and one timer per session, the datagrams of each call sent in one `SendDatagrams` batch |
| `httpflv.Handler`, `hls.Handler`, `rtc.Server` | fib/http handlers, on any of HTTP/1.1, HTTP/2 or HTTP/3 |
| `rtmp.Publish`/`Play`, `rtsp.Client` | dial through the engine (`DialWithHandler`), callback-driven |
| `srt.Dial`/`Publish`/`Play` | `Engine.OpenUDPPeer` from a binding of their own |
| `rtc.Pull`/`Push` | a PeerConnection with a port of its own, signaling over the fib/http client |

### The hub

A `hub.Hub` is a registry of streams by path. Each stream has at most one
publisher and any number of subscribers, and no protocol knows another: an
RTMP publisher's stream plays over RTSP, SRT, HTTP-FLV, HLS and WHEP because
each of them subscribes to the hub, not to RTMP.

- **Publish.** An ingest that knows its tracks up front (RTSP's ANNOUNCE, a
  WHIP offer) calls `Hub.Publish(path, tracks, info)`, then
  `Publisher.WritePacket` for every access unit, `Publisher.SetTracks` when the codec configuration changes, and
  `Publisher.Close` when it leaves. A second publisher of a busy path gets
  `ErrStreamBusy`.
- **Reserve.** An ingest that learns its tracks only from what it receives
  (SRT's MPEG-TS, an RTMP publisher before its metadata and sequence
  headers) calls `Hub.Reserve(path, info)` while it can still refuse its peer.
  The path is claimed and `OnPublish` is heard at once, so a second publisher
  is turned away in the handshake (RTMP's `NetStream.Publish.BadName`, SRT's
  `RejectConflict`), not accepted and dropped later. The stream goes live at
  the reserved Publisher's first `SetTracks`; until then subscribers that do
  not wait find nothing.
- **Subscribe.** An egress calls `Hub.Subscribe(path, sink, opts)` with a
  `hub.Sink`, which hears `OnTracks`, then `OnPacket` for every packet, and
  `OnClose` at the end (`ErrUnpublished`, `ErrNotFound` after waiting in vain,
  `ErrClosed`). With `SubscribeOptions.Wait`, a subscription may exist before
  its publisher and survives the publisher leaving for up to `Wait`, hearing
  `OnTracks` again from the next one: that is how a player that connects
  first, an HLS playlist across a reconnecting encoder, or a push relay waiting
  for its source keep their place. Packet times are the publisher's own, and a
  republish may restart them.

**GOP cache.** Each stream keeps what a new subscriber needs to start with a
picture: every packet from the last video keyframe on, audio included, or the
last second of an audio-only stream. `Subscribe` hands the subscriber
`OnTracks` and the cache under the stream's mutex, so what follows continues it
with nothing lost or reordered. The cache is bounded by
`Config.GOPCacheBytes` (16 MiB): a GOP that outgrows it is dropped and caching
resumes at the next keyframe. A subscriber without a cache to start from
(`NoGOPCache`, or a cache that was dropped) gets video from the next keyframe
and audio at once.

**Slow consumers.** `hub.NewDropper(next, backlog, limit)` is the policy every
egress wraps its sink in. `backlog` reports what the consumer has queued and
not yet sent (a fib connection's queued bytes, HTTP/2 or HTTP/3 flow control
holding data, SRT's unacknowledged payload); while it is over `limit`, packets
are dropped rather than queued, and once a video packet is dropped, video is
held back until the next keyframe, since a decoder cannot use the frames that
depend on a dropped one. Audio resumes as soon as the backlog is under the
limit. A sink's memory is thus bounded by the limit plus one packet, and the
publisher never waits. Egresses with a connection to close check a second,
higher limit and disconnect a consumer past it, or one whose output has not
moved for an idle timeout.

**Bounds.** Streams by `MaxStreams`, subscriptions by
`MaxSubscribersPerStream` (`ErrLimit` past either), the cache by
`GOPCacheBytes`, and each sink's queue by the sink itself. A stream with
neither publisher nor subscription leaves the registry. `Hub.Streams` returns
a snapshot of every stream: path, tracks, publisher, subscribers, bytes and
packets in.

### Threading rules

Fan-out is synchronous. `WritePacket` calls every sink's `OnPacket` under the
stream's mutex, from the publisher's goroutine, which is usually the fib worker
that read the packet. There is no queue of the hub's own between a publisher
and its subscribers. That makes three rules:

- **A sink must not block.** It muxes the packet and queues the bytes onto its
  connection without waiting (fib's `Send`, or `WriteNoWait` for a retained
  HTTP response), and drops when that queue is too long, as `NewDropper` does.
  A sink that waits stalls the publisher and every other subscriber of the
  stream.
- **A sink must not call into the hub** from its callbacks, with one
  exception: `Subscription.Close` never blocks and may be called from
  anywhere, a sink's own callbacks included; when the stream's mutex is busy
  it only marks the subscription, and whoever holds the mutex unlinks it.
  (`Subscription.Close` does not call `OnClose`.)
- **Packets are shared.** `Packet.Data` is immutable once handed to
  `WritePacket`; every subscriber reads the same bytes and none may change
  them. The hub's hooks are called without any of its locks held.

The protocol servers follow these rules themselves. RTMP collects a read
round's messages under the session's mutex and acts on them after releasing it,
since a player's sink is called with its publisher's lock held. The HLS
handler answers requests that must wait (the first segment, a blocking reload,
a hinted part) by retaining the request and responding from the goroutine that
completes what it waits for, not on the publisher's goroutine and not under a
lock, so no fib worker is held by a waiting player.

### Authentication

`hub.Config.OnPublish` and `OnSubscribe` see every publish and play, whatever
the protocol, with `PublishInfo`/`SubscribeInfo`: the protocol (`rtmp`,
`rtsp`, `srt`, `whip`, `whep`, `http-flv`, `ws-flv`, `hls`, and
`rtmp-pull`/`rtmp-push` for RTMP's relays), the remote address, and the
query of the URL, where tokens usually travel. Returning an error refuses the publish or
play. Each protocol also has a hook of its own for what only it knows:

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

`rtmp.ServerConfig` has `OnConnect`, `OnPublish` and `OnPlay` (its
`OnPublish` runs before the hub's), and `httpflv.Config` has `OnPlay`, which
runs after the hub's `OnSubscribe`. HLS subscribes once per stream and
format, for the first playlist request, so `OnSubscribe` sees that request
only; checking every HLS request belongs in an HTTP middleware in front of the
handler.

## The canonical packet

Every ingest converts what it receives into one form, and every egress muxes
from that form, so no pair of protocols needs code of its own. The form is
the one ISO BMFF (MP4) uses ([`media/av`](../media/av)):

- `av.Track.Config` is the codec's out-of-band configuration: the
  AVCDecoderConfigurationRecord for H.264, the HEVCDecoderConfigurationRecord
  for H.265, the AV1CodecConfigurationRecord for AV1, the AudioSpecificConfig
  for AAC, the OpusHead for Opus (optional), nil for G.711. It is immutable
  once published; a change is a new `[]*av.Track` (`Publisher.SetTracks`).
  `Width`, `Height`, `SampleRate` and `Channels` are filled in when known.
- `av.Packet.Data` is one access unit: AVCC (4-byte lengths, no access unit
  delimiters) for H.264 and H.265, with the parameter sets in `Track.Config`,
  though they may also appear in band; one temporal unit of OBUs without the
  temporal delimiter for AV1; one raw AAC access unit without ADTS; one Opus
  packet; G.711 samples.
- `DTS` and `PTS` are `time.Duration` from the start of the stream, so tracks
  of different clock rates compare directly; `av.ToTicks` and `av.FromTicks`
  convert to and from a codec clock exactly. `Keyframe` marks IDR/IRAP access
  units, and every audio packet.

| Protocol | Into `av` | Out of `av` |
| --- | --- | --- |
| RTMP, HTTP-FLV | FLV tag bodies already hold AVCC and the config records; sequence headers become `Track.Config`, the composition time offset gives `PTS`, 32-bit millisecond timestamps are unwrapped; packets are held (up to 512 packets or 1 s) until every track `onMetaData` announced has its sequence header | `onMetaData` and a sequence header per track first, then a tag body per packet; H.265 enhanced (hvc1) unless `EncoderConfig.LegacyHEVC`, AV1 and Opus always enhanced |
| RTSP, WHIP (RTP) | The depacketizer reassembles access units into AVCC, takes the configuration from `sprop-*`/`config=` in the SDP and follows parameter sets sent in band, replacing the track when they change it; since RTP has presentation times only, decode times are recovered from the reordering depth the SPS signals; WHIP times packets from their arrival | The packetizer cuts access units to the MTU (STAP-A/FU-A, AP/FU), sends the parameter sets ahead of every keyframe, and stamps from a random sequence number and timestamp; RTSP adds RTCP sender reports on one clock for every track |
| SRT, HLS-TS (MPEG-TS) | Annex B becomes AVCC with the config records built from the parameter sets in band; ADTS is stripped, its header giving the AudioSpecificConfig; Opus gets an OpusHead from its descriptor; 33-bit PTS and DTS are unwrapped; tracks are announced once every stream of the program has its configuration | Annex B with an access unit delimiter and the parameter sets before every keyframe that lacks them; AAC in ADTS; Opus as a private stream with ffmpeg's descriptors and control header; PAT and PMT before each keyframe and every 100 ms; PCR every 40 ms |
| HLS fMP4 | — (the `fmp4` reader is for tests and future ingests) | The configs go into avcC, hvcC, av1C, esds and dOps unchanged, the AVCC samples into mdat as they are |

## Codecs by protocol

What each protocol takes in (ingest) and sends out (egress). A track an
egress cannot carry is left out of that egress only; the others still get
it. FLV, and so RTMP and HTTP-FLV, carry one video and one audio track.

| Codec | RTMP, HTTP-FLV | RTSP (RTP) | SRT, HLS-TS (MPEG-TS) | HLS fMP4 | WHIP / WHEP |
| --- | --- | --- | --- | --- | --- |
| H.264 | ✓ | ✓ RFC 6184 (mode 1 sent; STAP-B, MTAP and FU-B read too) | ✓ stream type 0x1B | ✓ avc1 | ✓ in: modes 0 and 1; out: mode 1 at a profile-level-id the browser decodes |
| H.265 | ✓ Enhanced RTMP (hvc1) in and out; legacy codec id 12 in, and out with `LegacyHEVC` | ✓ RFC 7798, without DONL | ✓ 0x24 | ✓ hvc1 | ✓ in; out when the browser offers it |
| AV1 | ✓ Enhanced RTMP (av01) | — no payload format | — | ✓ av01 | — |
| AAC | ✓ | ✓ mpeg4-generic (AAC-hbr sent; AAC-lbr and MP4A-LATM read) | ✓ ADTS, 0x0F | ✓ mp4a | — dropped: WebRTC has no AAC |
| Opus | ✓ Enhanced RTMP | ✓ RFC 7587 | ✓ private stream with the Opus registration | ✓ Opus (dOps) | ✓ |
| G.711 (PCMU, PCMA) | ✓ sound formats 8 and 7 | ✓ | — | — | ✓ |
| MP3 | ✓ sound format 2 | — | — (skipped by the demuxer) | — | — |

What this means in practice, with the usual encoders:

- **H.264 + AAC** (OBS, ffmpeg, most cameras) plays everywhere except WHEP,
  which sends the video without the audio. A browser viewer that needs sound
  wants the stream published with Opus (over RTSP, SRT, WHIP or Enhanced
  RTMP), or a transcoder outside fib that republishes it.
- **H.264 + Opus** (browsers over WHIP) plays everywhere except RTMP and
  HTTP-FLV players that predate Enhanced RTMP's Opus, which is most of them:
  ffmpeg 5.1 and 7.1 cannot read Opus from FLV either.
- **H.265** needs Enhanced RTMP or legacy codec id 12 on RTMP and HTTP-FLV
  (ffmpeg 7.1 reads the enhanced form, 5.1 neither), and a browser that offers
  H.265 on WHEP.
- **G.711** (IP cameras) is carried by RTMP, HTTP-FLV, RTSP and WebRTC, and
  dropped from SRT and HLS, which play the video alone.
- A track a protocol cannot take in is dropped at ingest: the FLV decoder drops
  tags of codecs it does not know (VP6, Speex, Nellymoser...), and the MPEG-TS
  demuxer skips MP3, MPEG-2 video and AC-3. On the way out, the WHEP endpoint
  logs AAC, MP3 and AV1 the first time it leaves each out.

## One server, every protocol

One engine and one hub serve every protocol: RTMP on TCP 1935, RTSP on TCP
8554 with RTP over UDP on 8000 and 8001, SRT on UDP 9000, and on 8080 HTTP-FLV,
WebSocket-FLV, HLS (fMP4, or MPEG-TS with `?format=ts`), LL-HLS under `/ll/`,
and WHIP and WHEP, whose PeerConnections share UDP port 8080. Any protocol may
publish a path under `live/`, and every other protocol plays it.

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

Publish with any of:

```sh
# RTMP (OBS: server rtmp://host/live, stream key s)
ffmpeg -re -i in.mp4 -c copy -f flv rtmp://127.0.0.1/live/s
# RTSP, over TCP or UDP
ffmpeg -re -i in.mp4 -c copy -f rtsp -rtsp_transport tcp rtsp://127.0.0.1:8554/live/cam
# SRT: the access control syntax with m=publish
ffmpeg -re -i in.mp4 -c copy -f mpegts 'srt://127.0.0.1:9000?streamid=#!::r=live/srt,m=publish'
# WHIP: a browser or an encoder that speaks WHIP POSTs its offer to http://127.0.0.1:8080/whip/live/web
```

and play `live/s` with any of:

| Protocol | URL |
| --- | --- |
| RTMP | `rtmp://127.0.0.1/live/s` |
| RTSP | `rtsp://127.0.0.1:8554/live/s` (TCP or UDP) |
| SRT | `srt://127.0.0.1:9000?streamid=live/s` |
| HTTP-FLV | `http://127.0.0.1:8080/live/s.flv` |
| WebSocket-FLV | `ws://127.0.0.1:8080/live/s.flv` |
| HLS, fMP4 segments | `http://127.0.0.1:8080/live/s/index.m3u8` |
| HLS, MPEG-TS segments | `http://127.0.0.1:8080/live/s/index.m3u8?format=ts` |
| LL-HLS | `http://127.0.0.1:8080/ll/live/s/index.m3u8` |
| WHEP | POST an SDP offer to `http://127.0.0.1:8080/whep/live/s` |

Paths map the same way everywhere: `rtmp://host/app/stream` is `app/stream`,
as are `rtsp://host:8554/app/stream`, `/app/stream.flv`,
`/app/stream/index.m3u8`, `/whip/app/stream`, `/whep/app/stream`, and the SRT
stream ID `app/stream` or `#!::r=app/stream,m=publish`.

Things a real server adds:

- **Addresses.** A Mux on a wildcard address advertises every interface that
  is up; behind a one-to-one NAT, set `webrtc.MuxConfig.PublicAddrs`. RTSP over
  UDP needs ports 8000 and 8001 open as well (`ServerConfig.UDPRTPAddr`,
  `UDPRTCPAddr`). See [WebRTC](webrtc.md#a-server-answering-browser-offers).
- **Limits.** `hub.Config.MaxStreams` and `MaxSubscribersPerStream`,
  `hls.HandlerConfig.MaxStreams`, `rtc.Config.MaxSessions`,
  `srt.ServerConfig.MaxConns`, and fib's own connection limits.
- **TLS.** RTMPS is `tls.NewServer(config, rtmpServer)` as the binding's
  handler; HTTPS, HTTP/2 and HTTP/3 serve HTTP-FLV, HLS and WHIP/WHEP as they
  serve any fib/http handler.
- **Statistics.** `Hub.Streams()` for what is published and watched,
  `srt.Session.Stats` for loss and round-trip times.

## Clients and relays

Every client dials through a fib engine and reports through callbacks; none
blocks a fib worker. Each protocol has a pull relay, which plays a remote
stream into a hub path, and a push relay, which subscribes to a hub path and
publishes it to a remote server:

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

- A pull relay publishes the path when the remote's tracks are known
  (`rtmp.Pull` reserves it first, so a busy path fails at once with
  `hub.ErrStreamBusy`), passes a change of tracks on as `SetTracks`, and
  unpublishes when the remote stream ends. A refused publish closes the
  client.
- A push relay starts with the stream's first tracks, drops packets the
  remote is too slow for as `hub.NewDropper` does, and ends when the stream
  does. RTSP cannot announce new tracks within a session, so a change of
  tracks closes an RTSP push; WHIP pushes hold the latest GOP until the
  PeerConnection connects.
- Reconnecting is the application's: `done`/`OnClose` say a relay ended, and
  starting it again is one call. A push relay's `Wait` keeps it in place across
  a republish of its source.
- Without the hub, the clients are plain players and publishers:
  `rtmp.Play(engine, url, config, sink, ready)` plays into any `hub.Sink`,
  `rtmp.Publish` returns a `ClientConn` that is a `hub.Sink` itself,
  `rtsp.Play`/`rtsp.Publish` and `rtc.Pull`/`rtc.Push` take `OnTracks` and
  `OnPacket` callbacks and have `WritePacket`, and `srt.Dial` gives a raw
  session carrying whatever bytes the application writes.
- The RTSP client over UDP adds a pair of UDP bindings to its engine for each
  stream; the SRT client opens its own binding with `Engine.OpenUDPPeer`; the
  WHIP/WHEP clients open a PeerConnection with a port of their own unless
  `ClientConfig.WebRTC` names a Mux or an engine.

## Configuration and limits

Every field is optional but the `Hub` (and, for `rtc.Config`, a `WebRTC` with
a Mux or an engine); zero values take the defaults below. Timeouts are
everywhere, and every buffer a peer can fill is bounded.

**hub.Config**

| Field | Default | Meaning |
| --- | --- | --- |
| `GOPCacheBytes` | 16 MiB | GOP cache per stream; negative keeps none |
| `MaxStreams`, `MaxSubscribersPerStream` | unlimited | `ErrLimit` past them |
| `OnPublish`, `OnUnpublish`, `OnSubscribe` | none | Authentication and observation, called without the hub's locks |

**rtmp.ServerConfig** (and `ClientConfig`, which shares most fields)

| Field | Default | Meaning |
| --- | --- | --- |
| `HandshakeTimeout` | 10 s | From connect until publish or play (client: plus `DialTimeout`, 10 s) |
| `IdleTimeout` | 30 s | A publisher that sends nothing, or a player whose output does not move |
| `PlayWait` | 0 | How long a play waits for a missing publisher; 0 answers `StreamNotFound` at once |
| `MaxMessageSize` | 8 MiB | Largest message a peer may send |
| `ChunkSize`, `WindowAckSize` | 4096, 2,500,000 | What the server writes and asks of its clients |
| `WriteQueueLimit`, `MaxWriteQueue` | 2 MiB, 16 MiB | Drop past the first (video to the next keyframe), disconnect past the second |
| `Encoder.LegacyHEVC` | false | Write H.265 with codec id 12 instead of Enhanced RTMP |
| `OnConnect`, `OnPublish`, `OnPlay`, `OnClose` | none | Hooks; `OnPublish` may return a sink, which lets a server run without a hub |

**httpflv.Config**: `Path` (`/app/stream.flv` plays `app/stream`), `OnPlay`,
`CORS`, `Wait` (0: 404 for a missing stream), `WriteQueueLimit` and
`MaxWriteQueue` (2 MiB and 16 MiB; on HTTP/2 and HTTP/3 the backlog is the
stream's flow control holding more than 64 KiB, and only HTTP/1 players are
disconnected), `IdleTimeout` (30 s), `Encoder`, and `WebSocket` (whose zero
value accepts every origin).

**rtsp.ServerConfig**

| Field | Default | Meaning |
| --- | --- | --- |
| `UDPRTPAddr`, `UDPRTCPAddr` | `:8000`, `:8001` | The RTP/RTCP pair `ListenUDP` binds, shared by every session |
| `Password`, `Realm`, `AuthMethods` | none, `fib`, Digest | Authentication; Basic sends passwords in the clear. Digest nonces are stateless and last 5 minutes |
| `SessionTimeout`, `IdleTimeout` | 60 s, 60 s | A session without a request, RTCP or (recording) RTP; a connection without a session |
| `MaxHeaderBytes`, `MaxBodyBytes` | 16 KiB, 256 KiB | Bounds of a request |
| `MaxQueuedBytes` | 4 MiB | A player's backlog past which packets drop; twice as much closes it |
| `MTU`, `SenderReportInterval` | 1400, 5 s | RTP packet size, RTCP sender reports |
| `ReorderSize`, `ReorderDelay` | 128 packets, 100 ms | The reorder buffer of each stream recorded over UDP |
| `OnRequest` | none | Sees, and may answer, every authenticated request |

`rtsp.ClientConfig` has `Transport` (TCP or UDP), `DialTimeout`,
`RequestTimeout` and `ReadTimeout` (10 s each), `KeepAlive` (half the session
timeout the server gives, 30 s at most), `MTU`, `MaxQueuedBytes`, and the
reorder buffer's size and delay.

**srt.ServerConfig** (and `ClientConfig`)

| Field | Default | Meaning |
| --- | --- | --- |
| `Latency` | 120 ms | TSBPD latency asked for; the larger side wins |
| `Passphrase`, `PBKeyLen` | none, 16 | Encryption: 10 to 79 bytes; key length 16, 24 or 32. With a passphrase, callers without one are rejected, and the reverse |
| `HandshakeTimeout`, `PeerIdleTimeout` | 3 s, 5 s | Handshake, and a peer that sends nothing |
| `FlowWindow`, `SendBufferPackets` | 8192, 8192 packets | Receive buffer and peer's in-flight packets; unacknowledged plus waiting packets |
| `MaxSendBacklog` | 4 MiB | Unacknowledged payload past which a player's packets drop |
| `PlayWait` | 0 | How long a player waits for a missing stream; 0 rejects it with `RejectNotFound` |
| `MaxConns` | unlimited | Sessions; past it callers get `RejectOverload` |
| `OnConnect`, `OnSession`, `ParseStreamID` | none | Authentication, taking a session out of the hub's hands, a stream ID syntax of one's own |
| `Muxer`, `Demuxer` | MPEG-TS defaults | Tables every 100 ms, PCR every 40 ms, a PES bounded at 4 MiB, 256 packets held while tracks are learned |

Payloads are at most 1456 bytes; players get 7 transport packets (1316 bytes)
per SRT packet.

**hls.HandlerConfig** and its `Muxer hls.Config`

| Field | Default | Meaning |
| --- | --- | --- |
| `Muxer.Format`, `Formats` | FMP4, none | The default segment format, and others a player may ask for with `?format=` (each format played runs a Muxer of its own) |
| `Muxer.TargetDuration` | 2 s | Segments are cut at the first keyframe this far from the segment's start; set it to the keyframe interval |
| `Muxer.PlaylistSize`, `KeepSegments` | 6, 3 | Segments listed, and kept after leaving the list |
| `Muxer.MaxBytes` | 64 MiB | Media held per Muxer; a segment past a quarter of it is cut without a keyframe |
| `Muxer.PartDuration` | 0 (off) | LL-HLS part length; 200 ms to 1 s is usual |
| `Muxer.MaxWaiters` | 4096 | Requests blocked on one Muxer |
| `Prefix` | `/` | URL path before the stream's path |
| `IdleTimeout`, `WaitTimeout` | 30 s, 10 s | A Muxer without requests is stopped; a request waits at most this long |
| `RepublishWait` | 10 s | The playlist continues after a discontinuity when a publisher returns within it |
| `AllowOrigin`, `MaxStreams` | none, unlimited | CORS, and Muxers running at once |

**rtc.Config** (WHIP/WHEP server)

| Field | Default | Meaning |
| --- | --- | --- |
| `WebRTC` | — | The PeerConnections' configuration: a `Mux` and `ICELite` for a server |
| `WHIPPrefix`, `WHEPPrefix`, `ResourcePrefix` | `/whip/`, `/whep/`, `/rtc/session/` | Endpoints and session resources |
| `MaxSessions` | unlimited | WHIP and WHEP together; 503 past it |
| `Authorize`, `AllowOrigin`, `LinkICEServers` | none | Bearer tokens, CORS, ICE servers sent as Link headers |
| `GatherTimeout`, `ConnectTimeout` | 5 s, 15 s | Gathering before the answer, connecting after it |
| `RepublishWait` | 10 s | How long a viewer waits for the next publisher |
| `NoGOPCache`, `MaxHeldBytes` | false, 8 MiB | Start viewers at the cached GOP, held while they connect |
| `MaxLoss` | 0.25 | Reported loss past which a viewer's packets drop (video to the next keyframe) |
| `MTU`, `ReorderSize`, `ReorderDelay` | 1200, 128, 100 ms | RTP size, and a WHIP track's reorder buffer |
| `PLIInterval`, `KeyFrameInterval` | 500 ms, 0 | Least time between keyframe requests; periodic requests |
| `MaxOfferBytes` | 64 KiB | An offer or a PATCH body |
| `OnKeyFrameRequest` | none | A viewer's keyframe request for a stream no WHIP session of this server publishes |

`rtc.ClientConfig` has `Timeout` (15 s), `Token`, `Header`, `MTU`,
`MaxHeldBytes`, the reorder buffer, `PLIInterval` and `KeyFrameInterval`.

Fixed bounds inside the cores:

| What | Bound |
| --- | --- |
| An access unit reassembled from RTP | 8 MiB (`DepacketizerConfig.MaxAccessUnit`) |
| FLV packets held until every announced track has its sequence header | 512 packets or 1 s |
| AMF nesting | `amf.MaxDepth`; counts bounded by the bytes left |
| MPEG-TS PES | 4 MiB, larger ones dropped |
| SRT receive and send buffers | `FlowWindow` and `SendBufferPackets` |
| HLS media per Muxer | `MaxBytes`, old segments first |

## Intentionally not implemented

Collected from the packages' documentation. Each is left out because live
streaming as encoders and players speak it does not need it, because it needs
a codec fib does not have, or because it belongs to the application.

- **Everywhere**: transcoding of any kind (audio, video, resolution, bit
  rate), and with it multiple renditions; recording, DVR and video on demand
  (every stream is live).
- **Hub**: two publishers racing for a free path can still leave one
  `OnPublish` without an `OnUnpublish`, since hooks run without the lock; a
  busy path is refused before `OnPublish` runs.
- **RTMP**: RTMPE and RTMPT; shared objects (their messages are ignored);
  AMF3 replies (a client with objectEncoding 3 gets AMF0 ones); recorded
  streams (play's start and duration are ignored); Enhanced RTMP v2's
  multitrack messages (a multitrack tag is skipped).
- **FLV and AMF**: VP8, VP9 and the original codecs other than those in the
  matrix (Sorenson H.263, VP6, Screen Video, Nellymoser, Speex, ADPCM: parsed,
  then dropped); FLV encryption headers; encoding AMF3, its externalizable
  objects, and AMF0's reserved markers.
- **HTTP-FLV**: a pull client (`rtmp.Pull` relays from RTMP; pulling HTTP-FLV
  needs a fib/http client that streams response bodies into `flv.Demuxer`,
  which is left for later); serving recorded files.
- **RTSP**: RTSP 2.0; RTSP over TLS (rtsps) and over HTTP tunnels; multicast;
  a PLAY Range that seeks, Scale and Speed; sessions that outlive their
  connection; RTCP receiver reports to recorders.
- **RTP payloads**: H.264's interleaved mode (read, not reordered by decoding
  order number); DONL in H.265; MP3 and AV1 payload formats; mpeg4-generic
  modes other than AAC's.
- **SRT**: rendezvous mode; file (message and stream) mode and its congestion
  control; AES-GCM; packet filters (FEC); socket groups (bonding); the HSv4
  handshake of SRT before 1.3; starting a key refresh (the key a Conn begins
  with lasts about 2^31 packets, more than a day at 20 Mbit/s; refreshes the
  peer starts are followed).
- **MPEG-TS**: MP3, MPEG-2 video, AC-3 and other stream types; more than one
  program; PSI other than PAT and PMT; scrambled packets; 192- and 204-byte
  packets; the T-STD buffer model (the PCR leads the DTS by a fixed time).
- **fMP4**: edit lists (a stream with B-frames starts presenting a little
  after 0); encryption (cenc); sample groups, subsample information and mfra;
  G.711 and MP3 tracks.
- **HLS**: delta playlist updates (`_HLS_skip`) and rendition reports;
  byte-range parts; `EXT-X-PROGRAM-DATE-TIME`; multiple variants; I-frame
  playlists; encryption. `EXT-X-TARGETDURATION` is raised when a segment comes
  out longer than the target (a keyframe interval longer than
  `TargetDuration`), which the specification does not strictly allow.
- **WHIP/WHEP**: ICE restarts (answered 422); simulcast (only a section's
  first layer is received); more streams per kind than the offer has
  sections; RTX and FEC; renegotiation when a stream's codec changes (the old
  codec's track stops); A/V synchronization from RTCP sender reports (WHIP
  times packets by arrival, which keeps tracks in step to within the network's
  jitter).
- **Codec helpers**: the H.264 PPS, slice headers, SVC/MVC and the subset SPS;
  the H.265 SPS beyond the bit depths and reordering (no frame rate or aspect
  ratio), and SHVC/MV-HEVC; AAC program config elements, LATM in the codec
  package, ADTS frames of several raw data blocks, CRC checks; OpusTags and
  multistream self-delimited framing.

## How it was verified

Every package has its own tests in CI, which run without ffmpeg: the
sans-IO cores against each other and against captured bytes (`testdata/`
fixtures that ffmpeg 5.1 and 7.1 wrote for every codec, among them H.264
4:4:4, 4:2:2 10-bit, interlaced and cropped streams and H.265 Main 10), with
fuzz targets for every parser that takes untrusted bytes, and the servers and
clients over loopback on fib engines. The whole `media` tree passes
`go test -race` in the Linux container below.

The interop runs used real implementations in Docker over loopback: ffmpeg
5.1 (Debian bookworm, libx264, libx265, libopus, libsrt) and ffmpeg 7.1
(Debian trixie, which reads Enhanced RTMP's H.265 and AV1), and headless
Chromium. The harnesses are scratch modules outside the repository, since
fib's `go.mod` takes no dependency and the in-repo tests must pass without
ffmpeg.

| Area | Peer | What it checks |
| --- | --- | --- |
| RTMP, HTTP-FLV | ffmpeg 5.1, 7.1 | ffmpeg publishes H.264+AAC (5.1), H.265+AAC and AV1+AAC by Enhanced RTMP (7.1); ffprobe and ffmpeg play them back over RTMP and HTTP-FLV, four players at once, with monotonic DTS; fib's client plays into an FLV file ffmpeg decodes; fib's client publishes to and plays from ffmpeg's `-listen 1` (H.264 on 5.1, H.265 on 7.1); RTMPS push and pull |
| RTMP, HTTP-FLV slow players | ffmpeg 5.1 | An RTMP and an HTTP-FLV player that stop reading a 6 Mbit/s 720p stream are disconnected, while healthy players beside them decode; the server's heap stays between 11 and 20 MB throughout |
| RTSP | ffmpeg 5.1, 7.1 | ffmpeg records over TCP and UDP; ffprobe and ffmpeg play over TCP and UDP: H.264+AAC, H.265+Opus, H.264+PCMA; a relay through fib's client (TCP and UDP, H.264 and H.265); fib's client pushing to ffmpeg's listen mode (TCP and UDP); Digest and Basic, with no and wrong credentials refused; a stalled player while another decodes, with the heap flat |
| SRT | ffmpeg 5.1, 7.1 (libsrt) | ffmpeg publishes and plays back H.264+AAC and H.265+Opus, unencrypted and with pbkeylen 16, 24 and 32, also through a relay dropping 2–10% of datagrams both ways, with clean decodes; fib's client pushes to and pulls from ffmpeg in listener mode |
| MPEG-TS | ffmpeg 5.1, 7.1 | ffmpeg-made H.264+AAC, H.265+Opus and AAC-only streams demux, and remuxing them decodes without warnings |
| fMP4 | ffmpeg 5.1, 7.1 | Init segments and fragments of H.264+AAC, H.265+AAC and H.264+Opus, from raw feeds and from ffmpeg's own fMP4 with B-frames, decode without errors, and remuxes keep every packet's timestamps and keyframe flags |
| HLS | ffmpeg 5.1, 7.1 | fMP4: H.264+AAC, H.265+AAC, H.264+Opus, AAC only and LL-HLS with 500 ms parts play 10 s with no errors; a same-codec republish plays across the discontinuity; a playlist checker finds target durations, sequence numbers, discontinuities, parts, blocking reloads and preload hints as the specification has them. MPEG-TS: H.264+AAC (5.1) and H.265+AAC (7.1) from an RTMP push play from the TS variant, plain and LL-HLS |
| Every ingest × every egress | ffmpeg 5.1, 7.1 | ffmpeg publishes over RTMP, RTSP/TCP, RTSP/UDP and SRT, in H.264+AAC, H.265+AAC, H.264+Opus and H.264+PCMA, and plays each stream over RTMP, RTSP/TCP, RTSP/UDP, SRT, HTTP-FLV, HLS fMP4, HLS TS and LL-HLS: 6 s copied and decoded with no errors, the codecs and picture size right, 120–180 video frames, monotonic DTS, and G.711 left out (video alone) on SRT and HLS. Every combination ffmpeg can publish and read passed on both versions; the rest are those ffmpeg cannot do (Opus in FLV either way, Enhanced RTMP on 5.1, G.711 in MPEG-TS) |
| WHIP, WHEP | ffmpeg 5.1, fib's clients | ffmpeg publishes H.264+Opus over RTSP; fib's WHEP client pulls it from the WHEP endpoint into one path and fib's WHIP client pushes it to the WHIP endpoint as another; ffmpeg plays both over RTSP and decodes them with no errors |
| WHIP, WHEP | headless Chromium | WHEP playback of ffmpeg's H.264+Opus: connected, 640×360, 244 frames decoded, no packets lost; WHIP publish from Chromium's fake camera and microphone (OpenH264 Constrained Baseline and Opus), which ffmpeg then plays over RTSP and decodes with no errors |
| The example above | ffmpeg 5.1, 7.1, fib's WHEP client | Built as it is here; ffmpeg publishes H.264+AAC over RTMP and SRT and H.264+Opus over RTSP at once. The RTMP stream plays over RTMP, RTSP over TCP and UDP, SRT, HTTP-FLV, HLS fMP4 and TS, and LL-HLS; the SRT stream over HLS and RTMP; the RTSP stream over SRT and HLS; ffmpeg decodes 4 s of each with no errors, and fib's WHEP client receives the RTSP stream's H.264 and Opus (some 400 packets in 5 s). The WebSocket-FLV upgrade is answered 101, and a publish outside `live/` is refused by the hook |

Not checked against another implementation: H.265 with FLV's legacy codec id
12 (upstream ffmpeg 7.1 reports the stream as unknown, so only fib's own tests
cover it), Opus over RTMP or HTTP-FLV (ffmpeg 5.1 and 7.1 read no Opus from
FLV), WebSocket-FLV beyond its upgrade, and the SRT client against libsrt
servers other than ffmpeg's listener.
