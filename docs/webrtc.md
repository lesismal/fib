# WebRTC: Design, Usage and Limits

[English](webrtc.md) | [简体中文](webrtc.zh-CN.md)

This document describes fib's WebRTC stack: how it is put together, how a
server and a client use it, what it supports, what it leaves out, and how it
was checked against other implementations. Runnable programs are in
[fib-examples](https://github.com/lesismal/fib-examples):
[`webrtc`](https://github.com/lesismal/fib-examples/tree/main/webrtc), a page
whose data channel and camera a server echoes back, and
[`webrtc/sfu`](https://github.com/lesismal/fib-examples/tree/main/webrtc/sfu),
one publisher forwarded to many viewers, both answering WHIP and WHEP offers
through the
[`webrtc/whip`](https://github.com/lesismal/fib-examples/tree/main/webrtc/whip)
package they share. Without a browser,
[`webrtc/datachannel`](https://github.com/lesismal/fib-examples/tree/main/webrtc/datachannel)
and [`webrtc/media`](https://github.com/lesismal/fib-examples/tree/main/webrtc/media)
connect two Go PeerConnections,
[`webrtc/turnserver`](https://github.com/lesismal/fib-examples/tree/main/webrtc/turnserver)
is a standalone TURN server over UDP, TCP and TLS,
[`webrtc/stun`](https://github.com/lesismal/fib-examples/tree/main/webrtc/stun)
a STUN client and server, and
[`tls/dtls`](https://github.com/lesismal/fib-examples/tree/main/tls/dtls) and
[`udp/openpeer`](https://github.com/lesismal/fib-examples/tree/main/udp/openpeer)
use DTLS and `Engine.OpenUDPPeer` on their own.

The peer is [`webrtc`](../webrtc), and the protocols under it are packages of
their own: [`webrtc/stun`](../webrtc/stun), [`webrtc/ice`](../webrtc/ice),
[`webrtc/turn`](../webrtc/turn) with
[`webrtc/turn/turnserver`](../webrtc/turn/turnserver),
[`webrtc/sdp`](../webrtc/sdp), [`webrtc/sctp`](../webrtc/sctp),
[`webrtc/srtp`](../webrtc/srtp) and [`webrtc/rtp`](../webrtc/rtp), with DTLS
1.2 in [`tls`](../tls) (`dtls*.go`). All of it is written from scratch on the
standard library: there is no dependency on pion or any other module.

## What is supported (overview)

| Area | Content |
| --- | --- |
| PeerConnection | The RTCPeerConnection model: `CreateOffer` (with ICE restart), `CreateAnswer`, `SetLocalDescription`, `SetRemoteDescription`, `AddICECandidate`/`OnICECandidate`, the connection, ICE, gathering and signaling states with their callbacks, renegotiation from either side, `GatheringComplete`, `SelectedCandidatePair`, and `Offer`/`Answer` helpers that run a whole exchange as WHIP and WHEP do; max-bundle and rtcp-mux on one transport |
| Mux | One UDP port of a fib engine shared by any number of PeerConnections, as a server wants; a PeerConnection without one opens a port of its own, as a client does |
| ICE | RFC 8445 full and lite agents, both roles, role conflicts, frozen/waiting pairs, paced and triggered checks, peer-reflexive candidates, regular nomination, restarts; trickle (RFC 8838); consent freshness (RFC 7675); server-reflexive candidates from STUN servers; relay candidates from TURN servers |
| STUN | RFC 8489 messages decoded in place, the attributes of STUN, ICE and TURN, short- and long-term MESSAGE-INTEGRITY, MESSAGE-INTEGRITY-SHA256, FINGERPRINT, and a client transaction with RFC 8489's retransmissions |
| TURN | A client (RFC 8656 over UDP, TCP or TLS: allocations, refreshes, permissions, channels, 300/401/438 handling) and a server core safe for concurrent use, with long-term credentials, stateless nonces, quotas per server, user and allocation, a relay policy that refuses internal addresses, events and statistics, limits and credentials changed in place, and draining; `turnserver` runs the server on a fib engine over UDP, TCP and TLS. The PeerConnection reaches TURN servers over UDP, TCP and TLS |
| DTLS | DTLS 1.2 (RFC 6347) with use_srtp (RFC 5764), sans-IO, sharing fib's TLS 1.2 handshake code: ECDHE with AES-GCM and ChaCha20-Poly1305, ECDSA, Ed25519 and RSA keys, X25519, P-256 and P-384, flights cut to the MTU and retransmitted, a 64-record replay window, a stateless cookie, and certificates checked by SDP fingerprint |
| SRTP | SRTP and SRTCP (RFC 3711) keyed from DTLS-SRTP: AES_CM_128_HMAC_SHA1_80 and _32, AEAD_AES_128_GCM and AEAD_AES_256_GCM (RFC 7714), rollover counters and replay windows per SSRC; no allocation per packet once an SSRC is known |
| RTP and RTCP | RTP parsed in place, RFC 8285 header extensions in both forms, helpers for MID, RID, abs-send-time, transport-wide sequence numbers and audio level; RTCP SR, RR, SDES, BYE, APP, NACK, PLI, FIR, REMB and transport-cc feedback, compound packets walked without allocating |
| Data channels | SCTP over DTLS (RFC 8261) with DCEP (RFC 8832): ordered and unordered, reliable or limited by retransmissions or lifetime (RFC 3758), negotiated channels, stream reset to close (RFC 6525), messages up to 256 KiB by default, a buffered amount with a low-water callback |
| Media | Unified plan transceivers at the RTP level: `TrackLocal` written by the application and sent by any number of PeerConnections under their own SSRC, payload type and extension ids, `TrackRemote` received by MID, SSRC or payload type, one per simulcast RID; sender and receiver reports, PLI and FIR passed on; Opus, VP8, VP9, H.264 and AV1 negotiated |
| SDP | Descriptions kept line by line, so they marshal back byte for byte; typed accessors and builders for every JSEP attribute; Chrome, Firefox and Safari offers and answers in the tests |
| Interop | pion (each layer and the whole PeerConnection), OpenSSL for DTLS, and Chromium through the fib-examples pages; see [How it was verified](#how-it-was-verified) |

## Architecture

### Sans-IO layers

Each protocol is a state machine that never touches a socket, a goroutine or
a clock of its own. The caller hands it what arrives and the current time,
asks it when it next needs the time, and gives it a callback for what it
sends. That is what lets one PeerConnection run them all under one mutex and
one timer, and what lets the tests run two peers through an in-memory network
that loses, reorders and duplicates packets under a fake clock.

| Layer | Package | Takes packets with | Sends through | Time |
| --- | --- | --- | --- | --- |
| ICE agent | `webrtc/ice` `Agent` | `HandlePacket(local, from, b, now)` | `Config.Send(local, to, b)` | `NextTimeout` / `HandleTimeout` |
| TURN client | `webrtc/turn` `Client`, `ICERelay` | `HandlePacket(from, b, now)` | `ClientConfig.Send` | `NextTimeout` / `HandleTimeout` |
| TURN server | `webrtc/turn` `Server` | `HandlePacket(t, b, now)`, `HandleRelayPacket`, `ClientClosed` | `ServerConfig.Send`, `Relay.SendTo`; relayed addresses opened through `ServerConfig.AllocateRelay` | `NextTimeout` / `HandleTimeout` |
| DTLS | `tls` `DTLSConn` | `HandleDatagram(b, now)` | `DTLSOptions.Output` | `NextTimeout` / `HandleTimeout` |
| SRTP | `webrtc/srtp` `Context` | `UnprotectRTP` / `UnprotectRTCP` | returns the packet (`ProtectRTP` / `ProtectRTCP`) | none |
| SCTP | `webrtc/sctp` `Association` | `HandlePacket(b, now)` | `Config.Output` | `NextTimeout` / `HandleTimeout` |
| SDP, RTP, RTCP, STUN | `webrtc/sdp`, `webrtc/rtp`, `webrtc/stun` | parsed in place | built into the caller's buffer | none |

Each of these is called by one goroutine at a time, except the TURN server
core, which takes calls from any number at once and relays for different
allocations in parallel.

The ICE agent knows its transports only as opaque `LocalID`s: the Mux's socket
is one, each TURN allocation another. A candidate pair is keyed by its
`LocalID` and the remote address, never by a connection object, for the reason
the next section gives.

### The Mux and the PeerConnection on the engine

A `Mux` is one UDP binding of a fib engine, added with `Engine.AddBinding`,
that any number of PeerConnections share: every peer reaches every
PeerConnection at the one address. What arrives is sorted in two steps.

First, by sender. A datagram from an address a PeerConnection already talks to
goes to that PeerConnection, whatever it carries. A STUN Binding request from
an address nobody knows yet is how a browser first reaches a server, from an
address the server may never have been told of (its host candidates are mDNS
names, and NATs rewrite the rest): the request's USERNAME begins with the ICE
ufrag of the PeerConnection it is for, and the Mux looks that ufrag up. The ICE
agent there checks the request's MESSAGE-INTEGRITY before the address is
trusted with anything more. Anything else from an unknown address is dropped.

Second, inside the PeerConnection, by the first byte, as RFC 7983 lays it out:

| First byte | Protocol | Goes to |
| --- | --- | --- |
| 0–3 | STUN | the ICE agent |
| 20–63 | DTLS | the DTLS connection, whose application data is SCTP |
| 64–79 | TURN ChannelData | the TURN client, when it comes from a TURN server the PeerConnection allocated on |
| 128–191 | RTP and RTCP (by payload type) | SRTP, then the track or the RTCP handling |

Everything is keyed by remote address, never by `fib.Connection`. With
`Config.ReusePort` a binding has a socket on every poller and the kernel picks
which one a peer's datagrams arrive on, so one address may have a connection
on several. The Mux sends to an address through whichever connection it last
saw for it, or opens one with `Engine.OpenUDPPeer` when the PeerConnection
speaks first (a full agent's checks, a STUN request). `OpenUDPPeer` waits for
the socket's loop, so the Mux calls it on a goroutine of its own and queues up
to 8 datagrams for the address meanwhile; ICE and DTLS send again whatever is
lost. Each connection gets an idle timeout of 45 seconds
(`DefaultMuxIdleTimeout`), longer than the 30 seconds ICE consent may go
unanswered, so a live path is never closed under a peer that still consents,
and the consent checks every 5 seconds keep it open however long media pauses.

A PeerConnection without a Mux opens a Mux of its own on `Configuration.Engine`
at `ListenAddr` (`:0` by default) and closes it with itself: one port per peer,
as a browser has. That is the client's way, and the only way to use TURN.

### Threading and callbacks

Packets arrive on fib's workers, possibly several at once for one
PeerConnection, the application calls in from its own goroutines, and one
timer per PeerConnection, rearmed to the earliest deadline of ICE, DTLS, SCTP,
TURN and RTCP reports, fires on another. A mutex per PeerConnection orders
them all.

Application callbacks never run under that mutex. They are queued while it is
held and run after it is released, in order and one at a time, on whichever
goroutine finds them queued: a worker, the timer or the application's own call.
So a callback may call back into the PeerConnection (an `OnMessage` that
answers with `Send`, an `OnTrack` that calls `RequestKeyFrame`), but a callback
that blocks delays every later one of its PeerConnection, and the other
connections of the worker it runs on. Hand slow work to a goroutine or a pool.

Buffers follow fib's usual rule: the `pkt` of `TrackRemote.OnRTP` and
`PeerConnection.OnRTCP` is valid only during the call, while a `DataChannelMessage`'s `Data`
belongs to the callback, which may keep it. `TrackLocal.WriteRTP` may be called
from any goroutine and does not keep the packet.

## Quick start

### A server answering browser offers

A server has a public address, so it is an ICE-lite agent: it never checks
anything itself, only answers the browser's checks. Its PeerConnections share
one Mux, and since a Mux knows its host candidates at once, an answer is ready
as soon as it is set. That makes HTTP signaling in one round trip simple: the
browser POSTs its offer, with its candidates, and gets the answer back, which
is WHIP (RFC 9725) and WHEP.

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

The page side is a few lines of JavaScript: `createDataChannel`, `createOffer`
and `setLocalDescription`, wait for `iceGatheringState` to be `complete`, POST
`localDescription.sdp` with `Content-Type: application/sdp`, and
`setRemoteDescription({type: 'answer', sdp})` with the response.

Things a real server adds, all of them in the fib-examples `webrtc` program:

- **Addresses.** A Mux on a wildcard address advertises the address of every
  interface that is up (`ice.HostAddrs`, without loopback and link-local); one
  on a specific address advertises that. A server behind a one-to-one NAT, in
  the cloud say, sets `MuxConfig.PublicAddrs` to the addresses the world
  reaches it at, with the Mux's port.
- **One certificate.** `Configuration.Certificates` takes a DTLS certificate;
  without one, each PeerConnection makes an ECDSA P-256 key of its own. A
  server makes one with `tls.GenerateWebRTCCertificate` (valid for 30 days)
  and shares it.
- **Session bookkeeping.** Keep each PeerConnection under the id its
  `Location` names, close it on DELETE, and close and forget it when
  `OnConnectionStateChange` reports failed: a browser that went away without a
  DELETE stops answering, and a lite PeerConnection fails once its peer has
  been silent for 30 seconds. Cap how many you hold.

### A client PeerConnection

A client, or a server that dials out, is a full ICE agent on a port of its own.
`Offer` creates and sets the offer and returns it once every candidate is
gathered, ready for the WHIP POST:

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

With signaling that carries candidates as they come, a WebSocket say, trickle
them instead of waiting. `SessionDescription` and `ICECandidateInit` marshal
to the JSON a browser's `RTCSessionDescriptionInit` and `RTCIceCandidateInit`
do, so they cross to a page unchanged:

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

`CreateOffer(&webrtc.OfferOptions{ICERestart: true})` restarts ICE with new
credentials; data and media keep flowing on the old pair until the new checks
select one. Adding a track or the first data channel calls
`OnNegotiationNeeded`, and either side may offer again.

## Data channels

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

- A channel may be created before the PeerConnection connects; it opens once
  the SCTP association is up. The first one adds a data section to the next
  offer.
- `DataChannelInit` mirrors `RTCDataChannelInit`: `Protocol`, `Unordered`,
  `MaxRetransmits` or `MaxPacketLifeTime` (at most one), and `Negotiated` with
  an `ID` for channels both sides create without DCEP. Stream ids are even on
  the DTLS client and odd on the server.
- `Send` sends binary and `SendText` text; empty messages go as the PPIDs
  RFC 8831 gives them. `Close` resets the stream once what is queued has gone,
  and the peer's channel closes too.
- The largest message received is `Configuration.MaxMessageSize`, 256 KiB by
  default, advertised as SDP's max-message-size; the peer's limit is honored
  when sending. The receiver window is `SCTPReceiveWindow`, 1 MiB.
- A message whose fragments arrive in order is reassembled straight into one
  buffer, and the DATA path does not allocate once warm. The spare buffers an
  association keeps are bounded by `SCTPPoolBytes`, 64 KiB per size class by
  default, which suits a server holding thousands of mostly idle channels;
  raise it for one busy association, or set it negative to keep none.

## Media

The PeerConnection carries RTP and never looks inside a payload: there is no
codec, packetizer or jitter buffer. What it does is what a selective
forwarding unit needs. It negotiates codecs and header extensions, rewrites
SSRCs, payload types and extension ids per peer, protects and unprotects with
SRTP, sends sender and receiver reports, and passes key frame requests on.

### Forwarding with TrackLocal and TrackRemote

A `TrackLocal` is a stream the application writes RTP packets to, from an
encoder or from a `TrackRemote`. Any number of PeerConnections may send it,
each under its own SSRC, payload type and header extension ids, and each drops
what it cannot send yet (not connected, or the codec not negotiated). A
`TrackRemote` is what a peer sends: `OnTrack` reports it with its first
packet, and its `OnRTP`, set inside `OnTrack`, receives every packet from then
on, decrypted.

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

- **Codecs.** `Configuration.Codecs`, `DefaultCodecs()` when nil, is offered
  and intersected with the peer's: Opus, VP8, VP9 profile 0, H.264 with
  packetization-mode=1 in the four profiles browsers offer, and AV1, at
  Chrome's payload types. A forwarder that cannot transcode must make every
  peer agree on what it forwards; the simplest way is to list just those
  codecs, VP8 and Opus say, which every browser has. RTX and FEC are not
  offered: nothing keeps the history to repair from.
- **Header extensions.** The MID, the RID of simulcast layers,
  abs-send-time and the audio level are negotiated
  (`DefaultHeaderExtensions`). The PeerConnection writes the MID and
  abs-send-time itself; others are carried by URI, which is why a forwarding
  track takes the remote's ids with `SetHeaderExtensions`. Extensions whose ids
  it was not told of are dropped.
- **Where a track goes.** `AddTrack` before `SetRemoteDescription` (or before
  `Answer`) puts the track on a media section of its kind in the peer's
  offer, so the answer sends it there; added later, it takes effect at the
  next negotiation. `AddTransceiver(kind, webrtc.DirectionRecvOnly)` only
  receives. `RTPSender.ReplaceTrack` switches tracks without negotiating, and
  `RemoveTrack` stops sending.
- **Receiving.** Packets find their track by MID, by a signaled SSRC or by
  payload type, and a simulcast sender's layers arrive as one `TrackRemote`
  per RID. `Configuration.MaxRemoteSSRCs`, 64 by default, caps the streams
  received: packets of further SSRCs are dropped before they are decrypted.
- **Sequence numbers and timestamps** go out as written. When a forwarder
  switches sources under one track, the receiver sees a jump, which browsers
  ride out, and the new source needs a key frame.

### RTCP

The PeerConnection sends sender reports for what it sends and receiver reports
for what it receives every `RTCPReportInterval` (1s; negative turns them off)
while media flows. A sender report maps wall time to RTP time by the track's
`SetReportClock(wall, ts)`, which says timestamp `ts` was sampled at `wall`; a
sender of several tracks gives them all the same wall time for the same media
time, so that a receiver can line them up. Without it, a report takes the
latest packet's timestamp as sampled when the packet went. A PLI or FIR from the peer for a stream this side sends
calls that `RTPSender`'s `OnKeyFrameRequest`; `TrackRemote.RequestKeyFrame`
sends a PLI and `RequestFIR` a FIR. Every compound RTCP packet the peer sends
also reaches `OnRTCP`, decrypted, and `WriteRTCP` sends any the `rtp` package
builds:

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

NACKs reach `OnRTCP` too, and go unanswered unless the application answers
them: the PeerConnection keeps no packets to resend.

## TURN

### The client side

`Configuration.ICEServers` takes STUN servers, asked for a server-reflexive
candidate, and TURN servers, on each of which the PeerConnection makes an
allocation that becomes a relay candidate of the ICE agent (`turn.ICERelay`
connects the two). `ICETransportPolicyRelay` gathers relay candidates only, so
everything goes through the TURN server and the peer never learns this side's
addresses.

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

- TURN needs a PeerConnection with a port of its own: a TURN server tells its
  clients apart by their address, which the PeerConnections of a Mux share.
  `NewPeerConnection` refuses a TURN server with a Mux, and the relay policy
  without a TURN server, with `ErrConfiguration`.
- A lite PeerConnection gathers host candidates only. It asks no STUN
  server, so `stun:` URLs are accepted and left unused, but it refuses any
  TURN URL (`turn:` or `turns:`) and the relay policy with
  `ErrConfiguration`, with a Mux or a port of its own: it could never gather
  the relay candidate they ask for.
- URLs are `stun:host[:port]` and `turn:host[:port]`, port 3478 by default,
  `turn:host[:port]?transport=tcp` for TURN over TCP, and
  `turns:host[:port]`, port 5349 by default, for TURN over TLS, resolved when
  the PeerConnection is made. `stuns:` and `turns:...?transport=udp` (DTLS)
  URLs are skipped.
- Over TCP or TLS only the leg to the server is a stream: the allocation
  still relays UDP to the peer (RFC 8656), so the peer's side is the same.
  The connection is dialed on the PeerConnection's engine without blocking,
  when gathering starts, and its TLS handshake runs in fib's `tls` package.
  `Configuration.TURNTLSConfig` is the TLS configuration, for private roots
  or a client certificate; nil means the system's roots, and without a
  `ServerName` the URL's host is the name verified.
- An allocation that cannot be made, over any transport, ends that server's
  part of gathering without a candidate: a refused or timed-out connect
  (10s), a failed TLS handshake, a stream that does not frame as TURN, or an
  error response. One lost later, with its connection or a failed refresh,
  has its relay candidate withdrawn from the ICE agent: its pairs fail, and
  a selected pair on it is given up, after which a controlling agent
  nominates another pair that works, and ICE fails, after
  `ICEFailedTimeout`, if none does. The other servers' candidates go on.
- The client handles the 401 challenge, 438 with a fresh nonce and 300 to an
  alternate server (over UDP; on a stream every message goes on the one
  connection), refreshes the allocation, a permission per peer address
  (every 4 minutes) and the channels it binds, sends again a request that
  goes unanswered, and deallocates on `Close`, which also closes a TCP or
  TLS connection once the deallocation is sent.

`turn.Client` is the same client used on its own: `ClientConfig.Stream`
pads its ChannelData messages to four bytes and sends each request once,
and the host frames what the stream brings with `turn.FrameLen` before
passing each message to `HandlePacket`, as the PeerConnection does.

Browsers are the usual TURN clients, though: a fib server on a shared Mux is
reached through the browser's relay, and needs no TURN of its own.

### Running turnserver

`turnserver.Server` runs the `turn.Server` core on a fib engine, so a WebRTC
service can run its TURN server on the same engine as its HTTP signaling and
its Mux. The listening address is a UDP binding with the Server as its handler;
each allocation's relayed address is a UDP binding of its own, opened with
`AddBinding` on a port the kernel picks or in `PortMin`–`PortMax`, and removed
when the allocation ends.

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

**Never waiting on a worker.** The core is safe for concurrent use: each
allocation has a lock of its own and the tables that find one are sharded, so
datagrams of different allocations are relayed in parallel from fib's workers,
and no lock is held while it calls back. What has to wait for an event loop,
`AddBinding` for a relayed address and `OpenUDPPeer` for a peer that has not
spoken first, runs on `SetupWorkers` goroutines (4) fed by a queue of
`SetupQueue` (1024). An Allocate is answered once its address is bound; a full
queue refuses it with 508, or drops the datagram, which `Server.Unsent`
counts.

**Authentication.** `ServerConfig.Auth` returns a user's long-term key,
MD5(username:realm:password), so a server may store keys rather than
passwords. `StaticAuth` serves a fixed set of users. `EphemeralAuth` accepts
the time-limited credentials of the TURN REST API scheme that coturn's
use-auth-secret and most WebRTC services use: `EphemeralCredentials` makes a
username `<expiry as Unix seconds>:<user>` and the base64 of
HMAC-SHA1(secret, username) as its password, so the web service and the TURN
server share only the secret, and the server needs no list of users. Nonces
are not stored: each carries its expiry and an HMAC of it and the client's
address under `NonceSecret` (random unless set; servers behind one load
balancer share one), and a stale one is answered 438 with a fresh one after
`NonceLifetime`, 10 minutes.

**What it will relay to.** A TURN server reachable from the Internet must not
become a proxy into the network it runs in, which anyone with credentials,
ephemeral ones from a public page included, could otherwise reach. By default
it refuses permissions and channels, with 403, for every address
`turn.DeniedByDefault` names: unspecified, loopback, private (RFC 1918 and
unique local), link-local, multicast and broadcast, shared address space (RFC
6598), the reserved and special-purpose IPv4 ranges, and the IPv6 prefixes that
reach IPv4 through a translator (NAT64, 6to4), site-local and discard-only
ones. `AllowedPeers` lets prefixes through before that check, `DeniedPeers`
refuses more, and `PeerAllowed`, when set, decides alone. A deployment whose
WebRTC servers sit on a private network lists exactly their prefixes in
`AllowedPeers`; the fib-examples `webrtc` program allows only its own Mux's
addresses and its relay address.

`PeerAllowed` takes a `netip.AddrPort`. A CreatePermission asks with port 0,
since a permission is for an address alone; a ChannelBind asks with its peer,
and every datagram relayed either way asks again with its port. With
`HoldsRelay` a server can let two of its own clients relay to each other
through its relayed ports while every other port of its address stays closed:

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

**Other limits.** Allocations last `DefaultLifetime` (600s) unless the client
asks for more, up to `MaxLifetime` (3600s); permissions last 300s and channel
bindings 600s, and a channel number or peer is quarantined for five minutes
before it is bound anew. `MaxAllocations` (refused with 508) and
`MaxAllocationsPerUser` (486) cap allocations, and `MaxPermissions` and
`MaxChannels` what one allocation holds (508 past either, a permission a
channel installs counting); zero means no cap. These are `turn.Limits`,
which `ServerConfig` embeds. `OnRelay` sees every relayed datagram's size and
user before it goes, and returning false drops it: that is the hook for
bandwidth limits and accounting. Binding requests are answered without
credentials, for ICE's server-reflexive candidates.

### TURN over TCP and TLS

Some networks let only TCP through, or only TLS on port 443, and their
clients are why WebRTC services run TURN at all. `ListenTCP` and `ListenTLS`
add a TCP binding for the same Server, after `Listen` or `Start`; a browser
reaches the TLS one as `turns:host:443`, or a TCP one as
`turn:host:3478?transport=tcp`. Each connection is one client, its messages
framed by `turn.FrameLen`; its allocation still relays UDP to peers, and ends
when the connection closes. `StreamHandler` is the handler for a binding the
caller adds itself.

```go
if _, err := server.ListenTCP(":3478"); err != nil {
	panic(err)
}
if _, err := server.ListenTLS(":443", &stdtls.Config{Certificates: []stdtls.Certificate{cert}}); err != nil {
	panic(err)
}
```

`MaxStreamMessage` (8 KiB) bounds a message, and so what a connection
buffers, and closes a connection whose next message claims more;
`StreamIdleTimeout` (30s) closes one that goes that long without an
allocation, before its first or after its last.

### Events and statistics

`ServerConfig.OnEvent` hears of allocations made, refreshed and freed (with
the reason, deleted, expired or closed, and the traffic relayed each way), of
requests refused after their credentials passed (with the error code), and of
credentials that failed. It runs on the goroutine that handled the packet and
must not wait. `Core().Stats()` returns the allocations, pending allocations,
permissions and channels held, the traffic relayed, and the datagrams dropped,
requests refused and auth failures; it walks every allocation, so it is for a
metrics scrape.

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

### Reloading and shutting down

Limits and credentials change in place, from the next request on, without a
restart and without losing what is held. An allocation, permission or channel
held past a lowered quota is kept until it ends, and a user the new `AuthFunc`
refuses can no longer refresh. `Shutdown` drains a server being replaced: new
allocations are refused with 508, held ones go on relaying and refreshing, and
it closes once none is left or its context ends.

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

`Server.Close` frees every allocation at once, removing its binding, and
closes the TCP and TLS clients' connections; the listening bindings are the
caller's to remove. `turnserver.New` and `Server.Start` take an existing UDP
binding instead of `Listen` making one, which is how a caller learns the port
of a binding on `:0` from `Engine.BindingAddrs`.

`turnserver.Config` embeds `turn.ServerConfig`, which needs `Realm` and `Auth`
(the Server sets `Send` and `AllocateRelay` itself), and adds:

| Field | Default | Meaning |
| --- | --- | --- |
| `RelayIP` | required | The specific address relayed ports bind to; its family is every allocation's |
| `PublicIP` | `RelayIP` | The address clients are told, behind a 1:1 NAT |
| `PortMin`, `PortMax` | kernel's pick | The range relayed ports are taken from |
| `SetupWorkers`, `SetupQueue` | 4, 1024 | Goroutines that bind relayed addresses and open peers, and the work queued for them |
| `MaxStreamMessage` | 8 KiB | Largest message from a TCP or TLS client |
| `StreamIdleTimeout` | 30s | How long a TCP or TLS client may go without an allocation |

## Configuration and limits

`webrtc.Configuration`, all optional but a `Mux` or an `Engine`:

| Field | Default | Meaning |
| --- | --- | --- |
| `Mux` / `Engine`, `ListenAddr` | — / `:0` | Share a Mux, or open a port of its own on the engine |
| `Certificates` | a fresh ECDSA P-256 certificate | The DTLS certificate; share one among a server's PeerConnections |
| `ICEServers`, `ICETransportPolicy` | none, all | STUN and TURN servers (TURN over UDP, TCP or TLS); relay-only gathering, which needs a TURN server |
| `TURNTLSConfig` | nil: system roots, the URL's host | TLS configuration for `turns:` servers |
| `ICELite` | false | Answer checks only, always controlled, host candidates only; STUN servers unused, TURN servers and the relay policy refused |
| `Codecs`, `HeaderExtensions` | `DefaultCodecs()`, `DefaultHeaderExtensions()` | What is offered and accepted |
| `MaxMessageSize` | 256 KiB | Largest data channel message received |
| `SCTPReceiveWindow` | 1 MiB | SCTP receiver window |
| `SCTPPoolBytes` | 64 KiB per size class | Spare SCTP buffers kept; negative keeps none |
| `ICEDisconnectedTimeout`, `ICEFailedTimeout`, `ICEKeepaliveInterval` | 8s, 30s, 5s | When the selected pair is disconnected, when ICE fails, how often consent is checked; a lite PeerConnection applies the first two to its peer's silence |
| `RTCPReportInterval` | 1s | Sender and receiver reports; negative sends none |
| `MaxRemoteSSRCs` | 64 | Media streams received, each costing SRTP state and a TrackRemote |
| `MaxRemoteCandidates` | 64 | Trickled candidates held until the remote description arrives |

`webrtc.MuxConfig`: `Network` (`udp`, `udp4` or `udp6`), `Addr` (`:0`),
`PublicAddrs` and `IdleTimeout` (45s, `DefaultMuxIdleTimeout`). The Mux sets
the idle timeout on each of its connections itself, so the engine's
`Config.UDPIdleTimeout` does not cut a quiet peer short.

Fixed bounds, chosen so that a server with many peers holds little for each
and a peer cannot make it hold more:

| What | Bound |
| --- | --- |
| Remote addresses one PeerConnection routes to itself on its Mux | 256 |
| DTLS datagrams kept before the remote description says who the DTLS client is | 16 |
| Datagrams queued for an address while its connection opens | 8 |
| A fragmented DTLS ClientHello, reassembled before it is read | 8 KiB |
| DTLS records, SRTP and SRTCP packets: replay windows | 64 |
| DTLS flights: MTU, retransmission timer | 1200 bytes, 1s doubling to 60s |
| SRTP profiles a PeerConnection offers | AEAD_AES_128_GCM, AES128_CM_HMAC_SHA1_80 |

Each PeerConnection holds one ICE agent, one DTLS connection, two SRTP
contexts and one SCTP association, and one timer; on a Mux it costs no socket
and no goroutine while idle. The tests run 200 ICE-lite PeerConnections on one
Mux, each echoing on a data channel to a client of its own.

## Intentionally not implemented

Collected from the packages' documentation; each is left out because WebRTC as
browsers speak it does not need it, or because it belongs to the application.

- **PeerConnection**: the provisional answer (`pranswer`) and rollback;
  BundlePolicy other than max-bundle; ICE over TCP; resolving the mDNS names
  of browsers' host candidates (a peer's checks reveal its address as a
  peer-reflexive candidate anyway); RTX and FEC; transport-wide congestion
  control feedback and bandwidth estimation; sending simulcast (receiving it,
  by RID, works); the RTP packetization of codecs; DTLS 1.3; SDES keying;
  statistics beyond the RTCP reports; TURN on a shared Mux, and TURN over
  DTLS (`turns:...?transport=udp`).
- **ICE**: ICE over TCP (RFC 6544); aggressive nomination, which RFC 8445
  deprecates; components other than the one bundled data stream.
- **DTLS**: DTLS 1.3, and suites other than ECDHE with an AEAD.
- **SRTP**: MKIs, key derivation rates other than zero, the NULL cipher, and
  encrypted header extensions (RFC 6904).
- **SCTP**: multi-homing, association restart, I-DATA and message
  interleaving (RFC 8260), SCTP-AUTH, the zero checksum (RFC 9653), ECN, path
  MTU discovery, and RE-CONFIG requests other than resetting outgoing streams;
  a peer that reneges on gap-acknowledged data.
- **RTP**: header extension profiles other than RFC 8285's (kept, not read),
  RTCP extended reports (RFC 3611), codec payload formats.
- **SDP**: the k=, z= and r= lines beyond keeping them; multicast and port
  counts beyond parsing them.
- **STUN**: the OpaqueString and SASLprep profiles for passwords (non-ASCII
  credentials are the caller's to prepare), USERHASH, the STUN URI and DNS
  discovery.
- **TURN**: relaying TCP to peers (RFC 6062), DTLS to the server, dual
  allocations, EVEN-PORT and RESERVATION-TOKEN, DONT-FRAGMENT (answered 420),
  the SHA-256 password algorithm, USERHASH and the nonce cookie of RFC 8489,
  the ICMP attribute, and mobility (RFC 8016).

## How it was verified

Every layer has its own tests in CI, most of them on an in-memory network
under a fake clock with loss, reordering, duplication and small MTUs, plus
fuzz targets for each parser that takes untrusted bytes. The interop runs
against other implementations were made over loopback with scratch modules
outside the repository, since fib's `go.mod` takes no dependency; the versions
are those the commit messages record.

The checks against pion's PeerConnection, TURN and DTLS now live in
[`webrtc/interop`](../webrtc/interop), a module of its own that may depend
on pion. CI runs them on every push and pull request, in the `webrtc-interop`
job (non-blocking for now):

- fib offers and pion answers, without trickle; pion offers and fib answers,
  with trickle. Each time a data channel echoes messages up to 60 KiB, and a
  VP8 track from fib and an Opus track from pion arrive with the SSRC and
  payload type each side signaled.
- The same with both sides gathering relay candidates only, through fib's
  `turnserver`: the selected pair is relay to relay, with two allocations.
- fib's DTLS against pion/dtls, fib as client and as server, with ECDSA and
  RSA certificates: use_srtp picks AES-128-GCM, both export the same keying
  material, and data is echoed.

They need only Go, and run on macOS as well as Linux:

```sh
cd webrtc/interop && go test -race ./...
```

The other rows below (pion/ice, srtp, rtp and rtcp, sctp and datachannel,
sdp, turn as a library, OpenSSL and Chromium) were run by hand.

| Peer | What it checks |
| --- | --- |
| pion/webrtc v4.2.26 | pion offers and fib answers, and the other way round, trickle and non-trickle: data channel messages up to 60 KiB echoed, fake VP8 and Opus RTP looped back both ways with the SSRC and payload type each answer signals, and both sides gathering relay candidates only through `turnserver` |
| pion/ice v4.4.8 | Connects and carries data both ways in both roles, with either side lite, and through role conflicts |
| pion/dtls v3.1.15, OpenSSL 3.6 `s_server`/`s_client` | Both directions, ECDSA and RSA, use_srtp, equal exported keying material, data echoed |
| pion/srtp v3.1.3 | Each unprotects what the other protects, every profile: 142,744 RTP packets each way across four sequence number wraps with reordering, and 3,000 SRTCP packets |
| pion/rtp v1.10.6, pion/rtcp v1.2.20 | 40,000 random RTP packets and 10,000 compound RTCP packets of every type marshaled by one and parsed by the other |
| pion/sctp v1.12.0, pion/datachannel v1.6.3 | fib as client, as server and with both sending INIT, at 0% and 2% loss: large ordered, unordered and partially reliable messages, negotiated channels, closes from either side, FORWARD TSN at 10% loss |
| pion/sdp v3.0.20 | Both read the same values from Chrome, Firefox and Safari descriptions and from generated ones, and each parses the other's output unchanged |
| pion/turn v4 | fib's client through pion's server, and pion's client through `turnserver`, allocating, refreshing, permitting, binding and relaying |
| Chromium | The fib-examples pages against their servers: data channel echo, a canvas video track sent as VP8 and decoded back from the server, relay-only ICE through `turnserver` on the same engine, and the SFU forwarding video and audio to a viewer with its PLI passed to the publisher |
| RFC test vectors | STUN (RFC 5769), SRTP key derivation and key stream (RFC 3711, RFC 6188), SRTP and SRTCP with AES-GCM (RFC 7714) |
| fib against fib | Two PeerConnections on one engine and on two: data channels up to 200 KiB both ways, VP8 and Opus with SRTP, PLI and RTCP reports, renegotiation from either side, ICE restarts with data still flowing, close seen by the peer; 200 ICE-lite PeerConnections on one Mux; a relay-only pair through `turnserver`; `turn.Client` over TCP and TLS through `turnserver`, with and without channels |

fib's SCTP moved 64 KiB messages fib to fib at 240 MB/s over loopback where
pion to pion managed 163 MB/s; in memory, two associations move them at about
1.9 GB/s with no allocation per message. A 1200-byte SRTP packet is protected
in 207ns with AES-128-GCM and 890ns with AES-CM and HMAC-SHA1 on an M4 Pro.
