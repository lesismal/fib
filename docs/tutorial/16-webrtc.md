# WebRTC

[English](16-webrtc.md) | [简体中文](16-webrtc.zh-CN.md)

Package `webrtc` runs WebRTC peers on fib's engine: a `PeerConnection` that speaks to
browsers and other WebRTC stacks with data channels and RTP media. Everything under it is
written from scratch on the standard library — ICE, DTLS 1.2 with DTLS-SRTP, SRTP, SCTP
data channels, offer/answer, and a TURN client and server — so the examples build without
pion or any other module. This chapter covers the model, a signalling server on a shared
UDP port, offer/answer and trickle ICE, data channels, RTP forwarding for an SFU, and TURN;
the full design and limits are in [the WebRTC reference](../webrtc.md).

## The model

A `PeerConnection` is an `RTCPeerConnection`: one ICE agent, one DTLS 1.2 connection, two
SRTP contexts and one SCTP association, all bundled on a single transport and negotiated by
offer and answer (JSEP with BUNDLE and rtcp-mux). It does not own a socket. A `Mux` is one
UDP binding of the engine shared by any number of `PeerConnection`s, which is what a server
with thousands of peers wants: every peer reaches every `PeerConnection` at the one address.
A `PeerConnection` without a `Mux` opens a port of its own at `Configuration.ListenAddr`
(`:0` by default), which is the client's way and the only way to use TURN. On the server
side the peer connection is usually ICE-lite: it only answers the browser's connectivity
checks and gathers host candidates, since its address is public.

The `Mux` sorts what arrives in two steps. First by sender: a datagram from an address a
`PeerConnection` already talks to goes to it. A STUN Binding request from an unknown address
— how a browser first reaches a server, possibly from an address the server was never told
of — is routed by the ICE ufrag in its `USERNAME`, after that `PeerConnection`'s ICE agent
verifies the request's `MESSAGE-INTEGRITY`. Then, inside the `PeerConnection`, by the first
byte as RFC 7983 lays it out: 0–3 STUN, 20–63 DTLS (whose application data is SCTP), 64–79
TURN ChannelData, 128–191 RTP and RTCP. Everything is keyed by remote address, never by
`fib.Connection`, because `Config.ReusePort` may deliver one peer's datagrams on several
connections.

Threading follows fib's rules but has one twist: packets arrive on workers, one timer per
`PeerConnection` fires on its own goroutine, and the application calls in from anywhere. A
mutex per `PeerConnection` serializes them. Application callbacks are queued while the mutex
is held and run *outside* it, in order and one at a time, on whichever goroutine finds them
queued. So a callback may call back into the connection (`Send` from `OnMessage`,
`RequestKeyFrame` from `OnTrack`), but a callback that blocks delays every later callback of
that connection and the worker it runs on. A packet passed to `TrackRemote.OnRTP` or
`OnRTCP` is valid only during the call; a `DataChannelMessage`'s `Data` is the callback's
to keep.

## A data-channel server on a shared Mux

A server has a public address, so it is an ICE-lite agent, its PeerConnections share one
`Mux`, and `webrtc.Answer` does the whole exchange — offer in, answer out with every
candidate — so plain HTTP signalling is enough. This is WHIP, and WHEP for media:

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

The page side is a handful of calls: `createDataChannel`, `createOffer`,
`setLocalDescription`, wait for `iceGatheringState === "complete"`, POST
`localDescription.sdp` as `application/sdp`, and `setRemoteDescription({type: "answer",
sdp})` with the response. The browser's host candidates are mDNS names the server does not
resolve; its connectivity checks arrive from a real address and are accepted as a
peer-reflexive candidate, which is why the server still connects.

A real server adds three things this program leaves out: one shared DTLS certificate made
with `tls.GenerateWebRTCCertificate`, session bookkeeping (keep each `PeerConnection` under
the id in its `Location`, close it on `DELETE` and on `failed`), and address configuration
for NAT — `MuxConfig.PublicAddrs` when the host is behind a one-to-one NAT.

## Offer, answer and trickle ICE

`webrtc.Offer(ctx, pc, opts)` creates and sets an offer, waits for gathering to complete,
and returns an offer carrying every candidate; `webrtc.Answer` mirrors it. That is the
right shape for HTTP signalling, but a WebSocket signalling channel usually trickles
candidates as they are gathered instead:

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

Candidates that arrive before the remote description are held, up to
`Configuration.MaxRemoteCandidates` (64). On the client side, `Offer` is also the whole
exchange, so a Go client against an HTTP WHIP endpoint is short:

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

Renegotiation works from either side: `OnNegotiationNeeded` fires when a track or the first
data channel is added, and `CreateOffer(&webrtc.OfferOptions{ICERestart: true})` restarts ICE
with fresh credentials while data and media keep flowing on the old pair.

## Media and SFU forwarding

The `PeerConnection` carries RTP and never looks inside a payload: there is no codec,
packetizer or jitter buffer. What it does is what a selective forwarding unit needs —
negotiate codecs and header extensions, rewrite SSRCs, payload types and extension ids per
peer, protect with SRTP, send RTCP reports, and pass key frame requests on. The application
writes RTP packets to a `TrackLocal` and receives them from a `TrackRemote`:

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

Three rules make an SFU work with this. **Agree on codecs**: a forwarder cannot transcode, so
list only the codecs every peer has (`Configuration.Codecs`, e.g. VP8 and Opus) and let the
negotiation intersect the rest. **Keep the header extension ids**: a forwarding track takes
the remote's ids with `SetHeaderExtensions`; extensions it was not told of are dropped.
**Request key frames**: a new viewer starts on a key frame, so route its `OnKeyFrameRequest`
to the publisher's `TrackRemote.RequestKeyFrame`. Sequence numbers and timestamps go out as
written, so switching sources under one track makes the receiver see a jump and need a new
key frame.

## TURN

A client reaches a TURN server through `Configuration.ICEServers`, and the allocation
becomes a relay candidate of its ICE agent. TURN needs a `PeerConnection` with a port of
its own: a TURN server tells its clients apart by address, which the `PeerConnection`s of
one `Mux` share, so `NewPeerConnection` refuses a TURN URL with a `Mux`:

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

The server side is package `turnserver`, which runs the `turn` core on the same engine. It
needs a specific `RelayIP` (and `PublicIP` behind a one-to-one NAT), a realm, and an
`AuthFunc` — `turn.StaticAuth` for fixed users, or `turn.EphemeralAuth` for the time-limited
credentials a web service hands out:

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

server, err := turnserver.Listen(engine, "127.0.0.1:3478", cfg)
if err != nil {
	panic(err)
}
defer server.Close()
go engine.Run()

// For each browser, the web service makes the credentials it puts in RTCIceServer:
user, pass := turn.EphemeralCredentials(secret, "alice", time.Now().Add(time.Hour))
log.Printf("RTCIceServer{urls: turn:127.0.0.1:3478, username: %q, credential: %q}", user, pass)
```

By default the server refuses to relay to loopback, private and link-local addresses, so it
cannot be used as a proxy into the network it runs in; `AllowedPeers` names the exceptions.
`ListenTCP` and `ListenTLS` add the transports clients behind a firewall-only-443 network
need. TURN relays UDP to peers even over TCP or TLS: only the leg to the server is a stream.

## Interop and limits

The stack is checked against other implementations, not only itself:
[`webrtc/interop`](../../webrtc/interop) is a small separate module that runs pion's
PeerConnection, DTLS and TURN against fib's on every push, and Chromium is exercised through the fib-examples pages — data channel echo, canvas
VP8, relay-only ICE through `turnserver`, and an SFU forwarding video with PLI passed back
to the publisher. A browser page must be served over HTTPS (or from `localhost`) for
`getUserMedia`, but the signalling endpoint can be plain HTTP.

What is deliberately left out: the provisional answer and rollback, bundle policies other
than max-bundle, ICE over TCP, resolving browsers' mDNS host candidates, FEC and sending
NACKs, transport-wide congestion control and bandwidth estimation, simulcast *sending*
(receiving it by RID works), RTP packetization of codecs, DTLS 1.3, SDES keying, statistics
beyond RTCP reports, and TURN on a shared `Mux`. Messages are limited by
`Configuration.MaxMessageSize` (256 KiB by default), which SDP advertises.

## Pitfalls

- **A callback must not block.** Running `OnMessage`/`OnTrack` work inline delays every later
  callback of that connection and the worker it runs on; hand slow work to a goroutine.
- **Packet buffers do not survive the callback.** `OnRTP` and `OnRTCP` packets are valid only
  during the call; copy what you keep. `DataChannelMessage.Data` you may keep.
- **ICE-lite needs a full peer.** Browsers are full agents; a `PeerConnection` marked
  `ICELite` refuses TURN servers and the relay-only policy, and a lite-to-lite pair never
  connects.
- **TURN does not share a `Mux`.** Give the `PeerConnection` an `Engine` and let it open its
  own port, or `NewPeerConnection` returns `ErrConfiguration`.
- **The server must keep the `PeerConnection`.** `Answer` does not return it; store it under
  the session id and close it on `DELETE` or on `failed`, or a vanished browser leaks one
  connection forever.
- **Do not set channel callbacks late.** `OnDataChannel` should set `OnMessage`/`OnOpen`
  before it returns: an early message arrives on a channel with no callback and is dropped.
- **Offer/answer must be serialized.** Two goroutines calling `CreateOffer` or
  `SetRemoteDescription` on one `PeerConnection` interleave signaling states; one signaller
  per connection.
- **Negotiation is not automatic.** `AddTrack` before the answer puts the track on a media
  section of the offer, so the answer sends it there; added later, it takes effect at the next
  negotiation — an SFU that adds a viewer's track after answering must offer again.

## See also

- [WebRTC: design, usage and limits](../webrtc.md) — the layers, threading, configuration and
  the interop matrix
- [TLS](10-tls.md) — DTLS 1.2, certificates and the `tls` package the media stack uses
- [Streaming media](17-media.md) — WHIP and WHEP endpoints on a media hub
- [Engine and configuration](02-engine.md) — UDP bindings, `AddBinding` and `OpenUDPPeer`
- [The README](../../README.md) — the feature table and the runnable examples
- [fib-examples](https://github.com/lesismal/fib-examples) — browser pages, SFU, datachannel and
  TURN programs
