// Package rtc is the WebRTC ingest and egress of fib's media stack: a WHIP
// endpoint (RFC 9725) that publishes what a browser or an encoder sends
// into the hub, a WHEP endpoint (draft-ietf-wish-whep) that plays hub
// streams to browsers, and the WHIP and WHEP clients that push a hub
// stream to another server or pull one from it. It is built on fib's own
// WebRTC stack (package webrtc) for the PeerConnection and on package
// rtpcodec for the RTP payload formats, and it never transcodes.
//
// Server is a fib/http Handler. POST an SDP offer (application/sdp) to
// Config.WHIPPrefix plus a hub path to publish that path, or to
// Config.WHEPPrefix plus a path to play it; the answer comes back with 201
// Created, once its candidates are gathered, and a Location under
// Config.ResourcePrefix, which DELETE ends and PATCH trickles candidates to
// (application/trickle-ice-sdpfrag, RFC 8840). An ICE restart is not
// supported and is answered 422, as RFC 9725 section 4.3.2 asks. OPTIONS
// answers CORS preflights when Config.AllowOrigin is set, and the ICE
// servers clients are to use can be sent as Link headers. Authorization
// takes a bearer token through Config.Authorize (a refusal is 401 without
// a token, 403 with one), and the hub's OnPublish and OnSubscribe hooks
// see the request's query. A publish to a path that has a publisher is
// answered 409 Conflict, a play of a path that has none 404, an offer with
// nothing to send or receive 406, and one past Config.MaxSessions 503.
//
// Every PeerConnection of a server shares one UDP port, a webrtc.Mux the
// application makes on its fib engine and gives in Config.WebRTC, with
// ICELite for a server with a public address:
//
//	h := hub.New(hub.Config{})
//	mux, _ := webrtc.NewMux(engine, webrtc.MuxConfig{Addr: ":8189"})
//	srv, _ := rtc.NewServer(rtc.Config{Hub: h, WebRTC: webrtc.Configuration{Mux: mux, ICELite: true}})
//	router.Handle("/*", srv)
//
// A WHIP session accepts H.264 (packetization mode 0 or 1), H.265, Opus,
// PCMU and PCMA, whatever the publisher offers of them, and publishes one
// hub track per media section: the depacketizer of each, behind a reorder
// buffer, follows the parameter sets sent in band, and the publisher's
// tracks change (Publisher.SetTracks) when they do. Packets are timed from
// when they arrive, a track's first at the session's start, which keeps
// the tracks in step to within the network's jitter. The publisher is
// asked for a keyframe (a PLI) when a track starts, after a loss until a
// keyframe comes, when a viewer asks for one, and every
// Config.KeyFrameInterval if set, at most once a Config.PLIInterval.
//
// A WHEP session sends the hub stream's tracks WebRTC carries: H.264,
// negotiated in packetization mode 1 with the browser's profile-level-id
// that decodes the stream's profile (its own, or one containing it),
// H.265 when the browser offers it, Opus, PCMU and PCMA. AAC, MP3 and AV1
// are left out, with a log line the first time each is met, as WebRTC
// cannot carry the first two and rtpcodec has no AV1 payload format. A
// viewer starts at the hub's cached GOP, held while it connects; its RTP
// times are continuous for as long as it plays, across a republish of the
// stream (which it waits Config.RepublishWait for), and a keyframe
// request it makes goes to the stream's publisher when that is a WHIP
// session of the same server, or to Config.OnKeyFrameRequest otherwise.
//
// Nothing waits on a viewer. Fib's WebRTC sends RTP straight to the
// socket, with no queue to measure and no congestion control, so a slow
// viewer is told by its receiver reports instead: while the fraction of
// packets it reports lost is over Config.MaxLoss, its packets are dropped,
// video until the next keyframe (hub.NewDropper). What a viewer holds
// while it connects is bounded by Config.MaxHeldBytes, each session by
// Config.ConnectTimeout until it connects and by ICE consent after, and
// the sessions by Config.MaxSessions.
//
// Pull plays a WHEP endpoint and Push publishes to a WHIP endpoint, each
// on a PeerConnection of its own, with the signaling over a fib/http
// client; PullToHub and PushFromHub join them to a hub, as relays do. The
// clients never block their caller: the offer is made and sent on a
// goroutine of their own, and their callbacks report what follows.
//
// Left out: ICE restarts, simulcast (only the first layer of a section is
// received), sending more than one stream per kind than the offer has
// sections for, RTX and FEC, transcoding, renegotiation when a stream's
// codec changes (the track of the old codec stops), and A/V
// synchronization from RTCP sender reports.
package rtc
