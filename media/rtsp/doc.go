// Package rtsp implements RTSP 1.0 (RFC 2326) for fib's media stack: a
// Server that plays the streams of a hub to RTSP players and records into
// it what RTSP publishers send, and a Client that plays a stream from an
// RTSP server or publishes one to it. Media travels as RTP (package
// rtpcodec), interleaved on the RTSP connection (RTP/AVP/TCP) or over UDP
// unicast (RTP/AVP).
//
// The message layer is sans-IO: a Parser splits what a connection reads
// into requests, responses and interleaved frames, bounding each header
// and body, and Request, Response and AppendInterleaved write them.
// ParseTransports, ParseSession and ParseRTPInfo read the headers RTSP
// adds to HTTP's.
//
// A Server is a fib.Handler; bind it to a TCP address:
//
//	h := hub.New(hub.Config{})
//	srv := rtsp.NewServer(rtsp.ServerConfig{Hub: h})
//	engine, _ := fib.New(fib.DefaultConfig(), fib.Binding{Addr: ":8554", Handler: srv})
//	_ = srv.ListenUDP(engine) // RTP/AVP over UDP too, on :8000 and :8001
//
// rtsp://host:8554/live/cam plays or records the hub stream "live/cam".
// The server answers OPTIONS, DESCRIBE (the SDP of the stream's tracks),
// SETUP, PLAY (with Range and RTP-Info), PAUSE, TEARDOWN, GET_PARAMETER and
// SET_PARAMETER (keep-alives), and ANNOUNCE, SETUP with mode=record and
// RECORD for publishers. A player starts with the hub's cached GOP (over
// UDP, which cannot take its burst, at the next keyframe) and gets RTCP
// sender reports so it can line the tracks up. A player over TCP is cut
// back to keyframes (hub.NewDropper) when it falls behind, and dropped
// when it falls further. Over UDP its RTP goes to the socket as it comes;
// once the socket refuses a video datagram, video waits for the next
// keyframe, and a player whose RTCP receiver reports keep saying it loses
// more than ServerConfig.MaxPlayerLoss is closed.
// With ServerConfig.PlayWait a player outlives its publisher and carries
// on with the next one of the same codecs, its RTP times going on from
// where they were. A recorder's packets go through a reorder buffer when
// they come over UDP, and its streams are lined up by its RTCP sender
// reports; the parameter sets it sends in band update the hub's tracks.
// Sessions end with their connection, on TEARDOWN, or after
// ServerConfig.SessionTimeout without a request or RTCP. Authentication is
// Digest (RFC 2617 with MD5, as RTSP clients speak it) or Basic, through
// ServerConfig.Password; the hub's OnPublish and OnSubscribe hooks decide
// who may publish and play what.
//
// The UDP bindings are shared by every session: the server tells a
// session's datagrams apart by the client's address and port, which SETUP
// gave, or for a recorder behind a NAT that changed its port, by the SSRC
// its Transport declared, from the same IP. A session's RTP goes out from
// the server's RTP port through Engine.OpenUDPPeer.
//
// A Client dials through a fib.Engine and is asynchronous: Play
// DESCRIBEs, SETs UP every track it can carry and PLAYs, and hands the
// tracks and packets to callbacks; Publish ANNOUNCEs tracks, SETs them UP
// for recording and RECORDs, after which WritePacket sends. Both answer a
// 401 with Digest or Basic credentials from the URL or the config, send
// keep-alives, and take TCP interleaved or UDP, for which the client adds
// a pair of UDP bindings to its engine for each stream. PlayToHub
// publishes what a client plays into a hub (a relay or pull proxy), and
// PublishFromHub publishes a hub stream to a server (a push relay).
//
// Left out: RTSP 2.0, RTSP over TLS (rtsps) and over HTTP tunnels,
// multicast, the Range of a PLAY that seeks (every stream is live, played
// from now), Scale and Speed, and sessions that outlive their connection:
// a session belongs to the connection that set it up, as every common
// client keeps it open. The server does not send RTCP receiver reports to
// recorders.
package rtsp
