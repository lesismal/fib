// Package rtmp implements RTMP (Adobe "Real-Time Messaging Protocol"
// specification, 2012) with Enhanced RTMP's codecs (H.265, AV1 and Opus by
// FourCC, see package flv): a server that takes publishers into a hub and
// plays hub streams to players, and a client that publishes to or plays
// from any RTMP server.
//
// Conn is the sans-IO core: the handshake, both the simple one (C2 and S2
// echo) and the Flash digest one ("complex handshake", HMAC-SHA256 under
// Adobe's player and server keys) that Flash and Adobe-derived clients
// check; the chunk stream in both directions, with the four chunk header
// formats, extended timestamps (also in the chunks that continue a
// message, and tolerated when a peer leaves them out there), chunk size
// changes and aborts; and the protocol control messages: acknowledgement
// windows, acknowledgements, set peer bandwidth and ping. Feed gives it what
// arrived, it sends through a callback, and it hands every other message to
// another; aggregate messages are split. ParseCommand and WriteCommand
// read and write the AMF0 and AMF3 command messages.
//
// Server is a fib.Handler. It answers connect, releaseStream, FCPublish,
// createStream, publish, play, getStreamLength, FCUnpublish, deleteStream
// and closeStream, and publishes to ServerConfig.Hub at path "app/stream"
// (rtmp://host/app/stream), with the query that the app or the stream name
// carried (?token=...) in hub.PublishInfo for the hub's hooks to check.
// The publish command reserves the path (hub.Hub.Reserve), so a second
// publisher hears NetStream.Publish.BadName at once, and the stream goes
// live when the publisher's metadata and sequence headers have made its
// tracks known; until then players that do not wait find no stream.
// Players start from the hub's GOP cache, with onMetaData and the sequence
// headers first, and a player that falls behind has its packets dropped
// (video until the next keyframe, hub.NewDropper) and, past a second limit
// or when its output stops moving for IdleTimeout, is disconnected, so a
// player never grows the server's memory. With ServerConfig.PlayWait a
// player waits for a stream not yet published, and for the next publisher
// after its own leaves, its timestamps increasing across the republish
// (flv.Timeline). Callbacks (OnConnect, OnPublish,
// OnPlay) authorize, and let a server run without a hub.
//
// Publish and Play are the client: they dial through a fib.Engine, run the
// digest handshake, and report through a callback once the server has
// accepted; nothing blocks a fib worker. A publishing ClientConn is a
// hub.Sink. Push relays a hub stream to a server, and Pull a server's
// stream into a hub.
//
// RTMPS is RTMP over TLS: serve it with fib/tls in front of the Server
// (fib.Binding{Addr: ":443", Handler: tls.NewServer(config, server)}),
// and the client dials rtmps:// URLs over fib/tls itself.
//
// Left out: RTMPE and RTMPT (encrypted and HTTP-tunnelled RTMP), shared
// objects (their messages are ignored), AMF3 replies (a client that
// connects with objectEncoding 3 gets AMF0 ones, which every client
// takes), recorded streams (play's start and duration are ignored, the
// stream is always live), and Enhanced RTMP's multitrack messages.
package rtmp
