// Package httpflv serves hub streams as HTTP-FLV, an FLV file that never
// ends sent as the body of a GET response, and as WebSocket-FLV, the same
// bytes in binary WebSocket messages: what web players such as flv.js and
// mpegts.js, and ffmpeg, play with low latency without RTMP.
//
// Handler is a fib/http handler. GET /app/stream.flv plays the hub stream
// "app/stream" (Config.Path changes the mapping), with the request's query
// in hub.SubscribeInfo for the hub's OnSubscribe hook. A request with a
// WebSocket upgrade, over HTTP/1.1 or as HTTP/2's and HTTP/3's extended
// CONNECT, is answered with WebSocket-FLV. A player starts from the hub's
// GOP cache, the FLV header, onMetaData and the sequence headers first;
// H.265, AV1 and Opus are written with Enhanced RTMP's FourCC headers, or
// H.265 with codec id 12 when Config.Encoder asks for it.
//
// Nothing waits on a slow player. Writes are queued without waiting, and
// a player whose backlog passes Config.WriteQueueLimit has its packets
// dropped, video until the next keyframe (hub.NewDropper): on HTTP/1 and
// WebSocket the backlog is the connection's queued output, on HTTP/2 and
// HTTP/3 the stream's flow control holding more than 64 KiB. An HTTP/1
// player past Config.MaxWriteQueue, or one whose output has not moved for
// Config.IdleTimeout, is disconnected.
//
// A player waits up to Config.Wait for a stream that has no publisher yet,
// or for the next publisher after its own leaves; its FLV timestamps keep
// increasing across the republish (flv.Timeline).
//
// Left out: an HTTP-FLV pull client (rtmp.Pull relays from RTMP), which
// is not written yet: it would feed the body of a fib/http client
// response (ClientConfig.StreamResponseBody, ClientResponse.OnBody) into
// flv.Demuxer. Serving recorded files is left out too.
package httpflv
