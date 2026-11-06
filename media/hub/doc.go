// Package hub joins the protocols of fib's media stack: a registry of
// streams by path, each fed by one Publisher and fanned out to any number
// of subscribers, so a stream an RTMP encoder publishes plays over RTSP,
// HTTP-FLV, HLS or WebRTC. Packets are in the canonical form of package
// av, and no protocol knows about another.
//
// A protocol's ingest calls Hub.Publish with the stream's tracks, then
// Publisher.WritePacket for every access unit, and Publisher.Close when
// the publisher leaves. An egress calls Hub.Subscribe with a Sink, which
// hears OnTracks, then OnPacket for every packet, and OnClose at the end.
// Config's hooks (OnPublish, OnSubscribe) are where authentication goes,
// for every protocol at once.
//
// An ingest that learns its tracks only from what it receives (SRT's
// MPEG-TS, an RTMP publisher before its metadata) calls Hub.Reserve
// instead, while it can still refuse its peer: the path is claimed, and
// OnPublish heard, at once, so a second publisher is turned away in the
// handshake; the stream goes live, as Publish would make it, at the
// reserved Publisher's first SetTracks, and until then subscribers either
// wait (SubscribeOptions.Wait) or find nothing.
//
// Fan-out is synchronous: each stream has one mutex, and WritePacket calls
// every sink's OnPacket under it, from the publisher's goroutine, usually
// a fib worker. A sink therefore must not block (it queues onto its
// connection and drops when that is too far behind, as NewDropper does)
// and must not call into the hub, with one exception: Subscription.Close
// never blocks and may be called from anywhere, a sink's own callbacks
// included, because it only marks the subscription when the stream's
// mutex is busy and leaves the unlinking to whoever holds it. The hub's
// hooks are called without any of its locks held.
//
// Each stream keeps a GOP cache so a new subscriber starts at once with a
// picture rather than waiting for the next keyframe: a stream with video
// keeps every packet from the last video keyframe on (audio included);
// an audio-only stream keeps its last second. The cache is bounded by
// Config.GOPCacheBytes: a GOP that outgrows it is dropped and caching
// resumes at the next keyframe. Subscribe delivers OnTracks and the cache
// under the stream's mutex, so the subscriber's packets continue it with
// nothing lost or out of order. A subscriber without a cache to start
// from (NoGOPCache, or a cache dropped) gets video from the next keyframe
// and audio at once.
//
// A subscription with SubscribeOptions.Wait may exist before its
// publisher, which is how a player that connects first, or a relay that
// reconnects, keeps its place: it hears OnTracks when a publisher
// arrives, survives the publisher leaving for up to Wait, and hears
// OnTracks again from the next one. SubscribeOptions.StartWait, when set,
// bounds the wait for the first publisher apart from Wait, as an egress
// that waits for a stream to start and for it to come back for different
// times does. Packet times are the publisher's own; a republish may
// restart them, and SetTracks may keep them or restart them: the egresses
// keep their timestamps going on across either (Publisher.SetTracks says
// how they tell the two apart).
//
// Everything is bounded: the cache by GOPCacheBytes, streams by
// MaxStreams, subscriptions by MaxSubscribersPerStream, and each sink's
// queue by the sink itself. A stream with neither publisher nor
// subscription leaves the registry. Hub.Streams returns a snapshot of
// every stream, Hub.Stream one of a single path, and Hub.Close ends them
// all.
package hub
