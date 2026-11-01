// Package rtpcodec carries the media stack's tracks over RTP: it turns the
// av.Packets of a track into RTP packets of the track's payload format and
// back, and reads and writes the SDP media sections that describe those
// formats. RTSP and WebRTC are built on it; it never touches a socket.
//
// The payload formats are H.264 (RFC 6184: single NAL unit packets, STAP-A
// and FU-A in packetization mode 1, with STAP-B, MTAP16, MTAP24 and FU-B
// read as well), H.265 (RFC 7798: single NAL unit packets, aggregation
// packets and fragmentation units, without DONL), AV1 (the AOM RTP payload
// format v1.0: OBU elements aggregated or fragmented under an aggregation
// header, the temporal unit ended by the marker), AAC as mpeg4-generic
// (RFC 3640 AAC-hbr and AAC-lbr: AU headers, several access units in a
// packet, an access unit fragmented over packets) and as MP4A-LATM (RFC
// 6416, with the StreamMuxConfig out of band or in band), Opus (RFC 7587)
// and G.711 PCMU and PCMA (RFC 3551).
//
// A Format is one payload format of a media section: the payload type, the
// clock rate and the av.Track it carries. ParseMedia and ParseSession read
// formats from SDP (package webrtc/sdp), taking the codec configuration
// from a=fmtp: sprop-parameter-sets, sprop-vps/sps/pps, config= and the AU
// header lengths. AV1's fmtp (profile, level-idx, tier) carries no
// configuration, which comes from the sequence headers sent in band.
// NewFormat makes the format a track is sent in, and Format.Media writes
// its media section.
//
// A Packetizer sends a track: it cuts each access unit into packets of at
// most an MTU, aggregates small NAL units and OBUs, sends the parameter sets
// (or AV1's sequence header) ahead of every keyframe so a receiver that
// joins late can decode, leaves out AV1's temporal delimiters, and stamps the
// packets from a random initial sequence number and timestamp. Its
// SenderReport ties the RTP timestamps to wall-clock time for RTCP.
//
// A Depacketizer receives one: it reassembles access units (by marker bit,
// or by a change of timestamp when a marker is lost), unwraps 32-bit
// timestamps into media time, drops what a gap in the sequence numbers
// damaged and has video wait for the next keyframe after it, and follows
// parameter sets and sequence headers sent in band, replacing its Track
// when they change the configuration. RTP carries only presentation times.
// AV1 temporal units come in presentation order, so their decode times are
// the same; for H.264 and H.265 video with B-frames it recovers decode
// times from the reordering depth the SPS signals (H.264
// max_num_reorder_frames, H.265 sps_max_num_reorder_pics), assuming the
// deepest reasonable one when the SPS does not say.
//
// Over UDP packets come out of order or not at all; a Reorder buffer, sized
// and timed by the caller, puts them back in order before the Depacketizer
// sees them. Everything is bounded: the access unit being reassembled by
// DepacketizerConfig.MaxAccessUnit, the Reorder buffer by its size.
//
// Left out: the interleaved packetization mode of H.264 (whose packets are
// read but not reordered by decoding order number), DONL in H.265, AV1's
// scalability structures beyond passing their OBUs through, the RTP
// payload format of MP3, and mpeg4-generic modes other than AAC's.
package rtpcodec
