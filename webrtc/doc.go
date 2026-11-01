// Package webrtc runs WebRTC peers on fib: a PeerConnection that speaks to
// browsers and other WebRTC stacks with data channels and RTP media, over
// the UDP ports of a fib engine.
//
// It puts the sans-IO protocols of its subpackages together: ICE (package
// ice, RFC 8445, trickle RFC 8838, consent RFC 7675), with relay candidates
// through TURN (package turn, RFC 8656), DTLS 1.2 with DTLS-SRTP (package
// fib/tls, RFC 6347 and RFC 5764), SRTP and SRTCP (package srtp, RFC 3711
// and RFC 7714), RTP and RTCP (package rtp), SCTP data channels (package
// sctp, RFC 8831 and RFC 8832), and offer and answer (package sdp, JSEP as
// RFC 9429 has it, with BUNDLE, RFC 9143, and rtcp-mux).
//
// A Mux is a UDP port shared by any number of PeerConnections, which is
// what a server with thousands of peers wants: datagrams are sorted by the
// RFC 7983 rules and routed by the address they come from, and a STUN
// request from a new address by the ICE ufrag its USERNAME names. A
// PeerConnection without a Mux opens a port of its own, as a client does.
// Everything a PeerConnection keeps it keys by remote address, never by
// fib connection, since fib's ReusePort may deliver one address's
// datagrams on several.
//
// The API follows RTCPeerConnection: CreateOffer, CreateAnswer,
// SetLocalDescription, SetRemoteDescription, AddICECandidate and
// OnICECandidate, the connection, ICE, gathering and signaling states,
// CreateDataChannel and OnDataChannel, transceivers in unified plan,
// AddTrack and OnTrack. Answer and Offer do a whole exchange for signaling
// that carries no trickled candidates, such as WHIP. Media is RTP: the
// application writes RTP packets to a TrackLocal and receives them from a
// TrackRemote, and the PeerConnection rewrites SSRCs, payload types and
// header extension ids, protects and unprotects them, answers with
// receiver and sender reports, and passes on key frame requests: what an
// SFU forwards, with no codec of its own. Codecs are negotiated by their
// SDP description against Configuration.Codecs.
//
// Callbacks from fib's workers and from timers are serialized by a mutex
// per PeerConnection, and one timer per PeerConnection drives ICE, DTLS,
// SCTP, TURN and RTCP. Application callbacks run outside that mutex, in
// order, one at a time.
//
// Left out: the provisional answer and rollback, BundlePolicy other than
// max-bundle, ICE over TCP, resolving the mDNS names of browsers' host
// candidates (a peer's checks reveal its address as a peer-reflexive
// candidate anyway), RTX and FEC, transport-wide congestion control
// feedback and bandwidth estimation, simulcast sending (receiving it, by
// RID, is supported), the RTP packetization of codecs, DTLS 1.3, SDES
// keying, and statistics beyond the RTCP reports. A TURN relay needs a
// PeerConnection with a port of its own, not a shared Mux; the
// PeerConnection reaches TURN servers over UDP, TCP
// (turn:host?transport=tcp) or TLS (turns:host), and the server relays to
// the peer over UDP.
package webrtc
