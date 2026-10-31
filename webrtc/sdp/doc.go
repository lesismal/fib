// Package sdp parses and writes session descriptions (RFC 8866) as WebRTC
// peers exchange them in offers and answers (JSEP, RFC 9429).
//
// A SessionDescription keeps every line it was parsed from, in order, as a
// type letter and an uninterpreted value, so marshaling one that was parsed
// gives back the same bytes: attributes and line types this package knows
// nothing of survive, as does the order a browser wrote them in, which some
// peers depend on. The typed accessors read those lines when asked, and the
// setters and builders edit them in place, so a description can be taken
// from a peer, changed and sent on without losing what it did not change.
//
// The accessors cover what a WebRTC peer needs: BUNDLE groups (RFC 9143),
// mid (RFC 5888), setup (RFC 4145, RFC 8842), fingerprint (RFC 8122), the ICE
// attributes of RFC 8839 (ice-ufrag, ice-pwd, ice-options, ice-lite,
// candidate, end-of-candidates), rtcp-mux (RFC 5761), rtcp-rsize
// (RFC 5506), rtpmap and fmtp (RFC 8866), rtcp-fb (RFC 4585), extmap
// (RFC 8285), ssrc and ssrc-group (RFC 5576), msid (RFC 8830), the
// direction attributes, sctp-port and max-message-size (RFC 8841), rid
// (RFC 8851) and simulcast (RFC 8853), and the m=application UDP/DTLS/SCTP
// webrtc-datachannel section of RFC 8841.
//
// A candidate is kept as the attribute's text, "candidate:..."; parsing it
// is the ICE agent's business. Parsing checks a description's structure:
// that it starts with v=0, o= and s=, has a t= line, that every line is a
// lowercase letter, '=' and a value, that each m= line is well formed and
// that a media section holds no session-only lines. It does not check the
// values of attributes, whose accessors skip a malformed one or report it,
// and it accepts any line type letter, which RFC 8866 would have it reject,
// so that what a peer adds later still round-trips. Lines may end in CRLF or
// a bare LF; Marshal writes CRLF. Nothing here deals with the k=, z= or r=
// lines beyond keeping them, nor with SDP's multicast and port-count forms
// beyond parsing them.
package sdp
