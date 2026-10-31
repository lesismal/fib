// Package sctp implements SCTP as WebRTC runs it over DTLS (RFC 8261), and
// the data channels built on it (RFC 8831, with the Data Channel
// Establishment Protocol of RFC 8832).
//
// An Association is sans-IO. Its packets are DTLS application data: the
// caller hands it each one received with HandlePacket, and it sends
// through Config.Output, one SCTP packet per call, the buffer valid only
// for the call. Time comes from the caller too: HandleTimeout runs what is
// due at NextTimeout. Nothing here starts a goroutine, takes a lock or
// sleeps, and calls must not overlap.
//
// From RFC 9260 it implements association setup by either side or both at
// once (section 5.2.1), with an HMAC-SHA256 state cookie; DATA and SACK
// with gap blocks and duplicate TSNs; fragmentation and reassembly up to a
// configurable maximum message size, ordered and unordered delivery; the
// retransmission timer with Karn's rule (section 6.3), fast retransmit
// and congestion control (section 7); delayed SACKs (every second packet
// or 200ms); the receiver window; HEARTBEAT; SHUTDOWN and ABORT; and the
// CRC32c checksum. Partial reliability follows RFC 3758 (FORWARD TSN)
// with RFC 7496's limits on retransmissions and lifetime, and streams are
// reset as RFC 6525 describes, which is how data channels close.
//
// Data channels take even stream ids on the DTLS client and odd ones on
// the server, open with DCEP OPEN and ACK or by prior agreement
// (negotiated), and carry strings and binary as PPIDs 51 and 53, empty ones
// as 56 and 57.
//
// It leaves out what a WebRTC association does not use: multi-homing (DTLS
// gives one path), association restart (a peer that restarts is not
// recognized), I-DATA and message interleaving (RFC 8260), SCTP-AUTH (RFC
// 4895), the zero checksum (RFC 9653), ECN, path MTU discovery, and the
// RE-CONFIG requests other than resetting outgoing streams, which it
// answers with "denied". A peer's reset request whose data has not all
// arrived is answered "in progress" for the peer to repeat. Gap-acknowledged
// chunks are taken as delivered; a peer that reneges is not supported.
package sctp
