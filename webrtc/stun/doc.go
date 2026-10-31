// Package stun implements the STUN message format of RFC 8489, the attributes
// ICE (RFC 8445) and TURN (RFC 8656) carry in it, and a sans-IO client
// transaction that retransmits a request as RFC 8489 section 6.2.1 asks.
//
// A Message is decoded in place: Decode keeps the buffer it is given and the
// values its getters return are slices of it, so parsing a datagram does not
// allocate once the Message has room for its attributes. A Message is built
// the same way, attribute by attribute into Raw, so that MESSAGE-INTEGRITY,
// MESSAGE-INTEGRITY-SHA256 and FINGERPRINT, which cover every byte before
// them, are computed over exactly the bytes that are sent.
//
// MESSAGE-INTEGRITY is computed with a key the caller derives: the password
// itself for short-term credentials (ICE), or LongTermKey for long-term ones
// (TURN). The package does not apply the OpaqueString profile of RFC 8265 to
// passwords, nor SASLprep: a caller with non-ASCII credentials prepares them
// first. It does not implement the server side of the long-term mechanism
// (nonce cookies, USERHASH lookups), the STUN URI, DNS discovery, or the
// ALTERNATE-SERVER redirection a client may follow; the attribute types of
// those are defined so that callers can.
package stun
