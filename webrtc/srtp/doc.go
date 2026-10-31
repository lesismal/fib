// Package srtp implements the Secure Real-time Transport Protocol (RFC
// 3711) and its RTCP counterpart, SRTCP, as WebRTC keys them through
// DTLS-SRTP (RFC 5764).
//
// A Context holds the session keys one side of a session uses to protect
// what it sends, or to unprotect what it receives: NewContexts splits the
// keying material DTLS exports into the two, by the layout of RFC 5764
// section 4.2. The protection profiles are AES_CM_128_HMAC_SHA1_80 and
// AES_CM_128_HMAC_SHA1_32 (RFC 3711, RFC 5764) and AEAD_AES_128_GCM and
// AEAD_AES_256_GCM (RFC 7714). Session keys are derived from the master key
// and salt with the AES-CM PRF of RFC 3711 section 4.3, with a key
// derivation rate of zero, so they never change.
//
// ProtectRTP, UnprotectRTP, ProtectRTCP and UnprotectRTCP append their
// result to a dst the caller gives, which may be the input's own bytes
// for work in place. A Context keeps per-SSRC state: the rollover counter
// of RFC 3711 section 3.3.1, estimated for each packet received as its
// Appendix A describes, the SRTCP index it sends, and replay windows
// (section 3.3.2) of a configurable size. Once a Context has seen an SSRC,
// protecting and unprotecting its packets does not allocate.
//
// A Context is not safe for concurrent use. The package leaves out MKIs,
// key derivation rates other than zero, the NULL cipher and the
// encrypted header extensions of RFC 6904, none of which WebRTC uses.
package srtp
