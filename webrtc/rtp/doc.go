// Package rtp parses and builds the RTP and RTCP packets WebRTC media is
// carried in.
//
// RTP (RFC 3550 section 5) is read in place by Header.Unmarshal and
// Packet.Unmarshal: the payload, the CSRC list and the header extension
// block refer to the bytes given, so a Packet reused from one packet to
// the next parses without allocating. Header extensions follow RFC 8285,
// in both its one-byte and two-byte forms; GetExtension and Extensions read
// them without allocating, SetExtension and DelExtension rewrite the block,
// and helpers encode the values WebRTC sends in them: the MID and RID
// (RFC 8843, RFC 8852), abs-send-time, the transport-wide sequence number of
// draft-holmer-rmcat-transport-wide-cc-extensions-01 and the audio level of
// RFC 6464. SeqUnwrapper extends 16-bit sequence numbers to 64 bits.
//
// RTCP packets are each a type with Unmarshal, Append and MarshalSize:
// SenderReport, ReceiverReport, SourceDescription, Goodbye and
// ApplicationDefined (RFC 3550 section 6), NACK and PLI (RFC 4585), FIR
// (RFC 5104), REMB (draft-alvestrand-rmcat-remb) and TransportCC (the
// feedback of draft-holmer-rmcat-transport-wide-cc-extensions-01, which
// TransportCCRecorder builds from the arrival times of packets). NextRTCP
// walks a compound packet without allocating, leaving the caller to decode
// each packet into a value it reuses; UnmarshalRTCP does the same into new
// values. Neither insists on the compound packet rules of RFC 3550 section
// 6.1, so the reduced-size RTCP of RFC 5506, a single feedback packet on its
// own, parses as any other.
//
// The package leaves out header extension profiles other than RFC 8285's,
// whose blocks it keeps but cannot look inside, the RTCP extended reports
// of RFC 3611, and the payload formats of codecs.
package rtp
