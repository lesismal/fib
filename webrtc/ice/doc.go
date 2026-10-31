// Package ice implements Interactive Connectivity Establishment for WebRTC:
// a sans-IO agent that finds a working pair of addresses between two peers
// behind NATs and keeps checking that the peer still consents to receive.
//
// It implements RFC 8445 (full and lite agents, the controlling and
// controlled roles with tie-breakers and role conflicts, candidate pairs,
// their priorities, foundations and frozen and waiting states, paced
// ordinary checks and triggered checks, peer-reflexive candidates, regular
// nomination, and ICE restarts), trickle ICE (RFC 8838: candidates on both
// sides may arrive at any time, and end-of-candidates), consent freshness
// (RFC 7675), and the a=candidate attribute of RFC 8839.
//
// An Agent is given its transports as opaque LocalIDs, each a socket or a
// TURN allocation, and sends by naming one and a remote address. It knows a
// pair by that LocalID and the remote address, never by a connection object:
// a socket shared by several pollers may deliver a peer's reply on any of
// them. Everything the agent sends goes through Config.Send, every packet
// the caller's socket receives that stun.IsMessage accepts comes in through
// HandlePacket, and time passes through NextTimeout and HandleTimeout.
//
// WebRTC bundles everything onto one component of one data stream, which is
// all the agent handles: candidates for other components are refused. It
// leaves out ICE over TCP (RFC 6544), aggressive nomination, which RFC 8445
// deprecates, and resolving the mDNS names browsers give host candidates,
// which its caller does. It does not add the peer-reflexive local candidate
// a check may reveal: pairs are keyed by their local base, so such a valid
// pair is the very pair that was checked. Relay candidates come from the
// caller, who runs the TURN allocation and relays what the agent sends
// through it.
package ice
