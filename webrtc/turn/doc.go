// Package turn implements Traversal Using Relays around NAT, RFC 8656: a
// sans-IO client that gets a relayed address from a TURN server and sends
// and receives through it, and a sans-IO server core that hands out such
// addresses and relays for its clients. Package turnserver runs the server
// on a fib engine.
//
// Both speak TURN over UDP and relay UDP, which is what WebRTC uses: the
// client, Client, makes an allocation with the long-term credentials of RFC
// 8489 section 9.2 (the 401 challenge for the realm and nonce, a retry with
// a fresh nonce on 438, and a move to another server on 300), refreshes it
// before its lifetime is over, installs and refreshes a permission for each
// peer IP address, binds and refreshes channels, sends to peers in Send
// indications or, once a channel is bound, in ChannelData messages, and
// takes what the server relays back out of Data indications and ChannelData.
// Close deallocates. ICERelay connects an allocation to an ice.Agent as a
// relay candidate.
//
// The server, Server, keeps allocations by their 5-tuple, opens their
// relayed addresses through its host, checks credentials with a pluggable
// AuthFunc, issues nonces that expire without being stored, enforces
// permissions, channel bindings, lifetimes and quotas, and refuses to relay
// to loopback, private and other internal addresses unless told to, so that
// it cannot be used to reach into the network it runs in. It caps each
// allocation's permissions and channels, asks a host's PeerAllowed about a
// peer's port as well as its address, tells ServerConfig.OnEvent of
// allocations made, refreshed and freed and of requests refused, and
// counts what it holds and relays in Stats. StaticAuth,
// EphemeralAuth and EphemeralCredentials cover fixed users and the
// time-limited credentials of the TURN REST API that WebRTC services hand
// out.
//
// Left out: TURN over TCP and TLS, and relaying TCP (RFC 6062); DTLS to the
// server; dual allocations (ADDITIONAL-ADDRESS-FAMILY), EVEN-PORT and
// RESERVATION-TOKEN; DONT-FRAGMENT, which the server refuses with 420 as RFC
// 8656 section 7.2 has a server that cannot set the DF bit do; the SHA-256
// password algorithm, USERHASH and the nonce cookie of RFC 8489 section
// 9.2; the ICMP attribute; and the mobility extension of RFC 8016. The
// server answers Binding requests, as TURN servers do for ICE's
// server-reflexive candidates, without credentials.
//
// Output goes through callbacks set at construction (ClientConfig.Send,
// ServerConfig.Send, Relay.SendTo), whose buffers are valid only during the
// call. The client does not lock: its caller serializes calls to it. The
// server may be called from any number of goroutines at once, and relays
// for different allocations in parallel, each allocation behind a lock of
// its own; it opens relayed addresses asynchronously, answering an Allocate
// when ServerConfig.AllocateRelay's done runs, so that a host whose sockets
// take a while to open need not hold up the goroutine the request came in
// on. It holds no lock while it calls back into the host.
package turn
