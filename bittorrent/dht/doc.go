// Package dht implements a node of the BitTorrent mainline DHT (BEP 5),
// the Kademlia network clients find each other's peers through without a
// tracker, wire-compatible with libtorrent, Transmission and
// anacrolix/dht.
//
// A Node keeps a routing table of k-buckets (K = 8) that split around its
// own ID, with the good, questionable and bad node states of BEP 5, a
// replacement cache a bucket, a ping before a questionable node is evicted,
// and a refresh of each bucket that has been quiet for 15 minutes. Only
// nodes that answered a query of ours enter it, at most one a public IP. It
// answers ping, find_node, get_peers and announce_peer (with implied_port),
// sample_infohashes (BEP 51), and get and put of immutable and mutable
// items (BEP 44, ed25519 from the standard library); get_peers hands out
// tokens bound to the asker's IP, valid for 5 to 10 minutes, and the peers
// announced are kept for 30 minutes, bounded in torrents and peers a
// torrent. Queries are rate-limited a source IP.
//
// Its lookups (FindNode, GetPeers, Announce, Get, GetMutable, Put) are the
// iterative lookups of Kademlia: Alpha (3) queries in flight to the
// closest nodes known, until the K closest that did not fail have all
// answered. GetPeers hands the peers over as they arrive; Announce then
// announces to the K closest that gave a token, as Put stores an item.
//
// BEP 42: a Node's ID is derived from its external address when it is
// known (Config.PublicIP, or the "ip" key of responses, by vote), and
// Config.EnforceNodeID keeps nodes whose IDs do not match their addresses
// out of the table. BEP 43: Config.ReadOnly makes a node that queries but
// answers nothing. BEP 32: a Node is of one address family, and an IPv6
// one (Config.IPv6) speaks "nodes6"; a client on both runs two Nodes.
//
// The Node owns no socket. HandleDatagram takes the datagrams that arrive,
// claiming those that are bencoded dictionaries, and Config.Send sends,
// so that the DHT, uTP and the UDP tracker client can share one fib UDP
// socket, as BitTorrent clients do. Listen runs a Node on a fib UDP binding
// of its own. Timers are time.AfterFunc: one a query in flight, and one
// that maintains the node every few seconds.
//
//	node, err := dht.Listen(engine, ":6881", dht.Config{})
//	_ = node.Bootstrap(ctx) // routers, and saved nodes from AddNodes
//	n, err := node.Announce(ctx, infoHash, 0, true, func(peers []netip.AddrPort) {
//		// connect to peers
//	})
//
//	// Sharing a socket: in the UDP handler,
//	if !node.HandleDatagram(from, b) {
//		// uTP or the tracker client
//	}
//
// State saves the node's ID and routing table across a restart.
package dht
