// Package tracker implements BitTorrent trackers and their clients, over
// HTTP and HTTPS (BEP 3, with compact peer lists, BEP 23, IPv6 peers, BEP
// 7, and scrapes, BEP 48) and over UDP (BEP 15, with the URL data of BEP
// 41), on fib's sockets.
//
// The clients:
//
//   - HTTPClient announces and scrapes through a fib/http Client, which
//     brings HTTPS, keep-alive and HTTP/2 with it.
//   - UDPClient speaks BEP 15 without a socket of its own: it sends through
//     a function of the caller's and is handed the replies through
//     HandleDatagram, so a BitTorrent client can share the one UDP socket
//     it speaks uTP and the DHT on (IsUDPReply tells a tracker's replies
//     from theirs). ListenUDPClient gives it fib UDP bindings of its own
//     instead. It keeps connection IDs for their minute and retransmits
//     after 15 * 2^n seconds, as BEP 15 says, with a configurable base and
//     cap.
//   - Client picks one of the two by the URL's scheme, and Tiers announces
//     to the tiers of a torrent's announce-list as BEP 12 says.
//
// Server is a tracker on both protocols at once, over one Store: an
// fib/http handler (ServeHTTP) and the fib.Handler of UDP bindings (UDP).
// Its UDP connection IDs are HMACs of the client's address and the minute,
// so it keeps no state per client. MemoryStore, its default Store, bounds
// the torrents and peers it keeps, expires peers that stop announcing, and
// hands out random peers. ServerConfig has the hooks of a private tracker:
// an allow-list of info hashes and a check of each request, with its
// passkey.
//
//	srv := tracker.NewServer(tracker.ServerConfig{})
//	engine, _ := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":6969", Handler: fibhttp.NewHandler(srv)},
//		fib.Binding{Network: "udp", Addr: ":6969", Handler: srv.UDP()})
//	go engine.Run()
//
//	udp, _ := tracker.ListenUDPClient(engine, tracker.UDPClientConfig{})
//	c := &tracker.Client{
//		HTTP: tracker.NewHTTPClient(fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())),
//		UDP:  udp,
//	}
//	tiers := tracker.NewTiers(mi.Trackers())
//	resp, url, err := tiers.Announce(ctx, c, tracker.AnnounceRequest{
//		InfoHash: mi.Info.InfoHashV1(), PeerID: id, Port: 6881, Left: size,
//		Event: tracker.EventStarted,
//	})
package tracker
