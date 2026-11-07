package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"sync"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bufferpool"
)

// The defaults of UDPClientConfig, BEP 15's.
const (
	DefaultUDPTimeout        = 15 * time.Second
	DefaultUDPRetransmits    = 8
	connectionIDLifetime     = time.Minute
	maxCachedConnectionIDs   = 1024
	maxRetransmitWait        = 2 * time.Hour
	maxPendingUDPTransaction = 1 << 16
)

// UDPClientConfig configures a UDPClient.
type UDPClientConfig struct {
	// Send sends a datagram to a tracker. With a socket shared with uTP
	// and the DHT, it sends on that socket, and the socket's reader hands
	// the client every datagram that may be a tracker's (see IsUDPReply)
	// through HandleDatagram. ListenUDPClient sets it to send from
	// bindings of the client's own. It may keep b.
	Send func(to netip.AddrPort, b []byte) error
	// Timeout is how long the client waits for an answer before it sends
	// a request again, doubling each time, and Retransmits how many times
	// it does: BEP 15's 15 seconds and 8 times (the last wait is then
	// 3840 seconds) when zero; a negative Retransmits sends once. The
	// context of a request bounds it too.
	Timeout     time.Duration
	Retransmits int
	// Network is the family of the trackers' addresses the client uses:
	// "udp4", "udp6", or "udp" (or empty) for either, IPv4 first.
	Network string
	// LookupNetIP resolves the host names of tracker URLs;
	// net.DefaultResolver's when nil.
	LookupNetIP func(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// UDPClient announces to and scrapes UDP trackers (BEP 15), with the URL
// data of BEP 41. It does no I/O of its own: it sends through
// UDPClientConfig.Send and reads the datagrams it is handed through
// HandleDatagram, so that a BitTorrent client may speak to trackers from
// the one UDP socket it speaks uTP and the DHT on, as the trackers then
// see it at the port it accepts peers on. ListenUDPClient gives it UDP
// bindings of its own instead. Its methods may be called from many
// goroutines at once.
//
// A request first gets a connection ID from the tracker, which the client
// keeps for a minute, for its later requests to the same tracker; a
// request that goes unanswered is sent again after Timeout, then after
// twice that, and so on.
type UDPClient struct {
	cfg  UDPClientConfig
	done chan struct{}

	mu      sync.Mutex
	pending map[uint32]*udpWait
	conns   map[netip.AddrPort]connectionID
	closed  bool

	// own is the bindings of a client from ListenUDPClient.
	own *ownBindings
}

type udpWait struct {
	to netip.AddrPort
	ch chan []byte
}

type connectionID struct {
	id      uint64
	expires time.Time
}

// NewUDPClient returns a UDPClient that sends through cfg.Send.
func NewUDPClient(cfg UDPClientConfig) *UDPClient {
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultUDPTimeout
	}
	if cfg.Retransmits == 0 {
		cfg.Retransmits = DefaultUDPRetransmits
	} else if cfg.Retransmits < 0 {
		cfg.Retransmits = 0
	}
	if cfg.LookupNetIP == nil {
		cfg.LookupNetIP = net.DefaultResolver.LookupNetIP
	}
	return &UDPClient{
		cfg:     cfg,
		done:    make(chan struct{}),
		pending: map[uint32]*udpWait{},
		conns:   map[netip.AddrPort]connectionID{},
	}
}

// HandleDatagram takes a datagram from the shared socket, and reports
// whether it was the answer to one of the client's requests: one whose
// transaction ID is pending, from the address the request went to. One it
// does not take is someone else's. The client copies what it keeps of b.
func (c *UDPClient) HandleDatagram(from netip.AddrPort, b []byte) bool {
	r, ok := parseUDPReply(b)
	if !ok {
		return false
	}
	c.mu.Lock()
	w := c.pending[r.tx]
	if w == nil || !sameAddr(w.to, from) {
		c.mu.Unlock()
		return false
	}
	delete(c.pending, r.tx)
	c.mu.Unlock()
	w.ch <- append([]byte(nil), b...) // buffered for one, and sent once
	return true
}

func sameAddr(a, b netip.AddrPort) bool {
	return a.Port() == b.Port() && a.Addr().Unmap() == b.Addr().Unmap()
}

// Close fails the requests in flight with ErrClosed, and those after, and
// removes the bindings of a client from ListenUDPClient.
func (c *UDPClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	c.mu.Unlock()
	if c.own != nil {
		return c.own.close()
	}
	return nil
}

// Announce announces r to the UDP tracker of the URL tracker
// ("udp://host:port/path?query"; the path and query go to the tracker as
// the URL data of BEP 41). The peers it returns are of the family of the
// tracker's address. An error packet from the tracker is an *Error.
func (c *UDPClient) Announce(ctx context.Context, tracker string, r *AnnounceRequest) (*AnnounceResponse, error) {
	to, urlData, err := c.resolve(ctx, tracker)
	if err != nil {
		return nil, err
	}
	body, err := c.do(ctx, to, actionAnnounce, func(dst []byte, conn uint64, tx uint32) []byte {
		return appendUDPAnnounce(dst, conn, tx, r, urlData)
	})
	if err != nil {
		return nil, err
	}
	return parseUDPAnnounceReply(body, !to.Addr().Is4())
}

// Scrape asks the UDP tracker of the URL tracker for the state of the
// swarms of hashes, in requests of up to 74 of them each. Torrents the
// tracker does not know may be missing from the map it returns, or there
// with zero counts, as the tracker chooses.
func (c *UDPClient) Scrape(ctx context.Context, tracker string, hashes []metainfo.InfoHash) (map[metainfo.InfoHash]Stats, error) {
	to, _, err := c.resolve(ctx, tracker)
	if err != nil {
		return nil, err
	}
	files := make(map[metainfo.InfoHash]Stats, len(hashes))
	for len(hashes) > 0 {
		batch := hashes[:min(len(hashes), maxScrapeHashes)]
		hashes = hashes[len(batch):]
		body, err := c.do(ctx, to, actionScrape, func(dst []byte, conn uint64, tx uint32) []byte {
			return appendUDPScrape(dst, conn, tx, batch)
		})
		if err != nil {
			return nil, err
		}
		for h, st := range parseUDPScrapeReply(body, batch) {
			files[h] = st
		}
	}
	return files, nil
}

// resolve returns the address of a UDP tracker URL and its URL data.
func (c *UDPClient) resolve(ctx context.Context, tracker string) (netip.AddrPort, string, error) {
	u, err := url.Parse(tracker)
	if err != nil {
		return netip.AddrPort{}, "", err
	}
	if u.Scheme != "udp" {
		return netip.AddrPort{}, "", ErrScheme
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return netip.AddrPort{}, "", err
	}
	port, err := net.LookupPort("udp", portStr)
	if err != nil || port == 0 {
		return netip.AddrPort{}, "", errors.New("tracker: invalid port in " + tracker)
	}
	urlData := u.EscapedPath()
	if u.RawQuery != "" {
		urlData += "?" + u.RawQuery
	}
	network := c.cfg.Network
	if network == "" || network == "udp" {
		network = "ip"
	} else {
		network = "ip" + network[len("udp"):]
	}
	var addrs []netip.Addr
	if a, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{a.Unmap()}
	} else if addrs, err = c.cfg.LookupNetIP(ctx, network, host); err != nil {
		return netip.AddrPort{}, "", err
	}
	var v6 netip.Addr
	for _, a := range addrs {
		a = a.Unmap()
		switch {
		case a.Is4() && network != "ip6":
			return netip.AddrPortFrom(a, uint16(port)), urlData, nil
		case !a.Is4() && network != "ip4" && !v6.IsValid():
			v6 = a
		}
	}
	if !v6.IsValid() {
		return netip.AddrPort{}, "", errors.New("tracker: no address of the network for " + host)
	}
	return netip.AddrPortFrom(v6, uint16(port)), urlData, nil
}

// do makes a request of action to the tracker at to, connecting first when
// the client has no connection ID of the tracker's, and returns the body of
// its reply. build appends the request.
func (c *UDPClient) do(ctx context.Context, to netip.AddrPort, action uint32,
	build func(dst []byte, conn uint64, tx uint32) []byte) ([]byte, error) {
	n := 0 // retransmissions so far, which connecting counts too
	for {
		conn, ok := c.connectionID(to)
		if !ok {
			body, err := c.exchange(ctx, to, &n, actionConnect, false, func(dst []byte, tx uint32) []byte {
				return appendConnect(dst, tx)
			})
			if err != nil {
				return nil, err
			}
			if len(body) < 8 {
				return nil, errShortPacket
			}
			conn = binary.BigEndian.Uint64(body)
			c.setConnectionID(to, conn)
		}
		body, err := c.exchange(ctx, to, &n, action, true, func(dst []byte, tx uint32) []byte {
			return build(dst, conn, tx)
		})
		if errors.Is(err, errConnectionExpired) {
			continue
		}
		return body, err
	}
}

// errConnectionExpired ends an exchange whose connection ID has expired
// while it waited, for do to connect again.
var errConnectionExpired = errors.New("tracker: connection ID expired")

// exchange sends a request and waits for its reply, sending it again each
// time the wait, doubled each time, runs out. With usesID, the request
// carries the connection ID of to, and the exchange gives up with
// errConnectionExpired when that has expired before a retransmission.
func (c *UDPClient) exchange(ctx context.Context, to netip.AddrPort, n *int, action uint32, usesID bool,
	build func(dst []byte, tx uint32) []byte) ([]byte, error) {
	w := &udpWait{to: to, ch: make(chan []byte, 1)}
	tx, err := c.register(w)
	if err != nil {
		return nil, err
	}
	defer c.unregister(tx)
	pkt := build(nil, tx)
	timer := time.NewTimer(0)
	defer timer.Stop()
	<-timer.C
	for {
		if err := c.cfg.Send(to, pkt); err != nil {
			return nil, err
		}
		timer.Reset(c.wait(*n))
		select {
		case b := <-w.ch:
			r, _ := parseUDPReply(b)
			switch r.action {
			case action:
				return r.body, nil
			case actionError:
				return nil, &Error{Reason: string(r.body)}
			}
			if r.action == actionConnect || usesID {
				// The tracker answered another request: likely one with
				// a connection ID it no longer takes.
				c.forgetConnectionID(to)
			}
			return nil, errors.New("tracker: UDP tracker answered with another action")
		case <-timer.C:
			if *n >= c.cfg.Retransmits {
				return nil, ErrTimeout
			}
			*n++
			if usesID {
				if _, ok := c.connectionID(to); !ok {
					return nil, errConnectionExpired
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, ErrClosed
		}
	}
}

// wait returns how long to wait for the reply to the nth retransmission:
// Timeout * 2^n.
func (c *UDPClient) wait(n int) time.Duration {
	d := c.cfg.Timeout
	for ; n > 0 && d < maxRetransmitWait; n-- {
		d *= 2
	}
	return min(d, maxRetransmitWait)
}

// register files w under a random transaction ID nobody else's is.
func (c *UDPClient) register(w *udpWait) (uint32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, ErrClosed
	}
	if len(c.pending) >= maxPendingUDPTransaction {
		return 0, errors.New("tracker: too many UDP tracker requests in flight")
	}
	var b [4]byte
	for {
		_, _ = rand.Read(b[:])
		tx := binary.BigEndian.Uint32(b[:])
		if _, taken := c.pending[tx]; !taken {
			c.pending[tx] = w
			return tx, nil
		}
	}
}

func (c *UDPClient) unregister(tx uint32) {
	c.mu.Lock()
	delete(c.pending, tx)
	c.mu.Unlock()
}

func (c *UDPClient) connectionID(to netip.AddrPort) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.conns[to]
	if ok && !time.Now().Before(id.expires) {
		delete(c.conns, to)
		ok = false
	}
	return id.id, ok
}

func (c *UDPClient) setConnectionID(to netip.AddrPort, id uint64) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.conns) >= maxCachedConnectionIDs {
		for k, v := range c.conns {
			if !now.Before(v.expires) {
				delete(c.conns, k)
			}
		}
	}
	if len(c.conns) < maxCachedConnectionIDs {
		c.conns[to] = connectionID{id: id, expires: now.Add(connectionIDLifetime)}
	}
}

func (c *UDPClient) forgetConnectionID(to netip.AddrPort) {
	c.mu.Lock()
	delete(c.conns, to)
	c.mu.Unlock()
}

// ListenUDPClient returns a UDPClient that speaks from UDP bindings of its
// own, added to engine, which must be running: one of each family, opened
// when the client first speaks to a tracker of the family, on a port the
// system picks. Close removes them. Its requests must not be made from a
// callback of engine's event loop, as fib.Engine.AddBinding must not.
// cfg.Send is ignored.
func ListenUDPClient(engine *fib.Engine, cfg UDPClientConfig) (*UDPClient, error) {
	if engine == nil {
		return nil, errors.New("tracker: nil engine")
	}
	o := &ownBindings{engine: engine, bindings: [2]int{-1, -1}}
	cfg.Send = o.send
	c := NewUDPClient(cfg)
	c.own = o
	o.c = c
	return c, nil
}

// ownBindings are the UDP bindings of a client from ListenUDPClient.
type ownBindings struct {
	engine *fib.Engine
	c      *UDPClient

	mu       sync.Mutex
	bindings [2]int // IPv4, IPv6; -1 until opened
	closed   bool
}

func (o *ownBindings) send(to netip.AddrPort, b []byte) error {
	fam := 0
	if !to.Addr().Unmap().Is4() {
		fam = 1
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrClosed
	}
	if o.bindings[fam] < 0 {
		network := [2]string{"udp4", "udp6"}[fam]
		i, err := o.engine.AddBinding(fib.Binding{Network: network, Addr: ":0", Handler: ownHandler{o.c}})
		if err != nil {
			o.mu.Unlock()
			return err
		}
		o.bindings[fam] = i
	}
	binding := o.bindings[fam]
	o.mu.Unlock()
	if fam == 0 {
		to = netip.AddrPortFrom(to.Addr().Unmap(), to.Port())
	}
	fc, err := o.engine.OpenUDPPeer(binding, to)
	if err != nil {
		return err
	}
	_, err = fc.Write(b)
	return err
}

func (o *ownBindings) close() error {
	o.mu.Lock()
	o.closed = true
	bindings := o.bindings
	o.mu.Unlock()
	var errs []error
	for _, i := range bindings {
		if i >= 0 {
			errs = append(errs, o.engine.RemoveBinding(i))
		}
	}
	return errors.Join(errs...)
}

// ownHandler reads the datagrams of a client's own bindings.
type ownHandler struct{ c *UDPClient }

func (h ownHandler) OnOpen(*fib.Connection)                 {}
func (h ownHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h ownHandler) OnClose(*fib.Connection, error)         {}

func (h ownHandler) OnData(c *fib.Connection, b []byte) { h.OnDatagrams(c, [][]byte{b}) }

func (h ownHandler) OnDatagrams(c *fib.Connection, dgrams [][]byte) {
	from := c.RemoteAddrPort()
	for _, d := range dgrams {
		h.c.HandleDatagram(from, d) // copies what it keeps
		bufferpool.Put(d)
	}
}
