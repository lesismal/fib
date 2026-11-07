package dht

import (
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// Listen runs a Node on a UDP binding of its own at addr ("host:port"),
// which it adds to engine: "udp6" when cfg.IPv6, else "udp4". The engine
// may be running or not yet; the node serves once it runs, and Close
// removes the binding. cfg.Send is set by Listen.
//
// A client that shares one socket between the DHT, uTP and a UDP tracker
// client makes the Node with New instead, and hands it the datagrams that
// HandleDatagram claims.
func Listen(engine *fib.Engine, addr string, cfg Config) (*Node, error) {
	network := "udp4"
	if cfg.IPv6 {
		network = "udp6"
	}
	t := &transport{
		engine:  engine,
		conns:   map[netip.AddrPort]*fib.Connection{},
		opening: map[netip.AddrPort][][]byte{},
	}
	cfg.Send = t.send
	n, err := New(cfg)
	if err != nil {
		return nil, err
	}
	t.node = n
	i, err := engine.AddBinding(fib.Binding{Network: network, Addr: addr, Handler: transportHandler{t}})
	if err != nil {
		_ = n.Close()
		return nil, err
	}
	t.binding = i
	n.mu.Lock()
	if addrs, err := engine.BindingAddrs(i); err == nil && len(addrs) > 0 {
		n.addr = addrs[0]
	}
	n.onClose = t.close
	n.mu.Unlock()
	return n, nil
}

// LocalAddr returns the address of the binding Listen added, nil for a
// Node of New.
func (n *Node) LocalAddr() net.Addr {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.addr
}

// maxOpening bounds the peers being opened at once; past it, datagrams to
// new addresses are dropped, as a congested network would.
const maxOpening = 256

// transport sends a Node's datagrams on a fib UDP binding. fib keeps a
// connection for each peer address; a datagram to an address with none
// waits for Engine.OpenUDPPeer, on a goroutine of its own since that waits
// for the socket's event loop. Everything is keyed by address, not by
// connection, as with Config.ReusePort replies may arrive on another of the
// binding's sockets.
type transport struct {
	engine  *fib.Engine
	binding int
	node    *Node

	mu      sync.Mutex
	conns   map[netip.AddrPort]*fib.Connection
	opening map[netip.AddrPort][][]byte
	closed  bool
}

func (t *transport) send(addr netip.AddrPort, b []byte) {
	t.mu.Lock()
	c := t.conns[addr]
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return
	}
	if c != nil {
		err := c.Send(b)
		if err == nil || !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE) {
			return
		}
		t.mu.Lock()
		if t.conns[addr] == c {
			delete(t.conns, addr)
		}
		t.mu.Unlock()
	}
	t.mu.Lock()
	if q, ok := t.opening[addr]; ok {
		if len(q) < 8 {
			t.opening[addr] = append(q, b)
		}
		t.mu.Unlock()
		return
	}
	if len(t.opening) >= maxOpening {
		t.mu.Unlock()
		return
	}
	t.opening[addr] = [][]byte{b}
	t.mu.Unlock()
	go t.open(addr)
}

func (t *transport) open(addr netip.AddrPort) {
	c, err := t.engine.OpenUDPPeer(t.binding, addr)
	t.mu.Lock()
	q := t.opening[addr]
	delete(t.opening, addr)
	if err == nil && !t.closed {
		if old := t.conns[addr]; old != nil {
			c = old
		} else {
			t.conns[addr] = c
		}
	}
	t.mu.Unlock()
	if err != nil {
		return
	}
	for _, b := range q {
		_ = c.Send(b)
	}
}

func (t *transport) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	_ = t.engine.RemoveBinding(t.binding)
}

// transportHandler is the fib handler of a Listen node's binding.
type transportHandler struct{ t *transport }

func (h transportHandler) OnOpen(*fib.Connection)                 {}
func (h transportHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h transportHandler) OnData(c *fib.Connection, b []byte) {
	h.OnDatagrams(c, [][]byte{b})
}

// OnDatagrams hands a burst of a peer's datagrams to the node.
func (h transportHandler) OnDatagrams(c *fib.Connection, dgrams [][]byte) {
	t := h.t
	from := unmap(c.RemoteAddrPort())
	t.mu.Lock()
	if t.conns[from] == nil && !t.closed {
		t.conns[from] = c
	}
	t.mu.Unlock()
	for _, d := range dgrams {
		t.node.HandleDatagram(from, d)
		bufferpool.Put(d)
	}
}

func (h transportHandler) OnClose(c *fib.Connection, _ error) {
	t := h.t
	from := unmap(c.RemoteAddrPort())
	t.mu.Lock()
	if t.conns[from] == c {
		delete(t.conns, from)
	}
	t.mu.Unlock()
}

var _ fib.DatagramsHandler = transportHandler{}
