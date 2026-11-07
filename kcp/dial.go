package kcp

import (
	"crypto/rand"
	"encoding/binary"
	"net"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// Dial starts a conversation of a random ID with the KCP server at addr
// ("host:port"), from a UDP binding of its own that it adds to engine,
// which must be running, and removes once the session closes. KCP has no
// handshake: the session is ready at once, and the server hears of it with
// the first segment it sends. Dial waits for the engine's event loop, so
// it must not be called from a callback that loop runs.
func Dial(engine *fib.Engine, addr string, cfg Config) (*Session, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	return DialConv(engine, addr, binary.LittleEndian.Uint32(b[:]), cfg)
}

// DialConv is Dial with the conversation ID conv.
func DialConv(engine *fib.Engine, addr string, conv uint32, cfg Config) (*Session, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	raddr := ua.AddrPort()
	s, err := newSession(cfg, conv, raddr)
	if err != nil {
		return nil, err
	}
	network := "udp4"
	if !raddr.Addr().Unmap().Is4() {
		network = "udp6"
	}
	binding, err := engine.AddBinding(fib.Binding{Network: network, Addr: ":0", Handler: clientHandler{s}})
	if err != nil {
		return nil, err
	}
	s.onClosed = func(*Session) { go func() { _ = engine.RemoveBinding(binding) }() }
	fc, err := engine.OpenUDPPeer(binding, raddr)
	if err != nil {
		_ = engine.RemoveBinding(binding)
		return nil, err
	}
	fc.SetUDPIdleTimeout(-1)
	s.mu.Lock()
	s.fc = fc
	s.local = fc.LocalAddr()
	s.unlock() // arms the idle timer
	return s, nil
}

// clientHandler reads the datagrams of a dialed session's binding.
type clientHandler struct{ s *Session }

func (h clientHandler) OnOpen(*fib.Connection)                 {}
func (h clientHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h clientHandler) OnData(c *fib.Connection, b []byte) { h.OnDatagrams(c, [][]byte{b}) }

func (h clientHandler) OnDatagrams(c *fib.Connection, dgrams [][]byte) {
	if r := c.RemoteAddrPort(); r.Port() != h.s.remote.Port() || r.Addr().Unmap() != h.s.remote.Addr().Unmap() {
		return
	}
	batch := make([][]byte, 0, len(dgrams))
	for _, d := range dgrams {
		if o := open(h.s.cfg.Crypt, d); o != nil {
			batch = append(batch, o)
		}
	}
	h.s.input(c, batch...)
	for _, d := range dgrams {
		bufferpool.Put(d)
	}
}

func (h clientHandler) OnClose(c *fib.Connection, err error) { h.s.connClosed(c, err) }

func (h clientHandler) OnEngineClose(*fib.Connection) { _ = h.s.Close() }
