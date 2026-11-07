package peer

import (
	"crypto/rc4"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bittorrent/peer/mse"
	"github.com/lesismal/fib/bufferpool"
)

// Handler handles the connections of a Torrent. Its callbacks for one
// connection run one at a time, in order: on the connection's fib round,
// or the goroutine reading a net.Conn.
type Handler interface {
	// OnHandshake runs once both handshakes are done, before any message:
	// the place to send the bitfield, or have all or have none, and the
	// extended handshake.
	OnHandshake(c *Conn)
	// OnMessage runs for each message, the Conn's state already updated by
	// it. m and its Data are valid only during the call.
	OnMessage(c *Conn, m *Message)
	// OnClose runs once when the connection has closed, with what closed
	// it; also for a dial that failed, and never before OnHandshake if a
	// handshake was begun with this torrent's peer.
	OnClose(c *Conn, err error)
}

// WritableHandler is a Handler told when a Conn that was Congested is no
// longer: on a goroutine of the transport's, which may run alongside the
// other callbacks.
type WritableHandler interface {
	Handler
	OnWritable(c *Conn)
}

// HandlerFuncs is a Handler of functions, any of them nil.
type HandlerFuncs struct {
	Handshake func(c *Conn)
	Message   func(c *Conn, m *Message)
	Close     func(c *Conn, err error)
	Writable  func(c *Conn)
}

// OnHandshake implements Handler.
func (h *HandlerFuncs) OnHandshake(c *Conn) {
	if h.Handshake != nil {
		h.Handshake(c)
	}
}

// OnMessage implements Handler.
func (h *HandlerFuncs) OnMessage(c *Conn, m *Message) {
	if h.Message != nil {
		h.Message(c, m)
	}
}

// OnClose implements Handler.
func (h *HandlerFuncs) OnClose(c *Conn, err error) {
	if h.Close != nil {
		h.Close(c, err)
	}
}

// OnWritable implements WritableHandler.
func (h *HandlerFuncs) OnWritable(c *Conn) {
	if h.Writable != nil {
		h.Writable(c)
	}
}

type phase uint8

const (
	phaseMSEInit phase = iota
	phaseMSERecv
	phaseHandshake
	phaseMessages
)

// Conn is a peer connection: the handshake, optionally under MSE, then
// messages. It tracks the choke and interest of both sides and the
// requests outstanding, keeps the connection alive and times it out. Its
// methods are safe for concurrent use. Messages sent while a callback
// runs go out together when it returns; sent from elsewhere they go at
// once, or when Batch's function returns.
type Conn struct {
	s         *Server
	initiator bool
	policy    mse.Policy

	// The receive path's: one round or read at a time.
	phase     phase
	mseInit   *mse.Initiator
	mseRecv   *mse.Receiver
	mseKey    [20]byte
	decrypt   *rc4.Cipher
	hs        []byte
	gotHeader bool
	parser    Parser

	// Set before OnHandshake and fixed after.
	torrent  *Torrent
	handler  Handler
	infoHash [20]byte
	v2       bool
	local    Reserved
	remote   Handshake
	method   mse.CryptoMethod
	obfusc   bool

	lastRecv atomic.Int64
	finished atomic.Bool

	mu           sync.Mutex
	tr           transport
	established  bool
	closed       bool
	closeErr     error
	encrypt      *rc4.Cipher
	out          []byte
	inRound      bool
	batch        int
	hasPiece     bool
	congested    bool
	lastSend     int64
	deadline     time.Time
	timer        *time.Timer
	amChoking    bool
	amInterest   bool
	peerChoking  bool
	peerInterest bool
	outstanding  map[Request]struct{}
	allowedFast  map[uint32]struct{}
	remoteExt    *ExtendedHandshake
	attachment   any
}

func (s *Server) newConn(initiator bool, t *Torrent, ih [20]byte, policy mse.Policy) *Conn {
	c := &Conn{
		s: s, initiator: initiator, policy: policy,
		torrent: t, infoHash: ih,
		amChoking: true, peerChoking: true,
		outstanding: make(map[Request]struct{}),
	}
	c.parser.MaxLength = s.cfg.MaxMessageLength
	if t != nil {
		c.handler = t.Handler
		c.local = s.reserved
		// Handshaking with a hybrid's v1 hash, offer the upgrade to v2.
		c.local.SetV2(!t.InfoHashV2.IsZero() && ih == t.InfoHash)
		c.v2 = !t.InfoHashV2.IsZero() && ih == t.InfoHashV2.Truncate()
	}
	return c
}

// attach starts c on tr: the timer, and an initiator's first bytes.
func (c *Conn) attach(tr transport) {
	now := time.Now()
	c.lastRecv.Store(now.UnixNano())
	c.mu.Lock()
	c.tr = tr
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		tr.close(err)
		return
	}
	c.lastSend = now.UnixNano()
	if d := c.s.cfg.HandshakeTimeout; d > 0 {
		c.deadline = now.Add(d)
	}
	c.armLocked(now)
	var first []byte
	if !c.initiator {
		c.mseRecv = mse.NewReceiver(&c.s.keys, c.policy)
		c.phase = phaseMSERecv
	} else {
		hs := AppendHandshake(nil, &Handshake{Reserved: c.local, InfoHash: c.infoHash, PeerID: c.s.cfg.PeerID})
		if c.policy == mse.Allow {
			first = hs
			c.phase = phaseHandshake
		} else {
			// The handshake goes as the initial payload, with MSE's third
			// message rather than a round trip after it.
			c.mseInit = mse.NewInitiator(c.infoHash, c.policy.Provide(), hs)
			var err error
			if first, err = c.mseInit.Start(); err != nil {
				c.mu.Unlock()
				c.closeWith(err)
				return
			}
			c.obfusc = true
			c.phase = phaseMSEInit
		}
	}
	if len(first) > 0 {
		c.rawLocked(first)
	}
	c.mu.Unlock()
}

// rawLocked sends b ahead of any queued message, encrypted if the stream
// is: the handshake's own bytes.
func (c *Conn) rawLocked(b []byte) {
	buf := bufferpool.Get(len(b))
	if c.encrypt != nil {
		c.encrypt.XORKeyStream(buf, b)
	} else {
		copy(buf, b)
	}
	c.lastSend = time.Now().UnixNano()
	c.tr.send(buf)
}

// input is what the transport read.
func (c *Conn) input(data []byte) {
	c.lastRecv.Store(time.Now().UnixNano())
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.inRound = true
	c.mu.Unlock()
	err := c.process(data)
	c.mu.Lock()
	c.inRound = false
	if err == nil && c.batch == 0 {
		c.flushLocked()
	}
	c.mu.Unlock()
	if err != nil {
		c.closeWith(err)
	}
}

func (c *Conn) process(data []byte) error {
	if c.decrypt != nil {
		c.decrypt.XORKeyStream(data, data)
	}
	for {
		switch c.phase {
		case phaseMSEInit, phaseMSERecv:
			var out []byte
			var done bool
			var err error
			if c.phase == phaseMSEInit {
				out, done, err = c.mseInit.Feed(data)
			} else {
				out, done, err = c.mseRecv.Feed(data)
			}
			if err != nil {
				return err
			}
			c.mu.Lock()
			if len(out) > 0 {
				c.rawLocked(out)
			}
			if !done {
				c.mu.Unlock()
				return nil
			}
			var r *mse.Result
			if c.phase == phaseMSEInit {
				r = c.mseInit.Result()
				c.mseInit = nil
				data = r.Rest
			} else {
				r = c.mseRecv.Result()
				c.mseRecv = nil
				if r.PlainHandshake {
					data = r.Rest
				} else {
					c.obfusc = true
					c.mseKey = r.SKey
					data = append(r.IA, r.Rest...)
				}
			}
			c.method = r.Method
			if r.Method == mse.RC4 {
				c.encrypt = r.Encrypt
				c.decrypt = r.Decrypt
			}
			c.mu.Unlock()
			c.phase = phaseHandshake
		case phaseHandshake:
			if len(data) == 0 {
				return nil
			}
			c.hs = append(c.hs, data...)
			data = nil
			if !c.gotHeader && len(c.hs) >= HandshakeHeaderLen {
				if err := c.handshakeHeader(); err != nil {
					return err
				}
				c.gotHeader = true
			}
			if len(c.hs) < HandshakeLen {
				return nil
			}
			copy(c.remote.PeerID[:], c.hs[HandshakeHeaderLen:])
			data = c.hs[HandshakeLen:]
			c.hs = nil
			c.phase = phaseMessages
			c.mu.Lock()
			c.established = true
			// From the handshake's deadline to the keepalive's and the
			// idle timeout's.
			c.armLocked(time.Now())
			c.mu.Unlock()
			c.handler.OnHandshake(c)
		case phaseMessages:
			if len(data) == 0 {
				return nil
			}
			return c.parser.Feed(data, c.onMessage)
		}
	}
}

// handshakeHeader handles the peer's handshake up to its peer ID: a
// receiver finds the torrent and answers, an initiator checks the answer.
func (c *Conn) handshakeHeader() error {
	if err := ParseHandshakeHeader(c.hs, &c.remote); err != nil {
		return err
	}
	ih := c.remote.InfoHash
	if c.initiator {
		switch {
		case ih == c.infoHash:
		case c.local.V2() && ih == c.torrent.InfoHashV2.Truncate():
			c.infoHash, c.v2 = ih, true
		default:
			return ErrInfoHashMismatch
		}
		return nil
	}
	if c.obfusc && ih != c.mseKey {
		// The info hash must be the SKEY the stream was set up with.
		return ErrInfoHashMismatch
	}
	t := c.s.lookup(ih)
	if t == nil {
		return ErrUnknownTorrent
	}
	reply := ih
	if !t.InfoHashV2.IsZero() {
		short := t.InfoHashV2.Truncate()
		if ih == short {
			c.v2 = true
		} else if c.remote.Reserved.V2() {
			// BEP 52: a peer that handshakes with a hybrid's v1 hash and
			// sets the v2 bit takes the truncated v2 hash in answer, and
			// the connection is v2's from then on.
			reply, c.v2 = short, true
		}
	}
	c.local = c.s.reserved
	c.local.SetV2(!t.InfoHashV2.IsZero())
	c.torrent, c.handler, c.infoHash = t, t.Handler, reply
	hs := AppendHandshake(nil, &Handshake{Reserved: c.local, InfoHash: reply, PeerID: c.s.cfg.PeerID})
	c.mu.Lock()
	if !c.closed {
		c.rawLocked(hs)
	}
	c.mu.Unlock()
	return nil
}

func (c *Conn) onMessage(m *Message) error {
	if m.Keepalive {
		c.handler.OnMessage(c, m)
		return nil
	}
	fast := c.Fast()
	if m.Type.Fast() && !fast {
		return ErrProtocol
	}
	c.mu.Lock()
	switch m.Type {
	case MsgChoke:
		c.peerChoking = true
		if !fast {
			// Without the Fast extension a choke drops every request; with
			// it each is rejected, or served, explicitly.
			clear(c.outstanding)
		}
	case MsgUnchoke:
		c.peerChoking = false
	case MsgInterested:
		c.peerInterest = true
	case MsgNotInterested:
		c.peerInterest = false
	case MsgPiece, MsgRejectRequest:
		delete(c.outstanding, m.Request())
	case MsgAllowedFast:
		if c.allowedFast == nil {
			c.allowedFast = make(map[uint32]struct{})
		}
		c.allowedFast[m.Index] = struct{}{}
	case MsgExtended:
		if !c.Extended() {
			c.mu.Unlock()
			return ErrProtocol
		}
		if m.ExtendedID == 0 {
			h, err := ParseExtendedHandshake(m.Data)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			if c.remoteExt != nil && h.M != nil {
				// A later handshake updates the extensions it names.
				for k, v := range c.remoteExt.M {
					if _, ok := h.M[k]; !ok {
						h.M[k] = v
					}
				}
			}
			c.remoteExt = h
		}
	}
	c.mu.Unlock()
	c.handler.OnMessage(c, m)
	return nil
}

// finish is the transport's word that the connection has closed.
func (c *Conn) finish(err error) {
	if c.finished.Swap(true) {
		return
	}
	c.mu.Lock()
	c.closed = true
	if c.closeErr != nil {
		err = c.closeErr
	}
	if c.timer != nil {
		c.timer.Stop()
	}
	if c.out != nil {
		bufferpool.Put(c.out)
		c.out = nil
	}
	c.mu.Unlock()
	if c.handler != nil {
		c.handler.OnClose(c, err)
	}
}

// closeWith closes the connection, err the reason OnClose hears.
func (c *Conn) closeWith(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closeErr = err
	tr := c.tr
	c.mu.Unlock()
	if tr != nil {
		tr.close(err)
	} else {
		// Still connecting: the dial's end finds the Conn closed.
		if err == nil {
			err = ErrClosed
		}
		c.finish(err)
	}
}

// Close closes the connection; OnClose follows.
func (c *Conn) Close() error {
	c.closeWith(nil)
	return nil
}

// CloseWithError closes the connection, OnClose hearing err.
func (c *Conn) CloseWithError(err error) { c.closeWith(err) }

// armLocked schedules the timer for the next deadline: the handshake's,
// the idle timeout's or the next keepalive's.
func (c *Conn) armLocked(now time.Time) {
	cfg := &c.s.cfg
	var next time.Time
	if !c.established {
		next = c.deadline
	} else {
		if cfg.IdleTimeout > 0 {
			next = time.Unix(0, c.lastRecv.Load()).Add(cfg.IdleTimeout)
		}
		if cfg.KeepaliveInterval > 0 {
			k := time.Unix(0, c.lastSend).Add(cfg.KeepaliveInterval)
			if next.IsZero() || k.Before(next) {
				next = k
			}
		}
	}
	if next.IsZero() {
		if !c.established && (cfg.IdleTimeout > 0 || cfg.KeepaliveInterval > 0) {
			// No handshake timeout: look again once a keepalive could be
			// due, by when the handshake may be done.
			next = now.Add(max(cfg.IdleTimeout, cfg.KeepaliveInterval))
		} else {
			return
		}
	}
	d := max(next.Sub(now), 10*time.Millisecond)
	if c.timer == nil {
		c.timer = time.AfterFunc(d, c.tick)
	} else {
		c.timer.Reset(d)
	}
}

func (c *Conn) tick() {
	now := time.Now()
	cfg := &c.s.cfg
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var err error
	switch {
	case !c.established:
		if !c.deadline.IsZero() && !now.Before(c.deadline) {
			err = ErrHandshakeTimeout
		}
	case cfg.IdleTimeout > 0 && now.Sub(time.Unix(0, c.lastRecv.Load())) >= cfg.IdleTimeout:
		err = ErrIdleTimeout
	case cfg.KeepaliveInterval > 0 && now.Sub(time.Unix(0, c.lastSend)) >= cfg.KeepaliveInterval:
		c.appendLocked(&Message{Keepalive: true})
		if !c.inRound && c.batch == 0 {
			c.flushLocked()
		}
		c.lastSend = now.UnixNano()
	}
	if err == nil {
		c.armLocked(now)
	}
	c.mu.Unlock()
	if err != nil {
		c.closeWith(err)
	}
}

func (c *Conn) appendLocked(m *Message) {
	n := EncodedLen(m)
	if c.out == nil {
		c.out = bufferpool.Get(max(n, 4096))[:0]
	} else {
		c.out = bufferpool.Grow(c.out, len(c.out)+n)
	}
	c.out = AppendMessage(c.out, m)
	if m.Type == MsgPiece && !m.Keepalive {
		c.hasPiece = true
	}
}

// flushLocked hands what is queued to the transport, once the handshake
// is done.
func (c *Conn) flushLocked() {
	if !c.established || c.closed || len(c.out) == 0 {
		return
	}
	b := c.out
	c.out = nil
	if c.encrypt != nil {
		c.encrypt.XORKeyStream(b, b)
	}
	c.lastSend = time.Now().UnixNano()
	c.tr.send(b)
	if c.hasPiece {
		c.hasPiece = false
		if !c.congested && c.tr.notifyOutput(int64(c.s.cfg.MaxQueuedBytes), c.drained) {
			c.congested = true
		}
	}
}

func (c *Conn) drained(err error) {
	c.mu.Lock()
	c.congested = false
	closed := c.closed
	c.mu.Unlock()
	if !closed && err == nil {
		if h, ok := c.handler.(WritableHandler); ok {
			h.OnWritable(c)
		}
	}
}

// Send queues m, tracking what it says: a choke, unchoke, interest, or a
// request or cancel. Before the handshake is done it waits for it.
func (c *Conn) Send(m *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sendLocked(m)
}

func (c *Conn) sendLocked(m *Message) error {
	if c.closed {
		return ErrClosed
	}
	if !m.Keepalive {
		switch m.Type {
		case MsgChoke:
			c.amChoking = true
		case MsgUnchoke:
			c.amChoking = false
		case MsgInterested:
			c.amInterest = true
		case MsgNotInterested:
			c.amInterest = false
		case MsgRequest:
			c.outstanding[m.Request()] = struct{}{}
		case MsgCancel:
			if !c.fastLocked() {
				// Without a reject to come, a cancelled request is gone; a
				// piece that crossed the cancel still arrives.
				delete(c.outstanding, m.Request())
			}
		}
	}
	c.appendLocked(m)
	if !c.inRound && c.batch == 0 {
		c.flushLocked()
	}
	return nil
}

// Batch runs fn with c's sends held, and sends what fn sent together.
func (c *Conn) Batch(fn func()) {
	c.mu.Lock()
	c.batch++
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.batch--
		if c.batch == 0 && !c.inRound {
			c.flushLocked()
		}
		c.mu.Unlock()
	}()
	fn()
}

// SendPiece sends a block, a piece message. The block is copied.
func (c *Conn) SendPiece(index, begin uint32, block []byte) error {
	return c.Send(&Message{Type: MsgPiece, Index: index, Begin: begin, Data: block})
}

// Request requests r, unless the peer is choking this side (and has not
// allowed r's piece fast), or as many requests as the peer's reqq and
// Config.MaxRequests allow are outstanding. A request already outstanding
// is not sent again.
func (c *Conn) Request(r Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peerChoking {
		if _, ok := c.allowedFast[r.Index]; !ok || !c.fastLocked() {
			return ErrChoked
		}
	}
	if _, ok := c.outstanding[r]; ok {
		return nil
	}
	if len(c.outstanding) >= c.requestLimitLocked() {
		return ErrRequestLimit
	}
	return c.sendLocked(&Message{Type: MsgRequest, Index: r.Index, Begin: r.Begin, Length: r.Length})
}

// Cancel cancels r, if outstanding.
func (c *Conn) Cancel(r Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.outstanding[r]; !ok {
		return nil
	}
	return c.sendLocked(&Message{Type: MsgCancel, Index: r.Index, Begin: r.Begin, Length: r.Length})
}

// Reject answers a request this side will not serve: with a reject
// request message under the Fast extension, and with silence without it.
func (c *Conn) Reject(r Request) error {
	if !c.Fast() {
		return nil
	}
	return c.Send(&Message{Type: MsgRejectRequest, Index: r.Index, Begin: r.Begin, Length: r.Length})
}

func (c *Conn) requestLimitLocked() int {
	n := c.s.cfg.MaxRequests
	if c.remoteExt != nil && c.remoteExt.Reqq > 0 {
		n = min(n, c.remoteExt.Reqq)
	}
	return n
}

// RequestLimit is how many requests may be outstanding.
func (c *Conn) RequestLimit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requestLimitLocked()
}

// Outstanding is how many requests are outstanding.
func (c *Conn) Outstanding() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.outstanding)
}

// Requests appends the requests outstanding to dst.
func (c *Conn) Requests(dst []Request) []Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	for r := range c.outstanding {
		dst = append(dst, r)
	}
	return dst
}

// SendExtendedHandshake sends the extended handshake h, with this side's
// extensions as m, and the reqq and v of the Config, where h leaves them
// unset.
func (c *Conn) SendExtendedHandshake(h *ExtendedHandshake) error {
	if !c.Extended() {
		return ErrNoExtension
	}
	cp := *h
	if cp.M == nil {
		cp.M = make(map[string]int, len(c.s.cfg.Extensions))
		for k, v := range c.s.cfg.Extensions {
			cp.M[k] = int(v)
		}
	}
	if cp.Reqq == 0 {
		cp.Reqq = c.s.cfg.Reqq
	}
	if cp.Version == "" {
		cp.Version = c.s.cfg.Version
	}
	if cp.YourIP == nil {
		if ap, ok := addrPort(c.RemoteAddr()); ok {
			cp.YourIP = CompactAddr(ap.Addr())
		}
	}
	b, err := cp.Marshal()
	if err != nil {
		return err
	}
	return c.Send(&Message{Type: MsgExtended, ExtendedID: 0, Data: b})
}

// SendExtended sends payload as extension name's message, with the ID the
// peer's extended handshake gave it.
func (c *Conn) SendExtended(name string, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remoteExt == nil {
		return ErrNoExtension
	}
	id := c.remoteExt.ID(name)
	if id == 0 {
		return ErrNoExtension
	}
	return c.sendLocked(&Message{Type: MsgExtended, ExtendedID: id, Data: payload})
}

// ExtensionName is the extension this side receives with id, by
// Config.Extensions; empty for one it has none for.
func (c *Conn) ExtensionName(id uint8) string {
	for k, v := range c.s.cfg.Extensions {
		if v == id && id != 0 {
			return k
		}
	}
	return ""
}

// RemoteExtensions is the peer's extended handshake, nil before it.
func (c *Conn) RemoteExtensions() *ExtendedHandshake {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.remoteExt
}

// Torrent is the connection's torrent, nil before a receiver knows it.
func (c *Conn) Torrent() *Torrent { return c.torrent }

// InfoHash is the info hash of the handshake: v1, or v2 truncated.
func (c *Conn) InfoHash() [20]byte { return c.infoHash }

// V2 reports a connection of the torrent's v2 side (BEP 52): begun with
// the truncated v2 hash, or upgraded to it.
func (c *Conn) V2() bool { return c.v2 }

// PeerID is the peer's ID.
func (c *Conn) PeerID() [20]byte { return c.remote.PeerID }

// RemoteReserved is the peer's reserved bytes.
func (c *Conn) RemoteReserved() Reserved { return c.remote.Reserved }

// Fast reports the Fast extension on both sides.
func (c *Conn) Fast() bool { return c.local.Fast() && c.remote.Reserved.Fast() }

func (c *Conn) fastLocked() bool { return c.Fast() }

// Extended reports the extension protocol on both sides.
func (c *Conn) Extended() bool { return c.local.Extended() && c.remote.Reserved.Extended() }

// Obfuscated reports a handshake made under MSE.
func (c *Conn) Obfuscated() bool { return c.obfusc }

// Encrypted reports the payload RC4-encrypted.
func (c *Conn) Encrypted() bool { return c.method == mse.RC4 }

// Initiator reports a connection this side began.
func (c *Conn) Initiator() bool { return c.initiator }

// RemoteAddr is the peer's address.
func (c *Conn) RemoteAddr() net.Addr {
	c.mu.Lock()
	tr := c.tr
	c.mu.Unlock()
	if tr == nil {
		return nil
	}
	return tr.remoteAddr()
}

// LocalAddr is this side's address.
func (c *Conn) LocalAddr() net.Addr {
	c.mu.Lock()
	tr := c.tr
	c.mu.Unlock()
	if tr == nil {
		return nil
	}
	return tr.localAddr()
}

// PeerChoking reports the peer choking this side; AmChoking this side
// choking the peer; PeerInterested and AmInterested the interest.
func (c *Conn) PeerChoking() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.peerChoking }

// AmChoking reports this side choking the peer.
func (c *Conn) AmChoking() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.amChoking }

// PeerInterested reports the peer interested in this side's pieces.
func (c *Conn) PeerInterested() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.peerInterest }

// AmInterested reports this side interested in the peer's pieces.
func (c *Conn) AmInterested() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.amInterest }

// AllowedFast reports piece i allowed fast by the peer: requestable while
// choked.
func (c *Conn) AllowedFast(i uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.allowedFast[i]
	return ok
}

// Congested reports more queued for the socket than Config.MaxQueuedBytes
// after a piece message: an uploader holds further pieces until
// OnWritable.
func (c *Conn) Congested() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.congested }

// Established reports the handshake done.
func (c *Conn) Established() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.established }

// SetAttachment keeps v with the connection, for the handler.
func (c *Conn) SetAttachment(v any) { c.mu.Lock(); c.attachment = v; c.mu.Unlock() }

// Attachment is what SetAttachment kept.
func (c *Conn) Attachment() any { c.mu.Lock(); defer c.mu.Unlock(); return c.attachment }
