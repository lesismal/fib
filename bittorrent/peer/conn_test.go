//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package peer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer/mse"
)

const testTimeout = 20 * time.Second

func runEngine(t *testing.T, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = "peer-" + t.Name()
	config.LogSettings = false
	engine, err := fib.New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return engine
}

// makeTorrent makes a torrent of size random bytes in one file.
func makeTorrent(t testing.TB, size int, version metainfo.Version, pieceLength int64) (*metainfo.MetaInfo, []byte) {
	t.Helper()
	data := make([]byte, size)
	r := rand.New(rand.NewPCG(uint64(size), 7))
	for i := range data {
		data[i] = byte(r.Uint32())
	}
	b := metainfo.Builder{Version: version, PieceLength: pieceLength}
	mi, err := b.Build(fstest.MapFS{"data.bin": {Data: data}}, "data.bin")
	if err != nil {
		t.Fatal(err)
	}
	return mi, data
}

// seeder serves a torrent's data and metadata from memory, holding
// requests while its Conn is congested.
type seeder struct {
	info []byte
	data []byte
	mi   *metainfo.MetaInfo

	mu       sync.Mutex
	served   int
	pending  map[*Conn][]Request
	writable int
}

func newSeeder(mi *metainfo.MetaInfo, data []byte) *seeder {
	return &seeder{info: mi.InfoBytes, data: data, mi: mi, pending: map[*Conn][]Request{}}
}

func (s *seeder) OnHandshake(c *Conn) {
	if c.Extended() {
		_ = c.SendExtendedHandshake(&ExtendedHandshake{MetadataSize: len(s.info)})
	}
	if c.Fast() {
		_ = c.Send(&Message{Type: MsgHaveAll})
	} else {
		f := NewBitfield(s.mi.Info.PieceCount())
		f.SetAll()
		_ = c.Send(&Message{Type: MsgBitfield, Data: f.Bytes()})
	}
}

func (s *seeder) OnMessage(c *Conn, m *Message) {
	switch m.Type {
	case MsgInterested:
		_ = c.Send(&Message{Type: MsgUnchoke})
	case MsgRequest:
		s.mu.Lock()
		s.pending[c] = append(s.pending[c], m.Request())
		s.mu.Unlock()
		s.serve(c)
	case MsgCancel:
	case MsgExtended:
		if c.ExtensionName(m.ExtendedID) != ExtMetadata {
			return
		}
		mm, err := ParseMetadataMessage(m.Data)
		if err != nil || mm.Type != MetadataRequest {
			return
		}
		reply := MetadataMessage{Type: MetadataReject, Piece: mm.Piece}
		if p, ok := MetadataPiece(s.info, mm.Piece); ok {
			reply = MetadataMessage{Type: MetadataData, Piece: mm.Piece, TotalSize: len(s.info), Data: p}
		}
		_ = c.SendExtended(ExtMetadata, AppendMetadataMessage(nil, &reply))
	}
}

func (s *seeder) serve(c *Conn) {
	c.Batch(func() {
		for !c.Congested() {
			s.mu.Lock()
			q := s.pending[c]
			if len(q) == 0 {
				s.mu.Unlock()
				return
			}
			r := q[0]
			s.pending[c] = q[1:]
			s.served++
			s.mu.Unlock()
			off := s.mi.Info.PieceOffset(int(r.Index)) + int64(r.Begin)
			if r.Length > MaxBlockSize || off+int64(r.Length) > int64(len(s.data)) {
				_ = c.Reject(r)
				continue
			}
			_ = c.SendPiece(r.Index, r.Begin, s.data[off:off+int64(r.Length)])
		}
	})
}

func (s *seeder) OnWritable(c *Conn) {
	s.mu.Lock()
	s.writable++
	s.mu.Unlock()
	s.serve(c)
}

func (s *seeder) OnClose(c *Conn, err error) {
	s.mu.Lock()
	delete(s.pending, c)
	s.mu.Unlock()
}

// leecher fetches a torrent's metadata with ut_metadata, unless it has
// it, and then every block, verifying each piece.
type leecher struct {
	ih metainfo.InfoHash

	mu       sync.Mutex
	meta     *MetadataAssembler
	info     *metainfo.Info
	data     []byte
	queue    []Request
	got      map[Request]bool
	pieceGot []int
	verified int
	conn     *Conn
	done     chan struct{}
	closed   chan error
	failure  error
}

func newLeecher(ih metainfo.InfoHash, info *metainfo.Info) *leecher {
	l := &leecher{ih: ih, done: make(chan struct{}), closed: make(chan error, 1)}
	if info != nil {
		l.setInfo(info)
	}
	return l
}

func (l *leecher) setInfo(info *metainfo.Info) {
	l.info = info
	l.data = make([]byte, info.TotalLength())
	l.got = map[Request]bool{}
	l.pieceGot = make([]int, info.PieceCount())
	for i := 0; i < info.PieceCount(); i++ {
		size := info.PieceSize(i)
		for b := int64(0); b < size; b += BlockSize {
			l.queue = append(l.queue, Request{uint32(i), uint32(b), uint32(min(BlockSize, size-b))})
		}
	}
}

func (l *leecher) fail(c *Conn, err error) {
	l.mu.Lock()
	if l.failure == nil {
		l.failure = err
	}
	l.mu.Unlock()
	c.CloseWithError(err)
}

func (l *leecher) OnHandshake(c *Conn) {
	l.mu.Lock()
	l.conn = c
	l.mu.Unlock()
	if c.Extended() {
		_ = c.SendExtendedHandshake(&ExtendedHandshake{})
	}
	if c.Fast() {
		_ = c.Send(&Message{Type: MsgHaveNone})
	}
	_ = c.Send(&Message{Type: MsgInterested})
}

// fillLocked keeps the pipeline full.
func (l *leecher) fillLocked(c *Conn) {
	for len(l.queue) > 0 {
		if err := c.Request(l.queue[0]); err != nil {
			return
		}
		l.queue = l.queue[1:]
	}
}

func (l *leecher) OnMessage(c *Conn, m *Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch m.Type {
	case MsgExtended:
		if m.ExtendedID == 0 {
			rext := c.RemoteExtensions()
			if l.info != nil || l.meta != nil || rext.MetadataSize == 0 {
				return
			}
			meta, err := NewMetadataAssembler(rext.MetadataSize, 0)
			if err != nil {
				go l.fail(c, err)
				return
			}
			l.meta = meta
			for i := range meta.Pieces() {
				_ = c.SendExtended(ExtMetadata, AppendMetadataMessage(nil, &MetadataMessage{Type: MetadataRequest, Piece: i}))
			}
			return
		}
		if c.ExtensionName(m.ExtendedID) != ExtMetadata || l.meta == nil || l.info != nil {
			return
		}
		mm, err := ParseMetadataMessage(m.Data)
		if err != nil {
			go l.fail(c, err)
			return
		}
		if err := l.meta.Add(mm); err != nil {
			go l.fail(c, err)
			return
		}
		if l.meta.Complete() {
			info, err := l.meta.Info(l.ih, metainfo.InfoHashV2{})
			if err != nil {
				go l.fail(c, err)
				return
			}
			l.setInfo(info)
			l.fillLocked(c)
		}
	case MsgUnchoke, MsgAllowedFast:
		l.fillLocked(c)
	case MsgChoke:
		if !c.Fast() {
			l.requeueLocked(c)
		}
	case MsgRejectRequest:
		l.queue = append(l.queue, m.Request())
	case MsgPiece:
		r := m.Request()
		if l.info == nil || l.got[r] || int(r.Index) >= l.info.PieceCount() {
			return
		}
		off := l.info.PieceOffset(int(r.Index)) + int64(r.Begin)
		if off+int64(len(m.Data)) > int64(len(l.data)) {
			go l.fail(c, errors.New("piece out of range"))
			return
		}
		copy(l.data[off:], m.Data)
		l.got[r] = true
		l.pieceGot[r.Index] += len(m.Data)
		if int64(l.pieceGot[r.Index]) == l.info.PieceSize(int(r.Index)) {
			p := l.data[l.info.PieceOffset(int(r.Index)):][:l.info.PieceSize(int(r.Index))]
			if err := l.info.VerifyPieceV1(int(r.Index), p); err != nil {
				go l.fail(c, fmt.Errorf("piece %d: %w", r.Index, err))
				return
			}
			l.verified++
			if l.verified == l.info.PieceCount() {
				close(l.done)
				return
			}
		}
		l.fillLocked(c)
	}
}

func (l *leecher) requeueLocked(c *Conn) {
	out := map[Request]bool{}
	for _, r := range c.Requests(nil) {
		out[r] = true
	}
	queued := map[Request]bool{}
	for _, r := range l.queue {
		queued[r] = true
	}
	for i := 0; i < l.info.PieceCount(); i++ {
		size := l.info.PieceSize(i)
		for b := int64(0); b < size; b += BlockSize {
			r := Request{uint32(i), uint32(b), uint32(min(BlockSize, size-b))}
			if !l.got[r] && !out[r] && !queued[r] {
				l.queue = append(l.queue, r)
			}
		}
	}
}

func (l *leecher) OnClose(c *Conn, err error) {
	select {
	case l.closed <- err:
	default:
	}
}

func (l *leecher) wait(t *testing.T) {
	t.Helper()
	select {
	case <-l.done:
	case err := <-l.closed:
		l.mu.Lock()
		f := l.failure
		l.mu.Unlock()
		t.Fatalf("closed before done: %v (failure %v)", err, f)
	case <-time.After(testTimeout):
		l.mu.Lock()
		t.Fatalf("timed out: %d of %d pieces", l.verified, len(l.pieceGot))
	}
}

func policyPtr(p mse.Policy) *mse.Policy { return &p }

func TestTransferFib(t *testing.T) {
	mi, data := makeTorrent(t, 1<<20+12345, metainfo.V1, 64<<10)
	ih := mi.Info.InfoHashV1()
	for _, tc := range []struct {
		name         string
		server, dial mse.Policy
		encrypted    bool
		obfuscated   bool
	}{
		{"plain", mse.Allow, mse.Allow, false, false},
		{"mse-plaintext-method", mse.Allow, mse.Prefer, false, true},
		{"mse-rc4", mse.Prefer, mse.Prefer, true, true},
		{"require", mse.Require, mse.Require, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sd := newSeeder(mi, data)
			srv := NewServer(Config{Encryption: tc.server, MaxQueuedBytes: 64 << 10})
			if err := srv.AddTorrent(&Torrent{InfoHash: ih, Handler: sd}); err != nil {
				t.Fatal(err)
			}
			engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
			addrs, _ := engine.BindingAddrs(0)

			l := newLeecher(ih, nil)
			cli := NewServer(Config{})
			if err := cli.AddTorrent(&Torrent{InfoHash: ih, Handler: l}); err != nil {
				t.Fatal(err)
			}
			c, err := cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: ih, Encryption: policyPtr(tc.dial)})
			if err != nil {
				t.Fatal(err)
			}
			l.wait(t)
			if c.Encrypted() != tc.encrypted || c.Obfuscated() != tc.obfuscated {
				t.Fatalf("encrypted %v obfuscated %v", c.Encrypted(), c.Obfuscated())
			}
			if c.PeerID() != srv.PeerID() || !c.Fast() || !c.Extended() || c.V2() {
				t.Fatalf("peer %q fast %v ext %v v2 %v", c.PeerID(), c.Fast(), c.Extended(), c.V2())
			}
			if string(l.data) != string(data) {
				t.Fatal("data differs")
			}
			sd.mu.Lock()
			t.Logf("served %d blocks, writable %d times", sd.served, sd.writable)
			sd.mu.Unlock()
			_ = c.Close()
		})
	}
}

// TestTransferNetConn runs each side over a net.Conn in turn: the leecher
// dialing a fib server, and a fib leecher dialing a net.Listener.
func TestTransferNetConn(t *testing.T) {
	mi, data := makeTorrent(t, 300<<10, metainfo.V1, 32<<10)
	ih := mi.Info.InfoHashV1()

	t.Run("client", func(t *testing.T) {
		srv := NewServer(Config{Encryption: mse.Prefer})
		_ = srv.AddTorrent(&Torrent{InfoHash: ih, Handler: newSeeder(mi, data)})
		engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
		addrs, _ := engine.BindingAddrs(0)
		conn, err := net.Dial("tcp", addrs[0].String())
		if err != nil {
			t.Fatal(err)
		}
		l := newLeecher(ih, nil)
		cli := NewServer(Config{Encryption: mse.Prefer})
		_ = cli.AddTorrent(&Torrent{InfoHash: ih, Handler: l})
		c, err := cli.Open(conn, DialOptions{InfoHash: ih})
		if err != nil {
			t.Fatal(err)
		}
		l.wait(t)
		if !c.Encrypted() {
			t.Fatal("not encrypted")
		}
		_ = c.Close()
	})

	t.Run("server", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		srv := NewServer(Config{MaxQueuedBytes: 32 << 10})
		sd := newSeeder(mi, data)
		_ = srv.AddTorrent(&Torrent{InfoHash: ih, Handler: sd})
		go func() { _ = srv.Serve(ln) }()
		engine := runEngine(t)
		l := newLeecher(ih, mi.Info)
		cli := NewServer(Config{})
		_ = cli.AddTorrent(&Torrent{InfoHash: ih, Handler: l})
		c, err := cli.Dial(engine, DialOptions{Addr: ln.Addr().String(), InfoHash: ih})
		if err != nil {
			t.Fatal(err)
		}
		l.wait(t)
		_ = c.Close()
	})
}

func TestV2Upgrade(t *testing.T) {
	mi, data := makeTorrent(t, 100<<10, metainfo.Hybrid, 16<<10)
	v1, v2 := mi.Info.InfoHashV1(), mi.Info.InfoHashV2()
	for _, tc := range []struct {
		name     string
		dialIH   [20]byte
		srvV2    bool
		wantV2   bool
		wantIH   [20]byte
		cliHasV2 bool
	}{
		{"v1-upgraded", v1, true, true, v2.Truncate(), true},
		{"v1-no-upgrade", v1, false, false, v1, true},
		{"v2-direct", v2.Truncate(), true, true, v2.Truncate(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srvT := &Torrent{InfoHash: v1, Handler: newSeeder(mi, data)}
			if tc.srvV2 {
				srvT.InfoHashV2 = v2
			}
			srv := NewServer(Config{})
			_ = srv.AddTorrent(srvT)
			engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
			addrs, _ := engine.BindingAddrs(0)
			l := newLeecher(v1, mi.Info)
			cli := NewServer(Config{})
			_ = cli.AddTorrent(&Torrent{InfoHash: v1, InfoHashV2: v2, Handler: l})
			c, err := cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: tc.dialIH})
			if err != nil {
				t.Fatal(err)
			}
			l.wait(t)
			if c.V2() != tc.wantV2 || c.InfoHash() != tc.wantIH {
				t.Fatalf("v2 %v ih %x, want %v %x", c.V2(), c.InfoHash(), tc.wantV2, tc.wantIH)
			}
			_ = c.Close()
		})
	}
}

// closer records why connections closed.
type closer struct {
	HandlerFuncs
	errs chan error
}

func newCloser() *closer {
	c := &closer{errs: make(chan error, 4)}
	c.Close = func(_ *Conn, err error) { c.errs <- err }
	return c
}

func (c *closer) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-c.errs:
		return err
	case <-time.After(testTimeout):
		t.Fatal("no close")
		return nil
	}
}

func TestRefusals(t *testing.T) {
	ih := [20]byte{1}
	other := [20]byte{2}

	t.Run("unknown torrent", func(t *testing.T) {
		srv := NewServer(Config{})
		_ = srv.AddTorrent(&Torrent{InfoHash: ih, Handler: newCloser()})
		engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
		addrs, _ := engine.BindingAddrs(0)
		h := newCloser()
		cli := NewServer(Config{})
		_ = cli.AddTorrent(&Torrent{InfoHash: other, Handler: h})
		for _, p := range []mse.Policy{mse.Allow, mse.Prefer} {
			if _, err := cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: other, Encryption: policyPtr(p)}); err != nil {
				t.Fatal(err)
			}
			if err := h.wait(t); err == nil {
				t.Fatalf("%v: closed without an error", p)
			}
		}
		if _, err := cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: [20]byte{9}}); !errors.Is(err, ErrUnknownTorrent) {
			t.Fatalf("dial for no torrent: %v", err)
		}
	})

	t.Run("plaintext to require", func(t *testing.T) {
		srv := NewServer(Config{Encryption: mse.Require})
		_ = srv.AddTorrent(&Torrent{InfoHash: ih, Handler: newCloser()})
		engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
		addrs, _ := engine.BindingAddrs(0)
		h := newCloser()
		cli := NewServer(Config{})
		_ = cli.AddTorrent(&Torrent{InfoHash: ih, Handler: h})
		_, _ = cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: ih})
		_ = h.wait(t)
	})

	t.Run("dial failure", func(t *testing.T) {
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		addr := ln.Addr().String()
		ln.Close()
		engine := runEngine(t)
		h := newCloser()
		cli := NewServer(Config{})
		_ = cli.AddTorrent(&Torrent{InfoHash: ih, Handler: h})
		if _, err := cli.Dial(engine, DialOptions{Addr: addr, InfoHash: ih}); err != nil {
			t.Fatal(err)
		}
		if err := h.wait(t); err == nil {
			t.Fatal("failed dial closed without an error")
		}
	})

	t.Run("duplicate torrent", func(t *testing.T) {
		s := NewServer(Config{})
		tor := &Torrent{InfoHash: ih, Handler: newCloser()}
		if err := s.AddTorrent(tor); err != nil {
			t.Fatal(err)
		}
		if err := s.AddTorrent(&Torrent{InfoHash: ih, Handler: newCloser()}); !errors.Is(err, ErrTorrentExists) {
			t.Fatal(err)
		}
		s.RemoveTorrent(tor)
		if err := s.AddTorrent(tor); err != nil {
			t.Fatal(err)
		}
	})
}

func TestKeepaliveAndTimeouts(t *testing.T) {
	ih := [20]byte{3}
	keepalives := make(chan struct{}, 16)
	srvH := newCloser()
	srvH.Message = func(c *Conn, m *Message) {
		if m.Keepalive {
			select {
			case keepalives <- struct{}{}:
			default:
			}
		}
	}
	srv := NewServer(Config{IdleTimeout: 300 * time.Millisecond, KeepaliveInterval: -1})
	_ = srv.AddTorrent(&Torrent{InfoHash: ih, Handler: srvH})
	engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
	addrs, _ := engine.BindingAddrs(0)

	// A client that keeps alive every 50ms is never idle.
	cliH := newCloser()
	cli := NewServer(Config{KeepaliveInterval: 50 * time.Millisecond, IdleTimeout: -1})
	_ = cli.AddTorrent(&Torrent{InfoHash: ih, Handler: cliH})
	c, _ := cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: ih})
	time.Sleep(time.Second)
	if len(keepalives) < 3 {
		t.Fatalf("%d keepalives in a second", len(keepalives))
	}
	select {
	case err := <-srvH.errs:
		t.Fatalf("closed while kept alive: %v", err)
	default:
	}
	_ = c.Close()
	_ = srvH.wait(t)

	// One that says nothing is closed as idle.
	quiet := NewServer(Config{KeepaliveInterval: -1, IdleTimeout: -1})
	qh := newCloser()
	_ = quiet.AddTorrent(&Torrent{InfoHash: ih, Handler: qh})
	_, _ = quiet.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: ih})
	if err := srvH.wait(t); !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("idle: %v", err)
	}
	_ = qh.wait(t)

	// A peer that never handshakes times out.
	hs := NewServer(Config{HandshakeTimeout: 200 * time.Millisecond})
	engine2 := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: hs})
	addrs2, _ := engine2.BindingAddrs(0)
	conn, err := net.Dial("tcp", addrs2[0].String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("silent peer not closed")
	}
}

func TestRequestLimitAndChoke(t *testing.T) {
	ih := [20]byte{4}
	ready := make(chan *Conn, 1)
	got := make(chan *Message, 64)
	srvH := &HandlerFuncs{Message: func(c *Conn, m *Message) {
		if m.Type == MsgRequest || m.Type == MsgCancel {
			c2 := *m
			got <- &c2
		}
	}, Handshake: func(c *Conn) {
		_ = c.SendExtendedHandshake(&ExtendedHandshake{Reqq: 3})
		_ = c.Send(&Message{Type: MsgAllowedFast, Index: 7})
	}}
	srv := NewServer(Config{})
	_ = srv.AddTorrent(&Torrent{InfoHash: ih, Handler: srvH})
	engine := runEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
	addrs, _ := engine.BindingAddrs(0)
	cliH := &HandlerFuncs{Message: func(c *Conn, m *Message) {
		if m.Type == MsgAllowedFast {
			ready <- c
		}
	}}
	cli := NewServer(Config{})
	_ = cli.AddTorrent(&Torrent{InfoHash: ih, Handler: cliH})
	_, _ = cli.Dial(engine, DialOptions{Addr: addrs[0].String(), InfoHash: ih})
	var c *Conn
	select {
	case c = <-ready:
	case <-time.After(testTimeout):
		t.Fatal("no allowed fast")
	}
	if err := c.Request(Request{1, 0, BlockSize}); !errors.Is(err, ErrChoked) {
		t.Fatalf("choked: %v", err)
	}
	for i := range 3 {
		if err := c.Request(Request{7, uint32(i) * BlockSize, BlockSize}); err != nil {
			t.Fatalf("allowed fast %d: %v", i, err)
		}
	}
	if c.RequestLimit() != 3 {
		t.Fatalf("limit %d", c.RequestLimit())
	}
	if err := c.Request(Request{7, 3 * BlockSize, BlockSize}); !errors.Is(err, ErrRequestLimit) {
		t.Fatalf("over reqq: %v", err)
	}
	_ = c.Cancel(Request{7, 0, BlockSize})
	for range 4 {
		select {
		case <-got:
		case <-time.After(testTimeout):
			t.Fatal("server missed a message")
		}
	}
	if c.Outstanding() != 3 {
		// With the Fast extension a cancel waits for its reject or piece.
		t.Fatalf("outstanding %d", c.Outstanding())
	}
}

func TestMetadataAssembler(t *testing.T) {
	// A v1 torrent of 1000 pieces has an info dictionary of two pieces.
	b := metainfo.Builder{Version: metainfo.V1, PieceLength: 16 << 10}
	mi, err := b.Build(fstest.MapFS{"zeros": {Data: make([]byte, 1000*16<<10)}}, "zeros")
	if err != nil {
		t.Fatal(err)
	}
	raw := mi.InfoBytes
	if len(raw) <= MetadataPieceSize {
		t.Fatalf("info of %d bytes is one piece", len(raw))
	}
	a, err := NewMetadataAssembler(len(raw), 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := a.Pieces() - 1; i >= 0; i-- {
		p, _ := MetadataPiece(raw, i)
		if err := a.Add(&MetadataMessage{Type: MetadataData, Piece: i, TotalSize: len(raw), Data: p}); err != nil {
			t.Fatal(err)
		}
	}
	info, err := a.Info(mi.Info.InfoHashV1(), metainfo.InfoHashV2{})
	if err != nil || info.InfoHashV1() != mi.Info.InfoHashV1() {
		t.Fatalf("info %v", err)
	}
	if _, err := a.Info(metainfo.InfoHash{1}, metainfo.InfoHashV2{}); !errors.Is(err, ErrMetadataHash) || a.Complete() {
		t.Fatalf("wrong hash: %v", err)
	}
	if err := a.Add(&MetadataMessage{Type: MetadataData, Piece: 0, Data: []byte("short")}); !errors.Is(err, ErrMetadataPiece) {
		t.Fatalf("short piece: %v", err)
	}
	if err := a.Add(&MetadataMessage{Type: MetadataData, Piece: 0, TotalSize: 5, Data: raw[:MetadataPieceSize]}); !errors.Is(err, ErrMetadataSize) {
		t.Fatalf("other size: %v", err)
	}
	if _, err := NewMetadataAssembler(DefaultMaxMetadataSize+1, 0); !errors.Is(err, ErrMetadataSize) {
		t.Fatal("huge metadata accepted")
	}
}
