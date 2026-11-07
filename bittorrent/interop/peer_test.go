package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
	fmse "github.com/lesismal/fib/bittorrent/peer/mse"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent"
	ametainfo "github.com/anacrolix/torrent/metainfo"
	amse "github.com/anacrolix/torrent/mse"
	"github.com/anacrolix/torrent/storage"
)

const peerTimeout = 60 * time.Second

// peerTorrent writes size random bytes to a file in a new directory and
// makes a torrent of it with fib's Builder: the directory, the torrent and
// the data.
func peerTorrent(t *testing.T, size int, version metainfo.Version) (string, *metainfo.MetaInfo, []byte) {
	t.Helper()
	dir := t.TempDir()
	data := make([]byte, size)
	r := rand.New(rand.NewPCG(uint64(size), 3))
	for i := range data {
		data[i] = byte(r.Uint32())
	}
	p := filepath.Join(dir, "payload.bin")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	b := metainfo.Builder{Version: version, PieceLength: 32 << 10}
	mi, err := b.BuildPath(p)
	if err != nil {
		t.Fatal(err)
	}
	return dir, mi, data
}

// anacrolixTorrent is mi as anacrolix reads it.
func anacrolixTorrent(t *testing.T, mi *metainfo.MetaInfo) *ametainfo.MetaInfo {
	t.Helper()
	enc, err := bencode.Marshal(mi)
	if err != nil {
		t.Fatal(err)
	}
	ami, err := ametainfo.Load(bytes.NewReader(enc))
	if err != nil {
		t.Fatal(err)
	}
	return ami
}

// newAnacrolix starts an anacrolix client on 127.0.0.1 over TCP alone:
// no DHT, uTP, trackers, web seeds or port mapping.
func newAnacrolix(t *testing.T, dir string, seed bool, obfuscation torrent.HeaderObfuscationPolicy, provides amse.CryptoMethod) *torrent.Client {
	t.Helper()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dir
	cfg.DefaultStorage = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir: dir,
		// anacrolix gives the single file of a v2 or hybrid torrent its
		// name as a path below the name: keep it at the name, where the
		// Builder found it.
		FilePathMaker: func(o storage.FilePathMakerOpts) string {
			p := o.File.BestPath()
			if len(p) == 1 && p[0] == o.Info.BestName() {
				return p[0]
			}
			return filepath.Join(append([]string{o.Info.BestName()}, p...)...)
		},
		PieceCompletion: storage.NewMapPieceCompletion(),
		UsePartFiles:    g.Some(false),
	})
	cfg.Seed = seed
	cfg.NoDHT = true
	cfg.DisableUTP = true
	cfg.DisableTrackers = true
	cfg.DisableWebtorrent = true
	cfg.DisableWebseeds = true
	cfg.DisableIPv6 = true
	cfg.NoDefaultPortForwarding = true
	cfg.ListenHost = func(string) string { return "127.0.0.1" }
	cfg.ListenPort = 0
	// anacrolix's connection writer can miss a wake-up: when a reason to
	// request arrives (our unchoke, say) between its filling the write
	// buffer and its waiting on writeCond, the Broadcast finds no channel
	// to close and is lost (chansync.BroadcastCond), and nothing requests
	// until the writer's keepalive timer, KeepAliveTimeout (a minute),
	// wakes it. A seeder with nothing more to say leaves it there, so that
	// a download stalled a minute, and the test timed out, a few times in a
	// hundred. A short keepalive bounds the stall to that.
	cfg.KeepAliveTimeout = 2 * time.Second
	cfg.HeaderObfuscationPolicy = obfuscation
	if provides != 0 {
		cfg.CryptoProvides = provides
	}
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func runFib(t *testing.T, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = "bt-peer-" + t.Name()
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

// fibLeecher fetches a torrent's info dictionary with ut_metadata and then
// every block, keeping the pipeline full, and checks each piece's SHA-1.
type fibLeecher struct {
	ih metainfo.InfoHash

	mu       sync.Mutex
	meta     *peer.MetadataAssembler
	info     *metainfo.Info
	data     []byte
	queue    []peer.Request
	got      map[peer.Request]bool
	pieceGot []int64
	verified int
	extSeen  bool
	done     chan struct{}
	closed   chan error
	failure  error
}

func newFibLeecher(ih metainfo.InfoHash) *fibLeecher {
	return &fibLeecher{ih: ih, done: make(chan struct{}), closed: make(chan error, 1)}
}

func (l *fibLeecher) fail(c *peer.Conn, err error) {
	if l.failure == nil {
		l.failure = err
	}
	go c.CloseWithError(err)
}

func (l *fibLeecher) OnHandshake(c *peer.Conn) {
	if c.Extended() {
		_ = c.SendExtendedHandshake(&peer.ExtendedHandshake{})
	}
	if c.Fast() {
		_ = c.Send(&peer.Message{Type: peer.MsgHaveNone})
	}
	_ = c.Send(&peer.Message{Type: peer.MsgInterested})
}

func (l *fibLeecher) fillLocked(c *peer.Conn) {
	for len(l.queue) > 0 {
		if c.Request(l.queue[0]) != nil {
			return
		}
		l.queue = l.queue[1:]
	}
}

func (l *fibLeecher) setInfoLocked(info *metainfo.Info) {
	l.info = info
	l.data = make([]byte, info.TotalLength())
	l.got = map[peer.Request]bool{}
	l.pieceGot = make([]int64, info.PieceCount())
	for i := 0; i < info.PieceCount(); i++ {
		size := info.PieceSize(i)
		for b := int64(0); b < size; b += peer.BlockSize {
			l.queue = append(l.queue, peer.Request{Index: uint32(i), Begin: uint32(b), Length: uint32(min(peer.BlockSize, size-b))})
		}
	}
}

func (l *fibLeecher) OnMessage(c *peer.Conn, m *peer.Message) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch m.Type {
	case peer.MsgExtended:
		if m.ExtendedID == 0 {
			l.extSeen = true
			rext := c.RemoteExtensions()
			if l.meta != nil || rext.MetadataSize == 0 {
				return
			}
			if rext.ID(peer.ExtMetadata) == 0 {
				l.fail(c, errors.New("peer has no ut_metadata"))
				return
			}
			meta, err := peer.NewMetadataAssembler(rext.MetadataSize, 0)
			if err != nil {
				l.fail(c, err)
				return
			}
			l.meta = meta
			for i := range meta.Pieces() {
				_ = c.SendExtended(peer.ExtMetadata, peer.AppendMetadataMessage(nil, &peer.MetadataMessage{Type: peer.MetadataRequest, Piece: i}))
			}
			return
		}
		if c.ExtensionName(m.ExtendedID) != peer.ExtMetadata || l.meta == nil || l.info != nil {
			return
		}
		mm, err := peer.ParseMetadataMessage(m.Data)
		if err != nil {
			l.fail(c, err)
			return
		}
		if mm.Type != peer.MetadataData {
			l.fail(c, fmt.Errorf("ut_metadata %d for piece %d", mm.Type, mm.Piece))
			return
		}
		if err := l.meta.Add(mm); err != nil {
			l.fail(c, err)
			return
		}
		if l.meta.Complete() {
			info, err := l.meta.Info(l.ih, metainfo.InfoHashV2{})
			if err != nil {
				l.fail(c, err)
				return
			}
			l.setInfoLocked(info)
			l.fillLocked(c)
		}
	case peer.MsgUnchoke, peer.MsgAllowedFast:
		l.fillLocked(c)
	case peer.MsgRejectRequest:
		l.queue = append(l.queue, m.Request())
	case peer.MsgPiece:
		r := m.Request()
		if l.info == nil || l.got[r] || int(r.Index) >= l.info.PieceCount() {
			return
		}
		off := l.info.PieceOffset(int(r.Index)) + int64(r.Begin)
		if off+int64(len(m.Data)) > int64(len(l.data)) {
			l.fail(c, errors.New("block out of range"))
			return
		}
		copy(l.data[off:], m.Data)
		l.got[r] = true
		l.pieceGot[r.Index] += int64(len(m.Data))
		i := int(r.Index)
		if l.pieceGot[i] == l.info.PieceSize(i) {
			if err := l.info.VerifyPieceV1(i, l.data[l.info.PieceOffset(i):][:l.info.PieceSize(i)]); err != nil {
				l.fail(c, fmt.Errorf("piece %d: %w", i, err))
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

func (l *fibLeecher) OnClose(c *peer.Conn, err error) {
	select {
	case l.closed <- err:
	default:
	}
}

func (l *fibLeecher) wait(t *testing.T) {
	t.Helper()
	select {
	case <-l.done:
	case err := <-l.closed:
		l.mu.Lock()
		defer l.mu.Unlock()
		t.Fatalf("closed after %d pieces: %v (failure %v)", l.verified, err, l.failure)
	case <-time.After(peerTimeout):
		l.mu.Lock()
		defer l.mu.Unlock()
		t.Fatalf("timed out after %d pieces", l.verified)
	}
}

// TestPeerFibDownloadsFromAnacrolix has a fib peer fetch the info
// dictionary from an anacrolix seeder with ut_metadata and download every
// piece from it, in plaintext, with MSE then plaintext, and with MSE and
// RC4.
func TestPeerFibDownloadsFromAnacrolix(t *testing.T) {
	dir, mi, data := peerTorrent(t, 2<<20+777, metainfo.Hybrid)
	ih := mi.Info.InfoHashV1()
	cl := newAnacrolix(t, dir, true, torrent.HeaderObfuscationPolicy{Preferred: true}, 0)
	at, err := cl.AddTorrent(anacrolixTorrent(t, mi))
	if err != nil {
		t.Fatal(err)
	}
	if err := at.VerifyData(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-at.Complete().On():
	case <-time.After(peerTimeout):
		t.Fatal("anacrolix does not have the data it seeds")
	}
	addr := cl.ListenAddrs()[0].String()
	engine := runFib(t)

	for _, tc := range []struct {
		name      string
		policy    fmse.Policy
		encrypted bool
	}{
		{"plaintext", fmse.Allow, false},
		{"mse-plaintext", fmse.Prefer, false},
		{"mse-rc4-required", fmse.Require, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newFibLeecher(ih)
			s := peer.NewServer(peer.Config{})
			if err := s.AddTorrent(&peer.Torrent{InfoHash: ih, InfoHashV2: mi.Info.InfoHashV2(), Handler: l}); err != nil {
				t.Fatal(err)
			}
			policy := tc.policy
			c, err := s.Dial(engine, peer.DialOptions{Addr: addr, InfoHash: ih, Encryption: &policy})
			if err != nil {
				t.Fatal(err)
			}
			l.wait(t)
			if !bytes.Equal(l.data, data) {
				t.Fatal("downloaded data differs")
			}
			if c.Encrypted() != tc.encrypted || c.Obfuscated() != (tc.policy != fmse.Allow) {
				t.Fatalf("encrypted %v obfuscated %v", c.Encrypted(), c.Obfuscated())
			}
			if !c.Fast() || !c.Extended() || !l.extSeen {
				t.Fatalf("fast %v extended %v", c.Fast(), c.Extended())
			}
			if c.PeerID() != cl.PeerID() {
				t.Fatalf("peer ID %q, anacrolix's is %q", c.PeerID(), cl.PeerID())
			}
			_ = c.Close()
		})
	}
}

// fibSeeder serves a torrent's info dictionary and data from memory.
type fibSeeder struct {
	mi   *metainfo.MetaInfo
	data []byte

	mu      sync.Mutex
	pending map[*peer.Conn][]peer.Request
	served  int
	conns   []*peer.Conn
}

func (s *fibSeeder) OnHandshake(c *peer.Conn) {
	s.mu.Lock()
	s.conns = append(s.conns, c)
	s.mu.Unlock()
	if c.Extended() {
		_ = c.SendExtendedHandshake(&peer.ExtendedHandshake{MetadataSize: len(s.mi.InfoBytes), UploadOnly: 1})
	}
	if c.Fast() {
		_ = c.Send(&peer.Message{Type: peer.MsgHaveAll})
	} else {
		f := peer.NewBitfield(s.mi.Info.PieceCount())
		f.SetAll()
		_ = c.Send(&peer.Message{Type: peer.MsgBitfield, Data: f.Bytes()})
	}
}

func (s *fibSeeder) OnMessage(c *peer.Conn, m *peer.Message) {
	switch m.Type {
	case peer.MsgInterested:
		_ = c.Send(&peer.Message{Type: peer.MsgUnchoke})
	case peer.MsgRequest:
		s.mu.Lock()
		s.pending[c] = append(s.pending[c], m.Request())
		s.mu.Unlock()
		s.serve(c)
	case peer.MsgCancel:
		s.mu.Lock()
		q := s.pending[c]
		for i, r := range q {
			if r == m.Request() {
				s.pending[c] = append(q[:i:i], q[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
	case peer.MsgExtended:
		if c.ExtensionName(m.ExtendedID) != peer.ExtMetadata {
			return
		}
		mm, err := peer.ParseMetadataMessage(m.Data)
		if err != nil || mm.Type != peer.MetadataRequest {
			return
		}
		reply := peer.MetadataMessage{Type: peer.MetadataReject, Piece: mm.Piece}
		if p, ok := peer.MetadataPiece(s.mi.InfoBytes, mm.Piece); ok {
			reply = peer.MetadataMessage{Type: peer.MetadataData, Piece: mm.Piece, TotalSize: len(s.mi.InfoBytes), Data: p}
		}
		_ = c.SendExtended(peer.ExtMetadata, peer.AppendMetadataMessage(nil, &reply))
	}
}

// serve sends the pieces requested until the connection is congested;
// OnWritable goes on.
func (s *fibSeeder) serve(c *peer.Conn) {
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
			if r.Length == 0 || r.Length > peer.MaxBlockSize || off+int64(r.Length) > int64(len(s.data)) {
				_ = c.Reject(r)
				continue
			}
			_ = c.SendPiece(r.Index, r.Begin, s.data[off:off+int64(r.Length)])
		}
	})
}

func (s *fibSeeder) OnWritable(c *peer.Conn) { s.serve(c) }

func (s *fibSeeder) OnClose(c *peer.Conn, err error) {
	s.mu.Lock()
	delete(s.pending, c)
	s.mu.Unlock()
}

// TestPeerAnacrolixDownloadsFromFib has anacrolix, knowing only the info
// hash, fetch the info dictionary from a fib seeder with ut_metadata and
// download the whole torrent from it: with anacrolix's default policy,
// which begins with MSE and selects plaintext; with MSE and RC4 forced;
// and with plaintext forced.
func TestPeerAnacrolixDownloadsFromFib(t *testing.T) {
	_, mi, data := peerTorrent(t, 3<<20+4321, metainfo.Hybrid)
	ih := mi.Info.InfoHashV1()
	for _, tc := range []struct {
		name        string
		server      fmse.Policy
		obfuscation torrent.HeaderObfuscationPolicy
		provides    amse.CryptoMethod
		encrypted   bool
		obfuscated  bool
	}{
		{"default", fmse.Allow, torrent.HeaderObfuscationPolicy{Preferred: true}, 0, false, true},
		{"rc4-forced", fmse.Allow, torrent.HeaderObfuscationPolicy{Preferred: true, RequirePreferred: true}, amse.CryptoMethodRC4, true, true},
		{"rc4-required-both-sides", fmse.Require, torrent.HeaderObfuscationPolicy{Preferred: true, RequirePreferred: true}, amse.AllSupportedCrypto, true, true},
		{"plaintext-forced", fmse.Prefer, torrent.HeaderObfuscationPolicy{Preferred: false, RequirePreferred: true}, 0, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sd := &fibSeeder{mi: mi, data: data, pending: map[*peer.Conn][]peer.Request{}}
			s := peer.NewServer(peer.Config{Encryption: tc.server, MaxQueuedBytes: 256 << 10})
			if err := s.AddTorrent(&peer.Torrent{InfoHash: ih, InfoHashV2: mi.Info.InfoHashV2(), Handler: sd}); err != nil {
				t.Fatal(err)
			}
			engine := runFib(t, fib.Binding{Addr: "127.0.0.1:0", Handler: s})
			addrs, err := engine.BindingAddrs(0)
			if err != nil {
				t.Fatal(err)
			}

			dir := t.TempDir()
			cl := newAnacrolix(t, dir, false, tc.obfuscation, tc.provides)
			at, _ := cl.AddTorrentInfoHash(ametainfo.Hash(ih))
			at.AddPeers([]torrent.PeerInfo{{Addr: addrs[0].(*net.TCPAddr), Trusted: true}})
			ctx, cancel := context.WithTimeout(context.Background(), peerTimeout)
			defer cancel()
			select {
			case <-at.GotInfo():
			case <-ctx.Done():
				t.Fatal("anacrolix got no metadata from fib")
			}
			at.DownloadAll()
			select {
			case <-at.Complete().On():
			case <-ctx.Done():
				t.Fatalf("anacrolix incomplete: %d of %d bytes", at.BytesCompleted(), len(data))
			}
			r := at.NewReader()
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatal("anacrolix's data differs")
			}
			sd.mu.Lock()
			conns := append([]*peer.Conn(nil), sd.conns...)
			served := sd.served
			sd.mu.Unlock()
			if len(conns) == 0 || served == 0 {
				t.Fatalf("%d connections, %d blocks served", len(conns), served)
			}
			c := conns[len(conns)-1]
			if c.Encrypted() != tc.encrypted || c.Obfuscated() != tc.obfuscated {
				t.Fatalf("encrypted %v obfuscated %v", c.Encrypted(), c.Obfuscated())
			}
			if c.PeerID() != cl.PeerID() {
				t.Fatalf("peer ID %q, anacrolix's is %q", c.PeerID(), cl.PeerID())
			}
		})
	}
}

// TestPeerMSEWithAnacrolix runs fib's MSE against anacrolix's, each in
// each role, finding the key among many.
func TestPeerMSEWithAnacrolix(t *testing.T) {
	var keys fmse.KeyTable
	var all [][20]byte
	for i := range 100 {
		k := [20]byte{byte(i), 0xbe, 0xef}
		keys.Add(k)
		all = append(all, k)
	}
	skey := all[57]
	ia := []byte("initial payload")

	for _, method := range []amse.CryptoMethod{amse.CryptoMethodPlaintext, amse.CryptoMethodRC4} {
		t.Run(fmt.Sprintf("anacrolix-initiates-%d", method), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			errc := make(chan error, 1)
			go func() {
				rw, m, err := amse.InitiateHandshake(a, skey[:], ia, method)
				if err == nil && m != method {
					err = fmt.Errorf("anacrolix got method %d", m)
				}
				if err == nil {
					_, err = rw.Write([]byte("ping"))
				}
				errc <- err
			}()
			c, err := fmse.Server(b, &keys, fmse.Allow, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if c.Result.SKey != skey || fmse.CryptoMethod(method) != c.Result.Method {
				t.Fatalf("result %+v", c.Result)
			}
			buf := make([]byte, len(ia)+4)
			if _, err := io.ReadFull(c, buf); err != nil {
				t.Fatal(err)
			}
			if string(buf) != string(ia)+"ping" {
				t.Fatalf("read %q", buf)
			}
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		})
		t.Run(fmt.Sprintf("fib-initiates-%d", method), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			type result struct {
				rw  io.ReadWriter
				m   amse.CryptoMethod
				err error
			}
			resc := make(chan result, 1)
			go func() {
				rw, m, err := amse.ReceiveHandshake(context.Background(), b, func(f func([]byte) bool) {
					for _, k := range all {
						if !f(k[:]) {
							return
						}
					}
				}, func(amse.CryptoMethod) amse.CryptoMethod { return method })
				resc <- result{rw, m, err}
			}()
			c, err := fmse.Client(a, skey, fmse.Both, ia, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			res := <-resc
			if res.err != nil || res.m != method {
				t.Fatalf("anacrolix: %v method %d", res.err, res.m)
			}
			go func() { _, _ = c.Write([]byte("pong")) }()
			buf := make([]byte, len(ia)+4)
			if _, err := io.ReadFull(res.rw, buf); err != nil {
				t.Fatal(err)
			}
			if string(buf) != string(ia)+"pong" {
				t.Fatalf("anacrolix read %q", buf)
			}
		})
	}
}
