package interop

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent"
	"github.com/lesismal/fib/bittorrent/bencode"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/tracker"
	fibhttp "github.com/lesismal/fib/http"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent"
	ametainfo "github.com/anacrolix/torrent/metainfo"
	amse "github.com/anacrolix/torrent/mse"
	"github.com/anacrolix/torrent/storage"
)

// The client tests run fib's whole client against anacrolix/torrent's:
// each downloads from the other, from a .torrent file and from a magnet
// link that has only the info hash, over TCP and over uTP (anacrolix's
// uTP is go-libutp, cgo), with encryption required, and a hybrid torrent;
// one finds its peer through a fib tracker.

const clientTimeout = 90 * time.Second

// clientFiles writes a multi-file torrent's content below dir/name: a few
// files of sizes that do not fall on piece boundaries.
func clientFiles(t *testing.T, dir, name string) map[string][]byte {
	t.Helper()
	r := rand.New(rand.NewPCG(7, 11))
	out := map[string][]byte{}
	for i, n := range []int{1<<20 + 333, 200_000, 77, 600_001} {
		b := make([]byte, n)
		for j := range b {
			b[j] = byte(r.Uint32())
		}
		rel := filepath.Join(name, fmt.Sprintf("sub%d", i%2), fmt.Sprintf("f%d.bin", i))
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		out[rel] = b
	}
	return out
}

func clientTorrent(t *testing.T, dir, name string, v metainfo.Version, trackers ...string) *metainfo.MetaInfo {
	t.Helper()
	b := metainfo.Builder{Version: v, PieceLength: 64 << 10}
	if len(trackers) > 0 {
		b.Trackers = [][]string{trackers}
	}
	mi, err := b.BuildPath(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return mi
}

type clientCase struct {
	name    string
	version metainfo.Version
	magnet  bool
	utp     bool
	require bool
}

func clientCases() []clientCase {
	return []clientCase{
		{name: "torrent-tcp", version: metainfo.V1},
		{name: "torrent-utp", version: metainfo.V1, utp: true},
		{name: "magnet-tcp", version: metainfo.V1, magnet: true},
		{name: "magnet-utp", version: metainfo.V1, magnet: true, utp: true},
		{name: "magnet-tcp-rc4-required", version: metainfo.V1, magnet: true, require: true},
		{name: "torrent-utp-rc4-required", version: metainfo.V1, utp: true, require: true},
		{name: "hybrid-magnet-tcp", version: metainfo.Hybrid, magnet: true},
		{name: "hybrid-torrent-utp", version: metainfo.Hybrid, utp: true},
	}
}

// newAnacrolixClient starts an anacrolix client on 127.0.0.1, over TCP or
// over uTP alone, without DHT, PEX or port mapping.
func newAnacrolixClient(t *testing.T, dir string, seed bool, c clientCase, trackers bool) *torrent.Client {
	t.Helper()
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = dir
	cfg.DefaultStorage = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir:   dir,
		PieceCompletion: storage.NewMapPieceCompletion(),
		UsePartFiles:    g.Some(false),
	})
	cfg.Seed = seed
	cfg.NoDHT = true
	cfg.DisablePEX = true
	cfg.DisableUTP = !c.utp
	cfg.DisableTCP = c.utp
	cfg.DisableTrackers = !trackers
	cfg.DisableWebtorrent = true
	cfg.DisableWebseeds = true
	cfg.DisableIPv6 = true
	cfg.NoDefaultPortForwarding = true
	cfg.ListenHost = func(string) string { return "127.0.0.1" }
	cfg.ListenPort = 0
	if c.require {
		cfg.HeaderObfuscationPolicy = torrent.HeaderObfuscationPolicy{Preferred: true, RequirePreferred: true}
		cfg.CryptoProvides = amse.CryptoMethodRC4
	}
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

// peerLog keeps what a fib client's events said of its connections.
type peerLog struct {
	mu     sync.Mutex
	peers  []bittorrent.PeerInfo
	closes []string
}

func (l *peerLog) event(e bittorrent.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch e.Type {
	case bittorrent.EventPeerConnected:
		l.peers = append(l.peers, *e.PeerInfo)
	case bittorrent.EventPeerDisconnected:
		l.closes = append(l.closes, fmt.Sprintf("%v: %v", e.Peer, e.Err))
	}
}

func (l *peerLog) connected() []bittorrent.PeerInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]bittorrent.PeerInfo(nil), l.peers...)
}

func (l *peerLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return fmt.Sprintf("connected %v, closed %v", l.peers, l.closes)
}

func newFibClient(t *testing.T, dir string, c clientCase, trackers bool) (*bittorrent.Client, *peerLog) {
	t.Helper()
	log := &peerLog{}
	cfg := bittorrent.Config{
		OnEvent:         log.event,
		DataDir:         dir,
		ListenAddr:      "127.0.0.1:0",
		DisableDHT:      true,
		DisableLSD:      true,
		DisablePEX:      true,
		DisableTrackers: !trackers,
		DisableTCP:      c.utp,
		DisableUTP:      !c.utp,
	}
	if c.require {
		cfg.Encryption = bittorrent.EncryptionRequire
	}
	cl, err := bittorrent.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl, log
}

func anacrolixPort(cl *torrent.Client) int {
	for _, a := range cl.ListenAddrs() {
		switch a := a.(type) {
		case *net.TCPAddr:
			return a.Port
		case *net.UDPAddr:
			return a.Port
		}
	}
	return 0
}

// sameFiles checks that dir has files as want has them.
func sameFiles(t *testing.T, dir string, want map[string][]byte) {
	t.Helper()
	for rel, b := range want {
		got, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("%s differs", rel)
		}
	}
}

// fibWait waits for a fib torrent's download.
func fibWait(t *testing.T, tor *bittorrent.Torrent, log *peerLog) {
	t.Helper()
	select {
	case <-tor.Done():
	case <-time.After(clientTimeout):
		t.Fatalf("fib incomplete: %+v; %v", tor.Stats(), log)
	}
}

func checkPeers(t *testing.T, peers []bittorrent.PeerInfo, c clientCase) {
	t.Helper()
	if len(peers) == 0 {
		t.Fatal("never saw a peer")
	}
	for _, p := range peers {
		if p.UTP != c.utp {
			t.Errorf("%v: uTP %v", p.Addr, p.UTP)
		}
		if c.require && !p.Encrypted {
			t.Errorf("%v: not encrypted", p.Addr)
		}
	}
}

func TestClientFibDownloadsFromAnacrolix(t *testing.T) {
	for _, c := range clientCases() {
		t.Run(c.name, func(t *testing.T) {
			if c.utp && !haveLibutp {
				t.Skip("anacrolix's uTP is go-libutp only with cgo")
			}
			sdir := t.TempDir()
			files := clientFiles(t, sdir, "content")
			mi := clientTorrent(t, sdir, "content", c.version)
			seeder := newAnacrolixClient(t, sdir, true, c, false)
			at, err := seeder.AddTorrent(anacrolixTorrent(t, mi))
			if err != nil {
				t.Fatal(err)
			}
			if err := at.VerifyData(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-at.Complete().On():
			case <-time.After(clientTimeout):
				t.Fatal("anacrolix does not have the data it seeds")
			}
			addr := fmt.Sprintf("127.0.0.1:%d", anacrolixPort(seeder))

			ldir := t.TempDir()
			cl, log := newFibClient(t, ldir, c, false)
			var tor *bittorrent.Torrent
			if c.magnet {
				m := &metainfo.Magnet{InfoHash: mi.Info.InfoHashV1(), Peers: []string{addr}}
				tor, err = cl.AddMagnet(m.String(), nil)
			} else {
				tor, err = cl.AddTorrent(mi, &bittorrent.AddOptions{Peers: []string{addr}})
			}
			if err != nil {
				t.Fatal(err)
			}
			fibWait(t, tor, log)
			sameFiles(t, ldir, files)
			checkPeers(t, log.connected(), c)
		})
	}
}

func TestClientAnacrolixDownloadsFromFib(t *testing.T) {
	for _, c := range clientCases() {
		t.Run(c.name, func(t *testing.T) {
			if c.utp && !haveLibutp {
				t.Skip("anacrolix's uTP is go-libutp only with cgo")
			}
			sdir := t.TempDir()
			files := clientFiles(t, sdir, "content")
			mi := clientTorrent(t, sdir, "content", c.version)
			seeder, log := newFibClient(t, sdir, c, false)
			st, err := seeder.AddTorrent(mi, nil)
			if err != nil {
				t.Fatal(err)
			}
			fibWait(t, st, log)

			ldir := t.TempDir()
			cl := newAnacrolixClient(t, ldir, false, c, false)
			var at *torrent.Torrent
			if c.magnet {
				at, _ = cl.AddTorrentInfoHash(ametainfo.Hash(mi.Info.InfoHashV1()))
			} else {
				at, err = cl.AddTorrent(anacrolixTorrent(t, mi))
				if err != nil {
					t.Fatal(err)
				}
			}
			addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: seeder.Port()}
			at.AddPeers([]torrent.PeerInfo{{Addr: addr, Trusted: true}})
			ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
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
				t.Fatalf("anacrolix incomplete: %d of %d bytes; fib %+v; %v", at.BytesCompleted(), mi.Info.TotalLength(), st.Stats(), log)
			}
			for _, f := range at.Files() {
				rel := filepath.FromSlash(f.Path())
				want, ok := files[filepath.Join("content", rel)]
				if !ok {
					continue // a padding file
				}
				r := f.NewReader()
				got, err := io.ReadAll(r)
				r.Close()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s differs in anacrolix", rel)
				}
			}
			checkPeers(t, log.connected(), c)
			if up := st.Stats().Uploaded; up < mi.Info.TotalLength()/2 {
				t.Fatalf("fib uploaded %d bytes", up)
			}
		})
	}
}

// TestClientTrackerDiscovery has an anacrolix seeder announce to a fib
// tracker, where fib's client, with no more than a magnet link of the
// tracker, finds it.
func TestClientTrackerDiscovery(t *testing.T) {
	srv := tracker.NewServer(tracker.ServerConfig{Interval: 2 * time.Second})
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	engine, err := fib.New(cfg, fib.Binding{Network: "tcp4", Addr: "127.0.0.1:0", Handler: fibhttp.NewHandler(srv)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	addrs, _ := engine.BindingAddrs(0)
	announce := "http://" + addrs[0].String() + "/announce"

	c := clientCase{name: "tracker", version: metainfo.V1}
	sdir := t.TempDir()
	files := clientFiles(t, sdir, "content")
	mi := clientTorrent(t, sdir, "content", c.version, announce)
	seeder := newAnacrolixClient(t, sdir, true, c, true)
	at, err := seeder.AddTorrent(anacrolixTorrent(t, mi))
	if err != nil {
		t.Fatal(err)
	}
	if err := at.VerifyData(); err != nil {
		t.Fatal(err)
	}

	ldir := t.TempDir()
	cl, log := newFibClient(t, ldir, c, true)
	m := &metainfo.Magnet{InfoHash: mi.Info.InfoHashV1(), Trackers: []string{announce}}
	tor, err := cl.AddMagnet(m.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fibWait(t, tor, log)
	sameFiles(t, ldir, files)
	want := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(anacrolixPort(seeder)))
	peers := log.connected()
	for _, p := range peers {
		if p.Addr == want {
			return
		}
	}
	t.Fatalf("fib's peers %v are not anacrolix at %v", peers, want)
}

// libtorrentPeer runs libtorrent_peer.py, a libtorrent-rasterbar peer, and
// hands its output lines to a channel.
type libtorrentPeer struct {
	cmd   *exec.Cmd
	port  int
	lines chan string
}

func startLibtorrent(t *testing.T, args ...string) *libtorrentPeer {
	t.Helper()
	cmd := exec.Command("python3", append([]string{"libtorrent_peer.py"}, args...)...)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &libtorrentPeer{cmd: cmd, lines: make(chan string, 1024)}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			p.lines <- sc.Text()
		}
		close(p.lines)
	}()
	for line := range p.lines {
		if _, err := fmt.Sscanf(line, "PORT %d", &p.port); err == nil {
			return p
		}
	}
	t.Fatal("libtorrent_peer.py ended without a port")
	return nil
}

// wait waits for the line "DONE", logging the rest.
func (p *libtorrentPeer) wait(t *testing.T, what string) {
	t.Helper()
	deadline := time.After(clientTimeout)
	for {
		select {
		case line, ok := <-p.lines:
			if !ok {
				t.Fatalf("libtorrent ended before %s", what)
			}
			if line == "DONE" {
				return
			}
			t.Log("libtorrent:", line)
		case <-deadline:
			t.Fatalf("libtorrent: no %s", what)
		}
	}
}

// TestClientLibtorrent runs fib's client against libtorrent-rasterbar's,
// through its Python binding, both ways: it needs python3-libtorrent, so
// it runs only with FIB_LIBTORRENT set, as libtorrent.sh runs it in a
// container.
func TestClientLibtorrent(t *testing.T) {
	if os.Getenv("FIB_LIBTORRENT") == "" {
		t.Skip("set FIB_LIBTORRENT=1 with python3-libtorrent installed; libtorrent.sh does")
	}
	cases := append(clientCases(),
		clientCase{name: "v2-magnet-tcp", version: metainfo.V2, magnet: true},
		clientCase{name: "v2-torrent-utp", version: metainfo.V2, utp: true},
		clientCase{name: "v2-magnet-utp-rc4-required", version: metainfo.V2, magnet: true, utp: true, require: true},
	)
	flags := func(c clientCase) []string {
		var f []string
		if c.utp {
			f = append(f, "--utp")
		}
		if c.require {
			f = append(f, "--require")
		}
		return f
	}
	writeTorrent := func(t *testing.T, mi *metainfo.MetaInfo) string {
		enc, err := bencode.Marshal(mi)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(t.TempDir(), "t.torrent")
		if err := os.WriteFile(p, enc, 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	magnet := func(mi *metainfo.MetaInfo) string {
		m := &metainfo.Magnet{}
		if mi.Info.HasV1() {
			m.InfoHash = mi.Info.InfoHashV1()
		} else {
			m.InfoHashV2 = mi.Info.InfoHashV2()
		}
		return m.String()
	}
	for _, c := range cases {
		t.Run("fib-from-libtorrent/"+c.name, func(t *testing.T) {
			sdir := t.TempDir()
			files := clientFiles(t, sdir, "content")
			mi := clientTorrent(t, sdir, "content", c.version)
			lt := startLibtorrent(t, append([]string{"seed", writeTorrent(t, mi), sdir}, flags(c)...)...)
			addr := fmt.Sprintf("127.0.0.1:%d", lt.port)
			ldir := t.TempDir()
			cl, log := newFibClient(t, ldir, c, false)
			var tor *bittorrent.Torrent
			var err error
			if c.magnet {
				m, _ := metainfo.ParseMagnet(magnet(mi))
				m.Peers = []string{addr}
				tor, err = cl.AddMagnet(m.String(), nil)
			} else {
				tor, err = cl.AddTorrent(mi, &bittorrent.AddOptions{Peers: []string{addr}})
			}
			if err != nil {
				t.Fatal(err)
			}
			fibWait(t, tor, log)
			sameFiles(t, ldir, files)
			checkPeers(t, log.connected(), c)
		})
		t.Run("libtorrent-from-fib/"+c.name, func(t *testing.T) {
			sdir := t.TempDir()
			files := clientFiles(t, sdir, "content")
			mi := clientTorrent(t, sdir, "content", c.version)
			seeder, log := newFibClient(t, sdir, c, false)
			st, err := seeder.AddTorrent(mi, nil)
			if err != nil {
				t.Fatal(err)
			}
			fibWait(t, st, log)
			src := writeTorrent(t, mi)
			if c.magnet {
				src = magnet(mi)
			}
			ldir := t.TempDir()
			lt := startLibtorrent(t, append([]string{"leech", src, ldir, "--peer", fmt.Sprintf("127.0.0.1:%d", seeder.Port())}, flags(c)...)...)
			lt.wait(t, "download")
			sameFiles(t, ldir, files)
			checkPeers(t, log.connected(), c)
		})
	}
}
