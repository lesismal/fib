package bittorrent

import (
	"net"
	"strconv"
	"testing"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

func runEngine(t *testing.T) *fib.Engine {
	t.Helper()
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	engine, err := fib.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() { engine.Stop(); <-done; _ = engine.Close() })
	return engine
}

// Two clients on one engine exchange a torrent; closing one leaves the
// engine and the other working, and frees its port, TCP and UDP, on the
// engine.
func TestSharedEngine(t *testing.T) {
	engine := runEngine(t)
	shared := func(c *Config) { c.SharedEngine = engine }
	sdir, ldir := t.TempDir(), t.TempDir()
	files := testFiles(t, sdir, "content", 200_000, 30_000)
	mi := buildTorrent(t, sdir, "content", metainfo.Hybrid, 16<<10)
	seeder, _ := seed(t, sdir, mi, shared)
	leecher := newTestClient(t, ldir, shared)
	if seeder.Engine() != engine || leecher.Engine() != engine {
		t.Fatal("Engine is not the shared engine")
	}
	lt, err := leecher.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, lt)
	checkFiles(t, ldir, files)

	port := leecher.Port()
	if err := leecher.Close(); err != nil {
		t.Fatal(err)
	}
	// The port is free again on the engine, for TCP and UDP alike.
	again := newTestClient(t, t.TempDir(), shared, func(c *Config) {
		c.ListenAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	})
	if again.Port() != port {
		t.Fatalf("port %d, want %d", again.Port(), port)
	}
	_ = again.Close()

	// The seeder still serves, over TCP and over uTP.
	for name, only := range map[string]clientOpt{
		"tcp": func(c *Config) { c.DisableUTP = true },
		"utp": func(c *Config) { c.DisableTCP = true },
	} {
		dir := t.TempDir()
		l := newTestClient(t, dir, only)
		tor, err := l.AddTorrent(mi, &AddOptions{Peers: []string{addrOf(seeder)}})
		if err != nil {
			t.Fatal(err)
		}
		waitDone(t, tor)
		checkFiles(t, dir, files)
		t.Logf("%s download from the remaining client done", name)
	}

	// Closing the last client leaves the engine running.
	_ = seeder.Close()
	if _, err := engine.AddBinding(fib.Binding{Network: "tcp4", Addr: "127.0.0.1:0", Handler: seeder.server}); err != nil {
		t.Fatalf("engine stopped with its clients: %v", err)
	}
}
