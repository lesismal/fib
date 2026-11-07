package bittorrent

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

func TestWebSeed(t *testing.T) {
	for _, c := range []struct {
		name    string
		version metainfo.Version
		sizes   []int
		single  bool
	}{
		{"multi-hybrid", metainfo.Hybrid, []int{300_000, 5, 120_000}, false},
		{"single-v1-file-url", metainfo.V1, []int{250_000}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			sdir, ldir := t.TempDir(), t.TempDir()
			name := "content"
			files := testFiles(t, sdir, name, c.sizes...)
			var ranges atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") != "" {
					ranges.Add(1)
				}
				http.FileServer(http.Dir(sdir)).ServeHTTP(w, r)
			}))
			defer srv.Close()
			b := metainfo.Builder{Version: c.version, PieceLength: 16 << 10, WebSeeds: []string{srv.URL + "/"}}
			if c.single {
				b.WebSeeds = []string{srv.URL + "/" + name}
			}
			mi, err := b.BuildPath(filepath.Join(sdir, name))
			if err != nil {
				t.Fatal(err)
			}
			leecher := newTestClient(t, ldir)
			lt, err := leecher.AddTorrent(mi, nil)
			if err != nil {
				t.Fatal(err)
			}
			waitDone(t, lt)
			checkFiles(t, ldir, files)
			if ranges.Load() == 0 {
				t.Fatal("no range requests")
			}
		})
	}
}
