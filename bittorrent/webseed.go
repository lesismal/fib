package bittorrent

import (
	"context"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Web seeds (BEP 19, "GetRight style"): HTTP servers that have the
// torrent's files at the torrent's url-list URLs, from which pieces come
// with range requests, through the client's fib/http Client. A web seed
// downloads whole pieces that no peer is downloading, a few at a time;
// pieces it gets wrong count against it, and after a few it is dropped.

const (
	webSeedConcurrency = 4
	webSeedMaxFails    = 5
	webSeedTimeout     = time.Minute
)

type webSeed struct {
	url      string
	inFlight int
	fails    int
	nextTry  time.Time
}

// startWebSeedsLocked starts web seed downloads where a seed is free and
// a piece wants downloading.
func (t *Torrent) startWebSeedsLocked(now time.Time) {
	if t.info == nil || t.checking || t.storage == nil || !t.running() || t.missing == 0 || t.c.httpClient == nil {
		return
	}
	if t.seeds == nil {
		for _, u := range t.webSeeds {
			if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
				t.seeds = append(t.seeds, &webSeed{url: u})
			}
		}
	}
	for _, ws := range t.seeds {
		for ws.inFlight < webSeedConcurrency && ws.fails < webSeedMaxFails && !now.Before(ws.nextTry) && t.downAllow() {
			i, ok := t.pickPiece(func(i int) bool { return t.wanted(i) })
			if !ok {
				return
			}
			p := newPartial(i, t.pieces[i].dataLen)
			for b := range p.blocks {
				p.blocks[b].reqs++
			}
			t.pieces[i].partial = p
			t.partials[i] = p
			ws.inFlight++
			go t.fetchWebSeed(ws, i, p)
		}
	}
}

// webSeedURL is the URL of file f of the torrent at a web seed: the URL
// itself for a single-file torrent, unless it ends in a slash, which takes
// the torrent's name; the name and the file's path below it for a
// multi-file torrent.
func webSeedURL(base string, info *metainfo.Info, f metainfo.File) string {
	if len(info.Files) == 0 && f.Path == nil {
		if strings.HasSuffix(base, "/") {
			return base + url.PathEscape(info.Name)
		}
		return base
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	parts := []string{url.PathEscape(info.Name)}
	for _, p := range f.Path {
		parts = append(parts, url.PathEscape(p))
	}
	return base + strings.Join(parts, "/")
}

func (t *Torrent) fetchWebSeed(ws *webSeed, i int, p *partial) {
	t.mu.Lock()
	info, st := t.info, t.storage
	n := t.pieces[i].dataLen
	ctx := t.run
	t.mu.Unlock()
	if ctx == nil {
		ctx = t.c.ctx
	}
	off := info.PieceOffset(i)
	buf := make([]byte, info.PieceSize(i))
	err := t.webSeedRead(ctx, ws.url, info, off, buf[:n])
	if err == nil && st != nil {
		err = writeData(st, info, off, buf[:n])
	}
	t.mu.Lock()
	for b := range p.blocks {
		if p.blocks[b].reqs > 0 {
			p.blocks[b].reqs--
		}
	}
	verify := false
	if err == nil && t.partials[i] == p && !p.busy {
		for b := range p.blocks {
			if !p.blocks[b].done {
				p.blocks[b].done = true
				p.done++
			}
		}
		t.downloaded.Add(n)
		t.downRate.add(int(n))
		t.downTake(int(n))
		verify = p.writing == 0
		p.busy = verify
	}
	if err != nil {
		ws.fails++
		ws.nextTry = time.Now().Add(5 * time.Second << min(ws.fails, 6))
	}
	before := t.hashFails
	t.mu.Unlock()
	if verify {
		t.verifyPiece(i, p)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	ws.inFlight--
	if t.hashFails > before {
		ws.fails++
	}
	// The next piece goes at once rather than at the next tick.
	t.startWebSeedsLocked(time.Now())
}

// webSeedRead reads the torrent's bytes at off into p from a web seed,
// with a range request per file the range covers.
func (t *Torrent) webSeedRead(ctx context.Context, base string, info *metainfo.Info, off int64, p []byte) error {
	files := info.FileList()
	for _, seg := range info.Segments(off, int64(len(p))) {
		f := files[seg.File]
		if f.IsPad() {
			continue
		}
		start := f.Offset + seg.Offset - off
		dst := p[start : start+seg.Length]
		rctx, cancel := context.WithTimeout(ctx, webSeedTimeout)
		err := t.webSeedGet(rctx, webSeedURL(base, info, f), seg.Offset, dst)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Torrent) webSeedGet(ctx context.Context, u string, off int64, dst []byte) error {
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(dst))-1))
	req.Header.Set("User-Agent", "fib-bittorrent/1")
	resp, err := t.c.httpClient.Go(req).Wait()
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case stdhttp.StatusPartialContent:
	case stdhttp.StatusOK:
		// A server without ranges sends the whole file: skip to off.
		if _, err := io.CopyN(io.Discard, resp.Body, off); err != nil {
			return err
		}
	default:
		return fmt.Errorf("bittorrent: web seed %s: %s", u, resp.Status)
	}
	if _, err := io.ReadFull(resp.Body, dst); err != nil {
		return fmt.Errorf("bittorrent: web seed %s: %w", u, err)
	}
	return nil
}
