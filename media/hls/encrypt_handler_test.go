//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package hls

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// keyServer answers the key URIs of h's Muxers ahead of h, as a caller
// does with its own access control: key-<session>-<epoch>.key, for a
// session h runs and an epoch it has reached.
type keyServer struct {
	h      *Handler
	prefix string
}

func (k *keyServer) ServeHTTP(c *http.Context) {
	path := strings.TrimPrefix(c.Request.URL.Path, k.prefix)
	i := strings.LastIndexByte(path, '/')
	name := path[i+1:]
	rest, ok := strings.CutPrefix(name, "key-")
	if !ok || i <= 0 {
		k.h.ServeHTTP(c)
		return
	}
	session, epochs, _ := strings.Cut(strings.TrimSuffix(rest, ".key"), "-")
	epoch, err := strconv.ParseUint(epochs, 10, 64)
	m := k.h.SessionMuxer(path[:i], session)
	if err != nil || m == nil {
		_ = c.WriteResponse(http.Response{StatusCode: 404})
		return
	}
	if cur, ok := m.KeyEpoch(); !ok || epoch > cur {
		_ = c.WriteResponse(http.Response{StatusCode: 404})
		return
	}
	key := keyOf(session, epoch)
	_ = c.WriteResponse(http.Response{StatusCode: 200, Body: key[:]})
}

func TestHandlerEncryption(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	refuse := errors.New("no keys")
	h := NewHandler(HandlerConfig{
		Hub:     hb,
		Prefix:  "/hls",
		Formats: []Format{MPEGTS},
		Muxer:   Config{PartDuration: 500 * time.Millisecond},
		Encrypt: func(path string) (*Encryption, error) {
			switch path {
			case "live/clear":
				return nil, nil
			case "live/refused":
				return nil, refuse
			}
			return (&testKeys{}).encryption(2), nil
		},
	})
	base := serveHandler(t, h, &keyServer{h: h, prefix: "/hls/"}) + "/hls/"
	pub, write := publish(t, hb, "live/s", []*av.Track{h264Track(t), aacTrack(t)})
	defer pub.Close()
	pubR, _ := publish(t, hb, "live/refused", []*av.Track{h264Track(t), aacTrack(t)})
	defer pubR.Close()

	// A stream whose encryption fails is not served at all.
	if code := statusOf(t, base+"live/refused/media.m3u8"); code != 503 || h.Muxer("live/refused") != nil {
		t.Fatal(code)
	}
	got := start(t, h, base+"live/s/media.m3u8", "live/s")
	gotTS := make(chan int, 1)
	go func() { gotTS <- statusOf(t, base+"live/s/media.m3u8?format=ts") }()
	until(t, "a TS muxer", func() bool { return h.FormatMuxer("live/s", "ts") != nil })
	write(9 * time.Second)
	if code, codeTS := <-got, <-gotTS; code != 200 || codeTS != 200 {
		t.Fatal(code, codeTS)
	}
	for _, format := range []string{"", "ts"} {
		q := ""
		if format != "" {
			q = "?format=" + format
		}
		resp, body := get(t, "GET", base+"live/s/media.m3u8"+q)
		if resp.StatusCode != 200 {
			t.Fatal(resp.StatusCode)
		}
		m := h.FormatMuxer("live/s", format)
		if h.SessionMuxer("live/s", m.Session()) != m || h.SessionMuxer("live/x", m.Session()) != nil || h.SessionMuxer("live/s", "x") != nil {
			t.Fatal("SessionMuxer")
		}
		keys, inForce := playlistKeys(t, []byte(body))
		if len(keys) < 2 {
			t.Fatal(body)
		}
		p := parsePlaylist(t, []byte(body))
		// The keys the playlist names are served, a later one not.
		fetched := map[string][]byte{}
		for _, k := range keys {
			resp, key := get(t, "GET", base+"live/s/"+k.uri)
			if resp.StatusCode != 200 || len(key) != 16 {
				t.Fatal(k.uri, resp.StatusCode)
			}
			fetched[k.uri] = []byte(key)
		}
		cur, _ := m.KeyEpoch()
		if code := statusOf(t, base+"live/s/"+keyURI(m.Session(), cur+1)); code != 404 {
			t.Fatal(code)
		}
		for _, name := range append(p.segs, p.parts...) {
			resp, data := get(t, "GET", base+"live/s/"+name)
			if resp.StatusCode != 200 {
				t.Fatal(name, resp.StatusCode)
			}
			clear := decrypt(t, [16]byte(fetched[inForce[name]]), msnOf(t, name), []byte(data))
			if format == "ts" && clear[0] != 0x47 || format == "" && !bytes.Contains(clear[:8], []byte("moof")) {
				t.Fatal(name, "does not decrypt to media")
			}
		}
		for _, name := range p.maps {
			if _, data := get(t, "GET", base+"live/s/"+name); !strings.Contains(data[:8], "ftyp") {
				t.Fatal("init encrypted")
			}
		}
	}
	// A stream Encrypt leaves clear is clear.
	pubC, writeC := publish(t, hb, "live/clear", []*av.Track{h264Track(t), aacTrack(t)})
	defer pubC.Close()
	gotC := start(t, h, base+"live/clear/media.m3u8", "live/clear")
	writeC(5 * time.Second)
	if code := <-gotC; code != 200 {
		t.Fatal(code)
	}
	if _, body := get(t, "GET", base+"live/clear/media.m3u8"); strings.Contains(body, "#EXT-X-KEY") || !strings.Contains(body, "#EXTINF") {
		t.Fatal(body)
	}
	if _, ok := h.Muxer("live/clear").KeyEpoch(); ok {
		t.Fatal("clear stream has keys")
	}
}
