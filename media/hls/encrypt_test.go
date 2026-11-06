package hls

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
)

// testKeys supplies the keys of an Encryption: one for each session and
// epoch, recording what was asked.
type testKeys struct {
	asked []uint64
	fail  uint64 // the epoch whose key is refused, 0 for none
	uri   string // a URI in place of the usual
}

func keyOf(session string, epoch uint64) [16]byte {
	sum := sha256.Sum256([]byte(session + "/" + strconv.FormatUint(epoch, 10)))
	return [16]byte(sum[:16])
}

func keyURI(session string, epoch uint64) string {
	return "key-" + session + "-" + strconv.FormatUint(epoch, 10) + ".key"
}

// inline runs the encryption on the publisher's goroutine, as it cuts,
// so that what a test fed is encrypted when feeding returns.
func inline(job func()) bool {
	job()
	return true
}

// encryption returns an Encryption run inline; async one run as it is by
// default, on goroutines of its own.
func (k *testKeys) encryption(rotation int) *Encryption {
	e := k.async(rotation)
	e.Run = inline
	return e
}

func (k *testKeys) async(rotation int) *Encryption {
	var mu sync.Mutex
	return &Encryption{Rotation: rotation, Key: func(session string, epoch uint64) ([16]byte, string, error) {
		mu.Lock()
		defer mu.Unlock()
		k.asked = append(k.asked, epoch)
		if k.fail != 0 && epoch == k.fail {
			return [16]byte{}, "", errors.New("no key")
		}
		if k.uri != "" {
			return keyOf(session, epoch), k.uri, nil
		}
		return keyOf(session, epoch), keyURI(session, epoch), nil
	}}
}

// decrypt decrypts b, a segment or part of METHOD=AES-128, and strips its
// padding.
func decrypt(t testing.TB, key [16]byte, msn uint64, b []byte) []byte {
	t.Helper()
	if len(b) == 0 || len(b)%aes.BlockSize != 0 {
		t.Fatalf("%d bytes encrypted", len(b))
	}
	block, _ := aes.NewCipher(key[:])
	iv := msnIV(msn)
	out := make([]byte, len(b))
	cipher.NewCBCDecrypter(block, iv[:]).CryptBlocks(out, b)
	p := int(out[len(out)-1])
	if p < 1 || p > aes.BlockSize || !bytes.Equal(out[len(out)-p:], bytes.Repeat([]byte{byte(p)}, p)) {
		t.Fatalf("bad padding %d", p)
	}
	return out[:len(out)-p]
}

// keyLine is an EXT-X-KEY of a playlist and what follows it.
type keyLine struct {
	method, uri string
	before      string // the next line that is not a key: a map, part or segment
}

// playlistKeys returns the EXT-X-KEYs of a media playlist, and for each
// segment and part the URI of the key in force, "" for none.
func playlistKeys(t testing.TB, b []byte) (keys []keyLine, inForce map[string]string) {
	t.Helper()
	inForce = map[string]string{}
	cur := ""
	sc := bufio.NewScanner(bytes.NewReader(b))
	pending := -1
	for sc.Scan() {
		l := sc.Text()
		uri := ""
		switch {
		case strings.HasPrefix(l, "#EXT-X-KEY:"):
			attrs := strings.TrimPrefix(l, "#EXT-X-KEY:")
			k := keyLine{}
			for a := range strings.SplitSeq(attrs, ",") {
				name, v, _ := strings.Cut(a, "=")
				switch name {
				case "METHOD":
					k.method = v
				case "URI":
					k.uri = strings.Trim(v, "\"")
				case "IV":
					t.Fatalf("IV named: %s", l)
				}
			}
			if k.method == "NONE" {
				cur = ""
			} else {
				cur = k.uri
			}
			keys = append(keys, k)
			pending = len(keys) - 1
			continue
		case strings.HasPrefix(l, "#EXT-X-MAP:"):
			uri = strings.Trim(strings.TrimPrefix(l, "#EXT-X-MAP:URI="), "\"")
		case strings.HasPrefix(l, "#EXT-X-PART:"):
			_, uri, _ = strings.Cut(l, "URI=\"")
			uri, _, _ = strings.Cut(uri, "\"")
		case !strings.HasPrefix(l, "#") && l != "":
			uri = l
		default:
			continue
		}
		inForce[uri] = cur
		if pending >= 0 {
			keys[pending].before = uri
			pending = -1
		}
	}
	return keys, inForce
}

// msnOf returns the media sequence number of a segment's or part's name.
func msnOf(t testing.TB, name string) uint64 {
	t.Helper()
	_, rest, _ := strings.Cut(name, "-")
	_, rest, _ = strings.Cut(rest, "-")
	num, _, _ := strings.Cut(strings.TrimSuffix(strings.TrimSuffix(rest, ".m4s"), ".ts"), "-")
	n, err := strconv.ParseUint(num, 10, 64)
	if err != nil {
		t.Fatal(name)
	}
	return n
}

// twin is a clear Muxer and an encrypted one fed the same stream, whose
// files must be the same bytes, encrypted or not.
type twin struct {
	clear, enc *Muxer
	keys       *testKeys
	rotation   int
}

func newTwin(config Config, rotation int) *twin {
	tw := &twin{keys: &testKeys{}, rotation: rotation}
	tw.clear = NewMuxer(config)
	config.Encryption = tw.keys.encryption(rotation)
	tw.enc = NewMuxer(config)
	return tw
}

func (tw *twin) tracks(tracks []*av.Track) {
	tw.clear.OnTracks(tracks)
	tw.enc.OnTracks(tracks)
}

func (tw *twin) feed(pkts []*av.Packet) {
	feed(tw.clear, pkts)
	feed(tw.enc, pkts)
}

// clearName is name, of the encrypted Muxer, as the clear one names it.
func (tw *twin) clearName(name string) string {
	return strings.Replace(name, "-"+tw.enc.session+"-", "-"+tw.clear.session+"-", 1)
}

// check checks the encrypted Muxer's playlist: a key wherever the epoch
// changes, named before the segment's first part and after any EXT-X-MAP,
// clear initialization sections, and segments and parts that decrypt with
// the key in force and their msn's IV to the clear Muxer's bytes. It
// returns the playlist's keys.
func (tw *twin) check(t *testing.T) []keyLine {
	t.Helper()
	b, ok := tw.enc.MediaPlaylist(nil)
	if !ok {
		t.Fatal("no playlist")
	}
	cb, _ := tw.clear.MediaPlaylist(nil)
	if strings.Count(string(b), "#EXT-X-KEY") == 0 || strings.Contains(string(cb), "#EXT-X-KEY") {
		t.Fatalf("%s", b)
	}
	// Without its keys, the playlist is the clear one's.
	var stripped []string
	for l := range strings.SplitSeq(string(b), "\n") {
		if !strings.HasPrefix(l, "#EXT-X-KEY") {
			stripped = append(stripped, tw.clearName(l))
		}
	}
	if strings.Join(stripped, "\n") != string(cb) {
		t.Fatalf("%s\n---\n%s", b, cb)
	}
	keys, inForce := playlistKeys(t, b)
	for i, k := range keys {
		if i > 0 && keys[i-1].method == k.method && keys[i-1].uri == k.uri {
			t.Fatalf("a key repeated: %s", b)
		}
		if k.method == "NONE" && !strings.HasPrefix(k.before, "init-") {
			t.Fatalf("METHOD=NONE before %s: %s", k.before, b)
		}
	}
	p := parsePlaylist(t, b)
	for _, name := range p.maps {
		if inForce[name] != "" {
			t.Fatalf("%s under a key: %s", name, b)
		}
		data, ok := tw.enc.File(name)
		want, _ := tw.clear.File(tw.clearName(name))
		if !ok || !bytes.Equal(data, want) {
			t.Fatal("init section not clear", name)
		}
	}
	for _, name := range append(p.segs, p.parts...) {
		msn := msnOf(t, name)
		epoch := uint64(0)
		if tw.rotation > 0 {
			epoch = msn / uint64(tw.rotation)
		}
		if inForce[name] != keyURI(tw.enc.session, epoch) {
			t.Fatalf("%s under %q: %s", name, inForce[name], b)
		}
		data, ok := tw.enc.File(name)
		want, _ := tw.clear.File(tw.clearName(name))
		if !ok || len(want) == 0 {
			t.Fatal("missing", name)
		}
		if got := decrypt(t, keyOf(tw.enc.session, epoch), msn, data); !bytes.Equal(got, want) {
			t.Fatalf("%s decrypts to %d bytes, not the %d clear", name, len(got), len(want))
		}
	}
	return keys
}

func TestMuxerEncryption(t *testing.T) {
	tw := newTwin(Config{PlaylistSize: 4, KeepSegments: 1}, 2)
	tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 50}
	tw.feed(src.until(9 * time.Second))
	// Segments 0..3: keys of epochs 0 and 1, the MAP before the first.
	keys := tw.check(t)
	if len(keys) != 2 || keys[0].before != string(tw.enc.segName(nil, 0)) {
		t.Fatalf("%+v", keys)
	}
	b, _ := tw.enc.MediaPlaylist(nil)
	if i, j := bytes.Index(b, []byte("#EXT-X-MAP")), bytes.Index(b, []byte("#EXT-X-KEY")); i < 0 || j < i {
		t.Fatalf("%s", b)
	}
	tw.feed(src.until(15 * time.Second))
	tw.check(t)
	// Every epoch's key was asked for once, in order.
	if fmt.Sprint(tw.keys.asked) != "[0 1 2 3]" {
		t.Fatal(tw.keys.asked)
	}
	if e, ok := tw.enc.KeyEpoch(); !ok || e != tw.enc.Next()/2 || tw.enc.Next() != 7 {
		t.Fatal(e, ok, tw.enc.Next())
	}
	if _, ok := tw.clear.KeyEpoch(); ok {
		t.Fatal("clear Muxer has keys")
	}
	// The end encrypts the last segment.
	tw.clear.Close()
	tw.enc.Close()
	tw.check(t)
}

func TestMuxerEncryptionOneKey(t *testing.T) {
	tw := newTwin(Config{PlaylistSize: 5}, 0)
	tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 50}
	tw.feed(src.until(11 * time.Second))
	if keys := tw.check(t); len(keys) != 1 || fmt.Sprint(tw.keys.asked) != "[0]" {
		t.Fatal(keys, tw.keys.asked)
	}
	if e, ok := tw.enc.KeyEpoch(); !ok || e != 0 {
		t.Fatal(e, ok)
	}
}

func TestMuxerEncryptionDiscontinuity(t *testing.T) {
	tw := newTwin(Config{PlaylistSize: 6, KeepSegments: -1}, 3)
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	tw.tracks(tracks)
	src := &source{video: true, audio: 1, gop: 50}
	tw.feed(src.until(5 * time.Second))
	// A republish with the same tracks: a discontinuity, the same init and
	// key.
	tw.tracks(tracks)
	src = &source{video: true, audio: 1, gop: 50}
	tw.feed(src.until(4100 * time.Millisecond))
	keys := tw.check(t)
	if len(keys) != 2 {
		t.Fatalf("%+v", keys)
	}
	// A codec change: a new init, clear, between METHOD=NONE and the key
	// named again.
	tw.tracks([]*av.Track{h265Track(t), aacTrack(t)})
	src = &source{video: true, audio: 1, gop: 50, base: time.Hour}
	tw.feed(src.until(time.Hour + 4100*time.Millisecond))
	keys = tw.check(t)
	b, _ := tw.enc.MediaPlaylist(nil)
	p := parsePlaylist(t, b)
	if len(p.maps) != 2 {
		t.Fatalf("%s", b)
	}
	none := -1
	for i, k := range keys {
		if k.method == "NONE" {
			none = i
		}
	}
	if none < 1 || keys[none].before != p.maps[1] || none+1 >= len(keys) || keys[none+1].method != "AES-128" {
		t.Fatalf("%+v\n%s", keys, b)
	}
	if !strings.Contains(string(b), "#EXT-X-DISCONTINUITY\n#EXT-X-KEY:METHOD=NONE\n#EXT-X-MAP:") {
		t.Fatalf("%s", b)
	}
	// A clock jump: a discontinuity with the same init needs no key tag.
	src.base += time.Minute
	tw.feed(src.until(time.Hour + time.Minute + 6100*time.Millisecond))
	tw.check(t)
}

func TestMuxerEncryptionParts(t *testing.T) {
	tw := newTwin(Config{PartDuration: 500 * time.Millisecond, PlaylistSize: 4}, 2)
	tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 50}
	tw.feed(src.until(10600 * time.Millisecond))
	tw.check(t)
	b, _ := tw.enc.MediaPlaylist(nil)
	p := parsePlaylist(t, b)
	if len(p.parts) < 12 || p.hint == "" {
		t.Fatalf("%s", b)
	}
	// A key tag comes before a segment's first part, never between its
	// parts.
	lines := strings.Split(string(b), "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "#EXT-X-KEY") && i > 0 && strings.HasPrefix(lines[i-1], "#EXT-X-PART:") {
			t.Fatalf("a key among parts: %s", b)
		}
	}
	// Parts of the open segment are encrypted as they come.
	open := tw.enc.open
	if open == nil || len(open.parts) == 0 {
		t.Fatal("no open parts")
	}
	name := string(tw.enc.partName(nil, open.msn, 0))
	data, ok := tw.enc.File(name)
	want, _ := tw.clear.File(tw.clearName(name))
	if !ok || !bytes.Equal(decrypt(t, keyOf(tw.enc.session, open.msn/2), open.msn, data), want) {
		t.Fatal("open part")
	}
	// The parts of segments the playlist no longer lists go, but for one.
	tw.enc.mu.Lock()
	from := tw.enc.partsFrom(tw.enc.first())
	for i, s := range tw.enc.segs {
		if (s.penc != nil) != (i >= from-1) {
			t.Error(i, from, s.penc != nil)
		}
	}
	tw.enc.mu.Unlock()
	if name := string(tw.enc.partName(nil, tw.enc.segs[0].msn, 0)); from > 1 {
		if _, ok := tw.enc.File(name); ok {
			t.Fatal("old part served", name)
		}
	}
	// What is held is the clear Muxer's, the padding, and the parts of the
	// last few segments, in buffers with room for a quarter more.
	if c, e := tw.clear.Bytes(), tw.enc.Bytes(); e < c || e > c*5/2 {
		t.Fatal(c, e)
	}
	tw.clear.Close()
	tw.enc.Close()
	tw.check(t)
}

func TestMuxerEncryptionTS(t *testing.T) {
	for _, ll := range []time.Duration{0, 500 * time.Millisecond} {
		tw := newTwin(Config{Format: MPEGTS, PlaylistSize: 4, PartDuration: ll}, 1)
		tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
		src := &source{video: true, audio: 1, gop: 50}
		tw.feed(avcc(src.until(9 * time.Second)))
		// Four segments, and with parts the open segment's key too.
		keys := tw.check(t)
		if len(keys) != 4 && (ll == 0 || len(keys) != 5) {
			t.Fatalf("%v %+v", ll, keys)
		}
	}
}

func TestMuxerEncryptionKeyError(t *testing.T) {
	// A key refused ends the stream, with nothing of it served clear.
	k := &testKeys{fail: 2}
	m := NewMuxer(Config{PlaylistSize: 6, Encryption: k.encryption(1)})
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 50}
	woke := make(chan struct{})
	if w, err := m.wait(5, -1, func() { close(woke) }); w == nil || err != nil {
		t.Fatal(w, err)
	}
	feed(m, src.until(9*time.Second))
	<-woke
	p := media(t, m)
	if !p.ended || len(p.segs) != 2 || !m.Ended() || fmt.Sprint(k.asked) != "[0 1 2]" {
		t.Fatalf("%+v %v", p, k.asked)
	}
	if _, ok := m.File(string(m.segName(nil, 2))); ok {
		t.Fatal("served")
	}
	// A URI the playlist cannot carry is an error too, as is no Key.
	for _, enc := range []*Encryption{(&testKeys{uri: "a\"b"}).encryption(0), {Run: inline}} {
		m := NewMuxer(Config{Encryption: enc})
		m.OnTracks([]*av.Track{h264Track(t)})
		feed(m, (&source{video: true, audio: -1, gop: 50}).until(5*time.Second))
		if _, ok := m.MediaPlaylist(nil); ok || !m.Ended() {
			t.Fatal("played without a key")
		}
	}
}

func TestMuxerEncryptionMaxBytes(t *testing.T) {
	// Encrypted parts count against MaxBytes like the rest.
	k := &testKeys{}
	m := NewMuxer(Config{MaxBytes: 8192, PlaylistSize: 10, PartDuration: 200 * time.Millisecond, Encryption: k.encryption(1)})
	m.OnTracks([]*av.Track{h264Track(t)})
	src := &source{video: true, audio: -1, gop: 1000}
	feed(m, src.until(20*time.Second))
	if m.Bytes() > 8192+4096 {
		t.Fatal(m.Bytes())
	}
	if _, ok := m.MediaPlaylist(nil); !ok {
		t.Fatal("no playlist")
	}
}

func TestMuxerEncryptionKeyHidden(t *testing.T) {
	// No key is printed, by any verb, by anything that holds it.
	tw := newTwin(Config{PartDuration: 500 * time.Millisecond}, 1)
	tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
	tw.feed((&source{video: true, audio: 1, gop: 50}).until(7 * time.Second))
	m := tw.enc
	var printed []string
	for _, v := range []any{m, *m.enc, m.enc, m.config, m.key, *m.key, m.open, *m.open, m.segs, *m.segs[0]} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x"} {
			printed = append(printed, fmt.Sprintf(verb, v))
		}
	}
	all := strings.Join(printed, "\n")
	for epoch := range uint64(4) {
		key := keyOf(m.session, epoch)
		for _, s := range []string{hex.EncodeToString(key[:]), strings.ToUpper(hex.EncodeToString(key[:])), fmt.Sprint(key[:]), fmt.Sprintf("%#v", key), string(key[:])} {
			if strings.Contains(all, s) {
				t.Fatalf("key %d printed as %q", epoch, s)
			}
		}
		// A hex of its first 8 bytes is enough to give it away.
		if strings.Contains(all, hex.EncodeToString(key[:8])) {
			t.Fatalf("key %d printed", epoch)
		}
	}
	if s := m.key.String(); !strings.Contains(s, "epoch") {
		t.Fatal(s)
	}
}

func TestMuxerEncryptionAllocs(t *testing.T) {
	// Encrypting allocates little beside muxing: the ciphers are made once
	// a key period, and a segment's parts' ciphertext grows as one buffer.
	run := func(enc bool) (allocs uint64, segs uint64) {
		config := Config{PlaylistSize: 4, PartDuration: 500 * time.Millisecond}
		if enc {
			config.Encryption = (&testKeys{}).encryption(0)
		}
		m := NewMuxer(config)
		m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
		src := &source{video: true, audio: 1, gop: 50}
		feed(m, src.until(10*time.Second))
		pkts := src.until(60 * time.Second)
		before := m.Next()
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		feed(m, pkts)
		runtime.ReadMemStats(&b)
		return b.Mallocs - a.Mallocs, m.Next() - before
	}
	c, n := run(false)
	e, _ := run(true)
	t.Logf("%d segments: %d allocations clear, %d encrypted", n, c, e)
	if e > c+6*n {
		t.Fatal(c, e, n)
	}
}

// settle waits for m's encryption to have run out of work.
func settle(t testing.TB, m *Muxer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		done := !m.running && len(m.jobs) == 0
		m.mu.Unlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the encryption does not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMuxerEncryptionAsync(t *testing.T) {
	// Encrypted on goroutines of their own, as by default, while requests
	// read the playlist and every file it names: everything listed is
	// encrypted, and the race detector finds nothing shared unguarded.
	for _, ll := range []time.Duration{0, 200 * time.Millisecond} {
		tw := newTwin(Config{PartDuration: ll, PlaylistSize: 4}, 2)
		tw.enc.enc.Run = nil
		tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				b, ok := tw.enc.MediaPlaylist(nil)
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				p := parsePlaylist(t, b)
				for _, name := range append(append(p.maps, p.segs...), p.parts...) {
					if data, ok := tw.enc.File(name); ok && !strings.HasPrefix(name, "init-") && len(data)%aes.BlockSize != 0 {
						t.Error(name, "listed before it is encrypted")
					}
				}
			}
		}()
		src := &source{video: true, audio: 1, gop: 25}
		for to := time.Second; to <= 12*time.Second; to += 100 * time.Millisecond {
			tw.feed(src.until(to))
		}
		settle(t, tw.enc)
		close(stop)
		<-done
		if tw.enc.Ended() {
			t.Fatal("ended:", tw.enc.encErr)
		}
		tw.check(t)
		tw.clear.Close()
		tw.enc.Close()
		settle(t, tw.enc)
		if p := media(t, tw.enc); !p.ended {
			t.Fatal("not ended")
		}
		tw.check(t)
	}
}

// gateBlock is a cipher whose blocks wait for gate to close: an encryption
// as slow as a test wants.
type gateBlock struct {
	cipher.Block
	gate chan struct{}
}

func (b gateBlock) Encrypt(dst, src []byte) {
	<-b.gate
	b.Block.Encrypt(dst, src)
}

// quick fails unless fn returns within a short while: it held nothing a
// cipher holds.
func quick(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(what, "waits on the encryption")
	}
}

func TestMuxerEncryptionNotHeld(t *testing.T) {
	// A segment being encrypted, however slowly, holds up neither the
	// publisher nor any request: it is listed once encrypted.
	for _, slow := range []string{"cipher", "key"} {
		gate := make(chan struct{})
		k := &testKeys{}
		enc := k.async(1)
		if slow == "cipher" {
			newCipher = func(key []byte) (cipher.Block, error) {
				b, err := aes.NewCipher(key)
				return gateBlock{b, gate}, err
			}
		} else {
			key := enc.Key
			enc.Key = func(session string, epoch uint64) ([16]byte, string, error) {
				<-gate
				return key(session, epoch)
			}
		}
		m := NewMuxer(Config{PartDuration: 500 * time.Millisecond, Encryption: enc})
		m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
		src := &source{video: true, audio: 1, gop: 50}
		woke := make(chan struct{})
		if w, err := m.wait(-1, -1, func() { close(woke) }); w == nil || err != nil {
			t.Fatal(w, err)
		}
		quick(t, "the publisher", func() { feed(m, src.until(5*time.Second)) })
		quick(t, "a playlist", func() {
			if _, ok := m.MediaPlaylist(nil); ok {
				t.Error("listed before encrypted")
			}
		})
		quick(t, "a segment", func() {
			if _, ok := m.File(string(m.segName(nil, 0))); ok {
				t.Error("served before encrypted")
			}
			if _, ok := m.File(string(m.partName(nil, 0, 0))); ok {
				t.Error("part served before encrypted")
			}
		})
		quick(t, "the state", func() { _ = m.status(); _ = m.Bytes(); _, _ = m.KeyEpoch() })
		select {
		case <-woke:
			t.Fatal("woken before the segment is listed")
		default:
		}
		close(gate)
		<-woke
		settle(t, m)
		p := media(t, m)
		if len(p.segs) != 2 || len(p.parts) == 0 {
			t.Fatalf("%s: %+v", slow, p)
		}
		newCipher = aes.NewCipher
	}
}

func TestMuxerEncryptionBacklog(t *testing.T) {
	// An encryption not run until a burst of segments was cut, as when the
	// scheduler is slow, or the hub hands over its GOP cache, catches up.
	var later []func()
	enc := (&testKeys{}).async(1)
	enc.Run = func(job func()) bool {
		later = append(later, job)
		return true
	}
	m := NewMuxer(Config{Encryption: enc})
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
	feed(m, (&source{video: true, audio: 1, gop: 25}).until(30*time.Second))
	if m.Ended() || len(m.cut) < 10 || len(later) != 1 {
		t.Fatal(m.Ended(), len(m.cut), len(later))
	}
	if _, ok := m.MediaPlaylist(nil); ok {
		t.Fatal("listed before encrypted")
	}
	later[0]()
	if p := media(t, m); len(m.cut) != 0 || len(p.segs) != DefaultPlaylistSize || m.Ended() {
		t.Fatalf("%d waiting, %+v", len(m.cut), p)
	}

	// One that falls behind by more than half of MaxBytes ends the
	// stream: what it held goes.
	var jobs []func()
	enc = (&testKeys{}).async(1)
	enc.Run = func(job func()) bool {
		jobs = append(jobs, job)
		return true
	}
	m = NewMuxer(Config{Encryption: enc, MaxBytes: 32 << 10})
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
	feed(m, (&source{video: true, audio: 1, gop: 50}).until(30*time.Second))
	if !m.Ended() || len(m.cut) != 0 || m.Bytes() > 32<<10 || len(jobs) != 1 {
		t.Fatal(m.Ended(), len(m.cut), m.Bytes(), len(jobs))
	}
	// The encryption, run late, finds nothing to do.
	jobs[0]()
	if m.running || len(m.segs) != 0 {
		t.Fatal("ran after the end")
	}
	if !m.ready(3, -1) {
		t.Fatal("not ready after the end")
	}
}

// benchStream returns a second of a 4.8 Mb/s H.264 and AAC stream, with a
// keyframe at its start, and its bytes.
func benchStream() ([]*av.Packet, int) {
	video := make([]byte, 20000)
	key := make([]byte, 100000)
	audio := make([]byte, 400)
	var pkts []*av.Packet
	for f := range 25 {
		d := time.Duration(f) * 40 * time.Millisecond
		data := video
		if f == 0 {
			data = key
		}
		pkts = append(pkts, &av.Packet{Track: 0, DTS: d, PTS: d, Keyframe: f == 0, Data: data})
	}
	for s := range 47 {
		d := av.FromTicks(int64(s)*1024, 48000)
		pkts = append(pkts, &av.Packet{Track: 1, DTS: d, PTS: d, Keyframe: true, Data: audio})
	}
	slices.SortStableFunc(pkts, func(a, b *av.Packet) int { return int(a.DTS - b.DTS) })
	size := 0
	for _, p := range pkts {
		size += len(p.Data)
	}
	return pkts, size
}

// feedSecond feeds m second i of pkts, a keyframe every 2 s.
func feedSecond(m *Muxer, pkts []*av.Packet, i int) {
	base := time.Duration(i) * time.Second
	for _, p := range pkts {
		q := *p
		q.DTS += base
		q.PTS += base
		q.Keyframe = q.Track == 1 || p.Keyframe && i%2 == 0
		m.OnPacket(&q)
	}
}

// benchCases are the Muxers benchmarked: clear, encrypted inline on the
// publisher's goroutine (all the CPU encryption takes) or on goroutines of
// their own, as by default (what the publisher spends), without and with
// 500 ms parts.
var benchCases = []struct {
	name string
	part time.Duration
	enc  int // 1: inline, 2: on goroutines of their own
}{{"clear", 0, 0}, {"aes128", 0, 1}, {"aes128-async", 0, 2}, {"clear-ll", 500 * time.Millisecond, 0}, {"aes128-ll", 500 * time.Millisecond, 1}, {"aes128-ll-async", 500 * time.Millisecond, 2}}

func benchMuxer(b *testing.B, part time.Duration, enc int) *Muxer {
	config := Config{PartDuration: part}
	switch enc {
	case 1:
		config.Encryption = (&testKeys{}).encryption(10)
	case 2:
		config.Encryption = (&testKeys{}).async(10)
	}
	m := NewMuxer(config)
	m.OnTracks([]*av.Track{h264Track(b), aacTrack(b)})
	return m
}

// BenchmarkMuxerEncryption muxes a 4.8 Mb/s H.264 and AAC stream, 2 s
// segments: an op is a second of media, and what the publisher's
// goroutine spends on it. The encryption on goroutines of their own is
// waited for, untimed, after each second, as a stream in real time would
// have it done.
func BenchmarkMuxerEncryption(b *testing.B) {
	pkts, size := benchStream()
	for _, c := range benchCases {
		b.Run(c.name, func(b *testing.B) {
			m := benchMuxer(b, c.part, c.enc)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				feedSecond(m, pkts, i)
				if c.enc == 2 {
					b.StopTimer()
					settle(b, m)
					b.StartTimer()
				}
			}
			if m.Ended() {
				b.Fatal("ended")
			}
		})
	}
}

// BenchmarkMuxerPlaylistWait muxes as BenchmarkMuxerEncryption does while
// another goroutine renders the media playlist over and over, and reports
// the renders over 300 µs a second of media, and the 99.99th percentile:
// how often, and how long, a request waits on the Muxer's lock (or the GC).
func BenchmarkMuxerPlaylistWait(b *testing.B) {
	pkts, _ := benchStream()
	for _, c := range benchCases {
		b.Run(c.name, func(b *testing.B) {
			m := benchMuxer(b, c.part, c.enc)
			feedSecond(m, pkts, 0)
			feedSecond(m, pkts, 1)
			feedSecond(m, pkts, 2)
			settle(b, m)
			stop := make(chan struct{})
			waits := make(chan []time.Duration)
			go func() {
				var d []time.Duration
				buf := make([]byte, 0, 4096)
				for {
					select {
					case <-stop:
						waits <- d
						return
					default:
					}
					t := time.Now()
					buf, _ = m.MediaPlaylist(buf[:0])
					d = append(d, time.Since(t))
				}
			}()
			for i := 3; b.Loop(); i++ {
				feedSecond(m, pkts, i)
				if c.enc == 2 {
					settle(b, m)
				}
			}
			close(stop)
			d := <-waits
			slow := 0
			for _, w := range d {
				if w > 300*time.Microsecond {
					slow++
				}
			}
			slices.Sort(d)
			b.ReportMetric(float64(slow)/float64(b.N), "slow/op")
			b.ReportMetric(float64(d[len(d)*9999/10000].Nanoseconds())/1000, "p9999-µs")
		})
	}
}
