package hub

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
)

var (
	videoTrack = &av.Track{Codec: av.CodecH264, Width: 640, Height: 360}
	audioTrack = &av.Track{Codec: av.CodecAAC, SampleRate: 48000, Channels: 2}
	avTracks   = []*av.Track{videoTrack, audioTrack}
	audioOnly  = []*av.Track{audioTrack}
)

// recorder is a Sink that records what it hears.
type recorder struct {
	mu      sync.Mutex
	tracks  int
	pkts    []*av.Packet
	closed  []error
	onPkt   func(*av.Packet)
	onTrack func()
}

func (r *recorder) OnTracks(tracks []*av.Track) {
	r.mu.Lock()
	r.tracks++
	f := r.onTrack
	r.mu.Unlock()
	if f != nil {
		f()
	}
}

func (r *recorder) OnPacket(pkt *av.Packet) {
	r.mu.Lock()
	r.pkts = append(r.pkts, pkt)
	f := r.onPkt
	r.mu.Unlock()
	if f != nil {
		f(pkt)
	}
}

func (r *recorder) OnClose(err error) {
	r.mu.Lock()
	r.closed = append(r.closed, err)
	r.mu.Unlock()
}

func (r *recorder) state() (tracks int, dts []time.Duration, closed []error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pkts {
		dts = append(dts, p.DTS)
	}
	return r.tracks, dts, slices.Clone(r.closed)
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func vpkt(n int, key bool) *av.Packet {
	return &av.Packet{Track: 0, DTS: ms(n), PTS: ms(n), Keyframe: key, Data: make([]byte, 100)}
}

func apkt(n int) *av.Packet {
	return &av.Packet{Track: 1, DTS: ms(n), PTS: ms(n), Keyframe: true, Data: make([]byte, 10)}
}

func dtsOf(ns ...int) []time.Duration {
	var d []time.Duration
	for _, n := range ns {
		d = append(d, ms(n))
	}
	return d
}

func TestPublishSubscribe(t *testing.T) {
	var unpublished []string
	h := New(Config{OnUnpublish: func(path string) { unpublished = append(unpublished, path) }})
	if _, err := h.Subscribe("live/a", &recorder{}, SubscribeOptions{}); err != ErrNotFound {
		t.Fatal(err)
	}
	p, err := h.Publish("live/a", avTracks, PublishInfo{Protocol: "rtmp", Query: "k=1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Publish("live/a", avTracks, PublishInfo{}); err != ErrStreamBusy {
		t.Fatal(err)
	}
	// Before the first keyframe nothing is cached.
	p.WritePacket(vpkt(0, false))
	p.WritePacket(apkt(1))
	p.WritePacket(vpkt(10, true))
	p.WritePacket(apkt(11))
	p.WritePacket(vpkt(20, false))

	early := &recorder{}
	if _, err := h.Subscribe("live/a", early, SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	late := &recorder{}
	if _, err := h.Subscribe("live/a", late, SubscribeOptions{NoGOPCache: true}); err != nil {
		t.Fatal(err)
	}
	p.WritePacket(apkt(21))
	p.WritePacket(vpkt(30, false))
	p.WritePacket(vpkt(40, true))
	p.WritePacket(&av.Packet{Track: 2})  // no such track
	p.WritePacket(&av.Packet{Track: -1}) // no such track
	p.WritePacket(nil)

	tr, dts, _ := early.state()
	if tr != 1 || !slices.Equal(dts, dtsOf(10, 11, 20, 21, 30, 40)) {
		t.Fatalf("early: %d %v", tr, dts)
	}
	// Without the cache: audio at once, video from the next keyframe.
	tr, dts, _ = late.state()
	if tr != 1 || !slices.Equal(dts, dtsOf(21, 40)) {
		t.Fatalf("late: %d %v", tr, dts)
	}

	infos := h.Streams()
	if len(infos) != 1 || infos[0].Path != "live/a" || !infos[0].Publishing || infos[0].Subscribers != 2 ||
		infos[0].PacketsIn != 8 || infos[0].BytesIn != 5*100+3*10 || infos[0].Publisher.Query != "k=1" ||
		infos[0].Since.IsZero() || len(infos[0].Tracks) != 2 {
		t.Fatalf("%+v", infos)
	}

	p.Close()
	p.Close()
	p.WritePacket(vpkt(50, true))
	for _, r := range []*recorder{early, late} {
		if _, _, closed := r.state(); !slices.Equal(closed, []error{ErrUnpublished}) {
			t.Fatalf("closed %v", closed)
		}
	}
	if len(h.Streams()) != 0 || !slices.Equal(unpublished, []string{"live/a"}) {
		t.Fatalf("streams %v unpublished %v", h.Streams(), unpublished)
	}
	// The path is free again.
	if p, err = h.Publish("live/a", avTracks, PublishInfo{}); err != nil {
		t.Fatal(err)
	}
	p.Close()
}

func TestHooksAndLimits(t *testing.T) {
	deny := errors.New("denied")
	h := New(Config{
		MaxStreams:              2,
		MaxSubscribersPerStream: 1,
		OnPublish: func(path string, info PublishInfo) error {
			if info.Query != "ok" {
				return deny
			}
			return nil
		},
		OnSubscribe: func(path string, info SubscribeInfo) error {
			if info.Query != "ok" {
				return deny
			}
			return nil
		},
	})
	ok := PublishInfo{Query: "ok"}
	if _, err := h.Publish("a", avTracks, PublishInfo{}); err != deny {
		t.Fatal(err)
	}
	a, err := h.Publish("a", avTracks, ok)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Subscribe("a", &recorder{}, SubscribeOptions{}); err != deny {
		t.Fatal(err)
	}
	sub := SubscribeOptions{Info: SubscribeInfo{Query: "ok"}}
	s1, err := h.Subscribe("a", &recorder{}, sub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Subscribe("a", &recorder{}, sub); err != ErrLimit {
		t.Fatal(err)
	}
	// A closed subscription frees its place.
	s1.Close()
	s1.Close()
	if _, err := h.Subscribe("a", &recorder{}, sub); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Publish("b", avTracks, ok); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Publish("c", avTracks, ok); err != ErrLimit {
		t.Fatal(err)
	}
	sub.Wait = time.Hour
	if _, err := h.Subscribe("c", &recorder{}, sub); err != ErrLimit {
		t.Fatal(err)
	}
	h.Close()
	h.Close()
	if _, err := h.Publish("a", avTracks, ok); err != ErrClosed {
		t.Fatal(err)
	}
	if _, err := h.Subscribe("a", &recorder{}, SubscribeOptions{Info: SubscribeInfo{Query: "ok"}}); err != ErrClosed {
		t.Fatal(err)
	}
	a.WritePacket(vpkt(0, true))
	a.SetTracks(audioOnly)
	a.Close()
}

func TestHubClose(t *testing.T) {
	var unpublished []string
	h := New(Config{OnUnpublish: func(path string) { unpublished = append(unpublished, path) }})
	p, _ := h.Publish("b", avTracks, PublishInfo{})
	r := &recorder{}
	sub, _ := h.Subscribe("b", r, SubscribeOptions{})
	w := &recorder{}
	if _, err := h.Subscribe("waiting", w, SubscribeOptions{Wait: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Publish("a", avTracks, PublishInfo{}); err != nil {
		t.Fatal(err)
	}
	h.Close()
	for _, r := range []*recorder{r, w} {
		if _, _, closed := r.state(); !slices.Equal(closed, []error{ErrClosed}) {
			t.Fatal(closed)
		}
	}
	p.Close()
	sub.Close()
	if !slices.Equal(unpublished, []string{"a", "b"}) || len(h.Streams()) != 0 {
		t.Fatal(unpublished)
	}
}

func TestGOPCache(t *testing.T) {
	h := New(Config{GOPCacheBytes: 3 * (100 + packetOverhead)})
	p, _ := h.Publish("v", avTracks, PublishInfo{})
	p.WritePacket(vpkt(0, true))
	p.WritePacket(vpkt(10, false))
	p.WritePacket(vpkt(20, true)) // resets the cache
	p.WritePacket(apkt(21))
	sub := func() []time.Duration {
		r := &recorder{}
		s, err := h.Subscribe("v", r, SubscribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, dts, _ := r.state()
		s.Close()
		return dts
	}
	if got := sub(); !slices.Equal(got, dtsOf(20, 21)) {
		t.Fatal(got)
	}
	// Over the bound: the cache is dropped until the next keyframe, and a
	// subscriber meanwhile waits for it.
	p.WritePacket(vpkt(30, false))
	p.WritePacket(vpkt(40, false))
	if got := sub(); got != nil {
		t.Fatal(got)
	}
	r := &recorder{}
	h.Subscribe("v", r, SubscribeOptions{})
	p.WritePacket(vpkt(50, false))
	p.WritePacket(apkt(51))
	p.WritePacket(vpkt(60, true))
	if _, dts, _ := r.state(); !slices.Equal(dts, dtsOf(51, 60)) {
		t.Fatal(dts)
	}
	if got := sub(); !slices.Equal(got, dtsOf(60)) {
		t.Fatal(got)
	}
	// SetTracks drops the cache and holds video back to a keyframe.
	p.SetTracks(avTracks)
	if tr, _, _ := r.state(); tr != 2 {
		t.Fatal(tr)
	}
	p.WritePacket(vpkt(70, false))
	p.WritePacket(vpkt(80, true))
	if _, dts, _ := r.state(); !slices.Equal(dts, dtsOf(51, 60, 80)) {
		t.Fatal(dts)
	}
	if got := sub(); !slices.Equal(got, dtsOf(80)) {
		t.Fatal(got)
	}
	p.Close()

	// No cache at all.
	h = New(Config{GOPCacheBytes: -1})
	p, _ = h.Publish("v", avTracks, PublishInfo{})
	p.WritePacket(vpkt(0, true))
	if got := sub(); got != nil {
		t.Fatal(got)
	}
}

func TestAudioOnlyCache(t *testing.T) {
	h := New(Config{})
	p, _ := h.Publish("a", audioOnly, PublishInfo{})
	for i := range 200 {
		p.WritePacket(&av.Packet{DTS: ms(20 * i), Keyframe: true, Data: []byte{1}})
	}
	r := &recorder{}
	h.Subscribe("a", r, SubscribeOptions{})
	_, dts, _ := r.state()
	// The last second: 51 packets 20 ms apart, the first exactly 1 s old.
	if len(dts) != 51 || dts[0] != ms(20*149) || dts[50] != ms(20*199) {
		t.Fatal(len(dts), dts[0], dts[len(dts)-1])
	}
	// Bounded by bytes too.
	h = New(Config{GOPCacheBytes: 10 * (packetOverhead + 1)})
	p, _ = h.Publish("a", audioOnly, PublishInfo{})
	for i := range 200 {
		p.WritePacket(&av.Packet{DTS: ms(i), Keyframe: true, Data: []byte{1}})
	}
	r = &recorder{}
	h.Subscribe("a", r, SubscribeOptions{})
	if _, dts, _ := r.state(); len(dts) != 10 || dts[9] != ms(199) {
		t.Fatal(dts)
	}
}

func TestWait(t *testing.T) {
	h := New(Config{})
	r := &recorder{}
	s, err := h.Subscribe("w", r, SubscribeOptions{Wait: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if infos := h.Streams(); len(infos) != 1 || infos[0].Publishing || infos[0].Subscribers != 1 {
		t.Fatal(infos)
	}
	p, _ := h.Publish("w", avTracks, PublishInfo{})
	p.WritePacket(vpkt(0, false)) // held back: no keyframe yet
	p.WritePacket(vpkt(10, true))
	p.Close()
	// Survives the unpublish; the next publisher starts it over.
	p, _ = h.Publish("w", avTracks, PublishInfo{})
	p.WritePacket(vpkt(0, true))
	tr, dts, closed := r.state()
	if tr != 2 || !slices.Equal(dts, dtsOf(10, 0)) || closed != nil {
		t.Fatal(tr, dts, closed)
	}
	p.Close()
	s.Close()
	if len(h.Streams()) != 0 {
		t.Fatal(h.Streams())
	}

	// Waits that run out.
	for _, tc := range []struct {
		publish bool
		want    error
	}{{false, ErrNotFound}, {true, ErrUnpublished}} {
		r := &recorder{}
		done := make(chan struct{})
		h.Subscribe("x", closeNotifier{r, done}, SubscribeOptions{Wait: 20 * time.Millisecond})
		if tc.publish {
			p, _ := h.Publish("x", avTracks, PublishInfo{})
			p.Close()
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("wait did not expire")
		}
		if _, _, closed := r.state(); !slices.Equal(closed, []error{tc.want}) {
			t.Fatal(closed)
		}
		waitFor(t, func() bool { return len(h.Streams()) == 0 })
	}
}

type closeNotifier struct {
	*recorder
	done chan struct{}
}

func (c closeNotifier) OnClose(err error) {
	c.recorder.OnClose(err)
	close(c.done)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for range 500 {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestStaleTimer(t *testing.T) {
	// A timer from an earlier wait that fires late does not end a newer
	// one.
	h := New(Config{})
	r := &recorder{}
	s, _ := h.Subscribe("t", r, SubscribeOptions{Wait: time.Hour})
	gen := s.timerGen
	p, _ := h.Publish("t", avTracks, PublishInfo{})
	s.expire(gen) // a publisher is there
	p.Close()     // arms a new wait
	s.expire(gen) // stale
	if _, _, closed := r.state(); closed != nil {
		t.Fatal(closed)
	}
	s.expire(s.timerGen)
	if _, _, closed := r.state(); !slices.Equal(closed, []error{ErrUnpublished}) {
		t.Fatal(closed)
	}
}

func TestCloseFromCallbacks(t *testing.T) {
	h := New(Config{})
	p, _ := h.Publish("c", avTracks, PublishInfo{})
	p.WritePacket(vpkt(0, true))

	// Close from the sink's own OnPacket: no deadlock, nothing after.
	var s *Subscription
	r := &recorder{}
	r.onPkt = func(pkt *av.Packet) {
		if pkt.DTS == ms(20) {
			s.Close()
		}
	}
	s, _ = h.Subscribe("c", r, SubscribeOptions{})
	// Close from OnTracks of SetTracks.
	var s2 *Subscription
	r2 := &recorder{}
	s2, _ = h.Subscribe("c", r2, SubscribeOptions{})
	r2.onTrack = func() { s2.Close() }
	// A sink that closes another subscription during fan-out.
	r3 := &recorder{}
	s3, _ := h.Subscribe("c", r3, SubscribeOptions{})
	r4 := &recorder{onPkt: func(pkt *av.Packet) {
		if pkt.DTS == ms(10) {
			s3.Close()
		}
	}}
	h.Subscribe("c", r4, SubscribeOptions{})

	p.WritePacket(vpkt(10, false))
	p.WritePacket(vpkt(20, false))
	p.WritePacket(vpkt(30, false))
	p.SetTracks(avTracks)
	p.WritePacket(vpkt(40, true))
	if _, dts, _ := r.state(); !slices.Equal(dts, dtsOf(0, 10, 20)) {
		t.Fatal(dts)
	}
	if tr, dts, _ := r2.state(); tr != 2 || !slices.Equal(dts, dtsOf(0, 10, 20, 30)) {
		t.Fatal(tr, dts)
	}
	// s3 is before r4 in fan-out order: it got packet 10, then was closed.
	if _, dts, _ := r3.state(); !slices.Equal(dts, dtsOf(0, 10)) {
		t.Fatal(dts)
	}
	if infos := h.Streams(); infos[0].Subscribers != 1 || len(p.st.subs) != 1 {
		t.Fatal(infos, len(p.st.subs))
	}
}

func TestCloseDuringFanOut(t *testing.T) {
	// Close from another goroutine while the publisher is inside the
	// sink: Close returns at once, and the sink hears nothing more.
	h := New(Config{})
	p, _ := h.Publish("f", avTracks, PublishInfo{})
	inside, release := make(chan struct{}), make(chan struct{})
	r := &recorder{}
	r.onPkt = func(pkt *av.Packet) {
		if pkt.DTS == 0 {
			close(inside)
			<-release
		}
	}
	s, _ := h.Subscribe("f", r, SubscribeOptions{})
	done := make(chan struct{})
	go func() {
		p.WritePacket(vpkt(0, true))
		p.WritePacket(vpkt(10, false))
		close(done)
	}()
	<-inside
	s.Close() // the publisher holds the stream: Close must not wait for it
	close(release)
	<-done
	if _, dts, _ := r.state(); !slices.Equal(dts, dtsOf(0)) {
		t.Fatal(dts)
	}
	if len(p.st.subs) != 0 {
		t.Fatal("not unlinked")
	}
	p.Close()
	if len(h.Streams()) != 0 {
		t.Fatal(h.Streams())
	}
}

func TestReapDeferred(t *testing.T) {
	// When the hub's mutex is busy, an emptied stream leaves the registry
	// from a goroutine.
	h := New(Config{})
	p, _ := h.Publish("r", avTracks, PublishInfo{})
	h.mu.Lock()
	p.Close()
	if len(h.streams) != 1 {
		t.Fatal("reaped under a held lock")
	}
	h.mu.Unlock()
	waitFor(t, func() bool { return len(h.Streams()) == 0 })
	// A stream that gained a subscriber meanwhile stays.
	p, _ = h.Publish("r", avTracks, PublishInfo{})
	h.mu.Lock()
	p.Close()
	st := p.st
	st.mu.Lock()
	st.subs = append(st.subs, &Subscription{st: st, sink: &recorder{}})
	st.mu.Unlock()
	h.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	if infos := h.Streams(); len(infos) != 1 {
		t.Fatal(infos)
	}
}

func TestDropper(t *testing.T) {
	var backlog int64
	r := &recorder{}
	d := NewDropper(r, func() int64 { return backlog }, 100)
	d.OnTracks(avTracks)
	d.OnPacket(vpkt(0, true))
	backlog = 200
	d.OnPacket(vpkt(10, false))
	d.OnPacket(apkt(11))
	backlog = 0
	d.OnPacket(apkt(12))
	d.OnPacket(vpkt(20, false)) // waits for a keyframe
	d.OnPacket(vpkt(30, true))
	d.OnPacket(vpkt(40, false))
	d.OnPacket(&av.Packet{Track: 5, DTS: ms(41)}) // unknown track: not video
	d.OnClose(ErrClosed)
	tr, dts, closed := r.state()
	if tr != 1 || !slices.Equal(dts, dtsOf(0, 12, 30, 40, 41)) || !slices.Equal(closed, []error{ErrClosed}) {
		t.Fatal(tr, dts, closed)
	}
}

// TestStress runs publishers that republish and subscribers that come,
// go and close themselves from their callbacks, on a few paths at once,
// under -race: nothing deadlocks, and every subscription sees its
// publisher's packets in order.
func TestStress(t *testing.T) {
	h := New(Config{GOPCacheBytes: 4096})
	const paths, rounds = 4, 30
	var wg sync.WaitGroup
	for i := range paths {
		path := fmt.Sprint("s/", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range rounds {
				p, err := h.Publish(path, avTracks, PublishInfo{})
				if err != nil {
					continue // another publisher of the path, or a busy one
				}
				for n := range 50 {
					p.WritePacket(&av.Packet{Track: n % 2, DTS: ms(n), Keyframe: n%10 == 0 || n%2 == 1, Data: make([]byte, 64)})
				}
				if round%5 == 0 {
					p.SetTracks(avTracks)
				}
				p.Close()
			}
		}()
		for j := range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for round := range rounds {
					r := &orderSink{t: t}
					var s *Subscription
					var mu sync.Mutex
					r.onPacket = func(n int) {
						if j%3 == 0 && n == 25 {
							mu.Lock()
							if s != nil {
								s.Close()
							}
							mu.Unlock()
						}
					}
					opts := SubscribeOptions{NoGOPCache: j%2 == 0}
					if round%2 == 0 {
						opts.Wait = time.Millisecond
					}
					sub, err := h.Subscribe(path, r, opts)
					if err != nil {
						continue
					}
					mu.Lock()
					s = sub
					mu.Unlock()
					time.Sleep(time.Duration(round%3) * 100 * time.Microsecond)
					sub.Close()
				}
			}()
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 200 {
			h.Streams()
		}
	}()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("deadlock")
	}
	waitFor(t, func() bool { return len(h.Streams()) == 0 })
	h.Close()
}

// orderSink checks that packets between two OnTracks come in DTS order.
type orderSink struct {
	t        *testing.T
	last     time.Duration
	closed   bool
	onPacket func(n int)
}

func (o *orderSink) OnTracks([]*av.Track) { o.last = -1 }

func (o *orderSink) OnPacket(pkt *av.Packet) {
	if o.closed {
		o.t.Error("packet after OnClose")
	}
	if pkt.DTS < o.last {
		o.t.Errorf("DTS %v after %v", pkt.DTS, o.last)
	}
	o.last = pkt.DTS
	if o.onPacket != nil {
		o.onPacket(int(pkt.DTS / time.Millisecond))
	}
}

func (o *orderSink) OnClose(error) { o.closed = true }
