package hub

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestReserve(t *testing.T) {
	var published, unpublished []string
	var hookMu sync.Mutex
	h := New(Config{
		OnPublish: func(path string, info PublishInfo) error {
			hookMu.Lock()
			defer hookMu.Unlock()
			published = append(published, path+"/"+info.Protocol)
			return nil
		},
		OnUnpublish: func(path string) {
			hookMu.Lock()
			defer hookMu.Unlock()
			unpublished = append(unpublished, path)
		},
	})
	waiting := &recorder{}
	ws, err := h.Subscribe("r", waiting, SubscribeOptions{Wait: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	p, err := h.Reserve("r", PublishInfo{Protocol: "srt"})
	if err != nil {
		t.Fatal(err)
	}
	// OnPublish ran at reserve time.
	if !slices.Equal(published, []string{"r/srt"}) {
		t.Fatal(published)
	}
	// The path is claimed: a second publisher, reserving or not, is busy.
	if _, err := h.Publish("r", avTracks, PublishInfo{}); err != ErrStreamBusy {
		t.Fatal(err)
	}
	if _, err := h.Reserve("r", PublishInfo{}); err != ErrStreamBusy {
		t.Fatal(err)
	}
	// Not live yet: a player without Wait finds nothing.
	if _, err := h.Subscribe("r", &recorder{}, SubscribeOptions{}); err != ErrNotFound {
		t.Fatal(err)
	}
	infos := h.Streams()
	if len(infos) != 1 || infos[0].Publishing || !infos[0].Reserved || infos[0].Publisher.Protocol != "srt" || infos[0].Subscribers != 1 {
		t.Fatalf("%+v", infos)
	}
	// Packets before the tracks are dropped; the waiting subscriber has
	// heard nothing.
	p.WritePacket(vpkt(0, true))
	if tr, dts, closed := waiting.state(); tr != 0 || dts != nil || closed != nil {
		t.Fatal(tr, dts, closed)
	}
	// A late subscriber with Wait waits too.
	late := &recorder{}
	if _, err := h.Subscribe("r", late, SubscribeOptions{Wait: time.Hour}); err != nil {
		t.Fatal(err)
	}

	// The first SetTracks makes it live.
	p.SetTracks(avTracks)
	p.WritePacket(vpkt(10, true))
	for _, r := range []*recorder{waiting, late} {
		if tr, dts, closed := r.state(); tr != 1 || !slices.Equal(dts, dtsOf(10)) || closed != nil {
			t.Fatal(tr, dts, closed)
		}
	}
	infos = h.Streams()
	if len(infos) != 1 || !infos[0].Publishing || infos[0].Reserved || len(infos[0].Tracks) != 2 {
		t.Fatalf("%+v", infos)
	}
	now := &recorder{}
	if _, err := h.Subscribe("r", now, SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	if tr, dts, _ := now.state(); tr != 1 || !slices.Equal(dts, dtsOf(10)) {
		t.Fatal(tr, dts)
	}
	// Later SetTracks are codec changes, as before.
	p.SetTracks(audioOnly)
	if tr, _, _ := waiting.state(); tr != 2 {
		t.Fatal(tr)
	}
	p.Close()
	if _, _, closed := now.state(); !slices.Equal(closed, []error{ErrUnpublished}) {
		t.Fatal(closed)
	}
	hookMu.Lock()
	if !slices.Equal(unpublished, []string{"r"}) {
		t.Fatal(unpublished)
	}
	hookMu.Unlock()
	ws.Close()
	// late still waits for the next publisher.
	if infos := h.Streams(); len(infos) != 1 || infos[0].Subscribers != 1 {
		t.Fatalf("%+v", infos)
	}
}

func TestReserveRefused(t *testing.T) {
	deny := errors.New("deny")
	h := New(Config{OnPublish: func(string, PublishInfo) error { return deny }})
	if _, err := h.Reserve("a", PublishInfo{}); err != deny {
		t.Fatal(err)
	}
	h = New(Config{MaxStreams: 1})
	if _, err := h.Reserve("a", PublishInfo{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Reserve("b", PublishInfo{}); err != ErrLimit {
		t.Fatal(err)
	}
	h.Close()
	if _, err := h.Reserve("c", PublishInfo{}); err != ErrClosed {
		t.Fatal(err)
	}
}

func TestReserveClose(t *testing.T) {
	// Released before going live: the path is free again, OnUnpublish is
	// heard, and a waiting subscriber keeps waiting for the next one with
	// the timer it had.
	var mu sync.Mutex
	unpublished := 0
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return unpublished
	}
	h := New(Config{OnUnpublish: func(string) {
		mu.Lock()
		unpublished++
		mu.Unlock()
	}})
	r := &recorder{}
	s, _ := h.Subscribe("c", r, SubscribeOptions{Wait: time.Hour})
	gen := s.timerGen
	p, _ := h.Reserve("c", PublishInfo{})
	p.Close()
	p.Close()
	p.SetTracks(avTracks) // after Close: ignored
	if count() != 1 || s.timerGen != gen {
		t.Fatal(count(), s.timerGen, gen)
	}
	if tr, _, closed := r.state(); tr != 0 || closed != nil {
		t.Fatal(tr, closed)
	}
	p2, err := h.Publish("c", avTracks, PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := r.state(); tr != 1 {
		t.Fatal(tr)
	}
	p2.Close()

	// A reservation that never goes live lets the wait run out.
	r2 := &recorder{}
	done := make(chan struct{})
	h.Subscribe("d", closeNotifier{r2, done}, SubscribeOptions{Wait: 20 * time.Millisecond})
	p3, _ := h.Reserve("d", PublishInfo{})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not expire")
	}
	if _, _, closed := r2.state(); !slices.Equal(closed, []error{ErrNotFound}) {
		t.Fatal(closed)
	}
	// The reservation outlives its subscribers until closed, and the hub's
	// Close unpublishes it.
	if infos := h.Streams(); len(infos) != 2 || !infos[1].Reserved {
		t.Fatalf("%+v", infos)
	}
	h.Close()
	if count() != 3 {
		t.Fatal(count())
	}
	p3.SetTracks(avTracks)
	p3.Close()
}

func TestReserveRace(t *testing.T) {
	// Concurrent reserves of one path: exactly one wins each round.
	h := New(Config{})
	sub := &recorder{}
	h.Subscribe("race", sub, SubscribeOptions{Wait: time.Hour})
	for range 50 {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var won []*Publisher
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p, err := h.Reserve("race", PublishInfo{})
				if err != nil {
					return
				}
				mu.Lock()
				won = append(won, p)
				mu.Unlock()
				p.SetTracks(avTracks)
				p.WritePacket(vpkt(0, true))
			}()
		}
		wg.Wait()
		if len(won) != 1 {
			t.Fatal(len(won))
		}
		won[0].Close()
	}
	if tr, dts, _ := sub.state(); tr != 50 || len(dts) != 50 {
		t.Fatal(tr, len(dts))
	}
}
