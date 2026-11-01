package hub

import (
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestStartWait(t *testing.T) {
	h := New(Config{})
	// StartWait alone: the subscription waits for its first publisher,
	// and ends with it, as one without Wait does.
	r := &recorder{}
	if _, err := h.Subscribe("s", r, SubscribeOptions{StartWait: time.Hour}); err != nil {
		t.Fatal(err)
	}
	p, _ := h.Publish("s", avTracks, PublishInfo{})
	p.WritePacket(vpkt(0, true))
	p.Close()
	if tr, dts, closed := r.state(); tr != 1 || !slices.Equal(dts, dtsOf(0)) || !slices.Equal(closed, []error{ErrUnpublished}) {
		t.Fatal(tr, dts, closed)
	}
	waitFor(t, func() bool { return len(h.Streams()) == 0 })

	// StartWait running out ends the subscription with ErrNotFound, where
	// the longer Wait would have it wait on.
	done := make(chan struct{})
	r = &recorder{}
	if _, err := h.Subscribe("s", closeNotifier{r, done}, SubscribeOptions{StartWait: 20 * time.Millisecond, Wait: time.Hour}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("start wait did not expire")
	}
	if _, _, closed := r.state(); !slices.Equal(closed, []error{ErrNotFound}) {
		t.Fatal(closed)
	}
	waitFor(t, func() bool { return len(h.Streams()) == 0 })

	// A long StartWait, then Wait for the next publisher.
	done = make(chan struct{})
	r = &recorder{}
	if _, err := h.Subscribe("s", closeNotifier{r, done}, SubscribeOptions{StartWait: time.Hour, Wait: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	p, _ = h.Publish("s", avTracks, PublishInfo{})
	p.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not expire")
	}
	if tr, _, closed := r.state(); tr != 1 || !slices.Equal(closed, []error{ErrUnpublished}) {
		t.Fatal(tr, closed)
	}

	// A reservation released leaves a subscription that has not started
	// waiting, even without Wait.
	r = &recorder{}
	s, err := h.Subscribe("q", r, SubscribeOptions{StartWait: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	rp, _ := h.Reserve("q", PublishInfo{})
	rp.Close()
	if _, _, closed := r.state(); closed != nil {
		t.Fatal(closed)
	}
	p, _ = h.Publish("q", avTracks, PublishInfo{})
	if tr, _, _ := r.state(); tr != 1 {
		t.Fatal(tr)
	}
	p.Close()
	s.Close()
}

func TestStream(t *testing.T) {
	h := New(Config{})
	if _, ok := h.Stream("a"); ok {
		t.Fatal("stream of an empty hub")
	}
	for i := range 1000 {
		if _, err := h.Publish("p/"+strconv.Itoa(i), avTracks, PublishInfo{Protocol: "rtmp"}); err != nil {
			t.Fatal(err)
		}
	}
	info, ok := h.Stream("p/500")
	if !ok || info.Path != "p/500" || !info.Publishing || info.Publisher.Protocol != "rtmp" || len(info.Tracks) != 2 {
		t.Fatalf("%v %+v", ok, info)
	}
	if _, ok := h.Stream("p/1000"); ok {
		t.Fatal("found a stream never published")
	}
	s, _ := h.Subscribe("w", &recorder{}, SubscribeOptions{Wait: time.Hour})
	if info, ok := h.Stream("w"); !ok || info.Publishing || info.Subscribers != 1 {
		t.Fatalf("%v %+v", ok, info)
	}
	s.Close()
	if _, ok := h.Stream("w"); ok {
		t.Fatal("stream left after its last subscription")
	}
	// A stream out of the registry, though still held, is not found.
	st := &stream{hub: h, path: "gone", removed: true}
	if _, ok := st.info(); ok {
		t.Fatal("removed stream")
	}
}
