package reqbody

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSink records what a Pusher hands it. room says whether it has room;
// when it has none, the next Room records that a Kick is owed.
type fakeSink struct {
	mu      sync.Mutex
	room    bool
	owed    bool
	got     bytes.Buffer
	ends    int
	failed  error
	sendErr error
	// onSend, if set, runs inside Send.
	onSend func()
}

func (s *fakeSink) Room() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.room {
		s.owed = true
	}
	return s.room
}

func (s *fakeSink) Send(p []byte) error {
	s.mu.Lock()
	onSend := s.onSend
	err := s.sendErr
	if err == nil {
		s.got.Write(p)
	}
	s.mu.Unlock()
	if onSend != nil {
		onSend()
	}
	return err
}

func (s *fakeSink) End() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ends++
	return nil
}

func (s *fakeSink) Fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = err
}

func (s *fakeSink) state() (string, int, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.got.String(), s.ends, s.failed, s.owed
}

// open gives the sink room again and kicks the body, as a transport whose
// room came back would.
func (s *fakeSink) open(p *Pusher) {
	s.mu.Lock()
	s.room, s.owed = true, false
	s.mu.Unlock()
	p.Kick()
}

func TestPusherSendsWhatIsWrittenThenTheEnd(t *testing.T) {
	p := NewPusher(0)
	if p.limit != DefaultPushLimit {
		t.Fatalf("limit %d", p.limit)
	}
	// Written before a transport takes it: it waits.
	_, _ = p.Write([]byte("hello "))
	sink := &fakeSink{room: true}
	if err := p.Attach(sink, -1); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Write([]byte("world"))
	if n, err := p.Write(nil); n != 0 || err != nil {
		t.Fatalf("empty write: %d %v", n, err)
	}
	if got, ends, _, _ := sink.state(); got != "hello world" || ends != 0 {
		t.Fatalf("sent %q, ends %d before Close", got, ends)
	}
	_ = p.Close()
	if got, ends, failed, _ := sink.state(); got != "hello world" || ends != 1 || failed != nil {
		t.Fatalf("sent %q, ends %d, failed %v", got, ends, failed)
	}
	if _, err := p.Write([]byte("late")); !errors.Is(err, ErrPushClosed) {
		t.Fatalf("write after Close: %v", err)
	}
	// Kicking an ended body does nothing.
	p.Kick()
	if _, ends, _, _ := sink.state(); ends != 1 {
		t.Fatalf("ends %d", ends)
	}
}

// A sink without room is owed a Kick, and gets the rest, and the end, only
// once it has room again.
func TestPusherWaitsForRoom(t *testing.T) {
	p := NewPusher(8)
	sink := &fakeSink{}
	if err := p.Attach(sink, 10); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Write([]byte("01234"))
	_, _ = p.Write([]byte("56789"))
	_ = p.Close()
	if got, ends, _, owed := sink.state(); got != "" || ends != 0 || !owed {
		t.Fatalf("sent %q, ends %d, owed %v without room", got, ends, owed)
	}
	if p.Buffered() != 10 {
		t.Fatalf("buffered %d", p.Buffered())
	}
	sink.open(p)
	if got, ends, failed, _ := sink.state(); got != "0123456789" || ends != 1 || failed != nil {
		t.Fatalf("sent %q, ends %d, failed %v", got, ends, failed)
	}
}

// The end waits for room too, behind what the transport still holds.
func TestPusherEndWaitsForRoom(t *testing.T) {
	p := NewPusher(0)
	sink := &fakeSink{room: true}
	_ = p.Attach(sink, -1)
	_, _ = p.Write([]byte("abc"))
	sink.mu.Lock()
	sink.room = false
	sink.mu.Unlock()
	_ = p.Close()
	if _, ends, _, owed := sink.state(); ends != 0 || !owed {
		t.Fatalf("ends %d, owed %v", ends, owed)
	}
	sink.open(p)
	if _, ends, _, _ := sink.state(); ends != 1 {
		t.Fatalf("ends %d", ends)
	}
}

func TestPusherOnDrain(t *testing.T) {
	p := NewPusher(8)
	sink := &fakeSink{}
	_ = p.Attach(sink, -1)
	if p.OnDrain(func() { t.Error("drained an empty body") }) {
		t.Fatal("OnDrain armed under the limit")
	}
	_, _ = p.Write(bytes.Repeat([]byte("x"), 6))
	_, _ = p.Write(bytes.Repeat([]byte("y"), 6))
	drained := 0
	if !p.OnDrain(func() { drained++ }) {
		t.Fatal("OnDrain not armed past the limit")
	}
	sink.open(p)
	if drained != 1 {
		t.Fatalf("drained %d times", drained)
	}
	// Past the limit again, the request ending runs the callback too.
	sink.mu.Lock()
	sink.room = false
	sink.mu.Unlock()
	_, _ = p.Write(bytes.Repeat([]byte("z"), 6))
	_, _ = p.Write(bytes.Repeat([]byte("z"), 6))
	if !p.OnDrain(func() { drained++ }) {
		t.Fatal("OnDrain not armed past the limit")
	}
	p.Stop()
	p.Stop()
	if drained != 2 || p.Buffered() != 0 {
		t.Fatalf("drained %d, buffered %d after Stop", drained, p.Buffered())
	}
	if p.OnDrain(func() {}) {
		t.Fatal("OnDrain armed on a stopped body")
	}
	if _, err := p.Write([]byte("x")); !errors.Is(err, ErrPushStopped) {
		t.Fatalf("write after Stop: %v", err)
	}
}

func TestPusherFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		declared int64
		write    []string
		fail     error
		want     string
	}{
		{"short", 10, []string{"abc"}, nil, "ContentLength=10 with Body length 3"},
		{"long", 2, []string{"abc"}, nil, "ContentLength=2 with Body length at least 3"},
		{"producer", -1, []string{"abc"}, errors.New("upstream gone"), "upstream gone"},
		{"unexpected", -1, nil, nil, io.ErrUnexpectedEOF.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPusher(0)
			sink := &fakeSink{room: true}
			_ = p.Attach(sink, tc.declared)
			for _, w := range tc.write {
				_, _ = p.Write([]byte(w))
			}
			switch {
			case tc.fail != nil:
				p.CloseWithError(tc.fail)
			case tc.name == "unexpected":
				p.CloseWithError(nil)
			default:
				_ = p.Close()
			}
			_, ends, failed, _ := sink.state()
			if ends != 0 || failed == nil || !strings.Contains(failed.Error(), tc.want) {
				t.Fatalf("ends %d, failed %v, want %q", ends, failed, tc.want)
			}
			if _, err := p.Write([]byte("x")); !errors.Is(err, ErrPushClosed) {
				t.Fatalf("write after failure: %v", err)
			}
		})
	}
}

// A transport that fails ends the pumping; it reports the failure itself.
func TestPusherSendError(t *testing.T) {
	p := NewPusher(0)
	sink := &fakeSink{room: true, sendErr: errors.New("broken pipe")}
	_ = p.Attach(sink, -1)
	_, _ = p.Write([]byte("abc"))
	_ = p.Close()
	if _, ends, failed, _ := sink.state(); ends != 0 || failed != nil {
		t.Fatalf("ends %d, failed %v after the transport failed", ends, failed)
	}
}

func TestPusherAttachOnce(t *testing.T) {
	p := NewPusher(0)
	first := &fakeSink{room: true}
	_ = p.Attach(first, -1)
	// Nothing has gone yet, so a second attempt may take it over.
	second := &fakeSink{room: true}
	if err := p.Attach(second, -1); err != nil {
		t.Fatal(err)
	}
	_, _ = p.Write([]byte("abc"))
	if err := p.Attach(&fakeSink{}, -1); !errors.Is(err, ErrNotReplayable) {
		t.Fatalf("attach after sending: %v", err)
	}
	if got, _, _, _ := second.state(); got != "abc" {
		t.Fatalf("second sink got %q", got)
	}
}

// What is written and what makes room while another goroutine pumps is left
// to that goroutine, which looks again before it lets go.
func TestPusherKickWhilePumping(t *testing.T) {
	p := NewPusher(0)
	sink := &fakeSink{room: true}
	var once sync.Once
	sink.onSend = func() {
		once.Do(func() {
			// Inside Send: this Write's Kick finds the body pumping.
			_, _ = p.Write([]byte("-more"))
		})
	}
	_ = p.Attach(sink, -1)
	_, _ = p.Write([]byte("first"))
	_ = p.Close()
	if got, ends, _, _ := sink.state(); got != "first-more" || ends != 1 {
		t.Fatalf("sent %q, ends %d", got, ends)
	}
}

// A body stopped while a piece is being sent drops the rest, and one
// stopped between Room and the piece sends nothing more.
func TestPusherStoppedMidPump(t *testing.T) {
	p := NewPusher(0)
	sink := &fakeSink{room: true}
	sink.onSend = func() { p.Stop() }
	_ = p.Attach(sink, -1)
	_, _ = p.Write([]byte("abc"))
	if _, err := p.Write([]byte("def")); !errors.Is(err, ErrPushStopped) {
		t.Fatalf("write after Stop: %v", err)
	}
	if got, ends, _, _ := sink.state(); got != "abc" || ends != 0 {
		t.Fatalf("sent %q, ends %d", got, ends)
	}

	// Room that stops the body before the piece is taken.
	p = NewPusher(0)
	stopper := &stopOnRoom{p: p}
	_, _ = p.Write([]byte("abc"))
	_ = p.Attach(stopper, -1)
	if stopper.sent != 0 {
		t.Fatalf("sent %d bytes of a stopped body", stopper.sent)
	}
	// And a body closed whose end finds it stopped at the last room check.
	p = NewPusher(0)
	stopper = &stopOnRoom{p: p}
	_ = p.Close()
	_ = p.Attach(stopper, -1)
	if stopper.ends != 0 {
		t.Fatal("ended a stopped body")
	}
}

// stopOnRoom stops its body from inside Room, which reports room anyway.
type stopOnRoom struct {
	p          *Pusher
	sent, ends int
}

func (s *stopOnRoom) Room() bool          { s.p.Stop(); return true }
func (s *stopOnRoom) Send(p []byte) error { s.sent += len(p); return nil }
func (s *stopOnRoom) End() error          { s.ends++; return nil }
func (s *stopOnRoom) Fail(error)          {}

// Read serves a transport that reads the body, waiting for what is written.
func TestPusherRead(t *testing.T) {
	p := NewPusher(4)
	got := make(chan string, 1)
	go func() {
		data, err := io.ReadAll(p)
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		got <- string(data)
	}()
	_, _ = p.Write([]byte("abc"))
	_, _ = p.Write([]byte("defgh"))
	drained := make(chan struct{})
	if p.OnDrain(func() { close(drained) }) {
		<-drained
	}
	time.Sleep(10 * time.Millisecond)
	_, _ = p.Write([]byte("ij"))
	_ = p.Close()
	if s := <-got; s != "abcdefghij" {
		t.Fatalf("read %q", s)
	}

	failed := NewPusher(0)
	failed.CloseWithError(errors.New("cut"))
	if _, err := failed.Read(make([]byte, 4)); err == nil || err.Error() != "cut" {
		t.Fatalf("read of a failed body: %v", err)
	}
	stopped := NewPusher(0)
	stopped.Stop()
	if _, err := stopped.Read(make([]byte, 4)); !errors.Is(err, ErrPushStopped) {
		t.Fatalf("read of a stopped body: %v", err)
	}
}

// A pushed body streams whatever its declared length, and a Source of one
// hands it out as a Pusher, which closing the Source stops.
func TestSourceOfPusher(t *testing.T) {
	p := NewPusher(0)
	req, _ := stdhttp.NewRequest("POST", "http://example.com/", nil)
	req.Body, req.ContentLength = p, 10
	if !Streams(req) {
		t.Fatal("a pushed body of 10 bytes does not stream")
	}
	s := NewSource(req)
	if s.Pusher() != p {
		t.Fatal("Source lost its Pusher")
	}
	s.Close()
	s.Close()
	if _, err := p.Write([]byte("x")); !errors.Is(err, ErrPushStopped) {
		t.Fatalf("write after the Source closed: %v", err)
	}
	other, _ := stdhttp.NewRequest("POST", "http://example.com/", strings.NewReader("x"))
	if NewSource(other).Pusher() != nil {
		t.Fatal("a read body came out as a Pusher")
	}
}

// A body that fails while pieces still wait for room fails its request at
// once, drops them, and lets a waiting producer go.
func TestPusherFailsWithQueue(t *testing.T) {
	p := NewPusher(4)
	sink := &fakeSink{}
	_ = p.Attach(sink, -1)
	_, _ = p.Write([]byte("abcdefgh"))
	drained := false
	if !p.OnDrain(func() { drained = true }) {
		t.Fatal("OnDrain not armed")
	}
	p.CloseWithError(errors.New("gone"))
	if _, _, failed, _ := sink.state(); failed == nil || failed.Error() != "gone" {
		t.Fatalf("failed %v with pieces waiting", failed)
	}
	if !drained || p.Buffered() != 0 {
		t.Fatalf("drained %v, buffered %d", drained, p.Buffered())
	}
}

// discardSink always has room and keeps nothing.
type discardSink struct{}

func (discardSink) Room() bool        { return true }
func (discardSink) Send([]byte) error { return nil }
func (discardSink) End() error        { return nil }
func (discardSink) Fail(error)        {}

// BenchmarkPusherWrite is a producer whose transport keeps up, writing
// 64 KiB at a time.
func BenchmarkPusherWrite(b *testing.B) {
	p := NewPusher(0)
	if err := p.Attach(discardSink{}, -1); err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	for b.Loop() {
		_, _ = p.Write(buf)
	}
}
