//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package arpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/taskpool"
)

// TestMessageHeaderAccessors pins down where each setter of Message puts its
// field, so that a header built or changed by hand, as a MessageCoder may,
// reads back the same through the getters and lays out as the wire format
// says.
func TestMessageHeaderAccessors(t *testing.T) {
	m := NewMessage(CmdRequest, "/m", "payload", 7, nil, nil, nil)
	defer m.Release()
	if m.Len() != HeadLen+len("/m")+len("payload") || m.BodyLen() != len("/m")+len("payload") {
		t.Fatalf("Len %d, BodyLen %d", m.Len(), m.BodyLen())
	}

	m.SetStreamLocal(true)
	m.SetStreamEOF(true)
	m.SetCmd(CmdStream)
	if m.Cmd() != CmdStream || !m.IsStreamLocal() || !m.IsStreamEOF() {
		t.Fatalf("SetCmd lost the stream bits: cmd byte %#x", m.Buffer[HeaderIndexCmd])
	}
	m.SetStreamLocal(false)
	m.SetStreamEOF(false)
	if m.Cmd() != CmdStream || m.IsStreamLocal() || m.IsStreamEOF() {
		t.Fatalf("clearing the stream bits: cmd byte %#x", m.Buffer[HeaderIndexCmd])
	}

	m.SetAsync(true)
	m.SetError(true)
	if !m.IsAsync() || !m.IsError() || m.Error() == nil || m.Error().Error() != "payload" {
		t.Fatalf("flags %#x, Error %v", m.Buffer[HeaderIndexFlag], m.Error())
	}
	m.SetError(false)
	m.SetAsync(false)
	if m.IsAsync() || m.IsError() || m.Error() != nil {
		t.Fatalf("cleared flags %#x", m.Buffer[HeaderIndexFlag])
	}

	for _, i := range []int{0, 3, 7} {
		if err := m.SetFlagBit(i, true); err != nil || !m.IsFlagBitSet(i) {
			t.Fatalf("SetFlagBit(%d) = %v, set %v", i, err, m.IsFlagBitSet(i))
		}
	}
	if m.Buffer[HeaderIndexReserved] != 0x89 {
		t.Fatalf("reserved byte %#x, want 0x89", m.Buffer[HeaderIndexReserved])
	}
	if err := m.SetFlagBit(3, false); err != nil || m.IsFlagBitSet(3) {
		t.Fatalf("clearing flag bit 3 = %v", err)
	}
	for _, i := range []int{-1, 8} {
		if err := m.SetFlagBit(i, true); err != ErrInvalidFlagBitIndex {
			t.Fatalf("SetFlagBit(%d) = %v, want ErrInvalidFlagBitIndex", i, err)
		}
		if m.IsFlagBitSet(i) {
			t.Fatalf("IsFlagBitSet(%d) = true", i)
		}
	}

	m.SetSeq(1 << 40)
	m.SetBodyLen(3)
	m.SetMethodLen(1)
	if m.Seq() != 1<<40 || m.BodyLen() != 3 || m.MethodLen() != 1 || m.Method() != "/" {
		t.Fatalf("seq %d, body length %d, method %q", m.Seq(), m.BodyLen(), m.Method())
	}
}

// TestMessageValuesAndRetain pins down the values a Message carries for the
// coders, which ignore a nil key or value, and that a retained Message
// survives the release that would otherwise free it.
func TestMessageValuesAndRetain(t *testing.T) {
	freed := 0
	h := NewHandler()
	h.HandleMalloc(func(size int) []byte { return make([]byte, size) })
	h.HandleFree(func([]byte) { freed++ })
	m := h.NewMessage(CmdNotify, "/v", nil, 1, nil, nil)
	if m.Values() != nil {
		t.Fatalf("a new Message has values %v", m.Values())
	}
	m.Set(nil, 1)
	m.Set("k", nil)
	if m.Values() != nil {
		t.Fatal("Set stored a nil key or value")
	}
	m.Set("k", "v")
	if v, ok := m.Get("k"); !ok || v != "v" {
		t.Fatalf("Get = %v, %v", v, ok)
	}
	if _, ok := m.Get("missing"); ok {
		t.Fatal("Get found a missing key")
	}

	if n := m.Retain(); n != 1 {
		t.Fatalf("Retain = %d", n)
	}
	if n := m.Release(); n != 0 || freed != 0 {
		t.Fatalf("the first Release of a retained Message = %d, freed %d", n, freed)
	}
	if n := m.Release(); n != -1 || freed != 1 {
		t.Fatalf("the last Release = %d, freed %d", n, freed)
	}
}

// TestNewMessageDefaultsAndEncodeError pins down NewMessage with no Handler,
// which takes DefaultHandler's buffers, and with a value the codec cannot
// encode, which is logged and leaves the payload empty rather than failing.
func TestNewMessageDefaultsAndEncodeError(t *testing.T) {
	m := NewMessage(CmdNotify, "/n", map[string]int{"a": 1}, 1, nil, nil, nil)
	if m.handler != DefaultHandler || string(m.Data()) != `{"a":1}` {
		t.Fatalf("handler %p, payload %q", m.handler, m.Data())
	}
	m.Release()

	m = NewMessage(CmdNotify, "/n", make(chan int), 1, nil, nil, nil)
	if len(m.Data()) != 0 || m.Method() != "/n" {
		t.Fatalf("an unencodable value gave payload %q", m.Data())
	}
	m.Release()
}

// TestCodecValueKinds pins down the payloads that bypass the Codec, pointers
// to them included, and that a nil Codec means DefaultCodec both ways.
func TestCodecValueKinds(t *testing.T) {
	b, s, e := []byte("bytes"), "string", errors.New("error")
	for _, tc := range []struct {
		v    any
		want string
	}{
		{nil, ""},
		{b, "bytes"},
		{&b, "bytes"},
		{s, "string"},
		{&s, "string"},
		{e, "error"},
		{&e, "error"},
		{struct{ A int }{1}, `{"A":1}`},
	} {
		got, err := valueToBytes(nil, tc.v)
		if err != nil || string(got) != tc.want {
			t.Errorf("valueToBytes(%T) = %q, %v, want %q", tc.v, got, err, tc.want)
		}
	}

	var gotErr error
	if err := bytesToValue(nil, []byte("remote failure"), &gotErr); err != nil || gotErr == nil || gotErr.Error() != "remote failure" {
		t.Fatalf("bytesToValue(*error) = %v, %v", gotErr, err)
	}
	var st struct{ A int }
	if err := bytesToValue(nil, []byte(`{"A":2}`), &st); err != nil || st.A != 2 {
		t.Fatalf("bytesToValue with no codec = %+v, %v", st, err)
	}
	if err := bytesToValue(nil, []byte("x"), nil); err != nil {
		t.Fatalf("bytesToValue(nil) = %v", err)
	}
}

// TestSetLoggerNilRestoresDefault pins down that SetLogger(nil) puts the
// package back on fib/log's logger, which is slog.Default until configured.
func TestSetLoggerNilRestoresDefault(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	SetLogger(nil)
	defer SetLogger(discard)
	if logger() != log.Logger() {
		t.Fatal("SetLogger(nil) did not restore fib/log's logger")
	}
	SetLogger(discard)
	if logger() != discard {
		t.Fatal("SetLogger did not take the logger")
	}
}

// TestContextAccessors pins down the Context's own accessors: its values are
// the Message's, and as a context.Context it never ends. A Context with no
// Message, as a pooled one is, has no values.
func TestContextAccessors(t *testing.T) {
	var empty Context
	if empty.Values() != nil || empty.Value("k") != nil {
		t.Fatal("a Context with no Message has values")
	}

	ctx := newContext(nil, NewMessage(CmdRequest, "/c", "body", 1, nil, nil, nil), nil)
	defer ctx.Release()
	ctx.Set("k", "v")
	if v, ok := ctx.Get("k"); !ok || v != "v" || ctx.Value("k") != "v" || ctx.Values()["k"] != "v" {
		t.Fatalf("Get = %v, %v; Value = %v; Values = %v", v, ok, ctx.Value("k"), ctx.Values())
	}
	var raw []byte
	if err := ctx.Bind(&raw); err != nil || string(raw) != "body" {
		t.Fatalf("Bind(*[]byte) = %q, %v", raw, err)
	}
	if _, ok := ctx.Deadline(); ok || ctx.Done() != nil || ctx.Err() != nil {
		t.Fatal("the Context has a deadline or ends")
	}
	var _ context.Context = ctx
}

// TestHandlerSettings pins down the getters of Handler's settings, and the
// settings that undo another: EnablePool(false) and SetAsyncExecutor(nil).
func TestHandlerSettings(t *testing.T) {
	h := NewHandler()
	if h.MaxReconnectTimes() != 0 || h.ReconnectInterval() != time.Second || !h.AsyncResponse() ||
		h.StreamQueueSize() != 4 || h.MaxBodyLen() != DefaultMaxBodyLen || h.TaskPool() != nil || h.Coders() != nil {
		t.Fatal("NewHandler defaults changed")
	}
	h.SetMaxReconnectTimes(3)
	h.SetReconnectInterval(time.Minute)
	h.SetAsyncResponse(false)
	h.SetStreamQueueSize(9)
	h.SetMaxBodyLen(100)
	coder := xorCoder{1}
	h.UseCoder(coder)
	h.UseCoder(nil)
	if h.MaxReconnectTimes() != 3 || h.ReconnectInterval() != time.Minute || h.AsyncResponse() ||
		h.StreamQueueSize() != 9 || h.MaxBodyLen() != 100 || len(h.Coders()) != 1 {
		t.Fatal("a setting did not take")
	}

	h.EnablePool(true)
	if h.malloc == nil || h.free == nil || h.onContextDone == nil || h.onMessageDone == nil {
		t.Fatal("EnablePool(true) left a hook unset")
	}
	h.EnablePool(false)
	if h.malloc != nil || h.free != nil || h.onContextDone != nil || h.onMessageDone != nil {
		t.Fatal("EnablePool(false) left a hook set")
	}

	ran := 0
	h.SetAsyncExecutor(func(f func()) { ran++; f() })
	pool := h.TaskPool()
	if pool == nil {
		t.Fatal("SetAsyncExecutor set no pool")
	}
	count := 0
	tasks := []taskpool.Task{funcTask(func() { count++ }), funcTask(func() { count++ })}
	if n := pool.GoTasks(tasks); n != 2 || ran != 2 || count != 2 {
		t.Fatalf("GoTasks = %d, executor ran %d, tasks ran %d", n, ran, count)
	}
	h.SetAsyncExecutor(nil)
	if h.TaskPool() != nil {
		t.Fatal("SetAsyncExecutor(nil) kept the pool")
	}

	h.Use(nil)
	if len(h.middles) != 0 {
		t.Fatal("Use(nil) added a middleware")
	}
}

// TestHandlerClone pins down that a clone has methods, stream methods and
// singleflights of its own: registering on one leaves the other as it was.
func TestHandlerClone(t *testing.T) {
	h := NewHandler()
	h.Handle("/a", func(*Context) {})
	h.HandleStream("/s", func(*Stream) {})
	h.Singleflight("/a")
	cp := h.Clone()
	cp.Handle("/b", func(*Context) {})
	cp.HandleStream("/s2", func(*Stream) {})
	cp.Singleflight("/b")
	if _, ok := cp.routes["/a"]; !ok {
		t.Fatal("the clone lost /a")
	}
	if _, ok := cp.streams["/s"]; !ok {
		t.Fatal("the clone lost stream /s")
	}
	if _, ok := cp.SingleflightKey("/a", 1); !ok {
		t.Fatal("the clone lost the singleflight of /a")
	}
	if _, ok := h.routes["/b"]; ok {
		t.Fatal("registering on the clone changed the original's methods")
	}
	if _, ok := h.streams["/s2"]; ok {
		t.Fatal("registering on the clone changed the original's streams")
	}
	if _, ok := h.SingleflightKey("/b", 1); ok {
		t.Fatal("registering on the clone changed the original's singleflights")
	}
}

type keyStringer struct{}

func (k keyStringer) String() string { return "stringer" }

// TestSingleflightDefaultKey pins down the key of a method shared without a
// key function: String of a fmt.Stringer, and fmt.Sprint of anything else.
func TestSingleflightDefaultKey(t *testing.T) {
	h := NewHandler()
	h.Singleflight("/m")
	h.Singleflight("/n", nil)
	if key, ok := h.SingleflightKey("/m", keyStringer{}); !ok || key != "stringer" {
		t.Fatalf("key of a Stringer = %q, %v", key, ok)
	}
	if key, ok := h.SingleflightKey("/n", 42); !ok || key != "42" {
		t.Fatalf("key of an int = %q, %v", key, ok)
	}
	if _, ok := h.SingleflightKey("/other", 42); ok {
		t.Fatal("a method that does not share its calls has a key")
	}
}

func expectArpcPanic(t *testing.T, what, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("%s did not panic", what)
		}
		if msg, _ := r.(string); !strings.Contains(msg, want) {
			t.Fatalf("%s panicked with %v, want it to mention %q", what, r, want)
		}
	}()
	f()
}

type noEligibleService struct{}

func (noEligibleService) Helper(int) string { return "" }

// TestHandlerRegistrationPanics pins down the registrations that are
// programming errors, which panic at once rather than surfacing on the
// first request, and Register's error for a nil service.
func TestHandlerRegistrationPanics(t *testing.T) {
	h := NewHandler()
	h.Handle("/dup", func(*Context) {})
	h.HandleStream("/dup", func(*Stream) {})
	long := strings.Repeat("m", MaxMethodLen+1)
	expectArpcPanic(t, "Handle of the empty method", "HandleNotFound", func() { h.Handle("", func(*Context) {}) })
	expectArpcPanic(t, "Handle of a method too long", "MaxMethodLen", func() { h.Handle(long, func(*Context) {}) })
	expectArpcPanic(t, "Handle of a nil handler", "nil handler", func() { h.Handle("/nil", nil) })
	expectArpcPanic(t, "Handle twice", "handler exists", func() { h.Handle("/dup", func(*Context) {}) })
	expectArpcPanic(t, "HandleStream of the empty method", "stream method length", func() { h.HandleStream("", func(*Stream) {}) })
	expectArpcPanic(t, "HandleStream of a method too long", "stream method length", func() { h.HandleStream(long, func(*Stream) {}) })
	expectArpcPanic(t, "HandleStream of a nil handler", "nil stream handler", func() { h.HandleStream("/nil", nil) })
	expectArpcPanic(t, "HandleStream twice", "stream handler exists", func() { h.HandleStream("/dup", func(*Stream) {}) })
	expectArpcPanic(t, "Singleflight of the empty method", "empty method", func() { h.Singleflight("") })
	expectArpcPanic(t, "Register with no eligible method", "no eligible method", func() { _ = h.Register("x", noEligibleService{}) })
	if err := h.Register("x", nil); err == nil {
		t.Fatal("Register(nil) succeeded")
	}
}
