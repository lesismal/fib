package sctp

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

type chanMsg struct {
	label string
	data  []byte
	text  bool
}

// chanEnds records the channels each end learns of and what they receive.
type chanEnds struct {
	opened [2][]*DataChannel
	msgs   [2][]chanMsg
	closes [2]int
}

func (e *chanEnds) configure(i int, c *Config) {
	c.OnDataChannel = func(ch *DataChannel) {
		e.opened[i] = append(e.opened[i], ch)
		e.watch(i, ch)
	}
}

func (e *chanEnds) watch(i int, ch *DataChannel) {
	ch.OnMessage = func(d []byte, text bool) {
		e.msgs[i] = append(e.msgs[i], chanMsg{ch.Label(), append([]byte(nil), d...), text})
	}
	ch.OnClose = func() { e.closes[i]++ }
}

func TestDataChannels(t *testing.T) {
	var e chanEnds
	l := newLink(t, 1, e.configure)
	a, b := l.ends[0], l.ends[1]
	// One channel opened before the association is up, one after.
	early, err := a.OpenChannel(ChannelOptions{Label: "early", Protocol: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	e.watch(0, early)
	opened := false
	early.OnOpen = func() { opened = true }
	if early.State() != ChannelConnecting || early.ID() != 0 {
		t.Fatalf("early: %v id %d", early.State(), early.ID())
	}
	if err := early.SendText("too soon"); !errors.Is(err, ErrChannelNotOpen) {
		t.Fatal(err)
	}
	l.connect(true)
	if !opened || early.State() != ChannelOpen {
		t.Fatal("early channel did not open")
	}
	late, _ := b.OpenChannel(ChannelOptions{Label: "late", Type: ChannelReliableUnordered})
	e.watch(1, late)
	if late.ID() != 1 || late.State() != ChannelOpen {
		t.Fatalf("server channel id %d state %v", late.ID(), late.State())
	}
	second, _ := a.OpenChannel(ChannelOptions{Label: "second"})
	if second.ID() != 2 {
		t.Fatalf("client's second channel id %d", second.ID())
	}
	_ = early.SendText("hello")
	_ = early.SendText("")
	_ = early.Send(nil)
	_ = early.Send([]byte{1, 2, 3})
	_ = late.Send(bytes.Repeat([]byte{7}, 100<<10/2))
	l.run(time.Minute, func() bool { return len(e.msgs[1]) == 4 && len(e.msgs[0]) == 1 })
	if len(e.opened[1]) != 2 || len(e.opened[0]) != 1 {
		t.Fatalf("opened %d %d", len(e.opened[0]), len(e.opened[1]))
	}
	rb := e.opened[1][0]
	if rb.Label() != "early" || rb.Protocol() != "p1" || rb.ID() != 0 || !rb.Ordered() {
		t.Fatalf("remote channel %q %q %d", rb.Label(), rb.Protocol(), rb.ID())
	}
	if !early.acked {
		t.Fatal("no DCEP ACK")
	}
	want := []chanMsg{{"early", []byte("hello"), true}, {"early", nil, true}, {"early", nil, false}, {"early", []byte{1, 2, 3}, false}}
	for i, m := range e.msgs[1] {
		if m.label != want[i].label || !bytes.Equal(m.data, want[i].data) || m.text != want[i].text {
			t.Fatalf("message %d: %+v", i, m)
		}
	}
	if ra := e.opened[0][0]; ra.Label() != "late" || ra.Type() != ChannelReliableUnordered || len(e.msgs[0][0].data) != 50<<10 {
		t.Fatal("server's channel")
	}

	// Closing on one side closes both.
	_ = early.SendText("last")
	_ = early.Close()
	if err := early.SendText("x"); !errors.Is(err, ErrChannelClosed) {
		t.Fatal(err)
	}
	l.run(time.Minute, func() bool { return e.closes[0] == 1 && e.closes[1] == 1 })
	if e.closes[0] != 1 || e.closes[1] != 1 || early.State() != ChannelClosed || rb.State() != ChannelClosed {
		t.Fatalf("closes %v", e.closes)
	}
	if string(e.msgs[1][4].data) != "last" {
		t.Fatal("data queued before close lost")
	}
	// Its id is free again, and the new channel starts at SSN 0.
	again, _ := a.OpenChannel(ChannelOptions{Label: "again"})
	if again.ID() != 0 {
		t.Fatalf("id %d", again.ID())
	}
	_ = again.SendText("x")
	l.run(time.Minute, func() bool { return len(e.opened[1]) == 3 && len(e.msgs[1]) == 6 })
	if len(e.msgs[1]) != 6 || e.msgs[1][5].label != "again" {
		t.Fatal("reopened channel")
	}
	// The peer closing closes ours.
	_ = e.opened[0][0].Close()
	l.run(time.Minute, func() bool { return late.State() == ChannelClosed })
	if late.State() != ChannelClosed {
		t.Fatal("peer close")
	}
	// Ending the association closes the rest.
	a.Abort()
	if second.State() != ChannelClosed || again.State() != ChannelClosed {
		t.Fatal("abort left channels open")
	}
}

func TestNegotiatedChannel(t *testing.T) {
	var e chanEnds
	l := newLink(t, 1, e.configure)
	l.connect(false)
	var chs [2]*DataChannel
	for i := range 2 {
		ch, err := l.ends[i].OpenChannel(ChannelOptions{Label: "n", Negotiated: true, ID: 7, Type: ChannelPartialReliableRexmitUnordered, ReliabilityParameter: 3})
		if err != nil {
			t.Fatal(err)
		}
		e.watch(i, ch)
		chs[i] = ch
	}
	if _, err := l.ends[0].OpenChannel(ChannelOptions{Negotiated: true, ID: 7}); !errors.Is(err, ErrStreamInUse) {
		t.Fatal(err)
	}
	_ = chs[0].SendText("a")
	_ = chs[1].SendText("b")
	l.run(time.Minute, func() bool { return len(e.msgs[0]) == 1 && len(e.msgs[1]) == 1 })
	if len(e.opened[0])+len(e.opened[1]) != 0 || string(e.msgs[1][0].data) != "a" || string(e.msgs[0][0].data) != "b" {
		t.Fatal("negotiated channel")
	}
	// An in-band channel skips the negotiated id.
	l.ends[1].stream(1).ch = nil
	ch, _ := l.ends[0].OpenChannel(ChannelOptions{})
	if ch.ID() != 0 {
		t.Fatalf("id %d", ch.ID())
	}
}

func TestBufferedAmountLow(t *testing.T) {
	var e chanEnds
	l := newLink(t, 1, e.configure)
	l.connect(false)
	ch, _ := l.ends[0].OpenChannel(ChannelOptions{Label: "b"})
	ch.SetBufferedAmountLowThreshold(10 << 10)
	low := 0
	ch.OnBufferedAmountLow = func() {
		low++
		if low < 3 {
			// Refill from the callback, as a sender pacing itself would.
			_ = ch.Send(make([]byte, 30<<10))
		}
	}
	_ = ch.Send(make([]byte, 30<<10))
	if ch.BufferedAmount() < 30<<10 {
		t.Fatalf("buffered %d", ch.BufferedAmount())
	}
	l.run(time.Minute, func() bool { return low == 3 && ch.BufferedAmount() == 0 })
	if low != 3 || ch.BufferedAmount() != 0 || len(e.msgs[1]) != 3 {
		t.Fatalf("low %d buffered %d messages %d", low, ch.BufferedAmount(), len(e.msgs[1]))
	}
}

func TestChannelIDsExhausted(t *testing.T) {
	l := newLink(t, 1, func(i int, c *Config) { c.Streams = 4 })
	l.connect(false)
	a := l.ends[0]
	for _, want := range []uint16{0, 2} {
		ch, err := a.OpenChannel(ChannelOptions{})
		if err != nil || ch.ID() != want {
			t.Fatal(err)
		}
	}
	if _, err := a.OpenChannel(ChannelOptions{}); !errors.Is(err, ErrNoStreamID) {
		t.Fatal(err)
	}
}

// TestChannelsLossy closes and reopens channels while the link loses and
// reorders packets.
func TestChannelsLossy(t *testing.T) {
	var e chanEnds
	l := newLink(t, 11, e.configure)
	l.connect(true)
	l.loss, l.dup, l.jitter = 0.1, 0.05, 30*time.Millisecond
	for round := range 5 {
		ch, err := l.ends[round%2].OpenChannel(ChannelOptions{Label: "r"})
		if err != nil {
			t.Fatal(err)
		}
		e.watch(round%2, ch)
		for range 10 {
			_ = ch.Send(make([]byte, 3000))
		}
		_ = ch.Close()
		if !l.run(10*time.Minute, func() bool { return ch.State() == ChannelClosed }) {
			t.Fatalf("round %d: channel stuck in %v", round, ch.State())
		}
	}
	if n := len(e.msgs[0]) + len(e.msgs[1]); n != 50 {
		t.Fatalf("%d messages", n)
	}
}
