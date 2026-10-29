package quic

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// waitClose waits for a connection's handler to hear it closed.
func waitClose(t *testing.T, h *testHandler) error {
	t.Helper()
	select {
	case err := <-h.closed:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the connection did not close")
		return nil
	}
}

// CloseWhenDone closes at once when no bidirectional stream is open, and
// otherwise once the last of them is finished; asking again, or closing a
// connection already closed, changes nothing.
func TestCloseWhenDone(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.client.CloseWhenDone(5, "idle")
	err := waitClose(t, p.serverH)
	var ae *ApplicationError
	if !errors.As(err, &ae) || ae.Code != 5 || !ae.Remote {
		t.Fatalf("server closed with %v", err)
	}
	p.client.CloseWhenDone(6, "again")
	p.client.Close(7, "again")
	if err := waitClose(t, p.clientH); err.(*ApplicationError).Code != 5 {
		t.Fatalf("client closed with %v", err)
	}

	p = newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("last"), false); err != nil {
		t.Fatal(err)
	}
	p.client.CloseWhenDone(5, "done")
	select {
	case err := <-p.serverH.closed:
		t.Fatalf("closed with a stream open: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitClose(t, p.serverH); !errors.As(err, &ae) || ae.Code != 5 {
		t.Fatalf("server closed with %v", err)
	}
	if got := p.clientH.received(s.ID()); string(got) != "last" {
		t.Fatalf("echo %q", got)
	}
}

// What a unidirectional stream was written before CloseWhenDone reaches the
// peer before the close does, as HTTP/3's GOAWAY on its control stream must,
// even when it was written while the handler was being called, which holds
// what is written until the calls end.
func TestCloseWhenDoneSendsWritten(t *testing.T) {
	p := newTestPair(t, 0, nil)
	var id atomic.Uint64
	p.serverH.onData = func(s *Stream, _ []byte, fin bool) {
		if !fin {
			return
		}
		u, err := s.Conn().OpenUniStream()
		if err != nil {
			t.Error(err)
			return
		}
		id.Store(u.ID())
		if err := u.Write([]byte("goaway"), false); err != nil {
			t.Error(err)
		}
		s.Conn().CloseWhenDone(5, "idle")
	}
	u, err := p.client.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := u.Write([]byte("hello"), true); err != nil {
		t.Fatal(err)
	}
	var ae *ApplicationError
	if err := waitClose(t, p.clientH); !errors.As(err, &ae) || ae.Code != 5 {
		t.Fatalf("client closed with %v", err)
	}
	if got := p.clientH.received(id.Load()); string(got) != "goaway" {
		t.Fatalf("the client got %q before the close", got)
	}
}

// A close that waits for unidirectional data the peer will not take, held
// back by its flow control, waits only so long.
func TestCloseWhenDoneBounded(t *testing.T) {
	p := newTestPair(t, 0, func(client, _ *Config) { client.StreamReceiveWindow = 1 << 10 })
	p.clientH.onData = func(s *Stream, _ []byte, _ bool) { s.HoldCredit() }
	u, err := p.server.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := u.Write(make([]byte, 64<<10), false); err != nil {
		t.Fatal(err)
	}
	p.server.CloseWhenDone(5, "idle")
	var ae *ApplicationError
	if err := waitClose(t, p.clientH); !errors.As(err, &ae) || ae.Code != 5 {
		t.Fatalf("client closed with %v", err)
	}
	if got := len(p.clientH.received(u.ID())); got == 0 || got >= 64<<10 {
		t.Fatalf("the client got %d bytes", got)
	}
}

// A client opens no more bidirectional streams than the server allows, and
// unidirectional ones on a count of their own.
func TestStreamLimits(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.MaxIncomingStreams = 1 })
	if _, err := p.client.OpenStream(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.client.OpenStream(); err != ErrStreamLimit {
		t.Fatalf("second stream: %v", err)
	}
	u, err := p.client.OpenUniStream()
	if err != nil || u.Bidirectional() || u.ID() != 2 {
		t.Fatalf("unidirectional stream %v: %v", u, err)
	}
}

// stopHandler is a testHandler that reports STOP_SENDING and resets, and
// what writing to a stream stopped that way gets.
type stopHandler struct {
	*testHandler
	stopped  chan uint64
	writeErr chan error
}

func (h *stopHandler) OnStopSending(s *Stream, code uint64) {
	buf := bufferpool.Get(3)
	h.writeErr <- s.WriteOwned(buf, false)
	h.stopped <- code
}

// STOP_SENDING from a client resets the server's sending side, which the
// server's handler hears of, and writes there fail from then on; a client
// that resets its own side tells the server so once, however often it asks.
func TestStopSendingAndReset(t *testing.T) {
	var sh *stopHandler
	p := newTestPairOpts(t, 0, nil, pairOpts{wrapServer: func(h *testHandler) Handler {
		sh = &stopHandler{testHandler: h, stopped: make(chan uint64, 1), writeErr: make(chan error, 1)}
		return sh
	}})
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("hello"), false); err != nil {
		t.Fatal(err)
	}
	s.StopSending(9)
	s.StopSending(10)
	select {
	case code := <-sh.stopped:
		if code != 9 {
			t.Fatalf("stop sending code %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server did not hear STOP_SENDING")
	}
	if err := <-sh.writeErr; err != ErrClosed {
		t.Fatalf("write after STOP_SENDING: %v", err)
	}
	s.Reset(3)
	s.Reset(4)
	select {
	case id := <-p.serverH.reset:
		if id != s.ID() {
			t.Fatalf("reset of stream %d", id)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server did not hear the reset")
	}
	if err := s.WaitSendable(0); err != ErrClosed {
		t.Fatalf("waiting on a reset stream: %v", err)
	}
	select {
	case id := <-p.serverH.reset:
		t.Fatalf("stream %d reset twice", id)
	case <-time.After(20 * time.Millisecond):
	}
}

// A receiver that holds its credit back paces the sender to what it
// reports consumed, and a writer waiting for room is woken as it is made:
// a transfer many times the stream and connection windows still
// completes.
func TestHeldCreditPacesSender(t *testing.T) {
	const window = 4 << 10
	p := newTestPair(t, 0, func(_, server *Config) {
		server.StreamReceiveWindow, server.ConnReceiveWindow = window, 2*window
	})
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		s.HoldCredit()
		s.Credit(0)
		s.Credit(len(data))
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("paced"), 64<<10/5)
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitSendable(0); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.serverH, s.ID())
	if got := p.serverH.received(s.ID()); !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, want %d", len(got), len(payload))
	}
}

// callsDoneHandler counts the runs of handler calls it is told the end of.
type callsDoneHandler struct {
	*testHandler
	runs atomic.Int32
}

func (h *callsDoneHandler) OnCallsDone(*Conn) { h.runs.Add(1) }

// A server that recycles the datagrams it is handed, with a handler told
// when each run of calls ends, serves as any other; and a connection
// reports the connection IDs it answers to and its handler.
func TestRecycledDatagramsAndCallsDone(t *testing.T) {
	var ch *callsDoneHandler
	p := newTestPairOpts(t, 0, func(_, server *Config) { server.RecycleDatagrams = true }, pairOpts{
		wrapServer: func(h *testHandler) Handler {
			ch = &callsDoneHandler{testHandler: h}
			return ch
		}})
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("recycled"), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); string(got) != "recycled" || ch.runs.Load() == 0 {
		t.Fatalf("echo %q after %d runs of calls", got, ch.runs.Load())
	}
	server := p.serverConn(t)
	if !server.Owns(server.LocalCID()) || !server.Owns(p.client.odcid) || p.client.Owns(p.client.odcid) {
		t.Fatal("connection IDs owned")
	}
	if p.client.Handler() != Handler(p.clientH) || p.client.EarlyDataAccepted() {
		t.Fatal("client handler or early data")
	}
}

// Records of sent packets with many frames are not kept for reuse with all
// their room, only with their inline room for a frame or two; a
// RESET_STREAM, MAX_DATA or HANDSHAKE_DONE that is lost is sent again, and
// the lost data of a stream since reset is not.
func TestLostResetAndMaxData(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseSent(&sentPacket{frames: make([]sentFrame, 0, 4*maxFreeFrames)})
	if p := c.freeSent[len(c.freeSent)-1]; cap(p.frames) > len(p.inline) {
		t.Fatalf("kept room for %d frames", cap(p.frames))
	}
	s := c.newStream(1)
	c.openedBidi = 1
	s.resetLocked(3)
	s.resetPending = false
	c.frameLost(spaceApp, &sentFrame{kind: sfResetStream, stream: s})
	c.frameLost(spaceApp, &sentFrame{kind: sfMaxData})
	c.frameLost(spaceApp, &sentFrame{kind: sfHandshakeDone})
	// Data of a reset stream is not sent again.
	c.frameLost(spaceApp, &sentFrame{kind: sfStream, stream: s, n: 4})
	if s.send.hasLost() || !c.handshakeDonePending {
		t.Fatalf("lost data queued %v, HANDSHAKE_DONE pending %v", s.send.hasLost(), c.handshakeDonePending)
	}
	if !s.resetPending || !c.maxDataPending {
		t.Fatalf("reset pending %v, max data pending %v", s.resetPending, c.maxDataPending)
	}
	sp := &sentPacket{}
	if b := c.appendControlFrames(nil, 100, sp); len(b) < 2 || b[0] != frameHandshakeDone || b[1] != frameMaxData || c.maxDataPending {
		t.Fatalf("control frames %x", b)
	}
	// Once the reset is acknowledged, the stream's sending side is over.
	c.frameAcked(spaceApp, &sentFrame{kind: sfResetStream, stream: s})
	if !s.resetAcked || !s.sendDone() {
		t.Fatal("acknowledged reset left the sending side open")
	}
}
