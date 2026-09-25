package quic

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"errors"
	mrand "math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/tlstest"
)

// pipeEnd is one side of an in-memory path. Datagrams are delivered in
// order on a goroutine of their own, as a network would, and some may be
// dropped on purpose.
type pipeEnd struct {
	queue chan []byte
	peer  *pipeEnd
	loss  float64
	// drop, when set, decides the fate of each datagram instead of loss.
	drop   func([]byte) bool
	mu     sync.Mutex
	closed bool
}

func (p *pipeEnd) Send(d []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return net.ErrClosed
	}
	if p.drop != nil && p.drop(d) || p.drop == nil && p.loss > 0 && mrand.Float64() < p.loss {
		return nil
	}
	select {
	case p.peer.queue <- bytes.Clone(d):
	default:
		// A full queue is a full socket buffer: the datagram is lost.
	}
	return nil
}

func (p *pipeEnd) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}

func newPipe() (a, b *pipeEnd) {
	a = &pipeEnd{queue: make(chan []byte, 4096)}
	b = &pipeEnd{queue: make(chan []byte, 4096)}
	a.peer, b.peer = b, a
	return a, b
}

// testHandler records what a connection hears.
type testHandler struct {
	mu        sync.Mutex
	handshake chan struct{}
	closed    chan error
	data      map[uint64][]byte
	fin       map[uint64]bool
	finished  chan uint64
	finSet    map[uint64]bool
	reset     chan uint64
	onData    func(s *Stream, data []byte, fin bool)
}

func newTestHandler() *testHandler {
	return &testHandler{
		handshake: make(chan struct{}),
		closed:    make(chan error, 1),
		data:      make(map[uint64][]byte),
		fin:       make(map[uint64]bool),
		finished:  make(chan uint64, 1024),
		finSet:    make(map[uint64]bool),
		reset:     make(chan uint64, 1024),
	}
}

func (h *testHandler) OnHandshake(*Conn) { close(h.handshake) }
func (h *testHandler) OnStreamData(s *Stream, data []byte, fin bool) {
	h.mu.Lock()
	h.data[s.ID()] = append(h.data[s.ID()], data...)
	h.fin[s.ID()] = fin
	h.mu.Unlock()
	if h.onData != nil {
		h.onData(s, data, fin)
	}
	if fin {
		h.finished <- s.ID()
	}
}
func (h *testHandler) OnStreamReset(s *Stream, code uint64) { h.reset <- s.ID() }
func (h *testHandler) OnStopSending(*Stream, uint64)        {}
func (h *testHandler) OnStreamsAvailable(*Conn)             {}
func (h *testHandler) OnClose(_ *Conn, err error)           { h.closed <- err }

func (h *testHandler) received(id uint64) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.data[id]
}

type testPair struct {
	client, server       *Conn
	clientH, serverH     *testHandler
	clientEnd, serverEnd *pipeEnd
	done                 chan struct{}
	wg                   sync.WaitGroup
	serverMu             sync.Mutex
	// stray gets what reaches the server while it has no connection.
	stray func(d []byte)
	// bursts has the server take what has piled up for it with one
	// HandleDatagrams, as a fib server does, rather than a datagram a call.
	bursts atomic.Bool
}

func (p *testPair) close() {
	close(p.done)
	p.wg.Wait()
}

// newTestPair connects a client and a server over a pipe. The server starts
// on the first datagram that reaches it, as a fib server does.
func newTestPair(t *testing.T, loss float64, configure func(client, server *Config)) *testPair {
	t.Helper()
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.NextProtos = []string{"test"}
	clientTLS.NextProtos = []string{"test"}
	cc := Config{TLSConfig: clientTLS}
	sc := Config{TLSConfig: serverTLS}
	if configure != nil {
		configure(&cc, &sc)
	}
	p := &testPair{clientH: newTestHandler(), serverH: newTestHandler(), done: make(chan struct{})}
	p.clientEnd, p.serverEnd = newPipe()
	p.clientEnd.loss, p.serverEnd.loss = loss, loss
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
	p.wg.Add(2)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case d := <-p.serverEnd.queue:
				p.serverMu.Lock()
				if p.server == nil {
					if p.stray != nil {
						p.stray(d)
					} else if IsInitial(d) {
						p.server = Accept(p.serverEnd, addr, sc, p.serverH, d)
					}
					p.serverMu.Unlock()
					continue
				}
				s := p.server
				p.serverMu.Unlock()
				if !p.bursts.Load() {
					s.HandleDatagram(d)
					continue
				}
				// A flight of datagrams is all there once the path has
				// been quiet for a while.
				burst := [][]byte{d}
				for more := true; more; {
					select {
					case d := <-p.serverEnd.queue:
						burst = append(burst, d)
					case <-time.After(2 * time.Millisecond):
						more = false
					}
				}
				s.HandleDatagrams(burst)
			case <-p.done:
				return
			}
		}
	}()
	p.client, err = Dial(p.clientEnd, addr, cc, p.clientH)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer p.wg.Done()
		for {
			select {
			case d := <-p.clientEnd.queue:
				p.client.HandleDatagram(d)
			case <-p.done:
				return
			}
		}
	}()
	t.Cleanup(p.close)
	select {
	case <-p.clientH.handshake:
	case err := <-p.clientH.closed:
		t.Fatalf("client closed during handshake: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("handshake timed out")
	}
	return p
}

func waitFinished(t *testing.T, h *testHandler, id uint64) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for !h.finSet[id] {
		select {
		case got := <-h.finished:
			h.finSet[got] = true
		case err := <-h.closed:
			t.Fatalf("connection closed: %v", err)
		case <-timeout:
			t.Fatalf("stream %d not finished", id)
		}
	}
}

func TestHandshakeAndEcho(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("hello, quic"), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); string(got) != "hello, quic" {
		t.Fatalf("echo %q", got)
	}
	state := p.client.ConnectionState()
	if state.NegotiatedProtocol != "test" || state.Version != tls.VersionTLS13 {
		t.Fatalf("state %+v", state)
	}
}

func testTransfer(t *testing.T, loss float64, size int) {
	p := newTestPair(t, loss, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	payload := make([]byte, size)
	_, _ = rand.Read(payload)
	var streams []*Stream
	for i := 0; i < 4; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(payload, true); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, s)
	}
	for _, s := range streams {
		waitFinished(t, p.clientH, s.ID())
		if got := p.clientH.received(s.ID()); !bytes.Equal(got, payload) {
			t.Fatalf("stream %d: got %d bytes, want %d", s.ID(), len(got), len(payload))
		}
	}
}

func TestTransfer(t *testing.T) { testTransfer(t, 0, 2<<20) }

func TestTransferWithLoss(t *testing.T) { testTransfer(t, 0.05, 256<<10) }

func TestHandshakeWithLoss(t *testing.T) {
	for i := 0; i < 5; i++ {
		testTransfer(t, 0.2, 16<<10)
	}
}

func TestManyStreams(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.MaxIncomingStreams = 10 })
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	done := 0
	for done < 100 {
		s, err := p.client.OpenStream()
		if errors.Is(err, ErrStreamLimit) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write([]byte("x"), true)
		waitFinished(t, p.clientH, s.ID())
		done++
	}
}

func TestStreamReset(t *testing.T) {
	p := newTestPair(t, 0, nil)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("partial"), false)
	s.Reset(7)
	select {
	case id := <-p.serverH.reset:
		if id != s.ID() {
			t.Fatalf("reset of %d", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reset")
	}
}

func TestApplicationClose(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.client.Close(0x42, "bye")
	select {
	case err := <-p.serverH.closed:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != 0x42 || appErr.Reason != "bye" || !appErr.Remote {
			t.Fatalf("server got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server not closed")
	}
	if err := <-p.clientH.closed; err == nil {
		t.Fatal("client closed without an error")
	}
}

func TestIdleTimeout(t *testing.T) {
	p := newTestPair(t, 0, func(client, _ *Config) { client.MaxIdleTimeout = 200 * time.Millisecond })
	select {
	case err := <-p.serverH.closed:
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("server got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not idle out")
	}
}

func TestKeepAlive(t *testing.T) {
	p := newTestPair(t, 0, func(client, _ *Config) {
		client.MaxIdleTimeout = 300 * time.Millisecond
		client.KeepAlivePeriod = 100 * time.Millisecond
	})
	select {
	case err := <-p.serverH.closed:
		t.Fatalf("server closed: %v", err)
	case err := <-p.clientH.closed:
		t.Fatalf("client closed: %v", err)
	case <-time.After(time.Second):
	}
}

func TestStatelessReset(t *testing.T) {
	key := NewResetKey()
	p := newTestPair(t, 0, func(_, server *Config) { server.ResetKey = &key })
	// The server forgets the connection; what the client sends next is
	// answered with a stateless reset.
	p.serverMu.Lock()
	p.server.Abort(errors.New("forgotten"))
	p.server = nil
	p.serverEnd.closed = false
	p.stray = func(d []byte) {
		if reset := key.StatelessReset(d); reset != nil {
			_ = p.serverEnd.Send(reset)
		}
	}
	p.serverMu.Unlock()
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("anyone there?"), true)
	select {
	case err := <-p.clientH.closed:
		if !errors.Is(err, ErrStatelessReset) {
			t.Fatalf("client got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client not reset")
	}
}

func TestVersionNegotiationPacket(t *testing.T) {
	d := make([]byte, 1200)
	d[0] = 0xc0
	copy(d[1:5], []byte{0x0a, 0x0a, 0x0a, 0x0a})
	d[5] = 8
	copy(d[6:14], "dcid----")
	d[14] = 4
	copy(d[15:19], "scid")
	vn := VersionNegotiation(d)
	if vn == nil {
		t.Fatal("no version negotiation")
	}
	h, err := parseHeader(vn, 0)
	if err != errUnknownVersion || h.version != 0 || string(h.dcid) != "scid" || string(h.scid) != "dcid----" {
		t.Fatalf("header %+v %v", h, err)
	}
	if IsInitial(d) {
		t.Fatal("unknown version taken for an Initial")
	}
}

func TestPathResponsesBounded(t *testing.T) {
	c := &Conn{}
	for i := 0; i < 100; i++ {
		c.queuePathResponse([8]byte{byte(i)})
	}
	if len(c.pathResponses) != maxPathResponses {
		t.Fatalf("%d responses queued, want %d", len(c.pathResponses), maxPathResponses)
	}
	for i, r := range c.pathResponses {
		if want := byte(100 - maxPathResponses + i); r[0] != want {
			t.Fatalf("response %d answers challenge %d, want %d", i, r[0], want)
		}
	}
}

// TestPathChallengeFlood sends many PATH_CHALLENGE frames in one packet: the
// connection answers without queueing them all, and stays up.
func TestPathChallengeFlood(t *testing.T) {
	p := newTestPair(t, 0, nil)
	var payload []byte
	for i := 0; i < 100; i++ {
		payload = append(payload, framePathChallenge, byte(i), 1, 2, 3, 4, 5, 6, 7)
	}
	c := p.client
	c.mu.Lock()
	plan := packetPlan{space: spaceApp, pn: c.spaces[spaceApp].nextPN, pnLen: 4, payload: payload, sp: &sentPacket{}}
	c.spaces[spaceApp].nextPN++
	d := c.sealPlan(nil, &plan)
	c.mu.Unlock()
	_ = p.clientEnd.Send(d)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("still there"), true)
	waitFinished(t, p.clientH, s.ID())
	p.server.mu.Lock()
	queued := len(p.server.pathResponses)
	p.server.mu.Unlock()
	if queued > maxPathResponses {
		t.Fatalf("%d responses queued", queued)
	}
}

// TestKeyUpdate lowers the AEAD confidentiality limit so that a short
// exchange passes it: the side that reaches it updates its keys, the peer
// follows, and the data keeps flowing.
func TestKeyUpdate(t *testing.T) {
	testAEADLimits.confidentiality.Store(4)
	t.Cleanup(func() { testAEADLimits.confidentiality.Store(0) })
	p := newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	for i := 0; i < 20; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write([]byte("round"), true); err != nil {
			t.Fatal(err)
		}
		waitFinished(t, p.clientH, s.ID())
		if got := p.clientH.received(s.ID()); string(got) != "round" {
			t.Fatalf("echo %q", got)
		}
	}
	// Both directions have updated their keys several times. One side may
	// be a phase ahead, having just started an update the other has not
	// answered yet, but no further than that.
	for _, c := range []*Conn{p.client, p.server} {
		c.mu.Lock()
		txGen, rxGen := c.txGen, c.rxGen
		c.mu.Unlock()
		if txGen == 0 || rxGen == 0 || txGen > rxGen+1 || rxGen > txGen+1 {
			t.Fatalf("client=%v: tx generation %d, rx generation %d", c.isClient, txGen, rxGen)
		}
	}
}

// TestIntegrityLimit ends a connection whose keys have been attacked with
// more forgeries than the AEAD allows (RFC 9001 section 6.6).
func TestIntegrityLimit(t *testing.T) {
	testAEADLimits.integrity.Store(3)
	t.Cleanup(func() { testAEADLimits.integrity.Store(0) })
	p := newTestPair(t, 0, nil)
	// Short header packets for the client's connection ID that no key opens.
	forged := make([]byte, 64)
	p.client.mu.Lock()
	copy(forged[1:], p.client.scid)
	p.client.mu.Unlock()
	forged[0] = 0x40
	for i := 0; i < 5; i++ {
		forged[len(forged)-1] = byte(i)
		_ = p.serverEnd.Send(forged)
	}
	select {
	case err := <-p.clientH.closed:
		var te *TransportError
		if !errors.As(err, &te) || te.Code != errAEADLimitReached {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection survived the forgeries")
	}
}

// watchSends records the size of every datagram end sends from now on.
func watchSends(end *pipeEnd) func() []int {
	var mu sync.Mutex
	var sizes []int
	end.mu.Lock()
	end.drop = func(d []byte) bool {
		mu.Lock()
		sizes = append(sizes, len(d))
		mu.Unlock()
		return false
	}
	end.mu.Unlock()
	return func() []int {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), sizes...)
	}
}

func TestMaxDatagramSize(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.MaxDatagramSize = 1350 })
	payload := make([]byte, 64<<10)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if fin {
			_ = s.Write(payload, true)
		}
	}
	serverSizes := watchSends(p.serverEnd)
	clientSizes := watchSends(p.clientEnd)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	largest := func(sizes []int) int { return slices.Max(append(sizes, 0)) }
	if got := largest(serverSizes()); got != 1350 {
		t.Errorf("server's largest datagram is %d bytes, want 1350", got)
	}
	if got := largest(clientSizes()); got != maxDatagram {
		t.Errorf("client's largest datagram is %d bytes, want the default %d", got, maxDatagram)
	}
}

// TestWriteDuringSlowHandlerCall writes from another goroutine while a
// handler call blocks: the write is sent without waiting for the call to
// return, as are acknowledgements and loss recovery.
func TestWriteDuringSlowHandlerCall(t *testing.T) {
	p := newTestPair(t, 0, nil)
	release := make(chan struct{})
	defer close(release)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if !fin {
			return
		}
		go func() {
			// Once the acknowledgement of the request is out, so that the
			// write is all there is to send.
			time.Sleep(100 * time.Millisecond)
			_ = s.Write([]byte("early"), true)
		}()
		<-release
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("request"), true); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-p.clientH.finished:
		if id != s.ID() || string(p.clientH.received(id)) != "early" {
			t.Fatalf("stream %d finished with %q", id, p.clientH.received(id))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the write waited for the handler call to return")
	}
}

// TestWritesDuringHandlerCallShareDatagrams writes many small pieces while
// the handler is told of a request: they leave together, not a datagram each.
func TestWritesDuringHandlerCallShareDatagrams(t *testing.T) {
	p := newTestPair(t, 0, nil)
	const pieces = 20
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if !fin {
			return
		}
		for i := 0; i < pieces; i++ {
			_ = s.Write([]byte("0123456789012345678901234567890123456789"), i == pieces-1)
		}
	}
	// A first request lets the end of the handshake go by, so that only the
	// answer to the second one is counted.
	first, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Write([]byte("request"), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, first.ID())
	time.Sleep(50 * time.Millisecond)
	serverSizes := watchSends(p.serverEnd)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("request"), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := len(p.clientH.received(s.ID())); got != pieces*40 {
		t.Fatalf("received %d bytes, want %d", got, pieces*40)
	}
	if n := len(serverSizes()); n > 2 {
		t.Fatalf("%d writes left in %d datagrams, want them together: %v", pieces, n, serverSizes())
	}
}

// Once the peer has acknowledged an ACK frame, the ranges it covered are no
// longer sent, so a connection whose peer's packet numbers have gaps, lost
// or skipped, keeps sending acknowledgements of a few ranges rather than of
// every range ever kept (RFC 9000 section 13.2.4).
func TestAckRangesForgotten(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	// The client's packets go missing now and then, which leaves gaps in
	// what the server has received.
	p.clientEnd.mu.Lock()
	p.clientEnd.loss = 0.1
	p.clientEnd.mu.Unlock()
	payload := make([]byte, 256<<10)
	_, _ = rand.Read(payload)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	p.server.mu.Lock()
	ranges := len(p.server.spaces[spaceApp].recv)
	p.server.mu.Unlock()
	if ranges >= maxAckRanges/2 {
		t.Fatalf("the server still acknowledges %d ranges", ranges)
	}
}

// A request whose answer comes from another goroutine, with the connection
// told to expect it, is acknowledged in the answer rather than in a packet of
// its own ahead of it, even when the request took packets enough to want an
// acknowledgement at once; one whose answer is late is acknowledged anyway.
func TestExpectedWriteCarriesTheAck(t *testing.T) {
	for _, delay := range []time.Duration{0, 20 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			p := newTestPair(t, 0, nil)
			time.Sleep(50 * time.Millisecond) // the handshake's last packets
			p.bursts.Store(true)
			sends := watchSends(p.serverEnd)
			p.serverH.onData = func(s *Stream, data []byte, fin bool) {
				if !fin {
					return
				}
				s.Conn().ExpectWrite()
				go func() {
					time.Sleep(delay)
					_ = s.Write(bytes.Repeat([]byte("a"), 500), true)
					s.Conn().WriteDone()
				}()
			}
			s, err := p.client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			// Three packets of request, which want an acknowledgement at once.
			if err := s.Write(make([]byte, 3000), true); err != nil {
				t.Fatal(err)
			}
			waitFinished(t, p.clientH, s.ID())
			sent := sends()
			small := 0
			for _, n := range sent {
				if n < 100 {
					small++
				}
			}
			if late := delay > flushHold; late != (small > 0) {
				t.Fatalf("answered after %v: the server sent %v", delay, sent)
			}
		})
	}
}

// Answers to a burst of requests written on goroutines of their own, with
// the connection told to expect them, leave packed together rather than in a
// packet each.
func TestExpectedWritesArePacked(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.MaxDatagramSize = 1350 })
	time.Sleep(50 * time.Millisecond)
	p.bursts.Store(true)
	const requests = 4
	var mu sync.Mutex
	var streams []*Stream
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if !fin {
			return
		}
		s.Conn().ExpectWrite()
		mu.Lock()
		streams = append(streams, s)
		ready := len(streams) == requests
		mu.Unlock()
		if !ready {
			return
		}
		// Every request of the burst has arrived: each is answered on a
		// goroutine of its own.
		for _, s := range streams {
			go func() {
				_ = s.Write(bytes.Repeat([]byte("a"), 600), true)
				s.Conn().WriteDone()
			}()
		}
	}
	sends := watchSends(p.serverEnd)
	var ids []uint64
	for i := 0; i < requests; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write([]byte("request"), true); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, s.ID())
	}
	for _, id := range ids {
		waitFinished(t, p.clientH, id)
	}
	if sent := sends(); len(sent) > requests/2 {
		t.Fatalf("%d answers of 600 bytes left in %v", requests, sent)
	}
}

// A write expected but not made, as by a handler that keeps its request,
// holds the connection's other writes back once, for flushHold, and not after.
func TestExpectedWriteThatDoesNotCome(t *testing.T) {
	p := newTestPair(t, 0, nil)
	time.Sleep(50 * time.Millisecond)
	sends := watchSends(p.clientEnd)
	p.client.ExpectWrite()
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := s.Write([]byte("x"), true); err != nil {
		t.Fatal(err)
	}
	for len(sends()) == 0 {
		if time.Since(start) > time.Second {
			t.Fatal("the write never left")
		}
		time.Sleep(100 * time.Microsecond)
	}
	if waited := time.Since(start); waited < flushHold/2 {
		t.Fatalf("the write left after %v, without waiting for the one expected", waited)
	}
	p.client.mu.Lock()
	awaited := p.client.awaited
	p.client.mu.Unlock()
	if awaited != 0 {
		t.Fatalf("%d writes still expected once they have held the rest back", awaited)
	}
	// Its WriteDone, when it comes at last, is harmless.
	p.client.WriteDone()
}

// serverConn waits for the server's side of the pair to be there.
func (p *testPair) serverConn(t *testing.T) *Conn {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		p.serverMu.Lock()
		s := p.server
		p.serverMu.Unlock()
		if s != nil {
			return s
		}
	}
	t.Fatal("no server connection")
	return nil
}

// A server lets its TLS connection go once the handshake is over, keeping
// the state it reports, and a client that sends it handshake data after that
// is told it was not expected.
func TestServerLetsTLSGo(t *testing.T) {
	p := newTestPair(t, 0, nil)
	time.Sleep(50 * time.Millisecond) // the handshake's last packets
	server := p.serverConn(t)
	server.mu.Lock()
	released := server.tls == nil
	server.mu.Unlock()
	if !released {
		t.Fatal("the server kept its TLS connection past the handshake")
	}
	if state := server.ConnectionState(); !state.HandshakeComplete || state.NegotiatedProtocol != "test" {
		t.Fatalf("the server reports %+v", state)
	}
	p.client.mu.Lock()
	p.client.spaces[spaceApp].cryptoSend.write([]byte{0x04, 0, 0, 0})
	p.client.wantFlush = true
	p.client.mu.Unlock()
	p.client.flush()
	select {
	case err := <-p.serverH.closed:
		var te *TransportError
		if !errors.As(err, &te) || te.Code != errCryptoBase+alertUnexpectedMessage {
			t.Fatalf("the server closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server took handshake data after the handshake")
	}
}

// A stream's final write ends the handler's calls for it and drops its
// Context, though the stream lives on until its data is acknowledged.
func TestWriteFinalReleasesStream(t *testing.T) {
	p := newTestPair(t, 0, nil)
	calls := make(chan *Stream, 16)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		calls <- s
		if s.Context == nil {
			s.Context = "request"
			_ = s.WriteFinal(bufferpool.Append(nil, []byte("answer")), false)
		}
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("first"), false); err != nil {
		t.Fatal(err)
	}
	server := <-calls
	waitFinished(t, p.clientH, s.ID())
	if got := string(p.clientH.received(s.ID())); got != "answer" {
		t.Fatalf("the client read %q", got)
	}
	// What arrives after the final write reaches no handler.
	if err := s.Write([]byte("second"), true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-calls:
		t.Fatal("the handler was called for a stream it was done with")
	case <-time.After(100 * time.Millisecond):
	}
	server.conn.mu.Lock()
	context, released := server.Context, server.released
	server.conn.mu.Unlock()
	if context != nil || !released {
		t.Fatalf("the stream kept its Context %v", context)
	}
}

// A connection that expects a write holds back what it has to send, but with
// nothing to send other than an acknowledgement that is not yet due it holds
// nothing: it sets no hold for the timer to end, and leaves the
// acknowledgement to its own deadline.
func TestHoldWaitsForSomethingToSend(t *testing.T) {
	p := newTestPair(t, 0, nil)
	time.Sleep(50 * time.Millisecond)
	type held struct {
		deadline, ackDeadline, timerAt time.Time
		ackNow                         bool
	}
	seen := make(chan held, 1)
	release := make(chan struct{})
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if !fin {
			return
		}
		c := s.Conn()
		c.ExpectWrite()
		go func() {
			// Once the handler call is over and the connection has
			// flushed, it waits for this write.
			time.Sleep(10 * time.Millisecond)
			c.mu.Lock()
			app := &c.spaces[spaceApp]
			seen <- held{c.flushDeadline, app.ackDeadline, c.timerAt, app.ackNow}
			c.mu.Unlock()
			<-release
			_ = s.WriteFinal(bufferpool.Append(nil, []byte("answer")), true)
		}()
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("request"), true); err != nil {
		t.Fatal(err)
	}
	h := <-seen
	close(release)
	waitFinished(t, p.clientH, s.ID())
	if h.ackNow {
		t.Skip("the request wanted an acknowledgement at once")
	}
	if !h.deadline.IsZero() {
		t.Fatal("a hold was set with nothing to hold back")
	}
	if h.ackDeadline.IsZero() || h.timerAt.IsZero() || h.timerAt.After(h.ackDeadline) {
		t.Fatalf("the timer is set for %v, after the acknowledgement's deadline %v", h.timerAt, h.ackDeadline)
	}
}

// batchSink is a BatchSender that keeps nothing.
type batchSink struct{ batches int }

func (b *batchSink) Send([]byte) error          { return nil }
func (b *batchSink) SendBatch(d [][]byte) error { b.batches++; return nil }
func (b *batchSink) Close() error               { return nil }

// A round of several datagrams is handed to a BatchSender without
// allocating the list of them.
func TestSendDatagramsAllocs(t *testing.T) {
	sink := &batchSink{}
	c := &Conn{pc: sink}
	buf := make([]byte, 3000)
	lens := []uint16{1000, 1000, 1000}
	if allocs := testing.AllocsPerRun(100, func() { c.sendDatagrams(buf, lens) }); allocs != 0 {
		t.Fatalf("a batched round allocated %v times", allocs)
	}
	if sink.batches == 0 {
		t.Fatal("the round was not sent as a batch")
	}
}
