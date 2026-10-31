package quic

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

// cidRecorder is a server handler that keeps the connection IDs it is told
// of, as a server routing them does.
type cidRecorder struct {
	*testHandler
	mu     sync.Mutex
	active map[string]bool
	calls  int
}

func (r *cidRecorder) OnConnectionID(c *Conn, cid []byte, active bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if active {
		if !c.Owns(cid) {
			panic("told of a connection ID the connection does not own")
		}
		r.active[string(cid)] = true
	} else {
		delete(r.active, string(cid))
	}
}

func (r *cidRecorder) has(cid []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active[string(cid)]
}

// A server issues connection IDs once the handshake is over, as many as the
// client allows, telling its handler of each before the client has it; the
// client may send to any of them, and one it retires is replaced.
func TestConnectionIDsIssuedAndRetired(t *testing.T) {
	var rec *cidRecorder
	p := newTestPairOpts(t, 0, nil, pairOpts{wrapServer: func(h *testHandler) Handler {
		rec = &cidRecorder{testHandler: h, active: make(map[string]bool)}
		return rec
	}})
	server := p.serverConn(t)
	// The client allows two, the handshake's among them.
	var issued []byte
	waitFor(t, "NEW_CONNECTION_ID", func() bool {
		p.client.mu.Lock()
		defer p.client.mu.Unlock()
		if pc, ok := p.client.peerCIDs[1]; ok {
			issued = pc.cid
			return true
		}
		return false
	})
	if !server.Owns(issued) || !rec.has(issued) || len(server.LocalCIDs()) != 2 {
		t.Fatalf("issued %x: owned %v, routed %v, %d owned", issued, server.Owns(issued), rec.has(issued), len(server.LocalCIDs()))
	}
	// The client moves to the new one and retires the first.
	p.client.mu.Lock()
	p.client.dcid, p.client.dcidSeq = issued, 1
	// As a client that retires it does, it forgets the first, or the
	// replacement would be one more than it allows.
	delete(p.client.peerCIDs, 0)
	p.client.mu.Unlock()
	first := server.LocalCID()
	sendRaw(p, frame(frameRetireConnectionID, 0))
	// One look at what the server owns for all of it: separate looks can
	// see two before the retirement and the first gone before the
	// replacement, which leaves no replacement to look for.
	var replacement []byte
	waitFor(t, "the first retired and replaced", func() bool {
		cids := server.LocalCIDs()
		if len(cids) != 2 {
			return false
		}
		kept := false
		replacement = nil
		for _, cid := range cids {
			switch {
			case bytes.Equal(cid, first):
				return false
			case bytes.Equal(cid, issued):
				kept = true
			default:
				replacement = cid
			}
		}
		return kept && replacement != nil
	})
	waitFor(t, "the replacement at the client", func() bool {
		p.client.mu.Lock()
		defer p.client.mu.Unlock()
		pc, ok := p.client.peerCIDs[2]
		return ok && bytes.Equal(pc.cid, replacement)
	})
	if !rec.has(replacement) || rec.has(first) {
		t.Fatal("the handler was not told of the change")
	}
	// The connection still works on the connection ID the client moved to.
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("moved"), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); string(got) != "moved" {
		t.Fatalf("echo %q", got)
	}
}

// RETIRE_CONNECTION_ID for one never issued, or for the one the packet that
// carries it was sent to, is a PROTOCOL_VIOLATION (RFC 9000 section 19.16).
func TestRetireConnectionIDErrors(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.handleFrames(spaceApp, frame(frameRetireConnectionID, 5), time.Now())
	var te *TransportError
	if !errors.As(err, &te) || te.Code != errProtocolViolation {
		t.Fatalf("unissued: %v", err)
	}
	c.rxDCID = c.scid
	_, err = c.handleFrames(spaceApp, frame(frameRetireConnectionID, 0), time.Now())
	if !errors.As(err, &te) || te.Code != errProtocolViolation {
		t.Fatalf("its own: %v", err)
	}
	// Lost NEW_CONNECTION_ID goes again while it is in force.
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: randomCID(), token: make([]byte, 16)})
	c.nextCIDSeq = 2
	c.localCIDs[1].pending, c.cidsPending = true, true
	sp := &sentPacket{}
	if b := c.appendNewCIDs(nil, 100, sp); len(b) != 1+1+1+1+cidLen+16 || c.cidsPending {
		t.Fatalf("NEW_CONNECTION_ID %x", b)
	}
	c.frameLost(spaceApp, &sp.frames[0])
	if !c.cidsPending {
		t.Fatal("lost NEW_CONNECTION_ID not sent again")
	}
}

// ActiveConnectionIDs set to one issues none beyond the handshake's.
func TestNoConnectionIDsIssued(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.ActiveConnectionIDs = 1 })
	server := p.serverConn(t)
	time.Sleep(50 * time.Millisecond)
	if n := len(server.LocalCIDs()); n != 1 {
		t.Fatalf("%d connection IDs", n)
	}
}
