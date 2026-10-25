package reqbody

import (
	stdhttp "net/http"
	"strings"
	"sync"
	"time"
)

// ExpectsContinue reports whether req asks the server, with
// "Expect: 100-continue", to say it wants the body before the body is sent,
// and has a body to hold back. hasBody says whether it has one.
func ExpectsContinue(req *stdhttp.Request, hasBody bool) bool {
	if !hasBody || !req.ProtoAtLeast(1, 1) && req.ProtoMajor != 0 {
		return false
	}
	for _, value := range req.Header["Expect"] {
		for item := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(item), "100-continue") {
				return true
			}
		}
	}
	return false
}

// Gate holds back the body of a request that expects 100-continue until the
// server says it wants it, with a 100 (Continue) response, or until a wait
// runs out, after which the client sends it anyway, as RFC 9110 section
// 10.1.1 allows: a server that does not know the expectation never answers
// it. A final response that comes first shuts the gate, so the body is never
// sent: the server has answered without it.
//
// The methods may be called from any goroutine, and on a nil Gate, which is
// what a request without the expectation has, do nothing.
type Gate struct {
	mu    sync.Mutex
	state gateState
	timer *time.Timer
	open  func()
}

type gateState uint8

const (
	gateWaiting gateState = iota
	gateOpen
	gateShut
)

// NewGate returns a gate that runs open, once, when the server asks for the
// body or after the wait Start sets, whichever comes first, unless Shut
// comes before both. open runs on the goroutine that calls Open, or on a
// timer's. The gate is made before the request's header is sent, so that a
// 100 that answers the header finds it, and started once the header is on
// its way, so that a short wait cannot send the body ahead of it.
func NewGate(open func()) *Gate {
	return &Gate{open: open}
}

// Start begins the wait, after which the body goes whatever the server has
// said, unless the gate has opened or shut already.
func (g *Gate) Start(wait time.Duration) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == gateWaiting {
		g.timer = time.AfterFunc(wait, g.Open)
	}
}

// stopLocked stops the wait, if it has begun.
func (g *Gate) stopLocked() {
	if g.timer != nil {
		g.timer.Stop()
	}
}

// Open lets the body go, which a 100 (Continue) response does, unless the
// gate has opened or shut already.
func (g *Gate) Open() {
	if g == nil {
		return
	}
	g.mu.Lock()
	if g.state != gateWaiting {
		g.mu.Unlock()
		return
	}
	g.state = gateOpen
	g.stopLocked()
	open := g.open
	g.open = nil
	g.mu.Unlock()
	open()
}

// Shut keeps the body from going, which a final response does, and reports
// whether it was held back: true when the gate had not opened, so the body
// has not been sent, nor any of it.
func (g *Gate) Shut() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	switch g.state {
	case gateWaiting:
		g.state = gateShut
		g.stopLocked()
		g.open = nil
		return true
	case gateShut:
		return true
	}
	return false
}
