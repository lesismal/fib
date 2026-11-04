package reqbody

import fib "github.com/lesismal/fib"

// Splicer is where a pushed body's bytes may come from without its producer
// writing them: a connection they are still arriving on, from which the
// kernel moves them to the transport's with splice(2); see
// fib.Connection.SpliceTo and Pusher.OfferSplice.
type Splicer interface {
	// SpliceTo starts moving the body's n bytes to conn, after whatever conn
	// has queued, and reports whether it did. written is told once they have
	// all reached conn's socket, or why they will not.
	SpliceTo(conn *fib.Connection, n int64, written func(error)) bool
	// Decline tells the source that the body will be written after all,
	// through the Pusher, as if it had never been offered.
	Decline()
}

// OfferSplice offers s, before the request is sent, as where the body's bytes
// come from: a transport that can take them so, an HTTP/1 connection without
// TLS on Linux, takes s with TakeSplice and, if s.SpliceTo starts, sends the
// body that way, and the producer writes nothing. A transport that takes the
// body any other way, through Attach or Read, declines s first, and so does
// one that takes s and cannot splice: the producer then writes the body as it
// would have. A request that ends before either drops s unheard.
func (p *Pusher) OfferSplice(s Splicer) {
	p.mu.Lock()
	p.splice = s
	p.mu.Unlock()
}

// TakeSplice takes the Splicer OfferSplice offered, or nil when there is none
// or it has been taken. The caller either splices the body through it or
// declines it.
func (p *Pusher) TakeSplice() Splicer {
	p.mu.Lock()
	s := p.splice
	p.splice = nil
	p.mu.Unlock()
	return s
}

// declineSplice declines an offered Splicer nothing has taken.
func (p *Pusher) declineSplice() {
	if s := p.TakeSplice(); s != nil {
		s.Decline()
	}
}
