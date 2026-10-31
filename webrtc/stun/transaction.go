package stun

import (
	"errors"
	"time"
)

// ErrTimeout is what a Transaction reports when its request went unanswered.
var ErrTimeout = errors.New("stun: transaction timed out")

// The defaults of RFC 8489 section 6.2.1.
const (
	DefaultRTO      = 500 * time.Millisecond
	DefaultMaxSends = 7
	DefaultLastWait = 16
	reliableTimeout = 39500 * time.Millisecond
)

// TransactionConfig sets how a Transaction retransmits. The zero value is
// RFC 8489's: an RTO of 500ms that doubles after each send, 7 sends (Rc),
// and a final wait of 16 RTOs (Rm), so that a request is given up 39.5s
// after it was first sent.
type TransactionConfig struct {
	// RTO is the wait before the first retransmission.
	RTO time.Duration
	// MaxRTO, if positive, caps the RTO as it doubles. RFC 8489 lets a
	// client do so; ICE does, to keep checking a pair at a steady pace.
	MaxRTO time.Duration
	// MaxSends is the number of times the request is sent, Rc.
	MaxSends int
	// LastWait is how many initial RTOs to wait after the last send, Rm.
	LastWait int
	// Reliable is set over TCP or TLS, where the request is sent once and
	// the transaction waits 39.5s for its response.
	Reliable bool
}

// Transaction is the client side of one STUN transaction over an unreliable
// transport: it keeps the request and says when to send it again. It does no
// I/O; its owner sends the request, feeds the clock through HandleTimeout,
// and calls Finish when a response with the transaction's ID arrives.
type Transaction struct {
	// ID is the request's transaction ID, which its response echoes.
	ID [TransactionIDSize]byte
	// Request is the encoded request, a copy the Transaction owns.
	Request []byte

	cfg      TransactionConfig
	rto      time.Duration
	sends    int
	deadline time.Time
	done     bool
}

// Start begins the transaction for the encoded request req at now, copying
// req, which the caller sends once itself. It reuses the memory of an
// earlier transaction, so that a Transaction may be kept and started again.
func (t *Transaction) Start(req []byte, now time.Time, cfg TransactionConfig) {
	if cfg.RTO <= 0 {
		cfg.RTO = DefaultRTO
	}
	if cfg.MaxSends <= 0 {
		cfg.MaxSends = DefaultMaxSends
	}
	if cfg.LastWait <= 0 {
		cfg.LastWait = DefaultLastWait
	}
	t.Request = append(t.Request[:0], req...)
	copy(t.ID[:], req[8:HeaderSize])
	t.cfg = cfg
	t.rto = cfg.RTO
	t.sends = 1
	t.done = false
	if cfg.Reliable {
		t.deadline = now.Add(reliableTimeout)
	} else if t.sends == cfg.MaxSends {
		t.deadline = now.Add(time.Duration(cfg.LastWait) * cfg.RTO)
	} else {
		t.deadline = now.Add(t.rto)
	}
}

// NextTimeout is when HandleTimeout must next be called, if the transaction
// is still waiting for its response.
func (t *Transaction) NextTimeout() (time.Time, bool) {
	return t.deadline, !t.done && !t.deadline.IsZero()
}

// HandleTimeout advances the transaction to now. It returns the request when
// it is time to send it again, and ErrTimeout once the transaction gives up,
// after which it is done.
func (t *Transaction) HandleTimeout(now time.Time) ([]byte, error) {
	if t.done || t.deadline.IsZero() || now.Before(t.deadline) {
		return nil, nil
	}
	if t.cfg.Reliable || t.sends >= t.cfg.MaxSends {
		t.done = true
		return nil, ErrTimeout
	}
	t.sends++
	t.rto *= 2
	if t.cfg.MaxRTO > 0 && t.rto > t.cfg.MaxRTO {
		t.rto = t.cfg.MaxRTO
	}
	if t.sends == t.cfg.MaxSends {
		t.deadline = t.deadline.Add(time.Duration(t.cfg.LastWait) * t.cfg.RTO)
	} else {
		t.deadline = t.deadline.Add(t.rto)
	}
	return t.Request, nil
}

// Finish ends the transaction, when its response has arrived or its owner no
// longer wants one.
func (t *Transaction) Finish() { t.done = true }

// Done reports whether the transaction has ended.
func (t *Transaction) Done() bool { return t.done }

// Sends is how many times the request has been sent so far.
func (t *Transaction) Sends() int { return t.sends }
