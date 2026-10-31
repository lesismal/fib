package stun

import (
	"errors"
	"testing"
	"time"
)

func TestTransactionSchedule(t *testing.T) {
	var m Message
	m.Build(BindingRequest, NewTransactionID())
	start := time.Unix(1000, 0)
	for _, tc := range []struct {
		name  string
		cfg   TransactionConfig
		sends []time.Duration // when each retransmission is due
		fail  time.Duration
	}{
		{"RFC 8489", TransactionConfig{}, []time.Duration{500, 1500, 3500, 7500, 15500, 31500}, 39500},
		{"capped", TransactionConfig{RTO: 100 * time.Millisecond, MaxRTO: 400 * time.Millisecond, MaxSends: 5, LastWait: 4},
			[]time.Duration{100, 300, 700, 1100}, 1500},
		{"one send", TransactionConfig{MaxSends: 1, LastWait: 2}, nil, 1000},
		{"reliable", TransactionConfig{Reliable: true}, nil, 39500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tx Transaction
			tx.Start(m.Raw, start, tc.cfg)
			if tx.ID != m.TransactionID || string(tx.Request) != string(m.Raw) {
				t.Fatal("Start did not keep the request")
			}
			for i, at := range tc.sends {
				due, ok := tx.NextTimeout()
				if !ok || due != start.Add(at*time.Millisecond) {
					t.Fatalf("retransmission %d due %v, want %v", i, due.Sub(start), at*time.Millisecond)
				}
				if b, err := tx.HandleTimeout(due.Add(-time.Nanosecond)); b != nil || err != nil {
					t.Fatal("fired early")
				}
				if b, err := tx.HandleTimeout(due); b == nil || err != nil {
					t.Fatalf("retransmission %d: %v", i, err)
				}
			}
			due, ok := tx.NextTimeout()
			if !ok || due != start.Add(tc.fail*time.Millisecond) {
				t.Fatalf("gives up at %v, want %v", due.Sub(start), tc.fail*time.Millisecond)
			}
			if _, err := tx.HandleTimeout(due); !errors.Is(err, ErrTimeout) {
				t.Fatalf("HandleTimeout = %v", err)
			}
			if _, ok := tx.NextTimeout(); ok || !tx.Done() || tx.Sends() != len(tc.sends)+1 {
				t.Fatalf("after timeout: pending %v done %v sends %d", ok, tx.Done(), tx.Sends())
			}
		})
	}
}

func TestTransactionFinish(t *testing.T) {
	var m Message
	m.Build(BindingRequest, NewTransactionID())
	var tx Transaction
	now := time.Unix(0, 0)
	tx.Start(m.Raw, now, TransactionConfig{})
	tx.Finish()
	if _, ok := tx.NextTimeout(); ok {
		t.Fatal("finished transaction still has a timeout")
	}
	if b, err := tx.HandleTimeout(now.Add(time.Hour)); b != nil || err != nil {
		t.Fatal("finished transaction fired")
	}
	tx.Start(m.Raw, now, TransactionConfig{})
	if _, ok := tx.NextTimeout(); !ok {
		t.Fatal("restarted transaction has no timeout")
	}
}
