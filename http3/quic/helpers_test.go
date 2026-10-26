package quic

import (
	"testing"
	"time"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitForWithin(t, 5*time.Second, what, cond)
}

// waitForWithin is waitFor with its own deadline, for a state that needs a
// send round or a callback the runner may starve: the Windows runners under
// -race can take seconds to get back to a test, and a fixed five-second guess
// is a property of the runner rather than of the code under test.
func waitForWithin(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("%s: timed out", what)
}
