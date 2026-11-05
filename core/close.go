package core

import "time"

// closeRoundWait is how long Close waits for the rounds still running when it
// is called before it returns without them. Their connections are cut by
// then, so a round that reads or writes ends at once; only one whose handler
// is held up by something else, a database or a sleep, outlasts the wait.
const closeRoundWait = 100 * time.Millisecond

// afterRounds runs release, what Close cannot do while a round is still
// running, once wait reports that none is: at once, returning what release
// does, if they end within closeRoundWait, and otherwise on a goroutine of
// its own when they do, returning nil now. A handler cannot be interrupted,
// so this is how Close keeps one that does not return from holding it up, as
// net/http.Server.Close does not wait for its handlers either.
func afterRounds(wait func(), release func() error) error {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	timer := time.NewTimer(closeRoundWait)
	defer timer.Stop()
	select {
	case <-done:
		return release()
	case <-timer.C:
		go func() {
			<-done
			_ = release()
		}()
		return nil
	}
}
