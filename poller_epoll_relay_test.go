//go:build linux

package fib

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// relaySpin keeps the writers' spins from being optimized away.
var relaySpin atomic.Int64

// TestEpollRelayLosesNoWakeup drives epollWaiter through its arm, park and
// disarm cycle with events landing in every window of it: before the relay
// is armed, between arming and the look after it, after that look and
// before the runtime parks the loop, while it is parked, and as the relay is
// disarmed again. Each of a few pipes, registered edge-triggered as a
// connection's socket is, has a writer that writes a byte once the last one
// has been taken, after a delay drawn from a schedule that spans those
// windows. A byte that wait has not reported half a second after it was
// written is a lost wakeup. A watchdog then wakes the loop through a pipe of
// its own, and the loop tells the two ways it can be lost apart: the event
// waiting in the loop's instance, which the relay failed to report, or not
// there at all.
func TestEpollRelayLosesNoWakeup(t *testing.T) {
	// One pipe is a single connection's ping-pong, which leaves the loop
	// parking between every two events; more keep it busier.
	for _, pipes := range []int{1, 8} {
		t.Run(strconv.Itoa(pipes), func(t *testing.T) { testEpollRelay(t, pipes) })
	}
}

func testEpollRelay(t *testing.T, pipes int) {
	rounds := 24000 / pipes
	if testing.Short() {
		rounds /= 10
	}
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(epfd)
	w := newEpollWaiter(epfd)
	if w == nil {
		t.Skip("the relay cannot be set up here")
	}
	defer w.close()

	// fds[pipes] is the watchdog's.
	fds := make([][2]int, pipes+1)
	for i := range fds {
		if err := syscall.Pipe2(fds[i][:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
			t.Fatal(err)
		}
		defer syscall.Close(fds[i][0])
		defer syscall.Close(fds[i][1])
		ev := syscall.EpollEvent{Events: syscall.EPOLLIN | epollET, Fd: int32(i)}
		if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, fds[i][0], &ev); err != nil {
			t.Fatal(err)
		}
	}
	one := []byte{1}
	drain := func(fd int) bool {
		var buf [64]byte
		got := false
		for {
			n, err := syscall.Read(fd, buf[:])
			if n > 0 {
				got = true
				continue
			}
			if err == syscall.EINTR {
				continue
			}
			return got
		}
	}

	// written is when the byte a pipe's writer wrote last was written, or 0
	// once the loop has taken it.
	written := make([]atomic.Int64, pipes)
	acks := make([]chan struct{}, pipes)
	stop := make(chan struct{})
	// The writers hold raw descriptor numbers: they must be gone before the
	// defer closes those, or a write can land in a descriptor a later test has
	// been given, and its connection gets a byte nobody sent it. Waiting for
	// them is deferred after the closers, so it runs before them.
	var wg sync.WaitGroup
	defer wg.Wait()
	defer close(stop)
	for i := 0; i < pipes; i++ {
		acks[i] = make(chan struct{}, 1)
		acks[i] <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rnd := uint64(i + 1)
			for {
				select {
				case <-acks[i]:
				case <-stop:
					return
				}
				// Mostly spins of a random length up to some tens of
				// microseconds, which sweep the whole of wait, and now and
				// then a yield or a sleep, which leaves the loop parked.
				rnd = rnd*6364136223846793005 + 1442695040888963407
				switch r := rnd >> 33; r % 16 {
				case 0:
					runtime.Gosched()
				case 1:
					time.Sleep(time.Microsecond)
				case 2:
					time.Sleep(50 * time.Microsecond)
				default:
					spin := int64(0)
					for n := int64(0); n < int64(r>>4)%20000; n++ {
						spin += n
					}
					relaySpin.Store(spin)
				}
				written[i].Store(time.Now().UnixNano())
				for {
					if _, err := syscall.Write(fds[i][1], one); err != syscall.EINTR {
						break
					}
				}
			}
		}(i)
	}

	const lostAfter = 500 * time.Millisecond
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			for i := range written {
				if at := written[i].Load(); at != 0 && time.Since(time.Unix(0, at)) > lostAfter {
					_, _ = syscall.Write(fds[pipes][1], one)
					break
				}
			}
		}
	}()

	taken := make([]int, pipes)
	var lostInRelay, lostInInstance int
	events := make([]syscall.EpollEvent, 4)
	take := func(i int, late bool) {
		if !drain(fds[i][0]) {
			return
		}
		at := written[i].Swap(0)
		if late || at != 0 && time.Since(time.Unix(0, at)) > lostAfter {
			lostInRelay++
		}
		taken[i]++
		acks[i] <- struct{}{}
	}
	deadline := time.Now().Add(2 * time.Minute)
	for done := 0; done < pipes; {
		if time.Now().After(deadline) {
			t.Fatalf("still waiting after two minutes; taken %v", taken)
		}
		n, err := w.wait(epfd, events)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		rescued := false
		for _, ev := range events[:n] {
			if i := int(ev.Fd); i == pipes {
				drain(fds[pipes][0])
				rescued = true
			} else {
				take(i, false)
			}
		}
		if rescued {
			// The watchdog woke the loop for a byte the relay did not
			// report. One the loop's instance holds is reported now, by a
			// look that does not park; one it does not hold is only in its
			// pipe.
			n, _ := pollEvents(epfd, events)
			for _, ev := range events[:n] {
				if i := int(ev.Fd); i < pipes {
					take(i, true)
				}
			}
			for i := range written {
				if at := written[i].Load(); at != 0 && time.Since(time.Unix(0, at)) > lostAfter && drain(fds[i][0]) {
					written[i].Store(0)
					lostInInstance++
					taken[i]++
					acks[i] <- struct{}{}
				}
			}
		}
		done = 0
		for _, n := range taken {
			if n >= rounds {
				done++
			}
		}
	}
	if lostInRelay != 0 || lostInInstance != 0 {
		t.Fatalf("%d wakeups lost in the relay and %d in the loop's instance, over %d events",
			lostInRelay, lostInInstance, pipes*rounds)
	}
}

// stopRelayWriters tells a relay test's writers to stop and waits for them to
// return. They have to be gone before the descriptors they write to are
// closed: a write to one that was closed under it, and since given to a
// socket, would put its byte on that socket's stream.
func stopRelayWriters(stop chan struct{}, writers *sync.WaitGroup) {
	close(stop)
	writers.Wait()
}

// TestRelayWritersStoppedBeforeTheirPipeCloses pins the teardown rule of
// TestEpollRelayLosesNoWakeup: its loop acks a writer for a byte it then
// stops taking, so a writer is still to write when the test ends, and it must
// be waited for before the pipes close. The kernel hands out the lowest free
// descriptors, so the pipe's numbers come back for the sockets of the test
// that follows, as they did for the echo test whose peer read an 0x01 ahead
// of its echo.
func TestRelayWritersStoppedBeforeTheirPipeCloses(t *testing.T) {
	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	var writers sync.WaitGroup
	var wrote atomic.Bool
	writers.Add(1)
	go func() {
		defer writers.Done()
		// A writer whose loop has taken its last ack has nothing left to
		// look at stop for: it writes after its spin. A sleep stands in for
		// its being descheduled there while the test ends.
		time.Sleep(2 * time.Millisecond)
		wrote.Store(true)
		for {
			if _, err := syscall.Write(p[1], []byte{1}); err != syscall.EINTR {
				break
			}
		}
	}()
	stopRelayWriters(make(chan struct{}), &writers)
	for _, fd := range p {
		if err := syscall.Close(fd); err != nil {
			t.Fatal(err)
		}
	}
	// The pair takes the descriptors the pipe had, as the next test's
	// sockets do.
	pair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(pair[1])
	defer syscall.Close(pair[0])
	if pair[0] != p[0] || pair[1] != p[1] {
		t.Fatalf("the socket pair took descriptors %v, not the pipe's %v", pair, p)
	}
	// A writer that was not waited for writes its byte in the pair's second
	// end once it wakes; one that was waited for wrote it to the pipe, which
	// the close above took with it.
	if err := syscall.SetNonblock(pair[0], true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(200 * time.Millisecond)
	var b [1]byte
	for {
		switch n, err := syscall.Read(pair[0], b[:]); {
		case n > 0:
			t.Fatalf("a stray 0x%02x reached the descriptor the pipe's number was reused for", b[0])
		case err != nil && err != syscall.EAGAIN && err != syscall.EINTR:
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !wrote.Load() {
		t.Fatal("the writer never wrote")
	}
}
