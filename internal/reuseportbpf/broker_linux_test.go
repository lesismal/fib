//go:build linux && (amd64 || arm64)

package reuseportbpf

import (
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// child is a stand-in for one prefork child: its end of a pair from b, as
// a child gets it at balanceFD, kept open until the test ends.
func child(t *testing.T, b *Broker) int {
	t.Helper()
	f, err := b.Pair()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return int(f.Fd())
}

// fakeBroker is a broker whose groups are pipes rather than maps and
// programs, so that the protocol runs without CAP_BPF; made counts the groups
// it made, and refuse, when set, is what making one fails with.
func fakeBroker(t *testing.T, refuse error) (*Broker, *atomic.Int32) {
	t.Helper()
	var made atomic.Int32
	b := NewBroker()
	b.create = func(size int) (*Group, error) {
		made.Add(1)
		if refuse != nil {
			return nil, refuse
		}
		// The broker owns the group, and closes it with itself.
		var p [2]int
		if err := syscall.Pipe(p[:]); err != nil {
			return nil, err
		}
		third, err := syscall.Dup(p[0])
		if err != nil {
			return nil, err
		}
		return &Group{Sockarray: p[0], Counter: p[1], Prog: third}, nil
	}
	t.Cleanup(func() { b.Close() })
	return b, &made
}

// Every child asking for an address's group gets the descriptors of the one
// group the broker made for it, the first time any asked; another address
// gets a group of its own, and asking for an address with another size than
// it was made with is an error, not a fallback.
func TestBrokerHandsEachChildTheSameGroup(t *testing.T) {
	b, made := fakeBroker(t, nil)
	one, two := child(t, b), child(t, b)

	g1, err := Join(one, "127-0-0-1_8080", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer g1.Close()
	g2, err := Join(two, "127-0-0-1_8080", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()
	if made.Load() != 1 {
		t.Fatalf("made %d groups for one address", made.Load())
	}
	b.mu.Lock()
	kept := *b.groups["127-0-0-1_8080"].g
	b.mu.Unlock()
	for _, g := range []*Group{g1, g2} {
		if fdInode(t, g.Sockarray) != fdInode(t, kept.Sockarray) || fdInode(t, g.Prog) != fdInode(t, kept.Prog) {
			t.Fatalf("a child got %+v, not the broker's group %+v", *g, kept)
		}
		if g.Counter != -1 {
			t.Errorf("a child was handed the counter: %+v", *g)
		}
	}

	g3, err := Join(one, "127-0-0-1_8443", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer g3.Close()
	if made.Load() != 2 || fdInode(t, g3.Sockarray) == fdInode(t, g1.Sockarray) {
		t.Fatalf("another address shares a group: made %d", made.Load())
	}

	if g, err := Join(two, "127-0-0-1_8080", 3); err == nil || errors.Is(err, ErrUnsupported) {
		g.Close()
		t.Fatalf("Join with another size = %v, want a mismatch error", err)
	}
}

// A group the broker could not make, as without CAP_BPF, is reported to
// every child as ErrUnsupported, and not tried again.
func TestBrokerRemembersUnsupported(t *testing.T) {
	b, made := fakeBroker(t, ErrUnsupported)
	one, two := child(t, b), child(t, b)
	for _, conn := range []int{one, two} {
		if g, err := Join(conn, "k", 2); !errors.Is(err, ErrUnsupported) {
			g.Close()
			t.Fatalf("Join = %v, want ErrUnsupported", err)
		}
	}
	if made.Load() != 1 {
		t.Fatalf("tried to make the group %d times", made.Load())
	}
}

// Another failure to make the group reaches the child as it is.
func TestBrokerReportsFailures(t *testing.T) {
	b, _ := fakeBroker(t, errors.New("no map today"))
	if g, err := Join(child(t, b), "k", 2); err == nil || errors.Is(err, ErrUnsupported) || err.Error() != "no map today" {
		g.Close()
		t.Fatalf("Join = %v", err)
	}
}

// A request the broker cannot read is answered with an error, and the
// broker goes on answering.
func TestBrokerAnswersMalformedRequests(t *testing.T) {
	b, _ := fakeBroker(t, nil)
	for _, req := range []string{"", "4", "x k", "1 k", "4 "} {
		if _, err := b.answer(req); err == nil {
			t.Errorf("answer(%q) succeeded", req)
		}
	}
	conn := child(t, b)
	if err := syscall.Sendmsg(conn, []byte("nonsense"), nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, maxRequest)
	n, _, _, _, err := syscall.Recvmsg(conn, buf, nil, 0)
	if err != nil || n == 0 || buf[0] != statusFailed {
		t.Fatalf("answer to nonsense: %q %v", buf[:n], err)
	}
	g, err := Join(conn, "k", 2)
	if err != nil {
		t.Fatalf("Join after a malformed request: %v", err)
	}
	g.Close()
}

// The broker stops serving a child once the child's end is closed, as it is
// when the child exits.
func TestBrokerStopsWhenTheChildCloses(t *testing.T) {
	b, _ := fakeBroker(t, nil)
	f, err := b.Pair()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the broker still serves a closed pair")
	}
}

// A child whose master is gone hears so, rather than waiting.
func TestJoinWithoutAMaster(t *testing.T) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fds[1])
	syscall.Close(fds[0])
	if g, err := Join(fds[1], "k", 2); err == nil {
		g.Close()
		t.Fatal("Join with no master succeeded")
	}
}

// A closed broker makes no more groups, and says so to a child that asks.
func TestClosedBrokerRefuses(t *testing.T) {
	b, made := fakeBroker(t, nil)
	conn := child(t, b)
	b.Close()
	if g, err := Join(conn, "k", 2); err == nil {
		g.Close()
		t.Fatal("a closed broker handed out a group")
	}
	if made.Load() != 0 {
		t.Fatalf("a closed broker made %d groups", made.Load())
	}
}

// A nil broker, which NewBroker never returns here but does where there is
// no selector, has no pair to give and nothing to close.
func TestNilBroker(t *testing.T) {
	var b *Broker
	if f, err := b.Pair(); f != nil || err != ErrUnsupported {
		t.Fatalf("Pair = %v, %v", f, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

// Two children, each with two of a group's four sockets, join the group a
// real broker makes, and the selector hands their connections out in turn
// across all four. It needs CAP_BPF, and skips without it.
func TestJoinGroupRoundRobin(t *testing.T) {
	const sockets, conns = 4, 40
	fds, addr := reuseGroup(t, sockets)
	b := NewBroker()
	defer b.Close()
	one, two := child(t, b), child(t, b)

	g1, err := JoinGroup(one, "test", sockets, 0, fds[:2])
	if errors.Is(err, ErrUnsupported) {
		t.Skipf("reuseport balancing unsupported here (need CAP_BPF): %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer g1.Close()
	g2, err := JoinGroup(two, "test", sockets, 2, fds[2:])
	if err != nil {
		t.Fatal(err)
	}
	defer g2.Close()

	var dials []net.Conn
	for i := 0; i < conns; i++ {
		c, err := net.DialTimeout("tcp", addr.String(), time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		dials = append(dials, c)
	}
	time.Sleep(100 * time.Millisecond)
	counts := make([]int, sockets)
	total := 0
	for i, fd := range fds {
		counts[i] = drain(t, fd)
		total += counts[i]
	}
	for _, c := range dials {
		c.Close()
	}
	if total != conns {
		t.Fatalf("accepted %d of %d: %v", total, conns, counts)
	}
	for _, c := range counts {
		if c < conns/sockets-3 || c > conns/sockets+3 {
			t.Fatalf("uneven distribution across the brokered group: %v", counts)
		}
	}
}
