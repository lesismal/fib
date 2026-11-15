//go:build linux && (amd64 || arm64)

package reuseportbpf

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// A Broker is a prefork master's side of the children's groups: it makes an
// address's group the first time a child asks for it, keeps it for as long
// as the master runs, and hands its descriptors to every child that asks.
// Making the maps and loading the program is what needs CAP_BPF; a group
// that could not be made is remembered, and every child asking for it is
// told so.
//
// A child asks over its end of a SOCK_SEQPACKET socket pair, one per child,
// which the master hands it at start (see Pair): a request is the group's
// size and the address's key, and the answer is a status byte, with the
// sockarray's and the program's descriptors passed with it (SCM_RIGHTS)
// when the group is there. The child registers its own sockets in the
// sockarray at its places, and attaches the program to one of them.
type Broker struct {
	// mu guards groups and closed, and is held from finding a group until
	// its descriptors are sent, so that Close cannot close them in between.
	mu     sync.Mutex
	groups map[string]*brokered
	closed bool
	// create makes a group of a size: NewGroup, or a test's stand-in.
	create func(size int) (*Group, error)
	wg     sync.WaitGroup
}

// brokered is one address's group, or why it could not be made.
type brokered struct {
	size int
	g    *Group
	err  error
}

// NewBroker returns a Broker that has made no group yet.
func NewBroker() *Broker {
	return &Broker{groups: make(map[string]*brokered), create: NewGroup}
}

// Pair makes the socket pair for one child and returns the child's end, to
// hand it in exec.Cmd.ExtraFiles and close once the child has started. The
// broker answers on its own end until the child closes its end, by exiting.
func (b *Broker) Pair() (*os.File, error) {
	if b == nil {
		return nil, ErrUnsupported
	}
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_SEQPACKET|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.serve(fds[0])
		syscall.Close(fds[0])
	}()
	return os.NewFile(uintptr(fds[1]), "fib-prefork-balance"), nil
}

// Close drops the groups the broker made. The children's reuseport groups
// keep what they took of them; once every process holding a descriptor has
// exited the kernel frees the rest.
func (b *Broker) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	for key, e := range b.groups {
		e.g.Close()
		delete(b.groups, key)
	}
	return nil
}

// Answer statuses.
const (
	statusOK          = 0
	statusUnsupported = 1
	statusFailed      = 2
)

// maxRequest bounds a request: a size and an address's key.
const maxRequest = 512

// serve answers the requests that arrive on fd, a broker's end of a pair,
// until the child's end is closed.
func (b *Broker) serve(fd int) {
	buf := make([]byte, maxRequest)
	for {
		n, _, _, _, err := syscall.Recvmsg(fd, buf, nil, 0)
		if err == syscall.EINTR {
			continue
		}
		if err != nil || n == 0 {
			return
		}
		b.mu.Lock()
		g, err := b.answer(string(buf[:n]))
		err = b.reply(fd, g, err)
		b.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// answer finds or makes the group a request names; b.mu is held.
func (b *Broker) answer(request string) (*Group, error) {
	sizeText, key, ok := strings.Cut(request, " ")
	size, err := strconv.Atoi(sizeText)
	if !ok || err != nil || size < 2 || key == "" {
		return nil, fmt.Errorf("reuseportbpf: malformed request %q", request)
	}
	if b.closed {
		return nil, errors.New("reuseportbpf: the broker is closed")
	}
	if e := b.groups[key]; e != nil {
		if e.size != size {
			return nil, fmt.Errorf("reuseportbpf: %s asked for with %d sockets, made with %d", key, size, e.size)
		}
		return e.g, e.err
	}
	g, err := b.create(size)
	b.groups[key] = &brokered{size: size, g: g, err: err}
	return g, err
}

// reply sends the answer to one request: g's descriptors, or why there are
// none; b.mu is held.
func (b *Broker) reply(fd int, g *Group, err error) error {
	switch {
	case err == nil:
		rights := syscall.UnixRights(g.Sockarray, g.Prog)
		return syscall.Sendmsg(fd, []byte{statusOK}, rights, nil, 0)
	case errors.Is(err, ErrUnsupported):
		return syscall.Sendmsg(fd, append([]byte{statusUnsupported}, err.Error()...), nil, nil, 0)
	default:
		return syscall.Sendmsg(fd, append([]byte{statusFailed}, err.Error()...), nil, nil, 0)
	}
}

// joinMu keeps a child's requests one at a time, so that every answer goes to
// the request it answers, whichever engine of the child asks.
var joinMu sync.Mutex

// joinTimeout bounds the wait for the master's answer.
const joinTimeout = 5 * time.Second

// Join asks the broker on the other end of conn, the child's end of its
// pair, for the group of the address key, of size sockets in all, and
// returns the sockarray's and the program's descriptors.
func Join(conn int, key string, size int) (*Group, error) {
	joinMu.Lock()
	defer joinMu.Unlock()
	tv := syscall.NsecToTimeval(joinTimeout.Nanoseconds())
	_ = syscall.SetsockoptTimeval(conn, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)
	if err := syscall.Sendmsg(conn, []byte(strconv.Itoa(size)+" "+key), nil, nil, 0); err != nil {
		return nil, fmt.Errorf("reuseportbpf: ask the master: %w", err)
	}
	buf := make([]byte, maxRequest)
	oob := make([]byte, syscall.CmsgSpace(2*4))
	var n, oobn int
	var err error
	for {
		n, oobn, _, _, err = syscall.Recvmsg(conn, buf, oob, syscall.MSG_CMSG_CLOEXEC)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("reuseportbpf: hear from the master: %w", err)
	}
	if n == 0 {
		return nil, errors.New("reuseportbpf: the master closed the pair")
	}
	fds := receivedFDs(oob[:oobn])
	switch buf[0] {
	case statusOK:
		if len(fds) != 2 {
			closeAll(fds)
			return nil, fmt.Errorf("reuseportbpf: the master passed %d descriptors, want 2", len(fds))
		}
		return &Group{Sockarray: fds[0], Counter: -1, Prog: fds[1]}, nil
	case statusUnsupported:
		closeAll(fds)
		if why := string(buf[1:n]); why != ErrUnsupported.Error() {
			return nil, fmt.Errorf("%w: %s", ErrUnsupported, why)
		}
		return nil, ErrUnsupported
	default:
		closeAll(fds)
		return nil, errors.New(string(buf[1:n]))
	}
}

// receivedFDs are the descriptors passed in a message's control data.
func receivedFDs(oob []byte) []int {
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return nil
	}
	var fds []int
	for i := range msgs {
		if got, err := syscall.ParseUnixRights(&msgs[i]); err == nil {
			fds = append(fds, got...)
		}
	}
	return fds
}

func closeAll(fds []int) {
	for _, fd := range fds {
		syscall.Close(fd)
	}
}

// JoinGroup round-robins a group of size sockets that spans a prefork
// master's children: it asks the master's broker over conn for the group of
// the address key, registers this child's sockets fds at base and on, and
// attaches the program to the first of them.
func JoinGroup(conn int, key string, size, base int, fds []int) (*Group, error) {
	if size < 2 || len(fds) == 0 {
		return nil, ErrUnsupported
	}
	g, err := Join(conn, key, size)
	if err != nil {
		return nil, err
	}
	if err := register(g, base, fds); err != nil {
		g.Close()
		return nil, err
	}
	return g, nil
}
