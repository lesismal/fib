//go:build windows

package sys

import (
	"errors"
	"syscall"
	"unsafe"
)

// Winsock and NT values the syscall package does not export.
const (
	WSAEWOULDBLOCK = syscall.Errno(10035)
	// SoExclusiveAddrUse is SO_EXCLUSIVEADDRUSE, which winsock2.h defines
	// as ~SO_REUSEADDR.
	SoExclusiveAddrUse = ^syscall.SO_REUSEADDR
	WSAEINVAL          = syscall.Errno(10022)
	WSAEMSGSIZE        = syscall.Errno(10040)
	WSAECONNRESET      = syscall.Errno(10054)
	// sioUDPConnReset is SIO_UDP_CONNRESET, which stops an ICMP port
	// unreachable from failing a UDP socket's next receive.
	SioUDPConnReset = 0x9800000C
	SoError         = 0x1007
	MsgOOB          = 0x1
	fionbio         = 0x8004667e
)

// These three are KnownDLLs, which Windows always loads from the system
// directory, so resolving them by name cannot pick up a planted copy.
var (
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")
	modws2_32   = syscall.NewLazyDLL("ws2_32.dll")
	modntdll    = syscall.NewLazyDLL("ntdll.dll")

	procGetQueuedCompletionStatusEx = modkernel32.NewProc("GetQueuedCompletionStatusEx")
	procIoctlsocket                 = modws2_32.NewProc("ioctlsocket")
	procRtlNtStatusToDosError       = modntdll.NewProc("RtlNtStatusToDosError")
)

// overlappedEntry is OVERLAPPED_ENTRY: one completion dequeued from the port.
type OverlappedEntry struct {
	Key        uintptr
	Overlapped *syscall.Overlapped
	// status is the operation's NTSTATUS; zero means it succeeded.
	Status uintptr
	Qty    uint32
}

// getQueuedCompletionStatusEx dequeues up to len(entries) completions in one
// call, which is what lets one loop round serve a batch the way one epoll_wait
// does.
func GetQueuedCompletionStatusEx(port syscall.Handle, entries []OverlappedEntry, timeout uint32) (int, error) {
	var removed uint32
	r1, _, err := syscall.SyscallN(procGetQueuedCompletionStatusEx.Addr(), uintptr(port),
		uintptr(unsafe.Pointer(&entries[0])), uintptr(len(entries)), uintptr(unsafe.Pointer(&removed)),
		uintptr(timeout), 0)
	if r1 == 0 {
		return 0, err
	}
	return int(removed), nil
}

// ntStatusError converts a failed completion's NTSTATUS into the Win32 error
// the same failure reports through the synchronous calls.
func NtStatusError(status uintptr) error {
	code, _, _ := syscall.SyscallN(procRtlNtStatusToDosError.Addr(), status)
	return syscall.Errno(code)
}

// setNonblock puts a socket in non-blocking mode. syscall.SetNonblock is a
// no-op on Windows, which is why this goes to ioctlsocket directly.
func SetNonblock(fd syscall.Handle) error {
	mode := uint32(1)
	r1, _, err := syscall.SyscallN(procIoctlsocket.Addr(), uintptr(fd), uintptr(fionbio), uintptr(unsafe.Pointer(&mode)))
	if r1 != 0 {
		return err
	}
	return nil
}

// newSocket creates a stream socket the completion port can drive: socket()
// gives it the overlapped attribute, and the handle is kept from child
// processes. family is an IP family, or AF_UNIX for a Unix socket.
func NewSocket(family int) (syscall.Handle, error) {
	proto := syscall.IPPROTO_TCP
	if family == syscall.AF_UNIX {
		proto = 0
	}
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(family, syscall.SOCK_STREAM, proto)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	return fd, err
}

// newDatagramSocket creates a non-blocking UDP socket the completion port can
// drive. An unconnected socket also stops reporting ICMP port unreachable as
// a receive error: one peer that went away would otherwise fail the receive
// every other peer's datagrams arrive on.
func NewDatagramSocket(family int) (syscall.Handle, error) {
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(family, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return fd, err
	}
	if err = SetNonblock(fd); err != nil {
		syscall.Closesocket(fd)
		return syscall.InvalidHandle, err
	}
	var off uint32
	var returned uint32
	_ = syscall.WSAIoctl(fd, SioUDPConnReset, (*byte)(unsafe.Pointer(&off)), uint32(unsafe.Sizeof(off)),
		nil, 0, &returned, nil, 0)
	return fd, nil
}

func DefaultBacklog() int { return syscall.SOMAXCONN }

// listenBacklog is what listen is told for a backlog. SOMAXCONN leaves the
// depth to Winsock, a couple of hundred on many versions, and a connection
// burst beyond it is refused outright; SOMAXCONN_HINT(n), a negative n, asks
// for n itself, which Winsock keeps between 200 and 65535.
func ListenBacklog(backlog int) int {
	const hintMax = 65535
	if backlog <= 200 {
		return backlog
	}
	return -min(backlog, hintMax)
}

func IsWouldBlock(err error) bool { return err == WSAEWOULDBLOCK }

// WSABuf describes b, or as much of it as one WSABUF can hold.
//
// It returns the WSABUF rather than filling one in, as WSABufs does, for a
// caller whose b may be on its stack: a pointer stored through a slice goes to
// the heap as far as the compiler can tell, even when the slice is of an array
// on the caller's stack, so WSABufs puts what it describes there, and a frame
// header handed to Connection.SendParts would be allocated each time. A
// WSABUF assigned straight into the caller's array leaves b where it is.
func WSABuf(b []byte) syscall.WSABuf {
	return syscall.WSABuf{Len: uint32(min(len(b), MaxWSABufLen)), Buf: unsafe.SliceData(b)}
}

// WSABufs fills bufs from the non-empty slices in data and reports how many
// it used. What data points at escapes to the heap; see WSABuf.
func WSABufs(bufs []syscall.WSABuf, data ...[]byte) int {
	count := 0
	for _, b := range data {
		if len(b) == 0 {
			continue
		}
		if count == len(bufs) {
			break
		}
		bufs[count] = WSABuf(b)
		count++
		if len(b) > MaxWSABufLen {
			// The rest of this slice has to go before anything after it.
			break
		}
	}
	return count
}

// maxWSABufLen keeps one buffer's length within WSABUF's 32-bit field. A
// longer slice is simply sent in more than one call.
const MaxWSABufLen = 1 << 30

// The codes a refused or unroutable connect fails with: ConnectEx reports
// the ERROR_ ones, a plain connect the WSAE ones.
const (
	errorConnectionRefused  = syscall.Errno(1225)
	errorNetworkUnreachable = syscall.Errno(1231)
	errorHostUnreachable    = syscall.Errno(1232)
	wsaeAccess              = syscall.Errno(10013)
	wsaeAddrInUse           = syscall.Errno(10048)
	wsaeNetUnreach          = syscall.Errno(10051)
	wsaeConnRefused         = syscall.Errno(10061)
	wsaeHostUnreach         = syscall.Errno(10065)
)

// PortableErrno is errno as a failed connect reports it. A code Windows
// names its own way is wrapped so that errors.Is also matches the error
// package syscall names it by on every platform, as the Unix engines report
// it: a caller checking for syscall.ECONNREFUSED hears of a refused connect
// on Windows too, where net's dial has it match only the Windows code. The
// Windows code is still what it unwraps to.
func PortableErrno(errno syscall.Errno) error {
	var portable syscall.Errno
	switch errno {
	case errorConnectionRefused, wsaeConnRefused:
		portable = syscall.ECONNREFUSED
	case errorNetworkUnreachable, wsaeNetUnreach:
		portable = syscall.ENETUNREACH
	case errorHostUnreachable, wsaeHostUnreach:
		portable = syscall.EHOSTUNREACH
	case wsaeAddrInUse:
		portable = syscall.EADDRINUSE
	default:
		return errno
	}
	return &connectErrno{errno: errno, portable: portable}
}

// IsAddrInUse reports whether err says a bind found its address taken. A
// port another socket holds exclusively, or one Windows reserves, fails
// with WSAEACCES rather than WSAEADDRINUSE, and is just as unavailable.
func IsAddrInUse(err error) bool {
	return errors.Is(err, wsaeAddrInUse) || errors.Is(err, wsaeAccess)
}

// connectErrno is a Windows code that also matches its portable error.
type connectErrno struct {
	errno, portable syscall.Errno
}

func (e *connectErrno) Error() string { return e.errno.Error() }

func (e *connectErrno) Unwrap() error { return e.errno }

func (e *connectErrno) Is(target error) bool { return target == e.portable }

// Timeout and Temporary are errno's, for a caller asking net.Error.
func (e *connectErrno) Timeout() bool { return e.errno.Timeout() }

func (e *connectErrno) Temporary() bool { return e.errno.Temporary() }
