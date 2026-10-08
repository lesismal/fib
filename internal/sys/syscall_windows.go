//go:build windows

package sys

import (
	"syscall"
	"unsafe"
)

// Winsock and NT values the syscall package does not export.
const (
	WSAEWOULDBLOCK = syscall.Errno(10035)
	WSAEINVAL      = syscall.Errno(10022)
	WSAEMSGSIZE    = syscall.Errno(10040)
	WSAECONNRESET  = syscall.Errno(10054)
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

// wsaBufs fills bufs from the non-empty slices in data and reports how many it
// used.
func WSABufs(bufs []syscall.WSABuf, data ...[]byte) int {
	count := 0
	for _, b := range data {
		if len(b) == 0 {
			continue
		}
		if count == len(bufs) {
			break
		}
		bufs[count] = syscall.WSABuf{Len: uint32(min(len(b), MaxWSABufLen)), Buf: &b[0]}
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
