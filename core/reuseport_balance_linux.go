//go:build linux && (amd64 || arm64)

package core

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"
)

// lastPinErr records why the pinned balancer last fell back, for a test to
// print; it is unused in normal operation.
var lastPinErr error

// pinFallback records a stage and its error for lastPinErr, and folds it to
// the unsupported sentinel as unsupportedIfDenied does.
func pinFallback(stage string, err error) error {
	lastPinErr = fmt.Errorf("%s: %w", stage, err)
	return unsupportedIfDenied(err)
}

// This file carries fib's whole eBPF dependency: a hand-assembled
// BPF_PROG_TYPE_SK_REUSEPORT program and the two maps it needs, loaded
// through bpf(2) directly so that fib keeps to the standard library. The
// program hands each new connection to the next socket of a reuseport group
// in turn, round-robin, where the kernel's own default spreads them by a hash
// of their addresses, which leaves a group of a few sockets serving a few
// connections badly balanced. It needs CAP_BPF (CAP_SYS_ADMIN before Linux
// 5.8); without it, or on an older kernel, loading fails and the caller keeps
// plain SO_REUSEPORT.

// bpf command numbers (uapi/linux/bpf.h).
const (
	bpfMapCreate     = 0
	bpfMapUpdateElem = 2
	bpfProgLoad      = 5
	bpfObjPin        = 6
	bpfObjGet        = 7
)

// bpf map and program types used here.
const (
	bpfMapTypeArray             = 2
	bpfMapTypeReusePortSockHash = 0x14 // BPF_MAP_TYPE_REUSEPORT_SOCKARRAY
	bpfProgTypeSKReusePort      = 21
)

// bpf helper function ids (uapi/linux/bpf.h).
const (
	helperMapLookupElem     = 1
	helperSKSelectReusePort = 82
)

// bpfAny is BPF_ANY, the update flag that creates or replaces an entry.
const bpfAny = 0

// A reusePortBalancer holds the maps and program kept alive for one
// reuseport group. The group holds its own references once the program is
// attached, so Close only drops fib's handles.
type reusePortBalancer struct {
	sockarray int
	counter   int
	prog      int
}

// bpfInsn is one eBPF instruction, as the kernel reads it: an opcode, a
// register byte holding the destination in its low nibble and the source in
// its high nibble, a signed offset and a signed immediate.
type bpfInsn struct {
	code uint8
	regs uint8
	off  int16
	imm  int32
}

// instruction opcodes (uapi/linux/bpf.h, uapi/linux/filter.h).
const (
	opMov64Reg = 0xbf // BPF_ALU64 | BPF_MOV | BPF_X
	opMov64Imm = 0xb7 // BPF_ALU64 | BPF_MOV | BPF_K
	opAdd64Imm = 0x07 // BPF_ALU64 | BPF_ADD | BPF_K
	opMod32Imm = 0x94 // BPF_ALU   | BPF_MOD | BPF_K
	opStMemW   = 0x62 // BPF_ST    | BPF_MEM | BPF_W
	opStxMemW  = 0x63 // BPF_STX   | BPF_MEM | BPF_W
	opLdxMemW  = 0x61 // BPF_LDX   | BPF_MEM | BPF_W
	opLdImmDW  = 0x18 // BPF_LD    | BPF_IMM | BPF_DW
	opJeqImm   = 0x15 // BPF_JMP   | BPF_JEQ | BPF_K
	opJa       = 0x05 // BPF_JMP   | BPF_JA
	opCall     = 0x85 // BPF_JMP   | BPF_CALL
	opExit     = 0x95 // BPF_JMP   | BPF_EXIT
)

// pseudoMapFD is the source register value of the LD_IMM64 that loads a map
// by its file descriptor.
const pseudoMapFD = 1

func regs(dst, src int) uint8 { return uint8(dst) | uint8(src)<<4 }

// selectProgram assembles the round-robin selector for a group of size
// sockets. It reads a counter from the counter map, increments it, and picks
// the socket at counter%size from the sockarray with bpf_sk_select_reuseport,
// falling back to socket 0 when the counter is somehow missing. size is baked
// in as an immediate.
func selectProgram(counterFD, sockarrayFD, size int) []bpfInsn {
	const (
		r0  = 0
		r1  = 1
		r2  = 2
		r3  = 3
		r4  = 4
		r6  = 6
		r10 = 10
	)
	return []bpfInsn{
		{code: opMov64Reg, regs: regs(r6, r1)},                                // r6 = ctx
		{code: opStMemW, regs: regs(r10, 0), off: -4},                         // *(u32*)(r10-4) = 0 (counter key)
		{code: opLdImmDW, regs: regs(r1, pseudoMapFD), imm: int32(counterFD)}, // r1 = counter map
		{},                                      // LD_IMM64 second half
		{code: opMov64Reg, regs: regs(r2, r10)}, // r2 = r10
		{code: opAdd64Imm, regs: regs(r2, 0), imm: -4},                          // r2 += -4  (&key)
		{code: opCall, imm: helperMapLookupElem},                                // r0 = lookup(counter, &key)
		{code: opJeqImm, regs: regs(r0, 0), off: 7},                             // if r0 == 0 goto zero
		{code: opLdxMemW, regs: regs(r1, r0)},                                   // r1 = *(u32*)r0
		{code: opMov64Reg, regs: regs(r2, r1)},                                  // r2 = r1
		{code: opAdd64Imm, regs: regs(r2, 0), imm: 1},                           // r2 += 1
		{code: opStxMemW, regs: regs(r0, r2)},                                   // *(u32*)r0 = r2
		{code: opMod32Imm, regs: regs(r1, 0), imm: int32(size)},                 // r1 %= size
		{code: opStxMemW, regs: regs(r10, r1), off: -8},                         // *(u32*)(r10-8) = r1 (index)
		{code: opJa, off: 1},                                                    // goto select
		{code: opStMemW, regs: regs(r10, 0), off: -8},                           // zero: *(u32*)(r10-8) = 0
		{code: opMov64Reg, regs: regs(r1, r6)},                                  // select: r1 = ctx
		{code: opLdImmDW, regs: regs(r2, pseudoMapFD), imm: int32(sockarrayFD)}, // r2 = sockarray
		{},                                      // LD_IMM64 second half
		{code: opMov64Reg, regs: regs(r3, r10)}, // r3 = r10
		{code: opAdd64Imm, regs: regs(r3, 0), imm: -8}, // r3 += -8  (&index)
		{code: opMov64Imm, regs: regs(r4, 0)},          // r4 = 0 (flags)
		{code: opCall, imm: helperSKSelectReusePort},   // bpf_sk_select_reuseport
		{code: opMov64Imm, regs: regs(r0, 0), imm: 1},  // r0 = SK_PASS
		{code: opExit},
	}
}

func bpf(cmd int, attr unsafe.Pointer, size uintptr) (int, syscall.Errno) {
	r, _, e := syscall.Syscall(sysBPF, uintptr(cmd), uintptr(attr), size)
	return int(r), e
}

type bpfMapCreateAttr struct {
	mapType    uint32
	keySize    uint32
	valueSize  uint32
	maxEntries uint32
	mapFlags   uint32
}

func createMap(mapType, keySize, valueSize, maxEntries int) (int, error) {
	attr := bpfMapCreateAttr{
		mapType:    uint32(mapType),
		keySize:    uint32(keySize),
		valueSize:  uint32(valueSize),
		maxEntries: uint32(maxEntries),
	}
	fd, errno := bpf(bpfMapCreate, unsafe.Pointer(&attr), unsafe.Sizeof(attr))
	if errno != 0 {
		return -1, errno
	}
	return fd, nil
}

type bpfProgLoadAttr struct {
	progType    uint32
	insnCnt     uint32
	insns       uint64
	license     uint64
	logLevel    uint32
	logSize     uint32
	logBuf      uint64
	kernVersion uint32
	progFlags   uint32
	progName    [16]byte
}

// progLoadLog, when set, is handed to the verifier to fill with its log, for
// a test to print when a load fails. It stays nil on the listen path, so no
// log buffer is allocated there.
var progLoadLog []byte

func loadProgram(insns []bpfInsn) (int, error) {
	license := []byte("GPL\x00")
	attr := bpfProgLoadAttr{
		progType: bpfProgTypeSKReusePort,
		insnCnt:  uint32(len(insns)),
		insns:    uint64(uintptr(unsafe.Pointer(&insns[0]))),
		license:  uint64(uintptr(unsafe.Pointer(&license[0]))),
	}
	if progLoadLog != nil {
		attr.logLevel = 1
		attr.logSize = uint32(len(progLoadLog))
		attr.logBuf = uint64(uintptr(unsafe.Pointer(&progLoadLog[0])))
	}
	fd, errno := bpf(bpfProgLoad, unsafe.Pointer(&attr), unsafe.Sizeof(attr))
	runtime.KeepAlive(insns)
	runtime.KeepAlive(license)
	runtime.KeepAlive(progLoadLog)
	if errno != 0 {
		return -1, errno
	}
	return fd, nil
}

type bpfMapUpdateAttr struct {
	mapFD uint32
	_     uint32
	key   uint64
	value uint64
	flags uint64
}

// registerSocket puts socketFD into the sockarray at index, so that the
// program can select it. The value a REUSEPORT_SOCKARRAY holds is the socket
// descriptor, which the kernel resolves to the socket.
func registerSocket(sockarrayFD, index, socketFD int) error {
	key := uint32(index)
	value := uint32(socketFD)
	attr := bpfMapUpdateAttr{
		mapFD: uint32(sockarrayFD),
		key:   uint64(uintptr(unsafe.Pointer(&key))),
		value: uint64(uintptr(unsafe.Pointer(&value))),
		flags: bpfAny,
	}
	_, errno := bpf(bpfMapUpdateElem, unsafe.Pointer(&attr), unsafe.Sizeof(attr))
	runtime.KeepAlive(&key)
	runtime.KeepAlive(&value)
	if errno != 0 {
		return errno
	}
	return nil
}

// attach makes the program the reuseport group's selector, set on one of the
// group's sockets, after which the group keeps its own reference to it.
func attach(socketFD, progFD int) error {
	return syscall.SetsockoptInt(socketFD, syscall.SOL_SOCKET, soAttachReusePortEBPF, progFD)
}

// newReusePortBalancer round-robins the group of listening sockets fds, which
// must already share an address through SO_REUSEPORT. It creates the maps,
// loads the program, registers every socket and attaches the program to the
// first. It returns errReusePortBalanceUnsupported, with nothing left behind,
// where bpf(2) is refused, so the caller keeps plain SO_REUSEPORT.
func newReusePortBalancer(fds []int) (*reusePortBalancer, error) {
	if len(fds) < 2 {
		return nil, errReusePortBalanceUnsupported
	}
	sockarray, err := createMap(bpfMapTypeReusePortSockHash, 4, 4, len(fds))
	if err != nil {
		return nil, unsupportedIfDenied(err)
	}
	counter, err := createMap(bpfMapTypeArray, 4, 4, 1)
	if err != nil {
		syscall.Close(sockarray)
		return nil, unsupportedIfDenied(err)
	}
	prog, err := loadProgram(selectProgram(counter, sockarray, len(fds)))
	if err != nil {
		syscall.Close(sockarray)
		syscall.Close(counter)
		return nil, unsupportedIfDenied(err)
	}
	b := &reusePortBalancer{sockarray: sockarray, counter: counter, prog: prog}
	for i, fd := range fds {
		if err := registerSocket(sockarray, i, fd); err != nil {
			b.Close()
			return nil, err
		}
	}
	if err := attach(fds[0], prog); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

type bpfObjAttr struct {
	pathname  uint64
	bpfFD     uint32
	fileFlags uint32
}

// objPin pins the object fd into bpffs at path, so another process can open
// it by that path with objGet.
func objPin(fd int, path string) error {
	p := append([]byte(path), 0)
	attr := bpfObjAttr{pathname: uint64(uintptr(unsafe.Pointer(&p[0]))), bpfFD: uint32(fd)}
	_, errno := bpf(bpfObjPin, unsafe.Pointer(&attr), unsafe.Sizeof(attr))
	runtime.KeepAlive(p)
	if errno != 0 {
		return errno
	}
	return nil
}

// objGet opens the object pinned in bpffs at path, or reports the errno; it
// returns ENOENT when nothing is pinned there yet.
func objGet(path string) (int, error) {
	p := append([]byte(path), 0)
	attr := bpfObjAttr{pathname: uint64(uintptr(unsafe.Pointer(&p[0])))}
	fd, errno := bpf(bpfObjGet, unsafe.Pointer(&attr), unsafe.Sizeof(attr))
	runtime.KeepAlive(p)
	if errno != 0 {
		return -1, errno
	}
	return fd, nil
}

// openPinnedReusePortBalancer round-robins a reuseport group whose sockets
// span several processes, as prefork's children do: one selector and its maps
// are shared through bpffs, pinned at path ("_sock", "_ctr", "_prog" beside
// it, whose parent directory the caller has made). The group holds size
// sockets in all; this process registers its fds at baseIndex and on, and
// attaches the selector to its first. The first process to arrive creates and
// pins the objects under a lock; the rest open them. It falls back like
// newReusePortBalancer where bpf(2) is refused.
func openPinnedReusePortBalancer(path string, size, baseIndex int, fds []int) (*reusePortBalancer, error) {
	if size < 2 || len(fds) == 0 {
		return nil, errReusePortBalanceUnsupported
	}
	unlock, err := lockPin(path)
	if err != nil {
		lastPinErr = fmt.Errorf("lock %s: %w", path, err)
		return nil, errReusePortBalanceUnsupported
	}
	defer unlock()

	b := &reusePortBalancer{sockarray: -1, counter: -1, prog: -1}
	prog, err := objGet(path + "_prog")
	if err == nil {
		b.prog = prog
		sock, err := objGet(path + "_sock")
		if err != nil {
			b.Close()
			return nil, pinFallback("objGet sock", err)
		}
		b.sockarray = sock
	} else {
		sock, err := createMap(bpfMapTypeReusePortSockHash, 4, 4, size)
		if err != nil {
			return nil, pinFallback("createMap sock", err)
		}
		b.sockarray = sock
		counter, err := createMap(bpfMapTypeArray, 4, 4, 1)
		if err != nil {
			b.Close()
			return nil, pinFallback("createMap counter", err)
		}
		b.counter = counter
		prog, err := loadProgram(selectProgram(counter, sock, size))
		if err != nil {
			b.Close()
			return nil, pinFallback("loadProgram", err)
		}
		b.prog = prog
		for _, pin := range []struct {
			fd   int
			path string
		}{{sock, path + "_sock"}, {counter, path + "_ctr"}, {prog, path + "_prog"}} {
			if err := objPin(pin.fd, pin.path); err != nil {
				b.Close()
				return nil, pinFallback("objPin "+pin.path, err)
			}
		}
	}
	for j, fd := range fds {
		if err := registerSocket(b.sockarray, baseIndex+j, fd); err != nil {
			b.Close()
			return nil, err
		}
	}
	if err := attach(fds[0], b.prog); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

// lockPin serializes the children racing to create the pinned objects for
// path, so that one creates and pins them and the rest open them. bpffs holds
// only pinned objects, not lock files, so the lock lives in the temp
// directory, named for path.
func lockPin(path string) (func(), error) {
	name := make([]byte, 0, len(path))
	for i := 0; i < len(path); i++ {
		if c := path[i]; c == '/' || c == ':' {
			name = append(name, '_')
		} else {
			name = append(name, c)
		}
	}
	lockPath := filepath.Join(os.TempDir(), "fib-reuseport-"+string(name)+".lock")
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX); err != nil {
		syscall.Close(fd)
		return nil, err
	}
	return func() {
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		_ = syscall.Close(fd)
	}, nil
}

// unsupportedIfDenied turns the "not permitted" errors, which mean the
// process lacks CAP_BPF or the kernel is too old, into
// errReusePortBalanceUnsupported, so the caller falls back rather than
// failing to listen. Other errors are real and returned as they are.
// reusePortDebug keeps unsupportedIfDenied from folding EINVAL into the
// unsupported sentinel, so a test can see a real bug in the maps or the
// program rather than a skip. It is false in normal use.
var reusePortDebug bool

func unsupportedIfDenied(err error) error {
	switch err {
	case syscall.EPERM, syscall.EACCES, syscall.ENOSYS:
		return errReusePortBalanceUnsupported
	case syscall.EINVAL:
		if !reusePortDebug {
			return errReusePortBalanceUnsupported
		}
	}
	return err
}

func (b *reusePortBalancer) Close() error {
	if b == nil {
		return nil
	}
	if b.prog >= 0 {
		syscall.Close(b.prog)
	}
	if b.counter >= 0 {
		syscall.Close(b.counter)
	}
	if b.sockarray >= 0 {
		syscall.Close(b.sockarray)
	}
	b.prog, b.counter, b.sockarray = -1, -1, -1
	return nil
}
