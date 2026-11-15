//go:build linux && (amd64 || arm64)

package reuseportbpf

import (
	"runtime"
	"syscall"
	"unsafe"
)

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

// ProgLoadLog, when set, is handed to the verifier to fill with its log, for
// a test to print when a load fails. It stays nil on the listen path, so no
// log buffer is allocated there.
var ProgLoadLog []byte

func loadProgram(insns []bpfInsn) (int, error) {
	license := []byte("GPL\x00")
	attr := bpfProgLoadAttr{
		progType: bpfProgTypeSKReusePort,
		insnCnt:  uint32(len(insns)),
		insns:    uint64(uintptr(unsafe.Pointer(&insns[0]))),
		license:  uint64(uintptr(unsafe.Pointer(&license[0]))),
	}
	if ProgLoadLog != nil {
		attr.logLevel = 1
		attr.logSize = uint32(len(ProgLoadLog))
		attr.logBuf = uint64(uintptr(unsafe.Pointer(&ProgLoadLog[0])))
	}
	fd, errno := bpf(bpfProgLoad, unsafe.Pointer(&attr), unsafe.Sizeof(attr))
	runtime.KeepAlive(insns)
	runtime.KeepAlive(license)
	runtime.KeepAlive(ProgLoadLog)
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

// unsupportedIfDenied turns the "not permitted" errors, which mean the
// process lacks CAP_BPF or the kernel is too old, into
// ErrUnsupported, so the caller falls back rather than
// failing to listen. Other errors are real and returned as they are.

func unsupportedIfDenied(err error) error {
	switch err {
	case syscall.EPERM, syscall.EACCES, syscall.ENOSYS:
		return ErrUnsupported
	case syscall.EINVAL:
		if !Debug {
			return ErrUnsupported
		}
	}
	return err
}

// NewGroup makes the maps and program of a selector for a group of size
// sockets, none registered yet. It returns ErrUnsupported, with nothing left
// behind, where bpf(2) is refused.
func NewGroup(size int) (*Group, error) {
	if size < 2 {
		return nil, ErrUnsupported
	}
	g := &Group{Sockarray: -1, Counter: -1, Prog: -1}
	var err error
	if g.Sockarray, err = createMap(bpfMapTypeReusePortSockHash, 4, 4, size); err != nil {
		return nil, unsupportedIfDenied(err)
	}
	if g.Counter, err = createMap(bpfMapTypeArray, 4, 4, 1); err != nil {
		g.Close()
		return nil, unsupportedIfDenied(err)
	}
	if g.Prog, err = loadProgram(selectProgram(g.Counter, g.Sockarray, size)); err != nil {
		g.Close()
		return nil, unsupportedIfDenied(err)
	}
	return g, nil
}

// Register puts the listening socket fd in the group's place index, where
// the program selects it.
func (g *Group) Register(index, fd int) error { return registerSocket(g.Sockarray, index, fd) }

// Attach makes the group's program the selector of fd's reuseport group,
// which keeps its own reference to it from then on.
func (g *Group) Attach(fd int) error { return attach(fd, g.Prog) }

// Close closes the descriptors g holds, once; a nil g does nothing.
func (g *Group) Close() error {
	if g == nil {
		return nil
	}
	for _, fd := range []*int{&g.Prog, &g.Counter, &g.Sockarray} {
		if *fd >= 0 {
			syscall.Close(*fd)
			*fd = -1
		}
	}
	return nil
}

// Balance round-robins the listening sockets fds of one process, which must
// already share an address through SO_REUSEPORT: it makes the group's
// selector, registers every socket in order and attaches the program to the
// first.
func Balance(fds []int) (*Group, error) {
	if len(fds) < 2 {
		return nil, ErrUnsupported
	}
	g, err := NewGroup(len(fds))
	if err != nil {
		return nil, err
	}
	if err := register(g, 0, fds); err != nil {
		g.Close()
		return nil, err
	}
	return g, nil
}

// register puts fds in g from place base on, and attaches g's program.
func register(g *Group, base int, fds []int) error {
	for i, fd := range fds {
		if err := g.Register(base+i, fd); err != nil {
			return err
		}
	}
	return g.Attach(fds[0])
}
