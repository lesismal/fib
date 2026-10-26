//go:build windows

package sys

import (
	"unsafe"
)

// memoryStatusEx is MEMORYSTATUSEX, which GlobalMemoryStatusEx fills.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

var procGlobalMemoryStatusEx = modkernel32.NewProc("GlobalMemoryStatusEx")

// MemoryLimit reports the machine's physical memory, or zero when it could
// not be read. A Job object's memory limit is not visible here.
func MemoryLimit() int64 {
	var status memoryStatusEx
	status.Length = uint32(unsafe.Sizeof(status))
	ok, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&status)))
	if ok == 0 {
		return 0
	}
	return int64(status.TotalPhys)
}
