//go:build dragonfly || freebsd || netbsd || openbsd

package udpbatch

import "syscall"

// batchOne is unset: an ordinary receive costs less than the batched call
// here, and nothing coalesces a run of datagrams into one read, so a read of
// one datagram has nothing to split.
const batchOne = false

// batchHeader is empty: these kernels are not given batched datagram calls
// here, so a Batch reads, and Send sends, one datagram at a time.
type batchHeader struct{}

func (b *Batch) recvBatch(int, int) (int, error) { return 0, syscall.ENOSYS }

func sendBatchSys(int, syscall.Sockaddr, [][]byte) (int, error) { return 0, syscall.ENOSYS }

// sendPartsSys reports that a message of several buffers is not taken here:
// SendParts joins the parts instead.
func sendPartsSys(int, syscall.Sockaddr, [][]byte) error { return syscall.ENOSYS }

// maxParts is what a message of several buffers would hold, which these
// kernels do not take; the parts of a datagram are joined instead.
const maxParts = Size
