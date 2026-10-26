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
