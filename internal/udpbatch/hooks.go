//go:build linux || darwin

package udpbatch

import "syscall"

// SetSingle makes the batch read one datagram at a time, as it does once the
// kernel has refused a batched receive. For tests.
func (b *Batch) SetSingle(v bool) { b.single = v }

// SetSingleSends makes Send send one datagram at a time, as it does once the
// kernel has refused a batched send, and reports what it was. For tests.
func SetSingleSends(v bool) (prev bool) { return singleSends.Swap(v) }

// SingleSends reports whether Send has fallen back to one datagram at a time.
func SingleSends() bool { return singleSends.Load() }

// SendSys is the batched send itself, without Send's fallback. For tests.
func SendSys(fd int, to syscall.Sockaddr, datagrams [][]byte) (int, error) {
	return sendBatchSys(fd, to, datagrams)
}
