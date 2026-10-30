//go:build darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

// kernelRecords says SendRawRecord cannot send here: there is no kernel TLS.
const kernelRecords = false

// sysSendRecord is never reached here, since SendRawRecord queues no record.
func (c *Connection) sysSendRecord(uint8, []byte) (int, error) { return 0, errNoKernelRecords }

// sysWriteMore writes buffers: there is no telling the kernel here that more
// follows them.
func (c *Connection) sysWriteMore(buffers [][]byte) (int, error) { return c.sysWritev(buffers) }

// corkKernelTLS does nothing here, where no kernel seals.
func (c *Connection) corkKernelTLS(bool) bool { return false }
