//go:build darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

// kernelRecords says SendRawRecord cannot send here: there is no kernel TLS.
const kernelRecords = false

// sysSendRecord is never reached here, since SendRawRecord queues no record.
func (c *Connection) sysSendRecord(uint8, []byte) (int, error) { return 0, errNoKernelRecords }
