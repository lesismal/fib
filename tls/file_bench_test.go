//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	stdtls "crypto/tls"
	"io"
	"testing"

	fib "github.com/lesismal/fib"
)

// BenchmarkSendFileTLS sends a 4MB file to a crypto/tls client over
// loopback: by sendfile with the kernel sealing, sealed here as the socket
// drains, and, for comparison, read whole and handed to Send, as SendFile
// did over TLS before.
func BenchmarkSendFileTLS(b *testing.B) {
	const size = 4 << 20
	f, _ := writeFile(b, size)
	for _, mode := range []string{"kernel", "sealed", "copy"} {
		b.Run(mode, func(b *testing.B) {
			layers, config, addr := fileServer(b, mode == "kernel", stdtls.VersionTLS13, 0, nil)
			conn := dialFileServer(b, addr, config)
			l := <-layers
			buf := make([]byte, 256<<10)
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if mode == "copy" {
					err = sendFileWhole(l.c, f, size)
				} else {
					err = l.c.SendFile(f, 0, size)
				}
				if err != nil {
					b.Fatal(err)
				}
				for left := size; left > 0; {
					n, err := conn.Read(buf[:min(left, len(buf))])
					if err != nil {
						b.Fatal(err)
					}
					left -= n
				}
			}
			b.StopTimer()
			if mode == "kernel" {
				l.wmu.Lock()
				on := l.ktls == ktlsOn
				l.wmu.Unlock()
				if !on {
					b.Log("the kernel did not seal")
				}
			}
		})
	}
}

// sendFileWhole is what SendFile did over TLS before a layer could carry a
// file: the range read and handed to Send a chunk at a time, all of it queued
// before it returns.
func sendFileWhole(c *fib.Connection, f io.ReaderAt, size int) error {
	buf := make([]byte, 64<<10)
	for off := 0; off < size; {
		n, err := f.ReadAt(buf[:min(size-off, len(buf))], int64(off))
		if n > 0 {
			if err := c.Send(buf[:n]); err != nil {
				return err
			}
			off += n
		}
		if err != nil && off < size {
			return err
		}
	}
	return nil
}
