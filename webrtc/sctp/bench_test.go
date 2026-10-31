package sctp

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkThroughput moves messages between two associations over a
// perfect in-memory link, timing the protocol work of both ends.
func BenchmarkThroughput(b *testing.B) {
	for _, size := range []int{256, 1 << 10, 16 << 10, 64 << 10} {
		for _, unordered := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/unordered=%v", size, unordered), func(b *testing.B) {
				received := 0
				l := newLink(b, 1, func(i int, c *Config) {
					c.OnMessage = func(uint16, uint32, []byte) { received++ }
					c.HeartbeatInterval = -1
				})
				l.delay = time.Millisecond
				l.connect(false)
				msg := make([]byte, size)
				a := l.ends[0]
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					for a.BufferedAmount(0) > 1<<20 {
						l.run(time.Second, func() bool { return a.BufferedAmount(0) <= 512<<10 })
					}
					_ = a.Send(0, PPIDBinary, msg, SendOptions{Unordered: unordered})
					_ = i
				}
				l.run(time.Hour, func() bool { return received == b.N })
				if received != b.N {
					b.Fatalf("%d of %d", received, b.N)
				}
			})
		}
	}
}
