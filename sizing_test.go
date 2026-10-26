package fib

import (
	"testing"
)

func TestMachineSizing(t *testing.T) {
	const (
		kiB = 1 << 10
		miB = 1 << 20
		giB = 1 << 30
	)
	for _, c := range []struct {
		name       string
		m          Machine
		readBuffer int
		pending    int64
		maxConns   int
		wantW      int
		wantB      int64
		wantQ      int64
	}{
		{
			name: "unknown machine keeps the conservative watermark",
			m:    Machine{}, readBuffer: 64 * kiB,
			wantW: 64 * kiB,
		},
		{
			name: "fib read buffer, no budget, 2 GiB and many descriptors",
			m:    Machine{CPUs: 2, Memory: 2 * giB, FDs: 1 << 20}, readBuffer: 16 * kiB,
			wantW: 64 * kiB,
		},
		{
			name: "fib read buffer, 64 GiB",
			m:    Machine{CPUs: 64, Memory: 64 * giB, FDs: 1 << 20}, readBuffer: 16 * kiB,
			wantW: 64 * kiB,
		},
		{
			name: "fit read buffer, 2 GiB, many descriptors",
			m:    Machine{CPUs: 4, Memory: 2 * giB, FDs: 1 << 20}, readBuffer: 64 * kiB, pending: 1 * giB, maxConns: 100_000,
			wantW: 64 * kiB, wantB: 1 * giB, wantQ: 32 * miB,
		},
		{
			name: "fit read buffer, 2 GiB, few descriptors",
			m:    Machine{CPUs: 4, Memory: 2 * giB, FDs: 1024}, readBuffer: 64 * kiB, pending: 1 * giB,
			wantW: 256 * kiB, wantB: 1 * giB, wantQ: 32 * miB,
		},
		{
			name: "fit read buffer, 4 GiB",
			m:    Machine{CPUs: 4, Memory: 4 * giB, FDs: 1 << 20}, readBuffer: 64 * kiB, pending: 2 * giB, maxConns: 100_000,
			wantW: 80 * kiB, wantB: 2 * giB, wantQ: 32 * miB,
		},
		{
			name: "fit read buffer, 8 GiB",
			m:    Machine{CPUs: 8, Memory: 8 * giB, FDs: 1 << 20}, readBuffer: 64 * kiB, pending: 4 * giB, maxConns: 100_000,
			wantW: 160 * kiB, wantB: 4 * giB, wantQ: 32 * miB,
		},
		{
			name: "fit read buffer, 16 GiB reaches the cap",
			m:    Machine{CPUs: 16, Memory: 16 * giB, FDs: 1 << 20}, readBuffer: 64 * kiB, pending: 8 * giB, maxConns: 100_000,
			wantW: 256 * kiB, wantB: 8 * giB, wantQ: 32 * miB,
		},
		{
			name: "a small connection cap lets each have more",
			m:    Machine{CPUs: 4, Memory: 4 * giB, FDs: 1 << 20}, readBuffer: 64 * kiB, pending: 2 * giB, maxConns: 10_000,
			wantW: 256 * kiB, wantB: 2 * giB, wantQ: 32 * miB,
		},
		{
			name: "a small descriptor limit lets each connection have more",
			m:    Machine{CPUs: 4, Memory: 4 * giB, FDs: 1024}, readBuffer: 64 * kiB, pending: 2 * giB,
			wantW: 256 * kiB, wantB: 2 * giB, wantQ: 32 * miB,
		},
		{
			name: "32 KiB read buffer scales the watermark with it",
			m:    Machine{CPUs: 8, Memory: 8 * giB, FDs: 1 << 20}, readBuffer: 32 * kiB, pending: 4 * giB, maxConns: 100_000,
			wantW: 128 * kiB, wantB: 4 * giB, wantQ: 32 * miB,
		},
		{
			name: "128 KiB read buffer is held to the round size",
			m:    Machine{CPUs: 16, Memory: 16 * giB, FDs: 1 << 20}, readBuffer: 128 * kiB, pending: 8 * giB, maxConns: 100_000,
			wantW: 256 * kiB, wantB: 8 * giB, wantQ: 32 * miB,
		},
		{
			name: "a small budget keeps the queue cap small",
			m:    Machine{}, readBuffer: 64 * kiB, pending: 64 * miB,
			wantW: 64 * kiB, wantB: 64 * miB, wantQ: 8 * miB,
		},
		{
			name: "no budget leaves the hard cap off",
			m:    Machine{CPUs: 16, Memory: 16 * giB, FDs: 1 << 20}, readBuffer: 64 * kiB,
			wantW: 256 * kiB,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := c.m.Sizing(c.readBuffer, c.pending, c.maxConns)
			if got.WriteWatermark != c.wantW {
				t.Errorf("WriteWatermark = %d, want %d", got.WriteWatermark, c.wantW)
			}
			if got.MaxPendingBytes != c.wantB {
				t.Errorf("MaxPendingBytes = %d, want %d", got.MaxPendingBytes, c.wantB)
			}
			if got.MaxWriteQueueBytes != c.wantQ {
				t.Errorf("MaxWriteQueueBytes = %d, want %d", got.MaxWriteQueueBytes, c.wantQ)
			}
			if got.WriteWatermark < sizingWatermarkMin || got.WriteWatermark > sizingWatermarkMax {
				t.Errorf("WriteWatermark %d outside [%d, %d]", got.WriteWatermark, sizingWatermarkMin, sizingWatermarkMax)
			}
			if got.WriteWatermark%sizingWatermarkStep != 0 {
				t.Errorf("WriteWatermark %d is not a multiple of %d", got.WriteWatermark, sizingWatermarkStep)
			}
			if got.MaxWriteQueueBytes < 0 || got.MaxWriteQueueBytes > sizingQueueMax {
				t.Errorf("MaxWriteQueueBytes %d outside [0, %d]", got.MaxWriteQueueBytes, sizingQueueMax)
			}
		})
	}
}

// The watermark never grows past four read buffers: beyond that the read
// buffer, not the watermark, is the amount one round can queue.
func TestSizingWatermarkFitsTheReadBuffer(t *testing.T) {
	for _, readBuffer := range []int{16 << 10, 32 << 10, 64 << 10, 128 << 10} {
		for _, m := range []Machine{
			{Memory: 512 << 20, FDs: 1024},
			{Memory: 4 << 30, FDs: 1 << 20},
			{Memory: 64 << 30, FDs: 1 << 20},
			{Memory: 1 << 40, FDs: 1 << 22},
		} {
			got := m.Sizing(readBuffer, 0, 0).WriteWatermark
			if want := 4 * readBuffer; got > want {
				t.Errorf("%d CPUs %d GiB: watermark %d exceeds four read buffers %d", m.CPUs, m.Memory>>30, got, want)
			}
		}
	}
}

// A budget is never turned on by sizing: it has to be passed in.
func TestSizingDoesNotEnableABudget(t *testing.T) {
	got := Machine{Memory: 64 << 30}.Sizing(64<<10, 0, 0)
	if got.MaxPendingBytes != 0 {
		t.Errorf("MaxPendingBytes = %d, want 0", got.MaxPendingBytes)
	}
	if got.MaxWriteQueueBytes != 0 {
		t.Errorf("MaxWriteQueueBytes = %d, want 0", got.MaxWriteQueueBytes)
	}
}

func TestDefaultConfigSizing(t *testing.T) {
	config := DefaultConfig()
	if config.ReadBufferSize != defaultReadBufferSize {
		t.Fatalf("ReadBufferSize = %d, want %d", config.ReadBufferSize, defaultReadBufferSize)
	}
	if config.WriteBufferHighWatermark != defaultWriteHighWatermark {
		t.Fatalf("WriteBufferHighWatermark = %d, want %d for the default read buffer", config.WriteBufferHighWatermark, defaultWriteHighWatermark)
	}
	if config.MaxPendingBytes != 0 || config.MaxWriteQueueBytes != 0 {
		t.Fatalf("MaxPendingBytes = %d, MaxWriteQueueBytes = %d, want both off", config.MaxPendingBytes, config.MaxWriteQueueBytes)
	}

	// A larger read buffer gets the machine-sized watermark, and it is the
	// same one the helper reports.
	for _, readBuffer := range []int{32 << 10, 64 << 10} {
		config := DefaultConfigFor(readBuffer)
		want := DefaultWriteWatermark(readBuffer, 0, 0)
		if config.ReadBufferSize != readBuffer {
			t.Errorf("DefaultConfigFor(%d).ReadBufferSize = %d", readBuffer, config.ReadBufferSize)
		}
		if config.WriteBufferHighWatermark != want {
			t.Errorf("DefaultConfigFor(%d).WriteBufferHighWatermark = %d, want %d", readBuffer, config.WriteBufferHighWatermark, want)
		}
		if want < sizingWatermarkMin || want > sizingWatermarkMax {
			t.Errorf("watermark %d outside [%d, %d]", want, sizingWatermarkMin, sizingWatermarkMax)
		}
	}
}
