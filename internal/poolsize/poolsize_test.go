package poolsize

import "testing"

func TestCeiling(t *testing.T) {
	const gib = int64(1) << 30
	for _, c := range []struct {
		name string
		pool Pool
		m    Machine
		want int
		got  int
	}{
		{"memory unknown keeps want", Engine, Machine{CPUs: 8}, 8000, 8000},
		{"plenty of memory keeps want", Engine, Machine{CPUs: 8, Memory: 32 * gib, Processes: 1}, 8000, 8000},
		// 512 MiB: budget 256 MiB, engine's 30% is 76.8 MiB, at 64 KiB a
		// worker 1228 workers.
		{"small container caps", Engine, Machine{CPUs: 8, Memory: 512 << 20, Processes: 1}, 8000, 1228},
		// 64 MiB: the budget pays for 153, under the floor of 64 a CPU.
		{"floor per CPU", Engine, Machine{CPUs: 8, Memory: 64 << 20, Processes: 1}, 8000, 512},
		{"floor never above want", Engine, Machine{CPUs: 8, Memory: 64 << 20, Processes: 1}, 100, 100},
		// 128 GiB over 32 prefork children: 2 GiB each, handshake's 15%
		// is 307 MiB, at 64 KiB 4915 handshakes.
		{"prefork children share memory", Handshake, Machine{CPUs: 2, Memory: 128 * gib, Processes: 32}, 65536, 4915},
	} {
		if got := c.pool.Ceiling(c.m, c.want); got != c.got {
			t.Errorf("%s: Ceiling(%+v, %d) = %d, want %d", c.name, c.m, c.want, got, c.got)
		}
	}
}

func TestSharesAddUp(t *testing.T) {
	total := 0
	for _, p := range []Pool{Engine, Streams, Handshake, Client, File, Flush, Notify} {
		total += p.Share
	}
	if total != 100 {
		t.Fatalf("shares add up to %d, want 100", total)
	}
}

func TestDetect(t *testing.T) {
	m := Detect()
	if m.CPUs < 1 || m.Processes < 1 || m.Memory < 0 {
		t.Fatalf("Detect() = %+v", m)
	}
}
