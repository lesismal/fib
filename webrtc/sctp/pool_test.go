package sctp

import "testing"

// TestPoolBytes checks that Config.PoolBytes bounds what each size class of
// the buffer pool keeps.
func TestPoolBytes(t *testing.T) {
	for _, tc := range []struct {
		keep, size, want int
	}{
		{0, 64, 100},
		{0, 512 << 10, poolKeep},
		{4096, 64, 64},
		{4096, 4096, 1},
		{4096, 8192, 0},
		{-1, 64, 0},
	} {
		p := bufPool{keepSize: tc.keep}
		for range 100 {
			p.put(make([]byte, tc.size))
		}
		if got := len(p.classes[poolClass(tc.size)]); got != tc.want {
			t.Errorf("PoolBytes %d, %d-byte buffers: kept %d, want %d", tc.keep, tc.size, got, tc.want)
		}
	}
	if a := NewAssociation(Config{PoolBytes: 1 << 10}); a.pool.keepSize != 1<<10 {
		t.Fatalf("NewAssociation kept PoolBytes %d", a.pool.keepSize)
	}
}
