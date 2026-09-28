package taskpool

import (
	"runtime"
	"testing"
)

func TestShardCountStaysWithinBounds(t *testing.T) {
	limit := runtime.GOMAXPROCS(0)
	for _, workers := range []int{1, workersPerShard - 1, workersPerShard, 1000} {
		got := shardCount(workers)
		if got < 1 {
			t.Fatalf("shardCount(%d) = %d, want at least 1", workers, got)
		}
		if got > limit {
			t.Fatalf("shardCount(%d) = %d, want at most GOMAXPROCS %d", workers, got, limit)
		}
		if got > workers {
			t.Fatalf("shardCount(%d) = %d, want no more shards than workers", workers, got)
		}
	}
	if shardCount(workersPerShard-1) != 1 {
		t.Fatal("a pool smaller than one shard should not be sharded")
	}
}
