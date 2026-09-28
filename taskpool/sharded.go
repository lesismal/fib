package taskpool

import "runtime"

// workersPerShard is how many workers of an adaptive pool share one queue. A
// shard wants enough workers that a batch landing on it has somewhere to go,
// and few enough that they are not all contending for the same lock.
const workersPerShard = 8

// shardCount picks how many shards workerCount workers should be split over.
// It never exceeds GOMAXPROCS, since shards past that point would contend for
// cores rather than relieve contention.
func shardCount(workerCount int) int {
	shards := workerCount / workersPerShard
	if limit := runtime.GOMAXPROCS(0); shards > limit {
		shards = limit
	}
	if shards < 1 {
		shards = 1
	}
	return shards
}
