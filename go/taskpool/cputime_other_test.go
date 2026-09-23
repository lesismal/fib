//go:build !unix

package taskpool

import "time"

// cpuTime is not measured here, so the benchmarks report zero for it.
func cpuTime() time.Duration { return 0 }
