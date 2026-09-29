//go:build !unix && !windows

package benchmark

import "time"

// cpuTime is not measured here, so cpu-ns/op reports zero.
func cpuTime() time.Duration { return 0 }
