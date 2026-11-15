//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package core

import "testing"

// ReusePortByCPU sizes the selector's group by the sockets a process puts in
// it: one per listening poller, or one, times the prefork children (one
// outside prefork), for TCP only.
func TestListenOptionsCPUSelectGroup(t *testing.T) {
	config := DefaultConfig()
	config.ReusePortByCPU = true
	config.ReusePort = true
	config.IOPollers = true
	config.IOPollerCount = 3
	config.EngineLoop = false

	want := 1
	if pollersListen(config, "tcp") {
		want = 3
	}
	if got := listenOptions(config, "tcp", false).CPUSelectGroup; got != want*preforkChildren() {
		t.Errorf("tcp group = %d, want %d", got, want*preforkChildren())
	}
	if got := listenOptions(config, "udp", false).CPUSelectGroup; got != 0 {
		t.Errorf("udp group = %d, want 0", got)
	}
	if got := listenOptions(config, "unix", false).CPUSelectGroup; got != 0 {
		t.Errorf("unix group = %d, want 0", got)
	}
	config.ReusePortByCPU = false
	if got := listenOptions(config, "tcp", false).CPUSelectGroup; got != 0 {
		t.Errorf("off: tcp group = %d, want 0", got)
	}
}
