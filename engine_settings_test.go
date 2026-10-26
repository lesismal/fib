//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// Settings reports what the engine runs with, after the defaults have filled
// in what the config left out, and its line carries every basic value.
func TestEngineSettings(t *testing.T) {
	config := DefaultConfig()
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	got := engine.Settings()
	if got.Name != config.Name {
		t.Errorf("Name = %q, want %q", got.Name, config.Name)
	}
	if got.Network != "tcp" {
		t.Errorf("Network = %q, want tcp", got.Network)
	}
	if got.ReadBufferSize != config.ReadBufferSize {
		t.Errorf("ReadBufferSize = %d, want %d", got.ReadBufferSize, config.ReadBufferSize)
	}
	if got.WriteBufferHighWatermark != config.WriteBufferHighWatermark {
		t.Errorf("WriteBufferHighWatermark = %d, want %d", got.WriteBufferHighWatermark, config.WriteBufferHighWatermark)
	}
	if got.MaxPendingBytes != 0 || got.MaxWriteQueueBytes != 0 || got.MaxConns != 0 {
		t.Errorf("limits = pending %d, queue %d, conns %d, want all off",
			got.MaxPendingBytes, got.MaxWriteQueueBytes, got.MaxConns)
	}
	if got.WorkerCount != config.WorkerCount || got.MaxEvents != config.MaxEvents {
		t.Errorf("pool = %d workers, %d events, want %d, %d", got.WorkerCount, got.MaxEvents, config.WorkerCount, config.MaxEvents)
	}
	if got.UDPIdleTimeout != DefaultUDPIdleTimeout {
		t.Errorf("UDPIdleTimeout = %v, want %v", got.UDPIdleTimeout, DefaultUDPIdleTimeout)
	}

	line := got.String()
	for _, want := range []string{
		"name=fib",
		"network=tcp",
		"readBuffer=16384",
		"writeWatermark=65536",
		"maxPendingBytes=0",
		"maxWriteQueueBytes=0",
		"maxConns=0",
		"taskPoolMode=adaptive",
		"ioPollers=true",
		"coalesceWrites=true",
		"useWritev=true",
		"socketSyscalls=true",
		"udpIdleTimeout=1m0s",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("Settings.String() = %q, missing %q", line, want)
		}
	}
	if strings.Contains(line, "%!") {
		t.Errorf("Settings.String() has a formatting error: %q", line)
	}
}

// The startup log prints the settings line, with the sized values, when
// LogStatus asks for it.
func TestLogRunPrintsSettings(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)

	config := DefaultConfig()
	config.LogStatus = true
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	engine.logRun()

	out := buf.String()
	if !strings.Contains(out, "fib: engine config") {
		t.Fatalf("logRun did not print the settings line: %q", out)
	}
	for _, want := range []string{
		"engine=" + DefaultName,
		"readBuffer=16384",
		"writeWatermark=65536",
		"maxPendingBytes=0",
		"maxWriteQueueBytes=0",
		"maxConns=0",
		"coalesceWrites=true",
		"socketSyscalls=true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("logRun output %q is missing %q", out, want)
		}
	}
}
