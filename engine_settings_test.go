//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/lesismal/fib/log"
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
	if !got.LogSettings {
		t.Error("LogSettings = false, want fib's default on")
	}

	line := got.String()
	for _, want := range []string{
		"name=fib",
		"network=tcp",
		"readBuffer=16384",
		"readBufferStart=16384",
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
		"logStatus=false",
		"logSettings=true",
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
	// The CI environment silences the list by default; this test is about
	// the list, so it asks for it.
	t.Setenv(logSettingsEnv, "1")
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
		t.Fatalf("logRun did not print the settings header: %q", out)
	}
	for _, want := range []string{
		"engine=" + DefaultName,
		"parameter=name",
		"parameter=readBuffer",
		"parameter=writeWatermark",
		"parameter=maxPendingBytes",
		"value=\"16384 (16 KiB)\"",
		"value=\"65536 (64 KiB)\"",
		"source=default",
		"about=",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("logRun output %q is missing %q", out, want)
		}
	}
}

// A setting the caller changed is reported as custom, one fib sized from the
// read buffer and the machine as default.
func TestSettingsParameterSources(t *testing.T) {
	source := func(settings Settings, name string) string {
		t.Helper()
		for _, p := range settings.Parameters() {
			if p.Name == name {
				return p.Source
			}
		}
		t.Fatalf("no parameter %q", name)
		return ""
	}

	engine, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"name", "readBuffer", "writeWatermark", "maxPendingBytes", "useWritev", "ioPollers"} {
		if got := source(engine.Settings(), name); got != "default" {
			t.Errorf("DefaultConfig %s source = %q, want default", name, got)
		}
	}
	engine.Close()

	config := DefaultConfig()
	config.Name = "custom"
	config.ReadBufferSize = 64 << 10
	config.WriteBufferHighWatermark = 0 // sized from the new read buffer
	config.MaxPendingBytes = 1 << 30
	config.MaxConns = 100_000
	config.MaxWriteQueueBytes = 32 << 20
	config.UseWritev = false
	config.IOPollers = false
	config.LogStatus = true
	engine, err = New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	settings := engine.Settings()
	for _, name := range []string{"name", "readBuffer", "maxPendingBytes", "maxConns", "maxWriteQueueBytes", "useWritev", "ioPollers", "logStatus"} {
		if got := source(settings, name); got != "custom" {
			t.Errorf("%s source = %q, want custom", name, got)
		}
	}
	// The watermark was left to fib, so it is default even though its
	// effective value follows the custom read buffer and budget.
	if got := source(settings, "writeWatermark"); got != "default" {
		t.Errorf("writeWatermark source = %q, want default", got)
	}
}

// The settings list is on by default and can be turned off; the method still
// prints it on demand.
func TestLogSettingsDefaultAndDisable(t *testing.T) {
	t.Setenv(logSettingsEnv, "1")
	if !DefaultConfig().LogSettings {
		t.Fatal("DefaultConfig does not log its settings")
	}
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)

	config := DefaultConfig()
	config.LogStatus = false
	config.LogSettings = false
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	engine.logRun()
	if buf.Len() != 0 {
		t.Fatalf("logRun with LogSettings false wrote %q", buf.String())
	}
	engine.LogSettings()
	if out := buf.String(); !strings.Contains(out, "fib: engine config") || !strings.Contains(out, "parameter=readBuffer") {
		t.Fatalf("LogSettings did not print the list: %q", out)
	}
}

// FIB_LOG_SETTINGS silences the automatic settings list, as fib's CI sets it,
// while Engine.LogSettings still prints the list on demand.
func TestLogSettingsEnvSilences(t *testing.T) {
	for _, value := range []string{"0", "false", "off", "no", "FALSE", " off "} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(logSettingsEnv, value)
			var buf bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			defer slog.SetDefault(old)

			engine, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close()
			engine.logRun()
			if buf.Len() != 0 {
				t.Fatalf("logRun with %s=%q wrote %q", logSettingsEnv, value, buf.String())
			}
			engine.LogSettings()
			if out := buf.String(); !strings.Contains(out, "fib: engine config") || !strings.Contains(out, "parameter=readBuffer") {
				t.Fatalf("LogSettings with %s=%q printed %q", logSettingsEnv, value, out)
			}
		})
	}
}

// In fib/log's FormatLine the settings are one record with a table below it,
// a row a parameter, its columns aligned.
func TestLogSettingsLineTable(t *testing.T) {
	var buf bytes.Buffer
	log.Setup(log.Config{Output: &buf})
	defer log.SetLogger(nil)
	config := DefaultConfig()
	config.ReadBufferSize = 64 << 10
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	engine.LogSettings()
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	parameters := engine.Settings().Parameters()
	if len(lines) != len(parameters)+3 {
		t.Fatalf("got %d lines, want a record, a key, a header and %d rows:\n%s", len(lines), len(parameters), buf.String())
	}
	if !strings.Contains(lines[0], "INFO  fib: engine config engine=fib parameters=") || lines[1] != "\tsettings:" {
		t.Fatalf("record %q, %q", lines[0], lines[1])
	}
	value, source, about := strings.Index(lines[2], "value"), strings.Index(lines[2], "source"), strings.Index(lines[2], "about")
	for i, p := range parameters {
		row := lines[i+3]
		if !strings.HasPrefix(row, "\t\t"+p.Name+" ") || !strings.HasPrefix(row[value:], p.Value+"  ") ||
			!strings.HasPrefix(row[source:], p.Source+" ") || row[about:] != p.About {
			t.Errorf("row %q for %+v", row, p)
		}
	}
}
