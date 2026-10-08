//go:build linux || darwin || windows

package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/http/upload/service"
	fibhttp "github.com/lesismal/fib/http"
)

func TestRunUploadsAFile(t *testing.T) {
	dir := t.TempDir()
	engineConfig := fib.DefaultConfig()
	engineConfig.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(engineConfig, fibhttp.NewHandlerWithConfig(service.Config(0), service.Handler(dir)))
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := engine.LocalAddr()
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	}()

	path := filepath.Join(t.TempDir(), "in.bin")
	data := bytes.Repeat([]byte("client "), 1<<16)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"-url", "http://" + addr.String(), "-file", path}, &out); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	if !strings.Contains(out.String(), "OK over HTTP/1.1") {
		t.Fatalf("output %q", out.String())
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "in.bin")); !bytes.Equal(stored, data) {
		t.Fatal("stored file differs")
	}
	if err := run([]string{"-nope"}, &out); err == nil {
		t.Fatal("accepted an unknown flag")
	}
}

func TestMainReportsAnError(t *testing.T) {
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	var got error
	fatal = func(err error) { got = err }
	os.Args = []string{"client", "-mode", "nonsense"}
	main()
	if got == nil || !strings.Contains(got.Error(), "nonsense") {
		t.Fatalf("fatal got %v", got)
	}
}

func TestFatalPrintsAndExits(t *testing.T) {
	if os.Getenv("CLIENT_FATAL") == "1" {
		fatal(errors.New("exit now"))
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestFatalPrintsAndExits")
	cmd.Env = append(os.Environ(), "CLIENT_FATAL=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "error: exit now") {
		t.Fatalf("child: %v %q", err, out)
	}
}
