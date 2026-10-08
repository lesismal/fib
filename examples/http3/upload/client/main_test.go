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
	"github.com/lesismal/fib/examples/http/upload/uploadtest"
	"github.com/lesismal/fib/http3"
)

func TestRunUploadsOverHTTP3(t *testing.T) {
	serverTLS, _, certFile := uploadtest.TLS(t)
	dir := t.TempDir()
	config := http3.DefaultConfig()
	config.TLSConfig = serverTLS
	config.StreamRequestBody = true
	engineConfig := fib.DefaultConfig()
	engineConfig.Network = "udp"
	engineConfig.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(engineConfig, http3.NewHandlerWithConfig(config, service.Handler(dir)))
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := engine.LocalUDPAddr()
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	}()

	path := filepath.Join(t.TempDir(), "in.bin")
	data := uploadtest.Bytes(3<<20+5, 7)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	url := "https://" + addr.String()
	for _, mode := range []string{"upload", "resume", "echo"} {
		var out bytes.Buffer
		args := []string{"-url", url, "-ca", certFile, "-file", path, "-mode", mode, "-name", mode + ".bin", "-chunk", "1000000"}
		if err := run(args, &out); err != nil {
			t.Fatalf("%s: %v: %s", mode, err, out.String())
		}
		if !strings.Contains(out.String(), "OK over HTTP/3.0") {
			t.Fatalf("%s: output %q", mode, out.String())
		}
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "resume.bin")); !bytes.Equal(stored, data) {
		t.Fatal("resumed file differs")
	}
}

func TestRunErrors(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"-nope"}, &out); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run([]string{"-ca", filepath.Join(t.TempDir(), "missing.pem")}, &out); err == nil {
		t.Error("trusted a certificate that is not there")
	}
	_, _, certFile := uploadtest.TLS(t)
	if err := run([]string{"-ca", certFile, "-mode", "nonsense"}, &out); err == nil {
		t.Error("accepted an unknown mode")
	}
}

func TestMainReportsAnError(t *testing.T) {
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	var got error
	fatal = func(err error) { got = err }
	os.Args = []string{"client", "-nope"}
	main()
	if got == nil {
		t.Fatal("fatal was not called")
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
