package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"0": 0, "123": 123, "5B": 5, " 7 KiB ": 7 << 10, "2MiB": 2 << 20, "1GiB": 1 << 30,
		"3KB": 3000, "4MB": 4e6, "1GB": 1e9,
	}
	for in, want := range cases {
		if got, err := parseSize(in); err != nil || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "x", "-1", "1.5MB", "MB"} {
		if _, err := parseSize(in); err == nil {
			t.Errorf("parseSize(%q) succeeded", in)
		}
	}
}

func TestRunWritesAReproducibleFile(t *testing.T) {
	dir := t.TempDir()
	var first string
	for i, seed := range []string{"1", "1", "2"} {
		path := filepath.Join(dir, fmt.Sprintf("f%d.bin", i))
		var out bytes.Buffer
		if err := run([]string{"-o", path, "-size", "3MiB", "-seed", seed}, &out); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) != 3<<20 {
			t.Fatalf("file: %d bytes, %v", len(data), err)
		}
		want := fmt.Sprintf("%s bytes=%d sha256=%x\n", path, len(data), sha256.Sum256(data))
		if out.String() != want {
			t.Fatalf("output %q, want %q", out.String(), want)
		}
		switch i {
		case 0:
			first = string(data)
		case 1:
			if string(data) != first {
				t.Fatal("the same seed made a different file")
			}
		case 2:
			if string(data) == first {
				t.Fatal("another seed made the same file")
			}
		}
	}
}

type failing struct {
	writeErr, closeErr error
}

func (f failing) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return len(p), nil
}
func (f failing) Close() error { return f.closeErr }

func TestRunErrors(t *testing.T) {
	boom := errors.New("boom")
	old := create
	defer func() { create = old }()

	if err := run([]string{"-nope"}, io.Discard); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run([]string{"-size", "lots"}, io.Discard); err == nil {
		t.Error("accepted a bad size")
	}
	if err := run([]string{"-o", filepath.Join(t.TempDir(), "no", "dir", "f"), "-size", "1"}, io.Discard); err == nil {
		t.Error("wrote into a directory that is not there")
	}
	create = func(string) (io.WriteCloser, error) { return failing{writeErr: boom}, nil }
	if err := run([]string{"-size", "1MiB"}, io.Discard); !errors.Is(err, boom) {
		t.Errorf("write failure = %v", err)
	}
	create = func(string) (io.WriteCloser, error) { return failing{closeErr: boom}, nil }
	if err := run([]string{"-size", "1MiB"}, io.Discard); !errors.Is(err, boom) {
		t.Errorf("close failure = %v", err)
	}
	create = func(string) (io.WriteCloser, error) { return failing{}, nil }
	if err := run([]string{"-size", "1"}, failingWriter{boom}); !errors.Is(err, boom) {
		t.Errorf("output failure = %v", err)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestMainReportsAnError(t *testing.T) {
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	var got error
	fatal = func(err error) { got = err }
	os.Args = []string{"mkfile", "-size", "nonsense"}
	main()
	if got == nil || !strings.Contains(got.Error(), "nonsense") {
		t.Fatalf("fatal got %v", got)
	}
}

func TestFatalPrintsAndExits(t *testing.T) {
	// The default fatal exits the process, so it is exercised in a child.
	if os.Getenv("MKFILE_FATAL") == "1" {
		fatal(errors.New("exit now"))
		return
	}
	cmd := execSelf(t, "-test.run=TestFatalPrintsAndExits")
	cmd.Env = append(os.Environ(), "MKFILE_FATAL=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(out), "error: exit now") {
		t.Fatalf("child: %v %q", err, out)
	}
}

func execSelf(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	return exec.Command(os.Args[0], args...)
}
