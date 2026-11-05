//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdhttp "net/http"
	"os"
	"path/filepath"
	"testing"
)

// localName refuses, for Windows, the names that would leave the root there
// or name something other than a file under it, whichever system it runs
// on, and on any other system only a name with a NUL.
func TestFileCacheLocalName(t *testing.T) {
	for _, name := range []string{
		`..\..\windows\win.ini`, `a\..\..\b`, `sub\file`, `C:\windows\win.ini`, `C:file`, `file.txt:stream`,
		"a\x00b",
	} {
		if localName(name, true) {
			t.Errorf("%q is local on Windows", name)
		}
	}
	for _, name := range []string{"a.css", "sub/file.txt", "a b/c-d_e.f", "x..y"} {
		if !localName(name, true) || !localName(name, false) {
			t.Errorf("%q is not local", name)
		}
	}
	for _, name := range []string{`a\b`, "a:b"} {
		if !localName(name, false) {
			t.Errorf("%q is not local where '\\' and ':' are ordinary bytes", name)
		}
	}
	if localName("a\x00b", false) {
		t.Error("a name with a NUL is local")
	}
	for _, name := range []string{"NUL", "sub/CON", "COM1.txt", "com\u00b9", "lpt9.tar.gz", "aux ", "CONOUT$", "nul .txt"} {
		if localName(name, true) {
			t.Errorf("device name %q is local on Windows", name)
		}
		if !localName(name, false) {
			t.Errorf("%q is not local where it names no device", name)
		}
	}
	for _, name := range []string{"COM10", "console", "nul2.txt", "lpt", "auxiliary/x"} {
		if !localName(name, true) {
			t.Errorf("%q is not local on Windows", name)
		}
	}
}

// A name with backslashes, as "..%5C" in a request target decodes to, does
// not reach a file outside the root, on Windows or anywhere else.
func TestFileCacheBackslashTraversal(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ok.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	fc, err := NewFileCache(FileCacheConfig{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fc.Close() })
	addr := serveHTTP1(t, func(c *Context) { fc.ServeFile(c, c.Request.URL.Path[len("/static/"):]) })
	for _, name := range []string{"..%5Csecret.txt", "sub%5C..%5C..%5Csecret.txt", "ok.txt%00"} {
		if resp, body := getStatic(t, addr, name); resp.StatusCode != stdhttp.StatusNotFound {
			t.Errorf("%s: %d %q, want 404", name, resp.StatusCode, body)
		}
	}
	if resp, body := getStatic(t, addr, "ok.txt"); resp.StatusCode != stdhttp.StatusOK || body != "ok" {
		t.Fatalf("ok.txt: %d %q", resp.StatusCode, body)
	}
}
