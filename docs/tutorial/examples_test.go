package tutorial

import (
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// program is a complete Go program found in a code block: a block whose
// first code line is package main and which holds no build constraint.
type program struct {
	code string
	file string
	line int
}

var (
	goBlock     = regexp.MustCompile("(?s)```go\n(.*?)\n```")
	buildTags   = regexp.MustCompile(`(?m)^//go:build|^// \+build`)
	packageMain = regexp.MustCompile(`(?m)^package main\b`)
)

// programsByFile collects the complete programs of the tutorial, deduplicated
// by content: the English and the Chinese page of a chapter hold the same
// code, which only needs building once. The second result counts the programs
// of each page before deduplication, which is what the strict check needs: the
// Chinese page of a chapter does show a complete program even though its code
// is the English page's and only built once.
func programsByFile(t *testing.T, files []string) ([]program, map[string]int) {
	t.Helper()
	var out []program
	counts := map[string]int{}
	seen := map[[32]byte]bool{}
	for _, file := range files {
		text := readPage(t, file)
		for _, m := range goBlock.FindAllStringSubmatchIndex(text, -1) {
			code := text[m[2]:m[3]]
			head := code
			if i := strings.Index(head, "\n"); i >= 0 {
				// The package clause is in the first few lines; a fragment
				// never has one.
				head = head[:min(len(head), 300)]
			}
			if !packageMain.MatchString(head) {
				continue
			}
			if buildTags.MatchString(code) {
				// Built only on some systems; the reader runs it there. The
				// tutorial documents that a block with a build constraint is
				// not compiled by this test.
				continue
			}
			counts[file]++
			sum := sha256.Sum256([]byte(strings.TrimSpace(code)))
			if seen[sum] {
				continue
			}
			seen[sum] = true
			line := 1 + strings.Count(text[:m[2]], "\n")
			out = append(out, program{code: code, file: file, line: line})
		}
	}
	return out, counts
}

// TestCodeExamples builds every complete Go program the tutorial shows, in a
// temporary module that depends on this checkout, so that no sample is stale
// or invented.
func TestCodeExamples(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command in PATH")
	}
	files := chapters(t)
	programs, counts := programsByFile(t, files)
	if len(programs) == 0 {
		t.Fatal("no complete Go programs found in docs/tutorial")
	}
	// A strict run also asks that the tutorial stays a code tutorial: every
	// chapter shows at least one complete program. It is not on by default so
	// that a writer can run the test while only some chapters exist.
	if os.Getenv("FIB_TUTORIAL_STRICT") != "" {
		// The chapters whose examples need generated protobuf code, a
		// browser or an external media peer: a fragment is the honest form
		// there, and the package docs show the buildable pieces.
		optional := map[string]bool{
			"11-grpc.md":         true,
			"11-grpc.zh-CN.md":   true,
			"16-webrtc.md":       true,
			"16-webrtc.zh-CN.md": true,
		}
		for _, f := range files {
			base := filepath.Base(f)
			if strings.HasPrefix(base, ".") || strings.HasPrefix(base, "README") {
				continue
			}
			if counts[f] == 0 && !optional[f] {
				t.Errorf("%s: no complete Go program; every chapter shows at least one", f)
			}
		}
	}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	goMod := fmt.Sprintf(`module tutorialexamples

go 1.27.3

require github.com/lesismal/fib v0.0.0

replace github.com/lesismal/fib => %q
`, filepath.ToSlash(root))
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o644); err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(programs))
	for i, p := range programs {
		name := fmt.Sprintf("ex%03d", i)
		names[i] = name
		sub := filepath.Join(dir, name)
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(sub, "main.go"), []byte(p.code+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(goBin, "build", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=", "GOWORK=off", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		var b strings.Builder
		fmt.Fprintf(&b, "building the tutorial's %d complete programs failed: %v\n\n%s\n", len(programs), err, out)
		for i, p := range programs {
			fmt.Fprintf(&b, "%s -> %s:%d\n", names[i], p.file, p.line)
		}
		t.Fatal(b.String())
	}
	t.Logf("built %d complete programs from %d pages", len(programs), len(files))
}
