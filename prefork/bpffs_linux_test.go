package prefork

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// setupBPFFS makes a directory of the run's own under bpffs, which
// removeBPFFS takes away again, or reports "" where bpffs is not mounted or
// the directory cannot be made there.
func TestSetupBPFFS(t *testing.T) {
	dir := setupBPFFS()
	if dir == "" {
		if fi, err := os.Stat(bpffsMount); err == nil && fi.IsDir() {
			t.Skip("bpffs is mounted, but this process cannot make a directory under it")
		}
		t.Skip("bpffs is not mounted")
	}
	if filepath.Dir(dir) != bpffsMount || !strings.HasPrefix(filepath.Base(dir), "fib-prefork-") {
		t.Errorf("setupBPFFS = %q", dir)
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Fatalf("the directory was not made: %v", err)
	}
	removeBPFFS(dir)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("removeBPFFS left %s: %v", dir, err)
	}
}
