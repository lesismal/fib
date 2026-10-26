package sys

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCgroupMemoryLimitV2(t *testing.T) {
	dir := t.TempDir()
	proc, root := filepath.Join(dir, "cgroup"), filepath.Join(dir, "cgroup2")
	writeTestFile(t, proc, "0::/a/b\n")
	// The cgroup's own limit is the smallest, and a parent above it also
	// counts: a process is held to the lowest limit on its path.
	writeTestFile(t, filepath.Join(root, "memory.max"), "8589934592\n")
	writeTestFile(t, filepath.Join(root, "a", "memory.max"), "4294967296\n")
	writeTestFile(t, filepath.Join(root, "a", "b", "memory.max"), "2147483648\n")

	if got, want := cgroupMemoryLimit(proc, root, filepath.Join(dir, "cgroup1")), int64(2147483648); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}

func TestCgroupMemoryLimitSkipsMaxAndUnlimited(t *testing.T) {
	dir := t.TempDir()
	proc, root, v1 := filepath.Join(dir, "cgroup"), filepath.Join(dir, "cgroup2"), filepath.Join(dir, "cgroup1")
	writeTestFile(t, proc, "0::/\n5:memory:/docker/x\n")
	writeTestFile(t, filepath.Join(root, "memory.max"), "max\n")
	// Version 1 writes a page-aligned value near 1<<63 for no limit.
	writeTestFile(t, filepath.Join(v1, "docker", "x", "memory.limit_in_bytes"), "9223372036854771712\n")
	writeTestFile(t, filepath.Join(v1, "memory.limit_in_bytes"), "9223372036854771712\n")

	if got := cgroupMemoryLimit(proc, root, v1); got != 0 {
		t.Fatalf("limit = %d, want 0 for an unlimited cgroup", got)
	}
}

func TestCgroupMemoryLimitV1(t *testing.T) {
	dir := t.TempDir()
	proc, root, v1 := filepath.Join(dir, "cgroup"), filepath.Join(dir, "cgroup2"), filepath.Join(dir, "cgroup1")
	writeTestFile(t, proc, "4:memory,cpu:/docker/x\n")
	writeTestFile(t, filepath.Join(v1, "docker", "x", "memory.limit_in_bytes"), "1073741824\n")

	if got, want := cgroupMemoryLimit(proc, root, v1), int64(1073741824); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}

// A missing /proc/self/cgroup still leaves the roots, which is where a
// process outside any named cgroup lives.
func TestCgroupMemoryLimitFallsBackToTheRoot(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "cgroup2")
	writeTestFile(t, filepath.Join(root, "memory.max"), "536870912\n")

	if got, want := cgroupMemoryLimit(filepath.Join(dir, "missing"), root, filepath.Join(dir, "cgroup1")), int64(536870912); got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}

func writeTestFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}
