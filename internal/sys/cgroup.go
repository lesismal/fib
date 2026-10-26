package sys

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// cgroupMemoryLimit reads the smallest memory limit at or above the cgroup in
// /proc/self/cgroup, whose path is relative to the given roots.
func cgroupMemoryLimit(procPath, rootV2, rootV1 string) int64 {
	limit := int64(0)
	add := func(n int64) {
		if n > 0 && (limit <= 0 || n < limit) {
			limit = n
		}
	}
	if data, err := os.ReadFile(procPath); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			// Version 2 is the line with no controllers: "0::/path".
			if path, ok := strings.CutPrefix(line, "0::"); ok {
				for _, p := range cgroupPaths(rootV2, path, "memory.max") {
					add(readLimit(p))
				}
				continue
			}
			parts := strings.SplitN(line, ":", 3)
			if len(parts) != 3 || !hasController(parts[1], "memory") {
				continue
			}
			// Version 1 names the memory controller's own hierarchy, as in
			// "5:memory:/docker/abc".
			for _, p := range cgroupPaths(rootV1, parts[2], "memory.limit_in_bytes") {
				add(readLimit(p))
			}
		}
	}
	// The roots cover a process whose /proc/self/cgroup could not be read, and
	// a cgroup whose path is not below either root.
	add(readLimit(filepath.Join(rootV2, "memory.max")))
	add(readLimit(filepath.Join(rootV1, "memory.limit_in_bytes")))
	return limit
}

// cgroupPaths lists the file at path's cgroup and at each ancestor's, ending
// with the root's.
func cgroupPaths(root, cgroup, file string) []string {
	dir := filepath.Join(root, strings.TrimPrefix(filepath.Clean("/"+cgroup), "/"))
	root = filepath.Clean(root)
	paths := make([]string, 0, 8)
	for {
		paths = append(paths, filepath.Join(dir, file))
		if dir == root {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir || !strings.HasPrefix(parent+string(filepath.Separator), root+string(filepath.Separator)) {
			// The path left the root, as ".." in a malformed file could.
			break
		}
		dir = parent
	}
	return paths
}

func hasController(list, name string) bool {
	for list != "" {
		var part string
		if i := strings.IndexByte(list, ','); i >= 0 {
			part, list = list[:i], list[i+1:]
		} else {
			part, list = list, ""
		}
		if part == name {
			return true
		}
	}
	return false
}

// readLimit reads one memory limit file: zero for a missing one, "max", or a
// version 1 "unlimited" value (a page-aligned number near 1<<63).
func readLimit(path string) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<60 {
		return 0
	}
	return n
}
