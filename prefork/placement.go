package prefork

import (
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strconv"
	"strings"
)

// Placement says which CPUs each child may run on. Whatever it says, a child
// is kept to CPUs the master may use itself: its affinity mask, which a
// cgroup's cpuset or taskset narrows.
type Placement int

const (
	// PlaceAny leaves every child free to run on any CPU the master may use,
	// for the kernel's scheduler to move between them, NUMA nodes included.
	PlaceAny Placement = iota
	// PlaceNode keeps each child on the CPUs of one NUMA node, the children
	// spread over the nodes in proportion to how many of those CPUs each
	// has. A child's threads then touch its heap first from its node's CPUs,
	// and Linux allocates a page on the node of the CPU that first touches
	// it, so the child's memory is its node's without a memory policy; see
	// Config.BindMemory for one that holds when the node runs short. The
	// kernel's scheduler still moves a child's threads between its node's
	// CPUs.
	PlaceNode
	// PlaceCPU is PlaceNode with each node's CPUs divided between the
	// children placed on it, each child keeping a share of its own, as nearly
	// equal as the count allows and with a core's SMT siblings in the same
	// share, so that children do not take turns on a CPU or its caches. A
	// node with more children than CPUs gives some children the same CPU.
	PlaceCPU
)

// String names p as the constant that declares it.
func (p Placement) String() string {
	switch p {
	case PlaceAny:
		return "PlaceAny"
	case PlaceNode:
		return "PlaceNode"
	case PlaceCPU:
		return "PlaceCPU"
	}
	return "Placement(" + strconv.Itoa(int(p)) + ")"
}

// nodeEnv carries, to a child placed on a node, the node's number; see Node.
const nodeEnv = "FIB_PREFORK_NODE"

// Node reports, in a child that Config.Placement placed on a NUMA node, the
// node's number, and -1 in any other process.
func Node() int {
	n, err := strconv.Atoi(os.Getenv(nodeEnv))
	if err != nil || n < 0 || !IsChild() {
		return -1
	}
	return n
}

// A numaNode is one NUMA node's number and the CPUs of it the master may use,
// in the order shares are cut from: ascending, except that a core's SMT
// siblings follow its lowest-numbered CPU.
type numaNode struct {
	id   int
	cpus []int
}

// A slot is where one child runs: the node and the CPUs it may use.
type slot struct {
	node int
	cpus []int
}

// readTopology reads the NUMA nodes from sysfs, the system directory of
// /sys/devices/system, keeping of each the CPUs usable says the master may
// use and leaving out a node with none: one with memory alone, CXL or
// persistent memory, or whose CPUs a cpuset keeps from the master. A kernel
// without NUMA has no node directory, and its usable CPUs make one node, 0.
func readTopology(sysfs fs.FS, usable []int) ([]numaNode, error) {
	if len(usable) == 0 {
		return nil, fmt.Errorf("no usable CPUs")
	}
	allowed := make(map[int]bool, len(usable))
	for _, cpu := range usable {
		allowed[cpu] = true
	}
	var nodes []numaNode
	entries, err := fs.ReadDir(sysfs, "node")
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, e := range entries {
		id, err := strconv.Atoi(strings.TrimPrefix(e.Name(), "node"))
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "node") || err != nil || id < 0 {
			continue
		}
		b, err := fs.ReadFile(sysfs, "node/"+e.Name()+"/cpulist")
		if err != nil {
			return nil, err
		}
		list, err := parseCPUList(string(b))
		if err != nil {
			return nil, fmt.Errorf("node%d cpulist: %w", id, err)
		}
		var cpus []int
		for _, cpu := range list {
			if allowed[cpu] {
				cpus = append(cpus, cpu)
			}
		}
		if len(cpus) > 0 {
			nodes = append(nodes, numaNode{id: id, cpus: siblingOrder(sysfs, cpus)})
		}
	}
	if len(nodes) == 0 {
		cpus := slices.Clone(usable)
		slices.Sort(cpus)
		return []numaNode{{id: 0, cpus: siblingOrder(sysfs, cpus)}}, nil
	}
	slices.SortFunc(nodes, func(a, b numaNode) int { return a.id - b.id })
	return nodes, nil
}

// siblingOrder orders cpus, ascending, so that each core's SMT siblings among
// them follow the first of them, by each CPU's thread_siblings_list. A CPU
// whose list cannot be read is a core of its own.
func siblingOrder(sysfs fs.FS, cpus []int) []int {
	in := make(map[int]bool, len(cpus))
	for _, cpu := range cpus {
		in[cpu] = true
	}
	ordered := make([]int, 0, len(cpus))
	seen := make(map[int]bool, len(cpus))
	for _, cpu := range cpus {
		if seen[cpu] {
			continue
		}
		seen[cpu] = true
		ordered = append(ordered, cpu)
		b, err := fs.ReadFile(sysfs, "cpu/cpu"+strconv.Itoa(cpu)+"/topology/thread_siblings_list")
		if err != nil {
			continue
		}
		siblings, err := parseCPUList(string(b))
		if err != nil {
			continue
		}
		for _, s := range siblings {
			if in[s] && !seen[s] {
				seen[s] = true
				ordered = append(ordered, s)
			}
		}
	}
	return ordered
}

// parseCPUList parses the kernel's list format, as in "0-7,16-23" or "3": a
// comma-separated list of CPUs and inclusive ranges of them. Empty is no
// CPUs.
func parseCPUList(s string) ([]int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var cpus []int
	for part := range strings.SplitSeq(s, ",") {
		lo, hi, isRange := strings.Cut(part, "-")
		first, err := strconv.Atoi(lo)
		if err != nil || first < 0 {
			return nil, fmt.Errorf("bad CPU list %q", s)
		}
		last := first
		if isRange {
			if last, err = strconv.Atoi(hi); err != nil || last < first {
				return nil, fmt.Errorf("bad CPU list %q", s)
			}
		}
		for cpu := first; cpu <= last; cpu++ {
			cpus = append(cpus, cpu)
		}
	}
	return cpus, nil
}

// plan places n children on nodes as p says, child i (from 1) in slot i-1.
// Each node takes a share of the children in proportion to its CPUs, by
// largest remainder, ties going to the lower-numbered node, and its children
// are numbered consecutively, the lower-numbered node's first. PlaceNode
// gives each child its whole node; PlaceCPU cuts the node's CPUs, in order,
// into as many runs as it has children.
func plan(nodes []numaNode, n int, p Placement) []slot {
	total := 0
	for _, node := range nodes {
		total += len(node.cpus)
	}
	counts := make([]int, len(nodes))
	given := 0
	for i, node := range nodes {
		counts[i] = n * len(node.cpus) / total
		given += counts[i]
	}
	for given < n {
		best := -1
		bestRem := -1
		for i, node := range nodes {
			// The remainder of n*cpus/total, compared over the common
			// denominator; a node already given its extra child is skipped.
			if rem := n * len(node.cpus) % total; counts[i] == n*len(node.cpus)/total && rem > bestRem {
				best, bestRem = i, rem
			}
		}
		counts[best]++
		given++
	}
	slots := make([]slot, 0, n)
	for i, node := range nodes {
		m := counts[i]
		for j := range m {
			cpus := node.cpus
			if p == PlaceCPU {
				c := len(cpus)
				lo, hi := j*c/m, (j+1)*c/m
				if hi <= lo {
					hi = lo + 1
				}
				cpus = cpus[lo:hi]
			}
			slots = append(slots, slot{node: node.id, cpus: slices.Clone(cpus)})
		}
	}
	return slots
}
