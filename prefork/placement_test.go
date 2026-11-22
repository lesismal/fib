package prefork

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"testing/fstest"
)

func TestParseCPUList(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []int
		bad  bool
	}{
		{in: "", want: nil},
		{in: "\n", want: nil},
		{in: "3\n", want: []int{3}},
		{in: "0-3,8,10-11", want: []int{0, 1, 2, 3, 8, 10, 11}},
		{in: "x", bad: true},
		{in: "3-1", bad: true},
		{in: "1,", bad: true},
	} {
		got, err := parseCPUList(tc.in)
		if (err != nil) != tc.bad || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseCPUList(%q) = %v, %v; want %v, bad %v", tc.in, got, err, tc.want, tc.bad)
		}
	}
}

// fakeSysfs is /sys/devices/system of a machine whose nodes hold the given
// CPU lists, each CPU of a core with the CPU smt away from it when smt is
// above zero.
func fakeSysfs(nodes map[int]string, cpus, smt int) fstest.MapFS {
	fsys := fstest.MapFS{}
	for id, list := range nodes {
		fsys["node/node"+strconv.Itoa(id)+"/cpulist"] = &fstest.MapFile{Data: []byte(list + "\n")}
	}
	// Files beside the node directories that are not nodes.
	fsys["node/online"] = &fstest.MapFile{Data: []byte("0-1\n")}
	fsys["node/has_cpu"] = &fstest.MapFile{Data: []byte("0-1\n")}
	for cpu := range cpus {
		siblings := strconv.Itoa(cpu)
		if smt > 0 {
			core := cpu % smt
			siblings = fmt.Sprintf("%d,%d", core, core+smt)
		}
		fsys["cpu/cpu"+strconv.Itoa(cpu)+"/topology/thread_siblings_list"] = &fstest.MapFile{Data: []byte(siblings + "\n")}
	}
	return fsys
}

func seq(lo, hi int) []int {
	var s []int
	for i := lo; i <= hi; i++ {
		s = append(s, i)
	}
	return s
}

func TestReadTopology(t *testing.T) {
	// Two nodes of 4 cores, 2 threads each: CPU c and c+8 are siblings,
	// node 0 has cores 0-3, node 1 cores 4-7, and node 2 has memory only.
	fsys := fakeSysfs(map[int]string{0: "0-3,8-11", 1: "4-7,12-15", 2: ""}, 16, 8)
	nodes, err := readTopology(fsys, seq(0, 15))
	if err != nil {
		t.Fatal(err)
	}
	want := []numaNode{
		{id: 0, cpus: []int{0, 8, 1, 9, 2, 10, 3, 11}},
		{id: 1, cpus: []int{4, 12, 5, 13, 6, 14, 7, 15}},
	}
	if !reflect.DeepEqual(nodes, want) {
		t.Fatalf("nodes = %v, want %v", nodes, want)
	}

	// A cpuset that leaves node 1 out, and one of node 0's siblings.
	nodes, err = readTopology(fsys, []int{0, 1, 2, 3, 8, 9, 10})
	if err != nil {
		t.Fatal(err)
	}
	want = []numaNode{{id: 0, cpus: []int{0, 8, 1, 9, 2, 10, 3}}}
	if !reflect.DeepEqual(nodes, want) {
		t.Fatalf("nodes = %v, want %v", nodes, want)
	}

	// No node directory: a kernel without NUMA.
	nodes, err = readTopology(fstest.MapFS{}, []int{3, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	want = []numaNode{{id: 0, cpus: []int{1, 2, 3}}}
	if !reflect.DeepEqual(nodes, want) {
		t.Fatalf("nodes = %v, want %v", nodes, want)
	}

	if _, err := readTopology(fsys, nil); err == nil {
		t.Fatal("no usable CPUs: want an error")
	}
	bad := fakeSysfs(map[int]string{0: "zero"}, 1, 0)
	if _, err := readTopology(bad, []int{0}); err == nil {
		t.Fatal("bad cpulist: want an error")
	}
}

func TestPlan(t *testing.T) {
	two := []numaNode{{id: 0, cpus: seq(0, 7)}, {id: 1, cpus: seq(8, 15)}}
	for _, tc := range []struct {
		name  string
		nodes []numaNode
		n     int
		p     Placement
		want  []slot
	}{
		{name: "node even", nodes: two, n: 4, p: PlaceNode, want: []slot{
			{0, seq(0, 7)}, {0, seq(0, 7)}, {1, seq(8, 15)}, {1, seq(8, 15)},
		}},
		{name: "cpu even", nodes: two, n: 4, p: PlaceCPU, want: []slot{
			{0, seq(0, 3)}, {0, seq(4, 7)}, {1, seq(8, 11)}, {1, seq(12, 15)},
		}},
		{name: "cpu odd", nodes: two, n: 3, p: PlaceCPU, want: []slot{
			{0, seq(0, 3)}, {0, seq(4, 7)}, {1, seq(8, 15)},
		}},
		{name: "one child", nodes: two, n: 1, p: PlaceCPU, want: []slot{{0, seq(0, 7)}}},
		{name: "more children than CPUs", nodes: []numaNode{{id: 3, cpus: []int{5, 6}}}, n: 3, p: PlaceCPU, want: []slot{
			{3, []int{5}}, {3, []int{5}}, {3, []int{6}},
		}},
		{name: "uneven nodes", nodes: []numaNode{{id: 0, cpus: seq(0, 1)}, {id: 1, cpus: seq(2, 7)}}, n: 4, p: PlaceCPU, want: []slot{
			{0, seq(0, 1)}, {1, seq(2, 3)}, {1, seq(4, 5)}, {1, seq(6, 7)},
		}},
		{name: "largest remainder", nodes: []numaNode{{id: 0, cpus: seq(0, 2)}, {id: 1, cpus: seq(3, 8)}}, n: 2, p: PlaceNode, want: []slot{
			{0, seq(0, 2)}, {1, seq(3, 8)},
		}},
		{name: "remainder ties to lower node", nodes: []numaNode{{id: 0, cpus: seq(0, 1)}, {id: 1, cpus: seq(2, 3)}, {id: 2, cpus: seq(4, 5)}}, n: 2, p: PlaceNode, want: []slot{
			{0, seq(0, 1)}, {1, seq(2, 3)},
		}},
	} {
		got := plan(tc.nodes, tc.n, tc.p)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: plan = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPlanCoversEveryCPUOnce(t *testing.T) {
	nodes := []numaNode{{id: 0, cpus: seq(0, 47)}, {id: 1, cpus: seq(48, 95)}, {id: 2, cpus: seq(96, 127)}}
	for n := 1; n <= 128; n++ {
		slots := plan(nodes, n, PlaceCPU)
		if len(slots) != n {
			t.Fatalf("n=%d: %d slots", n, len(slots))
		}
		seen := map[int]int{}
		for _, s := range slots {
			if len(s.cpus) == 0 {
				t.Fatalf("n=%d: a slot without CPUs", n)
			}
			for _, cpu := range s.cpus {
				seen[cpu]++
			}
		}
		// Every node with a child has all its CPUs given out, once each.
		for _, s := range slots {
			for _, node := range nodes {
				if node.id != s.node {
					continue
				}
				for _, cpu := range node.cpus {
					if seen[cpu] != 1 {
						t.Fatalf("n=%d: CPU %d given %d times", n, cpu, seen[cpu])
					}
				}
			}
		}
	}
}

func TestPlacementString(t *testing.T) {
	for p, want := range map[Placement]string{PlaceAny: "PlaceAny", PlaceNode: "PlaceNode", PlaceCPU: "PlaceCPU", 7: "Placement(7)"} {
		if got := p.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(p), got, want)
		}
	}
}
