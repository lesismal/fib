package prefork_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/reuseportbpf"
	"github.com/lesismal/fib/prefork"
)

// The tests start this test binary again as the children; TestMain runs the
// child's part there instead of the tests. The mode and the address travel
// in the environment, which the children inherit.
const (
	modeEnv  = "PREFORK_TEST_MODE"
	addrEnv  = "PREFORK_TEST_ADDR"
	readyEnv = "PREFORK_TEST_READY"
)

func TestMain(m *testing.M) {
	if prefork.IsChild() {
		os.Exit(runChild())
	}
	os.Exit(m.Run())
}

// runChild serves the test's address, answering every connection with the
// child's index and GOMAXPROCS, until it is asked to stop; in "fail" mode the
// second child fails at once instead.
func runChild() int {
	err := prefork.Run(prefork.Config{}, func(ctx context.Context) error {
		switch os.Getenv(modeEnv) {
		case "fail":
			if prefork.Child() == 2 {
				return errors.New("child failed on purpose")
			}
		case "exit":
			return nil
		case "place":
			report(placeReport())
			<-ctx.Done()
			return nil
		case "stopfail":
			announceReady()
			<-ctx.Done()
			return errors.New("child failed to stop on purpose")
		case "hang":
			announceReady()
			time.Sleep(time.Minute)
			return nil
		case "sigterm":
			// SIGTERM's default action ends the child, as it would a
			// program that never asks to be told of it.
			signal.Reset(syscall.SIGTERM)
			announceReady()
			time.Sleep(time.Minute)
			return nil
		}
		reply := fmt.Sprintf("%d %d %d", prefork.Child(), runtime.GOMAXPROCS(0), prefork.Children())
		config := fib.DefaultConfig()
		if os.Getenv(modeEnv) == "balance" {
			// The child also says where it found its socket to the master's
			// reuseport broker.
			config.BalanceReusePort = true
			reply += " " + os.Getenv(reuseportbpf.BrokerEnv)
		}
		engine, err := fib.New(config, fib.Binding{Addr: os.Getenv(addrEnv), Handler: fib.HandlerFuncs{Data: func(c *fib.Connection, _ []byte) {
			_ = c.Send([]byte(reply))
		}}})
		if err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- engine.Run() }()
		select {
		case <-ctx.Done():
			engine.Stop()
			<-done
			return engine.Close()
		case err := <-done:
			return err
		}
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// announceReady tells the test that this child is running serve, and so
// past the start-up a signal would end it in, by connecting to the address
// the test listens on.
func announceReady() {
	if conn, err := net.Dial("tcp", os.Getenv(readyEnv)); err == nil {
		_ = conn.Close()
	}
}

// report sends the test, at the address it listens on for children to
// announce themselves, what this child has to say.
func report(s string) {
	if conn, err := net.Dial("tcp", os.Getenv(readyEnv)); err == nil {
		_, _ = conn.Write([]byte(s))
		_ = conn.Close()
	}
}

// placeReport says where this child runs: its index, its node, its
// GOMAXPROCS, the CPUs it may use and its memory policy, as /proc shows the
// first mapping's, or "-" where /proc has no numa_maps.
func placeReport() string {
	policy := "-"
	if b, err := os.ReadFile("/proc/self/numa_maps"); err == nil {
		if f := strings.Fields(string(b)); len(f) > 1 {
			policy = f[1]
		}
	}
	return fmt.Sprintf("%d %d %d %s %s", prefork.Child(), prefork.Node(), runtime.GOMAXPROCS(0), cpusAllowed("/proc/self/status"), policy)
}

// cpusAllowed reads the Cpus_allowed_list a /proc status file shows.
func cpusAllowed(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "?"
	}
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(line, "Cpus_allowed_list:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return "?"
}

// freeAddr returns a loopback address with a port nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestChildrenShareTheAddress(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv(addrEnv, addr)
	t.Setenv(modeEnv, "serve")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- prefork.RunContext(ctx, prefork.Config{Children: 3, ProcsPerChild: 2}, func(context.Context) error {
			return errors.New("serve ran in the master")
		})
	}()

	// Connections are spread by a hash of their addresses, so new ones
	// reach every child before long.
	seen := map[string]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < 3 && time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Write([]byte("hi"))
		buf := make([]byte, 32)
		n, _ := conn.Read(buf)
		_ = conn.Close()
		if n > 0 {
			reply := strings.Fields(string(buf[:n]))
			if len(reply) != 3 || reply[1] != "2" || reply[2] != "3" {
				t.Fatalf("child answered %q, want its index, GOMAXPROCS 2 and 3 children", buf[:n])
			}
			seen[reply[0]] = true
		}
	}
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("RunContext = %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the children did not stop")
	}
	for i := 1; i <= 3; i++ {
		if !seen[strconv.Itoa(i)] {
			t.Errorf("child %d never answered; answers came from %v", i, seen)
		}
	}
}

func TestOutsidePrefork(t *testing.T) {
	if prefork.IsChild() || prefork.Child() != 0 || prefork.Children() != 1 {
		t.Fatalf("IsChild %v, Child %d, Children %d; want false, 0, 1", prefork.IsChild(), prefork.Child(), prefork.Children())
	}
}

func TestAChildThatFailsStopsTheOthers(t *testing.T) {
	t.Setenv(addrEnv, freeAddr(t))
	t.Setenv(modeEnv, "fail")
	result := make(chan error, 1)
	go func() {
		result <- prefork.RunContext(context.Background(), prefork.Config{Children: 3, ProcsPerChild: 1},
			func(context.Context) error { return errors.New("serve ran in the master") })
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "child 2") {
			t.Fatalf("RunContext = %v, want child 2's failure", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the master did not stop after a child failed")
	}
}

// runUntilReady runs the master in mode with config, waits for its n
// children to announce that they are serving, then cancels its context,
// and returns what RunContext returned.
func runUntilReady(t *testing.T, mode string, n int, config prefork.Config) error {
	t.Helper()
	_, err := runReporting(t, mode, n, config)
	return err
}

// runReporting is runUntilReady that also returns what each child sent when
// it announced itself, in the order they came.
func runReporting(t *testing.T, mode string, n int, config prefork.Config) ([]string, error) {
	t.Helper()
	ready, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ready.Close()
	t.Setenv(readyEnv, ready.Addr().String())
	t.Setenv(modeEnv, mode)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- prefork.RunContext(ctx, config, func(context.Context) error {
			return errors.New("serve ran in the master")
		})
	}()
	var reports []string
	deadline := time.Now().Add(15 * time.Second)
	for len(reports) < n {
		_ = ready.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond))
		conn, err := ready.Accept()
		if err != nil {
			select {
			case err := <-result:
				// The master gave up before every child was up.
				return reports, err
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("a child never announced itself: %v", err)
			}
			continue
		}
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		b, _ := io.ReadAll(conn)
		_ = conn.Close()
		reports = append(reports, string(b))
	}
	cancel()
	select {
	case err := <-result:
		return reports, err
	case <-time.After(15 * time.Second):
		t.Fatal("the master did not return")
		return nil, nil
	}
}

// A child that fails as it is asked to stop fails the master, which reports
// it although stopping was what the master wanted.
func TestAChildThatFailsToStopIsReported(t *testing.T) {
	err := runUntilReady(t, "stopfail", 1, prefork.Config{Children: 1, ProcsPerChild: 1})
	if err == nil || !strings.Contains(err.Error(), "child 1") {
		t.Fatalf("RunContext = %v, want child 1's failure", err)
	}
}

// A child still running ShutdownTimeout after it was asked to stop is killed,
// and the master says so.
func TestAChildThatIgnoresStopIsKilled(t *testing.T) {
	err := runUntilReady(t, "hang", 1, prefork.Config{Children: 1, ProcsPerChild: 1, ShutdownTimeout: 100 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "killed") {
		t.Fatalf("RunContext = %v, want the children killed", err)
	}
}

// A child that SIGTERM ends outright, rather than asking serve to return, has
// stopped as asked.
func TestAChildEndedBySIGTERMStoppedAsAsked(t *testing.T) {
	if err := runUntilReady(t, "sigterm", 1, prefork.Config{Children: 1, ProcsPerChild: 1}); err != nil {
		t.Fatalf("RunContext = %v", err)
	}
}

// BalanceReusePort runs the children as usual where the selectors can be
// made, and where they cannot.
func TestBalanceReusePortRuns(t *testing.T) {
	if err := runUntilReady(t, "sigterm", 1, prefork.Config{Children: 1, ProcsPerChild: 1, BalanceReusePort: true}); err != nil {
		t.Fatalf("RunContext = %v", err)
	}
}

// With BalanceReusePort every child is handed its socket to the master's
// broker, and where this process may make a selector (CAP_BPF), new
// connections go to the children in turn: children of two Ps, with one
// poller each, every one of them a share of the address's group.
func TestBalanceReusePortSpreadsOverChildren(t *testing.T) {
	const children, conns = 4, 40
	addr := freeAddr(t)
	t.Setenv(addrEnv, addr)
	t.Setenv(modeEnv, "balance")
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- prefork.RunContext(ctx, prefork.Config{Children: children, ProcsPerChild: 2, BalanceReusePort: true},
			func(context.Context) error { return errors.New("serve ran in the master") })
	}()
	defer func() {
		cancel()
		select {
		case err := <-result:
			if err != nil {
				t.Errorf("RunContext = %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("the children did not stop")
		}
	}()

	ask := func() (string, bool) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return "", false
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = conn.Write([]byte("hi"))
		buf := make([]byte, 32)
		n, _ := conn.Read(buf)
		reply := strings.Fields(string(buf[:n]))
		if len(reply) != 4 {
			t.Fatalf("child answered %q, want its index, GOMAXPROCS, children and broker", buf[:n])
		}
		if reply[3] != "3" {
			t.Fatalf("child %s found its broker at %q, want descriptor 3", reply[0], reply[3])
		}
		return reply[0], true
	}
	// Every child is up once each has answered.
	seen := map[string]bool{}
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < children && time.Now().Before(deadline) {
		if child, ok := ask(); ok {
			seen[child] = true
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if len(seen) < children {
		t.Fatalf("only children %v answered", seen)
	}

	if g, err := reuseportbpf.NewGroup(2); err != nil {
		t.Skipf("selectors cannot be made here (need CAP_BPF): %v; the children got their brokers", err)
	} else {
		g.Close()
	}
	// Round-robin from wherever the counter stands: any run of conns, a
	// multiple of the children, gives each the same share.
	counts := map[string]int{}
	for i := 0; i < conns; i++ {
		child, ok := ask()
		if !ok {
			t.Fatalf("dial %d failed", i)
		}
		counts[child]++
	}
	for i := 1; i <= children; i++ {
		if got := counts[strconv.Itoa(i)]; got != conns/children {
			t.Fatalf("children took %v, want %d each", counts, conns/children)
		}
	}
}

// A child that returns before it was asked to stop, even without an error,
// ends the run: the master cannot serve with fewer children than it started.
// The default sizing starts at least one.
func TestAChildThatExitsEarlyStopsTheMaster(t *testing.T) {
	t.Setenv(modeEnv, "exit")
	result := make(chan error, 1)
	go func() {
		result <- prefork.RunContext(context.Background(), prefork.Config{},
			func(context.Context) error { return errors.New("serve ran in the master") })
	}()
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "before it was asked to stop") {
			t.Fatalf("RunContext = %v, want a child that exited early", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the master did not stop after a child exited")
	}
}

// A placeReport parsed: the child, its node, GOMAXPROCS, CPUs and memory
// policy.
type placed struct {
	child, node, procs int
	cpus               []int
	policy             string
}

func parsePlaced(t *testing.T, s string) placed {
	t.Helper()
	f := strings.Fields(s)
	if len(f) != 5 {
		t.Fatalf("child reported %q", s)
	}
	var p placed
	p.child, _ = strconv.Atoi(f[0])
	p.node, _ = strconv.Atoi(f[1])
	p.procs, _ = strconv.Atoi(f[2])
	p.cpus = cpuList(t, f[3])
	p.policy = f[4]
	return p
}

// cpuList parses the kernel's CPU list format.
func cpuList(t *testing.T, s string) []int {
	t.Helper()
	var cpus []int
	for part := range strings.SplitSeq(strings.TrimSpace(s), ",") {
		lo, hi, isRange := strings.Cut(part, "-")
		first, err := strconv.Atoi(lo)
		if err != nil {
			t.Fatalf("bad CPU list %q", s)
		}
		last := first
		if isRange {
			if last, err = strconv.Atoi(hi); err != nil {
				t.Fatalf("bad CPU list %q", s)
			}
		}
		for cpu := first; cpu <= last; cpu++ {
			cpus = append(cpus, cpu)
		}
	}
	return cpus
}

// nodeCPUs reads which of usable are node's CPUs.
func nodeCPUs(t *testing.T, node int, usable []int) []int {
	t.Helper()
	b, err := os.ReadFile("/sys/devices/system/node/node" + strconv.Itoa(node) + "/cpulist")
	if err != nil {
		// A kernel without NUMA: one node of every usable CPU.
		return usable
	}
	var cpus []int
	for _, cpu := range cpuList(t, string(b)) {
		if slices.Contains(usable, cpu) {
			cpus = append(cpus, cpu)
		}
	}
	return cpus
}

// threadMasks reads the CPUs every thread of this process may use.
func threadMasks(t *testing.T) map[string]string {
	t.Helper()
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		t.Fatal(err)
	}
	masks := map[string]string{}
	for _, task := range tasks {
		masks[task.Name()] = cpusAllowed("/proc/self/task/" + task.Name() + "/status")
	}
	return masks
}

// checkMastersThreads fails the test if any thread of the master is left on
// fewer CPUs than it started with: the thread the children were started
// from must be put back.
func checkMastersThreads(t *testing.T, usable string) {
	t.Helper()
	for task, mask := range threadMasks(t) {
		if mask != usable && mask != "?" {
			t.Errorf("master's thread %s may use CPUs %s after the run, want %s", task, mask, usable)
		}
	}
}

// PlaceCPU gives every child CPUs of its own, all on its node, and leaves the
// master's threads as they were.
func TestPlaceCPU(t *testing.T) {
	usable := cpusAllowed("/proc/self/status")
	all := cpuList(t, usable)
	if len(all) < 2 {
		t.Skipf("need 2 CPUs, have %s", usable)
	}
	const children = 2
	reports, err := runReporting(t, "place", children, prefork.Config{Children: children, ProcsPerChild: 1, Placement: prefork.PlaceCPU})
	if err != nil {
		t.Fatalf("RunContext = %v", err)
	}
	owner := map[int]int{}
	for _, r := range reports {
		p := parsePlaced(t, r)
		if p.node < 0 || p.procs != 1 || len(p.cpus) == 0 {
			t.Fatalf("child reported %q: want a node, GOMAXPROCS 1 and CPUs", r)
		}
		onNode := nodeCPUs(t, p.node, all)
		for _, cpu := range p.cpus {
			if !slices.Contains(onNode, cpu) {
				t.Fatalf("child %d may use CPU %d, not one of node %d's %v", p.child, cpu, p.node, onNode)
			}
			if other, ok := owner[cpu]; ok {
				t.Fatalf("children %d and %d share CPU %d", other, p.child, cpu)
			}
			owner[cpu] = p.child
		}
	}
	checkMastersThreads(t, usable)
}

// PlaceNode gives every child all its node's CPUs.
func TestPlaceNode(t *testing.T) {
	usable := cpusAllowed("/proc/self/status")
	all := cpuList(t, usable)
	const children = 2
	reports, err := runReporting(t, "place", children, prefork.Config{Children: children, ProcsPerChild: 1, Placement: prefork.PlaceNode})
	if err != nil {
		t.Fatalf("RunContext = %v", err)
	}
	for _, r := range reports {
		p := parsePlaced(t, r)
		if p.node < 0 {
			t.Fatalf("child reported %q: want a node", r)
		}
		if want := nodeCPUs(t, p.node, all); !slices.Equal(p.cpus, want) {
			t.Fatalf("child %d may use CPUs %v, want node %d's %v", p.child, p.cpus, p.node, want)
		}
	}
	checkMastersThreads(t, usable)
}

// BindMemory binds each child's memory to its node, or, where this process
// may not set a memory policy (Docker's seccomp profile without
// CAP_SYS_NICE), fails the run with set_mempolicy's or get_mempolicy's
// error.
func TestBindMemory(t *testing.T) {
	usable := cpusAllowed("/proc/self/status")
	reports, err := runReporting(t, "place", 1, prefork.Config{Children: 1, ProcsPerChild: 1, Placement: prefork.PlaceNode, BindMemory: true})
	if err != nil {
		if !strings.Contains(err.Error(), "mempolicy") {
			t.Fatalf("RunContext = %v, want a memory policy error", err)
		}
		t.Skipf("no memory policy here: %v", err)
	}
	p := parsePlaced(t, reports[0])
	if p.policy != "-" && p.policy != "bind:"+strconv.Itoa(p.node) {
		t.Fatalf("child on node %d has memory policy %q, want bind:%d", p.node, p.policy, p.node)
	}
	checkMastersThreads(t, usable)
}

// An unknown placement fails the run before any child starts.
func TestUnknownPlacement(t *testing.T) {
	err := prefork.RunContext(context.Background(), prefork.Config{Children: 1, Placement: 9},
		func(context.Context) error { return errors.New("serve ran in the master") })
	if err == nil || !strings.Contains(err.Error(), "Placement(9)") {
		t.Fatalf("RunContext = %v, want the placement refused", err)
	}
}

// Outside a placed child Node is -1.
func TestNodeOutsidePrefork(t *testing.T) {
	if n := prefork.Node(); n != -1 {
		t.Fatalf("Node() = %d, want -1", n)
	}
}
