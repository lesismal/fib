package prefork_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
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
		engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: os.Getenv(addrEnv), Handler: fib.HandlerFuncs{Data: func(c *fib.Connection, _ []byte) {
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
	for range n {
		_ = ready.(*net.TCPListener).SetDeadline(time.Now().Add(15 * time.Second))
		conn, err := ready.Accept()
		if err != nil {
			t.Fatalf("a child never announced itself: %v", err)
		}
		_ = conn.Close()
	}
	cancel()
	select {
	case err := <-result:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("the master did not return")
		return nil
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

// BalanceReusePort runs the children as usual where it can hand them a bpffs
// directory, and where it cannot.
func TestBalanceReusePortRuns(t *testing.T) {
	if err := runUntilReady(t, "sigterm", 1, prefork.Config{Children: 1, ProcsPerChild: 1, BalanceReusePort: true}); err != nil {
		t.Fatalf("RunContext = %v", err)
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
