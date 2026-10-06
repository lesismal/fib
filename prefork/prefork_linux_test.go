package prefork_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/prefork"
)

// The tests start this test binary again as the children; TestMain runs the
// child's part there instead of the tests. The mode and the address travel
// in the environment, which the children inherit.
const (
	modeEnv = "PREFORK_TEST_MODE"
	addrEnv = "PREFORK_TEST_ADDR"
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
		if os.Getenv(modeEnv) == "fail" && prefork.Child() == 2 {
			return errors.New("child failed on purpose")
		}
		config := fib.DefaultConfig()
		config.Addr = os.Getenv(addrEnv)
		reply := fmt.Sprintf("%d %d %d", prefork.Child(), runtime.GOMAXPROCS(0), prefork.Children())
		engine, err := fib.Bind(config, fib.HandlerFuncs{Data: func(c *fib.Connection, _ []byte) {
			_ = c.Send([]byte(reply))
		}})
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
