//go:build linux && (amd64 || arm64)

package core

import (
	"bytes"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/reuseportbpf"
)

// brokerChild has this process stand in for prefork child child of children:
// b, made here as a master would make it, hands it selectors over the pair it
// finds in the environment, as a child started by package prefork does.
func brokerChild(t *testing.T, b *reuseportbpf.Broker, child, children int) {
	t.Helper()
	f, err := b.Pair()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	t.Setenv(reuseportbpf.BrokerEnv, strconv.Itoa(int(f.Fd())))
	t.Setenv(preforkChildEnv, strconv.Itoa(child))
	t.Setenv(preforkChildrenEnv, strconv.Itoa(children))
}

// TestEngineBalanceReusePort runs the whole openBinding path with
// Config.BalanceReusePort set: the engine attaches a selector to its pollers'
// shared listening group and still echoes. It skips the attach check where
// bpf(2) is refused, but the echo must work either way.
func TestEngineBalanceReusePort(t *testing.T) {
	reuseportbpf.Debug = true
	defer func() { reuseportbpf.Debug = false }()
	config := pollerConfig("balance", 4)
	config.ReusePort = true
	config.BalanceReusePort = true
	server, addr := startEchoServer(t, config, echoHandler())

	for i := 0; i < 20; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		msg := []byte("ping")
		if _, err := c.Write(msg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		buf := make([]byte, len(msg))
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := c.Read(buf); err != nil || !bytes.Equal(buf, msg) {
			t.Fatalf("echo %d: %q %v", i, buf, err)
		}
		c.Close()
	}

	server.listenMu.Lock()
	n := len(server.balancers)
	server.listenMu.Unlock()
	if n == 0 {
		t.Skip("no selector attached (need CAP_BPF); echo worked without it")
	}
}

// TestPreforkBalanceEnv checks that a prefork child's socket to the broker
// and its place are read from the environment, and that a non-child, a lone
// child, or one handed no socket is not balanced.
func TestPreforkBalanceEnv(t *testing.T) {
	t.Setenv(reuseportbpf.BrokerEnv, "3")
	t.Setenv(preforkChildEnv, "2")
	t.Setenv(preforkChildrenEnv, "4")
	conn, child, children, ok := preforkBalance()
	if !ok || conn != 3 || child != 2 || children != 4 {
		t.Fatalf("preforkBalance() = %d %d %d %v", conn, child, children, ok)
	}
	t.Setenv(preforkChildrenEnv, "1")
	if _, _, _, ok := preforkBalance(); ok {
		t.Error("a lone child should not be balanced")
	}
	t.Setenv(preforkChildrenEnv, "4")
	for _, v := range []string{"", "x", "-1"} {
		t.Setenv(reuseportbpf.BrokerEnv, v)
		if _, _, _, ok := preforkBalance(); ok {
			t.Errorf("with %s=%q there is no broker to ask", reuseportbpf.BrokerEnv, v)
		}
	}
}
