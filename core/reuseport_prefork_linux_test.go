//go:build linux

package core

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/reuseportbpf"
)

// offeredGroups records the groups attachBalancer tries a selector on while
// the test runs, attached or not.
func offeredGroups(t *testing.T) func() [][]int {
	t.Helper()
	var mu sync.Mutex
	var groups [][]int
	attachBalancerHook = func(g []int) {
		mu.Lock()
		groups = append(groups, append([]int(nil), g...))
		mu.Unlock()
	}
	t.Cleanup(func() { attachBalancerHook = nil })
	return func() [][]int {
		mu.Lock()
		defer mu.Unlock()
		return append([][]int(nil), groups...)
	}
}

// A prefork child handed a socket to its master's broker offers its
// listening sockets to
// Config.BalanceReusePort's selector one per address, whether a single
// poller listens or the engine's own loop does, for the addresses New binds
// and for those AddBinding adds; outside prefork a single socket is offered
// nothing, since there is nothing to choose between. The selector itself
// needs CAP_BPF, which TestPreforkChildrenRoundRobinOneSocketEach covers;
// here it falls back to the kernel's hash and the server still serves.
func TestPreforkChildOffersOneSocketGroups(t *testing.T) {
	engineLoopConfig := DefaultConfig()
	engineLoopConfig.EngineLoop = true
	for _, tc := range []struct {
		name    string
		config  Config
		prefork bool
		want    int
	}{
		{"one poller", pollerConfig("pf-one", 1), true, 1},
		{"engine loop", engineLoopConfig, true, 1},
		{"one poller outside prefork", pollerConfig("pf-none", 1), false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.prefork {
				brokerChild(t, reuseportbpf.NewBroker(), 1, 2)
			} else {
				t.Setenv(reuseportbpf.BrokerEnv, "")
			}
			groups := offeredGroups(t)
			config := tc.config
			config.ReusePort = true
			config.BalanceReusePort = true
			server, addr := startEchoServer(t, config, echoHandler())
			c, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			err = echoOnce(c, "hi")
			c.Close()
			if err != nil {
				t.Fatal(err)
			}
			got := groups()
			if len(got) != tc.want {
				t.Fatalf("New offered %d groups %v, want %d", len(got), got, tc.want)
			}
			for _, g := range got {
				if len(g) != 1 {
					t.Fatalf("group %v, want one socket", g)
				}
			}

			if _, err := server.AddBinding(Binding{Addr: "127.0.0.1:0", Handler: echoHandler()}); err != nil {
				t.Fatal(err)
			}
			if got := groups(); len(got) != 2*tc.want {
				t.Fatalf("after AddBinding %d groups %v, want %d", len(got), got, 2*tc.want)
			}
		})
	}
}

// Two engines, each as a prefork child with one poller, share an address
// and the one selector a broker makes for it, as a prefork master's would,
// which hands their connections out in turn: the case package prefork's
// default child, with two Ps and so one poller, is in. It needs CAP_BPF, and
// skips without it.
func TestPreforkChildrenRoundRobinOneSocketEach(t *testing.T) {
	const conns = 40
	broker := reuseportbpf.NewBroker()
	defer broker.Close()

	tried := offeredGroups(t)
	var opened [2]atomic.Int32
	addr := "127.0.0.1:0"
	var servers []*Engine
	for child := 1; child <= 2; child++ {
		brokerChild(t, broker, child, 2)
		config := pollerConfig(fmt.Sprintf("pf-rr-%d", child), 1)
		config.ReusePort = true
		config.BalanceReusePort = true
		n := &opened[child-1]
		server, err := New(config, Binding{Addr: addr, Handler: HandlerFuncs{Open: func(*Connection) { n.Add(1) }}})
		if err != nil {
			t.Fatal(err)
		}
		if child == 1 {
			a, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			addr = a.String()
		}
		runDone := make(chan error, 1)
		go func() { runDone <- server.Run() }()
		t.Cleanup(func() {
			server.Stop()
			<-runDone
		})
		servers = append(servers, server)
	}
	if got := tried(); len(got) != 2 {
		t.Fatalf("selectors tried for %v, want one socket from each child", got)
	}
	for i, s := range servers {
		s.listenMu.Lock()
		n := len(s.balancers)
		s.listenMu.Unlock()
		if n == 0 {
			t.Skipf("child %d attached no selector (need CAP_BPF)", i+1)
		}
	}

	var dials []net.Conn
	for i := 0; i < conns; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		dials = append(dials, c)
	}
	deadline := time.Now().Add(2 * time.Second)
	for opened[0].Load()+opened[1].Load() < conns && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	for _, c := range dials {
		c.Close()
	}
	a, b := int(opened[0].Load()), int(opened[1].Load())
	if a+b != conns {
		t.Fatalf("opened %d+%d of %d", a, b, conns)
	}
	if d := a - b; d > 3 || d < -3 {
		t.Fatalf("children opened %d and %d, want about %d each", a, b, conns/2)
	}
}
