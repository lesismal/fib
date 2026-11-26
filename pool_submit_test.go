package fib

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// Where fib's code runs, as the submission sites below name it. A pool may
// be waited on by a submission, so it is a node of the wait graph; a
// goroutine of no pool is not, since nothing waits on it.
const (
	onLoop      = "loop"              // an event loop
	onEngine    = "engine"            // an engine's workers, which read connections
	onHandlers  = "handlers"          // the handler pool, or a pool an application gives arpc or gRPC
	onClient    = "fib-client"        // sidepool.Client
	onNotify    = "fib-notify"        // sidepool.Notify, behind sidepool.Post
	onFile      = "fib-file"          // sidepool.File
	onFlush     = "fib-flush"         // sidepool.Flush
	onHandshake = "fib-tls-handshake" // sidepool.Handshake
	onGoroutine = "goroutine"         // an application's goroutine, a timer's, or one of fib's own
	toAnyPool   = "any pool"          // whatever pool the caller names
	notAPool    = ""                  // a call that submits to no pool, such as an HTTP client's Go
)

// submitSite is one call in fib that hands work to a pool: where it is, the
// goroutines that may make it, the pool it submits to, and whether it may
// wait for room there.
type submitSite struct {
	file, fn, call string
	from           []string
	to             string
	wait           bool
}

// submitSites lists every call in fib that submits to a pool. A task must
// never wait to submit to the pool it runs on, since a pool whose every
// worker so waits has none left to make room, and no chain of waits may lead
// from a pool back to itself. Code that may run on the pool it wants to
// submit to has the engine's loop submit instead, with fib.Engine.Hand or
// fib.Connection.Hand, and is not listed: the loop never waits.
//
// A new submission makes TestPoolSubmissions fail until it is listed here,
// with where it may run worked out from its callers.
var submitSites = []submitSite{
	// Connections' rounds, and what an engine worker hands on.
	{"core/engine.go", "Engine.submitReady", "pool.GoTasks", []string{onLoop}, onEngine, true},
	{"core/engine_other.go", "Engine.submit", "e.taskPool.GoTask", []string{onGoroutine}, onEngine, true},
	{"http/stream_pool_native.go", "StreamPool.Submit", "b.pool.GoTasks", []string{onEngine}, onHandlers, true},
	{"http/stream_pool_native.go", "StreamPool.submit", "pool.GoTask", []string{onEngine}, onHandlers, true},
	{"http/stream_pool_native.go", "runOnStreamsFromReader", "handlerPool(conn).Go", []string{onEngine}, onHandlers, true},
	{"http/stream_pool_native.go", "handOn", "pool.Go", []string{onGoroutine}, onHandlers, true},
	// An HTTP/2 flush tries the handler pool first, which never waits, and
	// else goes to the flush pool, which may.
	{"http/h2_server.go", "h2ServerConn.scheduleFlush", "sc.flushPool.TryGo",
		[]string{onEngine, onHandlers, onClient, onNotify, onGoroutine}, onHandlers, false},
	{"http/h2_server.go", "h2ServerConn.scheduleFlush", "sidepool.Flush().GoTask",
		[]string{onEngine, onHandlers, onClient, onNotify, onGoroutine}, onFlush, true},
	{"grpc/transport.go", "transport.run", "pool.GoTask", []string{onEngine}, onHandlers, true},
	{"arpc/client.go", "Client.runTask", "pool.GoTask", []string{onEngine}, onHandlers, true},

	// The loop's own submissions for Hand.
	{"core/hand.go", "offerHand", "tp.TryGo", []string{onLoop}, toAnyPool, false},
	{"core/hand.go", "offerHand", "pool.GoTask", []string{onGoroutine}, toAnyPool, true},
	{"core/hand_other.go", "Engine.Hand", "pool.GoTask", []string{onGoroutine}, toAnyPool, true},

	// The client pool, from loops and engine workers only.
	{"sidepool/sidepool.go", "Go", "Client().Go", []string{onLoop, onEngine}, onClient, true},
	{"http/client.go", "Client.dialed", "sidepool.Go", []string{onLoop}, onClient, true},
	{"http/client.go", "clientConn.OnClose", "sidepool.Go", []string{onLoop, onEngine}, onClient, true},
	{"http/h2_client.go", "h2ClientConn.shutdown", "sidepool.Go", []string{onLoop, onEngine}, onClient, true},
	{"websocket/client.go", "clientConn.fail", "sidepool.Go", []string{onLoop}, onClient, true},
	{"socks/server_resolve.go", "Server.dial", "sidepool.Go", []string{onEngine}, onClient, true},

	// The file pool, which runs nothing that waits on another pool.
	{"http/send_file.go", "Context.SendFileStall", "sidepool.File().Go",
		[]string{onEngine, onHandlers, onClient, onNotify, onGoroutine}, onFile, true},
	{"http/send_file.go", "fileSend.resume", "sidepool.File().Go", []string{onNotify}, onFile, true},

	// TLS handshakes, which never wait.
	{"tls/quic.go", "QUICConn.async", "sidepool.Handshake().TryGo", []string{onLoop, onEngine}, onHandshake, false},
	{"tls/tls.go", "Handler.startBlocking", "sidepool.Handshake().TryGo", []string{onLoop, onEngine}, onHandshake, false},
	{"tls/tls.go", "layer.async", "sidepool.Handshake().TryGo", []string{onLoop, onEngine}, onHandshake, false},

	// sidepool.Post, which never waits: a callback it cannot queue runs on
	// a goroutine of its own.
	{"sidepool/post.go", "post", "pool.TryGo", []string{onLoop, onEngine, onHandlers, onClient, onNotify, onFlush, onGoroutine}, onNotify, false},
	{"core/connection.go", "Connection.fireNoticesLocked", "sidepool.Post", []string{onLoop, onEngine, onHandlers, onClient, onNotify, onFlush, onGoroutine}, onNotify, false},
	{"core/engine_unix.go", "udpWaiters.ready", "sidepool.Post", []string{onLoop, onEngine}, onNotify, false},
	{"core/engine_unix.go", "udpWaiters.closeFD", "sidepool.Post", []string{onLoop, onEngine}, onNotify, false},
	{"core/splice.go", "Connection.dropSpliceInLocked", "sidepool.Post", []string{onLoop, onEngine, onHandlers, onGoroutine}, onNotify, false},
	{"http/async_body.go", "Context.pump", "sidepool.Post", []string{onEngine, onHandlers, onClient, onNotify, onGoroutine}, onNotify, false},
	{"http/async_header.go", "Context.resumeHeader", "sidepool.Post", []string{onEngine, onHandlers, onClient, onNotify, onGoroutine}, onNotify, false},
	{"http/client_push.go", "goKick", "sidepool.Post", []string{onEngine, onClient, onNotify, onGoroutine}, onNotify, false},
	{"http/h2_server.go", "h2ServerConn.armDrainLocked", "sidepool.Post", []string{onEngine, onHandlers, onClient, onNotify, onGoroutine}, onNotify, false},
	{"http/h2_tunnel.go", "h2ServerConn.postCallback", "sidepool.Post", []string{onEngine, onHandlers, onNotify, onGoroutine}, onNotify, false},
	{"http3/quic/conn.go", "Conn.wakeWritersLocked", "sidepool.Post", []string{onEngine, onHandlers, onNotify, onGoroutine}, onNotify, false},
	{"http3/tunnel.go", "tunnelWriter.NotifySendable", "sidepool.Post", []string{onEngine, onHandlers, onNotify, onGoroutine}, onNotify, false},
	{"webrtc/turnstream.go", "PeerConnection.dialTURN", "sidepool.Post", []string{onEngine, onGoroutine}, onNotify, false},

	// Calls named like a submission that submit to no pool.
	{"bittorrent/tracker/http_client.go", "HTTPClient.get", "c.client.Go", nil, notAPool, false},
	{"bittorrent/webseed.go", "Torrent.webSeedGet", "t.c.httpClient.Go", nil, notAPool, false},
	{"http3/altsvc.go", "AltSvcClient.doTCP", "a.tcp.Go", nil, notAPool, false},
}

// submitNames are the methods and functions that hand work to a pool.
var submitNames = map[string]bool{"Go": true, "GoTask": true, "GoTasks": true, "TryGo": true, "Post": true}

// scanSubmissions finds every call in fib's code, tests aside, whose callee
// is named like a submission, keyed "file|function|call" with how many times
// each occurs. Package taskpool, which implements the pools, and the nested
// modules, interop programs and benchmarks are not fib's code here.
func scanSubmissions(t *testing.T) map[string]int {
	t.Helper()
	found := map[string]int{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != "." && (strings.HasPrefix(name, ".") || name == "testdata" || name == "taskpool" ||
				name == "benchmark" || name == "cmd" || strings.Contains(name, "interop")) {
				return filepath.SkipDir
			}
			if path != "." {
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			fn := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				recv := fd.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if index, ok := recv.(*ast.IndexExpr); ok {
					recv = index.X
				}
				fn = exprString(fset, recv) + "." + fn
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if ok && submitNames[sel.Sel.Name] {
					found[filepath.ToSlash(path)+"|"+fn+"|"+exprString(fset, sel)]++
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func exprString(fset *token.FileSet, expr ast.Expr) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, expr)
	return b.String()
}

// TestPoolSubmissions checks that submitSites lists every submission in fib
// and nothing that is gone, that no submission is made from the pool it
// submits to but with TryGo or sidepool.Post, which never wait, and that the
// waits it lists lead from no pool back to itself.
func TestPoolSubmissions(t *testing.T) {
	found := scanSubmissions(t)
	listed := map[string]bool{}
	for _, s := range submitSites {
		key := s.file + "|" + s.fn + "|" + s.call
		if listed[key] {
			t.Errorf("%s is listed twice", key)
		}
		listed[key] = true
		if found[key] == 0 {
			t.Errorf("%s is listed in submitSites but not found: drop it, or fix where it is", key)
		}
		if slices.Contains(s.from, s.to) {
			// A task may submit to its own pool only with TryGo or
			// sidepool.Post, which never wait.
			switch {
			case s.wait:
				t.Errorf("%s may wait to submit to %s from %s itself", key, s.to, s.to)
			case !strings.HasSuffix(s.call, "TryGo") && s.call != "sidepool.Post":
				t.Errorf("%s submits to %s from %s itself: only TryGo and sidepool.Post may, since they never wait", key, s.to, s.to)
			}
		}
	}
	var unlisted []string
	for key := range found {
		if !listed[key] {
			unlisted = append(unlisted, key)
		}
	}
	sort.Strings(unlisted)
	for _, key := range unlisted {
		t.Errorf("%s submits to a pool but is not listed in submitSites: list it, with where it may run", key)
	}
	if cycle := waitCycle(submitSites); cycle != nil {
		t.Errorf("the submissions that may wait form a cycle: %s", strings.Join(cycle, " -> "))
	}
}

// waitCycle returns a cycle of the graph whose edges lead from where a
// submission that may wait is made to the pool it waits on, or nil.
func waitCycle(sites []submitSite) []string {
	edges := map[string]map[string]bool{}
	for _, s := range sites {
		if !s.wait || s.to == notAPool || s.to == toAnyPool {
			continue
		}
		for _, from := range s.from {
			if from == onGoroutine {
				continue
			}
			if edges[from] == nil {
				edges[from] = map[string]bool{}
			}
			edges[from][s.to] = true
		}
	}
	const (
		unvisited = iota
		visiting
		done
	)
	state := map[string]int{}
	var stack []string
	var visit func(node string) []string
	visit = func(node string) []string {
		state[node] = visiting
		stack = append(stack, node)
		next := make([]string, 0, len(edges[node]))
		for to := range edges[node] {
			next = append(next, to)
		}
		sort.Strings(next)
		for _, to := range next {
			switch state[to] {
			case visiting:
				i := slices.Index(stack, to)
				return append(slices.Clone(stack[i:]), to)
			case unvisited:
				if cycle := visit(to); cycle != nil {
					return cycle
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[node] = done
		return nil
	}
	nodes := make([]string, 0, len(edges))
	for node := range edges {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		if state[node] == unvisited {
			if cycle := visit(node); cycle != nil {
				return cycle
			}
		}
	}
	return nil
}

// TestWaitCycle checks that waitCycle finds a cycle and leaves a chain alone.
func TestWaitCycle(t *testing.T) {
	chain := []submitSite{
		{from: []string{onLoop}, to: onEngine, wait: true},
		{from: []string{onEngine}, to: onHandlers, wait: true},
	}
	if cycle := waitCycle(chain); cycle != nil {
		t.Errorf("a chain reported as the cycle %v", cycle)
	}
	cyclic := append(chain, submitSite{from: []string{onHandlers}, to: onEngine, wait: true})
	if cycle := waitCycle(cyclic); fmt.Sprint(cycle) != fmt.Sprint([]string{onEngine, onHandlers, onEngine}) {
		t.Errorf("waitCycle found %v, want engine -> handlers -> engine", cycle)
	}
	nowait := append(chain, submitSite{from: []string{onHandlers}, to: onEngine})
	if cycle := waitCycle(nowait); cycle != nil {
		t.Errorf("a submission that does not wait made the cycle %v", cycle)
	}
}
