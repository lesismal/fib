//go:build linux || darwin || windows

package http

import (
	"bufio"
	"net"
	stdhttp "net/http"
	"sync"
	"testing"

	fib "github.com/lesismal/fib"
)

// BenchmarkServerKeepAlive drives the server the way a load generator does:
// several connections each with requests in flight, so one read usually
// carries more than one request and one round answers more than one. It covers
// parsing, the handler and the reply write path together, which the parser
// benchmarks cannot. Client and server share the process, so treat the result
// as a relative measure rather than an absolute server cost.
func BenchmarkServerKeepAlive(b *testing.B) {
	benchmarkServer(b, []byte("GET /hello HTTP/1.1\r\nHost: localhost\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n"))
}

// BenchmarkServerKeepAlivePOST is the same with a body, which is the path that
// reads and copies one.
func BenchmarkServerKeepAlivePOST(b *testing.B) {
	benchmarkServer(b, []byte("POST /hello HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\n"+
		"Content-Length: 12\r\n\r\nhello, world"))
}

// BenchmarkServerKeepAliveReuse is BenchmarkServerKeepAlivePOST with the
// server recycling everything Config.ReuseRequests and its siblings let it.
func BenchmarkServerKeepAliveReuse(b *testing.B) {
	config := DefaultConfig()
	setReuseAll(&config)
	benchmarkServerWith(b, config, []byte("POST /hello HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\n"+
		"Content-Length: 12\r\n\r\nhello, world"))
}

func benchmarkServer(b *testing.B, request []byte) {
	benchmarkServerWith(b, DefaultConfig(), request)
}

// BenchmarkServerKeepAliveHooked is BenchmarkServerKeepAlive with an
// OnResponse hook on every response, as a middleware such as compress
// registers one.
func BenchmarkServerKeepAliveHooked(b *testing.B) {
	benchmarkServerHandler(b, DefaultConfig(), []byte("GET /hello HTTP/1.1\r\nHost: localhost\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n"),
		HandlerFunc(func(c *Context) {
			c.OnResponse(func(*Response) {})
			_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
		}))
}

var benchReply = []byte("hello")

func benchmarkServerWith(b *testing.B, config Config, request []byte) {
	benchmarkServerHandler(b, config, request, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
	}))
}

func benchmarkServerHandler(b *testing.B, config Config, request []byte, handler Handler) {
	const connections = 8
	reply := benchReply

	server, addr := startBenchServer(b, config, handler)
	defer server()

	perConnection := (b.N + connections - 1) / connections
	conns := make([]net.Conn, connections)
	for i := range conns {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		conns[i] = conn
		defer conn.Close()
	}

	b.SetBytes(int64(len(request)))
	b.ResetTimer()
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(2)
		go func(conn net.Conn) {
			defer wg.Done()
			for sent := 0; sent < perConnection; sent++ {
				if _, err := conn.Write(request); err != nil {
					return
				}
			}
		}(conn)
		go func(conn net.Conn) {
			defer wg.Done()
			reader := bufio.NewReader(conn)
			for got := 0; got < perConnection; got++ {
				response, err := stdhttp.ReadResponse(reader, nil)
				if err != nil {
					b.Error(err)
					return
				}
				_, _ = response.Body.Read(make([]byte, len(reply)))
				_ = response.Body.Close()
			}
		}(conn)
	}
	wg.Wait()
	b.StopTimer()
}

func startBenchServer(b *testing.B, httpConfig Config, handler Handler) (stop func(), addr string) {
	b.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := fib.Bind(config, NewHandlerWithConfig(httpConfig, handler))
	if err != nil {
		b.Fatal(err)
	}
	local, err := server.LocalAddr()
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Run() }()
	return func() {
		server.Stop()
		<-done
		_ = server.Close()
	}, local.String()
}

// BenchmarkServerShortLived opens a connection for every ten requests and
// closes it after them, as HttpArena's limited-conn profile does, so that
// what a connection costs to accept, set up and tear down shows beside what
// its requests cost. One op is one connection.
func BenchmarkServerShortLived(b *testing.B) {
	const perConnection = 10
	request := []byte("GET /hello HTTP/1.1\r\nHost: localhost\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n")
	server, addr := startBenchServer(b, DefaultConfig(), HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
	}))
	defer server()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 4096)
		for pb.Next() {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				b.Error(err)
				return
			}
			for i := 0; i < perConnection; i++ {
				if _, err := conn.Write(request); err != nil {
					b.Error(err)
					return
				}
				// The reply is small enough to arrive in one read.
				if _, err := conn.Read(buf); err != nil {
					b.Error(err)
					return
				}
			}
			conn.Close()
		}
	})
}
