//go:build linux || darwin || windows

package http

import (
	"bufio"
	"net"
	stdhttp "net/http"
	"sync"
	"testing"

	fib "github.com/lesismal/fib/go"
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

func benchmarkServer(b *testing.B, request []byte) {
	const connections = 8
	reply := []byte("hello")

	server, addr := startBenchServer(b, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", reply)
	}))
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

func startBenchServer(b *testing.B, handler Handler) (stop func(), addr string) {
	b.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	server, err := fib.Bind(config, NewHandler(handler))
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
