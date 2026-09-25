//go:build linux || darwin

package websocket

import (
	"testing"

	fib "github.com/lesismal/fib"
)

// A WebSocket connection runs on whichever pool the engine's configuration
// gives it, the inline one under IOPollers included: unlike HTTP/1, it does
// not ask for workers.
func TestWebSocketFollowsEnginePoolUnderPollers(t *testing.T) {
	onWorkers := make(chan bool, 1)
	handler := NewHandler(HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			onWorkers <- c.conn.RunsOnWorkers()
			if err := c.WriteMessage(opcode, payload); err != nil {
				t.Error(err)
			}
		},
	})
	config := fib.DefaultConfig()
	config.Name = "websocket-pollers"
	config.Addr = "127.0.0.1:0"
	config.IOPollers = true
	config.IOPollerCount = 1
	server, err := fib.Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	}()

	conn, reader := dialWebSocket(t, addr.String())
	defer conn.Close()
	if _, err := conn.Write(clientFrame(Text, true, []byte("hi"))); err != nil {
		t.Fatal(err)
	}
	opcode, payload, err := readServerFrame(reader)
	if err != nil || opcode != Text || string(payload) != "hi" {
		t.Fatalf("read %v %q, %v", opcode, payload, err)
	}
	if <-onWorkers {
		t.Fatal("a WebSocket connection asked for workers")
	}
}
