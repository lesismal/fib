//go:build linux || darwin

package websocket

import (
	"testing"

	fib "github.com/lesismal/fib"
)

// A WebSocket connection runs its rounds on the engine's workers under
// IOPollers, as an HTTP/1 connection does, unless ReadOnPollers keeps them on
// its poller.
func TestWebSocketRunsOnWorkersUnderPollers(t *testing.T) {
	for _, readOnPollers := range []bool{false, true} {
		onWorkers := make(chan bool, 1)
		config := DefaultConfig()
		config.ReadOnPollers = readOnPollers
		handler := NewHandlerWithConfig(config, HandlerFuncs{
			Message: func(c *Connection, opcode Opcode, payload []byte) {
				onWorkers <- c.conn.RunsOnWorkers()
				if err := c.WriteMessage(opcode, payload); err != nil {
					t.Error(err)
				}
			},
		})
		engineConfig := fib.DefaultConfig()
		engineConfig.Name = "websocket-pollers"
		engineConfig.Addr = "127.0.0.1:0"
		engineConfig.IOPollers = true
		engineConfig.IOPollerCount = 1
		server, err := fib.Bind(engineConfig, handler)
		if err != nil {
			t.Fatal(err)
		}
		addr, err := server.LocalAddr()
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- server.Run() }()

		conn, reader := dialWebSocket(t, addr.String())
		if _, err := conn.Write(clientFrame(Text, true, []byte("hi"))); err != nil {
			t.Fatal(err)
		}
		opcode, payload, err := readServerFrame(reader)
		if err != nil || opcode != Text || string(payload) != "hi" {
			t.Fatalf("ReadOnPollers %v: read %v %q, %v", readOnPollers, opcode, payload, err)
		}
		if got := <-onWorkers; got == readOnPollers {
			t.Fatalf("ReadOnPollers %v: connection runs on workers %v", readOnPollers, got)
		}
		conn.Close()
		server.Stop()
		<-runDone
		_ = server.Close()
	}
}
