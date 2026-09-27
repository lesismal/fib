//go:build linux || darwin

package websocket

import (
	"testing"

	fib "github.com/lesismal/fib"
)

// A WebSocket connection's rounds run on the engine's workers, and a poller
// only hands them on, so a message callback that blocks holds up only its own
// connection, not the others on its loop.
func TestWebSocketRoundsRunOnWorkers(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := NewHandler(HandlerFuncs{
		Message: func(c *Connection, opcode Opcode, payload []byte) {
			if string(payload) == "block" {
				entered <- struct{}{}
				<-release
			}
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

	blocked, blockedReader := dialWebSocket(t, addr.String())
	defer blocked.Close()
	if _, err := blocked.Write(clientFrame(Text, true, []byte("block"))); err != nil {
		t.Fatal(err)
	}
	<-entered
	other, otherReader := dialWebSocket(t, addr.String())
	defer other.Close()
	if _, err := other.Write(clientFrame(Text, true, []byte("fast"))); err != nil {
		close(release)
		t.Fatal(err)
	}
	opcode, payload, err := readServerFrame(otherReader)
	if err != nil || opcode != Text || string(payload) != "fast" {
		close(release)
		t.Fatalf("the other connection read %v %q, %v while one on its loop was blocked", opcode, payload, err)
	}
	close(release)
	if opcode, payload, err := readServerFrame(blockedReader); err != nil || opcode != Text || string(payload) != "block" {
		t.Fatalf("read %v %q, %v", opcode, payload, err)
	}
}
