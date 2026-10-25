//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"net"
	"testing"
	"time"
)

// readTCPBye reads from conn until "bye" arrives.
func readTCPBye(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 16)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("no bye: %v", err)
		}
		if string(buf[:n]) == "bye" {
			return
		}
	}
}

// TestEngineCloseTellsTCPPeers checks that Engine.Close has the handler of a
// TCP connection told while it can still send, before OnClose. The end of the
// stream alone cannot carry a protocol's last frame, such as HTTP/2's GOAWAY,
// and a handler that hears only once sends are refused has none. Stop, which
// leaves connections open, tells nobody.
func TestEngineCloseTellsTCPPeers(t *testing.T) {
	handler := &engineCloseRecorder{}
	server, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	server.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if told := handler.recorded(); len(told) != 0 {
		t.Fatalf("Stop told the handler %q", told)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	readTCPBye(t, conn)
	awaitRecorded(t, handler, []string{"engine close", "close"})
}
