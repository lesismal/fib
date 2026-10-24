//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

// engineCloseRecorder echoes what arrives, says "bye" when Engine.Close is
// about to end a connection, and records the order it is told things in.
type engineCloseRecorder struct {
	mu   sync.Mutex
	told []string
}

func (h *engineCloseRecorder) record(what string) {
	h.mu.Lock()
	h.told = append(h.told, what)
	h.mu.Unlock()
}

func (h *engineCloseRecorder) recorded() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.told)
}

func (h *engineCloseRecorder) OnOpen(*Connection)                 {}
func (h *engineCloseRecorder) OnPriorityData(*Connection, []byte) {}
func (h *engineCloseRecorder) OnData(c *Connection, b []byte)     { _ = c.Send(b) }
func (h *engineCloseRecorder) OnClose(*Connection, error)         { h.record("close") }
func (h *engineCloseRecorder) OnEngineClose(c *Connection) {
	h.record("engine close")
	if err := c.Send([]byte("bye")); err != nil {
		h.record("send failed: " + err.Error())
	}
}

// awaitRecorded waits for h to have been told want.
func awaitRecorded(t *testing.T, h *engineCloseRecorder, want []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !slices.Equal(h.recorded(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("told %q, want %q", h.recorded(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readBye reads from conn until "bye" arrives.
func readBye(t *testing.T, conn *net.UDPConn) {
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

// TestEngineCloseTellsUDPPeers checks that Engine.Close has the handler of
// a UDP listener's peer told, while it can still send, before OnClose.
func TestEngineCloseTellsUDPPeers(t *testing.T) {
	handler := &engineCloseRecorder{}
	config := DefaultConfig()
	config.Network = "udp"
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	conn, err := net.DialUDP("udp", nil, addr)
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
	readBye(t, conn)
	awaitRecorded(t, handler, []string{"engine close", "close"})
}

// TestEngineCloseTellsDialedUDP checks that Engine.Close has the handler of
// a UDP connection the engine dialed told, while it can still send.
func TestEngineCloseTellsDialedUDP(t *testing.T) {
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	engine, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	handler := &engineCloseRecorder{}
	dialed := make(chan error, 1)
	if err := engine.DialWithHandler("udp", peer.LocalAddr().String(), time.Second, handler, func(_ *Connection, err error) {
		dialed <- err
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-dialed; err != nil {
		t.Fatal(err)
	}
	engine.Stop()
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	readBye(t, peer)
	awaitRecorded(t, handler, []string{"engine close", "close"})
}
