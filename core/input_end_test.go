//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package core

import (
	"io"
	"net"
	"testing"
	"time"
)

// inputEndHandler keeps a connection open past the end of its peer's input
// when keep says so, and reports what it was told.
type inputEndHandler struct {
	HandlerFuncs
	keep   bool
	ended  chan *Connection
	closed chan error
}

func (h *inputEndHandler) OnInputEnd(c *Connection) bool {
	h.ended <- c
	return h.keep
}

func (h *inputEndHandler) OnClose(_ *Connection, err error) { h.closed <- err }

func startInputEndServer(t *testing.T, h *inputEndHandler) net.Conn {
	t.Helper()
	config := DefaultConfig()
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		engine.Close()
	})
	client, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	_ = client.SetDeadline(time.Now().Add(10 * time.Second))
	return client
}

// A handler that keeps the connection past the end of the peer's input can
// still send on it, after OnData has had everything sent before the end, and
// the peer reads it all, then the end once the handler closes.
func TestInputEndKeepsTheWritingSide(t *testing.T) {
	h := &inputEndHandler{keep: true, ended: make(chan *Connection, 2), closed: make(chan error, 1)}
	got := make(chan string, 4)
	h.Data = func(_ *Connection, b []byte) { got <- string(b) }
	client := startInputEndServer(t, h)
	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	c := <-h.ended
	var data string
	for len(data) < len("request") {
		select {
		case b := <-got:
			data += b
		default:
			t.Fatalf("OnInputEnd came before the data sent ahead of the end: had %q", data)
		}
	}
	select {
	case err := <-h.closed:
		t.Fatalf("the connection closed at the end of input: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := c.Send([]byte("late reply")); err != nil {
		t.Fatal(err)
	}
	c.CloseAfterSend()
	reply, err := io.ReadAll(client)
	if err != nil || string(reply) != "late reply" {
		t.Fatalf("client read %q, %v", reply, err)
	}
	select {
	case <-h.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose never ran")
	}
	select {
	case <-h.ended:
		t.Fatal("OnInputEnd ran twice")
	default:
	}
}

// A handler that does not keep the connection has it closed with io.EOF, as
// one without OnInputEnd does.
func TestInputEndRefusedCloses(t *testing.T) {
	h := &inputEndHandler{ended: make(chan *Connection, 2), closed: make(chan error, 1)}
	client := startInputEndServer(t, h)
	if err := client.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-h.closed:
		if err != io.EOF {
			t.Fatalf("closed with %v, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connection stayed open")
	}
	if rest, _ := io.ReadAll(client); len(rest) != 0 {
		t.Fatalf("client read %q", rest)
	}
}
