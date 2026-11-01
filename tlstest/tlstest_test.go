package tlstest

import (
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"
)

// The configs a call returns make a handshake with each other, and each call
// returns fresh ones around the same certificate.
func TestConfigs(t *testing.T) {
	server, client, err := Configs()
	if err != nil {
		t.Fatal(err)
	}
	server2, client2, err := Configs()
	if err != nil {
		t.Fatal(err)
	}
	if server == server2 || client == client2 {
		t.Fatal("a call returned the configs of another")
	}
	if !server.Certificates[0].Leaf.Equal(server2.Certificates[0].Leaf) {
		t.Fatal("the calls returned different certificates")
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	deadline := time.Now().Add(10 * time.Second)
	_ = a.SetDeadline(deadline)
	_ = b.SetDeadline(deadline)
	done := make(chan error, 1)
	go func() {
		s := tls.Server(a, server)
		err := s.Handshake()
		if err == nil {
			_, err = s.Write([]byte("ok"))
		}
		done <- err
	}()
	c := tls.Client(b, client)
	got := make([]byte, 2)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "ok" {
		t.Fatalf("client read %q, %v", got, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server: %v", err)
	}
}
