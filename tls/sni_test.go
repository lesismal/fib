//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"net"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// sniEntry is one name of a server_name extension.
type sniEntry struct {
	typ  uint8
	name string
}

// sniExt is a server_name extension holding entries.
func sniExt(entries ...sniEntry) []byte {
	var b builder
	b.vec16(func(b *builder) {
		for _, e := range entries {
			b.u8(e.typ)
			b.vec16(func(b *builder) { b.raw([]byte(e.name)) })
		}
	})
	return b.b
}

// A server_name is read as crypto/tls reads it: an empty list, an empty name,
// a second host_name or one with a trailing dot does not parse, and crypto/tls
// is left to refuse it; a name of another type beside the host_name is
// skipped.
func TestServerNameValidated(t *testing.T) {
	base := s13Hello(t)
	for _, tc := range []struct {
		name string
		ext  []byte
		ok   bool
		want string
	}{
		{"one host_name", sniExt(sniEntry{0, "a.example"}), true, "a.example"},
		{"another type beside it", sniExt(sniEntry{1, "x"}, sniEntry{0, "a.example"}), true, "a.example"},
		{"an empty list", []byte{0, 0}, false, ""},
		{"an empty name", sniExt(sniEntry{0, ""}), false, ""},
		{"an empty name of another type", sniExt(sniEntry{1, ""}, sniEntry{0, "a.example"}), false, ""},
		{"two host_names", sniExt(sniEntry{0, "a.example"}, sniEntry{0, "b.example"}), false, ""},
		{"a trailing dot", sniExt(sniEntry{0, "a.example."}), false, ""},
	} {
		ch, ok := parseClientHello(base.with(extServerName, tc.ext).msg())
		if ok != tc.ok || ok && ch.serverName != tc.want {
			t.Errorf("%s: parsed %v (%+v)", tc.name, ok, ch)
		}
	}
}

// Over the wire, a ClientHello with two host_names goes to crypto/tls, which
// refuses it, rather than being served under the first.
func TestNativeServerRefusesTwoServerNames(t *testing.T) {
	fellBack := countFallbacks(t)
	serverConfig, _ := tlsConfigs(t)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hello := s13Hello(t).with(extServerName, sniExt(sniEntry{0, "localhost"}, sniEntry{0, "other.example"})).record()
	if _, err := conn.Write(hello); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reply := make([]byte, 7)
	n, _ := conn.Read(reply)
	// An alert record, not a ServerHello.
	if n < 1 || reply[0] != recordTypeAlert {
		t.Fatalf("answered with %x", reply[:n])
	}
	if fellBack.Load() != 1 {
		t.Fatal("not handed to crypto/tls")
	}
}
