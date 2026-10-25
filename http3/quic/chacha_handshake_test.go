package quic

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestChaCha20Poly1305Handshake runs a real QUIC handshake whose TLS settles
// on TLS_CHACHA20_POLY1305_SHA256, and moves data over it both ways: packet
// protection and header protection are then the ChaCha20 of chacha.go and
// chachaHeaderProtector, which the AES hardware of the development and CI
// machines would otherwise keep out of every handshake.
//
// crypto/tls picks its TLS 1.3 ciphersuites from whether the CPU has AES-GCM
// instructions, a decision it makes once at startup and does not let a
// Config change, so the test runs itself again with GODEBUG=cpu.aes=off,
// where both ends prefer ChaCha20, and the child checks what was negotiated.
// The Initial and Handshake packets are still AES-GCM, as RFC 9001 requires,
// on the standard library's portable implementation.
func TestChaCha20Poly1305Handshake(t *testing.T) {
	if os.Getenv("FIB_QUIC_CHACHA20") == "1" {
		testChaCha20Handshake(t)
		return
	}
	if runtime.GOOS == "windows" {
		// Windows keeps AES on with GODEBUG=cpu.aes=off: its TLS settles on
		// TLS_AES_128_GCM_SHA256 either way, so the suite is not this test's to
		// force there as it is here. The other platforms run it.
		t.Skip("Windows does not turn AES off for crypto/tls with GODEBUG=cpu.aes=off")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestChaCha20Poly1305Handshake$", "-test.v")
	// cpu.aes=off goes with whatever else GODEBUG was set to, since a second
	// GODEBUG variable in the environment is not what the child reads.
	env := os.Environ()
	for i, kv := range env {
		if strings.HasPrefix(kv, "GODEBUG=") {
			env[i] += ",cpu.aes=off"
			break
		}
		if i == len(env)-1 {
			env = append(env, "GODEBUG=cpu.aes=off")
		}
	}
	if len(env) == 0 {
		env = append(env, "GODEBUG=cpu.aes=off")
	}
	cmd.Env = append(env, "FIB_QUIC_CHACHA20=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the handshake that should have settled on ChaCha20 failed: %v\n%s", err, out)
	}
}

// testChaCha20Handshake is the body of TestChaCha20Poly1305Handshake, which
// the child process runs.
func testChaCha20Handshake(t *testing.T) {
	p := newTestPair(t, 0, nil)
	server := p.serverConn(t)
	for _, end := range []struct {
		name string
		c    *Conn
	}{{"client", p.client}, {"server", server}} {
		if got := end.c.ConnectionState().CipherSuite; got != tls.TLS_CHACHA20_POLY1305_SHA256 {
			t.Fatalf("%s settled on cipher suite %#04x, want TLS_CHACHA20_POLY1305_SHA256 %#04x",
				end.name, got, tls.TLS_CHACHA20_POLY1305_SHA256)
		}
		end.c.mu.Lock()
		tx, rx := end.c.spaces[spaceApp].tx, end.c.spaces[spaceApp].rx
		end.c.mu.Unlock()
		if tx == nil || rx == nil {
			t.Fatalf("%s has no 1-RTT keys", end.name)
		}
		if tx.suite != tls.TLS_CHACHA20_POLY1305_SHA256 || rx.suite != tls.TLS_CHACHA20_POLY1305_SHA256 {
			t.Fatalf("%s protects packets with suites %#04x and %#04x", end.name, tx.suite, rx.suite)
		}
		if _, ok := tx.hp.(chachaHeaderProtector); !ok {
			t.Fatalf("%s protects headers with %T, want chachaHeaderProtector", end.name, tx.hp)
		}
	}
	// Data both ways, so that both directions' keys seal and open, and both
	// ends' header protection unmask, over many packets.
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	payload := make([]byte, 256<<10)
	_, _ = rand.Read(payload)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); !bytes.Equal(got, payload) {
		t.Fatalf("the echo returned %d bytes of the %d sent", len(got), len(payload))
	}
}
