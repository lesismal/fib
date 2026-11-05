//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// An empty handshake record is refused, before the ClientHello and after it,
// as crypto/tls refuses one, rather than waited past.
func TestEmptyHandshakeRecordRefused(t *testing.T) {
	config, _ := ecdsaServer(t)
	empty := []byte{recordTypeHandshake, 3, 1, 0, 0}
	srv := newSrvHS(config, nil)
	consumed, out, _, herr := srv.process(bytes.Repeat(empty, 3))
	if herr == nil || herr == errFallback || herr.alert != alertUnexpectedMessage || consumed != 0 || len(out) != 0 {
		t.Fatalf("empty records ahead of the hello: consumed %d, %d out, %v", consumed, len(out), herr)
	}

	// Ahead of a ClientHello's second half, once the first has been read.
	msg := s13Hello(t).msg()
	in := appendRecord(nil, recordTypeHandshake, 0x0301, msg[:10])
	srv = newSrvHS(config, nil)
	if _, _, _, herr := srv.process(in); herr != nil {
		t.Fatal(herr)
	}
	in = append(in, empty...)
	if _, _, _, herr := srv.process(in); herr == nil || herr.alert != alertUnexpectedMessage {
		t.Fatalf("an empty record inside the hello: %v", herr)
	}

	// After the ClientHello, from a client and from a server.
	srv = newSrvHS(config, nil)
	var sink []byte
	if herr := srv.record(appendRecord(nil, recordTypeHandshake, 0x0303, nil), &sink); herr == nil || herr.alert != alertUnexpectedMessage {
		t.Fatalf("server: %v", herr)
	}
	cli := newHS12(t, nil, nil)
	if herr := cli.record(appendRecord(nil, recordTypeHandshake, 0x0303, nil), &sink); herr == nil || herr.alert != alertUnexpectedMessage {
		t.Fatalf("client: %v", herr)
	}
}

// A ClientHello that arrives in many small records, offered again each time
// with more behind it, is read once: what peek read before it does not read
// again, and the hello is served all the same.
func TestPeekReadsEachRecordOnce(t *testing.T) {
	config, _ := ecdsaServer(t)
	msg := s13Hello(t).msg()
	var in []byte
	srv := newSrvHS(config, nil)
	for i := 0; i < len(msg); i += 7 {
		in = appendRecord(in, recordTypeHandshake, 0x0301, msg[i:min(i+7, len(msg))])
		consumed, out, _, herr := srv.process(in)
		if herr != nil {
			t.Fatal(herr)
		}
		if i+7 < len(msg) {
			if consumed != 0 || srv.peekAt != len(in) {
				t.Fatalf("after %d bytes: consumed %d, peeked to %d of %d", i+7, consumed, srv.peekAt, len(in))
			}
			continue
		}
		if consumed != len(in) || len(out) == 0 || srv.state != srvAwaitFinished {
			t.Fatalf("consumed %d of %d, %d out, state %d", consumed, len(in), len(out), srv.state)
		}
	}
}

// dialRaw connects to addr and returns the connection.
func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// waitClosed waits for an error on closed, well before the handshake timeout.
func waitClosed(t *testing.T, closed <-chan error, check func(error) bool) {
	t.Helper()
	select {
	case err := <-closed:
		if !check(err) {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connection was not closed")
	}
}

// writeUntilClosed sends chunk over and over until the server hangs up, or
// limit bytes have gone.
func writeUntilClosed(conn net.Conn, chunk []byte, limit int) {
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	for sent := 0; sent < limit; sent += len(chunk) {
		if _, err := conn.Write(chunk); err != nil {
			return
		}
	}
}

// A server is not made to hold, or to read over and over, what a client
// streams ahead of a ClientHello that never completes: empty records are
// refused at once, and a hello split into records too small to complete it
// within maxHandshakeBuffer is given up on, well before the handshake times
// out.
func TestNativeServerBoundsWhatAHelloHolds(t *testing.T) {
	serverConfig, _ := tlsConfigs(t)
	closed := make(chan error, 4)
	handler := NewServer(serverConfig, fib.HandlerFuncs{Close: func(_ *fib.Connection, err error) { closed <- err }})
	handler.HandshakeTimeout = time.Minute
	_, addr := startEchoServer(t, fib.DefaultConfig(), handler)

	conn := dialRaw(t, addr)
	go writeUntilClosed(conn, bytes.Repeat([]byte{recordTypeHandshake, 3, 1, 0, 0}, 1000), 64<<20)
	waitClosed(t, closed, func(err error) bool { return err != nil && strings.Contains(err.Error(), "empty handshake record") })

	conn = dialRaw(t, addr)
	// A ClientHello of 60000 bytes, a byte a record.
	head := appendRecord(nil, recordTypeHandshake, 0x0301, []byte{hsClientHello, 0, 0xea, 0x60})
	one := bytes.Repeat(appendRecord(nil, recordTypeHandshake, 0x0301, []byte{3}), 4096)
	go func() {
		_, _ = conn.Write(head)
		writeUntilClosed(conn, one, 64<<20)
	}()
	waitClosed(t, closed, func(err error) bool { return errors.Is(err, errHandshakeBuffer) })
}

// What a client sends while GetCertificate has not answered is held up to
// maxHandshakeBuffer and no further, by a handshake without a worker and by
// one crypto/tls runs.
func TestHandshakeBufferBoundWhileGetCertificateWaits(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "crypto/tls"}[blocking], func(t *testing.T) {
			serverConfig, _ := tlsConfigs(t)
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			cert := serverConfig.Certificates[0]
			serverConfig.Certificates = nil
			serverConfig.GetCertificate = func(*stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
				<-release
				return &cert, nil
			}
			closed := make(chan error, 1)
			handler := NewServer(serverConfig, fib.HandlerFuncs{Close: func(_ *fib.Connection, err error) { closed <- err }})
			handler.Blocking = blocking
			handler.HandshakeTimeout = time.Minute
			_, addr := startEchoServer(t, fib.DefaultConfig(), handler)

			conn := dialRaw(t, addr)
			hello := s13Hello(t).record()
			junk := bytes.Repeat(appendRecord(nil, recordTypeApplicationData, 0x0303, make([]byte, 1000)), 16)
			_, _ = conn.Write(hello)
			writeUntilClosed(conn, junk, 64<<20)
			// The server hangs up while GetCertificate still waits; a
			// handshake crypto/tls runs reports it once it returns.
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.Copy(io.Discard, conn); err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the server did not hang up")
			}
			once.Do(func() { close(release) })
			waitClosed(t, closed, func(err error) bool { return errors.Is(err, errHandshakeBuffer) })
		})
	}
}
