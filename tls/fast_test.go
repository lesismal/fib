//go:build linux || darwin || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"

	fib "github.com/lesismal/fib"
)

// layerOf hands the tests a server connection's layer once its handshake has
// settled.
type layerOf chan *layer

func (l layerOf) handler() fib.Handler {
	return &settledHandler{HandlerFuncs: fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		if err := c.Send(b); err != nil {
			c.Close()
		}
	}}, layers: l}
}

type settledHandler struct {
	fib.HandlerFuncs
	layers layerOf
}

func (h *settledHandler) OnHandshake(c *fib.Connection, _ stdtls.ConnectionState) {
	h.layers <- c.Layer().(*layer)
}

// taken reports whether the connection left crypto/tls, once the handshake's
// worker has set it up either way.
func taken(l *layer) bool {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	return l.tx != nil && l.conn == nil
}

func echo(t *testing.T, conn net.Conn, sizes ...int) {
	t.Helper()
	for i, size := range sizes {
		payload := bytes.Repeat([]byte{byte('a' + i)}, size)
		go func() { _, _ = conn.Write(payload) }()
		got := make([]byte, size)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("echo of %d bytes: %v", size, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("echo of %d bytes came back different", size)
		}
	}
}

// Every version and suite a standard client can pick gets its echoes back,
// across record boundaries, and the AES-GCM ones on the layer's own record
// layer.
func TestTakeOverBySuite(t *testing.T) {
	cases := []struct {
		name    string
		version uint16
		suite   uint16
		fast    bool
	}{
		{"TLS13", stdtls.VersionTLS13, 0, true},
		{"TLS12-AES128-GCM", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, true},
		{"TLS12-AES256-GCM", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, true},
		{"TLS12-CHACHA20", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, false},
		{"TLS12-AES128-CBC", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, true},
		{"TLS12-AES128-CBC-SHA256", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, true},
		{"TLS11-AES128-CBC", stdtls.VersionTLS11, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, true},
		{"TLS11-AES256-CBC", stdtls.VersionTLS11, stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, true},
		{"TLS10-AES128-CBC", stdtls.VersionTLS10, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			serverConfig, clientConfig := suiteConfigs(t, tc.version, tc.suite)
			if tc.suite != 0 {
				clientConfig.CipherSuites = []uint16{tc.suite}
			}
			layers := make(layerOf, 1)
			_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, layers.handler()))
			conn, err := stdtls.Dial("tcp", addr, clientConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
			l := <-layers
			if got := taken(l); got != tc.fast {
				t.Fatalf("taken over from crypto/tls: %v, want %v", got, tc.fast)
			}
			echo(t, conn, 1, 1024, maxPlaintext, maxPlaintext+1, 3*maxPlaintext+7, 1<<20)
			state, ok := ConnectionState(l.c)
			if !ok || state.Version != tc.version || (tc.suite != 0 && state.CipherSuite != tc.suite) {
				t.Fatalf("ConnectionState: %v %x %x", ok, state.Version, state.CipherSuite)
			}
		})
	}
}

// A standard client resumes a session with a ticket the server issued, which
// for TLS 1.3 went out under the keys the layer then took over: the layer
// counted it, or the client's first record would not have authenticated. A
// TLS 1.2 resumption logs no secret, and stays with crypto/tls.
func TestTakeOverResumes(t *testing.T) {
	for _, version := range []uint16{stdtls.VersionTLS12, stdtls.VersionTLS13} {
		t.Run(stdtls.VersionName(version), func(t *testing.T) {
			serverConfig, clientConfig := tlsConfigs(t)
			clientConfig.MaxVersion = version
			clientConfig.ClientSessionCache = stdtls.NewLRUClientSessionCache(4)
			layers := make(layerOf, 2)
			_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, layers.handler()))
			for i := 0; i < 2; i++ {
				conn, err := stdtls.Dial("tcp", addr, clientConfig)
				if err != nil {
					t.Fatal(err)
				}
				_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
				// The echo also has the client read the ticket, which a TLS
				// 1.3 server sends after the handshake.
				echo(t, conn, 100)
				if got, want := taken(<-layers), i == 0 || version == stdtls.VersionTLS13; got != want {
					t.Fatalf("connection %d taken over: %v, want %v", i, got, want)
				}
				if resumed := conn.ConnectionState().DidResume; resumed != (i == 1) {
					t.Fatalf("connection %d resumed: %v", i, resumed)
				}
				conn.Close()
			}
		})
	}
}

// requestKeyUpdate has a taken-over connection update its keys and ask its
// peer to update theirs, as crypto/tls has no call for.
func requestKeyUpdate(t *testing.T, l *layer) {
	t.Helper()
	l.wmu.Lock()
	defer l.wmu.Unlock()
	if err := l.sealSendLocked(recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 1, 1}, nil); err != nil {
		t.Fatal(err)
	}
	next, err := l.tx.next()
	if err != nil {
		t.Fatal(err)
	}
	l.tx = next
}

// Keys updated by either side, asked for or not, carry on the connection,
// against crypto/tls and between two engines.
func TestKeyUpdate(t *testing.T) {
	t.Run("client asks a standard server", func(t *testing.T) {
		serverConfig, clientConfig := tlsConfigs(t)
		listener, err := stdtls.Listen("tcp", "127.0.0.1:0", serverConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
		client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
		layers := make(chan *layer, 1)
		var mu sync.Mutex
		var got []byte
		received := make(chan struct{}, 16)
		handler := &settledHandler{HandlerFuncs: fib.HandlerFuncs{Data: func(_ *fib.Connection, b []byte) {
			mu.Lock()
			got = append(got, b...)
			mu.Unlock()
			received <- struct{}{}
		}}, layers: layers}
		if err := Dial(client, "tcp", listener.Addr().String(), 5*time.Second, clientConfig, handler, nil); err != nil {
			t.Fatal(err)
		}
		l := <-layers
		if !taken(l) {
			t.Fatal("client not taken over from crypto/tls")
		}
		want := []byte{}
		for i := 0; i < 3; i++ {
			requestKeyUpdate(t, l)
			msg := bytes.Repeat([]byte{byte('x' + i)}, 5000)
			want = append(want, msg...)
			if err := l.c.Send(msg); err != nil {
				t.Fatal(err)
			}
			waitFor(t, &mu, &got, len(want), received)
		}
		mu.Lock()
		defer mu.Unlock()
		if !bytes.Equal(got, want) {
			t.Fatal("echo mismatch across key updates")
		}
	})

	t.Run("between engines", func(t *testing.T) {
		serverConfig, clientConfig := tlsConfigs(t)
		serverLayers := make(layerOf, 1)
		_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, serverLayers.handler()))
		client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
		clientLayers := make(chan *layer, 1)
		var mu sync.Mutex
		var got []byte
		received := make(chan struct{}, 16)
		handler := &settledHandler{HandlerFuncs: fib.HandlerFuncs{Data: func(_ *fib.Connection, b []byte) {
			mu.Lock()
			got = append(got, b...)
			mu.Unlock()
			received <- struct{}{}
		}}, layers: clientLayers}
		if err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, handler, nil); err != nil {
			t.Fatal(err)
		}
		cl, sl := <-clientLayers, <-serverLayers
		if !taken(cl) || !taken(sl) {
			t.Fatal("not taken over from crypto/tls")
		}
		want := []byte{}
		for i, l := range []*layer{cl, sl, cl, sl} {
			requestKeyUpdate(t, l)
			msg := bytes.Repeat([]byte{byte('a' + i)}, 20000)
			want = append(want, msg...)
			if err := cl.c.Send(msg); err != nil {
				t.Fatal(err)
			}
			waitFor(t, &mu, &got, len(want), received)
		}
		mu.Lock()
		defer mu.Unlock()
		if !bytes.Equal(got, want) {
			t.Fatal("echo mismatch across key updates")
		}
	})
}

func waitFor(t *testing.T, mu *sync.Mutex, got *[]byte, n int, received chan struct{}) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		mu.Lock()
		have := len(*got)
		mu.Unlock()
		if have >= n {
			return
		}
		select {
		case <-received:
		case <-deadline:
			t.Fatalf("received %d of %d bytes", have, n)
		}
	}
}

// A record that does not authenticate ends the connection with bad_record_mac,
// which the peer hears.
func TestTamperedRecordFails(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint16
		suite   uint16
	}{
		{"TLS13", stdtls.VersionTLS13, 0},
		{"TLS12-CBC", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA},
		{"TLS11-CBC", stdtls.VersionTLS11, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA},
	} {
		t.Run(tc.name, func(t *testing.T) { testTamperedRecordFails(t, tc.version, tc.suite) })
	}
}

func testTamperedRecordFails(t *testing.T, version, suite uint16) {
	serverConfig, clientConfig := suiteConfigs(t, version, suite)
	closed := make(chan error, 1)
	layers := make(layerOf, 1)
	inner := layers.handler().(*settledHandler)
	inner.Close = func(_ *fib.Connection, err error) { closed <- err }
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, inner))
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	tamper := &tamperConn{Conn: raw}
	conn := stdtls.Client(tamper, clientConfig)
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.Handshake(); err != nil {
		t.Fatal(err)
	}
	if !taken(<-layers) {
		t.Fatal("not taken over from crypto/tls")
	}
	echo(t, conn, 10)
	tamper.flip.Store(true)
	_, _ = conn.Write([]byte("hello"))
	select {
	case err := <-closed:
		var alert stdtls.AlertError
		if !errors.As(err, &alert) || alert != alertBadRecordMAC {
			t.Fatalf("closed with %v, want bad_record_mac", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection was not closed")
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil || !bytes.Contains([]byte(err.Error()), []byte("bad record MAC")) {
		t.Fatalf("client read %v, want the alert", err)
	}
}

// tamperConn flips a bit in the last byte of what it writes once flip is set.
type tamperConn struct {
	net.Conn
	flip atomicBool
}

type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (b *atomicBool) Store(v bool) { b.mu.Lock(); b.v = v; b.mu.Unlock() }
func (b *atomicBool) Load() bool   { b.mu.Lock(); defer b.mu.Unlock(); return b.v }

func (c *tamperConn) Write(p []byte) (int, error) {
	if c.flip.Load() && len(p) > 0 {
		p = append([]byte(nil), p...)
		p[len(p)-1] ^= 1
	}
	return c.Conn.Write(p)
}

// A config the layer cannot take over from leaves every connection with
// crypto/tls: a server one that picks another Config per client, a client one
// that keeps sessions.
func TestTakeOverDeclined(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	perClient := serverConfig.Clone()
	perClient.GetConfigForClient = func(*stdtls.ClientHelloInfo) (*stdtls.Config, error) { return nil, nil }
	layers := make(layerOf, 1)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(perClient, layers.handler()))
	conn, err := stdtls.Dial("tcp", addr, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if taken(<-layers) {
		t.Fatal("taken over despite GetConfigForClient")
	}
	echo(t, conn, 1, 70000)

	caching := clientConfig.Clone()
	caching.ClientSessionCache = stdtls.NewLRUClientSessionCache(1)
	if NewClient(caching, nil).fastConfigFor() != nil {
		t.Fatal("a client with a session cache would be taken over")
	}
}

func (h *Handler) fastConfigFor() *stdtls.Config {
	config, reg := h.config()
	if reg == nil {
		return nil
	}
	return config
}

// Records that reach the server a few bytes at a time, split inside their
// headers and bodies, are put back together across rounds.
func TestTakeOverDrip(t *testing.T) {
	for _, version := range []uint16{stdtls.VersionTLS12, stdtls.VersionTLS13} {
		t.Run(stdtls.VersionName(version), func(t *testing.T) {
			serverConfig, clientConfig := tlsConfigs(t)
			clientConfig.MaxVersion = version
			layers := make(layerOf, 1)
			_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, layers.handler()))
			raw, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			conn := stdtls.Client(&dripConn{Conn: raw}, clientConfig)
			_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
			if err := conn.Handshake(); err != nil {
				t.Fatal(err)
			}
			if !taken(<-layers) {
				t.Fatal("not taken over from crypto/tls")
			}
			echo(t, conn, 1, 5, 300, 2000)
		})
	}
}

// dripConn writes what it is given a few bytes at a time, pausing between
// them so that each part arrives in a read of its own.
type dripConn struct {
	net.Conn
	n int
}

func (c *dripConn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		c.n++
		k := min(len(p), 1+c.n%7)
		if _, err := c.Conn.Write(p[:k]); err != nil {
			return written, err
		}
		written += k
		p = p[k:]
		time.Sleep(50 * time.Microsecond)
	}
	return written, nil
}

// A connection that leaves crypto/tls lets its Conn go: nothing it keeps,
// such as its ConnectionState, still points into it.
func TestTakeOverReleasesConn(t *testing.T) {
	var mu sync.Mutex
	var dropped []weak.Pointer[stdtls.Conn]
	droppedConn = func(c *stdtls.Conn) {
		mu.Lock()
		dropped = append(dropped, weak.Make(c))
		mu.Unlock()
	}
	defer func() { droppedConn = nil }()
	for _, version := range []uint16{stdtls.VersionTLS12, stdtls.VersionTLS13} {
		serverConfig, clientConfig := tlsConfigs(t)
		clientConfig.MaxVersion = version
		layers := make(layerOf, 1)
		_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, layers.handler()))
		conn, err := stdtls.Dial("tcp", addr, clientConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if !taken(<-layers) {
			t.Fatal("not taken over from crypto/tls")
		}
		echo(t, conn, 100)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dropped) != 2 {
		t.Fatalf("%d Conns dropped, want 2", len(dropped))
	}
	for i := 0; i < 5; i++ {
		runtime.GC()
	}
	for i, p := range dropped {
		if p.Value() != nil {
			t.Errorf("the Conn of connection %d is still reachable", i)
		}
	}
}

// suiteConfigs are configs that settle on version and, unless it is zero,
// suite: the server offers every suite the tests pick from.
func suiteConfigs(t *testing.T, version, suite uint16) (server, client *stdtls.Config) {
	t.Helper()
	server, client = tlsConfigs(t)
	server.MinVersion = stdtls.VersionTLS10
	server.CipherSuites = []uint16{
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
	}
	client.MinVersion, client.MaxVersion = version, version
	if suite != 0 {
		client.CipherSuites = []uint16{suite}
	}
	return server, client
}

// Sealed and opened again, CBC records of every length around the block
// come back whole, and any one bit flipped in one fails to open, with the
// same alert wherever it is.
func TestCBCRecords(t *testing.T) {
	for _, version := range []uint16{stdtls.VersionTLS11, stdtls.VersionTLS12} {
		for _, id := range []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA} {
			s, ok := lookupSuite(version, id)
			if !ok {
				t.Fatalf("%x %x: not a record layer suite", version, id)
			}
			random := bytes.Repeat([]byte{7}, 32)
			// The sender's keys and the receiver's copy of them.
			client, _, err := newKeysFromMaster(version, s, bytes.Repeat([]byte{1}, 48), random, random)
			if err != nil {
				t.Fatal(err)
			}
			server, _, err := newKeysFromMaster(version, s, bytes.Repeat([]byte{1}, 48), random, random)
			if err != nil {
				t.Fatal(err)
			}
			for n := 0; n <= 70; n++ {
				payload := bytes.Repeat([]byte{byte(n)}, n)
				record, err := client.seal(nil, recordTypeApplicationData, payload[:n/2], payload[n/2:])
				if err != nil {
					t.Fatal(err)
				}
				typ, plain, _, ok := server.open(append([]byte(nil), record...))
				if !ok || typ != recordTypeApplicationData || !bytes.Equal(plain, payload) {
					t.Fatalf("%x %d bytes: opened %v %d %q", version, n, ok, typ, plain)
				}
				for bit := recordHeaderLen * 8; bit < len(record)*8; bit += 3 {
					tampered := append([]byte(nil), record...)
					tampered[bit/8] ^= 1 << (bit % 8)
					server.seq--
					if _, _, alert, ok := server.open(tampered); ok || alert != alertBadRecordMAC {
						t.Fatalf("%x %d bytes, bit %d flipped: opened %v, alert %d", version, n, bit, ok, alert)
					}
					server.seq++
				}
			}
		}
	}
}

// A TLS 1.2 Finished is a handshake record whose encrypted body may start
// like a hello, as a CBC one's random IV does now and then: the hello that
// came first is the one that names the connection, unless it asked for a
// retry.
func TestHelloRandomTakesTheFirstHello(t *testing.T) {
	record := func(typ byte, body ...byte) []byte {
		return append([]byte{typ, 3, 3, byte(len(body) >> 8), byte(len(body))}, body...)
	}
	hello := func(hsType byte, random [32]byte) []byte {
		body := append([]byte{hsType, 0, 0, 38, 3, 3}, random[:]...)
		return record(recordTypeHandshake, append(body, 0, 0, 0, 0)...)
	}
	var want, lookalike [32]byte
	for i := range want {
		want[i], lookalike[i] = byte(i), byte(0xff-i)
	}
	var stream []byte
	stream = append(stream, hello(2, helloRetryRandom)...)
	stream = append(stream, record(recordTypeChangeCipherSpec, 1)...)
	stream = append(stream, hello(2, want)...)
	stream = append(stream, record(recordTypeChangeCipherSpec, 1)...)
	stream = append(stream, hello(2, lookalike)...)
	if got, ok := helloRandom(stream, 2); !ok || got != want {
		t.Fatalf("helloRandom: %x %v, want %x", got, ok, want)
	}
}
