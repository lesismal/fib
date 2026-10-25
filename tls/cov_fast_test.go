//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	stdtls "crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// covFastPair is a client and a server engine connection, both taken over
// from crypto/tls, with what each side's handler hears.
type covFastPair struct {
	cl, sl       *layer
	clientClosed chan error
	serverClosed chan error
	clientData   chan []byte
}

// covFastStart connects a client engine to a server engine under version.
func covFastStart(t *testing.T, version uint16, options DialOptions) *covFastPair {
	t.Helper()
	serverConfig, clientConfig := tlsConfigs(t)
	clientConfig.MaxVersion = version
	p := &covFastPair{
		clientClosed: make(chan error, 1),
		serverClosed: make(chan error, 1),
		clientData:   make(chan []byte, 64),
	}
	serverLayers := make(layerOf, 1)
	server := serverLayers.handler().(*settledHandler)
	server.Close = func(_ *fib.Connection, err error) { p.serverClosed <- err }
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, server))
	clientEngine, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	clientLayers := make(chan *layer, 1)
	client := &settledHandler{HandlerFuncs: fib.HandlerFuncs{
		Data:  func(_ *fib.Connection, b []byte) { p.clientData <- bytes.Clone(b) },
		Close: func(_ *fib.Connection, err error) { p.clientClosed <- err },
	}, layers: clientLayers}
	if err := DialWithOptions(clientEngine, "tcp", addr, 5*time.Second, clientConfig, options, client, nil); err != nil {
		t.Fatal(err)
	}
	p.cl, p.sl = <-clientLayers, <-serverLayers
	if !taken(p.cl) || !taken(p.sl) {
		t.Fatal("not taken over from crypto/tls")
	}
	covFastSettled(t, p.cl)
	covFastSettled(t, p.sl)
	return p
}

// covFastSettled waits for the handshake's worker to have delivered what
// arrived with the handshake, after which only reads take readMu.
func covFastSettled(t *testing.T, l *layer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		l.mu.Lock()
		settling := l.settling
		l.mu.Unlock()
		if !settling {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the handshake did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

// covFastSend seals a record of typ carrying payload under l's keys and sends
// it, whatever it is.
func covFastSend(t *testing.T, l *layer, typ byte, payload []byte) {
	t.Helper()
	l.wmu.Lock()
	defer l.wmu.Unlock()
	if err := l.sealSendLocked(typ, payload, nil); err != nil {
		t.Fatal(err)
	}
}

// covFastEcho has the client send msg and waits for the server's echo.
func (p *covFastPair) covFastEcho(t *testing.T, msg string) {
	t.Helper()
	if err := p.cl.c.Send([]byte(msg)); err != nil {
		t.Fatal(err)
	}
	var got []byte
	deadline := time.After(10 * time.Second)
	for len(got) < len(msg) {
		select {
		case b := <-p.clientData:
			got = append(got, b...)
		case err := <-p.serverClosed:
			t.Fatalf("server closed instead of echoing: %v", err)
		case <-deadline:
			t.Fatalf("no echo of %q", msg)
		}
	}
	if string(got) != msg {
		t.Fatalf("echo %q, want %q", got, msg)
	}
}

// covFastWantAlert waits for a close on ch with the alert op ("local error"
// for one sent, "remote error" for one received) names.
func covFastWantAlert(t *testing.T, ch chan error, op string, alert uint8) {
	t.Helper()
	select {
	case err := <-ch:
		var opErr *net.OpError
		var got stdtls.AlertError
		if !errors.As(err, &opErr) || opErr.Op != op || !errors.As(err, &got) || uint8(got) != alert {
			t.Fatalf("closed with %v, want %s alert %d", err, op, alert)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("not closed; want %s alert %d", op, alert)
	}
}

func covFastWantClose(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("not closed")
	}
	return nil
}

// Records a TLS 1.3 peer cannot send after the handshake end the connection
// with unexpected_message, which the peer hears; a close_notify ends it
// cleanly, and any other alert as the error it names.
func TestCovFastTLS13Refusals(t *testing.T) {
	type send struct {
		typ     byte
		payload []byte
	}
	for _, tc := range []struct {
		name  string
		sends []send
	}{
		{"alert of three bytes", []send{{recordTypeAlert, []byte{alertLevelError, alertUnexpectedMessage, 0}}}},
		{"unknown content type", []send{{25, []byte("x")}}},
		{"data inside a split handshake message", []send{
			{recordTypeHandshake, []byte{typeNewSessionTicket, 0, 0, 10, 1, 2}},
			{recordTypeApplicationData, []byte("x")},
		}},
		{"oversized handshake message", []send{{recordTypeHandshake, []byte{typeNewSessionTicket, 0x10, 0, 0}}}},
		{"KeyUpdate with a bad request", []send{{recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 1, 2}}}},
		{"KeyUpdate of the wrong length", []send{{recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 2, 0, 0}}}},
		{"KeyUpdate not ending its record", []send{{recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 1, 0, typeKeyUpdate}}}},
		{"ClientHello after the handshake", []send{{recordTypeHandshake, []byte{1, 0, 0, 0}}}},
		{"NewSessionTicket to a server", []send{{recordTypeHandshake, []byte{typeNewSessionTicket, 0, 0, 0}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := covFastStart(t, stdtls.VersionTLS13, DialOptions{})
			p.covFastEcho(t, "before")
			for _, s := range tc.sends {
				covFastSend(t, p.cl, s.typ, s.payload)
			}
			covFastWantAlert(t, p.serverClosed, "local error", alertUnexpectedMessage)
			covFastWantAlert(t, p.clientClosed, "remote error", alertUnexpectedMessage)
		})
	}

	t.Run("close_notify", func(t *testing.T) {
		p := covFastStart(t, stdtls.VersionTLS13, DialOptions{})
		covFastSend(t, p.cl, recordTypeAlert, []byte{alertLevelWarning, alertCloseNotify})
		if err := covFastWantClose(t, p.serverClosed); err != io.EOF {
			t.Fatalf("server closed with %v, want io.EOF", err)
		}
	})

	t.Run("warning alert", func(t *testing.T) {
		// TLS 1.3 has no warnings but close_notify and user_canceled, and
		// user_canceled is followed by a close: either way the connection ends.
		p := covFastStart(t, stdtls.VersionTLS13, DialOptions{})
		covFastSend(t, p.cl, recordTypeAlert, []byte{alertLevelWarning, 90})
		covFastWantAlert(t, p.serverClosed, "remote error", 90)
	})

	t.Run("records without data", func(t *testing.T) {
		p := covFastStart(t, stdtls.VersionTLS13, DialOptions{})
		// As many as are allowed in a row, then data, which starts the count
		// again.
		for i := 0; i < maxUselessRecords; i++ {
			covFastSend(t, p.cl, recordTypeApplicationData, nil)
		}
		p.covFastEcho(t, "data")
		for i := 0; i <= maxUselessRecords; i++ {
			covFastSend(t, p.cl, recordTypeApplicationData, nil)
		}
		covFastWantAlert(t, p.serverClosed, "local error", alertUnexpectedMessage)
		covFastWantAlert(t, p.clientClosed, "remote error", alertUnexpectedMessage)
	})

	t.Run("KeyUpdate answer cannot be sealed", func(t *testing.T) {
		p := covFastStart(t, stdtls.VersionTLS13, DialOptions{})
		p.sl.wmu.Lock()
		p.sl.tx.seq = ^uint64(0)
		p.sl.wmu.Unlock()
		covFastSend(t, p.cl, recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 1, 1})
		if err := covFastWantClose(t, p.serverClosed); !errors.Is(err, errSequenceExhausted) {
			t.Fatalf("server closed with %v, want %v", err, errSequenceExhausted)
		}
	})
}

// A client that keeps tickets refuses a malformed one with decode_error.
func TestCovFastMalformedTicket(t *testing.T) {
	p := covFastStart(t, stdtls.VersionTLS13, DialOptions{SessionCache: NewQUICSessionCache(4)})
	p.cl.readMu.Lock()
	sink := p.cl.sink
	p.cl.readMu.Unlock()
	if sink == nil {
		t.Fatal("the client keeps no tickets")
	}
	p.covFastEcho(t, "before")
	covFastSend(t, p.sl, recordTypeHandshake, []byte{typeNewSessionTicket, 0, 0, 1, 0})
	covFastWantAlert(t, p.clientClosed, "local error", alertDecodeError)
	covFastWantAlert(t, p.serverClosed, "remote error", alertDecodeError)
}

// A TLS 1.2 connection taken over by a client through crypto/tls ignores a
// warning alert and refuses renegotiation.
func TestCovFastTLS12(t *testing.T) {
	t.Run("warning alert", func(t *testing.T) {
		p := covFastStart(t, stdtls.VersionTLS12, DialOptions{Blocking: true})
		if p.cl.state.Version != stdtls.VersionTLS12 {
			t.Fatalf("version %x", p.cl.state.Version)
		}
		p.covFastEcho(t, "before")
		covFastSend(t, p.cl, recordTypeAlert, []byte{alertLevelWarning, 90})
		p.covFastEcho(t, "after")
	})
	t.Run("renegotiation", func(t *testing.T) {
		p := covFastStart(t, stdtls.VersionTLS12, DialOptions{Blocking: true})
		covFastSend(t, p.cl, recordTypeHandshake, []byte{0, 0, 0, 0})
		covFastWantAlert(t, p.serverClosed, "local error", alertNoRenegotiation)
		covFastWantAlert(t, p.clientClosed, "remote error", alertNoRenegotiation)
	})
}

// A record whose header claims more than the protocol allows ends the
// connection with record_overflow, whether its header arrives whole or in
// parts; once the connection has closed, what is left goes undelivered.
func TestCovFastOpenStream(t *testing.T) {
	var delivered []byte
	handler := fib.HandlerFuncs{Data: func(_ *fib.Connection, b []byte) { delivered = append(delivered, b...) }}
	for _, split := range []bool{false, true} {
		name := "whole header"
		if split {
			name = "split header"
		}
		t.Run(name, func(t *testing.T) {
			p := covFastStart(t, stdtls.VersionTLS13, DialOptions{})
			header := []byte{recordTypeApplicationData, 3, 3, 0xff, 0xff}
			p.sl.readMu.Lock()
			var ok bool
			if split {
				if !p.sl.openStream(handler, header[:3]) || len(p.sl.pend) != 3 {
					p.sl.readMu.Unlock()
					t.Fatal("the start of a header was not kept")
				}
				ok = p.sl.openStream(handler, header[3:])
			} else {
				ok = p.sl.openStream(handler, header)
			}
			p.sl.readMu.Unlock()
			if ok {
				t.Fatal("an oversized record was accepted")
			}
			covFastWantAlert(t, p.serverClosed, "local error", alertRecordOverflow)
			covFastWantAlert(t, p.clientClosed, "remote error", alertRecordOverflow)

			// Closed now: a record that would open is dropped.
			p.cl.wmu.Lock()
			record, err := p.cl.tx.seal(nil, recordTypeApplicationData, []byte("late"), nil)
			p.cl.wmu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			p.sl.readMu.Lock()
			ok = p.sl.openStream(handler, record)
			p.sl.readMu.Unlock()
			if ok || len(delivered) != 0 {
				t.Fatalf("input after the close: %v, delivered %q", ok, delivered)
			}
		})
	}
}

// covFastRecConn hands what crypto/tls reads and writes to a capture, as the
// layer does.
type covFastRecConn struct {
	net.Conn
	c *capture
}

func (r *covFastRecConn) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	r.c.readBytes(p[:n])
	return n, err
}

func (r *covFastRecConn) Write(p []byte) (int, error) {
	r.c.wrote(p)
	return r.Conn.Write(p)
}

// covFastHandshake runs a crypto/tls handshake over loopback TCP, the
// server's side through wrap, and returns both Conns and the server's socket.
func covFastHandshake(t *testing.T, serverConfig, clientConfig *stdtls.Config, wrap func(net.Conn) net.Conn) (server, client *stdtls.Conn, serverRaw net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	clientRaw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	serverRaw = <-accepted
	if serverRaw == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { clientRaw.Close(); serverRaw.Close() })
	_ = clientRaw.SetDeadline(time.Now().Add(10 * time.Second))
	_ = serverRaw.SetDeadline(time.Now().Add(10 * time.Second))
	server = stdtls.Server(wrap(serverRaw), serverConfig)
	client = stdtls.Client(clientRaw, clientConfig)
	var wg sync.WaitGroup
	var serverErr error
	wg.Add(1)
	go func() { defer wg.Done(); serverErr = server.Handshake() }()
	clientErr := client.Handshake()
	wg.Wait()
	if serverErr != nil || clientErr != nil {
		t.Fatalf("handshake: server %v, client %v", serverErr, clientErr)
	}
	return server, client, serverRaw
}

// covFastWithoutLast is stream without its last record.
func covFastWithoutLast(stream []byte) []byte {
	last := 0
	for i := 0; i+recordHeaderLen <= len(stream); {
		last = i
		i += recordHeaderLen + int(binary.BigEndian.Uint16(stream[i+3:i+5]))
	}
	return stream[:last]
}

// A TLS 1.2 server connection crypto/tls handshook leaves it under keys that
// carry on where its records stopped, unless its record counts are not what
// the protocol says; a TLS 1.3 one whose secrets were not logged stays.
func TestCovFastTakeOver(t *testing.T) {
	t.Run("TLS 1.2", func(t *testing.T) {
		serverConfig, clientConfig := suiteConfigs(t, stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
		fast, reg := fastConfig(serverConfig, false)
		c := &capture{reg: reg}
		server, client, serverRaw := covFastHandshake(t, fast, clientConfig, func(conn net.Conn) net.Conn {
			return &covFastRecConn{Conn: conn, c: c}
		})
		if !c.registered || c.master == nil {
			t.Fatal("the handshake was not captured")
		}

		// The same handshake as if the client's Finished had not been seen.
		short := &capture{reg: reg, random: c.random, master: c.master,
			written: bytes.Clone(c.written), read: covFastWithoutLast(bytes.Clone(c.read))}
		if (&layer{conn: server, capture: short}).takeOver() {
			t.Fatal("taken over without the client's Finished")
		}

		l := &layer{conn: server, capture: c}
		if !l.takeOver() || l.conn != nil || l.capture != nil {
			t.Fatal("not taken over")
		}
		if l.state.Version != stdtls.VersionTLS12 || l.rx.seq != 1 || l.tx.seq != 1 {
			t.Fatalf("version %x, rx seq %d, tx seq %d", l.state.Version, l.rx.seq, l.tx.seq)
		}
		if _, taken := reg.m[c.random]; taken {
			t.Fatal("the capture is still registered")
		}
		// The client's crypto/tls reads what the layer seals, and the layer
		// opens what it writes.
		record, err := l.tx.seal(nil, recordTypeApplicationData, []byte("from the layer"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := serverRaw.Write(record); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len("from the layer"))
		if _, err := io.ReadFull(client, got); err != nil || string(got) != "from the layer" {
			t.Fatalf("client read %q, %v", got, err)
		}
		if _, err := client.Write([]byte("from the client")); err != nil {
			t.Fatal(err)
		}
		in := make([]byte, recordHeaderLen)
		if _, err := io.ReadFull(serverRaw, in); err != nil {
			t.Fatal(err)
		}
		in = append(in, make([]byte, binary.BigEndian.Uint16(in[3:5]))...)
		if _, err := io.ReadFull(serverRaw, in[recordHeaderLen:]); err != nil {
			t.Fatal(err)
		}
		if typ, plain, alert, ok := l.rx.open(in); !ok || typ != recordTypeApplicationData || string(plain) != "from the client" {
			t.Fatalf("opened %v %d %q, alert %d", ok, typ, plain, alert)
		}
	})

	t.Run("TLS 1.3 without secrets", func(t *testing.T) {
		serverConfig, clientConfig := suiteConfigs(t, stdtls.VersionTLS13, 0)
		server, _, _ := covFastHandshake(t, serverConfig, clientConfig, func(conn net.Conn) net.Conn { return conn })
		c := &capture{reg: &registry{m: make(map[[32]byte]*capture)}, clientSecret: []byte{1}}
		l := &layer{conn: server, capture: c}
		if l.takeOver() || l.conn != server || l.rx != nil {
			t.Fatal("taken over without the server's traffic secret")
		}
	})
}

// covFastHello is a stream whose first record is a ClientHello with random.
func covFastHello(random [32]byte) []byte {
	body := append([]byte{1, 0, 0, 38, 3, 3}, random[:]...)
	body = append(body, 0, 0, 0, 0)
	return append([]byte{recordTypeHandshake, 3, 1, byte(len(body) >> 8), byte(len(body))}, body...)
}

// Two connections whose ClientHellos carry one random cannot tell their key
// log lines apart: both are marked, neither leaves crypto/tls, and each
// releases only its own registration. A stream that does not hold a hello
// yet registers nothing.
func TestCovFastRegisterConflict(t *testing.T) {
	var random [32]byte
	for i := range random {
		random[i] = byte(i + 1)
	}
	stream := covFastHello(random)
	reg := &registry{m: make(map[[32]byte]*capture)}

	early := &capture{reg: reg}
	early.register(stream[:20])
	if early.registered || len(reg.m) != 0 {
		t.Fatal("registered before the random arrived")
	}
	if _, ok := helloRandom(stream[:20], 1); ok {
		t.Fatal("a random found in a cut hello")
	}
	if _, ok := helloRandom(append([]byte{recordTypeAlert, 3, 3, 0, 2, 1, 0}, stream...)[:7], 1); ok {
		t.Fatal("a random found in an alert")
	}

	a, b := &capture{reg: reg}, &capture{reg: reg}
	a.register(stream)
	b.register(stream)
	a.register(stream) // already registered: nothing changes
	if !a.conflict || !b.conflict || reg.m[random] != a {
		t.Fatalf("conflict a %v b %v, registered %p want %p", a.conflict, b.conflict, reg.m[random], a)
	}
	if (&layer{capture: b}).takeOver() {
		t.Fatal("taken over despite the conflict")
	}
	if reg.m[random] != a {
		t.Fatal("the conflicting capture released the other's registration")
	}
	if (&layer{capture: a}).takeOver() {
		t.Fatal("taken over despite the conflict")
	}
	if len(reg.m) != 0 {
		t.Fatal("registration not released")
	}
}

// The key log keeps the secrets of a registered connection, passes every
// line on, and passes over lines it cannot read or that name no connection.
func TestCovFastKeyLog(t *testing.T) {
	var random, other [32]byte
	for i := range random {
		random[i], other[i] = byte(i), byte(0xff-i)
	}
	reg := &registry{m: make(map[[32]byte]*capture)}
	c := &capture{reg: reg}
	c.register(covFastHello(random))
	var out bytes.Buffer
	w := keyLogWriter{next: &out, reg: reg}
	r, o := hex.EncodeToString(random[:]), hex.EncodeToString(other[:])
	lines := []string{
		"CLIENT_RANDOM onlytwo\n",
		"CLIENT_RANDOM " + string(bytes.Repeat([]byte{'z'}, 64)) + " 00\n",
		"CLIENT_RANDOM " + r + " zz\n",
		"CLIENT_RANDOM " + o + " 0011\n",
		"EXPORTER_SECRET " + r + " 0022\n",
	}
	for _, line := range lines {
		if n, err := w.Write([]byte(line)); n != len(line) || err != nil {
			t.Fatalf("Write: %d %v", n, err)
		}
	}
	if c.master != nil || c.clientSecret != nil || c.serverSecret != nil {
		t.Fatal("a secret kept from a line that does not give one")
	}
	for _, line := range []string{
		"CLIENT_RANDOM " + r + " 0102\n",
		"CLIENT_TRAFFIC_SECRET_0 " + r + " 0304\n",
		"SERVER_TRAFFIC_SECRET_0 " + r + " 0506\n",
	} {
		lines = append(lines, line)
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(c.master, []byte{1, 2}) || !bytes.Equal(c.clientSecret, []byte{3, 4}) || !bytes.Equal(c.serverSecret, []byte{5, 6}) {
		t.Fatalf("kept %x %x %x", c.master, c.clientSecret, c.serverSecret)
	}
	var all string
	for _, line := range lines {
		all += line
	}
	if out.String() != all {
		t.Fatal("not every line was passed on")
	}
	c.release()
}

// A Config that allows nothing above TLS 1.0 never leaves crypto/tls.
func TestCovFastConfigVersions(t *testing.T) {
	if config, reg := fastConfig(&stdtls.Config{MaxVersion: stdtls.VersionTLS10}, false); config != nil || reg != nil {
		t.Fatal("a TLS 1.0 Config would be taken over")
	}
	if config, reg := fastConfig(&stdtls.Config{MaxVersion: stdtls.VersionTLS11}, false); config == nil || reg == nil {
		t.Fatal("a TLS 1.1 Config would not be taken over")
	}
}

// A record that has not arrived whole is not counted, nor anything behind it.
func TestCovFastSealedCountStopsShort(t *testing.T) {
	seal, open := recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	record, err := seal.seal(nil, recordTypeApplicationData, []byte("ticket"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := sealedCount(record, open); n != 1 {
		t.Fatalf("whole record counted %d", n)
	}
	if n := sealedCount(record[:len(record)-1], open); n != 0 {
		t.Fatalf("cut record counted %d", n)
	}
}

// A sequence number that would wrap stops a send before anything is sealed.
func TestCovFastSequenceExhausted(t *testing.T) {
	seal, _ := recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	seal.seq = ^uint64(0) - 1
	l := &layer{}
	if err := l.sealSendWith(seal, recordTypeApplicationData, make([]byte, maxPlaintext+1), nil); !errors.Is(err, errSequenceExhausted) {
		t.Fatalf("sealSendWith: %v, want %v", err, errSequenceExhausted)
	}
	if seal.seq != ^uint64(0)-1 {
		t.Fatal("a record was sealed")
	}
}

// Keys of a length AES does not take, or longer than HKDF can expand, are
// refused rather than built.
func TestCovFastKeyErrors(t *testing.T) {
	if _, err := newGCM(make([]byte, 17)); err == nil {
		t.Fatal("a 17-byte AES key was taken")
	}
	secret := bytes.Repeat([]byte{9}, sha256.Size)
	if _, err := newKeys13(suite{keyLen: 17, hash: sha256.New}, sha256.New, secret); err == nil {
		t.Fatal("TLS 1.3 keys of 17 bytes were made")
	}
	if _, err := newKeys13(suite{keyLen: 255*sha256.Size + 1, hash: sha256.New}, sha256.New, secret); err == nil {
		t.Fatal("TLS 1.3 keys longer than HKDF expands were made")
	}
	master, random := bytes.Repeat([]byte{1}, 48), bytes.Repeat([]byte{2}, 32)
	for _, s := range []suite{
		{mode: modeGCM12, keyLen: 17, hash: sha256.New},
		{mode: modeCBC, keyLen: 17, hash: sha256.New, mac: sha1.New, macLen: sha1.Size},
	} {
		if c, srv, err := newKeysFromMaster(stdtls.VersionTLS12, s, master, random, random); err == nil || c != nil || srv != nil {
			t.Fatalf("mode %d: keys of 17 bytes were made", s.mode)
		}
	}
}
