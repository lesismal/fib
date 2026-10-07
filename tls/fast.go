package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// Once crypto/tls has completed a handshake that settled on an AES-GCM suite
// of TLS 1.3 or 1.2, or an AES-CBC suite of TLS 1.2 or 1.1, the connection
// leaves it: the layer protects the connection's records itself, with
// record.go, and drops the crypto/tls Conn.
//
// crypto/tls does everything a record passes through one at a time and
// behind copies: the layer hands it ciphertext, it decrypts into a buffer of
// its own and copies the plaintext out, and on the way back it seals into
// another before the connection copies the record into its queue. Its Conn
// also keeps what the handshake left behind, buffers and state, for the life
// of the connection. Here a record is decrypted where the round read it and
// its plaintext handed on from there, and a send is sealed once, into a buffer
// the connection copies; what a connection keeps is its two directions' keys.
//
// The keys come from crypto/tls through Config.KeyLogWriter, the one way it
// hands them out: TLS 1.3's traffic secrets, and the master secret of TLS 1.2
// and 1.1. The
// Handler gives crypto/tls a clone of its Config whose KeyLogWriter passes
// every line on to the Config's own and keeps the secrets of the connections
// it is serving, found in the Handler's registry by the client random their
// line names. A TLS 1.2 or 1.1 handshake that resumes a session logs no
// secret, and its connection stays with crypto/tls.
//
// Where the two directions' sequence numbers stand when the handshake ends
// is found the same way the peer will check them: the records crypto/tls
// wrote and read during the handshake are kept, and those that authenticate
// under the keys that follow it are counted. For TLS 1.3 that is the session
// tickets a server sends in its first flight; for TLS 1.2 and 1.1, each
// side's Finished.
//
// Anything else stays with crypto/tls: another suite or version, a server
// Config with GetConfigForClient, whose Config would log the keys instead, a
// client Config with a ClientSessionCache, which would need the tickets
// crypto/tls no longer reads, or with renegotiation. So does a connection
// whose keys did not arrive, or whose counts are not what the protocol says.

// fastConfig is the Config crypto/tls runs a handshake with when the
// connection may leave it afterwards, and the registry its key log files
// secrets in, or nil when it may not.
func fastConfig(config *stdtls.Config, client bool) (*stdtls.Config, *registry) {
	if config == nil || (config.MaxVersion != 0 && config.MaxVersion < stdtls.VersionTLS11) {
		return nil, nil
	}
	if client {
		if config.ClientSessionCache != nil || config.Renegotiation != stdtls.RenegotiateNever {
			return nil, nil
		}
	} else if config.GetConfigForClient != nil {
		return nil, nil
	}
	reg := &registry{m: make(map[[32]byte]*capture)}
	clone := config.Clone()
	clone.KeyLogWriter = keyLogWriter{next: config.KeyLogWriter, reg: reg}
	return clone, reg
}

// registry finds a Handler's captures by their client random, for its key
// log. Each Handler has its own, so that a client and a server in one
// process, which name a connection by the same random, keep apart.
type registry struct {
	sync.Mutex
	m map[[32]byte]*capture
}

// capture collects, while crypto/tls runs a handshake, what the layer needs to
// take the connection over from it. crypto/tls reads, writes and logs keys for
// a connection on the goroutine that runs its handshake, so only mu's holders
// elsewhere, the key log, need a lock.
type capture struct {
	client bool
	reg    *registry

	// written and read are every byte crypto/tls wrote and read.
	written []byte
	read    []byte

	// random is the client random, which names the connection's lines in the
	// key log, once registered says it has been found.
	random     [32]byte
	registered bool

	mu sync.Mutex
	// The secrets from the key log: TLS 1.3's two traffic secrets, or TLS
	// 1.2's master secret. conflict marks a client random another connection
	// in the registry already had, whose lines cannot be told apart.
	clientSecret []byte
	serverSecret []byte
	master       []byte
	conflict     bool
}

// wrote records what crypto/tls wrote, and a client's random from its
// ClientHello.
func (c *capture) wrote(p []byte) {
	c.written = bufferpool.Append(c.written, p)
	if c.client {
		c.register(c.written)
	}
}

// readBytes records what crypto/tls read, and a server's client random from
// the ClientHello.
func (c *capture) readBytes(p []byte) {
	c.read = bufferpool.Append(c.read, p)
	if !c.client {
		c.register(c.read)
	}
}

// register files c under the client random once stream holds the ClientHello
// that carries it.
func (c *capture) register(stream []byte) {
	if c.registered {
		return
	}
	random, ok := helloRandom(stream, 1)
	if !ok {
		return
	}
	c.random, c.registered = random, true
	c.reg.Lock()
	if other, taken := c.reg.m[random]; taken {
		other.mu.Lock()
		other.conflict = true
		other.mu.Unlock()
		c.conflict = true
	} else {
		c.reg.m[random] = c
	}
	c.reg.Unlock()
}

// release gives up what the capture holds.
func (c *capture) release() {
	if c.registered {
		c.reg.Lock()
		if c.reg.m[c.random] == c {
			delete(c.reg.m, c.random)
		}
		c.reg.Unlock()
	}
	bufferpool.Put(c.written)
	bufferpool.Put(c.read)
	c.written, c.read = nil, nil
}

// helloRandom finds, in a stream of records, the random of the first
// handshake message of hsType, 1 for ClientHello or 2 for ServerHello, that
// opens a record: crypto/tls writes both at the start of one. A ServerHello
// that is a HelloRetryRequest is passed over for the real one behind it; a
// ClientHello sent again after one keeps its random. The first one counts
// because a TLS 1.2 Finished is a handshake record too, whose encrypted
// body, a CBC one's random IV in particular, may start with hsType.
func helloRandom(stream []byte, hsType byte) (random [32]byte, ok bool) {
	for len(stream) >= recordHeaderLen {
		size := recordHeaderLen + int(binary.BigEndian.Uint16(stream[3:5]))
		if size > len(stream) {
			// The rest has not arrived; a hello's random is well within the
			// first 43 bytes of its record, which may be enough.
			size = len(stream)
		}
		record := stream[:size]
		// Record header, then the message's type, three bytes of length and
		// two of version, then the random.
		if record[0] == recordTypeHandshake && len(record) >= 43 && record[5] == hsType {
			copy(random[:], record[11:43])
			if hsType != 2 || random != helloRetryRandom {
				return random, true
			}
		}
		stream = stream[size:]
	}
	return [32]byte{}, false
}

// helloRetryRandom is the random of a ServerHello that is a HelloRetryRequest
// (RFC 8446 section 4.1.3).
var helloRetryRandom = [32]byte{
	0xcf, 0x21, 0xad, 0x74, 0xe5, 0x9a, 0x61, 0x11, 0xbe, 0x1d, 0x8c, 0x02, 0x1e, 0x65, 0xb8, 0x91,
	0xc2, 0xa2, 0x11, 0x16, 0x7a, 0xbb, 0x8c, 0x5e, 0x07, 0x9e, 0x09, 0xe2, 0xc8, 0xa8, 0x33, 0x9c,
}

// keyLogWriter keeps the secrets crypto/tls logs for the connections reg is
// capturing, and passes every line on to next.
type keyLogWriter struct {
	next io.Writer
	reg  *registry
}

func (w keyLogWriter) Write(line []byte) (int, error) {
	w.keep(line)
	if w.next != nil {
		return w.next.Write(line)
	}
	return len(line), nil
}

// keep parses a line of the NSS key log format crypto/tls writes, "LABEL
// <client random> <secret>", in hex.
func (w keyLogWriter) keep(line []byte) {
	var fields [3][]byte
	n := 0
	for start, i := 0, 0; i <= len(line) && n < 3; i++ {
		if i == len(line) || line[i] == ' ' || line[i] == '\n' {
			if i > start {
				fields[n] = line[start:i]
				n++
			}
			start = i + 1
		}
	}
	if n != 3 || hex.DecodedLen(len(fields[1])) != 32 {
		return
	}
	var random [32]byte
	if _, err := hex.Decode(random[:], fields[1]); err != nil {
		return
	}
	secret := make([]byte, hex.DecodedLen(len(fields[2])))
	if _, err := hex.Decode(secret, fields[2]); err != nil {
		return
	}
	w.reg.Lock()
	c := w.reg.m[random]
	w.reg.Unlock()
	if c == nil {
		return
	}
	c.mu.Lock()
	switch string(fields[0]) {
	case "CLIENT_TRAFFIC_SECRET_0":
		c.clientSecret = secret
	case "SERVER_TRAFFIC_SECRET_0":
		c.serverSecret = secret
	case "CLIENT_RANDOM":
		c.master = secret
	}
	c.mu.Unlock()
}

// takeOver moves an established connection off crypto/tls if it can, and
// reports whether it did. It runs on the handshake's worker once the
// handshake has completed, before anything is sent or read past it.
func (t *layer) takeOver() bool {
	c := t.capture
	t.capture = nil
	if c == nil {
		return false
	}
	defer c.release()
	c.mu.Lock()
	conflict, clientSecret, serverSecret, master := c.conflict, c.clientSecret, c.serverSecret, c.master
	c.mu.Unlock()
	if conflict {
		return false
	}
	state := t.conn.ConnectionState()
	s, ok := lookupSuite(state.Version, state.CipherSuite)
	if !ok {
		return false
	}
	var client, server *recordKeys
	var err error
	if state.Version == stdtls.VersionTLS13 {
		if clientSecret == nil || serverSecret == nil {
			return false
		}
		if client, err = newKeys13(s.keyLen, s.hash, clientSecret); err != nil {
			return false
		}
		if server, err = newKeys13(s.keyLen, s.hash, serverSecret); err != nil {
			return false
		}
	} else {
		serverHello := c.written
		if c.client {
			serverHello = c.read
		}
		serverRandom, ok := helloRandom(serverHello, 2)
		if master == nil || !ok {
			return false
		}
		if client, server, err = newKeysFromMaster(state.Version, s, master, c.random[:], serverRandom[:]); err != nil {
			return false
		}
	}
	rx, tx := client, server
	if c.client {
		rx, tx = server, client
	}
	rx.seq = sealedCount(c.read, rx)
	tx.seq = sealedCount(c.written, tx)
	if !rx.tls13() && (rx.seq != 1 || tx.seq != 1) {
		// Each side's Finished, and nothing else, goes under the keys a TLS
		// 1.2 or 1.1 handshake ends with.
		return false
	}
	// TLSUnique points into the Conn, which it would keep alive.
	state.TLSUnique = bytes.Clone(state.TLSUnique)
	t.state = &state
	t.rx, t.tx = rx, tx
	if droppedConn != nil {
		droppedConn(t.conn)
	}
	t.conn = nil
	return true
}

// sealedCount is how many of the records in stream, in order, authenticate
// under k at the sequence numbers that follow from zero. For TLS 1.2 only the
// records after the ChangeCipherSpec are candidates, for TLS 1.3 only those
// that look like application data, which every protected record does.
func sealedCount(stream []byte, k *recordKeys) uint64 {
	var n uint64
	changed := false
	for len(stream) >= recordHeaderLen {
		size := recordHeaderLen + int(binary.BigEndian.Uint16(stream[3:5]))
		if size > len(stream) {
			break
		}
		record := stream[:size]
		stream = stream[size:]
		typ := record[0]
		if typ == recordTypeChangeCipherSpec {
			changed = true
			continue
		}
		if k.tls13() && typ != recordTypeApplicationData || !k.tls13() && !changed {
			continue
		}
		if k.authenticates(record, n) {
			n++
		}
	}
	return n
}

// droppedConn, set by tests, sees each Conn a connection leaves.
var droppedConn func(*stdtls.Conn)

// maxUselessRecords bounds the records in a row that carry no application
// data, as crypto/tls does, so that a peer cannot keep the connection busy
// with them.
const maxUselessRecords = 16

// drainFast decrypts the records in what was collected during the handshake
// and in the round's bytes and hands their plaintext to handler. The caller
// holds readMu.
func (t *layer) drainFast(handler fib.Handler) {
	t.mu.Lock()
	in, src := t.in[t.inHead:], t.src
	t.mu.Unlock()
	ok := true
	if len(in) > 0 {
		ok = t.openStream(handler, in)
		t.mu.Lock()
		t.releaseInLocked()
		t.mu.Unlock()
	}
	if ok && len(src) > 0 {
		t.openStream(handler, src)
	}
	t.mu.Lock()
	t.src = nil
	t.mu.Unlock()
}

// openStream decrypts the records in data, which it may overwrite, keeping
// the start of one data ends before in pend. It reports false once the
// connection has failed or closed.
func (t *layer) openStream(handler fib.Handler, data []byte) bool {
	maxSize := recordHeaderLen + t.rx.maxBody()
	for len(data) > 0 {
		if t.dropsInput() {
			return false
		}
		var record []byte
		pending := len(t.pend) > 0 || len(data) < recordHeaderLen
		if pending {
			if need := recordHeaderLen - len(t.pend); need > 0 {
				k := min(need, len(data))
				t.pend = bufferpool.Append(t.pend, data[:k])
				data = data[k:]
				if len(t.pend) < recordHeaderLen {
					return true
				}
			}
			size := recordHeaderLen + int(binary.BigEndian.Uint16(t.pend[3:5]))
			if size > maxSize {
				t.fail(alertRecordOverflow)
				return false
			}
			k := min(size-len(t.pend), len(data))
			t.pend = bufferpool.Append(t.pend, data[:k])
			data = data[k:]
			if len(t.pend) < size {
				return true
			}
			record = t.pend
		} else {
			size := recordHeaderLen + int(binary.BigEndian.Uint16(data[3:5]))
			if size > maxSize {
				t.fail(alertRecordOverflow)
				return false
			}
			if len(data) < size {
				t.pend = bufferpool.Append(t.pend, data)
				return true
			}
			record, data = data[:size], data[size:]
		}
		ok := t.openRecord(handler, record)
		if pending {
			bufferpool.Put(t.pend)
			t.pend = nil
		}
		if !ok {
			return false
		}
	}
	return true
}

// dropsInput reports whether what is left of the input goes undelivered: once
// the connection has closed, unless the handshake's worker is still
// delivering what arrived with the handshake, which the peer sent before it
// closed and which the close leaves in place until then.
func (t *layer) dropsInput() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed && !t.settling
}

// openRecord decrypts one record and acts on it.
func (t *layer) openRecord(handler fib.Handler, record []byte) bool {
	typ, plain, alert, ok := t.rx.open(record)
	if !ok {
		t.fail(alert)
		return false
	}
	if t.rx.tls13() && typ != recordTypeHandshake && len(t.hsBuf) > 0 {
		// A handshake message split across records must not have other
		// records between its parts.
		t.fail(alertUnexpectedMessage)
		return false
	}
	switch typ {
	case recordTypeApplicationData:
		if len(plain) > 0 {
			t.useless = 0
			handler.OnData(t.c, plain)
			return true
		}
	case recordTypeAlert:
		if !t.handleAlert(plain) {
			return false
		}
	case recordTypeHandshake:
		if !t.handlePostHandshake(plain) {
			return false
		}
	default:
		t.fail(alertUnexpectedMessage)
		return false
	}
	if t.useless++; t.useless > maxUselessRecords {
		t.fail(alertUnexpectedMessage)
		return false
	}
	return true
}

// handleAlert acts on an alert the peer sent.
func (t *layer) handleAlert(data []byte) bool {
	if len(data) != 2 {
		t.fail(alertUnexpectedMessage)
		return false
	}
	if data[1] == alertCloseNotify {
		// The peer has finished cleanly.
		t.c.CloseWithError(io.EOF)
		return false
	}
	if !t.rx.tls13() && data[0] == alertLevelWarning {
		return true
	}
	t.c.CloseWithError(&net.OpError{Op: "remote error", Err: stdtls.AlertError(data[1])})
	return false
}

// handlePostHandshake acts on the handshake messages that follow the
// handshake: TLS 1.3's KeyUpdate, and the session tickets a server sends a
// client, which this client does not keep; see fastConfig. A TLS 1.2 peer
// that sends one is asking to renegotiate, which is refused.
func (t *layer) handlePostHandshake(data []byte) bool {
	if !t.rx.tls13() {
		t.fail(alertNoRenegotiation)
		return false
	}
	t.hsBuf = bufferpool.Append(t.hsBuf, data)
	for len(t.hsBuf) >= 4 {
		size := 4 + (int(t.hsBuf[1])<<16 | int(t.hsBuf[2])<<8 | int(t.hsBuf[3]))
		if size > 4+maxPlaintext*4 {
			t.fail(alertUnexpectedMessage)
			return false
		}
		if len(t.hsBuf) < size {
			return true
		}
		msg := t.hsBuf[:size]
		rest := len(t.hsBuf) - size
		switch {
		case msg[0] == typeNewSessionTicket && t.client:
		case msg[0] == typeKeyUpdate:
			// A KeyUpdate ends its record, since the next one is under the
			// new keys.
			if size != 5 || msg[4] > 1 || rest != 0 {
				t.fail(alertUnexpectedMessage)
				return false
			}
			requested := msg[4] == 1
			next, err := t.rx.next()
			if err != nil {
				t.fail(alertUnexpectedMessage)
				return false
			}
			t.rx = next
			if requested && !t.updateWriteKeys() {
				return false
			}
		default:
			t.fail(alertUnexpectedMessage)
			return false
		}
		t.hsBuf = t.hsBuf[:copy(t.hsBuf, t.hsBuf[size:])]
	}
	if len(t.hsBuf) == 0 {
		bufferpool.Put(t.hsBuf)
		t.hsBuf = nil
	}
	return true
}

// updateWriteKeys answers a KeyUpdate that asked for ours to be updated: a
// KeyUpdate of our own, not asking for one back, under the current keys, and
// the new keys from then on.
func (t *layer) updateWriteKeys() bool {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if err := t.sealSendLocked(recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 1, 0}, nil); err != nil {
		t.c.CloseWithError(err)
		return false
	}
	next, err := t.tx.next()
	if err != nil {
		t.c.CloseWithError(err)
		return false
	}
	t.tx = next
	return true
}

// fail answers a record the connection cannot go on from with alert, and
// closes it. The alert is flushed first, since a close drops what is still
// queued.
func (t *layer) fail(alert uint8) {
	t.wmu.Lock()
	_ = t.sealSendLocked(recordTypeAlert, []byte{alertLevelError, alert}, nil)
	t.wmu.Unlock()
	_ = t.c.Flush()
	t.c.CloseWithError(&net.OpError{Op: "local error", Err: stdtls.AlertError(alert)})
}

var errSequenceExhausted = errors.New("fib: tls sequence number exhausted")

// sealSendLocked seals first followed by second into records of typ and sends
// them. The caller holds wmu.
func (t *layer) sealSendLocked(typ byte, first, second []byte) error {
	n := len(first) + len(second)
	records := max(1, (n+maxPlaintext-1)/maxPlaintext)
	if t.tx.seq > ^uint64(0)-uint64(records) {
		return errSequenceExhausted
	}
	buf := bufferpool.Get(n + records*t.tx.overhead())[:0]
	for {
		a := first[:min(len(first), maxPlaintext)]
		b := second[:min(len(second), maxPlaintext-len(a))]
		var err error
		if buf, err = t.tx.seal(buf, typ, a, b); err != nil {
			bufferpool.Put(buf)
			return err
		}
		first, second = first[len(a):], second[len(b):]
		if len(first)+len(second) == 0 {
			break
		}
	}
	// The connection takes the sealed records over, rather than copying
	// them into its queue while it is corked.
	return t.c.SendRawPooled(buf)
}
