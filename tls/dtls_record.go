package tls

import (
	"bytes"
	"crypto/hmac"
	stdtls "crypto/tls"
	"errors"
	"slices"
	"time"
)

// The record and message framing of DTLS 1.2 (RFC 6347 sections 4.1 and
// 4.2.3): records with their epoch and sequence number, protected by the
// record keys of record.go, and handshake messages cut into fragments that
// fit a datagram and put back together, in order, on the other side.

const (
	// dtlsRecordHeaderLen is a record's header: type, version, epoch, a
	// 48-bit sequence number, length.
	dtlsRecordHeaderLen = 13
	// dtlsHandshakeHeaderLen is a handshake fragment's header: type, length,
	// message_seq, fragment_offset, fragment_length.
	dtlsHandshakeHeaderLen = 12

	hsHelloVerifyRequest = 3

	// dtlsMaxAhead bounds how far past the next message a message may be
	// that is kept for later, dtlsMaxBuffered the bytes those take, and
	// dtlsMaxFuture the records of the next epoch kept until its keys are
	// known.
	dtlsMaxAhead    = 16
	dtlsMaxBuffered = 256 << 10
	dtlsMaxFuture   = 16
	// dtlsMaxHello bounds a ClientHello a server reassembles before it has
	// checked its cookie.
	dtlsMaxHello = 8 << 10
	// dtlsMaxSpans bounds the separate parts of a message a reassembly
	// keeps: a fragment that would start another one past it is dropped, to
	// come again. A message cut to fit a path's MTU, its fragments come in
	// any order, has far fewer.
	dtlsMaxSpans = 256
	// dtlsMinFragment is the smallest fragment a datagram is filled with
	// before the next is started.
	dtlsMinFragment = 64

	maxDTLSSeq = 1<<48 - 1
)

var errDTLSSeqExhausted = errors.New("tls: DTLS record sequence numbers exhausted")

// replayWindow is the sliding window of RFC 6347 section 4.1.2.6 over the
// sequence numbers of one epoch's records: the highest seen and, for the 64
// up to it, which have been.
type replayWindow struct {
	top  uint64
	bits uint64
	any  bool
}

// fresh reports whether seq is neither too old nor seen.
func (w *replayWindow) fresh(seq uint64) bool {
	if !w.any || seq > w.top {
		return true
	}
	d := w.top - seq
	return d < 64 && w.bits&(1<<d) == 0
}

// mark records seq, once its record has been authenticated.
func (w *replayWindow) mark(seq uint64) {
	switch {
	case !w.any:
		w.any, w.top, w.bits = true, seq, 1
	case seq > w.top:
		if shift := seq - w.top; shift >= 64 {
			w.bits = 1
		} else {
			w.bits = w.bits<<shift | 1
		}
		w.top = seq
	default:
		w.bits |= 1 << (w.top - seq)
	}
}

// dtlsMessage is a handshake message of the peer being put together from its
// fragments: spans are the parts of body that have come, sorted and apart.
type dtlsMessage struct {
	seq   uint16
	typ   byte
	epoch uint16
	got   bool
	body  []byte
	spans [][2]int
}

// add takes the fragment frag at off. The span it covers is merged into the
// spans it overlaps or touches where it falls among them, found by a binary
// search, rather than the spans sorted and merged again, so a fragment costs
// a search and a move of the spans after it. A fragment that would make more
// than dtlsMaxSpans spans is dropped.
func (m *dtlsMessage) add(off int, frag []byte) {
	if len(frag) == 0 {
		m.got = true
		return
	}
	end := off + len(frag)
	// The first span that ends at or after off, and the first after it that
	// starts past end: the ones from i to j meet the fragment.
	i, _ := slices.BinarySearchFunc(m.spans, off, func(s [2]int, off int) int { return s[1] - off })
	j := i
	for j < len(m.spans) && m.spans[j][0] <= end {
		j++
	}
	if i == j {
		if len(m.spans) >= dtlsMaxSpans {
			return
		}
		m.spans = slices.Insert(m.spans, i, [2]int{off, end})
	} else {
		merged := [2]int{min(off, m.spans[i][0]), max(end, m.spans[j-1][1])}
		m.spans = slices.Replace(m.spans, i, j, merged)
	}
	copy(m.body[off:], frag)
	m.got = true
}

func (m *dtlsMessage) complete() bool {
	return m.got && (len(m.body) == 0 || len(m.spans) == 1 && m.spans[0] == [2]int{0, len(m.body)})
}

// dtlsForm is m as the transcript has it: unfragmented, with its DTLS header.
func (m *dtlsMessage) dtlsForm() []byte {
	return appendDTLSMessage(nil, m.typ, m.seq, m.body)
}

// tlsForm is m with the header of a TLS message, as the TLS 1.2 code reads
// it.
func (m *dtlsMessage) tlsForm() []byte {
	n := len(m.body)
	return append([]byte{m.typ, byte(n >> 16), byte(n >> 8), byte(n)}, m.body...)
}

// appendDTLSMessage appends the handshake message of typ with body, whole,
// under message_seq seq.
func appendDTLSMessage(dst []byte, typ byte, seq uint16, body []byte) []byte {
	n := len(body)
	dst = append(dst, typ, byte(n>>16), byte(n>>8), byte(n), byte(seq>>8), byte(seq), 0, 0, 0, byte(n>>16), byte(n>>8), byte(n))
	return append(dst, body...)
}

// handleRecord handles one record of a datagram.
func (c *DTLSConn) handleRecord(rec []byte, now time.Time) {
	typ := rec[0]
	if rec[1] != 0xfe || rec[2] != 0xfd && rec[2] != 0xff {
		return
	}
	epoch := uint16(rec[3])<<8 | uint16(rec[4])
	var seq uint64
	for _, b := range rec[5:11] {
		seq = seq<<8 | uint64(b)
	}
	body := rec[dtlsRecordHeaderLen:]
	if len(body) > maxCiphertext12 {
		return
	}
	switch epoch {
	case 0:
		if !c.isClient && c.state == dtlsIdle {
			c.serverFirstHello(rec, seq, now)
			return
		}
		// Nothing of epoch 0 can be trusted once the handshake is complete
		// but the handshake messages, whose retransmissions say ours were
		// lost. Before, a peer that fails has no keys to send its alert
		// under.
		if typ == recordTypeApplicationData || c.state == dtlsDone && typ == recordTypeAlert {
			return
		}
	case 1:
		if c.rx == nil {
			if c.state != dtlsIdle && len(c.future) < dtlsMaxFuture {
				c.future = append(c.future, bytes.Clone(rec))
			}
			return
		}
		if rec[2] != 0xfd || !c.replay.fresh(seq) {
			return
		}
		// The keys open a TLS record: its header is the last five bytes of
		// the DTLS one, with the type and version written over the end of
		// the sequence number, which goes to the keys whole, with the epoch.
		c.rx.seq = 1<<48 | seq
		rec[8], rec[9], rec[10] = typ, 0xfe, 0xfd
		inner, plain, _, ok := c.rx.open(rec[8:])
		if !ok {
			return
		}
		c.replay.mark(seq)
		typ, body = inner, plain
	default:
		return
	}
	switch typ {
	case recordTypeAlert:
		c.peerAlert(body)
	case recordTypeHandshake:
		c.handshakeRecord(body, epoch, now)
	case recordTypeApplicationData:
		c.appData(body)
	}
	// A change_cipher_spec changes nothing: a record says its epoch, and
	// one of epoch 1 is opened once its keys are known.
}

// appData hands the application data of a record to OnData, or keeps it, on
// a client, until the server's Finished has come: the server may send
// before it has arrived.
func (c *DTLSConn) appData(body []byte) {
	if c.state == dtlsDone {
		if !c.isClient {
			// The client has our Finished; nothing will ask for it again.
			c.flight = nil
		}
		if c.opts.OnData != nil {
			c.opts.OnData(body)
		}
		return
	}
	if c.isClient && len(c.early) < dtlsMaxFuture {
		c.early = append(c.early, bytes.Clone(body))
	}
}

// handshakeRecord takes the handshake fragments of a record.
func (c *DTLSConn) handshakeRecord(body []byte, epoch uint16, now time.Time) {
	for len(body) > 0 && c.err == nil {
		if len(body) < dtlsHandshakeHeaderLen {
			return
		}
		typ := body[0]
		length := int(body[1])<<16 | int(body[2])<<8 | int(body[3])
		seq := uint16(body[4])<<8 | uint16(body[5])
		off := int(body[6])<<16 | int(body[7])<<8 | int(body[8])
		n := int(body[9])<<16 | int(body[10])<<8 | int(body[11])
		if dtlsHandshakeHeaderLen+n > len(body) || off+n > length || length > maxHandshakeMessage {
			return
		}
		frag := body[dtlsHandshakeHeaderLen : dtlsHandshakeHeaderLen+n]
		body = body[dtlsHandshakeHeaderLen+n:]
		c.fragment(typ, length, seq, off, frag, epoch, now)
	}
}

// fragment takes a fragment of a handshake message, and handles each message
// it completes in order.
func (c *DTLSConn) fragment(typ byte, length int, seq uint16, off int, frag []byte, epoch uint16, now time.Time) {
	// Only a Finished is protected, and it always is.
	if (typ == hsFinished) != (epoch == 1) {
		return
	}
	if c.isClient && c.state == dtlsAwaitServerHello && seq > c.rxMsgSeq && int(seq) < int(c.rxMsgSeq)+dtlsMaxAhead &&
		(typ == hsHelloVerifyRequest || typ == hsServerHello) {
		// The server numbers its first message after a HelloVerifyRequest
		// it kept no record of by our ClientHello, and others start over.
		c.rxMsgSeq = seq
		c.partial = slices.DeleteFunc(c.partial, func(m *dtlsMessage) bool { return m.seq < seq })
	}
	if seq < c.rxMsgSeq {
		// The peer sent its last flight again, so it did not get ours.
		if int(seq) == c.peerLast && c.flight != nil && !c.resent {
			c.resent = true
			c.sendFlight()
		}
		return
	}
	if c.state == dtlsDone || int(seq) >= int(c.rxMsgSeq)+dtlsMaxAhead {
		// No renegotiation, and nothing too far ahead.
		return
	}
	var m *dtlsMessage
	buffered := 0
	for _, p := range c.partial {
		if p.seq == seq {
			m = p
		}
		buffered += len(p.body)
	}
	if m == nil {
		if buffered+length > dtlsMaxBuffered {
			return
		}
		m = &dtlsMessage{seq: seq, typ: typ, epoch: epoch, body: make([]byte, length)}
		c.partial = append(c.partial, m)
	}
	if m.typ != typ || len(m.body) != length {
		return
	}
	m.add(off, frag)
	for c.err == nil {
		i := slices.IndexFunc(c.partial, func(p *dtlsMessage) bool { return p.seq == c.rxMsgSeq })
		if i < 0 || !c.partial[i].complete() {
			return
		}
		m := c.partial[i]
		c.partial = slices.Delete(c.partial, i, i+1)
		c.rxMsgSeq++
		var herr *hsError
		if c.isClient {
			herr = c.clientMessage(m, now)
		} else {
			herr = c.serverMessage(m, now)
		}
		if herr != nil {
			c.fail(herr)
		}
	}
}

// serverFirstHello handles a record of epoch 0 that comes to a server with no
// handshake yet: a ClientHello, which is answered with a HelloVerifyRequest
// unless it carries the cookie for it. All that is kept of one without the
// cookie is a hello cut into fragments, as OpenSSL cuts it, while they come:
// a buffer of at most dtlsMaxHello bytes.
func (c *DTLSConn) serverFirstHello(rec []byte, recSeq uint64, now time.Time) {
	if rec[0] != recordTypeHandshake {
		return
	}
	body := rec[dtlsRecordHeaderLen:]
	for len(body) >= dtlsHandshakeHeaderLen && c.state == dtlsIdle && c.err == nil {
		typ := body[0]
		length := int(body[1])<<16 | int(body[2])<<8 | int(body[3])
		seq := uint16(body[4])<<8 | uint16(body[5])
		off := int(body[6])<<16 | int(body[7])<<8 | int(body[8])
		n := int(body[9])<<16 | int(body[10])<<8 | int(body[11])
		if dtlsHandshakeHeaderLen+n > len(body) || off+n > length {
			return
		}
		frag := body[dtlsHandshakeHeaderLen : dtlsHandshakeHeaderLen+n]
		body = body[dtlsHandshakeHeaderLen+n:]
		if typ != hsClientHello || length > dtlsMaxHello {
			continue
		}
		if c.hello == nil || c.hello.seq != seq || len(c.hello.body) != length {
			c.hello = &dtlsMessage{seq: seq, typ: typ, body: make([]byte, length)}
		}
		c.hello.add(off, frag)
		if c.hello.complete() {
			m := c.hello
			c.hello = nil
			c.firstHello(m, recSeq, now)
		}
	}
}

// firstHello handles a whole ClientHello that came to a server with no
// handshake yet, the last of whose fragments came in record recSeq.
func (c *DTLSConn) firstHello(m *dtlsMessage, recSeq uint64, now time.Time) {
	hello := m.body
	r := newReader(hello)
	r.take(2 + 32)
	r.vec8()
	cookieAt := len(hello) - len(r.b)
	cookie := r.vec8()
	restAt := len(hello) - len(r.b)
	if !r.ok {
		return
	}
	if !c.opts.InsecureSkipHelloVerify {
		want := c.cookieFor(hello[:cookieAt], hello[restAt:])
		if want == nil {
			return
		}
		if !hmac.Equal(cookie, want) {
			c.helloVerifyRequest(recSeq, m.seq, want)
			return
		}
	}
	// The client has shown it receives at its address: the handshake begins,
	// with the transcript from this ClientHello, and our messages numbered
	// from its message_seq, our records from its sequence number, past the
	// HelloVerifyRequest's (RFC 6347 section 4.2.1).
	h := newServerHSFor(c.config, nil, nil, nil)
	h.allow13, h.allow12, h.version, h.dtls = false, true, stdtls.VersionTLS12, true
	c.srv = h
	c.txSeq[0] = recSeq
	c.txMsgSeq, c.rxMsgSeq, c.peerLast = m.seq, m.seq+1, int(m.seq)
	h.transcript = m.dtlsForm()
	n := len(hello) - (restAt - cookieAt)
	msg := append(make([]byte, 0, 4+n), hsClientHello, byte(n>>16), byte(n>>8), byte(n))
	msg = append(append(msg, hello[:cookieAt]...), hello[restAt:]...)
	if herr := c.serverClientHello(msg, now); herr != nil {
		c.fail(herr)
	}
}

// helloVerifyRequest answers a ClientHello without the cookie with one, in a
// record numbered as the hello's last was and a message numbered as it was.
func (c *DTLSConn) helloVerifyRequest(recSeq uint64, msgSeq uint16, cookie []byte) {
	n := 3 + len(cookie)
	out := append(c.out[:0], recordTypeHandshake, 0xfe, 0xff, 0, 0)
	out = append(out, byte(recSeq>>40), byte(recSeq>>32), byte(recSeq>>24), byte(recSeq>>16), byte(recSeq>>8), byte(recSeq))
	out = append(out, byte((dtlsHandshakeHeaderLen+n)>>8), byte(dtlsHandshakeHeaderLen+n))
	out = appendDTLSMessage(out, hsHelloVerifyRequest, msgSeq, []byte{0xfe, 0xff, byte(len(cookie))})
	out = append(out, cookie...)
	// The lengths appendDTLSMessage wrote are without the cookie.
	at := len(out) - n - dtlsHandshakeHeaderLen
	out[at+1], out[at+2], out[at+3] = byte(n>>16), byte(n>>8), byte(n)
	out[at+9], out[at+10], out[at+11] = byte(n>>16), byte(n>>8), byte(n)
	c.out = out
	c.opts.Output(out)
}

// queueMessage adds the TLS handshake message msg, header included, to the
// flight being made, numbered and in its DTLS form, and to the transcript.
func (c *DTLSConn) queueMessage(msg []byte, epoch uint16) {
	m := appendDTLSMessage(nil, msg[0], c.txMsgSeq, msg[4:])
	c.txMsgSeq++
	t := c.transcript()
	*t = append(*t, m...)
	c.flight = append(c.flight, flightEntry{msg: m, epoch: epoch})
}

// transcript is the handshake's transcript, which its TLS 1.2 code hashes and
// signs.
func (c *DTLSConn) transcript() *[]byte {
	if c.isClient {
		return &c.cli.transcript
	}
	return &c.srv.transcript
}

// startFlight sends a new flight, with the timer at its first value if an
// answer is awaited for it.
func (c *DTLSConn) startFlight(now time.Time, awaitAnswer bool) {
	c.rto = dtlsInitialRTO
	c.timerOn = awaitAnswer
	c.deadline = now.Add(c.rto)
	c.sendFlight()
}

// sendFlight sends the flight, its messages cut into fragments that fill
// datagrams of at most the MTU.
func (c *DTLSConn) sendFlight() {
	dg := c.out[:0]
	flush := func() {
		if len(dg) > 0 {
			c.opts.Output(dg)
			dg = dg[:0]
		}
	}
	var err error
	for _, e := range c.flight {
		if e.ccs {
			if len(dg)+dtlsRecordHeaderLen+1 > c.mtu {
				flush()
			}
			if dg, err = c.appendRecord(dg, 0, recordTypeChangeCipherSpec, []byte{1}); err != nil {
				c.fail(&hsError{alert: alertInternalError, err: err})
				return
			}
			continue
		}
		body := e.msg[dtlsHandshakeHeaderLen:]
		overhead := c.recordOverhead(e.epoch) + dtlsHandshakeHeaderLen
		for off := 0; ; {
			room := c.mtu - len(dg) - overhead
			n := len(body) - off
			if n > room && room < dtlsMinFragment && len(dg) > 0 {
				flush()
				continue
			}
			n = min(n, room)
			frag := append(c.frag[:0], e.msg[:6]...)
			frag = append(frag, byte(off>>16), byte(off>>8), byte(off), byte(n>>16), byte(n>>8), byte(n))
			frag = append(frag, body[off:off+n]...)
			c.frag = frag
			if dg, err = c.appendRecord(dg, e.epoch, recordTypeHandshake, frag); err != nil {
				c.fail(&hsError{alert: alertInternalError, err: err})
				return
			}
			if off += n; off >= len(body) {
				break
			}
		}
	}
	flush()
	c.out = dg
}

// recordOverhead is what a record of epoch adds to its payload.
func (c *DTLSConn) recordOverhead(epoch uint16) int {
	if epoch == 0 {
		return dtlsRecordHeaderLen
	}
	return dtlsRecordHeaderLen + c.tx.overhead() - recordHeaderLen
}

var zeroPrefix [recordHeaderLen + cbcBlockLen]byte

// appendRecord appends a record of typ carrying payload to dst, in the clear
// in epoch 0 and sealed under our keys in epoch 1.
func (c *DTLSConn) appendRecord(dst []byte, epoch uint16, typ byte, payload []byte) ([]byte, error) {
	seq := c.txSeq[epoch]
	if seq > maxDTLSSeq {
		return dst, errDTLSSeqExhausted
	}
	c.txSeq[epoch]++
	start := len(dst)
	if epoch == 0 {
		n := len(payload)
		dst = append(dst, typ, 0xfe, 0xfd, 0, 0, byte(seq>>40), byte(seq>>32), byte(seq>>24), byte(seq>>16), byte(seq>>8), byte(seq),
			byte(n>>8), byte(n))
		return append(dst, payload...), nil
	}
	// The keys seal a TLS record, whose five-byte header ends where the DTLS
	// one does: the record is sealed eight bytes in, and the DTLS header
	// written over the TLS one's type and version, keeping its length. The
	// sequence number the keys authenticate is the epoch and ours, together.
	k := c.tx
	k.seq = uint64(epoch)<<48 | seq
	dst = append(dst, zeroPrefix[:dtlsRecordHeaderLen-recordHeaderLen]...)
	tlsStart := len(dst)
	dst = append(dst, zeroPrefix[:k.prefixLen()]...)
	dst = append(dst, payload...)
	dst, err := k.sealAt(dst, tlsStart, typ)
	if err != nil {
		return dst[:start], err
	}
	dst[start], dst[start+1], dst[start+2] = typ, 0xfe, 0xfd
	dst[start+3], dst[start+4] = byte(epoch>>8), byte(epoch)
	for i := 0; i < 6; i++ {
		dst[start+5+i] = byte(seq >> (40 - 8*i))
	}
	return dst, nil
}
