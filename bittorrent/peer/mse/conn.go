package mse

import (
	"crypto/rc4"
	"net"
	"sync"
	"time"
)

// Conn is a net.Conn after a handshake: it reads what the peer sent with
// the handshake first, the receiver's initial payload included, and
// encrypts and decrypts with RC4 when that was selected.
type Conn struct {
	net.Conn
	// Result is the handshake's outcome.
	Result *Result

	rmu     sync.Mutex
	pending []byte
	dec     *rc4.Cipher

	wmu  sync.Mutex
	enc  *rc4.Cipher
	wbuf []byte
}

func newConn(c net.Conn, r *Result) *Conn {
	mc := &Conn{Conn: c, Result: r, enc: r.Encrypt, dec: r.Decrypt}
	mc.pending = append(append(mc.pending, r.IA...), r.Rest...)
	return mc
}

// Read reads what the handshake left over, then from the connection.
func (c *Conn) Read(b []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	if len(c.pending) > 0 {
		n := copy(b, c.pending)
		c.pending = c.pending[n:]
		return n, nil
	}
	n, err := c.Conn.Read(b)
	if c.dec != nil && n > 0 {
		c.dec.XORKeyStream(b[:n], b[:n])
	}
	return n, err
}

// Write encrypts b, when RC4 was selected, and writes it.
func (c *Conn) Write(b []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.enc == nil {
		return c.Conn.Write(b)
	}
	// The keystream advances as it encrypts, so a short write cannot be
	// retried with other bytes: a failed write leaves the stream broken,
	// as it leaves any connection.
	if cap(c.wbuf) < len(b) {
		c.wbuf = make([]byte, len(b))
	}
	buf := c.wbuf[:len(b)]
	c.enc.XORKeyStream(buf, b)
	return c.Conn.Write(buf)
}

// handshake runs h over conn: out first when non-empty, then reads and
// feeds until done.
func handshake(conn net.Conn, out []byte, feed func([]byte) ([]byte, bool, error), timeout time.Duration) error {
	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
		defer conn.SetDeadline(time.Time{})
	}
	if len(out) > 0 {
		if _, err := conn.Write(out); err != nil {
			return err
		}
	}
	buf := make([]byte, 2048)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			out, done, ferr := feed(buf[:n])
			if ferr != nil {
				return ferr
			}
			if len(out) > 0 {
				if _, err := conn.Write(out); err != nil {
					return err
				}
			}
			if done {
				return nil
			}
		}
		if err != nil {
			return err
		}
	}
}

// Client runs the initiator's handshake over conn for skey, providing
// provide, with ia as the initial payload, within timeout when it is
// positive. The Conn it returns carries the payload.
func Client(conn net.Conn, skey [20]byte, provide CryptoMethod, ia []byte, timeout time.Duration) (*Conn, error) {
	h := NewInitiator(skey, provide, ia)
	out, err := h.Start()
	if err != nil {
		return nil, err
	}
	if err := handshake(conn, out, h.Feed, timeout); err != nil {
		return nil, err
	}
	return newConn(conn, h.Result()), nil
}

// Server runs the receiver's handshake over conn, finding the SKEY with
// keys and treating plaintext and methods as policy says, within timeout
// when it is positive. The Conn it returns carries the payload, the
// initiator's initial payload first; for a plaintext handshake it reads
// the handshake as it came, and Result.PlainHandshake is true.
func Server(conn net.Conn, keys KeyFinder, policy Policy, timeout time.Duration) (*Conn, error) {
	h := NewReceiver(keys, policy)
	if err := handshake(conn, nil, h.Feed, timeout); err != nil {
		return nil, err
	}
	return newConn(conn, h.Result()), nil
}
