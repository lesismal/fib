package tls

import (
	"container/list"
	"crypto/x509"
	"sync"
	"time"
)

// What a QUIC client keeps to resume a connection (RFC 8446 section 4.6.1):
// the session ticket the server sent, the secret it stands for, and what
// early data needs to know of the connection it came from. crypto/tls's
// ClientSessionCache holds its own sessions, whose secrets only it can read,
// so a client that runs without a worker has a cache of its own.

// QUICSession is a session a QUIC client can resume. Its contents are private
// to this package; a cache only holds it.
type QUICSession struct {
	ticket    []byte
	psk       []byte
	suite     uint16
	received  time.Time
	lifetime  time.Duration
	ageAdd    uint32
	alpn      string
	earlyData bool
	// maxEarly is how much early data the ticket allows over TCP.
	maxEarly uint32
	// extra is what the QUIC layer keeps with the session: the server's
	// transport parameters, which a client sending early data works to.
	extra [][]byte
	// certs are the DER certificates the server sent, for the connection
	// state of a connection that resumes the session.
	certs [][]byte
	// chains are the server's verified chains, nil if they were not
	// verified, for whether the session may be offered (see sessionTrusted)
	// and the connection state of one that resumes it. systemRoots says they
	// were verified against the system's roots, Config.RootCAs being nil.
	chains      [][]*x509.Certificate
	systemRoots bool
}

// usable says whether the session can still be offered at now.
func (s *QUICSession) usable(now time.Time) bool {
	age := now.Sub(s.received)
	return age >= -time.Minute && age < s.lifetime
}

// QUICSessionCache holds the sessions of a QUIC client, by key: the server
// name. An implementation must be safe for concurrent use. Put with a nil
// session removes the key; a session is used for one connection and taken out,
// since a server can be asked to accept early data on a ticket once.
type QUICSessionCache interface {
	Get(key string) *QUICSession
	Put(key string, s *QUICSession)
}

// NewQUICSessionCache returns a cache of at most capacity sessions that drops
// the one used least recently.
func NewQUICSessionCache(capacity int) QUICSessionCache {
	if capacity < 1 {
		capacity = 64
	}
	return &lruSessions{cap: capacity, order: list.New(), items: make(map[string]*list.Element)}
}

type lruSessions struct {
	mu    sync.Mutex
	cap   int
	order *list.List
	items map[string]*list.Element
}

type lruEntry struct {
	key     string
	session *QUICSession
}

func (c *lruSessions) Get(key string) *QUICSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		c.order.MoveToFront(e)
		return e.Value.(*lruEntry).session
	}
	return nil
}

func (c *lruSessions) Put(key string, s *QUICSession) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[key]; ok {
		if s == nil {
			c.order.Remove(e)
			delete(c.items, key)
			return
		}
		e.Value.(*lruEntry).session = s
		c.order.MoveToFront(e)
		return
	}
	if s == nil {
		return
	}
	c.items[key] = c.order.PushFront(&lruEntry{key: key, session: s})
	for c.order.Len() > c.cap {
		last := c.order.Back()
		c.order.Remove(last)
		delete(c.items, last.Value.(*lruEntry).key)
	}
}
