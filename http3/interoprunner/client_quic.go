package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/lesismal/fib/http3/quic"
	fibtls "github.com/lesismal/fib/tls"
)

// fetchRaw runs the client side of the runner's HTTP/0.9 testcases: fib's
// QUIC transport, with a request line sent on each stream and the file read
// off it. The testcases differ in how many connections they use, whether they
// resume one and, for chacha20, which cipher suite the ClientHello offers, not
// in the protocol.
func fetchRaw(testcase string, urls []*url.URL) error {
	host, port := urls[0].Hostname(), urls[0].Port()
	if port == "" {
		port = "443"
	}
	for _, u := range urls {
		if u.Hostname() != host || (u.Port() != "" && u.Port() != port) {
			return fmt.Errorf("REQUESTS spans more than one server: %s and %s", urls[0], u)
		}
	}
	opts := dialOptions{host: host, port: port}
	if testcase == "keyupdate" {
		// The test wants a key update during the first megabyte; 100ms of
		// sending is well inside it at the runner's 10Mbps.
		opts.keyUpdateInterval = 100 * time.Millisecond
	}
	if testcase == "chacha20" {
		// The runner checks that the client's ClientHello offers
		// ChaCha20-Poly1305 and nothing else.
		opts.suites = []uint16{tls.TLS_CHACHA20_POLY1305_SHA256}
	}

	switch testcase {
	case "multiconnect":
		for _, u := range urls {
			d, err := dialRaw(opts)
			if err != nil {
				return err
			}
			d.setTotal(1)
			if err := d.waitHandshake(60 * time.Second); err != nil {
				d.close()
				return err
			}
			if err := d.download(u); err != nil {
				d.close()
				return err
			}
			if err := d.waitDownloads(60 * time.Second); err != nil {
				d.close()
				return err
			}
			d.close()
		}
		return nil
	case "resumption":
		return fetchResumed(opts, urls, false)
	case "zerortt":
		return fetchResumed(opts, urls, true)
	default:
		d, err := dialRaw(opts)
		if err != nil {
			return err
		}
		d.setTotal(len(urls))
		if err := d.waitHandshake(60 * time.Second); err != nil {
			d.close()
			return err
		}
		for _, u := range urls {
			if err := d.download(u); err != nil {
				d.close()
				return err
			}
		}
		if err := d.waitDownloads(120 * time.Second); err != nil {
			d.close()
			return err
		}
		d.close()
		return nil
	}
}

// fetchResumed is the resumption and zerortt testcases: the first URL on a
// fresh connection, the rest on one that resumes its session, with the rest
// sent as 0-RTT data when use0RTT is set.
func fetchResumed(opts dialOptions, urls []*url.URL, use0RTT bool) error {
	if len(urls) < 2 {
		name := "resumption"
		if use0RTT {
			name = "zerortt"
		}
		return fmt.Errorf("%s needs at least two REQUESTS, got %d", name, len(urls))
	}
	cache := newTicketCache()
	opts.sessionCache = cache
	opts.early = use0RTT

	first, err := dialRaw(opts)
	if err != nil {
		return err
	}
	first.setTotal(1)
	if err := first.waitHandshake(60 * time.Second); err != nil {
		first.close()
		return err
	}
	if err := first.download(urls[0]); err != nil {
		first.close()
		return err
	}
	if err := first.waitDownloads(60 * time.Second); err != nil {
		first.close()
		return err
	}
	select {
	case <-cache.stored:
	case <-time.After(10 * time.Second):
		first.close()
		return errors.New("no session ticket arrived within 10s")
	}
	if first.qc.ConnectionState().DidResume {
		first.close()
		return errors.New("the first connection resumed a session")
	}
	first.close()

	second, err := dialRaw(opts)
	if err != nil {
		return err
	}
	second.setTotal(len(urls) - 1)
	if use0RTT {
		if !second.qc.CanSendEarly() {
			second.close()
			return errors.New("the resumed connection cannot send 0-RTT data")
		}
		// Open and write every request before waiting for the handshake, so
		// that they go in the client's first flight.
		for _, u := range urls[1:] {
			if err := second.download(u); err != nil {
				second.close()
				return err
			}
		}
	} else {
		if err := second.waitHandshake(60 * time.Second); err != nil {
			second.close()
			return err
		}
		if !second.qc.ConnectionState().DidResume {
			second.close()
			return errors.New("the second connection did not resume the session")
		}
		for _, u := range urls[1:] {
			if err := second.download(u); err != nil {
				second.close()
				return err
			}
		}
	}
	if err := second.waitDownloads(120 * time.Second); err != nil {
		second.close()
		return err
	}
	if use0RTT && !second.qc.EarlyDataAccepted() {
		second.close()
		return errors.New("the server did not accept the 0-RTT data")
	}
	second.close()
	return nil
}

type dialOptions struct {
	host              string
	port              string
	early             bool
	keyUpdateInterval time.Duration
	sessionCache      tls.ClientSessionCache
	// suites, when set, is the ClientHello's TLS 1.3 cipher suites; see
	// fetchRaw's chacha20 case.
	suites []uint16
}

func dialRaw(opts dialOptions) (*rawDownloader, error) {
	raddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(opts.host, opts.port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		// The runner's certificate names the simulated addresses; the runner
		// expects the client not to verify it.
		InsecureSkipVerify: true,
		ServerName:         opts.host,
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{hqInterop},
		KeyLogWriter:       keyLogWriter(),
		ClientSessionCache: opts.sessionCache,
	}
	config := quic.Config{
		TLSConfig:             tlsConfig,
		MaxIdleTimeout:        120 * time.Second,
		HandshakeTimeout:      60 * time.Second,
		MaxIncomingStreams:    100,
		MaxIncomingUniStreams: 16,
		// Small next to the files the transfer testcase moves, so that fib's
		// flow control has to grow the windows, as the testcase intends.
		StreamReceiveWindow: 128 << 10,
		ConnReceiveWindow:   1 << 20,
		EarlyData:           opts.early,
		KeyUpdateInterval:   opts.keyUpdateInterval,
	}
	if opts.suites != nil {
		// crypto/tls offers the TLS 1.3 suites itself and cannot be told to
		// offer one alone, so this connection's handshake is fib's, whose
		// ClientHello the options narrow.
		config.NewTLS = func(_ bool, cryptoConfig *tls.Config) quic.TLSConn {
			conn := fibtls.NewQUICClient(cryptoConfig)
			if conn == nil {
				return nil
			}
			conn.SetCipherSuites(opts.suites)
			return conn
		}
	}
	d := &rawDownloader{
		dir:       downloadsDir(),
		files:     make(map[uint64]*rawDownload),
		allDone:   make(chan struct{}),
		handshake: make(chan struct{}),
	}
	qc, err := quic.Dial(&endpointPath{sock: sock}, raddr, config, d)
	if err != nil {
		_ = sock.Close()
		return nil, err
	}
	d.qc = qc
	d.sock = sock
	go d.readLoop()
	return d, nil
}

// rawDownloader is one client connection: it downloads the requests put on
// it and reports when they are all done.
type rawDownloader struct {
	qc   *quic.Conn
	sock *net.UDPConn
	dir  string

	handshake     chan struct{}
	handshakeOnce sync.Once

	mu        sync.Mutex
	files     map[uint64]*rawDownload
	total     int
	completed int
	err       error
	finished  bool
	allDone   chan struct{}
	closing   bool
}

type rawDownload struct {
	f    *os.File
	name string
}

func (d *rawDownloader) readLoop() {
	for {
		buf := make([]byte, 64<<10)
		n, err := d.sock.Read(buf)
		if err != nil {
			return
		}
		datagram := make([]byte, n)
		copy(datagram, buf[:n])
		d.qc.HandleDatagram(datagram)
	}
}

func (d *rawDownloader) waitHandshake(timeout time.Duration) error {
	select {
	case <-d.handshake:
		return nil
	case <-time.After(timeout):
		return errors.New("handshake did not complete")
	}
}

// download opens a stream, names the file it will hold, and sends the
// HTTP/0.9 request line on it.
func (d *rawDownloader) download(u *url.URL) error {
	name := path.Base(u.Path)
	if name == "." || name == "/" || name == "" {
		name = "index"
	}
	dest := filepath.Join(d.dir, name)
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	s, err := d.openStream(120 * time.Second)
	if err != nil {
		_ = f.Close()
		return err
	}
	d.mu.Lock()
	d.files[s.ID()] = &rawDownload{f: f, name: name}
	d.mu.Unlock()
	// HTTP/0.9 ends the request at the end of the stream: quic-go's server
	// reads the request line and then waits for the fin before answering.
	if err := s.Write([]byte("GET "+u.RequestURI()+"\r\n"), true); err != nil {
		d.finishStream(s.ID(), err)
		return err
	}
	return nil
}

func (d *rawDownloader) openStream(timeout time.Duration) (*quic.Stream, error) {
	deadline := time.Now().Add(timeout)
	for {
		s, err := d.qc.OpenStream()
		if err == nil {
			return s, nil
		}
		if !errors.Is(err, quic.ErrStreamLimit) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (d *rawDownloader) waitDownloads(timeout time.Duration) error {
	select {
	case <-d.allDone:
		d.mu.Lock()
		err := d.err
		d.mu.Unlock()
		return err
	case <-time.After(timeout):
		return fmt.Errorf("downloads did not finish within %s", timeout)
	}
}

// finishStream closes a stream's file and counts it done. It is called once
// per stream, by whichever handler call ends it.
func (d *rawDownloader) finishStream(id uint64, err error) {
	d.mu.Lock()
	dl := d.files[id]
	if dl == nil {
		d.mu.Unlock()
		return
	}
	delete(d.files, id)
	d.completed++
	if err != nil && d.err == nil {
		d.err = err
	}
	finish := (d.completed == d.total && d.total > 0) || d.err != nil
	if finish && !d.finished {
		d.finished = true
		close(d.allDone)
	}
	d.mu.Unlock()
	_ = dl.f.Close()
}

func (d *rawDownloader) setTotal(n int) {
	d.mu.Lock()
	d.total = n
	d.mu.Unlock()
}

func (d *rawDownloader) fail(err error) {
	d.mu.Lock()
	if d.err == nil {
		d.err = err
	}
	if !d.finished {
		d.finished = true
		close(d.allDone)
	}
	d.mu.Unlock()
}

func (d *rawDownloader) close() {
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		return
	}
	d.closing = true
	d.mu.Unlock()
	d.qc.Close(0, "")
	_ = d.sock.Close()
}

// The quic.Handler calls. They are one at a time per connection.

func (d *rawDownloader) OnHandshake(*quic.Conn) {
	d.handshakeOnce.Do(func() { close(d.handshake) })
}

func (d *rawDownloader) OnStreamsAvailable(*quic.Conn)      {}
func (d *rawDownloader) OnStopSending(*quic.Stream, uint64) {}
func (d *rawDownloader) OnStreamReset(s *quic.Stream, code uint64) {
	d.finishStream(s.ID(), fmt.Errorf("stream %d reset by the server with code %d", s.ID(), code))
}

func (d *rawDownloader) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	d.mu.Lock()
	dl := d.files[s.ID()]
	d.mu.Unlock()
	if dl == nil {
		return
	}
	if len(data) > 0 {
		if _, err := dl.f.Write(data); err != nil {
			d.finishStream(s.ID(), err)
			return
		}
	}
	if fin {
		d.finishStream(s.ID(), nil)
	}
}

func (d *rawDownloader) OnClose(_ *quic.Conn, err error) {
	d.mu.Lock()
	closing := d.closing
	d.mu.Unlock()
	if !closing {
		d.fail(fmt.Errorf("connection closed: %w", err))
	}
}

// ticketCache is the client session cache of the resumption and zerortt
// testcases, with a channel that says when the first ticket has arrived.
type ticketCache struct {
	tls.ClientSessionCache
	once   sync.Once
	stored chan struct{}
}

func newTicketCache() *ticketCache {
	return &ticketCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan struct{})}
}

func (c *ticketCache) Put(key string, state *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, state)
	c.once.Do(func() { close(c.stored) })
}
