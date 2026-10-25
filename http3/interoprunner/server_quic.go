package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/lesismal/fib/http3/quic"
)

// hqInterop is the ALPN of HTTP/0.9 over QUIC, which the runner's non-HTTP/3
// testcases use: a request line and the raw file on a QUIC stream.
const hqInterop = "hq-interop"

// serveRaw runs the server side of the runner's QUIC-level testcases. The
// runner's clients speak HTTP/0.9 for those, so this is fib's QUIC transport
// with a file server on top rather than fib's HTTP/3 server; the http3
// testcase goes through serveH3 instead.
func serveRaw(testcase string) error {
	cert, err := loadServerCertificate()
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{hqInterop},
		KeyLogWriter: keyLogWriter(),
	}
	tokenKey := quic.NewTokenKey()
	resetKey := quic.NewResetKey()
	config := quic.Config{
		TLSConfig:          tlsConfig,
		MaxIdleTimeout:     120 * time.Second,
		HandshakeTimeout:   30 * time.Second,
		MaxIncomingStreams: 200,
		TokenKey:           tokenKey,
		ResetKey:           &resetKey,
		// The zerortt testcase has the runner's client resume a session and
		// send its requests in the first flight.
		EarlyData: testcase == "zerortt",
	}

	sock, err := net.ListenUDP("udp", &net.UDPAddr{Port: 443})
	if err != nil {
		return err
	}
	s := &rawServer{
		sock:       sock,
		config:     config,
		tokenKey:   tokenKey,
		resetKey:   resetKey,
		forceRetry: testcase == "retry",
		www:        wwwDir(),
		conns:      make(map[string]*quic.Conn),
		paths:      make(map[string]*endpointPath),
	}
	logf("HTTP/0.9 over QUIC server listening on :443 (testcase %s, retry %v), serving %s",
		testcase, s.forceRetry, s.www)
	return s.serve()
}

type rawServer struct {
	sock     *net.UDPConn
	config   quic.Config
	tokenKey *quic.TokenKey
	resetKey quic.ResetKey
	// forceRetry makes the server answer a client's first Initial with a
	// Retry, as the retry testcase asks.
	forceRetry bool
	www        string

	mu    sync.Mutex
	conns map[string]*quic.Conn
	// paths are the PacketConns of the peer addresses seen: a connection
	// treats a different PacketConn as a different path, so one address must
	// keep the same one for the life of the server.
	paths map[string]*endpointPath
}

func (s *rawServer) serve() error {
	go func() {
		waitSignal()
		_ = s.sock.Close()
	}()
	for {
		buf := make([]byte, 64<<10)
		n, addr, err := s.sock.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("read: %v", err)
			continue
		}
		s.handle(addr, buf[:n])
	}
}

// handle gives a datagram to the connection its connection ID names, starts
// one if it is a client's first, and answers what no connection takes.
func (s *rawServer) handle(addr *net.UDPAddr, data []byte) {
	if vn := quic.VersionNegotiation(data); vn != nil {
		_, _ = s.sock.WriteToUDP(vn, addr)
		return
	}
	path := s.pathFor(addr)
	cid := quic.DatagramDCID(data)
	if qc := s.route(cid); qc != nil {
		qc.HandlePathDatagram(path, addr, data)
		return
	}
	if !quic.IsInitial(data) {
		// A short header packet for a connection this server does not have:
		// tell the client, rather than leave it to time out. Handshake
		// packets cannot be answered that way, and are dropped.
		if len(data) > 0 && data[0]&0x80 == 0 {
			if reset := s.resetKey.StatelessReset(data); reset != nil {
				_, _ = s.sock.WriteToUDP(reset, addr)
			}
		}
		return
	}
	// The retry testcase wants a Retry for a client without a good token: one
	// that has none, or whose NEW_TOKEN token is not good, which is no token
	// at all (RFC 9000 section 8.1.3). An Initial whose Retry token is not
	// good goes to Accept, which answers it with INVALID_TOKEN.
	if s.forceRetry {
		status, kind := s.tokenKey.CheckToken(data, addr)
		if status != quic.TokenValid && kind != quic.TokenKindRetry {
			if retry := s.tokenKey.Retry(data, addr); retry != nil {
				_, _ = s.sock.WriteToUDP(retry, addr)
			}
			return
		}
	}
	conn := &rawServerConn{s: s, www: s.www, streams: make(map[uint64]*rawRequest)}
	qc := quic.Accept(path, addr, s.config, conn, data)
	if qc == nil {
		return
	}
	conn.qc = qc
	// The client's first Initial named a connection ID of its choosing,
	// which its 0-RTT packets keep using until it has the server's; route
	// that too. Accept knows it through Conn.Owns, but a map entry saves
	// the scan.
	s.mu.Lock()
	s.conns[string(cid)] = qc
	for _, own := range qc.LocalCIDs() {
		s.conns[string(own)] = qc
	}
	s.mu.Unlock()
}

// pathFor returns the PacketConn of a peer address, making it once.
func (s *rawServer) pathFor(addr *net.UDPAddr) *endpointPath {
	key := addr.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.paths[key]; p != nil {
		return p
	}
	p := &endpointPath{sock: s.sock, addr: addr}
	s.paths[key] = p
	return p
}

func (s *rawServer) route(cid []byte) *quic.Conn {
	if len(cid) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if qc := s.conns[string(cid)]; qc != nil {
		return qc
	}
	// A connection ID that is not in the map can still be one of the
	// connection's handshake IDs, which Accept named as its own.
	for _, qc := range s.conns {
		if qc != nil && qc.Owns(cid) {
			s.conns[string(cid)] = qc
			return qc
		}
	}
	return nil
}

func (s *rawServer) setRoute(cid []byte, qc *quic.Conn) {
	s.mu.Lock()
	s.conns[string(cid)] = qc
	s.mu.Unlock()
}

func (s *rawServer) forget(qc *quic.Conn) {
	s.mu.Lock()
	for cid, conn := range s.conns {
		if conn == qc {
			delete(s.conns, cid)
		}
	}
	s.mu.Unlock()
}

// rawServerConn is one QUIC connection's HTTP/0.9 server. The handler's
// calls are one at a time per connection, so the map of requests needs no
// lock.
type rawServerConn struct {
	s       *rawServer
	qc      *quic.Conn
	www     string
	streams map[uint64]*rawRequest
}

type rawRequest struct {
	header []byte
	served bool
}

func (c *rawServerConn) OnHandshake(*quic.Conn)                 {}
func (c *rawServerConn) OnStreamsAvailable(*quic.Conn)          {}
func (c *rawServerConn) OnStopSending(*quic.Stream, uint64)     {}
func (c *rawServerConn) OnStreamReset(s *quic.Stream, _ uint64) { delete(c.streams, s.ID()) }

func (c *rawServerConn) OnConnectionID(conn *quic.Conn, cid []byte, active bool) {
	if active {
		// The connection's own argument, not c.qc: a connection issues its
		// first extra connection IDs while Accept is still inside the
		// datagram that started it, before the accept path has set c.qc.
		c.s.setRoute(cid, conn)
		return
	}
	c.s.mu.Lock()
	delete(c.s.conns, string(cid))
	c.s.mu.Unlock()
}

func (c *rawServerConn) OnClose(qc *quic.Conn, err error) {
	if err != nil {
		logf("server connection %s closed: %v", qc.RemoteAddr(), err)
	}
	c.s.forget(qc)
}

func (c *rawServerConn) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	if !s.Bidirectional() {
		return
	}
	req := c.streams[s.ID()]
	if req == nil {
		req = &rawRequest{}
		c.streams[s.ID()] = req
	}
	if req.served {
		return
	}
	req.header = append(req.header, data...)
	if i := bytes.Index(req.header, []byte("\r\n")); i >= 0 {
		req.served = true
		c.answer(s, string(req.header[:i]))
		return
	}
	if len(req.header) > 8<<10 {
		// A request line this long is not HTTP/0.9, whatever it is.
		req.served = true
		_ = s.Write([]byte("bad request\n"), true)
	}
}

func (c *rawServerConn) answer(s *quic.Stream, line string) {
	const prefix = "GET "
	if !strings.HasPrefix(line, prefix) {
		_ = s.Write([]byte("bad request\n"), true)
		return
	}
	body, err := readWWW(c.www, strings.TrimSpace(line[len(prefix):]))
	if err != nil {
		_ = s.Write([]byte("not found\n"), true)
		return
	}
	if err := s.Write(body, true); err != nil {
		log.Printf("write %s: %v", line, err)
	}
}

// endpointPath is a PacketConn to one peer through a UDP socket. Close does
// nothing: the socket is shared by every peer, and stays open for them.
type endpointPath struct {
	sock *net.UDPConn
	addr *net.UDPAddr
}

func (p *endpointPath) Send(datagram []byte) error {
	if p.addr == nil {
		_, err := p.sock.Write(datagram)
		return err
	}
	_, err := p.sock.WriteToUDP(datagram, p.addr)
	return err
}

func (p *endpointPath) Close() error { return nil }
