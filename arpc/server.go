//go:build linux || darwin || windows

package arpc

import (
	"context"
	"sync"
	"sync/atomic"

	fib "github.com/lesismal/fib"
)

// Server serves arpc on the connections an engine accepts, each one a Client
// sharing the Server's Codec and Handler. It is the fib.Handler to Bind:
//
//	engine, err := fib.Bind(config, server)
//
// or, for TLS, tls.NewServer(tlsConfig, server). Run and Serve make the
// engine themselves.
type Server struct {
	// Codec encodes and decodes payloads.
	Codec Codec
	// Handler handles the messages and events of every connection.
	Handler *Handler
	// MaxLoad is how many connections the Server serves at once; one beyond
	// it is closed as it is accepted. Zero or less is no limit.
	MaxLoad int64

	accepted atomic.Int64
	currLoad atomic.Int64
	seq      atomic.Uint64

	mu      sync.Mutex
	clients map[*Client]struct{}
	// engine is the engine Serve made, and served closes when Serve returns.
	engine *fib.Engine
	served chan struct{}
}

// NewServer returns a Server with DefaultCodec and a clone of DefaultHandler.
func NewServer() *Server {
	return &Server{Codec: DefaultCodec, Handler: DefaultHandler.Clone(), clients: map[*Client]struct{}{}}
}

// OnOpen serves a connection the engine accepted.
func (s *Server) OnOpen(conn *fib.Connection) {
	if load := s.currLoad.Add(1); s.MaxLoad > 0 && load > s.MaxLoad {
		s.currLoad.Add(-1)
		conn.Close()
		return
	}
	s.accepted.Add(1)
	c := newClient(s.Codec, s.Handler)
	c.server = s
	c.attach(conn)
	s.mu.Lock()
	if s.clients == nil {
		s.clients = map[*Client]struct{}{}
	}
	s.clients[c] = struct{}{}
	s.mu.Unlock()
	if s.Handler.onConnected != nil {
		c.goFunc(c.connected)
	}
}

// OnData reads the connection's messages and dispatches them.
func (s *Server) OnData(conn *fib.Connection, data []byte) {
	if st, ok := conn.Attachment().(*connState); ok {
		st.feed(conn, data)
	}
}

// OnPriorityData ignores out-of-band data, which arpc does not use.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose ends the connection's Client.
func (s *Server) OnClose(conn *fib.Connection, err error) {
	st, ok := conn.Attachment().(*connState)
	if !ok {
		// Turned away by MaxLoad.
		return
	}
	st.release()
	st.client.onClose(conn)
}

func (s *Server) removeClient(c *Client) {
	s.mu.Lock()
	_, ok := s.clients[c]
	delete(s.clients, c)
	s.mu.Unlock()
	if ok {
		s.currLoad.Add(-1)
	}
}

// Accepted returns how many connections the Server has served, not counting
// those MaxLoad turned away.
func (s *Server) Accepted() int64 { return s.accepted.Load() }

// CurrLoad returns how many connections the Server serves now.
func (s *Server) CurrLoad() int64 { return s.currLoad.Load() }

// Run serves addr over TCP, on an engine of fib.DefaultConfig, until Stop.
func (s *Server) Run(addr string) error {
	config := fib.DefaultConfig()
	config.Addr = addr
	return s.Serve(config)
}

// Serve binds an engine of config to the Server and runs it until Stop, after
// which it closes the engine and returns.
func (s *Server) Serve(config fib.Config) error {
	engine, err := fib.Bind(config, s)
	if err != nil {
		return err
	}
	served := make(chan struct{})
	defer close(served)
	s.mu.Lock()
	s.engine, s.served = engine, served
	s.mu.Unlock()
	err = engine.Run()
	if closeErr := engine.Close(); err == nil {
		err = closeErr
	}
	// Closing the engine ended the connections without OnClose.
	s.mu.Lock()
	clients := make([]*Client, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()
	for _, c := range clients {
		c.terminate()
	}
	return err
}

// Engine returns the engine Serve or Run made, or nil.
func (s *Server) Engine() *fib.Engine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.engine
}

// Stop stops the engine Serve or Run made, which then closes every
// connection, and returns without waiting for it; Shutdown waits. On an
// engine of the caller's own, it closes the Server's connections.
func (s *Server) Stop() error {
	s.mu.Lock()
	engine := s.engine
	var conns []*fib.Connection
	if engine == nil {
		for c := range s.clients {
			if conn := c.Conn(); conn != nil {
				conns = append(conns, conn)
			}
		}
	}
	s.mu.Unlock()
	if engine != nil {
		engine.Stop()
	}
	for _, conn := range conns {
		conn.Close()
	}
	return nil
}

// Shutdown is Stop, and waits for Serve to return, or returns ErrTimeout if
// ctx ends first.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	served := s.served
	s.mu.Unlock()
	_ = s.Stop()
	if served == nil {
		return nil
	}
	select {
	case <-served:
		return nil
	case <-ctx.Done():
		return ErrTimeout
	}
}

// NewMessage builds a Message with the Server's Handler and Codec and a
// sequence number of the Server's, for Client.PushMsg.
func (s *Server) NewMessage(cmd byte, method string, v any, values ...map[any]any) *Message {
	return NewMessage(cmd, method, v, s.seq.Add(1), s.Handler, s.Codec, firstValues(values))
}

// Broadcast sends a notify for method with v to every connection.
func (s *Server) Broadcast(method string, v any, values ...map[any]any) {
	s.BroadcastWithFilter(method, v, nil, values...)
}

// BroadcastWithFilter sends a notify for method with v to every connection
// whose Client filter accepts; a nil filter accepts all.
func (s *Server) BroadcastWithFilter(method string, v any, filter func(*Client) bool, values ...map[any]any) {
	msg := s.NewMessage(CmdNotify, method, v, values...)
	coded := len(s.Handler.coders) != 0
	s.mu.Lock()
	for c := range s.clients {
		if filter != nil && !filter(c) {
			continue
		}
		conn := c.Conn()
		if conn == nil {
			continue
		}
		if !coded {
			// The connection copies what the socket cannot take at once, so
			// every connection sends the one message.
			_ = c.write(conn, msg.Buffer, nil)
			continue
		}
		// A coder may change the Message it encodes, so each connection
		// encodes a copy of its own.
		buf := s.Handler.Malloc(len(msg.Buffer))
		copy(buf, msg.Buffer)
		_ = c.sendMessage(conn, s.Handler.NewMessageWithBuffer(buf))
	}
	s.mu.Unlock()
	msg.Release()
}

// ForEach calls f for every connection's Client. It holds the Server's lock,
// so f must not call a method of the Server that takes it.
func (s *Server) ForEach(f func(*Client)) {
	s.ForEachWithFilter(f, nil)
}

// ForEachWithFilter is ForEach for the Clients filter accepts; a nil filter
// accepts all.
func (s *Server) ForEachWithFilter(f func(*Client), filter func(*Client) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.clients {
		if filter == nil || filter(c) {
			f(c)
		}
	}
}
