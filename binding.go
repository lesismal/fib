package fib

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

// Binding is one group of addresses an engine listens on, the network they
// share, and the handler that serves the connections, or the UDP peers, they
// take. New takes them, so that one engine serves several protocols at once,
// each on addresses of its own: HTTP on one port, HTTPS on another, HTTP/3 on
// a UDP port, gRPC on a fourth and a Unix socket for administration, all on a
// single event loop, descriptor table, task pool and buffer pool, where an
// engine per protocol gives each its own copy of all four.
//
// The handler is whatever stack the protocol needs:
//
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":80", Handler: fibhttp.NewHandler(router)},
//		fib.Binding{Addrs: []string{":443", ":8443"}, Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(router))},
//		fib.Binding{Network: "udp", Addr: ":443", Handler: http3.NewHandler(tlsConfig, router)},
//		fib.Binding{Addr: ":9000", Handler: grpcServer},
//		fib.Binding{Network: "unix", Addr: "/run/app.sock", Handler: admin},
//	)
//
// Everything else, the backlog, the buffers, the watermarks, the pollers and
// ReusePort, is the engine's and applies to every binding alike. With
// IOPollers, each binding's connections are spread over the same pollers as
// every other binding's.
//
// Bindings can also be added and removed while the engine runs; see
// Engine.AddBinding, Engine.RemoveBinding and Engine.ShutdownBinding.
type Binding struct {
	// Network and Addr name the listener the way net.Listen does: Network is
	// "tcp", "tcp4" or "tcp6", and Addr is a "host:port" such as ":9000",
	// "127.0.0.1:9000" or "[::1]:9000". A host resolves through the net
	// package, and a zero port asks the kernel to choose one. An empty
	// Network means Config.Network, and an empty Addr means ":0", so a
	// binding always listens somewhere; BindingAddrs reports where.
	//
	// "tcp" with no host listens on both families where the kernel has IPv6,
	// "tcp4" and "tcp6" pin it to one. This is the same choice net.Listen
	// makes from the same arguments.
	//
	// "unix" listens on a Unix domain stream socket, with Addr as its path, as
	// net.Listen does: the path must not exist yet, it is removed when the
	// binding or the engine closes, and on Linux a leading '@' names an
	// abstract socket. Connections on it behave exactly like TCP ones.
	//
	// "udp", "udp4" and "udp6" bind UDP sockets instead, as net.ListenPacket
	// does. Each peer address that sends a datagram becomes a connection of
	// its own, whose OnData receives one datagram per call and whose Send
	// sends one; see Config.UDPIdleTimeout for how such a connection ends.
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of the binding's
	// addresses and Addr is ignored; they all share Network.
	Addrs []string
	// Handler serves the binding's connections. Every binding names one.
	Handler Handler
}

// errNoBinding is what BindingAddrs reports for an index that names none of
// the engine's bindings.
var errNoBinding = errors.New("fib: engine has no such binding")

// errNoHandler is what New and AddBinding report for a binding that names
// no Handler.
var errNoHandler = errors.New("fib: binding has no Handler")

// New creates an engine that listens on every binding's addresses and serves
// each binding's connections with the binding's own Handler. Either every
// address listens or, with the error that stopped one, none does;
// BindingAddrs reports where each binding listens, by its index among
// bindings. AddBinding adds more later.
//
// With no bindings, New creates a client engine, whose connections are the
// ones it dials. A dial names the handler that serves its connection through
// DialWithHandler; Dial's connections get one that does nothing.
//
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":80", Handler: fibhttp.NewHandler(router)},
//		fib.Binding{Network: "udp", Addr: ":443", Handler: http3.NewHandler(tlsConfig, router)},
//	)
func New(config Config, bindings ...Binding) (*Engine, error) {
	resolved, err := bindingsOf(config, bindings)
	if err != nil {
		return nil, err
	}
	return newEngine(config, resolved)
}

// bindingState is what an engine keeps to open and close bindings while it
// runs; see AddBinding.
type bindingState struct {
	// bindMu orders AddBinding, RemoveBinding and ShutdownBinding, which
	// change the engine's listeners a binding at a time.
	bindMu sync.Mutex
	// bindConfig is the Config the engine was created with, less its
	// Bindings, which the sockets AddBinding opens take their options from.
	bindConfig Config
	// configCustom records which settings the caller set rather than leaving
	// them for fib's defaults. Settings reports it, and the startup log says
	// which parameters are custom.
	configCustom map[string]bool
	// bindingCount is how many binding indices the engine has handed out,
	// the ones New was given first. Guarded by bindMu.
	bindingCount int
	// drainingBindings is the set of bindings ShutdownBinding is draining,
	// or nil while none is, which is all a connection's ShuttingDown then
	// has to look at. Written under bindMu.
	drainingBindings atomic.Pointer[map[int]struct{}]
}

// AddBinding has the engine listen on b's addresses too, as if New had been
// given it, and serve what they take with b's Handler, which it must name. It returns the binding's index,
// which BindingAddrs, RemoveBinding and ShutdownBinding take; indices are
// never reused, so one stays the binding's after it is removed, naming none.
// It may be called before Run or while the engine runs, and fails with
// net.ErrClosed once the engine has stopped.
//
// The binding's sockets are opened as New would open them, with the
// engine's Backlog and ReusePort, and are placed on the engine's loops as
// its other bindings' are: see Config.IOPollers, Config.EngineLoop and
// Config.ReusePort. Either every address listens or, with the error that
// stopped one, none does.
func (e *Engine) AddBinding(b Binding) (int, error) {
	e.bindMu.Lock()
	defer e.bindMu.Unlock()
	if e.stopping.Load() {
		return -1, net.ErrClosed
	}
	if b.Handler == nil {
		return -1, errNoHandler
	}
	b = resolveBinding(b, e.bindConfig)
	i := e.bindingCount
	if err := e.openBinding(b, i); err != nil {
		return -1, err
	}
	e.bindingCount++
	return i, nil
}

// RemoveBinding stops the engine listening on the i-th binding's addresses:
// its listeners stop taking connections and are closed, and their Unix
// socket files removed, before it returns. The connections they took stay
// open, as those a closed net.Listener took do, and Close or Shutdown ends
// them with the rest. A UDP address is the exception, since its peers have no
// socket of their own: they are closed with the socket, and OnClose receives
// net.ErrClosed. ShutdownBinding lets them finish first instead.
func (e *Engine) RemoveBinding(i int) error {
	e.bindMu.Lock()
	defer e.bindMu.Unlock()
	if i < 0 || i >= e.bindingCount {
		return errNoBinding
	}
	found, err := e.closeBinding(i, false)
	if err == nil && !found {
		err = errNoBinding
	}
	return err
}

// ShutdownBinding removes the i-th binding gracefully, as Shutdown stops the
// engine: its TCP and Unix listeners are closed at once, as RemoveBinding
// closes them, and its connections are left to finish, each told through
// OnShutdown if its handler is a ShutdownHandler, and its ShuttingDown
// reporting true. Its UDP sockets go on reading the datagrams of the peers
// they know, and open no connection for a new one. Once the binding has no
// connection left, or once ctx is done, when those still open are closed
// with net.ErrClosed, its UDP sockets are closed too. It returns ctx.Err() if
// ctx ended the wait, and nil otherwise. The engine's other bindings, and the
// connections it dialed, are left alone.
func (e *Engine) ShutdownBinding(ctx context.Context, i int) error {
	e.bindMu.Lock()
	if i < 0 || i >= e.bindingCount {
		e.bindMu.Unlock()
		return errNoBinding
	}
	e.setBindingDraining(i, true)
	found, err := e.closeBinding(i, true)
	if err == nil && !found {
		err = errNoBinding
	}
	if err != nil {
		e.setBindingDraining(i, false)
		e.bindMu.Unlock()
		return err
	}
	e.bindMu.Unlock()
	err = e.awaitDrained(ctx, i)
	if err != nil {
		e.closeBindingConnections(i)
	}
	e.bindMu.Lock()
	defer e.bindMu.Unlock()
	if _, closeErr := e.closeBinding(i, false); err == nil && !errors.Is(closeErr, net.ErrClosed) {
		err = closeErr
	}
	e.setBindingDraining(i, false)
	return err
}

// setBindingDraining adds the i-th binding to the set ShutdownBinding is
// draining, or takes it out. Callers hold bindMu.
func (e *Engine) setBindingDraining(i int, draining bool) {
	set := make(map[int]struct{})
	if old := e.drainingBindings.Load(); old != nil {
		for b := range *old {
			set[b] = struct{}{}
		}
	}
	if draining {
		set[i] = struct{}{}
	} else {
		delete(set, i)
	}
	if len(set) == 0 {
		e.drainingBindings.Store(nil)
		return
	}
	e.drainingBindings.Store(&set)
}

// bindingDraining reports whether ShutdownBinding is draining the binding
// whose index plus one is id; see Connection.bindingID.
func (e *Engine) bindingDraining(id int32) bool {
	set := e.drainingBindings.Load()
	if set == nil || id <= 0 {
		return false
	}
	_, ok := (*set)[int(id-1)]
	return ok
}

// ShuttingDown reports whether the connection is to finish: its engine's
// Shutdown has begun, or ShutdownBinding is draining the binding that took
// it. A protocol checks it to end the connection once its exchange in flight
// is done, as the HTTP server closes a connection behind the response it
// writes then.
func (c *Connection) ShuttingDown() bool {
	root := c.engine.root()
	return root.draining.Load() || root.bindingDraining(c.bindingID)
}

// bindingsOf returns what New is to listen on: bindings, each with its
// network filled in from config where it leaves it out, and its addresses in
// Addrs. It fails for a binding that names no handler.
func bindingsOf(config Config, bindings []Binding) ([]Binding, error) {
	resolved := make([]Binding, len(bindings))
	for i, b := range bindings {
		if b.Handler == nil {
			return nil, errNoHandler
		}
		resolved[i] = resolveBinding(b, config)
	}
	return resolved, nil
}

func resolveBinding(b Binding, config Config) Binding {
	if b.Network == "" {
		b.Network = config.Network
	}
	if b.Network == "" {
		b.Network = "tcp"
	}
	if len(b.Addrs) == 0 {
		b.Addrs = []string{b.Addr}
	} else {
		// The caller's slice stays the caller's.
		b.Addrs = append([]string(nil), b.Addrs...)
	}
	b.Addr = ""
	return b
}
