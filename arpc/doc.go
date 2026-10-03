// Package arpc is github.com/lesismal/arpc served by fib's engine: two-way
// calls and notifies, asynchronous calls, streams, broadcast, middleware,
// message coders, service registration and singleflight, on the same wire
// format, so a client or server of either package talks to one of the other.
//
// A Server is a fib.Handler. Bind it to an engine, wrap it with tls.NewServer
// first for TLS, or let Run and Serve make the engine:
//
//	server := arpc.NewServer()
//	server.Handler.Handle("/echo", func(ctx *arpc.Context) {
//		var s string
//		if err := ctx.Bind(&s); err == nil {
//			ctx.Write(s)
//		}
//	})
//	engine, err := fib.Bind(config, server)
//
// A Client is one connection, on either side: Dial and NewClient open one on
// an engine and reconnect it when it breaks, and a Server makes one for every
// connection it accepts. Both make calls to the peer and serve the peer's.
//
// # Where handlers run
//
// A request or notify is handled on the connection's engine worker, as it is
// read, or away from it on a task pool. Handler.SetAsyncResponse picks one
// for every method registered after it, true by default, and the optional
// bool of Handle and HandleStream picks for one method. A handler run where
// the connection is read holds up the rest of that connection until it
// returns; one that calls the same peer and waits for the answer never gets
// it.
//
// The pool is the one fib's HTTP/2 and HTTP/3 run their request handlers on,
// fib.Engine.HandlerPool of the connection's engine, unless
// Handler.SetTaskPool names another. Once the pool stops taking work, which it
// does after its engines close, a handler runs where the connection is read.
//
// # Differences from github.com/lesismal/arpc
//
// What a Client sends goes to the connection at once, by fib.Connection's
// nonblocking Send, whose queue and watermarks bound what waits for the
// socket, so there is no send queue or send goroutine, and nothing to
// configure about them: the batch, writev, send queue and buffer size options
// are gone, as are the reader options, and the timeouts of Notify, PushMsg and
// Context.WriteWithTimeout bound nothing. A Client cannot Restart; one that
// loses its connection reconnects on its own.
package arpc
