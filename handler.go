package fib

// Handler callbacks run on a logical worker, except OnOpen, which runs on the
// event-loop goroutine. OnClose runs in the connection's last round, where its
// OnData ran, after every OnData before it; that is the event loop where the
// engine runs rounds there. Data is only valid for the duration of OnData.
type Handler interface {
	OnOpen(*Connection)
	OnData(*Connection, []byte)
	OnPriorityData(*Connection, []byte)
	OnClose(*Connection, error)
}

// DatagramsHandler is a Handler that takes a UDP connection's datagrams in
// bursts. When a Handler implements it, the datagrams that have arrived for
// a connection by the time its worker runs go to OnDatagrams together, in
// the order they arrived, instead of to OnData one at a time. A protocol
// that answers what it receives - with acknowledgements, say - can then
// answer the whole burst at once, in fewer datagrams than it would one
// arrival at a time. The portable backend, on platforms other than Linux,
// macOS and Windows, keeps calling OnData.
//
// Each datagram belongs to the handler, as a UDP connection's OnData data
// does; the slice holding them does not, and must not be kept once
// OnDatagrams returns. A datagram is a buffer from package bufferpool, so a
// handler that is done with one, with nothing left referring to it, may give
// it back with bufferpool.Put for the next one to be read into; one it keeps
// or drops is collected like any other slice.
type DatagramsHandler interface {
	Handler
	OnDatagrams(c *Connection, datagrams [][]byte)
}

// HandlerFuncs allows callers to implement only the callbacks they need.
type HandlerFuncs struct {
	Open         func(*Connection)
	Data         func(*Connection, []byte)
	PriorityData func(*Connection, []byte)
	Close        func(*Connection, error)
}

func (h HandlerFuncs) OnOpen(c *Connection) {
	if h.Open != nil {
		h.Open(c)
	}
}

func (h HandlerFuncs) OnData(c *Connection, b []byte) {
	if h.Data != nil {
		h.Data(c, b)
	}
}

func (h HandlerFuncs) OnPriorityData(c *Connection, b []byte) {
	if h.PriorityData != nil {
		h.PriorityData(c, b)
	}
}

func (h HandlerFuncs) OnClose(c *Connection, err error) {
	if h.Close != nil {
		h.Close(c, err)
	}
}
