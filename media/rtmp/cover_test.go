package rtmp

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

func TestConnPartialAndLarge(t *testing.T) {
	// A server fed its handshake in pieces.
	var out []byte
	s := NewConn(ConnConfig{Output: func(b []byte) error { out = append(out, b...); return nil }})
	hello := clientHello(true)
	for _, piece := range [][]byte{hello[:100], hello[100:], make([]byte, 1000), make([]byte, handshakeSize-1000)} {
		if err := s.Feed(piece); err != nil {
			t.Fatal(err)
		}
	}
	if !s.HandshakeDone() {
		t.Fatal("server handshake")
	}
	// A client fed its in pieces.
	c := NewConn(ConnConfig{Client: true, Output: func([]byte) error { return nil }})
	_ = c.Start()
	if err := c.Feed(out[:2000]); err != nil || c.HandshakeDone() {
		t.Fatal(err)
	}
	if err := c.Feed(out[2000:]); err != nil || !c.HandshakeDone() {
		t.Fatal(err)
	}
	// Messages over a megabyte, written and read, let their buffers go.
	p := newPipe(t, true)
	p.chunk = 70000
	big := bytes.Repeat([]byte{3}, 1<<21)
	if err := p.client.WriteMessage(&Message{Type: TypeVideo, StreamID: 1, Payload: big}); err != nil {
		t.Fatal(err)
	}
	if err := p.client.SetChunkSize(1 << 20); err != nil {
		t.Fatal(err)
	}
	if err := p.client.WriteMessage(&Message{Type: TypeVideo, StreamID: 1, Payload: big[:300000]}); err != nil {
		t.Fatal(err)
	}
	p.pump(t)
	if len(p.srvMsgs) != 2 || !bytes.Equal(p.srvMsgs[0].Payload, big) || len(p.srvMsgs[1].Payload) != 300000 {
		t.Fatal(len(p.srvMsgs))
	}
	// An error from a message inside an aggregate stops the Feed.
	stop := errors.New("stop")
	sc := serverConn(t, func(*Message) error { return stop }, nil)
	agg := []byte{TypeVideo, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0xaa, 0, 0, 0, 12}
	b := append(chunkHeader(0, 7, 0, uint32(len(agg)), TypeAggregate, 1), agg...)
	if err := sc.Feed(b); err != stop {
		t.Fatal(err)
	}
	// A closed client outputs nothing.
	c = NewConn(ConnConfig{Client: true, Output: func([]byte) error { return nil }})
	c.Close()
	if err := c.Start(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

// fakeServer accepts one connection and runs fn on it with a server Conn,
// fn reading commands through next.
func fakeServer(t *testing.T, fn func(conn *Conn, next func(name string) *Command, nc net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		defer nc.Close()
		var msgs []*Message
		conn := NewConn(ConnConfig{
			Output:    func(b []byte) error { _, err := nc.Write(b); return err },
			OnMessage: func(m *Message) error { c := copyMsg(m); msgs = append(msgs, &c); return nil }})
		buf := make([]byte, 65536)
		next := func(name string) *Command {
			for {
				for i, m := range msgs {
					if cmd, err := ParseCommand(m); err == nil && cmd.Name == name {
						msgs = msgs[i+1:]
						return cmd
					}
				}
				_ = nc.SetReadDeadline(time.Now().Add(testTimeout))
				n, err := nc.Read(buf)
				if err != nil {
					return nil
				}
				_ = conn.Feed(buf[:n])
			}
		}
		fn(conn, next, nc)
	}()
	return "rtmp://" + ln.Addr().String() + "/a/b"
}

func TestClientEdges(t *testing.T) {
	e := run(t)
	// A server that answers the handshake with the wrong version.
	url := fakeServer(t, func(conn *Conn, next func(string) *Command, nc net.Conn) {
		b := make([]byte, 1+2*handshakeSize)
		b[0] = 6
		_, _ = nc.Write(b)
		time.Sleep(time.Second)
	})
	if _, err := play(t, e, url, ClientConfig{}, newRecorder(0)); !errors.Is(err, ErrHandshake) {
		t.Fatal(err)
	}
	// A server that sends a malformed command, starts the play twice and
	// then a malformed chunk, which ends the play.
	url = fakeServer(t, func(conn *Conn, next func(string) *Command, nc net.Conn) {
		c := next("connect")
		_ = conn.WriteMessage(&Message{Type: TypeCommandAMF0, Payload: []byte{0x20}})
		_ = conn.WriteCommand(0, "_result", c.TransactionID, nil, nil)
		c = next("createStream")
		_ = conn.WriteCommand(0, "_result", c.TransactionID, nil, 1.0)
		next("play")
		_ = conn.WriteCommand(1, "onStatus", 0, nil, status("status", "NetStream.Play.Start", ""))
		_ = conn.WriteCommand(1, "onStatus", 0, nil, status("status", "NetStream.Play.Start", ""))
		_ = conn.WriteCommand(1, "onStatus", 0, nil, status("status", "NetStream.Play.Stop", ""))
		time.Sleep(100 * time.Millisecond)
		_, _ = nc.Write([]byte{0x44, 0, 0, 0, 0, 0, 1, 8, 1}) // type 1 header on a new chunk stream
		time.Sleep(time.Second)
	})
	closed := make(chan error, 1)
	r := newRecorder(0)
	if _, err := play(t, e, url, ClientConfig{OnClose: func(c *ClientConn, err error) { closed <- err }}, r); err != nil {
		t.Fatal(err)
	}
	r.waitClosed(t)
	<-closed
	// A nil ready, and a dial on an engine that is closed.
	if _, err := Publish(e, url, ClientConfig{}, nil); err != nil {
		t.Fatal(err)
	}
	dead, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	_ = dead.Close()
	if _, err := Publish(dead, url, ClientConfig{}, func(*ClientConn, error) {}); err == nil {
		t.Fatal("dial on a closed engine")
	}
	(&clientHandler{}).OnPriorityData(nil, nil)
	if err := (&ClientConn{}).Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServerEdges(t *testing.T) {
	h := hub.New(hub.Config{OnPublish: func(path string, _ hub.PublishInfo) error {
		if path == "live/refused" {
			return errors.New("no")
		}
		return nil
	}})
	defer h.Close()
	tracks, packets := stream(t)
	conns := make(chan *ServerConn, 4)
	also := newRecorder(0)
	url := serve(t, ServerConfig{Hub: h, IdleTimeout: -1,
		OnPublish: func(c *ServerConn, path, query string) (hub.Sink, error) {
			conns <- c
			return also, nil
		}})
	// The hub refusing a publish closes the sink OnPublish gave.
	r := dialRaw(t, url+"/live/refused")
	_ = r.conn.WriteCommand(0, "connect", 1, amf.ECMAArray{{Key: "app", Value: "live"}})
	r.command("_result")
	_ = r.conn.WriteCommand(1, "publish", 0, nil, "refused")
	r.status("NetStream.Publish.Denied")
	<-conns
	if err := also.waitClosed(t); err == nil {
		t.Fatal("no error")
	}
	// publish with no arguments is refused.
	r = dialRaw(t, url+"/live/x")
	r.connect("live")
	_ = r.conn.WriteCommand(1, "publish", 0, nil)
	r.status("NetStream.Publish.BadName")
	// A malformed chunk closes the connection.
	r = dialRaw(t, url+"/live/x")
	r.connect("live")
	_, _ = r.nc.Write([]byte{0x44, 0, 0, 0, 0, 0, 1, 8, 1})
	r.closed()
	// A large message makes the round's arena go once it is done with.
	r = dialRaw(t, url+"/live/big")
	r.connect("live")
	_ = r.conn.WriteCommand(1, "publish", 0, nil, "big")
	r.status("NetStream.Publish.Start")
	<-conns
	_ = r.conn.SetChunkSize(1 << 20)
	_ = r.conn.WriteMessage(&Message{Type: TypeVideo, StreamID: 1, Payload: make([]byte, 5<<20)})
	_ = r.conn.WriteMessage(&Message{Type: TypeVideo, StreamID: 1, Payload: []byte{0x17, 2, 0, 0, 0}})
	_ = r.conn.WriteCommand(0, "getStreamLength", 5, nil, "big")
	r.command("_result")
	// The server closing a publisher's connection: the client hears it,
	// and what it is given afterwards goes nowhere.
	e := run(t)
	clientClosed := make(chan error, 1)
	pub, err := publish(t, e, url+"/live/s", ClientConfig{OnClose: func(c *ClientConn, err error) { clientClosed <- err }})
	if err != nil {
		t.Fatal(err)
	}
	sc := <-conns
	if err := sc.Close(); err != nil {
		t.Fatal(err)
	}
	<-clientClosed
	pub.OnTracks(tracks)
	pub.OnPacket(packets[0])
	// The callbacks fib never makes for RTMP.
	NewServer(ServerConfig{}).OnPriorityData(nil, nil)
	// Without fib's output accounting, the backlog reads as empty.
	saved := outputwait.Queued
	outputwait.Queued = nil
	if sc.backlog() != 0 {
		t.Fatal("backlog")
	}
	outputwait.Queued = saved
}

func TestEgressEdges(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	url := serve(t, ServerConfig{Hub: h, PlayWait: time.Second})
	e := run(t)
	got := newRecorder(1)
	if _, err := play(t, e, url+"/live/s", ClientConfig{}, got); err != nil {
		t.Fatal(err)
	}
	pub, err := publish(t, e, url+"/live/s", ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// Tracks FLV cannot carry send nothing, and a packet of a track the
	// stream does not have is dropped.
	pub.OnTracks([]*av.Track{{Codec: av.CodecUnknown}})
	pub.OnPacket(&av.Packet{Track: 0, Data: []byte{1}})
	pub.OnTracks([]*av.Track{{Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}})
	pub.OnPacket(&av.Packet{Track: 5, Data: []byte{1}})
	pub.OnPacket(&av.Packet{Track: 0, Keyframe: true, Data: []byte{1, 2}})
	got.wait(t)
	// A publish that ends hands its client's sink OnClose.
	pub.OnClose(nil)
	got.waitClosed(t)
}
