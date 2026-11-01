package rtmp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

const testTimeout = 10 * time.Second

func run(t *testing.T, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = "rtmp-" + t.Name()
	engine, err := fib.New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return engine
}

func addrOf(t *testing.T, e *fib.Engine, i int) string {
	t.Helper()
	addrs, err := e.BindingAddrs(i)
	if err != nil {
		t.Fatal(err)
	}
	return addrs[0].String()
}

// serve runs a Server on 127.0.0.1 and returns its rtmp:// base URL.
func serve(t *testing.T, config ServerConfig) string {
	t.Helper()
	e := run(t, fib.Binding{Addr: "127.0.0.1:0", Handler: NewServer(config)})
	return "rtmp://" + addrOf(t, e, 0)
}

// stream is a real stream: the tracks and packets of the H.264 and AAC
// fixture of package flv.
func stream(t testing.TB) ([]*av.Track, []*av.Packet) {
	t.Helper()
	b, err := os.ReadFile("../flv/testdata/h264_aac.flv")
	if err != nil {
		t.Fatal(err)
	}
	var tracks []*av.Track
	var packets []*av.Packet
	dec := flv.NewDecoder(flv.DecoderConfig{
		OnTracks: func(tr []*av.Track) { tracks = tr },
		OnPacket: func(p *av.Packet) { packets = append(packets, p) },
	})
	if err := flv.NewDemuxer(flv.DemuxerConfig{}).Feed(b, func(tag flv.Tag) error {
		return dec.WriteTag(tag.Type, tag.Timestamp, tag.Data)
	}); err != nil {
		t.Fatal(err)
	}
	dec.Flush()
	return tracks, packets
}

// recorder is a hub.Sink that keeps what it hears.
type recorder struct {
	mu      sync.Mutex
	tracks  [][]*av.Track
	packets []*av.Packet
	closed  chan error
	want    int
	enough  chan struct{}
}

func newRecorder(want int) *recorder {
	return &recorder{closed: make(chan error, 1), want: want, enough: make(chan struct{})}
}

func (r *recorder) OnTracks(t []*av.Track) {
	r.mu.Lock()
	r.tracks = append(r.tracks, t)
	r.mu.Unlock()
}

func (r *recorder) OnPacket(p *av.Packet) {
	r.mu.Lock()
	r.packets = append(r.packets, p)
	if len(r.packets) == r.want {
		close(r.enough)
	}
	r.mu.Unlock()
}

func (r *recorder) OnClose(err error) {
	select {
	case r.closed <- err:
	default:
	}
}

func (r *recorder) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.enough:
	case <-time.After(testTimeout):
		r.mu.Lock()
		defer r.mu.Unlock()
		t.Fatalf("%d packets of %d", len(r.packets), r.want)
	}
}

func (r *recorder) waitClosed(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.closed:
		return err
	case <-time.After(testTimeout):
		t.Fatal("not closed")
		return nil
	}
}

func waitReady(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatal("not ready")
		return nil
	}
}

func publish(t *testing.T, engine *fib.Engine, url string, config ClientConfig) (*ClientConn, error) {
	t.Helper()
	ready := make(chan error, 1)
	cc, err := Publish(engine, url, config, func(_ *ClientConn, err error) { ready <- err })
	if err != nil {
		t.Fatal(err)
	}
	return cc, waitReady(t, ready)
}

func play(t *testing.T, engine *fib.Engine, url string, config ClientConfig, sink hub.Sink) (*ClientConn, error) {
	t.Helper()
	ready := make(chan error, 1)
	cc, err := Play(engine, url, config, sink, func(_ *ClientConn, err error) { ready <- err })
	if err != nil {
		t.Fatal(err)
	}
	return cc, waitReady(t, ready)
}

func checkStream(t *testing.T, r *recorder, tracks []*av.Track, packets []*av.Packet) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.tracks) == 0 {
		t.Fatal("no tracks")
	}
	got := r.tracks[len(r.tracks)-1]
	if len(got) != len(tracks) || got[0].Codec != tracks[0].Codec || !bytes.Equal(got[0].Config, tracks[0].Config) ||
		got[1].Codec != tracks[1].Codec || got[0].Width != tracks[0].Width {
		t.Fatalf("tracks %+v", got)
	}
	for i, p := range r.packets[:len(packets)] {
		w := packets[i]
		if p.Track != w.Track || p.Keyframe != w.Keyframe || !bytes.Equal(p.Data, w.Data) ||
			p.DTS != w.DTS || p.CTS() != w.CTS() {
			t.Fatalf("packet %d: %+v want %+v", i, p, w)
		}
	}
}

func TestPublishPlay(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "rtmp", true: "rtmps"}[secure], func(t *testing.T) {
			h := hub.New(hub.Config{})
			defer h.Close()
			closed := make(chan error, 4)
			server := NewServer(ServerConfig{Hub: h, PlayWait: time.Second,
				OnClose: func(c *ServerConn, err error) { closed <- err }})
			var handler fib.Handler = server
			scheme := "rtmp://"
			if secure {
				handler = fibtls.NewServer(serverTLS, server)
				scheme = "rtmps://"
			}
			e := run(t, fib.Binding{Addr: "127.0.0.1:0", Handler: handler})
			url := scheme + addrOf(t, e, 0) + "/live/s?token=1"
			config := ClientConfig{TLSConfig: clientTLS}
			tracks, packets := stream(t)
			// A player that comes before the publisher waits for it.
			early := newRecorder(len(packets))
			player, err := play(t, e, url, config, early)
			if err != nil {
				t.Fatal(err)
			}
			pub, err := publish(t, e, url, config)
			if err != nil {
				t.Fatal(err)
			}
			if pub.URL().Path() != "live/s" || pub.Conn() == nil {
				t.Fatal(pub.URL())
			}
			if info := h.Streams(); len(info) != 1 || info[0].Publisher.Query != "token=1" || info[0].Publisher.Protocol != "rtmp" {
				t.Fatalf("%+v", info)
			}
			pub.OnTracks(tracks)
			for _, p := range packets {
				pub.OnPacket(p)
			}
			early.wait(t)
			checkStream(t, early, tracks, packets)
			// A player that comes later starts from the GOP cache, with a
			// keyframe.
			late := newRecorder(1)
			if _, err := play(t, e, url, config, late); err != nil {
				t.Fatal(err)
			}
			late.wait(t)
			late.mu.Lock()
			if !late.packets[0].Keyframe && late.packets[0].Track == 0 {
				t.Fatal("late player starts without a keyframe")
			}
			late.mu.Unlock()
			// Unpublishing ends the players.
			_ = pub.Close()
			if err := early.waitClosed(t); err == nil {
				t.Fatal("no error")
			}
			late.waitClosed(t)
			_ = player.Close()
		})
	}
}

func TestRefusals(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	refuse := errors.New("no")
	url := serve(t, ServerConfig{Hub: h,
		OnConnect: func(c *ServerConn) error {
			if c.App() == "closed" {
				return refuse
			}
			if c.TcURL() == "" || c.ConnectObject() == nil || c.Conn() == nil {
				t.Error("connect details missing")
			}
			return nil
		},
		OnPublish: func(c *ServerConn, path, query string) (hub.Sink, error) {
			if query == "bad" {
				return nil, refuse
			}
			return nil, nil
		},
		OnPlay: func(c *ServerConn, path, query string) error {
			if query == "bad" {
				return refuse
			}
			return nil
		},
	})
	e := run(t)
	code := func(err error) string {
		var se *StatusError
		if errors.As(err, &se) {
			return se.Code
		}
		return "not a status: " + errors.Join(err).Error()
	}
	_, err := publish(t, e, url+"/closed/s", ClientConfig{})
	if code(err) != "NetConnection.Connect.Rejected" {
		t.Fatal(err)
	}
	_, err = publish(t, e, url+"/live/s?bad", ClientConfig{})
	if code(err) != "NetStream.Publish.Unauthorized" {
		t.Fatal(err)
	}
	_, err = play(t, e, url+"/live/s?bad", ClientConfig{}, newRecorder(0))
	if code(err) != "NetStream.Play.Failed" {
		t.Fatal(err)
	}
	_, err = play(t, e, url+"/live/none", ClientConfig{}, newRecorder(0))
	if code(err) != "NetStream.Play.StreamNotFound" {
		t.Fatal(err)
	}
	first, err := publish(t, e, url+"/live/s", ClientConfig{SimpleHandshake: true})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	_, err = publish(t, e, url+"/live/s", ClientConfig{})
	if code(err) != "NetStream.Publish.BadName" {
		t.Fatal(err)
	}
	// A hub that refuses through its own hook.
	h2 := hub.New(hub.Config{
		OnPublish:   func(string, hub.PublishInfo) error { return refuse },
		OnSubscribe: func(string, hub.SubscribeInfo) error { return refuse },
	})
	defer h2.Close()
	url2 := serve(t, ServerConfig{Hub: h2})
	_, err = publish(t, e, url2+"/live/s", ClientConfig{})
	if code(err) != "NetStream.Publish.Denied" {
		t.Fatal(err)
	}
	_, err = play(t, e, url2+"/live/s", ClientConfig{}, newRecorder(0))
	if code(err) != "NetStream.Play.Failed" {
		t.Fatal(err)
	}
	// No hub and no sink.
	url3 := serve(t, ServerConfig{})
	_, err = publish(t, e, url3+"/live/s", ClientConfig{})
	if code(err) != "NetStream.Publish.Denied" {
		t.Fatal(err)
	}
	// Dial failures.
	if _, err := Publish(e, "http://x/a/b", ClientConfig{}, nil); err == nil {
		t.Fatal("bad URL")
	}
	_, err = publish(t, e, "rtmp://127.0.0.1:1/live/s", ClientConfig{DialTimeout: time.Second})
	if err == nil {
		t.Fatal("dial to nothing")
	}
}

func TestHubless(t *testing.T) {
	tracks, packets := stream(t)
	ingest := newRecorder(len(packets))
	players := make(chan hub.Sink, 1)
	url := serve(t, ServerConfig{
		OnPublish: func(c *ServerConn, path, query string) (hub.Sink, error) {
			if path != "live/s" {
				t.Errorf("path %q", path)
			}
			return ingest, nil
		},
		OnPlay: func(c *ServerConn, path, query string) error {
			players <- c.Player()
			return nil
		},
	})
	e := run(t)
	pub, err := publish(t, e, url+"/live/s", ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	pub.OnTracks(tracks)
	for _, p := range packets {
		pub.OnPacket(p)
	}
	ingest.wait(t)
	checkStream(t, ingest, tracks, packets)
	_ = pub.Close()
	ingest.waitClosed(t)

	got := newRecorder(len(packets))
	if _, err := play(t, e, url+"/live/s", ClientConfig{}, got); err != nil {
		t.Fatal(err)
	}
	player := <-players
	player.OnTracks(tracks)
	for _, p := range packets {
		player.OnPacket(p)
	}
	got.wait(t)
	checkStream(t, got, tracks, packets)
	player.OnClose(hub.ErrUnpublished)
	got.waitClosed(t)
}

func TestRelays(t *testing.T) {
	tracks, packets := stream(t)
	local := hub.New(hub.Config{})
	remote := hub.New(hub.Config{})
	defer local.Close()
	defer remote.Close()
	url := serve(t, ServerConfig{Hub: remote, PlayWait: time.Second})
	e := run(t)
	// Push a local stream to the server.
	pub, err := local.Publish("src", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	pushed := make(chan error, 1)
	push, err := Push(e, local, "src", url+"/live/pushed", 0, ClientConfig{}, func(err error) { pushed <- err })
	if err != nil {
		t.Fatal(err)
	}
	if err := waitReady(t, pushed); err != nil {
		t.Fatal(err)
	}
	if push.Conn() == nil {
		t.Fatal("no conn")
	}
	// Pull it back into the local hub under another path.
	pulled := make(chan error, 1)
	pull, err := Pull(e, local, url+"/live/pushed", "back", ClientConfig{}, func(err error) { pulled <- err })
	if err != nil {
		t.Fatal(err)
	}
	if err := waitReady(t, pulled); err != nil {
		t.Fatal(err)
	}
	got := newRecorder(len(packets))
	if _, err := local.Subscribe("back", got, hub.SubscribeOptions{Wait: time.Second}); err != nil {
		t.Fatal(err)
	}
	for _, p := range packets {
		pub.WritePacket(p)
	}
	got.wait(t)
	checkStream(t, got, tracks, packets)
	// Pulling into a path that has a publisher fails.
	busy := make(chan error, 1)
	if _, err := Pull(e, local, url+"/live/pushed", "src", ClientConfig{}, func(err error) { busy <- err }); err != nil {
		t.Fatal(err)
	}
	if err := waitReady(t, busy); !errors.Is(err, hub.ErrStreamBusy) {
		t.Fatal(err)
	}
	// Pushing a path with no stream fails.
	none := make(chan error, 1)
	if _, err := Push(e, local, "none", url+"/live/none", 0, ClientConfig{}, func(err error) { none <- err }); err != nil {
		t.Fatal(err)
	}
	if err := waitReady(t, none); !errors.Is(err, hub.ErrNotFound) {
		t.Fatal(err)
	}
	pull.Close()
	push.Close()
	got.waitClosed(t)
	// A relay closed before it starts does not start.
	closedEarly := make(chan error, 1)
	r, err := Push(e, local, "src", url+"/live/early", 0, ClientConfig{}, func(err error) { closedEarly <- err })
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err := waitReady(t, closedEarly); err == nil {
		t.Fatal("started after Close")
	}
	if _, err := Push(e, local, "src", "bad", 0, ClientConfig{}, nil); err == nil {
		t.Fatal("bad URL")
	}
	if _, err := Pull(e, local, "bad", "x", ClientConfig{}, nil); err == nil {
		t.Fatal("bad URL")
	}
}

// rawClient speaks RTMP over a blocking net.Conn through a Conn, for tests
// that need to send what the package's own client never would.
type rawClient struct {
	t    *testing.T
	nc   net.Conn
	conn *Conn
	msgs []*Message
	buf  []byte
}

func dialRaw(t *testing.T, url string) *rawClient {
	t.Helper()
	u, err := ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	r := &rawClient{t: t, nc: nc, buf: make([]byte, 64<<10)}
	r.conn = NewConn(ConnConfig{Client: true,
		Output:    func(b []byte) error { _, err := nc.Write(b); return err },
		OnMessage: func(m *Message) error { c := copyMsg(m); r.msgs = append(r.msgs, &c); return nil }})
	_ = r.conn.Start()
	for !r.conn.HandshakeDone() {
		r.read()
	}
	return r
}

// read reads once, and reports false at the end of the connection.
func (r *rawClient) read() bool {
	r.t.Helper()
	_ = r.nc.SetReadDeadline(time.Now().Add(testTimeout))
	n, err := r.nc.Read(r.buf)
	if err != nil {
		return false
	}
	if err := r.conn.Feed(r.buf[:n]); err != nil {
		r.t.Fatal(err)
	}
	return true
}

// command waits for the command named name.
func (r *rawClient) command(name string) *Command {
	r.t.Helper()
	for {
		for i, m := range r.msgs {
			if m.Type == TypeCommandAMF0 {
				if cmd, err := ParseCommand(m); err == nil && cmd.Name == name {
					r.msgs = r.msgs[i+1:]
					return cmd
				}
			}
		}
		if !r.read() {
			r.t.Fatalf("closed waiting for %s", name)
		}
	}
}

// status waits for an onStatus with code, passing over others.
func (r *rawClient) status(code string) {
	r.t.Helper()
	for {
		if _, c, _ := statusCode(r.command("onStatus").Args[0]); c == code {
			return
		}
	}
}

func (r *rawClient) closed() bool {
	r.t.Helper()
	for r.read() {
	}
	return true
}

func (r *rawClient) connect(app string) {
	r.t.Helper()
	_ = r.conn.WriteCommand(0, "connect", 1, amf.Object{{Key: "app", Value: app}, {Key: "objectEncoding", Value: 3.0}})
	if res := r.command("_result"); res.TransactionID != 1 {
		r.t.Fatal(res)
	}
}

func TestServerCommands(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	url := serve(t, ServerConfig{Hub: h})
	// A command before connect closes the connection.
	r := dialRaw(t, url+"/live/s")
	_ = r.conn.WriteCommand(0, "createStream", 2, nil)
	r.closed()
	// The commands the server answers, and the ones it does not know.
	r = dialRaw(t, url+"/live/s")
	r.connect("live")
	_ = r.conn.WriteCommand(0, "connect", 9, nil) // a second connect is ignored
	for i, name := range []string{"releaseStream", "FCPublish", "getStreamLength", "_checkbw"} {
		_ = r.conn.WriteCommand(0, name, float64(10+i), nil, "s")
		if res := r.command("_result"); res.TransactionID != float64(10+i) {
			t.Fatal(name, res)
		}
	}
	_ = r.conn.WriteCommand(0, "whatever", 20, nil)
	if res := r.command("_error"); res.TransactionID != 20 {
		t.Fatal(res)
	}
	_ = r.conn.WriteCommand(0, "whatever", 0, nil)                                  // no answer
	_ = r.conn.WriteCommand(0, "receiveVideo", 0, nil)                              // ignored
	_ = r.conn.WriteMessage(&Message{Type: TypeCommandAMF0, Payload: []byte{0x20}}) // malformed: ignored
	_ = r.conn.WriteCommand(0, "createStream", 21, nil)
	res := r.command("_result")
	id := uint32(res.Args[0].(float64))
	// AMF3 publish, with an empty name refused.
	p, _ := amf.Append([]byte{0}, "publish", 0.0, nil, "")
	_ = r.conn.WriteMessage(&Message{Type: TypeCommandAMF3, StreamID: id, Payload: p})
	st := r.command("onStatus")
	if _, code, _ := statusCode(st.Args[0]); code != "NetStream.Publish.BadName" {
		t.Fatal(code)
	}
	r.closed()

	// Publishing, then deleteStream ends the publish; closeStream on a
	// stream not playing does nothing.
	r = dialRaw(t, url+"/live/s")
	r.connect("live")
	_ = r.conn.WriteCommand(0, "createStream", 2, nil)
	id = uint32(r.command("_result").Args[0].(float64))
	_ = r.conn.WriteCommand(id, "publish", 0, nil, "s", "live")
	if _, code, _ := statusCode(r.command("onStatus").Args[0]); code != "NetStream.Publish.Start" {
		t.Fatal(code)
	}
	// A second publish on the same connection is refused.
	_ = r.conn.WriteCommand(id, "publish", 0, nil, "t", "live")
	if _, code, _ := statusCode(r.command("onStatus").Args[0]); code != "NetStream.Publish.BadName" {
		t.Fatal(code)
	}
	r.closed()
	waitFor(t, func() bool { return len(h.Streams()) == 0 })

	r = dialRaw(t, url+"/live/s")
	r.connect("live")
	_ = r.conn.WriteCommand(1, "publish", 0, nil, "s", "live")
	r.command("onStatus")
	_ = r.conn.WriteCommand(1, "closeStream", 0, nil)
	waitFor(t, func() bool { return len(h.Streams()) == 0 })
	_ = r.conn.WriteCommand(1, "publish", 0, nil, "s", "live")
	r.command("onStatus")
	_ = r.conn.WriteCommand(0, "deleteStream", 0, nil, 1.0)
	waitFor(t, func() bool { return len(h.Streams()) == 0 })

	// Playing, then closeStream and a second play.
	pub, _ := h.Publish("live/p", nil, hub.PublishInfo{})
	defer pub.Close()
	r = dialRaw(t, url+"/live/p")
	r.connect("live")
	_ = r.conn.WriteCommand(1, "play", 0, nil, "p")
	r.status("NetStream.Play.Start")
	_ = r.conn.WriteCommand(1, "play", 0, nil, "p")
	r.status("NetStream.Play.Failed")
	r.closed()
	r = dialRaw(t, url+"/live/p")
	r.connect("live")
	_ = r.conn.WriteCommand(1, "play", 0, nil, "p")
	r.status("NetStream.Play.Start")
	_ = r.conn.WriteCommand(1, "closeStream", 0, nil)
	waitFor(t, func() bool { return h.Streams()[0].Subscribers == 0 })
	_ = r.conn.WriteCommand(1, "play", 0, nil, "")
	if _, code, _ := statusCode(r.command("onStatus").Args[0]); code != "NetStream.Play.Failed" {
		t.Fatal(code)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestTimeouts(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	closed := make(chan error, 4)
	url := serve(t, ServerConfig{Hub: h, HandshakeTimeout: 200 * time.Millisecond, IdleTimeout: 300 * time.Millisecond,
		OnClose: func(c *ServerConn, err error) { closed <- err }})
	u, _ := ParseURL(url + "/a/b")
	// A client that sends nothing is closed after HandshakeTimeout.
	nc, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_ = nc.SetReadDeadline(time.Now().Add(testTimeout))
	if _, err := io.ReadAll(nc); err != nil {
		t.Fatal(err)
	}
	<-closed
	// A publisher that goes quiet is closed after IdleTimeout.
	e := run(t)
	pub, err := publish(t, e, url+"/live/s", ClientConfig{IdleTimeout: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	select {
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(testTimeout):
		t.Fatal("idle publisher not closed")
	}
	// A client whose server never answers fails its handshake.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			defer c.Close()
			time.Sleep(time.Second)
		}
	}()
	_, err = publish(t, e, "rtmp://"+ln.Addr().String()+"/a/b", ClientConfig{HandshakeTimeout: 100 * time.Millisecond})
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestShutdown(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	server := NewServer(ServerConfig{Hub: h, HandshakeTimeout: -1, IdleTimeout: -1})
	// run's cleanup closes the engine only once Run has returned, as
	// Engine.Close requires.
	engine := run(t, fib.Binding{Addr: "127.0.0.1:0", Handler: server})
	r := dialRaw(t, "rtmp://"+addrOf(t, engine, 0)+"/a/b")
	r.connect("a")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	r.closed()
	// The handler's callbacks tolerate a connection without a session.
	c := &fib.Connection{}
	server.OnData(c, nil)
	server.OnClose(c, nil)
	server.OnPriorityData(c, nil)
}

func TestClientAnswersServerCalls(t *testing.T) {
	// A server that calls the client, sends its status in a _error, and
	// ends a play with StreamEOF.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	answered := make(chan *Command, 1)
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
				n, err := nc.Read(buf)
				if err != nil {
					return nil
				}
				_ = conn.Feed(buf[:n])
			}
		}
		c := next("connect")
		_ = conn.WriteCommand(0, "onBWDone", 0, nil)
		_ = conn.WriteCommand(0, "serverCall", 7, nil)
		_ = conn.WriteCommand(0, "_result", c.TransactionID, nil, status("status", "NetConnection.Connect.Success", ""))
		answered <- next("_result")
		c = next("createStream")
		_ = conn.WriteCommand(0, "_result", c.TransactionID, nil, 1.0)
		next("play")
		_ = conn.WriteCommand(1, "onStatus", 0, nil, status("status", "NetStream.Play.Start", ""))
		_ = conn.WriteUserControl(EventStreamEOF, 1)
		next("never")
	}()
	e := run(t)
	r := newRecorder(0)
	if _, err := play(t, e, "rtmp://"+ln.Addr().String()+"/a/b", ClientConfig{}, r); err != nil {
		t.Fatal(err)
	}
	if cmd := <-answered; cmd.TransactionID != 7 {
		t.Fatal(cmd)
	}
	r.waitClosed(t)
}

func TestClientStatusErrors(t *testing.T) {
	// Servers that answer createStream with an _error, or with a _result
	// that holds no stream id.
	for _, mode := range []string{"_error", "empty"} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		go func() {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			defer nc.Close()
			var cmds []*Command
			conn := NewConn(ConnConfig{
				Output: func(b []byte) error { _, err := nc.Write(b); return err },
				OnMessage: func(m *Message) error {
					if cmd, err := ParseCommand(m); err == nil {
						cmds = append(cmds, cmd)
					}
					return nil
				}})
			buf := make([]byte, 65536)
			for {
				n, err := nc.Read(buf)
				if err != nil {
					return
				}
				_ = conn.Feed(buf[:n])
				for _, c := range cmds {
					switch c.Name {
					case "connect":
						_ = conn.WriteCommand(0, "_result", c.TransactionID, nil, nil)
					case "createStream":
						if mode == "_error" {
							_ = conn.WriteCommand(0, "_error", c.TransactionID, nil, status("error", "X.Failed", "no"))
						} else {
							_ = conn.WriteCommand(0, "_result", c.TransactionID, nil)
						}
					}
				}
				cmds = nil
			}
		}()
		e := run(t)
		_, err = publish(t, e, "rtmp://"+ln.Addr().String()+"/a/b", ClientConfig{})
		if err == nil {
			t.Fatal(mode)
		}
	}
}
