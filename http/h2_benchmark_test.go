//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	stdtls "crypto/tls"
	"encoding/binary"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/hpack"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// The HTTP/2 server benchmarks drive one connection the way h2load does:
// h2BenchStreams requests in flight on it at once, each answered as it ends,
// with a new one sent in its place. One op is one request. The client is a
// few frames written by hand and a reader that only counts the streams that
// end, so that most of what is measured is the server; client and server
// still share the process, so compare runs with each other rather than with
// a load generator's numbers.
const h2BenchStreams = 100

// BenchmarkH2ServerGET answers a GET with a short body, as baseline-h2c does.
func BenchmarkH2ServerGET(b *testing.B) {
	benchmarkH2Server(b, h2BenchOptions{handler: h2BenchReply(benchReply)})
}

// BenchmarkH2ServerPOST sends each request with a short body.
func BenchmarkH2ServerPOST(b *testing.B) {
	benchmarkH2Server(b, h2BenchOptions{handler: h2BenchReply(benchReply), body: []byte("hello, world")})
}

// BenchmarkH2ServerStatic answers with a cached file's body and header, as
// FileCache does and static-h2 measures, in the sizes of a small asset and of
// one that fills several frames.
func BenchmarkH2ServerStatic(b *testing.B) {
	for _, size := range []int{1 << 10, 16 << 10, 64 << 10} {
		b.Run(strconv.Itoa(size>>10)+"KB", func(b *testing.B) {
			benchmarkH2Server(b, h2BenchOptions{handler: h2BenchStatic(size)})
		})
	}
}

// BenchmarkH2ServerStaticTLS is BenchmarkH2ServerStatic over TLS, where the
// body is also sealed into records.
func BenchmarkH2ServerStaticTLS(b *testing.B) {
	for _, size := range []int{1 << 10, 16 << 10, 64 << 10} {
		b.Run(strconv.Itoa(size>>10)+"KB", func(b *testing.B) {
			benchmarkH2Server(b, h2BenchOptions{handler: h2BenchStatic(size), tls: true})
		})
	}
}

// BenchmarkH2ServerStreamedPOST sends a body that streams to its handler
// (Config.StreamRequestBody), which reads it as it arrives.
func BenchmarkH2ServerStreamedPOST(b *testing.B) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	benchmarkH2Server(b, h2BenchOptions{config: &config, body: make([]byte, 4<<10),
		handler: HandlerFunc(func(c *Context) {
			c.Retain()
			c.OnBody(func(_ []byte, fin bool, err error) {
				if fin || err != nil {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
					c.Release()
				}
			})
		})})
}

// BenchmarkH2ServerPush pushes a resource with each response.
func BenchmarkH2ServerPush(b *testing.B) {
	benchmarkH2Server(b, h2BenchOptions{push: true, handler: HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/" {
			_ = c.Push("/pushed", nil)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", benchReply)
	})})
}

type h2BenchOptions struct {
	config  *Config
	handler Handler
	body    []byte
	tls     bool
	// push has the client allow server push, and count a request as done
	// once the pushed stream has ended too.
	push bool
}

func h2BenchReply(reply []byte) Handler {
	return HandlerFunc(func(c *Context) { _ = c.Respond(stdhttp.StatusOK, "text/plain", reply) })
}

func h2BenchStatic(size int) Handler {
	body := make([]byte, size)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	header := stdhttp.Header{"Content-Type": {"text/css; charset=utf-8"},
		"Last-Modified": {"Mon, 02 Jan 2006 15:04:05 GMT"}}
	return HandlerFunc(func(c *Context) {
		_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Header: header, Body: body})
	})
}

func benchmarkH2Server(b *testing.B, o h2BenchOptions) {
	config := DefaultConfig()
	if o.config != nil {
		config = *o.config
	}
	var handler fib.Handler = NewHandlerWithConfig(config, o.handler)
	var clientTLS *stdtls.Config
	if o.tls {
		serverConfig, clientConfig, err := tlstest.Configs()
		if err != nil {
			b.Fatal(err)
		}
		handler = fibtls.NewServer(ConfigureTLS(serverConfig), handler)
		clientTLS = clientConfig
		clientTLS.NextProtos = []string{"h2"}
	}
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		b.Fatal(err)
	}
	local, err := engine.LocalAddr()
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	defer func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	}()

	var conn net.Conn
	conn, err = net.Dial("tcp", local.String())
	if err != nil {
		b.Fatal(err)
	}
	if clientTLS != nil {
		tc := stdtls.Client(conn, clientTLS)
		if err = tc.Handshake(); err != nil {
			b.Fatal(err)
		}
		conn = tc
	}
	defer conn.Close()

	// The client's windows are as large as HTTP/2 allows, as h2load's are
	// by default, so that flow control paces nothing.
	enablePush := uint32(0)
	if o.push {
		enablePush = 1
	}
	preface := append([]byte(h2Preface), h2AppendSettings(nil,
		[2]uint32{uint32(h2SettingInitialWindowSize), h2MaxWindow},
		[2]uint32{uint32(h2SettingEnablePush), enablePush})...)
	preface = h2AppendWindowUpdate(preface, 0, h2MaxWindow-h2DefaultWindow)
	if _, err = conn.Write(preface); err != nil {
		b.Fatal(err)
	}

	slots := make(chan struct{}, h2BenchStreams)
	var wmu sync.Mutex
	write := func(p []byte) error {
		wmu.Lock()
		defer wmu.Unlock()
		_, err := conn.Write(p)
		return err
	}
	var finished atomic.Int64
	readDone := make(chan error, 1)
	b.ReportAllocs()
	b.ResetTimer()
	go func() { readDone <- h2BenchRead(conn, write, slots, int64(b.N), o.push, &finished) }()

	enc := hpack.NewEncoder()
	var out, block []byte
	length := strconv.Itoa(len(o.body))
	id := uint32(1)
	for sent := 0; sent < b.N; {
		slots <- struct{}{}
		out = out[:0]
		// Every slot free now goes into one write, as a load generator
		// batches what it has to send.
		for n := 1; ; n++ {
			block = enc.Begin(block[:0])
			if o.body != nil {
				block = enc.AppendField(block, ":method", "POST", false)
			} else {
				block = enc.AppendField(block, ":method", "GET", false)
			}
			block = enc.AppendField(block, ":scheme", "https", false)
			block = enc.AppendField(block, ":authority", "localhost", false)
			block = enc.AppendField(block, ":path", "/", false)
			if o.body != nil {
				block = enc.AppendField(block, "content-length", length, false)
			}
			out = h2AppendHeaderBlock(out, id, block, o.body == nil, h2DefaultMaxFrameSize)
			if o.body != nil {
				out = h2AppendFrameHeader(out, h2FrameData, h2FlagEndStream, id, len(o.body))
				out = append(out, o.body...)
			}
			id += 2
			sent++
			if sent == b.N || n == h2BenchStreams {
				break
			}
			select {
			case slots <- struct{}{}:
				continue
			default:
			}
			break
		}
		if err := write(out); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-readDone; err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

// h2BenchRead reads the server's frames until n requests have ended, freeing
// a slot as each does, answering SETTINGS and topping the connection's window
// up as it runs low.
func h2BenchRead(conn net.Conn, write func([]byte) error, slots chan struct{}, n int64, push bool, finished *atomic.Int64) error {
	buf := make([]byte, 0, 256<<10)
	var received int64
	// With push, a request is done once both its response and the pushed
	// one have ended, which the even ids say.
	ends := int64(1)
	if push {
		ends = 2
	}
	var ended int64
	for finished.Load() < n {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		m, err := conn.Read(buf[len(buf):cap(buf)])
		if err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		buf = buf[:len(buf)+m]
		offset := 0
		for {
			f, size, err := h2ReadFrame(buf[offset:], h2MaxFrameSizeLimit)
			if err != nil {
				return err
			}
			if size == 0 {
				break
			}
			offset += size
			switch f.typ {
			case h2FrameSettings:
				if !f.has(h2FlagAck) {
					if err := write(h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0)); err != nil {
						return err
					}
				}
			case h2FrameData:
				received += int64(f.length)
				if received >= 1<<30 {
					if err := write(h2AppendWindowUpdate(nil, 0, uint32(received))); err != nil {
						return err
					}
					received = 0
				}
			case h2FrameGoAway:
				return errorString("GOAWAY code " + strconv.Itoa(int(binary.BigEndian.Uint32(f.payload[4:]))))
			case h2FrameRSTStream:
				return errorString("RST_STREAM on stream " + strconv.Itoa(int(f.streamID)))
			}
			if (f.typ == h2FrameHeaders || f.typ == h2FrameData) && f.has(h2FlagEndStream) {
				ended++
				if ended == ends {
					ended = 0
					finished.Add(1)
					<-slots
				}
			}
		}
		buf = buf[:copy(buf, buf[offset:])]
	}
	return nil
}

type errorString string

func (e errorString) Error() string { return string(e) }
