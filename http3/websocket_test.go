//go:build linux || darwin || windows

package http3

import (
	"bytes"
	"sync"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/websocket"
)

// streamReader gathers what arrives on the request stream of a raw client
// and splits it into HTTP/3 frames.
type streamReader struct {
	t       *testing.T
	mu      sync.Mutex
	data    []byte
	fin     bool
	control []byte
	arrived chan struct{}
}

func newStreamReader(t *testing.T, r *rawConn, request *quic.Stream) *streamReader {
	sr := &streamReader{t: t, arrived: make(chan struct{}, 1)}
	r.mu.Lock()
	r.onStream = func(s *quic.Stream, data []byte, fin bool) {
		sr.mu.Lock()
		switch {
		case s == request:
			sr.data = append(sr.data, data...)
			sr.fin = sr.fin || fin
		case !s.Bidirectional():
			sr.control = append(sr.control, data...)
		}
		sr.mu.Unlock()
		select {
		case sr.arrived <- struct{}{}:
		default:
		}
	}
	r.mu.Unlock()
	return sr
}

// frame waits for the next whole frame on the request stream, or for its end,
// which ok reports.
func (sr *streamReader) frame() (typ uint64, payload []byte, ok bool) {
	sr.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		sr.mu.Lock()
		typ, n1 := quic.ReadVarint(sr.data)
		length, n2 := uint64(0), 0
		if n1 > 0 {
			length, n2 = quic.ReadVarint(sr.data[n1:])
		}
		if n1 > 0 && n2 > 0 && uint64(len(sr.data)) >= uint64(n1+n2)+length {
			payload := append([]byte(nil), sr.data[n1+n2:uint64(n1+n2)+length]...)
			sr.data = sr.data[uint64(n1+n2)+length:]
			sr.mu.Unlock()
			return typ, payload, true
		}
		if sr.fin && len(sr.data) == 0 {
			sr.mu.Unlock()
			return 0, nil, false
		}
		sr.mu.Unlock()
		select {
		case <-sr.arrived:
		case <-deadline:
			sr.t.Fatal("timed out waiting for a frame")
		}
	}
}

// settings waits for the server's SETTINGS and returns them.
func (sr *streamReader) settings() map[uint64]uint64 {
	sr.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		sr.mu.Lock()
		control := sr.control
		sr.mu.Unlock()
		if len(control) > 1 {
			// The stream type, then the SETTINGS frame.
			typ, n1 := quic.ReadVarint(control[1:])
			length, n2 := quic.ReadVarint(control[1+n1:])
			if typ == frameSettings && n2 > 0 && uint64(len(control)) >= uint64(1+n1+n2)+length {
				settings := map[uint64]uint64{}
				for p := control[1+n1+n2 : uint64(1+n1+n2)+length]; len(p) > 0; {
					id, n := quic.ReadVarint(p)
					value, m := quic.ReadVarint(p[n:])
					settings[id] = value
					p = p[n+m:]
				}
				return settings
			}
		}
		select {
		case <-sr.arrived:
		case <-deadline:
			sr.t.Fatal("timed out waiting for SETTINGS")
		}
	}
}

// maskedFrame is a client's WebSocket frame.
func maskedFrame(opcode websocket.Opcode, payload []byte) []byte {
	mask := [4]byte{9, 8, 7, 6}
	frame := []byte{0x80 | byte(opcode), 0x80 | byte(len(payload))}
	frame = append(frame, mask[:]...)
	for i, b := range payload {
		frame = append(frame, b^mask[i&3])
	}
	return frame
}

func dataFrame(payload []byte) []byte {
	return append(appendFrameHeader(nil, frameData, len(payload)), payload...)
}

func TestWebSocketOverExtendedConnect(t *testing.T) {
	closed := make(chan uint16, 1)
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, opcode websocket.Opcode, payload []byte) {
			_ = c.WriteMessage(opcode, payload)
		},
		Close: func(_ *websocket.Connection, code uint16, _ string, _ error) { closed <- code },
	})
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		if _, err := ws.Upgrade(c, nil); err != nil {
			t.Logf("upgrade: %v", err)
		}
	})
	r := dialRaw(t, base)
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	sr := newStreamReader(t, r, s)
	if settings := sr.settings(); settings[settingEnableConnectProtocol] != 1 {
		t.Fatalf("SETTINGS_ENABLE_CONNECT_PROTOCOL not advertised: %v", settings)
	}
	request := headersFrame(
		qpack.HeaderField{Name: ":method", Value: "CONNECT"},
		qpack.HeaderField{Name: ":protocol", Value: "websocket"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
		qpack.HeaderField{Name: ":path", Value: "/ws"},
		qpack.HeaderField{Name: "sec-websocket-version", Value: "13"},
	)
	// A frame sent with the request waits for the handler to accept it.
	if err := s.Write(append(request, dataFrame(maskedFrame(websocket.Text, []byte("early")))...), false); err != nil {
		t.Fatal(err)
	}
	typ, payload, ok := sr.frame()
	if !ok || typ != frameHeaders {
		t.Fatalf("frame %d, want HEADERS", typ)
	}
	var decoder qpack.Decoder
	fields, err := decodeFields(&decoder, nil, payload, 1<<16)
	if err != nil || len(fields) == 0 || fields[0].Name != ":status" || fields[0].Value != "200" {
		t.Fatalf("response %v %v", fields, err)
	}
	expectEcho := func(want string) {
		t.Helper()
		typ, payload, ok := sr.frame()
		if !ok || typ != frameData || !bytes.Equal(payload, append([]byte{0x81, byte(len(want))}, want...)) {
			t.Fatalf("frame %d %q, want the echo of %q", typ, payload, want)
		}
	}
	expectEcho("early")
	if err := s.Write(dataFrame(maskedFrame(websocket.Text, []byte("over h3"))), false); err != nil {
		t.Fatal(err)
	}
	expectEcho("over h3")

	// The close handshake, then FIN each way.
	if err := s.Write(dataFrame(maskedFrame(websocket.Close, []byte{0x03, 0xe8})), false); err != nil {
		t.Fatal(err)
	}
	if typ, payload, ok := sr.frame(); !ok || typ != frameData || len(payload) < 2 || payload[0] != 0x88 {
		t.Fatalf("frame %d %x, want the close echo", typ, payload)
	}
	if _, _, ok := sr.frame(); ok {
		t.Fatal("stream not ended after the close")
	}
	_ = s.Close()
	select {
	case code := <-closed:
		if code != websocket.CloseNormal {
			t.Fatalf("close code %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose not called")
	}
}
