//go:build linux || darwin || windows

package http

import (
	"io"
	"testing"
)

// FuzzParser feeds the HTTP/1 parser arbitrary bytes, split in two at an
// arbitrary point, since a connection arrives in whatever pieces TCP
// delivers. Whatever it is given it either parses or reports, and never
// panics.
func FuzzParser(f *testing.F) {
	f.Add([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"), uint8(0))
	f.Add([]byte("POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 4\r\n\r\nbody"), uint8(20))
	f.Add([]byte("POST /p HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nbody\r\n0\r\n\r\n"), uint8(30))
	f.Add([]byte("GET / HTTP/1.0\r\n\r\nGET / HTTP/1.0\r\n\r\n"), uint8(18))
	f.Fuzz(func(t *testing.T, data []byte, split uint8) {
		cut := int(split) % (len(data) + 1)
		config := DefaultConfig()
		config.MaxHeaderBytes = 1 << 16
		config.MaxBodyBytes = 1 << 16
		parser := NewParser(config)
		for _, chunk := range [][]byte{data[:cut], data[cut:]} {
			requests, err := parser.Feed(chunk)
			if err != nil {
				return
			}
			for _, request := range requests {
				if request.Method == "" || request.URL == nil {
					t.Fatalf("parsed a request without a method or URL: %+v", request)
				}
				if request.Body != nil {
					if _, err := io.Copy(io.Discard, request.Body); err != nil {
						t.Fatalf("reading the parsed body: %v", err)
					}
				}
			}
		}
	})
}

// FuzzH2Frame reads arbitrary bytes as HTTP/2 frames, which is what a
// connection hands the server before anything else has looked at them.
func FuzzH2Frame(f *testing.F) {
	f.Add(h2AppendSettings(nil))
	f.Add(h2AppendRSTStream(nil, 1, H2ProtocolError))
	f.Add(h2AppendHeaderBlock(nil, 1, []byte{0x82}, true, h2DefaultMaxFrameSize))
	f.Add(h2AppendWindowUpdate(nil, 0, 1<<20))
	f.Add([]byte{0x00, 0x00, 0x08, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Fuzz(func(t *testing.T, data []byte) {
		buf := data
		for len(buf) > 0 {
			frame, n, err := h2ReadFrame(buf, h2DefaultMaxFrameSize)
			if err != nil || n == 0 {
				return
			}
			if n > len(buf) || len(frame.payload) > int(h2DefaultMaxFrameSize) {
				t.Fatalf("frame of %d bytes from %d, payload %d", n, len(buf), len(frame.payload))
			}
			if frame.typ == h2FrameSettings && frame.streamID == 0 && !frame.has(h2FlagAck) {
				_ = h2ParseSettings(frame.payload, func(h2SettingID, uint32) error { return nil })
			}
			buf = buf[n:]
		}
	})
}
