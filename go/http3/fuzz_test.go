//go:build linux || darwin || windows

package http3

import (
	"net/url"
	"reflect"
	"testing"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/http3/internal/qpack"
)

// FuzzFrameParser feeds the frame parser arbitrary bytes, split in two at
// an arbitrary point, since a stream arrives in whatever pieces QUIC
// delivers. Whatever it is given it either parses or reports, and never
// panics.
func FuzzFrameParser(f *testing.F) {
	f.Add(appendHeadersFrame(nil, []byte{0x00, 0x00, 0xc0 | 25}), uint8(0))
	f.Add(append(appendFrameHeader(nil, frameData, 4), 'b', 'o', 'd', 'y'), uint8(2))
	f.Add(appendSettings(nil, [2]uint64{settingMaxFieldSectionSize, 1 << 20}), uint8(1))
	f.Add([]byte{0x3f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, split uint8) {
		cut := int(split) % (len(data) + 1)
		p := frameParser{maxFrame: 1 << 16}
		var payloads int
		for _, chunk := range [][]byte{data[:cut], data[cut:]} {
			err := p.feed(chunk, func([]byte) error { return nil }, func(typ uint64, payload []byte) error {
				if uint64(len(payload)) > p.maxFrame {
					t.Fatalf("frame of %d bytes past the limit", len(payload))
				}
				payloads++
				return nil
			})
			if err != nil {
				return
			}
		}
	})
}

// FuzzNewRequest builds requests from arbitrary field sections: a request
// either comes out well formed or is refused.
func FuzzNewRequest(f *testing.F) {
	f.Add("GET", "https", "localhost", "/", "cookie", "a=1")
	f.Add("CONNECT", "", "localhost:443", "", "x-a", "b")
	f.Add("POST", "https", "localhost", "/p", "te", "trailers")
	f.Fuzz(func(t *testing.T, method, scheme, authority, path, name, value string) {
		fields := headerFields(method, scheme, authority, path, name, value)
		req, err := newRequest(fields, nil, nil)
		if err != nil {
			return
		}
		switch {
		case req.Method == "":
			t.Fatal("a request without a method")
		case req.URL == nil:
			t.Fatal("a request without a URL")
		case req.Method != "CONNECT" && (scheme == "http" || scheme == "https") && req.Host == "":
			// Only schemes with a mandatory authority component have to
			// name one (RFC 9114 section 4.3.1); an extension scheme may
			// leave it out.
			t.Fatal("a request for http or https without an authority")
		}
		if _, ok := req.Header["Host"]; ok {
			t.Fatal("a Host field left in the header")
		}
		if req.Method != "CONNECT" && path != "*" {
			// The request target is taken as net/url takes it.
			want, err := url.ParseRequestURI(path)
			if err != nil {
				t.Fatalf("%q was taken, but net/url refuses it: %v", path, err)
			}
			if !reflect.DeepEqual(req.URL, want) {
				t.Fatalf("%q parsed as %#v, net/url makes %#v of it", path, *req.URL, *want)
			}
		}
		// The same request built in a block, with room for its values, is
		// the same request.
		var block fibhttp.StreamRequest
		var values [2]string
		inBlock, err := newRequest(fields, &block, values[:])
		if err != nil {
			t.Fatalf("refused in a block: %v", err)
		}
		if inBlock.Method != req.Method || !reflect.DeepEqual(inBlock.URL, req.URL) || inBlock.Host != req.Host ||
			!reflect.DeepEqual(inBlock.Header, req.Header) {
			t.Fatalf("built in a block as %+v, on its own as %+v", inBlock, req)
		}
	})
}

// headerFields is a request's field section, pseudo-headers first.
func headerFields(method, scheme, authority, path, name, value string) []qpack.HeaderField {
	fields := []qpack.HeaderField{{Name: ":method", Value: method}}
	if scheme != "" {
		fields = append(fields, qpack.HeaderField{Name: ":scheme", Value: scheme})
	}
	if authority != "" {
		fields = append(fields, qpack.HeaderField{Name: ":authority", Value: authority})
	}
	if path != "" {
		fields = append(fields, qpack.HeaderField{Name: ":path", Value: path})
	}
	if name != "" {
		fields = append(fields, qpack.HeaderField{Name: name, Value: value})
	}
	return fields
}
