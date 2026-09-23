package http

import (
	"bufio"
	"bytes"
	stdhttp "net/http"
	"reflect"
	"testing"
)

// requestHeadCorpus is requests in and around the shape parseSimpleRequestHead
// takes on, each ending where the parser hands it over: after the blank line.
var requestHeadCorpus = []string{
	"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
	"GET /echo HTTP/1.1\r\nHost: localhost\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n",
	"POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 1024\r\nContent-Type: application/octet-stream\r\n\r\n",
	"POST /echo HTTP/1.1\r\nhost: a\r\ncontent-length: 5\r\nx-custom-thing: 1\r\n\r\n",
	"GET /a/b;c=d/e:f@g HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /search?q=a+b&x=%20y HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /a? HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /a?? HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /a?b? HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET //double/slash HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /%41 HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /a!b*c(d) HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /a#frag HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET * HTTP/1.1\r\nHost: a\r\n\r\n",
	"OPTIONS * HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET http://example.com/x HTTP/1.1\r\nHost: other\r\n\r\n",
	"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
	"GET / HTTP/1.0\r\n\r\n",
	"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n",
	"GET / HTTP/1.0\r\nConnection: Keep-Alive, foo\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nConnection: foo,  CLOSE \r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nConnection: cloſe\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive\r\nConnection: close\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
	"GET / HTTP/1.1\r\n\r\n",
	"GET / HTTP/1.1\r\nHost:\r\n\r\n",
	"GET / HTTP/1.1\r\nHost:   spaced \t \r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Empty:\r\nX-Tab:\tv\t\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Multi: 1\r\nX-Multi: 2\r\nx-multi: 3\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Obs: caf\xc3\xa9\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Ctl: a\x01b\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Del: a\x7fb\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Cr: a\rb\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nBad Key: v\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nKey : v\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\n: v\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nNoColon\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nX-Fold: a\r\n b\r\n\r\n",
	"GET / HTTP/1.1\r\n Host: a\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\nX-Lf: b\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nPragma: no-cache\r\n\r\n",
	"GET / HTTP/1.1\r\nHost: a\r\nPragma: no-cache\r\nCache-Control: max-age=0\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 007\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: -1\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: +1\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length:\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 99999999999999999999\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n",
	"POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n",
	"POST / HTTP/1.1\r\nHost: a\r\nTrailer: X-T\r\nContent-Length: 1\r\n\r\n",
	"PRI * HTTP/2.0\r\n\r\n",
	"get / HTTP/1.1\r\nHost: a\r\n\r\n",
	"PROPFIND /dav HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET / HTTP/1.2\r\nHost: a\r\n\r\n",
	"GET / http/1.1\r\nHost: a\r\n\r\n",
	"GET  / HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET / HTTP/1.1 \r\nHost: a\r\n\r\n",
	"GET /\x00 HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /?\x01 HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET /?caf\xc3\xa9 HTTP/1.1\r\nHost: a\r\n\r\n",
	"\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n",
	"GET / HTTP/1.1\nHost: a\r\n\r\n",
}

func TestParseRequestHeadMatchesNetHTTP(t *testing.T) {
	simple := 0
	for _, raw := range requestHeadCorpus {
		if compareRequestHead(t, []byte(raw)) {
			simple++
		}
	}
	// The corpus is mostly requests the fast path is meant for; if it takes
	// on none of them, the comparison above has checked nothing.
	if simple < 25 {
		t.Fatalf("the simple parser took %d of the corpus, want most of it", simple)
	}
}

func FuzzParseRequestHead(f *testing.F) {
	for _, raw := range requestHeadCorpus {
		f.Add([]byte(raw))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		// The parser is only ever given a header through its blank line.
		end := bytes.Index(data, []byte("\r\n\r\n"))
		if end < 0 {
			return
		}
		compareRequestHead(t, data[:end+4])
	})
}

// compareRequestHead checks that parseRequestHead makes of head what
// net/http.ReadRequest does, and reports whether the simple parser took it on.
func compareRequestHead(t *testing.T, head []byte) bool {
	t.Helper()
	want, wantErr := stdhttp.ReadRequest(bufio.NewReader(bytes.NewReader(head)))
	got, _, gotErr := parseRequestHead(head)
	if (wantErr == nil) != (gotErr == nil) {
		t.Fatalf("%q: error %v, net/http's %v", head, gotErr, wantErr)
	}
	if wantErr != nil {
		if gotErr.Error() != wantErr.Error() {
			t.Fatalf("%q: error %q, net/http's %q", head, gotErr, wantErr)
		}
		return false
	}
	simple := parseSimpleRequestHead(head) != nil
	checks := []struct {
		name      string
		got, want any
	}{
		{"Method", got.Method, want.Method},
		{"URL", got.URL, want.URL},
		{"Proto", got.Proto, want.Proto},
		{"ProtoMajor", got.ProtoMajor, want.ProtoMajor},
		{"ProtoMinor", got.ProtoMinor, want.ProtoMinor},
		{"Header", got.Header, want.Header},
		{"Host", got.Host, want.Host},
		{"ContentLength", got.ContentLength, want.ContentLength},
		{"TransferEncoding", got.TransferEncoding, want.TransferEncoding},
		{"Close", got.Close, want.Close},
		{"Trailer", got.Trailer, want.Trailer},
		{"RequestURI", got.RequestURI, want.RequestURI},
	}
	for _, check := range checks {
		if !reflect.DeepEqual(check.got, check.want) {
			t.Fatalf("%q (simple=%v): %s = %#v, net/http's %#v", head, simple, check.name, check.got, check.want)
		}
	}
	if got.Body != stdhttp.NoBody {
		t.Fatalf("%q: body %T, want http.NoBody", head, got.Body)
	}
	return simple
}

func BenchmarkParseRequestHead(b *testing.B) {
	head := []byte("POST /echo HTTP/1.1\r\nHost: 127.0.0.1:11001\r\nContent-Type: application/octet-stream\r\n" +
		"Content-Length: 1024\r\n\r\n")
	for _, parse := range []struct {
		name string
		fn   func([]byte) (*stdhttp.Request, error)
	}{
		{"simple", func(head []byte) (*stdhttp.Request, error) {
			req, _, err := parseRequestHead(head)
			return req, err
		}},
		{"nethttp", func(head []byte) (*stdhttp.Request, error) {
			req, _, err := readRequest(head, false)
			return req, err
		}},
	} {
		b.Run(parse.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := parse.fn(head); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
