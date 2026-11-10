package http

import (
	"io"
	stdhttp "net/http"
	"testing"
)

// Fields set on Context.Header before a whole response go with it, whichever
// call sends it: Respond and its helpers, and WriteResponse, whose own fields
// win. Trailer announcements and Content-Length set there do not.
func TestRespondSendsHeader(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		c.Header().Set("X-Mw", "set")
		c.Header().Set("Content-Type", "text/html")
		c.Header().Set("Content-Length", "999")
		c.Header().Set("Trailer", "X-Sum")
		switch c.Request.URL.Path {
		case "/string":
			_ = c.String(stdhttp.StatusAccepted, "hi")
		case "/json":
			_ = c.JSON(stdhttp.StatusOK, map[string]int{"n": 1})
		case "/hooked":
			c.OnHeader(func(_ int, h stdhttp.Header) { h.Set("X-Hook", "yes") })
			_ = c.String(stdhttp.StatusOK, "hi")
		case "/write":
			_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Header: stdhttp.Header{"X-Mw": {"own"}}, Body: []byte("hi")})
		case "/empty":
			_ = c.Respond(stdhttp.StatusOK, "", []byte("hi"))
		}
	})
	var h1, h2 stdhttp.Protocols
	h1.SetHTTP1(true)
	h2.SetUnencryptedHTTP2(true)
	cases := []struct{ path, mw, ctype, hook string }{
		{"/string", "set", "text/plain; charset=utf-8", ""},
		{"/json", "set", "application/json", ""},
		{"/hooked", "set", "text/plain; charset=utf-8", "yes"},
		{"/write", "own", "text/html", ""},
		{"/empty", "set", "text/html", ""},
	}
	for name, protocols := range map[string]*stdhttp.Protocols{"HTTP/1.1": &h1, "HTTP/2": &h2} {
		client := &stdhttp.Client{Transport: &stdhttp.Transport{Protocols: protocols}}
		defer client.CloseIdleConnections()
		for _, tc := range cases {
			resp, err := client.Get("http://" + addr + tc.path)
			if err != nil {
				t.Fatalf("%s %s: %v", name, tc.path, err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("%s %s: %v", name, tc.path, err)
			}
			if got := resp.Header.Get("X-Mw"); got != tc.mw {
				t.Errorf("%s %s: X-Mw %q, want %q", name, tc.path, got, tc.mw)
			}
			if got := resp.Header.Get("Content-Type"); got != tc.ctype {
				t.Errorf("%s %s: Content-Type %q, want %q", name, tc.path, got, tc.ctype)
			}
			if got := resp.Header.Get("X-Hook"); got != tc.hook {
				t.Errorf("%s %s: X-Hook %q, want %q", name, tc.path, got, tc.hook)
			}
			if resp.ContentLength != int64(len(body)) || resp.Header.Get("Trailer") != "" {
				t.Errorf("%s %s: length %d for %d bytes, Trailer %q", name, tc.path, resp.ContentLength, len(body), resp.Header.Get("Trailer"))
			}
		}
	}
}
