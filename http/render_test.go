//go:build linux || darwin || windows

package http

import (
	"html/template"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestContextRenderHelpers(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "note.txt")
	if err := os.WriteFile(file, []byte("file body"), 0o644); err != nil {
		t.Fatal(err)
	}
	type item struct {
		ID int `json:"id" xml:"id"`
	}
	type wrapper struct{ A item }
	tmpl := template.Must(template.New("page").Parse(`{{define "hi"}}<b>{{.}}</b>{{end}}`))
	addr := serveHTTP1(t, func(c *Context) {
		switch c.Request.URL.Path {
		case "/string":
			_ = c.String(201, "n=%d", 7)
		case "/plain":
			pct := "100%"
			_ = c.String(200, "%s", pct)
		case "/data":
			_ = c.Data(200, "image/png", []byte{1, 2})
		case "/ijson":
			_ = c.IndentedJSON(200, item{1})
		case "/xml":
			_ = c.XML(200, item{1})
		case "/ixml":
			_ = c.IndentedXML(200, wrapper{item{1}})
		case "/html":
			_ = c.HTML(200, "<p>x</p>")
		case "/tmpl":
			_ = c.HTMLTemplate(200, tmpl, "hi", "<x>")
		case "/badtmpl":
			if err := c.HTMLTemplate(200, tmpl, "nope", nil); err == nil {
				t.Error("executed a missing template")
			}
			_ = c.String(500, "failed")
		case "/file":
			c.File(file)
		case "/attach":
			c.FileAttachment(file, `a"b/c.txt`)
		}
	})
	tests := []struct {
		path, ctype, body string
		status            int
	}{
		{"/string", "text/plain; charset=utf-8", "n=7", 201},
		{"/plain", "text/plain; charset=utf-8", "100%", 200},
		{"/data", "image/png", "\x01\x02", 200},
		{"/ijson", "application/json", "{\n    \"id\": 1\n}", 200},
		{"/xml", "application/xml; charset=utf-8", "<item><id>1</id></item>", 200},
		{"/ixml", "application/xml; charset=utf-8", "<wrapper>\n    <A>\n        <id>1</id>\n    </A>\n</wrapper>", 200},
		{"/html", "text/html; charset=utf-8", "<p>x</p>", 200},
		{"/tmpl", "text/html; charset=utf-8", "<b>&lt;x&gt;</b>", 200},
		{"/badtmpl", "text/plain; charset=utf-8", "failed", 500},
		{"/file", "text/plain; charset=utf-8", "file body", 200},
	}
	for _, tc := range tests {
		resp, err := stdhttp.Get("http://" + addr + tc.path)
		if err != nil {
			t.Fatal(tc.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.status || resp.Header.Get("Content-Type") != tc.ctype || string(body) != tc.body {
			t.Errorf("%s = %d %q %q; want %d %q %q", tc.path, resp.StatusCode, resp.Header.Get("Content-Type"), body, tc.status, tc.ctype, tc.body)
		}
	}
	resp, err := stdhttp.Get("http://" + addr + "/attach")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if got := resp.Header.Get("Content-Disposition"); got != `attachment; filename="c.txt"` || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Errorf("FileAttachment: Content-Disposition %q", got)
	}
}
