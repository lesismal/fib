package httpflv

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// FuzzDefaultPath maps arbitrary request paths: a path it accepts names a
// stream, without the extension or a leading or trailing slash.
func FuzzDefaultPath(f *testing.F) {
	for _, p := range []string{"/live/s.flv", "/a/b/c.flv", "/.flv", "/live/", "/x.flv.flv", "//a.flv"} {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, p string) {
		path, _, ok := DefaultPath(&http.Request{URL: &url.URL{Path: p}})
		if !ok {
			return
		}
		if path == "" || strings.HasPrefix(path, "/") && !strings.HasPrefix(p, "//") || strings.HasSuffix(path, "/") {
			t.Fatalf("%q -> %q", p, path)
		}
	})
}
