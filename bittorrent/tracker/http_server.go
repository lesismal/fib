//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tracker

import (
	"net/netip"
	"strings"

	fibhttp "github.com/lesismal/fib/http"
)

// ServeHTTP serves the tracker over HTTP: a request is an announce when
// the last segment of its path begins with "announce", and a scrape when
// it begins with "scrape". Refusals are answered as BEP 3 has them, with a
// 200 response holding a failure reason; any other path gets 404.
func (s *Server) ServeHTTP(c *fibhttp.Context) {
	u := c.Request.URL
	path := u.Path
	last := path[strings.LastIndexByte(path, '/')+1:]
	var body []byte
	switch {
	case strings.HasPrefix(last, "announce"):
		body = s.httpAnnounce(remoteOf(c), path, u.RawQuery)
	case strings.HasPrefix(last, "scrape"):
		body = s.httpScrape(remoteOf(c), path, u.RawQuery)
	default:
		_ = c.Respond(404, "text/plain; charset=utf-8", []byte("not found\n"))
		return
	}
	_ = c.Respond(200, "text/plain", body)
}

// remoteOf returns the address a request came from.
func remoteOf(c *fibhttp.Context) netip.AddrPort {
	if c.Conn != nil {
		if ap := c.Conn.RemoteAddrPort(); ap.IsValid() {
			return ap
		}
	}
	ap, _ := netip.ParseAddrPort(c.Request.RemoteAddr)
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}
