//go:build linux || darwin || windows

// Package proxy is the core of the gateway example: a reverse proxy that
// takes HTTP/1.1, HTTP/2, HTTP/3 and WebSocket requests and forwards them to
// upstream servers, without blocking an engine worker anywhere on the way.
//
// Everything is a callback:
//
//   - A request body that arrives after its header is taken with
//     Context.OnBody, piece by piece, instead of being read by a handler that
//     waits for it.
//   - The upstream request goes out through the asynchronous clients of
//     package http and package http3. The handler retains its Context and
//     returns; the client's callback answers.
//   - The upstream response body is taken with ClientResponse.OnBody and
//     written to the downstream response as it arrives, so a download of any
//     size passes through in constant memory, and the slower of the two peers
//     paces the other.
//   - A WebSocket request is upgraded in the handler, which is the only place
//     Upgrade can be called; the upstream is then dialed with websocket.Dialer,
//     whose callback joins the two, and from there each side's messages are
//     written to the other from the handler callbacks.
//
// A route maps a path prefix to an upstream. The upstream URL's scheme
// chooses how it is reached: http:// is HTTP/1.1, https:// is HTTP/2 if the
// server picks it through ALPN and HTTP/1.1 otherwise, and h3:// is HTTP/3.
// WebSocket requests to an h3:// upstream go to wss:// on the same host and
// port, since the dialer speaks WebSocket over HTTP/1.1.
package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
)

// Route sends the requests under a path prefix to an upstream.
type Route struct {
	// Prefix is the path the route serves, without a trailing slash; "/"
	// serves everything. The prefix is stripped from the path that goes
	// upstream, so that /api/users to a route on /api for http://backend/v1
	// arrives there as /v1/users.
	Prefix string
	Target *url.URL
}

// ParseRoute reads "prefix=target", such as "/api=http://127.0.0.1:9000".
func ParseRoute(spec string) (Route, error) {
	prefix, target, ok := strings.Cut(spec, "=")
	if !ok || !strings.HasPrefix(prefix, "/") {
		return Route{}, fmt.Errorf("route %q: want /prefix=scheme://host:port", spec)
	}
	u, err := url.Parse(target)
	if err != nil {
		return Route{}, fmt.Errorf("route %q: %w", spec, err)
	}
	switch u.Scheme {
	case "http", "https", "h3":
	default:
		return Route{}, fmt.Errorf("route %q: scheme must be http, https or h3", spec)
	}
	if u.Host == "" {
		return Route{}, fmt.Errorf("route %q: no host", spec)
	}
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		prefix = "/"
	}
	return Route{Prefix: prefix, Target: u}, nil
}

func (r *Route) matches(path string) bool {
	return r.Prefix == "/" || path == r.Prefix || strings.HasPrefix(path, r.Prefix+"/")
}

// upstreamPath is where the request for escapedPath goes on the upstream: the
// target's own path, then what follows the route's prefix.
func (r *Route) upstreamPath(escapedPath string) string {
	rest := escapedPath
	if r.Prefix != "/" {
		rest = strings.TrimPrefix(escapedPath, r.Prefix)
	}
	if rest == "" || rest[0] != '/' {
		rest = "/" + rest
	}
	return strings.TrimSuffix(r.Target.EscapedPath(), "/") + rest
}

// httpURL is the URL of the request on the upstream. h3 upstreams are reached
// with https URLs by the HTTP/3 client.
func (r *Route) httpURL(u *url.URL) string {
	scheme := r.Target.Scheme
	if scheme == "h3" {
		scheme = "https"
	}
	return withQuery(scheme+"://"+r.Target.Host+r.upstreamPath(u.EscapedPath()), u)
}

// wsURL is the same for a WebSocket handshake.
func (r *Route) wsURL(u *url.URL) string {
	scheme := "ws"
	if r.Target.Scheme != "http" {
		scheme = "wss"
	}
	return withQuery(scheme+"://"+r.Target.Host+r.upstreamPath(u.EscapedPath()), u)
}

func withQuery(target string, u *url.URL) string {
	if u.RawQuery != "" {
		return target + "?" + u.RawQuery
	}
	return target
}

// Config is what a Gateway is built from.
type Config struct {
	Routes []Route
	// TLSConfig is how https://, h3:// and wss:// upstreams are verified. Nil
	// means the system roots.
	TLSConfig *tls.Config
	// Timeout bounds one upstream request, from the call until its response
	// body has ended; zero means no limit beyond the downstream request's own
	// cancellation. It is also what turns a hung upstream into a 504.
	Timeout time.Duration
	// MaxRequestBody bounds a request body. The gateway collects it before it
	// sends the request, since a client request is sent whole, so this is also
	// the memory one upload can take. Zero means 64 MiB.
	MaxRequestBody int64
	// Subprotocols are the WebSocket subprotocols the gateway accepts from
	// clients and asks the upstream for; the handshake picks the first one a
	// client offers. Without any, no subprotocol is negotiated.
	Subprotocols []string
	// AltSvc, if set, is added to the responses of HTTP/1.1 and HTTP/2
	// requests, which is how a browser learns that HTTP/3 is here.
	AltSvc string
	// Logf, if set, is told about each request once it is done.
	Logf func(format string, args ...any)
}

// Gateway forwards requests to the upstream its routes choose.
type Gateway struct {
	config Config
	routes []Route
	engine *fib.Engine
	h1     *fibhttp.Client
	h3     *http3.Client
}

// New returns a gateway. It cannot forward anything until Attach gives it an
// engine to dial from, which is what lets the engine be built around the
// gateway's own handler first.
func New(config Config) (*Gateway, error) {
	if len(config.Routes) == 0 {
		return nil, errors.New("proxy: no routes")
	}
	if config.MaxRequestBody <= 0 {
		config.MaxRequestBody = 64 << 20
	}
	routes := append([]Route(nil), config.Routes...)
	// The longest prefix wins.
	sort.SliceStable(routes, func(i, j int) bool { return len(routes[i].Prefix) > len(routes[j].Prefix) })
	return &Gateway{config: config, routes: routes}, nil
}

// Attach makes the gateway dial its upstreams through engine, which should be
// one of the engines serving it, as docs/hacks.md recommends: the upstream
// clients then share the server's event loop and workers instead of bringing
// their own. It has to be called before the engine runs.
func (g *Gateway) Attach(engine *fib.Engine) {
	g.engine = engine
	clientConfig := fibhttp.DefaultClientConfig()
	clientConfig.Timeout = g.config.Timeout
	clientConfig.TLSConfig = g.config.TLSConfig
	// Responses stream to the callback as their header arrives, whatever their
	// size: that is what a gateway wants, and what keeps its memory flat.
	clientConfig.StreamResponseBody = true
	clientConfig.MaxConnsPerHost = 256
	clientConfig.MaxIdleConnsPerHost = 64
	g.h1 = fibhttp.NewClient(engine, clientConfig)

	h3Config := http3.DefaultClientConfig()
	h3Config.Timeout = g.config.Timeout
	h3Config.TLSConfig = g.config.TLSConfig
	h3Config.StreamResponseBody = true
	g.h3 = http3.NewClient(engine, h3Config)
}

// Close stops the upstream clients. Close them before the engine.
func (g *Gateway) Close() {
	if g.h1 != nil {
		g.h1.Close()
	}
	if g.h3 != nil {
		g.h3.Close()
	}
}

// HTTPConfig is the server configuration the gateway wants for its HTTP/1.1
// and HTTP/2 listeners: bodies over a threshold reach the handler while they
// are still arriving, so the gateway takes them with OnBody.
func (g *Gateway) HTTPConfig() fibhttp.Config {
	config := fibhttp.DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = streamThreshold
	config.MaxStreamedBodyBytes = g.config.MaxRequestBody
	return config
}

// HTTP3Config is HTTPConfig for the HTTP/3 listener.
func (g *Gateway) HTTP3Config(tlsConfig *tls.Config) http3.Config {
	config := http3.DefaultConfig()
	config.TLSConfig = tlsConfig
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = streamThreshold
	config.MaxStreamedBodyBytes = g.config.MaxRequestBody
	return config
}

// streamThreshold is the request body size from which the handler is run
// before the body is complete. Smaller bodies arrive with the request.
const streamThreshold = 64 << 10

// Handler is what the listeners serve: the same handler for HTTP/1.1,
// HTTP/2 and HTTP/3.
func (g *Gateway) Handler() fibhttp.HandlerFunc { return g.serve }

func (g *Gateway) serve(c *fibhttp.Context) {
	r := c.Request
	route := g.route(r.URL.Path)
	if route == nil {
		_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("no route\n"))
		return
	}
	if r.ProtoMajor < 3 && g.config.AltSvc != "" {
		c.Header().Set("Alt-Svc", g.config.AltSvc)
	}
	if isWebSocket(r) {
		g.serveWebSocket(c, route)
		return
	}
	g.serveHTTP(c, route)
}

func (g *Gateway) route(path string) *Route {
	for i := range g.routes {
		if g.routes[i].matches(path) {
			return &g.routes[i]
		}
	}
	return nil
}

func (g *Gateway) logf(format string, args ...any) {
	if g.config.Logf != nil {
		g.config.Logf(format, args...)
	}
}

// isWebSocket reports whether r asks to become a WebSocket: an HTTP/1.1
// upgrade, or the extended CONNECT of RFC 8441 and RFC 9220 on HTTP/2 and
// HTTP/3.
func isWebSocket(r *stdhttp.Request) bool {
	if r.ProtoMajor >= 2 {
		return r.Method == stdhttp.MethodConnect && strings.EqualFold(r.Header.Get(":protocol"), "websocket")
	}
	return r.Method == stdhttp.MethodGet && hasToken(r.Header, "Upgrade", "websocket") && hasToken(r.Header, "Connection", "upgrade")
}

func hasToken(h stdhttp.Header, name, token string) bool {
	for _, value := range h.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// hopByHop are the headers that describe one connection and not the message,
// which a proxy must not pass on (RFC 9110 section 7.6.1).
var hopByHop = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// copyHeader copies src to dst without the hop-by-hop headers, nor those the
// Connection header names, nor the pseudo-headers an HTTP/2 or HTTP/3 request
// may carry. skip lists further names to leave out.
func copyHeader(dst, src stdhttp.Header, skip ...string) {
	drop := make(map[string]bool, len(hopByHop)+len(skip)+2)
	for _, name := range hopByHop {
		drop[name] = true
	}
	for _, name := range skip {
		drop[stdhttp.CanonicalHeaderKey(name)] = true
	}
	for _, value := range src.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				drop[stdhttp.CanonicalHeaderKey(token)] = true
			}
		}
	}
	for name, values := range src {
		if drop[name] || strings.HasPrefix(name, ":") {
			continue
		}
		dst[name] = append(dst[name], values...)
	}
}

// addForwarded tells the upstream who asked, and over what.
func addForwarded(h stdhttp.Header, r *stdhttp.Request) {
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior := h.Get("X-Forwarded-For"); prior != "" {
			ip = prior + ", " + ip
		}
		h.Set("X-Forwarded-For", ip)
	}
	if h.Get("X-Forwarded-Proto") == "" {
		proto := "http"
		if r.TLS != nil {
			proto = "https"
		}
		h.Set("X-Forwarded-Proto", proto)
	}
	if h.Get("X-Forwarded-Host") == "" && r.Host != "" {
		h.Set("X-Forwarded-Host", r.Host)
	}
	h.Add("Via", fmt.Sprintf("%d.%d fib-gateway", r.ProtoMajor, r.ProtoMinor))
}

// failure maps an upstream error to the status the client is told.
func failure(err error) int {
	if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return stdhttp.StatusGatewayTimeout
	}
	return stdhttp.StatusBadGateway
}
