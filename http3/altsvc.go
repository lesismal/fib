//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// AltSvcClient sends a request over HTTP/3 to an origin that advertised it
// with Alt-Svc (RFC 7838), and over HTTP/1.1 or HTTP/2 through an
// *fibhttp.Client otherwise. The first request to an origin goes over TCP; a
// response carrying "Alt-Svc: h3=..." has the requests after it, to the same
// origin, sent over HTTP/3, to the address the header names, with the
// origin's name still what the server's certificate is verified for and what
// the request's :authority carries.
//
// When the QUIC attempt fails, a request that is safe to repeat is sent again
// over TCP, and the origin stays on TCP for a while, so that a network that
// blocks UDP costs one timeout rather than one per request. What a request is
// bounded by is the client that carries it: *fibhttp.ClientConfig for a
// request over TCP, *http3.ClientConfig for one over HTTP/3.
type AltSvcClient struct {
	tcp *fibhttp.Client
	h3  *Client

	mu   sync.Mutex
	alts map[string]altSvcEntry
}

// altSvcEntry is where one origin's HTTP/3 requests go.
type altSvcEntry struct {
	addr, serverName string
	// until is when the advertisement's max-age runs out; retryAt is when an
	// origin whose HTTP/3 attempt failed is tried again.
	until   time.Time
	retryAt time.Time
}

// altSvcBackoff is how long an origin whose HTTP/3 attempt failed is left on
// TCP, so that a QUIC handshake that never completes is paid once rather
// than on every request.
const altSvcBackoff = 30 * time.Second

// NewAltSvcClient returns a client that sends requests over h3 to the origins
// it advertises with Alt-Svc, and over tcp to the rest. tcp and h3 are the
// caller's: Close stops both. h3's TLSConfig should leave ServerName empty, so
// that each origin's own name is what its certificate is verified for.
func NewAltSvcClient(tcp *fibhttp.Client, h3 *Client) *AltSvcClient {
	return &AltSvcClient{tcp: tcp, h3: h3, alts: make(map[string]altSvcEntry)}
}

// Do sends req as the client it is for, and calls callback exactly once with
// its response or the reason there is none.
func (a *AltSvcClient) Do(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error)) {
	a.start(req, callback, false, nil)
}

// Start is Do, and returns an AltSvcCall that abandons the request.
func (a *AltSvcClient) Start(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error)) AltSvcCall {
	c := &altSvcCall{}
	a.start(req, callback, false, c)
	return AltSvcCall{c: c}
}

// StartUpgrade is *fibhttp.Client.StartUpgrade over TCP, which is the only one
// of the two clients that can switch a connection to another protocol.
func (a *AltSvcClient) StartUpgrade(req *stdhttp.Request, handler fibhttp.TunnelHandler,
	callback func(*fibhttp.ClientResponse, error)) fibhttp.Call {
	return a.tcp.StartUpgrade(req, handler, callback)
}

// Go sends req like Do and returns a Future for its outcome, which arrives
// whole however the h3 client is configured, as *fibhttp.Client.Go does.
func (a *AltSvcClient) Go(req *stdhttp.Request) *AltSvcFuture {
	f := &AltSvcFuture{done: make(chan struct{})}
	a.start(req, func(resp *fibhttp.ClientResponse, err error) { a.finishFuture(f, resp, err) }, true, nil)
	return f
}

// Close stops both clients: the TCP one, and the HTTP/3 one.
func (a *AltSvcClient) Close() {
	if a.h3 != nil {
		a.h3.Close()
	}
	if a.tcp != nil {
		a.tcp.Close()
	}
}

// AltSvcFuture is the outcome of AltSvcClient.Go.
type AltSvcFuture struct {
	done     chan struct{}
	resp     *stdhttp.Response
	err      error
	finished sync.Once
}

// Wait blocks until the response has arrived or the request has failed.
func (f *AltSvcFuture) Wait() (*stdhttp.Response, error) {
	<-f.done
	return f.resp, f.err
}

// Done is closed once Wait would no longer block.
func (f *AltSvcFuture) Done() <-chan struct{} { return f.done }

func (a *AltSvcClient) finishFuture(f *AltSvcFuture, resp *fibhttp.ClientResponse, err error) {
	f.finished.Do(func() {
		if resp != nil {
			f.resp = resp.Response
		}
		f.err = err
		close(f.done)
	})
}

// AltSvcCall is a request AltSvcClient.Start sent, for abandoning it.
type AltSvcCall struct{ c *altSvcCall }

// Cancel abandons the request, as cancelling its context would. It may be
// called from any goroutine, any number of times; once the request has been
// settled it does nothing, as does the zero AltSvcCall.
func (c AltSvcCall) Cancel(err error) {
	if c.c == nil {
		return
	}
	c.c.abandon(err)
}

// altSvcCall is the abandonment shared by a request that may still change
// clients: one whose HTTP/3 attempt failed is sent over TCP, and a Cancel
// reaches whichever of the two is carrying it then.
type altSvcCall struct {
	mu     sync.Mutex
	cancel func(error)
	// canceled records a Cancel that came before there was anything to
	// cancel; err is what it carried.
	canceled bool
	err      error
}

func (c *altSvcCall) hold(cancel func(error)) {
	c.mu.Lock()
	if c.canceled {
		err := c.err
		c.mu.Unlock()
		cancel(err)
		return
	}
	c.cancel = cancel
	c.mu.Unlock()
}

// ended reports whether Cancel has been called, which means the h3 client's
// callback was the cancellation and must not be turned into a TCP fallback.
func (c *altSvcCall) ended() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.canceled
}

func (c *altSvcCall) abandon(err error) {
	if err == nil {
		err = fibhttp.ErrClientClosed
	}
	c.mu.Lock()
	c.canceled, c.err = true, err
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel(err)
	}
}

// start sends req as the client it is for. call, when not nil, is where a
// Cancel reaches the request, and is moved on if it falls back to TCP.
func (a *AltSvcClient) start(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error), whole bool, call *altSvcCall) {
	if a.tcp == nil {
		callback(nil, fibhttp.ErrClientClosed)
		return
	}
	if a.h3 == nil {
		a.doTCP(req, callback, whole, call)
		return
	}
	if entry, ok := a.route(req); ok {
		// A request the h3 client rejects outright calls the callback before
		// do returns, so the fallback has to be able to say it took the
		// request over before the h3 client's Call is what a Cancel reaches.
		var fellBack atomic.Bool
		c := a.h3.do(req, func(resp *fibhttp.ClientResponse, err error) {
			if resp == nil && err != nil && (call == nil || !call.ended()) {
				// The QUIC attempt failed, so this origin goes back to TCP for
				// a while, and a request that is safe to repeat goes over TCP
				// at once: a network that blocks UDP pays for one attempt
				// rather than a handshake timeout per request.
				key := originKey(req)
				a.mu.Lock()
				if e, ok := a.alts[key]; ok {
					e.retryAt = time.Now().Add(altSvcBackoff)
					a.alts[key] = e
				}
				a.mu.Unlock()
				if a.fallback(req) {
					fellBack.Store(true)
					a.doTCP(req, callback, whole, call)
					return
				}
			}
			a.delivered(req, resp)
			callback(resp, err)
		}, whole, altTarget{addr: entry.addr, serverName: entry.serverName})
		if call != nil && !fellBack.Load() {
			call.hold(func(err error) { Call{r: c}.Cancel(err) })
		}
		return
	}
	a.doTCP(req, callback, whole, call)
}

// doTCP sends req through the TCP client.
func (a *AltSvcClient) doTCP(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error), whole bool, call *altSvcCall) {
	wrapped := func(resp *fibhttp.ClientResponse, err error) {
		a.delivered(req, resp)
		callback(resp, err)
	}
	if call == nil {
		if whole {
			// Go wants the whole response whatever the client's config says,
			// and only tcp.Go asks for that; its Future is waited on here so
			// that the outcome still reaches the caller's callback.
			wait := a.tcp.Go(req)
			go func() {
				resp, err := wait.Wait()
				if err != nil {
					wrapped(nil, err)
					return
				}
				wrapped(&fibhttp.ClientResponse{Response: resp}, nil)
			}()
			return
		}
		a.tcp.Do(req, wrapped)
		return
	}
	c := a.tcp.Start(req, wrapped)
	call.hold(c.Cancel)
}

// fallback reports whether a request whose HTTP/3 attempt failed may be sent
// over TCP, which duplicates a request that may have reached the server: only
// one that is safe to repeat, with a body that can be read again, is.
func (a *AltSvcClient) fallback(req *stdhttp.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}
	if req.Body != nil && req.Body != stdhttp.NoBody {
		if req.GetBody == nil {
			return false
		}
		body, err := req.GetBody()
		if err != nil {
			return false
		}
		req.Body = body
	}
	switch req.Method {
	case "", stdhttp.MethodGet, stdhttp.MethodHead, stdhttp.MethodOptions, stdhttp.MethodTrace:
		return true
	}
	return false
}

// delivered remembers an Alt-Svc header that came with resp.
func (a *AltSvcClient) delivered(req *stdhttp.Request, resp *fibhttp.ClientResponse) {
	if resp == nil || resp.Response == nil {
		return
	}
	a.remember(req, resp.Header)
}

// route reports where req should go, when it goes over HTTP/3.
func (a *AltSvcClient) route(req *stdhttp.Request) (altSvcEntry, bool) {
	if a.h3 == nil {
		return altSvcEntry{}, false
	}
	key := originKey(req)
	if key == "" {
		return altSvcEntry{}, false
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.alts[key]
	if !ok || !now.Before(e.until) || now.Before(e.retryAt) {
		return altSvcEntry{}, false
	}
	return e, true
}

// originKey is the origin req is for, as an Alt-Svc cache keys it, or "" for
// one HTTP/3 cannot serve.
func originKey(req *stdhttp.Request) string {
	if req == nil || req.URL == nil || req.URL.Scheme != "https" {
		return ""
	}
	host := req.URL.Hostname()
	if host == "" {
		return ""
	}
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	return "https://" + net.JoinHostPort(host, port)
}

// remember takes the Alt-Svc headers of a response to req, keeping the h3
// alternative that applies to req's origin. It is only called for a secure
// origin: an alternative service for one travels over TLS too.
func (a *AltSvcClient) remember(req *stdhttp.Request, header stdhttp.Header) {
	key := originKey(req)
	if key == "" {
		return
	}
	values := header.Values("Alt-Svc")
	if len(values) == 0 {
		return
	}
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if strings.EqualFold(part, "clear") {
				delete(a.alts, key)
				continue
			}
			protocol, authority, params, ok := parseAltSvc(part)
			if !ok || protocol != "h3" {
				continue
			}
			maxAge := altSvcMaxAge(params)
			if maxAge <= 0 {
				delete(a.alts, key)
				continue
			}
			addr, ok := altSvcAddr(authority, req.URL.Hostname())
			if !ok {
				continue
			}
			e := altSvcEntry{addr: addr, serverName: req.URL.Hostname(), until: now.Add(maxAge)}
			if old, ok := a.alts[key]; ok {
				// A failure's backoff outlives a fresh advertisement, so that
				// re-advertising an alternative that does not work does not
				// make every request pay for it again.
				e.retryAt = old.retryAt
			}
			a.alts[key] = e
		}
	}
}

// parseAltSvc splits one alternative: protocol="authority"; params.
func parseAltSvc(value string) (protocol, authority, params string, ok bool) {
	eq := strings.IndexByte(value, '=')
	if eq <= 0 {
		return "", "", "", false
	}
	protocol = strings.ToLower(strings.TrimSpace(value[:eq]))
	rest := strings.TrimSpace(value[eq+1:])
	if len(rest) < 2 || rest[0] != '"' {
		return "", "", "", false
	}
	end := strings.IndexByte(rest[1:], '"')
	if end < 0 {
		return "", "", "", false
	}
	authority = rest[1 : 1+end]
	params = rest[1+end+1:]
	return protocol, authority, params, true
}

// altSvcMaxAge is the ma parameter of an alternative, in seconds, or the
// default of a day when it has none.
func altSvcMaxAge(params string) time.Duration {
	for _, param := range strings.Split(params, ";") {
		param = strings.TrimSpace(param)
		name, value, ok := strings.Cut(param, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "ma") {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || n < 0 {
			return 0
		}
		return time.Duration(n) * time.Second
	}
	return 24 * time.Hour
}

// altSvcAddr is the address an alternative's authority names, with an empty
// host taken as the origin's, or false when it names no port to dial.
func altSvcAddr(authority, originHost string) (string, bool) {
	host, port := "", ""
	switch {
	case strings.HasPrefix(authority, ":"):
		host, port = originHost, authority[1:]
	case strings.Contains(authority, ":"):
		var err error
		host, port, err = net.SplitHostPort(authority)
		if err != nil {
			return "", false
		}
	default:
		return "", false
	}
	if host == "" {
		host = originHost
	}
	if port == "" {
		return "", false
	}
	return net.JoinHostPort(host, port), true
}
