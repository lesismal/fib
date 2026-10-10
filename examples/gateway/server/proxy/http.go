//go:build linux || darwin || windows

package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"sync/atomic"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// doer is what both upstream clients, package http's and package http3's,
// offer: send a request, be called back with a response whose body can be
// taken as it arrives.
type doer interface {
	Do(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error))
}

func (g *Gateway) client(route *Route) doer {
	if route.Target.Scheme == "h3" {
		return g.h3
	}
	return g.h1
}

// serveHTTP forwards one request. The request body is collected without
// blocking: one that arrived with the request is used where it is, and a
// large one is taken with OnBody as its pieces come off the connection. When
// it is complete the upstream request goes out, and the handler is long
// gone: the response is written from the upstream's callbacks.
func (g *Gateway) serveHTTP(c *fibhttp.Context, route *Route) {
	r := c.Request
	if r.ContentLength > g.config.MaxRequestBody {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain; charset=utf-8", []byte("request body too large\n"))
		return
	}
	target := route.httpURL(r.URL)
	header := make(stdhttp.Header, len(r.Header)+4)
	copyHeader(header, r.Header, "Content-Length", "Expect")
	addForwarded(header, r)
	send := func(body []byte) { g.forward(c, route, r.Method, target, header, body) }

	if c.BodyComplete() {
		// Nothing more to wait for: no body, or one small enough to have come
		// with the request. Body is the server's own buffer, which the client
		// reads before Do returns.
		send(c.Body())
		return
	}
	var body []byte
	var rejected bool
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil || rejected {
			// The request failed, and the server has cancelled it.
			return
		}
		if int64(len(body)+len(data)) > g.config.MaxRequestBody {
			rejected = true
			_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain; charset=utf-8", []byte("request body too large\n"))
			return
		}
		body = append(body, data...)
		if fin {
			send(body)
		}
	})
}

// forward sends the upstream request and arranges for its response to be
// written to c from the callbacks.
func (g *Gateway) forward(c *fibhttp.Context, route *Route, method, target string, header stdhttp.Header, body []byte) {
	ctx, cancel := context.WithCancel(context.Background())
	var reader io.Reader
	if len(body) > 0 {
		reader = bytes.NewReader(body)
	}
	req, err := stdhttp.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		cancel()
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain; charset=utf-8", []byte(err.Error()+"\n"))
		return
	}
	req.Header = header
	x := &exchange{g: g, c: c, cancel: cancel, start: time.Now(), method: method, path: c.Request.URL.Path,
		proto: c.Request.Proto, http1: c.Request.ProtoMajor == 1}
	// The handler may return, or the body callback that called forward finish,
	// before the upstream answers. The Context is ours until x.release.
	c.Retain()
	// The downstream client going away cancels the upstream request, whose
	// callbacks then wind the exchange up.
	c.OnCancel(func(error) { cancel() })
	g.client(route).Do(req, x.onResponse)
}

// exchange is one proxied request, from its upstream callbacks. They arrive
// one at a time, so its fields need no lock; only done is shared, since the
// downstream going away can end the exchange from elsewhere.
type exchange struct {
	g      *Gateway
	c      *fibhttp.Context
	cancel context.CancelFunc
	start  time.Time

	method, path, proto string
	http1               bool

	resp      *fibhttp.ClientResponse
	started   bool
	streaming bool
	written   int64

	done atomic.Bool
}

// release gives the downstream request back, once, and drops the upstream's.
func (x *exchange) release() {
	if x.done.CompareAndSwap(false, true) {
		x.cancel()
		x.c.Release()
	}
}

func (x *exchange) onResponse(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		x.fail(err)
		return
	}
	x.resp = resp
	// Not begun yet, so that an upstream that fails before it sends a byte
	// of body can still be answered with a 502.
	x.streaming = !resp.BodyComplete()
	resp.OnBody(x.onBody)
}

// begin sends the response header downstream.
func (x *exchange) begin() {
	x.started = true
	h := x.c.Header()
	copyHeader(h, x.resp.Header, "Alt-Svc", "Date")
	if announced := x.resp.Header.Values("Trailer"); len(announced) > 0 {
		h["Trailer"] = announced
	}
	h.Add("Via", viaOf(x.resp))
	x.c.WriteHeader(x.resp.StatusCode)
}

func viaOf(resp *fibhttp.ClientResponse) string {
	return fmt.Sprintf("%d.%d fib-gateway", resp.ProtoMajor, resp.ProtoMinor)
}

// onBody writes what the upstream sends to the downstream response. It runs
// on the worker that reads the upstream connection and does not wait for
// anything: on HTTP/1.1 a write is queued, and on HTTP/2 and HTTP/3 it is
// paced by the client's flow control, which holds the upstream read back
// through the upstream stream's own flow control in turn.
func (x *exchange) onBody(data []byte, fin bool, err error) {
	if x.done.Load() {
		return
	}
	if err != nil {
		x.broken(err)
		return
	}
	if !x.started {
		x.begin()
	}
	if len(data) > 0 {
		n, werr := x.c.Write(data)
		x.written += int64(n)
		if werr != nil {
			// The downstream is gone. Stopping the upstream request ends the
			// callbacks, so release now.
			x.g.logf("%s %s %s: downstream write: %v", x.proto, x.method, x.path, werr)
			x.release()
			return
		}
		if x.streaming && !fin {
			x.c.Flush()
		}
	}
	if !fin {
		return
	}
	// Trailers go out after the body; the Trailer header announced their
	// names, and the prefix marks values set now.
	for name, values := range x.resp.Trailer() {
		for _, value := range values {
			x.c.Header().Add(stdhttp.TrailerPrefix+name, value)
		}
	}
	_ = x.c.Finish()
	x.g.logf("%s %s %s -> %d (%s upstream, %d bytes, %v)", x.proto, x.method, x.path,
		x.resp.StatusCode, x.resp.Proto, x.written, time.Since(x.start).Round(time.Millisecond))
	x.release()
}

// fail answers a request whose upstream never produced a response.
func (x *exchange) fail(err error) {
	if errors.Is(err, context.Canceled) {
		// The downstream client left; nobody is waiting for an answer.
		x.release()
		return
	}
	status := failure(err)
	x.g.logf("%s %s %s -> %d: %v", x.proto, x.method, x.path, status, err)
	_ = x.c.Respond(status, "text/plain; charset=utf-8", []byte(stdhttp.StatusText(status)+": "+err.Error()+"\n"))
	x.release()
}

// broken handles an upstream that failed after its response began.
func (x *exchange) broken(err error) {
	if errors.Is(err, context.Canceled) {
		x.release()
		return
	}
	if !x.started {
		// Nothing has gone downstream yet, so it is an ordinary failure.
		x.fail(err)
		return
	}
	x.g.logf("%s %s %s: upstream failed mid-body: %v", x.proto, x.method, x.path, err)
	if x.http1 {
		// A chunked or length-delimited response that stops short can only be
		// told apart from a complete one by closing the connection.
		x.c.Conn.Close()
	} else {
		// A stream ends cleanly or not at all; this one ends with a trailer
		// that says what happened.
		x.c.Header().Set(stdhttp.TrailerPrefix+"X-Gateway-Error", err.Error())
		_ = x.c.Finish()
	}
	x.release()
}
