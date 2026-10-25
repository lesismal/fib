//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"strings"
)

// Server push (RFC 9113 section 8.4): the server promises a request the client
// has not made yet, on a stream of its own, and answers it there, so that the
// response is on its way before the client would have asked for it.

var (
	// ErrPushLimit is what Push returns when the client already has as many
	// pushed streams open as its SETTINGS_MAX_CONCURRENT_STREAMS allows.
	ErrPushLimit = errors.New("http2: push would exceed the client's concurrent stream limit")
	// errPushAfterResponse is what Push returns once the response it would
	// accompany has been sent in full.
	errPushAfterResponse = errors.New("http2: push after the response was sent")
)

// h2PushForbiddenHeaders are fields a promised request may not carry: it has
// no body, and the rest are the connection's own business.
var h2PushForbiddenHeaders = map[string]bool{
	"content-length":    true,
	"content-encoding":  true,
	"trailer":           true,
	"te":                true,
	"expect":            true,
	"host":              true,
	"connection":        true,
	"keep-alive":        true,
	"proxy-connection":  true,
	"transfer-encoding": true,
	"upgrade":           true,
}

// push promises target on st and serves the promised request through the
// handler before returning.
func (st *h2ServerStream) push(parent *stdhttp.Request, target string, opts *stdhttp.PushOptions, reader bool) error {
	if st.pushed {
		// Promises ride on streams the client opened, never on pushed ones.
		return stdhttp.ErrNotSupported
	}
	sc := st.sc
	method, header := stdhttp.MethodGet, stdhttp.Header(nil)
	if opts != nil {
		if opts.Method != "" {
			method = opts.Method
		}
		header = opts.Header
	}
	if method != stdhttp.MethodGet && method != stdhttp.MethodHead {
		return errors.New("http2: push method must be GET or HEAD, not " + method)
	}
	scheme := "http"
	if sc.tlsState != nil {
		scheme = "https"
	}
	authority, path := parent.Host, target
	if !strings.HasPrefix(target, "/") {
		u, err := url.Parse(target)
		if err != nil || u.Scheme != scheme || u.Host == "" {
			return errors.New("http2: push target must be an absolute path or a " + scheme + " URL: " + target)
		}
		authority, path = u.Host, u.RequestURI()
	}
	u, err := url.ParseRequestURI(path)
	if err != nil {
		return err
	}
	for key := range header {
		if h2PushForbiddenHeaders[strings.ToLower(key)] {
			return errors.New("http2: promised request may not carry " + key)
		}
	}
	if err = h2CheckHeader(header); err != nil {
		return err
	}
	sc.mu.Lock()
	switch {
	case !sc.pushEnabled:
		sc.mu.Unlock()
		return stdhttp.ErrNotSupported
	case sc.closed || sc.goAway || sc.peerGoingAway || st.reset:
		sc.mu.Unlock()
		return errH2StreamClosed
	case st.localDone:
		sc.mu.Unlock()
		return errPushAfterResponse
	case sc.pushedStreams >= sc.peerMaxPush || sc.nextPushID > h2MaxStreamID:
		sc.mu.Unlock()
		return ErrPushLimit
	case sc.limitsHeaderListLocked() && h2FieldSize(":method", method)+h2FieldSize(":scheme", scheme)+
		h2FieldSize(":authority", authority)+h2FieldSize(":path", path)+h2HeaderSize(header) > sc.peerMaxHeaderList:
		// The promised request is a header list the client takes too.
		sc.mu.Unlock()
		return errH2HeaderListTooLarge
	}
	id := sc.nextPushID
	sc.nextPushID += 2
	block := sc.enc.Begin(nil)
	block = sc.enc.AppendField(block, ":method", method, false)
	block = sc.enc.AppendField(block, ":scheme", scheme, false)
	block = sc.enc.AppendField(block, ":authority", authority, false)
	block = sc.enc.AppendField(block, ":path", path, false)
	block = sc.appendHeaderLocked(block, header)
	promised := sc.newStream(id)
	promised.pushed, promised.remoteDone = true, true
	// The Context that serves it takes a reference of its own before the
	// promise is sent, so that a client resetting the stream straight away
	// leaves it to the handler rather than recycling it under the handler.
	promised.refs.Add(1)
	sc.streams[id] = promised
	sc.pushedStreams++
	sc.busyLocked()
	sc.sendLocked(h2AppendPushPromise(nil, st.id, id, block, sc.peerMaxFrame))
	sc.mu.Unlock()

	// The promised request is the stream's own, as a client's request is, so
	// that a recycled stream hands its Request, URL and Context to the next
	// push rather than allocating others.
	req := &promised.block.Request
	*req = stdhttp.Request{
		Method:     method,
		Proto:      "HTTP/2.0",
		ProtoMajor: 2,
		Host:       authority,
		RemoteAddr: sc.remoteAddr,
		RequestURI: path,
		TLS:        sc.tlsState,
		Body:       stdhttp.NoBody,
	}
	promised.block.URL = *u
	req.URL = &promised.block.URL
	switch {
	case promised.pooled && sc.reuseHeaders:
		pooled := headerPool.Get().(*pooledHeader)
		promised.headerFromPool = pooled
		req.Header = pooled.header
	default:
		req.Header = make(stdhttp.Header, len(header))
	}
	for key, values := range header {
		key = textproto.CanonicalMIMEHeaderKey(key)
		req.Header[key] = append(req.Header[key], values...)
	}
	promised.req = req
	c := promised.block.bind(sc.conn, nil)
	c.stream = promised
	sc.armWriteTimeout(promised)

	// Its handler runs here, on the goroutine of the one pushing it, which
	// may be the one reading the connection.
	serveRequestOn(sc.handler.handler, c, reader)
	return nil
}
