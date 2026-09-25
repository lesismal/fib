//go:build linux || darwin || windows

package http3

// The conformance suite runs the fib HTTP/3 server against a quic-go client
// and the fib HTTP/3 client against a quic-go server, through the peer in
// ./interop. That peer is a module of its own, so quic-go never becomes a
// dependency of fib; the tests build it as a program and drive it over a
// pipe with one JSON object per line.
//
// Both sides answer the same requests: the query string says what the
// response should hold, and the response reports what the request held, so
// each case checks the same ground in both directions.

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	stdhttp "net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/internal/tlstest"
)

// hashBody is what both sides report a body as, so that large ones are
// checked without being carried around.
func hashBody(b []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// filler is the deterministic body both sides generate.
func filler(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

// conformanceHandler answers what the peer's handler answers, so that a
// case reads the same whichever side serves it. The query decides the
// response: status, size, echo, trailer, header, interim and delay.
func conformanceHandler(c *fibhttp.Context, r *stdhttp.Request) {
	query := r.URL.Query()
	if ms, _ := strconv.Atoi(query.Get("delay")); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	body, _ := io.ReadAll(r.Body)
	header := stdhttp.Header{
		"X-Method":    {r.Method},
		"X-Path":      {r.URL.RequestURI()},
		"X-Proto":     {r.Proto},
		"X-Host":      {r.Host},
		"X-Body-Len":  {strconv.Itoa(len(body))},
		"X-Body-Hash": {hashBody(body)},
	}
	for key, values := range r.Trailer {
		header["X-Req-Trailer-"+key] = values
	}
	for key, values := range r.Header {
		if strings.HasPrefix(key, "X-Echo-") {
			header[key] = values
		}
	}
	if v := query.Get("header"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, value, ok := strings.Cut(pair, ":"); ok {
				header.Add(name, value)
			}
		}
	}
	if query.Get("interim") != "" {
		_ = c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {"</style.css>; rel=preload"}})
	}
	status := stdhttp.StatusOK
	if v, err := strconv.Atoi(query.Get("status")); err == nil && v >= 200 {
		status = v
	}
	response := fibhttp.Response{StatusCode: status, Header: header}
	switch size, _ := strconv.Atoi(query.Get("size")); {
	case size > 0:
		response.Body = filler(size)
	case query.Get("echo") != "":
		response.Body = body
	}
	if v := query.Get("trailer"); v != "" {
		response.Trailer = make(stdhttp.Header)
		var names []string
		for _, pair := range strings.Split(v, ",") {
			if name, value, ok := strings.Cut(pair, ":"); ok {
				response.Trailer.Set(name, value)
				names = append(names, stdhttp.CanonicalHeaderKey(name))
			}
		}
		header["Trailer"] = names
	}
	_ = c.WriteResponse(response)
}

// interopSpec is a request for the peer's client, and interopResult what
// comes back; they match the peer's own JSON.
type interopSpec struct {
	Method   string              `json:"method,omitempty"`
	Path     string              `json:"path,omitempty"`
	Header   map[string][]string `json:"header,omitempty"`
	Trailer  map[string][]string `json:"trailer,omitempty"`
	Body     string              `json:"body,omitempty"`
	BodySize int                 `json:"bodySize,omitempty"`
	Count    int                 `json:"count,omitempty"`
	Command  string              `json:"command,omitempty"`
}

type interopResult struct {
	Status   int                 `json:"status"`
	Proto    string              `json:"proto"`
	Header   map[string][]string `json:"header"`
	Trailer  map[string][]string `json:"trailer"`
	Body     string              `json:"body"`
	BodyLen  int                 `json:"bodyLen"`
	BodyHash string              `json:"bodyHash"`
	Count    int                 `json:"count"`
	Error    string              `json:"error"`
}

func (r interopResult) header(name string) string {
	if values := r.Header[stdhttp.CanonicalHeaderKey(name)]; len(values) > 0 {
		return values[0]
	}
	return ""
}

var (
	interopOnce sync.Once
	interopPath string
	interopErr  error
)

// interopBinary builds the peer once for the whole test binary. Building it
// downloads quic-go into the module cache, so a machine that cannot reach
// the proxy skips these tests unless FIB_REQUIRE_H3_INTEROP says otherwise,
// as CI does.
func interopBinary(t *testing.T) string {
	t.Helper()
	interopOnce.Do(func() {
		dir, err := os.MkdirTemp("", "fib-http3-interop")
		if err != nil {
			interopErr = err
			return
		}
		out := filepath.Join(dir, "interop")
		if runtime.GOOS == "windows" {
			out += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", out, ".")
		cmd.Dir = "interop"
		if output, err := cmd.CombinedOutput(); err != nil {
			interopErr = fmt.Errorf("building the interop peer: %w\n%s", err, output)
			return
		}
		interopPath = out
	})
	if interopErr != nil {
		if os.Getenv("FIB_REQUIRE_H3_INTEROP") != "" {
			t.Fatalf("the interop peer is required: %v", interopErr)
		}
		t.Skipf("skipping: %v", interopErr)
	}
	return interopPath
}

// writePEM writes the test certificate and its key where the peer, which
// takes files, can read them, and returns the certificate and key paths.
func writePEM(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	cert := serverTLS.Certificates[0]
	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("unexpected key type %T", cert.PrivateKey)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// peer is the interop program, running as either side, driven over its
// standard input and output.
type peer struct {
	t       *testing.T
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  *bufio.Scanner
	stderr  *bytes.Buffer
	mu      sync.Mutex
	stopped bool
}

func startPeer(t *testing.T, args ...string) *peer {
	t.Helper()
	cmd := exec.Command(interopBinary(t), args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &peer{t: t, cmd: cmd, stdin: stdin, stderr: &bytes.Buffer{}}
	cmd.Stderr = p.stderr
	p.stdout = bufio.NewScanner(stdout)
	p.stdout.Buffer(make([]byte, 1<<20), 8<<20)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.stop)
	return p
}

func (p *peer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return
	}
	p.stopped = true
	_ = p.stdin.Close()
	done := make(chan struct{})
	go func() { _ = p.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
}

// do sends one specification and returns what the peer answers.
func (p *peer) do(spec interopSpec) interopResult {
	p.t.Helper()
	line, err := json.Marshal(spec)
	if err != nil {
		p.t.Fatal(err)
	}
	if _, err := p.stdin.Write(append(line, '\n')); err != nil {
		p.t.Fatalf("writing to the peer: %v (stderr: %s)", err, p.stderr)
	}
	if !p.stdout.Scan() {
		err := p.stdout.Err()
		p.t.Fatalf("the peer said nothing: %v (stderr: %s)", err, p.stderr)
	}
	var result interopResult
	if err := json.Unmarshal(p.stdout.Bytes(), &result); err != nil {
		p.t.Fatalf("the peer said %q: %v", p.stdout.Text(), err)
	}
	return result
}

// ok is do for a request that has to succeed with status 200.
func (p *peer) ok(spec interopSpec) interopResult {
	p.t.Helper()
	result := p.do(spec)
	if result.Error != "" {
		p.t.Fatalf("%s %s: %s", spec.Method, spec.Path, result.Error)
	}
	if result.Status != stdhttp.StatusOK {
		p.t.Fatalf("%s %s: status %d", spec.Method, spec.Path, result.Status)
	}
	return result
}

// startPeerServer starts the peer's HTTP/3 server and returns its base URL.
func startPeerServer(t *testing.T, args ...string) (*peer, string) {
	t.Helper()
	certFile, keyFile := writePEM(t)
	args = append([]string{"-mode", "server", "-cert", certFile, "-key", keyFile, "-addr", "127.0.0.1:0"}, args...)
	p := startPeer(t, args...)
	if !p.stdout.Scan() {
		t.Fatalf("the peer server did not start: %v", p.stderr)
	}
	line := p.stdout.Text()
	addr, ok := strings.CutPrefix(line, "listening ")
	if !ok {
		t.Fatalf("the peer server said %q", line)
	}
	port := addr[strings.LastIndex(addr, ":")+1:]
	return p, "https://localhost:" + port
}

// TestHTTP3ConformanceServer checks the fib server against a quic-go
// client: every case says what it wants in the query and checks both what
// came back and what the server saw.
func TestHTTP3ConformanceServer(t *testing.T) {
	certFile, _ := writePEM(t)
	url := startServer(t, Config{MaxBodyBytes: 8 << 20, MaxHeaderBytes: 16 << 10}, conformanceHandler)
	p := startPeer(t, "-mode", "client", "-url", url, "-ca", certFile)

	t.Run("GET", func(t *testing.T) {
		got := p.ok(interopSpec{Path: "/hello?echo=1"})
		if got.Proto != "HTTP/3.0" || got.header("X-Proto") != "HTTP/3.0" {
			t.Fatalf("proto %q, server saw %q", got.Proto, got.header("X-Proto"))
		}
		if got.header("X-Method") != "GET" || got.header("X-Path") != "/hello?echo=1" {
			t.Fatalf("server saw %s %s", got.header("X-Method"), got.header("X-Path"))
		}
		if got.header("X-Host") != strings.TrimPrefix(url, "https://") {
			t.Fatalf("authority %q", got.header("X-Host"))
		}
	})

	t.Run("POST", func(t *testing.T) {
		body := "the quick brown fox"
		got := p.ok(interopSpec{Method: "POST", Path: "/echo?echo=1", Body: body})
		if got.Body != body || got.header("X-Body-Hash") != hashBody([]byte(body)) {
			t.Fatalf("body %q, hash %s", got.Body, got.header("X-Body-Hash"))
		}
	})

	t.Run("RequestHeaders", func(t *testing.T) {
		got := p.ok(interopSpec{Path: "/h", Header: map[string][]string{
			"X-Echo-One": {"first", "second"},
			"Cookie":     {"a=1", "b=2"},
		}})
		if values := got.Header["X-Echo-One"]; len(values) != 2 || values[0] != "first" || values[1] != "second" {
			t.Fatalf("echoed headers %v", got.Header["X-Echo-One"])
		}
	})

	t.Run("ResponseHeaders", func(t *testing.T) {
		got := p.ok(interopSpec{Path: "/r?header=Set-Cookie:a%3D1,Set-Cookie:b%3D2,X-One:two"})
		if values := got.Header["Set-Cookie"]; len(values) != 2 {
			t.Fatalf("Set-Cookie %v", values)
		}
		if got.header("X-One") != "two" {
			t.Fatalf("X-One %q", got.header("X-One"))
		}
	})

	t.Run("LargeUpload", func(t *testing.T) {
		const size = 4 << 20
		got := p.ok(interopSpec{Method: "PUT", Path: "/up", BodySize: size})
		if got.header("X-Body-Len") != strconv.Itoa(size) || got.header("X-Body-Hash") != hashBody(filler(size)) {
			t.Fatalf("server saw %s bytes, hash %s", got.header("X-Body-Len"), got.header("X-Body-Hash"))
		}
	})

	t.Run("LargeDownload", func(t *testing.T) {
		const size = 4 << 20
		got := p.ok(interopSpec{Path: "/down?size=" + strconv.Itoa(size)})
		if got.BodyLen != size || got.BodyHash != hashBody(filler(size)) {
			t.Fatalf("got %d bytes, hash %s", got.BodyLen, got.BodyHash)
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		got := p.ok(interopSpec{Path: "/c?size=4096", Count: 50})
		if got.Count != 50 || got.BodyLen != 4096 {
			t.Fatalf("%d requests, last %d bytes", got.Count, got.BodyLen)
		}
	})

	t.Run("HEAD", func(t *testing.T) {
		got := p.ok(interopSpec{Method: "HEAD", Path: "/h?size=1024"})
		if got.BodyLen != 0 || got.header("Content-Length") != "1024" {
			t.Fatalf("HEAD body %d, content-length %q", got.BodyLen, got.header("Content-Length"))
		}
	})

	t.Run("NoContent", func(t *testing.T) {
		got := p.do(interopSpec{Path: "/n?status=204&size=16"})
		if got.Status != stdhttp.StatusNoContent || got.BodyLen != 0 || got.header("Content-Length") != "" {
			t.Fatalf("status %d, body %d, content-length %q", got.Status, got.BodyLen, got.header("Content-Length"))
		}
	})

	t.Run("ResponseTrailers", func(t *testing.T) {
		got := p.ok(interopSpec{Path: "/t?echo=1&trailer=X-Sum:42", Body: "x"})
		if values := got.Trailer[stdhttp.CanonicalHeaderKey("X-Sum")]; len(values) != 1 || values[0] != "42" {
			t.Fatalf("trailers %v", got.Trailer)
		}
	})

	t.Run("RequestTrailers", func(t *testing.T) {
		got := p.ok(interopSpec{Method: "POST", Path: "/rt", Body: "body",
			Trailer: map[string][]string{"X-Check": {"done"}}})
		if got.header("X-Req-Trailer-X-Check") != "done" {
			t.Fatalf("the server saw trailers %v", got.Header)
		}
	})

	t.Run("InterimResponse", func(t *testing.T) {
		got := p.ok(interopSpec{Path: "/i?interim=1&echo=1", Body: "after hints"})
		if got.Body != "after hints" {
			t.Fatalf("body %q", got.Body)
		}
	})

	t.Run("Continue", func(t *testing.T) {
		got := p.ok(interopSpec{Method: "POST", Path: "/expect?echo=1", Body: "with expect",
			Header: map[string][]string{"Expect": {"100-continue"}}})
		if got.Body != "with expect" {
			t.Fatalf("body %q", got.Body)
		}
	})

	t.Run("ExpectationFailed", func(t *testing.T) {
		got := p.do(interopSpec{Method: "POST", Path: "/expect", Body: "x",
			Header: map[string][]string{"Expect": {"something-else"}}})
		if got.Status != stdhttp.StatusExpectationFailed {
			t.Fatalf("status %d (%s)", got.Status, got.Error)
		}
	})

	t.Run("BodyTooLarge", func(t *testing.T) {
		got := p.do(interopSpec{Method: "POST", Path: "/big", BodySize: 9 << 20})
		if got.Status != stdhttp.StatusRequestEntityTooLarge {
			t.Fatalf("status %d (%s)", got.Status, got.Error)
		}
	})

	t.Run("HeaderTooLarge", func(t *testing.T) {
		got := p.do(interopSpec{Path: "/hdr", Header: map[string][]string{
			"X-Echo-Big": {strings.Repeat("v", 20<<10)},
		}})
		if got.Status != stdhttp.StatusRequestHeaderFieldsTooLarge && got.Error == "" {
			t.Fatalf("status %d", got.Status)
		}
	})

	t.Run("StatusCodes", func(t *testing.T) {
		for _, status := range []int{201, 301, 404, 418, 500} {
			got := p.do(interopSpec{Path: "/s?status=" + strconv.Itoa(status)})
			if got.Status != status {
				t.Fatalf("status %d, want %d", got.Status, status)
			}
		}
	})

	t.Run("StillServing", func(t *testing.T) {
		// Everything above left the connection usable.
		got := p.ok(interopSpec{Path: "/last?echo=1", Body: "alive"})
		if got.Body != "alive" {
			t.Fatalf("body %q", got.Body)
		}
	})
}

// TestHTTP3ConformanceClient checks the fib client against a quic-go
// server.
func TestHTTP3ConformanceClient(t *testing.T) {
	_, url := startPeerServer(t)
	client := newClient(t, nil)

	get := func(t *testing.T, path string) *stdhttp.Response {
		t.Helper()
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+path, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	t.Run("GET", func(t *testing.T) {
		resp := get(t, "/hello?echo=1")
		if resp.ProtoMajor != 3 || resp.Header.Get("X-Proto") != "HTTP/3.0" {
			t.Fatalf("proto %s, server saw %q", resp.Proto, resp.Header.Get("X-Proto"))
		}
		if resp.TLS == nil || resp.TLS.NegotiatedProtocol != NextProto {
			t.Fatalf("TLS state %+v", resp.TLS)
		}
		if resp.Header.Get("X-Path") != "/hello?echo=1" {
			t.Fatalf("server saw %q", resp.Header.Get("X-Path"))
		}
	})

	t.Run("POST", func(t *testing.T) {
		body := "the quick brown fox"
		resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, url+"/echo?echo=1", strings.NewReader(body))).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != body {
			t.Fatalf("echo %q", got)
		}
		if resp.Header.Get("X-Body-Hash") != hashBody([]byte(body)) {
			t.Fatalf("server hashed %q", resp.Header.Get("X-Body-Hash"))
		}
	})

	t.Run("LargeUpload", func(t *testing.T) {
		const size = 4 << 20
		payload := filler(size)
		resp, err := client.Go(mustRequest(t, stdhttp.MethodPut, url+"/up", bytes.NewReader(payload))).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if resp.Header.Get("X-Body-Len") != strconv.Itoa(size) || resp.Header.Get("X-Body-Hash") != hashBody(payload) {
			t.Fatalf("server saw %s bytes, hash %s", resp.Header.Get("X-Body-Len"), resp.Header.Get("X-Body-Hash"))
		}
	})

	t.Run("LargeDownload", func(t *testing.T) {
		const size = 4 << 20
		resp := get(t, "/down?size="+strconv.Itoa(size))
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if len(body) != size || hashBody(body) != hashBody(filler(size)) {
			t.Fatalf("got %d bytes", len(body))
		}
		if resp.ContentLength != size {
			t.Fatalf("content length %d", resp.ContentLength)
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		const n = 50
		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				path := fmt.Sprintf("/c/%d?echo=1", i)
				resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, url+path, strings.NewReader(strconv.Itoa(i)))).Wait()
				if err != nil {
					errs[i] = err
					return
				}
				if got := readBody(t, resp); got != strconv.Itoa(i) {
					errs[i] = fmt.Errorf("request %d echoed %q", i, got)
				}
			}(i)
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		client.mu.Lock()
		conns := len(client.conns)
		client.mu.Unlock()
		if conns != 1 {
			t.Fatalf("%d connections, want 1", conns)
		}
	})

	t.Run("HEAD", func(t *testing.T) {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodHead, url+"/h?size=1024", nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if readBody(t, resp) != "" || resp.ContentLength != 1024 {
			t.Fatalf("HEAD content length %d", resp.ContentLength)
		}
	})

	t.Run("NoContent", func(t *testing.T) {
		resp := get(t, "/n?status=204&size=16")
		if resp.StatusCode != stdhttp.StatusNoContent || readBody(t, resp) != "" {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("ResponseTrailers", func(t *testing.T) {
		resp := get(t, "/t?size=8&trailer=X-Sum:42")
		if _, err := io.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		}
		if got := resp.Trailer.Get("X-Sum"); got != "42" {
			t.Fatalf("trailers %v", resp.Trailer)
		}
	})

	t.Run("RequestTrailers", func(t *testing.T) {
		req := mustRequest(t, stdhttp.MethodPost, url+"/rt?echo=1", strings.NewReader("body"))
		req.Trailer = stdhttp.Header{"X-Check": {"done"}}
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Header.Get("X-Req-Trailer-X-Check"); got != "done" {
			t.Fatalf("the server saw trailers %v", resp.Header)
		}
	})

	t.Run("InterimResponse", func(t *testing.T) {
		resp := get(t, "/i?interim=1&size=4")
		if resp.StatusCode != stdhttp.StatusOK || len(readBody(t, resp)) != 4 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("StatusCodes", func(t *testing.T) {
		for _, status := range []int{201, 301, 404, 418, 500} {
			resp := get(t, "/s?status="+strconv.Itoa(status))
			if resp.StatusCode != status {
				t.Fatalf("status %d, want %d", resp.StatusCode, status)
			}
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		quick := newClient(t, func(config *ClientConfig) { config.Timeout = 200 * time.Millisecond })
		_, err := quick.Go(mustRequest(t, stdhttp.MethodGet, url+"/slow?delay=3000", nil)).Wait()
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("timeout: %v", err)
		}
		// The connection is still good for the next request.
		resp, err := quick.Go(mustRequest(t, stdhttp.MethodGet, url+"/after?echo=1", nil)).Wait()
		if err != nil {
			t.Fatalf("after a timeout: %v", err)
		}
		_ = readBody(t, resp)
	})
}

// TestHTTP3ConformanceClientGoAway checks that the fib client finishes what
// it has in flight when the server retires the connection with GOAWAY.
func TestHTTP3ConformanceClientGoAway(t *testing.T) {
	p, url := startPeerServer(t)
	client := newClient(t, nil)
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/first", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	slow := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/slow?delay=500&size=32", nil))
	time.Sleep(100 * time.Millisecond)
	if result := p.do(interopSpec{Command: "shutdown"}); result.Error != "" {
		t.Fatalf("shutdown: %s", result.Error)
	}
	resp, err := slow.Wait()
	if err != nil {
		t.Fatalf("the request in flight when GOAWAY arrived failed: %v", err)
	}
	if len(readBody(t, resp)) != 32 {
		t.Fatal("short body")
	}
}

// TestHTTP3ConformanceClientRetry makes the server validate the client's
// address with a Retry packet before the handshake goes on.
func TestHTTP3ConformanceClientRetry(t *testing.T) {
	_, url := startPeerServer(t, "-retry")
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, url+"/retry?echo=1", strings.NewReader("after retry"))).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "after retry" {
		t.Fatalf("body %q", got)
	}
}

// TestHTTP3ConformanceClientBadCertificate checks that the client refuses a
// server its TLS configuration does not trust.
func TestHTTP3ConformanceClientBadCertificate(t *testing.T) {
	_, url := startPeerServer(t)
	client := newClient(t, func(config *ClientConfig) {
		config.TLSConfig = &tls.Config{ServerName: "localhost"}
	})
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait(); err == nil {
		t.Fatal("an untrusted certificate was accepted")
	}
}
