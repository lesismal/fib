// Command interop is the peer the HTTP/3 conformance tests run against: an
// HTTP/3 server and an HTTP/3 client built on quic-go, driven over stdin and
// stdout with one JSON object per line.
//
// It is a module of its own so that quic-go stays out of fib's own module:
// nothing in github.com/lesismal/fib/go imports it, and the tests run it as
// a subprocess.
//
//	interop -mode server -cert cert.pem -key key.pem [-retry]
//	interop -mode client -ca cert.pem -url https://127.0.0.1:1234
//
// In server mode it prints "listening <address>" once it is up, then reads
// commands; in client mode it reads request specifications. Either way a
// line of JSON goes back for each line that comes in, so a test can drive it
// step by step.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	addr := flag.String("addr", "127.0.0.1:0", "server listen address")
	certFile := flag.String("cert", "", "PEM certificate, server mode")
	keyFile := flag.String("key", "", "PEM private key, server mode")
	caFile := flag.String("ca", "", "PEM certificate to trust, client mode")
	url := flag.String("url", "", "base URL, client mode")
	retry := flag.Bool("retry", false, "server mode: validate every source address with a Retry")
	flag.Parse()

	var err error
	switch *mode {
	case "server":
		err = runServer(*addr, *certFile, *keyFile, *retry)
	case "client":
		err = runClient(*url, *caFile)
	default:
		err = errors.New("-mode must be server or client")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "interop:", err)
		os.Exit(1)
	}
}

// request is what the test asks the client to send.
type request struct {
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Header   map[string][]string `json:"header"`
	Trailer  map[string][]string `json:"trailer"`
	Body     string              `json:"body"`
	BodySize int                 `json:"bodySize"`
	// Count requests sent at once on the connection, all identical.
	Count int `json:"count"`
	// Command is for the server side: "shutdown" or "quit".
	Command string `json:"command"`
}

// result is what comes back for each request.
type result struct {
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

// hashBody is the FNV-1a of a body, so that large ones are checked without
// being carried around.
func hashBody(b []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// filler is the deterministic body both sides can generate.
func filler(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func runClient(base, caFile string) error {
	if base == "" {
		return errors.New("-url is required")
	}
	pool := x509.NewCertPool()
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return err
		}
		if !pool.AppendCertsFromPEM(pem) {
			return errors.New("no certificate in " + caFile)
		}
	}
	tr := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}
	defer tr.Close()
	client := &http.Client{Transport: tr, Timeout: 60 * time.Second}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		var spec request
		if err := json.Unmarshal([]byte(line), &spec); err != nil {
			return err
		}
		if spec.Command == "quit" {
			return nil
		}
		res := doRequests(client, base, spec)
		if err := out.Encode(res); err != nil {
			return err
		}
	}
	return in.Err()
}

// doRequests sends a specification's requests, all at once when it asks for
// more than one, and returns the last result, or the first failure.
func doRequests(client *http.Client, base string, spec request) result {
	count := max(spec.Count, 1)
	results := make([]result, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = doRequest(client, base, spec)
		}(i)
	}
	wg.Wait()
	for _, r := range results {
		if r.Error != "" {
			r.Count = count
			return r
		}
	}
	last := results[count-1]
	last.Count = count
	return last
}

func doRequest(client *http.Client, base string, spec request) result {
	method := spec.Method
	if method == "" {
		method = http.MethodGet
	}
	var body io.Reader
	switch {
	case spec.BodySize > 0:
		body = strings.NewReader(string(filler(spec.BodySize)))
	case spec.Body != "":
		body = strings.NewReader(spec.Body)
	}
	req, err := http.NewRequest(method, base+spec.Path, body)
	if err != nil {
		return result{Error: err.Error()}
	}
	for key, values := range spec.Header {
		req.Header[http.CanonicalHeaderKey(key)] = values
	}
	if len(spec.Trailer) > 0 {
		req.Trailer = make(http.Header, len(spec.Trailer))
		for key, values := range spec.Trailer {
			req.Trailer[http.CanonicalHeaderKey(key)] = values
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{Error: err.Error()}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return result{Status: resp.StatusCode, Error: err.Error()}
	}
	res := result{
		Status:   resp.StatusCode,
		Proto:    resp.Proto,
		Header:   resp.Header,
		Trailer:  resp.Trailer,
		BodyLen:  len(data),
		BodyHash: hashBody(data),
	}
	if len(data) <= 4096 {
		res.Body = string(data)
	}
	return res
}

func runServer(addr, certFile, keyFile string, retry bool) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	pconn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	server := &http3.Server{
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
		Handler:   http.HandlerFunc(serveHTTP),
	}
	transport := &quic.Transport{Conn: pconn}
	if retry {
		// Every connection is answered with a Retry, which is what makes a
		// client prove its address before the handshake goes on.
		transport.VerifySourceAddress = func(net.Addr) bool { return true }
	}
	listener, err := transport.ListenEarly(server.TLSConfig, nil)
	if err != nil {
		return err
	}
	fmt.Printf("listening %s\n", pconn.LocalAddr().String())
	os.Stdout.Sync()

	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ServeListener(listener) }()

	in := bufio.NewScanner(os.Stdin)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		var spec request
		if err := json.Unmarshal([]byte(line), &spec); err != nil {
			return err
		}
		switch spec.Command {
		case "shutdown":
			// A graceful shutdown sends GOAWAY and waits for the requests
			// already running.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := server.Shutdown(ctx)
			cancel()
			res := result{}
			if err != nil {
				res.Error = err.Error()
			}
			if err := out.Encode(res); err != nil {
				return err
			}
		case "quit":
			_ = server.Close()
			<-serveDone
			return nil
		default:
			if err := out.Encode(result{Error: "unknown command " + spec.Command}); err != nil {
				return err
			}
		}
	}
	_ = server.Close()
	<-serveDone
	return in.Err()
}

// serveHTTP answers the conformance tests' requests. The query decides what
// comes back, so that one endpoint covers every case.
func serveHTTP(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if ms, _ := strconv.Atoi(query.Get("delay")); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("X-Read-Error", err.Error())
	}
	// Trailers of the request are only there once its body has been read.
	for key, values := range r.Trailer {
		w.Header()["X-Req-Trailer-"+key] = values
	}
	w.Header().Set("X-Method", r.Method)
	w.Header().Set("X-Path", r.URL.RequestURI())
	w.Header().Set("X-Proto", r.Proto)
	w.Header().Set("X-Host", r.Host)
	w.Header().Set("X-Body-Len", strconv.Itoa(len(body)))
	w.Header().Set("X-Body-Hash", hashBody(body))
	for key, values := range r.Header {
		if strings.HasPrefix(key, "X-Echo-") {
			w.Header()[key] = values
		}
	}
	if v := query.Get("header"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, value, ok := strings.Cut(pair, ":"); ok {
				w.Header().Add(name, value)
			}
		}
	}
	var trailers []string
	if v := query.Get("trailer"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, _, ok := strings.Cut(pair, ":"); ok {
				trailers = append(trailers, http.CanonicalHeaderKey(name))
			}
		}
		w.Header()["Trailer"] = trailers
	}
	if query.Get("interim") != "" {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Del("Link")
	}
	status := http.StatusOK
	if v, err := strconv.Atoi(query.Get("status")); err == nil && v >= 200 {
		status = v
	}
	size, _ := strconv.Atoi(query.Get("size"))
	if size > 0 && status != http.StatusNoContent && status != http.StatusNotModified {
		w.Header().Set("Content-Length", strconv.Itoa(size))
	}
	w.WriteHeader(status)
	switch {
	case size > 0:
		out := filler(size)
		for len(out) > 0 {
			n := min(len(out), 64<<10)
			if _, err := w.Write(out[:n]); err != nil {
				return
			}
			out = out[n:]
		}
	case query.Get("echo") != "":
		_, _ = w.Write(body)
	}
	if v := query.Get("trailer"); v != "" {
		for _, pair := range strings.Split(v, ",") {
			if name, value, ok := strings.Cut(pair, ":"); ok {
				w.Header().Set(http.CanonicalHeaderKey(name), value)
			}
		}
	}
}
