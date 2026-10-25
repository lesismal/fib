// Command server is the origin the load-test campaign drives: HTTP/1 and
// h2c on a TCP port, HTTP/1 and HTTP/2 over TLS on another, and HTTP/3 on a
// UDP port, all from one process and one handler so that the process's RSS,
// goroutines and descriptors describe the protocol under test whichever one
// it is.
//
// It is a benchmark tool, not an example: it caches response bodies, reports
// its own runtime state at /stats, and exits on SIGINT or SIGTERM.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	stdhttp "net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
)

func main() {
	plain := flag.String("plain", "127.0.0.1:0", "HTTP/1 and h2c listen address")
	tlsAddr := flag.String("tls", "127.0.0.1:0", "HTTP/1 and HTTP/2 over TLS listen address")
	udpAddr := flag.String("h3", "127.0.0.1:0", "HTTP/3 (UDP) listen address")
	maxStreams := flag.Uint64("max-streams", 512, "HTTP/2 max concurrent streams per connection")
	maxStreams3 := flag.Uint64("h3-max-streams", 512, "HTTP/3 max concurrent streams per connection")
	addrFile := flag.String("addr-file", "", "write the bound addresses to this file as one JSON object")
	blocking := flag.Bool("blocking-tls", false, "run TLS handshakes through crypto/tls rather than the state machine")
	flag.Parse()

	cert := makeCertificate()
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	handler := fibhttp.HandlerFunc(serve)

	httpConfig := fibhttp.DefaultConfig()
	httpConfig.MaxConcurrentStreams = uint32(*maxStreams)
	httpHandler := fibhttp.NewHandlerWithConfig(httpConfig, handler)

	h3Config := http3.DefaultConfig()
	h3Config.TLSConfig = tlsConfig
	h3Config.MaxConcurrentStreams = *maxStreams3
	h3Config.BlockingTLSHandshake = *blocking
	h3Config.MaxBodyBytes = 32 << 20

	engineConfig := fib.DefaultConfig()
	engineConfig.UDPIdleTimeout = 60 * time.Second

	engine, err := fib.New(engineConfig,
		fib.Binding{Network: "tcp", Addr: *plain, Handler: httpHandler},
		fib.Binding{Network: "tcp", Addr: *tlsAddr,
			Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), httpHandler)},
		fib.Binding{Network: "udp", Addr: *udpAddr, Handler: http3.NewHandlerWithConfig(h3Config, handler)},
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
	go func() {
		if err := engine.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "server: engine:", err)
		}
	}()
	defer engine.Close()

	plainAddr, err := boundAddr(engine, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
	tlsBound, err := boundAddr(engine, 1)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
	udpBound, err := boundAddr(engine, 2)
	if err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
	addresses := map[string]string{
		"plain": plainAddr,
		"tls":   tlsBound,
		"h3":    udpBound,
		"pid":   strconv.Itoa(os.Getpid()),
	}
	line, _ := json.Marshal(addresses)
	fmt.Println(string(line))
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, append(line, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "server: addr-file:", err)
		}
	}
	os.Stdout.Sync()

	stopSampler := make(chan struct{})
	go samplePeaks(stopSampler)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
	close(stopSampler)
}

// boundAddr returns the host:port of the engine's i-th binding.
func boundAddr(engine *fib.Engine, i int) (string, error) {
	addrs, err := engine.BindingAddrs(i)
	if err != nil {
		return "", err
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("binding %d has no address", i)
	}
	addr := addrs[0]
	switch a := addr.(type) {
	case *net.TCPAddr:
		return net.JoinHostPort(hostOf(a.IP, a.Zone), strconv.Itoa(a.Port)), nil
	case *net.UDPAddr:
		return net.JoinHostPort(hostOf(a.IP, a.Zone), strconv.Itoa(a.Port)), nil
	default:
		return addr.String(), nil
	}
}

func hostOf(ip net.IP, zone string) string {
	if ip == nil {
		return "127.0.0.1"
	}
	if ip.IsUnspecified() {
		if ip.To4() != nil {
			return "127.0.0.1"
		}
		return "::1"
	}
	host := ip.String()
	if zone != "" {
		host += "%" + zone
	}
	return host
}

var (
	requests  atomic.Int64
	bytesOut  atomic.Int64
	pathHits  sync.Map // string -> *atomic.Int64
	peakG     atomic.Int64
	peakRSS   atomic.Int64
	peakFDs   atomic.Int64
	startTime = time.Now()
	bodyCache sync.Map // int -> []byte
)

func serve(c *fibhttp.Context) {
	path := c.Request.URL.Path
	if path == "/stats" {
		writeStats(c)
		return
	}
	count := pathCounter(path)
	count.Add(1)
	switch path {
	case "/plain":
		body := []byte("ok\n")
		requests.Add(1)
		bytesOut.Add(int64(len(body)))
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", body)
	case "/big":
		n := queryInt(c, "n", 1<<20, 64<<20)
		body := bodyFor(n)
		requests.Add(1)
		bytesOut.Add(int64(len(body)))
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	case "/upload":
		n, err := io.Copy(io.Discard, c.Request.Body)
		if err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain; charset=utf-8", []byte("bad body\n"))
			return
		}
		requests.Add(1)
		bytesOut.Add(n)
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("uploaded\n"))
	case "/stream":
		stream(c)
	default:
		_ = c.Respond(stdhttp.StatusNotFound, "text/plain; charset=utf-8", []byte("not found\n"))
	}
}

// stream writes a body of n bytes in chunks, optionally pausing between them.
// It is how a handler that is slower than its client is exercised.
func stream(c *fibhttp.Context) {
	n := queryInt(c, "n", 1<<20, 64<<20)
	chunk := queryInt(c, "chunk", 16<<10, 1<<20)
	delay := time.Duration(queryInt(c, "delayus", 0, 60_000_000)) * time.Microsecond
	c.WriteHeader(stdhttp.StatusOK)
	written := 0
	for written < n {
		size := min(chunk, n-written)
		if _, err := c.Write(bodyFor(size)); err != nil {
			return
		}
		c.Flush()
		written += size
		requests.Add(1)
		bytesOut.Add(int64(size))
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func queryInt(c *fibhttp.Context, name string, def, maxValue int) int {
	s := c.Request.URL.Query().Get(name)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return def
	}
	return min(n, maxValue)
}

func bodyFor(n int) []byte {
	if v, ok := bodyCache.Load(n); ok {
		return v.([]byte)
	}
	body := make([]byte, n)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	actual, _ := bodyCache.LoadOrStore(n, body)
	return actual.([]byte)
}

func pathCounter(path string) *atomic.Int64 {
	v, ok := pathHits.Load(path)
	if ok {
		return v.(*atomic.Int64)
	}
	c := new(atomic.Int64)
	actual, _ := pathHits.LoadOrStore(path, c)
	return actual.(*atomic.Int64)
}

// stats is one snapshot of what the process and the engine are doing.
type stats struct {
	UptimeSeconds  float64           `json:"uptime_seconds"`
	Goroutines     int               `json:"goroutines"`
	GoroutinesPeak int64             `json:"goroutines_peak"`
	FDs            int               `json:"fds"`
	FDsPeak        int64             `json:"fds_peak"`
	RSSBytes       int64             `json:"rss_bytes"`
	RSSPeak        int64             `json:"rss_peak"`
	HeapAlloc      uint64            `json:"heap_alloc"`
	HeapInuse      uint64            `json:"heap_inuse"`
	HeapSys        uint64            `json:"heap_sys"`
	NumGC          uint32            `json:"num_gc"`
	Requests       int64             `json:"requests"`
	BytesOut       int64             `json:"bytes_out"`
	Paths          map[string]int64  `json:"paths"`
	Engine         fib.Stats         `json:"engine"`
	Extra          map[string]string `json:"extra,omitempty"`
}

func snapshot() stats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s := stats{
		UptimeSeconds:  time.Since(startTime).Seconds(),
		Goroutines:     runtime.NumGoroutine(),
		GoroutinesPeak: peakG.Load(),
		FDs:            fdCount(),
		FDsPeak:        peakFDs.Load(),
		RSSBytes:       rssBytes(),
		RSSPeak:        peakRSS.Load(),
		HeapAlloc:      m.HeapAlloc,
		HeapInuse:      m.HeapInuse,
		HeapSys:        m.HeapSys,
		NumGC:          m.NumGC,
		Requests:       requests.Load(),
		BytesOut:       bytesOut.Load(),
		Paths:          make(map[string]int64),
	}
	pathHits.Range(func(k, v any) bool {
		s.Paths[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return s
}

func writeStats(c *fibhttp.Context) {
	query := c.Request.URL.Query()
	if query.Get("gc") == "1" {
		// Report what is live, not what the last allocation peak left behind.
		runtime.GC()
	}
	if query.Get("reset") == "1" {
		requests.Store(0)
		bytesOut.Store(0)
		pathHits.Range(func(_, v any) bool {
			v.(*atomic.Int64).Store(0)
			return true
		})
		peakG.Store(int64(runtime.NumGoroutine()))
		peakRSS.Store(rssBytes())
		peakFDs.Store(int64(fdCount()))
	}
	body, err := json.Marshal(snapshot())
	if err != nil {
		_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain; charset=utf-8", []byte(err.Error()))
		return
	}
	_ = c.Respond(stdhttp.StatusOK, "application/json", append(body, '\n'))
}

func samplePeaks(stop <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		update := func(v *atomic.Int64, n int64) {
			for {
				old := v.Load()
				if n <= old || v.CompareAndSwap(old, n) {
					return
				}
			}
		}
		update(&peakG, int64(runtime.NumGoroutine()))
		update(&peakRSS, rssBytes())
		update(&peakFDs, int64(fdCount()))
	}
}

func fdCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

// rssBytes reads the process's resident set size from /proc, which heap
// statistics do not show.
func rssBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return kb << 10
	}
	return 0
}

// makeCertificate issues a throwaway certificate for localhost and
// 127.0.0.1, as package tlstest does for the tests.
func makeCertificate() tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "fib loadtest"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		panic(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
