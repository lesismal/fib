// Command h3load is a sustained HTTP/3 load driver built on fib's own client.
// It opens one QUIC connection per client, keeps a fixed number of requests
// in flight across them for a fixed time, and reports throughput, latency
// percentiles, errors and per-second completion counts.
//
//	go run ./benchmark/loadtest/h3load -url https://127.0.0.1:18444 -conns 4 -streams 256 -duration 30s
//
// The server's certificate is not verified, as the load campaign runs against
// the throwaway certificate of benchmark/loadtest/server.
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	stdhttp "net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/http3"
)

// histogram resolution: one bucket per microsecond, up to maxHistogramUS.
const maxHistogramUS = 2_000_000

type result struct {
	Protocol  string   `json:"protocol"`
	URL       string   `json:"url"`
	Conns     int      `json:"conns"`
	Streams   int      `json:"streams"`
	Duration  float64  `json:"duration_s"`
	Warmup    float64  `json:"warmup_s"`
	Requests  int64    `json:"requests"`
	RPS       float64  `json:"rps"`
	Status2xx int64    `json:"status_2xx"`
	Status3xx int64    `json:"status_3xx"`
	Status4xx int64    `json:"status_4xx"`
	Status5xx int64    `json:"status_5xx"`
	Errors    int64    `json:"errors"`
	MeanUS    float64  `json:"mean_us"`
	SDUS      float64  `json:"sd_us"`
	P50US     int64    `json:"p50_us"`
	P90US     int64    `json:"p90_us"`
	P99US     int64    `json:"p99_us"`
	MaxUS     int64    `json:"max_us"`
	Overflow  int64    `json:"over_2s"`
	Stuck     int64    `json:"stuck"`
	Seconds   []int64  `json:"seconds"`
	ErrorTop  []string `json:"error_top,omitempty"`
}

func main() {
	url := flag.String("url", "https://127.0.0.1:18444", "base URL")
	path := flag.String("path", "/plain", "request path, query included")
	method := flag.String("method", "GET", "request method")
	conns := flag.Int("conns", 1, "QUIC connections, one per client")
	streams := flag.Int("streams", 256, "requests in flight in total")
	duration := flag.Duration("duration", 30*time.Second, "measurement duration")
	warmup := flag.Duration("warmup", 3*time.Second, "warmup before the measurement")
	timeout := flag.Duration("timeout", 30*time.Second, "per-request timeout")
	bodySize := flag.Int("body", 0, "request body size in bytes")
	serverName := flag.String("server-name", "localhost", "TLS server name")
	timeline := flag.Bool("timeline", false, "print the per-second completion counts")
	grace := flag.Duration("grace", 15*time.Second, "how long to wait for in-flight requests after the measurement")
	flag.Parse()

	if *conns < 1 {
		*conns = 1
	}
	if *streams < 1 {
		*streams = 1
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "h3load: -duration must be positive")
		os.Exit(1)
	}

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		fmt.Fprintln(os.Stderr, "h3load: engine:", err)
		os.Exit(1)
	}
	go engine.Run()
	defer func() {
		engine.Stop()
		engine.Close()
	}()

	clients := make([]*http3.Client, *conns)
	for i := range clients {
		config := http3.DefaultClientConfig()
		config.Timeout = *timeout
		config.HandshakeTimeout = *timeout
		config.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: *serverName}
		clients[i] = http3.NewClient(engine, config)
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	var (
		requests  atomic.Int64
		status2xx atomic.Int64
		status3xx atomic.Int64
		status4xx atomic.Int64
		status5xx atomic.Int64
		errCount  atomic.Int64
		bytesRead atomic.Int64
		over2s    atomic.Int64
		maxUS     atomic.Int64
		active    atomic.Int64
	)
	hist := make([]atomic.Int64, maxHistogramUS)
	start := time.Now()
	measureStart := start.Add(*warmup)
	end := measureStart.Add(*duration)
	seconds := int(*duration/time.Second) + 2
	perSecond := make([]atomic.Int64, seconds)

	var errMu sync.Mutex
	errTop := make(map[string]int64)

	recordError := func(err error) {
		if err == nil {
			return
		}
		errCount.Add(1)
		msg := err.Error()
		if len(msg) > 120 {
			msg = msg[:120]
		}
		errMu.Lock()
		if len(errTop) < 32 {
			errTop[msg]++
		}
		errMu.Unlock()
	}

	record := func(started time.Time, elapsed time.Duration, status int, n int64, err error) {
		if started.Before(measureStart) || !started.Before(end) {
			return
		}
		requests.Add(1)
		bytesRead.Add(n)
		us := elapsed.Microseconds()
		if us < 0 {
			us = 0
		}
		if us >= maxHistogramUS {
			over2s.Add(1)
		} else {
			hist[us].Add(1)
		}
		for {
			old := maxUS.Load()
			if us <= old || maxUS.CompareAndSwap(old, us) {
				break
			}
		}
		switch {
		case err != nil:
			recordError(err)
			return
		case status >= 500:
			status5xx.Add(1)
		case status >= 400:
			status4xx.Add(1)
		case status >= 300:
			status3xx.Add(1)
		default:
			status2xx.Add(1)
		}
		if delta := started.Sub(measureStart); delta >= 0 {
			if i := int(delta / time.Second); i >= 0 && i < len(perSecond) {
				perSecond[i].Add(1)
			}
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	defer close(stop)
	for ci, client := range clients {
		per := *streams / *conns
		if ci < *streams%*conns {
			per++
		}
		for j := 0; j < per; j++ {
			wg.Add(1)
			go func(cl *http3.Client) {
				defer wg.Done()
				for {
					now := time.Now()
					if !now.Before(end) {
						return
					}
					body := io.Reader(nil)
					if *bodySize > 0 {
						body = bytes.NewReader(make([]byte, *bodySize))
					}
					req, err := stdhttp.NewRequest(*method, *url+*path, body)
					if err != nil {
						recordError(err)
						return
					}
					active.Add(1)
					t0 := time.Now()
					resp, err := cl.Go(req).Wait()
					var n int64
					status := 0
					if err == nil {
						status = resp.StatusCode
						n, err = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
					}
					active.Add(-1)
					record(t0, time.Since(t0), status, n, err)
					select {
					case <-stop:
						return
					default:
					}
				}
			}(client)
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	var stuck int64
	select {
	case <-done:
	case <-time.After(*duration + *warmup + *grace):
		stuck = active.Load()
	}

	res := result{
		Protocol:  "h3",
		URL:       *url + *path,
		Conns:     *conns,
		Streams:   *streams,
		Duration:  duration.Seconds(),
		Warmup:    warmup.Seconds(),
		Requests:  requests.Load(),
		Status2xx: status2xx.Load(),
		Status3xx: status3xx.Load(),
		Status4xx: status4xx.Load(),
		Status5xx: status5xx.Load(),
		Errors:    errCount.Load(),
		MaxUS:     maxUS.Load(),
		Overflow:  over2s.Load(),
		Stuck:     stuck,
	}
	if res.Requests > 0 {
		res.RPS = float64(res.Requests) / duration.Seconds()
	}
	res.P50US, res.P90US, res.P99US = percentiles(hist, res.Requests)
	res.MeanUS, res.SDUS = moments(hist, res.Requests)
	for i := range perSecond {
		res.Seconds = append(res.Seconds, perSecond[i].Load())
	}
	errMu.Lock()
	for msg, n := range errTop {
		res.ErrorTop = append(res.ErrorTop, fmt.Sprintf("%d x %s", n, msg))
	}
	sort.Strings(res.ErrorTop)
	errMu.Unlock()

	if *timeline {
		fmt.Printf("seconds: %v\n", res.Seconds)
	}
	fmt.Printf("h3 %s conns=%d streams=%d: %d requests in %.1fs, %.0f req/s, %.1f MiB/s, "+
		"p50=%.2fms p90=%.2fms p99=%.2fms max=%.2fms errors=%d stuck=%d\n",
		res.URL, res.Conns, res.Streams, res.Requests, res.Duration, res.RPS,
		float64(bytesRead.Load())/(1<<20)/res.Duration,
		float64(res.P50US)/1000, float64(res.P90US)/1000, float64(res.P99US)/1000,
		float64(res.MaxUS)/1000, res.Errors, res.Stuck)
	for _, e := range res.ErrorTop {
		fmt.Println("  error:", e)
	}
	encoded, _ := json.Marshal(res)
	fmt.Println("RESULT " + string(encoded))
}

// moments returns the mean and standard deviation of the latencies in hist,
// in microseconds. count is how many samples hist holds.
func moments(hist []atomic.Int64, count int64) (mean, sd float64) {
	if count <= 0 {
		return 0, 0
	}
	var sum, sumsq float64
	var seen int64
	for us := range hist {
		n := hist[us].Load()
		if n == 0 {
			continue
		}
		seen += n
		x := float64(us)
		sum += x * float64(n)
		sumsq += x * x * float64(n)
	}
	mean = sum / float64(count)
	variance := sumsq/float64(count) - mean*mean
	if variance < 0 {
		variance = 0
	}
	return mean, math.Sqrt(variance)
}

// percentiles returns the 50th, 90th and 99th percentile of the latencies in
// hist, in microseconds. count is how many samples hist holds.
func percentiles(hist []atomic.Int64, count int64) (p50, p90, p99 int64) {
	if count <= 0 {
		return 0, 0, 0
	}
	targets := [3]int64{count * 50 / 100, count * 90 / 100, count * 99 / 100}
	var out [3]int64
	at := 0
	var seen int64
	for us := range hist {
		n := hist[us].Load()
		if n == 0 {
			continue
		}
		seen += n
		for at < 3 && seen >= targets[at] {
			out[at] = int64(us)
			at++
		}
		if at == 3 {
			break
		}
	}
	return out[0], out[1], out[2]
}
