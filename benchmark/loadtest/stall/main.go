// Command stall is the badly-behaved client of the load campaign: it asks
// for a large response and then reads it slowly, or stops reading it
// altogether, for a fixed time. Run alongside h2load or h3load, it exercises
// the server's backpressure, flow control and cleanup paths, the ones a
// well-behaved load generator never reaches.
//
//	# 16 stalled HTTP/1 connections while h2load serves the fast ones
//	go run ./benchmark/loadtest/stall -proto h1 -url http://127.0.0.1:18080 -streams 16 -stall
//
// Each stream gets a connection of its own, so a stalled stream is a stalled
// connection and the slow clients cannot hide behind the fast ones.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
)

type result struct {
	Protocol string  `json:"protocol"`
	Streams  int     `json:"streams"`
	Duration float64 `json:"duration_s"`
	Bytes    int64   `json:"bytes"`
	Errors   int64   `json:"errors"`
	Empty    int64   `json:"no_response"`
	Stalled  bool    `json:"stall"`
	DelayMS  int64   `json:"delay_ms"`
}

func main() {
	proto := flag.String("proto", "h2c", "h1, h2c, h2 or h3")
	base := flag.String("url", "http://127.0.0.1:18080", "base URL; scheme picks the default port")
	path := flag.String("path", "/big?n=8388608", "request path, query included")
	streams := flag.Int("streams", 16, "slow connections")
	duration := flag.Duration("duration", 30*time.Second, "how long to hold the bodies")
	chunk := flag.Int("chunk", 16<<10, "bytes to read at a time")
	delay := flag.Duration("delay", 100*time.Millisecond, "pause after each chunk")
	stall := flag.Bool("stall", false, "stop reading after the response header")
	serverName := flag.String("server-name", "localhost", "TLS server name")
	flag.Parse()

	if *streams < 1 {
		*streams = 1
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "stall: -duration must be positive")
		os.Exit(1)
	}
	u, err := url.Parse(*base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stall: -url:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var bytesRead atomic.Int64
	var errCount atomic.Int64
	var empty atomic.Int64
	fail := func(err error) {
		if err == nil {
			return
		}
		errCount.Add(1)
		if *verbose {
			fmt.Fprintln(os.Stderr, "stall:", err)
		}
	}

	switch *proto {
	case "h1":
		var wg sync.WaitGroup
		for i := 0; i < *streams; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				n, got, err := readHTTP1(ctx, u, *path, *chunk, *delay, *stall)
				bytesRead.Add(n)
				if !got {
					empty.Add(1)
				}
				fail(err)
			}()
		}
		wg.Wait()
	case "h2c", "h2":
		engine, err := fib.New(fib.DefaultConfig())
		if err != nil {
			fmt.Fprintln(os.Stderr, "stall: engine:", err)
			os.Exit(1)
		}
		go engine.Run()
		defer func() { engine.Stop(); engine.Close() }()

		var wg sync.WaitGroup
		for i := 0; i < *streams; i++ {
			w := &httpSlow{
				engine:     engine,
				url:        *base + *path,
				proto:      *proto,
				delay:      *delay,
				stall:      *stall,
				timeout:    *duration + time.Minute,
				serverName: *serverName,
				bytes:      &bytesRead,
				fail:       fail,
				empty:      &empty,
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.run(ctx)
			}()
		}
		wg.Wait()
	case "h3":
		engine, err := fib.New(fib.DefaultConfig())
		if err != nil {
			fmt.Fprintln(os.Stderr, "stall: engine:", err)
			os.Exit(1)
		}
		go engine.Run()
		defer func() { engine.Stop(); engine.Close() }()

		var wg sync.WaitGroup
		for i := 0; i < *streams; i++ {
			w := &h3Slow{
				engine:     engine,
				url:        *base + *path,
				delay:      *delay,
				stall:      *stall,
				timeout:    *duration + time.Minute,
				serverName: *serverName,
				bytes:      &bytesRead,
				fail:       fail,
				empty:      &empty,
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.run(ctx)
			}()
		}
		wg.Wait()
	default:
		fmt.Fprintln(os.Stderr, "stall: -proto must be h1, h2c, h2 or h3")
		os.Exit(1)
	}

	res := result{
		Protocol: "stall-" + *proto,
		Streams:  *streams,
		Duration: duration.Seconds(),
		Bytes:    bytesRead.Load(),
		Errors:   errCount.Load(),
		Empty:    empty.Load(),
		Stalled:  *stall,
		DelayMS:  delay.Milliseconds(),
	}
	fmt.Printf("stall-%s: %d streams, %d bytes, %d errors, %d without a response\n",
		*proto, res.Streams, res.Bytes, res.Errors, res.Empty)
	encoded, _ := json.Marshal(res)
	fmt.Println("RESULT " + string(encoded))
}

var verbose = flag.Bool("v", false, "log each stream error")

// readHTTP1 sends one request and reads the response body slowly, or not at
// all. It returns once the body ends or ctx is done, and whether the response
// header arrived.
func readHTTP1(ctx context.Context, u *url.URL, path string, chunk int, delay time.Duration, stall bool) (int64, bool, error) {
	host := u.Host
	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return 0, false, err
	}
	defer conn.Close()
	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()
	request := "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nUser-Agent: fib-stall\r\nConnection: close\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		return 0, false, err
	}
	reader := bufio.NewReaderSize(conn, 32<<10)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return 0, false, err
		}
		if line == "\r\n" {
			break
		}
	}
	if stall {
		<-ctx.Done()
		return 0, true, nil
	}
	var total int64
	buf := make([]byte, chunk)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, err := reader.Read(buf)
		total += int64(n)
		if n > 0 && delay > 0 {
			select {
			case <-ctx.Done():
				return total, true, nil
			case <-time.After(delay):
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, true, nil
			}
			return total, true, err
		}
		select {
		case <-ctx.Done():
			return total, true, nil
		default:
		}
	}
}

// httpSlow is one streamed HTTP/1.1 or HTTP/2 response taken slowly.
type httpSlow struct {
	engine     *fib.Engine
	url, proto string
	delay      time.Duration
	stall      bool
	timeout    time.Duration
	serverName string
	bytes      *atomic.Int64
	empty      *atomic.Int64
	fail       func(error)
}

func (w *httpSlow) run(ctx context.Context) {
	config := fibhttp.DefaultClientConfig()
	config.Timeout = w.timeout
	config.StreamResponseBody = true
	config.StreamResponseBodyThreshold = 0
	config.MaxStreamedBodyBytes = 0
	config.UnencryptedHTTP2 = w.proto == "h2c"
	if w.proto == "h2" {
		config.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: w.serverName}
	}
	client := fibhttp.NewClient(w.engine, config)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, w.url, nil)
	if err != nil {
		w.fail(err)
		return
	}
	var got atomic.Bool
	var seen atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(done) }) }
	client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		if err != nil {
			w.fail(err)
			finish()
			return
		}
		got.Store(true)
		resp.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				w.fail(err)
				finish()
				return
			}
			seen.Add(int64(len(data)))
			if fin {
				finish()
				return
			}
			if w.stall {
				<-ctx.Done()
				finish()
				return
			}
			select {
			case <-ctx.Done():
				finish()
				return
			case <-time.After(w.delay):
			}
		})
	})
	select {
	case <-ctx.Done():
	case <-done:
	}
	w.bytes.Add(seen.Load())
	if !got.Load() {
		w.empty.Add(1)
	}
}

// h3Slow is one streamed HTTP/3 response taken slowly.
type h3Slow struct {
	engine     *fib.Engine
	url        string
	delay      time.Duration
	stall      bool
	timeout    time.Duration
	serverName string
	bytes      *atomic.Int64
	empty      *atomic.Int64
	fail       func(error)
}

func (w *h3Slow) run(ctx context.Context) {
	config := http3.DefaultClientConfig()
	config.Timeout = w.timeout
	config.StreamResponseBody = true
	config.StreamResponseBodyThreshold = 0
	config.MaxStreamedBodyBytes = 0
	config.TLSConfig = &tls.Config{InsecureSkipVerify: true, ServerName: w.serverName}
	client := http3.NewClient(w.engine, config)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, w.url, nil)
	if err != nil {
		w.fail(err)
		return
	}
	var got atomic.Bool
	var seen atomic.Int64
	done := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(done) }) }
	client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		if err != nil {
			w.fail(err)
			finish()
			return
		}
		got.Store(true)
		resp.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				w.fail(err)
				finish()
				return
			}
			seen.Add(int64(len(data)))
			if fin {
				finish()
				return
			}
			if w.stall {
				<-ctx.Done()
				finish()
				return
			}
			select {
			case <-ctx.Done():
				finish()
				return
			case <-time.After(w.delay):
			}
		})
	})
	select {
	case <-ctx.Done():
	case <-done:
	}
	w.bytes.Add(seen.Load())
	if !got.Load() {
		w.empty.Add(1)
	}
}
