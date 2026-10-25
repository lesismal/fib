package main

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
)

// fetchH3 is the client of the runner's http3 testcase: fib's HTTP/3 client
// downloads every URL into DOWNLOADS, on one connection, in parallel.
func fetchH3(urls []*url.URL) error {
	host := urls[0].Hostname()
	tlsConfig := &tls.Config{
		// The runner's certificate names the server's simulated addresses,
		// not the ones the client resolves; as the runner expects, the
		// client does not verify it.
		InsecureSkipVerify: true,
		ServerName:         host,
		MinVersion:         tls.VersionTLS13,
		KeyLogWriter:       keyLogWriter(),
	}
	config := http3.DefaultClientConfig()
	config.TLSConfig = tlsConfig
	config.StreamResponseBody = true
	config.MaxResponseBodyBytes = 64 << 20
	config.Timeout = 60 * time.Second

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		return err
	}
	go engine.Run()
	client := http3.NewClient(engine, config)
	defer func() {
		client.Close()
		engine.Stop()
	}()

	var wg sync.WaitGroup
	errs := make(chan error, len(urls))
	for _, u := range urls {
		wg.Add(1)
		go func(u *url.URL) {
			defer wg.Done()
			errs <- downloadH3(client, u, downloadsDir())
		}(u)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func downloadH3(client *http3.Client, u *url.URL, dir string) error {
	name := path.Base(u.Path)
	if name == "." || name == "/" || name == "" {
		name = "index"
	}
	dest := filepath.Join(dir, name)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		if err != nil {
			done <- err
			return
		}
		if resp.StatusCode != http.StatusOK {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			done <- fmt.Errorf("%s: status %d", u, resp.StatusCode)
			return
		}
		f, err := os.Create(dest)
		if err != nil {
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			done <- err
			return
		}
		var once sync.Once
		finish := func(err error) {
			once.Do(func() {
				if cerr := f.Close(); err == nil {
					err = cerr
				}
				done <- err
			})
		}
		resp.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				finish(err)
				return
			}
			if len(data) > 0 {
				if _, werr := f.Write(data); werr != nil {
					finish(werr)
					return
				}
			}
			if fin {
				finish(nil)
			}
		})
	})
	if err := <-done; err != nil {
		_ = os.Remove(dest)
		return err
	}
	return nil
}
