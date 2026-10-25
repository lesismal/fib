package main

import (
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
)

// serveH3 is the server of the runner's http3 testcase: fib's HTTP/3 server
// with the runner's certificate, serving /www on port 443.
func serveH3() error {
	cert, err := loadServerCertificate()
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		KeyLogWriter: keyLogWriter(),
	}
	config := http3.DefaultConfig()
	config.TLSConfig = tlsConfig
	config.MaxIdleTimeout = 120 * time.Second

	handler := http3.NewHandlerWithConfig(config, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		body, err := readWWW(wwwDir(), c.Request.URL.Path)
		if err != nil {
			_ = c.Respond(http.StatusNotFound, "text/plain; charset=utf-8", []byte("not found\n"))
			return
		}
		_ = c.Respond(http.StatusOK, "application/octet-stream", body)
	}))

	engineConfig := fib.DefaultConfig()
	// The runner's client may keep a connection for the whole testcase; do
	// not let fib's UDP peer timeout end it while it is quiet.
	engineConfig.UDPIdleTimeout = 150 * time.Second
	engine, err := fib.New(engineConfig, fib.Binding{Network: "udp", Addr: ":443", Handler: handler})
	if err != nil {
		return err
	}
	go engine.Run()
	logf("HTTP/3 server listening on :443, serving %s", wwwDir())

	waitSignal()
	engine.Stop()
	return nil
}

// readWWW reads a file below root the way an HTTP file server would, from a
// request path that came off the wire.
func readWWW(root, requestPath string) ([]byte, error) {
	if i := strings.IndexByte(requestPath, '?'); i >= 0 {
		requestPath = requestPath[:i]
	}
	clean := path.Clean("/" + strings.TrimPrefix(requestPath, "/"))
	if clean == "/" {
		clean = "/index.html"
	}
	root = filepath.Clean(root)
	full := filepath.Join(root, filepath.FromSlash(clean))
	if full != root && !strings.HasPrefix(full, root+string(os.PathSeparator)) {
		return nil, os.ErrNotExist
	}
	return os.ReadFile(full)
}

// waitSignal blocks until the runner stops the container.
func waitSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
}
