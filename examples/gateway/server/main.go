//go:build linux || darwin || windows

// Command server is a reverse proxy and gateway. It accepts HTTP/1.1, HTTP/2,
// HTTP/3 and WebSocket requests and forwards them to upstream servers by path
// prefix, without blocking an engine worker: request bodies, upstream dials,
// upstream responses and WebSocket messages are all handled in callbacks.
//
//	go run ./examples/gateway/upstream
//	go run ./examples/gateway/server
//	go run ./examples/gateway/client
//
// It listens on one address for both TLS transports, as the HTTP/3 example
// does: TCP carries HTTPS, which is HTTP/2 for clients that choose it through
// ALPN and HTTP/1.1 for the rest, and UDP carries HTTP/3. The responses on TCP
// carry Alt-Svc, which is how a browser learns that it can switch. With
// -plain-addr it also serves cleartext HTTP/1.1 and h2c, and ws://.
//
// A route is "prefix=target", where target is http:// for an HTTP/1.1
// upstream, https:// for one that speaks HTTP/2 if it can, or h3:// for an
// HTTP/3 upstream. By default it forwards to the upstream example, with the
// same request reaching it over each protocol:
//
//	/h1 -> http://127.0.0.1:9000     HTTP/1.1
//	/h2 -> https://127.0.0.1:9443    HTTP/2
//	/h3 -> h3://127.0.0.1:9443       HTTP/3
//
// Without -cert and -key it issues itself a certificate for localhost and
// writes it where the gateway's client looks for it. -ca names the certificate
// of the upstreams, which is the upstream example's.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/gateway/server/proxy"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
)

// routeFlags collects repeated -route flags.
type routeFlags []proxy.Route

func (r *routeFlags) String() string { return fmt.Sprint(len(*r), " routes") }

func (r *routeFlags) Set(spec string) error {
	route, err := proxy.ParseRoute(spec)
	if err != nil {
		return err
	}
	*r = append(*r, route)
	return nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8446", "listen address: TCP for HTTPS (HTTP/2 and HTTP/1.1), UDP for HTTP/3")
	plainAddr := flag.String("plain-addr", "127.0.0.1:8081", "listen address for cleartext HTTP/1.1, h2c and ws://; empty to disable")
	timeout := flag.Duration("timeout", 5*time.Minute, "longest an upstream request may take, body included; 0 for no limit")
	subprotocols := flag.String("subprotocols", "chat,superchat", "WebSocket subprotocols to accept from clients and ask the upstream for")
	maxBody := flag.Int64("max-request-body", 64<<20, "largest request body to forward")
	var routes routeFlags
	flag.Var(&routes, "route", "prefix=target, repeatable (default: /h1, /h2 and /h3 to the upstream example)")
	serverCerts := certs.ServerFlagsWithOut(flag.CommandLine, certs.GatewayCertFile)
	upstreamCerts := certs.RegisterClientFlags()
	flag.Parse()

	if len(routes) == 0 {
		for _, spec := range []string{
			"/h1=http://127.0.0.1:9000",
			"/h2=https://127.0.0.1:9443",
			"/h3=h3://127.0.0.1:9443",
		} {
			route, _ := proxy.ParseRoute(spec)
			routes = append(routes, route)
		}
	}
	tlsConfig, err := serverCerts.Config()
	if err != nil {
		example.Fatal(err)
	}
	upstreamTLS, err := upstreamCerts.Config()
	if err != nil {
		example.Fatal(err)
	}
	_, port, err := net.SplitHostPort(*addr)
	if err != nil {
		example.Fatal(err)
	}
	portNum, _ := strconv.Atoi(port)

	gateway, err := proxy.New(proxy.Config{
		Routes:         routes,
		TLSConfig:      upstreamTLS,
		Timeout:        *timeout,
		MaxRequestBody: *maxBody,
		Subprotocols:   strings.FieldsFunc(*subprotocols, func(r rune) bool { return r == ',' || r == ' ' }),
		AltSvc:         http3.AltSvc(portNum),
		Logf:           log.Printf,
	})
	if err != nil {
		example.Fatal(err)
	}
	engines, err := newEngines(gateway, tlsConfig, *addr, *plainAddr)
	if err != nil {
		example.Fatal(err)
	}
	// The upstream clients dial through the first engine, the HTTPS one.
	gateway.Attach(engines[0])
	run(gateway, engines, fmt.Sprintf("gateway listening on https://%s (HTTP/2, HTTP/1.1; HTTP/3 on UDP)%s",
		*addr, plainBanner(*plainAddr)))
}

func plainBanner(plainAddr string) string {
	if plainAddr == "" {
		return ""
	}
	return fmt.Sprintf(", http://%s (HTTP/1.1, h2c, ws://)", plainAddr)
}

// newEngines binds the gateway's listeners: the HTTPS and HTTP/3 pair on addr,
// and the cleartext one on plainAddr if it is not empty. They all serve the
// gateway's one handler.
func newEngines(gateway *proxy.Gateway, tlsConfig *tls.Config, addr, plainAddr string) ([]*fib.Engine, error) {
	handler := gateway.Handler()
	httpConfig := gateway.HTTPConfig()

	// HTTPS: fibtls decrypts in front of the HTTP handler, which serves HTTP/2
	// and HTTP/1.1 alike once ConfigureTLS has offered h2 through ALPN.
	tcpConfig := fib.DefaultConfig()
	tcpConfig.Addr = addr
	tcp, err := fib.Bind(tcpConfig, fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandlerWithConfig(httpConfig, handler)))
	if err != nil {
		return nil, err
	}
	engines := []*fib.Engine{tcp}

	// HTTP/3: a UDP engine whose handler is http3's.
	udpConfig := fib.DefaultConfig()
	udpConfig.Network = "udp"
	udpConfig.Addr = addr
	udp, err := fib.Bind(udpConfig, http3.NewHandlerWithConfig(gateway.HTTP3Config(tlsConfig), handler))
	if err != nil {
		return nil, err
	}
	engines = append(engines, udp)

	if plainAddr != "" {
		plainConfig := fib.DefaultConfig()
		plainConfig.Addr = plainAddr
		plain, err := fib.Bind(plainConfig, fibhttp.NewHandlerWithConfig(httpConfig, handler))
		if err != nil {
			return nil, err
		}
		engines = append(engines, plain)
	}
	return engines, nil
}

// run serves until Ctrl-C, then closes the upstream clients, which has to be
// done before the engines.
func run(gateway *proxy.Gateway, engines []*fib.Engine, banner string) {
	done := make(chan error, len(engines))
	for _, engine := range engines {
		go func() { done <- engine.Run() }()
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println(banner)
	running := len(engines)
	select {
	case <-interrupt:
	case err := <-done:
		running--
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	gateway.Close()
	for _, engine := range engines {
		engine.Stop()
	}
	for ; running > 0; running-- {
		<-done
	}
	for _, engine := range engines {
		_ = engine.Close()
	}
}
