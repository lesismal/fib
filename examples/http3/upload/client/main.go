//go:build linux || darwin || windows

// Command client sends a file to the HTTP/3 upload server; see package
// uploadclient for what -mode does. It uses fib's HTTP/3 client, which holds
// each request's body, and each response's, in memory: so -mode upload and
// -mode echo need the file to fit, while -mode resume sends it in -chunk sized
// requests and holds only one at a time, which is what to use for a large file.
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
//	go run ./examples/http3/upload/client -mode resume -file /tmp/big.bin -name part.bin -max-chunks 3
//	go run ./examples/http3/upload/client -mode resume -file /tmp/big.bin -name part.bin
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/lesismal/fib/examples/certs"
	"github.com/lesismal/fib/examples/example"
	"github.com/lesismal/fib/examples/http/upload/uploadclient"
	"github.com/lesismal/fib/http3"
)

// fatal reports an error and exits; a test replaces it.
var fatal = func(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fatal(err)
	}
}

// doer sends requests through fib's HTTP/3 client and waits for the response.
type doer struct{ client *http3.Client }

func (d doer) Do(req *http.Request) (*http.Response, error) { return d.client.Go(req).Wait() }

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(out)
	options := uploadclient.Register(flags, "https://127.0.0.1:8445")
	certFlags := certs.ClientFlagsOn(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	tlsConfig, err := certFlags.Config()
	if err != nil {
		return err
	}
	// The client dials UDP through an engine of any kind; this one has no
	// listener of its own.
	engine, stop := example.ClientEngine()
	defer stop()
	config := http3.DefaultClientConfig()
	config.TLSConfig = tlsConfig
	// A large body takes longer than the default, and a response is buffered
	// whole, so it has to be allowed to be as large as the file.
	config.Timeout = 10 * time.Minute
	config.MaxResponseBodyBytes = 4 << 30
	client := http3.NewClient(engine, config)
	defer client.Close()
	return options.Run(doer{client}, out)
}
