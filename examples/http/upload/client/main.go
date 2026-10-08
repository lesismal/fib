//go:build linux || darwin || windows

// Command client sends a file to the upload server over HTTP/1.1, streaming it
// from disk so that neither end holds it in memory; see package uploadclient
// for what -mode does.
//
// It uses net/http's client: fib's own client buffers a request body whole,
// which is what an upload of this size must not do.
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
//	go run ./examples/http/upload/client -mode resume -file /tmp/big.bin -name part.bin -max-chunks 3
//	go run ./examples/http/upload/client -mode resume -file /tmp/big.bin -name part.bin
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/lesismal/fib/examples/http/upload/uploadclient"
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

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(out)
	options := uploadclient.Register(flags, "http://127.0.0.1:8080")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return options.Run(http.DefaultClient, out)
}
