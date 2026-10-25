// Command fib-interop is the QUIC Interop Runner endpoint for fib's HTTP/3
// stack. It speaks both roles the runner asks of an implementation:
//
//	ROLE=server  serve the files in /www on UDP port 443, for the testcase
//	             TESTCASE, with the certificate in /certs
//	ROLE=client  download the URLs in REQUESTS into DOWNLOADS
//
// The runner drives most testcases over HTTP/0.9, a request line and the
// raw file on a QUIC stream, so the QUIC-level testcases (handshake,
// transfer, retry, resumption, 0-RTT, multiconnect) run on fib's QUIC
// transport directly; TESTCASE=http3 runs over fib's full HTTP/3 server and
// client. A testcase the endpoint cannot serve exits 127, which the runner
// takes as "unsupported" and reports as such instead of a failure.
package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("fib-interop: ")

	role := os.Getenv("ROLE")
	testcase := os.Getenv("TESTCASE")
	if testcase == "" {
		fatalf("TESTCASE is not set")
	}
	prepareDirs()

	var err error
	switch role {
	case "server":
		err = runServer(testcase)
	case "client":
		// The runner's compliance check starts the client with a random
		// testcase and no REQUESTS, and expects 127: decide that the
		// testcase is known before reading anything else.
		if !clientTestcaseKnown(testcase) {
			err = unsupported(testcase, "fib's endpoint has no client role for it")
			break
		}
		urls, perr := requestURLs()
		if perr != nil {
			fatalf("%v", perr)
		}
		err = runClient(testcase, urls)
	default:
		fatalf("ROLE must be server or client, not %q", role)
	}
	if err != nil {
		if errors.Is(err, errUnsupported) {
			// The runner's contract: 127 means "this implementation does not
			// support this testcase", not a failure.
			fmt.Printf("unsupported test case: %s\n", testcase)
			os.Exit(127)
		}
		log.Printf("failed: %v", err)
		os.Exit(1)
	}
	log.Printf("done: role=%s testcase=%s", role, testcase)
}

// errUnsupported marks a testcase the endpoint does not implement, which is
// reported to the runner as exit status 127.
var errUnsupported = errors.New("unsupported test case")

func unsupported(testcase, why string) error {
	log.Printf("testcase %q is not supported: %s", testcase, why)
	return errUnsupported
}

func logf(format string, args ...any) { log.Printf(format, args...) }

func fatalf(format string, args ...any) {
	log.Printf(format, args...)
	os.Exit(1)
}

// requestURLs is the runner's REQUESTS: the URLs to download, space
// separated.
func requestURLs() ([]*url.URL, error) {
	fields := strings.Fields(os.Getenv("REQUESTS"))
	if len(fields) == 0 {
		return nil, errors.New("REQUESTS is empty")
	}
	urls := make([]*url.URL, 0, len(fields))
	for _, field := range fields {
		u, err := url.Parse(field)
		if err != nil {
			return nil, fmt.Errorf("bad REQUESTS entry %q: %w", field, err)
		}
		if u.Scheme != "https" {
			return nil, fmt.Errorf("REQUESTS entry %q is not https", field)
		}
		urls = append(urls, u)
	}
	return urls, nil
}

func runServer(testcase string) error {
	switch testcase {
	case "http3":
		return serveH3()
	case "handshake", "transfer", "retry", "resumption", "zerortt", "chacha20", "multiconnect":
		// These are the testcases the runner names to a server. The rest of
		// its matrix reaches the server under one of these names, for
		// instance transfer for a blackhole or a rebinding test.
		return serveRaw(testcase)
	case "connectionmigration":
		return unsupported(testcase, "fib does not send preferred_address, so it cannot offer the client an address to migrate to")
	case "ecn":
		return unsupported(testcase, "fib does not mark or validate ECN")
	case "v2":
		return unsupported(testcase, "fib speaks QUIC version 1 only")
	case "versionnegotiation":
		return unsupported(testcase, "the runner disables this testcase")
	default:
		return unsupported(testcase, "fib's endpoint has no server role for it")
	}
}

// clientTestcaseKnown reports whether TESTCASE is one the client has a role
// for, supported or not, so that an unknown one exits 127 before REQUESTS
// is looked at.
func clientTestcaseKnown(testcase string) bool {
	switch testcase {
	case "http3", "handshake", "transfer", "retry", "resumption", "zerortt",
		"multiconnect", "keyupdate", "chacha20", "ecn", "v2", "versionnegotiation":
		return true
	}
	return false
}

func runClient(testcase string, urls []*url.URL) error {
	switch testcase {
	case "http3":
		return fetchH3(urls)
	case "handshake", "transfer", "retry", "resumption", "zerortt", "multiconnect", "keyupdate":
		return fetchRaw(testcase, urls)
	case "chacha20":
		return unsupported(testcase, "TLS 1.3 selects the cipher suite, and crypto/tls offers AES-128-GCM before ChaCha20-Poly1305, so fib's ClientHello cannot offer ChaCha20 alone")
	case "ecn":
		return unsupported(testcase, "fib does not mark or validate ECN")
	case "v2":
		return unsupported(testcase, "fib speaks QUIC version 1 only")
	case "versionnegotiation":
		return unsupported(testcase, "fib's client cannot be made to start with a reserved version")
	default:
		return unsupported(testcase, "fib's endpoint has no client role for it")
	}
}
