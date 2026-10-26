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

// baseTestcase canonicalizes TESTCASE to the name the endpoint's raw
// HTTP/0.9 paths implement. The runner does not pass the loss and corruption
// cases to an endpoint by their own names: its TestCase.testname maps
// handshakeloss and handshakecorruption to multiconnect, and transferloss and
// transfercorruption to transfer, while the simulator drops or corrupts the
// packets. Accepting all four names as aliases keeps the endpoint right when
// it is started with one directly, and makes the mapping explicit here.
func baseTestcase(testcase string) string {
	switch testcase {
	case "handshakeloss", "handshakecorruption":
		// The check counts one handshake per file, so the client has to use
		// one connection per file, which is what multiconnect does.
		return "multiconnect"
	case "transferloss", "transfercorruption":
		return "transfer"
	}
	return testcase
}

// testcaseAction is what an endpoint role does with TESTCASE: run one of the
// raw HTTP/0.9 paths, run the HTTP/3 path, or report it as unsupported, which
// main turns into the runner's exit status 127.
type testcaseAction int

const (
	actionUnsupported testcaseAction = iota
	actionRaw
	actionH3
)

// dispatchServer reports the server's action for TESTCASE, with the canonical
// name the raw path runs. TESTCASE may be one of the loss or corruption
// aliases; see baseTestcase.
func dispatchServer(testcase string) (testcaseAction, string) {
	base := baseTestcase(testcase)
	switch base {
	case "http3":
		return actionH3, base
	case "handshake", "transfer", "retry", "resumption", "zerortt", "chacha20", "multiconnect":
		// These are the testcases the runner names to a server. The rest of
		// its matrix reaches the server under one of these names, for
		// instance transfer for a blackhole or a rebinding test.
		return actionRaw, base
	}
	return actionUnsupported, base
}

func runServer(testcase string) error {
	action, base := dispatchServer(testcase)
	switch action {
	case actionH3:
		return serveH3()
	case actionRaw:
		return serveRaw(base)
	}
	switch testcase {
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
	switch baseTestcase(testcase) {
	case "http3", "handshake", "transfer", "retry", "resumption", "zerortt",
		"multiconnect", "keyupdate", "chacha20", "ecn", "v2", "versionnegotiation":
		return true
	}
	return false
}

// dispatchClient reports the client's action for TESTCASE, with the canonical
// name the raw path runs. TESTCASE may be one of the loss or corruption
// aliases; see baseTestcase.
func dispatchClient(testcase string) (testcaseAction, string) {
	base := baseTestcase(testcase)
	switch base {
	case "http3":
		return actionH3, base
	case "handshake", "transfer", "retry", "resumption", "zerortt", "multiconnect", "keyupdate", "chacha20":
		return actionRaw, base
	}
	return actionUnsupported, base
}

func runClient(testcase string, urls []*url.URL) error {
	action, base := dispatchClient(testcase)
	switch action {
	case actionH3:
		return fetchH3(urls)
	case actionRaw:
		return fetchRaw(base, urls)
	}
	switch testcase {
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
