// Command mkfile writes the test file the upload example sends: pseudo-random
// bytes, so that nothing compresses and a corrupted or truncated upload cannot
// match by accident. The content depends only on -seed, so the same flags
// make the same file, and it prints the file's SHA-256 for comparing with what
// the server reports.
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
package main

import (
	"bufio"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
)

// fatal reports an error and exits; a test replaces it.
var fatal = func(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// create opens the file to write; a test replaces it.
var create = func(path string) (io.WriteCloser, error) { return os.Create(path) }

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fatal(err)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("mkfile", flag.ContinueOnError)
	flags.SetOutput(out)
	path := flags.String("o", "upload-test.bin", "file to write")
	size := flags.String("size", "256MiB", "size, e.g. 100MB, 256MiB, 1GiB or a number of bytes")
	seed := flags.Uint64("seed", 1, "seed of the content")
	if err := flags.Parse(args); err != nil {
		return err
	}
	n, err := parseSize(*size)
	if err != nil {
		return err
	}
	file, err := create(*path)
	if err != nil {
		return err
	}
	defer file.Close()

	var key [32]byte
	for i := 0; i < 8; i++ {
		key[i] = byte(*seed >> (8 * i))
	}
	sum := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(file, sum), 1<<20)
	// ChaCha8 never fails to read, and w writes to a file: a failure of that
	// file surfaces at Flush or Close.
	_, _ = io.CopyN(w, rand.NewChaCha8(key), n)
	if err := w.Flush(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s bytes=%d sha256=%x\n", *path, n, sum.Sum(nil))
	return err
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"B", 1}}
	mult := int64(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSuffix(s, u.suffix), u.mult
			break
		}
	}
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return v * mult, nil
}
