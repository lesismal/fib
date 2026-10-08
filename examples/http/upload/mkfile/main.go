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

func main() {
	out := flag.String("o", "upload-test.bin", "file to write")
	size := flag.String("size", "256MiB", "size, e.g. 100MB, 256MiB, 1GiB or a number of bytes")
	seed := flag.Uint64("seed", 1, "seed of the content")
	flag.Parse()

	n, err := parseSize(*size)
	if err != nil {
		fatal(err)
	}
	file, err := os.Create(*out)
	if err != nil {
		fatal(err)
	}
	defer file.Close()

	var key [32]byte
	for i := 0; i < 8; i++ {
		key[i] = byte(*seed >> (8 * i))
	}
	rng := rand.NewChaCha8(key)
	sum := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(file, sum), 1<<20)
	if _, err := io.CopyN(w, rng, n); err != nil {
		fatal(err)
	}
	if err := w.Flush(); err != nil {
		fatal(err)
	}
	fmt.Printf("%s bytes=%d sha256=%x\n", *out, n, sum.Sum(nil))
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
