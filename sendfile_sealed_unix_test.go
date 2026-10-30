//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"
)

// Every sealed chunk but a file's last tells the kernel more follows it, so
// that TCP fills its segments across chunks rather than pushing each.
func TestSendFileRawSealedChunksSayMoreFollows(t *testing.T) {
	const size = 3*sendFileChunk + 100
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var mu sync.Mutex
	var mores []bool
	write := writeSealed
	writeSealed = func(c *Connection, data []byte, more bool) (int, error) {
		mu.Lock()
		mores = append(mores, more)
		mu.Unlock()
		return write(c, data, more)
	}
	t.Cleanup(func() { writeSealed = write })
	c, client := connectOne(t, HandlerFuncs{})
	if err := c.SendFileRaw(f, 0, size, &frameSealer{}); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(20 * time.Second))
	if got := unseal(t, client, size); !bytes.Equal(got, data) {
		t.Fatal("the peer read other bytes than the file's")
	}
	mu.Lock()
	defer mu.Unlock()
	// A chunk the socket takes in part is written again with the same mark,
	// so the marks run: more for the first three chunks, then not for the
	// last.
	more := 0
	for more < len(mores) && mores[more] {
		more++
	}
	for _, m := range mores[more:] {
		if m {
			t.Fatalf("writes said more follows as %v, after the last chunk began", mores)
		}
	}
	if more < 3 || more == len(mores) {
		t.Fatalf("writes said more follows as %v, want it for the first three chunks and not the last", mores)
	}
}
