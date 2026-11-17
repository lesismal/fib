package bufferpool

import (
	"sync"
	"testing"
	"unsafe"
)

// countingAllocator hands out plain buffers and remembers which are out, so a
// test can see what reached it.
type countingAllocator struct {
	mu      sync.Mutex
	out     map[*byte]bool
	gets    int
	puts    int
	foreign int
}

func newCountingAllocator() *countingAllocator {
	return &countingAllocator{out: map[*byte]bool{}}
}

func (a *countingAllocator) Get(size int) []byte {
	buf := make([]byte, size, size+7)
	a.mu.Lock()
	a.gets++
	a.out[unsafe.SliceData(buf)] = true
	a.mu.Unlock()
	return buf
}

func (a *countingAllocator) Put(buf []byte) {
	p := unsafe.SliceData(buf[:cap(buf)])
	a.mu.Lock()
	if a.out[p] {
		delete(a.out, p)
		a.puts++
	} else {
		a.foreign++
	}
	a.mu.Unlock()
}

// useAllocator installs a for the rest of the test.
func useAllocator(t *testing.T, a Allocator) {
	t.Helper()
	SetAllocator(a)
	t.Cleanup(func() { SetAllocator(nil) })
}

func TestASetAllocatorServesGetAndPut(t *testing.T) {
	a := newCountingAllocator()
	useAllocator(t, a)

	buf := Get(100)
	if len(buf) != 100 || cap(buf) != 107 {
		t.Fatalf("Get(100) has len %d cap %d, want the allocator's 100 and 107", len(buf), cap(buf))
	}
	Put(buf)
	if a.gets != 1 || a.puts != 1 || len(a.out) != 0 {
		t.Fatalf("allocator saw %d gets and %d puts with %d out, want 1, 1 and 0", a.gets, a.puts, len(a.out))
	}
}

func TestASetAllocatorIsNotAskedForNothing(t *testing.T) {
	a := newCountingAllocator()
	useAllocator(t, a)

	if buf := Get(0); buf != nil {
		t.Fatalf("Get(0) = %v, want nil", buf)
	}
	if buf := Get(-1); buf != nil {
		t.Fatalf("Get(-1) = %v, want nil", buf)
	}
	Put(nil)
	Put([]byte{})
	if a.gets != 0 || a.puts != 0 || a.foreign != 0 {
		t.Fatalf("allocator saw %d gets, %d puts and %d foreign puts, want none", a.gets, a.puts, a.foreign)
	}
}

func TestASetAllocatorTakesEverySize(t *testing.T) {
	a := newCountingAllocator()
	useAllocator(t, a)

	for _, size := range []int{1, MinSize, MaxSize + 1} {
		buf := Get(size)
		if len(buf) != size {
			t.Fatalf("Get(%d) has len %d", size, len(buf))
		}
		Put(buf)
	}
	if a.gets != 3 || a.puts != 3 {
		t.Fatalf("allocator saw %d gets and %d puts, want 3 each", a.gets, a.puts)
	}
}

func TestASetAllocatorIsWhatGrowAndJoinUse(t *testing.T) {
	a := newCountingAllocator()
	useAllocator(t, a)

	buf := Append(nil, []byte("hello"))
	buf = Grow(buf, 4096)
	buf = Join(buf, []byte(", "), make([]byte, 8192))
	if string(buf[:7]) != "hello, " {
		t.Fatalf("bytes after growing = %q, want them kept", buf[:7])
	}
	Put(buf)
	if a.gets != 3 || a.puts != 3 || len(a.out) != 0 || a.foreign != 0 {
		t.Fatalf("allocator saw %d gets, %d puts, %d out and %d foreign, want 3, 3, 0 and 0",
			a.gets, a.puts, len(a.out), a.foreign)
	}
}

func TestASetAllocatorDecidesAboutForeignBuffers(t *testing.T) {
	a := newCountingAllocator()
	useAllocator(t, a)

	Put(make([]byte, 4096))
	if a.foreign != 1 {
		t.Fatalf("allocator saw %d foreign puts, want 1", a.foreign)
	}
}

func TestSetAllocatorNilRestoresThePool(t *testing.T) {
	a := newCountingAllocator()
	SetAllocator(a)
	SetAllocator(nil)
	t.Cleanup(func() { SetAllocator(nil) })

	buf := Get(1000)
	if cap(buf) != Align(1000) {
		t.Fatalf("Get(1000) has cap %d, want the pool's %d", cap(buf), Align(1000))
	}
	Put(buf)
	if a.gets != 0 || a.puts != 0 {
		t.Fatalf("a removed allocator saw %d gets and %d puts", a.gets, a.puts)
	}
}
