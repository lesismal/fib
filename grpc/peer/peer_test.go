package peer

import (
	"context"
	"net"
	"testing"
)

// A Peer put in a context comes back out of it; a context without one has
// none.
func TestContext(t *testing.T) {
	if p, ok := FromContext(context.Background()); ok || p != nil {
		t.Fatalf("FromContext of a bare context = %v, %v", p, ok)
	}
	want := &Peer{Addr: &net.TCPAddr{Port: 1}, LocalAddr: &net.TCPAddr{Port: 2}}
	if p, ok := FromContext(NewContext(context.Background(), want)); !ok || p != want {
		t.Fatalf("FromContext = %v, %v", p, ok)
	}
}
