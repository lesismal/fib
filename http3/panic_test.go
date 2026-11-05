package http3

import (
	"errors"
	"net"
	"testing"

	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// closeRecorder is a QUIC handler that keeps what its connection closed
// with.
type closeRecorder struct {
	nopQUICHandler
	err *error
}

func (h closeRecorder) OnClose(_ *quic.Conn, err error) { *h.err = err }

// What elsewhere runs on a goroutine of its own, where nothing would recover
// a panic, closes its connection with H3_INTERNAL_ERROR when it panics,
// rather than taking the process down; on the pool, the panic goes on to the
// pool's panic handler after the close.
func TestElsewherePanicClosesConnection(t *testing.T) {
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	for _, goOn := range []bool{false, true} {
		var closeErr error
		qc, err := quic.Dial(&capturePath{}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
			quic.Config{TLSConfig: clientTLS}, closeRecorder{err: &closeErr})
		if err != nil {
			t.Fatal(err)
		}
		recovered := func() (r any) {
			defer func() { r = recover() }()
			defer closePanicked(qc, goOn)
			panic("retire bug")
		}()
		if goOn != (recovered == "retire bug") {
			t.Fatalf("goes on %v: recovered %v", goOn, recovered)
		}
		var ae *quic.ApplicationError
		if !errors.As(closeErr, &ae) || ae.Code != uint64(ErrCodeInternalError) {
			t.Fatalf("closed with %v, want H3_INTERNAL_ERROR", closeErr)
		}
	}
}
