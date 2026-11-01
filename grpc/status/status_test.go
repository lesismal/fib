package status

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lesismal/fib/grpc/codes"
)

func TestStatus(t *testing.T) {
	err := Errorf(codes.NotFound, "no %s", "thing")
	if Code(err) != codes.NotFound || Convert(err).Message() != "no thing" {
		t.Fatalf("Errorf = %v", err)
	}
	if err.Error() != "rpc error: code = NotFound desc = no thing" {
		t.Fatalf("Error() = %q", err.Error())
	}
	if Error(codes.OK, "fine") != nil || Code(nil) != codes.OK {
		t.Fatal("OK is not nil")
	}
	wrapped := fmt.Errorf("lookup: %w", err)
	s, ok := FromError(wrapped)
	if !ok || s.Code() != codes.NotFound || s.Message() != wrapped.Error() {
		t.Fatalf("wrapped = %v, %v", s, ok)
	}
	if s, ok := FromError(errors.New("plain")); ok || s.Code() != codes.Unknown {
		t.Fatalf("plain = %v, %v", s, ok)
	}
	if !errors.Is(err, Error(codes.NotFound, "no thing")) {
		t.Fatal("errors.Is of equal statuses")
	}
	if FromContextError(context.DeadlineExceeded).Code() != codes.DeadlineExceeded ||
		FromContextError(context.Canceled).Code() != codes.Canceled {
		t.Fatal("FromContextError")
	}
	if codes.Code(99).String() != "Code(99)" || codes.Unavailable.String() != "Unavailable" {
		t.Fatal("codes.String")
	}
}

// nilStatusError claims a status and has none.
type nilStatusError struct{}

func (nilStatusError) Error() string       { return "no status" }
func (nilStatusError) GRPCStatus() *Status { return nil }

// TestStatusEdges covers an error whose GRPCStatus is nil, which is Unknown
// rather than OK, and the context errors that are neither deadline nor
// cancellation.
func TestStatusEdges(t *testing.T) {
	if s, ok := FromError(nilStatusError{}); ok || s.Code() != codes.Unknown || s.Message() != "no status" {
		t.Fatalf("FromError of a nil status = %v, %v", s, ok)
	}
	if FromContextError(nil) != nil {
		t.Fatal("FromContextError(nil)")
	}
	if s := FromContextError(errors.New("other")); s.Code() != codes.Unknown {
		t.Fatalf("FromContextError of another error = %v", s)
	}
}

// TestNilAndDetails covers a nil Status, which is OK with nothing to say, and
// one built with Newf or WithDetails, whose details come back unchanged.
func TestNilAndDetails(t *testing.T) {
	var s *Status
	if s.Code() != codes.OK || s.Message() != "" || s.Details() != nil || s.Err() != nil {
		t.Fatalf("nil Status = %v %q %v %v", s.Code(), s.Message(), s.Details(), s.Err())
	}
	if s, ok := FromError(nil); !ok || s != nil {
		t.Fatalf("FromError(nil) = %v, %v", s, ok)
	}
	if s := Newf(codes.Internal, "n=%d", 3); s.Code() != codes.Internal || s.Message() != "n=3" {
		t.Fatalf("Newf = %v", s)
	}
	d := WithDetails(codes.Aborted, "busy", []byte{1, 2})
	if d.Code() != codes.Aborted || d.Message() != "busy" || string(d.Details()) != "\x01\x02" {
		t.Fatalf("WithDetails = %v %v", d, d.Details())
	}
	got, ok := FromError(d.Err())
	if !ok || got != d {
		t.Fatalf("FromError of a status error = %v, %v", got, ok)
	}
}
