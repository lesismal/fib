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
