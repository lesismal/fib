// Package status carries the outcome of an RPC, a code and a message, as an
// error, like google.golang.org/grpc/status.
package status

import (
	"context"
	"errors"
	"fmt"

	"github.com/lesismal/fib/grpc/codes"
)

// Status is the outcome of an RPC. A nil *Status is OK.
type Status struct {
	code    codes.Code
	message string
	details []byte
}

// New returns a Status of code and msg.
func New(c codes.Code, msg string) *Status { return &Status{code: c, message: msg} }

// Newf is New with a formatted message.
func Newf(c codes.Code, format string, a ...any) *Status { return New(c, fmt.Sprintf(format, a...)) }

// WithDetails returns a Status of code and msg carrying details, the
// encoded google.rpc.Status sent as grpc-status-details-bin, which this
// package passes along without decoding.
func WithDetails(c codes.Code, msg string, details []byte) *Status {
	return &Status{code: c, message: msg, details: details}
}

// Error returns an error of code and msg, or nil for codes.OK.
func Error(c codes.Code, msg string) error { return New(c, msg).Err() }

// Errorf is Error with a formatted message.
func Errorf(c codes.Code, format string, a ...any) error { return Error(c, fmt.Sprintf(format, a...)) }

// Code returns the status code, OK for a nil Status.
func (s *Status) Code() codes.Code {
	if s == nil {
		return codes.OK
	}
	return s.code
}

// Message returns the status message.
func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return s.message
}

// Details returns the encoded details, or nil.
func (s *Status) Details() []byte {
	if s == nil {
		return nil
	}
	return s.details
}

// Err returns the Status as an error, nil when it is OK.
func (s *Status) Err() error {
	if s.Code() == codes.OK {
		return nil
	}
	return &statusError{s: s}
}

// String describes the Status.
func (s *Status) String() string {
	return fmt.Sprintf("rpc error: code = %s desc = %s", s.Code(), s.Message())
}

type statusError struct{ s *Status }

func (e *statusError) Error() string        { return e.s.String() }
func (e *statusError) GRPCStatus() *Status  { return e.s }
func (e *statusError) Is(target error) bool { return isStatus(e.s, target) }

func isStatus(s *Status, target error) bool {
	t, ok := target.(*statusError)
	return ok && t.s.code == s.code && t.s.message == s.message
}

// FromError returns the Status err carries, and true, when err is nil or
// holds one: an error, or an error it wraps, with a GRPCStatus method. For
// any other error it returns a Status of codes.Unknown with err's text, and
// false.
func FromError(err error) (*Status, bool) {
	if err == nil {
		return nil, true
	}
	var se interface {
		error
		GRPCStatus() *Status
	}
	if errors.As(err, &se) {
		s := se.GRPCStatus()
		if s == nil {
			return New(codes.Unknown, err.Error()), false
		}
		if se != err {
			// Wrapped: the message is the wrapping error's, which says more.
			return &Status{code: s.code, message: err.Error(), details: s.details}, true
		}
		return s, true
	}
	return New(codes.Unknown, err.Error()), false
}

// Convert is FromError without the boolean.
func Convert(err error) *Status {
	s, _ := FromError(err)
	return s
}

// Code returns the code of err's Status: OK for nil, Unknown for an error
// without one.
func Code(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	return Convert(err).Code()
}

// FromContextError converts a context's error to a Status: DeadlineExceeded
// for context.DeadlineExceeded, Canceled for context.Canceled, Unknown for
// anything else.
func FromContextError(err error) *Status {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		return New(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return New(codes.Canceled, err.Error())
	}
	return New(codes.Unknown, err.Error())
}
