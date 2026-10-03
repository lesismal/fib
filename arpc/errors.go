//go:build linux || darwin || windows

package arpc

import "errors"

var (
	// ErrClientTimeout is what a call gets when its timeout passes, or its
	// context ends, before the response arrives.
	ErrClientTimeout = errors.New("arpc: timeout")
	// ErrClientInvalidTimeoutZero is what a call with a timeout of zero gets.
	ErrClientInvalidTimeoutZero = errors.New("arpc: invalid timeout, should not be 0")
	// ErrClientInvalidTimeoutLessThanZero is what a negative timeout gets.
	ErrClientInvalidTimeoutLessThanZero = errors.New("arpc: invalid timeout, should not be < 0")
	// ErrClientInvalidAsyncHandler is what CallAsync gets without a handler.
	ErrClientInvalidAsyncHandler = errors.New("arpc: invalid async handler, should not be nil")
	// ErrClientReconnecting is what a Client gets while it has no connection:
	// it lost the one it had and is dialing another. A call whose connection
	// broke before its response arrived gets it too.
	ErrClientReconnecting = errors.New("arpc: client reconnecting")
	// ErrClientStopped is what a Client gets once it has stopped.
	ErrClientStopped = errors.New("arpc: client stopped")
	// ErrClientInvalidPoolDialers is what NewClientPoolFromDialers gets
	// without a dialer.
	ErrClientInvalidPoolDialers = errors.New("arpc: invalid dialers, empty array")

	// ErrInvalidRspMessage is what a call gets for a response that is not
	// CmdResponse.
	ErrInvalidRspMessage = errors.New("arpc: invalid response message cmd")
	// ErrMethodNotFound is the error response to a request for a method no
	// handler is registered for.
	ErrMethodNotFound = errors.New("method not found")
	// ErrInvalidFlagBitIndex is what Message.SetFlagBit gets for an index
	// outside 0 to 7.
	ErrInvalidFlagBitIndex = errors.New("arpc: invalid index, should be 0-7")
	// ErrBodyTooLarge is why a connection that sent a message longer than
	// Handler.MaxBodyLen was closed.
	ErrBodyTooLarge = errors.New("arpc: message body too large")
	// ErrInvalidCmd is why a connection that sent a message of an unknown
	// command was closed.
	ErrInvalidCmd = errors.New("arpc: invalid message cmd")

	// ErrContextResponseToNotify is what writing a response to a notify, which
	// expects none, gets.
	ErrContextResponseToNotify = errors.New("arpc: should not response to a context with notify message")

	// ErrStreamClosedSend is what sending on a Stream whose sending side has
	// closed gets.
	ErrStreamClosedSend = errors.New("arpc: stream has closed send")

	// ErrTimeout is what an asynchronous call's handler gets when no response
	// arrived in time, and what Server.Shutdown gets when its context ends
	// first.
	ErrTimeout = errors.New("arpc: timeout")
)
