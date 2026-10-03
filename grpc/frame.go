//go:build linux || darwin || windows

package grpc

import (
	"encoding/binary"
	"fmt"
)

// HTTP/2 framing (RFC 9113 sections 4 and 6), as much of it as gRPC uses.

const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

const (
	frameHeaderLen     = 9
	defaultMaxFrame    = 16384
	maxFrameSizeLimit  = 1<<24 - 1
	defaultWindow      = 65535
	maxWindow          = 1<<31 - 1
	defaultHeaderTable = 4096

	// streamWindow and connWindow are the receive windows this side
	// advertises: what a peer may send on one stream, and on all of them,
	// before this side has taken it.
	streamWindow = 1 << 20
	connWindow   = 16 << 20
	// maxHeaderBlock bounds the header block of one HEADERS and its
	// CONTINUATIONs.
	maxHeaderBlock = 1 << 20
)

type frameType uint8

const (
	frameData         frameType = 0x0
	frameHeaders      frameType = 0x1
	framePriority     frameType = 0x2
	frameRSTStream    frameType = 0x3
	frameSettings     frameType = 0x4
	framePushPromise  frameType = 0x5
	framePing         frameType = 0x6
	frameGoAway       frameType = 0x7
	frameWindowUpdate frameType = 0x8
	frameContinuation frameType = 0x9
)

const (
	flagEndStream  = 0x1
	flagAck        = 0x1
	flagEndHeaders = 0x4
	flagPadded     = 0x8
	flagPriority   = 0x20
)

const (
	settingHeaderTableSize      = 0x1
	settingEnablePush           = 0x2
	settingMaxConcurrentStreams = 0x3
	settingInitialWindowSize    = 0x4
	settingMaxFrameSize         = 0x5
	settingMaxHeaderListSize    = 0x6
)

// errCode is an HTTP/2 error code, carried by RST_STREAM and GOAWAY.
type errCode uint32

const (
	errNo            errCode = 0x0
	errProtocol      errCode = 0x1
	errInternal      errCode = 0x2
	errFlowControl   errCode = 0x3
	errStreamClosed  errCode = 0x5
	errFrameSize     errCode = 0x6
	errRefusedStream errCode = 0x7
	errCancel        errCode = 0x8
	errCompression   errCode = 0x9
	errEnhanceCalm   errCode = 0xb
)

// connError ends the whole connection with a GOAWAY of its code.
type connError struct {
	code errCode
	msg  string
}

func (e *connError) Error() string { return fmt.Sprintf("grpc: http2 error %d: %s", e.code, e.msg) }

// streamError resets one stream.
type streamError struct {
	id   uint32
	code errCode
}

func (e *streamError) Error() string {
	return fmt.Sprintf("grpc: http2 stream %d error %d", e.id, e.code)
}

func protocolError(format string, a ...any) error {
	return &connError{code: errProtocol, msg: fmt.Sprintf(format, a...)}
}

// frame is one frame read: its header, and its payload as part of what the
// connection read or of the buffer that gathered it.
type frame struct {
	length   uint32
	typ      frameType
	flags    uint8
	streamID uint32
	payload  []byte
}

func (f *frame) has(flag uint8) bool { return f.flags&flag != 0 }

func appendFrameHeader(dst []byte, typ frameType, flags uint8, id uint32, length int) []byte {
	return append(dst, byte(length>>16), byte(length>>8), byte(length), byte(typ), flags,
		byte(id>>24)&0x7f, byte(id>>16), byte(id>>8), byte(id))
}

func appendSettings(dst []byte, settings ...uint32) []byte {
	dst = appendFrameHeader(dst, frameSettings, 0, 0, len(settings)/2*6)
	for i := 0; i < len(settings); i += 2 {
		dst = binary.BigEndian.AppendUint16(dst, uint16(settings[i]))
		dst = binary.BigEndian.AppendUint32(dst, settings[i+1])
	}
	return dst
}

func appendWindowUpdate(dst []byte, id uint32, increment uint32) []byte {
	dst = appendFrameHeader(dst, frameWindowUpdate, 0, id, 4)
	return binary.BigEndian.AppendUint32(dst, increment)
}

func appendRSTStream(dst []byte, id uint32, code errCode) []byte {
	dst = appendFrameHeader(dst, frameRSTStream, 0, id, 4)
	return binary.BigEndian.AppendUint32(dst, uint32(code))
}

func appendGoAway(dst []byte, lastID uint32, code errCode, debug string) []byte {
	dst = appendFrameHeader(dst, frameGoAway, 0, 0, 8+len(debug))
	dst = binary.BigEndian.AppendUint32(dst, lastID)
	dst = binary.BigEndian.AppendUint32(dst, uint32(code))
	return append(dst, debug...)
}

func appendPing(dst []byte, ack bool, data []byte) []byte {
	var flags uint8
	if ack {
		flags = flagAck
	}
	dst = appendFrameHeader(dst, framePing, flags, 0, 8)
	return append(dst, data[:8]...)
}

// appendHeaderBlock frames a header block as HEADERS and as many
// CONTINUATIONs as maxFrame needs.
func appendHeaderBlock(dst []byte, id uint32, block []byte, endStream bool, maxFrame int) []byte {
	typ := frameHeaders
	var flags uint8
	if endStream {
		flags = flagEndStream
	}
	for {
		n := min(len(block), maxFrame)
		f := flags
		if n == len(block) {
			f |= flagEndHeaders
		}
		dst = appendFrameHeader(dst, typ, f, id, n)
		dst = append(dst, block[:n]...)
		block = block[n:]
		if len(block) == 0 {
			return dst
		}
		typ, flags = frameContinuation, 0
	}
}

// parseFrameHeader reads the header in b, which has frameHeaderLen bytes.
func parseFrameHeader(b []byte) frame {
	return frame{
		length:   uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]),
		typ:      frameType(b[3]),
		flags:    b[4],
		streamID: binary.BigEndian.Uint32(b[5:]) & 0x7fffffff,
	}
}

// stripPadding removes the padding of a DATA or HEADERS frame.
func stripPadding(f *frame) error {
	if !f.has(flagPadded) {
		return nil
	}
	if len(f.payload) < 1 {
		return protocolError("padded frame too short")
	}
	pad := int(f.payload[0])
	if pad >= len(f.payload) {
		return protocolError("padding longer than the frame")
	}
	f.payload = f.payload[1 : len(f.payload)-pad]
	return nil
}
