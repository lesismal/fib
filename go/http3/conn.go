//go:build linux || darwin || windows

package http3

import (
	"errors"

	"github.com/lesismal/fib/go/http3/internal/qpack"
	"github.com/lesismal/fib/go/http3/internal/quic"
)

// What both sides of an HTTP/3 connection do alike: the control stream
// each opens with its SETTINGS, and the unidirectional streams the peer
// opens.

// peerStreams tracks the unidirectional streams the peer has opened, of
// which there may be one of each critical kind, and what its control stream
// has said.
type peerStreams struct {
	control, encoder, decoder bool
	// isClient is whether this side is the client, which decides what the
	// peer may send on its control stream.
	isClient bool
	// settings is whether the peer's SETTINGS have arrived, and
	// maxFieldSection what they allow a field section to weigh.
	settings        bool
	maxFieldSection uint64
	// goneAway is whether a GOAWAY has arrived and with what identifier;
	// the peer may only lower it (RFC 9114 section 5.2).
	goneAway bool
	goAwayID uint64
	// maxPushID is the largest the peer has granted, which it may only
	// raise. Push is never enabled here, so it stays at zero.
	maxPushID uint64
	// onGoAway receives the peer's GOAWAY.
	onGoAway func(id uint64) error
}

// openControl opens this side's control stream and sends SETTINGS on it.
func openControl(qc *quic.Conn, maxFieldSection int) (*quic.Stream, error) {
	s, err := qc.OpenUniStream()
	if err != nil {
		return nil, err
	}
	b := quic.AppendVarint(nil, streamControl)
	// The dynamic table capacity and blocked streams are left at their
	// default of zero, which is what keeps the QPACK streams silent.
	b = appendSettings(b, [2]uint64{settingMaxFieldSectionSize, uint64(maxFieldSection)})
	return s, s.Write(b, false)
}

// uniStream is a unidirectional stream the peer opened.
type uniStream struct {
	peer   *peerStreams
	s      *quic.Stream
	typ    int64
	head   []byte
	parser frameParser
	// fail ends the connection; it is how errors on the stream are
	// reported.
	fail func(error)
}

func newUniStream(peer *peerStreams, s *quic.Stream, fail func(error)) *uniStream {
	return &uniStream{peer: peer, s: s, typ: -1, fail: fail, parser: frameParser{maxFrame: 16 << 10}}
}

func (u *uniStream) feed(data []byte, fin bool) {
	if err := u.handle(data, fin); err != nil {
		u.fail(err)
	}
}

func (u *uniStream) handle(data []byte, fin bool) error {
	if u.typ < 0 {
		u.head = append(u.head, data...)
		typ, n := quic.ReadVarint(u.head)
		if n == 0 {
			return nil
		}
		u.typ = int64(typ)
		data = u.head[n:]
		u.head = nil
		p := u.peer
		switch typ {
		case streamControl:
			if p.control {
				return connErr(ErrCodeStreamCreationError, "second control stream")
			}
			p.control = true
		case streamQPACKEncoder:
			if p.encoder {
				return connErr(ErrCodeStreamCreationError, "second QPACK encoder stream")
			}
			p.encoder = true
		case streamQPACKDecoder:
			if p.decoder {
				return connErr(ErrCodeStreamCreationError, "second QPACK decoder stream")
			}
			p.decoder = true
		case streamPush:
			// Push is never enabled: no MAX_PUSH_ID is sent, and a client
			// may not push at all.
			return connErr(ErrCodeIDError, "push stream without MAX_PUSH_ID")
		default:
			// Unknown stream types are for extensions; nothing here reads
			// them (RFC 9114 section 6.2).
			u.s.StopSending(uint64(ErrCodeStreamCreationError))
			return nil
		}
	}
	switch u.typ {
	case streamControl:
		if err := u.parser.feed(data, func([]byte) error {
			return connErr(ErrCodeFrameUnexpected, "DATA on the control stream")
		}, u.controlFrame); err != nil {
			return err
		}
	case streamQPACKEncoder:
		// This side allows no dynamic table, so the peer may only set the
		// capacity to zero; anything it inserts is an error (RFC 9204
		// section 4.3).
		if err := u.encoderInstructions(data); err != nil {
			return err
		}
	case streamQPACKDecoder:
		// Every field section sent from here is encoded without the
		// dynamic table, so the peer has nothing to acknowledge.
	default:
		return nil
	}
	if fin {
		return connErr(ErrCodeClosedCriticalStream, "critical stream closed")
	}
	return nil
}

// encoderInstructions checks what the peer sends on its QPACK encoder
// stream. This side sets a dynamic table capacity of zero, so the only
// instruction the peer may send is one setting the capacity to zero; an
// insertion or a duplicate refers to a table that does not exist.
func (u *uniStream) encoderInstructions(data []byte) error {
	u.head = append(u.head, data...)
	for len(u.head) > 0 {
		b := u.head[0]
		if b&0xe0 != 0x20 {
			// Insert With Name Reference, Insert With Literal Name or
			// Duplicate, none of which a table of no capacity can hold.
			return connErr(ErrCodeQPACKEncoderStreamError, "QPACK insertion with a dynamic table capacity of zero")
		}
		capacity, n, ok := readPrefixedInt(u.head, 5)
		if !ok {
			// The instruction is not all here yet.
			return nil
		}
		if capacity != 0 {
			return connErr(ErrCodeQPACKEncoderStreamError, "QPACK dynamic table capacity above the maximum of zero")
		}
		u.head = u.head[n:]
	}
	u.head = nil
	return nil
}

// readPrefixedInt reads an integer with an n-bit prefix from the front of b
// (RFC 7541 section 5.1), reporting how many bytes it took and whether all
// of it was there.
func readPrefixedInt(b []byte, n uint8) (uint64, int, bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	mask := uint64(1)<<n - 1
	v := uint64(b[0]) & mask
	if v < mask {
		return v, 1, true
	}
	var shift uint
	for i := 1; i < len(b); i++ {
		v += uint64(b[i]&0x7f) << shift
		if b[i]&0x80 == 0 {
			return v, i + 1, true
		}
		if shift += 7; shift >= 56 {
			return 0, 0, true
		}
	}
	return 0, 0, false
}

func (u *uniStream) controlFrame(typ uint64, payload []byte) error {
	p := u.peer
	if !p.settings {
		if typ != frameSettings {
			return connErr(ErrCodeMissingSettings, "control stream does not start with SETTINGS")
		}
		p.settings = true
		maxField, err := parseSettings(payload)
		p.maxFieldSection = maxField
		return err
	}
	switch typ {
	case frameSettings:
		return connErr(ErrCodeFrameUnexpected, "second SETTINGS")
	case frameGoAway:
		id, n := quic.ReadVarint(payload)
		if n == 0 || n != len(payload) {
			return connErr(ErrCodeFrameError, "malformed GOAWAY")
		}
		// A server goes away at a request stream, a client at a push ID,
		// and neither may raise what it named before (RFC 9114 section
		// 5.2).
		if p.isClient && id&0x3 != 0 {
			return connErr(ErrCodeIDError, "GOAWAY with a stream that is not a request stream")
		}
		if p.goneAway && id > p.goAwayID {
			return connErr(ErrCodeIDError, "GOAWAY raising the identifier it named before")
		}
		p.goneAway, p.goAwayID = true, id
		if p.onGoAway != nil {
			return p.onGoAway(id)
		}
	case frameMaxPushID:
		id, n := quic.ReadVarint(payload)
		if n == 0 || n != len(payload) {
			return connErr(ErrCodeFrameError, "malformed MAX_PUSH_ID")
		}
		if p.isClient {
			return connErr(ErrCodeFrameUnexpected, "MAX_PUSH_ID from a server")
		}
		if p.maxPushID > 0 && id < p.maxPushID {
			return connErr(ErrCodeIDError, "MAX_PUSH_ID lowering what it granted before")
		}
		p.maxPushID = id
	case frameCancelPush:
		if _, n := quic.ReadVarint(payload); n == 0 || n != len(payload) {
			return connErr(ErrCodeFrameError, "malformed CANCEL_PUSH")
		}
		// Nothing here promises a push, so there is none to cancel.
		return connErr(ErrCodeIDError, "CANCEL_PUSH without a promised push")
	case frameData, frameHeaders, framePushPromise:
		return connErr(ErrCodeFrameUnexpected, "request frame on the control stream")
	default:
		if reservedFrame(typ) {
			return connErr(ErrCodeFrameUnexpected, "HTTP/2 frame type")
		}
	}
	return nil
}

// closeWith ends a QUIC connection with the code an error calls for.
func closeWith(qc *quic.Conn, err error) {
	var ce *connError
	switch {
	case errors.As(err, &ce):
		qc.Close(uint64(ce.code), ce.reason)
	case errors.Is(err, qpack.ErrDecompression):
		qc.Close(uint64(ErrCodeQPACKDecompressionFailed), err.Error())
	default:
		qc.Close(uint64(ErrCodeInternalError), err.Error())
	}
}

// abortStream ends both directions of a request stream with code.
func abortStream(s *quic.Stream, code ErrorCode) {
	s.StopSending(uint64(code))
	s.Reset(uint64(code))
}
