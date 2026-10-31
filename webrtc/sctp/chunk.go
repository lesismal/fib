package sctp

import (
	"encoding/binary"
	"hash/crc32"
)

// Chunk types this package sends or understands (RFC 9260 section 3.2,
// RFC 6525 for RE-CONFIG, RFC 3758 for FORWARD-TSN).
const (
	chunkData             = 0
	chunkInit             = 1
	chunkInitAck          = 2
	chunkSack             = 3
	chunkHeartbeat        = 4
	chunkHeartbeatAck     = 5
	chunkAbort            = 6
	chunkShutdown         = 7
	chunkShutdownAck      = 8
	chunkError            = 9
	chunkCookieEcho       = 10
	chunkCookieAck        = 11
	chunkShutdownComplete = 14
	chunkReconfig         = 130
	chunkForwardTSN       = 192
)

// DATA chunk flags.
const (
	flagEnd       = 1
	flagBegin     = 2
	flagUnordered = 4
	flagImmediate = 8
)

// flagTagReflected is the T bit of ABORT and SHUTDOWN COMPLETE: the packet
// carries the sender's own verification tag, as it has none of the peer's.
const flagTagReflected = 1

// Parameter types of INIT, INIT ACK, HEARTBEAT and RE-CONFIG.
const (
	paramHeartbeatInfo       = 1
	paramStateCookie         = 7
	paramOutgoingReset       = 13
	paramIncomingReset       = 14
	paramSSNTSNReset         = 15
	paramReconfigResponse    = 16
	paramAddOutgoingStreams  = 17
	paramAddIncomingStreams  = 18
	paramSupportedExtensions = 0x8008
	paramForwardTSNSupported = 0xc000
)

// Error causes this package sends.
const (
	causeInvalidStream      = 1
	causeUnrecognizedChunk  = 6
	causeNoUserData         = 9
	causeUserInitiatedAbort = 12
	causeProtocolViolation  = 13
)

// RE-CONFIG response results (RFC 6525 section 4.4).
const (
	reconfigSuccessNothing   = 0
	reconfigSuccessPerformed = 1
	reconfigDenied           = 2
	reconfigErrorBadSeq      = 5
	reconfigInProgress       = 6
)

const (
	headerSize      = 12
	chunkHeaderSize = 4
	// dataHeaderSize is a DATA chunk's chunk header and fields before its
	// user data.
	dataHeaderSize = 16
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var zeroChecksum [4]byte

// checksum is the CRC32c of packet b with its checksum field taken as zero
// (RFC 9260 appendix A), which spares a packet being checked a copy.
func checksum(b []byte) uint32 {
	c := crc32.Update(0, castagnoli, b[:8])
	c = crc32.Update(c, castagnoli, zeroChecksum[:])
	return crc32.Update(c, castagnoli, b[headerSize:])
}

// parsePacket checks packet b's length and checksum and returns its
// verification tag and the chunks that follow the common header.
func parsePacket(b []byte) (vtag uint32, chunks []byte, err error) {
	if len(b) < headerSize+chunkHeaderSize {
		return 0, nil, ErrMalformed
	}
	if binary.LittleEndian.Uint32(b[8:]) != checksum(b) {
		return 0, nil, ErrChecksum
	}
	return binary.BigEndian.Uint32(b[4:]), b[headerSize:], nil
}

// nextChunk splits off the first chunk of b: its type, flags and value, and
// what follows its padding. The last chunk of a packet may leave its padding
// out.
func nextChunk(b []byte) (typ, flags uint8, value, rest []byte, err error) {
	if len(b) < chunkHeaderSize {
		return 0, 0, nil, nil, ErrMalformed
	}
	n := int(binary.BigEndian.Uint16(b[2:]))
	if n < chunkHeaderSize || n > len(b) {
		return 0, 0, nil, nil, ErrMalformed
	}
	p := (n + 3) &^ 3
	if p > len(b) {
		p = len(b)
	}
	return b[0], b[1], b[chunkHeaderSize:n], b[p:], nil
}

// nextParam splits off the first TLV parameter of b, the same way.
func nextParam(b []byte) (typ uint16, value, rest []byte, err error) {
	if len(b) < 4 {
		return 0, nil, nil, ErrMalformed
	}
	n := int(binary.BigEndian.Uint16(b[2:]))
	if n < 4 || n > len(b) {
		return 0, nil, nil, ErrMalformed
	}
	p := (n + 3) &^ 3
	if p > len(b) {
		p = len(b)
	}
	return binary.BigEndian.Uint16(b), b[4:n], b[p:], nil
}

// initChunk is what an INIT or INIT ACK says.
type initChunk struct {
	tag        uint32
	rwnd       uint32
	outStreams uint16
	inStreams  uint16
	tsn        uint32
	cookie     []byte
	forwardTSN bool
	reconfig   bool
}

func parseInit(v []byte, c *initChunk) error {
	if len(v) < 16 {
		return ErrMalformed
	}
	*c = initChunk{
		tag:        binary.BigEndian.Uint32(v),
		rwnd:       binary.BigEndian.Uint32(v[4:]),
		outStreams: binary.BigEndian.Uint16(v[8:]),
		inStreams:  binary.BigEndian.Uint16(v[10:]),
		tsn:        binary.BigEndian.Uint32(v[12:]),
	}
	// RFC 9260 section 3.3.2: a zero tag or stream count is invalid.
	if c.tag == 0 || c.outStreams == 0 || c.inStreams == 0 {
		return ErrMalformed
	}
	for p := v[16:]; len(p) > 0; {
		typ, pv, rest, err := nextParam(p)
		if err != nil {
			return err
		}
		p = rest
		switch typ {
		case paramStateCookie:
			c.cookie = pv
		case paramForwardTSNSupported:
			c.forwardTSN = true
		case paramSupportedExtensions:
			for _, t := range pv {
				switch t {
				case chunkForwardTSN:
					c.forwardTSN = true
				case chunkReconfig:
					c.reconfig = true
				}
			}
		}
	}
	return nil
}

// sackChunk is a SACK, its gap blocks and duplicate TSNs left in wire form
// so reading one allocates nothing.
type sackChunk struct {
	cum  uint32
	rwnd uint32
	gaps []byte
	dups []byte
}

func parseSack(v []byte, s *sackChunk) error {
	if len(v) < 12 {
		return ErrMalformed
	}
	ng := int(binary.BigEndian.Uint16(v[8:]))
	nd := int(binary.BigEndian.Uint16(v[10:]))
	if len(v) < 12+4*ng+4*nd {
		return ErrMalformed
	}
	s.cum = binary.BigEndian.Uint32(v)
	s.rwnd = binary.BigEndian.Uint32(v[4:])
	s.gaps = v[12 : 12+4*ng]
	s.dups = v[12+4*ng : 12+4*ng+4*nd]
	return nil
}

// tsnLT and its kin compare TSNs in serial number arithmetic (RFC 1982),
// which a 32-bit TSN that wraps needs.
func tsnLT(a, b uint32) bool { return int32(a-b) < 0 }
func tsnLE(a, b uint32) bool { return int32(a-b) <= 0 }
func tsnGT(a, b uint32) bool { return int32(a-b) > 0 }

// ssnLE compares stream sequence numbers the same way.
func ssnLE(a, b uint16) bool { return int16(a-b) <= 0 }
