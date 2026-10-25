// Package inflate decodes DEFLATE (RFC 1951), and the zlib (RFC 1950) and
// gzip (RFC 1952) formats around it, from whatever of the input has arrived
// into whatever room the caller has, and carries on from there on the next
// call: unlike compress/flate, it never reads from an io.Reader, so it never
// needs a source that blocks until more input comes. The HTTP/1 transfer
// codings are decoded with it, as body bytes are taken off a connection that
// never waits for its peer.
//
// What it costs to hold a Decoder is its 32 KiB window and a few Huffman
// tables, whatever the input inflates to: the caller decides how much output
// it takes at a time, and a Decoder never produces more than it is given room
// for.
package inflate

import (
	"errors"
	"hash/crc32"
	"io"
)

// Format is the framing around the DEFLATE data.
type Format uint8

const (
	// Raw is DEFLATE data with no framing.
	Raw Format = iota
	// Zlib is the zlib format, RFC 1950, with an Adler-32 checksum.
	Zlib
	// Gzip is the gzip format, RFC 1952, with a CRC-32 checksum. A stream
	// may hold more than one member, one after another, as gzip and Go's
	// compress/gzip read it.
	Gzip
	// ZlibOrRaw is Zlib, or Raw when the first two bytes are not a zlib
	// header: what HTTP's "deflate" coding names is zlib, but some senders
	// have always sent raw DEFLATE under that name, and the two are told
	// apart the way browsers tell them apart.
	ZlibOrRaw
)

var (
	// ErrCorrupt is input that is not in the format.
	ErrCorrupt = errors.New("inflate: corrupt input")
	// ErrChecksum is input whose checksum or length does not match what it
	// decoded to.
	ErrChecksum = errors.New("inflate: checksum mismatch")
	// ErrTrailingData is input past the end of a zlib or raw stream, or past
	// a gzip member that does not begin another.
	ErrTrailingData = errors.New("inflate: data after the end of the stream")
)

// errMore is what the steps of a decode report when they need input that has
// not arrived; Decode turns it into io.ErrUnexpectedEOF for input that has
// all been given.
var errMore = errors.New("inflate: more input needed")

const (
	windowSize = 1 << 15
	windowMask = windowSize - 1
)

// The states of a Decoder.
const (
	stateStart uint8 = iota
	stateGzipHeader
	stateZlibHeader
	stateBlockHeader
	stateStoredLength
	stateStored
	stateDynamicCounts
	stateDynamicCodeLengths
	stateDynamicLengths
	stateCodes
	stateTrailer
	stateEnd
)

// Decoder decodes one stream. The zero value is not usable; see NewDecoder.
type Decoder struct {
	format Format
	state  uint8
	err    error

	// bits holds nbits bits of input taken from src and not yet decoded,
	// low bits first, as DEFLATE packs them. Input moves here as it is
	// needed, and stays here between calls: what Decode reports it used is
	// what has moved, whether or not it has been decoded yet.
	bits  uint64
	nbits uint

	// The buffers of the call in progress: src from pos on is input not yet
	// taken, dst up to n is output written, and mark is where the output
	// not yet added to the checksum begins.
	src  []byte
	pos  int
	dst  []byte
	n    int
	mark int

	// step and count say how far a header or trailer has got, flags is a
	// gzip header's, head collects the bytes of a header or trailer that are
	// read before they are looked at, and headCRC is the CRC-32 of the gzip
	// header so far.
	step    uint8
	flags   byte
	count   int
	head    [10]byte
	headCRC uint32

	// last says the block being decoded is the stream's last, and stored is
	// what a stored block has still to copy.
	last   bool
	stored int
	// lit and dist are the codes of the block being decoded: the fixed
	// ones, or dynLit and dynDist built from its header.
	lit, dist        *huffman
	dynLit, dynDist  huffman
	codeLengthCode   huffman
	nlen, ndist      int
	ncode, have      int
	codeLengths      [19]uint8
	lengths          [maxLitCodes + maxDistCodes]uint8
	copyLen, copyDst int

	// window holds the last windowSize bytes of output, which a match may
	// copy from, and written counts the output of the stream, or of the
	// gzip member, so far.
	window  []byte
	written int64

	// crc and size are what a gzip member checks, adler what a zlib stream
	// does.
	crc   uint32
	size  uint32
	adler uint32
}

// NewDecoder returns a Decoder for a stream in format.
func NewDecoder(format Format) *Decoder {
	return &Decoder{format: format}
}

// Done reports whether the stream has ended: everything given has been
// decoded and checked, and a gzip stream is between members, which is where
// it may end.
func (d *Decoder) Done() bool { return d.state == stateEnd && d.err == nil }

// Decode decodes src into dst, and reports how much of dst it wrote and how
// much of src it used. final says src holds the rest of the input, all of
// it: a stream that does not end within it fails with io.ErrUnexpectedEOF.
//
// Decode returns when dst is full, when it has used all of src, or when the
// stream has ended, which Done then reports. Input it used is the Decoder's
// from then on, whether or not it decoded it yet, so the next call is given
// what follows it. An error is the Decoder's last word: every later call
// returns it again.
func (d *Decoder) Decode(dst, src []byte, final bool) (n, used int, err error) {
	if d.err != nil {
		return 0, 0, d.err
	}
	d.dst, d.src, d.n, d.pos, d.mark = dst, src, 0, 0, 0
	err = d.run()
	d.sum()
	n, used = d.n, d.pos
	d.dst, d.src = nil, nil
	if err == errMore {
		err = nil
		if final {
			err = io.ErrUnexpectedEOF
		}
	}
	d.err = err
	return n, used, err
}

// run decodes until it needs input that is not there, dst is full, or the
// stream ends.
func (d *Decoder) run() error {
	for {
		var err error
		state := d.state
		switch state {
		case stateStart:
			switch d.format {
			case Gzip:
				d.state = stateGzipHeader
			case Zlib, ZlibOrRaw:
				d.state = stateZlibHeader
			default:
				d.state = stateBlockHeader
			}
			d.step, d.count = 0, 0
			continue
		case stateGzipHeader:
			err = d.gzipHeader()
		case stateZlibHeader:
			err = d.zlibHeader()
		case stateBlockHeader:
			err = d.blockHeader()
		case stateStoredLength:
			err = d.storedLength()
		case stateStored:
			err = d.storedData()
		case stateDynamicCounts:
			err = d.dynamicCounts()
		case stateDynamicCodeLengths:
			err = d.dynamicCodeLengths()
		case stateDynamicLengths:
			err = d.dynamicLengths()
		case stateCodes:
			err = d.codes()
		case stateTrailer:
			err = d.trailer()
		case stateEnd:
			if d.nbits < 8 && d.pos == len(d.src) {
				return nil
			}
			if d.format != Gzip {
				return ErrTrailingData
			}
			// Another member follows: it starts afresh.
			d.state, d.step, d.count = stateGzipHeader, 0, 0
			d.crc, d.size, d.written = 0, 0, 0
			continue
		}
		if err != nil {
			return err
		}
		if d.state == state && (state == stateCodes || state == stateStored) {
			// Those return without moving on only when dst is full.
			return nil
		}
	}
}

// fill moves input into the bit buffer while it has room for another byte.
func (d *Decoder) fill() {
	for d.nbits <= 56 && d.pos < len(d.src) {
		d.bits |= uint64(d.src[d.pos]) << d.nbits
		d.pos++
		d.nbits += 8
	}
}

// need reports whether n bits are in the bit buffer, moving input there as
// it can.
func (d *Decoder) need(n uint) bool {
	if d.nbits < n {
		d.fill()
	}
	return d.nbits >= n
}

func (d *Decoder) drop(n uint) {
	d.bits >>= n
	d.nbits -= n
}

// align drops the bits up to the next byte boundary.
func (d *Decoder) align() { d.drop(d.nbits & 7) }

// readByte takes the next byte of a header or trailer: from the bit buffer,
// whose bits are aligned to a byte by then, and then from the input.
func (d *Decoder) readByte() (byte, bool) {
	if d.nbits >= 8 {
		b := byte(d.bits)
		d.drop(8)
		return b, true
	}
	if d.pos < len(d.src) {
		b := d.src[d.pos]
		d.pos++
		return b, true
	}
	return 0, false
}

// sum adds the output written since the last call to the checksums.
func (d *Decoder) sum() {
	if d.mark == d.n {
		return
	}
	out := d.dst[d.mark:d.n]
	d.mark = d.n
	switch d.format {
	case Gzip:
		d.crc = crc32.Update(d.crc, crc32.IEEETable, out)
		d.size += uint32(len(out))
	case Zlib, ZlibOrRaw:
		d.adler = adler32(d.adler, out)
	}
}

// gzip header flags.
const (
	gzipText    = 1 << 0
	gzipHCRC    = 1 << 1
	gzipExtra   = 1 << 2
	gzipName    = 1 << 3
	gzipComment = 1 << 4
)

// gzipHeader reads a gzip member's header, a byte at a time.
func (d *Decoder) gzipHeader() error {
	for {
		switch d.step {
		case 0:
			for d.count < 10 {
				b, ok := d.readByte()
				if !ok {
					return errMore
				}
				d.head[d.count] = b
				d.count++
			}
			if d.head[0] != 0x1f || d.head[1] != 0x8b || d.head[2] != 8 || d.head[3]&0xe0 != 0 {
				return ErrCorrupt
			}
			d.flags = d.head[3]
			d.headCRC = crc32.ChecksumIEEE(d.head[:10])
			d.step, d.count = 1, 0
		case 1:
			if d.flags&gzipExtra == 0 {
				d.step = 3
				continue
			}
			for d.count < 2 {
				b, ok := d.headerByte()
				if !ok {
					return errMore
				}
				d.head[d.count] = b
				d.count++
			}
			d.count = int(d.head[0]) | int(d.head[1])<<8
			d.step = 2
		case 2:
			for d.count > 0 {
				if _, ok := d.headerByte(); !ok {
					return errMore
				}
				d.count--
			}
			d.step = 3
		case 3, 4:
			flag := byte(gzipName)
			if d.step == 4 {
				flag = gzipComment
			}
			if d.flags&flag != 0 {
				for {
					b, ok := d.headerByte()
					if !ok {
						return errMore
					}
					if b == 0 {
						break
					}
				}
			}
			d.step++
			d.count = 0
		default:
			if d.flags&gzipHCRC != 0 {
				for d.count < 2 {
					b, ok := d.readByte()
					if !ok {
						return errMore
					}
					d.head[d.count] = b
					d.count++
				}
				if uint16(d.head[0])|uint16(d.head[1])<<8 != uint16(d.headCRC) {
					return ErrChecksum
				}
			}
			d.state = stateBlockHeader
			return nil
		}
	}
}

// headerByte is readByte for a byte the gzip header's CRC covers.
func (d *Decoder) headerByte() (byte, bool) {
	b, ok := d.readByte()
	if ok {
		d.headCRC = crc32.Update(d.headCRC, crc32.IEEETable, []byte{b})
	}
	return b, ok
}

// zlibHeader reads a zlib header, or, for ZlibOrRaw, finds there is none.
func (d *Decoder) zlibHeader() error {
	for d.count < 2 {
		b, ok := d.readByte()
		if !ok {
			return errMore
		}
		d.head[d.count] = b
		d.count++
	}
	cmf, flg := d.head[0], d.head[1]
	if cmf&0x0f != 8 || cmf>>4 > 7 || (uint16(cmf)<<8|uint16(flg))%31 != 0 {
		if d.format != ZlibOrRaw {
			return ErrCorrupt
		}
		// Raw DEFLATE: the two bytes are its first. Nothing else is in the
		// bit buffer yet, since they were the first two of the stream.
		d.format = Raw
		d.bits = uint64(cmf) | uint64(flg)<<8
		d.nbits = 16
	} else if flg&0x20 != 0 {
		// A preset dictionary, which HTTP has no way to name.
		return ErrCorrupt
	} else {
		d.format = Zlib
	}
	d.adler = 1
	d.state = stateBlockHeader
	return nil
}

func (d *Decoder) blockHeader() error {
	if !d.need(3) {
		return errMore
	}
	d.last = d.bits&1 != 0
	kind := (d.bits >> 1) & 3
	d.drop(3)
	switch kind {
	case 0:
		d.state = stateStoredLength
	case 1:
		d.lit, d.dist = &fixedLit, &fixedDist
		d.state = stateCodes
	case 2:
		d.state = stateDynamicCounts
	default:
		return ErrCorrupt
	}
	return nil
}

func (d *Decoder) storedLength() error {
	d.align()
	if !d.need(32) {
		return errMore
	}
	length := uint16(d.bits)
	if uint16(d.bits>>16) != ^length {
		return ErrCorrupt
	}
	d.drop(32)
	d.stored = int(length)
	d.state = stateStored
	return nil
}

func (d *Decoder) storedData() error {
	for d.stored > 0 {
		if d.n == len(d.dst) {
			return nil
		}
		if d.nbits >= 8 {
			d.put(byte(d.bits))
			d.drop(8)
			d.stored--
			continue
		}
		if d.pos == len(d.src) {
			return errMore
		}
		k := min(d.stored, len(d.dst)-d.n, len(d.src)-d.pos)
		d.record(d.src[d.pos : d.pos+k])
		copy(d.dst[d.n:], d.src[d.pos:d.pos+k])
		d.n += k
		d.pos += k
		d.stored -= k
	}
	d.endBlock()
	return nil
}

// put writes a byte of output.
func (d *Decoder) put(b byte) {
	if d.window == nil {
		d.window = make([]byte, windowSize)
	}
	d.window[d.written&windowMask] = b
	d.written++
	d.dst[d.n] = b
	d.n++
}

// record adds p, written to dst by the caller, to the window.
func (d *Decoder) record(p []byte) {
	if d.window == nil {
		d.window = make([]byte, windowSize)
	}
	if len(p) > windowSize {
		d.written += int64(len(p) - windowSize)
		p = p[len(p)-windowSize:]
	}
	for len(p) > 0 {
		at := int(d.written & windowMask)
		k := copy(d.window[at:], p)
		p = p[k:]
		d.written += int64(k)
	}
}

// endBlock moves on from a block that has ended: to the next one, or to the
// trailer after the last.
func (d *Decoder) endBlock() {
	if !d.last {
		d.state = stateBlockHeader
		return
	}
	d.align()
	d.state, d.count = stateTrailer, 0
}

var codeLengthOrder = [19]uint8{16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15}

const (
	maxLitCodes  = 286
	maxDistCodes = 30
)

func (d *Decoder) dynamicCounts() error {
	if !d.need(14) {
		return errMore
	}
	d.nlen = int(d.bits&31) + 257
	d.ndist = int(d.bits>>5&31) + 1
	d.ncode = int(d.bits>>10&15) + 4
	d.drop(14)
	if d.nlen > maxLitCodes || d.ndist > maxDistCodes {
		return ErrCorrupt
	}
	d.have = 0
	d.codeLengths = [19]uint8{}
	d.state = stateDynamicCodeLengths
	return nil
}

func (d *Decoder) dynamicCodeLengths() error {
	for d.have < d.ncode {
		if !d.need(3) {
			return errMore
		}
		d.codeLengths[codeLengthOrder[d.have]] = uint8(d.bits & 7)
		d.drop(3)
		d.have++
	}
	if d.codeLengthCode.build(d.codeLengths[:]) != 0 {
		// The code length code has to be complete.
		return ErrCorrupt
	}
	d.have = 0
	d.state = stateDynamicLengths
	return nil
}

func (d *Decoder) dynamicLengths() error {
	total := d.nlen + d.ndist
	for d.have < total {
		if d.nbits < 14 {
			d.fill()
		}
		sym, n := d.codeLengthCode.decode(d.bits, d.nbits)
		if n == 0 {
			if sym == symInvalid {
				return ErrCorrupt
			}
			return errMore
		}
		if sym < 16 {
			d.drop(n)
			d.lengths[d.have] = uint8(sym)
			d.have++
			continue
		}
		var value uint8
		var extra uint
		repeat := 3
		switch sym {
		case 16:
			if d.have == 0 {
				return ErrCorrupt
			}
			value, extra = d.lengths[d.have-1], 2
		case 17:
			extra = 3
		default:
			extra, repeat = 7, 11
		}
		if d.nbits < n+extra {
			return errMore
		}
		repeat += int(d.bits >> n & (1<<extra - 1))
		d.drop(n + extra)
		if d.have+repeat > total {
			return ErrCorrupt
		}
		for range repeat {
			d.lengths[d.have] = value
			d.have++
		}
	}
	if d.lengths[256] == 0 {
		// No code for the end of the block.
		return ErrCorrupt
	}
	// An incomplete code is allowed only when it has a single code of one
	// bit, as zlib allows it.
	if left := d.dynLit.build(d.lengths[:d.nlen]); left < 0 || left > 0 && d.nlen != int(d.dynLit.count[0]+d.dynLit.count[1]) {
		return ErrCorrupt
	}
	if left := d.dynDist.build(d.lengths[d.nlen:total]); left < 0 || left > 0 && d.ndist != int(d.dynDist.count[0]+d.dynDist.count[1]) {
		return ErrCorrupt
	}
	d.lit, d.dist = &d.dynLit, &d.dynDist
	d.state = stateCodes
	return nil
}

var (
	lengthBase  = [29]uint16{3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258}
	lengthExtra = [29]uint8{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0}
	distBase    = [30]uint16{1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577}
	distExtra   = [30]uint8{0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13}
)

// codes decodes a compressed block's symbols until it ends, dst is full, or
// the input runs out. A symbol is decoded whole or not at all: its bits stay
// in the bit buffer until all of them are there, which they always can be,
// since one never takes more than 48 of them.
func (d *Decoder) codes() error {
	for {
		if d.copyLen > 0 {
			d.copyOut()
			if d.copyLen > 0 {
				return nil
			}
		}
		if d.n == len(d.dst) {
			return nil
		}
		if d.nbits < 48 {
			d.fill()
		}
		bits, nbits := d.bits, d.nbits
		sym, n := d.lit.decode(bits, nbits)
		if n == 0 {
			if sym == symInvalid {
				return ErrCorrupt
			}
			return errMore
		}
		if sym < 256 {
			d.drop(n)
			d.put(byte(sym))
			continue
		}
		if sym == 256 {
			d.drop(n)
			d.endBlock()
			return nil
		}
		sym -= 257
		if sym >= len(lengthBase) {
			return ErrCorrupt
		}
		bits >>= n
		nbits -= n
		extra := uint(lengthExtra[sym])
		if nbits < extra {
			return errMore
		}
		length := int(lengthBase[sym]) + int(bits&(1<<extra-1))
		bits >>= extra
		nbits -= extra
		dsym, dn := d.dist.decode(bits, nbits)
		if dn == 0 {
			if dsym == symInvalid {
				return ErrCorrupt
			}
			return errMore
		}
		if dsym >= len(distBase) {
			return ErrCorrupt
		}
		bits >>= dn
		nbits -= dn
		extra = uint(distExtra[dsym])
		if nbits < extra {
			return errMore
		}
		dist := int(distBase[dsym]) + int(bits&(1<<extra-1))
		if int64(dist) > d.written {
			return ErrCorrupt
		}
		d.bits, d.nbits = bits>>extra, nbits-extra
		d.copyLen, d.copyDst = length, dist
	}
}

// copyOut copies as much of the match in progress as dst has room for.
func (d *Decoder) copyOut() {
	k := min(d.copyLen, len(d.dst)-d.n)
	from := d.written - int64(d.copyDst)
	for i := range k {
		b := d.window[(from+int64(i))&windowMask]
		d.window[(d.written+int64(i))&windowMask] = b
		d.dst[d.n+i] = b
	}
	d.written += int64(k)
	d.n += k
	d.copyLen -= k
}

// trailer reads and checks what follows the last block.
func (d *Decoder) trailer() error {
	size := 0
	switch d.format {
	case Gzip:
		size = 8
	case Zlib:
		size = 4
	}
	for d.count < size {
		b, ok := d.readByte()
		if !ok {
			return errMore
		}
		d.head[d.count] = b
		d.count++
	}
	d.sum()
	h := d.head[:]
	switch d.format {
	case Gzip:
		crc := uint32(h[0]) | uint32(h[1])<<8 | uint32(h[2])<<16 | uint32(h[3])<<24
		size := uint32(h[4]) | uint32(h[5])<<8 | uint32(h[6])<<16 | uint32(h[7])<<24
		if crc != d.crc || size != d.size {
			return ErrChecksum
		}
	case Zlib:
		if uint32(h[0])<<24|uint32(h[1])<<16|uint32(h[2])<<8|uint32(h[3]) != d.adler {
			return ErrChecksum
		}
	}
	d.state = stateEnd
	return nil
}

// adler32 adds p to an Adler-32 checksum.
func adler32(adler uint32, p []byte) uint32 {
	const mod, nmax = 65521, 5552
	s1, s2 := adler&0xffff, adler>>16
	for len(p) > 0 {
		q := p[:min(len(p), nmax)]
		p = p[len(q):]
		for _, b := range q {
			s1 += uint32(b)
			s2 += s1
		}
		s1 %= mod
		s2 %= mod
	}
	return s2<<16 | s1
}
