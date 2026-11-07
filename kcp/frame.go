package kcp

import "encoding/binary"

// framer turns a session's KCP packets into datagrams and back: kcp-go's
// FEC shards, when DataShards and ParityShards are set, inside its crypt,
// when Crypt is.
type framer struct {
	crypt   *Crypt
	enc     *fecEncoder
	dec     *fecDecoder
	scratch []byte
	// recovered counts the packets the FEC recovered.
	recovered uint64
}

func newFramer(crypt *Crypt, data, parity int) (*framer, error) {
	f := &framer{crypt: crypt}
	if data > 0 || parity > 0 {
		var err error
		if f.enc, err = newFECEncoder(data, parity); err != nil {
			return nil, err
		}
		if f.dec, err = newFECDecoder(data, parity); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// overhead is how many bytes the framing adds to a KCP packet.
func (f *framer) overhead() int {
	n := f.crypt.headerSize() + f.crypt.trailerSize()
	if f.enc != nil {
		n += fecHeaderSizePlus2
	}
	return n
}

// encode passes the datagrams of a KCP packet to emit, valid only for the
// call: the packet's, then any parity it completes.
func (f *framer) encode(p []byte, nowMs int64, emit func([]byte)) {
	ch := f.crypt.headerSize()
	b := f.scratch[:0]
	b = append(b, make([]byte, ch)...)
	if f.enc != nil {
		b = append(b, make([]byte, fecHeaderSizePlus2)...)
	}
	b = append(b, p...)
	b = append(b, make([]byte, f.crypt.trailerSize())...)[:len(b)]
	var parity [][]byte
	if f.enc != nil {
		parity = f.enc.add(b[ch:], nowMs)
	}
	f.scratch = b
	emit(f.crypt.seal(b))
	for _, sh := range parity {
		b = f.scratch[:0]
		b = append(b, make([]byte, ch)...)
		b = append(b, sh...)
		b = append(b, make([]byte, f.crypt.trailerSize())...)[:len(b)]
		f.scratch = b
		emit(f.crypt.seal(b))
	}
}

// open decrypts a datagram in place, returning what the crypt wraps, or
// nil for one that fails its check.
func open(crypt *Crypt, b []byte) []byte {
	b, err := crypt.open(b)
	if err != nil || len(b) < fecHeaderSize {
		return nil
	}
	return b
}

// decode passes the KCP packets of an opened datagram to input: its own,
// and those its FEC recovers. A datagram may be of FEC shards when the
// session has no FEC of its own; its data shards are read and its parity
// ignored.
func (f *framer) decode(b []byte, input func([]byte)) {
	switch binary.LittleEndian.Uint16(b[4:]) {
	case typeData, typeParity:
		if len(b) < fecHeaderSizePlus2 {
			return
		}
		if binary.LittleEndian.Uint16(b[4:]) == typeData {
			input(b[fecHeaderSizePlus2:])
		}
		if f.dec == nil {
			return
		}
		for _, r := range f.dec.decode(b) {
			if sz := int(binary.LittleEndian.Uint16(r)); sz >= 2 && sz <= len(r) {
				f.recovered++
				input(r[2:sz])
			}
		}
	case typeOOB:
	default:
		input(b)
	}
}
