package rtpcodec

import (
	"errors"

	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/internal/bitio"
)

// ErrLATM is what the MP4A-LATM depacketizer reports for an AudioMuxElement
// or StreamMuxConfig it cannot read: malformed, or using what the package
// leaves out (several programs or layers, frame length types other than 0).
var ErrLATM = errors.New("rtpcodec: unsupported or malformed LATM")

// streamMuxConfig is the part of a StreamMuxConfig (ISO/IEC 14496-3 section
// 1.7.3) a depacketizer needs: the one stream's AudioSpecificConfig,
// byte-aligned, and the framing of its AudioMuxElements.
type streamMuxConfig struct {
	asc          []byte
	numSubFrames int
}

// parseStreamMuxConfig parses the StreamMuxConfig of an out-of-band
// config= parameter.
func parseStreamMuxConfig(b []byte) (*streamMuxConfig, error) {
	r := bitio.NewReader(b)
	return readStreamMuxConfig(&r)
}

// latmValue reads LatmGetValue().
func latmValue(r *bitio.Reader) int {
	n := int(r.Bits(2))
	v := 0
	for i := 0; i <= n; i++ {
		v = v<<8 | int(r.Bits(8))
	}
	return v
}

func readStreamMuxConfig(r *bitio.Reader) (*streamMuxConfig, error) {
	version := r.Bit()
	if version == 1 {
		if r.Bit() == 1 { // audioMuxVersionA
			return nil, ErrLATM
		}
		latmValue(r) // taraBufferFullness
	}
	c := &streamMuxConfig{}
	r.Bit() // allStreamsSameTimeFraming
	c.numSubFrames = int(r.Bits(6))
	if r.Bits(4) != 0 || r.Bits(3) != 0 { // numProgram, numLayer
		return nil, ErrLATM
	}
	if version == 1 {
		ascLen := latmValue(r)
		start := *r
		n, err := ascBits(r)
		if err != nil || n > ascLen {
			return nil, ErrLATM
		}
		c.asc = copyBits(&start, n)
		r.Skip(ascLen - n)
	} else {
		start := *r
		n, err := ascBits(r)
		if err != nil {
			return nil, ErrLATM
		}
		c.asc = copyBits(&start, n)
	}
	if r.Bits(3) != 0 { // frameLengthType
		return nil, ErrLATM
	}
	r.Bits(8)     // latmBufferFullness
	if r.Flag() { // otherDataPresent: its length, which is not needed
		if version == 1 {
			latmValue(r)
		} else {
			for i := 0; r.Flag() && i < 8; i++ {
				r.Bits(8)
			}
			r.Bits(8)
		}
	}
	if r.Flag() { // crcCheckPresent
		r.Bits(8)
	}
	if r.Err() != nil {
		return nil, ErrLATM
	}
	var cfg aac.Config
	if cfg.Unmarshal(c.asc) != nil {
		return nil, ErrLATM
	}
	return c, nil
}

// ascBits reads an AudioSpecificConfig of the object types that carry a
// GASpecificConfig, and returns its length in bits: inside a
// StreamMuxConfig it is not byte-aligned, and nothing gives its length.
func ascBits(r *bitio.Reader) (int, error) {
	start := r.Pos()
	objectType := func() uint32 {
		t := r.Bits(5)
		if t == 31 {
			t = 32 + r.Bits(6)
		}
		return t
	}
	rate := func() {
		if r.Bits(4) == 15 {
			r.Bits(24)
		}
	}
	aot := objectType()
	rate()
	ch := r.Bits(4)
	if aot == aac.ObjectTypeSBR || aot == aac.ObjectTypePS {
		rate()
		aot = objectType()
	}
	switch aot {
	case 1, 2, 3, 4, 6, 7, 17, 19, 20, 21, 22, 23:
	default:
		return 0, ErrLATM
	}
	if ch == 0 {
		// A program_config_element, which is not read.
		return 0, ErrLATM
	}
	r.Bit() // frameLengthFlag
	if r.Flag() {
		r.Bits(14) // coreCoderDelay
	}
	ext := r.Flag()
	if aot == 6 || aot == 20 {
		r.Bits(3) // layerNr
	}
	if ext {
		if aot == 22 {
			r.Bits(16) // numOfSubFrame, layer_length
		}
		if aot == 17 || aot == 19 || aot == 20 || aot == 23 {
			r.Bits(3) // the resilience flags
		}
		r.Bit() // extensionFlag3
	}
	if r.Err() != nil {
		return 0, ErrLATM
	}
	return r.Pos() - start, nil
}

// copyBits reads n bits from r into bytes, the last padded with zeros.
func copyBits(r *bitio.Reader, n int) []byte {
	w := bitio.NewWriter(make([]byte, 0, (n+7)/8))
	for n > 0 {
		k := min(n, 32)
		w.Bits(uint64(r.Bits(k)), k)
		n -= k
	}
	return w.Bytes()
}

// appendStreamMuxConfig appends the StreamMuxConfig of one AAC stream with
// the AudioSpecificConfig asc: version 0, one subframe per element, frame
// length type 0, no other data and no CRC.
func appendStreamMuxConfig(dst, asc []byte) ([]byte, error) {
	r := bitio.NewReader(asc)
	n, err := ascBits(&r)
	if err != nil {
		return dst, err
	}
	w := bitio.NewWriter(dst)
	w.Bits(0, 1)     // audioMuxVersion
	w.Bits(1, 1)     // allStreamsSameTimeFraming
	w.Bits(0, 6+4+3) // numSubFrames, numProgram, numLayer
	r = bitio.NewReader(asc)
	for n > 0 {
		k := min(n, 32)
		w.Bits(uint64(r.Bits(k)), k)
		n -= k
	}
	w.Bits(0, 3)    // frameLengthType
	w.Bits(0xff, 8) // latmBufferFullness
	w.Bits(0, 2)    // otherDataPresent, crcCheckPresent
	return w.Bytes(), nil
}

// readAudioMuxElement reads one AudioMuxElement and calls emit with the
// payload of each of its subframes. With cpresent it starts with
// useSameStreamMux and maybe a new StreamMuxConfig, which replaces *config.
func readAudioMuxElement(b []byte, cpresent bool, config **streamMuxConfig, emit func(au []byte)) error {
	r := bitio.NewReader(b)
	if cpresent && !r.Flag() {
		c, err := readStreamMuxConfig(&r)
		if err != nil {
			return err
		}
		*config = c
	}
	c := *config
	if c == nil {
		return ErrLATM
	}
	var aus [][]byte
	for i := 0; i <= c.numSubFrames; i++ {
		n := 0
		for {
			v := int(r.Bits(8))
			n += v
			if v != 255 || r.Err() != nil {
				break
			}
		}
		if r.Err() != nil || n*8 > r.Left() {
			return ErrLATM
		}
		aus = append(aus, copyBits(&r, n*8))
	}
	for _, au := range aus {
		emit(au)
	}
	return nil
}

// appendAudioMuxElement appends the AudioMuxElement of one access unit, with
// its StreamMuxConfig out of band.
func appendAudioMuxElement(dst, au []byte) []byte {
	n := len(au)
	for ; n >= 255; n -= 255 {
		dst = append(dst, 255)
	}
	dst = append(dst, byte(n))
	return append(dst, au...)
}
