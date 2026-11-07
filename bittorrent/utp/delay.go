package utp

const (
	// curDelaySize is how many of the latest delay samples the queuing
	// delay is the least of, as libutp's CUR_DELAY_SIZE.
	curDelaySize = 3
	// delayBaseHistory is how many minutes the base delay is the least
	// sample of, as libutp's DELAY_BASE_HISTORY: the clocks of the two ends
	// are not synchronized, so a one-way delay means something only next to
	// the least one seen lately.
	delayBaseHistory = 13
	// delayBaseStep is how long, in microseconds, each entry of the base
	// delay history covers.
	delayBaseStep = 60 * 1000 * 1000
)

// delayHist is libutp's DelayHist: the base delay of a direction, the
// least one-way delay seen in the last delayBaseHistory minutes, and the
// latest samples above it, whose least is the queuing delay LEDBAT
// steers by. Samples are 32-bit microsecond differences between two
// unsynchronized clocks, compared as numbers that wrap.
type delayHist struct {
	base        uint32
	cur         [curDelaySize]uint32
	curIdx      int
	baseHist    [delayBaseHistory]uint32
	baseIdx     int
	baseTime    int64
	initialized bool
}

// shift moves the base delay up by offset, to follow the drift of the
// other end's clock that a falling base delay the other way shows.
func (d *delayHist) shift(offset uint32) {
	for i := range d.baseHist {
		d.baseHist[i] += offset
	}
	d.base += offset
}

// add takes a sample taken at now, in microseconds.
func (d *delayHist) add(sample uint32, now int64) {
	if !d.initialized {
		for i := range d.baseHist {
			d.baseHist[i] = sample
		}
		d.base = sample
		d.baseTime = now
		d.initialized = true
	}
	if tsLess(sample, d.baseHist[d.baseIdx]) {
		d.baseHist[d.baseIdx] = sample
	}
	if tsLess(sample, d.base) {
		d.base = sample
	}
	d.cur[d.curIdx] = sample - d.base
	d.curIdx = (d.curIdx + 1) % curDelaySize
	if now-d.baseTime > delayBaseStep {
		d.baseTime = now
		d.baseIdx = (d.baseIdx + 1) % delayBaseHistory
		d.baseHist[d.baseIdx] = sample
		d.base = d.baseHist[0]
		for _, b := range d.baseHist {
			if tsLess(b, d.base) {
				d.base = b
			}
		}
	}
}

// value is the queuing delay: the least of the latest samples.
func (d *delayHist) value() uint32 {
	v := ^uint32(0)
	for _, s := range d.cur {
		v = min(v, s)
	}
	return v
}
