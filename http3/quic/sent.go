package quic

// sentQueue is a space's sent packets that are neither acknowledged nor
// lost yet, by packet number: a ring of slots from base on, the packet
// numbered pn in the slot pn-base from head, and nil there once it is
// acknowledged or declared lost. The slots before the first packet still
// waiting are dropped as it moves on. An acknowledgement goes straight to
// the slots of the packets it names, and loss detection to those up to the
// largest acknowledged, rather than through every packet in flight.
type sentQueue struct {
	slots []*sentPacket
	head  int
	// n is how many slots are in use from head, and live how many of those
	// hold a packet.
	n, live int
	base    uint64
}

// maxIdleSlots bounds the slots a queue keeps once it is empty: a
// connection that had many packets in flight at once has the room for them
// again, and one that is idle need not.
const maxIdleSlots = 1024

func (q *sentQueue) len() int { return q.live }

// end is one past the largest packet number the queue has a slot for.
func (q *sentQueue) end() uint64 { return q.base + uint64(q.n) }

// slot is the slot of the packet numbered pn, which the queue has one for.
func (q *sentQueue) slot(pn uint64) **sentPacket {
	return &q.slots[(q.head+int(pn-q.base))&(len(q.slots)-1)]
}

// push adds a packet numbered after every one already there.
func (q *sentQueue) push(p *sentPacket) {
	if q.live == 0 {
		q.head, q.n, q.base = 0, 0, p.pn
	}
	if p.pn < q.end() {
		panic("quic: packet numbers out of order")
	}
	for gap := p.pn - q.end(); ; gap-- {
		if q.n == len(q.slots) {
			q.grow()
		}
		q.slots[(q.head+q.n)&(len(q.slots)-1)] = nil
		q.n++
		if gap == 0 {
			break
		}
	}
	*q.slot(p.pn) = p
	q.live++
}

func (q *sentQueue) grow() {
	slots := make([]*sentPacket, max(16, 2*len(q.slots)))
	for i := 0; i < q.n; i++ {
		slots[i] = q.slots[(q.head+i)&(len(q.slots)-1)]
	}
	q.slots, q.head = slots, 0
}

// take removes the packet numbered pn, if the queue still has it.
func (q *sentQueue) take(pn uint64) *sentPacket {
	if pn < q.base || pn >= q.end() {
		return nil
	}
	s := q.slot(pn)
	p := *s
	if p == nil {
		return nil
	}
	*s = nil
	q.live--
	return p
}

// trim drops the slots before the first packet still waiting.
func (q *sentQueue) trim() {
	for q.n > 0 && q.slots[q.head] == nil {
		q.head = (q.head + 1) & (len(q.slots) - 1)
		q.n--
		q.base++
	}
	if q.n == 0 {
		q.head = 0
		if len(q.slots) > maxIdleSlots {
			q.slots = nil
		}
	}
}

// each calls f with the packets numbered from base up to below end, in
// order, until it returns false.
func (q *sentQueue) each(end uint64, f func(p *sentPacket) bool) {
	end = min(end, q.end())
	for pn := q.base; pn < end; pn++ {
		if p := *q.slot(pn); p != nil && !f(p) {
			return
		}
	}
}

// reset empties the queue, leaving the packets to the caller.
func (q *sentQueue) reset() {
	clear(q.slots)
	q.head, q.n, q.live = 0, 0, 0
}
