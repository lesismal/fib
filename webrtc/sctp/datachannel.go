package sctp

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	ErrChannelClosed  = errors.New("sctp: data channel closed")
	ErrChannelNotOpen = errors.New("sctp: data channel not open yet")
	ErrStreamInUse    = errors.New("sctp: stream id in use")
	ErrNoStreamID     = errors.New("sctp: no stream id left")
)

// ChannelType is a DCEP channel type (RFC 8832 section 5.1): whether
// messages are ordered, and what partial reliability, if any, they have.
type ChannelType uint8

const (
	ChannelReliable                       ChannelType = 0x00
	ChannelReliableUnordered              ChannelType = 0x80
	ChannelPartialReliableRexmit          ChannelType = 0x01
	ChannelPartialReliableRexmitUnordered ChannelType = 0x81
	ChannelPartialReliableTimed           ChannelType = 0x02
	ChannelPartialReliableTimedUnordered  ChannelType = 0x82

	channelUnorderedBit    ChannelType = 0x80
	channelReliabilityMask ChannelType = 0x7f
)

// DCEP message types and the size of OPEN before its label.
const (
	dcepAck            = 0x02
	dcepOpen           = 0x03
	dcepOpenHeaderSize = 12

	defaultChannelPriority = 256
)

// ChannelOptions describes a data channel to open.
type ChannelOptions struct {
	Label    string
	Protocol string
	// Type and ReliabilityParameter are DCEP's: the parameter is the
	// retransmission limit or the lifetime in milliseconds.
	Type                 ChannelType
	ReliabilityParameter uint32
	Priority             uint16
	// Negotiated channels are agreed out of band on stream ID; no DCEP
	// message is sent and both sides open the channel themselves.
	Negotiated bool
	ID         uint16
}

// ChannelState is a data channel's readyState.
type ChannelState uint8

const (
	ChannelConnecting ChannelState = iota
	ChannelOpen
	ChannelClosing
	ChannelClosed
)

// DataChannel is one RFC 8831 data channel: a pair of SCTP streams with
// the same id. Its methods share the association's serialization: call
// them from the goroutine that drives the association, or inside its
// callbacks.
type DataChannel struct {
	a          *Association
	id         uint16
	label      string
	protocol   string
	typ        ChannelType
	param      uint32
	priority   uint16
	negotiated bool
	state      ChannelState
	// acked is whether the peer acknowledged OPEN; until it has, messages
	// go ordered (RFC 8832 section 6).
	acked     bool
	outReset  bool
	inReset   bool
	lowWater  int
	openFired bool

	// OnOpen runs when a channel opened before the association was up
	// becomes open; one opened later is open at once.
	OnOpen func()
	// OnMessage receives each message; data is only valid during the call.
	OnMessage func(data []byte, text bool)
	// OnClose runs once when the channel is closed both ways.
	OnClose func()
	// OnBufferedAmountLow runs when BufferedAmount falls to the threshold
	// set with SetBufferedAmountLowThreshold.
	OnBufferedAmountLow func()
}

func (c *DataChannel) ID() uint16                      { return c.id }
func (c *DataChannel) Label() string                   { return c.label }
func (c *DataChannel) Protocol() string                { return c.protocol }
func (c *DataChannel) Type() ChannelType               { return c.typ }
func (c *DataChannel) ReliabilityParameter() uint32    { return c.param }
func (c *DataChannel) Negotiated() bool                { return c.negotiated }
func (c *DataChannel) State() ChannelState             { return c.state }
func (c *DataChannel) Ordered() bool                   { return c.typ&channelUnorderedBit == 0 }
func (c *DataChannel) BufferedAmount() int             { return c.a.BufferedAmount(c.id) }
func (c *DataChannel) BufferedAmountLowThreshold() int { return c.lowWater }

// SetBufferedAmountLowThreshold sets the low-water mark of
// OnBufferedAmountLow.
func (c *DataChannel) SetBufferedAmountLowThreshold(n int) { c.lowWater = n }

// OpenChannel opens a data channel. An in-band channel takes the lowest
// free stream id of this side's parity and announces itself with DCEP
// OPEN. Before the association is up the channel waits in
// ChannelConnecting and OnOpen tells when it opens.
func (a *Association) OpenChannel(o ChannelOptions) (*DataChannel, error) {
	if a.closed || a.state > StateEstablished {
		return nil, ErrClosed
	}
	limit := 65535
	if a.state == StateEstablished {
		limit = int(a.outStreams)
	}
	id := o.ID
	if o.Negotiated {
		if int(id) >= limit {
			return nil, fmt.Errorf("%w: %d", ErrInvalidStream, id)
		}
		if a.inUse(id) {
			return nil, fmt.Errorf("%w: %d", ErrStreamInUse, id)
		}
	} else {
		start := 1
		if a.cfg.Client {
			start = 0
		}
		found := false
		for i := start; i < limit; i += 2 {
			if !a.inUse(uint16(i)) {
				id, found = uint16(i), true
				break
			}
		}
		if !found {
			return nil, ErrNoStreamID
		}
	}
	if len(o.Label) > 0xffff || len(o.Protocol) > 0xffff {
		return nil, fmt.Errorf("%w: label or protocol too long", ErrMessageTooLarge)
	}
	c := &DataChannel{
		a: a, id: id, label: o.Label, protocol: o.Protocol, typ: o.Type,
		param: o.ReliabilityParameter, priority: o.Priority, negotiated: o.Negotiated,
		acked: o.Negotiated,
	}
	if c.priority == 0 {
		c.priority = defaultChannelPriority
	}
	a.stream(id).ch = c
	a.chans = append(a.chans, c)
	if a.state == StateEstablished {
		if !o.Negotiated {
			if err := a.sendOpen(c); err != nil {
				a.dropChannel(c)
				return nil, err
			}
		}
		c.state = ChannelOpen
		c.openFired = true
	}
	return c, nil
}

func (a *Association) inUse(id uint16) bool {
	if int(id) >= len(a.streams) || a.streams[id] == nil {
		return false
	}
	st := a.streams[id]
	return st.ch != nil || st.resetting
}

func (a *Association) sendOpen(c *DataChannel) error {
	n := dcepOpenHeaderSize + len(c.label) + len(c.protocol)
	b := a.pool.get(n)
	defer a.pool.put(b)
	b[0] = dcepOpen
	b[1] = byte(c.typ)
	binary.BigEndian.PutUint16(b[2:], c.priority)
	binary.BigEndian.PutUint32(b[4:], c.param)
	binary.BigEndian.PutUint16(b[8:], uint16(len(c.label)))
	binary.BigEndian.PutUint16(b[10:], uint16(len(c.protocol)))
	copy(b[12:], c.label)
	copy(b[12+len(c.label):], c.protocol)
	return a.send(c.id, PPIDDCEP, b, "", SendOptions{})
}

// openChannels opens the channels made before the association came up.
func (a *Association) openChannels() {
	for _, c := range a.chans {
		if c.state != ChannelConnecting {
			continue
		}
		if c.id >= a.outStreams {
			c.closed()
			continue
		}
		if !c.negotiated {
			if err := a.sendOpen(c); err != nil {
				continue
			}
		}
		c.state = ChannelOpen
		if c.OnOpen != nil && !c.openFired {
			c.openFired = true
			c.OnOpen()
		}
	}
}

// channelMessage handles a message on a stream with a channel or a DCEP
// message.
func (a *Association) channelMessage(sid uint16, st *stream, ppid uint32, data []byte) {
	if ppid == PPIDDCEP {
		a.handleDCEP(sid, st, data)
		return
	}
	c := st.ch
	if c == nil || c.state == ChannelClosed {
		return
	}
	if !c.acked {
		// Data from the peer means it has our OPEN (RFC 8832 section 6).
		c.acked = true
	}
	if c.OnMessage == nil {
		return
	}
	switch ppid {
	case PPIDString, 52:
		c.OnMessage(data, true)
	case PPIDStringEmpty:
		c.OnMessage(nil, true)
	case PPIDBinaryEmpty:
		c.OnMessage(nil, false)
	default:
		c.OnMessage(data, false)
	}
}

func (a *Association) handleDCEP(sid uint16, st *stream, b []byte) {
	if len(b) == 0 {
		return
	}
	switch b[0] {
	case dcepAck:
		if st.ch != nil {
			st.ch.acked = true
		}
	case dcepOpen:
		if len(b) < dcepOpenHeaderSize {
			return
		}
		ll := int(binary.BigEndian.Uint16(b[8:]))
		pl := int(binary.BigEndian.Uint16(b[10:]))
		if len(b) < dcepOpenHeaderSize+ll+pl || st.ch != nil || st.resetting {
			return
		}
		c := &DataChannel{
			a:         a,
			id:        sid,
			typ:       ChannelType(b[1]),
			priority:  binary.BigEndian.Uint16(b[2:]),
			param:     binary.BigEndian.Uint32(b[4:]),
			label:     string(b[12 : 12+ll]),
			protocol:  string(b[12+ll : 12+ll+pl]),
			state:     ChannelOpen,
			acked:     true,
			openFired: true,
		}
		st.ch = c
		a.chans = append(a.chans, c)
		if err := a.send(sid, PPIDDCEP, []byte{dcepAck}, "", SendOptions{}); err != nil {
			return
		}
		if a.cfg.OnDataChannel != nil {
			a.cfg.OnDataChannel(c)
		}
	}
}

// Send sends a binary message.
func (c *DataChannel) Send(data []byte) error {
	if len(data) == 0 {
		return c.send(PPIDBinaryEmpty, []byte{0}, "")
	}
	return c.send(PPIDBinary, data, "")
}

// SendText sends a text message.
func (c *DataChannel) SendText(s string) error {
	if len(s) == 0 {
		return c.send(PPIDStringEmpty, []byte{0}, "")
	}
	return c.send(PPIDString, nil, s)
}

func (c *DataChannel) send(ppid uint32, data []byte, s string) error {
	switch c.state {
	case ChannelConnecting:
		// Its OPEN goes out when the association comes up; data must not
		// get ahead of it.
		return ErrChannelNotOpen
	case ChannelClosing, ChannelClosed:
		return ErrChannelClosed
	}
	o := SendOptions{Unordered: c.acked && c.typ&channelUnorderedBit != 0}
	switch c.typ & channelReliabilityMask {
	case 1:
		o.PR, o.Limit = PRRetransmits, c.param
	case 2:
		o.PR, o.Limit = PRLifetime, c.param
	}
	return c.a.send(c.id, ppid, data, s, o)
}

// Close closes the channel: once what it has queued is sent, its outgoing
// stream is reset, and the peer resets its own in turn (RFC 8831 section
// 6.7). OnClose runs when both are done.
func (c *DataChannel) Close() error {
	switch c.state {
	case ChannelClosing, ChannelClosed:
		return nil
	case ChannelConnecting:
		c.a.dropChannel(c)
		c.closed()
		return nil
	}
	c.state = ChannelClosing
	return c.a.ResetStream(c.id)
}

// outgoingReset is the association's word that this side's stream is
// reset.
func (c *DataChannel) outgoingReset() {
	c.outReset = true
	if c.inReset {
		c.a.dropChannel(c)
		c.closed()
	}
}

// incomingReset is the association's word that the peer reset its stream.
func (c *DataChannel) incomingReset() {
	c.inReset = true
	switch {
	case c.outReset:
		c.a.dropChannel(c)
		c.closed()
	case c.state != ChannelClosing:
		c.state = ChannelClosing
		_ = c.a.ResetStream(c.id)
	}
}

func (a *Association) dropChannel(c *DataChannel) {
	if int(c.id) < len(a.streams) && a.streams[c.id] != nil && a.streams[c.id].ch == c {
		a.streams[c.id].ch = nil
	}
	for i, x := range a.chans {
		if x == c {
			a.chans = append(a.chans[:i], a.chans[i+1:]...)
			break
		}
	}
}

func (c *DataChannel) closed() {
	if c.state == ChannelClosed {
		return
	}
	c.state = ChannelClosed
	if c.OnClose != nil {
		c.OnClose()
	}
}
