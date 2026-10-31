package webrtc

import (
	"slices"
	"sync/atomic"

	"github.com/lesismal/fib/webrtc/sctp"
)

// DataChannelInit are the options of CreateDataChannel, as
// RTCDataChannelInit has them.
type DataChannelInit struct {
	// Protocol is the subprotocol the channel announces.
	Protocol string
	// Unordered lets messages arrive out of order.
	Unordered bool
	// MaxRetransmits and MaxPacketLifeTime, in milliseconds, make the
	// channel partially reliable: a message is given up after that many
	// retransmissions or that long. At most one may be set.
	MaxRetransmits    *uint16
	MaxPacketLifeTime *uint16
	// Negotiated channels are agreed on out of band, with ID: both sides
	// create them, and no DCEP message is sent.
	Negotiated bool
	ID         uint16
}

// DataChannelState is RTCDataChannelState.
type DataChannelState = sctp.ChannelState

const (
	DataChannelStateConnecting = sctp.ChannelConnecting
	DataChannelStateOpen       = sctp.ChannelOpen
	DataChannelStateClosing    = sctp.ChannelClosing
	DataChannelStateClosed     = sctp.ChannelClosed
)

// DataChannelMessage is a message received on a data channel. Data belongs
// to the callback, which may keep it.
type DataChannelMessage struct {
	Data     []byte
	IsString bool
}

// DataChannel is an RTCDataChannel: an sctp.DataChannel whose calls take
// the PeerConnection's mutex, and whose callbacks run as the
// PeerConnection's do. It may be created before the PeerConnection is
// connected, and opens once the SCTP association is up.
type DataChannel struct {
	pc   *PeerConnection
	ch   *sctp.DataChannel
	opts sctp.ChannelOptions

	// Guarded by pc.mu.
	closed    bool
	openFired bool
	onOpen    func()
	onClose   func()
	onLow     func()
	low       int

	onMessage atomic.Pointer[func(DataChannelMessage)]
}

// CreateDataChannel creates a data channel. The first one adds a data
// section to the next offer.
func (pc *PeerConnection) CreateDataChannel(label string, init *DataChannelInit) (*DataChannel, error) {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return nil, ErrClosed
	}
	o := sctp.ChannelOptions{Label: label}
	if init != nil {
		o.Protocol, o.Negotiated, o.ID = init.Protocol, init.Negotiated, init.ID
		switch {
		case init.MaxRetransmits != nil && init.MaxPacketLifeTime != nil:
			return nil, ErrConfiguration
		case init.MaxRetransmits != nil:
			o.Type, o.ReliabilityParameter = sctp.ChannelPartialReliableRexmit, uint32(*init.MaxRetransmits)
		case init.MaxPacketLifeTime != nil:
			o.Type, o.ReliabilityParameter = sctp.ChannelPartialReliableTimed, uint32(*init.MaxPacketLifeTime)
		}
		if init.Unordered {
			o.Type |= sctp.ChannelReliableUnordered
		}
	}
	d := &DataChannel{pc: pc, opts: o}
	pc.channels = append(pc.channels, d)
	if pc.sctp != nil {
		d.open()
	} else if pc.dataMid == "" && len(pc.channels) == 1 {
		pc.negotiationNeeded()
	}
	return d, nil
}

// OnDataChannel sets the callback for each data channel the peer opens.
// It should set the channel's callbacks before it returns.
func (pc *PeerConnection) OnDataChannel(fn func(*DataChannel)) {
	pc.mu.Lock()
	pc.onDataChannel = fn
	pc.mu.Unlock()
}

func (pc *PeerConnection) onRemoteChannel(ch *sctp.DataChannel) {
	d := &DataChannel{pc: pc, opts: sctp.ChannelOptions{
		Label: ch.Label(), Protocol: ch.Protocol(), Type: ch.Type(),
		ReliabilityParameter: ch.ReliabilityParameter(), Negotiated: ch.Negotiated(), ID: ch.ID(),
	}}
	pc.channels = append(pc.channels, d)
	d.wire(ch)
	if fn := pc.onDataChannel; fn != nil {
		pc.queue(func() { fn(d) })
	}
}

// open opens the channel on the association. Under pc.mu.
func (d *DataChannel) open() {
	ch, err := d.pc.sctp.OpenChannel(d.opts)
	if err != nil {
		d.closedLocked()
		return
	}
	d.wire(ch)
}

func (d *DataChannel) wire(ch *sctp.DataChannel) {
	d.ch = ch
	ch.OnOpen = d.fireOpen
	ch.OnMessage = func(data []byte, text bool) {
		m := DataChannelMessage{Data: append([]byte(nil), data...), IsString: text}
		d.pc.queue(func() {
			if fn := d.onMessage.Load(); fn != nil && *fn != nil {
				(*fn)(m)
			}
		})
	}
	ch.OnClose = d.closedLocked
	ch.SetBufferedAmountLowThreshold(d.low)
	ch.OnBufferedAmountLow = func() {
		if fn := d.onLow; fn != nil {
			d.pc.queue(fn)
		}
	}
	if ch.State() == sctp.ChannelOpen {
		d.fireOpen()
	}
}

// fireOpen queues OnOpen once the channel is open and the callback set.
func (d *DataChannel) fireOpen() {
	if d.openFired || d.onOpen == nil || d.ch == nil || d.ch.State() != sctp.ChannelOpen {
		return
	}
	d.openFired = true
	d.pc.queue(d.onOpen)
}

// closedLocked marks the channel closed and queues OnClose.
func (d *DataChannel) closedLocked() {
	if d.closed {
		return
	}
	d.closed = true
	pc := d.pc
	if i := slices.Index(pc.channels, d); i >= 0 {
		pc.channels = slices.Delete(pc.channels, i, i+1)
	}
	if fn := d.onClose; fn != nil {
		pc.queue(fn)
	}
}

// Label returns the channel's label.
func (d *DataChannel) Label() string { return d.opts.Label }

// Protocol returns the channel's subprotocol.
func (d *DataChannel) Protocol() string { return d.opts.Protocol }

// Ordered reports whether messages arrive in order.
func (d *DataChannel) Ordered() bool { return d.opts.Type&sctp.ChannelReliableUnordered == 0 }

// Negotiated reports whether the channel was agreed on out of band.
func (d *DataChannel) Negotiated() bool { return d.opts.Negotiated }

// ID returns the SCTP stream id, once the channel has one.
func (d *DataChannel) ID() (uint16, bool) {
	d.pc.mu.Lock()
	defer d.pc.mu.Unlock()
	if d.ch == nil {
		return 0, false
	}
	return d.ch.ID(), true
}

// ReadyState returns the channel's state.
func (d *DataChannel) ReadyState() DataChannelState {
	d.pc.mu.Lock()
	defer d.pc.mu.Unlock()
	switch {
	case d.closed:
		return DataChannelStateClosed
	case d.ch == nil:
		return DataChannelStateConnecting
	}
	return d.ch.State()
}

// Send sends a binary message.
func (d *DataChannel) Send(data []byte) error {
	d.pc.mu.Lock()
	defer d.pc.unlock()
	if d.closed || d.pc.closed {
		return sctp.ErrChannelClosed
	}
	if d.ch == nil {
		return sctp.ErrChannelNotOpen
	}
	return d.ch.Send(data)
}

// SendText sends a text message.
func (d *DataChannel) SendText(s string) error {
	d.pc.mu.Lock()
	defer d.pc.unlock()
	if d.closed || d.pc.closed {
		return sctp.ErrChannelClosed
	}
	if d.ch == nil {
		return sctp.ErrChannelNotOpen
	}
	return d.ch.SendText(s)
}

// Close closes the channel, both ways, once what it has queued is sent.
func (d *DataChannel) Close() error {
	d.pc.mu.Lock()
	defer d.pc.unlock()
	if d.closed {
		return nil
	}
	if d.ch == nil {
		d.closedLocked()
		return nil
	}
	return d.ch.Close()
}

// BufferedAmount returns the bytes queued to send.
func (d *DataChannel) BufferedAmount() int {
	d.pc.mu.Lock()
	defer d.pc.mu.Unlock()
	if d.ch == nil || d.closed {
		return 0
	}
	return d.ch.BufferedAmount()
}

// SetBufferedAmountLowThreshold sets the level OnBufferedAmountLow fires
// at.
func (d *DataChannel) SetBufferedAmountLowThreshold(n int) {
	d.pc.mu.Lock()
	defer d.pc.mu.Unlock()
	d.low = n
	if d.ch != nil {
		d.ch.SetBufferedAmountLowThreshold(n)
	}
}

// OnOpen sets the callback for the channel opening; set on a channel that
// is open already, it runs at once.
func (d *DataChannel) OnOpen(fn func()) {
	d.pc.mu.Lock()
	d.onOpen = fn
	d.fireOpen()
	d.pc.unlock()
}

// OnMessage sets the callback for each message.
func (d *DataChannel) OnMessage(fn func(DataChannelMessage)) { d.onMessage.Store(&fn) }

// OnClose sets the callback for the channel closing.
func (d *DataChannel) OnClose(fn func()) {
	d.pc.mu.Lock()
	d.onClose = fn
	d.pc.mu.Unlock()
}

// OnBufferedAmountLow sets the callback for BufferedAmount falling to the
// threshold.
func (d *DataChannel) OnBufferedAmountLow(fn func()) {
	d.pc.mu.Lock()
	d.onLow = fn
	d.pc.mu.Unlock()
}
