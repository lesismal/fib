package bittorrent

import (
	"fmt"
	"net/netip"
)

// EventType is what an Event reports.
type EventType int

// The events of a client.
const (
	// EventMetadata: the torrent's info dictionary is known.
	EventMetadata EventType = iota
	// EventChecked: the check of the data in storage is done.
	EventChecked
	// EventPieceVerified: Piece passed its check.
	EventPieceVerified
	// EventHashFailed: Piece failed its check, and is downloaded again.
	EventHashFailed
	// EventCompleted: every piece wanted is had.
	EventCompleted
	// EventPeerConnected and EventPeerDisconnected: a connection to Peer
	// began, or ended with Err.
	EventPeerConnected
	EventPeerDisconnected
	// EventPeerBanned: Peer (an IP with port zero, or a loopback address
	// and port) is banned, for Err; Torrent is nil.
	EventPeerBanned
	// EventSeedLimit: the torrent reached Config.SeedRatio or SeedTime and
	// paused.
	EventSeedLimit
	// EventStateChanged: the torrent was removed.
	EventStateChanged
	// EventError: Err went wrong, a storage error say.
	EventError
	// EventDropped: Dropped events were dropped before this one, the
	// queue for Config.OnEvent being full (Config.EventQueue); Torrent is
	// nil.
	EventDropped
)

func (e EventType) String() string {
	names := [...]string{"metadata", "checked", "piece verified", "hash failed", "completed",
		"peer connected", "peer disconnected", "peer banned", "seed limit", "state changed", "error", "dropped"}
	if int(e) < len(names) {
		return names[e]
	}
	return fmt.Sprintf("EventType(%d)", int(e))
}

// Event is something that happened in a client, as Config.OnEvent hears
// it.
type Event struct {
	Type    EventType
	Torrent *Torrent
	Piece   int
	Peer    netip.AddrPort
	Err     error
	// PeerInfo describes the connection of EventPeerConnected and
	// EventPeerDisconnected.
	PeerInfo *PeerInfo
	// Dropped is the number of events EventDropped stands for.
	Dropped int
}

// droppable reports whether an event of type e may be dropped when the
// queue for Config.OnEvent is full: the piece and peer events, which
// come in bursts and which Torrent.Stats and Torrent.Peers sum up.
func (e EventType) droppable() bool {
	switch e {
	case EventPieceVerified, EventHashFailed, EventPeerConnected, EventPeerDisconnected:
		return true
	}
	return false
}

// DroppedEvents is how many events the client has dropped, its queue for
// Config.OnEvent being full.
func (c *Client) DroppedEvents() int64 {
	return c.evDropped.Load()
}

// emit queues e for Config.OnEvent; it never blocks, and may be called
// with locks held. A full queue drops a piece or peer event, the oldest
// queued one for an event that is never dropped.
func (c *Client) emit(e Event) {
	if c.cfg.OnEvent == nil {
		return
	}
	c.evMu.Lock()
	defer c.evMu.Unlock()
	if c.evClosed {
		return
	}
	if len(c.events) >= c.cfg.EventQueue {
		if e.Type.droppable() {
			c.dropLocked()
			return
		}
		for i := range c.events {
			if c.events[i].Type.droppable() {
				n := copy(c.events[i:], c.events[i+1:])
				c.events[i+n] = Event{}
				c.events = c.events[:i+n]
				c.dropLocked()
				break
			}
		}
	}
	c.events = append(c.events, e)
	c.evCond.Signal()
}

func (c *Client) dropLocked() {
	c.evDrops++
	c.evDropped.Add(1)
	// The waiting dispatcher reports it even when nothing else is queued.
	c.evCond.Signal()
}

// dispatch hands the queued events to Config.OnEvent, in order, with an
// EventDropped after those taken when some were dropped.
func (c *Client) dispatch() {
	defer close(c.evDone)
	for {
		c.evMu.Lock()
		for len(c.events) == 0 && c.evDrops == 0 && !c.evClosed {
			c.evCond.Wait()
		}
		if c.evClosed && len(c.events) == 0 && c.evDrops == 0 {
			c.evMu.Unlock()
			return
		}
		evs, drops := c.events, c.evDrops
		c.events, c.evSpare, c.evDrops = c.evSpare[:0], nil, 0
		c.evMu.Unlock()
		for _, e := range evs {
			c.cfg.OnEvent(e)
		}
		if drops > 0 {
			c.cfg.OnEvent(Event{Type: EventDropped, Dropped: drops})
		}
		clear(evs)
		c.evMu.Lock()
		if cap(evs) <= 2*c.cfg.EventQueue {
			c.evSpare = evs[:0]
		}
		c.evMu.Unlock()
	}
}
