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
)

func (e EventType) String() string {
	names := [...]string{"metadata", "checked", "piece verified", "hash failed", "completed",
		"peer connected", "peer disconnected", "peer banned", "seed limit", "state changed", "error"}
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
}

// emit queues e for Config.OnEvent; it never blocks, and may be called
// with locks held.
func (c *Client) emit(e Event) {
	if c.cfg.OnEvent == nil {
		return
	}
	c.evMu.Lock()
	if !c.evClosed {
		c.events = append(c.events, e)
		c.evCond.Signal()
	}
	c.evMu.Unlock()
}

// dispatch hands the queued events to Config.OnEvent, in order.
func (c *Client) dispatch() {
	defer close(c.evDone)
	for {
		c.evMu.Lock()
		for len(c.events) == 0 && !c.evClosed {
			c.evCond.Wait()
		}
		if c.evClosed && len(c.events) == 0 {
			c.evMu.Unlock()
			return
		}
		evs := c.events
		c.events = nil
		c.evMu.Unlock()
		for _, e := range evs {
			c.cfg.OnEvent(e)
		}
	}
}
