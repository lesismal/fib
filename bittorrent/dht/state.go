package dht

import (
	"errors"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// State is what a node keeps across a restart: its ID, so that the nodes
// that know it keep knowing it, and the nodes of its routing table, to
// bootstrap from without the routers. It bencodes as a dictionary of "id"
// and the compact "nodes" and "nodes6", the form libtorrent's session
// state has.
//
//	b, _ := bencode.Marshal(node.State())
//	// ... restart ...
//	var st dht.State
//	_ = bencode.Unmarshal(b, &st)
//	node, _ := dht.New(dht.Config{ID: st.ID, Send: send})
//	node.AddNodes(st.Nodes...)
type State struct {
	ID    ID
	Nodes []NodeInfo
}

// State returns the node's state.
func (n *Node) State() State { return State{ID: n.ID(), Nodes: n.Nodes()} }

type stateDict struct {
	ID     []byte `bencode:"id"`
	Nodes  []byte `bencode:"nodes,omitempty"`
	Nodes6 []byte `bencode:"nodes6,omitempty"`
}

// MarshalBencode encodes the state.
func (s State) MarshalBencode() ([]byte, error) {
	return bencode.Marshal(stateDict{
		ID:     s.ID[:],
		Nodes:  appendCompactNodes(nil, s.Nodes, false),
		Nodes6: appendCompactNodes(nil, s.Nodes, true),
	})
}

// UnmarshalBencode decodes a state.
func (s *State) UnmarshalBencode(b []byte) error {
	var d stateDict
	if err := bencode.Unmarshal(b, &d); err != nil {
		return err
	}
	if len(d.ID) != 20 {
		return errors.New("dht: state without a valid id")
	}
	n4, err := parseCompactNodes(d.Nodes, false)
	if err != nil {
		return err
	}
	n6, err := parseCompactNodes(d.Nodes6, true)
	if err != nil {
		return err
	}
	s.ID = ID(d.ID)
	s.Nodes = append(n4, n6...)
	return nil
}
