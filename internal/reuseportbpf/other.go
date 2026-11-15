//go:build !(linux && (amd64 || arm64))

package reuseportbpf

import "os"

// Elsewhere there is no selector to make: every call reports ErrUnsupported
// and the caller keeps the kernel's hash, or its platform's own spreading.

// ProgLoadLog has no use here; it exists so that tests build everywhere.
var ProgLoadLog []byte

func NewGroup(int) (*Group, error)                           { return nil, ErrUnsupported }
func (g *Group) Register(int, int) error                     { return ErrUnsupported }
func (g *Group) Attach(int) error                            { return ErrUnsupported }
func (g *Group) Close() error                                { return nil }
func Balance([]int) (*Group, error)                          { return nil, ErrUnsupported }
func Join(int, string, int) (*Group, error)                  { return nil, ErrUnsupported }
func JoinGroup(int, string, int, int, []int) (*Group, error) { return nil, ErrUnsupported }

// Broker has nothing to broker here; NewBroker returns nil, whose methods
// report ErrUnsupported or do nothing.
type Broker struct{}

func NewBroker() *Broker                  { return nil }
func (b *Broker) Pair() (*os.File, error) { return nil, ErrUnsupported }
func (b *Broker) Close() error            { return nil }
