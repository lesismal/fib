// Package metadata is the metadata of an RPC: the headers and trailers that
// travel with it, like google.golang.org/grpc/metadata.
package metadata

import (
	"context"
	"fmt"
	"strings"
)

// MD maps lowercase keys to values. A key ending in "-bin" holds binary
// values, which travel base64-encoded.
type MD map[string][]string

// New returns an MD of m, with its keys lowercased.
func New(m map[string]string) MD {
	md := make(MD, len(m))
	for k, v := range m {
		key := strings.ToLower(k)
		md[key] = append(md[key], v)
	}
	return md
}

// Pairs returns an MD of key, value pairs. It panics on an odd count.
func Pairs(kv ...string) MD {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("metadata: Pairs got the odd number of input pairs for metadata: %d", len(kv)))
	}
	md := make(MD, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		key := strings.ToLower(kv[i])
		md[key] = append(md[key], kv[i+1])
	}
	return md
}

// Len returns the number of keys.
func (md MD) Len() int { return len(md) }

// Copy returns a copy of md.
func (md MD) Copy() MD {
	out := make(MD, len(md))
	for k, v := range md {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// Get returns the values of key.
func (md MD) Get(k string) []string { return md[strings.ToLower(k)] }

// Set sets the values of key.
func (md MD) Set(k string, vals ...string) {
	if len(vals) == 0 {
		return
	}
	md[strings.ToLower(k)] = vals
}

// Append adds values to key.
func (md MD) Append(k string, vals ...string) {
	if len(vals) == 0 {
		return
	}
	k = strings.ToLower(k)
	md[k] = append(md[k], vals...)
}

// Delete removes key.
func (md MD) Delete(k string) { delete(md, strings.ToLower(k)) }

// Join merges the MDs into one, the values of a key in the order given.
func Join(mds ...MD) MD {
	out := MD{}
	for _, md := range mds {
		for k, v := range md {
			out[k] = append(out[k], v...)
		}
	}
	return out
}

type incomingKey struct{}
type outgoingKey struct{}

// NewIncomingContext returns a copy of ctx carrying md as the metadata that
// came with an RPC, which a server's handler reads.
func NewIncomingContext(ctx context.Context, md MD) context.Context {
	return context.WithValue(ctx, incomingKey{}, md)
}

// FromIncomingContext returns a copy of the metadata that came with the RPC
// of ctx.
func FromIncomingContext(ctx context.Context) (MD, bool) {
	md, ok := ctx.Value(incomingKey{}).(MD)
	if !ok {
		return nil, false
	}
	return md.Copy(), true
}

// ValueFromIncomingContext returns the incoming values of key.
func ValueFromIncomingContext(ctx context.Context, key string) []string {
	md, ok := ctx.Value(incomingKey{}).(MD)
	if !ok {
		return nil
	}
	if v, ok := md[strings.ToLower(key)]; ok {
		return append([]string(nil), v...)
	}
	return nil
}

type outgoing struct {
	md    MD
	added [][]string
}

// NewOutgoingContext returns a copy of ctx carrying md as the metadata to
// send with the RPCs it makes, in place of any it carried.
func NewOutgoingContext(ctx context.Context, md MD) context.Context {
	return context.WithValue(ctx, outgoingKey{}, outgoing{md: md})
}

// AppendToOutgoingContext returns a copy of ctx with the key, value pairs
// added to its outgoing metadata. It panics on an odd count.
func AppendToOutgoingContext(ctx context.Context, kv ...string) context.Context {
	if len(kv)%2 == 1 {
		panic(fmt.Sprintf("metadata: AppendToOutgoingContext got an odd number of input pairs for metadata: %d", len(kv)))
	}
	o, _ := ctx.Value(outgoingKey{}).(outgoing)
	added := make([][]string, len(o.added)+1)
	copy(added, o.added)
	pairs := make([]string, len(kv))
	for i := range kv {
		if i%2 == 0 {
			pairs[i] = strings.ToLower(kv[i])
		} else {
			pairs[i] = kv[i]
		}
	}
	added[len(added)-1] = pairs
	return context.WithValue(ctx, outgoingKey{}, outgoing{md: o.md, added: added})
}

// FromOutgoingContext returns a copy of the metadata ctx sends.
func FromOutgoingContext(ctx context.Context) (MD, bool) {
	o, ok := ctx.Value(outgoingKey{}).(outgoing)
	if !ok {
		return nil, false
	}
	out := o.md.Copy()
	for _, pairs := range o.added {
		for i := 0; i < len(pairs); i += 2 {
			out[pairs[i]] = append(out[pairs[i]], pairs[i+1])
		}
	}
	return out, true
}
