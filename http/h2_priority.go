//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"strings"

	"github.com/lesismal/fib/hpack"
)

// h2UrgencyWeights is the send weight of each urgency of RFC 9218's priority
// header: urgency 0, the most urgent, gets eight frames a turn at its
// connection's window, then four, then two, and the default urgency 3 and
// every less urgent one get the single frame a turn that every stream gets
// without a priority. The RFC leaves the size of these steps to the server;
// these do what its guidance asks, a more urgent response getting more of the
// window, while a client that sends no priority, or the default urgency, sees
// the round robin the server has always used. PRIORITY_UPDATE, the RFC's
// other way to set a stream's priority, is not implemented, and neither is
// the priority tree of RFC 7540, which RFC 9113 deprecates.
var h2UrgencyWeights = [8]uint8{8, 4, 2, 1, 1, 1, 1, 1}

// h2PriorityWeight is the send weight a request's priority header asks for,
// or 1. RFC 9218's dictionary carries "u", the urgency, from 0, the most
// urgent, to 7, and "i", whether the response may be delivered
// incrementally. Only the urgency matters here: this server buffers a
// response body whole before it sends it, so the incremental hint has nothing
// to reorder. A member this does not understand is left out, as the RFC
// requires, and if the header names the urgency more than once, the last one
// wins.
func h2PriorityWeight(fields []hpack.HeaderField) uint8 {
	weight := uint8(1)
	for i := range fields {
		f := &fields[i]
		if f.Name != "priority" {
			continue
		}
		for item := range strings.SplitSeq(f.Value, ",") {
			key, value, _ := strings.Cut(strings.TrimSpace(item), "=")
			if key != "u" || len(value) != 1 || value[0] < '0' || value[0] > '7' {
				continue
			}
			weight = h2UrgencyWeights[value[0]-'0']
		}
	}
	return weight
}
