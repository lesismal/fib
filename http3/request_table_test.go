//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import "testing"

// A request table holds requests whose slots meet, as well as those that
// fill their own, and lets each go by its own ID only.
func TestRequestTable(t *testing.T) {
	var table requestTable
	ids := []uint64{0, 4, 8, requestSlots * 4, 2 * requestSlots * 4, 4 + requestSlots*4}
	reqs := make(map[uint64]*requestStream)
	for _, id := range ids {
		rs := &requestStream{}
		reqs[id] = rs
		if !table.add(id, rs) {
			t.Fatalf("add %d reported it there already", id)
		}
	}
	if table.add(ids[3], reqs[ids[3]]) || table.add(0, reqs[0]) {
		t.Fatal("a request added twice was counted twice")
	}
	if table.n != len(ids) || len(table.all()) != len(ids) {
		t.Fatalf("n %d, all %d, want %d", table.n, len(table.all()), len(ids))
	}
	if table.remove(0, reqs[requestSlots*4]) || table.remove(12, reqs[0]) {
		t.Fatal("removed a request by another's ID")
	}
	for _, id := range ids {
		if !table.remove(id, reqs[id]) {
			t.Fatalf("remove %d found nothing", id)
		}
		if table.remove(id, reqs[id]) {
			t.Fatalf("removed %d twice", id)
		}
	}
	if table.n != 0 || table.all() != nil {
		t.Fatalf("n %d after removing all", table.n)
	}
}
