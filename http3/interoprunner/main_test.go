//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package main

import (
	"errors"
	"testing"
)

// TestBaseTestcase pins the aliases: the runner maps the loss and corruption
// testcases to multiconnect or transfer, and the endpoint accepts those names
// directly as the same paths.
func TestBaseTestcase(t *testing.T) {
	tests := []struct {
		testcase string
		want     string
	}{
		{"handshakeloss", "multiconnect"},
		{"handshakecorruption", "multiconnect"},
		{"transferloss", "transfer"},
		{"transfercorruption", "transfer"},
		{"http3", "http3"},
		{"handshake", "handshake"},
		{"transfer", "transfer"},
		{"retry", "retry"},
		{"multiconnect", "multiconnect"},
		{"keyupdate", "keyupdate"},
		{"ecn", "ecn"},
		{"nosuchtestcase", "nosuchtestcase"},
	}
	for _, tt := range tests {
		if got := baseTestcase(tt.testcase); got != tt.want {
			t.Errorf("baseTestcase(%q) = %q, want %q", tt.testcase, got, tt.want)
		}
	}
}

// TestDispatch covers both roles for every name the endpoint dispatches: the
// corruption aliases run the raw path of the loss cases, the loss names stay
// accepted, the canonical names keep their action, and unknown names are
// unsupported. It is pure: no socket, certificate or file is needed.
func TestDispatch(t *testing.T) {
	tests := []struct {
		testcase string
		server   testcaseAction
		client   testcaseAction
		known    bool
		base     string
	}{
		// The corruption testcases: same raw path as the loss cases, and the
		// runner itself hands the endpoint multiconnect or transfer.
		{"handshakeloss", actionRaw, actionRaw, true, "multiconnect"},
		{"handshakecorruption", actionRaw, actionRaw, true, "multiconnect"},
		{"transferloss", actionRaw, actionRaw, true, "transfer"},
		{"transfercorruption", actionRaw, actionRaw, true, "transfer"},
		{"http3", actionH3, actionH3, true, "http3"},
		{"handshake", actionRaw, actionRaw, true, "handshake"},
		{"transfer", actionRaw, actionRaw, true, "transfer"},
		{"retry", actionRaw, actionRaw, true, "retry"},
		{"resumption", actionRaw, actionRaw, true, "resumption"},
		{"zerortt", actionRaw, actionRaw, true, "zerortt"},
		{"chacha20", actionRaw, actionRaw, true, "chacha20"},
		{"multiconnect", actionRaw, actionRaw, true, "multiconnect"},
		// keyupdate is transfer on the server and its own name on the client.
		{"keyupdate", actionUnsupported, actionRaw, true, "keyupdate"},
		// Known names both roles turn down, and a name neither role knows.
		{"connectionmigration", actionUnsupported, actionUnsupported, false, "connectionmigration"},
		{"ecn", actionUnsupported, actionUnsupported, true, "ecn"},
		{"v2", actionUnsupported, actionUnsupported, true, "v2"},
		{"versionnegotiation", actionUnsupported, actionUnsupported, true, "versionnegotiation"},
		{"nosuchtestcase", actionUnsupported, actionUnsupported, false, "nosuchtestcase"},
	}
	for _, tt := range tests {
		t.Run(tt.testcase, func(t *testing.T) {
			if got := baseTestcase(tt.testcase); got != tt.base {
				t.Errorf("baseTestcase(%q) = %q, want %q", tt.testcase, got, tt.base)
			}
			if action, base := dispatchServer(tt.testcase); action != tt.server || base != tt.base {
				t.Errorf("dispatchServer(%q) = %v, %q, want %v, %q",
					tt.testcase, action, base, tt.server, tt.base)
			}
			if action, base := dispatchClient(tt.testcase); action != tt.client || base != tt.base {
				t.Errorf("dispatchClient(%q) = %v, %q, want %v, %q",
					tt.testcase, action, base, tt.client, tt.base)
			}
			if got := clientTestcaseKnown(tt.testcase); got != tt.known {
				t.Errorf("clientTestcaseKnown(%q) = %v, want %v", tt.testcase, got, tt.known)
			}
		})
	}
}

func TestUnsupported(t *testing.T) {
	// What a dispatch with actionUnsupported leaves runServer and runClient
	// to: an error main maps to the runner's exit status 127.
	if err := unsupported("nosuchtestcase", "test"); !errors.Is(err, errUnsupported) {
		t.Fatalf("unsupported(...) = %v, want errUnsupported", err)
	}
}
