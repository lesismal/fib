package metadata

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestMetadata(t *testing.T) {
	md := Pairs("Key", "a", "key", "b")
	if fmt.Sprint(md.Get("KEY")) != "[a b]" {
		t.Fatalf("Pairs = %v", md)
	}
	md.Set("x", "1")
	md.Append("x", "2")
	md.Delete("key")
	if fmt.Sprint(md) != "map[x:[1 2]]" {
		t.Fatalf("md = %v", md)
	}
	joined := Join(New(map[string]string{"A": "1"}), Pairs("a", "2"))
	if fmt.Sprint(joined["a"]) != "[1 2]" {
		t.Fatalf("Join = %v", joined)
	}

	ctx := NewOutgoingContext(context.Background(), Pairs("k", "v"))
	ctx = AppendToOutgoingContext(ctx, "K2", "v2", "k", "v3")
	out, ok := FromOutgoingContext(ctx)
	if !ok || fmt.Sprint(out["k"]) != "[v v3]" || fmt.Sprint(out["k2"]) != "[v2]" {
		t.Fatalf("outgoing = %v", out)
	}
	if out, ok := FromOutgoingContext(context.Background()); ok || out != nil {
		t.Fatalf("outgoing of a bare context = %v, %v", out, ok)
	}
	in := NewIncomingContext(context.Background(), Pairs("k", "v"))
	got, _ := FromIncomingContext(in)
	got["k"][0] = "changed"
	if fmt.Sprint(ValueFromIncomingContext(in, "K")) != "[v]" {
		t.Fatal("FromIncomingContext did not copy")
	}
}

// TestMetadataEdges covers what the common paths skip: setting or appending
// no values changes nothing, a context without metadata has none, a key it
// lacks has no values, and odd pairs panic rather than drop a value.
func TestMetadataEdges(t *testing.T) {
	md := Pairs("k", "v")
	md.Set("k")
	md.Append("k")
	if md.Len() != 1 || fmt.Sprint(md["k"]) != "[v]" {
		t.Fatalf("md = %v", md)
	}
	ctx := context.Background()
	if got, ok := FromIncomingContext(ctx); ok || got != nil {
		t.Fatalf("FromIncomingContext of a bare context = %v, %v", got, ok)
	}
	if ValueFromIncomingContext(ctx, "k") != nil {
		t.Fatal("ValueFromIncomingContext of a bare context")
	}
	if ValueFromIncomingContext(NewIncomingContext(ctx, md), "missing") != nil {
		t.Fatal("ValueFromIncomingContext of a missing key")
	}
	odd := strings.Fields("k")
	for name, f := range map[string]func(){
		"Pairs":                   func() { Pairs(odd...) },
		"AppendToOutgoingContext": func() { AppendToOutgoingContext(ctx, odd...) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s of an odd count did not panic", name)
				}
			}()
			f()
		}()
	}
}
