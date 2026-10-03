package metadata

import (
	"context"
	"fmt"
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
	in := NewIncomingContext(context.Background(), Pairs("k", "v"))
	got, _ := FromIncomingContext(in)
	got["k"][0] = "changed"
	if fmt.Sprint(ValueFromIncomingContext(in, "K")) != "[v]" {
		t.Fatal("FromIncomingContext did not copy")
	}
}
