package codes

import "testing"

// Every code has its name, and a code past the last one says its number.
func TestString(t *testing.T) {
	if OK.String() != "OK" || Unauthenticated.String() != "Unauthenticated" || NotFound.String() != "NotFound" {
		t.Errorf("names: %v %v %v", OK, Unauthenticated, NotFound)
	}
	for c := OK; c <= Unauthenticated; c++ {
		if c.String() == "" {
			t.Errorf("code %d has no name", uint32(c))
		}
	}
	if got := Code(17).String(); got != "Code(17)" {
		t.Errorf("Code(17) = %q", got)
	}
}
