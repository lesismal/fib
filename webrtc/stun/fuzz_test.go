package stun

import (
	"bytes"
	"testing"
)

// FuzzDecode feeds Decode arbitrary bytes: it either fails or yields a
// message whose getters and checks run without panicking, and which, built
// again attribute by attribute, decodes to the same attributes.
func FuzzDecode(f *testing.F) {
	f.Add(sampleRequest)
	f.Add(sampleIPv4Response)
	f.Add(sampleIPv6Response)
	f.Add(sampleLongTermRequest)
	f.Add(sampleSHA256Request)
	f.Fuzz(func(t *testing.T, b []byte) {
		var m Message
		if m.Decode(b) != nil {
			return
		}
		if !IsMessage(b) && b[0] < 4 {
			t.Fatal("decoded a message IsMessage rejects")
		}
		for _, typ := range []AttrType{AttrMappedAddress, AttrXORMappedAddress, AttrXORPeerAddress} {
			_, _ = m.Address(typ)
			_, _ = m.XORAddress(typ)
		}
		_, _, _ = m.ErrorCode()
		_, _ = m.UnknownAttributes(nil)
		_, _ = m.Uint32(AttrPriority)
		_, _ = m.Uint64(AttrICEControlling)
		_, _ = m.Lifetime()
		_ = m.CheckIntegrity([]byte("key"))
		_ = m.CheckIntegritySHA256([]byte("key"))
		_ = m.CheckFingerprint()

		var r Message
		r.Build(m.Type, m.TransactionID)
		for i := range m.Len() {
			typ, v := m.Attr(i)
			r.Add(typ, v)
		}
		var d Message
		if err := d.Decode(r.Raw); err != nil {
			t.Fatalf("re-encoded message does not decode: %v", err)
		}
		if d.Type != m.Type || d.TransactionID != m.TransactionID || d.Len() != m.Len() {
			t.Fatalf("re-encoded %v %d attrs, was %v %d", d.Type, d.Len(), m.Type, m.Len())
		}
		for i := range m.Len() {
			t1, v1 := m.Attr(i)
			t2, v2 := d.Attr(i)
			if t1 != t2 || !bytes.Equal(v1, v2) {
				t.Fatalf("attribute %d: %v %x became %v %x", i, t1, v1, t2, v2)
			}
		}
	})
}
