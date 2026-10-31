package ice

import (
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// FuzzHandlePacket feeds an agent that is checking arbitrary packets, valid
// and forged checks among them: it answers or drops them and never panics,
// and its timers stay sane.
func FuzzHandlePacket(f *testing.F) {
	const ufrag, pwd = "aaaa", "aaaaaaaaaaaaaaaaaaaaaa"
	var m stun.Message
	m.Build(stun.BindingRequest, [12]byte{1})
	m.AddString(stun.AttrUsername, ufrag+":bbbb")
	m.AddUint32(stun.AttrPriority, 100)
	m.AddUint64(stun.AttrICEControlling, 7)
	m.AddFlag(stun.AttrUseCandidate)
	m.AddIntegrity([]byte(pwd))
	m.AddFingerprint()
	f.Add(m.Raw, uint8(0))
	m.Build(stun.BindingSuccess, [12]byte{2})
	m.AddXORAddress(stun.AttrXORMappedAddress, addrA)
	m.AddIntegrity([]byte("bbbbbbbbbbbbbbbbbbbbbb"))
	f.Add(m.Raw, uint8(1))
	m.Build(stun.BindingError, [12]byte{3})
	m.AddErrorCode(stun.CodeRoleConflict, "")
	f.Add(m.Raw, uint8(2))
	f.Fuzz(func(t *testing.T, b []byte, from uint8) {
		a, err := NewAgent(Config{Ufrag: ufrag, Pwd: pwd, Send: func(LocalID, netip.AddrPort, []byte) {}})
		if err != nil {
			t.Fatal(err)
		}
		now := time.Unix(0, 0)
		_ = a.AddLocalCandidate(Candidate{Type: Host, Address: addrA.Addr(), Port: addrA.Port(), LocalID: 1}, now)
		_ = a.SetRemoteCredentials("bbbb", "bbbbbbbbbbbbbbbbbbbbbb", now)
		_ = a.AddRemoteCandidate(Candidate{Type: Host, Address: addrB.Addr(), Port: addrB.Port(), Priority: 1}, now)
		a.HandleTimeout(now)
		src := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, 0, from}), 2000)
		_ = a.HandlePacket(1, src, b, now)
		for range 3 {
			next, ok := a.NextTimeout()
			if !ok {
				break
			}
			a.HandleTimeout(next)
		}
	})
}
