package turn

import (
	"bytes"
	"net/netip"
	"testing"

	"github.com/lesismal/fib/webrtc/stun"
)

func TestFrameLen(t *testing.T) {
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	m.AddString(stun.AttrSoftware, "x")
	badCookie := bytes.Clone(m.Raw)
	badCookie[4] ^= 1
	for _, tc := range []struct {
		name string
		b    []byte
		n    int
		err  error
	}{
		{"empty", nil, 0, nil},
		{"short", []byte{0, 1, 0}, 0, nil},
		{"STUN header part", m.Raw[:6], 0, nil},
		{"STUN", m.Raw, len(m.Raw), nil},
		{"STUN prefix of more", append(bytes.Clone(m.Raw), 0x40), len(m.Raw), nil},
		{"STUN length not of four", []byte{0, 1, 0, 3, 0x21, 0x12, 0xa4, 0x42}, 0, ErrNotTURN},
		{"bad cookie", badCookie, 0, ErrNotTURN},
		{"ChannelData padded", AppendChannelData(nil, MinChannel, []byte("hello")), 12, nil},
		{"ChannelData aligned", AppendChannelData(nil, MaxChannel, []byte("four")), 8, nil},
		{"TLS record", []byte{0x16, 3, 1, 0, 5}, 0, ErrNotTURN},
	} {
		n, err := FrameLen(tc.b)
		if n != tc.n || err != tc.err {
			t.Errorf("%s: %d, %v; want %d, %v", tc.name, n, err, tc.n, tc.err)
		}
	}
}

// FuzzFrameLen frames arbitrary bytes as a stream would: whatever a frame
// claims is within MaxFrameSize, and framing never loops.
func FuzzFrameLen(f *testing.F) {
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	f.Add(append(bytes.Clone(m.Raw), padChannelData(AppendChannelData(nil, MinChannel, []byte("abc")))...))
	f.Add([]byte{0x40, 0, 0xff, 0xff})
	f.Add([]byte{0, 1, 0xff, 0xfc, 0x21, 0x12, 0xa4, 0x42})
	f.Fuzz(func(t *testing.T, b []byte) {
		for len(b) > 0 {
			n, err := FrameLen(b)
			if err != nil || n == 0 {
				return
			}
			if n < ChannelDataHeaderSize || n > MaxFrameSize || n%4 != 0 {
				t.Fatalf("frame of %d", n)
			}
			if n > len(b) {
				return
			}
			b = b[n:]
		}
	})
}

// Over a stream the server pads ChannelData, and a closed connection frees
// its allocation, or the one being made.
func TestServerStream(t *testing.T) {
	c := newCore(t, ServerConfig{})
	u := c.user(clientAddr, testUser, testPass)
	tcp := FiveTuple{Client: clientAddr, Server: serverAddr, Transport: TransportTCP}
	if tcp.Transport.String() != "tcp" || TransportUDP.String() != "udp" {
		t.Fatal("transport names")
	}
	// Drive the requests by hand on the TCP 5-tuple.
	ut := &tcpUser{u}
	if r := ut.do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatal(code(r))
	}
	if _, ok := c.s.RelayFor(FiveTuple{Client: clientAddr, Server: serverAddr}); ok {
		t.Fatal("the UDP 5-tuple has the TCP one's allocation")
	}
	_ = ut.do(stun.MethodChannelBind, channelReq(MinChannel, peerAddr))
	relayed, _ := c.s.RelayFor(tcp)
	n := len(c.sent)
	if err := c.s.HandleRelayPacket(relayed, peerAddr, []byte("hello"), c.now); err != nil {
		t.Fatal(err)
	}
	if b := c.sent[n].b; len(b) != 12 || c.sent[n].t != tcp {
		t.Fatalf("ChannelData over TCP: %x to %v", b, c.sent[n].t)
	}
	c.s.ClientClosed(tcp)
	if c.s.Allocations() != 0 || !c.relays[relayed].closed {
		t.Fatal("a closed connection kept its allocation")
	}
	c.s.ClientClosed(tcp)

	// An allocation in the making for a connection that closes.
	c2, dones := asyncCore(t, ServerConfig{})
	w := &tcpUser{c2.user(clientAddr, testUser, testPass)}
	_ = w.do(stun.MethodRefresh, nil)
	_ = w.send(stun.MethodAllocate, allocateReq)
	c2.s.ClientClosed(tcp)
	late := &coreRelay{c: c2, addr: netip.AddrPortFrom(relayIP, 53000)}
	m := len(c2.sent)
	(*dones)[0](late, nil)
	if !late.closed || len(c2.sent) != m || c2.s.Allocations() != 0 {
		t.Fatal("an allocation outlived its connection")
	}
}

// tcpUser is a user whose requests come over TCP.
type tcpUser struct{ u *user }

func (w *tcpUser) tuple() FiveTuple {
	return FiveTuple{Client: w.u.addr, Server: serverAddr, Transport: TransportTCP}
}

// do is user.do on the TCP 5-tuple.
func (w *tcpUser) do(method stun.Method, add func(m *stun.Message)) *stun.Message {
	w.u.c.t.Helper()
	for range 3 {
		w.send(method, add)
		r := w.u.c.last()
		if code(r) == stun.CodeUnauthorized && w.u.nonce == "" {
			n, _ := r.Get(stun.AttrNonce)
			w.u.nonce = string(n)
			continue
		}
		return r
	}
	w.u.c.t.Fatal("no answer but challenges")
	return nil
}

func (w *tcpUser) send(method stun.Method, add func(m *stun.Message)) error {
	var m stun.Message
	m.Build(stun.MessageType{Method: method, Class: stun.ClassRequest}, stun.NewTransactionID())
	if add != nil {
		add(&m)
	}
	if w.u.nonce != "" {
		m.AddString(stun.AttrUsername, w.u.name)
		m.AddString(stun.AttrRealm, testRealm)
		m.AddString(stun.AttrNonce, w.u.nonce)
		m.AddIntegrity(w.u.key)
	}
	return w.u.c.s.HandlePacket(w.tuple(), m.Raw, w.u.c.now)
}

// A client over a stream pads its ChannelData and sends each request once.
func TestClientStream(t *testing.T) {
	var sent [][]byte
	c, err := NewClient(ClientConfig{Server: serverAddr, Username: testUser, Password: testPass, Stream: true, BindChannels: true,
		Send: func(_ netip.AddrPort, b []byte) { sent = append(sent, bytes.Clone(b)) }})
	if err != nil {
		t.Fatal(err)
	}
	if !c.cfg.Transaction.Reliable {
		t.Fatal("a stream client retransmits")
	}
	// Bind a channel by hand and send on it.
	c.state = clientAllocated
	c.chans[peerAddr] = &channel{number: MinChannel, bound: true}
	if err := c.SendTo(peerAddr, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if b := sent[len(sent)-1]; len(b) != 12 || !bytes.Equal(b[4:9], []byte("hello")) {
		t.Fatalf("ChannelData %x", b)
	}
}
