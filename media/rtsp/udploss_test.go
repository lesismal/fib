package rtsp

import (
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc/rtp"
)

// playUDP sets a raw client up to play path over UDP to two local sockets,
// and returns them, with the stream's route on the server.
func playUDP(t *testing.T, base string, srv *Server, path string) (*rawClient, *net.UDPConn, *net.UDPConn, *udpRoute) {
	t.Helper()
	listen := func() *net.UDPConn {
		uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { uc.Close() })
		return uc
	}
	rtpConn, rtcpConn := listen(), listen()
	rtpPort, rtcpPort := rtpConn.LocalAddr().(*net.UDPAddr).Port, rtcpConn.LocalAddr().(*net.UDPAddr).Port
	r := dialRaw(t, base)
	expect(t, r.do(MethodDescribe, base+path), StatusOK)
	expect(t, r.do(MethodSetup, base+path+"/trackID=0",
		"Transport: RTP/AVP;unicast;client_port="+strconv.Itoa(rtpPort)+"-"+strconv.Itoa(rtcpPort)), StatusOK)
	expect(t, r.do(MethodPlay, base+path), StatusOK)
	srv.mu.Lock()
	route := srv.routes[netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(rtpPort))]
	srv.mu.Unlock()
	if route == nil {
		t.Fatal("no route for the player's RTP port")
	}
	return r, rtpConn, rtcpConn, route
}

// TestPlayerUDPVideoWait checks that a UDP player whose video datagram the
// socket refused sends no more video until the next keyframe.
func TestPlayerUDPVideoWait(t *testing.T) {
	h := hub.New(hub.Config{})
	base, srv, e := serve(t, ServerConfig{Hub: h})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	_, rtpConn, _, route := playUDP(t, base, srv, "s")
	p, st := route.sess.player, route.st
	var video []*av.Packet
	for _, pkt := range testPackets(20) {
		if pkt.Track == 0 {
			video = append(video, pkt)
		}
	}
	key, delta := video[0].Data, video[1].Data
	// The player starts at a keyframe once subscribed, which PLAY's
	// response may come before: send keyframes until one arrives.
	for i := 0; ; i++ {
		at := time.Duration(i) * 40 * time.Millisecond
		pub.WritePacket(&av.Packet{Track: 0, DTS: at, PTS: at, Keyframe: true, Data: key})
		_ = rtpConn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, _, err := rtpConn.ReadFromUDP(make([]byte, 2048)); err == nil {
			break
		}
		if i > 200 {
			t.Fatal("the player never received a datagram")
		}
	}
	waiting := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return st.videoWait
	}
	// A sender that refuses everything loses the next frame.
	p.mu.Lock()
	working := st.rtpOut
	lost := newUDPSender(e, 0, netip.AddrPort{}, 0)
	lost.close()
	st.rtpOut = lost
	p.mu.Unlock()
	pub.WritePacket(&av.Packet{Track: 0, DTS: 20 * time.Second, PTS: 20 * time.Second, Data: delta})
	if !waiting() {
		t.Fatal("a lost video datagram did not hold video back")
	}
	p.mu.Lock()
	st.rtpOut = working
	p.mu.Unlock()
	// Frames that need the lost one stay held back.
	pub.WritePacket(&av.Packet{Track: 0, DTS: 21 * time.Second, PTS: 21 * time.Second, Data: delta})
	if !waiting() {
		t.Fatal("video resumed before a keyframe")
	}
	pub.WritePacket(&av.Packet{Track: 0, DTS: 22 * time.Second, PTS: 22 * time.Second, Keyframe: true, Data: key})
	if waiting() {
		t.Fatal("a keyframe did not resume video")
	}
}

// receiverReport returns an RTCP receiver report about ssrc with fraction
// lost out of 256.
func receiverReport(t *testing.T, ssrc uint32, fraction uint8) []byte {
	t.Helper()
	rr := &rtp.ReceiverReport{SSRC: 1, Reports: []rtp.ReceptionReport{{SSRC: ssrc, FractionLost: fraction}}}
	b, err := rtp.AppendRTCP(nil, rr)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPlayerUDPLoss checks that a UDP player whose receiver reports say it
// loses more than MaxPlayerLoss for PlayerLossWindow is closed, and that a
// report under it starts the window again.
func TestPlayerUDPLoss(t *testing.T) {
	h := hub.New(hub.Config{})
	window := 300 * time.Millisecond
	base, srv, _ := serve(t, ServerConfig{Hub: h, MaxPlayerLoss: 0.25, PlayerLossWindow: window})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	r, _, rtcpConn, route := playUDP(t, base, srv, "s")
	ssrc := route.st.pk.SSRC()
	_, rtcpPort := srv.UDPPorts()
	server := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rtcpPort}
	send := func(fraction uint8) {
		_, _ = rtcpConn.WriteToUDP(receiverReport(t, ssrc, fraction), server)
	}
	// Heavy loss broken by a good report, and loss under the limit, for
	// longer than the window: the player stays.
	deadline := time.Now().Add(2 * window)
	for i := 0; time.Now().Before(deadline); i++ {
		switch {
		case i%4 == 3:
			send(10)
		default:
			send(200)
		}
		time.Sleep(window / 6)
	}
	// Reports about another source, other RTCP and garbage count for
	// nothing.
	sdes, _ := rtp.AppendRTCP(nil, &rtp.SourceDescription{})
	_, _ = rtcpConn.WriteToUDP(append(sdes, 0x80), server)
	(&player{}).receiverReport(nil, receiverReport(t, ssrc, 255), time.Now())
	_, _ = rtcpConn.WriteToUDP(receiverReport(t, ssrc+1, 255), server)
	expect(t, r.do(MethodOptions, base+"s"), StatusOK)
	// Heavy loss for the whole window closes it.
	closed := make(chan error, 1)
	go func() {
		for {
			if err, ok := r.next(false).(error); ok {
				closed <- err
				return
			}
		}
	}()
	for {
		send(200)
		select {
		case err := <-closed:
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("the player's connection stayed open")
			}
			return
		case <-time.After(window / 6):
		}
		if time.Since(deadline) > testTimeout {
			t.Fatal("a player losing most of its packets was not closed")
		}
	}
}
