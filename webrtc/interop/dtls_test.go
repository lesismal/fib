package interop

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"net/netip"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/pion/dtls/v3"
)

// srtpLabel is the exporter label DTLS-SRTP takes its keys with (RFC 5764
// section 4.2).
const srtpLabel = "EXTRACTOR-dtls_srtp"

// fibDTLS runs a sans-IO fib DTLSConn on a UDP socket: one goroutine owns
// the connection, handing it each datagram, calling HandleTimeout when it
// is due, and making the writes queued on writes once it is established.
type fibDTLS struct {
	conn   *fibtls.DTLSConn
	udp    *net.UDPConn
	peer   netip.AddrPort
	writes chan []byte
	data   chan []byte
	done   chan struct{} // closed once the handshake is complete
	ended  chan error

	// Set once the handshake is complete, before done closes.
	keys     []byte
	profile  uint16
	peerCert []byte
}

func newFibDTLS(t *testing.T, client bool, cert stdtls.Certificate, peerCert []byte, peer netip.AddrPort) *fibDTLS {
	t.Helper()
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	d := &fibDTLS{udp: udp, peer: peer, writes: make(chan []byte, 16), data: make(chan []byte, 16),
		done: make(chan struct{}), ended: make(chan error, 1)}
	config := &stdtls.Config{
		Certificates: []stdtls.Certificate{cert},
		// WebRTC checks the peer's certificate against the fingerprint
		// its SDP carries; here, against the certificate itself.
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 || !bytes.Equal(raw[0], peerCert) {
				return errors.New("not the peer's certificate")
			}
			return nil
		},
	}
	opts := fibtls.DTLSOptions{
		Output: func(b []byte) { _, _ = udp.WriteToUDPAddrPort(b, d.peer) },
		OnData: func(b []byte) {
			d.data <- append([]byte(nil), b...)
		},
		SRTPProfiles: []uint16{fibtls.SRTP_AEAD_AES_128_GCM, fibtls.SRTP_AES128_CM_HMAC_SHA1_80},
	}
	if client {
		config.InsecureSkipVerify = true
		d.conn, err = fibtls.NewDTLSClient(config, opts)
	} else {
		config.ClientAuth = stdtls.RequireAnyClientCert
		d.conn, err = fibtls.NewDTLSServer(config, opts)
	}
	if err != nil {
		t.Fatal(err)
	}
	go d.run()
	t.Cleanup(func() {
		_ = udp.Close()
		<-d.ended
	})
	return d
}

func (d *fibDTLS) addr() netip.AddrPort { return d.udp.LocalAddr().(*net.UDPAddr).AddrPort() }

func (d *fibDTLS) run() {
	defer close(d.ended)
	if err := d.conn.Start(time.Now()); err != nil {
		d.ended <- err
		return
	}
	buf := make([]byte, 2048)
	established := false
	for {
		if established {
			for queued := true; queued; {
				select {
				case b := <-d.writes:
					if err := d.conn.Write(b); err != nil {
						d.ended <- err
						return
					}
				default:
					queued = false
				}
			}
		}
		deadline := time.Now().Add(10 * time.Millisecond)
		if at, ok := d.conn.NextTimeout(); ok && at.Before(deadline) {
			deadline = at
		}
		_ = d.udp.SetReadDeadline(deadline)
		n, from, err := d.udp.ReadFromUDPAddrPort(buf)
		now := time.Now()
		var ne net.Error
		switch {
		case errors.As(err, &ne) && ne.Timeout():
			if at, ok := d.conn.NextTimeout(); ok && !now.Before(at) {
				d.conn.HandleTimeout(now)
			}
			continue
		case err != nil:
			return // the socket was closed
		}
		d.peer = from
		if err := d.conn.HandleDatagram(buf[:n], now); err != nil {
			d.ended <- err
			return
		}
		if !established && d.conn.HandshakeComplete() {
			established = true
			d.keys, _ = d.conn.ExportKeyingMaterial(srtpLabel, nil, 60)
			d.profile, _ = d.conn.SelectedSRTPProfile()
			if certs := d.conn.ConnectionState().PeerCertificates; len(certs) > 0 {
				d.peerCert = certs[0].Raw
			}
			close(d.done)
		}
	}
}

// rsaCertificate makes a self-signed RSA certificate, as some WebRTC
// stacks still use.
func rsaCertificate(t *testing.T) stdtls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "WebRTC"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func pionDTLSConfig(cert stdtls.Certificate, peerCert []byte) *dtls.Config {
	return &dtls.Config{
		Certificates:           []stdtls.Certificate{cert},
		InsecureSkipVerify:     true,
		ClientAuth:             dtls.RequireAnyClientCert,
		ExtendedMasterSecret:   dtls.RequireExtendedMasterSecret,
		SRTPProtectionProfiles: []dtls.SRTPProtectionProfile{dtls.SRTP_AEAD_AES_128_GCM, dtls.SRTP_AES128_CM_HMAC_SHA1_80},
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 || !bytes.Equal(raw[0], peerCert) {
				return errors.New("not the peer's certificate")
			}
			return nil
		},
	}
}

// TestDTLS runs fib's DTLS against pion's both ways round, with ECDSA and
// RSA certificates that each side checks the other's against: use_srtp
// must pick AES-128-GCM, both sides must export the same SRTP keying
// material, and data must be echoed.
func TestDTLS(t *testing.T) {
	for _, kind := range []string{"ECDSA", "RSA"} {
		for _, fibClient := range []bool{true, false} {
			name := kind + "/fib server"
			if fibClient {
				name = kind + "/fib client"
			}
			t.Run(name, func(t *testing.T) {
				var fibCert, pionCert stdtls.Certificate
				if kind == "RSA" {
					fibCert, pionCert = rsaCertificate(t), rsaCertificate(t)
				} else {
					var err error
					if fibCert, err = fibtls.GenerateWebRTCCertificate(); err != nil {
						t.Fatal(err)
					}
					if pionCert, err = fibtls.GenerateWebRTCCertificate(); err != nil {
						t.Fatal(err)
					}
				}
				runDTLS(t, fibClient, fibCert, pionCert)
			})
		}
	}
}

func runDTLS(t *testing.T, fibClient bool, fibCert, pionCert stdtls.Certificate) {
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	config := pionDTLSConfig(pionCert, fibCert.Certificate[0])
	var f *fibDTLS
	var p *dtls.Conn
	if fibClient {
		l, err := dtls.Listen("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, config)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		accepted := make(chan net.Conn, 1)
		go func() {
			c, _ := l.Accept()
			accepted <- c
		}()
		f = newFibDTLS(t, true, fibCert, pionCert.Certificate[0], l.Addr().(*net.UDPAddr).AddrPort())
		select {
		case c := <-accepted:
			if c == nil {
				t.Fatal("pion accepted nothing")
			}
			p = c.(*dtls.Conn)
		case <-ctx.Done():
			t.Fatal("pion accepted nothing")
		}
	} else {
		f = newFibDTLS(t, false, fibCert, pionCert.Certificate[0], netip.AddrPort{})
		var err error
		if p, err = dtls.Dial("udp4", net.UDPAddrFromAddrPort(f.addr()), config); err != nil {
			t.Fatal(err)
		}
	}
	defer p.Close()
	if err := p.HandshakeContext(ctx); err != nil {
		t.Fatalf("pion handshake: %v", err)
	}
	select {
	case <-f.done:
	case err := <-f.ended:
		t.Fatalf("fib ended: %v", err)
	case <-ctx.Done():
		t.Fatal("fib's handshake did not complete")
	}

	state, ok := p.ConnectionState()
	if !ok {
		t.Fatal("pion has no connection state")
	}
	keys, err := state.ExportKeyingMaterial(srtpLabel, nil, 60)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(keys, f.keys) {
		t.Fatalf("keying material differs:\nfib  %x\npion %x", f.keys, keys)
	}
	profile, _ := p.SelectedSRTPProtectionProfile()
	if f.profile != fibtls.SRTP_AEAD_AES_128_GCM || uint16(profile) != f.profile {
		t.Fatalf("SRTP profile fib %#x pion %#x, want %#x", f.profile, profile, fibtls.SRTP_AEAD_AES_128_GCM)
	}
	if !bytes.Equal(f.peerCert, pionCert.Certificate[0]) {
		t.Fatal("fib does not report pion's certificate")
	}

	// Echo: pion echoes what fib sends when fib is the client, and fib
	// what pion sends when it is the server.
	msgs := [][]byte{[]byte("hello over DTLS"), randomBytes(1000)}
	if fibClient {
		go func() {
			buf := make([]byte, 2048)
			for {
				n, err := p.Read(buf)
				if err != nil {
					return
				}
				_, _ = p.Write(buf[:n])
			}
		}()
		for _, m := range msgs {
			f.writes <- m
			select {
			case got := <-f.data:
				if !bytes.Equal(got, m) {
					t.Fatalf("got %d bytes back, want %d", len(got), len(m))
				}
			case <-ctx.Done():
				t.Fatalf("%d bytes did not come back", len(m))
			}
		}
		return
	}
	go func() {
		for {
			select {
			case b := <-f.data:
				f.writes <- b
			case <-ctx.Done():
				return
			}
		}
	}()
	buf := make([]byte, 2048)
	for _, m := range msgs {
		if _, err := p.Write(m); err != nil {
			t.Fatal(err)
		}
		_ = p.SetReadDeadline(time.Now().Add(waitTimeout))
		n, err := p.Read(buf)
		if err != nil {
			t.Fatalf("%d bytes did not come back: %v", len(m), err)
		}
		if !bytes.Equal(buf[:n], m) {
			t.Fatalf("got %d bytes back, want %d", n, len(m))
		}
	}
}
