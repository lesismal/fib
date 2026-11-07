package bittorrent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/netip"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// Local Service Discovery (BEP 14): peers on the local network announce
// the torrents they have to a multicast group, in HTTP-like messages. fib
// has no multicast UDP bindings, so this joins the group with the
// standard library's net.ListenMulticastUDP, on a goroutine of its own:
// a few messages every five minutes need no event loop.

// LSDGroup is BEP 14's IPv4 multicast group.
const LSDGroup = "239.192.152.143:6771"

// lsd is local service discovery: a reader of the group and a sender to
// it. It is the PeerSource of the torrents that are not private.
type lsd struct {
	c      *Client
	cookie string
	group  *net.UDPAddr
	recv   *net.UDPConn
	send   *net.UDPConn

	mu       sync.Mutex
	torrents map[metainfo.InfoHash]*Torrent
}

func startLSD(c *Client) (*lsd, error) {
	group, err := net.ResolveUDPAddr("udp4", LSDGroup)
	if err != nil {
		return nil, err
	}
	recv, err := net.ListenMulticastUDP("udp4", nil, group)
	if err != nil {
		return nil, err
	}
	send, err := net.ListenUDP("udp4", nil)
	if err != nil {
		recv.Close()
		return nil, err
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	l := &lsd{c: c, cookie: hex.EncodeToString(b[:]), group: group, recv: recv, send: send, torrents: map[metainfo.InfoHash]*Torrent{}}
	go l.readLoop()
	return l, nil
}

func (l *lsd) close() {
	l.recv.Close()
	l.send.Close()
}

func (l *lsd) readLoop() {
	buf := make([]byte, 1500)
	for {
		n, from, err := l.recv.ReadFromUDP(buf)
		if err != nil {
			return
		}
		port, hashes, cookie, ok := parseLSD(buf[:n])
		if !ok || cookie == l.cookie {
			continue
		}
		ap := netip.AddrPortFrom(from.AddrPort().Addr().Unmap(), port)
		l.mu.Lock()
		var ts []*Torrent
		for _, h := range hashes {
			if t := l.torrents[h]; t != nil {
				ts = append(ts, t)
			}
		}
		l.mu.Unlock()
		for _, t := range ts {
			t.addPeers(SourceLSD, false, []netip.AddrPort{ap})
		}
	}
}

// parseLSD parses a BT-SEARCH message: its port, info hashes and cookie.
func parseLSD(b []byte) (port uint16, hashes []metainfo.InfoHash, cookie string, ok bool) {
	r := textproto.NewReader(bufio.NewReader(bytes.NewReader(b)))
	line, err := r.ReadLine()
	if err != nil || !strings.HasPrefix(line, "BT-SEARCH * HTTP/1.") {
		return 0, nil, "", false
	}
	h, err := r.ReadMIMEHeader()
	if err != nil && len(h) == 0 {
		return 0, nil, "", false
	}
	p, err := strconv.ParseUint(strings.TrimSpace(h.Get("Port")), 10, 16)
	if err != nil || p == 0 {
		return 0, nil, "", false
	}
	for _, v := range h.Values("Infohash") {
		if ih, err := metainfo.ParseInfoHash(strings.TrimSpace(v)); err == nil {
			hashes = append(hashes, ih)
		}
	}
	if len(hashes) == 0 {
		return 0, nil, "", false
	}
	return uint16(p), hashes, strings.TrimSpace(h.Get("Cookie")), true
}

// appendLSD appends the BT-SEARCH message announcing hashes at port.
func appendLSD(dst []byte, port int, cookie string, hashes []metainfo.InfoHash) []byte {
	dst = fmt.Appendf(dst, "BT-SEARCH * HTTP/1.1\r\nHost: %s\r\nPort: %d\r\n", LSDGroup, port)
	for _, h := range hashes {
		dst = fmt.Appendf(dst, "Infohash: %s\r\n", strings.ToUpper(hex.EncodeToString(h[:])))
	}
	return fmt.Appendf(dst, "cookie: %s\r\n\r\n\r\n", cookie)
}

// Find implements PeerSource: it announces the torrent every five
// minutes, as BEP 14 allows, and hears the announces of others while it
// runs.
func (l *lsd) Find(ctx context.Context, t *Torrent, found func([]netip.AddrPort)) {
	hashes := t.announceHashes()
	l.mu.Lock()
	for _, h := range hashes {
		l.torrents[h] = t
	}
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		for _, h := range hashes {
			if l.torrents[h] == t {
				delete(l.torrents, h)
			}
		}
		l.mu.Unlock()
	}()
	for {
		_, _ = l.send.WriteToUDP(appendLSD(nil, l.c.port, l.cookie, hashes), l.group)
		select {
		case <-ctx.Done():
			return
		case <-time.After(l.c.cfg.LSDInterval):
		}
	}
}
