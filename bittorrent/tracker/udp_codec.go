package tracker

import (
	"encoding/binary"
	"errors"
	"math"
	"net/netip"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// This file is the UDP tracker protocol's packets (BEP 15), with the URL
// data option of BEP 41, without any I/O.

// protocolID is the connection ID of a connect request.
const protocolID = 0x41727101980

const (
	actionConnect  = 0
	actionAnnounce = 1
	actionScrape   = 2
	actionError    = 3
)

// The fixed sizes of the packets.
const (
	udpHeaderLen       = 16 // connection ID, action, transaction ID
	udpReplyHeaderLen  = 8  // action, transaction ID
	udpAnnounceLen     = 98
	udpAnnounceRespLen = 20
	// maxScrapeHashes is how many info hashes one UDP scrape asks for; BEP
	// 15 says about 74 fit a packet.
	maxScrapeHashes = 74
	// maxURLData bounds the URL data a server reads from the options of
	// BEP 41, which may be split over options of up to 255 bytes each.
	maxURLData = 2048
)

// BEP 41 option types.
const (
	optionEnd     = 0
	optionNOP     = 1
	optionURLData = 2
)

func appendUDPHeader(dst []byte, conn uint64, action, tx uint32) []byte {
	dst = binary.BigEndian.AppendUint64(dst, conn)
	dst = binary.BigEndian.AppendUint32(dst, action)
	return binary.BigEndian.AppendUint32(dst, tx)
}

func appendConnect(dst []byte, tx uint32) []byte {
	return appendUDPHeader(dst, protocolID, actionConnect, tx)
}

// appendUDPAnnounce appends the announce request of r. urlData is the path
// and query of the tracker's URL, sent in options of BEP 41 when not empty.
func appendUDPAnnounce(dst []byte, conn uint64, tx uint32, r *AnnounceRequest, urlData string) []byte {
	dst = appendUDPHeader(dst, conn, actionAnnounce, tx)
	dst = append(dst, r.InfoHash[:]...)
	dst = append(dst, r.PeerID[:]...)
	left := r.Left
	if left < 0 {
		left = math.MaxInt64
	}
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.Downloaded))
	dst = binary.BigEndian.AppendUint64(dst, uint64(left))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.Uploaded))
	dst = binary.BigEndian.AppendUint32(dst, uint32(r.Event))
	var ip [4]byte
	if a := r.IP.Unmap(); a.Is4() {
		ip = a.As4()
	}
	dst = append(dst, ip[:]...)
	dst = binary.BigEndian.AppendUint32(dst, r.Key)
	numWant := int32(-1)
	switch {
	case r.NumWant > 0:
		numWant = int32(min(r.NumWant, math.MaxInt32))
	case r.NumWant < 0:
		numWant = 0
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(numWant))
	dst = binary.BigEndian.AppendUint16(dst, r.Port)
	for len(urlData) > 0 {
		n := min(len(urlData), 255)
		dst = append(dst, optionURLData, byte(n))
		dst = append(dst, urlData[:n]...)
		urlData = urlData[n:]
	}
	return dst
}

func appendUDPScrape(dst []byte, conn uint64, tx uint32, hashes []metainfo.InfoHash) []byte {
	dst = appendUDPHeader(dst, conn, actionScrape, tx)
	for _, h := range hashes {
		dst = append(dst, h[:]...)
	}
	return dst
}

// udpRequest is a request as a tracker reads it.
type udpRequest struct {
	conn       uint64
	action, tx uint32
	// announce is an announce's fields; announce.IP holds the IP field
	// when it is not zero. numWant is the num_want field, -1 for the
	// tracker's default.
	announce AnnounceRequest
	numWant  int
	urlData  string
	// hashes are a scrape's info hashes, at most maxScrapeHashes.
	hashes []metainfo.InfoHash
}

var errShortPacket = errors.New("tracker: short UDP tracker packet")

// parseUDPRequest reads a request. It reads an announce's options
// leniently: they end at an end option, at the end of the packet, or at one
// that runs past it.
func parseUDPRequest(b []byte) (*udpRequest, error) {
	if len(b) < udpHeaderLen {
		return nil, errShortPacket
	}
	r := &udpRequest{
		conn:   binary.BigEndian.Uint64(b),
		action: binary.BigEndian.Uint32(b[8:]),
		tx:     binary.BigEndian.Uint32(b[12:]),
	}
	switch r.action {
	case actionConnect:
	case actionAnnounce:
		if len(b) < udpAnnounceLen {
			return nil, errShortPacket
		}
		a := &r.announce
		copy(a.InfoHash[:], b[16:36])
		copy(a.PeerID[:], b[36:56])
		a.Downloaded = int64(binary.BigEndian.Uint64(b[56:]))
		a.Left = int64(binary.BigEndian.Uint64(b[64:]))
		a.Uploaded = int64(binary.BigEndian.Uint64(b[72:]))
		a.Event = Event(binary.BigEndian.Uint32(b[80:]))
		if ip := [4]byte(b[84:88]); ip != [4]byte{} {
			a.IP = netip.AddrFrom4(ip)
		}
		a.Key = binary.BigEndian.Uint32(b[88:])
		r.numWant = int(max(int32(binary.BigEndian.Uint32(b[92:])), -1))
		a.NumWant = numWantField(r.numWant)
		a.Port = binary.BigEndian.Uint16(b[96:])
		r.urlData = parseOptions(b[udpAnnounceLen:])
	case actionScrape:
		n := min((len(b)-udpHeaderLen)/20, maxScrapeHashes)
		r.hashes = make([]metainfo.InfoHash, n)
		for i := range r.hashes {
			copy(r.hashes[i][:], b[udpHeaderLen+20*i:])
		}
	default:
		return nil, errors.New("tracker: unknown UDP tracker action")
	}
	return r, nil
}

// parseOptions returns the URL data of BEP 41 options.
func parseOptions(b []byte) string {
	var data []byte
	for len(b) > 0 {
		switch b[0] {
		case optionEnd:
			return string(data)
		case optionNOP:
			b = b[1:]
			continue
		}
		if len(b) < 2 || len(b) < 2+int(b[1]) {
			break
		}
		if b[0] == optionURLData && len(data)+int(b[1]) <= maxURLData {
			data = append(data, b[2:2+int(b[1])]...)
		}
		b = b[2+int(b[1]):]
	}
	return string(data)
}

// udpReply is a reply as a client reads it.
type udpReply struct {
	action, tx uint32
	body       []byte
}

// parseUDPReply reads a reply's header; ok is false for a packet that
// cannot be one.
func parseUDPReply(b []byte) (r udpReply, ok bool) {
	if len(b) < udpReplyHeaderLen {
		return r, false
	}
	r.action = binary.BigEndian.Uint32(b)
	if r.action > actionError {
		return r, false
	}
	r.tx = binary.BigEndian.Uint32(b[4:])
	r.body = b[udpReplyHeaderLen:]
	return r, true
}

// parseUDPAnnounceReply reads the body of an announce's reply, whose peers
// are IPv6 ones when the tracker was asked over IPv6.
func parseUDPAnnounceReply(body []byte, ipv6 bool) (*AnnounceResponse, error) {
	if len(body) < udpAnnounceRespLen-udpReplyHeaderLen {
		return nil, errShortPacket
	}
	r := &AnnounceResponse{
		Interval:   seconds(int64(int32(binary.BigEndian.Uint32(body)))),
		Incomplete: clampInt(int64(int32(binary.BigEndian.Uint32(body[4:])))),
		Complete:   clampInt(int64(int32(binary.BigEndian.Uint32(body[8:])))),
	}
	size := compactV4
	if ipv6 {
		size = compactV6
	}
	r.Peers = parseCompact(nil, body[12:], size)
	return r, nil
}

// parseUDPScrapeReply reads the body of a scrape's reply to hashes. A
// tracker that knows fewer of them than were asked for answers fewer.
func parseUDPScrapeReply(body []byte, hashes []metainfo.InfoHash) map[metainfo.InfoHash]Stats {
	files := make(map[metainfo.InfoHash]Stats, len(hashes))
	for _, h := range hashes {
		if len(body) < 12 {
			break
		}
		files[h] = Stats{
			Complete:   clampInt(int64(int32(binary.BigEndian.Uint32(body)))),
			Downloaded: clampInt(int64(int32(binary.BigEndian.Uint32(body[4:])))),
			Incomplete: clampInt(int64(int32(binary.BigEndian.Uint32(body[8:])))),
		}
		body = body[12:]
	}
	return files
}

func appendUDPReplyHeader(dst []byte, action, tx uint32) []byte {
	dst = binary.BigEndian.AppendUint32(dst, action)
	return binary.BigEndian.AppendUint32(dst, tx)
}

func appendUDPError(dst []byte, tx uint32, msg string) []byte {
	return append(appendUDPReplyHeader(dst, actionError, tx), msg...)
}

func appendInt32(dst []byte, n int) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(int32(min(max(n, 0), math.MaxInt32))))
}

// IsUDPReply reports whether b may be a UDP tracker's reply: a packet of at
// least 8 bytes whose first 4 are an action, 0 to 3. It is for a socket
// shared with uTP and the DHT, whose packets never begin so (a uTP packet's
// first byte is its type and version, 0x01 to 0x41, and a DHT message is a
// bencoded dictionary, 'd'), to choose whom to hand a datagram to; the
// UDPClient's HandleDatagram decides for certain.
func IsUDPReply(b []byte) bool {
	_, ok := parseUDPReply(b)
	return ok
}
