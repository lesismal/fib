// Package socks speaks the SOCKS proxy protocols over the engine's
// connections: SOCKS5 (RFC 1928) with username/password authentication (RFC
// 1929), and SOCKS4 with its 4a extension.
//
// A Server is a fib.Handler that serves SOCKS clients: it is bound as any other
// handler is, or put behind package mux on a port shared with other protocols,
// whose mux.SOCKS matcher recognises it. It carries CONNECT, BIND and UDP
// ASSOCIATE, and once a CONNECT's destination answers, what either side
// sends goes to the other from the callback that received it, with no
// goroutine in between:
//
//	server := socks.NewServer(socks.Config{})
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":1080", Handler: server},
//		fib.Binding{Network: "udp", Addr: ":1080", Handler: server}, // for UDP ASSOCIATE
//	)
//
// A Dialer opens connections through a SOCKS proxy without blocking, the way
// fib.Engine.DialWithHandler opens them directly: the handler it is given
// sees an ordinary connection, opened once the proxy has reached the
// destination. Its Dial method fits the Dial fields of package http's
// ClientConfig, package websocket's DialerConfig and package tls's
// DialOptions, which then reach their servers through the proxy:
//
//	proxy, err := socks.ParseURL("socks5h://user:pass@127.0.0.1:1080")
//	clientConfig := fibhttp.DefaultClientConfig()
//	clientConfig.Dial = proxy.Dial
package socks

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
)

// DialFunc opens a connection the way fib.Engine.DialWithHandler does, on
// engine: it reports the outcome to done exactly once, after handler's
// OnOpen when it succeeds, and returns an error instead, without calling
// done, only when the dial cannot start. Dialer.Dial is one, and so is
// DialDirect.
type DialFunc func(engine *fib.Engine, network, addr string, timeout time.Duration,
	handler fib.Handler, done func(*fib.Connection, error)) error

// DialDirect is the DialFunc that dials without a proxy: engine's
// DialWithHandler.
func DialDirect(engine *fib.Engine, network, addr string, timeout time.Duration,
	handler fib.Handler, done func(*fib.Connection, error)) error {
	return engine.DialWithHandler(network, addr, timeout, handler, done)
}

// Version is a version of the protocol.
type Version uint8

const (
	// Version5 is SOCKS5 (RFC 1928): IPv4, IPv6 and host names, CONNECT,
	// BIND and UDP ASSOCIATE, and authentication.
	Version5 Version = iota
	// Version4 is SOCKS4: CONNECT and BIND to IPv4 addresses, which the
	// client resolves.
	Version4
	// Version4A is SOCKS4 with the 4a extension, which lets the client hand
	// the proxy a host name to resolve.
	Version4A
)

func (v Version) String() string {
	switch v {
	case Version5:
		return "SOCKS5"
	case Version4:
		return "SOCKS4"
	case Version4A:
		return "SOCKS4a"
	}
	return "SOCKS(" + strconv.Itoa(int(v)) + ")"
}

// Command is what a client asks of the proxy, numbered as on the wire.
type Command uint8

const (
	// CommandConnect asks the proxy to connect to the destination and relay
	// the connection to it.
	CommandConnect Command = 1
	// CommandBind asks the proxy to accept one connection from the
	// destination, as the data connection of FTP's active mode needs, and
	// relay it. A Server accepts it only from the destination's IP, or from
	// one of the addresses its name resolves to; a destination of the
	// unspecified address, 0.0.0.0 or ::, lets any host connect, and an
	// Allow that refuses such BINDs keeps that from happening.
	CommandBind Command = 2
	// CommandUDPAssociate asks the proxy to relay UDP datagrams (SOCKS5
	// only).
	CommandUDPAssociate Command = 3
)

func (c Command) String() string {
	switch c {
	case CommandConnect:
		return "connect"
	case CommandBind:
		return "bind"
	case CommandUDPAssociate:
		return "udp associate"
	}
	return "command(" + strconv.Itoa(int(c)) + ")"
}

// The SOCKS5 reply codes (RFC 1928 section 6).
const (
	ReplySucceeded           byte = 0x00
	ReplyGeneralFailure      byte = 0x01
	ReplyNotAllowed          byte = 0x02
	ReplyNetworkUnreachable  byte = 0x03
	ReplyHostUnreachable     byte = 0x04
	ReplyConnectionRefused   byte = 0x05
	ReplyTTLExpired          byte = 0x06
	ReplyCommandNotSupported byte = 0x07
	ReplyAddressNotSupported byte = 0x08
)

// The SOCKS4 reply codes.
const (
	reply4Granted  byte = 0x5a
	reply4Rejected byte = 0x5b
)

// ReplyError is a request the proxy refused, with the code its reply gave.
// A Config.Allow that returns one refuses the request with its Code.
type ReplyError struct {
	Version Version
	Code    byte
}

func (e *ReplyError) Error() string {
	if e.Version != Version5 {
		switch e.Code {
		case reply4Rejected:
			return "socks: request rejected or failed"
		case 0x5c:
			return "socks: request rejected: proxy cannot reach the client's identd"
		case 0x5d:
			return "socks: request rejected: identd reports a different user"
		}
	} else if text := replyText(e.Code); text != "" {
		return "socks: " + text
	}
	return "socks: request failed with code " + strconv.Itoa(int(e.Code))
}

func replyText(code byte) string {
	switch code {
	case ReplyGeneralFailure:
		return "general server failure"
	case ReplyNotAllowed:
		return "connection not allowed by ruleset"
	case ReplyNetworkUnreachable:
		return "network unreachable"
	case ReplyHostUnreachable:
		return "host unreachable"
	case ReplyConnectionRefused:
		return "connection refused"
	case ReplyTTLExpired:
		return "TTL expired"
	case ReplyCommandNotSupported:
		return "command not supported"
	case ReplyAddressNotSupported:
		return "address type not supported"
	}
	return ""
}

var (
	// ErrAuthFailed is what a dial reports when the proxy turned down its
	// username and password.
	ErrAuthFailed = errors.New("socks: authentication failed")
	// ErrNoAcceptableMethod is what a dial reports when the proxy accepts
	// none of the authentication methods the dialer offers.
	ErrNoAcceptableMethod = errors.New("socks: no acceptable authentication method")
	// ErrMalformed is what a dial reports when the proxy does not speak the
	// protocol.
	ErrMalformed = errors.New("socks: malformed message")
)

const (
	version5     = 5
	version4     = 4
	authVersion  = 1
	methodNone   = 0x00
	methodPasswd = 0x02
	methodRefuse = 0xff

	atypIPv4   = 1
	atypDomain = 3
	atypIPv6   = 4
)

// appendAddr appends host and port as a SOCKS5 address: the IP, or the host
// name when host is not an IP literal.
func appendAddr(b []byte, host string, port uint16) ([]byte, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return appendIP(b, ip, port), nil
	}
	if len(host) == 0 || len(host) > 255 {
		return b, errors.New("socks: host name must be 1 to 255 bytes")
	}
	b = append(b, atypDomain, byte(len(host)))
	b = append(b, host...)
	return append(b, byte(port>>8), byte(port)), nil
}

// appendIP appends ip and port as a SOCKS5 address.
func appendIP(b []byte, ip netip.Addr, port uint16) []byte {
	ip = ip.Unmap()
	if ip.Is4() {
		a := ip.As4()
		b = append(b, atypIPv4)
		b = append(b, a[:]...)
	} else {
		a := ip.As16()
		b = append(b, atypIPv6)
		b = append(b, a[:]...)
	}
	return append(b, byte(port>>8), byte(port))
}

// appendNetAddr appends addr, a *net.TCPAddr or *net.UDPAddr, as a SOCKS5
// address; anything else is the unspecified IPv4 address.
func appendNetAddr(b []byte, addr net.Addr) []byte {
	var ap netip.AddrPort
	switch a := addr.(type) {
	case *net.TCPAddr:
		ap = a.AddrPort()
	case *net.UDPAddr:
		ap = a.AddrPort()
	}
	if !ap.Addr().IsValid() {
		return appendIP(b, netip.IPv4Unspecified(), 0)
	}
	return appendIP(b, ap.Addr(), ap.Port())
}

// parseAddr reads a SOCKS5 address from the front of b: its host, an IP
// literal or a name, its port, and how many bytes it took. n is zero when b
// holds only part of it.
func parseAddr(b []byte) (host string, port uint16, n int, err error) {
	if len(b) < 1 {
		return "", 0, 0, nil
	}
	switch b[0] {
	case atypIPv4:
		n = 1 + 4 + 2
		if len(b) < n {
			return "", 0, 0, nil
		}
		host = netip.AddrFrom4([4]byte(b[1:5])).String()
	case atypIPv6:
		n = 1 + 16 + 2
		if len(b) < n {
			return "", 0, 0, nil
		}
		host = netip.AddrFrom16([16]byte(b[1:17])).String()
	case atypDomain:
		if len(b) < 2 {
			return "", 0, 0, nil
		}
		n = 2 + int(b[1]) + 2
		if len(b) < n {
			return "", 0, 0, nil
		}
		if b[1] == 0 {
			return "", 0, 0, ErrMalformed
		}
		host = string(b[2 : 2+b[1]])
	default:
		return "", 0, 0, &ReplyError{Code: ReplyAddressNotSupported}
	}
	return host, uint16(b[n-2])<<8 | uint16(b[n-1]), n, nil
}

// splitHostPort splits addr into its host and a port that fits 16 bits.
func splitHostPort(addr string) (string, uint16, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return "", 0, errors.New("socks: invalid port " + strconv.Quote(portText))
	}
	if zone := strings.IndexByte(host, '%'); zone >= 0 {
		// A zone means nothing to the proxy.
		host = host[:zone]
	}
	return host, uint16(port), nil
}

// replyCode is the SOCKS5 reply that tells a client why its request failed
// with err.
func replyCode(err error) byte {
	var replyErr *ReplyError
	var dnsErr *net.DNSError
	var netErr net.Error
	switch {
	case errors.As(err, &replyErr):
		return replyErr.Code
	case errors.Is(err, syscall.ECONNREFUSED):
		return ReplyConnectionRefused
	case errors.Is(err, syscall.ENETUNREACH):
		return ReplyNetworkUnreachable
	case errors.Is(err, syscall.EHOSTUNREACH), errors.As(err, &dnsErr), errors.Is(err, os.ErrDeadlineExceeded):
		return ReplyHostUnreachable
	case errors.As(err, &netErr) && netErr.Timeout():
		return ReplyHostUnreachable
	}
	return ReplyGeneralFailure
}

// socksAddr is a destination as the proxy is asked for it, which may be a
// name it resolves; it is what a dial's errors name.
type socksAddr struct {
	network string
	addr    string
}

func (a socksAddr) Network() string { return a.network }
func (a socksAddr) String() string  { return a.addr }
