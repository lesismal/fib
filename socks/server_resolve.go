package socks

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/sidepool"
)

// dial opens a connection to req's destination with Config.Dial. When Dial is
// the default and AllowResolved is set, the destination is resolved here
// rather than by the dial, AllowResolved decides the address it resolved to,
// and that address, not the name, is dialed, so that what is connected to is
// what AllowResolved granted.
func (s *Server) dial(engine *fib.Engine, network string, req *Request, handler fib.Handler,
	done func(*fib.Connection, error)) error {
	timeout := s.config.DialTimeout
	if s.config.AllowResolved == nil || !s.direct {
		return s.config.Dial(engine, network, req.Addr, timeout, handler, done)
	}
	host, portText, err := net.SplitHostPort(req.Addr)
	if err != nil {
		return err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return errors.New("socks: invalid port " + strconv.Quote(portText))
	}
	if host == "" {
		host = "0.0.0.0"
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		addr, err := s.allowResolved(req, []netip.Addr{ip}, uint16(port))
		if err != nil {
			return err
		}
		return DialDirect(engine, network, addr.String(), timeout, handler, done)
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	sidepool.Go(func() {
		ctx := context.Background()
		if !deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		var addr netip.AddrPort
		if err == nil {
			addr, err = s.allowResolved(req, ips, uint16(port))
		}
		if err == nil {
			remaining := timeout
			if !deadline.IsZero() {
				if remaining = time.Until(deadline); remaining <= 0 {
					remaining = time.Millisecond
				}
			}
			if err = DialDirect(engine, network, addr.String(), remaining, handler, done); err == nil {
				return
			}
		}
		done(nil, err)
	})
	return nil
}

// allowResolved returns the first of ips, IPv4 ones first as the net
// package's dial takes them, with port, that AllowResolved grants, or the
// refusal of the first of them when it grants none. An unspecified address
// is asked about as the loopback address the dial would reach.
func (s *Server) allowResolved(req *Request, ips []netip.Addr, port uint16) (netip.AddrPort, error) {
	var refused error
	for _, v4 := range [...]bool{true, false} {
		for _, ip := range ips {
			if ip = ip.Unmap(); ip.Is4() != v4 {
				continue
			}
			if ip.IsUnspecified() {
				ip = netip.IPv6Loopback()
				if v4 {
					ip = netip.AddrFrom4([4]byte{127, 0, 0, 1})
				}
			}
			addr := netip.AddrPortFrom(ip, port)
			err := s.config.AllowResolved(req, addr)
			if err == nil {
				return addr, nil
			}
			if refused == nil {
				refused = err
			}
		}
	}
	if refused == nil {
		return netip.AddrPort{}, &net.DNSError{Err: "no such host", Name: req.Addr, IsNotFound: true}
	}
	var replyErr *ReplyError
	if !errors.As(refused, &replyErr) {
		refused = &ReplyError{Version: req.Version, Code: ReplyNotAllowed}
	}
	return netip.AddrPort{}, refused
}
