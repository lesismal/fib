//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"testing"
	"time"
)

// TestClientSettingsTimeoutSetting checks what ClientConfig.SettingsTimeout's
// zero, a negative value and a positive one mean.
func TestClientSettingsTimeoutSetting(t *testing.T) {
	var config ClientConfig
	if got := config.settingsTimeout(); got != DefaultSettingsTimeout {
		t.Fatalf("zero gives %v, want DefaultSettingsTimeout", got)
	}
	config.SettingsTimeout = -1
	if got := config.settingsTimeout(); got != 0 {
		t.Fatalf("below zero gives %v, want no limit", got)
	}
	config.SettingsTimeout = time.Second
	if got := config.settingsTimeout(); got != time.Second {
		t.Fatalf("a set timeout gives %v", got)
	}
}

// TestClientHTTP2SettingsTimeout checks that a server that does not
// acknowledge the client's SETTINGS within SettingsTimeout is taken for
// wedged, its connection closed and its requests failed with
// ErrSettingsTimeout, and that a server that does acknowledge them, or a
// client without the limit, keeps its connection.
func TestClientHTTP2SettingsTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	t.Run("unacknowledged", func(t *testing.T) {
		closed := make(chan struct{})
		url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
			p.settings()
			p.drain()
			close(closed)
		})
		client := cvH2cClient(t, func(c *ClientConfig) { c.SettingsTimeout = timeout })
		start := time.Now()
		_, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
		if !errors.Is(err, ErrSettingsTimeout) {
			t.Fatalf("err = %v, want ErrSettingsTimeout", err)
		}
		if waited := time.Since(start); waited > 5*time.Second {
			t.Fatalf("took %v", waited)
		}
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("the connection was not closed")
		}
	})
	t.Run("acknowledged", func(t *testing.T) {
		url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
			p.settings()
			for {
				f, ok := p.next()
				if !ok {
					return
				}
				switch {
				case f.typ == h2FrameSettings && !f.has(h2FlagAck):
					p.frame(h2FrameSettings, h2FlagAck, 0)
				case f.typ == h2FrameHeaders:
					p.headers(f.streamID, false, ":status", "200")
					p.frame(h2FrameData, h2FlagEndStream, f.streamID, 'o', 'k')
				}
			}
		})
		client := cvH2cClient(t, func(c *ClientConfig) { c.SettingsTimeout = timeout })
		resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("body %q", got)
		}
		// Well past the timeout the connection is still there, since the
		// server acknowledged the SETTINGS.
		time.Sleep(2 * timeout)
		resp, err = client.Go(mustRequest(t, "GET", url, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("second body %q", got)
		}
	})
	t.Run("no limit", func(t *testing.T) {
		url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
			p.settings()
			p.drain()
		})
		client := cvH2cClient(t, func(c *ClientConfig) {
			c.SettingsTimeout = -1
			c.Timeout = 600 * time.Millisecond
		})
		_, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
		if errors.Is(err, ErrSettingsTimeout) || err == nil {
			t.Fatalf("err = %v, want the request's timeout", err)
		}
		client.Close()
	})
}
