package rtc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc"
	"github.com/lesismal/fib/webrtc/sdp"
)

// DefaultClientTimeout bounds a client's signaling and connecting when
// ClientConfig.Timeout is zero.
const DefaultClientTimeout = 15 * time.Second

var (
	// ErrClientClosed is what OnClose hears after Close.
	ErrClientClosed = errors.New("rtc: client closed")
	// ErrTimeout is what a client that did not connect in time ends with.
	ErrTimeout = errors.New("rtc: timed out connecting")
	// ErrNotPushing is what WritePacket returns on a client that plays.
	ErrNotPushing = errors.New("rtc: client does not push")
)

// StatusError is the response of a server that did not answer an offer
// with 201 Created.
type StatusError struct {
	StatusCode int
	Body       string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("rtc: server answered %d: %s", e.StatusCode, e.Body)
}

// ClientConfig configures a WHIP or WHEP client.
type ClientConfig struct {
	// Engine sends the HTTP requests, unless HTTPClient is set, and
	// opens the PeerConnection's own UDP port, unless WebRTC has a Mux or
	// an Engine. It must be running.
	Engine *fib.Engine
	// HTTPClient sends the requests; nil makes one on Engine, closed with
	// the client.
	HTTPClient *fibhttp.Client
	// WebRTC configures the PeerConnection. Codecs is what a playing
	// client offers to receive, DefaultReceiveCodecs() when nil; a pushing
	// client offers the codecs of its tracks whatever it says.
	WebRTC webrtc.Configuration
	// Token is sent as a bearer token, and Header with every request.
	Token  string
	Header http.Header
	// Timeout bounds the offer's exchange and the connecting after it;
	// zero means DefaultClientTimeout.
	Timeout time.Duration

	// MTU bounds the RTP packets a pushing client sends, and MaxHeldBytes
	// what it holds until it is connected (the latest GOP); zero means
	// DefaultMTU and DefaultMaxHeldBytes.
	MTU          int
	MaxHeldBytes int
	// ReorderSize and ReorderDelay size the reorder buffers of a playing
	// client, PLIInterval is the least time between its keyframe requests,
	// and KeyFrameInterval, when positive, how often it asks for one
	// anyway.
	ReorderSize                   int
	ReorderDelay                  time.Duration
	PLIInterval, KeyFrameInterval time.Duration

	// OnTracks gives the tracks a playing client receives, once the
	// answer has come and again when the stream's configuration changes;
	// OnPacket each packet, its Track indexing them. They are called one
	// at a time. OnReady says the PeerConnection is connected, and
	// OnClose that the client ended, with ErrClientClosed after Close.
	// OnKeyFrameRequest hears the server of a pushing client ask for a
	// keyframe. None of them may block.
	OnTracks          func(tracks []*av.Track)
	OnPacket          func(pkt *av.Packet)
	OnReady           func()
	OnClose           func(err error)
	OnKeyFrameRequest func()
}

// Client is a WHIP client, which pushes a stream to a WHIP endpoint, or a
// WHEP client, which plays one from a WHEP endpoint, over a PeerConnection
// of its own.
type Client struct {
	config ClientConfig
	url    *url.URL
	push   bool
	http   *fibhttp.Client
	own    bool

	snd *sender
	rcv atomic.Pointer[receiver]
	sub atomic.Pointer[hub.Subscription]

	mu       sync.Mutex
	pc       *webrtc.PeerConnection
	location string
	timer    *time.Timer
	closed   bool
	ready    bool
	done     chan struct{}
}

// Pull plays the stream of the WHEP endpoint at rawURL: it offers to
// receive video and audio in the codecs of config.WebRTC, then gives the
// tracks and packets it receives to OnTracks and OnPacket until the
// session or the client ends, which OnClose hears. It returns at once;
// the offer is made and sent on a goroutine of its own.
func Pull(config ClientConfig, rawURL string) (*Client, error) {
	c, err := newClient(config, rawURL, false)
	if err != nil {
		return nil, err
	}
	go c.connect(nil)
	return c, nil
}

// Push publishes a stream of tracks to the WHIP endpoint at rawURL: it
// offers to send the tracks WebRTC can carry (H.264, H.265, Opus and
// G.711; the others are left out), and sends what WritePacket is given
// once it is connected, holding the latest GOP until then. It returns at
// once; the offer is made and sent on a goroutine of its own.
func Push(config ClientConfig, rawURL string, tracks []*av.Track) (*Client, error) {
	c, err := newClient(config, rawURL, true)
	if err != nil {
		return nil, err
	}
	c.snd.OnTracks(tracks)
	go c.connect(tracks)
	return c, nil
}

// PullToHub plays the stream at rawURL and publishes it into h at path,
// as a relay does: the first tracks publish the stream, a change of them
// goes to the publisher's SetTracks, and the end of the client unpublishes
// it. config's own callbacks are called too, after the hub's. A refused
// publish closes the client.
func PullToHub(config ClientConfig, rawURL string, h *hub.Hub, path string) (*Client, error) {
	var pub *hub.Publisher
	var c *Client
	ready := make(chan struct{})
	userTracks, userPacket, userClose := config.OnTracks, config.OnPacket, config.OnClose
	config.OnTracks = func(tracks []*av.Track) {
		if pub == nil {
			<-ready
			p, err := h.Publish(path, slices.Clone(tracks), hub.PublishInfo{Protocol: "whep", Query: c.url.RawQuery})
			if err != nil {
				go c.finish(err)
				return
			}
			pub = p
		} else {
			pub.SetTracks(slices.Clone(tracks))
		}
		if userTracks != nil {
			userTracks(tracks)
		}
	}
	config.OnPacket = func(pkt *av.Packet) {
		if pub != nil {
			pub.WritePacket(pkt)
		}
		if userPacket != nil {
			userPacket(pkt)
		}
	}
	config.OnClose = func(err error) {
		// The receiver is closed: no OnTracks or OnPacket is running.
		if pub != nil {
			pub.Close()
		}
		if userClose != nil {
			userClose(err)
		}
	}
	c, err := Pull(config, rawURL)
	close(ready)
	return c, err
}

// PushFromHub subscribes to path in h and pushes the stream to the WHIP
// endpoint at rawURL, as a push relay does: the client offers the
// stream's first tracks, holds the latest GOP until it is connected, and
// drops packets, video until the next keyframe, while the server reports
// more than DefaultMaxLoss of them lost (hub.NewDropper). The end of the
// stream closes the client, and the end of the client the subscription.
func PushFromHub(config ClientConfig, rawURL string, h *hub.Hub, path string, opts hub.SubscribeOptions) (*Client, error) {
	c, err := newClient(config, rawURL, true)
	if err != nil {
		return nil, err
	}
	c.snd.onClose = func(err error) { go c.finish(err) }
	if opts.Info.Protocol == "" {
		opts.Info.Protocol = "whip"
	}
	first := make(chan []*av.Track, 1)
	sink := &firstTracks{sender: c.snd, first: first}
	sub, err := h.Subscribe(path, hub.NewDropper(sink, c.snd.backlog, int64(DefaultMaxLoss*256)), opts)
	if err != nil {
		c.finish(err)
		return nil, err
	}
	c.sub.Store(sub)
	go func() {
		select {
		case tracks := <-first:
			c.connect(tracks)
		case <-c.done:
		}
	}()
	return c, nil
}

// firstTracks is the sink of PushFromHub: the client's sender, which
// passes on the stream's first tracks for the offer.
type firstTracks struct {
	*sender
	first chan []*av.Track
	once  sync.Once
}

func (f *firstTracks) OnTracks(tracks []*av.Track) {
	f.sender.OnTracks(tracks)
	f.once.Do(func() { f.first <- tracks })
}

func newClient(config ClientConfig, rawURL string, push bool) (*Client, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("rtc: unsupported URL scheme %q", u.Scheme)
	}
	if config.HTTPClient == nil && config.Engine == nil {
		return nil, errors.New("rtc: ClientConfig needs an Engine or an HTTPClient")
	}
	if config.WebRTC.Mux == nil && config.WebRTC.Engine == nil {
		if config.Engine == nil {
			return nil, errors.New("rtc: ClientConfig needs an Engine or a WebRTC Mux or Engine")
		}
		config.WebRTC.Engine = config.Engine
	}
	if config.Timeout <= 0 {
		config.Timeout = DefaultClientTimeout
	}
	if config.MTU <= 0 {
		config.MTU = DefaultMTU
	}
	if config.MaxHeldBytes <= 0 {
		config.MaxHeldBytes = DefaultMaxHeldBytes
	}
	if config.PLIInterval <= 0 {
		config.PLIInterval = DefaultPLIInterval
	}
	c := &Client{config: config, url: u, push: push, http: config.HTTPClient, done: make(chan struct{})}
	if c.http == nil {
		hc := fibhttp.DefaultClientConfig()
		hc.Timeout = config.Timeout
		c.http, c.own = fibhttp.NewClient(config.Engine, hc), true
	}
	if push {
		c.snd = newSender(config.MaxHeldBytes)
	}
	c.timer = time.AfterFunc(config.Timeout, func() {
		c.mu.Lock()
		ready := c.ready
		c.mu.Unlock()
		if !ready {
			c.finish(ErrTimeout)
		}
	})
	return c, nil
}

// Resource returns the URL of the session the server made, once it has
// answered.
func (c *Client) Resource() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.location
}

// PeerConnection returns the client's PeerConnection, once it is made.
func (c *Client) PeerConnection() *webrtc.PeerConnection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pc
}

// Tracks returns the tracks a playing client receives, or those a pushing
// one was last given.
func (c *Client) Tracks() []*av.Track {
	if c.push {
		return slices.Clone(c.snd.Streams())
	}
	if r := c.rcv.Load(); r != nil {
		return r.Tracks()
	}
	return nil
}

// WritePacket sends pkt, of the tracks Push was given (or SetTracks
// last), on a pushing client; it holds it until the client is connected.
// The client keeps pkt, which must not be changed afterwards.
func (c *Client) WritePacket(pkt *av.Packet) error {
	if !c.push {
		return ErrNotPushing
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClientClosed
	}
	c.snd.OnPacket(pkt)
	return nil
}

// SetTracks gives a pushing client new tracks, as a change of codec
// configuration makes: packets of them go on the tracks of the same codec
// it was connected with, and a track of a codec it was not connected with
// is not sent, since it would need a new offer.
func (c *Client) SetTracks(tracks []*av.Track) {
	if c.push {
		c.snd.OnTracks(tracks)
	}
}

// RequestKeyFrame asks the server of a playing client for a keyframe of
// every video track, within the PLI interval.
func (c *Client) RequestKeyFrame() {
	if r := c.rcv.Load(); r != nil {
		r.requestKeyFrame()
	}
}

// Close ends the client: the session is deleted on the server, and
// OnClose hears ErrClientClosed.
func (c *Client) Close() { c.finish(ErrClientClosed) }

// connect makes the PeerConnection, the offer, and the request that sends
// it; on a goroutine of its own, since making a PeerConnection may open a
// port and the offer waits for its candidates.
func (c *Client) connect(tracks []*av.Track) {
	cfg := c.config.WebRTC
	var outs []*outTrack
	if c.push {
		var codecs []webrtc.Codec
		outs, codecs = planEgress(tracks, nil, c.config.MTU, nil)
		if len(outs) == 0 {
			c.finish(ErrNoMedia)
			return
		}
		cfg.Codecs = codecs
	} else if cfg.Codecs == nil {
		cfg.Codecs = DefaultReceiveCodecs()
	}
	pc, err := webrtc.NewPeerConnection(cfg)
	if err != nil {
		c.finish(err)
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = pc.Close()
		return
	}
	c.pc = pc
	c.mu.Unlock()
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		switch st {
		case webrtc.PeerConnectionStateConnected:
			go c.connected()
		case webrtc.PeerConnectionStateFailed:
			go c.finish(errors.New("rtc: connection failed"))
		case webrtc.PeerConnectionStateClosed:
			go c.finish(errors.New("rtc: connection closed"))
		}
	})
	if c.push {
		if err := c.addTracks(pc, outs); err != nil {
			c.finish(err)
			return
		}
	} else {
		pc.OnTrack(func(tr *webrtc.TrackRemote) {
			if r := c.rcv.Load(); r != nil {
				r.onTrack(tr)
			}
		})
		for _, kind := range []webrtc.MediaKind{webrtc.MediaKindVideo, webrtc.MediaKindAudio} {
			if _, err := pc.AddTransceiver(kind, webrtc.DirectionRecvOnly); err != nil {
				c.finish(err)
				return
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer cancel()
	offer, err := webrtc.Offer(ctx, pc, nil)
	if err != nil {
		c.finish(err)
		return
	}
	req, err := c.request(http.MethodPost, c.url.String(), []byte(offer.SDP))
	if err != nil {
		c.finish(err)
		return
	}
	req.Header.Set("Content-Type", contentTypeSDP)
	c.http.Do(req, func(resp *fibhttp.ClientResponse, err error) { c.answered(outs, resp, err) })
}

// addTracks adds the tracks of a pushing client, to send only.
func (c *Client) addTracks(pc *webrtc.PeerConnection, outs []*outTrack) error {
	for _, o := range outs {
		rs, err := pc.AddTrack(o.local)
		if err != nil {
			return err
		}
		rs.Transceiver().SetDirection(webrtc.DirectionSendOnly)
		o.rtp = rs
		rs.OnKeyFrameRequest(func() {
			if f := c.config.OnKeyFrameRequest; f != nil {
				f()
			}
		})
	}
	pc.OnRTCP(func(pkt []byte) {
		c.snd.onRTCP(func(ssrc uint32) bool {
			for _, o := range outs {
				if o.rtp.SSRC() == ssrc {
					return true
				}
			}
			return false
		}, pkt)
	})
	return nil
}

func (c *Client) request(method, target string, body []byte) (*http.Request, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, target, r)
	if err != nil {
		return nil, err
	}
	for k, v := range c.config.Header {
		req.Header[k] = slices.Clone(v)
	}
	if c.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.Token)
	}
	return req, nil
}

// answered takes the server's response to the offer.
func (c *Client) answered(outs []*outTrack, resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		c.finish(err)
		return
	}
	body, _ := resp.BodyBytes()
	if resp.StatusCode != http.StatusCreated {
		c.finish(&StatusError{StatusCode: resp.StatusCode, Body: string(bytes.TrimSpace(body))})
		return
	}
	loc, err := c.url.Parse(resp.Header.Get("Location"))
	if err != nil || resp.Header.Get("Location") == "" {
		c.finish(errors.New("rtc: answer without a Location"))
		return
	}
	c.mu.Lock()
	c.location = loc.String()
	pc := c.pc
	closed := c.closed
	c.mu.Unlock()
	if closed {
		c.deleteResource()
		return
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(body)}); err != nil {
		c.finish(err)
		return
	}
	if c.push {
		var sending []*outTrack
		for _, o := range outs {
			if d := o.rtp.Transceiver().CurrentDirection(); d == webrtc.DirectionSendOnly || d == webrtc.DirectionSendRecv {
				sending = append(sending, o)
			}
		}
		if len(sending) == 0 {
			c.finish(ErrNoMedia)
			return
		}
		c.snd.setTracks(sending)
		return
	}
	answer, _ := sdp.ParseString(string(body)) // the PeerConnection has parsed it
	r := newReceiver(recvConfig{
		reorderSize: c.config.ReorderSize, reorderDelay: c.config.ReorderDelay,
		keyFrameInterval: c.config.KeyFrameInterval, pliInterval: c.config.PLIInterval,
	}, answer, true)
	r.onTracks, r.onPacket = c.config.OnTracks, c.config.OnPacket
	r.mu.Lock()
	r.emitTracks()
	r.mu.Unlock()
	c.rcv.Store(r)
	if len(r.Tracks()) == 0 {
		c.finish(ErrNoMedia)
	}
}

// connected starts the media once the PeerConnection is.
func (c *Client) connected() {
	c.mu.Lock()
	if c.closed || c.ready {
		c.mu.Unlock()
		return
	}
	c.ready = true
	c.mu.Unlock()
	c.timer.Stop()
	if c.snd != nil {
		c.snd.start()
	}
	if r := c.rcv.Load(); r != nil {
		r.startPeriodic()
	}
	if f := c.config.OnReady; f != nil {
		f()
	}
}

// finish ends the client, once, with err.
func (c *Client) finish(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pc := c.pc
	c.mu.Unlock()
	close(c.done)
	c.timer.Stop()
	if sub := c.sub.Load(); sub != nil {
		sub.Close()
	}
	if c.snd != nil {
		c.snd.close()
	}
	if r := c.rcv.Load(); r != nil {
		r.close()
	}
	if pc != nil {
		_ = pc.Close()
	}
	c.deleteResource()
	if f := c.config.OnClose; f != nil {
		f(err)
	}
}

// deleteResource ends the session on the server, and closes the HTTP
// client of the client's own once that is done.
func (c *Client) deleteResource() {
	c.mu.Lock()
	loc := c.location
	c.location = ""
	c.mu.Unlock()
	done := func() {
		if c.own {
			c.http.Close()
		}
	}
	if loc == "" {
		done()
		return
	}
	req, err := c.request(http.MethodDelete, loc, nil)
	if err != nil {
		done()
		return
	}
	c.http.Do(req, func(*fibhttp.ClientResponse, error) { done() })
}
