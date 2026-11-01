package rtsp

import (
	"slices"
	"sync"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// PlayToHub plays the stream at rawURL and publishes it into h at path, as
// a relay or a pull proxy does: the first tracks publish the stream, a
// change of them goes to the publisher's SetTracks, and the end of the
// client unpublishes it. config's own callbacks are called too, after the
// hub has the tracks or packet. A refused publish closes the client.
func PlayToHub(config ClientConfig, rawURL string, h *hub.Hub, path string) (*Client, error) {
	r := &relay{h: h, path: path, user: config}
	config.OnTracks, config.OnPacket, config.OnClose = r.onTracks, r.onPacket, r.onClose
	c, err := Play(config, rawURL)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.c = c
	r.mu.Unlock()
	return c, nil
}

// relay publishes what a playing client gives. The client serializes its
// callbacks, so the publisher's calls are serialized too.
type relay struct {
	h    *hub.Hub
	path string
	user ClientConfig
	pub  *hub.Publisher

	mu sync.Mutex
	c  *Client
}

func (r *relay) onTracks(tracks []*av.Track) {
	if r.pub == nil {
		r.mu.Lock()
		c := r.c
		r.mu.Unlock()
		info := hub.PublishInfo{Protocol: "rtsp"}
		if c != nil {
			c.mu.Lock()
			if c.conn != nil {
				info.Remote = c.conn.RemoteAddrPort()
			}
			c.mu.Unlock()
		}
		pub, err := r.h.Publish(r.path, slices.Clone(tracks), info)
		if err != nil {
			if c != nil {
				c.Close()
			}
			return
		}
		r.pub = pub
	} else {
		r.pub.SetTracks(slices.Clone(tracks))
	}
	if f := r.user.OnTracks; f != nil {
		f(tracks)
	}
}

func (r *relay) onPacket(pkt *av.Packet) {
	if r.pub != nil {
		r.pub.WritePacket(pkt)
	}
	if f := r.user.OnPacket; f != nil {
		f(pkt)
	}
}

func (r *relay) onClose(err error) {
	if r.pub != nil {
		r.pub.Close()
		r.pub = nil
	}
	if f := r.user.OnClose; f != nil {
		f(err)
	}
}

// PublishFromHub subscribes to path in h and publishes the stream to
// rawURL, as a push relay does: the client starts with the stream's first
// tracks, and packets go to it from then on, those before RECORD is
// accepted held, up to MaxQueuedBytes, and those over its backlog dropped
// as hub.NewDropper drops them. The subscription waits up to wait for a
// publisher (see hub.SubscribeOptions); the end of the stream closes the
// client, and the end of the client the subscription. A change of the
// stream's tracks closes the client, since RTSP cannot announce again in a
// session.
func PublishFromHub(config ClientConfig, rawURL string, h *hub.Hub, path string, opts hub.SubscribeOptions) (*Pusher, error) {
	if config.MaxQueuedBytes <= 0 {
		config.MaxQueuedBytes = DefaultMaxQueuedBytes
	}
	p := &Pusher{config: config, url: rawURL}
	userReady, userClose := config.OnReady, config.OnClose
	p.config.OnReady = func() {
		p.ready()
		if userReady != nil {
			userReady()
		}
	}
	p.config.OnClose = func(err error) {
		p.mu.Lock()
		sub := p.sub
		p.closed = true
		p.mu.Unlock()
		if sub != nil {
			sub.Close()
		}
		if userClose != nil {
			userClose(err)
		}
	}
	sink := hub.NewDropper(p, p.backlog, config.MaxQueuedBytes)
	if opts.Info.Protocol == "" {
		opts.Info.Protocol = "rtsp"
	}
	sub, err := h.Subscribe(path, sink, opts)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	p.sub = sub
	if p.closed {
		sub.Close()
	}
	return p, p.err
}

// Pusher is a hub stream being published to an RTSP server; see
// PublishFromHub.
type Pusher struct {
	config ClientConfig
	url    string

	mu      sync.Mutex
	c       *Client
	sub     *hub.Subscription
	tracks  []*av.Track
	held    []*av.Packet
	heldN   int64
	isReady bool
	closed  bool
	err     error
}

// Client returns the client publishing, nil before the stream's tracks
// came.
func (p *Pusher) Client() *Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.c
}

// Close ends the subscription and the client.
func (p *Pusher) Close() {
	p.mu.Lock()
	c, sub := p.c, p.sub
	p.closed = true
	p.mu.Unlock()
	if sub != nil {
		sub.Close()
	}
	if c != nil {
		c.Close()
	}
}

func (p *Pusher) backlog() int64 {
	p.mu.Lock()
	c := p.c
	held := p.heldN
	p.mu.Unlock()
	if c == nil {
		return held
	}
	return held + c.Backlog()
}

func (p *Pusher) OnTracks(tracks []*av.Track) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if p.c != nil {
		if !slices.Equal(tracks, p.tracks) {
			go p.c.Close()
		}
		return
	}
	p.tracks = tracks
	c, err := Publish(p.config, p.url, tracks)
	if err != nil {
		p.err = err
		p.closed = true
		if p.sub != nil {
			p.sub.Close()
		}
		return
	}
	p.c = c
}

func (p *Pusher) OnPacket(pkt *av.Packet) {
	p.mu.Lock()
	if p.closed || p.c == nil {
		p.mu.Unlock()
		return
	}
	if !p.isReady {
		if p.heldN+int64(len(pkt.Data)) <= p.config.MaxQueuedBytes {
			p.held = append(p.held, pkt)
			p.heldN += int64(len(pkt.Data))
		}
		p.mu.Unlock()
		return
	}
	c := p.c
	p.mu.Unlock()
	_ = c.WritePacket(pkt)
}

// OnClose closes the client: the stream is gone.
func (p *Pusher) OnClose(error) {
	p.mu.Lock()
	c := p.c
	p.closed = true
	p.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

// ready sends what was held and lets packets through.
func (p *Pusher) ready() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pkt := range p.held {
		_ = p.c.WritePacket(pkt)
	}
	p.held, p.heldN, p.isReady = nil, 0, true
}
