// Package interop checks the media servers against ffmpeg: one engine
// serves every protocol, as in docs/media.md's one-server example, and
// ffmpeg publishes a stream over each ingest and plays it back over each
// egress. The checks are the tests of this package; run.sh runs them in a
// Debian container with ffmpeg, as CI does.
package interop

import (
	"errors"
	"strings"
	"time"

	"github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/hls"
	"github.com/lesismal/fib/media/httpflv"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/rtc"
	"github.com/lesismal/fib/media/rtmp"
	"github.com/lesismal/fib/media/rtsp"
	"github.com/lesismal/fib/media/srt"
	"github.com/lesismal/fib/webrtc"
)

// The ports the server listens on, those of the example: RTMP, RTSP with
// RTP and RTCP over UDP, SRT, and HTTP with WHIP and WHEP on UDP.
const (
	RTMPAddr = "127.0.0.1:1935"
	RTSPAddr = "127.0.0.1:8554"
	SRTAddr  = "127.0.0.1:9000"
	HTTPAddr = "127.0.0.1:8080"
)

// router sends each HTTP request to the handler of its protocol.
type router struct {
	flv, hls, llhls fibhttp.Handler
	rtc             *rtc.Server
}

func (r *router) ServeHTTP(c *fibhttp.Context) {
	p := c.Request.URL.Path
	switch {
	case strings.HasPrefix(p, "/whip/"), strings.HasPrefix(p, "/whep/"), strings.HasPrefix(p, "/rtc/session/"):
		r.rtc.ServeHTTP(c)
	case strings.HasSuffix(p, ".flv"):
		r.flv.ServeHTTP(c)
	case strings.HasPrefix(p, "/ll/"):
		r.llhls.ServeHTTP(c)
	default:
		r.hls.ServeHTTP(c)
	}
}

// Server is the example's server: one engine and one hub behind every
// protocol, which accepts publishes under live/ only.
type Server struct {
	Engine *fib.Engine
	Hub    *hub.Hub
	done   chan error
}

// Start starts the server on the wildcard addresses of the example's
// ports and returns once it is listening.
func Start() (*Server, error) {
	h := hub.New(hub.Config{
		OnPublish: func(path string, _ hub.PublishInfo) error {
			if !strings.HasPrefix(path, "live/") {
				return errors.New("publish under live/ only")
			}
			return nil
		},
	})
	wait := 10 * time.Second
	r := &router{
		flv: httpflv.NewHandler(httpflv.Config{Hub: h, Wait: wait, CORS: "*"}),
		hls: hls.NewHandler(hls.HandlerConfig{
			Hub:         h,
			Formats:     []hls.Format{hls.MPEGTS},
			AllowOrigin: "*",
		}),
		llhls: hls.NewHandler(hls.HandlerConfig{
			Hub:         h,
			Prefix:      "/ll/",
			Muxer:       hls.Config{PartDuration: 500 * time.Millisecond},
			Formats:     []hls.Format{hls.MPEGTS},
			AllowOrigin: "*",
		}),
	}
	rtspServer := rtsp.NewServer(rtsp.ServerConfig{Hub: h})
	srtServer, err := srt.NewServer(srt.ServerConfig{Hub: h, PlayWait: wait})
	if err != nil {
		return nil, err
	}
	config := fib.DefaultConfig()
	config.LogSettings = false
	engine, err := fib.New(config,
		fib.Binding{Addr: ":1935", Handler: rtmp.NewServer(rtmp.ServerConfig{Hub: h, PlayWait: wait})},
		fib.Binding{Addr: ":8554", Handler: rtspServer},
		fib.Binding{Network: "udp", Addr: ":9000", Handler: srtServer},
		fib.Binding{Addr: ":8080", Handler: fibhttp.NewHandler(r)},
	)
	if err != nil {
		return nil, err
	}
	s := &Server{Engine: engine, Hub: h, done: make(chan error, 1)}
	fail := func(err error) (*Server, error) {
		_ = engine.Close()
		return nil, err
	}
	if err := rtspServer.ListenUDP(engine); err != nil {
		return fail(err)
	}
	mux, err := webrtc.NewMux(engine, webrtc.MuxConfig{Addr: ":8080"})
	if err != nil {
		return fail(err)
	}
	r.rtc, err = rtc.NewServer(rtc.Config{
		Hub:         h,
		WebRTC:      webrtc.Configuration{Mux: mux, ICELite: true},
		AllowOrigin: "*",
	})
	if err != nil {
		return fail(err)
	}
	go func() { s.done <- engine.Run() }()
	return s, nil
}

// Close stops the server and waits for it.
func (s *Server) Close() {
	s.Engine.Stop()
	<-s.done
	_ = s.Engine.Close()
}
