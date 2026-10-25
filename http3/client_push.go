//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/internal/reqbody"
)

// pushSink sends a pushed request body (fibhttp.BodyWriter) on its stream, a
// DATA frame a piece, as the stream's flow control lets what is written
// before it go.
type pushSink struct {
	st   *clientStream
	push *reqbody.Pusher
}

func (s *pushSink) Room() bool {
	return !s.st.s.NotifySendable(reqbody.MaxQueued, func(err error) {
		// A stream that ended instead settles the request where it ended.
		if err == nil {
			s.push.Kick()
		}
	})
}

func (s *pushSink) Send(p []byte) error {
	out := bufferpool.Get(len(p) + 16)[:0]
	out = appendFrameHeader(out, frameData, len(p))
	out = append(out, p...)
	return s.st.s.WriteOwned(out, false)
}

func (s *pushSink) End() error {
	st := s.st
	var err error
	// The trailer's values are those of when the body has ended.
	_, trailer, terr := trailerFields(s.push.Trailer(st.r.req.Trailer))
	if max, limited := st.cc.peer.fieldLimit(); terr == nil && limited && fieldsSize(trailer) > max {
		// The server would refuse the trailer, and a stream that ends
		// without it would pass for whole: the request fails instead.
		s.Fail(errFieldSectionTooLarge)
		return errFieldSectionTooLarge
	}
	if terr == nil && len(trailer) > 0 {
		block := bufferpool.Append(nil, qpack.Prefix)
		for _, f := range trailer {
			block = qpack.AppendField(block, f.Name, f.Value, sensitive(f.Name))
		}
		out := appendHeadersFrame(bufferpool.Get(len(block) + 16)[:0], block)
		bufferpool.Put(block)
		err = st.s.WriteOwned(out, true)
	} else {
		err = st.s.Write(nil, true)
	}
	st.uploading.Store(false)
	return err
}

func (s *pushSink) Fail(err error) {
	s.st.uploading.Store(false)
	s.st.r.abort(err)
}
