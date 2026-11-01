package srt

import (
	"errors"
	"strings"
)

// Mode is what a caller asks to do with a stream.
type Mode int

const (
	// ModeRequest plays the stream: the listener sends, the caller
	// receives. It is the default.
	ModeRequest Mode = iota
	// ModePublish publishes it: the caller sends.
	ModePublish
	// ModeBidirectional sends both ways, which the Server refuses.
	ModeBidirectional
)

func (m Mode) String() string {
	switch m {
	case ModePublish:
		return "publish"
	case ModeBidirectional:
		return "bidirectional"
	}
	return "request"
}

// StreamID is a parsed stream ID: the plain "app/stream" form, or the
// access control syntax of the SRT Access Control guidelines,
// "#!::r=app/stream,m=publish,u=user".
type StreamID struct {
	// Resource is the stream's path: r= of the access control syntax,
	// or the whole plain stream ID before any '?', without a leading '/'.
	Resource string
	Mode     Mode
	// User, Session, Host and Type are u=, s=, h= and t=.
	User    string
	Session string
	Host    string
	Type    string
	// Query is what follows '?' in a plain stream ID, where tokens travel.
	Query string
	// Params holds every key of the access control syntax, the standard
	// ones included, for keys of an application's own.
	Params map[string]string
}

var (
	errNoResource = errors.New("srt: stream ID names no resource")
	errBadMode    = errors.New("srt: stream ID has an unknown m= mode")
	errBadSyntax  = errors.New("srt: malformed stream ID access control syntax")
)

// ParseStreamID parses a stream ID in either form. An empty resource is an
// error: the Server has nothing to look up.
func ParseStreamID(s string) (StreamID, error) {
	var id StreamID
	body, ok := strings.CutPrefix(s, "#!::")
	if !ok {
		id.Resource, id.Query, _ = strings.Cut(s, "?")
		id.Resource = strings.TrimPrefix(id.Resource, "/")
		if id.Resource == "" {
			return id, errNoResource
		}
		return id, nil
	}
	id.Params = map[string]string{}
	for kv := range strings.SplitSeq(body, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return id, errBadSyntax
		}
		id.Params[k] = v
		switch k {
		case "r":
			id.Resource = strings.TrimPrefix(v, "/")
		case "m":
			switch v {
			case "request":
				id.Mode = ModeRequest
			case "publish":
				id.Mode = ModePublish
			case "bidirectional":
				id.Mode = ModeBidirectional
			default:
				return id, errBadMode
			}
		case "u":
			id.User = v
		case "s":
			id.Session = v
		case "h":
			id.Host = v
		case "t":
			id.Type = v
		}
	}
	if id.Resource == "" {
		return id, errNoResource
	}
	return id, nil
}
