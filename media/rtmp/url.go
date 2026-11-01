package rtmp

import (
	"errors"
	"net"
	"net/url"
	"strings"
)

// ErrURL is a URL that does not name an RTMP stream.
var ErrURL = errors.New("rtmp: URL must be rtmp://host[:port]/app/stream, or rtmps")

// URL is an RTMP URL taken apart: rtmp[s]://host[:port]/app/stream[?query].
type URL struct {
	// Scheme is "rtmp" or "rtmps".
	Scheme string
	// Host is the host and port, the scheme's default port filled in.
	Host string
	// App is the first path segment, which the connect command carries,
	// and Stream the rest, which publish and play carry; a query goes with
	// the stream, where servers look for tokens.
	App    string
	Stream string
	Query  string
}

// ParseURL parses an RTMP URL.
func ParseURL(raw string) (*URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	r := &URL{Scheme: strings.ToLower(u.Scheme), Query: u.RawQuery}
	port := "1935"
	switch r.Scheme {
	case "rtmp":
	case "rtmps":
		port = "443"
	default:
		return nil, ErrURL
	}
	if u.Host == "" {
		return nil, ErrURL
	}
	r.Host = u.Host
	if u.Port() == "" {
		r.Host = net.JoinHostPort(u.Hostname(), port)
	}
	path := strings.TrimPrefix(u.Path, "/")
	app, stream, ok := strings.Cut(path, "/")
	if !ok || app == "" || stream == "" {
		return nil, ErrURL
	}
	r.App, r.Stream = app, stream
	return r, nil
}

// TcURL is the URL of the application, which the connect command carries
// as tcUrl.
func (u *URL) TcURL() string { return u.Scheme + "://" + u.Host + "/" + u.App }

// StreamName is the stream as publish and play name it, with the query.
func (u *URL) StreamName() string {
	if u.Query == "" {
		return u.Stream
	}
	return u.Stream + "?" + u.Query
}

// Path is the hub path of the stream, "app/stream".
func (u *URL) Path() string { return u.App + "/" + u.Stream }

// streamPath joins a connect's app and a publish's or play's stream name
// into a hub path and the query the two carried: tokens may ride on either,
// as "live?token=x" or "s?token=x".
func streamPath(app, name string) (path, query string) {
	app, appQuery, _ := strings.Cut(app, "?")
	name, nameQuery, _ := strings.Cut(name, "?")
	app = strings.Trim(app, "/")
	name = strings.Trim(name, "/")
	query = nameQuery
	if appQuery != "" {
		if query != "" {
			query += "&"
		}
		query += appQuery
	}
	if app == "" {
		return name, query
	}
	return app + "/" + name, query
}
