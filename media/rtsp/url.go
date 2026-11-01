package rtsp

import (
	"net/url"
	"strconv"
	"strings"
)

// streamPath returns the hub path of an RTSP URL, its path without the
// slashes around it, and its query.
//
// A "trackID=N" segment after the query is taken as the path's: a player
// of "rtsp://h/live/s?token=x" resolves the controls of the server's
// description against that URL as its base, and so sets up
// "rtsp://h/live/s?token=x/trackID=0", which ffmpeg and fib's own client
// both do.
func streamPath(raw string) (path, query string, ok bool) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "" && !strings.EqualFold(u.Scheme, "rtsp") && !strings.EqualFold(u.Scheme, "rtsps")) {
		return "", "", false
	}
	path, query = strings.Trim(u.Path, "/"), u.RawQuery
	if i := strings.LastIndexByte(query, '/'); i >= 0 {
		if _, n := splitTrackID(query[i:]); n >= 0 {
			path, query = path+query[i:], query[:i]
		}
	}
	return path, query, true
}

// splitTrackID splits a path ending in the "trackID=N" segment the server
// gives its streams as controls into the stream's path and N, or reports
// -1 when it has none.
func splitTrackID(path string) (string, int) {
	i := strings.LastIndexByte(path, '/')
	last := path[i+1:]
	v, ok := strings.CutPrefix(last, "trackID=")
	if !ok {
		return path, -1
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return path, -1
	}
	if i < 0 {
		return "", n
	}
	return path[:i], n
}

// resolveControl returns the URL of a stream whose a=control is control,
// against the base URL of its description.
func resolveControl(base, control string) string {
	switch {
	case control == "" || control == "*":
		return base
	case strings.Contains(control, "://"):
		return control
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return base + strings.TrimPrefix(control, "/")
}

// matchControl reports whether a SETUP of url is of the stream with
// control, against the base URL of its description.
func matchControl(base, control, raw string) bool {
	if control == "" {
		return false
	}
	want := resolveControl(base, control)
	if want == raw {
		return true
	}
	// Compare paths, as clients and servers write the same URL with and
	// without a port, or another host name.
	a, _, ok1 := streamPath(want)
	b, _, ok2 := streamPath(raw)
	return ok1 && ok2 && a == b
}

// stripUserinfo returns the URL without its user and password, and them.
func stripUserinfo(raw string) (clean, user, password string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", "", err
	}
	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
		u.User = nil
	}
	return u.String(), user, password, nil
}
