//go:build linux || darwin || windows

// Package csrf refuses cross-site request forgery: a request with an unsafe
// method — anything but GET, HEAD, OPTIONS and TRACE — is served only when it
// carries the token the server gave the client in a cookie, and when the
// browser does not report it as coming from another origin.
//
// The token is a double-submit one. A request with a safe method that has no
// token cookie gets one, and the handler finds the token with Token to put
// in its forms, or leaves it to a script to read from the cookie; an unsafe
// request sends it back in a header, a form field or a query parameter, as
// Config.Lookup says, and it has to match the cookie. The origin check is
// net/http's CrossOriginProtection, which goes by Sec-Fetch-Site, or by the
// Origin header's host against the request's.
package csrf

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strings"
	"time"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
)

var (
	// ErrTokenMissing is a request with no token, or no cookie to match it.
	ErrTokenMissing = errors.New("csrf: token missing")
	// ErrTokenInvalid is a request whose token does not match its cookie.
	ErrTokenInvalid = errors.New("csrf: token invalid")
)

const (
	DefaultCookieName = "csrf_"
	DefaultLookup     = "header:X-CSRF-Token"
	// DefaultExpiration is how long the token cookie lasts.
	DefaultExpiration = time.Hour
)

type Config struct {
	// Next passes the requests it reports true for straight on.
	Next middleware.Skipper
	// Lookup is where an unsafe request carries its token: "header:<name>",
	// "form:<field>" or "query:<parameter>"; DefaultLookup by default.
	Lookup string
	// The token cookie's name, DefaultCookieName by default, and its
	// attributes. SameSite is Lax by default. The cookie is readable by
	// scripts unless CookieHTTPOnly is set, since a script that sends the
	// token in a header reads it from there.
	CookieName     string
	CookieDomain   string
	CookiePath     string
	CookieSecure   bool
	CookieHTTPOnly bool
	CookieSameSite stdhttp.SameSite
	// CookieSessionOnly makes the cookie last as long as the browser's
	// session, rather than Expiration.
	CookieSessionOnly bool
	// Expiration is how long the cookie lasts; DefaultExpiration by
	// default.
	Expiration time.Duration
	// TrustedOrigins may send unsafe requests from another origin, such as
	// "https://app.example.com".
	TrustedOrigins []string
	// ErrorHandler answers a request refused with err; 403 Forbidden by
	// default.
	ErrorHandler func(c *fibhttp.Context, r *stdhttp.Request, err error)
}

// tokenKey is where Token finds the token in a request's context.
type tokenKey struct{}

// Token is the token of a request the middleware let through, to be sent
// with the next unsafe request, or "" for one it did not see.
func Token(r *stdhttp.Request) string {
	token, _ := r.Context().Value(tokenKey{}).(string)
	return token
}

// New returns the middleware. It panics on a Lookup or TrustedOrigins it
// cannot use.
func New(config ...Config) middleware.Middleware {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.Lookup == "" {
		cfg.Lookup = DefaultLookup
	}
	if cfg.CookieName == "" {
		cfg.CookieName = DefaultCookieName
	}
	if cfg.CookiePath == "" {
		cfg.CookiePath = "/"
	}
	if cfg.CookieSameSite == 0 {
		cfg.CookieSameSite = stdhttp.SameSiteLaxMode
	}
	if cfg.Expiration <= 0 {
		cfg.Expiration = DefaultExpiration
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = forbidden
	}
	extract, err := lookup(cfg.Lookup)
	if err != nil {
		panic(err)
	}
	origins := stdhttp.NewCrossOriginProtection()
	for _, origin := range cfg.TrustedOrigins {
		if err := origins.AddTrustedOrigin(origin); err != nil {
			panic("csrf: " + err.Error())
		}
	}
	return func(next fibhttp.Handler) fibhttp.Handler {
		return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
			if cfg.Next != nil && cfg.Next(c, r) {
				next.ServeHTTP(c, r)
				return
			}
			cookie := ""
			if got, err := r.Cookie(cfg.CookieName); err == nil && validToken(got.Value) {
				cookie = got.Value
			}
			switch r.Method {
			case stdhttp.MethodGet, stdhttp.MethodHead, stdhttp.MethodOptions, stdhttp.MethodTrace:
				if cookie == "" {
					cookie = newToken()
					setCookie := cfg.cookie(cookie).String()
					c.OnHeader(func(_ int, h stdhttp.Header) {
						h["Set-Cookie"] = append(h["Set-Cookie"], setCookie)
					})
				}
			default:
				if err := origins.Check(r); err != nil {
					cfg.ErrorHandler(c, r, err)
					return
				}
				sent := extract(r)
				switch {
				case cookie == "" || sent == "":
					cfg.ErrorHandler(c, r, ErrTokenMissing)
					return
				case subtle.ConstantTimeCompare([]byte(sent), []byte(cookie)) != 1:
					cfg.ErrorHandler(c, r, ErrTokenInvalid)
					return
				}
			}
			next.ServeHTTP(c, r.WithContext(context.WithValue(r.Context(), tokenKey{}, cookie)))
		})
	}
}

func (cfg *Config) cookie(token string) *stdhttp.Cookie {
	cookie := &stdhttp.Cookie{
		Name:     cfg.CookieName,
		Value:    token,
		Path:     cfg.CookiePath,
		Domain:   cfg.CookieDomain,
		Secure:   cfg.CookieSecure,
		HttpOnly: cfg.CookieHTTPOnly,
		SameSite: cfg.CookieSameSite,
	}
	if !cfg.CookieSessionOnly {
		cookie.MaxAge = int(cfg.Expiration / time.Second)
	}
	return cookie
}

// lookup returns what reads a request's token from where spec says.
func lookup(spec string) (func(*stdhttp.Request) string, error) {
	source, name, ok := strings.Cut(spec, ":")
	name = strings.TrimSpace(name)
	if !ok || name == "" {
		return nil, fmt.Errorf("csrf: bad Lookup %q", spec)
	}
	switch strings.TrimSpace(source) {
	case "header":
		name = stdhttp.CanonicalHeaderKey(name)
		return func(r *stdhttp.Request) string { return r.Header.Get(name) }, nil
	case "form":
		return func(r *stdhttp.Request) string { return r.PostFormValue(name) }, nil
	case "query":
		return func(r *stdhttp.Request) string { return r.URL.Query().Get(name) }, nil
	}
	return nil, fmt.Errorf("csrf: bad Lookup %q", spec)
}

// tokenLength is the length of a token: 32 random bytes, in unpadded
// URL-safe base64.
var tokenLength = base64.RawURLEncoding.EncodedLen(32)

func newToken() string {
	var b [32]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

// validToken reports whether token has the form newToken gives; a cookie
// that does not is replaced.
func validToken(token string) bool {
	if len(token) != tokenLength {
		return false
	}
	for i := 0; i < len(token); i++ {
		switch b := token[i]; {
		case 'a' <= b && b <= 'z', 'A' <= b && b <= 'Z', '0' <= b && b <= '9', b == '-', b == '_':
		default:
			return false
		}
	}
	return true
}

func forbidden(c *fibhttp.Context, _ *stdhttp.Request, _ error) {
	status := stdhttp.StatusForbidden
	_ = c.Respond(status, "text/plain; charset=utf-8", []byte(stdhttp.StatusText(status)+"\n"))
}
