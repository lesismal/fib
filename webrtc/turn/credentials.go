package turn

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// AuthFunc returns the long-term key of username in realm, the key MESSAGE-
// INTEGRITY is computed with, MD5(username ":" realm ":" password) as
// stun.LongTermKey derives it, and whether the user may use the server at
// all. Keeping keys rather than passwords lets a server store them without
// the passwords themselves.
type AuthFunc func(username, realm string) (key []byte, ok bool)

// StaticAuth is an AuthFunc for a fixed set of users and their passwords.
// It derives every key once, when called.
func StaticAuth(realm string, passwords map[string]string) AuthFunc {
	keys := make(map[string][]byte, len(passwords))
	for user, pass := range passwords {
		keys[user] = stun.LongTermKey(user, realm, pass)
	}
	return func(username, r string) ([]byte, bool) {
		if r != realm {
			return nil, false
		}
		k, ok := keys[username]
		return k, ok
	}
}

// EphemeralCredentials returns credentials for user valid until expiry, in
// the scheme of the TURN REST API draft (draft-uberti-behave-turn-rest) that
// coturn's use-auth-secret and most WebRTC services implement: the username
// is "<expiry as Unix seconds>:<user>" and the password is the base64 of
// HMAC-SHA1(secret, username). A web service that shares secret with the
// TURN server hands them to a browser for its RTCIceServer; the server needs
// no list of users, only the secret, through EphemeralAuth.
func EphemeralCredentials(secret []byte, user string, expiry time.Time) (username, password string) {
	username = strconv.FormatInt(expiry.Unix(), 10)
	if user != "" {
		username += ":" + user
	}
	return username, ephemeralPassword(secret, username)
}

func ephemeralPassword(secret []byte, username string) string {
	mac := hmac.New(sha1.New, secret)
	mac.Write([]byte(username))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// EphemeralAuth is the AuthFunc of a server that accepts the credentials
// EphemeralCredentials makes with secret: any username whose leading
// timestamp is still in the future by now, with the password the secret
// derives. now is the server's clock, time.Now unless a test fakes it.
func EphemeralAuth(secret []byte, now func() time.Time) AuthFunc {
	if now == nil {
		now = time.Now
	}
	return func(username, realm string) ([]byte, bool) {
		ts, _, _ := strings.Cut(username, ":")
		expiry, err := strconv.ParseInt(ts, 10, 64)
		if err != nil || now().Unix() >= expiry {
			return nil, false
		}
		return stun.LongTermKey(username, realm, ephemeralPassword(secret, username)), true
	}
}
