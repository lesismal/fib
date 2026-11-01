package rtsp

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// AuthMethod is a set of HTTP authentication schemes RTSP borrows.
type AuthMethod uint8

// The schemes: Digest (RFC 2617, RFC 7616 with MD5, as RTSP 1.0 clients
// speak it) and Basic (RFC 7617), which sends the password in the clear
// and is offered only when asked for.
const (
	AuthDigest AuthMethod = 1 << iota
	AuthBasic
)

// DefaultNonceLifetime is how long a Digest nonce of the server is good
// for; a client using an older one is told it is stale and retries with a
// new one, without asking its user again.
const DefaultNonceLifetime = 5 * time.Minute

// authServer checks the Authorization of requests. Its nonces carry their
// time and a MAC of it, so any connection can check any nonce without the
// server keeping them.
type authServer struct {
	realm    string
	methods  AuthMethod
	lifetime time.Duration
	secret   [32]byte
}

func newAuthServer(realm string, methods AuthMethod, lifetime time.Duration) *authServer {
	a := &authServer{realm: realm, methods: methods, lifetime: lifetime}
	if a.methods == 0 {
		a.methods = AuthDigest
	}
	if a.lifetime <= 0 {
		a.lifetime = DefaultNonceLifetime
	}
	_, _ = rand.Read(a.secret[:])
	return a
}

func (a *authServer) nonce(now time.Time) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(now.Unix()))
	return hex.EncodeToString(b[:]) + a.mac(b[:])
}

func (a *authServer) mac(b []byte) string {
	m := hmac.New(sha256.New, a.secret[:])
	m.Write(b)
	return hex.EncodeToString(m.Sum(nil)[:12])
}

// nonceValid reports whether nonce is one of the server's, and whether it
// is still fresh.
func (a *authServer) nonceValid(nonce string, now time.Time) (ours, fresh bool) {
	if len(nonce) != 16+24 {
		return false, false
	}
	b, err := hex.DecodeString(nonce[:16])
	if err != nil || !hmac.Equal([]byte(a.mac(b)), []byte(nonce[16:])) {
		return false, false
	}
	at := time.Unix(int64(binary.BigEndian.Uint64(b)), 0)
	return true, now.Sub(at) <= a.lifetime && at.Sub(now) < time.Minute
}

// challenges returns the WWW-Authenticate values of a 401.
func (a *authServer) challenges(now time.Time, stale bool) []string {
	var vs []string
	if a.methods&AuthDigest != 0 {
		v := `Digest realm="` + a.realm + `", nonce="` + a.nonce(now) + `", algorithm=MD5`
		if stale {
			v += ", stale=TRUE"
		}
		vs = append(vs, v)
	}
	if a.methods&AuthBasic != 0 {
		vs = append(vs, `Basic realm="`+a.realm+`"`)
	}
	return vs
}

// verify checks the Authorization value authz of a request of method,
// looking up the password of the user it names. It returns the user once
// the credentials check out, and stale when they would but for an expired
// nonce.
func (a *authServer) verify(method, authz string, now time.Time, password func(user string) (string, bool)) (user string, ok, stale bool) {
	scheme, rest, _ := strings.Cut(strings.TrimSpace(authz), " ")
	switch {
	case strings.EqualFold(scheme, "Basic") && a.methods&AuthBasic != 0:
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(rest))
		if err != nil {
			return "", false, false
		}
		u, p, found := strings.Cut(string(b), ":")
		want, known := password(u)
		if !found || !known || subtle.ConstantTimeCompare([]byte(p), []byte(want)) != 1 {
			return "", false, false
		}
		return u, true, false
	case strings.EqualFold(scheme, "Digest") && a.methods&AuthDigest != 0:
		p := parseAuthParams(rest)
		u := p["username"]
		ours, fresh := a.nonceValid(p["nonce"], now)
		if !ours || p["realm"] != a.realm {
			return "", false, false
		}
		if alg := p["algorithm"]; alg != "" && !strings.EqualFold(alg, "MD5") {
			return "", false, false
		}
		want, known := password(u)
		if !known {
			return "", false, false
		}
		expect := digestResponse(u, a.realm, want, method, p["uri"], p["nonce"], p["qop"], p["nc"], p["cnonce"])
		if subtle.ConstantTimeCompare([]byte(strings.ToLower(p["response"])), []byte(expect)) != 1 {
			return "", false, false
		}
		if !fresh {
			return "", false, true
		}
		return u, true, false
	}
	return "", false, false
}

func md5Hex(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

// digestResponse computes the response of RFC 2617 section 3.2.2.1 with
// MD5: without qop as RFC 2069 has it, or with qop=auth.
func digestResponse(user, realm, password, method, uri, nonce, qop, nc, cnonce string) string {
	ha1 := md5Hex(user + ":" + realm + ":" + password)
	ha2 := md5Hex(method + ":" + uri)
	if qop == "" {
		return md5Hex(ha1 + ":" + nonce + ":" + ha2)
	}
	return md5Hex(ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":" + qop + ":" + ha2)
}

// parseAuthParams parses the comma-separated name=value parameters of a
// challenge or credentials, values quoted or not.
func parseAuthParams(s string) map[string]string {
	p := make(map[string]string)
	for s = strings.TrimSpace(s); s != ""; {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			break
		}
		name := strings.ToLower(strings.TrimSpace(s[:eq]))
		s = strings.TrimSpace(s[eq+1:])
		var value string
		if strings.HasPrefix(s, `"`) {
			var b strings.Builder
			i := 1
			for ; i < len(s) && s[i] != '"'; i++ {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				b.WriteByte(s[i])
			}
			value = b.String()
			s = s[min(i+1, len(s)):]
		} else {
			end := strings.IndexByte(s, ',')
			if end < 0 {
				end = len(s)
			}
			value = strings.TrimSpace(s[:end])
			s = s[end:]
		}
		p[name] = value
		s = strings.TrimLeft(strings.TrimSpace(s), ",")
		s = strings.TrimSpace(s)
	}
	return p
}

// clientAuth answers a server's challenge.
type clientAuth struct {
	user, password string
	basic          bool
	realm, nonce   string
	opaque         string
	qop            bool
	nc             uint32
}

// newClientAuth picks the challenge to answer among the WWW-Authenticate
// values: Digest with MD5 rather than Basic. It reports false when there is
// none it can answer.
func newClientAuth(user, password string, challenges []string) (*clientAuth, bool) {
	var basic *clientAuth
	for _, ch := range challenges {
		scheme, rest, _ := strings.Cut(strings.TrimSpace(ch), " ")
		p := parseAuthParams(rest)
		switch {
		case strings.EqualFold(scheme, "Digest"):
			if alg := p["algorithm"]; alg != "" && !strings.EqualFold(alg, "MD5") {
				continue
			}
			a := &clientAuth{user: user, password: password, realm: p["realm"], nonce: p["nonce"], opaque: p["opaque"]}
			for _, q := range strings.Split(p["qop"], ",") {
				if strings.TrimSpace(q) == "auth" {
					a.qop = true
				}
			}
			return a, true
		case strings.EqualFold(scheme, "Basic"):
			basic = &clientAuth{user: user, password: password, basic: true, realm: p["realm"]}
		}
	}
	return basic, basic != nil
}

// authorization returns the Authorization value of a request.
func (a *clientAuth) authorization(method, uri string) string {
	if a.basic {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(a.user+":"+a.password))
	}
	v := `Digest username="` + a.user + `", realm="` + a.realm + `", nonce="` + a.nonce + `", uri="` + uri + `"`
	if a.qop {
		a.nc++
		var b [8]byte
		_, _ = rand.Read(b[:])
		cnonce := hex.EncodeToString(b[:])
		nc := strconv.FormatUint(uint64(a.nc)|1<<32, 16)[1:]
		v += `, response="` + digestResponse(a.user, a.realm, a.password, method, uri, a.nonce, "auth", nc, cnonce) +
			`", qop=auth, nc=` + nc + `, cnonce="` + cnonce + `"`
	} else {
		v += `, response="` + digestResponse(a.user, a.realm, a.password, method, uri, a.nonce, "", "", "") + `"`
	}
	if a.opaque != "" {
		v += `, opaque="` + a.opaque + `"`
	}
	return v + ", algorithm=MD5"
}
