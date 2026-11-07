package dht

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// KRPC error codes (BEP 5, BEP 44).
const (
	ErrCodeGeneric       = 201
	ErrCodeServer        = 202
	ErrCodeProtocol      = 203
	ErrCodeMethodUnknown = 204
	ErrCodeTooBig        = 205 // BEP 44: v longer than 1000 bytes
	ErrCodeBadSignature  = 206 // BEP 44
	ErrCodeSaltTooBig    = 207 // BEP 44: salt longer than 64 bytes
	ErrCodeCASMismatch   = 301 // BEP 44
	ErrCodeSeqTooLow     = 302 // BEP 44: seq less than the stored item's
)

// Error is a KRPC error message, the "e" list of a code and a message.
type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string { return "dht: remote error " + strconv.Itoa(e.Code) + ": " + e.Msg }

// MarshalBencode encodes e as [code, message].
func (e Error) MarshalBencode() ([]byte, error) { return bencode.Marshal([]any{e.Code, e.Msg}) }

// UnmarshalBencode decodes [code, message]. Some nodes send only the code,
// or the message first; whatever is there is taken.
func (e *Error) UnmarshalBencode(b []byte) error {
	var l []any
	if err := bencode.Unmarshal(b, &l); err != nil {
		return err
	}
	for _, v := range l {
		switch v := v.(type) {
		case int64:
			e.Code = int(v)
		case string:
			e.Msg = v
		}
	}
	return nil
}

// msg is a KRPC message: a query ("q"), a response ("r") or an error
// ("e"). Byte strings that must have a length (IDs, targets) are []byte so
// that a wrong length is a check of ours, answered with a protocol error,
// rather than a decoding failure.
type msg struct {
	T  string `bencode:"t"`
	Y  string `bencode:"y"`
	Q  string `bencode:"q,omitempty"`
	A  *args  `bencode:"a,omitempty"`
	R  *ret   `bencode:"r,omitempty"`
	E  *Error `bencode:"e,omitempty"`
	IP []byte `bencode:"ip,omitempty"` // BEP 42: the address the message is to, compact
	RO int    `bencode:"ro,omitempty"` // BEP 43: the sender answers no queries
	V  string `bencode:"v,omitempty"`  // the sender's client and version
}

type args struct {
	ID          []byte   `bencode:"id"`
	Target      []byte   `bencode:"target,omitempty"`
	InfoHash    []byte   `bencode:"info_hash,omitempty"`
	Token       []byte   `bencode:"token,omitempty"`
	Port        *int64   `bencode:"port,omitempty"`
	ImpliedPort int      `bencode:"implied_port,omitempty"`
	Want        []string `bencode:"want,omitempty"` // BEP 32: "n4", "n6"
	Seed        int      `bencode:"seed,omitempty"` // BEP 33, ignored
	NoSeed      int      `bencode:"noseed,omitempty"`

	// BEP 44.
	V    bencode.RawMessage `bencode:"v,omitempty"`
	K    []byte             `bencode:"k,omitempty"`
	Salt []byte             `bencode:"salt,omitempty"`
	Sig  []byte             `bencode:"sig,omitempty"`
	Seq  *int64             `bencode:"seq,omitempty"`
	Cas  *int64             `bencode:"cas,omitempty"`
}

type ret struct {
	ID     []byte   `bencode:"id"`
	Nodes  []byte   `bencode:"nodes,omitempty"`
	Nodes6 []byte   `bencode:"nodes6,omitempty"`
	Token  []byte   `bencode:"token,omitempty"`
	Values [][]byte `bencode:"values,omitempty"`

	// BEP 51. Samples is a pointer so that an empty list is still sent,
	// which tells an indexer that the node supports the query.
	Interval *int64  `bencode:"interval,omitempty"`
	Num      *int64  `bencode:"num,omitempty"`
	Samples  *[]byte `bencode:"samples,omitempty"`

	// BEP 44.
	V   bencode.RawMessage `bencode:"v,omitempty"`
	K   []byte             `bencode:"k,omitempty"`
	Sig []byte             `bencode:"sig,omitempty"`
	Seq *int64             `bencode:"seq,omitempty"`
}

var errMalformed = errors.New("dht: malformed KRPC message")

// parseMsg decodes a KRPC message and checks what every message of its
// kind must carry. A value of the wrong type in an optional key leaves
// that key out instead of failing the message, so that one odd field from
// some client does not cost the whole message.
func parseMsg(b []byte) (*msg, error) {
	if len(b) == 0 || b[0] != 'd' {
		return nil, errMalformed
	}
	m := new(msg)
	if err := bencode.Unmarshal(b, m); err != nil {
		var te *bencode.UnmarshalTypeError
		if !errors.As(err, &te) {
			return nil, err
		}
	}
	if m.T == "" || len(m.T) > 32 {
		return nil, errMalformed
	}
	switch m.Y {
	case "q":
		if m.Q == "" || m.A == nil {
			return nil, errMalformed
		}
	case "r":
		if m.R == nil || len(m.R.ID) != 20 {
			return nil, errMalformed
		}
	case "e":
		if m.E == nil {
			m.E = &Error{Code: ErrCodeGeneric}
		}
	default:
		return nil, errMalformed
	}
	return m, nil
}

// encode bencodes m. It cannot fail for the messages the node builds, so a
// failure is a bug.
func (m *msg) encode() []byte {
	b, err := bencode.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("dht: encoding a KRPC message: %v", err))
	}
	return b
}

// sender returns the ID the message carries, if any.
func (m *msg) sender() (ID, bool) {
	var id []byte
	switch {
	case m.A != nil:
		id = m.A.ID
	case m.R != nil:
		id = m.R.ID
	}
	if len(id) != 20 {
		return ID{}, false
	}
	return ID(id), true
}

// wants reports whether a query asks for the nodes of a family (BEP 32).
// Without "want" a node gets those of the family it asked on.
func (a *args) wants(v6, askedOnV6 bool) bool {
	if len(a.Want) == 0 {
		return v6 == askedOnV6
	}
	w := "n4"
	if v6 {
		w = "n6"
	}
	for _, x := range a.Want {
		if x == w {
			return true
		}
	}
	return false
}
