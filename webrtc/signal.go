package webrtc

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"strings"
)

// Answer answers a complete offer in one step, for signaling that carries
// one description each way and no trickled candidates, such as WHIP and
// WHEP over HTTP: it sets the offer, creates and sets the answer, waits
// for gathering to complete or ctx to end, and returns the answer with
// every candidate gathered.
func Answer(ctx context.Context, pc *PeerConnection, offer SessionDescription) (SessionDescription, error) {
	if err := pc.SetRemoteDescription(offer); err != nil {
		return SessionDescription{}, err
	}
	answer, err := pc.CreateAnswer()
	if err != nil {
		return SessionDescription{}, err
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return SessionDescription{}, err
	}
	return waitGathered(ctx, pc)
}

// Offer creates and sets an offer and returns it once gathering is
// complete, or ctx ends, with every candidate gathered: the offering side
// of Answer.
func Offer(ctx context.Context, pc *PeerConnection, opts *OfferOptions) (SessionDescription, error) {
	offer, err := pc.CreateOffer(opts)
	if err != nil {
		return SessionDescription{}, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return SessionDescription{}, err
	}
	return waitGathered(ctx, pc)
}

func waitGathered(ctx context.Context, pc *PeerConnection) (SessionDescription, error) {
	select {
	case <-pc.GatheringComplete():
	case <-ctx.Done():
	}
	d := pc.LocalDescription()
	if d == nil {
		return SessionDescription{}, ErrClosed
	}
	return *d, nil
}

// fingerprintOf is the fingerprint of a DER certificate under the SDP hash
// name alg (RFC 8122 section 5), and whether alg is one it knows.
func fingerprintOf(alg string, der []byte) (string, bool) {
	var h hash.Hash
	switch strings.ToLower(alg) {
	case "sha-1":
		h = sha1.New()
	case "sha-224":
		h = sha256.New224()
	case "sha-256":
		h = sha256.New()
	case "sha-384":
		h = sha512.New384()
	case "sha-512":
		h = sha512.New()
	default:
		return "", false
	}
	h.Write(der)
	s := strings.ToUpper(hex.EncodeToString(h.Sum(nil)))
	var b strings.Builder
	for i := 0; i < len(s); i += 2 {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(s[i : i+2])
	}
	return b.String(), true
}
