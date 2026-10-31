package webrtc

import "fmt"

// PeerConnectionState is RTCPeerConnectionState: ICE and DTLS taken
// together.
type PeerConnectionState uint8

const (
	PeerConnectionStateNew PeerConnectionState = iota
	PeerConnectionStateConnecting
	PeerConnectionStateConnected
	PeerConnectionStateDisconnected
	PeerConnectionStateFailed
	PeerConnectionStateClosed
)

var peerConnectionStateNames = [...]string{"new", "connecting", "connected", "disconnected", "failed", "closed"}

func (s PeerConnectionState) String() string {
	if int(s) < len(peerConnectionStateNames) {
		return peerConnectionStateNames[s]
	}
	return fmt.Sprintf("PeerConnectionState(%d)", uint8(s))
}

// ICEConnectionState is RTCIceConnectionState. There is no "completed":
// the agent keeps checking consent on the selected pair, which a browser
// reports as connected too.
type ICEConnectionState uint8

const (
	ICEConnectionStateNew ICEConnectionState = iota
	ICEConnectionStateChecking
	ICEConnectionStateConnected
	ICEConnectionStateDisconnected
	ICEConnectionStateFailed
	ICEConnectionStateClosed
)

var iceConnectionStateNames = [...]string{"new", "checking", "connected", "disconnected", "failed", "closed"}

func (s ICEConnectionState) String() string {
	if int(s) < len(iceConnectionStateNames) {
		return iceConnectionStateNames[s]
	}
	return fmt.Sprintf("ICEConnectionState(%d)", uint8(s))
}

// ICEGatheringState is RTCIceGatheringState.
type ICEGatheringState uint8

const (
	ICEGatheringStateNew ICEGatheringState = iota
	ICEGatheringStateGathering
	ICEGatheringStateComplete
)

var iceGatheringStateNames = [...]string{"new", "gathering", "complete"}

func (s ICEGatheringState) String() string {
	if int(s) < len(iceGatheringStateNames) {
		return iceGatheringStateNames[s]
	}
	return fmt.Sprintf("ICEGatheringState(%d)", uint8(s))
}

// SignalingState is RTCSignalingState, less the provisional answer states,
// which are not supported.
type SignalingState uint8

const (
	SignalingStateStable SignalingState = iota
	SignalingStateHaveLocalOffer
	SignalingStateHaveRemoteOffer
	SignalingStateClosed
)

var signalingStateNames = [...]string{"stable", "have-local-offer", "have-remote-offer", "closed"}

func (s SignalingState) String() string {
	if int(s) < len(signalingStateNames) {
		return signalingStateNames[s]
	}
	return fmt.Sprintf("SignalingState(%d)", uint8(s))
}

// SDPType is the type of a SessionDescription, as RTCSdpType spells it.
type SDPType string

const (
	SDPTypeOffer  SDPType = "offer"
	SDPTypeAnswer SDPType = "answer"
)

// SessionDescription is an offer or an answer, as RTCSessionDescriptionInit
// is; it marshals to the same JSON a browser's does.
type SessionDescription struct {
	Type SDPType `json:"type"`
	SDP  string  `json:"sdp"`
}

// ICECandidateInit is a trickled candidate, as RTCIceCandidateInit is,
// and marshals to the same JSON. Candidate is the a=candidate value,
// "candidate:...", or empty for the end of candidates.
type ICECandidateInit struct {
	Candidate        string `json:"candidate"`
	SDPMid           string `json:"sdpMid"`
	SDPMLineIndex    uint16 `json:"sdpMLineIndex"`
	UsernameFragment string `json:"usernameFragment,omitempty"`
}

// BundlePolicy is RTCBundlePolicy. Only max-bundle is supported: every
// media section and data channel share one ICE and DTLS transport.
type BundlePolicy uint8

const BundlePolicyMaxBundle BundlePolicy = 0

// Direction is a transceiver's direction, as SDP writes it.
type Direction string

const (
	DirectionSendRecv Direction = "sendrecv"
	DirectionSendOnly Direction = "sendonly"
	DirectionRecvOnly Direction = "recvonly"
	DirectionInactive Direction = "inactive"
)

func (d Direction) sends() bool    { return d == DirectionSendRecv || d == DirectionSendOnly }
func (d Direction) receives() bool { return d == DirectionSendRecv || d == DirectionRecvOnly }

func directionOf(send, recv bool) Direction {
	switch {
	case send && recv:
		return DirectionSendRecv
	case send:
		return DirectionSendOnly
	case recv:
		return DirectionRecvOnly
	}
	return DirectionInactive
}
