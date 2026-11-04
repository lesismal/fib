package webrtc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lesismal/fib/webrtc/ice"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// OfferOptions are the options of CreateOffer.
type OfferOptions struct {
	// ICERestart gives the offer new ICE credentials, which restarts ICE
	// once it is set and answered (RFC 8445 section 9). Media and data
	// keep flowing on the old pair until the new checks select one.
	ICERestart bool
}

// CreateOffer returns an offer for the PeerConnection's transceivers and,
// once a data channel has been created, its data channels, with the
// candidates gathered so far.
func (pc *PeerConnection) CreateOffer(opts *OfferOptions) (SessionDescription, error) {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return SessionDescription{}, ErrClosed
	}
	if pc.signaling == SignalingStateHaveRemoteOffer {
		return SessionDescription{}, ErrSignalingState
	}
	ufrag, pwd := pc.ufrag, pc.pwd
	switch {
	case opts != nil && opts.ICERestart:
		pc.pendingCreds = [2]string{randomString(8), randomString(24)}
		pc.pendingRestart = true
		fallthrough
	case pc.pendingRestart:
		ufrag, pwd = pc.pendingCreds[0], pc.pendingCreds[1]
	}
	for _, t := range pc.transceivers {
		if t.mid == "" {
			t.mid = pc.newMid()
		}
	}
	if pc.dataMid == "" && len(pc.channels) > 0 {
		pc.dataMid = pc.newMid()
	}
	sd := pc.buildDescription(SDPTypeOffer, ufrag, pwd)
	pc.addCandidates(sd)
	pc.lastOffer = sd.String()
	return SessionDescription{Type: SDPTypeOffer, SDP: pc.lastOffer}, nil
}

// CreateAnswer returns the answer to the remote offer: each of its media
// sections accepted with the codecs and header extensions both sides
// support, or rejected when there are none, and its data channels.
func (pc *PeerConnection) CreateAnswer() (SessionDescription, error) {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return SessionDescription{}, ErrClosed
	}
	if pc.signaling != SignalingStateHaveRemoteOffer {
		return SessionDescription{}, ErrSignalingState
	}
	for _, t := range pc.transceivers {
		if t.mid != "" {
			pc.resolve(t)
		}
	}
	sd := pc.buildDescription(SDPTypeAnswer, pc.ufrag, pc.pwd)
	pc.addCandidates(sd)
	pc.lastAnswer = sd.String()
	return SessionDescription{Type: SDPTypeAnswer, SDP: pc.lastAnswer}, nil
}

func (pc *PeerConnection) newMid() string {
	for {
		mid := itoa(pc.nextMid)
		pc.nextMid++
		used := mid == pc.dataMid
		for _, m := range pc.midOrder {
			used = used || m == mid
		}
		if !used {
			pc.midOrder = append(pc.midOrder, mid)
			return mid
		}
	}
}

// setup is the a=setup this side writes: actpass in an offer until the
// DTLS roles are settled, and its role after that, as RFC 8842 has it. An
// answer always has its role, which the remote offer settled.
func (pc *PeerConnection) setup() sdp.Setup {
	if pc.dtlsRole != "" {
		return pc.dtlsRole
	}
	return sdp.SetupActpass
}

// buildDescription writes the offer or answer for the sections in
// midOrder.
func (pc *PeerConnection) buildDescription(typ SDPType, ufrag, pwd string) *sdp.SessionDescription {
	pc.sessionVersion++
	sd := sdp.New(pc.sessionID, pc.sessionVersion)
	if pc.cfg.ICELite {
		sd.SetICELite(true)
	}
	sd.AddAttribute("msid-semantic", " WMS *")
	var bundle []string
	setup := pc.setup()
	for _, mid := range pc.midOrder {
		var md *sdp.MediaDescription
		if mid == pc.dataMid {
			md = pc.dataSection(typ)
		} else if t := pc.transceiverByMid(mid); t != nil {
			md = pc.mediaSection(typ, t)
		}
		if md == nil {
			md = pc.rejectedSection(mid)
		}
		if md == nil {
			continue
		}
		if !md.Rejected() {
			bundle = append(bundle, mid)
			md.SetICECredentials(ufrag, pwd)
			md.SetICEOptions("trickle")
			md.AddFingerprint(sdp.Fingerprint{Algorithm: "sha-256", Value: pc.fingerprint})
			md.SetSetup(setup)
		}
		md.SetMid(mid)
		pc.orderSection(md)
		sd.AddMedia(md)
	}
	if len(bundle) > 0 {
		sd.SetBundle(bundle...)
	}
	return sd
}

// orderSection moves the mid and the transport attributes ahead of the
// rest, the order browsers write them in.
func (pc *PeerConnection) orderSection(md *sdp.MediaDescription) {
	first := map[string]int{"ice-ufrag": 1, "ice-pwd": 2, "ice-options": 3, "fingerprint": 4, "setup": 5, "mid": 6}
	var head, attrs, tail []sdp.Line
	for _, l := range md.Lines {
		if l.Type != 'a' {
			head = append(head, l)
			continue
		}
		name, _, _ := l.Attribute()
		if first[name] > 0 {
			attrs = append(attrs, l)
		} else {
			tail = append(tail, l)
		}
	}
	for i := 1; i < len(attrs); i++ {
		for j := i; j > 0; j-- {
			a, _, _ := attrs[j-1].Attribute()
			b, _, _ := attrs[j].Attribute()
			if first[a] <= first[b] {
				break
			}
			attrs[j-1], attrs[j] = attrs[j], attrs[j-1]
		}
	}
	md.Lines = append(append(head, attrs...), tail...)
}

// rejectedSection rejects a section of the remote offer this side cannot
// take, keeping its kind, protocol and formats.
func (pc *PeerConnection) rejectedSection(mid string) *sdp.MediaDescription {
	if pc.remoteDesc == nil {
		return nil
	}
	rmd := pc.remoteDesc.MediaByMid(mid)
	if rmd == nil {
		return nil
	}
	m, ok := rmd.MediaName()
	if !ok {
		return nil
	}
	m.Port = 0
	md := &sdp.MediaDescription{}
	md.Lines = []sdp.Line{{Type: 'm', Value: m.String()}, {Type: 'c', Value: "IN IP4 0.0.0.0"}}
	md.SetDirection(sdp.Inactive)
	// RFC 5761: an answerer that multiplexes answers rtcp-mux, and
	// browsers refuse an answer that drops it from a section the offer
	// carried it in, rejected or not.
	if rmd.RTCPMux() {
		md.SetRTCPMux(true)
	}
	return md
}

func (pc *PeerConnection) dataSection(typ SDPType) *sdp.MediaDescription {
	if typ == SDPTypeAnswer && !pc.dataAccepted {
		return nil
	}
	md := sdp.NewDataChannelMedia()
	md.SetMaxMessageSize(uint64(pc.cfg.MaxMessageSize))
	return md
}

// mediaSection writes a transceiver's section: in an offer its own codecs,
// or those negotiated before, and in an answer those both sides support,
// each followed by its RTX format if it has one.
func (pc *PeerConnection) mediaSection(typ SDPType, t *Transceiver) *sdp.MediaDescription {
	var codecs []sdp.Codec
	rtx := false
	if typ == SDPTypeAnswer || len(t.codecs) > 0 {
		for _, c := range t.codecs {
			codecs = append(codecs, c.toSDP(c.recvPT))
			if c.rtxRecv != 0 {
				codecs = append(codecs, rtxSDP(c.rtxRecv, c.recvPT, c.ClockRate))
				rtx = true
			}
		}
	} else {
		for _, c := range pc.codecs {
			if c.Kind() == t.kind {
				codecs = append(codecs, c.toSDP(c.PayloadType))
				if pt, ok := pc.rtxPTs[c.PayloadType]; ok {
					codecs = append(codecs, rtxSDP(pt, c.PayloadType, c.ClockRate))
					rtx = true
				}
			}
		}
	}
	if len(codecs) == 0 || t.stopped {
		if typ == SDPTypeAnswer {
			return nil
		}
		// An offer's codecs are the configured ones of the kind, so with
		// none there are none to name.
		md := sdp.NewMedia(string(t.kind), 0, sdp.ProtoRTP)
		for _, c := range codecs[:min(1, len(codecs))] {
			md.AddCodec(c)
		}
		md.SetDirection(sdp.Inactive)
		return md
	}
	md := sdp.NewMedia(string(t.kind), 9, sdp.ProtoRTP)
	for _, e := range pc.exts {
		if e.Kind != "" && e.Kind != t.kind {
			continue
		}
		if e.URI == rtp.ExtensionURIRid && typ == SDPTypeAnswer && len(t.rids) == 0 {
			continue
		}
		id, ok := t.extIDs[e.URI]
		if !ok && typ == SDPTypeOffer && len(t.codecs) == 0 {
			id, ok = pc.extensionID(e.URI)
		}
		if ok {
			md.AddExtMap(sdp.ExtMap{ID: int(id), URI: e.URI})
		}
	}
	dir := t.direction
	if typ == SDPTypeAnswer {
		dir = t.current
	}
	if !t.sender.hasTrack() {
		dir = directionOf(false, dir.receives())
	}
	md.SetDirection(sdp.Direction(dir))
	tr := t.sender.track
	if dir.sends() && tr != nil {
		md.AddMSID(sdp.MSID{Stream: tr.streamID, Track: tr.id})
	}
	md.SetRTCPMux(true)
	md.SetRTCPRsize(true)
	for _, c := range codecs {
		md.AddCodec(c)
	}
	if typ == SDPTypeAnswer && len(t.rids) > 0 {
		var alts [][]sdp.SimulcastRID
		for _, rid := range t.rids {
			md.AddRID(sdp.RID{ID: rid, Direction: "recv"})
			alts = append(alts, []sdp.SimulcastRID{{ID: rid}})
		}
		md.SetSimulcast(sdp.Simulcast{Recv: alts})
	}
	if dir.sends() && tr != nil {
		ssrcs := []uint32{t.sender.ssrc}
		if rtx && t.sender.rtxSSRC != 0 {
			ssrcs = append(ssrcs, t.sender.rtxSSRC)
			md.AddSSRCGroup(sdp.SSRCGroup{Semantics: "FID", SSRCs: ssrcs})
		}
		for _, ssrc := range ssrcs {
			md.AddSSRC(sdp.SSRC{SSRC: ssrc, Attribute: "cname", Value: pc.cname})
			md.AddSSRC(sdp.SSRC{SSRC: ssrc, Attribute: "msid", Value: tr.streamID + " " + tr.id})
		}
	}
	return md
}

// extensionID returns the id a header extension has in this session, or
// gives it the lowest one free. Under BUNDLE an id means one URI across
// every section and every negotiation.
func (pc *PeerConnection) extensionID(uri string) (uint8, bool) {
	if id, ok := pc.extIDs[uri]; ok {
		return id, true
	}
	for id := uint8(1); id <= 14; id++ {
		used := false
		for _, v := range pc.extIDs {
			used = used || v == id
		}
		if !used {
			pc.extIDs[uri] = id
			return id, true
		}
	}
	return 0, false
}

// addCandidates writes the local candidates gathered so far into the first
// section, which bundles the rest, and end-of-candidates once gathering is
// over.
func (pc *PeerConnection) addCandidates(sd *sdp.SessionDescription) {
	for _, md := range sd.Media {
		if md.Rejected() {
			continue
		}
		for _, c := range pc.localCands {
			md.AddCandidate(c.String())
		}
		if pc.gathering == ICEGatheringStateComplete {
			md.SetEndOfCandidates(true)
		}
		return
	}
}

// LocalDescription returns the local description, with every candidate
// gathered so far: once GatheringComplete is closed, all of them.
func (pc *PeerConnection) LocalDescription() *SessionDescription {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.localDesc == nil {
		return nil
	}
	sd := pc.localDesc.Clone()
	for _, md := range sd.Media {
		md.DeleteAttribute("candidate")
		md.DeleteAttribute("end-of-candidates")
	}
	pc.addCandidates(sd)
	return &SessionDescription{Type: pc.localType, SDP: sd.String()}
}

// RemoteDescription returns the remote description last set.
func (pc *PeerConnection) RemoteDescription() *SessionDescription {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.remoteDesc == nil {
		return nil
	}
	return &SessionDescription{Type: pc.remoteType, SDP: pc.remoteDesc.String()}
}

// SetLocalDescription sets an offer or answer CreateOffer or CreateAnswer
// returned. Setting the first starts gathering candidates, which
// OnICECandidate reports; setting an answer starts DTLS.
func (pc *PeerConnection) SetLocalDescription(desc SessionDescription) error {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return ErrClosed
	}
	sd, err := sdp.ParseString(desc.SDP)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDescription, err)
	}
	ufrag, pwd := sessionCredentials(sd)
	now := time.Now()
	switch desc.Type {
	case SDPTypeOffer:
		if pc.signaling != SignalingStateStable && pc.signaling != SignalingStateHaveLocalOffer {
			return ErrSignalingState
		}
		if ufrag != "" && ufrag != pc.ufrag {
			if err := pc.restartLocal(ufrag, pwd, now); err != nil {
				return err
			}
		}
		pc.pendingRestart = false
		if err := pc.ensureAgent(true, now); err != nil {
			return err
		}
		pc.setSignaling(SignalingStateHaveLocalOffer)
	case SDPTypeAnswer:
		if pc.signaling != SignalingStateHaveRemoteOffer {
			return ErrSignalingState
		}
		if err := pc.ensureAgent(false, now); err != nil {
			return err
		}
		for _, t := range pc.transceivers {
			if t.mid != "" {
				pc.resolve(t)
			}
		}
		pc.setSignaling(SignalingStateStable)
		if err := pc.startTransports(now); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: type %q", ErrDescription, desc.Type)
	}
	pc.localDesc, pc.localType = sd, desc.Type
	if !pc.localSet {
		pc.localSet = true
		pc.emitAllCandidates()
	}
	return nil
}

// emitAllCandidates reports every local candidate, and the end of them if
// gathering is over: when the local description is first set, and again
// after an ICE restart, whose new credentials they go with.
func (pc *PeerConnection) emitAllCandidates() {
	for i := range pc.localCands {
		c := pc.localCands[i]
		pc.emitCandidate(&c)
	}
	if pc.gathering == ICEGatheringStateComplete {
		pc.emitCandidate(nil)
	}
}

// restartLocal takes new local credentials: the agent restarts and the Mux
// routes checks for the new ufrag here, keeping the previous one for the
// checks still in flight.
func (pc *PeerConnection) restartLocal(ufrag, pwd string, now time.Time) error {
	if pc.agent != nil {
		if err := pc.agent.Restart(ufrag, pwd, now); err != nil {
			return err
		}
	}
	if err := pc.mux.register(ufrag, pc); err != nil {
		return err
	}
	if pc.oldUfrag != "" {
		pc.mux.unregister(pc.oldUfrag, pc)
	}
	pc.oldUfrag = pc.ufrag
	pc.ufrag, pc.pwd = ufrag, pwd
	pc.remoteUfrag, pc.remotePwd = "", ""
	if pc.agent != nil && pc.localSet {
		pc.emitAllCandidates()
	}
	return nil
}

// sessionCredentials returns the ICE credentials of the first section that
// is not rejected, or of the session.
func sessionCredentials(sd *sdp.SessionDescription) (ufrag, pwd string) {
	for _, md := range sd.Media {
		if md.Rejected() {
			continue
		}
		ufrag, _ = sd.Lookup(md, "ice-ufrag")
		pwd, _ = sd.Lookup(md, "ice-pwd")
		return ufrag, pwd
	}
	ufrag, _ = sd.Attribute("ice-ufrag")
	pwd, _ = sd.Attribute("ice-pwd")
	return ufrag, pwd
}

// SetRemoteDescription applies the peer's offer or answer: its ICE
// credentials and candidates, its certificate fingerprints, and what each
// of its sections negotiates. An offer that adds media sections makes
// transceivers for them, taking those AddTrack made that have no section
// yet; an offer with new ICE credentials restarts ICE.
func (pc *PeerConnection) SetRemoteDescription(desc SessionDescription) error {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return ErrClosed
	}
	sd, err := sdp.ParseString(desc.SDP)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDescription, err)
	}
	offer := desc.Type == SDPTypeOffer
	switch {
	case offer && pc.signaling != SignalingStateStable && pc.signaling != SignalingStateHaveRemoteOffer:
		return ErrSignalingState
	case desc.Type == SDPTypeAnswer && pc.signaling != SignalingStateHaveLocalOffer:
		return ErrSignalingState
	case !offer && desc.Type != SDPTypeAnswer:
		return fmt.Errorf("%w: type %q", ErrDescription, desc.Type)
	}
	ufrag, pwd := sessionCredentials(sd)
	if ufrag == "" || pwd == "" {
		return fmt.Errorf("%w: no ICE credentials", ErrDescription)
	}
	var first *sdp.MediaDescription
	accepted := 0
	for _, md := range sd.Media {
		if _, ok := md.Mid(); !ok {
			return fmt.Errorf("%w: a media section has no mid", ErrDescription)
		}
		if !md.Rejected() {
			if first == nil {
				first = md
			}
			accepted++
		}
	}
	if first == nil {
		return fmt.Errorf("%w: no media section", ErrDescription)
	}
	if _, ok := sd.Bundle(); !ok && accepted > 1 {
		return fmt.Errorf("%w: sections are not bundled", ErrDescription)
	}
	var fps []sdp.Fingerprint
	for _, v := range append(first.Attributes("fingerprint"), sd.Attributes("fingerprint")...) {
		if f, err := sdp.ParseFingerprint(v); err == nil {
			fps = append(fps, f)
		}
	}
	if len(fps) == 0 {
		return fmt.Errorf("%w: no fingerprint", ErrDescription)
	}
	setupValue, _ := sd.Lookup(first, "setup")
	remoteSetup := sdp.Setup(setupValue)

	now := time.Now()
	if offer {
		pc.midOrder = pc.midOrder[:0]
	}
	for _, md := range sd.Media {
		mid, _ := md.Mid()
		if offer {
			pc.midOrder = append(pc.midOrder, mid)
		}
		if md.IsDataChannel() || md.Kind() == "application" {
			pc.applyRemoteData(md, offer)
			continue
		}
		kind := MediaKind(md.Kind())
		if kind != MediaKindAudio && kind != MediaKindVideo {
			continue
		}
		t := pc.transceiverByMid(mid)
		if t == nil && offer && !md.Rejected() {
			t = pc.claimTransceiver(kind)
			if t == nil {
				t = pc.newTransceiver(kind, DirectionRecvOnly)
			}
			t.mid = mid
		}
		if t != nil {
			pc.applyRemoteMedia(sd, md, t, offer)
		}
	}
	pc.remoteDesc, pc.remoteType = sd, desc.Type
	pc.remoteFprints = fps
	pc.remoteLite = sd.ICELite()

	if pc.dtlsRole == "" {
		switch {
		case offer && remoteSetup == sdp.SetupActive:
			pc.dtlsRole = sdp.SetupPassive
		case offer:
			pc.dtlsRole = sdp.SetupActive
		case remoteSetup == sdp.SetupPassive:
			pc.dtlsRole = sdp.SetupActive
		default:
			pc.dtlsRole = sdp.SetupPassive
		}
	}

	if err := pc.ensureAgent(!offer, now); err != nil {
		return err
	}
	if offer && pc.remoteUfrag != "" && ufrag != pc.remoteUfrag {
		// The peer restarts ICE, and so does this side, with credentials
		// of its own for the answer (RFC 9429 section 5.3.1).
		if err := pc.restartLocal(randomString(8), randomString(24), now); err != nil {
			return err
		}
	}
	if pc.remoteLite {
		pc.agent.SetRemoteLite(true)
		pc.controlling = pc.agent.Controlling()
	}
	if err := pc.agent.SetRemoteCredentials(ufrag, pwd, now); err != nil {
		return err
	}
	pc.remoteUfrag, pc.remotePwd = ufrag, pwd
	for _, md := range sd.Media {
		if md.Rejected() {
			continue
		}
		for _, line := range md.Candidates() {
			if c, err := ice.ParseCandidate(line); err == nil {
				pc.addRemoteCandidate(c, now)
			}
		}
		if md.EndOfCandidates() {
			pc.agent.EndOfRemoteCandidates()
		}
	}
	for _, line := range sd.Candidates() {
		if c, err := ice.ParseCandidate(line); err == nil {
			pc.addRemoteCandidate(c, now)
		}
	}
	pending := pc.pendingCands
	pc.pendingCands = nil
	for _, c := range pending {
		pc.addRemoteCandidate(c, now)
	}

	if offer {
		pc.setSignaling(SignalingStateHaveRemoteOffer)
		return nil
	}
	pc.setSignaling(SignalingStateStable)
	return pc.startTransports(now)
}

func (pc *PeerConnection) addRemoteCandidate(c ice.Candidate, now time.Time) {
	if err := pc.agent.AddRemoteCandidate(c, now); err != nil && !errors.Is(err, ice.ErrUnresolved) {
		return
	}
}

// AddICECandidate adds a candidate the peer trickled, or with an empty
// Candidate, the end of them. One that arrives before the remote
// description waits for it.
func (pc *PeerConnection) AddICECandidate(init ICECandidateInit) error {
	pc.mu.Lock()
	defer pc.unlock()
	if pc.closed {
		return ErrClosed
	}
	if init.UsernameFragment != "" && pc.remoteUfrag != "" && init.UsernameFragment != pc.remoteUfrag {
		// A candidate of an ICE generation that has been restarted.
		return nil
	}
	if strings.TrimSpace(init.Candidate) == "" {
		if pc.agent != nil && pc.remoteUfrag != "" {
			pc.agent.EndOfRemoteCandidates()
		}
		return nil
	}
	c, err := ice.ParseCandidate(init.Candidate)
	if err != nil {
		return err
	}
	if pc.agent == nil || pc.remoteUfrag == "" {
		if len(pc.pendingCands) >= pc.cfg.MaxRemoteCandidates {
			return ErrTooManyCandidates
		}
		pc.pendingCands = append(pc.pendingCands, c)
		return nil
	}
	pc.addRemoteCandidate(c, time.Now())
	return nil
}

// applyRemoteData takes the data channel section of a description.
func (pc *PeerConnection) applyRemoteData(md *sdp.MediaDescription, offer bool) {
	mid, _ := md.Mid()
	m, _ := md.MediaName()
	if md.Rejected() || m.Proto != sdp.ProtoSCTP {
		if offer {
			pc.dataAccepted = false
		}
		return
	}
	if offer {
		pc.dataMid = mid
		pc.dataAccepted = true
	}
	if p, ok := md.SCTPPort(); ok {
		pc.sctpPort = p
	}
	if n, ok := md.MaxMessageSize(); ok {
		pc.hasRemoteMs = true
		pc.remoteMaxMs = int(min(n, 1<<30))
	}
}

// applyRemoteMedia takes what a remote media section says of the
// transceiver t: the codecs and header extensions negotiated, the SSRCs
// and RIDs it will send, and which way media flows.
func (pc *PeerConnection) applyRemoteMedia(sd *sdp.SessionDescription, md *sdp.MediaDescription, t *Transceiver, offer bool) {
	if md.Rejected() {
		t.codecs = nil
		t.remoteDir = DirectionInactive
		pc.resolve(t)
		return
	}
	kind := md.Kind()
	remote := md.Codecs()
	// The RTX formats of the remote codecs, by the payload type they
	// repair, when this side answers NACKs.
	var remoteRtx map[uint8]uint8
	if pc.cfg.Retransmit.Packets > 0 {
		remoteRtx = remoteRTX(remote)
	}
	if offer {
		local := t.codecsOffered(pc.codecs)
		t.codecs = intersectCodecs(kind, remote, local)
		for i := range t.codecs {
			if pt, ok := remoteRtx[t.codecs[i].sendPT]; ok {
				t.codecs[i].rtxSend, t.codecs[i].rtxRecv = pt, pt
			}
		}
	} else {
		var offered []Codec
		offeredRtx := pc.rtxPTs
		if len(t.codecs) > 0 {
			offeredRtx = make(map[uint8]uint8)
			for _, c := range t.codecs {
				cc := c.Codec
				cc.PayloadType = c.recvPT
				offered = append(offered, cc)
				if c.rtxRecv != 0 {
					offeredRtx[c.recvPT] = c.rtxRecv
				}
			}
		} else {
			offered = t.codecsOffered(pc.codecs)
		}
		var out []negotiatedCodec
		for _, rc := range remote {
			if isRTX(rc.Name) {
				continue
			}
			r := codecFromSDP(kind, rc)
			for _, l := range offered {
				if l.matches(r) {
					nc := negotiatedCodec{Codec: r, sendPT: r.PayloadType, recvPT: l.PayloadType}
					if pt, ok := remoteRtx[r.PayloadType]; ok && offeredRtx[l.PayloadType] != 0 {
						nc.rtxSend, nc.rtxRecv = pt, offeredRtx[l.PayloadType]
					}
					out = append(out, nc)
					break
				}
			}
		}
		t.codecs = out
	}
	clear(t.extIDs)
	for _, e := range md.ExtMaps() {
		if e.ID < 1 || e.ID > 255 {
			continue
		}
		for _, x := range pc.exts {
			if x.URI == e.URI && (x.Kind == "" || x.Kind == t.kind) {
				t.extIDs[e.URI] = uint8(e.ID)
				pc.extIDs[e.URI] = uint8(e.ID)
			}
		}
	}
	pc.extMid = pc.extIDs[rtp.ExtensionURIMid]
	pc.extRid = pc.extIDs[rtp.ExtensionURIRid]
	for _, s := range md.SSRCs() {
		if len(pc.remoteSSRCs) < pc.cfg.MaxRemoteSSRCs {
			pc.remoteSSRCs[s.SSRC] = t.mid
		}
		if s.Attribute == "msid" && t.remoteStream == "" {
			stream, track, _ := strings.Cut(s.Value, " ")
			t.remoteStream, t.remoteTrack = stream, track
		}
	}
	if pc.cfg.Retransmit.Packets > 0 {
		for _, g := range md.SSRCGroups() {
			if g.Semantics == "FID" && len(g.SSRCs) == 2 && len(pc.rtxPrimary) < pc.cfg.MaxRemoteSSRCs {
				pc.rtxPrimary[g.SSRCs[1]] = g.SSRCs[0]
			}
		}
	}
	if ms := md.MSIDs(); len(ms) > 0 {
		t.remoteStream, t.remoteTrack = ms[0].Stream, ms[0].Track
	}
	if offer {
		t.rids = t.rids[:0]
		for _, r := range md.RIDs() {
			if r.Direction == "send" && len(t.rids) < 8 {
				t.rids = append(t.rids, r.ID)
			}
		}
		if _, ok := t.extIDs[rtp.ExtensionURIRid]; !ok {
			t.rids = t.rids[:0]
		}
	}
	t.remoteDir = Direction(sd.MediaDirection(md))
	pc.resolve(t)
}
