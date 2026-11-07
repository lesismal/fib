package bittorrent

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/peer"
)

// State is where a torrent is in its life.
type State int

// The states of a torrent.
const (
	// StateMetadata is a torrent from a magnet link fetching its info
	// dictionary from peers (BEP 9).
	StateMetadata State = iota
	// StateChecking is a torrent hashing the data it finds in storage.
	StateChecking
	// StateDownloading is a torrent missing pieces it wants.
	StateDownloading
	// StateSeeding is a torrent with every piece it wants, uploading.
	StateSeeding
	// StatePaused is a torrent with no connections and no announces,
	// until Start.
	StatePaused
	// StateStopped is a paused torrent whose storage is closed.
	StateStopped
	// StateRemoved is a torrent no longer in its client.
	StateRemoved
	// StateError is a torrent its storage failed.
	StateError
)

func (s State) String() string {
	switch s {
	case StateMetadata:
		return "metadata"
	case StateChecking:
		return "checking"
	case StateDownloading:
		return "downloading"
	case StateSeeding:
		return "seeding"
	case StatePaused:
		return "paused"
	case StateStopped:
		return "stopped"
	case StateRemoved:
		return "removed"
	case StateError:
		return "error"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// AddOptions are the options of a torrent being added.
type AddOptions struct {
	// Paused adds the torrent without starting it.
	Paused bool
	// Storage, when set, is the torrent's storage in place of the
	// client's.
	Storage StorageOpener
	// Sequential downloads pieces in order, for a file played while it
	// downloads; a Reader does that around its position anyway.
	Sequential bool
	// Peers are addresses ("host:port") to connect to, beside those
	// discovery finds.
	Peers []string
	// Trackers are added to the torrent's own, as one more tier each.
	Trackers []string
	// FilePriorities, when set, are the files' priorities by their index
	// in Info.FileList, in place of PriorityNormal for all (and of a
	// magnet link's so=).
	FilePriorities []int
}

// Torrent is a torrent in a Client: its pieces, its peers and its
// discovery. Its methods are safe for concurrent use.
type Torrent struct {
	c    *Client
	ih   metainfo.InfoHash
	ih2  metainfo.InfoHashV2
	key  [20]byte
	pt   *peer.Torrent
	opts AddOptions

	gotInfo chan struct{}
	removed chan struct{}

	uploaded, downloaded atomic.Int64

	mu         sync.Mutex
	name       string
	info       *metainfo.Info
	infoBytes  []byte
	layers     metainfo.PieceLayers
	trackers   [][]string
	webSeeds   []string
	nodes      []metainfo.Node
	selection  []metainfo.IndexRange
	opener     StorageOpener
	storage    Storage
	state      State
	paused     bool
	checking   bool
	err        error
	pieces     []pieceState
	partials   map[int]*partial
	have       *peer.Bitfield
	haveCount  int
	missing    int // wanted pieces not had
	sequential bool
	endgame    bool
	rng        *rand.Rand
	filePrio   []int
	urgent     map[int]int
	readers    map[*Reader]struct{}
	done       chan struct{}
	pieceSig   chan struct{}

	peers    map[*peer.Conn]*peerConn
	pool     map[netip.AddrPort]*candidate
	dials    map[*peer.Conn]*dialAttempt
	early    map[*peer.Conn]earlyClose
	halfOpen int

	meta       *peer.MetadataAssembler
	metaReqs   map[int]time.Time
	hashReqs   map[hashKey]hashReq
	layerParts map[[32]byte]*layerFetch

	banRecords map[blockKey][]blockRecord
	hashFails  int
	wasted     int64

	optimistic   *peerConn
	optimisticAt time.Time
	lastChoke    time.Time
	completedAt  time.Time
	seedStart    time.Time
	upRate       rate
	downRate     rate

	run       context.Context
	stopRun   context.CancelFunc
	completed chan struct{} // closed on completion, for the tracker loops
}

func newTorrent(c *Client, ih metainfo.InfoHash, ih2 metainfo.InfoHashV2, opts AddOptions) *Torrent {
	t := &Torrent{
		c: c, ih: ih, ih2: ih2, opts: opts,
		gotInfo:    make(chan struct{}),
		removed:    make(chan struct{}),
		partials:   map[int]*partial{},
		urgent:     map[int]int{},
		readers:    map[*Reader]struct{}{},
		done:       make(chan struct{}),
		pieceSig:   make(chan struct{}),
		peers:      map[*peer.Conn]*peerConn{},
		pool:       map[netip.AddrPort]*candidate{},
		dials:      map[*peer.Conn]*dialAttempt{},
		early:      map[*peer.Conn]earlyClose{},
		metaReqs:   map[int]time.Time{},
		hashReqs:   map[hashKey]hashReq{},
		layerParts: map[[32]byte]*layerFetch{},
		banRecords: map[blockKey][]blockRecord{},
		rng:        newRNG(),
		sequential: opts.Sequential,
		completed:  make(chan struct{}),
		state:      StateMetadata,
	}
	t.key = ih
	if ih.IsZero() {
		t.key = ih2.Truncate()
	}
	t.opener = opts.Storage
	if t.opener == nil {
		t.opener = c.cfg.Storage
	}
	t.pt = &peer.Torrent{InfoHash: ih, InfoHashV2: ih2, Handler: t}
	return t
}

// InfoHash is the torrent's v1 info hash, zero for a v2-only torrent.
func (t *Torrent) InfoHash() metainfo.InfoHash { return t.ih }

// InfoHashV2 is the torrent's v2 info hash, zero for a v1-only torrent.
func (t *Torrent) InfoHashV2() metainfo.InfoHashV2 { return t.ih2 }

// Name is the torrent's name: the info dictionary's, or the magnet
// link's display name before it.
func (t *Torrent) Name() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.name
}

// Info is the torrent's info dictionary, nil until GotInfo.
func (t *Torrent) Info() *metainfo.Info {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.info
}

// MetaInfo returns the torrent as a .torrent file would hold it, once
// its info dictionary is known: with its trackers, and its piece layers
// as far as they are known.
func (t *Torrent) MetaInfo() *metainfo.MetaInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.info == nil {
		return nil
	}
	mi := &metainfo.MetaInfo{InfoBytes: t.infoBytes, Info: t.info, Nodes: t.nodes}
	if len(t.trackers) > 0 {
		mi.AnnounceList = t.trackers
		if len(t.trackers[0]) > 0 {
			mi.Announce = t.trackers[0][0]
		}
	}
	mi.URLList = t.webSeeds
	if len(t.layers) > 0 {
		mi.PieceLayers = metainfo.PieceLayers{}
		for k, v := range t.layers {
			mi.PieceLayers[k] = v
		}
	}
	return mi
}

// GotInfo is closed once the info dictionary is known: at once for a
// torrent added from a .torrent file, once peers sent it for a magnet
// link.
func (t *Torrent) GotInfo() <-chan struct{} { return t.gotInfo }

// Done is closed once every piece the torrent wants is had and checked.
// Raising the priority of a file that is missing pieces gives a new
// channel.
func (t *Torrent) Done() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done
}

// Removed is closed once the torrent is removed from its client, or the
// client is closed.
func (t *Torrent) Removed() <-chan struct{} { return t.removed }

// State is the torrent's state.
func (t *Torrent) State() State {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// setInfoLocked takes the info dictionary: the pieces, the files and
// their priorities, the storage, and a check of what the storage holds.
func (t *Torrent) setInfoLocked(info *metainfo.Info) error {
	if t.info != nil {
		return nil
	}
	if info.HasV1() && !t.ih.IsZero() && info.InfoHashV1() != t.ih {
		return errors.New("bittorrent: info dictionary of another torrent")
	}
	if info.HasV2() && !t.ih2.IsZero() && info.InfoHashV2() != t.ih2 {
		return errors.New("bittorrent: info dictionary of another torrent")
	}
	// A magnet link of one hash of a hybrid torrent learns the other.
	if info.HasV1() {
		t.ih = info.InfoHashV1()
	}
	if info.HasV2() {
		t.ih2 = info.InfoHashV2()
	}
	t.info = info
	t.infoBytes = info.Bytes()
	t.name = info.Name
	n := info.PieceCount()
	t.pieces = make([]pieceState, n)
	t.have = peer.NewBitfield(n)
	files := info.FileList()
	t.filePrio = make([]int, len(files))
	for i := range files {
		t.filePrio[i] = PriorityNormal
		if len(t.selection) > 0 && !selected(t.selection, i) {
			t.filePrio[i] = PriorityNone
		}
		if i < len(t.opts.FilePriorities) {
			t.filePrio[i] = t.opts.FilePriorities[i]
		}
	}
	t.recomputePrioritiesLocked()
	for _, pc := range t.peers {
		pc.applyInfoLocked(n)
	}
	st, err := t.opener(info)
	if err != nil {
		t.failLocked(err)
		return err
	}
	t.storage = st
	close(t.gotInfo)
	t.c.emit(Event{Type: EventMetadata, Torrent: t})
	t.startCheckLocked()
	return nil
}

func selected(sel []metainfo.IndexRange, i int) bool {
	for _, r := range sel {
		if i >= r.First && i <= r.Last {
			return true
		}
	}
	return false
}

// recomputePrioritiesLocked derives each piece's priority from its files'
// and the Readers', and counts the wanted pieces missing.
func (t *Torrent) recomputePrioritiesLocked() {
	if t.info == nil {
		return
	}
	for i := range t.pieces {
		t.pieces[i].filePri = PriorityNone
	}
	for f, file := range t.info.FileList() {
		if file.IsPad() || file.Length == 0 {
			continue
		}
		p := t.filePrio[f]
		b, e := t.info.FilePieces(f)
		for i := b; i < e; i++ {
			if p > t.pieces[i].filePri {
				t.pieces[i].filePri = p
			}
		}
	}
	missing := 0
	for i := range t.pieces {
		ps := &t.pieces[i]
		ps.prio = max(ps.filePri, t.urgent[i])
		if !ps.have && ps.prio > PriorityNone {
			missing++
		}
	}
	t.missing = missing
	t.updateDoneLocked()
}

// updateDoneLocked moves the torrent between downloading and seeding as
// t.missing says.
func (t *Torrent) updateDoneLocked() {
	if t.info == nil || t.checking {
		return
	}
	isDone := false
	select {
	case <-t.done:
		isDone = true
	default:
	}
	switch {
	case t.missing == 0 && !isDone:
		t.completedAt = time.Now()
		if t.seedStart.IsZero() {
			t.seedStart = t.completedAt
		}
		if t.state == StateDownloading || t.state == StateMetadata {
			t.state = StateSeeding
		}
		select {
		case <-t.completed:
		default:
			close(t.completed)
		}
		t.onCompleteLocked()
		close(t.done)
		t.c.emit(Event{Type: EventCompleted, Torrent: t})
	case t.missing > 0 && isDone:
		t.done = make(chan struct{})
		if t.state == StateSeeding {
			t.state = StateDownloading
		}
		// Seeds dropped this side as a partial seed have something to
		// give again.
		for _, cand := range t.pool {
			cand.nextTry = time.Time{}
			cand.seed = false
		}
		for _, pc := range t.peers {
			t.updateInterestLocked(pc)
			t.fillRequestsLocked(pc)
		}
	case t.missing > 0 && (t.state == StateMetadata || t.state == StateChecking):
		t.state = StateDownloading
	case t.missing == 0 && (t.state == StateMetadata || t.state == StateChecking):
		t.state = StateSeeding
	}
}

// onCompleteLocked is the download's end: no more interest, upload only
// (BEP 21), empty files created, and a full seed drops the seeds it is
// connected to.
func (t *Torrent) onCompleteLocked() {
	full := t.haveCount == len(t.pieces)
	for _, pc := range t.peers {
		t.updateInterestLocked(pc)
		if pc.c.Extended() {
			_ = pc.c.SendExtendedHandshake(t.extHandshakeLocked())
		}
		if full && pc.seed {
			go pc.c.Close()
		}
	}
	if st := t.storage; st != nil {
		for f, file := range t.info.FileList() {
			if file.Length == 0 && !file.IsPad() && t.filePrio[f] > PriorityNone {
				_, _ = st.WriteAt(f, nil, 0)
			}
		}
	}
}

func (t *Torrent) failLocked(err error) {
	t.err = err
	t.state = StateError
	t.c.emit(Event{Type: EventError, Torrent: t, Err: err})
	t.stopRunningLocked()
}

// Err is the error that put the torrent in StateError.
func (t *Torrent) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// Start starts a paused or stopped torrent: it reopens its storage,
// announces, and connects to peers.
func (t *Torrent) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == StateRemoved {
		return
	}
	t.paused = false
	if t.state == StatePaused || t.state == StateStopped || t.state == StateError {
		t.err = nil
		if t.info != nil && t.storage == nil {
			st, err := t.opener(t.info)
			if err != nil {
				t.failLocked(err)
				return
			}
			t.storage = st
		}
		t.state = StateMetadata
		if t.info != nil {
			t.state = StateDownloading
			if t.missing == 0 {
				t.state = StateSeeding
			}
		}
	}
	t.startRunningLocked()
}

// Pause closes the torrent's connections and stops its announces,
// telling the trackers it left; Start resumes it.
func (t *Torrent) Pause() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pauseLocked(StatePaused)
}

// Stop pauses the torrent and closes its storage.
func (t *Torrent) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pauseLocked(StateStopped)
	if t.storage != nil {
		_ = t.storage.Close()
		t.storage = nil
	}
}

func (t *Torrent) pauseLocked(s State) {
	if t.state == StateRemoved {
		return
	}
	t.paused = true
	t.state = s
	t.stopRunningLocked()
	for c := range t.peers {
		go c.Close()
	}
}

// Remove removes the torrent from its client, closing its connections and
// storage, and with deleteData deletes what the storage holds when it
// can (see StorageRemover).
func (t *Torrent) Remove(deleteData bool) error {
	t.c.mu.Lock()
	if t.c.torrents[t.key] == t {
		delete(t.c.torrents, t.key)
	}
	t.c.mu.Unlock()
	t.c.server.RemoveTorrent(t.pt)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == StateRemoved {
		return nil
	}
	t.pauseLocked(StateRemoved)
	close(t.removed)
	var err error
	if t.storage != nil {
		if r, ok := t.storage.(StorageRemover); ok && deleteData {
			err = r.Remove()
		} else {
			err = t.storage.Close()
		}
		t.storage = nil
	}
	t.c.emit(Event{Type: EventStateChanged, Torrent: t})
	return err
}

// startRunningLocked starts discovery: the announces to trackers and the
// DHT, which run until the torrent pauses.
func (t *Torrent) startRunningLocked() {
	if t.run != nil || t.paused || t.state == StateRemoved || t.state == StateError {
		return
	}
	t.run, t.stopRun = context.WithCancel(t.c.ctx)
	t.startDiscoveryLocked(t.run)
}

func (t *Torrent) stopRunningLocked() {
	if t.stopRun != nil {
		t.stopRun()
		t.run, t.stopRun = nil, nil
	}
}

func (t *Torrent) running() bool {
	return t.run != nil && t.state != StateRemoved && t.state != StateError
}

// SetSequential turns sequential download on or off.
func (t *Torrent) SetSequential(on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sequential = on
}

// AddPeers adds peers to connect to, as "host:port" or netip.AddrPort
// strings; names are resolved.
func (t *Torrent) AddPeers(addrs ...string) {
	var aps []netip.AddrPort
	for _, a := range addrs {
		if ap, err := netip.ParseAddrPort(a); err == nil {
			aps = append(aps, ap)
			continue
		}
		if ap, err := resolveAddrPort(a); err == nil {
			aps = append(aps, ap)
		}
	}
	t.addPeers(SourceManual, false, aps)
}

// Files returns the torrent's files, padding files left out, nil before
// GotInfo.
func (t *Torrent) Files() []*File {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.info == nil {
		return nil
	}
	var out []*File
	for i, f := range t.info.FileList() {
		if f.IsPad() {
			continue
		}
		p := append([]string{t.info.Name}, f.Path...)
		out = append(out, &File{t: t, index: i, path: strings.Join(p, "/"), length: f.Length, offset: f.Offset})
	}
	return out
}

// File is a file of a torrent.
type File struct {
	t      *Torrent
	index  int
	path   string
	length int64
	offset int64
}

// Index is the file's index in the torrent's Info.FileList, which BEP 53's
// so= and AddOptions.FilePriorities count by.
func (f *File) Index() int { return f.index }

// Path is the file's path in the torrent, its name first, separated by
// slashes.
func (f *File) Path() string { return f.path }

// Length is the file's size.
func (f *File) Length() int64 { return f.length }

// Offset is where the file begins in the torrent's data.
func (f *File) Offset() int64 { return f.offset }

// Priority is the file's priority.
func (f *File) Priority() int {
	f.t.mu.Lock()
	defer f.t.mu.Unlock()
	return f.t.filePrio[f.index]
}

// SetPriority sets the file's priority: PriorityNone leaves it out of
// the download, PriorityLow to PriorityHigh order it.
func (f *File) SetPriority(p int) { f.t.SetFilePriority(f.index, p) }

// BytesCompleted is how much of the file is had and checked.
func (f *File) BytesCompleted() int64 {
	t := f.t
	t.mu.Lock()
	defer t.mu.Unlock()
	var n int64
	b, e := t.info.FilePieces(f.index)
	for i := b; i < e; i++ {
		if !t.pieces[i].have {
			continue
		}
		s := max(f.offset, t.info.PieceOffset(i))
		end := min(f.offset+f.length, t.info.PieceOffset(i)+t.info.PieceSize(i))
		if end > s {
			n += end - s
		}
	}
	return n
}

// NewReader returns a Reader of the file.
func (f *File) NewReader() *Reader { return f.t.newReader(f.offset, f.length) }

// SetFilePriority sets the priority of file i of Info.FileList.
func (t *Torrent) SetFilePriority(i, p int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.info == nil || i < 0 || i >= len(t.filePrio) {
		return
	}
	t.filePrio[i] = min(max(p, PriorityNone), PriorityHigh)
	t.recomputePrioritiesLocked()
	for _, pc := range t.peers {
		t.updateInterestLocked(pc)
		t.fillRequestsLocked(pc)
	}
}

// Stats are a torrent's numbers at a moment.
type Stats struct {
	State State
	// Size is the torrent's length, Wanted the bytes of the pieces it
	// wants, Completed those of them had and checked.
	Size, Wanted, Completed int64
	// Progress is Completed over Wanted.
	Progress float64
	// Pieces is the number of pieces, PiecesHave how many are had.
	Pieces, PiecesHave int
	// Downloaded and Uploaded are payload bytes since the torrent was
	// added; the rates are bytes per second, smoothed over seconds.
	Downloaded, Uploaded       int64
	DownloadRate, UploadRate   float64
	Peers, Seeders, Candidates int
	HalfOpen                   int
	// HashFails counts the pieces that failed their check, Wasted the
	// bytes received that were not needed or failed.
	HashFails int
	Wasted    int64
}

// Stats returns the torrent's numbers.
func (t *Torrent) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := Stats{
		State: t.state, Downloaded: t.downloaded.Load(), Uploaded: t.uploaded.Load(),
		DownloadRate: t.downRate.perSec, UploadRate: t.upRate.perSec,
		Peers: len(t.peers), Candidates: len(t.pool), HalfOpen: t.halfOpen,
		HashFails: t.hashFails, Wasted: t.wasted,
	}
	for _, pc := range t.peers {
		if pc.seed {
			s.Seeders++
		}
	}
	if t.info != nil {
		s.Size = t.info.TotalLength()
		s.Pieces = len(t.pieces)
		s.PiecesHave = t.haveCount
		for i := range t.pieces {
			ps := &t.pieces[i]
			if ps.prio > PriorityNone {
				n := t.info.PieceSize(i)
				s.Wanted += n
				if ps.have {
					s.Completed += n
				}
			}
		}
		if s.Wanted > 0 {
			s.Progress = float64(s.Completed) / float64(s.Wanted)
		} else {
			s.Progress = 1
		}
	}
	return s
}

// PeerInfo describes a connected peer.
type PeerInfo struct {
	Addr       netip.AddrPort
	ID         [20]byte
	Client     string
	Outgoing   bool
	UTP        bool
	Encrypted  bool
	Seed       bool
	Pieces     int
	Downloaded int64
	Uploaded   int64
	// DownloadRate and UploadRate are bytes per second.
	DownloadRate, UploadRate float64
	AmChoking, PeerChoking   bool
	Snubbed                  bool
}

// Peers returns the connected peers.
func (t *Torrent) Peers() []PeerInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]PeerInfo, 0, len(t.peers))
	for _, pc := range t.peers {
		pi := PeerInfo{
			Addr: pc.addr, ID: pc.c.PeerID(), Outgoing: pc.outgoing, UTP: pc.utp,
			Encrypted: pc.c.Encrypted(), Seed: pc.seed,
			Downloaded: pc.downTotal, Uploaded: pc.upTotal,
			DownloadRate: pc.down.perSec, UploadRate: pc.up.perSec,
			AmChoking: pc.c.AmChoking(), PeerChoking: pc.c.PeerChoking(), Snubbed: pc.snubbed,
		}
		if h := pc.c.RemoteExtensions(); h != nil {
			pi.Client = h.Version
		}
		if pc.bits != nil {
			pi.Pieces = pc.bits.Count()
		}
		out = append(out, pi)
	}
	return out
}

// HaveAll reports every piece had: the torrent is a full seed.
func (t *Torrent) HaveAll() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.info != nil && t.haveCount == len(t.pieces)
}

// HavePiece reports piece i had and checked.
func (t *Torrent) HavePiece(i int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.info != nil && i >= 0 && i < len(t.pieces) && t.pieces[i].have
}

// rate is a smoothed bytes-per-second measure.
type rate struct {
	cur    int64
	perSec float64
}

func (r *rate) add(n int) { r.cur += int64(n) }

// tick folds what was added during dt into the rate.
func (r *rate) tick(dt time.Duration) {
	if dt <= 0 {
		return
	}
	inst := float64(r.cur) / dt.Seconds()
	r.perSec = 0.6*r.perSec + 0.4*inst
	if r.perSec < 1 {
		r.perSec = 0
	}
	r.cur = 0
}
