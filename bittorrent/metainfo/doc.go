// Package metainfo reads and makes .torrent files and magnet links: v1
// (BEP 3), v2 (BEP 52) and hybrid torrents, with the info hashes that name
// them and the piece and file layout a client stores them by.
//
// A MetaInfo is a .torrent file; its Info is the info dictionary, kept as
// the bytes it was encoded as, since the info hash is the SHA-1 (v1) or
// SHA-256 (v2) of exactly those bytes. Parse and ParseInfo check what a
// client relies on: piece hashes that match the length, file paths that
// cannot escape the download directory, and in a hybrid torrent v1 and v2
// file lists that agree.
//
// Info.FileList gives the files in order with their offsets in the
// torrent's data, padding files (BEP 47) included; in a v2-only torrent
// each file starts at a piece boundary. Info.PieceSize, PieceSegments and
// Segments map pieces and byte ranges onto files, and VerifyPiece checks a
// piece against the v1 SHA-1, the v2 merkle tree (through the file's piece
// layer, or its pieces root for a file of one piece), or both. The merkle
// helpers (MerkleRoot, PadHash, LayerRoot, VerifyProof) are those the
// hash request messages of BEP 52 need.
//
// A Builder hashes files from an fs.FS or the operating system into a v1,
// v2 or hybrid torrent with trackers (BEP 12), web seeds (BEP 19), DHT
// nodes and the private flag (BEP 27).
//
// Magnet links (BEP 9) are parsed and formatted with v1 (btih, in hex or
// base32) and v2 (btmh) hashes, display name, trackers, web seeds, peers
// (x.pe), file selection (so, BEP 53) and length (xl).
//
//	mi, err := metainfo.Load("file.torrent")
//	fmt.Println(mi.Info.InfoHashV1(), mi.Magnet())
//	for i := 0; i < mi.Info.PieceCount(); i++ {
//		for _, seg := range mi.Info.PieceSegments(i) {
//			f := mi.Info.FileList()[seg.File]
//			// read seg.Length bytes of f at seg.Offset ...
//		}
//		err := mi.Info.VerifyPiece(i, data, mi.PieceLayers)
//	}
//
//	b := metainfo.Builder{Trackers: [][]string{{"udp://tracker.example:6969"}}}
//	mi, err := b.BuildPath("dir")
//	out, err := bencode.Marshal(mi)
package metainfo
