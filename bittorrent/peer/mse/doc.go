// Package mse implements Message Stream Encryption, also called Protocol
// Encryption (MSE/PE): the obfuscated handshake Vuze and libtorrent
// introduced for the BitTorrent peer wire protocol, which every major
// client speaks.
//
// The initiator A and the receiver B agree on a secret S by a 768-bit
// Diffie-Hellman exchange (Ya, Yb, each followed by up to 512 bytes of
// random padding). A then proves which torrent it wants without naming it:
// it sends HASH('req1', S) to mark where its padding ends, and
// HASH('req2', SKEY) xor HASH('req3', S), where SKEY is the info hash; a
// receiver serving many torrents looks that up in a KeyTable. Both sides
// then RC4-encrypt, with keys HASH('keyA'|'keyB', S, SKEY) whose first 1024
// bytes of keystream are discarded, the rest of the handshake: a zero
// verification constant VC, A's crypto_provide and B's crypto_select
// (plaintext 0x01, RC4 0x02), more padding, and A's initial payload IA,
// which is usually the BitTorrent handshake. With RC4 selected the whole
// stream stays encrypted; with plaintext only the handshake was.
//
// Initiator and Receiver are the two roles without any I/O: Feed takes
// what the peer sent and returns what to send in answer, so that the
// handshake runs on fib's event-driven connections as well as anywhere
// else. A Receiver also recognizes a plaintext BitTorrent handshake, the
// first 20 bytes being "\x13BitTorrent protocol", and accepts or refuses
// it as its Policy says. Client and Server run them over a net.Conn and
// return a Conn that carries the payload.
//
//	var keys mse.KeyTable
//	keys.Add(infoHash)
//	c, err := mse.Server(conn, &keys, mse.Prefer, 30*time.Second)
//	// c.Result.SKey is the torrent, c reads the BitTorrent handshake.
//
//	c, err := mse.Client(conn, infoHash, mse.Both, btHandshake, 30*time.Second)
package mse
