package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"reflect"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
)

// crypto/tls keeps two buffers in each Conn that it grows as it needs them and
// never gives back: rawInput, the ciphertext it has been handed and not yet
// decrypted, and hand, the handshake messages it has read and not yet
// processed. Both grow to fit the handshake's largest flight, and both are
// empty once it is over, hand for good and rawInput until the next record;
// across many connections, what they keep is a large part of what a
// connection costs. crypto/tls offers no way to give them back, so this file
// reaches them by name.
//
// rawInputOffset and handOffset are where the two sit in a Conn, or zero when
// this build of crypto/tls has no bytes.Buffer by that name, in which case
// they are left alone. A bytes.Buffer's zero value is an empty buffer, which
// crypto/tls grows again from nothing when it next needs one.
var rawInputOffset, handOffset = connBufferOffsets()

func connBufferOffsets() (rawInput, hand uintptr) {
	conn := reflect.TypeFor[stdtls.Conn]()
	buffer := reflect.TypeFor[bytes.Buffer]()
	offset := func(name string) uintptr {
		f, ok := conn.FieldByName(name)
		if !ok || f.Type != buffer {
			return 0
		}
		return f.Offset
	}
	return offset("rawInput"), offset("hand")
}

// connBuffer is the buffer at offset in t's Conn, or nil. Only whoever holds
// the Conn's reading side may touch it: the handshake while it runs, and a
// drain, under readMu, afterwards.
func (t *layer) connBuffer(offset uintptr) *bytes.Buffer {
	if offset == 0 {
		return nil
	}
	return (*bytes.Buffer)(unsafe.Add(unsafe.Pointer(t.conn), offset))
}

// releaseHandshakeBuffers empties what the handshake grew, once it is over.
// rawInput holds bytes only if the peer sent a partial record behind its last
// handshake message, and is kept then.
func (t *layer) releaseHandshakeBuffers() {
	if hand := t.connBuffer(handOffset); hand != nil && hand.Len() == 0 {
		*hand = bytes.Buffer{}
	}
	if raw := t.connBuffer(rawInputOffset); raw != nil && raw.Len() == 0 {
		*raw = bytes.Buffer{}
	}
}

// lendRawInput has a drain lend crypto/tls a buffer from the pool for
// rawInput, and take it back once crypto/tls has decrypted all of it, so that
// a connection between rounds keeps none.
var lendRawInput = true

// rawInputSize holds the largest record crypto/tls reads, 16 KiB of plaintext
// and its overhead, with the room it asks for beyond what it needs before it
// reads, so that it never outgrows the buffer it is lent.
const rawInputSize = 16<<10 + 2048 + 5 + 512

// lendInput lends rawInput a buffer if it has none.
func (t *layer) lendInput() {
	raw := t.connBuffer(rawInputOffset)
	if raw == nil || raw.Cap() != 0 {
		return
	}
	*raw = *bytes.NewBuffer(bufferpool.Get(rawInputSize)[:0])
}

// reclaimInput takes rawInput's buffer back to the pool, unless it still holds
// a record crypto/tls has only part of, which waits there for the next round.
func (t *layer) reclaimInput() {
	raw := t.connBuffer(rawInputOffset)
	if raw == nil || raw.Len() != 0 {
		return
	}
	raw.Reset()
	buf := raw.AvailableBuffer()
	*raw = bytes.Buffer{}
	bufferpool.Put(buf)
}
