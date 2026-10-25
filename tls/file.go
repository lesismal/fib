package tls

import (
	"errors"
	"sync/atomic"
	"syscall"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// A file a connection sends with SendFile goes one of two ways once the
// handshake is done and this package protects the records (fast.go).
//
// Where the kernel has TLS, which is Linux's with the suite one of AES-GCM's,
// the first file the connection sends hands the sealing of everything it
// sends from then on to the kernel, and the file goes by sendfile, from the
// page cache to the socket, sealed on its way. The switch is made at a moment
// when nothing sealed here is still queued, which the connection tells
// (fib.Connection.ControlIdle); a file sent at another moment is sealed here,
// and the next one tries again.
//
// Otherwise the file is sealed here, a chunk at a time as the socket takes
// it, by the connection's flush (fib.FileSealer): the records it will take
// are set aside in the sequence when it is queued, so that what is sent after
// it can be sealed at once behind them, and each chunk is sealed under them
// as it is read. Its chunks are a whole number of records' plaintext, so every
// record but the last is full and the count is known from the length alone.

// kernelTLSMissing records that the kernel has no TLS to give, or will not
// give it to this process, so that no connection asks again.
var kernelTLSMissing atomic.Bool

// fileSealer seals a file's records under keys of its own, from the sequence
// number set aside for its first record up to end.
type fileSealer struct {
	keys *recordKeys
	end  uint64
}

var errFileOverrun = errors.New("fib: tls file ran past its records")

// Seal seals plain into records, at most maxPlaintext bytes each, appended to
// dst.
func (s *fileSealer) Seal(dst, plain []byte) ([]byte, error) {
	for len(plain) > 0 {
		if s.keys.seq >= s.end {
			return dst, errFileOverrun
		}
		n := min(len(plain), maxPlaintext)
		var err error
		if dst, err = s.keys.seal(dst, recordTypeApplicationData, plain[:n], nil); err != nil {
			return dst, err
		}
		plain = plain[n:]
	}
	return dst, nil
}

// SealedLen is n and the most each of its records adds.
func (s *fileSealer) SealedLen(n int) int {
	return n + max(1, (n+maxPlaintext-1)/maxPlaintext)*s.keys.overhead()
}

// CanSendFile reports whether the layer protects the records itself, its
// handshake done, so that a file can be sealed as it goes; see fib.FileLayer.
// A connection that stays with crypto/tls, for a suite this package does not
// implement, cannot.
func (t *layer) CanSendFile() bool {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	return t.canSendFileLocked()
}

func (t *layer) canSendFileLocked() bool {
	return t.ready && !t.failed && t.tx != nil && t.native.Load() == nil
}

// SendFile sends count bytes of f from offset behind what was sent before it:
// by sendfile, with the kernel sealing them, or sealed here as the socket
// takes them. A file that cannot be queued once its records have been set
// aside leaves a gap in the sequence the peer cannot read past, so it ends the
// connection.
func (t *layer) SendFile(f fib.File, offset, count int64) error {
	t.wmu.Lock()
	defer t.wmu.Unlock()
	if t.closeAfterSend || t.isClosed() || !t.canSendFileLocked() {
		return syscall.EPIPE
	}
	if t.kernelSealsLocked() {
		if len(t.held) > 0 {
			if err := t.releaseHeldLocked(); err != nil {
				return err
			}
		}
		return t.c.SendFileRaw(f, offset, count, nil)
	}
	if len(t.held) > 0 {
		if err := t.releaseHeldLocked(); err != nil {
			return err
		}
	}
	records := uint64((count + maxPlaintext - 1) / maxPlaintext)
	if t.tx.seq > ^uint64(0)-records {
		return errSequenceExhausted
	}
	sealer := &fileSealer{keys: t.tx.fileKeys(t.tx.seq), end: t.tx.seq + records}
	t.tx.seq += records
	if err := t.c.SendFileRaw(f, offset, count, sealer); err != nil {
		t.failed = true
		t.c.CloseWithError(err)
		return err
	}
	return nil
}

// kernelSealsLocked reports whether the kernel seals what the connection
// sends, handing the sealing to it first if it has not been yet, the kernel
// can, and nothing sealed here is still queued. The caller holds wmu.
func (t *layer) kernelSealsLocked() bool {
	switch t.ktls {
	case ktlsOn:
		return true
	case ktlsOff:
		return false
	}
	if t.owner != nil && t.owner.NoKernelTLS || !kernelTLSFits(t.tx) {
		t.ktls = ktlsOff
		return false
	}
	ran, err := t.c.ControlIdle(func(fd uintptr) error { return startKernelTLS(fd, t.tx) })
	if !ran {
		// Something sealed here is still on its way; the next file tries
		// again.
		return false
	}
	if err != nil {
		t.ktls = ktlsOff
		return false
	}
	t.ktls = ktlsOn
	return true
}

// kernelSendLocked sends first followed by second as a record of typ for the
// kernel to seal: application data as any other send, which the kernel makes
// records of, and anything else as a record of its type. A KeyUpdate the
// kernel owes the peer goes first. The caller holds wmu.
func (t *layer) kernelSendLocked(typ byte, first, second []byte) error {
	if t.rekeyOwed {
		if err := t.kernelRekeyLocked(); err != nil {
			return err
		}
	}
	if typ == recordTypeApplicationData {
		return t.c.SendRawPooled(bufferpool.Join(nil, first, second))
	}
	if len(second) == 0 {
		return t.c.SendRawRecord(typ, first)
	}
	joined := bufferpool.Join(nil, first, second)
	defer bufferpool.Put(joined)
	return t.c.SendRawRecord(typ, joined)
}

// errRekeyRefused is what ends a connection whose kernel cannot move to new
// keys after the KeyUpdate that announced them went out.
var errRekeyRefused = errors.New("fib: tls: the kernel refused the next keys")

// kernelRekeyLocked answers the peer's request for a KeyUpdate where the
// kernel seals: the KeyUpdate goes to the socket, and the kernel takes the
// next keys, both at a moment when nothing is queued, so that no record
// sealed under the old keys follows it. While something is, it stays owed,
// for the next send to try again: the peer cannot tell that sends went ahead
// of it, which were on their way when its request came. A kernel that
// cannot take new keys, as Linux before 6.14 cannot, ends the connection.
// The caller holds wmu.
func (t *layer) kernelRekeyLocked() error {
	next, err := t.tx.next()
	if err != nil {
		t.c.CloseWithError(err)
		return err
	}
	var sent bool
	ran, err := t.c.ControlIdle(func(fd uintptr) error {
		var serr error
		sent, serr = sendKernelRecord(fd, recordTypeHandshake, []byte{typeKeyUpdate, 0, 0, 1, 0})
		if !sent {
			return serr
		}
		if err := startKernelTLS(fd, next); err != nil {
			return errRekeyRefused
		}
		return nil
	})
	if !ran || err == nil && !sent {
		return nil
	}
	if err != nil {
		t.failed = true
		t.c.CloseWithError(err)
		return err
	}
	t.tx = next
	t.rekeyOwed = false
	return nil
}
